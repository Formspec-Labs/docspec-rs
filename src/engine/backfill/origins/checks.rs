//! Bounded metadata qualification of immutable original scalar executions.

use super::*;
use iceberg::spec::{Schema, TableMetadata};
use iceberg::table::Table;

struct Compiled {
    dependencies: String,
    native: Schema,
    native_field_ids: BTreeMap<i32, i32>,
}

struct CheckedFile {
    completion: FileCompletion,
    files: Arc<Vec<DataFile>>,
    schema: Schema,
}

struct CheckedGroup {
    rows: u64,
    objects: Vec<ObjectRef>,
    dependencies: String,
}

#[derive(Debug, Serialize)]
struct SourceFile {
    bytes: u64,
    rows: u64,
}

/// Parsed and qualified immutable facts shared across one operation's scalar origin checks.
#[derive(Default)]
pub(in crate::engine) struct ScalarOriginChecks {
    identities: BTreeMap<String, ObjectRef>,
    originals: BTreeMap<String, Arc<ScalarOrigins>>,
    declarations: BTreeMap<String, Arc<OriginalDeclaration>>,
    schemas: BTreeMap<String, Arc<TableMetadata>>,
    source_files: BTreeMap<String, Arc<BTreeMap<String, SourceFile>>>,
    compiled: BTreeMap<(String, String), Arc<Compiled>>,
    associations: BTreeMap<(String, String), Arc<Compiled>>,
    files: BTreeMap<String, CheckedFile>,
    verified: BTreeMap<String, CheckedGroup>,
    qualified: BTreeSet<String>,
    bytes: usize,
    // Each parsing worker also holds its permit if the caller is canceled.
    memory: Vec<Arc<tokio::sync::OwnedSemaphorePermit>>,
}

fn check_identity(identities: &mut BTreeMap<String, ObjectRef>, object: &ObjectRef) -> Result<()> {
    match identities.get(&object.path) {
        Some(previous) if previous != object => Err(invalid_group(
            "conflicting object identity for scalar origin artifact path",
        )),
        Some(_) => Ok(()),
        None => {
            identities.insert(object.path.clone(), object.clone());
            Ok(())
        }
    }
}

fn admit_bytes(
    total_bytes: &mut usize,
    memory_permits: &mut Vec<Arc<tokio::sync::OwnedSemaphorePermit>>,
    bytes: u64,
    budget: &Budget,
) -> Result<Arc<tokio::sync::OwnedSemaphorePermit>> {
    let bytes = usize::try_from(bytes).map_err(|_| capacity())?;
    *total_bytes = total_bytes.checked_add(bytes).ok_or_else(capacity)?;
    if *total_bytes > budget.limits.max_metadata_bytes {
        return Err(capacity());
    }
    // JSON trees, decoded Avro records, map entries and compiled schema facts
    // coexist. Charge their expansion as well as the separately held input buffer.
    let memory = Arc::new(budget.reserve(bytes.saturating_mul(8).saturating_add(4096))?);
    memory_permits.push(memory.clone());
    Ok(memory)
}

async fn cached_manifest<T: serde::de::DeserializeOwned + Send + 'static>(
    identities: &mut BTreeMap<String, ObjectRef>,
    cache: &mut BTreeMap<String, Arc<T>>,
    total_bytes: &mut usize,
    memory_permits: &mut Vec<Arc<tokio::sync::OwnedSemaphorePermit>>,
    object: &ObjectRef,
    budget: &Budget,
) -> Result<Arc<T>> {
    check_identity(identities, object)?;
    if let Some(value) = cache.get(&object.path) {
        return Ok(value.clone());
    }
    let memory = admit_bytes(total_bytes, memory_permits, object.bytes, budget)?;
    let value: Arc<T> = Arc::new(read(object, budget, &mut 0, &memory).await?);
    cache.insert(object.path.clone(), value.clone());
    Ok(value)
}

impl ScalarOriginChecks {
    /// Checks supplemental fields using the same admitted original metadata cache.
    pub(in crate::engine) async fn qualification(
        &mut self,
        proof: &DependencyQualification,
        binding: &ComputedGroup,
        source: &TablePin,
        group: &TablePin,
        budget: &Budget,
    ) -> Result<()> {
        crate::engine::corrections::check_association(proof, binding, source)?;
        self.check(
            &proof.origins,
            ScalarOriginBinding {
                source,
                group,
                definition: &binding.definition,
                input_fields: &binding.input_fields,
                native_field_ids: &binding.native_field_ids,
            },
            budget,
        )
        .await?;
        self.correspondence(proof, source, budget).await
    }

    pub(in crate::engine) async fn correspondence(
        &mut self,
        proof: &DependencyQualification,
        source: &TablePin,
        budget: &Budget,
    ) -> Result<()> {
        crate::engine::corrections::validate_qualification(proof)?;
        if proof.target != source.into() {
            return Err(invalid_group(
                "dependency qualification target does not match source snapshot",
            ));
        }
        let key = crate::engine::corrections::qualification_key(proof)?;
        let originals = self.original(&proof.origins, budget).await?;
        // No file generations means there is no retained pre-correction context
        // from which to establish these previously undeclared requirements.
        if originals.files.is_empty() {
            return Err(crate::engine::corrections::unknown());
        }
        // A cached immutable proof never qualifies a different caller's object
        // list, including a target whose schema metadata differs from the originals.
        let objects = source_objects(source);
        if objects.len() != source.objects.len()
            || originals.files.iter().any(|original| {
                objects.get(original.completion.source.path.as_str())
                    != Some(&&original.completion.source)
            })
        {
            return Err(crate::engine::corrections::mismatch());
        }
        if self.qualified.contains(&key) {
            return Ok(());
        }
        let target = self.table(&proof.target, budget).await?;
        let files = self.source_files(&target, budget).await?;
        let mut remaining = files
            .iter()
            .map(|(path, file)| (path.as_str(), file))
            .collect::<BTreeMap<_, _>>();
        for original in &originals.files {
            let file = &original.completion;
            let selected = remaining
                .remove(file.source.path.as_str())
                .ok_or_else(crate::engine::corrections::mismatch)?;
            if selected.bytes != file.source.bytes || selected.rows != file.rows {
                return Err(crate::engine::corrections::mismatch());
            }
            self.correction_file(original, target.metadata().current_schema(), proof, budget)
                .await?;
        }
        if !remaining.is_empty() {
            return Err(crate::engine::corrections::mismatch());
        }
        self.admit(key.len() as u64, budget)?;
        self.qualified.insert(key);
        Ok(())
    }

    async fn correction_file(
        &mut self,
        original: &FileOrigin,
        target: &Schema,
        proof: &DependencyQualification,
        budget: &Budget,
    ) -> Result<()> {
        use crate::engine::corrections::{mismatch, unknown};
        if original.generation.revision <= 0 || original.generation.revision >= proof.first_revision
        {
            return Err(mismatch());
        }
        let execution = original.completion.execution.as_ref().ok_or_else(unknown)?;
        let declaration = self.declaration(&execution.declaration, budget).await?;
        if declaration.processor != PROCESSOR {
            return Err(unknown());
        }
        let metadata = original
            .completion
            .source_metadata
            .as_ref()
            .ok_or_else(unknown)?;
        let metadata = self.metadata(metadata, budget).await?;
        for id in &proof.fields {
            let old = expressions::field_interpretation(metadata.current_schema(), *id, true)
                .map_err(|_| unknown())?;
            let current =
                expressions::field_interpretation(target, *id, true).map_err(|_| mismatch())?;
            if !matches!(old.field_type.as_ref(), iceberg::spec::Type::Primitive(_))
                || old != current
            {
                return Err(mismatch());
            }
        }
        Ok(())
    }

    fn identity(&mut self, object: &ObjectRef) -> Result<()> {
        check_identity(&mut self.identities, object)
    }

    fn admit(
        &mut self,
        bytes: u64,
        budget: &Budget,
    ) -> Result<Arc<tokio::sync::OwnedSemaphorePermit>> {
        admit_bytes(&mut self.bytes, &mut self.memory, bytes, budget)
    }

    async fn original(
        &mut self,
        object: &ObjectRef,
        budget: &Budget,
    ) -> Result<Arc<ScalarOrigins>> {
        cached_manifest(
            &mut self.identities,
            &mut self.originals,
            &mut self.bytes,
            &mut self.memory,
            object,
            budget,
        )
        .await
    }

    async fn declaration(
        &mut self,
        object: &ObjectRef,
        budget: &Budget,
    ) -> Result<Arc<OriginalDeclaration>> {
        cached_manifest(
            &mut self.identities,
            &mut self.declarations,
            &mut self.bytes,
            &mut self.memory,
            object,
            budget,
        )
        .await
    }

    async fn metadata(
        &mut self,
        object: &ObjectRef,
        budget: &Budget,
    ) -> Result<Arc<TableMetadata>> {
        cached_manifest(
            &mut self.identities,
            &mut self.schemas,
            &mut self.bytes,
            &mut self.memory,
            object,
            budget,
        )
        .await
    }

    async fn table(&mut self, reference: &NativeSnapshotRef, budget: &Budget) -> Result<Table> {
        let metadata = self.metadata(&reference.metadata, budget).await?;
        if metadata.current_snapshot_id() != reference.snapshot
            || metadata.current_schema_id() != reference.schema_id
        {
            return Err(invalid_group(
                "table metadata snapshot or schema ID does not match snapshot reference",
            ));
        }
        let objects = storage::Objects::for_path(&reference.metadata.path);
        Ok(Table::builder()
            .identifier(iceberg::TableIdent::from_strs(["scalar", "originals"])?)
            .metadata(metadata)
            .metadata_location(&reference.metadata.path)
            .file_io(objects.file_io())
            .runtime(iceberg::Runtime::try_current()?)
            .readonly(true)
            .build()?)
    }

    async fn source_files(
        &mut self,
        table: &Table,
        budget: &Budget,
    ) -> Result<Arc<BTreeMap<String, SourceFile>>> {
        // Callers first resolve the immutable identity and snapshot/schema through
        // self.table. The cache retains only facts these scalar checks consume.
        let key = table.metadata_location_result()?;
        if let Some(files) = self.source_files.get(key) {
            return Ok(files.clone());
        }
        let inventory = storage::snapshot_files(table, budget).await?;
        let files = inventory
            .data
            .iter()
            .map(|file| {
                (
                    file.file_path().to_owned(),
                    SourceFile {
                        bytes: file.file_size_in_bytes(),
                        rows: file.record_count(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        if files.len() != inventory.data.len() {
            return Err(invalid_group(
                "source snapshot inventory contains duplicate data file paths",
            ));
        }
        let size = inputs::metadata_size(&(&key, &files), budget.limits.max_metadata_bytes)?;
        self.admit(size as u64, budget)?;
        let files = Arc::new(files);
        self.source_files.insert(key.to_owned(), files.clone());
        Ok(files)
    }

    async fn compile(
        &mut self,
        declaration: &ObjectRef,
        schema: &ObjectRef,
        budget: &Budget,
    ) -> Result<Arc<Compiled>> {
        self.identity(declaration)?;
        self.identity(schema)?;
        let key = (declaration.path.clone(), schema.path.clone());
        if let Some(value) = self.compiled.get(&key) {
            return Ok(value.clone());
        }
        let declaration = self.declaration(declaration, budget).await?;
        if declaration.processor != PROCESSOR {
            return Err(invalid_group(
                "original scalar declaration processor does not match current processor",
            ));
        }
        let schema = self.metadata(schema, budget).await?;
        let expressions = Expressions::compile(
            &budget.context(),
            schema.current_schema(),
            &declaration.definition,
            Some(&declaration.input_fields),
        )?;
        let value = Arc::new(Compiled {
            dependencies: expressions.dependencies,
            native: expressions.native,
            native_field_ids: expressions.native_field_ids,
        });
        let size = inputs::metadata_size(
            &(&value.dependencies, &value.native, &value.native_field_ids),
            budget.limits.max_metadata_bytes,
        )?;
        self.admit(size as u64, budget)?;
        self.compiled.insert(key, value.clone());
        Ok(value)
    }

    pub(in crate::engine) async fn file(
        &mut self,
        completion: &FileCompletion,
        schema: &Schema,
        budget: &Budget,
    ) -> Result<Arc<Vec<DataFile>>> {
        self.identity(&completion.source)?;
        self.identity(&completion.descriptor)?;
        for output in &completion.outputs {
            self.identity(output)?;
        }
        if let Some(previous) = self.files.get(&completion.descriptor.path) {
            let old = &previous.completion;
            if old.source != completion.source
                || old.source_metadata != completion.source_metadata
                || old.rows != completion.rows
                || old.outputs != completion.outputs
                || old.execution != completion.execution
                || &previous.schema != schema
            {
                return Err(invalid_group(
                    "cached file completion descriptor disagrees with current file completion",
                ));
            }
            return Ok(previous.files.clone());
        }
        self.admit(completion.descriptor.bytes, budget)?;
        let files = Arc::new(read_completion(completion, schema, budget).await?);
        self.files.insert(
            completion.descriptor.path.clone(),
            CheckedFile {
                completion: completion.clone(),
                files: files.clone(),
                schema: schema.clone(),
            },
        );
        Ok(files)
    }

    /// Checks one immutable scalar-origins descriptor against its selected source and field-group binding.
    pub(in crate::engine) async fn check(
        &mut self,
        object: &ObjectRef,
        binding: ScalarOriginBinding<'_>,
        budget: &Budget,
    ) -> Result<()> {
        let ScalarOriginBinding {
            source,
            group,
            definition,
            input_fields,
            native_field_ids,
        } = binding;
        let originals = self.original(object, budget).await?;
        // Association is caller-specific even when immutable metadata was checked.
        if object.owner != source.metadata.owner
            || object.owner != group.metadata.owner
            || originals.processor != PROCESSOR
            || &originals.definition != definition
            || &originals.input_fields != input_fields
            || source.rows != group.rows
        {
            return Err(invalid_group(
                "scalar origins owner, processor, definition, input fields, or row count disagrees with binding",
            ));
        }
        self.identity(&originals.dependencies.source.metadata)?;
        self.identity(&originals.dependencies.output.metadata)?;
        if let Some(logical) = &originals.dependencies.source_logical_metadata {
            self.identity(logical)?;
        }
        preparation::check_group_dependencies(
            "scalar origins",
            Some(&originals.dependencies),
            source,
            group,
        )?;
        let association = (object.path.clone(), source.metadata.path.clone());
        self.identity(&source.metadata)?;
        let current_source = self.metadata(&source.metadata, budget).await?;
        if current_source.current_schema_id() != source.schema_id
            || current_source.current_snapshot_id() != source.snapshot
        {
            return Err(invalid_group(
                "source metadata current schema or snapshot ID disagrees with source pin",
            ));
        }
        let expected = if let Some(expected) = self.associations.get(&association) {
            expected.clone()
        } else {
            let expressions = Expressions::compile(
                &budget.context(),
                current_source.current_schema(),
                definition,
                Some(input_fields),
            )?;
            let compiled = Arc::new(Compiled {
                dependencies: expressions.dependencies,
                native: expressions.native,
                native_field_ids: expressions.native_field_ids,
            });
            let size = inputs::metadata_size(
                &(
                    &compiled.dependencies,
                    &compiled.native,
                    &compiled.native_field_ids,
                ),
                budget.limits.max_metadata_bytes,
            )?;
            self.admit(size as u64, budget)?;
            self.associations.insert(association, compiled.clone());
            compiled
        };
        let output_metadata = self
            .metadata(&originals.dependencies.output.metadata, budget)
            .await?;
        if &expected.native_field_ids != native_field_ids
            || expected.native != **output_metadata.current_schema()
            || output_metadata.current_snapshot_id() != group.snapshot
            || output_metadata.current_schema_id() != group.schema_id
        {
            return Err(invalid_group(
                "output group schema, field IDs, or snapshot disagrees with expected compilation",
            ));
        }
        // Reusing parsed facts must not trust a different caller's object list or
        // row counts merely because its native snapshot fields still match.
        let objects = source_objects(group);
        let source_objects = (originals.dependencies.source.metadata == source.metadata)
            .then(|| source_objects(source));
        if objects.len() != group.objects.len()
            || source_objects
                .as_ref()
                .is_some_and(|objects| objects.len() != source.objects.len())
        {
            return Err(invalid_group(
                "source or group pin contains duplicate object paths",
            ));
        }
        for original in &originals.files {
            if source_objects.as_ref().is_some_and(|objects| {
                objects.get(original.completion.source.path.as_str())
                    != Some(&&original.completion.source)
            }) || original
                .completion
                .outputs
                .iter()
                .any(|reference| objects.get(reference.path.as_str()) != Some(&reference))
            {
                return Err(invalid_group(
                    "scalar origin file completion references an object absent from source or group pin",
                ));
            }
        }
        if let Some(checked) = self.verified.get(&object.path) {
            return if checked.rows == group.rows
                && checked.objects == group.objects
                && checked.dependencies == expected.dependencies
            {
                Ok(())
            } else {
                Err(invalid_group(
                    "cached verified scalar group disagrees with caller row count, objects, or dependencies",
                ))
            };
        }
        let output = self.table(&originals.dependencies.output, budget).await?;
        let captured = self.table(&originals.dependencies.source, budget).await?;
        let captured_expressions = Expressions::compile(
            &budget.context(),
            captured.metadata().current_schema(),
            definition,
            Some(input_fields),
        )?;
        if captured_expressions.dependencies != expected.dependencies
            || captured_expressions.native != expected.native
        {
            return Err(invalid_group(
                "captured source schema compiles to different dependencies or native schema",
            ));
        }
        let source_files = self.source_files(&captured, budget).await?;
        let output_files = storage::snapshot_files(&output, budget).await?;
        if objects.get(group.metadata.path.as_str()) != Some(&&group.metadata)
            || objects.len() != output_files.paths.len()
            || output_files
                .paths
                .iter()
                .any(|path| !objects.contains_key(path.as_str()))
        {
            return Err(invalid_group(
                "group pin objects do not match snapshot manifest and data file inventory",
            ));
        }
        let mut sources: BTreeMap<_, _> = source_files
            .iter()
            .map(|(path, file)| (path.as_str(), file))
            .collect();
        let mut outputs: BTreeMap<_, _> = output_files
            .data
            .iter()
            .map(|file| (file.file_path(), file))
            .collect();
        if outputs.len() != output_files.data.len() {
            return Err(invalid_group(
                "output snapshot inventory contains duplicate data file paths",
            ));
        }
        let mut rows = 0u64;
        let mut generations = BTreeSet::new();
        for original in &originals.files {
            tokio::task::consume_budget().await;
            let file = &original.completion;
            let captured_file = sources.remove(file.source.path.as_str()).ok_or_else(|| {
                invalid_group("scalar origin file is missing from captured source inventory")
            })?;
            if file.source.bytes != captured_file.bytes
                || file.rows != captured_file.rows
                || !generations.insert(original.generation.key.as_str())
                || original.generation.revision <= 0
            {
                return Err(invalid_group(
                    "scalar origin file bytes, rows, or generation revision is invalid or duplicated",
                ));
            }
            rows = rows
                .checked_add(file.rows)
                .ok_or_else(|| invalid_group("scalar origin file row counts overflow"))?;
            // An older completion stays explicitly unknown. Its output association
            // can still be checked; no later declaration is attributed to its work.
            if let Some(execution) = &file.execution {
                let source_metadata = file.source_metadata.as_ref().ok_or_else(|| {
                    invalid_group("scalar origin file execution is missing source_metadata")
                })?;
                let declaration = self.declaration(&execution.declaration, budget).await?;
                if declaration.definition != originals.definition
                    || declaration.input_fields != originals.input_fields
                    || execution.declaration.owner != object.owner
                {
                    return Err(invalid_group(
                        "file execution declaration disagrees with scalar origins definition or owner",
                    ));
                }
                let compiled = self
                    .compile(&execution.declaration, source_metadata, budget)
                    .await?;
                if compiled.dependencies != expected.dependencies
                    || compiled.native != expected.native
                    || &compiled.native_field_ids != native_field_ids
                {
                    return Err(invalid_group(
                        "file execution compiled schema or dependencies disagree with group binding",
                    ));
                }
            }
            let scope = original
                .generation
                .key
                .strip_prefix("phase/backfill/")
                .and_then(|rest| rest.split_once('/'))
                .and_then(|(scope, _)| scope.parse::<u64>().ok())
                .ok_or_else(|| {
                    invalid_group("scalar origin generation key has an invalid backfill prefix")
                })?;
            if original.generation.key != file_key(&file.source, &captured_expressions, scope)? {
                return Err(invalid_group(
                    "scalar origin generation key does not match expected file key",
                ));
            }
            for file in self.file(file, &expected.native, budget).await?.iter() {
                let selected = outputs.remove(file.file_path()).ok_or_else(|| {
                    invalid_group("completed scalar output file is missing from output snapshot")
                })?;
                if selected != file {
                    return Err(invalid_group(
                        "completed scalar output file descriptor disagrees with output snapshot entry",
                    ));
                }
            }
        }
        if !sources.is_empty() || !outputs.is_empty() || rows != group.rows {
            return Err(invalid_group(
                "scalar origins do not cover all source and output files or total row count",
            ));
        }
        let size = inputs::metadata_size(
            &(&group.objects, &expected.dependencies),
            budget.limits.max_metadata_bytes,
        )?;
        self.admit(size as u64, budget)?;
        self.verified.insert(
            object.path.clone(),
            CheckedGroup {
                rows,
                objects: group.objects.clone(),
                dependencies: expected.dependencies.clone(),
            },
        );
        Ok(())
    }
}

fn capacity() -> Error {
    Error::refusal(
        "metadata_capacity",
        "scalar original execution checks exceed metadata allowance",
    )
}

#[cfg(all(test, feature = "test-util"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cached_originals_reuse_metadata_and_recheck_caller_association() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(
            root.path(),
            Limits {
                compute_threads: 2,
                ..Limits::default()
            },
        )
        .await
        .unwrap();
        let owner = Id::new("originals").unwrap();
        let source = engine
            .write_frame(
                &owner,
                &root.path().join("objects/originals/source"),
                engine
                    .context()
                    .sql("SELECT CAST(NULL AS BIGINT) AS value WHERE FALSE")
                    .await
                    .unwrap(),
            )
            .await
            .unwrap()
            .table;
        let budget = &engine.inner.lifecycle.budget;
        let native = storage::open_pin(&source, budget).await.unwrap();
        let definition = GroupDefinition {
            group: Id::new("derived").unwrap(),
            definition: "v1".into(),
            fields: vec![ComputedField {
                id: 10,
                name: "answer".into(),
                expression: "value + 1".into(),
            }],
        };
        let expressions = Expressions::compile(
            &engine.context(),
            native.metadata().current_schema(),
            &definition,
            None,
        )
        .unwrap();
        let directory = root.path().join("objects/originals/derived");
        std::fs::create_dir_all(&directory).unwrap();
        let output = engine
            .create_output(&owner, &directory, expressions.native.clone())
            .await
            .unwrap();
        let group = storage::pin(&owner, &output, 0, budget, &[]).await.unwrap();
        let object = write(
            &owner,
            &directory.join("origins.json"),
            &ScalarOrigins {
                processor: PROCESSOR.into(),
                definition: definition.clone(),
                input_fields: expressions.input_fields.clone(),
                dependencies: GroupDependencies {
                    source: (&source).into(),
                    source_logical_metadata: source.logical_metadata.clone(),
                    output: (&group).into(),
                },
                files: vec![],
            },
            budget,
        )
        .unwrap();
        let mut checks = ScalarOriginChecks::default();
        checks
            .check(
                &object,
                ScalarOriginBinding {
                    source: &source,
                    group: &group,
                    definition: &definition,
                    input_fields: &expressions.input_fields,
                    native_field_ids: &expressions.native_field_ids,
                },
                budget,
            )
            .await
            .unwrap();
        let source_reference: NativeSnapshotRef = (&source).into();
        let cached_inventory = checks.source_files[&source.metadata.path].clone();
        let admitted = checks.bytes;
        let source_table = checks.table(&source_reference, budget).await.unwrap();
        assert!(Arc::ptr_eq(
            &checks.schemas[&source.metadata.path],
            &source_table.metadata_ref()
        ));
        let reused_inventory = checks.source_files(&source_table, budget).await.unwrap();
        assert!(Arc::ptr_eq(&cached_inventory, &reused_inventory));
        assert_eq!(
            checks.bytes, admitted,
            "source facts are admitted once per request"
        );
        let mut changed: serde_json::Value = serde_json::to_value(native.metadata()).unwrap();
        let schema = changed["schemas"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|schema| schema["schema-id"] == source.schema_id)
            .unwrap();
        let field = schema["fields"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|field| field["name"] == "value")
            .unwrap();
        field["initial-default"] = serde_json::json!(9);
        let mut changed_source = source.clone();
        changed_source.metadata = write(
            &owner,
            &directory.join("changed-source.json"),
            &changed,
            budget,
        )
        .unwrap();
        changed_source.logical_metadata = Some(source.metadata.clone());
        // A forged logical-equivalence marker must not let a changed read
        // default pass only because this origins object was checked earlier.
        assert_eq!(
            checks
                .check(
                    &object,
                    ScalarOriginBinding {
                        source: &changed_source,
                        group: &group,
                        definition: &definition,
                        input_fields: &expressions.input_fields,
                        native_field_ids: &expressions.native_field_ids,
                    },
                    budget
                )
                .await
                .unwrap_err()
                .code(),
            "invalid_group"
        );
        assert_eq!(
            ScalarOriginChecks::default()
                .check(
                    &object,
                    ScalarOriginBinding {
                        source: &changed_source,
                        group: &group,
                        definition: &definition,
                        input_fields: &expressions.input_fields,
                        native_field_ids: &expressions.native_field_ids,
                    },
                    budget
                )
                .await
                .unwrap_err()
                .code(),
            "invalid_group"
        );
        let retained_bytes = checks.bytes;
        for path in [&object.path, &source.metadata.path, &group.metadata.path] {
            std::fs::remove_file(path).unwrap();
        }
        checks
            .check(
                &object,
                ScalarOriginBinding {
                    source: &source,
                    group: &group,
                    definition: &definition,
                    input_fields: &expressions.input_fields,
                    native_field_ids: &expressions.native_field_ids,
                },
                budget,
            )
            .await
            .unwrap();
        assert_eq!(
            checks.bytes, retained_bytes,
            "cached metadata is neither reopened nor charged twice"
        );
        let mut wrong_source = source.clone();
        let mut wrong_group = group.clone();
        wrong_source.rows = 1;
        wrong_group.rows = 1;
        assert!(
            checks
                .check(
                    &object,
                    ScalarOriginBinding {
                        source: &wrong_source,
                        group: &wrong_group,
                        definition: &definition,
                        input_fields: &expressions.input_fields,
                        native_field_ids: &expressions.native_field_ids,
                    },
                    budget
                )
                .await
                .is_err()
        );
        wrong_group = group.clone();
        wrong_group.objects[0].digest = "0".repeat(64);
        assert!(
            checks
                .check(
                    &object,
                    ScalarOriginBinding {
                        source: &source,
                        group: &wrong_group,
                        definition: &definition,
                        input_fields: &expressions.input_fields,
                        native_field_ids: &expressions.native_field_ids,
                    },
                    budget
                )
                .await
                .is_err()
        );
        let mut wrong_binding = expressions.native_field_ids.clone();
        *wrong_binding.values_mut().next().unwrap() += 1;
        assert!(
            checks
                .check(
                    &object,
                    ScalarOriginBinding {
                        source: &source,
                        group: &group,
                        definition: &definition,
                        input_fields: &expressions.input_fields,
                        native_field_ids: &wrong_binding,
                    },
                    budget
                )
                .await
                .is_err()
        );
    }
}

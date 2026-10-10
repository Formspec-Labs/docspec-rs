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

/// Parsed and qualified immutable facts shared across one operation's checks.
#[derive(Default)]
pub(in crate::engine) struct Checks {
    identities: BTreeMap<String, ObjectRef>,
    originals: BTreeMap<String, Arc<ScalarOrigins>>,
    declarations: BTreeMap<String, Arc<OriginalDeclaration>>,
    schemas: BTreeMap<String, Arc<TableMetadata>>,
    compiled: BTreeMap<(String, String), Arc<Compiled>>,
    associations: BTreeMap<(String, String), Arc<Compiled>>,
    files: BTreeMap<String, CheckedFile>,
    verified: BTreeMap<String, CheckedGroup>,
    bytes: usize,
    // Each parsing worker also holds its permit if the caller is canceled.
    memory: Vec<Arc<tokio::sync::OwnedSemaphorePermit>>,
}

impl Checks {
    fn identity(&mut self, object: &ObjectRef) -> Result<()> {
        match self.identities.get(&object.path) {
            Some(previous) if previous != object => Err(invalid_group()),
            Some(_) => Ok(()),
            None => {
                self.identities.insert(object.path.clone(), object.clone());
                Ok(())
            }
        }
    }

    fn admit(
        &mut self,
        bytes: u64,
        budget: &Budget,
    ) -> Result<Arc<tokio::sync::OwnedSemaphorePermit>> {
        let bytes = usize::try_from(bytes).map_err(|_| capacity())?;
        self.bytes = self.bytes.checked_add(bytes).ok_or_else(capacity)?;
        if self.bytes > budget.limits.max_metadata_bytes {
            return Err(capacity());
        }
        // JSON trees, decoded Avro records, map entries and compiled schema facts
        // coexist. Charge their expansion as well as the separately held input buffer.
        let memory = Arc::new(budget.reserve(bytes.saturating_mul(8).saturating_add(4096))?);
        self.memory.push(memory.clone());
        Ok(memory)
    }

    async fn original(
        &mut self,
        object: &ObjectRef,
        budget: &Budget,
    ) -> Result<Arc<ScalarOrigins>> {
        self.identity(object)?;
        if let Some(value) = self.originals.get(&object.path) {
            return Ok(value.clone());
        }
        let memory = self.admit(object.bytes, budget)?;
        let value: Arc<ScalarOrigins> = Arc::new(read(object, budget, &mut 0, &memory).await?);
        self.originals.insert(object.path.clone(), value.clone());
        Ok(value)
    }

    async fn declaration(
        &mut self,
        object: &ObjectRef,
        budget: &Budget,
    ) -> Result<Arc<OriginalDeclaration>> {
        self.identity(object)?;
        if let Some(value) = self.declarations.get(&object.path) {
            return Ok(value.clone());
        }
        let memory = self.admit(object.bytes, budget)?;
        let value: Arc<OriginalDeclaration> =
            Arc::new(read(object, budget, &mut 0, &memory).await?);
        self.declarations.insert(object.path.clone(), value.clone());
        Ok(value)
    }

    async fn metadata(
        &mut self,
        object: &ObjectRef,
        budget: &Budget,
    ) -> Result<Arc<TableMetadata>> {
        self.identity(object)?;
        if let Some(value) = self.schemas.get(&object.path) {
            return Ok(value.clone());
        }
        let memory = self.admit(object.bytes, budget)?;
        let value: Arc<TableMetadata> = Arc::new(read(object, budget, &mut 0, &memory).await?);
        self.schemas.insert(object.path.clone(), value.clone());
        Ok(value)
    }

    async fn table(&mut self, reference: &NativeSnapshotRef, budget: &Budget) -> Result<Table> {
        let metadata = self.metadata(&reference.metadata, budget).await?;
        if metadata.current_snapshot_id() != reference.snapshot
            || metadata.current_schema_id() != reference.schema_id
        {
            return Err(invalid_group());
        }
        Ok(Table::builder()
            .identifier(iceberg::TableIdent::from_strs(["scalar", "originals"])?)
            .metadata((*metadata).clone())
            .metadata_location(&reference.metadata.path)
            .file_io(iceberg::io::FileIO::new_with_fs())
            .runtime(iceberg::Runtime::try_current()?)
            .readonly(true)
            .build()?)
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
            return Err(invalid_group());
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
                return Err(invalid_group());
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

    #[expect(
        clippy::too_many_arguments,
        reason = "Check one immutable descriptor against its selected source and field-group binding"
    )]
    pub(in crate::engine) async fn check(
        &mut self,
        object: &ObjectRef,
        source: &TablePin,
        group: &TablePin,
        definition: &GroupDefinition,
        input_fields: &BTreeMap<String, i32>,
        native_field_ids: &BTreeMap<i32, i32>,
        budget: &Budget,
    ) -> Result<()> {
        let originals = self.original(object, budget).await?;
        // Association is caller-specific even when immutable metadata was checked.
        if object.owner != source.metadata.owner
            || object.owner != group.metadata.owner
            || originals.processor != PROCESSOR
            || &originals.definition != definition
            || &originals.input_fields != input_fields
            || source.rows != group.rows
        {
            return Err(invalid_group());
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
            return Err(invalid_group());
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
            return Err(invalid_group());
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
            return Err(invalid_group());
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
                return Err(invalid_group());
            }
        }
        if let Some(checked) = self.verified.get(&object.path) {
            return if checked.rows == group.rows
                && checked.objects == group.objects
                && checked.dependencies == expected.dependencies
            {
                Ok(())
            } else {
                Err(invalid_group())
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
            return Err(invalid_group());
        }
        let source_files = storage::snapshot_files(&captured, budget).await?;
        let output_files = storage::snapshot_files(&output, budget).await?;
        if objects.get(group.metadata.path.as_str()) != Some(&&group.metadata)
            || objects.len() != output_files.paths.len()
            || output_files
                .paths
                .iter()
                .any(|path| !objects.contains_key(path.as_str()))
        {
            return Err(invalid_group());
        }
        let mut sources: BTreeMap<_, _> = source_files
            .data
            .iter()
            .map(|file| (file.file_path(), file))
            .collect();
        let mut outputs: BTreeMap<_, _> = output_files
            .data
            .iter()
            .map(|file| (file.file_path(), file))
            .collect();
        if sources.len() != source_files.data.len() || outputs.len() != output_files.data.len() {
            return Err(invalid_group());
        }
        let mut rows = 0u64;
        let mut generations = BTreeSet::new();
        for original in &originals.files {
            tokio::task::consume_budget().await;
            let file = &original.completion;
            let captured_file = sources
                .remove(file.source.path.as_str())
                .ok_or_else(invalid_group)?;
            if file.source.bytes != captured_file.file_size_in_bytes()
                || file.rows != captured_file.record_count()
                || !generations.insert(original.generation.key.as_str())
                || original.generation.revision <= 0
            {
                return Err(invalid_group());
            }
            rows = rows.checked_add(file.rows).ok_or_else(invalid_group)?;
            // An older completion stays explicitly unknown. Its output association
            // can still be checked; no later declaration is attributed to its work.
            if let Some(execution) = &file.execution {
                let source_metadata = file.source_metadata.as_ref().ok_or_else(invalid_group)?;
                let declaration = self.declaration(&execution.declaration, budget).await?;
                if declaration.definition != originals.definition
                    || declaration.input_fields != originals.input_fields
                    || execution.declaration.owner != object.owner
                {
                    return Err(invalid_group());
                }
                let compiled = self
                    .compile(&execution.declaration, source_metadata, budget)
                    .await?;
                if compiled.dependencies != expected.dependencies
                    || compiled.native != expected.native
                    || &compiled.native_field_ids != native_field_ids
                {
                    return Err(invalid_group());
                }
            }
            let scope = original
                .generation
                .key
                .strip_prefix("phase/backfill/")
                .and_then(|rest| rest.split_once('/'))
                .and_then(|(scope, _)| scope.parse::<u64>().ok())
                .ok_or_else(invalid_group)?;
            if original.generation.key != file_key(&file.source, &captured_expressions, scope)? {
                return Err(invalid_group());
            }
            for file in self.file(file, &expected.native, budget).await?.iter() {
                let selected = outputs.remove(file.file_path()).ok_or_else(invalid_group)?;
                if selected != file {
                    return Err(invalid_group());
                }
            }
        }
        if !sources.is_empty() || !outputs.is_empty() || rows != group.rows {
            return Err(invalid_group());
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
        let native = storage::open_pin(&source).await.unwrap();
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
        let mut checks = Checks::default();
        checks
            .check(
                &object,
                &source,
                &group,
                &definition,
                &expressions.input_fields,
                &expressions.native_field_ids,
                budget,
            )
            .await
            .unwrap();
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
                    &changed_source,
                    &group,
                    &definition,
                    &expressions.input_fields,
                    &expressions.native_field_ids,
                    budget
                )
                .await
                .unwrap_err()
                .code(),
            "invalid_group"
        );
        assert_eq!(
            Checks::default()
                .check(
                    &object,
                    &changed_source,
                    &group,
                    &definition,
                    &expressions.input_fields,
                    &expressions.native_field_ids,
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
                &source,
                &group,
                &definition,
                &expressions.input_fields,
                &expressions.native_field_ids,
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
                    &wrong_source,
                    &wrong_group,
                    &definition,
                    &expressions.input_fields,
                    &expressions.native_field_ids,
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
                    &source,
                    &wrong_group,
                    &definition,
                    &expressions.input_fields,
                    &expressions.native_field_ids,
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
                    &source,
                    &group,
                    &definition,
                    &expressions.input_fields,
                    &wrong_binding,
                    budget
                )
                .await
                .is_err()
        );
    }
}

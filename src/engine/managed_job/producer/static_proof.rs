//! Checks original producer records against adopted facts and native ancestry.
// Rust guideline compliant 2026-02-21
use super::*;
use iceberg::spec::{Operation, TableMetadata};
use serde_json::Value;

#[derive(Deserialize)]
struct OriginalRecord {
    version: u32,
    input: Option<String>,
    selector: Value,
    job: String,
    commit: String,
    lockfile_digest: Option<String>,
    #[serde(default)]
    delta_only: bool,
    #[serde(default)]
    final_reconciliation_required: bool,
    base: Option<Value>,
    source: Option<Value>,
    origin: Option<Value>,
}

struct PinnedRecord {
    sha256: String,
    size: u64,
    record: OriginalRecord,
}

pub(super) struct Records {
    entries: BTreeMap<JobProducerRecordLocation, PinnedRecord>,
    used: BTreeSet<JobProducerRecordLocation>,
}

pub(super) fn record_mismatch(reason: &'static str) -> Error {
    Error::refusal("producer_record_mismatch", reason)
}

impl Records {
    pub(super) fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            used: BTreeSet::new(),
        }
    }

    /// Parses one original byte identity before interpreting any record fields.
    pub(super) fn add(&mut self, pin: &JobProducerRecord, bytes: &[u8]) -> Result<()> {
        if pin.size != bytes.len() as u64
            || pin.size > 1 << 20
            || sha256(bytes) != pin.sha256
            || self.entries.contains_key(&pin.source)
        {
            return Err(record_mismatch(
                "original producer record bytes or location differ",
            ));
        }
        let record = serde_json::from_slice(bytes)
            .map_err(|_| record_mismatch("invalid original producer record shape"))?;
        self.entries.insert(
            pin.source.clone(),
            PinnedRecord {
                sha256: pin.sha256.clone(),
                size: pin.size,
                record,
            },
        );
        Ok(())
    }

    pub(super) fn verify_input(
        &mut self,
        definition: &Definition,
        input: &Input,
        metadata: &TableMetadata,
    ) -> Result<()> {
        let NativeSelector::NativeTable {
            table_uuid,
            snapshot,
            schema_id,
            ..
        } = &input.selector;
        if metadata.uuid() != *table_uuid
            || metadata.current_snapshot_id() != *snapshot
            || metadata.current_schema_id() != *schema_id
        {
            return Err(record_mismatch(
                "selected producer snapshot or schema differs",
            ));
        }
        let head = take_record(&self.entries, &mut self.used, &input.head_record)?;
        if head.input.as_deref() != Some(input.name.as_str())
            || head.selector != serde_json::to_value(&input.selector)?
        {
            return Err(record_mismatch(
                "producer head selector differs from the adopted input",
            ));
        }
        let imported = input.import_jobs.contains(&head.job);
        verify_fact(definition, input, head, !imported)?;
        if imported {
            verify_origin(definition, input, head, true)?;
        } else if input.daily_jobs.contains(&head.job) {
            verify_chain(
                definition,
                input,
                metadata,
                &self.entries,
                &mut self.used,
                head,
            )?;
        }
        Ok(())
    }

    pub(super) fn require_complete(&self) -> Result<()> {
        if self.entries.len() != self.used.len() {
            return Err(record_mismatch(
                "unused original producer records were supplied",
            ));
        }
        Ok(())
    }
}

fn take_record<'a>(
    records: &'a BTreeMap<JobProducerRecordLocation, PinnedRecord>,
    used: &mut BTreeSet<JobProducerRecordLocation>,
    pin: &RecordPin,
) -> Result<&'a OriginalRecord> {
    let found = records
        .get(&pin.source)
        .filter(|found| found.sha256 == pin.sha256 && found.size == pin.size)
        .ok_or_else(|| {
            record_mismatch("original producer record is missing or differs from its pin")
        })?;
    if !used.insert(pin.source.clone()) {
        return Err(record_mismatch("producer record location is repeated"));
    }
    Ok(&found.record)
}

fn verify_fact(
    definition: &Definition,
    input: &Input,
    record: &OriginalRecord,
    require_versions: bool,
) -> Result<()> {
    if record.version != 1
        || record.delta_only
        || record.final_reconciliation_required
        || record
            .lockfile_digest
            .as_ref()
            .is_some_and(|digest| !valid_sha256(digest))
    {
        return Err(record_mismatch(
            "producer record is not a complete original for this input",
        ));
    }
    let fact = definition
        .producers
        .iter()
        .find(|fact| {
            fact.input == input.name && fact.commit == record.commit && fact.job == record.job
        })
        .ok_or_else(|| record_mismatch("original producer commit or job was not adopted"))?;
    if require_versions
        && input
            .expected_versions
            .iter()
            .any(|(term, expected)| fact.versions.get(term) != Some(expected))
    {
        return Err(record_mismatch(
            "adopted producer does not establish the required versions",
        ));
    }
    Ok(())
}

fn native_selector(value: &Value) -> Result<NativeSelector> {
    if value.get("snapshot").is_none() {
        return Err(record_mismatch(
            "producer selector must explicitly identify its snapshot",
        ));
    }
    let selector: NativeSelector = serde_json::from_value(value.clone())
        .map_err(|_| record_mismatch("invalid original native producer selector"))?;
    let NativeSelector::NativeTable {
        metadata_location,
        table_uuid,
        snapshot,
        schema_id,
    } = &selector;
    if metadata_location.trim().is_empty()
        || table_uuid.is_nil()
        || snapshot.is_some_and(|snapshot| snapshot <= 0)
        || *schema_id < 0
        || serde_json::to_value(&selector)? != *value
    {
        return Err(record_mismatch(
            "invalid exact native producer selector identity",
        ));
    }
    Ok(selector)
}

fn verify_origin(
    definition: &Definition,
    input: &Input,
    imported: &OriginalRecord,
    require_versions: bool,
) -> Result<()> {
    let origin: OriginalRecord = serde_json::from_value(
        imported
            .origin
            .as_ref()
            .and_then(|origin| origin.get("record"))
            .ok_or_else(|| record_mismatch("import has no original producer record"))?
            .clone(),
    )
    .map_err(|_| record_mismatch("invalid original import producer record"))?;
    if origin.input.as_deref() != Some(input.name.as_str()) {
        return Err(record_mismatch(
            "import origin belongs to another adopted input",
        ));
    }
    if input.import_jobs.contains(&origin.job) {
        return Err(record_mismatch(
            "an import cannot be its own original producer",
        ));
    }
    match origin.selector.get("kind").and_then(Value::as_str) {
        Some("native_table") => {
            native_selector(&origin.selector)?;
        }
        Some("parquet") => {
            let digest = origin.selector.get("sha256").and_then(Value::as_str);
            let size = origin.selector.get("size").and_then(Value::as_u64);
            let source = imported
                .source
                .as_ref()
                .ok_or_else(|| record_mismatch("import has no exact source byte pin"))?;
            if !digest.is_some_and(valid_sha256)
                || size.is_none_or(|size| size == 0)
                || digest != source.get("sha256").and_then(Value::as_str)
                || size != source.get("size").and_then(Value::as_u64)
            {
                return Err(record_mismatch(
                    "original Parquet origin differs from the import source",
                ));
            }
        }
        _ => {
            return Err(record_mismatch(
                "unsupported original import producer selector",
            ));
        }
    }
    verify_fact(definition, input, &origin, require_versions)
}

fn append_path(metadata: &TableMetadata, mut head: Option<i64>, base: Option<i64>) -> Result<()> {
    if head == base {
        return Err(record_mismatch(
            "daily producer base repeats its head snapshot",
        ));
    }
    // Bound the walk even for malformed native ancestry containing a cycle.
    let mut remaining = metadata.snapshots().len();
    while head != base {
        let snapshot = head
            .and_then(|id| metadata.snapshot_by_id(id))
            .filter(|snapshot| snapshot.summary().operation == Operation::Append)
            .ok_or_else(|| record_mismatch("selected producer ancestry is not append-only"))?;
        remaining = remaining
            .checked_sub(1)
            .ok_or_else(|| record_mismatch("selected producer ancestry contains a cycle"))?;
        head = snapshot.parent_snapshot_id();
    }
    Ok(())
}

fn base_pin(input: &Input, record: &Value) -> Result<(RecordPin, bool)> {
    let sha256 = record
        .get("sha256")
        .and_then(Value::as_str)
        .filter(|sha| valid_sha256(sha))
        .ok_or_else(|| record_mismatch("daily producer base has no exact record digest"))?;
    let size = record
        .get("size")
        .and_then(Value::as_u64)
        .filter(|size| *size > 0 && *size <= 1 << 20)
        .ok_or_else(|| record_mismatch("daily producer base has no exact record size"))?;
    let (source, anchor) = match (record.get("key"), record.get("anchor")) {
        (Some(key), None) => {
            let key = key
                .as_str()
                .filter(|key| !key.is_empty())
                .ok_or_else(|| record_mismatch("invalid original daily record key"))?;
            (
                JobProducerRecordLocation {
                    namespace: input.record_namespace.clone(),
                    key: key.into(),
                },
                false,
            )
        }
        (None, Some(anchor)) => {
            let anchor = anchor
                .as_str()
                .and_then(|name| {
                    input
                        .import_anchors
                        .iter()
                        .find(|anchor| anchor.anchor == name)
                })
                .filter(|anchor| anchor.record.sha256 == sha256 && anchor.record.size == size)
                .ok_or_else(|| record_mismatch("daily producer base anchor was not adopted"))?;
            (anchor.record.source.clone(), true)
        }
        _ => {
            return Err(record_mismatch(
                "daily producer base record address is ambiguous",
            ));
        }
    };
    Ok((
        RecordPin {
            source,
            sha256: sha256.into(),
            size,
        },
        anchor,
    ))
}

fn verify_chain<'a>(
    definition: &Definition,
    input: &Input,
    metadata: &TableMetadata,
    records: &'a BTreeMap<JobProducerRecordLocation, PinnedRecord>,
    used: &mut BTreeSet<JobProducerRecordLocation>,
    mut current: &'a OriginalRecord,
) -> Result<()> {
    let NativeSelector::NativeTable {
        table_uuid,
        mut snapshot,
        ..
    } = input.selector;
    loop {
        let base = current
            .base
            .as_ref()
            .ok_or_else(|| record_mismatch("daily producer has no original base"))?;
        let selector = base
            .get("selector")
            .ok_or_else(|| record_mismatch("daily producer base has no selector"))?;
        #[derive(Deserialize)]
        struct BaseSelection {
            metadata_location: String,
            table_uuid: uuid::Uuid,
            snapshot: Option<i64>,
            schema_id: i32,
        }
        let base_selection: BaseSelection = serde_json::from_value(selector.clone())
            .map_err(|_| record_mismatch("invalid daily native base selector"))?;
        if selector.get("snapshot").is_none()
            || base_selection.metadata_location.trim().is_empty()
            || base_selection.schema_id < 0
            || base_selection
                .snapshot
                .is_some_and(|snapshot| snapshot <= 0)
        {
            return Err(record_mismatch("daily base has no exact native selector"));
        }
        let next = base_selection.snapshot;
        if base_selection.table_uuid != table_uuid {
            return Err(record_mismatch(
                "daily producer base is from another native table",
            ));
        }
        append_path(metadata, snapshot, next)?;
        let base_record = base
            .get("record")
            .ok_or_else(|| record_mismatch("daily producer base has no record decision"))?;
        if next.is_none() {
            return if base_record.is_null() {
                Ok(())
            } else {
                Err(record_mismatch(
                    "an empty native base must not name a record",
                ))
            };
        }
        let (pin, anchor) = base_pin(input, base_record)?;
        let prior = take_record(records, used, &pin)?;
        if ["metadata_location", "table_uuid", "snapshot", "schema_id"]
            .iter()
            .any(|field| prior.selector.get(*field) != selector.get(*field))
        {
            return Err(record_mismatch(
                "daily producer base record selects another snapshot",
            ));
        }
        verify_fact(definition, input, prior, false)?;
        if input.import_jobs.contains(&prior.job) {
            if !anchor {
                return Err(record_mismatch(
                    "daily chain import must terminate at its adopted anchor",
                ));
            }
            return verify_origin(definition, input, prior, false);
        }
        if anchor || !input.daily_jobs.contains(&prior.job) {
            return Err(record_mismatch(
                "only daily producer records can continue a base chain",
            ));
        }
        snapshot = next;
        current = prior;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, Snapshot, SortOrder,
        Summary, TableMetadataBuilder, Type,
    };
    use serde_json::json;

    fn metadata(operation: Operation) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields([Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let make_snapshot = |id, parent, sequence, operation| {
            Snapshot::builder()
                .with_snapshot_id(id)
                .with_parent_snapshot_id(parent)
                .with_sequence_number(sequence)
                .with_timestamp_ms(1_700_000_000_000 + sequence)
                .with_manifest_list(format!("file:///original/list-{id}.avro"))
                .with_summary(Summary {
                    operation,
                    additional_properties: Default::default(),
                })
                .with_schema_id(0)
                .build()
        };
        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "file:///original".into(),
            FormatVersion::V2,
            Default::default(),
        )
        .unwrap()
        .assign_uuid(uuid::Uuid::parse_str("d9fdc74e-23c3-49a0-ae9c-dd71f9c0abb2").unwrap())
        .set_branch_snapshot(make_snapshot(30, None, 1, Operation::Append), "main")
        .unwrap()
        .set_branch_snapshot(make_snapshot(31, Some(30), 2, operation), "main")
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    fn original(namespace: &str, key: &str, value: &Value) -> (JobProducerRecord, Vec<u8>) {
        let bytes = serde_json::to_vec(value).unwrap();
        (
            JobProducerRecord {
                source: JobProducerRecordLocation {
                    namespace: namespace.into(),
                    key: key.into(),
                },
                sha256: sha256(&bytes),
                size: bytes.len() as u64,
                path: PathBuf::from("not-opened-by-static-check"),
            },
            bytes,
        )
    }

    fn record_pin(record: &JobProducerRecord) -> Value {
        json!({"source":record.source,"sha256":record.sha256,"size":record.size})
    }

    #[test]
    fn original_direct_producer_must_match_the_adopted_fact() {
        let mut definition = super::super::super::tests::producer_definition("fixture-v1");
        let selected = definition["inputs"][0]["selector"].clone();
        let mut direct = json!({"version":1,"input":"raw","selector":selected,
            "job":"jobs/build.py","commit":"b".repeat(40),"lockfile_digest":"6".repeat(64)});
        for changed in [false, true] {
            if changed {
                // Re-pin the altered bytes so this exercises fact membership,
                // not rejection of a simple digest mismatch.
                direct["job"] = json!("jobs/unregistered.py");
            }
            let (head, bytes) = original("fixture-records", "producer-31.json", &direct);
            definition["inputs"][0]["head_record"] = record_pin(&head);
            let definition: Definition = serde_json::from_value(definition.clone()).unwrap();
            definition.validate().unwrap();
            let mut records = Records::new();
            records.add(&head, &bytes).unwrap();
            let result = records.verify_input(
                &definition,
                &definition.inputs[0],
                &metadata(Operation::Append),
            );
            if changed {
                assert_eq!(result.unwrap_err().code(), "producer_record_mismatch");
            } else {
                result.unwrap();
                records.require_complete().unwrap();
            }
        }
    }

    #[test]
    fn original_daily_chain_uses_native_ancestry_and_preserves_older_import_versions() {
        let mut definition = super::super::super::tests::producer_definition("fixture-v1");
        let head_selector = definition["inputs"][0]["selector"].clone();
        let mut base_selector = head_selector.clone();
        base_selector["snapshot"] = json!(30);
        base_selector["metadata_location"] = json!("file:///original/base-metadata.json");
        let origin = json!({"version":1,"input":"raw",
            "selector":{"kind":"parquet","path":"original.parquet","sha256":"9".repeat(64),"size":57},
            "job":"jobs/build.py","commit":"b".repeat(40)});
        let imported = json!({"version":1,"input":"older_raw_label","selector":base_selector,
            "job":"jobs/import.py","commit":"d".repeat(40),
            "source":{"path":"original.parquet","sha256":"9".repeat(64),"size":57},
            "origin":{"record":origin},"verified":"original historical statement"});
        let (anchor, anchor_bytes) = original("fixture-git-at-head", "anchors/raw.json", &imported);
        let daily = json!({"version":1,"input":"raw","selector":head_selector,
            "job":"jobs/daily.py","commit":"e".repeat(40),
            "base":{"selector":base_selector,"record":{"anchor":"raw","sha256":anchor.sha256,"size":anchor.size}}});
        let (head, head_bytes) = original("fixture-records", "producer-31.json", &daily);
        definition["inputs"][0]["head_record"] = record_pin(&head);
        definition["inputs"][0]["import_anchors"] =
            json!([{"anchor":"raw","record":record_pin(&anchor),"blob":"7".repeat(40)}]);
        definition["inputs"][0]["daily_jobs"] = json!(["jobs/daily.py"]);
        definition["inputs"][0]["import_jobs"] = json!(["jobs/import.py"]);
        let mut daily_fact = definition["producers"][0].clone();
        daily_fact["job"] = json!("jobs/daily.py");
        daily_fact["commit"] = json!("e".repeat(40));
        let mut import_fact = definition["producers"][0].clone();
        import_fact["job"] = json!("jobs/import.py");
        import_fact["commit"] = json!("d".repeat(40));
        definition["producers"][0]["versions"]["parser"] = json!("older-v0");
        definition["producers"]
            .as_array_mut()
            .unwrap()
            .extend([daily_fact, import_fact]);
        let definition: Definition = serde_json::from_value(definition).unwrap();
        definition.validate().unwrap();
        for operation in [Operation::Append, Operation::Overwrite] {
            let valid = operation == Operation::Append;
            let mut records = Records::new();
            records.add(&head, &head_bytes).unwrap();
            records.add(&anchor, &anchor_bytes).unwrap();
            let result =
                records.verify_input(&definition, &definition.inputs[0], &metadata(operation));
            if valid {
                result.unwrap();
                records.require_complete().unwrap();
            } else {
                assert_eq!(result.unwrap_err().code(), "producer_record_mismatch");
            }
        }
    }
}

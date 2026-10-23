use super::*;

#[tokio::test]
async fn completed_plan_qualifies_admission_and_result_without_opening_inputs() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            scratch_bytes: 16 << 20,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    for fault in [
        "none",
        "missing-admission",
        "conflicting-admission",
        "dataset",
        "id",
        "version",
        "request",
        "unverified",
        "missing-source",
    ] {
        let spec = BackfillSpec {
            dataset: Id::new(fault).unwrap(),
            operation: Id::new("derived").unwrap(),
            base: Id::new("base").unwrap(),
            definition: "v1".into(),
            group: Id::new("length").unwrap(),
            group_definition: "scalar-v1".into(),
            fields: vec![ComputedField {
                id: 10,
                name: "text_length".into(),
                expression: "length(text)".into(),
            }],
        };
        let request = OperationRequest::Backfill(spec.clone());
        let operation = Operation {
            request_digest: if fault == "conflicting-admission" {
                "another-request".into()
            } else {
                fingerprint(&request).unwrap()
            },
            eligibility: 0,
            epoch: 0,
            boundary: 0,
            admission_fence: None,
            result_format: FORMAT,
            definition: spec.definition.clone(),
            base: Some(spec.base.clone()),
            base_manifest: None,
            schema_key: None,
            require_current_boundary: false,
        };
        // No source, manifest or native metadata objects exist. Only the
        // lifecycle receipt is needed to report a completed plan.
        let missing = ObjectRef {
            owner: spec.dataset.clone(),
            path: root.path().join("absent").to_string_lossy().into_owned(),
            digest: "a".repeat(64),
            bytes: 0,
        };
        let mut result = ReleasePin {
            id: spec.operation.clone(),
            dataset: DatasetPin {
                format: FORMAT,
                dataset: spec.dataset.clone(),
                version: spec.operation.clone(),
                groups: BTreeMap::from([(
                    "source".into(),
                    TablePin {
                        table: "source".into(),
                        metadata: missing.clone(),
                        snapshot: None,
                        schema_id: 0,
                        objects: vec![],
                        rows: 7,
                        logical_metadata: None,
                    },
                )]),
                computed: BTreeMap::new(),
                inputs: BTreeMap::new(),
                relationship: SourceRelationship::Independent,
                related: None,
                evidence: None,
                definition: spec.definition.clone(),
                eligibility: 0,
                boundary: 0,
                request,
                parent_manifest: None,
                processor: PROCESSOR.into(),
                validation: VerificationLevel::Routine,
                partition_by: Vec::new(),
            },
            manifest: missing,
            verified: true,
        };
        match fault {
            "dataset" => result.dataset.dataset = Id::new("other").unwrap(),
            "id" => result.id = Id::new("other").unwrap(),
            "version" => result.dataset.version = Id::new("other").unwrap(),
            "request" => {
                let OperationRequest::Backfill(ref mut stored) = result.dataset.request else {
                    unreachable!()
                };
                stored.group_definition = "different".into();
            }
            "unverified" => result.verified = false,
            "missing-source" => result.dataset.groups.clear(),
            _ => {}
        }
        let lifecycle = &engine.inner.lifecycle;
        let view = lifecycle.open(&spec.dataset).await.unwrap();
        let mut changes = vec![record(format!("result/{}", spec.operation), result).unwrap()];
        if fault != "missing-admission" {
            changes.push(record(format!("operation/{}", spec.operation), operation).unwrap());
        }
        lifecycle.commit(&view, changes).await.unwrap();
        let plan = engine.plan_backfill(&spec).await;
        match fault {
            "none" => {
                let plan = plan.unwrap();
                assert!(plan.completed);
                assert_eq!(plan.rows, 7);
                assert_eq!(plan.files_completed, None);
                assert_eq!(plan.files_pending, None);
            }
            "conflicting-admission" => {
                assert_eq!(plan.unwrap_err().code(), "operation_conflict");
            }
            _ => assert_eq!(plan.unwrap_err().code(), "release_mismatch"),
        }
    }
}

#[tokio::test]
async fn native_rename_reuses_file_completions_but_name_reuse_changes_input_meaning() {
    let root = tempfile::TempDir::new().unwrap();
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            scratch_bytes: 16 << 20,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let dataset = Id::new("cache").unwrap();
    let prepared = engine
        .write_frame(
            &dataset,
            &root.path().join("source"),
            engine
                .context()
                .sql("SELECT CAST(1 AS BIGINT) AS old, CAST(100 AS BIGINT) AS spare")
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    let source = prepared.table;
    let native = storage::open_pin(&source, &engine.inner.lifecycle.budget)
        .await
        .unwrap();
    let fields = native.metadata().current_schema().as_struct().fields();
    let old_id = fields[0].id;
    let spec = BackfillSpec {
        dataset: dataset.clone(),
        operation: Id::new("backfill").unwrap(),
        base: Id::new("base").unwrap(),
        definition: "v1".into(),
        group: Id::new("derived").unwrap(),
        group_definition: "scalar-v1".into(),
        fields: vec![ComputedField {
            id: 10,
            name: "answer".into(),
            expression: "old + 1".into(),
        }],
    };
    let operation = Operation {
        request_digest: "fixture".into(),
        eligibility: 0,
        epoch: 0,
        boundary: 0,
        admission_fence: None,
        result_format: FORMAT,
        definition: "v1".into(),
        base: Some(spec.base.clone()),
        base_manifest: None,
        schema_key: None,
        require_current_boundary: false,
    };
    let request = OperationRequest::Backfill(spec.clone());
    let first = engine
        .prepare_group(
            &request,
            &operation,
            &source,
            None,
            &spec.group_definition(),
            None,
            None,
            &mut crate::engine::verification::Checks::new(VerificationLevel::Routine),
        )
        .await
        .unwrap();
    let before = engine.inspect(&dataset).await.unwrap();
    let completions = before
        .iter()
        .filter(|(key, _)| key.starts_with("phase/backfill/"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    assert!(!completions.is_empty());
    let mut renamed = fields[0].as_ref().clone();
    renamed.name = "new name".into();
    renamed.doc = Some("presentation only".into());
    let new_schema = iceberg::spec::Schema::builder()
        .with_fields([
            fields[1].clone(),
            Arc::new(renamed),
            Arc::new(iceberg::spec::NestedField::optional(
                30,
                "old",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
            )),
        ])
        .build()
        .unwrap();
    let directory = root.path().join("evolved");
    let metadata = native
        .metadata()
        .clone()
        .into_builder(None)
        .set_location(directory.to_string_lossy().into_owned())
        .assign_uuid(uuid::Uuid::new_v4())
        .add_current_schema(new_schema)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let identifier =
        iceberg::TableIdent::new(native.identifier().namespace().clone(), "evolved".into());
    let evolved = storage::register_candidate(
        engine.inner.lifecycle.catalog.as_ref(),
        &identifier,
        storage::CandidateWriter::Local,
        move || Ok(metadata),
        &engine.inner.lifecycle.budget,
    )
    .await
    .unwrap();
    let evolved = storage::pin(
        &dataset,
        &evolved,
        source.rows,
        &engine.inner.lifecycle.budget,
        &source.objects,
    )
    .await
    .unwrap();
    let bound = engine
        .prepare_group(
            &request,
            &operation,
            &evolved,
            None,
            &spec.group_definition(),
            Some(&first.input_fields),
            None,
            &mut crate::engine::verification::Checks::new(VerificationLevel::Routine),
        )
        .await
        .unwrap();
    assert_eq!(bound.input_fields["old"], old_id);
    let after = engine.inspect(&dataset).await.unwrap();
    assert_eq!(
        completions,
        after
            .iter()
            .filter(|(key, _)| key.starts_with("phase/backfill/"))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>()
    );
    let data = |pin: &TablePin| {
        pin.objects
            .iter()
            .filter(|object| object.path.ends_with(".parquet"))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(data(&first.table), data(&bound.table));
    assert_ne!(first.table.metadata, bound.table.metadata);
    // Same SQL and current source pin, but fresh name resolution consumes
    // ID30 rather than ID1. Neither file nor whole-table reuse may collide.
    let fresh = engine
        .prepare_group(
            &request,
            &operation,
            &evolved,
            None,
            &spec.group_definition(),
            None,
            None,
            &mut crate::engine::verification::Checks::new(VerificationLevel::Routine),
        )
        .await
        .unwrap();
    assert_eq!(fresh.input_fields["old"], 30);
    assert_ne!(data(&fresh.table), data(&bound.table));
    for (pin, expected) in [(&bound.table, Some(2)), (&fresh.table, None)] {
        let table = storage::open_pin(pin, &engine.inner.lifecycle.budget)
            .await
            .unwrap();
        let batches = storage::read_batches(&table, &engine.inner.lifecycle.budget)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let values = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![expected]);
    }
}

#[test]
fn aggregate_completion_descriptors_cannot_exceed_assembly_allowance() {
    let object = ObjectRef {
        owner: Id::new("test").unwrap(),
        path: "descriptor".into(),
        digest: "a".repeat(64),
        bytes: 600,
    };
    let completion = FileCompletion {
        execution: None,
        source_metadata: None,
        source: object.clone(),
        rows: 0,
        descriptor: object,
        outputs: vec![],
    };
    let admitted = charge_assembly(0, &completion, 1024).unwrap();
    assert!(admitted < 1024);
    assert_eq!(
        charge_assembly(admitted, &completion, 1024)
            .unwrap_err()
            .code(),
        "metadata_capacity"
    );
}

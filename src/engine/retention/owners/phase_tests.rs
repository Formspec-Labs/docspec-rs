//! Propagated protection follows the consumed generation, including unknown older records.
#![cfg(feature = "test-util")]

use super::*;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 256 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 8,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 16 << 20,
    }
}

fn request(dataset: &str, operation: &str, input: &ObjectRef) -> RunSpec {
    RunSpec {
        dataset: id(dataset),
        operation: id(operation),
        base: None,
        relationship: None,
        input: input.path.clone(),
        expected_digest: input.digest.clone(),
        sql: "SELECT id FROM input".into(),
        definition: "v1".into(),
        partition_by: Vec::new(),
    }
}

async fn fixture() -> (tempfile::TempDir, Engine, ObjectRef, ObjectRef) {
    fixture_with_limits(limits()).await
}

async fn fixture_with_limits(profile: Limits) -> (tempfile::TempDir, Engine, ObjectRef, ObjectRef) {
    let fixture = fixture_with_values(profile, &[1, 1, 2]).await;
    assert_eq!(fixture.2.digest, fixture.3.digest);
    assert_eq!(fixture.2.bytes, fixture.3.bytes);
    fixture
}

fn external_input(path: &std::path::Path, values: &[i64]) -> ObjectRef {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let (digest, bytes) = storage::digest(path).unwrap();
    ObjectRef {
        owner: id("external"),
        path: path.to_string_lossy().into_owned(),
        digest,
        bytes,
    }
}

async fn fixture_with_values(
    profile: Limits,
    second_values: &[i64],
) -> (tempfile::TempDir, Engine, ObjectRef, ObjectRef) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("same-external.parquet");
    let external = external_input(&path, &[1, 1, 2]);
    let engine = Engine::open(root.path().join("workspace"), profile)
        .await
        .unwrap();
    let first = engine
        .run(&request("owner-a", "initial", &external))
        .await
        .unwrap();
    let external = external_input(&path, second_values);
    let second = engine
        .run(&request("owner-b", "initial", &external))
        .await
        .unwrap();
    // These are the admitted raw bytes. Generated native files include writer
    // occurrence columns, so they need not have identical material digests.
    let first = first.dataset.inputs["input"].clone();
    let second = second.dataset.inputs["input"].clone();
    assert_ne!(first.owner, second.owner);
    assert_ne!(first.path, second.path);
    std::fs::remove_file(path).unwrap();
    (root, engine, first, second)
}

fn release_files(release: &ReleasePin) -> BTreeMap<String, (u64, std::time::SystemTime)> {
    std::iter::once(&release.manifest)
        .chain(release.dataset.inputs.values())
        .chain(
            release
                .dataset
                .groups
                .values()
                .flat_map(|table| std::iter::once(&table.metadata).chain(table.objects.iter())),
        )
        .map(|object| {
            let metadata = std::fs::metadata(&object.path).unwrap();
            (
                object.path.clone(),
                (metadata.len(), metadata.modified().unwrap()),
            )
        })
        .collect()
}

async fn phase(engine: &Engine, dataset: &Id, name: &str) -> (String, i64, PhaseResult) {
    let view = engine.inner.lifecycle.open(dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let (key, revision, json) = rows
        .prefix("phase/")
        .find(|(key, _, _)| key.ends_with(&format!("/{name}")))
        .unwrap();
    (
        key.to_owned(),
        revision,
        serde_json::from_str(json).unwrap(),
    )
}

async fn binding(engine: &Engine, dataset: &Id, key: &str) -> ReferenceBinding {
    let view = engine.inner.lifecycle.open(dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let (revision, _, binding) = rows
        .get_record::<ReferenceBinding>(&binding_key(key))
        .unwrap()
        .unwrap();
    assert_eq!(
        rows.revision(key),
        Some(revision),
        "claim and protection must be one native commit"
    );
    assert_eq!(binding.revision, revision);
    engine.check_reference_binding(&view, key).await.unwrap();
    binding
}

async fn succession(engine: &Engine, dataset: &Id, operation: &Id) -> (i64, Succession) {
    let view = engine.inner.lifecycle.open(dataset).await.unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    let key = format!("succession/{operation}");
    assert!(
        rows.get_record::<ReferenceBinding>(&binding_key(&key))
            .unwrap()
            .is_none(),
        "accepted order does not create a payload-retention binding"
    );
    let (revision, _, link) = rows.get_record(&key).unwrap().unwrap();
    (revision, link)
}

#[tokio::test]
async fn material_reuse_carries_original_owner_scope_into_validation_and_readback() {
    let (_root, engine, input_a, input_b) = fixture().await;
    let first = request("consumer", "a", &input_a);
    let second = request("consumer", "b", &input_b);
    assert_eq!(
        engine
            .run_until(&first, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let original_admission = phase(&engine, &first.dataset, "admission").await;
    let original_processing = phase(&engine, &first.dataset, "processing").await;
    let admission_binding = binding(&engine, &first.dataset, &original_admission.0).await;
    let processing_binding = binding(&engine, &first.dataset, &original_processing.0).await;
    assert!(
        processing_binding
            .protections
            .iter()
            .any(|reference| reference.owner == input_a.owner)
    );
    assert_eq!(original_admission.2.operation, first.operation);
    assert_eq!(original_processing.2.operation, first.operation);

    // Admit B separately: acquiring B's new request scope is legitimate, while
    // the following processing/validation/readback work must inherit A's scope.
    assert_eq!(
        engine
            .run_until(&second, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let request_binding = binding(
        &engine,
        &second.dataset,
        &admission::dependencies_key(&second.operation),
    )
    .await;
    assert!(
        request_binding
            .protections
            .iter()
            .any(|reference| reference.owner == input_b.owner)
    );
    let mut owner_revisions = BTreeMap::new();
    for owner in [&input_a.owner, &input_b.owner] {
        owner_revisions.insert(
            owner.clone(),
            engine.inner.lifecycle.open(owner).await.unwrap().revision,
        );
    }
    assert_eq!(
        engine
            .run_until(&second, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(
        phase(&engine, &first.dataset, "admission").await,
        original_admission
    );
    assert_eq!(
        phase(&engine, &first.dataset, "processing").await,
        original_processing
    );
    assert_eq!(
        binding(&engine, &first.dataset, &original_admission.0).await,
        admission_binding
    );
    assert_eq!(
        binding(&engine, &first.dataset, &original_processing.0).await,
        processing_binding
    );
    let validation = phase(&engine, &second.dataset, "validation").await;
    assert_eq!(validation.2.operation, first.operation);
    assert_eq!(validation.2.table, original_processing.2.table);
    let validation_binding = binding(&engine, &second.dataset, &validation.0).await;
    let readback_key = format!("readback/{}", second.operation);
    let readback_binding = binding(&engine, &second.dataset, &readback_key).await;
    for reference in &processing_binding.protections {
        assert!(validation_binding.protections.contains(reference));
        assert!(readback_binding.protections.contains(reference));
    }
    let view = engine.inner.lifecycle.open(&second.dataset).await.unwrap();
    let readback: ReleasePin = engine
        .inner
        .lifecycle
        .get(&view, &readback_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(readback.dataset.inputs["input"], input_a);
    let retained_files = release_files(&readback);
    let selected = engine.run(&second).await.unwrap();
    assert_eq!(selected, readback);
    let result_binding = binding(&engine, &second.dataset, "result/b").await;
    let (succession_revision, link) = succession(&engine, &second.dataset, &second.operation).await;
    assert_eq!(result_binding.protections, readback_binding.protections);
    assert_eq!(result_binding.revision, succession_revision);
    assert_eq!(link.selected, selected.version_ref());
    assert!(link.predecessor.is_none());
    let selection_revision = engine
        .inner
        .lifecycle
        .open(&second.dataset)
        .await
        .unwrap()
        .revision;
    assert_eq!(engine.run(&second).await.unwrap(), selected);
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&second.dataset)
            .await
            .unwrap()
            .revision,
        selection_revision,
        "lost selection responses must reconcile without another commit"
    );
    assert_eq!(release_files(&selected), retained_files);
    for (owner, revision) in owner_revisions {
        assert_eq!(
            engine.inner.lifecycle.open(&owner).await.unwrap().revision,
            revision,
            "propagation must reuse grants rather than acquire owners again"
        );
    }
}

#[tokio::test]
async fn succession_keeps_actual_predecessor_without_inheriting_its_payload_scope() {
    let (root, engine, input_a, input_b) = fixture_with_values(limits(), &[10, 20]).await;
    assert_ne!(input_a.digest, input_b.digest);
    let first = request("consumer", "a", &input_a);
    let mut second = request("consumer", "b", &input_b);
    second.sql = "SELECT id + 1 AS id FROM input".into();
    for build in [&first, &second] {
        assert_eq!(
            engine
                .run_until(build, "readback")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
    }
    let first_scope = binding(&engine, &first.dataset, "readback/a").await;
    let second_scope = binding(&engine, &second.dataset, "readback/b").await;
    assert_eq!(first_scope.protections.len(), 1);
    assert_eq!(first_scope.protections[0].owner, input_a.owner);
    assert_eq!(second_scope.protections.len(), 1);
    assert_eq!(second_scope.protections[0].owner, input_b.owner);
    let mut owner_revisions = BTreeMap::new();
    for owner in [&input_a.owner, &input_b.owner] {
        owner_revisions.insert(
            owner.clone(),
            engine.inner.lifecycle.open(owner).await.unwrap().revision,
        );
    }
    let prior = engine.run(&first).await.unwrap();
    let selected = engine.run(&second).await.unwrap();
    assert!(selected.dataset.parent_manifest.is_none());
    let result_scope = binding(&engine, &second.dataset, "result/b").await;
    assert_eq!(result_scope.protections, second_scope.protections);
    let accepted = succession(&engine, &second.dataset, &second.operation).await;
    assert_eq!(accepted.0, result_scope.revision);
    assert_eq!(accepted.1.predecessor, Some(prior.version_ref()));
    assert_eq!(accepted.1.selected, selected.version_ref());
    assert_eq!(engine.run(&second).await.unwrap(), selected);
    assert_eq!(
        succession(&engine, &second.dataset, &second.operation).await,
        accepted
    );
    // Model unavailable old content in this private fixture. Accepted ordering
    // remains provable from the catalog even after a cold engine reopen.
    let old_payload = prior.dataset.groups["source"]
        .objects
        .iter()
        .find(|object| object.path.ends_with(".parquet"))
        .unwrap();
    assert!(
        !selected.dataset.groups["source"]
            .objects
            .contains(old_payload)
    );
    std::fs::remove_file(&prior.manifest.path).unwrap();
    std::fs::remove_file(&old_payload.path).unwrap();
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let proof = engine
        .prove_succession(&prior.version_ref(), &selected.version_ref())
        .await
        .unwrap();
    assert_eq!(proof.prior, prior.version_ref());
    assert_eq!(proof.target, selected.version_ref());
    assert_eq!(proof.observed_head, selected.version_ref());
    assert_eq!(proof.selections_advanced, 1);
    assert_eq!(proof.selections_behind, 0);
    assert_eq!(
        succession(&engine, &second.dataset, &second.operation).await,
        accepted
    );
    for (owner, revision) in owner_revisions {
        assert_eq!(
            engine.inner.lifecycle.open(&owner).await.unwrap().revision,
            revision,
            "selection and history proof do not acquire predecessor payload protection"
        );
    }
}

#[tokio::test]
async fn unknown_predecessor_scope_does_not_erase_the_selected_results_known_scope() {
    let (_root, engine, input_a, input_b) = fixture_with_values(limits(), &[10, 20]).await;
    let first = request("consumer", "a", &input_a);
    let mut second = request("consumer", "b", &input_b);
    second.sql = "SELECT id + 1 AS id FROM input".into();
    assert_eq!(
        engine
            .run_until(&first, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let original = phase(&engine, &first.dataset, "admission").await;
    let retirement = engine
        .plan_phase_retirement(
            &first.dataset,
            &id("legacy-admission"),
            std::slice::from_ref(&original.0),
            "replace the retired completion with an older writer's unbound generation",
        )
        .await
        .unwrap();
    engine.retire_phases(&retirement).await.unwrap();
    let view = engine.inner.lifecycle.open(&first.dataset).await.unwrap();
    let operation: Operation = engine
        .inner
        .lifecycle
        .get(&view, "operation/a")
        .await
        .unwrap()
        .unwrap();
    engine
        .complete(
            &OperationRequest::Build(first.clone()),
            &operation,
            original.2,
            None,
        )
        .await
        .unwrap();
    for build in [&first, &second] {
        assert_eq!(
            engine
                .run_until(build, "readback")
                .await
                .unwrap_err()
                .code(),
            "injected_failure"
        );
    }
    let second_scope = binding(&engine, &second.dataset, "readback/b").await;
    assert_eq!(second_scope.protections.len(), 1);
    assert_eq!(second_scope.protections[0].owner, input_b.owner);
    let prior = engine.run(&first).await.unwrap();
    let selected = engine.run(&second).await.unwrap();
    let view = engine.inner.lifecycle.open(&second.dataset).await.unwrap();
    for key in ["result/a", "succession/a", "succession/b"] {
        assert!(
            engine
                .retained_protections(&view, key)
                .await
                .unwrap()
                .is_none()
        );
    }
    let result_scope = binding(&engine, &second.dataset, "result/b").await;
    assert_eq!(result_scope.protections, second_scope.protections);
    let succession: Succession = engine
        .inner
        .lifecycle
        .get(&view, "succession/b")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(succession.predecessor, Some(prior.version_ref()));
    assert_eq!(succession.selected, selected.version_ref());
}

#[tokio::test]
async fn retired_bound_generation_does_not_lend_scope_to_an_unknown_recomputation() {
    let (_root, engine, input_a, input_b) = fixture().await;
    let build = request("consumer", "a", &input_a);
    assert_eq!(
        engine
            .run_until(&build, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let original_admission = phase(&engine, &build.dataset, "admission").await;
    let original_processing = phase(&engine, &build.dataset, "processing").await;
    let admission_binding = binding(&engine, &build.dataset, &original_admission.0).await;
    let processing_binding = binding(&engine, &build.dataset, &original_processing.0).await;
    let plan = engine
        .plan_phase_retirement(
            &build.dataset,
            &id("old-generation"),
            &[original_admission.0.clone(), original_processing.0.clone()],
            "discard the original recovery generations",
        )
        .await
        .unwrap();
    engine.retire_phases(&plan).await.unwrap();
    let view = engine.inner.lifecycle.open(&build.dataset).await.unwrap();
    let operation: Operation = engine
        .inner
        .lifecycle
        .get(&view, &format!("operation/{}", build.operation))
        .await
        .unwrap()
        .unwrap();
    assert!(
        engine
            .request_protections(&view, &build.operation)
            .await
            .unwrap()
            .is_some()
    );
    let mut older_writer_completion = original_admission.2.clone();
    older_writer_completion.outputs = vec![input_b.clone()];
    // An older writer can acknowledge the new phase without protection metadata.
    // The earlier binding row remains physically present but describes retired work.
    let completed = engine
        .complete(
            &OperationRequest::Build(build.clone()),
            &operation,
            older_writer_completion,
            None,
        )
        .await
        .unwrap();
    assert!(completed.protections.is_none());
    let view = engine.inner.lifecycle.open(&build.dataset).await.unwrap();
    assert!(
        engine
            .retained_protections(&view, &original_admission.0)
            .await
            .unwrap()
            .is_none()
    );
    let retained = engine
        .phase(
            &OperationRequest::Build(build.clone()),
            &operation,
            "admission",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.value.outputs, vec![input_b.clone()]);
    assert!(retained.protections.is_none());
    assert_eq!(
        engine
            .run_until(&build, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let recomputed = phase(&engine, &build.dataset, "processing").await;
    assert!(recomputed.1 > original_processing.1);
    assert_ne!(recomputed.2.table, original_processing.2.table);
    assert_eq!(
        recomputed.2.dependencies.as_ref().unwrap().admitted_inputs["input"],
        input_b
    );
    let view = engine.inner.lifecycle.open(&build.dataset).await.unwrap();
    assert!(
        engine
            .retained_protections(&view, &recomputed.0)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        engine
            .run_until(&build, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let validation = phase(&engine, &build.dataset, "validation").await;
    let view = engine.inner.lifecycle.open(&build.dataset).await.unwrap();
    assert!(
        engine
            .retained_protections(&view, &validation.0)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        engine
            .retained_protections(&view, "readback/a")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        engine
            .inner
            .lifecycle
            .get::<ReferenceBinding>(&view, &binding_key(&original_admission.0))
            .await
            .unwrap()
            .unwrap(),
        admission_binding
    );
    assert_eq!(
        engine
            .inner
            .lifecycle
            .get::<ReferenceBinding>(&view, &binding_key(&original_processing.0))
            .await
            .unwrap()
            .unwrap(),
        processing_binding
    );
    let readback: ReleasePin = engine
        .inner
        .lifecycle
        .get(&view, "readback/a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(readback.dataset.inputs["input"], input_b);
}

#[tokio::test]
async fn an_earlier_binding_requires_retirement_of_its_exact_native_generation() {
    let (_root, engine, input_a, _) = fixture().await;
    let build = request("consumer", "a", &input_a);
    assert_eq!(
        engine
            .run_until(&build, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let original = phase(&engine, &build.dataset, "processing").await;
    binding(&engine, &build.dataset, &original.0).await;
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&build.dataset).await.unwrap();
    // Rewriting a claim without its binding is a narrowly scoped corruption
    // fixture. It must never look like normal reuse of the earlier generation.
    let unretired = lifecycle
        .commit(&view, vec![record(&original.0, &original.2).unwrap()])
        .await
        .unwrap();
    assert_eq!(
        engine
            .retained_protections(&unretired, &original.0)
            .await
            .err()
            .unwrap()
            .code(),
        "invalid_owner_protection"
    );
    let plan = engine
        .plan_phase_retirement(
            &build.dataset,
            &id("different-generation"),
            std::slice::from_ref(&original.0),
            "retire only the newer unbound generation",
        )
        .await
        .unwrap();
    assert_ne!(plan.records[0].revision, original.1);
    engine.retire_phases(&plan).await.unwrap();
    let view = lifecycle.open(&build.dataset).await.unwrap();
    let newer = lifecycle
        .commit(&view, vec![record(&original.0, &original.2).unwrap()])
        .await
        .unwrap();
    assert_eq!(
        engine
            .retained_protections(&newer, &original.0)
            .await
            .err()
            .unwrap()
            .code(),
        "invalid_owner_protection"
    );
}

#[tokio::test]
async fn refresh_evidence_inherits_the_exact_captured_producer_scope() {
    let (_root, engine, input_a, input_b) = fixture().await;
    let left = engine
        .release(&input_a.owner, &id("initial"))
        .await
        .unwrap();
    let right = engine
        .release(&input_b.owner, &id("initial"))
        .await
        .unwrap();
    let refresh = RefreshSpec {
        dataset: id("joined"),
        operation: id("initial"),
        base: None,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Join(JoinTransform {
            left: left.version_ref(),
            right: right.version_ref(),
            join: JoinDefinition {
                keys: vec![JoinKey {
                    left: "id".into(),
                    right: "id".into(),
                }],
                kind: JoinKind::Inner,
                nulls: NullMatching::Equal,
                cardinality: JoinCardinality::ManyToMany,
                fields: vec![JoinField {
                    side: JoinSide::Left,
                    field: "id".into(),
                    name: "id".into(),
                }],
                filter: None,
                projections: BTreeMap::new(),
            },
            filter: None,
            projections: BTreeMap::new(),
        }),
    };
    assert_eq!(
        engine
            .refresh_until(&refresh, "evidence")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let request_binding = binding(
        &engine,
        &refresh.dataset,
        &admission::dependencies_key(&refresh.operation),
    )
    .await;
    assert_eq!(
        request_binding
            .protections
            .iter()
            .map(|reference| reference.owner.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([input_a.owner.clone(), input_b.owner.clone()])
    );
    let evidence = phase(&engine, &refresh.dataset, "evidence").await;
    let processing = phase(&engine, &refresh.dataset, "processing").await;
    assert_eq!(
        evidence.2.dependencies.as_ref().unwrap().tables,
        vec![NativeSnapshotRef::from(
            processing.2.table.as_ref().unwrap()
        )]
    );
    let evidence_binding = binding(&engine, &refresh.dataset, &evidence.0).await;
    assert_eq!(evidence_binding.protections, request_binding.protections);
    let mut owner_revisions = BTreeMap::new();
    for owner in [&input_a.owner, &input_b.owner] {
        owner_revisions.insert(
            owner.clone(),
            engine.inner.lifecycle.open(owner).await.unwrap().revision,
        );
    }
    assert_eq!(
        engine
            .refresh_until(&refresh, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    assert_eq!(phase(&engine, &refresh.dataset, "evidence").await, evidence);
    let validation = phase(&engine, &refresh.dataset, "validation").await;
    let validation_binding = binding(&engine, &refresh.dataset, &validation.0).await;
    let readback_binding = binding(&engine, &refresh.dataset, "readback/initial").await;
    assert_eq!(validation_binding.protections, evidence_binding.protections);
    assert_eq!(readback_binding.protections, evidence_binding.protections);
    let selected = engine.refresh(&refresh).await.unwrap();
    let result_binding = binding(&engine, &refresh.dataset, "result/initial").await;
    let (succession_revision, link) =
        succession(&engine, &refresh.dataset, &refresh.operation).await;
    assert_eq!(result_binding.protections, readback_binding.protections);
    assert_eq!(succession_revision, result_binding.revision);
    assert_eq!(link.selected, selected.version_ref());
    let view = engine.inner.lifecycle.open(&refresh.dataset).await.unwrap();
    assert!(
        engine
            .inner
            .lifecycle
            .get::<ReferenceBinding>(&view, &binding_key("input-progress/initial"))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(engine.refresh(&refresh).await.unwrap(), selected);
    for (owner, revision) in owner_revisions {
        assert_eq!(
            engine.inner.lifecycle.open(&owner).await.unwrap().revision,
            revision
        );
    }
}

#[tokio::test]
async fn schema_admission_and_coordinated_groups_preserve_foreign_base_scope() {
    // Match the scalar recovery fixture: its concurrent reader/writer and
    // metadata reservations already exceed the smaller Build-only profile.
    let (_root, engine, input_a, _) = fixture_with_limits(Limits {
        memory_bytes: 512 << 20,
        ..limits()
    })
    .await;
    let initial = request("consumer", "initial", &input_a);
    let base = engine.run(&initial).await.unwrap();
    let original_scope = binding(
        &engine,
        &base.dataset.dataset,
        &admission::dependencies_key(&base.id),
    )
    .await;
    let enriched = engine
        .backfill(&BackfillSpec {
            dataset: base.dataset.dataset.clone(),
            operation: id("derived"),
            base: base.id.clone(),
            definition: "v1".into(),
            group: id("next"),
            group_definition: "next-v1".into(),
            fields: vec![ComputedField {
                id: 10,
                name: "next_id".into(),
                expression: "id + 1".into(),
            }],
        })
        .await
        .unwrap();
    let schema = SchemaSpec {
        dataset: enriched.dataset.dataset.clone(),
        operation: id("renamed"),
        base: enriched.id.clone(),
        definition: "v1".into(),
        next_definition: "v2".into(),
        fields: vec![SchemaField {
            id: 1,
            name: "comment_id".into(),
        }],
        additions: vec![],
        write_defaults: BTreeMap::new(),
    };
    assert_eq!(
        engine
            .schema_until(&schema, "admission")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let records = engine.inspect(&schema.dataset).await.unwrap();
    let (admission_key, admission) = records
        .iter()
        .filter(|(key, _)| key.starts_with("phase/") && !key.starts_with("phase/backfill/"))
        .find_map(|(key, value)| {
            let phase: PhaseResult = serde_json::from_value(value.clone()).unwrap();
            (phase.phase == "admission" && phase.operation == schema.operation)
                .then(|| (key.clone(), phase))
        })
        .unwrap();
    assert_eq!(admission.outputs, vec![enriched.manifest.clone()]);
    assert_eq!(
        admission.dependencies.as_ref().unwrap().base_manifest,
        Some(enriched.manifest.clone())
    );
    let schema_binding = binding(&engine, &schema.dataset, &admission_key).await;
    assert_eq!(schema_binding.protections, original_scope.protections);
    assert!(
        schema_binding
            .protections
            .iter()
            .any(|reference| reference.owner == input_a.owner)
    );
    let owner_revision = engine
        .inner
        .lifecycle
        .open(&input_a.owner)
        .await
        .unwrap()
        .revision;
    assert_eq!(
        engine
            .schema_until(&schema, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let groups_key = format!("groups/{}", schema.operation);
    let groups_binding = binding(&engine, &schema.dataset, &groups_key).await;
    let readback_key = format!("readback/{}", schema.operation);
    let readback_binding = binding(&engine, &schema.dataset, &readback_key).await;
    assert_eq!(groups_binding.protections, schema_binding.protections);
    assert_eq!(readback_binding.protections, schema_binding.protections);
    let view = engine.inner.lifecycle.open(&schema.dataset).await.unwrap();
    let readback: ReleasePin = engine
        .inner
        .lifecycle
        .get(&view, &readback_key)
        .await
        .unwrap()
        .unwrap();
    let groups: CompletedGroups = engine
        .inner
        .lifecycle
        .get(&view, &groups_key)
        .await
        .unwrap()
        .unwrap();
    let CompletedGroups::Captured { outputs, source } = groups else {
        panic!("new coordinated completion must retain the exact source generation");
    };
    assert_eq!(
        source,
        NativeSnapshotRef::from(&readback.dataset.groups["source"])
    );
    assert_eq!(outputs["next"], readback.dataset.groups["next"].metadata);
    assert_eq!(readback.dataset.inputs["input"], input_a);
    assert_eq!(readback.dataset.definition, schema.next_definition);
    assert_eq!(
        engine
            .inner
            .lifecycle
            .open(&input_a.owner)
            .await
            .unwrap()
            .revision,
        owner_revision
    );
}

//! Canonical scalar acknowledgment preserves winner outputs and retained protection.

use super::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        scratch_bytes: 16 << 20,
        max_batch_bytes: 4 << 20,
        ..Limits::default()
    }
}

struct Fixture {
    request: OperationRequest,
    operation: Operation,
    expressions: Arc<Expressions>,
    key: String,
    first: FileCompletion,
    second: FileCompletion,
}

async fn fixture(engine: &Engine) -> Fixture {
    let dataset = id("scalar");
    let source = engine
        .write_frame(
            &dataset,
            &engine.inner.root.join("objects/scalar/source"),
            engine
                .context()
                .sql("SELECT CAST(1 AS BIGINT) AS value")
                .await
                .unwrap(),
        )
        .await
        .unwrap()
        .table;
    let spec = BackfillSpec {
        dataset: dataset.clone(),
        operation: id("derive"),
        base: id("base"),
        definition: "v1".into(),
        group: id("derived"),
        group_definition: "scalar-v1".into(),
        fields: vec![ComputedField {
            id: 10,
            name: "answer".into(),
            expression: "value + 1".into(),
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
    let native = storage::open_pin(&source, &engine.inner.lifecycle.budget)
        .await
        .unwrap();
    let expressions = Arc::new(
        Expressions::compile(
            &engine.context(),
            native.metadata().current_schema(),
            &spec.group_definition(),
            None,
        )
        .unwrap(),
    );
    let scan = storage::OccurrenceScan::open(&native, &engine.inner.lifecycle.budget)
        .await
        .unwrap();
    assert_eq!(scan.files().count(), 1);
    let objects = source_objects(&source);
    let file = scan.files().next().unwrap();
    let object = objects[file.file_path()];
    let key = file_key(object, &expressions, operation.eligibility).unwrap();
    let mut candidates = Vec::new();
    for _ in 0..2 {
        let directory = engine
            .reserve_group(&request, &operation, "processing")
            .await
            .unwrap();
        let output = engine
            .create_output_at(&dataset, &directory, expressions.native.clone())
            .await
            .unwrap();
        let mut completion = engine
            .compute_file(
                &dataset,
                &directory,
                &scan,
                object,
                file.record_count(),
                &output,
                expressions.clone(),
            )
            .await
            .unwrap();
        completion.source_metadata = Some(source.metadata.clone());
        completion.execution = Some(OriginalExecution {
            operation: request.operation().clone(),
            declaration: origins::write_at(
                &engine.inner.objects,
                &dataset,
                &directory.child("declaration.json").unwrap(),
                &OriginalDeclaration {
                    processor: PROCESSOR.into(),
                    definition: spec.group_definition(),
                    input_fields: expressions.input_fields.clone(),
                },
                &engine.inner.lifecycle.budget,
            )
            .await
            .unwrap(),
        });
        candidates.push(completion);
    }
    let second = candidates.pop().unwrap();
    let first = candidates.pop().unwrap();
    assert_ne!(first.outputs, second.outputs);
    Fixture {
        request,
        operation,
        expressions,
        key,
        first,
        second,
    }
}

fn pending(key: &str, completion: &FileCompletion) -> BTreeMap<String, FileCompletion> {
    BTreeMap::from([(key.into(), completion.clone())])
}

fn assembly(engine: &Engine, dataset: &Id) -> FileAssembly {
    FileAssembly {
        files: Vec::new(),
        inherited: Vec::new(),
        origins: Vec::new(),
        protections: Some(
            ReferenceProtections::empty(dataset, &engine.inner.lifecycle.budget).unwrap(),
        ),
        bytes: 0,
    }
}

#[tokio::test]
async fn completion_descriptors_refuse_duplicate_outputs_and_changed_bytes() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), limits()).await.unwrap();
    let fixture = fixture(&engine).await;
    let mut duplicate = fixture.first.clone();
    duplicate.outputs.push(duplicate.outputs[0].clone());
    assert_eq!(
        read_completion(
            &duplicate,
            &fixture.expressions.native,
            &engine.inner.lifecycle.budget
        )
        .await
        .unwrap_err()
        .code(),
        "invalid_group"
    );
    let mut changed = fixture.first.clone();
    changed.descriptor.digest = "0".repeat(64);
    assert_eq!(
        read_completion(
            &changed,
            &fixture.expressions.native,
            &engine.inner.lifecycle.budget
        )
        .await
        .unwrap_err()
        .code(),
        "corrupt_object"
    );
    changed = fixture.first.clone();
    changed.descriptor.bytes -= 1;
    assert_eq!(
        read_completion(
            &changed,
            &fixture.expressions.native,
            &engine.inner.lifecycle.budget
        )
        .await
        .unwrap_err()
        .code(),
        "corrupt_object"
    );
}

#[tokio::test]
async fn concurrent_acknowledgments_assemble_one_winner_with_its_own_scope() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), limits()).await.unwrap();
    let fixture = fixture(&engine).await;
    let mut first = pending(&fixture.key, &fixture.first);
    let mut second = pending(&fixture.key, &fixture.second);
    let scope =
        ReferenceProtections::empty(fixture.request.dataset(), &engine.inner.lifecycle.budget)
            .unwrap();
    // Both workers completed different physical outputs before either acknowledgment.
    // Their protection differs, so adopting the loser's scope is observable too.
    let (first, second) = tokio::join!(
        engine.acknowledge_files(
            &fixture.request,
            &fixture.operation,
            &mut first,
            Some(&scope)
        ),
        engine.acknowledge_files(&fixture.request, &fixture.operation, &mut second, None),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_eq!(first[0].generation, second[0].generation);
    assert_eq!(first[0].completion.outputs, second[0].completion.outputs);
    assert_eq!(
        first[0].completion.descriptor,
        second[0].completion.descriptor
    );
    assert_eq!(
        first[0].protections.is_some(),
        second[0].protections.is_some()
    );
    let winner = &first[0].completion;
    let loser = if winner.outputs == fixture.first.outputs {
        &fixture.second
    } else {
        &fixture.first
    };
    assert_eq!(
        first[0].protections.is_some(),
        winner.outputs == fixture.first.outputs
    );
    // Only the canonical descriptor may be read, even when the loser is unavailable.
    std::fs::remove_file(&loser.descriptor.path).unwrap();
    let expected = winner.outputs.clone();
    let execution = winner.execution.clone();
    let generation = first[0].generation.clone();
    let mut first_assembly = assembly(&engine, fixture.request.dataset());
    let mut second_assembly = assembly(&engine, fixture.request.dataset());
    first_assembly
        .append(
            first,
            &fixture.expressions,
            0,
            &engine.inner.lifecycle.budget,
            &mut crate::engine::verification::Checks::composition(),
        )
        .await
        .unwrap();
    second_assembly
        .append(
            second,
            &fixture.expressions,
            0,
            &engine.inner.lifecycle.budget,
            &mut crate::engine::verification::Checks::composition(),
        )
        .await
        .unwrap();
    assert_eq!(first_assembly.inherited, expected);
    assert_eq!(second_assembly.inherited, expected);
    for assembled in [&first_assembly, &second_assembly] {
        assert_eq!(assembled.origins.len(), 1);
        assert_eq!(assembled.origins[0].generation, generation);
        assert_eq!(assembled.origins[0].completion.execution, execution);
        assert_eq!(assembled.origins[0].completion.outputs, expected);
    }
    assert_eq!(
        first_assembly.protections.is_some(),
        second_assembly.protections.is_some()
    );
    let view = engine
        .inner
        .lifecycle
        .open(fixture.request.dataset())
        .await
        .unwrap();
    let rows = engine.inner.lifecycle.rows(&view).await.unwrap();
    assert_eq!(rows.revision(&fixture.key), Some(generation.revision));
    engine
        .check_reference_binding(&view, &fixture.key)
        .await
        .unwrap();
}

#[tokio::test]
async fn retired_completion_gets_a_new_generation_and_retry_keeps_that_winner() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), limits()).await.unwrap();
    let fixture = fixture(&engine).await;
    let scope =
        ReferenceProtections::empty(fixture.request.dataset(), &engine.inner.lifecycle.budget)
            .unwrap();
    let mut pending_first = pending(&fixture.key, &fixture.first);
    let original = engine
        .acknowledge_files(
            &fixture.request,
            &fixture.operation,
            &mut pending_first,
            None,
        )
        .await
        .unwrap()
        .remove(0);
    let plan = engine
        .plan_phase_retirement(
            fixture.request.dataset(),
            &id("retire"),
            std::slice::from_ref(&fixture.key),
            "replace retired work",
        )
        .await
        .unwrap();
    engine.retire_phases(&plan).await.unwrap();
    let mut pending_second = pending(&fixture.key, &fixture.second);
    let replacement = engine
        .acknowledge_files(
            &fixture.request,
            &fixture.operation,
            &mut pending_second,
            Some(&scope),
        )
        .await
        .unwrap();
    assert!(pending_second.is_empty());
    assert!(replacement[0].generation.revision > original.generation.revision);
    assert_eq!(replacement[0].completion.outputs, fixture.second.outputs);
    assert!(replacement[0].protections.is_some());
    assert_eq!(
        stop_after(Some("files"), "files").unwrap_err().code(),
        "injected_failure"
    );
    let generation = replacement[0].generation.clone();
    drop(replacement);
    drop(engine);
    let engine = Engine::open(root.path(), limits()).await.unwrap();
    let mut retry = pending(&fixture.key, &fixture.first);
    let retained = engine
        .acknowledge_files(&fixture.request, &fixture.operation, &mut retry, None)
        .await
        .unwrap();
    assert_eq!(retained[0].generation, generation);
    assert_eq!(retained[0].completion.outputs, fixture.second.outputs);
    assert!(retained[0].protections.is_some());
    let mut assembled = assembly(&engine, fixture.request.dataset());
    assembled
        .append(
            retained,
            &fixture.expressions,
            0,
            &engine.inner.lifecycle.budget,
            &mut crate::engine::verification::Checks::composition(),
        )
        .await
        .unwrap();
    assert_eq!(assembled.inherited, fixture.second.outputs);
    assert!(assembled.protections.is_some());
}

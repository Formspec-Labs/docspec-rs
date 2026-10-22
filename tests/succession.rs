//! Accepted version order is independent of input timestamps and change accelerators.

use super::scalar;

use arrow::array::{Int64Array, StringArray};
use docspec::{
    Engine,
    budget::{Budget, Limits},
    catalog::{Lifecycle, record},
    engine::verify_release,
    model::{
        Authority, OperationRequest, ReleasePin, RunSpec, Succession, SuccessionProof, UpdateSpec,
    },
    storage,
};
use futures::TryStreamExt;
use scalar::{fixture, id, input, limits, spec};
use std::{collections::BTreeMap, path::Path};
use tempfile::TempDir;

fn build(base: &ReleasePin, operation: &str) -> RunSpec {
    RunSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(operation),
        base: None,
        relationship: None,
        input: base.dataset.inputs["input"].path.clone(),
        expected_digest: base.dataset.inputs["input"].digest.clone(),
        sql: "SELECT * FROM input".into(),
        definition: base.dataset.definition.clone(),
        partition_by: Vec::new(),
    }
}

async fn record_revisions(root: &Path) -> BTreeMap<String, i64> {
    let catalog = storage::local_catalog(&root.join("workspace"))
        .await
        .unwrap();
    let budget = Budget::new(limits()).unwrap();
    let mut revisions = BTreeMap::new();
    for namespace in catalog.list_namespaces(None).await.unwrap() {
        for ident in catalog.list_tables(&namespace).await.unwrap() {
            if ident.name() != "lifecycle" {
                continue;
            }
            let native = catalog.load_table(&ident).await.unwrap();
            let stream = storage::read_batches(&native, &budget).await.unwrap();
            futures::pin_mut!(stream);
            while let Some(batch) = stream.try_next().await.unwrap() {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for (key, revision) in keys.iter().zip(values.iter()) {
                    revisions
                        .entry(key.unwrap().to_owned())
                        .and_modify(|old: &mut i64| *old = (*old).max(revision.unwrap()))
                        .or_insert(revision.unwrap());
                }
            }
        }
    }
    revisions
}

#[tokio::test]
async fn every_selection_records_actual_predecessor_in_the_same_native_commit() {
    let root = TempDir::new().unwrap();
    let (engine, first, _) = fixture(root.path(), false).await;
    let plain = engine
        .run(&build(&first, "aaa-unbased-rebuild"))
        .await
        .unwrap();
    assert!(plain.dataset.parent_manifest.is_none());
    assert_eq!(plain.dataset.groups, first.dataset.groups);
    let a = engine
        .backfill(&spec(&plain, "a", "a", "length", "length(text)"))
        .await
        .unwrap();
    // The request captures plain, but a is the actual predecessor when b selects.
    let b_request = spec(&plain, "b", "b", "next", "id + 1");
    let b = engine.backfill(&b_request).await.unwrap();
    assert_eq!(a.dataset.boundary, b.dataset.boundary);
    let update = UpdateSpec {
        dataset: first.dataset.dataset.clone(),
        operation: id("updated"),
        definition: "v1".into(),
        base: b.id.clone(),
        keys: vec!["id".into()],
        replacements: input(
            &root.path().join("changed.parquet"),
            &[(Some(1), Some("new".into()))],
        ),
        deletions: None,
        merge: None,
        relationship: None,
    };
    let updated = engine.update(&update).await.unwrap();
    let mut request = build(&updated, "rebuilt");
    request.base = Some(updated.id.clone());
    let rebuilt = engine.run(&request).await.unwrap();
    let versions = [&first, &plain, &a, &b, &updated, &rebuilt];
    let records = engine.inspect(&first.dataset.dataset).await.unwrap();
    for (index, version) in versions.iter().enumerate() {
        let link: Succession =
            serde_json::from_value(records[&format!("succession/{}", version.id)].clone()).unwrap();
        assert_eq!(link.selected, version.version_ref());
        assert_eq!(
            link.predecessor,
            index
                .checked_sub(1)
                .map(|previous| versions[previous].version_ref())
        );
        assert_eq!(link.boundary, version.dataset.boundary);
        assert_eq!(link.eligibility, version.dataset.eligibility);
    }
    let revisions = record_revisions(root.path()).await;
    for version in versions {
        assert_eq!(
            revisions[&format!("result/{}", version.id)],
            revisions[&format!("succession/{}", version.id)]
        );
    }
    assert_eq!(revisions["authority"], revisions["succession/rebuilt"]);
    let proof = engine
        .prove_succession(&first.version_ref(), &rebuilt.version_ref())
        .await
        .unwrap();
    assert_eq!(proof.selections_advanced, 5);
    assert_eq!(proof.selections_behind, 0);
    let proof = engine
        .prove_succession(&first.version_ref(), &plain.version_ref())
        .await
        .unwrap();
    assert_eq!(proof.selections_advanced, 1);
    assert_eq!(proof.selections_behind, 4);
    assert_eq!(proof.observed_head, rebuilt.version_ref());
    let equal = engine
        .prove_succession(&plain.version_ref(), &plain.version_ref())
        .await
        .unwrap();
    assert_eq!(equal.selections_advanced, 0);
    assert_eq!(equal.selections_behind, 4);
    assert_eq!(
        engine
            .prove_succession(&b.version_ref(), &a.version_ref())
            .await
            .unwrap_err()
            .code(),
        "coverage_regression"
    );
    assert_eq!(engine.backfill(&b_request).await.unwrap(), b);
    assert_eq!(engine.run(&request).await.unwrap(), rebuilt);
    assert_eq!(
        engine.inspect(&first.dataset.dataset).await.unwrap(),
        records
    );
    // Succession survives unavailable optional accelerators and input payloads.
    std::fs::remove_file(&first.dataset.inputs["input"].path).unwrap();
    let changes = engine
        .change_certificate(&updated.version_ref())
        .await
        .unwrap()
        .unwrap()
        .changes
        .source
        .unwrap();
    for table in [&changes.added, &changes.removed] {
        for object in &table.objects {
            if object.path.ends_with(".parquet") && Path::new(&object.path).exists() {
                std::fs::remove_file(&object.path).unwrap();
            }
        }
    }
    assert!(verify_release(&first, limits()).await.is_err());
    assert!(
        engine
            .prove_succession(&first.version_ref(), &rebuilt.version_ref())
            .await
            .is_ok()
    );
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn admission_and_prepared_releases_do_not_create_accepted_history() {
    let root = TempDir::new().unwrap();
    let (engine, first, _) = fixture(root.path(), false).await;
    let mut older = build(&first, "older");
    older.sql = "SELECT * FROM input WHERE id < 20".into();
    assert_eq!(
        engine
            .run_until(&older, "readback")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let records = engine.inspect(&first.dataset.dataset).await.unwrap();
    assert!(!records.contains_key("succession/older"));
    let candidate: ReleasePin = serde_json::from_value(records["readback/older"].clone()).unwrap();
    let newer = build(&first, "newer");
    assert_eq!(
        engine
            .run_until(&newer, "selection")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let before_retry = engine.inspect(&first.dataset.dataset).await.unwrap();
    let selected = engine.run(&newer).await.unwrap();
    assert_eq!(
        engine.inspect(&first.dataset.dataset).await.unwrap(),
        before_retry
    );
    assert_eq!(
        engine.run(&older).await.unwrap_err().code(),
        "coverage_regression"
    );
    assert_eq!(
        engine
            .prove_succession(&first.version_ref(), &candidate.version_ref())
            .await
            .unwrap_err()
            .code(),
        "unproven_succession"
    );
    let proof = engine
        .prove_succession(&first.version_ref(), &selected.version_ref())
        .await
        .unwrap();
    assert_eq!(proof.selections_advanced, 1);
    assert_eq!(
        engine.latest(&first.dataset.dataset).await.unwrap(),
        selected
    );
}

#[tokio::test]
async fn identity_mismatch_and_orphan_branches_cannot_establish_coverage() {
    let root = TempDir::new().unwrap();
    let (engine, first, _) = fixture(root.path(), false).await;
    let second = engine.run(&build(&first, "second")).await.unwrap();
    let mut wrong = first.version_ref();
    wrong.manifest.digest = "0".repeat(64);
    assert_eq!(
        engine
            .prove_succession(&wrong, &second.version_ref())
            .await
            .unwrap_err()
            .code(),
        "invalid_succession"
    );
    wrong = first.version_ref();
    wrong.dataset = id("another");
    assert_eq!(
        engine
            .prove_succession(&wrong, &second.version_ref())
            .await
            .unwrap_err()
            .code(),
        "invalid_succession"
    );
    // A plausible result/link pair outside the accepted head's chain is insufficient.
    let mut orphan = first.clone();
    orphan.id = id("orphan");
    orphan.dataset.version = orphan.id.clone();
    if let OperationRequest::Build(spec) = &mut orphan.dataset.request {
        spec.operation = orphan.id.clone();
    }
    let lifecycle = Lifecycle::new(
        storage::local_catalog(&root.path().join("workspace"))
            .await
            .unwrap(),
        Budget::new(limits()).unwrap(),
    );
    let view = lifecycle.open(&first.dataset.dataset).await.unwrap();
    lifecycle
        .commit(
            &view,
            vec![
                record("result/orphan", &orphan).unwrap(),
                record(
                    "succession/orphan",
                    Succession {
                        selected: orphan.version_ref(),
                        predecessor: Some(first.version_ref()),
                        boundary: first.dataset.boundary,
                        eligibility: first.dataset.eligibility,
                    },
                )
                .unwrap(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        engine
            .prove_succession(&first.version_ref(), &orphan.version_ref())
            .await
            .unwrap_err()
            .code(),
        "unproven_succession"
    );
    assert_eq!(
        engine
            .prove_succession(&orphan.version_ref(), &orphan.version_ref())
            .await
            .unwrap_err()
            .code(),
        "unproven_succession"
    );
    assert!(
        engine
            .prove_succession(&first.version_ref(), &second.version_ref())
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn missing_malformed_rebound_and_cyclic_history_refuses_without_payload_reads() {
    for kind in [
        "missing",
        "malformed",
        "revision",
        "predecessor_revision",
        "head_revision",
        "head_revision_later_authority",
        "boundary",
        "cycle",
    ] {
        let root = TempDir::new().unwrap();
        let (engine, first, _) = fixture(root.path(), true).await;
        let second = engine.run(&build(&first, "second")).await.unwrap();
        let lifecycle = Lifecycle::new(
            storage::local_catalog(&root.path().join("workspace"))
                .await
                .unwrap(),
            Budget::new(limits()).unwrap(),
        );
        let view = lifecycle.open(&first.dataset.dataset).await.unwrap();
        let mut link: Succession = lifecycle
            .get(&view, "succession/second")
            .await
            .unwrap()
            .unwrap();
        let mut changes = match kind {
            "missing" => {
                link.predecessor.as_mut().unwrap().version = id("missing");
                vec![
                    record("succession/second", &link).unwrap(),
                    record("result/second", &second).unwrap(),
                ]
            }
            "malformed" => {
                vec![record("succession/second", serde_json::json!({"unexpected": true})).unwrap()]
            }
            "revision" => vec![record("succession/second", &link).unwrap()],
            "head_revision" | "head_revision_later_authority" => vec![
                record("succession/second", &link).unwrap(),
                record("result/second", &second).unwrap(),
            ],
            "predecessor_revision" => {
                let first_key = format!("succession/{}", first.id);
                let first_link: Succession =
                    lifecycle.get(&view, &first_key).await.unwrap().unwrap();
                // Pair equality still holds, but this older selection now has a
                // later native revision than its child on the accepted chain.
                vec![
                    record(first_key, first_link).unwrap(),
                    record(format!("result/{}", first.id), &first).unwrap(),
                ]
            }
            "boundary" => {
                link.boundary += 1;
                vec![
                    record("succession/second", &link).unwrap(),
                    record("result/second", &second).unwrap(),
                ]
            }
            "cycle" => {
                link.predecessor = Some(second.version_ref());
                vec![
                    record("succession/second", &link).unwrap(),
                    record("result/second", &second).unwrap(),
                ]
            }
            _ => unreachable!(),
        };
        if matches!(kind, "missing" | "boundary" | "cycle") {
            // Keep the synthetic head anchor consistent so these fixtures reach
            // the intended link checks. Head rewriting is tested separately.
            let mut authority: Authority =
                lifecycle.get(&view, "authority").await.unwrap().unwrap();
            authority.selection_revision = Some(view.revision + 1);
            changes.push(record("authority", authority).unwrap());
        }
        lifecycle.commit(&view, changes).await.unwrap();
        if kind == "head_revision_later_authority" {
            engine
                .invalidate(&first.dataset.dataset, "later invalidation", None)
                .await
                .unwrap();
        }
        let error = engine
            .prove_succession(&first.version_ref(), &second.version_ref())
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            if kind == "missing" {
                "unavailable_succession"
            } else {
                "invalid_succession"
            },
            "{kind}: {error}"
        );
        if matches!(kind, "head_revision" | "head_revision_later_authority") {
            assert_eq!(
                engine
                    .prove_succession(&second.version_ref(), &second.version_ref())
                    .await
                    .unwrap_err()
                    .code(),
                "invalid_succession"
            );
            assert_eq!(
                engine
                    .run(&build(&second, "third"))
                    .await
                    .unwrap_err()
                    .code(),
                "invalid_succession",
                "a new selection must not adopt a replaced head commit"
            );
        }
    }
}

#[tokio::test]
async fn selected_head_cannot_predeclare_a_future_commit_or_disagree_with_its_release() {
    for kind in [
        "future",
        "dataset",
        "version",
        "boundary",
        "eligibility",
        "verified",
    ] {
        let root = TempDir::new().unwrap();
        let (engine, first, _) = fixture(root.path(), true).await;
        let lifecycle = Lifecycle::new(
            storage::local_catalog(&root.path().join("workspace"))
                .await
                .unwrap(),
            Budget::new(limits()).unwrap(),
        );
        let view = lifecycle.open(&first.dataset.dataset).await.unwrap();
        let mut authority: Authority = lifecycle.get(&view, "authority").await.unwrap().unwrap();
        match kind {
            "future" => authority.selection_revision = Some(view.revision + 2),
            "dataset" => authority.selected.as_mut().unwrap().dataset = id("other"),
            "version" => authority.selected.as_mut().unwrap().version = id("other"),
            "boundary" => authority.selected.as_mut().unwrap().boundary += 1,
            "eligibility" => authority.selected.as_mut().unwrap().eligibility += 1,
            "verified" => authority.release.as_mut().unwrap().verified = false,
            _ => unreachable!(),
        }
        let view = lifecycle
            .commit(&view, vec![record("authority", authority).unwrap()])
            .await
            .unwrap();
        if kind == "future" {
            let key = format!("succession/{}", first.id);
            let link: Succession = lifecycle.get(&view, &key).await.unwrap().unwrap();
            lifecycle
                .commit(
                    &view,
                    vec![
                        record(key, link).unwrap(),
                        record(format!("result/{}", first.id), &first).unwrap(),
                    ],
                )
                .await
                .unwrap();
        }
        assert_eq!(
            engine
                .prove_succession(&first.version_ref(), &first.version_ref())
                .await
                .unwrap_err()
                .code(),
            "invalid_succession",
            "{kind}"
        );
    }
}

#[tokio::test]
async fn small_history_budget_refuses_and_transports_share_the_proof_service() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let root = TempDir::new().unwrap();
    let (engine, first, _) = fixture(root.path(), true).await;
    let second = engine.run(&build(&first, "second")).await.unwrap();
    let router = docspec::http::router(engine.clone());
    let response = router.clone().oneshot(Request::builder().method("POST").uri("/succession").header("content-type", "application/json").body(Body::from(serde_json::to_vec(&serde_json::json!({"prior": first.version_ref(), "target": second.version_ref()})).unwrap())).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let proof: SuccessionProof =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(proof.selections_advanced, 1);
    let response = router.clone().oneshot(Request::builder().method("POST").uri("/succession").header("content-type", "application/json").body(Body::from(serde_json::to_vec(&serde_json::json!({"prior": second.version_ref(), "target": first.version_ref()})).unwrap())).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    drop(router);
    drop(engine);
    for (name, pin) in [("first", &first), ("second", &second)] {
        std::fs::write(
            root.path().join(format!("{name}.json")),
            serde_json::to_vec(pin).unwrap(),
        )
        .unwrap();
    }
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(root.path().join("workspace"))
        .arg("check-succession")
        .arg(root.path().join("first.json"))
        .arg(root.path().join("second.json"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let cli: SuccessionProof = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(cli, proof);
    let limited = Engine::open(
        root.path().join("workspace"),
        Limits {
            max_metadata_bytes: 1024,
            ..limits()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        limited
            .prove_succession(&first.version_ref(), &second.version_ref())
            .await
            .unwrap_err()
            .code(),
        "metadata_capacity"
    );
}

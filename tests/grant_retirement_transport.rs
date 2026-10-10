//! CLI and HTTP share exact owner-grant retirement decisions and refusals.

use super::scalar;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use docspec::{
    Engine,
    budget::Budget,
    catalog::{Lifecycle, record},
    model::{ObjectRef, RetentionPlan, Retirement, RetirementScope, RunSpec},
    storage,
};
use scalar::{fixture, id, limits};
use tower::ServiceExt;

fn post(path: &str, value: &impl serde::Serialize) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(value).unwrap()))
        .unwrap()
}

#[tokio::test]
async fn grant_retirement_matches_cli_http_and_preserves_files_after_cold_retry() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let (engine, base, _) = fixture(root.path(), true).await;
    let source = &base.dataset.inputs["input"];
    engine
        .run(&RunSpec {
            dataset: id("consumer"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: source.path.clone(),
            expected_digest: source.digest.clone(),
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    let owner = id("comments");
    let records = engine.inspect(&owner).await.unwrap();
    let (live_key, live_grant) = records
        .iter()
        .find(|(key, _)| key.starts_with("owner-protection/"))
        .unwrap();
    let live_key = live_key.clone();
    let objects: Vec<ObjectRef> = serde_json::from_value(live_grant["objects"].clone()).unwrap();
    let retained_files: Vec<_> = objects
        .iter()
        .map(|object| {
            let metadata = std::fs::metadata(&object.path).unwrap();
            (
                object.path.clone(),
                metadata.len(),
                metadata.modified().unwrap(),
            )
        })
        .collect();

    // Create an unused grant only in this test's private catalog. A real live
    // grant supplies the native table identity and immutable owned objects.
    let lifecycle = Lifecycle::new(
        storage::local_catalog(&workspace).await.unwrap(),
        Budget::new(limits()).unwrap(),
    );
    let unused_consumer = id("unused-consumer");
    let view = lifecycle.open(&owner).await.unwrap();
    let mut unused_grant = live_grant.clone();
    unused_grant["consumer"] = serde_json::json!(unused_consumer);
    unused_grant["revision"] = serde_json::json!(view.revision + 1);
    let grant_digest =
        blake3::hash(&serde_json::to_vec(&(&owner, &unused_consumer, &objects)).unwrap());
    let unused_key = format!("owner-protection/{}", grant_digest.to_hex());
    lifecycle
        .commit(&view, vec![record(&unused_key, unused_grant).unwrap()])
        .await
        .unwrap();
    drop(lifecycle);
    let before = engine.inspect(&owner).await.unwrap();
    let limits_path = root.path().join("limits.json");
    let keys_path = root.path().join("grant-keys.json");
    let plan_path = root.path().join("grant-retirement.json");
    std::fs::write(&limits_path, serde_json::to_vec(&limits()).unwrap()).unwrap();
    std::fs::write(&keys_path, serde_json::to_vec(&[&unused_key]).unwrap()).unwrap();
    drop(engine);

    let planned = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(&limits_path)
        .args(["plan-grant-retirement", "comments", "unused-grant"])
        .arg(&keys_path)
        .arg("consumer no longer needs this grant")
        .output()
        .unwrap();
    assert!(
        planned.status.success(),
        "{}",
        String::from_utf8_lossy(&planned.stderr)
    );
    let cli_plan: RetentionPlan = serde_json::from_slice(&planned.stdout).unwrap();
    assert_eq!(cli_plan.scope, RetirementScope::OwnerGrants);
    assert_eq!(cli_plan.records.len(), 1);
    assert_eq!(cli_plan.records[0].key, unused_key);
    std::fs::write(&plan_path, &planned.stdout).unwrap();

    let engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(engine.inspect(&owner).await.unwrap(), before);
    let router = docspec::http::router(engine.clone());
    let request = serde_json::json!({
        "dataset": "comments",
        "retirement": "unused-grant",
        "keys": [unused_key],
        "reason": "consumer no longer needs this grant",
    });
    let response = router
        .clone()
        .oneshot(post("/retention/grants/plan", &request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let http_plan: RetentionPlan =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(http_plan, cli_plan);

    let mut live_request = request.clone();
    live_request["retirement"] = serde_json::json!("live-grant");
    live_request["keys"] = serde_json::json!([live_key]);
    let response = router
        .clone()
        .oneshot(post("/retention/grants/plan", &live_request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let refusal: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(refusal["code"], "grant_in_use");

    let response = router
        .clone()
        .oneshot(post("/retention/grants/retire", &http_plan))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let decision: Retirement =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    let response = router
        .clone()
        .oneshot(post("/retention/grants/plan", &request))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let refusal: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(refusal["code"], "grant_retired");
    let mut conflicting = http_plan;
    conflicting.reason = "different decision".into();
    assert_eq!(
        router
            .clone()
            .oneshot(post("/retention/grants/retire", &conflicting))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(engine.latest(&owner).await.unwrap(), base);
    for (path, bytes, modified) in &retained_files {
        let metadata = std::fs::metadata(path).unwrap();
        assert_eq!(metadata.len(), *bytes);
        assert_eq!(metadata.modified().unwrap(), *modified);
    }
    drop(router);
    drop(engine);

    let repeated = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(&limits_path)
        .arg("retire-grants")
        .arg(&plan_path)
        .output()
        .unwrap();
    assert!(
        repeated.status.success(),
        "{}",
        String::from_utf8_lossy(&repeated.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Retirement>(&repeated.stdout).unwrap(),
        decision
    );
}

#[tokio::test]
async fn foreign_seal_protects_old_scalar_origins_after_cache_keys_are_recomputed() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let family = id("family");
    let generation = id("g1");
    let source = scalar::input(
        &root.path().join("source.parquet"),
        &[(Some(1), Some("é🎉".into())), (Some(2), None)],
    );
    let base = engine
        .run(&RunSpec {
            dataset: id("comments"),
            operation: id("initial"),
            base: None,
            relationship: Some(docspec::model::SourceRelationship::Coupled {
                family: family.clone(),
                generation: generation.clone(),
            }),
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    let request = scalar::spec(&base, "lengths", "length", "text_length", "length(text)");
    let enriched = engine.backfill(&request).await.unwrap();
    let origins_ref = enriched.dataset.computed["length"].origins.clone().unwrap();
    let origins: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&origins_ref.path).unwrap()).unwrap();
    let mut originals = std::collections::BTreeMap::new();
    originals.insert(origins_ref.path.clone(), origins_ref.clone());
    for origin in origins["files"].as_array().unwrap() {
        let completion = &origin["completion"];
        for value in [
            &completion["execution"]["declaration"],
            &completion["descriptor"],
            &completion["source_metadata"],
        ] {
            let object: ObjectRef = serde_json::from_value(value.clone()).unwrap();
            originals.insert(object.path.clone(), object);
        }
    }
    assert!(originals.len() > 1);
    let records = engine.inspect(&request.dataset).await.unwrap();
    let prefixes = [
        "phase/backfill/",
        "backfill-prepared/",
        "backfill-validation/",
    ];
    for prefix in prefixes {
        assert!(records.keys().any(|key| key.starts_with(prefix)));
    }
    let keys = records
        .keys()
        .filter(|key| prefixes.iter().any(|prefix| key.starts_with(prefix)))
        .cloned()
        .collect::<Vec<_>>();
    let retirement = engine
        .plan_phase_retirement(
            &request.dataset,
            &id("retire-scalar-cache"),
            &keys,
            "replace scalar recovery generations while retaining the selected result",
        )
        .await
        .unwrap();
    engine.retire_phases(&retirement).await.unwrap();
    let replacement = scalar::spec(
        &enriched,
        "recomputed-lengths",
        "length",
        "text_length",
        "length(text)",
    );
    let newer = engine.backfill(&replacement).await.unwrap();
    assert_ne!(
        newer.dataset.computed["length"].origins.as_ref(),
        Some(&origins_ref)
    );
    let replaced_records = engine.inspect(&request.dataset).await.unwrap();
    for key in &keys {
        assert_ne!(replaced_records[key], records[key]);
    }
    let seal = docspec::model::GenerationSeal {
        family: family.clone(),
        generation,
        members: std::collections::BTreeMap::from([(
            request.dataset.clone(),
            enriched.version_ref(),
        )]),
    };
    assert_eq!(engine.seal_generation(&seal).await.unwrap(), seal);
    let owner_records = engine.inspect(&request.dataset).await.unwrap();
    let (grant_key, grant) = owner_records
        .iter()
        .find(|(key, value)| {
            key.starts_with("owner-protection/") && value["consumer"] == family.as_str()
        })
        .unwrap();
    let granted: Vec<ObjectRef> = serde_json::from_value(grant["objects"].clone()).unwrap();
    for original in originals.values() {
        assert!(
            granted.contains(original),
            "foreign grant omitted {}",
            original.path
        );
        storage::verify_object(original).unwrap();
    }
    let family_records = engine.inspect(&family).await.unwrap();
    let binding = family_records
        .iter()
        .find(|(key, value)| {
            key.starts_with("reference-binding/") && value["key"] == "generation/g1"
        })
        .unwrap()
        .1;
    assert!(
        binding["protections"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reference| {
                reference["owner"] == request.dataset.as_str()
                    && reference["token"] == grant_key.as_str()
            })
    );
    assert_eq!(
        engine
            .plan_grant_retirement(
                &request.dataset,
                &id("old-origins-in-use"),
                std::slice::from_ref(grant_key),
                "probe the foreign family promise",
            )
            .await
            .unwrap_err()
            .code(),
        "grant_in_use"
    );
    let objects = originals.into_values().collect::<Vec<_>>();
    assert!(
        engine
            .inspect_retention(&objects)
            .await
            .unwrap()
            .objects
            .iter()
            .all(|object| object.blocker.is_some())
    );
    assert_eq!(engine.seal_generation(&seal).await.unwrap(), seal);
    assert_eq!(
        engine.inspect(&request.dataset).await.unwrap(),
        owner_records
    );
}

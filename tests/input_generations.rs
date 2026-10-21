//! Producer rules prevent mixed generations even when older inputs were independent.

use super::scalar;

use docspec::{
    Engine,
    budget::Limits,
    engine::{verify_input_coverage, verify_release},
    model::{
        GenerationSeal, Id, InputCoverage, ReleasePin, RunSpec, SourceRelationship, UpdateSpec,
        VersionRef,
    },
};
use scalar::{fixture, id, input, limits, spec};
use std::{collections::BTreeMap, path::Path};
use tempfile::TempDir;

fn coupled(family: &str, generation: &str) -> SourceRelationship {
    SourceRelationship::Coupled {
        family: id(family),
        generation: id(generation),
    }
}

fn versions(releases: &[&ReleasePin]) -> BTreeMap<Id, VersionRef> {
    releases
        .iter()
        .map(|release| (release.dataset.dataset.clone(), release.version_ref()))
        .collect()
}

fn seal(family: &str, generation: &str, releases: &[&ReleasePin]) -> GenerationSeal {
    GenerationSeal {
        family: id(family),
        generation: id(generation),
        members: versions(releases),
    }
}

async fn source(engine: &Engine, root: &Path, dataset: &str, operation: &str) -> ReleasePin {
    let source = input(
        &root.join(format!("{dataset}-{operation}.parquet")),
        &[(Some(1), Some("value".into())), (None, None)],
    );
    engine
        .run(&RunSpec {
            dataset: id(dataset),
            operation: id(operation),
            base: None,
            relationship: None,
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap()
}

fn rebuild(
    base: &ReleasePin,
    operation: &str,
    relationship: Option<SourceRelationship>,
) -> RunSpec {
    RunSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(operation),
        base: Some(base.id.clone()),
        relationship,
        input: base.dataset.inputs["input"].path.clone(),
        expected_digest: base.dataset.inputs["input"].digest.clone(),
        sql: "SELECT * FROM input".into(),
        definition: base.dataset.definition.clone(),
    }
}

#[tokio::test]
async fn independent_and_coupled_inputs_follow_the_entire_consumed_member_map() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let c0 = source(&engine, root.path(), "comments", "c0").await;
    let d0 = source(&engine, root.path(), "dockets", "d0").await;
    let independent = engine.capture_inputs(&versions(&[&c0, &d0])).await.unwrap();
    assert!(independent.generations.is_empty());
    let c1 = engine
        .run(&rebuild(&c0, "c1", Some(coupled("regulations", "g1"))))
        .await
        .unwrap();
    let d1 = engine
        .run(&rebuild(&d0, "d1", Some(coupled("regulations", "g1"))))
        .await
        .unwrap();
    let g1 = seal("regulations", "g1", &[&c1, &d1]);
    assert_eq!(
        engine.capture_inputs(&g1.members).await.unwrap_err().code(),
        "unsealed_generation"
    );
    assert_eq!(engine.seal_generation(&g1).await.unwrap(), g1);
    let c2 = engine
        .run(&rebuild(&c1, "c2", Some(coupled("regulations", "g2"))))
        .await
        .unwrap();
    let d2 = engine
        .run(&rebuild(&d1, "d2", Some(coupled("regulations", "g2"))))
        .await
        .unwrap();
    engine
        .seal_generation(&seal("regulations", "g2", &[&c2, &d2]))
        .await
        .unwrap();
    let c3 = engine
        .run(&rebuild(&c2, "c3", Some(coupled("regulations", "g3"))))
        .await
        .unwrap();
    assert_eq!(
        engine
            .seal_generation(&seal("regulations", "g3", &[&c3]))
            .await
            .unwrap_err()
            .code(),
        "family_membership"
    );
    // Historical complete generations remain admissible; neither current head nor
    // value equality decides which exact producer versions belong together.
    for releases in [[&c0, &d0], [&c1, &d1], [&c2, &d2]] {
        let coverage = engine.capture_inputs(&versions(&releases)).await.unwrap();
        verify_input_coverage(&coverage, limits()).await.unwrap();
    }
    for releases in [[&c2, &d1], [&c1, &d2], [&c2, &d0], [&c0, &d2]] {
        assert_eq!(
            engine
                .capture_inputs(&versions(&releases))
                .await
                .unwrap_err()
                .code(),
            "invalid_relationship"
        );
    }
    // A consumer can read a subset, but cannot consume a different version of a
    // named member. An unrelated independent source imposes no extra requirement.
    engine.capture_inputs(&versions(&[&c2])).await.unwrap();
    let other = source(&engine, root.path(), "other", "initial").await;
    engine
        .capture_inputs(&versions(&[&c2, &other]))
        .await
        .unwrap();
    let foreign = engine
        .run(&rebuild(
            &d2,
            "foreign",
            Some(coupled("other-family", "g1")),
        ))
        .await
        .unwrap();
    engine
        .seal_generation(&seal("other-family", "g1", &[&foreign]))
        .await
        .unwrap();
    assert_eq!(
        engine
            .capture_inputs(&versions(&[&c2, &foreign]))
            .await
            .unwrap_err()
            .code(),
        "invalid_relationship"
    );
}

#[tokio::test]
async fn omitted_relationship_inherits_and_explicit_transitions_require_the_selected_base() {
    let root = TempDir::new().unwrap();
    let (engine, initial, _) = fixture(root.path(), true).await;
    let mut transition = rebuild(&initial, "couple", Some(coupled("family", "g1")));
    transition.base = None;
    assert_eq!(
        engine.run(&transition).await.unwrap_err().code(),
        "relationship_base"
    );
    assert!(
        !engine
            .inspect(&id("comments"))
            .await
            .unwrap()
            .contains_key("operation/couple")
    );
    transition.base = Some(initial.id.clone());
    let coupled = engine.run(&transition).await.unwrap();
    let mut omitted = rebuild(&coupled, "inherit", None);
    omitted.base = None;
    assert_eq!(
        engine.plan(&omitted).await.unwrap_err().code(),
        "relationship_base"
    );
    assert_eq!(
        engine.run(&omitted).await.unwrap_err().code(),
        "relationship_base"
    );
    omitted.base = Some(coupled.id.clone());
    // Omission is distinct from an explicit Independent transition, including JSON callers.
    let mut json = serde_json::to_value(&omitted).unwrap();
    json.as_object_mut().unwrap().remove("relationship");
    let inherited = engine
        .run(&serde_json::from_value(json).unwrap())
        .await
        .unwrap();
    assert_eq!(inherited.dataset.relationship, coupled.dataset.relationship);
    assert_eq!(
        inherited.dataset.groups["source"],
        coupled.dataset.groups["source"]
    );
    let changed_request = RunSpec {
        relationship: Some(SourceRelationship::Independent),
        ..omitted
    };
    assert_eq!(
        engine.run(&changed_request).await.unwrap_err().code(),
        "operation_conflict"
    );
    let mut uncouple = rebuild(
        &inherited,
        "uncouple",
        Some(SourceRelationship::Independent),
    );
    uncouple.base = None;
    assert_eq!(
        engine.run(&uncouple).await.unwrap_err().code(),
        "relationship_base"
    );
    uncouple.base = Some(inherited.id.clone());
    let independent = engine.run(&uncouple).await.unwrap();
    assert_eq!(
        independent.dataset.relationship,
        SourceRelationship::Independent
    );
    verify_release(&independent, limits()).await.unwrap();
}

#[tokio::test]
async fn updates_and_backfills_inherit_but_resealing_requires_exact_new_members() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let first = source(&engine, root.path(), "comments", "initial").await;
    let c1 = engine
        .run(&rebuild(&first, "c1", Some(coupled("family", "g1"))))
        .await
        .unwrap();
    let g1 = seal("family", "g1", &[&c1]);
    engine.seal_generation(&g1).await.unwrap();
    let backfilled = engine
        .backfill(&spec(
            &c1,
            "length",
            "length",
            "length",
            "CAST(length(text) AS BIGINT)",
        ))
        .await
        .unwrap();
    assert_eq!(backfilled.dataset.relationship, c1.dataset.relationship);
    verify_release(&backfilled, limits()).await.unwrap();
    assert_eq!(
        engine
            .capture_inputs(&versions(&[&backfilled]))
            .await
            .unwrap_err()
            .code(),
        "invalid_relationship"
    );
    assert_eq!(
        engine
            .seal_generation(&seal("family", "g1", &[&backfilled]))
            .await
            .unwrap_err()
            .code(),
        "generation_conflict"
    );
    let next = rebuild(&backfilled, "c2", Some(coupled("family", "g2")));
    let c2 = engine.run(&next).await.unwrap();
    assert_eq!(
        c2.dataset.groups["source"],
        backfilled.dataset.groups["source"]
    );
    let files = |release: &ReleasePin| {
        release.dataset.groups["length"]
            .objects
            .iter()
            .filter(|object| object.path.ends_with(".parquet"))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(files(&c2), files(&backfilled));
    engine
        .seal_generation(&seal("family", "g2", &[&c2]))
        .await
        .unwrap();
    engine.capture_inputs(&versions(&[&c2])).await.unwrap();
    let replacements = input(
        &root.path().join("replace.parquet"),
        &[(Some(1), Some("correction".into()))],
    );
    let updated = engine
        .update(&UpdateSpec {
            dataset: c2.dataset.dataset.clone(),
            operation: id("update"),
            base: c2.id.clone(),
            definition: c2.dataset.definition.clone(),
            keys: vec!["id".into()],
            replacements,
            deletions: None,
            relationship: None,
        })
        .await
        .unwrap();
    assert_eq!(updated.dataset.relationship, c2.dataset.relationship);
    verify_release(&updated, limits()).await.unwrap();
    assert_eq!(
        engine
            .capture_inputs(&versions(&[&updated]))
            .await
            .unwrap_err()
            .code(),
        "invalid_relationship"
    );
}

#[tokio::test]
async fn stable_family_membership_and_exact_retry_survive_restart_and_invalidation() {
    let root = TempDir::new().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let first = source(&engine, root.path(), "comments", "initial").await;
    // Family equals a member: acquiring it twice would deadlock.
    let c1 = engine
        .run(&rebuild(&first, "c1", Some(coupled("comments", "g1"))))
        .await
        .unwrap();
    let first = source(&engine, root.path(), "dockets", "initial").await;
    let d1 = engine
        .run(&rebuild(&first, "d1", Some(coupled("comments", "g1"))))
        .await
        .unwrap();
    let full = seal("comments", "g1", &[&c1, &d1]);
    let partial = seal("comments", "g1", &[&c1]);
    let (left, right) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        tokio::join!(
            engine.seal_generation(&full),
            engine.seal_generation(&partial)
        )
    })
    .await
    .unwrap();
    assert!(left.is_ok() ^ right.is_ok());
    let (winner, loser) = if left.is_ok() {
        (full, partial)
    } else {
        (partial, full)
    };
    assert_eq!(
        engine.seal_generation(&loser).await.unwrap_err().code(),
        "generation_conflict"
    );
    let changed = GenerationSeal {
        generation: id("g2"),
        ..loser
    };
    assert_eq!(
        engine.seal_generation(&changed).await.unwrap_err().code(),
        "family_membership"
    );
    let coverage = engine.capture_inputs(&winner.members).await.unwrap();
    engine
        .invalidate(&id("comments"), "correct evidence", None)
        .await
        .unwrap();
    assert_eq!(
        engine
            .capture_inputs(&winner.members)
            .await
            .unwrap_err()
            .code(),
        "stale_eligibility"
    );
    verify_input_coverage(&coverage, limits()).await.unwrap();
    drop(engine);
    // Reconciliation does not require re-opening even bounded source manifests.
    for version in winner.members.values() {
        std::fs::remove_file(&version.manifest.path).unwrap();
    }
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(engine.seal_generation(&winner).await.unwrap(), winner);
}

#[tokio::test]
async fn offline_relationship_checks_need_only_captured_manifests_and_detect_tampering() {
    let root = TempDir::new().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let initial = source(&engine, root.path(), "comments", "initial").await;
    let release = engine
        .run(&rebuild(&initial, "coupled", Some(coupled("family", "g1"))))
        .await
        .unwrap();
    engine
        .seal_generation(&seal("family", "g1", &[&release]))
        .await
        .unwrap();
    let coverage = engine.capture_inputs(&versions(&[&release])).await.unwrap();
    drop(engine);
    std::fs::rename(
        workspace.join("catalog.sqlite"),
        workspace.join("hidden.sqlite"),
    )
    .unwrap();
    for object in release
        .dataset
        .inputs
        .values()
        .chain(release.dataset.groups["source"].objects.iter())
    {
        if object.path.ends_with(".parquet") && Path::new(&object.path).exists() {
            std::fs::remove_file(&object.path).unwrap();
        }
    }
    verify_input_coverage(&coverage, limits()).await.unwrap();
    assert!(!workspace.join("catalog.sqlite").exists());
    for mutation in [
        "missing-seal",
        "missing-family",
        "wrong-generation",
        "wrong-member",
        "extra-family",
    ] {
        let mut bad = coverage.clone();
        match mutation {
            "missing-seal" => {
                bad.generations.clear();
            }
            "missing-family" => {
                bad.families.clear();
            }
            "wrong-generation" => {
                bad.generations.get_mut(&id("family")).unwrap().generation = id("wrong")
            }
            "wrong-member" => {
                bad.generations
                    .get_mut(&id("family"))
                    .unwrap()
                    .members
                    .insert(id("comments"), initial.version_ref());
            }
            "extra-family" => {
                bad.families.insert(id("other"), [id("comments")].into());
            }
            _ => unreachable!(),
        }
        assert_eq!(
            verify_input_coverage(&bad, limits())
                .await
                .unwrap_err()
                .code(),
            "invalid_relationship",
            "{mutation}"
        );
    }
    let mut bounded = limits();
    let metadata_bytes = serde_json::to_vec(&coverage).unwrap().len();
    bounded.max_metadata_bytes = metadata_bytes + release.manifest.bytes as usize - 1;
    assert_eq!(
        verify_input_coverage(&coverage, bounded)
            .await
            .unwrap_err()
            .code(),
        "metadata_capacity"
    );
    // A small claimed size cannot authorize scanning a larger actual file.
    let mut bad = coverage.clone();
    bad.inputs.get_mut(&id("comments")).unwrap().manifest.bytes = 1;
    assert_eq!(
        verify_input_coverage(&bad, limits())
            .await
            .unwrap_err()
            .code(),
        "corrupt_object"
    );
    let path = &release.manifest.path;
    let mut bytes = std::fs::read(path).unwrap();
    bytes[0] ^= 1;
    std::fs::write(path, bytes).unwrap();
    assert_eq!(
        verify_input_coverage(&coverage, limits())
            .await
            .unwrap_err()
            .code(),
        "corrupt_object"
    );
}

#[tokio::test]
async fn invalid_seals_and_rebound_input_keys_refuse() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let release = source(&engine, root.path(), "comments", "initial").await;
    assert_eq!(
        engine
            .seal_generation(&seal("family", "g1", &[&release]))
            .await
            .unwrap_err()
            .code(),
        "invalid_relationship"
    );
    assert_eq!(
        engine
            .seal_generation(&seal("family", "g1", &[]))
            .await
            .unwrap_err()
            .code(),
        "invalid_relationship"
    );
    let mut inputs = BTreeMap::from([(id("wrong"), release.version_ref())]);
    assert_eq!(
        engine.capture_inputs(&inputs).await.unwrap_err().code(),
        "invalid_relationship"
    );
    inputs = versions(&[&release]);
    inputs.get_mut(&id("comments")).unwrap().manifest.owner = id("wrong");
    assert_eq!(
        engine.capture_inputs(&inputs).await.unwrap_err().code(),
        "invalid_relationship"
    );
    let empty = InputCoverage {
        inputs: BTreeMap::new(),
        families: BTreeMap::new(),
        generations: BTreeMap::new(),
    };
    assert_eq!(
        verify_input_coverage(&empty, Limits::default())
            .await
            .unwrap_err()
            .code(),
        "invalid_relationship"
    );
}

#[tokio::test]
async fn independent_release_verification_rejects_rewritten_producer_relationships() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let initial = source(&engine, root.path(), "comments", "initial").await;
    let explicit = engine
        .run(&rebuild(
            &initial,
            "explicit",
            Some(coupled("family", "g1")),
        ))
        .await
        .unwrap();
    let inherited = engine
        .run(&rebuild(&explicit, "inherited", None))
        .await
        .unwrap();
    let backfilled = engine
        .backfill(&spec(
            &inherited,
            "backfill",
            "length",
            "length",
            "CAST(length(text) AS BIGINT)",
        ))
        .await
        .unwrap();
    for (index, release) in [&initial, &explicit, &inherited, &backfilled]
        .into_iter()
        .enumerate()
    {
        let mut forged = release.clone();
        forged.dataset.relationship = coupled("forged", "generation");
        let path = root.path().join(format!("forged-{index}.json"));
        std::fs::write(&path, serde_json::to_vec(&forged.dataset).unwrap()).unwrap();
        let (digest, bytes) = docspec::storage::digest(&path).unwrap();
        forged.manifest = docspec::model::ObjectRef {
            owner: forged.dataset.dataset.clone(),
            path: path.to_string_lossy().into_owned(),
            digest,
            bytes,
        };
        let error = verify_release(&forged, limits()).await.unwrap_err();
        assert!(
            matches!(error.code(), "invalid_relationship" | "invalid_group"),
            "{index}: {error}"
        );
    }
}

#[tokio::test]
async fn cli_and_http_use_the_same_generation_guards() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let root = TempDir::new().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let first = source(&engine, root.path(), "comments", "initial").await;
    let release = engine
        .run(&rebuild(&first, "coupled", Some(coupled("family", "g1"))))
        .await
        .unwrap();
    let seal = seal("family", "g1", &[&release]);
    let app = docspec::http::router(engine.clone());
    let post = |path: &str, value: serde_json::Value| {
        Request::post(path)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&value).unwrap()))
            .unwrap()
    };
    let response = app
        .clone()
        .oneshot(post(
            "/generations/seal",
            serde_json::to_value(&seal).unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .oneshot(post(
            "/inputs/capture",
            serde_json::to_value(&seal.members).unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let coverage: InputCoverage =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(coverage.inputs, seal.members);
    drop(engine);
    let limits_path = root.path().join("limits.json");
    let seal_path = root.path().join("seal.json");
    let inputs_path = root.path().join("inputs.json");
    let coverage_path = root.path().join("coverage.json");
    for (path, value) in [
        (&limits_path, serde_json::to_value(limits()).unwrap()),
        (&seal_path, serde_json::to_value(&seal).unwrap()),
        (&inputs_path, serde_json::to_value(&seal.members).unwrap()),
        (&coverage_path, serde_json::to_value(&coverage).unwrap()),
    ] {
        std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
    }
    for (command, path) in [
        ("seal-generation", &seal_path),
        ("capture-inputs", &inputs_path),
    ] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
            .arg("--workspace")
            .arg(&workspace)
            .arg("--limits")
            .arg(&limits_path)
            .arg(command)
            .arg(path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{command}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    std::fs::rename(
        workspace.join("catalog.sqlite"),
        workspace.join("hidden.sqlite"),
    )
    .unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(&limits_path)
        .arg("verify-inputs")
        .arg(&coverage_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!workspace.join("catalog.sqlite").exists());
}

#[tokio::test]
async fn generation_retirement_matches_cli_http_and_keeps_the_exact_seal() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use docspec::model::{RetentionPlan, Retirement, RetirementScope};
    use tower::ServiceExt;

    let root = TempDir::new().unwrap();
    let workspace = root.path().join("workspace");
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let initial = source(&engine, root.path(), "comments", "initial").await;
    let release = engine
        .run(&rebuild(&initial, "coupled", Some(coupled("family", "g1"))))
        .await
        .unwrap();
    let sealed = seal("family", "g1", &[&release]);
    engine.seal_generation(&sealed).await.unwrap();
    let before = engine.inspect(&sealed.family).await.unwrap();
    let expected = engine
        .plan_generation_retirement(
            &sealed.family,
            &id("end-g1"),
            &sealed.generation,
            "end standalone availability",
        )
        .await
        .unwrap();
    let app = docspec::http::router(engine.clone());
    let post = |path: &str, value: serde_json::Value| {
        Request::post(path)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&value).unwrap()))
            .unwrap()
    };
    let response = app.clone().oneshot(post("/retention/generations/plan", serde_json::json!({
        "family": "family", "retirement": "end-g1", "generation": "g1", "reason": "end standalone availability"
    }))).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let http_plan: RetentionPlan =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(http_plan, expected);
    assert_eq!(
        http_plan.scope,
        RetirementScope::GenerationSeal {
            generation: id("g1")
        }
    );
    assert_eq!(engine.inspect(&sealed.family).await.unwrap(), before);
    let limits_path = root.path().join("limits.json");
    let plan_path = root.path().join("retirement.json");
    std::fs::write(&limits_path, serde_json::to_vec(&limits()).unwrap()).unwrap();
    std::fs::write(&plan_path, serde_json::to_vec(&http_plan).unwrap()).unwrap();
    drop(app);
    drop(engine);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(&limits_path)
        .args([
            "plan-generation-retirement",
            "family",
            "end-g1",
            "g1",
            "end standalone availability",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<RetentionPlan>(&output.stdout).unwrap(),
        http_plan
    );
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let app = docspec::http::router(engine.clone());
    let response = app
        .clone()
        .oneshot(post(
            "/retention/generations/retire",
            serde_json::to_value(&http_plan).unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let retired: Retirement =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(
        engine.inspect(&sealed.family).await.unwrap()["generation/g1"],
        before["generation/g1"]
    );
    let response = app
        .clone()
        .oneshot(post(
            "/inputs/capture",
            serde_json::to_value(&sealed.members).unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let error: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(error["code"], "generation_retired");
    drop(app);
    drop(engine);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--limits")
        .arg(&limits_path)
        .arg("retire-generation")
        .arg(&plan_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Retirement>(&output.stdout).unwrap(),
        retired
    );
}

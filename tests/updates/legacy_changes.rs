//! Older completed writes remain reusable without gaining local certificate trust.

#![cfg(feature = "test-util")]

use super::*;
use docspec::catalog::{Lifecycle, record};

#[tokio::test]
async fn recovered_processing_without_production_marker_does_not_mint_a_certificate() {
    let root = TempDir::new().unwrap();
    let (engine, _, before, rows) = fixture(root.path()).await;
    let replacements = vec![
        (Some(1), Some("replacement".into())),
        (None, Some("null key replacement".into())),
        (Some(1000), None),
    ];
    let deletions = vec![(Some(2), None)];
    let spec = update(root.path(), "initial", "legacy", &replacements, &deletions);
    assert_eq!(
        engine
            .update_until(&spec, "processing")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let retained = engine.inspect(&spec.dataset).await.unwrap();
    let (key, value) = retained
        .iter()
        .find(|(key, value)| {
            key.starts_with("phase/")
                && key.ends_with("/processing")
                && serde_json::from_value::<PhaseResult>((*value).clone())
                    .is_ok_and(|phase| phase.operation == spec.operation)
        })
        .unwrap();
    let original: PhaseResult = serde_json::from_value(value.clone()).unwrap();
    assert!(!original.changes.as_ref().unwrap().rules.is_empty());
    let mut legacy = value.clone();
    assert!(
        legacy["changes"]
            .as_object_mut()
            .unwrap()
            .remove("rules")
            .is_some()
    );
    let decoded: PhaseResult = serde_json::from_value(legacy.clone()).unwrap();
    assert!(decoded.changes.as_ref().unwrap().rules.is_empty());
    let mut expected = original.clone();
    expected.changes.as_mut().unwrap().rules.clear();
    assert_eq!(decoded, expected);
    assert!(!retained.contains_key(&format!("prepared-changes/{}", spec.operation)));
    assert!(!retained.contains_key(&format!("change-certificate/{}", spec.operation)));

    // Preserve the existing owner grants while atomically rebinding the changed
    // completion's digest and revision. Native output, admitted inputs,
    // dependencies and original bytes stay intact.
    let lifecycle = Lifecycle::new(
        storage::local_catalog(&root.path().join("workspace"))
            .await
            .unwrap(),
        Budget::new(limits()).unwrap(),
    );
    let view = lifecycle.open(&spec.dataset).await.unwrap();
    let (binding_key, binding) = retained
        .iter()
        .find(|(binding_key, binding)| {
            binding_key.starts_with("reference-binding/")
                && binding["key"].as_str() == Some(key.as_str())
        })
        .unwrap();
    assert_eq!(
        binding["digest"].as_str().unwrap(),
        blake3::hash(&serde_json::to_vec(value).unwrap())
            .to_hex()
            .as_str()
    );
    let mut legacy_binding = binding.clone();
    legacy_binding["revision"] = view.revision.checked_add(1).unwrap().into();
    legacy_binding["digest"] = blake3::hash(&serde_json::to_vec(&legacy).unwrap())
        .to_hex()
        .to_string()
        .into();
    assert_eq!(legacy_binding["protections"], binding["protections"]);
    assert_eq!(legacy_binding["consumer"], binding["consumer"]);
    assert_eq!(legacy_binding["key"], binding["key"]);
    lifecycle
        .commit(
            &view,
            vec![
                record(key, &legacy).unwrap(),
                record(binding_key, &legacy_binding).unwrap(),
            ],
        )
        .await
        .unwrap();
    drop(lifecycle);
    drop(engine);
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let plan = engine.plan_update(&spec).await.unwrap();
    assert!(
        plan.reusable_phases
            .iter()
            .any(|phase| phase == "processing")
    );
    assert!(
        !plan
            .required_phases
            .iter()
            .any(|phase| phase == "processing")
    );
    let release = engine.update(&spec).await.unwrap();
    assert_eq!(release.dataset.groups["source"], original.table.unwrap());
    assert_eq!(
        read(&release.dataset.groups["source"]).await,
        reconstruction(&rows, &replacements, &deletions)
    );
    verify_release(&release, limits()).await.unwrap();
    engine
        .prove_succession(&before.version_ref(), &release.version_ref())
        .await
        .unwrap();
    assert!(
        engine
            .change_certificate(&release.version_ref())
            .await
            .unwrap()
            .is_none()
    );
    let resumed = engine.inspect(&spec.dataset).await.unwrap();
    assert_eq!(&resumed[key], &legacy);
    assert_eq!(&resumed[binding_key], &legacy_binding);
    assert!(!resumed.contains_key(&format!("prepared-changes/{}", spec.operation)));
    assert!(!resumed.contains_key(&format!("change-certificate/{}", spec.operation)));
    assert_eq!(engine.update(&spec).await.unwrap(), release);
    assert_eq!(engine.inspect(&spec.dataset).await.unwrap(), resumed);
}

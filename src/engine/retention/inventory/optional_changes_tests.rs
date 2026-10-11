//! Change descriptors retain applied rows without rooting complete endpoint states.

use super::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn object(root: &Path, name: &str) -> ObjectRef {
    ObjectRef {
        owner: id("comments"),
        path: root.join(name).to_string_lossy().into_owned(),
        digest: "0".repeat(64),
        bytes: 17,
    }
}

fn table(root: &Path, name: &str) -> TablePin {
    let metadata = object(root, &format!("{name}.metadata.json"));
    TablePin {
        logical_metadata: None,
        table: name.into(),
        metadata: metadata.clone(),
        snapshot: Some(1),
        schema_id: 0,
        objects: vec![metadata, object(root, &format!("{name}.parquet"))],
        rows: 1,
    }
}

fn origin() -> RetentionBlocker {
    RetentionBlocker {
        table: crate::catalog::lifecycle_ident(&id("comments")).to_string(),
        key: "change-certificate/update".into(),
        revision: 2,
    }
}

fn fixture(root: &Path) -> (PreparedChanges, AppliedChangeSet) {
    let before = VersionRef {
        dataset: id("comments"),
        version: id("before"),
        manifest: object(root, "before.release.json"),
    };
    let changes = ChangePin {
        rules: crate::engine::changes::WRITE_RULES.into(),
        base_version: before.version.clone(),
        base_manifest: before.manifest.clone(),
        before_metadata: object(root, "before.metadata.json"),
        after_metadata: object(root, "after.metadata.json"),
        key_field_ids: vec![1],
        removed: table(root, "removed"),
        added: table(root, "added"),
    };
    let applied = AppliedChangeSet {
        format: 1,
        rules: crate::engine::changes::WRITE_RULES.into(),
        operation: id("update"),
        before: before.clone(),
        source: Some(changes),
        receipts: None,
    };
    let bytes = serde_json::to_vec(&applied).unwrap();
    let mut descriptor = object(root, "changes.json");
    std::fs::write(&descriptor.path, &bytes).unwrap();
    descriptor.digest = blake3::hash(&bytes).to_hex().to_string();
    descriptor.bytes = bytes.len() as u64;
    (
        PreparedChanges {
            format: 1,
            rules: applied.rules.clone(),
            operation: applied.operation.clone(),
            before,
            descriptor,
        },
        applied,
    )
}

#[tokio::test]
async fn descriptor_expands_once_without_opening_or_retaining_whole_endpoints() {
    let root = tempfile::tempdir().unwrap();
    let (prepared, applied) = fixture(root.path());
    let budget = Budget::new(Limits {
        compute_threads: 1,
        memory_bytes: 64 << 20,
        max_metadata_bytes: 1 << 20,
        ..Limits::default()
    })
    .unwrap();
    let mut inventory = Inventory::collecting(budget.limits.max_metadata_bytes).unwrap();
    inventory
        .prepared_changes(prepared.clone(), &origin())
        .unwrap();
    inventory
        .prepared_changes(prepared.clone(), &origin())
        .unwrap();
    inventory
        .expand(&storage::Objects::Local, &budget, root.path())
        .await
        .unwrap();
    assert_eq!(inventory.metadata_objects_read, 1);
    let references = inventory.into_references().unwrap();
    let changes = applied.source.unwrap();
    let expected = std::iter::once(prepared.descriptor)
        .chain([changes.before_metadata, changes.after_metadata])
        .chain(changes.removed.objects)
        .chain(changes.added.objects)
        .map(|object| (object.path.clone(), object))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(references, expected.into_values().collect::<Vec<_>>());
    assert!(!references.contains(&prepared.before.manifest));
    assert!(!Path::new(&prepared.before.manifest.path).exists());
}

#[test]
fn duplicate_descriptor_paths_cannot_borrow_another_operation_binding() {
    let root = tempfile::tempdir().unwrap();
    let (prepared, _) = fixture(root.path());
    let mut inventory = Inventory::collecting(1 << 20).unwrap();
    inventory
        .prepared_changes(prepared.clone(), &origin())
        .unwrap();
    let mut conflicting = prepared;
    conflicting.operation = id("other");
    assert_eq!(
        inventory
            .prepared_changes(conflicting, &origin())
            .unwrap_err()
            .code(),
        "retention_inventory"
    );
}

#[tokio::test]
async fn present_descriptor_must_match_its_exact_prepared_binding() {
    let root = tempfile::tempdir().unwrap();
    let (mut prepared, _) = fixture(root.path());
    prepared.operation = id("other");
    let budget = Budget::new(Limits {
        compute_threads: 1,
        memory_bytes: 64 << 20,
        max_metadata_bytes: 1 << 20,
        ..Limits::default()
    })
    .unwrap();
    let mut inventory = Inventory::collecting(budget.limits.max_metadata_bytes).unwrap();
    inventory.prepared_changes(prepared, &origin()).unwrap();
    assert!(
        inventory
            .expand(&storage::Objects::Local, &budget, root.path())
            .await
            .is_err()
    );
}

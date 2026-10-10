//! A native catalog backup must retain even currently unpromised registrations.
#![cfg(unix)]

use super::scalar;

use docspec::{Engine, storage};
use iceberg::{
    TableCreation, TableIdent,
    spec::{NestedField, PrimitiveType, Schema, Type},
};
use scalar::{fixture, id, limits};

#[tokio::test]
async fn restore_reconstructs_an_unpromised_native_registration() {
    let root = tempfile::tempdir().unwrap();
    let (engine, release, _) = fixture(root.path(), false).await;
    let workspace = std::fs::canonicalize(root.path().join("workspace")).unwrap();
    engine.shutdown().await.unwrap();
    drop(engine);
    let catalog = storage::local_catalog(&workspace).await.unwrap();
    let namespaces = catalog.list_namespaces(None).await.unwrap();
    let name = format!("data_{}", uuid::Uuid::new_v4().simple());
    let ident = TableIdent::new(namespaces[0].clone(), name.clone());
    // This registration has no selected result or phase. It represents the
    // native head remaining after logical promises have retired.
    let schema = Schema::builder()
        .with_fields([
            NestedField::optional(1, "value", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap();
    let table = catalog
        .create_table(
            &namespaces[0],
            TableCreation::builder().name(name).schema(schema).build(),
        )
        .await
        .unwrap();
    let metadata = table.metadata_location_result().unwrap().to_owned();
    drop(catalog);
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    let point = engine
        .create_restore_point(&id("comments"), &id("native-heads"))
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    drop(engine);
    // Model a later completed native detachment and metadata unlink. No source
    // payload is changed; this fixture only removes its unpromised native head.
    let catalog = storage::local_catalog(&workspace).await.unwrap();
    catalog.drop_table(&ident).await.unwrap();
    drop(catalog);
    std::fs::remove_file(&metadata).unwrap();
    Engine::restore_local(&workspace, &point, limits(), false)
        .await
        .unwrap();
    let catalog = storage::local_catalog(&workspace).await.unwrap();
    let restored = catalog.load_table(&ident).await.unwrap();
    assert_eq!(restored.metadata_location_result().unwrap(), metadata);
    drop(catalog);
    let engine = Engine::open(&workspace, limits()).await.unwrap();
    assert_eq!(engine.latest(&id("comments")).await.unwrap(), release);
    // A new complete inventory can still walk every restored registration.
    engine
        .create_restore_point(&id("comments"), &id("after-restore"))
        .await
        .unwrap();
}

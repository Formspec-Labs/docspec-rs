//! Shared enriched fixtures and an independent Rust value oracle.
use super::scalar::{Row, fixture, spec};
use arrow::{
    array::{Int64Array, StringArray},
    datatypes::DataType,
};
use docspec::{
    Engine,
    model::{ObjectRef, ReleasePin},
    storage,
};
use futures::TryStreamExt;
use std::path::Path;

pub(super) async fn enriched(root: &Path) -> (Engine, ReleasePin, Vec<Row>) {
    let (engine, source, rows) = fixture(root, false).await;
    let first = engine
        .backfill(&spec(
            &source,
            "lengths",
            "a",
            "text_length",
            "length(text)",
        ))
        .await
        .unwrap();
    let second = engine
        .backfill(&spec(&first, "ids", "b", "next_id", "id + 1"))
        .await
        .unwrap();
    (engine, second, rows)
}

pub(super) async fn check_values(engine: &Engine, release: &ReleasePin, rows: &[Row]) {
    let mut expected: Vec<_> = rows
        .iter()
        .map(|row| {
            (
                row.clone(),
                row.1.as_ref().map(|text| text.chars().count() as i64),
                row.0.map(|id| id + 1),
            )
        })
        .collect();
    expected.sort();
    let mut actual = vec![];
    let mut stream = engine.read_combined(release).await.unwrap();
    assert_eq!(stream.schema().fields().len(), 4);
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let lengths = arrow::compute::cast(batch.column(2), &DataType::Int64).unwrap();
        let lengths = lengths.as_any().downcast_ref::<Int64Array>().unwrap();
        let next = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        actual.extend(
            ids.iter()
                .zip(text.iter())
                .zip(lengths.iter())
                .zip(next.iter())
                .map(|(((id, text), length), next)| ((id, text.map(str::to_owned)), length, next)),
        );
    }
    actual.sort();
    assert_eq!(actual, expected);
}

pub(super) fn forged(root: &Path, name: &str, mut release: ReleasePin) -> ReleasePin {
    let path = root.join(format!("{name}.json"));
    std::fs::write(&path, serde_json::to_vec(&release.dataset).unwrap()).unwrap();
    let (digest, bytes) = storage::digest(&path).unwrap();
    release.manifest = ObjectRef {
        owner: release.dataset.dataset.clone(),
        path: path.to_string_lossy().into_owned(),
        digest,
        bytes,
    };
    release
}

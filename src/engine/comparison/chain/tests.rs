//! Discovery limits and catalog corruption are independent of payload reconstruction.

use super::*;
use parquet::arrow::ArrowWriter;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn input(root: &Path, value: i64) -> InputSpec {
    let path = root.join(format!("{value}.parquet"));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![value]))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(&path).unwrap().0,
    }
}

#[tokio::test]
async fn limits_and_wrong_revisions_do_not_become_empty_changes() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        root.path().join("workspace"),
        Limits {
            compute_threads: 2,
            batch_rows: 16,
            max_batch_bytes: 4 << 20,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    let input = input(root.path(), 1);
    let before = engine
        .run(&RunSpec {
            dataset: id("source"),
            operation: id("first"),
            base: None,
            relationship: None,
            input: input.path,
            expected_digest: input.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
        })
        .await
        .unwrap();
    let mut after = before.clone();
    for value in [2, 3] {
        after = engine
            .update(&UpdateSpec {
                dataset: id("source"),
                operation: id(&format!("update-{value}")),
                base: after.id.clone(),
                definition: "v1".into(),
                keys: vec!["id".into()],
                replacements: self::input(root.path(), value),
                deletions: None,
            })
            .await
            .unwrap();
    }
    let lifecycle = &engine.inner.lifecycle;
    let view = lifecycle.open(&id("source")).await.unwrap();
    let native = storage::open_pin(&before.dataset.groups["source"])
        .await
        .unwrap();
    let schema = storage::identity::public_schema(&native).unwrap();
    let result = engine
        .forward_changes(
            &view,
            (&before, &after),
            (
                &before.dataset.groups["source"],
                &after.dataset.groups["source"],
            ),
            &native,
            (schema.fields(), true),
            1,
        )
        .await
        .unwrap();
    assert!(matches!(
        result,
        Forward::Fallback(ComparisonFallback::HistoryLimit)
    ));
    let normal = engine.compare(&before, &after, "source").await.unwrap();
    assert_eq!(normal.descriptor.path, ComparisonPath::ForwardChanges);
    assert_eq!((normal.added_rows, normal.removed_rows), (Some(2), Some(0)));

    let key = format!("change-certificate/{}", after.id);
    let rows = lifecycle.rows(&view).await.unwrap();
    let (_, _, certificate) = rows.get_record::<ChangeCertificate>(&key).unwrap().unwrap();
    // A copied certificate in another native revision cannot borrow the result's trust.
    lifecycle
        .commit(&view, vec![record(&key, &certificate).unwrap()])
        .await
        .unwrap();
    let error = engine.compare(&before, &after, "source").await.unwrap_err();
    assert_eq!(error.code(), "invalid_changes");
    let forced = engine
        .compare_with_options(
            &before,
            &after,
            "source",
            &ComparisonOptions {
                identity: ComparisonIdentity::Values,
                force_full: true,
            },
        )
        .await
        .unwrap();
    assert_eq!((forced.added_rows, forced.removed_rows), (Some(2), Some(0)));
}

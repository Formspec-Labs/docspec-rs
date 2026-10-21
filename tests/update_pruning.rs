//! Key-scoped Update pruning must be exact on correctness and point-scoped on cost.
//!
//! `replace_keys` may skip decoding a retained file only when manifest bounds prove
//! that no affected key can occur in it. A skipped file that actually held a
//! replaced or deleted key would silently keep a stale row, so every pruning test
//! also compares the full result with an in-memory oracle.

use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::Limits,
    model::{Id, InputSpec, PhaseResult, ReleasePin, RunSpec, UpdateSpec},
    storage,
};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use std::{collections::BTreeMap, fs::File, path::Path, sync::Arc};
use tempfile::TempDir;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        batch_rows: 2,
        max_batch_bytes: 4 << 20,
        scratch_bytes: 16 << 20,
        ..Limits::default()
    }
}

fn input(path: &Path, rows: &[(Option<i64>, &str)]) -> InputSpec {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("text", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(path).unwrap().0,
    }
}

fn key_input(path: &Path, keys: &[Option<i64>]) -> InputSpec {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(keys.to_vec()))],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(path).unwrap().0,
    }
}

async fn rows(engine: &Engine, release: &ReleasePin) -> BTreeMap<Option<i64>, Vec<String>> {
    let batches: Vec<RecordBatch> = engine
        .read_group(release, "source")
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut out: BTreeMap<Option<i64>, Vec<String>> = BTreeMap::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let texts = batch
            .column_by_name("text")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let key = (!ids.is_null(row)).then(|| ids.value(row));
            out.entry(key)
                .or_default()
                .push(texts.value(row).to_owned());
        }
    }
    out
}

async fn processing(engine: &Engine, release: &ReleasePin) -> PhaseResult {
    engine
        .inspect(&release.dataset.dataset)
        .await
        .unwrap()
        .values()
        .filter_map(|value| serde_json::from_value::<PhaseResult>(value.clone()).ok())
        .find(|phase| phase.operation == release.id && phase.phase == "processing")
        .unwrap()
}

async fn update(
    engine: &Engine,
    root: &Path,
    base: &ReleasePin,
    operation: &str,
    replacements: &[(Option<i64>, &str)],
    deletions: Option<&[Option<i64>]>,
) -> ReleasePin {
    engine
        .update(&UpdateSpec {
            dataset: id("news"),
            operation: id(operation),
            definition: "news-v1".into(),
            base: base.id.clone(),
            keys: vec!["id".into()],
            replacements: input(&root.join(format!("{operation}.parquet")), replacements),
            deletions: deletions
                .map(|keys| key_input(&root.join(format!("{operation}-deletions.parquet")), keys)),
            relationship: None,
        })
        .await
        .unwrap()
}

/// Four retained files with disjoint key ranges; the update touches only the two
/// extreme files, so the affected-key min/max interval spans all four.
#[tokio::test]
async fn update_probes_only_files_whose_bounds_contain_an_affected_key() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let mut oracle: BTreeMap<Option<i64>, Vec<String>> = BTreeMap::new();
    let mut texts = Vec::new();
    for base in [0_i64, 100, 200, 300] {
        for offset in 0..4 {
            texts.push((base + offset, format!("v{}", base + offset)));
        }
    }
    let as_rows = |range: &[(i64, String)]| -> Vec<(Option<i64>, String)> {
        range.iter().map(|(k, v)| (Some(*k), v.clone())).collect()
    };
    let first_rows = as_rows(&texts[0..4]);
    let first_input: Vec<(Option<i64>, &str)> =
        first_rows.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let source = input(&root.path().join("initial.parquet"), &first_input);
    let mut release = engine
        .run(&RunSpec {
            dataset: id("news"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            definition: "news-v1".into(),
        })
        .await
        .unwrap();
    for (key, text) in &first_rows {
        oracle.entry(*key).or_default().push(text.clone());
    }
    for (index, chunk) in texts[4..].chunks(4).enumerate() {
        let chunk_rows = as_rows(chunk);
        let chunk_input: Vec<(Option<i64>, &str)> =
            chunk_rows.iter().map(|(k, v)| (*k, v.as_str())).collect();
        release = update(
            &engine,
            root.path(),
            &release,
            &format!("append-{index}"),
            &chunk_input,
            None,
        )
        .await;
        for (key, text) in &chunk_rows {
            oracle.entry(*key).or_default().push(text.clone());
        }
    }
    assert_eq!(rows(&engine, &release).await, oracle);

    // Correct key 0 (first file), delete key 303 (last file), insert a new key 500.
    let corrected = update(
        &engine,
        root.path(),
        &release,
        "extremes",
        &[(Some(0), "v0-corrected"), (Some(500), "v500")],
        Some(&[Some(303)]),
    )
    .await;
    oracle.insert(Some(0), vec!["v0-corrected".into()]);
    oracle.remove(&Some(303));
    oracle.insert(Some(500), vec!["v500".into()]);
    assert_eq!(rows(&engine, &corrected).await, oracle);

    let metrics = processing(&engine, &corrected).await.metrics;
    // The two interior files [100,103] and [200,203] contain no affected key and
    // are shared without a decode. Two files are probed and then rewritten.
    assert_eq!(metrics.files_reused, 2, "{metrics:?}");
    assert_eq!(metrics.files_scanned, 4, "{metrics:?}");
}

/// A NULL affected key matches NULL rows (`IS NOT DISTINCT FROM`); point pruning
/// must never skip a file that can hold a NULL key while the update carries one.
#[tokio::test]
async fn null_affected_keys_keep_files_that_may_hold_null_keys() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let source = input(
        &root.path().join("initial.parquet"),
        &[(None, "null-a"), (Some(1), "one"), (Some(2), "two")],
    );
    let first = engine
        .run(&RunSpec {
            dataset: id("news"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            definition: "news-v1".into(),
        })
        .await
        .unwrap();
    let second = update(
        &engine,
        root.path(),
        &first,
        "append",
        &[(Some(100), "hundred"), (Some(101), "hundred-one")],
        None,
    )
    .await;
    let third = update(
        &engine,
        root.path(),
        &second,
        "null-correction",
        &[(None, "null-b"), (Some(101), "hundred-one-b")],
        None,
    )
    .await;
    let actual = rows(&engine, &third).await;
    assert_eq!(actual[&None], vec!["null-b".to_owned()]);
    assert_eq!(actual[&Some(101)], vec!["hundred-one-b".to_owned()]);
    assert_eq!(actual[&Some(1)], vec!["one".to_owned()]);
    assert_eq!(actual[&Some(100)], vec!["hundred".to_owned()]);
    assert_eq!(actual.len(), 5);
}

#[tokio::test]
async fn sha256_string_keys_prune_disjoint_files_on_update() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();
    let write_str_input = |path: &Path, rows: &[(&str, &str)]| -> InputSpec {
        let schema = Arc::new(Schema::new(vec![
            Field::new("source_id", DataType::Utf8, false),
            Field::new("text", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        InputSpec {
            path: path.to_string_lossy().into_owned(),
            digest: storage::digest(path).unwrap().0,
        }
    };
    let make_sha = |prefix: char, idx: usize| -> String {
        format!("{}{:063x}", prefix, idx)
    };
    let file_a = [
        (make_sha('1', 0), "a0"),
        (make_sha('1', 1), "a1"),
    ];
    let file_b = [
        (make_sha('5', 0), "b0"),
        (make_sha('5', 1), "b1"),
    ];
    let file_c = [
        (make_sha('9', 0), "c0"),
        (make_sha('9', 1), "c1"),
    ];
    assert_eq!(file_a[0].0.len(), 64);

    let initial = write_str_input(
        &root.path().join("file_a.parquet"),
        &[(file_a[0].0.as_str(), file_a[0].1), (file_a[1].0.as_str(), file_a[1].1)],
    );
    let mut release = engine
        .run(&RunSpec {
            dataset: id("captures"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: initial.path,
            expected_digest: initial.digest,
            sql: "SELECT * FROM input".into(),
            definition: "captures-v1".into(),
        })
        .await
        .unwrap();
    for (name, chunk) in [("append-b", &file_b), ("append-c", &file_c)] {
        let repl = write_str_input(
            &root.path().join(format!("{name}.parquet")),
            &[(chunk[0].0.as_str(), chunk[0].1), (chunk[1].0.as_str(), chunk[1].1)],
        );
        release = engine
            .update(&UpdateSpec {
                dataset: id("captures"),
                operation: id(name),
                definition: "captures-v1".into(),
                base: release.id.clone(),
                keys: vec!["source_id".into()],
                replacements: repl,
                deletions: None,
                relationship: None,
            })
            .await
            .unwrap();
    }

    // Touch only a key in `file_c` ('9' prefix); `file_a` and `file_b` must be reused without scanning.
    let touch = write_str_input(
        &root.path().join("touch-c.parquet"),
        &[(file_c[0].0.as_str(), "c0-updated")],
    );
    let updated = engine
        .update(&UpdateSpec {
            dataset: id("captures"),
            operation: id("touch-c"),
            definition: "captures-v1".into(),
            base: release.id.clone(),
            keys: vec!["source_id".into()],
            replacements: touch,
            deletions: None,
            relationship: None,
        })
        .await
        .unwrap();
    let metrics = processing(&engine, &updated).await.metrics;
    assert_eq!(metrics.files_reused, 2, "{metrics:?}");
    // 1 retained file (`file_c`) + 1 replacement input file = 2 files scanned.
    assert_eq!(metrics.files_scanned, 2, "{metrics:?}");
}

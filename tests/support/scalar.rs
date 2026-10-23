//! Shared scalar fixtures; expectations use direct Rust values rather than SQL.
use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::Limits,
    model::{BackfillSpec, ComputedField, Id, InputSpec, ReleasePin, RunSpec},
    storage,
};
use parquet::arrow::ArrowWriter;
use std::{fs::File, path::Path, sync::Arc};

pub(super) type Row = (Option<i64>, Option<String>);

pub(super) fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

pub(super) fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        batch_rows: 16,
        scratch_bytes: 16 << 20,
        max_batch_bytes: 4 << 20,
        ..Limits::default()
    }
}

pub(super) fn write_parquet(path: &Path, batch: &RecordBatch, max_row_group_rows: Option<usize>) {
    let props = max_row_group_rows.map(|rows| {
        parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(rows))
            .build()
    });
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), props).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

pub(super) fn input(path: &Path, rows: &[Row]) -> InputSpec {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("text", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1.as_deref()).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    write_parquet(path, &batch, Some(16));
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(path).unwrap().0,
    }
}
pub(super) async fn fixture(root: &Path, empty: bool) -> (Engine, ReleasePin, Vec<Row>) {
    let rows = if empty {
        vec![]
    } else {
        (0_i64..512)
            .map(|i| {
                (
                    Some(i),
                    // Distinct hash blocks keep enough compressed bytes for a
                    // middle file untouched by the first/tail-key update fixture.
                    Some(format!(
                        "{}{}",
                        blake3::hash(&i.to_le_bytes()).to_hex(),
                        blake3::hash(&(i + 512).to_le_bytes()).to_hex()
                    )),
                )
            })
            .chain([
                (Some(1), Some("duplicate".into())),
                (None, None),
                (None, Some(String::new())),
                (Some(9), Some("é🎉".into())),
            ])
            .collect()
    };
    let source = input(&root.join("input.parquet"), &rows);
    let engine = Engine::open(
        root.join("workspace"),
        Limits {
            max_batch_bytes: 16 << 10,
            ..limits()
        },
    )
    .await
    .unwrap();
    let base = engine
        .run(&RunSpec {
            base: None,
            relationship: None,
            dataset: id("comments"),
            operation: id("initial"),
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    drop(engine);
    let engine = Engine::open(root.join("workspace"), limits())
        .await
        .unwrap();
    (engine, base, rows)
}
pub(super) fn spec(
    base: &ReleasePin,
    operation: &str,
    group: &str,
    name: &str,
    expression: &str,
) -> BackfillSpec {
    BackfillSpec {
        dataset: base.dataset.dataset.clone(),
        operation: id(operation),
        base: base.id.clone(),
        definition: "v1".into(),
        group: id(group),
        group_definition: "expression-v1".into(),
        fields: vec![ComputedField {
            id: 10,
            name: name.into(),
            expression: expression.into(),
        }],
    }
}

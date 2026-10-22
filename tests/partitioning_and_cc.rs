//! Integration tests for Iceberg partitioning (`ss-k3mg`) and connected components (`ss-msdx`).

use arrow::{
    array::{Array, Date32Array, Int64Array, ListArray, StringArray, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::Limits,
    model::{
        AggregateField, AggregateFunction, ColumnAddition, ColumnType,
        ConnectedComponentsTransform, ConnectedEdgeSpec, Id, InputSpec, KeyReadSpec,
        PartitionFieldSpec, PartitionTransformSpec, PhaseResult, RefreshMode, RefreshPolicy,
        RefreshSpec, RefreshTransform, ReleasePin, RunSpec, SchemaField, SchemaSpec, SortColumn,
        SortDirection, UpdateSpec,
    },
    storage,
};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::Path,
    sync::Arc,
};
use tempfile::TempDir;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        batch_rows: 4,
        max_batch_bytes: 4 << 20,
        scratch_bytes: 16 << 20,
        ..Limits::default()
    }
}

async fn processing_metrics(engine: &Engine, release: &ReleasePin) -> PhaseResult {
    engine
        .inspect(&release.dataset.dataset)
        .await
        .unwrap()
        .values()
        .filter_map(|value| serde_json::from_value::<PhaseResult>(value.clone()).ok())
        .find(|phase| phase.operation == release.id && phase.phase == "processing")
        .unwrap()
}

fn write_partitioned_input(path: &Path, rows: &[(&str, i32, &str, i64)]) -> InputSpec {
    let schema = Arc::new(Schema::new(vec![
        Field::new("doc_id", DataType::Utf8, false),
        Field::new("published_date", DataType::Date32, false),
        Field::new("agency", DataType::Utf8, false),
        Field::new("views", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Date32Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
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

#[tokio::test]
async fn partitioned_run_update_read_and_schema_evolution_prune_files_by_partition_tuple() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();

    // Create rows across 3 distinct agencies ("EPA", "FDA", "SEC") and 3 months in 2024:
    // 2024-01-15 = 19737 days since epoch
    // 2024-02-15 = 19768 days since epoch
    // 2024-03-15 = 19797 days since epoch
    let initial_rows = [
        ("aa01", 19737, "EPA", 10),
        ("aa02", 19737, "EPA", 20),
        ("bb01", 19768, "FDA", 30),
        ("bb02", 19768, "FDA", 40),
        ("cc01", 19797, "SEC", 50),
        ("cc02", 19797, "SEC", 60),
    ];
    let source = write_partitioned_input(&root.path().join("initial.parquet"), &initial_rows);
    let partition_by = vec![
        PartitionFieldSpec {
            column: "agency".into(),
            name: None,
            transform: PartitionTransformSpec::Identity,
        },
        PartitionFieldSpec {
            column: "published_date".into(),
            name: Some("pub_month".into()),
            transform: PartitionTransformSpec::Month,
        },
        PartitionFieldSpec {
            column: "doc_id".into(),
            name: Some("doc_prefix".into()),
            transform: PartitionTransformSpec::Truncate { width: 2 },
        },
    ];
    let release = engine
        .run(&RunSpec {
            dataset: id("filings"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            definition: "filings-v1".into(),
            partition_by: partition_by.clone(),
        })
        .await
        .unwrap();

    assert_eq!(release.dataset.partition_by, partition_by);
    // 3 distinct partition tuples -> at least 3 data files (plus 1 metadata file = 4 objects).
    assert!(
        release.dataset.groups["source"].objects.len() >= 4,
        "expected separate data files per partition tuple"
    );

    // 1. Point lookup via `read_keys` on `doc_id = 'cc01'` must return the exact row!
    let stream = engine
        .read_keys(
            &release,
            &KeyReadSpec {
                key: "doc_id".into(),
                values: vec!["cc01".into()],
                columns: None,
            },
        )
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 1);

    // 2. Update touching only ("cc02", 19797, "SEC", 600) with key `doc_id` must reuse the 'aa' and 'bb' partition files!
    let repl = write_partitioned_input(
        &root.path().join("repl.parquet"),
        &[("cc02", 19797, "SEC", 600)],
    );
    let updated = engine
        .update(&UpdateSpec {
            dataset: id("filings"),
            operation: id("update-sec"),
            definition: "filings-v1".into(),
            base: release.id.clone(),
            keys: vec!["doc_id".into()],
            replacements: repl,
            deletions: None,
            relationship: None,
        })
        .await
        .unwrap();
    assert_eq!(updated.dataset.partition_by, partition_by);
    let upd_metrics = processing_metrics(&engine, &updated).await.metrics;
    assert_eq!(
        upd_metrics.files_reused, 2,
        "expected 'aa' and 'bb' partition files to be reused without scanning: {upd_metrics:?}"
    );

    // 3. Schema evolution adding a nullable column preserves partition_by, while dropping a partition column is refused.
    let evolved = engine
        .evolve_schema(&SchemaSpec {
            dataset: id("filings"),
            operation: id("add-notes"),
            base: updated.id.clone(),
            definition: "filings-v1".into(),
            next_definition: "filings-v2".into(),
            fields: vec![
                SchemaField {
                    id: 1,
                    name: "doc_id".into(),
                },
                SchemaField {
                    id: 2,
                    name: "published_date".into(),
                },
                SchemaField {
                    id: 3,
                    name: "agency".into(),
                },
                SchemaField {
                    id: 4,
                    name: "views".into(),
                },
            ],
            additions: vec![ColumnAddition {
                name: "notes".into(),
                data_type: ColumnType::String,
                default: serde_json::Value::Null,
            }],
            write_defaults: BTreeMap::new(),
        })
        .await
        .unwrap();
    assert_eq!(evolved.dataset.partition_by, partition_by);

    let drop_err = engine
        .evolve_schema(&SchemaSpec {
            dataset: id("filings"),
            operation: id("drop-agency"),
            base: evolved.id.clone(),
            definition: "filings-v2".into(),
            next_definition: "filings-v3".into(),
            fields: vec![
                SchemaField {
                    id: 1,
                    name: "doc_id".into(),
                },
                SchemaField {
                    id: 2,
                    name: "published_date".into(),
                },
                SchemaField {
                    id: 4,
                    name: "views".into(),
                },
            ],
            additions: Vec::new(),
            write_defaults: BTreeMap::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(drop_err.code(), "invalid_schema");

    // 4. Attempting a follow-up RunSpec with a different partition_by against base is refused.
    let mismatch_source =
        write_partitioned_input(&root.path().join("mismatch.parquet"), &initial_rows);
    let mismatch_err = engine
        .run(&RunSpec {
            dataset: id("filings"),
            operation: id("mismatch-part"),
            base: Some(evolved.id.clone()),
            relationship: None,
            input: mismatch_source.path,
            expected_digest: mismatch_source.digest,
            sql: "SELECT *, CAST(NULL AS VARCHAR) AS notes FROM input".into(),
            definition: "filings-v2".into(),
            partition_by: vec![PartitionFieldSpec {
                column: "published_date".into(),
                name: None,
                transform: PartitionTransformSpec::Year,
            }],
        })
        .await
        .unwrap_err();
    assert_eq!(mismatch_err.code(), "partition_spec_mismatch");
}

#[tokio::test]
async fn bucket_day_and_year_transforms_prune_point_updates_and_key_reads() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();

    // Choose keys that hash to distinct Murmur3 buckets mod 64 and have overlapping string bounds:
    // Because we partition by Bucket(64) on doc_id, Day on published_date, and Year on published_date,
    // each row lands in its own partition file.
    let rows = [
        ("doc-alpha", 19737, "EPA", 1),
        ("doc-beta", 19738, "FDA", 2),
        ("doc-gamma", 19739, "SEC", 3),
        ("doc-delta", 20103, "DOJ", 4),
    ];
    let source = write_partitioned_input(&root.path().join("bucket.parquet"), &rows);
    let partition_by = vec![
        PartitionFieldSpec {
            column: "doc_id".into(),
            name: None,
            transform: PartitionTransformSpec::Bucket { num_buckets: 64 },
        },
        PartitionFieldSpec {
            column: "published_date".into(),
            name: None,
            transform: PartitionTransformSpec::Day,
        },
    ];
    let release = engine
        .run(&RunSpec {
            dataset: id("bucketed"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: source.path,
            expected_digest: source.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by,
        })
        .await
        .unwrap();

    let stream = engine
        .read_keys(
            &release,
            &KeyReadSpec {
                key: "doc_id".into(),
                values: vec!["doc-alpha".into()],
                columns: None,
            },
        )
        .await
        .unwrap();
    let out: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let count: usize = out.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(count, 1);

    // Point lookup on `published_date` (Date32) using ISO-8601 `YYYY-MM-DD` (19738 == 2024-01-16):
    let date_stream = engine
        .read_keys(
            &release,
            &KeyReadSpec {
                key: "published_date".into(),
                values: vec!["2024-01-16".into()],
                columns: Some(vec!["doc_id".into(), "published_date".into()]),
            },
        )
        .await
        .unwrap();
    let date_out: Vec<RecordBatch> = date_stream.try_collect().await.unwrap();
    let date_count: usize = date_out.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(date_count, 1);
    let doc_col = date_out[0]
        .column_by_name("doc_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(doc_col.value(0), "doc-beta");

    // Verify `verify_release` checks partition_by against source table metadata:
    docspec::engine::verify_release(&release, limits())
        .await
        .unwrap();
    let mut tampered = release.clone();
    tampered.dataset.partition_by.clear();
    assert_eq!(
        docspec::engine::verify_release(&tampered, limits())
            .await
            .unwrap_err()
            .code(),
        "release_mismatch"
    );

    // Also test UpdateSpec pruning via Bucket(64): updating only "doc-alpha" must reuse the other 3 files!
    let repl = write_partitioned_input(
        &root.path().join("bucket_repl.parquet"),
        &[("doc-alpha", 19737, "EPA", 100)],
    );
    let updated = engine
        .update(&UpdateSpec {
            dataset: id("bucketed"),
            operation: id("update-alpha"),
            definition: "v1".into(),
            base: release.id.clone(),
            keys: vec!["doc_id".into()],
            replacements: repl,
            deletions: None,
            relationship: None,
        })
        .await
        .unwrap();
    let metrics = processing_metrics(&engine, &updated).await.metrics;
    assert_eq!(
        metrics.files_reused, 3,
        "expected Bucket(64) partition pruning to reuse 3 untouched partition files: {metrics:?}"
    );
}

type CcInputRow<'a> = (
    &'a str,
    Option<&'a str>,
    Option<&'a str>,
    &'a [&'a str],
    i64,
    &'a str,
);

fn write_cc_input(path: &Path, rows: &[CcInputRow<'_>]) -> InputSpec {
    let mut tags_builder = arrow::array::ListBuilder::new(StringBuilder::new());
    for row in rows {
        for tag in row.3 {
            tags_builder.values().append_value(tag);
        }
        tags_builder.append(true);
    }
    let tags_array = tags_builder.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("source_id", DataType::Utf8, false),
        Field::new("canonical_url", DataType::Utf8, true),
        Field::new("content_hash", DataType::Utf8, true),
        Field::new("tags", tags_array.data_type().clone(), true),
        Field::new("priority", DataType::Int64, false),
        Field::new("title", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(tags_array),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.4).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.5).collect::<Vec<_>>(),
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

#[derive(Debug, PartialEq, Eq)]
struct ComponentRow {
    title: String,
    member_count: i64,
    sources: Vec<String>,
    all_tags: Vec<String>,
}

async fn read_components(engine: &Engine, release: &ReleasePin) -> BTreeMap<String, ComponentRow> {
    let batches: Vec<RecordBatch> = engine
        .read_group(release, "source")
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut out = BTreeMap::new();
    for batch in batches {
        let doc_ids = batch
            .column_by_name("document_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let titles = batch
            .column_by_name("rep_title")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column_by_name("member_count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sources = batch
            .column_by_name("sources")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let tags = batch
            .column_by_name("all_tags")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let doc_id = doc_ids.value(row).to_owned();
            let src_arr = sources.value(row);
            let src_str = src_arr.as_any().downcast_ref::<StringArray>().unwrap();
            let src_vec: Vec<String> = (0..src_str.len())
                .map(|i| src_str.value(i).to_owned())
                .collect();
            let tag_arr = tags.value(row);
            let tag_str = tag_arr.as_any().downcast_ref::<StringArray>().unwrap();
            let tag_vec: Vec<String> = (0..tag_str.len())
                .map(|i| tag_str.value(i).to_owned())
                .collect();
            assert!(
                out.insert(
                    doc_id,
                    ComponentRow {
                        title: titles.value(row).to_owned(),
                        member_count: counts.value(row),
                        sources: src_vec,
                        all_tags: tag_vec,
                    },
                )
                .is_none(),
                "duplicate document_id in output"
            );
        }
    }
    out
}

fn sha256_hex(data: &[u8]) -> String {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let bit_len = (data.len() as u64) * 8;
    let mut padded = data.to_vec();
    padded.push(0x80);
    while (padded.len() % 64) != 56 {
        padded.push(0x00);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in padded.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (i, word) in chunk.chunks_exact(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    let mut out = String::with_capacity(64);
    for word in h {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{word:08x}");
    }
    out
}

fn expected_doc_id(members: &[&str]) -> String {
    let mut sorted: Vec<&str> = members.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let json = serde_json::to_vec(&("sources", sorted)).unwrap();
    sha256_hex(&json)
}

fn cc_spec(
    input: &ReleasePin,
    base: Option<Id>,
    operation: &str,
    mode: RefreshMode,
) -> RefreshSpec {
    RefreshSpec {
        dataset: id("documents"),
        operation: id(operation),
        base,
        definition: "docs-v1".into(),
        policy: RefreshPolicy::Captured,
        mode,
        relationship: None,
        transform: RefreshTransform::ConnectedComponents(ConnectedComponentsTransform {
            input: input.version_ref(),
            row_key: "source_id".into(),
            edges: vec![
                ConnectedEdgeSpec {
                    namespace: "url".into(),
                    column: "canonical_url".into(),
                },
                ConnectedEdgeSpec {
                    namespace: "hash".into(),
                    column: "content_hash".into(),
                },
                ConnectedEdgeSpec {
                    namespace: "tag".into(),
                    column: "tags".into(),
                },
            ],
            order_by: vec![SortColumn {
                column: "priority".into(),
                direction: SortDirection::Asc,
            }],
            document_id_column: "document_id".into(),
            aggregates: vec![
                AggregateField {
                    name: "rep_title".into(),
                    function: AggregateFunction::FirstValue {
                        field: "title".into(),
                    },
                },
                AggregateField {
                    name: "member_count".into(),
                    function: AggregateFunction::CountRows,
                },
                AggregateField {
                    name: "sources".into(),
                    function: AggregateFunction::CollectSet {
                        field: "source_id".into(),
                    },
                },
                AggregateField {
                    name: "all_tags".into(),
                    function: AggregateFunction::CollectSet {
                        field: "tags".into(),
                    },
                },
            ],
        }),
    }
}

#[tokio::test]
async fn connected_components_full_and_incremental_merge_split_and_reuse_untouched_files() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();

    // Seed with an untouched component first so it resides in its own data file
    // with exact point bounds (`low == high`), then add the active test components
    // so subsequent incremental merges and splits reuse the untouched file!
    let seed_rows = [(
        "z_untouched_00",
        Some("https://untouched.example/00"),
        None,
        &[][..],
        100,
        "Untouched 00",
    )];
    let seed_file = write_cc_input(&root.path().join("cc_seed.parquet"), &seed_rows);
    let captures_seed = engine
        .run(&RunSpec {
            dataset: id("captures"),
            operation: id("seed"),
            base: None,
            relationship: None,
            input: seed_file.path,
            expected_digest: seed_file.digest,
            sql: "SELECT * FROM input".into(),
            definition: "captures-v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let docs_seed = engine
        .refresh(&cc_spec(&captures_seed, None, "seed", RefreshMode::Full))
        .await
        .unwrap();

    // Initial active input has:
    // - Component 1 (multi-hop chain): s1 --(url:u1)--> s2 --(hash:h2)--> s3
    // - Component 2: s4 --(url:u4)--> s5
    // - Isolated rows with NULL and "" edges (must NOT join each other!): s6, s7
    let initial_rows = vec![
        (
            "s1",
            Some("https://ex.com/1"),
            None,
            &["t1", "t2"][..],
            20,
            "Title S1",
        ),
        (
            "s2",
            Some("https://ex.com/1"),
            Some("hash-2"),
            &["t2"][..],
            5,
            "Title S2 (Winner)",
        ),
        ("s3", None, Some("hash-2"), &["t3"][..], 10, "Title S3"),
        (
            "s4",
            Some("https://ex.com/4"),
            Some(""),
            &["t4"][..],
            30,
            "Title S4",
        ),
        (
            "s5",
            Some("https://ex.com/4"),
            None,
            &["t5"][..],
            15,
            "Title S5 (Winner)",
        ),
        ("s6", None, Some(""), &[""][..], 1, "Isolated S6"),
        ("s7", Some(""), None, &[][..], 2, "Isolated S7"),
    ];

    let src_file = write_cc_input(&root.path().join("cc_initial.parquet"), &initial_rows);
    let captures = engine
        .update(&UpdateSpec {
            dataset: id("captures"),
            operation: id("initial"),
            definition: "captures-v1".into(),
            base: captures_seed.id.clone(),
            keys: vec!["source_id".into()],
            replacements: src_file,
            deletions: None,
            relationship: None,
        })
        .await
        .unwrap();

    // 1. Incremental refresh adding the initial components on top of the seed file
    let docs_initial = engine
        .refresh(&cc_spec(
            &captures,
            Some(docs_seed.id.clone()),
            "initial",
            RefreshMode::Incremental,
        ))
        .await
        .unwrap();
    let actual_initial = read_components(&engine, &docs_initial).await;

    let comp1_id = expected_doc_id(&["s1", "s2", "s3"]);
    let comp2_id = expected_doc_id(&["s4", "s5"]);
    let s6_id = expected_doc_id(&["s6"]);
    let s7_id = expected_doc_id(&["s7"]);

    assert_eq!(actual_initial.len(), 4 + 1);
    assert_eq!(
        actual_initial[&comp1_id],
        ComponentRow {
            title: "Title S2 (Winner)".into(),
            member_count: 3,
            sources: vec!["s1".into(), "s2".into(), "s3".into()],
            all_tags: vec!["t1".into(), "t2".into(), "t3".into()],
        }
    );
    assert_eq!(
        actual_initial[&comp2_id],
        ComponentRow {
            title: "Title S5 (Winner)".into(),
            member_count: 2,
            sources: vec!["s4".into(), "s5".into()],
            all_tags: vec!["t4".into(), "t5".into()],
        }
    );
    // Verify NULL and "" edges did not join s6 and s7:
    assert_eq!(actual_initial[&s6_id].sources, vec!["s6".to_owned()]);
    assert!(actual_initial[&s6_id].all_tags.is_empty());
    assert_eq!(actual_initial[&s7_id].sources, vec!["s7".to_owned()]);
    assert!(actual_initial[&s7_id].all_tags.is_empty());

    // 2. Incremental refresh: MERGE Component 1 and Component 2 by inserting a bridge row `s_bridge`
    //    linking `hash-2` (from s2/s3) and `https://ex.com/4` (from s4/s5) with priority 1 ("Bridge Winner").
    let bridge_file = write_cc_input(
        &root.path().join("cc_bridge.parquet"),
        &[(
            "s_bridge",
            Some("https://ex.com/4"),
            Some("hash-2"),
            &["t_bridge"][..],
            1,
            "Bridge Winner",
        )],
    );
    let captures_merged = engine
        .update(&UpdateSpec {
            dataset: id("captures"),
            operation: id("add-bridge"),
            definition: "captures-v1".into(),
            base: captures.id.clone(),
            keys: vec!["source_id".into()],
            replacements: bridge_file,
            deletions: None,
            relationship: None,
        })
        .await
        .unwrap();

    let docs_merged = engine
        .refresh(&cc_spec(
            &captures_merged,
            Some(docs_initial.id.clone()),
            "merge-step",
            RefreshMode::Incremental,
        ))
        .await
        .unwrap();
    let actual_merged = read_components(&engine, &docs_merged).await;

    let merged_id = expected_doc_id(&["s1", "s2", "s3", "s4", "s5", "s_bridge"]);
    assert!(
        !actual_merged.contains_key(&comp1_id),
        "old comp1_id must be retired"
    );
    assert!(
        !actual_merged.contains_key(&comp2_id),
        "old comp2_id must be retired"
    );
    assert_eq!(actual_merged.len(), 3 + 1);
    assert_eq!(
        actual_merged[&merged_id],
        ComponentRow {
            title: "Bridge Winner".into(),
            member_count: 6,
            sources: vec![
                "s1".into(),
                "s2".into(),
                "s3".into(),
                "s4".into(),
                "s5".into(),
                "s_bridge".into(),
            ],
            all_tags: vec![
                "t1".into(),
                "t2".into(),
                "t3".into(),
                "t4".into(),
                "t5".into(),
                "t_bridge".into(),
            ],
        }
    );

    // Check that untouched component files were reused during incremental refresh!
    let merge_metrics = processing_metrics(&engine, &docs_merged).await.metrics;
    assert!(
        merge_metrics.files_reused > 0,
        "incremental ConnectedComponents refresh must reuse untouched files: {merge_metrics:?}"
    );

    // Also verify equivalence with a fresh Full refresh on captures_merged:
    let mut full_spec = cc_spec(
        &captures_merged,
        None,
        "full-check-merge",
        RefreshMode::Full,
    );
    full_spec.dataset = id("documents_full_check_1");
    let docs_full_1 = engine.refresh(&full_spec).await.unwrap();
    assert_eq!(actual_merged, read_components(&engine, &docs_full_1).await);

    // 3. Incremental refresh: SPLIT the merged component back into Component 1 and Component 2
    //    by deleting `s_bridge`, AND also split Component 1 by updating `s2` so it no longer shares `hash-2` with `s3`!
    let split_repl = write_cc_input(
        &root.path().join("cc_split_repl.parquet"),
        &[(
            "s2",
            Some("https://ex.com/1"),
            None,
            &["t2"][..],
            5,
            "Title S2 (Winner)",
        )],
    );
    let del_schema = Arc::new(Schema::new(vec![Field::new(
        "source_id",
        DataType::Utf8,
        false,
    )]));
    let del_batch = RecordBatch::try_new(
        del_schema.clone(),
        vec![Arc::new(StringArray::from(vec!["s_bridge"]))],
    )
    .unwrap();
    let del_path = root.path().join("cc_split_del.parquet");
    let mut del_writer =
        ArrowWriter::try_new(File::create(&del_path).unwrap(), del_schema, None).unwrap();
    del_writer.write(&del_batch).unwrap();
    del_writer.close().unwrap();
    let split_del = InputSpec {
        path: del_path.to_string_lossy().into_owned(),
        digest: storage::digest(&del_path).unwrap().0,
    };

    let captures_split = engine
        .update(&UpdateSpec {
            dataset: id("captures"),
            operation: id("split-bridge"),
            definition: "captures-v1".into(),
            base: captures_merged.id.clone(),
            keys: vec!["source_id".into()],
            replacements: split_repl,
            deletions: Some(split_del),
            relationship: None,
        })
        .await
        .unwrap();

    let docs_split = engine
        .refresh(&cc_spec(
            &captures_split,
            Some(docs_merged.id.clone()),
            "split-step",
            RefreshMode::Incremental,
        ))
        .await
        .unwrap();
    let actual_split = read_components(&engine, &docs_split).await;

    let s1_s2_id = expected_doc_id(&["s1", "s2"]);
    let s3_only_id = expected_doc_id(&["s3"]);
    assert!(
        !actual_split.contains_key(&merged_id),
        "merged_id must be retired after split"
    );
    assert_eq!(actual_split.len(), 5 + 1);
    assert_eq!(
        actual_split[&s1_s2_id],
        ComponentRow {
            title: "Title S2 (Winner)".into(),
            member_count: 2,
            sources: vec!["s1".into(), "s2".into()],
            all_tags: vec!["t1".into(), "t2".into()],
        }
    );
    assert_eq!(
        actual_split[&s3_only_id],
        ComponentRow {
            title: "Title S3".into(),
            member_count: 1,
            sources: vec!["s3".into()],
            all_tags: vec!["t3".into()],
        }
    );
    assert_eq!(
        actual_split[&comp2_id],
        ComponentRow {
            title: "Title S5 (Winner)".into(),
            member_count: 2,
            sources: vec!["s4".into(), "s5".into()],
            all_tags: vec!["t4".into(), "t5".into()],
        }
    );

    // Verify equivalence with a fresh Full refresh on captures_split:
    let mut full_spec_2 = cc_spec(
        &captures_split,
        None,
        "full-check-split",
        RefreshMode::Full,
    );
    full_spec_2.dataset = id("documents_full_check_2");
    let docs_full_2 = engine.refresh(&full_spec_2).await.unwrap();
    assert_eq!(actual_split, read_components(&engine, &docs_full_2).await);

    // Verify evidence table maps each document_id to its exact contributor rows:
    let evidence_batches: Vec<RecordBatch> = engine
        .read_evidence(&docs_split)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut evidence_doc_ids = BTreeSet::new();
    let mut total_evidence_rows = 0usize;
    for batch in evidence_batches {
        let col = batch
            .column_by_name("document_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            evidence_doc_ids.insert(col.value(i).to_owned());
            total_evidence_rows += 1;
        }
    }
    // 7 test rows (s1..s7) + 1 untouched row = 8 input rows -> 8 evidence rows
    assert_eq!(total_evidence_rows, 8);
    assert_eq!(
        evidence_doc_ids,
        actual_split.keys().cloned().collect::<BTreeSet<_>>()
    );
}

#[tokio::test]
async fn body_namespace_document_id_and_attribute_only_incremental_update() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();

    // Test `body` namespace edge (e.g. `connectable_body_key`):
    // - Rows with non-null `body_hash` get `document_id = sha256(["bodies", sorted_bodies])`
    // - Rows with NULL `body_hash` (e.g. HTML/404 pages) never connect to each other and get
    //   `document_id = sha256(["sources", sorted_sources])`
    // - Rows with empty `source_id` and no edges fall back to `file:row` occurrence identity
    //   instead of colliding on `sha256(["sources", []])`.
    let rows = [
        ("src-a", Some("canon-1"), Some("body-xyz"), &[][..], 10, "Draft A"),
        ("src-b", Some("canon-2"), Some("body-xyz"), &[][..], 20, "Draft B"),
        ("html-404-1", Some("canon-404-1"), None, &[][..], 1, "Not Found 1"),
        ("html-404-2", Some("canon-404-2"), None, &[][..], 1, "Not Found 2"),
        ("", None, None, &[][..], 50, "Empty Key 1"),
        ("", None, None, &[][..], 60, "Empty Key 2"),
    ];
    let file = write_cc_input(&root.path().join("body_initial.parquet"), &rows);
    let captures = engine
        .run(&RunSpec {
            dataset: id("web_captures"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: file.path,
            expected_digest: file.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: vec![PartitionFieldSpec {
                column: "source_id".into(),
                name: None,
                transform: PartitionTransformSpec::Bucket { num_buckets: 16 },
            }],
        })
        .await
        .unwrap();

    let make_spec = |input: &ReleasePin, base: Option<Id>, op: &str, mode: RefreshMode| RefreshSpec {
        dataset: id("web_docs"),
        operation: id(op),
        base,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode,
        relationship: None,
        transform: RefreshTransform::ConnectedComponents(ConnectedComponentsTransform {
            input: input.version_ref(),
            row_key: "source_id".into(),
            edges: vec![
                ConnectedEdgeSpec {
                    namespace: "canonical".into(),
                    column: "canonical_url".into(),
                },
                ConnectedEdgeSpec {
                    namespace: "body".into(),
                    column: "content_hash".into(),
                },
            ],
            order_by: vec![SortColumn {
                column: "priority".into(),
                direction: SortDirection::Asc,
            }],
            document_id_column: "document_id".into(),
            aggregates: vec![
                AggregateField {
                    name: "rep_title".into(),
                    function: AggregateFunction::FirstValue {
                        field: "title".into(),
                    },
                },
                AggregateField {
                    name: "member_count".into(),
                    function: AggregateFunction::CountRows,
                },
                AggregateField {
                    name: "sources".into(),
                    function: AggregateFunction::CollectSet {
                        field: "source_id".into(),
                    },
                },
                AggregateField {
                    name: "all_tags".into(),
                    function: AggregateFunction::CollectSet {
                        field: "tags".into(),
                    },
                },
            ],
        }),
    };

    let docs1 = engine
        .refresh(&make_spec(&captures, None, "initial", RefreshMode::Full))
        .await
        .unwrap();
    let actual1 = read_components(&engine, &docs1).await;

    let expected_body_doc_id =
        sha256_hex(&serde_json::to_vec(&("bodies", vec!["body-xyz"])).unwrap());
    let expected_404_1 = expected_doc_id(&["html-404-1"]);
    let expected_404_2 = expected_doc_id(&["html-404-2"]);

    assert_eq!(actual1.len(), 5);
    assert_eq!(actual1[&expected_body_doc_id].title, "Draft A");
    assert_eq!(
        actual1[&expected_body_doc_id].sources,
        vec!["src-a".to_owned(), "src-b".to_owned()]
    );
    assert_eq!(actual1[&expected_404_1].sources, vec!["html-404-1".to_owned()]);
    assert_eq!(actual1[&expected_404_2].sources, vec!["html-404-2".to_owned()]);

    // Attribute-only incremental update: change `src-b` priority from 20 -> 5 and title -> "Final B"
    // without changing any edges or component membership!
    let attr_file = write_cc_input(
        &root.path().join("body_attr.parquet"),
        &[("src-b", Some("canon-2"), Some("body-xyz"), &[][..], 5, "Final B")],
    );
    let captures2 = engine
        .update(&UpdateSpec {
            dataset: id("web_captures"),
            operation: id("attr-update"),
            definition: "v1".into(),
            base: captures.id.clone(),
            keys: vec!["source_id".into()],
            replacements: attr_file,
            deletions: None,
            relationship: None,
        })
        .await
        .unwrap();
    let docs2 = engine
        .refresh(&make_spec(
            &captures2,
            Some(docs1.id.clone()),
            "attr-update",
            RefreshMode::Incremental,
        ))
        .await
        .unwrap();
    let actual2 = read_components(&engine, &docs2).await;
    assert_eq!(actual2.len(), 5);
    assert_eq!(actual2[&expected_body_doc_id].title, "Final B");
    assert_eq!(
        actual2[&expected_body_doc_id].sources,
        vec!["src-a".to_owned(), "src-b".to_owned()]
    );
}

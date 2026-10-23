//! Integration tests for `ss-023x`: `UpdateSpec.merge` column-level merge strategies
//! and ordered provenance aggregates (`FirstValueBy`, `CollectDistinctWithProvenance`).

use std::{collections::BTreeMap, fs::File, path::Path, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, Int64Array, ListArray, ListBuilder, StringArray, StringBuilder,
        StructArray, StructBuilder, TimestampMicrosecondArray,
    },
    datatypes::{DataType, Field, Fields, Int64Type, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::Limits,
    engine::{verify_applied_changes, verify_release_at},
    model::{
        AggregateField, AggregateFunction, AggregateTransform, AggregationDefinition, Id,
        InputSpec, NullOrdering, RefreshMode, RefreshPolicy, RefreshSpec, RefreshTransform,
        RunSpec, SortColumn, SortDirection, UpdateMergeSpec, UpdateMergeStrategy, UpdateSpec,
        VerificationLevel,
    },
    storage,
};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 512 << 20,
        scratch_bytes: 64 << 20,
        batch_rows: 128,
        max_batch_bytes: 4 << 20,
        max_metadata_bytes: 16 << 20,
    }
}

fn write_batch(path: &Path, batch: &RecordBatch) -> InputSpec {
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    InputSpec {
        path: path.to_string_lossy().into_owned(),
        digest: storage::digest(path).unwrap().0,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ArticleRow {
    url: String,
    updated_at: Option<i64>,
    first_seen_at: Option<i64>,
    last_seen_at: Option<i64>,
    title: Option<String>,
    author: Option<String>,
    tags: Option<Vec<String>>,
    scores: Option<Vec<i64>>,
    links: Option<Vec<(String, i64)>>,
}

fn article_schema() -> Arc<Schema> {
    let link_fields = Fields::from(vec![
        Field::new("href", DataType::Utf8, false),
        Field::new("weight", DataType::Int64, false),
    ]);
    Arc::new(Schema::new(vec![
        Field::new("url", DataType::Utf8, false),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "first_seen_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "last_seen_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("title", DataType::Utf8, true),
        Field::new("author", DataType::Utf8, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "scores",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
        Field::new(
            "links",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(link_fields),
                true,
            ))),
            true,
        ),
    ]))
}

fn build_article_batch(rows: &[ArticleRow]) -> RecordBatch {
    let schema = article_schema();
    let urls = StringArray::from_iter_values(rows.iter().map(|r| r.url.as_str()));
    let updated_at =
        TimestampMicrosecondArray::from(rows.iter().map(|r| r.updated_at).collect::<Vec<_>>())
            .with_timezone("UTC");
    let first_seen_at =
        TimestampMicrosecondArray::from(rows.iter().map(|r| r.first_seen_at).collect::<Vec<_>>())
            .with_timezone("UTC");
    let last_seen_at =
        TimestampMicrosecondArray::from(rows.iter().map(|r| r.last_seen_at).collect::<Vec<_>>())
            .with_timezone("UTC");
    let titles = StringArray::from(rows.iter().map(|r| r.title.as_deref()).collect::<Vec<_>>());
    let authors = StringArray::from(rows.iter().map(|r| r.author.as_deref()).collect::<Vec<_>>());

    let mut tags_builder = ListBuilder::new(StringBuilder::new());
    for r in rows {
        match &r.tags {
            Some(list) => {
                for item in list {
                    tags_builder.values().append_value(item);
                }
                tags_builder.append(true);
            }
            None => tags_builder.append(false),
        }
    }

    let scores = ListArray::from_iter_primitive::<Int64Type, _, _>(rows.iter().map(|r| {
        r.scores
            .as_ref()
            .map(|v| v.iter().copied().map(Some).collect::<Vec<_>>())
    }));

    let link_fields = Fields::from(vec![
        Field::new("href", DataType::Utf8, false),
        Field::new("weight", DataType::Int64, false),
    ]);
    let struct_builder = StructBuilder::new(
        link_fields,
        vec![
            Box::new(StringBuilder::new()),
            Box::new(arrow::array::Int64Builder::new()),
        ],
    );
    let mut links_builder = ListBuilder::new(struct_builder);
    for r in rows {
        match &r.links {
            Some(list) => {
                for (href, weight) in list {
                    links_builder
                        .values()
                        .field_builder::<StringBuilder>(0)
                        .unwrap()
                        .append_value(href);
                    links_builder
                        .values()
                        .field_builder::<arrow::array::Int64Builder>(1)
                        .unwrap()
                        .append_value(*weight);
                    links_builder.values().append(true);
                }
                links_builder.append(true);
            }
            None => links_builder.append(false),
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(urls) as ArrayRef,
            Arc::new(updated_at),
            Arc::new(first_seen_at),
            Arc::new(last_seen_at),
            Arc::new(titles),
            Arc::new(authors),
            Arc::new(tags_builder.finish()),
            Arc::new(scores),
            Arc::new(links_builder.finish()),
        ],
    )
    .unwrap()
}

async fn read_articles(
    engine: &Engine,
    release: &docspec::model::ReleasePin,
) -> BTreeMap<String, ArticleRow> {
    let mut stream = engine.read_group(release, "source").await.unwrap();
    let mut out = BTreeMap::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let urls = batch
            .column_by_name("url")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let updated_at = batch
            .column_by_name("updated_at")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let first_seen_at = batch
            .column_by_name("first_seen_at")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let last_seen_at = batch
            .column_by_name("last_seen_at")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let titles = batch
            .column_by_name("title")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let authors = batch
            .column_by_name("author")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let tags = batch
            .column_by_name("tags")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let scores = batch
            .column_by_name("scores")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let links = batch
            .column_by_name("links")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        for row in 0..batch.num_rows() {
            let tag_vec = (!tags.is_null(row)).then(|| {
                let arr = tags.value(row);
                let s = arr.as_any().downcast_ref::<StringArray>().unwrap();
                (0..s.len()).map(|i| s.value(i).to_owned()).collect()
            });
            let score_vec = (!scores.is_null(row)).then(|| {
                let arr = scores.value(row);
                let s = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                (0..s.len()).map(|i| s.value(i)).collect()
            });
            let link_vec = (!links.is_null(row)).then(|| {
                let arr = links.value(row);
                let st = arr.as_any().downcast_ref::<StructArray>().unwrap();
                let hrefs = st
                    .column_by_name("href")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let weights = st
                    .column_by_name("weight")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..st.len())
                    .map(|i| (hrefs.value(i).to_owned(), weights.value(i)))
                    .collect()
            });
            let article = ArticleRow {
                url: urls.value(row).to_owned(),
                updated_at: (!updated_at.is_null(row)).then(|| updated_at.value(row)),
                first_seen_at: (!first_seen_at.is_null(row)).then(|| first_seen_at.value(row)),
                last_seen_at: (!last_seen_at.is_null(row)).then(|| last_seen_at.value(row)),
                title: (!titles.is_null(row)).then(|| titles.value(row).to_owned()),
                author: (!authors.is_null(row)).then(|| authors.value(row).to_owned()),
                tags: tag_vec,
                scores: score_vec,
                links: link_vec,
            };
            assert!(out.insert(article.url.clone(), article).is_none());
        }
    }
    out
}

#[tokio::test]
async fn update_merge_strategies_preserve_history_set_union_and_fold_intra_batch_duplicates() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();

    let initial_rows = vec![
        // Row A: will receive a NEWER update (updated_at: 200 > 100) with null author (backfilled from existing).
        ArticleRow {
            url: "https://example.com/a".into(),
            updated_at: Some(100),
            first_seen_at: Some(100),
            last_seen_at: Some(100),
            title: Some("Title A v1".into()),
            author: Some("Alice".into()),
            tags: Some(vec!["news".into(), "tech".into()]),
            scores: Some(vec![10, 30]),
            links: Some(vec![("https://ref.example/1".into(), 2)]),
        },
        // Row B: will receive an OLDER update (updated_at: 50 < 100) that backfills missing author
        // and extends first_seen_at (Min), tags, scores, and links without overwriting title!
        ArticleRow {
            url: "https://example.com/b".into(),
            updated_at: Some(100),
            first_seen_at: Some(100),
            last_seen_at: Some(100),
            title: Some("Title B v2".into()),
            author: None,
            tags: Some(vec!["world".into()]),
            scores: None,
            links: Some(vec![("https://ref.example/b2".into(), 5)]),
        },
        // Row C: untouched row in the base table.
        ArticleRow {
            url: "https://example.com/c".into(),
            updated_at: Some(80),
            first_seen_at: Some(80),
            last_seen_at: Some(80),
            title: Some("Title C".into()),
            author: Some("Carol".into()),
            tags: None,
            scores: Some(vec![1]),
            links: None,
        },
    ];
    let init_spec = write_batch(
        &root.path().join("initial.parquet"),
        &build_article_batch(&initial_rows),
    );
    let base = engine
        .run(&RunSpec {
            dataset: id("articles"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: init_spec.path,
            expected_digest: init_spec.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();

    // Incoming batch has:
    // - Newer update for A (updated_at = 200)
    // - Older backfill for B (updated_at = 50)
    // - Duplicate keys for D in the same batch (updated_at = 150 and updated_at = 120)
    let incoming_rows = vec![
        ArticleRow {
            url: "https://example.com/a".into(),
            updated_at: Some(200),
            first_seen_at: Some(200),
            last_seen_at: Some(200),
            title: Some("Title A v2".into()),
            author: None, // LatestOrBackfill should preserve "Alice"
            tags: Some(vec!["ai".into(), "tech".into()]),
            scores: Some(vec![20, 30]),
            links: Some(vec![
                ("https://ref.example/0".into(), 9),
                ("https://ref.example/1".into(), 2),
            ]),
        },
        ArticleRow {
            url: "https://example.com/b".into(),
            updated_at: Some(50),
            first_seen_at: Some(50),
            last_seen_at: Some(50),
            title: Some("Title B v1 (older)".into()), // Replace should keep "Title B v2"
            author: Some("Bob (backfilled)".into()), // LatestOrBackfill should backfill None -> Some("Bob (backfilled)")
            tags: Some(vec!["archive".into(), "world".into()]),
            scores: Some(vec![5, 1]),
            links: Some(vec![("https://ref.example/b1".into(), 1)]),
        },
        // Intra-batch duplicate key D (first occurrence: newer 150, null author)
        ArticleRow {
            url: "https://example.com/d".into(),
            updated_at: Some(150),
            first_seen_at: Some(150),
            last_seen_at: Some(150),
            title: Some("Title D latest".into()),
            author: None,
            tags: Some(vec!["z".into()]),
            scores: Some(vec![100]),
            links: None,
        },
        // Intra-batch duplicate key D (second occurrence: older 120, non-null author)
        ArticleRow {
            url: "https://example.com/d".into(),
            updated_at: Some(120),
            first_seen_at: Some(120),
            last_seen_at: Some(120),
            title: Some("Title D older".into()),
            author: Some("Dan".into()),
            tags: Some(vec!["a".into(), "z".into()]),
            scores: Some(vec![50]),
            links: Some(vec![("https://ref.example/d".into(), 7)]),
        },
    ];
    let repl_spec = write_batch(
        &root.path().join("replacements.parquet"),
        &build_article_batch(&incoming_rows),
    );

    let merge_spec = UpdateMergeSpec {
        order_by: Some("updated_at".into()),
        columns: BTreeMap::from([
            ("updated_at".into(), UpdateMergeStrategy::Max),
            ("first_seen_at".into(), UpdateMergeStrategy::Min),
            ("last_seen_at".into(), UpdateMergeStrategy::Max),
            ("title".into(), UpdateMergeStrategy::Replace),
            ("author".into(), UpdateMergeStrategy::LatestOrBackfill),
            ("tags".into(), UpdateMergeStrategy::SetUnion),
            ("scores".into(), UpdateMergeStrategy::SetUnion),
            ("links".into(), UpdateMergeStrategy::SetUnion),
        ]),
    };

    let update_spec = UpdateSpec {
        dataset: id("articles"),
        operation: id("merged-1"),
        base: base.id.clone(),
        definition: "v1".into(),
        keys: vec!["url".into()],
        replacements: repl_spec,
        deletions: None,
        merge: Some(merge_spec.clone()),
        relationship: None,
    };

    // Verify JSON round-trip of UpdateSpec with merge
    let wire = serde_json::to_vec(&update_spec).unwrap();
    assert_eq!(
        serde_json::from_slice::<UpdateSpec>(&wire).unwrap(),
        update_spec
    );

    let updated = engine.update(&update_spec).await.unwrap();
    verify_release_at(&updated, limits(), VerificationLevel::Deep)
        .await
        .unwrap();
    let cert = engine
        .change_certificate(&updated.version_ref())
        .await
        .unwrap()
        .unwrap();
    verify_applied_changes(&base, &updated, &cert.changes, limits())
        .await
        .unwrap();

    let actual = read_articles(&engine, &updated).await;
    assert_eq!(actual.len(), 4);

    assert_eq!(
        actual["https://example.com/a"],
        ArticleRow {
            url: "https://example.com/a".into(),
            updated_at: Some(200),
            first_seen_at: Some(100),
            last_seen_at: Some(200),
            title: Some("Title A v2".into()),
            author: Some("Alice".into()),
            tags: Some(vec!["ai".into(), "news".into(), "tech".into()]),
            scores: Some(vec![10, 20, 30]),
            links: Some(vec![
                ("https://ref.example/0".into(), 9),
                ("https://ref.example/1".into(), 2),
            ]),
        }
    );

    assert_eq!(
        actual["https://example.com/b"],
        ArticleRow {
            url: "https://example.com/b".into(),
            updated_at: Some(100),
            first_seen_at: Some(50),
            last_seen_at: Some(100),
            title: Some("Title B v2".into()),
            author: Some("Bob (backfilled)".into()),
            tags: Some(vec!["archive".into(), "world".into()]),
            scores: Some(vec![1, 5]),
            links: Some(vec![
                ("https://ref.example/b1".into(), 1),
                ("https://ref.example/b2".into(), 5),
            ]),
        }
    );

    assert_eq!(actual["https://example.com/c"], initial_rows[2]);

    assert_eq!(
        actual["https://example.com/d"],
        ArticleRow {
            url: "https://example.com/d".into(),
            updated_at: Some(150),
            first_seen_at: Some(120),
            last_seen_at: Some(150),
            title: Some("Title D latest".into()),
            author: Some("Dan".into()),
            tags: Some(vec!["a".into(), "z".into()]),
            scores: Some(vec![50, 100]),
            links: Some(vec![("https://ref.example/d".into(), 7)]),
        }
    );

    // Validation refusals on invalid UpdateMergeSpec:
    for (name, bad_merge) in [
        (
            "unknown-order-by",
            UpdateMergeSpec {
                order_by: Some("nonexistent".into()),
                columns: BTreeMap::new(),
            },
        ),
        (
            "unknown-column",
            UpdateMergeSpec {
                order_by: None,
                columns: BTreeMap::from([("missing_col".into(), UpdateMergeStrategy::Min)]),
            },
        ),
        (
            "key-column-in-merge",
            UpdateMergeSpec {
                order_by: None,
                columns: BTreeMap::from([("url".into(), UpdateMergeStrategy::LatestOrBackfill)]),
            },
        ),
        (
            "set-union-on-scalar",
            UpdateMergeSpec {
                order_by: None,
                columns: BTreeMap::from([("title".into(), UpdateMergeStrategy::SetUnion)]),
            },
        ),
        (
            "min-on-list",
            UpdateMergeSpec {
                order_by: None,
                columns: BTreeMap::from([("tags".into(), UpdateMergeStrategy::Min)]),
            },
        ),
    ] {
        let mut bad_spec = update_spec.clone();
        bad_spec.operation = id(name);
        bad_spec.base = updated.id.clone();
        bad_spec.merge = Some(bad_merge);
        let err = engine.update(&bad_spec).await.unwrap_err();
        assert_eq!(err.code(), "invalid_merge", "case {name}: {err}");
    }
}

#[tokio::test]
async fn first_value_by_and_collect_distinct_with_provenance_full_and_incremental() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("capture_id", DataType::Int64, false),
        Field::new("fingerprint", DataType::Utf8, false),
        Field::new("outlet", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("pub_ts", DataType::Int64, true),
        Field::new("priority", DataType::Int64, true),
    ]));

    type CaptureRow<'a> = (
        i64,
        &'a str,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        Option<i64>,
        Option<i64>,
    );
    let write_captures = |name: &str, rows: &[CaptureRow<'_>]| -> InputSpec {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.4).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.5).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.6).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        write_batch(&root.path().join(name), &batch)
    };

    let initial_rows: Vec<CaptureRow<'_>> = vec![
        // Group "fp-1":
        // - capture 1 has earliest pub_ts=10, but title=None (FirstValueBy(title) must skip null title and pick capture 2!)
        // - capture 2 has pub_ts=20, priority=1, outlet="reuters.com", url="https://reuters.com/a1", title="Original Wire"
        // - capture 3 has pub_ts=20, priority=5, outlet="reuters.com", url="https://reuters.com/a2", title="Higher Priority Wire"
        //   (With order_by = [pub_ts ASC, priority DESC], capture 3 beats capture 2!)
        // - capture 4 has pub_ts=30, priority=2, outlet="apnews.com", url="https://apnews.com/a1", title="AP Copy"
        (
            1,
            "fp-1",
            Some("reuters.com"),
            Some("https://reuters.com/a0"),
            None,
            Some(10),
            Some(1),
        ),
        (
            2,
            "fp-1",
            Some("reuters.com"),
            Some("https://reuters.com/a1"),
            Some("Original Wire"),
            Some(20),
            Some(1),
        ),
        (
            3,
            "fp-1",
            Some("reuters.com"),
            Some("https://reuters.com/a2"),
            Some("Higher Priority Wire"),
            Some(20),
            Some(5),
        ),
        (
            4,
            "fp-1",
            Some("apnews.com"),
            Some("https://apnews.com/a1"),
            Some("AP Copy"),
            Some(30),
            Some(2),
        ),
        // Group "fp-2": all titles are NULL, outlet has one non-null and one null
        (
            5,
            "fp-2",
            None,
            Some("https://unknown.example/1"),
            None,
            Some(5),
            Some(1),
        ),
        (
            6,
            "fp-2",
            Some("bbc.com"),
            Some("https://bbc.com/b1"),
            None,
            Some(15),
            Some(1),
        ),
    ];

    let init_input = write_captures("captures-init.parquet", &initial_rows);
    let base_captures = engine
        .run(&RunSpec {
            dataset: id("captures"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: init_input.path,
            expected_digest: init_input.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();

    let order_by_earliest_high_prio = vec![
        SortColumn {
            field: "pub_ts".into(),
            direction: SortDirection::Asc,
            nulls: NullOrdering::NullsLast,
        },
        SortColumn {
            field: "priority".into(),
            direction: SortDirection::Desc,
            nulls: NullOrdering::NullsLast,
        },
        SortColumn::asc("capture_id"),
    ];

    let mut refresh_spec = RefreshSpec {
        dataset: id("clusters"),
        operation: id("initial"),
        base: None,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Aggregate(AggregateTransform {
            input: base_captures.version_ref(),
            grouping: AggregationDefinition {
                keys: vec!["fingerprint".into()],
                aggregates: vec![
                    AggregateField {
                        name: "mentions".into(),
                        function: AggregateFunction::CountRows,
                    },
                    AggregateField {
                        name: "canonical_title".into(),
                        function: AggregateFunction::FirstValueBy {
                            field: "title".into(),
                            order_by: order_by_earliest_high_prio.clone(),
                        },
                    },
                    AggregateField {
                        name: "latest_url".into(),
                        function: AggregateFunction::FirstValueBy {
                            field: "url".into(),
                            order_by: vec![
                                SortColumn::desc("pub_ts"),
                                SortColumn::asc("capture_id"),
                            ],
                        },
                    },
                    AggregateField {
                        name: "outlets".into(),
                        function: AggregateFunction::CollectDistinctWithProvenance {
                            field: "outlet".into(),
                            provenance_fields: vec!["url".into(), "pub_ts".into()],
                            order_by: order_by_earliest_high_prio.clone(),
                        },
                    },
                ],
                filter: None,
                projections: BTreeMap::new(),
            },
            filter: None,
            projections: BTreeMap::new(),
        }),
    };

    // Test JSON round-trip of RefreshSpec with FirstValueBy & CollectDistinctWithProvenance
    let wire = serde_json::to_vec(&refresh_spec).unwrap();
    assert_eq!(
        serde_json::from_slice::<RefreshSpec>(&wire).unwrap(),
        refresh_spec
    );

    let first_clusters = engine.refresh(&refresh_spec).await.unwrap();
    verify_release_at(&first_clusters, limits(), VerificationLevel::Deep)
        .await
        .unwrap();

    type ClusterSummary = (
        i64,
        Option<String>,
        Option<String>,
        Vec<(String, Option<String>, Option<i64>)>,
    );
    let read_clusters = |engine: &Engine, release: &docspec::model::ReleasePin| {
        let engine = engine.clone();
        let release = release.clone();
        async move {
            let mut stream = engine.read_group(&release, "source").await.unwrap();
            let mut map: BTreeMap<String, ClusterSummary> = BTreeMap::new();
            while let Some(batch) = stream.try_next().await.unwrap() {
                let fps = batch
                    .column_by_name("fingerprint")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let mentions = batch
                    .column_by_name("mentions")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let titles = batch
                    .column_by_name("canonical_title")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let latest_urls = batch
                    .column_by_name("latest_url")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let outlets = batch
                    .column_by_name("outlets")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();

                for row in 0..batch.num_rows() {
                    assert!(!outlets.is_null(row));
                    let arr = outlets.value(row);
                    let st = arr.as_any().downcast_ref::<StructArray>().unwrap();
                    let vals = st
                        .column_by_name("value")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let urls = st
                        .column_by_name("url")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let pub_ts = st
                        .column_by_name("pub_ts")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let outlet_vec = (0..st.len())
                        .map(|i| {
                            (
                                vals.value(i).to_owned(),
                                (!urls.is_null(i)).then(|| urls.value(i).to_owned()),
                                (!pub_ts.is_null(i)).then(|| pub_ts.value(i)),
                            )
                        })
                        .collect();
                    map.insert(
                        fps.value(row).to_owned(),
                        (
                            mentions.value(row),
                            (!titles.is_null(row)).then(|| titles.value(row).to_owned()),
                            (!latest_urls.is_null(row)).then(|| latest_urls.value(row).to_owned()),
                            outlet_vec,
                        ),
                    );
                }
            }
            map
        }
    };

    let actual_first = read_clusters(&engine, &first_clusters).await;
    assert_eq!(
        actual_first["fp-1"],
        (
            4,
            // capture 1 had pub_ts=10 but title=None; between capture 2 (priority=1) and capture 3 (priority=5) at pub_ts=20, capture 3 wins!
            Some("Higher Priority Wire".into()),
            Some("https://apnews.com/a1".into()),
            // For "reuters.com", capture 1 has pub_ts=10 (outlet="reuters.com" is non-null!), so capture 1 wins provenance for reuters.com!
            vec![
                (
                    "apnews.com".into(),
                    Some("https://apnews.com/a1".into()),
                    Some(30)
                ),
                (
                    "reuters.com".into(),
                    Some("https://reuters.com/a0".into()),
                    Some(10)
                ),
            ]
        )
    );
    assert_eq!(
        actual_first["fp-2"],
        (
            2,
            None,
            Some("https://bbc.com/b1".into()),
            vec![(
                "bbc.com".into(),
                Some("https://bbc.com/b1".into()),
                Some(15)
            )]
        )
    );

    // Now update captures:
    // - Delete capture 1 and capture 3 from fp-1, so capture 2 ("Original Wire", "https://reuters.com/a1", pub_ts=20) becomes the winner for both canonical_title and reuters.com provenance!
    // - Update capture 5 in fp-2 to have a non-null title "Late Breaking"
    let del_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "capture_id",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![1, 3]))],
    )
    .unwrap();
    let del_input = write_batch(&root.path().join("captures-del.parquet"), &del_batch);
    let repl_input = write_captures(
        "captures-repl.parquet",
        &[(
            5,
            "fp-2",
            None,
            Some("https://unknown.example/1"),
            Some("Late Breaking"),
            Some(5),
            Some(1),
        )],
    );
    let updated_captures = engine
        .update(&UpdateSpec {
            dataset: id("captures"),
            operation: id("update-1"),
            base: base_captures.id.clone(),
            definition: "v1".into(),
            keys: vec!["capture_id".into()],
            replacements: repl_input,
            deletions: Some(del_input),
            merge: None,
            relationship: None,
        })
        .await
        .unwrap();

    refresh_spec.operation = id("incremental-1");
    refresh_spec.base = Some(first_clusters.id.clone());
    refresh_spec.mode = RefreshMode::Incremental;
    let RefreshTransform::Aggregate(agg) = &mut refresh_spec.transform else {
        unreachable!()
    };
    agg.input = updated_captures.version_ref();

    let second_clusters = engine.refresh(&refresh_spec).await.unwrap();
    verify_release_at(&second_clusters, limits(), VerificationLevel::Deep)
        .await
        .unwrap();
    let cert = engine
        .change_certificate(&second_clusters.version_ref())
        .await
        .unwrap()
        .unwrap();
    verify_applied_changes(&first_clusters, &second_clusters, &cert.changes, limits())
        .await
        .unwrap();

    let actual_second = read_clusters(&engine, &second_clusters).await;
    assert_eq!(
        actual_second["fp-1"],
        (
            2,
            Some("Original Wire".into()),
            Some("https://apnews.com/a1".into()),
            vec![
                (
                    "apnews.com".into(),
                    Some("https://apnews.com/a1".into()),
                    Some(30)
                ),
                (
                    "reuters.com".into(),
                    Some("https://reuters.com/a1".into()),
                    Some(20)
                ),
            ]
        )
    );
    assert_eq!(
        actual_second["fp-2"],
        (
            2,
            Some("Late Breaking".into()),
            Some("https://bbc.com/b1".into()),
            vec![(
                "bbc.com".into(),
                Some("https://bbc.com/b1".into()),
                Some(15)
            )]
        )
    );
}

#[tokio::test]
async fn merge_multi_batch_chunking_and_schema_evolved_supply_flags() {
    use arrow::array::BooleanArray;
    use docspec::model::{ColumnAddition, ColumnType, SchemaField, SchemaSpec};

    let root = TempDir::new().unwrap();
    let tight_limits = Limits {
        batch_rows: 3,
        max_batch_bytes: 32 << 10,
        ..limits()
    };
    let engine = Engine::open(root.path().join("workspace"), tight_limits.clone())
        .await
        .unwrap();

    let base_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("updated_at", DataType::Int64, true),
        Field::new("title", DataType::Utf8, true),
    ]));

    let write_base_rows = |name: &str, rows: &[(i64, Option<i64>, Option<&str>)]| -> InputSpec {
        let batch = RecordBatch::try_new(
            base_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        write_batch(&root.path().join(format!("{name}.parquet")), &batch)
    };

    // 12 initial rows (> batch_rows = 3)
    let initial_rows: Vec<(i64, Option<i64>, Option<&str>)> = (1..=12)
        .map(|i| (i, Some(100), Some(if i == 1 { "Title 1" } else { "Init" })))
        .collect();
    let init_input = write_base_rows("init", &initial_rows);

    let base = engine
        .run(&RunSpec {
            dataset: id("docs"),
            operation: id("initial"),
            base: None,
            relationship: None,
            input: init_input.path,
            expected_digest: init_input.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();

    // Evolve schema to add `author` (field id 4, tracked by `__docspec_supplied_4`)
    // with non-null default "unassigned".
    let evolved = engine
        .evolve_schema(&SchemaSpec {
            dataset: id("docs"),
            operation: id("add-author"),
            base: base.id.clone(),
            definition: "v1".into(),
            next_definition: "v2".into(),
            fields: vec![
                SchemaField {
                    id: 1,
                    name: "id".into(),
                },
                SchemaField {
                    id: 2,
                    name: "updated_at".into(),
                },
                SchemaField {
                    id: 3,
                    name: "title".into(),
                },
            ],
            additions: vec![ColumnAddition {
                name: "author".into(),
                data_type: ColumnType::String,
                default: "unassigned".into(),
            }],
            write_defaults: BTreeMap::new(),
        })
        .await
        .unwrap();

    type EvolvedInputRow<'a> = (i64, Option<i64>, Option<&'a str>, Option<&'a str>);
    type StoredDocRow = (Option<i64>, Option<String>, Option<String>, bool);

    let evolved_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("updated_at", DataType::Int64, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("author", DataType::Utf8, true),
    ]));
    let write_with_author = |name: &str, rows: &[EvolvedInputRow<'_>]| -> InputSpec {
        let batch = RecordBatch::try_new(
            evolved_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        write_batch(&root.path().join(format!("{name}.parquet")), &batch)
    };

    let merge_spec = UpdateMergeSpec {
        order_by: Some("updated_at".into()),
        columns: BTreeMap::from([
            ("updated_at".into(), UpdateMergeStrategy::Max),
            ("title".into(), UpdateMergeStrategy::Replace),
            ("author".into(), UpdateMergeStrategy::LatestOrBackfill),
        ]),
    };

    // Step 1: Supply `author = "Alice"` on id=1 at updated_at=150, and backfill `author = "Bob"`
    // on id=2 at older updated_at=50 (where prior id=2 had unsupplied default "unassigned" at 100),
    // plus update ids 3..=16 (> batch_rows = 3) to exercise multi-batch chunking.
    let mut step1_rows: Vec<EvolvedInputRow<'_>> = vec![
        (1, Some(150), Some("Title 1 v2"), Some("Alice")),
        (2, Some(50), Some("Title 2 older"), Some("Bob")),
    ];
    for i in 3..=16 {
        step1_rows.push((i, Some(120), Some("Updated"), Some("Contributor")));
    }
    let step1_input = write_with_author("step1", &step1_rows);
    let after_step1 = engine
        .update(&UpdateSpec {
            dataset: id("docs"),
            operation: id("merge-step-1"),
            base: evolved.id.clone(),
            definition: "v2".into(),
            keys: vec!["id".into()],
            replacements: step1_input,
            deletions: None,
            merge: Some(merge_spec.clone()),
            relationship: None,
        })
        .await
        .unwrap();

    verify_release_at(&after_step1, tight_limits.clone(), VerificationLevel::Deep)
        .await
        .unwrap();

    // Step 2: Update id=1 at newer updated_at=200 using a file that OMITS `author`
    // (`__docspec_supplied_4 = false`, value defaults to "unassigned").
    // `LatestOrBackfill` must retain `author = "Alice"` AND `__docspec_supplied_4 = true`!
    // Also update id=2 at older updated_at=40 omitting `author`; id=2 must retain `author = "Bob"`
    // and `__docspec_supplied_4 = true`.
    let step2_input = write_base_rows(
        "step2-omitted-author",
        &[
            (1, Some(200), Some("Title 1 v3")),
            (2, Some(40), Some("Title 2 ancient")),
        ],
    );
    let after_step2 = engine
        .update(&UpdateSpec {
            dataset: id("docs"),
            operation: id("merge-step-2"),
            base: after_step1.id.clone(),
            definition: "v2".into(),
            keys: vec!["id".into()],
            replacements: step2_input,
            deletions: None,
            merge: Some(merge_spec),
            relationship: None,
        })
        .await
        .unwrap();

    verify_release_at(&after_step2, tight_limits.clone(), VerificationLevel::Deep)
        .await
        .unwrap();
    let cert2 = engine
        .change_certificate(&after_step2.version_ref())
        .await
        .unwrap()
        .unwrap();
    verify_applied_changes(
        &after_step1,
        &after_step2,
        &cert2.changes,
        tight_limits.clone(),
    )
    .await
    .unwrap();

    // Inspect physical native table including `__docspec_supplied_4`
    let budget = docspec::budget::Budget::new(tight_limits).unwrap();
    let native = storage::open_pin(&after_step2.dataset.groups["source"], &budget)
        .await
        .unwrap();
    let stream = storage::read_batches(&native, &budget).await.unwrap();
    futures::pin_mut!(stream);
    let mut by_id: BTreeMap<i64, StoredDocRow> = BTreeMap::new();
    while let Some(batch) = stream.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ts = batch
            .column_by_name("updated_at")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let titles = batch
            .column_by_name("title")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let authors = batch
            .column_by_name("author")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let supplied = batch
            .column_by_name("__docspec_supplied_4")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            by_id.insert(
                ids.value(i),
                (
                    (!ts.is_null(i)).then(|| ts.value(i)),
                    (!titles.is_null(i)).then(|| titles.value(i).to_owned()),
                    (!authors.is_null(i)).then(|| authors.value(i).to_owned()),
                    supplied.value(i),
                ),
            );
        }
    }
    assert_eq!(by_id.len(), 16);
    assert_eq!(
        by_id[&1],
        (
            Some(200),
            Some("Title 1 v3".into()),
            Some("Alice".into()),
            true
        )
    );
    assert_eq!(
        by_id[&2],
        (Some(100), Some("Init".into()), Some("Bob".into()), true)
    );
}

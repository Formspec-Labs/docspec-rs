//! Integration tests for `ss-65bz`: file-rewrite pruning metrics across key types,
//! filtered and expression-projected `JoinTransform` and `AggregateTransform`,
//! and chained multi-stage incremental `RefreshSpec` pipelines.

use std::{collections::BTreeMap, fs::File, path::Path, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Date32Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use docspec::{
    Engine,
    budget::Limits,
    engine::{verify_applied_changes, verify_release_at},
    model::{
        AggregateField, AggregateFunction, AggregateTransform, AggregationDefinition, BinaryOp,
        CaseWhen, Id, InputSpec, JoinCardinality, JoinDefinition, JoinField, JoinKey, JoinKind,
        JoinSide, JoinTransform, LiteralValue, NullMatching, PhaseResult, Predicate, RefreshMode,
        RefreshPolicy, RefreshSpec, RefreshTransform, ReleasePin, RunSpec, ScalarExpr, SortColumn,
        UpdateSpec, VerificationLevel, WriteMetrics,
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

async fn processing_metrics(engine: &Engine, release: &ReleasePin) -> WriteMetrics {
    engine
        .inspect(&release.dataset.dataset)
        .await
        .unwrap()
        .values()
        .filter_map(|value| serde_json::from_value::<PhaseResult>(value.clone()).ok())
        .find(|phase| phase.operation == release.id && phase.phase == "processing")
        .unwrap()
        .metrics
}

#[tokio::test]
async fn update_and_incremental_refresh_file_pruning_metrics_across_key_types() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();

    // 1. Multi-file table keyed by (day: Date32, ts: Timestamp(Microsecond, UTC))
    let ts_schema = Arc::new(Schema::new(vec![
        Field::new("day", DataType::Date32, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let write_ts = |name: &str, rows: &[(i32, i64, &str)]| -> InputSpec {
        let batch = RecordBatch::try_new(
            ts_schema.clone(),
            vec![
                Arc::new(Date32Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
                Arc::new(
                    TimestampMicrosecondArray::from_iter_values(rows.iter().map(|r| r.1))
                        .with_timezone("UTC"),
                ),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
            ],
        )
        .unwrap();
        write_batch(&root.path().join(name), &batch)
    };

    let f1 = write_ts(
        "f1.parquet",
        &[(19_000, 1_000_000, "a1"), (19_000, 2_000_000, "a2")],
    );
    let mut ts_rel = engine
        .run(&RunSpec {
            dataset: id("ts-events"),
            operation: id("init"),
            base: None,
            relationship: None,
            input: f1.path,
            expected_digest: f1.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();

    for (op, day, base_ts, label) in [
        ("append-2", 19_100, 10_000_000_i64, "b"),
        ("append-3", 19_200, 20_000_000_i64, "c"),
    ] {
        let f = write_ts(
            &format!("{op}.parquet"),
            &[
                (day, base_ts, &format!("{label}1")),
                (day, base_ts + 1_000, &format!("{label}2")),
            ],
        );
        ts_rel = engine
            .update(&UpdateSpec {
                dataset: id("ts-events"),
                operation: id(op),
                base: ts_rel.id.clone(),
                definition: "v1".into(),
                keys: vec!["day".into(), "ts".into()],
                replacements: f,
                deletions: None,
                merge: None,
                relationship: None,
            })
            .await
            .unwrap();
    }

    // Touch only file 3 (day = 19_200, ts = 20_000_000): files 1 and 2 must be skipped by pruning!
    let touch_f3 = write_ts("touch-f3.parquet", &[(19_200, 20_000_000, "c1-updated")]);
    let ts_updated = engine
        .update(&UpdateSpec {
            dataset: id("ts-events"),
            operation: id("touch-f3"),
            base: ts_rel.id.clone(),
            definition: "v1".into(),
            keys: vec!["day".into(), "ts".into()],
            replacements: touch_f3,
            deletions: None,
            merge: None,
            relationship: None,
        })
        .await
        .unwrap();

    let metrics = processing_metrics(&engine, &ts_updated).await;
    assert_eq!(metrics.files_skipped_by_pruning, 2, "{metrics:?}");
    assert_eq!(metrics.candidate_files_probed, 1, "{metrics:?}");
    assert_eq!(metrics.files_reused, 2, "{metrics:?}");
    assert_eq!(metrics.files_scanned, 2, "{metrics:?}");
    assert_eq!(metrics.data_files_written, 4, "{metrics:?}");

    // 2. Incremental AggregateTransform file pruning on multi-file source & target
    let mut agg_spec = RefreshSpec {
        dataset: id("daily-counts"),
        operation: id("init"),
        base: None,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Aggregate(AggregateTransform {
            input: ts_rel.version_ref(),
            grouping: AggregationDefinition {
                keys: vec!["day".into()],
                aggregates: vec![AggregateField {
                    name: "rows".into(),
                    function: AggregateFunction::CountRows,
                }],
                filter: None,
                projections: BTreeMap::new(),
            },
            filter: None,
            projections: BTreeMap::new(),
        }),
    };
    let agg_init = engine.refresh(&agg_spec).await.unwrap();

    agg_spec.operation = id("inc-1");
    agg_spec.base = Some(agg_init.id.clone());
    agg_spec.mode = RefreshMode::Incremental;
    let RefreshTransform::Aggregate(agg_transform) = &mut agg_spec.transform else {
        unreachable!()
    };
    agg_transform.input = ts_updated.version_ref();
    let agg_inc = engine.refresh(&agg_spec).await.unwrap();
    verify_release_at(&agg_inc, limits(), VerificationLevel::Deep)
        .await
        .unwrap();
    let agg_metrics = processing_metrics(&engine, &agg_inc).await;
    // Source has 3 files across 3 disjoint days; only day=19_200 changed, so 2 source files are pruned!
    assert!(
        agg_metrics.files_skipped_by_pruning >= 2,
        "expected source files skipped by pruning in incremental aggregate: {agg_metrics:?}"
    );
}

#[tokio::test]
async fn filtered_and_projected_join_transform_full_and_incremental() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();

    let left_schema = Arc::new(Schema::new(vec![
        Field::new("doc_id", DataType::Int64, false),
        Field::new("outlet_id", DataType::Utf8, false),
        Field::new("headline", DataType::Utf8, true),
        Field::new("word_count", DataType::Int64, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("outlet_id", DataType::Utf8, false),
        Field::new("tier", DataType::Int64, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("fallback_headline", DataType::Utf8, true),
    ]));

    let write_left = |name: &str, rows: &[(i64, &str, Option<&str>, i64)]| -> InputSpec {
        let batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.3))),
            ],
        )
        .unwrap();
        write_batch(&root.path().join(name), &batch)
    };
    let write_right = |name: &str, rows: &[(&str, i64, bool, Option<&str>)]| -> InputSpec {
        let batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(BooleanArray::from(
                    rows.iter().map(|r| Some(r.2)).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        write_batch(&root.path().join(name), &batch)
    };

    let l_init = write_left(
        "left-init.parquet",
        &[
            (1, "reuters", Some("Breaking Story"), 500), // matches filter (active=true, word_count >= 100)
            (2, "reuters", None, 50),                    // filtered out (word_count < 100)
            (3, "blog", Some("Short Post"), 400),        // filtered out (active=false)
        ],
    );
    let r_init = write_right(
        "right-init.parquet",
        &[
            ("reuters", 1, true, Some("Reuters Default")),
            ("blog", 3, false, Some("Blog Default")),
        ],
    );

    let left_rel = engine
        .run(&RunSpec {
            dataset: id("articles"),
            operation: id("init"),
            base: None,
            relationship: None,
            input: l_init.path,
            expected_digest: l_init.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();
    let right_rel = engine
        .run(&RunSpec {
            dataset: id("outlets"),
            operation: id("init"),
            base: None,
            relationship: None,
            input: r_init.path,
            expected_digest: r_init.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();

    // JoinTransform with structured filter and projections
    let mut join_spec = RefreshSpec {
        dataset: id("enriched-articles"),
        operation: id("init"),
        base: None,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Join(JoinTransform {
            left: left_rel.version_ref(),
            right: right_rel.version_ref(),
            join: JoinDefinition {
                keys: vec![JoinKey {
                    left: "outlet_id".into(),
                    right: "outlet_id".into(),
                }],
                kind: JoinKind::Inner,
                nulls: NullMatching::Never,
                cardinality: JoinCardinality::ManyToOne,
                fields: vec![
                    JoinField {
                        side: JoinSide::Left,
                        field: "doc_id".into(),
                        name: "doc_id".into(),
                    },
                    JoinField {
                        side: JoinSide::Left,
                        field: "outlet_id".into(),
                        name: "outlet".into(),
                    },
                    JoinField {
                        side: JoinSide::Right,
                        field: "tier".into(),
                        name: "tier".into(),
                    },
                ],
                filter: None,
                projections: BTreeMap::new(),
            },
            filter: Some(Predicate::And {
                exprs: vec![
                    Predicate::Eq {
                        left: ScalarExpr::Column {
                            field: "r.active".into(),
                        },
                        right: ScalarExpr::Literal {
                            value: LiteralValue::Bool(true),
                        },
                    },
                    Predicate::GtEq {
                        left: ScalarExpr::Column {
                            field: "l.word_count".into(),
                        },
                        right: ScalarExpr::Literal {
                            value: LiteralValue::Int64(100),
                        },
                    },
                ],
            }),
            projections: BTreeMap::from([
                (
                    "effective_headline".into(),
                    ScalarExpr::Coalesce {
                        exprs: vec![
                            ScalarExpr::Column {
                                field: "l.headline".into(),
                            },
                            ScalarExpr::Column {
                                field: "r.fallback_headline".into(),
                            },
                        ],
                    },
                ),
                (
                    "weighted_words".into(),
                    ScalarExpr::Binary {
                        left: Box::new(ScalarExpr::Column {
                            field: "l.word_count".into(),
                        }),
                        op: BinaryOp::Multiply,
                        right: Box::new(ScalarExpr::Column {
                            field: "tier".into(),
                        }),
                    },
                ),
                (
                    "size_bucket".into(),
                    ScalarExpr::Case {
                        when_then: vec![CaseWhen {
                            when: Predicate::GtEq {
                                left: ScalarExpr::Column {
                                    field: "weighted_words".into(),
                                },
                                right: ScalarExpr::Literal {
                                    value: LiteralValue::Int64(300),
                                },
                            },
                            then: ScalarExpr::Literal {
                                value: LiteralValue::String("long".into()),
                            },
                        }],
                        else_expr: Some(Box::new(ScalarExpr::Literal {
                            value: LiteralValue::String("short".into()),
                        })),
                    },
                ),
            ]),
        }),
    };

    let joined_first = engine.refresh(&join_spec).await.unwrap();
    verify_release_at(&joined_first, limits(), VerificationLevel::Deep)
        .await
        .unwrap();

    let read_joined = |engine: &Engine, rel: &ReleasePin| {
        let engine = engine.clone();
        let rel = rel.clone();
        async move {
            let mut stream = engine.read_group(&rel, "source").await.unwrap();
            let mut rows = BTreeMap::new();
            while let Some(batch) = stream.try_next().await.unwrap() {
                let ids = batch
                    .column_by_name("doc_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let outlets = batch
                    .column_by_name("outlet")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let headlines = batch
                    .column_by_name("effective_headline")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let weighted = batch
                    .column_by_name("weighted_words")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let buckets = batch
                    .column_by_name("size_bucket")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    rows.insert(
                        ids.value(i),
                        (
                            outlets.value(i).to_owned(),
                            headlines.value(i).to_owned(),
                            weighted.value(i),
                            buckets.value(i).to_owned(),
                        ),
                    );
                }
            }
            rows
        }
    };

    assert_eq!(
        read_joined(&engine, &joined_first).await,
        BTreeMap::from([(
            1,
            (
                "reuters".into(),
                "Breaking Story".into(),
                500,
                "long".into()
            )
        )])
    );

    // Update left and right so:
    // - doc 1 drops word_count to 20 -> LEAVES the filter!
    // - doc 2 increases word_count to 200 -> ENTERS the filter (and coalesces fallback_headline "Reuters Default")!
    // - outlet "blog" flips active=true -> doc 3 ENTERS the filter!
    let l_upd = write_left(
        "left-upd.parquet",
        &[(1, "reuters", Some("Breaking Story"), 20), (2, "reuters", None, 200)],
    );
    let r_upd = write_right(
        "right-upd.parquet",
        &[("blog", 3, true, Some("Blog Default"))],
    );
    let left_rel2 = engine
        .update(&UpdateSpec {
            dataset: id("articles"),
            operation: id("upd-1"),
            base: left_rel.id.clone(),
            definition: "v1".into(),
            keys: vec!["doc_id".into()],
            replacements: l_upd,
            deletions: None,
            merge: None,
            relationship: None,
        })
        .await
        .unwrap();
    let right_rel2 = engine
        .update(&UpdateSpec {
            dataset: id("outlets"),
            operation: id("upd-1"),
            base: right_rel.id.clone(),
            definition: "v1".into(),
            keys: vec!["outlet_id".into()],
            replacements: r_upd,
            deletions: None,
            merge: None,
            relationship: None,
        })
        .await
        .unwrap();

    join_spec.operation = id("inc-1");
    join_spec.base = Some(joined_first.id.clone());
    join_spec.mode = RefreshMode::Incremental;
    let RefreshTransform::Join(jt) = &mut join_spec.transform else {
        unreachable!()
    };
    jt.left = left_rel2.version_ref();
    jt.right = right_rel2.version_ref();

    let joined_second = engine.refresh(&join_spec).await.unwrap();
    verify_release_at(&joined_second, limits(), VerificationLevel::Deep)
        .await
        .unwrap();
    let cert = engine
        .change_certificate(&joined_second.version_ref())
        .await
        .unwrap()
        .unwrap();
    verify_applied_changes(&joined_first, &joined_second, &cert.changes, limits())
        .await
        .unwrap();

    assert_eq!(
        read_joined(&engine, &joined_second).await,
        BTreeMap::from([
            (
                2,
                (
                    "reuters".into(),
                    "Reuters Default".into(),
                    200,
                    "short".into()
                )
            ),
            (
                3,
                ("blog".into(), "Short Post".into(), 1200, "long".into())
            ),
        ])
    );
}

#[tokio::test]
async fn chained_incremental_refresh_and_two_stage_cross_date_syndication() {
    let root = TempDir::new().unwrap();
    let engine = Engine::open(root.path().join("workspace"), limits())
        .await
        .unwrap();

    let capture_schema = Arc::new(Schema::new(vec![
        Field::new("capture_id", DataType::Int64, false),
        Field::new("fingerprint_sha256", DataType::Utf8, false),
        Field::new("outlet_domain", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, true),
        Field::new("pub_date", DataType::Int64, false),
        Field::new("is_retracted", DataType::Boolean, false),
    ]));

    type SyndicationCapture<'a> = (i64, &'a str, &'a str, &'a str, Option<&'a str>, i64, bool);
    let write_captures = |name: &str, rows: &[SyndicationCapture<'_>]| -> InputSpec {
        let batch = RecordBatch::try_new(
            capture_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.3))),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.4).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.5))),
                Arc::new(BooleanArray::from(
                    rows.iter().map(|r| Some(r.6)).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        write_batch(&root.path().join(name), &batch)
    };

    // Initial captures:
    // - fp-alpha: 2 non-retracted captures across 2 days (syndication_count = 2 > 1 -> INCLUDED in Stage 1)
    // - fp-beta: 2 captures, but 1 is retracted (pre-aggregation filter excludes retracted -> count = 1 -> EXCLUDED by HAVING count > 1)
    // - fp-gamma: 2 non-retracted captures (syndication_count = 2 > 1 -> INCLUDED in Stage 1)
    let init_captures = write_captures(
        "captures-0.parquet",
        &[
            (
                1,
                "fp-alpha",
                "reuters.com",
                "https://reuters.com/a1",
                Some("Alpha Wire"),
                19_000,
                false,
            ),
            (
                2,
                "fp-alpha",
                "apnews.com",
                "https://apnews.com/a2",
                Some("Alpha AP"),
                19_002,
                false,
            ),
            (
                3,
                "fp-beta",
                "bbc.com",
                "https://bbc.com/b1",
                Some("Beta BBC"),
                19_005,
                false,
            ),
            (
                4,
                "fp-beta",
                "cnn.com",
                "https://cnn.com/b2",
                Some("Beta CNN Retracted"),
                19_006,
                true,
            ),
            (
                5,
                "fp-gamma",
                "nytimes.com",
                "https://nytimes.com/g1",
                Some("Gamma NYT"),
                19_010,
                false,
            ),
            (
                6,
                "fp-gamma",
                "wapo.com",
                "https://wapo.com/g2",
                Some("Gamma WaPo"),
                19_010,
                false,
            ),
        ],
    );

    let captures_r0 = engine
        .run(&RunSpec {
            dataset: id("raw-captures"),
            operation: id("r0"),
            base: None,
            relationship: None,
            input: init_captures.path,
            expected_digest: init_captures.digest,
            sql: "SELECT * FROM input".into(),
            definition: "v1".into(),
            partition_by: Vec::new(),
        })
        .await
        .unwrap();

    // Stage 1: Filtered + Projected AggregateTransform ("syndicated-clusters")
    // - Pre-aggregation filter: is_retracted = false
    // - Post-aggregation HAVING filter: syndication_count > 1
    // - Post-aggregation projection: date_span_days = latest_pub_date - earliest_pub_date
    let mut stage1_spec = RefreshSpec {
        dataset: id("syndicated-clusters"),
        operation: id("s1-r0"),
        base: None,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Aggregate(AggregateTransform {
            input: captures_r0.version_ref(),
            grouping: AggregationDefinition {
                keys: vec!["fingerprint_sha256".into()],
                aggregates: vec![
                    AggregateField {
                        name: "syndication_count".into(),
                        function: AggregateFunction::CountRows,
                    },
                    AggregateField {
                        name: "earliest_pub_date".into(),
                        function: AggregateFunction::Min {
                            field: "pub_date".into(),
                        },
                    },
                    AggregateField {
                        name: "latest_pub_date".into(),
                        function: AggregateFunction::Max {
                            field: "pub_date".into(),
                        },
                    },
                    AggregateField {
                        name: "canonical_title".into(),
                        function: AggregateFunction::FirstValueBy {
                            field: "title".into(),
                            order_by: vec![
                                SortColumn::asc("pub_date"),
                                SortColumn::asc("capture_id"),
                            ],
                        },
                    },
                    AggregateField {
                        name: "outlets".into(),
                        function: AggregateFunction::CollectDistinctWithProvenance {
                            field: "outlet_domain".into(),
                            provenance_fields: vec!["url".into(), "pub_date".into()],
                            order_by: vec![
                                SortColumn::asc("pub_date"),
                                SortColumn::asc("capture_id"),
                            ],
                        },
                    },
                ],
                filter: None,
                projections: BTreeMap::new(),
            },
            filter: Some(Predicate::And {
                exprs: vec![
                    Predicate::Eq {
                        left: ScalarExpr::Column {
                            field: "is_retracted".into(),
                        },
                        right: ScalarExpr::Literal {
                            value: LiteralValue::Bool(false),
                        },
                    },
                    Predicate::Gt {
                        left: ScalarExpr::Column {
                            field: "syndication_count".into(),
                        },
                        right: ScalarExpr::Literal {
                            value: LiteralValue::Int64(1),
                        },
                    },
                ],
            }),
            projections: BTreeMap::from([(
                "date_span_days".into(),
                ScalarExpr::Binary {
                    left: Box::new(ScalarExpr::Column {
                        field: "latest_pub_date".into(),
                    }),
                    op: BinaryOp::Subtract,
                    right: Box::new(ScalarExpr::Column {
                        field: "earliest_pub_date".into(),
                    }),
                },
            )]),
        }),
    };

    let stage1_r0 = engine.refresh(&stage1_spec).await.unwrap();
    verify_release_at(&stage1_r0, limits(), VerificationLevel::Deep)
        .await
        .unwrap();

    // Stage 2: Chained JoinTransform consuming Stage 1 ("syndicated-clusters") and "raw-captures"
    // to tag each non-retracted capture in a syndicated cluster with its cluster canonical_title and date_span_days!
    let mut stage2_spec = RefreshSpec {
        dataset: id("syndicated-members"),
        operation: id("s2-r0"),
        base: None,
        definition: "v1".into(),
        policy: RefreshPolicy::Captured,
        mode: RefreshMode::Full,
        relationship: None,
        transform: RefreshTransform::Join(JoinTransform {
            left: captures_r0.version_ref(),
            right: stage1_r0.version_ref(),
            join: JoinDefinition {
                keys: vec![JoinKey {
                    left: "fingerprint_sha256".into(),
                    right: "fingerprint_sha256".into(),
                }],
                kind: JoinKind::Inner,
                nulls: NullMatching::Never,
                cardinality: JoinCardinality::ManyToOne,
                fields: vec![
                    JoinField {
                        side: JoinSide::Left,
                        field: "capture_id".into(),
                        name: "capture_id".into(),
                    },
                    JoinField {
                        side: JoinSide::Left,
                        field: "fingerprint_sha256".into(),
                        name: "fingerprint_sha256".into(),
                    },
                    JoinField {
                        side: JoinSide::Left,
                        field: "outlet_domain".into(),
                        name: "outlet_domain".into(),
                    },
                    JoinField {
                        side: JoinSide::Right,
                        field: "canonical_title".into(),
                        name: "canonical_title".into(),
                    },
                    JoinField {
                        side: JoinSide::Right,
                        field: "syndication_count".into(),
                        name: "syndication_count".into(),
                    },
                    JoinField {
                        side: JoinSide::Right,
                        field: "date_span_days".into(),
                        name: "date_span_days".into(),
                    },
                ],
                filter: None,
                projections: BTreeMap::new(),
            },
            filter: Some(Predicate::Eq {
                left: ScalarExpr::Column {
                    field: "l.is_retracted".into(),
                },
                right: ScalarExpr::Literal {
                    value: LiteralValue::Bool(false),
                },
            }),
            projections: BTreeMap::from([(
                "lag_days".into(),
                ScalarExpr::Binary {
                    left: Box::new(ScalarExpr::Column {
                        field: "l.pub_date".into(),
                    }),
                    op: BinaryOp::Subtract,
                    right: Box::new(ScalarExpr::Column {
                        field: "r.earliest_pub_date".into(),
                    }),
                },
            )]),
        }),
    };

    let stage2_r0 = engine.refresh(&stage2_spec).await.unwrap();
    verify_release_at(&stage2_r0, limits(), VerificationLevel::Deep)
        .await
        .unwrap();

    let read_stage2 = |engine: &Engine, rel: &ReleasePin| {
        let engine = engine.clone();
        let rel = rel.clone();
        async move {
            let mut stream = engine.read_group(&rel, "source").await.unwrap();
            let mut out = BTreeMap::new();
            while let Some(batch) = stream.try_next().await.unwrap() {
                let cids = batch
                    .column_by_name("capture_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let fps = batch
                    .column_by_name("fingerprint_sha256")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let titles = batch
                    .column_by_name("canonical_title")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let counts = batch
                    .column_by_name("syndication_count")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let spans = batch
                    .column_by_name("date_span_days")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let lags = batch
                    .column_by_name("lag_days")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    out.insert(
                        cids.value(i),
                        (
                            fps.value(i).to_owned(),
                            titles.value(i).to_owned(),
                            counts.value(i),
                            spans.value(i),
                            lags.value(i),
                        ),
                    );
                }
            }
            out
        }
    };

    assert_eq!(
        read_stage2(&engine, &stage2_r0).await,
        BTreeMap::from([
            (1, ("fp-alpha".into(), "Alpha Wire".into(), 2, 2, 0)),
            (2, ("fp-alpha".into(), "Alpha Wire".into(), 2, 2, 2)),
            (5, ("fp-gamma".into(), "Gamma NYT".into(), 2, 0, 0)),
            (6, ("fp-gamma".into(), "Gamma NYT".into(), 2, 0, 0)),
        ])
    );

    // Now apply an update to raw-captures:
    // - Un-retract capture 4 in fp-beta (is_retracted = false) -> fp-beta crosses HAVING syndication_count > 1 and ENTERS Stage 1 and Stage 2!
    // - Retract capture 6 in fp-gamma (is_retracted = true) -> fp-gamma drops to syndication_count = 1 and LEAVES Stage 1 and Stage 2!
    // - Add capture 7 to fp-alpha at pub_date = 19_005 -> fp-alpha syndication_count becomes 3, date_span_days becomes 5!
    let upd_captures = write_captures(
        "captures-1.parquet",
        &[
            (
                4,
                "fp-beta",
                "cnn.com",
                "https://cnn.com/b2",
                Some("Beta CNN Unretracted"),
                19_006,
                false,
            ),
            (
                6,
                "fp-gamma",
                "wapo.com",
                "https://wapo.com/g2",
                Some("Gamma WaPo"),
                19_010,
                true,
            ),
            (
                7,
                "fp-alpha",
                "guardian.com",
                "https://guardian.com/a3",
                Some("Alpha Guardian"),
                19_005,
                false,
            ),
        ],
    );
    let captures_r1 = engine
        .update(&UpdateSpec {
            dataset: id("raw-captures"),
            operation: id("r1"),
            base: captures_r0.id.clone(),
            definition: "v1".into(),
            keys: vec!["capture_id".into()],
            replacements: upd_captures,
            deletions: None,
            merge: None,
            relationship: None,
        })
        .await
        .unwrap();

    // Incrementally refresh Stage 1!
    stage1_spec.operation = id("s1-r1");
    stage1_spec.base = Some(stage1_r0.id.clone());
    stage1_spec.mode = RefreshMode::Incremental;
    let RefreshTransform::Aggregate(s1_agg) = &mut stage1_spec.transform else {
        unreachable!()
    };
    s1_agg.input = captures_r1.version_ref();

    let stage1_r1 = engine.refresh(&stage1_spec).await.unwrap();
    verify_release_at(&stage1_r1, limits(), VerificationLevel::Deep)
        .await
        .unwrap();
    let s1_cert = engine
        .change_certificate(&stage1_r1.version_ref())
        .await
        .unwrap()
        .unwrap();
    verify_applied_changes(&stage1_r0, &stage1_r1, &s1_cert.changes, limits())
        .await
        .unwrap();

    // Incrementally refresh Stage 2 (consuming the incrementally refreshed Stage 1 release)!
    stage2_spec.operation = id("s2-r1");
    stage2_spec.base = Some(stage2_r0.id.clone());
    stage2_spec.mode = RefreshMode::Incremental;
    let RefreshTransform::Join(s2_join) = &mut stage2_spec.transform else {
        unreachable!()
    };
    s2_join.left = captures_r1.version_ref();
    s2_join.right = stage1_r1.version_ref();

    let stage2_r1 = engine.refresh(&stage2_spec).await.unwrap();
    verify_release_at(&stage2_r1, limits(), VerificationLevel::Deep)
        .await
        .unwrap();
    let s2_cert = engine
        .change_certificate(&stage2_r1.version_ref())
        .await
        .unwrap()
        .unwrap();
    verify_applied_changes(&stage2_r0, &stage2_r1, &s2_cert.changes, limits())
        .await
        .unwrap();

    let expected_stage2 = BTreeMap::from([
        (1, ("fp-alpha".into(), "Alpha Wire".into(), 3, 5, 0)),
        (2, ("fp-alpha".into(), "Alpha Wire".into(), 3, 5, 2)),
        (7, ("fp-alpha".into(), "Alpha Wire".into(), 3, 5, 5)),
        (3, ("fp-beta".into(), "Beta BBC".into(), 2, 1, 0)),
        (4, ("fp-beta".into(), "Beta BBC".into(), 2, 1, 1)),
    ]);
    assert_eq!(read_stage2(&engine, &stage2_r1).await, expected_stage2);

    // Also run a fresh Full refresh of Stage 2 on (captures_r1, stage1_r1) and verify exact agreement!
    let mut stage2_full_oracle = stage2_spec.clone();
    stage2_full_oracle.dataset = id("syndicated-members-oracle");
    stage2_full_oracle.operation = id("full-oracle");
    stage2_full_oracle.base = None;
    stage2_full_oracle.mode = RefreshMode::Full;
    let oracle_rel = engine.refresh(&stage2_full_oracle).await.unwrap();
    assert_eq!(read_stage2(&engine, &oracle_rel).await, expected_stage2);
}

//! Authored graph fixtures bind generic grouping without document policy.
// Rust guideline compliant 2026-02-21
use docspec::{budget::Limits, job::JobSpec};
use serde_json::{Value, json};

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 64 << 20,
        max_batch_bytes: 4 << 20,
        batch_rows: 2,
        scratch_bytes: 32 << 20,
        ..Limits::default()
    }
}
fn spec(root: &std::path::Path, sql: &str, node_limit: usize) -> JobSpec {
    serde_json::from_value(json!({
        "scratch": root, "inputs": [],
        "steps": [{"name":"edges", "sql":sql},
            {"name":"groups", "components":{"sql":"SELECT * FROM edges", "left":"l", "right":"r", "node_limit":node_limit}, "release_after":["edges"]}],
        "outputs": [], "queries":[{"name":"groups", "sql":"SELECT node,component FROM groups ORDER BY node"},
            {"name":"reuse", "sql":"SELECT count(*) AS n FROM groups a JOIN groups b ON a.node=b.node"}]
    })).unwrap()
}

#[tokio::test]
async fn graph_labels_are_minimum_utf8_nodes_independent_of_order_and_threads() {
    let root = tempfile::tempdir().unwrap();
    let edges = "SELECT * FROM (VALUES ('z','é'),('é','a'),('z','é'),('β','β'),('','k'),('singleton','singleton')) AS e(l,r)";
    let expected = json!([
        {"node":"", "component":""}, {"node":"a", "component":"a"},
        {"node":"k", "component":""}, {"node":"singleton", "component":"singleton"},
        {"node":"z", "component":"a"}, {"node":"é", "component":"a"},
        {"node":"β", "component":"β"}
    ]);
    for threads in [1, 2] {
        for reverse in [false, true] {
            let sql = format!(
                "{edges}{}",
                if reverse {
                    " ORDER BY l DESC,r DESC"
                } else {
                    ""
                }
            );
            let mut ceiling = limits();
            ceiling.compute_threads = threads;
            let result = spec(root.path(), &sql, 7).run(ceiling).await.unwrap();
            assert_eq!(result.queries[0].data, expected);
            assert_eq!(result.queries[1].data, json!([{"n":7}]));
        }
    }
}

#[tokio::test]
async fn graph_empty_self_edges_and_exact_node_cap_have_explicit_results() {
    let root = tempfile::tempdir().unwrap();
    let empty = spec(
        root.path(),
        "SELECT cast(NULL AS VARCHAR) AS l,cast(NULL AS VARCHAR) AS r WHERE false",
        1,
    )
    .run(limits())
    .await
    .unwrap();
    assert_eq!(empty.queries[0].data, json!([]));
    let exact = spec(root.path(), "SELECT 'b' AS l,'a' AS r", 2)
        .run(limits())
        .await
        .unwrap();
    assert_eq!(
        exact.queries[0].data,
        json!([{"node":"a","component":"a"},{"node":"b","component":"a"}])
    );
    let refused = spec(root.path(), "SELECT 'b' AS l,'a' AS r", 1)
        .run(limits())
        .await
        .unwrap_err();
    assert_eq!(refused.error.code(), "capacity");
}

#[tokio::test]
async fn graph_invalid_endpoints_and_memory_expansion_refuse() {
    let root = tempfile::tempdir().unwrap();
    for sql in [
        "SELECT cast(NULL AS VARCHAR) AS l,'a' AS r",
        "SELECT 1 AS l,'a' AS r",
        "SELECT 'a' AS l,'b' AS other",
    ] {
        let failure = spec(root.path(), sql, 10).run(limits()).await.unwrap_err();
        assert_eq!(failure.error.code(), "invalid_spec", "{failure}");
    }
    let mut ceiling = limits();
    ceiling.memory_bytes = 8 << 20;
    let failure = spec(
        root.path(),
        "SELECT repeat('a',3000000) AS l,repeat('b',3000000) AS r",
        2,
    )
    .run(ceiling)
    .await
    .unwrap_err();
    assert_eq!(failure.error.code(), "capacity", "{failure}");
    let mut mixed: Value =
        serde_json::to_value(spec(root.path(), "SELECT 'a' AS l,'b' AS r", 2)).unwrap();
    mixed["steps"][1]["sql"] = json!("SELECT 1");
    assert!(serde_json::from_value::<JobSpec>(mixed).is_err());
}

#[tokio::test]
async fn graph_rehashes_beyond_initial_capacity_and_releases_failed_growth() {
    use docspec::job::{ComponentsSettings, Job};
    let root = tempfile::tempdir().unwrap();
    let mut ceiling = limits();
    ceiling.memory_bytes = 1 << 20;
    ceiling.max_batch_bytes = 16384;
    let job = Job::new(ceiling, root.path().join("scratch")).unwrap();
    let large = ComponentsSettings {
        sql: "SELECT cast(value AS VARCHAR)||repeat('x',1000) AS l,cast(value+1 AS VARCHAR)||repeat('x',1000) AS r FROM generate_series(0,1000)".into(),
        left: "l".into(), right: "r".into(), node_limit: 2000,
    };
    let failure = job
        .connected_components("groups", &large)
        .await
        .unwrap_err();
    assert_eq!(failure.code(), "capacity", "{failure}");
    let small = ComponentsSettings {
        sql: "SELECT cast(value AS VARCHAR) AS l,cast(value+1 AS VARCHAR) AS r FROM generate_series(0,31)".into(),
        left: "l".into(), right: "r".into(), node_limit: 33,
    };
    // Reuse the name and the same job budget after failed graph growth.
    job.connected_components("groups", &small).await.unwrap();
    let rows = job.sql("SELECT count(*) AS n,count(DISTINCT node) AS unique_nodes,min(component) AS first_label,max(component) AS last_label FROM groups").await.unwrap().collect().await.unwrap();
    use arrow::util::display::array_value_to_string;
    assert_eq!(
        array_value_to_string(rows[0].column(0).as_ref(), 0).unwrap(),
        "33"
    );
    assert_eq!(
        array_value_to_string(rows[0].column(1).as_ref(), 0).unwrap(),
        "33"
    );
    assert_eq!(
        array_value_to_string(rows[0].column(2).as_ref(), 0).unwrap(),
        "0"
    );
    assert_eq!(
        array_value_to_string(rows[0].column(3).as_ref(), 0).unwrap(),
        "0"
    );
    job.release("groups").unwrap();
    drop(job);
    assert_eq!(
        std::fs::read_dir(root.path().join("scratch"))
            .unwrap()
            .count(),
        0
    );
}

#[tokio::test]
async fn graph_supports_binary_and_fixed_size_binary_keys() {
    use arrow::{
        array::{ArrayRef, BinaryArray, FixedSizeBinaryArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use docspec::job::{ComponentsSettings, Job};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("binary_edges.parquet");
    let k0 = [0x01u8; 32];
    let k1 = [0x10u8; 32];
    let k2 = [0xffu8; 32];
    let k3 = [0x20u8; 32];
    let schema = Arc::new(Schema::new(vec![
        Field::new("l_fsb", DataType::FixedSizeBinary(32), false),
        Field::new("r_fsb", DataType::FixedSizeBinary(32), false),
        Field::new("l_bin", DataType::Binary, false),
        Field::new("r_bin", DataType::Binary, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    [k2.as_slice(), k1.as_slice(), k3.as_slice()].into_iter(),
                )
                .unwrap(),
            ) as ArrayRef,
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    [k1.as_slice(), k0.as_slice(), k3.as_slice()].into_iter(),
                )
                .unwrap(),
            ) as ArrayRef,
            Arc::new(BinaryArray::from(vec![
                &b"\xff\x00"[..],
                &b"\x02"[..],
                &b"\x10\x20"[..],
            ])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![
                &b"\x02"[..],
                &b"\x01\xff"[..],
                &b"\x10\x20"[..],
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let job = Job::new(limits(), root.path().join("scratch")).unwrap();
    job.register_parquet("raw_edges", &input).await.unwrap();
    job.materialize("edges", "SELECT * FROM raw_edges")
        .await
        .unwrap();

    job.connected_components(
        "fsb_groups",
        &ComponentsSettings {
            sql: "SELECT l_fsb, r_fsb FROM edges".into(),
            left: "l_fsb".into(),
            right: "r_fsb".into(),
            node_limit: 8,
        },
    )
    .await
    .unwrap();
    let fsb_frame = job
        .sql("SELECT node, component FROM fsb_groups ORDER BY node")
        .await
        .unwrap();
    assert_eq!(
        fsb_frame.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(32)
    );
    assert_eq!(
        fsb_frame.schema().field(1).data_type(),
        &DataType::FixedSizeBinary(32)
    );
    let fsb_batches = fsb_frame.collect().await.unwrap();
    let fsb_all = arrow::compute::concat_batches(&fsb_batches[0].schema(), &fsb_batches).unwrap();
    let fsb_nodes = fsb_all
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let fsb_comps = fsb_all
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(fsb_all.num_rows(), 4);
    for (row, (expected_node, expected_comp)) in [(&k0, &k0), (&k1, &k0), (&k3, &k3), (&k2, &k0)]
        .into_iter()
        .enumerate()
    {
        assert_eq!(fsb_nodes.value(row), expected_node);
        assert_eq!(fsb_comps.value(row), expected_comp);
    }

    job.connected_components(
        "bin_groups",
        &ComponentsSettings {
            sql: "SELECT l_bin, r_bin FROM edges".into(),
            left: "l_bin".into(),
            right: "r_bin".into(),
            node_limit: 8,
        },
    )
    .await
    .unwrap();
    let bin_frame = job
        .sql("SELECT node, component FROM bin_groups ORDER BY node")
        .await
        .unwrap();
    assert_eq!(bin_frame.schema().field(0).data_type(), &DataType::Binary);
    let bin_batches = bin_frame.collect().await.unwrap();
    let bin_all = arrow::compute::concat_batches(&bin_batches[0].schema(), &bin_batches).unwrap();
    let bin_nodes = bin_all
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let bin_comps = bin_all
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(bin_all.num_rows(), 4);
    for (row, (expected_node, expected_comp)) in [
        (&b"\x01\xff"[..], &b"\x01\xff"[..]),
        (&b"\x02"[..], &b"\x01\xff"[..]),
        (&b"\x10\x20"[..], &b"\x10\x20"[..]),
        (&b"\xff\x00"[..], &b"\x01\xff"[..]),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(bin_nodes.value(row), expected_node);
        assert_eq!(bin_comps.value(row), expected_comp);
    }

    // Refuse mixed string and binary endpoints.
    let mixed = job
        .connected_components(
            "mixed",
            &ComponentsSettings {
                sql: "SELECT 'str' AS l, l_bin AS r FROM edges".into(),
                left: "l".into(),
                right: "r".into(),
                node_limit: 8,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(mixed.code(), "invalid_spec");
}

#[tokio::test]
async fn four_million_nodes_fit_budget_and_match_across_threads_and_orders() {
    use arrow::{
        array::{ArrayRef, FixedSizeBinaryArray, Int64Array},
        buffer::Buffer,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use docspec::job::{ComponentsSettings, Job};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    let root = tempfile::tempdir().unwrap();
    let edges_count = 2_000_000usize;
    let total_nodes = edges_count * 2;
    let schema = Arc::new(Schema::new(vec![
        Field::new("l", DataType::FixedSizeBinary(8), false),
        Field::new("r", DataType::FixedSizeBinary(8), false),
    ]));

    let write_edges = |path: &std::path::Path, reverse: bool| {
        let mut left_bytes = Vec::with_capacity(edges_count * 8);
        let mut right_bytes = Vec::with_capacity(edges_count * 8);
        for step in 0..edges_count {
            let idx = if reverse {
                edges_count - 1 - step
            } else {
                step
            };
            let a = (idx * 2) as u64;
            let b = (idx * 2 + 1) as u64;
            if reverse {
                left_bytes.extend_from_slice(&b.to_be_bytes());
                right_bytes.extend_from_slice(&a.to_be_bytes());
            } else {
                left_bytes.extend_from_slice(&a.to_be_bytes());
                right_bytes.extend_from_slice(&b.to_be_bytes());
            }
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_new(8, Buffer::from_vec(left_bytes), None).unwrap(),
                ) as ArrayRef,
                Arc::new(
                    FixedSizeBinaryArray::try_new(8, Buffer::from_vec(right_bytes), None).unwrap(),
                ) as ArrayRef,
            ],
        )
        .unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(65536))
            .build();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(path).unwrap(),
            schema.clone(),
            Some(props),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    };

    let forward_path = root.path().join("forward.parquet");
    let reverse_path = root.path().join("reverse.parquet");
    write_edges(&forward_path, false);
    write_edges(&reverse_path, true);

    let mut summaries = Vec::new();
    for (label, threads, path) in [
        ("forward_t1", 1, &forward_path),
        ("reverse_t2", 2, &reverse_path),
    ] {
        let job = Job::new(
            Limits {
                compute_threads: threads,
                // 320 MiB total budget (160 MiB custom pool) comfortably bounds 4M nodes (~130 MiB peak).
                memory_bytes: 320 << 20,
                max_batch_bytes: 8 << 20,
                max_metadata_bytes: 8 << 20,
                batch_rows: 65536,
                scratch_bytes: 256 << 20,
            },
            root.path().join(format!("scratch-{label}")),
        )
        .unwrap();
        job.register_parquet("edges", path).await.unwrap();
        job.connected_components(
            "groups",
            &ComponentsSettings {
                sql: "SELECT l, r FROM edges".into(),
                left: "l".into(),
                right: "r".into(),
                node_limit: total_nodes,
            },
        )
        .await
        .unwrap();
        let check = job
            .sql("SELECT count(*) AS n, sum(CASE WHEN node = component THEN 1 ELSE 0 END) AS roots, sum(CASE WHEN node < component THEN 1 ELSE 0 END) AS bad_mins, min(component) AS first_c, max(component) AS last_c FROM groups")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let n = check[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let roots = check[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let bad_mins = check[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let first_c = check[0]
            .column(3)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0)
            .to_vec();
        let last_c = check[0]
            .column(4)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0)
            .to_vec();
        assert_eq!(n, total_nodes as i64);
        assert_eq!(roots, edges_count as i64);
        assert_eq!(bad_mins, 0);
        summaries.push((n, roots, bad_mins, first_c, last_c));
    }
    assert_eq!(summaries[0], summaries[1]);
}

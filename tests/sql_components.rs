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

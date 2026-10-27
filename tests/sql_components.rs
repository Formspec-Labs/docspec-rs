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

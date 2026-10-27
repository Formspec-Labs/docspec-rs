//! Declarative catalog writes share the ordinary SQL job and guarded retry path.
// Rust guideline compliant 2026-02-21
use docspec::{budget::Limits, job::JobSpec};
use iceberg::{
    NamespaceIdent, TableIdent,
    spec::{NestedField, PrimitiveType, Schema, Type},
};
use serde_json::{Value, json};
use std::{process::Command, sync::Arc};
fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 64 << 20,
        max_batch_bytes: 2 << 20,
        max_metadata_bytes: 2 << 20,
        batch_rows: 64,
        scratch_bytes: 16 << 20,
    }
}
fn spec(root: &std::path::Path) -> Value {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .with_identifier_field_ids([1])
        .build()
        .unwrap();
    json!({"scratch":root.join("scratch"),"catalog_root":root.join("catalog"),"inputs":[],
        "steps":[{"name":"incoming","sql":"SELECT cast(value AS BIGINT) AS id FROM generate_series(1,2)"}],"outputs":[],
        "table_operations":[{"kind":"create_namespace","name":"namespace","namespace":["archive"],"properties":{}},
            {"kind":"create","name":"create","table":{"namespace":["archive"],"name":"captures"},"location":root.join("data"),"schema":schema,"format_version":2,"properties":{"gc.enabled":"true","commit.retry.num-retries":"0"}},
            {"kind":"append","name":"append","table":{"namespace":["archive"],"name":"captures"},"current_table":"existing","work_table":"pending","sql":"SELECT incoming.* FROM incoming LEFT ANTI JOIN existing ON incoming.id=existing.id","identity_sql":"SELECT 'immutable-rows-1-and-2-writer-v1' AS identity FROM pending LIMIT 1","layout":{"file_bytes":1048576,"row_group_rows":64,"row_group_bytes":1048576}}]})
}
#[tokio::test]
async fn table_job_creates_appends_and_repeated_work_is_a_noop() {
    let root = tempfile::tempdir().unwrap();
    let value = spec(root.path());
    let typed: JobSpec = serde_json::from_value(value.clone()).unwrap();
    let first = serde_json::to_value(typed.run(limits()).await.unwrap()).unwrap();
    assert_eq!(first["table_operations"].as_array().unwrap().len(), 3);
    assert_eq!(first["table_operations"][2]["files"][0]["rows"], 2);
    let catalog = docspec::storage::local_catalog(&root.path().join("catalog"))
        .await
        .unwrap();
    let ident = TableIdent::new(NamespaceIdent::new("archive".into()), "captures".into());
    let before = catalog.load_table(&ident).await.unwrap();
    let mut retry = value.clone();
    retry["table_operations"] = json!([value["table_operations"][2].clone()]);
    let retry: JobSpec = serde_json::from_value(retry).unwrap();
    let second = serde_json::to_value(retry.run(limits()).await.unwrap()).unwrap();
    assert_eq!(second["table_operations"][0]["files"], json!([]));
    assert_eq!(
        catalog
            .load_table(&ident)
            .await
            .unwrap()
            .metadata_location(),
        before.metadata_location()
    );
    assert_eq!(super::sql_tables::count(&before, "true").await, 2);
    assert!(!root.path().join("lifecycle").exists());
}

#[tokio::test]
async fn table_append_matches_native_names_when_incoming_order_differs() {
    let root = tempfile::tempdir().unwrap();
    let mut value = spec(root.path());
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "first_text",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                3,
                "second_text",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                4,
                "day",
                Type::Primitive(PrimitiveType::Date),
            )),
        ])
        .with_identifier_field_ids([1])
        .build()
        .unwrap();
    value["table_operations"][1]["schema"] = serde_json::to_value(schema).unwrap();
    value["steps"][0]["sql"] = json!(
        "SELECT 'second value' AS second_text,cast('2026-10-27' AS DATE) AS day,cast(1 AS BIGINT) AS id,'first value' AS first_text"
    );
    let result = serde_json::to_value(
        serde_json::from_value::<JobSpec>(value)
            .unwrap()
            .run(limits())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result["table_operations"][2]["files"][0]["rows"], 1);
    let catalog = docspec::storage::local_catalog(&root.path().join("catalog"))
        .await
        .unwrap();
    let table = catalog
        .load_table(&TableIdent::new(
            NamespaceIdent::new("archive".into()),
            "captures".into(),
        ))
        .await
        .unwrap();
    assert_eq!(super::sql_tables::count(&table,
        "id=1 AND first_text='first value' AND second_text='second value' AND day=cast('2026-10-27' AS DATE)").await,1);
}
#[tokio::test]
async fn table_job_refuses_invalid_bindings_before_opening_inputs() {
    let root = tempfile::tempdir().unwrap();
    let mut value = spec(root.path());
    value["inputs"] = json!([{"kind":"parquet","name":"existing","path":"missing"}]);
    let typed: JobSpec = serde_json::from_value(value).unwrap();
    let failure = typed.run(limits()).await.unwrap_err();
    assert_eq!(failure.error.code(), "invalid_spec");
    assert!(!root.path().join("catalog").exists());
    assert!(!root.path().join("scratch").exists());
}
#[tokio::test]
async fn nonempty_work_cannot_be_dropped_by_invalid_identity() {
    for identity in [
        "SELECT cast(NULL AS VARCHAR) AS identity",
        "SELECT '' AS identity",
        "SELECT 1 AS identity",
        "SELECT 'one' AS identity UNION ALL SELECT 'two' AS identity",
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut value = spec(root.path());
        value["table_operations"][2]["identity_sql"] = json!(identity);
        let failure = serde_json::from_value::<JobSpec>(value)
            .unwrap()
            .run(limits())
            .await
            .unwrap_err();
        assert_eq!(
            failure.error.code(),
            "invalid_spec",
            "{identity}: {failure}"
        );
        assert_eq!(
            serde_json::to_value(failure.completed).unwrap()["table_operations"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        let catalog = docspec::storage::local_catalog(&root.path().join("catalog"))
            .await
            .unwrap();
        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("archive".into()),
                "captures".into(),
            ))
            .await
            .unwrap();
        assert!(table.metadata().current_snapshot_id().is_none());
    }
}
#[test]
fn cli_retains_completed_native_authority_when_a_later_purge_refuses() {
    let root = tempfile::tempdir().unwrap();
    let mut value = spec(root.path());
    value["table_operations"].as_array_mut().unwrap().push(json!({"kind":"purge","name":"refused_purge","table":{"namespace":["archive"],"name":"captures"},"table_uuid":"00000000-0000-0000-0000-000000000000","metadata_location":"wrong"}));
    let path = root.path().join("job.json");
    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    let ceiling = root.path().join("limits.json");
    std::fs::write(&ceiling, serde_json::to_vec(&limits()).unwrap()).unwrap();
    let unused = root.path().join("unused-lifecycle");
    let output = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .args(["--workspace"])
        .arg(&unused)
        .arg("--limits")
        .arg(&ceiling)
        .args(["job", "--spec"])
        .arg(path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!unused.exists());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["status"], "failed");
    assert_eq!(report["error"]["code"], "stale_authority");
    let completed = report["completed"]["table_operations"].as_array().unwrap();
    assert_eq!(completed.len(), 3);
    assert!(
        completed[2]["metadata_location"]
            .as_str()
            .unwrap()
            .contains("metadata")
    );
    assert_eq!(completed[2]["files"][0]["rows"], 2);
}

#[tokio::test]
async fn injected_catalog_uses_the_same_job_and_rejects_competing_local_authority() {
    let root = tempfile::tempdir().unwrap();
    let catalog = docspec::storage::local_catalog(&root.path().join("injected"))
        .await
        .unwrap();
    let mut value = spec(root.path());
    let conflict = serde_json::from_value::<JobSpec>(value.clone())
        .unwrap()
        .run_with_catalog(limits(), catalog.clone())
        .await
        .unwrap_err();
    assert_eq!(conflict.error.code(), "invalid_spec");
    assert!(!root.path().join("scratch").exists());
    value.as_object_mut().unwrap().remove("catalog_root");
    let result = serde_json::from_value::<JobSpec>(value)
        .unwrap()
        .run_with_catalog(limits(), catalog)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(result).unwrap()["table_operations"][2]["files"][0]["rows"],
        2
    );
}

fn selected_input(operation: &Value) -> Value {
    json!({"kind":"native_table","name":"selected", "metadata_location":operation["metadata_location"],
        "table_uuid":operation["table_uuid"],"snapshot":operation.get("snapshot").cloned().unwrap_or(Value::Null),
        "schema_id":operation["schema_id"]})
}
fn selected_job(root: &std::path::Path, input: Value) -> Value {
    json!({"scratch":root.join("read-scratch"),"inputs":[input],"steps":[],"outputs":[],
        "queries":[{"name":"values","sql":"SELECT id FROM selected WHERE id>=2 ORDER BY id"}]})
}
#[tokio::test]
async fn native_selector_reads_exact_c7_snapshot_without_catalog_lookup() {
    let root = tempfile::tempdir().unwrap();
    let mut source = spec(root.path());
    let result = serde_json::to_value(
        serde_json::from_value::<JobSpec>(source.clone())
            .unwrap()
            .run(limits())
            .await
            .unwrap(),
    )
    .unwrap();
    let read = selected_job(root.path(), selected_input(&result["table_operations"][2]));
    source["steps"][0]["sql"] = json!("SELECT cast(3 AS BIGINT) AS id");
    source["table_operations"] = json!([source["table_operations"][2].clone()]);
    source["table_operations"][0]["identity_sql"] =
        json!("SELECT 'later-3' AS identity FROM pending LIMIT 1");
    serde_json::from_value::<JobSpec>(source)
        .unwrap()
        .run(limits())
        .await
        .unwrap();
    let result = serde_json::from_value::<JobSpec>(read)
        .unwrap()
        .run(limits())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(result).unwrap()["queries"][0]["data"],
        json!([{"id":2}])
    );
}
#[tokio::test]
async fn native_selector_null_snapshot_is_exactly_empty() {
    let root = tempfile::tempdir().unwrap();
    let mut source = spec(root.path());
    source["table_operations"].as_array_mut().unwrap().pop();
    let result = serde_json::to_value(
        serde_json::from_value::<JobSpec>(source)
            .unwrap()
            .run(limits())
            .await
            .unwrap(),
    )
    .unwrap();
    let read = selected_job(root.path(), selected_input(&result["table_operations"][1]));
    let result = serde_json::from_value::<JobSpec>(read)
        .unwrap()
        .run(limits())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(result).unwrap()["queries"][0]["data"],
        json!([])
    );
}
#[tokio::test]
async fn native_selector_refuses_each_mismatched_identity_before_output() {
    let root = tempfile::tempdir().unwrap();
    let result = serde_json::to_value(
        serde_json::from_value::<JobSpec>(spec(root.path()))
            .unwrap()
            .run(limits())
            .await
            .unwrap(),
    )
    .unwrap();
    let input = selected_input(&result["table_operations"][2]);
    for (key, value) in [
        ("table_uuid", json!("00000000-0000-0000-0000-000000000000")),
        ("snapshot", json!(0)),
        ("schema_id", json!(42)),
        ("snapshot", Value::Null),
    ] {
        let mut altered = input.clone();
        altered[key] = value;
        let failure = serde_json::from_value::<JobSpec>(selected_job(root.path(), altered))
            .unwrap()
            .run(limits())
            .await
            .unwrap_err();
        assert_eq!(failure.error.code(), "pin_mismatch", "{key}");
        assert!(failure.completed.outputs.is_empty());
    }
}

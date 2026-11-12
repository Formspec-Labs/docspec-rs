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
    spec_with_id(root, 1)
}
fn spec_with_id(root: &std::path::Path, field_id: i32) -> Value {
    let mut fields = Vec::new();
    // Creation assigns IDs by position. Give the second table a preceding
    // field so its id column really has a different native field identity.
    if field_id == 2 {
        fields.push(Arc::new(NestedField::required(
            1,
            "prefix",
            Type::Primitive(PrimitiveType::String),
        )));
    }
    fields.push(Arc::new(NestedField::required(
        field_id,
        "id",
        Type::Primitive(PrimitiveType::Long),
    )));
    let schema = Schema::builder()
        .with_fields(fields)
        .with_identifier_field_ids([field_id])
        .build()
        .unwrap();
    let incoming = if field_id == 2 {
        "SELECT 'fixture' AS prefix,cast(value AS BIGINT) AS id FROM generate_series(1,2)"
    } else {
        "SELECT cast(value AS BIGINT) AS id FROM generate_series(1,2)"
    };
    json!({"scratch":root.join("scratch"),"catalog_root":root.join("catalog"),"inputs":[],
        "steps":[{"name":"incoming","sql":incoming}],"outputs":[],
        "table_operations":[{"kind":"create_namespace","name":"namespace","namespace":["archive"],"properties":{}},
            {"kind":"create","name":"create","table":{"namespace":["archive"],"name":"captures"},"location":root.join("data"),"schema":schema,"format_version":2,"properties":{"gc.enabled":"true","commit.retry.num-retries":"0"}},
            {"kind":"append","name":"append","table":{"namespace":["archive"],"name":"captures"},"current_table":"existing","work_table":"pending","sql":"SELECT incoming.* FROM incoming LEFT ANTI JOIN existing ON incoming.id=existing.id","identity_sql":"SELECT 'immutable-rows-1-and-2-writer-v1' AS identity FROM pending LIMIT 1","layout":{"file_bytes":1048576,"row_group_rows":64,"row_group_bytes":1048576}}]})
}

async fn metadata_job(root: &std::path::Path) -> docspec::job::Job {
    let job = docspec::job::Job::new(limits(), root.join("union-scratch")).unwrap();
    for (name, id) in [("first", 1), ("second", 2)] {
        let result = serde_json::from_value::<JobSpec>(spec_with_id(&root.join(name), id))
            .unwrap()
            .run(limits())
            .await
            .unwrap();
        let head = &result.table_operations[2];
        job.register_native_snapshot(
            name,
            &docspec::storage::NativeTableSnapshot {
                metadata_location: head.metadata_location.clone().unwrap(),
                table_uuid: head.table_uuid.unwrap(),
                snapshot: head.snapshot,
                schema_id: head.schema_id.unwrap(),
            },
        )
        .await
        .unwrap();
        let frame = job.sql(&format!("SELECT id FROM {name}")).await.unwrap();
        assert_eq!(
            frame.schema().field(0).metadata().get("PARQUET:field_id"),
            Some(&id.to_string())
        );
    }
    job
}

async fn check_metadata_union(job: &docspec::job::Job, sql: &str, mut expected: Vec<Option<i64>>) {
    use arrow::array::{Array, Int64Array};
    // The aggregate checks the native planner's input-schema invariant. The
    // plain stream must also carry its declared metadata on every batch.
    let grouped = job
        .sql(&format!("SELECT count(*) AS n,sum(id) AS s FROM ({sql}) u"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        grouped[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        expected.len() as i64
    );
    assert_eq!(
        grouped[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        expected.iter().flatten().sum::<i64>()
    );
    let frame = job.sql(sql).await.unwrap();
    let declared = frame.schema().as_arrow().clone();
    let batches = frame.collect().await.unwrap();
    let mut actual = Vec::new();
    for batch in batches {
        assert_eq!(batch.schema().as_ref(), &declared);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        actual.extend((0..values.len()).map(|i| (!values.is_null(i)).then(|| values.value(i))));
    }
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn native_union_missing_metadata_keeps_the_declared_schema() {
    let root = tempfile::tempdir().unwrap();
    let job = metadata_job(root.path()).await;
    for sql in [
        "SELECT id FROM (SELECT id,id+100 AS unused FROM first UNION ALL SELECT cast(NULL AS BIGINT) AS id,cast(NULL AS BIGINT) AS unused) u",
        "SELECT id FROM (SELECT cast(NULL AS BIGINT) AS id,cast(NULL AS BIGINT) AS unused UNION ALL SELECT id,id+100 FROM first) u",
    ] {
        check_metadata_union(&job, sql, vec![Some(1), Some(2), None]).await;
    }
}

#[tokio::test]
async fn native_union_distinct_rebuilds_ancestor_and_subquery_schemas() {
    let root = tempfile::tempdir().unwrap();
    let job = metadata_job(root.path()).await;
    for union in ["UNION", "UNION ALL"] {
        for right in ["SELECT id FROM second", "SELECT cast(NULL AS BIGINT) AS id"] {
            let sql = format!("SELECT id FROM first {union} {right}");
            job.register_view("combined", &sql).await.unwrap();
            let expected = if right.contains("second") && union == "UNION ALL" {
                vec![Some(1), Some(2), Some(1), Some(2)]
            } else if right.contains("second") {
                vec![Some(1), Some(2)]
            } else {
                vec![Some(1), Some(2), None]
            };
            check_metadata_union(&job, &sql, expected.clone()).await;
            check_metadata_union(&job, "SELECT id FROM combined WHERE NOT EXISTS (SELECT 1 FROM combined x WHERE x.id=combined.id AND x.id<0)", expected).await;
            check_metadata_union(
                &job,
                "SELECT id FROM combined GROUP BY id",
                if right.contains("second") {
                    vec![Some(1), Some(2)]
                } else {
                    vec![Some(1), Some(2), None]
                },
            )
            .await;
            job.release("combined").unwrap();
        }
    }
}

#[tokio::test]
async fn scalar_union_metadata_rebuilds_its_outer_result_schema() {
    let root = tempfile::tempdir().unwrap();
    let job = metadata_job(root.path()).await;
    let sql = "SELECT (SELECT id FROM (SELECT id FROM first UNION SELECT id FROM second) u WHERE id=1) AS id";
    let frame = job.sql(sql).await.unwrap();
    assert!(frame.schema().field(0).metadata().is_empty());
    check_metadata_union(&job, sql, vec![Some(1)]).await;
}

#[tokio::test]
async fn native_union_conflicting_metadata_does_not_fabricate_field_identity() {
    let root = tempfile::tempdir().unwrap();
    let job = metadata_job(root.path()).await;
    check_metadata_union(
        &job,
        "SELECT id FROM (SELECT id,id+100 AS unused FROM first UNION ALL SELECT id,id+200 AS unused FROM second) u",
        vec![Some(1), Some(2), Some(1), Some(2)],
    )
    .await;
    let same = job
        .sql("SELECT id FROM first UNION ALL SELECT id FROM first")
        .await
        .unwrap();
    assert_eq!(
        same.schema()
            .field(0)
            .metadata()
            .get("PARQUET:field_id")
            .map(String::as_str),
        Some("1")
    );
    let different = job
        .sql("SELECT id FROM first UNION ALL SELECT id FROM second")
        .await
        .unwrap();
    assert!(
        !different
            .schema()
            .field(0)
            .metadata()
            .contains_key("PARQUET:field_id")
    );
}
#[tokio::test]
async fn native_try_cast_does_not_inherit_source_field_identity() {
    let root = tempfile::tempdir().unwrap();
    let job = metadata_job(root.path()).await;
    let sql = "SELECT try_cast(id AS DOUBLE) AS converted FROM first";
    let frame = job.sql(sql).await.unwrap();
    let declared = frame.schema().inner().clone();
    assert!(declared.field(0).metadata().is_empty());
    assert!(declared.field(0).is_nullable());
    for batch in frame.collect().await.unwrap() {
        assert_eq!(batch.schema(), declared);
    }
    let result = job
        .sql(&format!("SELECT min(converted) FROM ({sql}) t"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let values = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(values.value(0), 1.0);
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
async fn resolve_batches_fresh_heads_without_catalog_mutation_or_empty_append() {
    use super::sql_tables::AuthorityCatalog;
    let root = tempfile::tempdir().unwrap();
    let mut setup = spec(root.path());
    serde_json::from_value::<JobSpec>(setup.clone())
        .unwrap()
        .run(limits())
        .await
        .unwrap();
    let catalog = docspec::storage::local_catalog(&root.path().join("catalog"))
        .await
        .unwrap();
    let ident = TableIdent::new(NamespaceIdent::new("archive".into()), "captures".into());
    let before = catalog.load_table(&ident).await.unwrap();
    for name in ["readings", "filenames"] {
        catalog
            .create_table(
                ident.namespace(),
                iceberg::TableCreation::builder()
                    .name(name.into())
                    .location(root.path().join(name).to_string_lossy().into_owned())
                    .schema(before.metadata().current_schema().as_ref().clone())
                    .build(),
            )
            .await
            .unwrap();
    }
    let readonly = Arc::new(AuthorityCatalog::read_only(catalog.clone(), false));
    let value = json!({"scratch":root.path().join("resolve-scratch"),"inputs":[],"steps":[],"outputs":[],"table_operations":[
        {"kind":"resolve","name":"captures_head","table":{"namespace":["archive"],"name":"captures"}},
        {"kind":"resolve","name":"readings_head","table":{"namespace":["archive"],"name":"readings"}},
        {"kind":"resolve","name":"filenames_head","table":{"namespace":["archive"],"name":"filenames"}},
        {"kind":"resolve","name":"absent_head","table":{"namespace":["archive"],"name":"absent"}}]});
    let lookup: JobSpec = serde_json::from_value(value.clone()).unwrap();
    let first = serde_json::to_value(
        lookup
            .run_with_catalog(limits(), readonly.clone())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(readonly.load_count(), 4);
    for (index, name) in ["captures", "readings", "filenames"]
        .into_iter()
        .enumerate()
    {
        let table = catalog
            .load_table(&TableIdent::new(ident.namespace().clone(), name.into()))
            .await
            .unwrap();
        let head = &first["table_operations"][index];
        assert_eq!(head["kind"], "resolve");
        assert_eq!(head["exists"], true);
        assert_eq!(head["table"]["name"], name);
        assert_eq!(
            head["metadata_location"],
            table.metadata_location().unwrap()
        );
        assert_eq!(head["table_uuid"], table.metadata().uuid().to_string());
        assert_eq!(
            head["snapshot"],
            json!(table.metadata().current_snapshot_id())
        );
        assert_eq!(head["schema_id"], table.metadata().current_schema_id());
        assert_eq!(head["files"], json!([]));
        assert_eq!(head["retries"], 0);
    }
    assert_eq!(first["table_operations"][3]["exists"], false);
    assert!(first["table_operations"][3].get("table_uuid").is_none());
    let empty = &first["table_operations"][1];
    let empty_input: JobSpec = serde_json::from_value(json!({"scratch":root.path().join("empty-pin"),"inputs":[{"kind":"native_table","name":"empty","metadata_location":empty["metadata_location"],"table_uuid":empty["table_uuid"],"schema_id":empty["schema_id"],"snapshot":empty["snapshot"]}],"steps":[],"outputs":[],"queries":[{"name":"count","sql":"SELECT count(*) AS n FROM empty"}]})).unwrap();
    assert_eq!(
        empty_input.run(limits()).await.unwrap().queries[0].data,
        json!([{"n":0}])
    );
    setup["table_operations"] = json!([setup["table_operations"][2].clone()]);
    setup["steps"][0]["sql"] =
        json!("SELECT cast(value AS BIGINT) AS id FROM generate_series(1,3)");
    setup["table_operations"][0]["identity_sql"] =
        json!("SELECT 'immutable-row3-writer-v1' AS identity FROM pending LIMIT 1");
    serde_json::from_value::<JobSpec>(setup)
        .unwrap()
        .run(limits())
        .await
        .unwrap();
    let second = serde_json::to_value(
        lookup
            .run_with_catalog(limits(), readonly.clone())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(readonly.load_count(), 8);
    assert_ne!(
        first["table_operations"][0]["metadata_location"],
        second["table_operations"][0]["metadata_location"]
    );
    assert_eq!(
        first["table_operations"][0]["table_uuid"],
        second["table_operations"][0]["table_uuid"]
    );
    let unavailable = Arc::new(AuthorityCatalog::read_only(catalog, true));
    let error = lookup
        .run_with_catalog(limits(), unavailable)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("fixture catalog unavailable"));
    assert!(error.completed.table_operations.is_empty());
}

#[tokio::test]
async fn resolve_only_local_job_opens_catalog_read_only_and_never_creates_one() {
    let root = tempfile::tempdir().unwrap();
    let mut lookup = json!({"scratch":root.path().join("resolve-scratch"),"catalog_root":root.path().join("missing"),"inputs":[],"steps":[],"outputs":[],"table_operations":[{"kind":"resolve","name":"head","table":{"namespace":["archive"],"name":"captures"}}]});
    let error = serde_json::from_value::<JobSpec>(lookup.clone())
        .unwrap()
        .run(limits())
        .await
        .unwrap_err();
    assert!(!root.path().join("missing").exists(), "{error}");
    serde_json::from_value::<JobSpec>(spec(root.path()))
        .unwrap()
        .run(limits())
        .await
        .unwrap();
    lookup["catalog_root"] = json!(root.path().join("catalog"));
    let database = root.path().join("catalog/catalog.sqlite");
    let before = std::fs::read(&database).unwrap();
    let result = serde_json::from_value::<JobSpec>(lookup.clone())
        .unwrap()
        .run(limits())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(result).unwrap()["table_operations"][0]["exists"],
        true
    );
    assert_eq!(std::fs::read(database).unwrap(), before);
    let job = root.path().join("resolve.json");
    let ceiling = root.path().join("resolve-limits.json");
    std::fs::write(&job, serde_json::to_vec(&lookup).unwrap()).unwrap();
    std::fs::write(&ceiling, serde_json::to_vec(&limits()).unwrap()).unwrap();
    let unused = root.path().join("unused-lifecycle");
    let output = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&unused)
        .arg("--limits")
        .arg(ceiling)
        .arg("job")
        .arg("--spec")
        .arg(job)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!unused.exists());
    let cli: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(cli["table_operations"][0]["exists"], true);
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
    let advanced = serde_json::from_value::<JobSpec>(source)
        .unwrap()
        .run(limits())
        .await
        .unwrap();
    let advanced = serde_json::to_value(advanced).unwrap();
    let result = serde_json::from_value::<JobSpec>(read)
        .unwrap()
        .run(limits())
        .await
        .unwrap();
    let result = serde_json::to_value(result).unwrap();
    assert_eq!(result["queries"][0]["data"], json!([{"id":2}]));
    // The later catalog head has two snapshots and files. These facts belong
    // to the original immutable input, not a fresh head or another scan.
    assert_eq!(
        result["native_inputs"],
        json!([{"name":"selected","data_file_count":1,"snapshot_count":1,"manifest_count":null,"planning_seconds":null}])
    );
    let current = selected_job(
        root.path(),
        selected_input(&advanced["table_operations"][0]),
    );
    let current = serde_json::to_value(
        serde_json::from_value::<JobSpec>(current)
            .unwrap()
            .run(limits())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(current["queries"][0]["data"], json!([{"id":2},{"id":3}]));
    assert_eq!(
        current["native_inputs"],
        json!([{"name":"selected","data_file_count":2,"snapshot_count":2,"manifest_count":null,"planning_seconds":null}])
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
    let result = serde_json::to_value(result).unwrap();
    assert_eq!(result["queries"][0]["data"], json!([]));
    assert_eq!(
        result["native_inputs"],
        json!([{"name":"selected","data_file_count":0,"snapshot_count":0,"manifest_count":null,"planning_seconds":null}])
    );
}

#[tokio::test]
async fn native_input_observations_need_no_payload_or_optional_counter() {
    let root = tempfile::tempdir().unwrap();
    let created = serde_json::to_value(
        serde_json::from_value::<JobSpec>(spec(root.path()))
            .unwrap()
            .run(limits())
            .await
            .unwrap(),
    )
    .unwrap();
    let head = &created["table_operations"][2];
    let local = |value: &str| {
        url::Url::parse(value)
            .ok()
            .and_then(|url| url.to_file_path().ok())
            .unwrap_or_else(|| std::path::PathBuf::from(value))
    };
    // Keep the bytes, but make the referenced payload unavailable. Only the
    // selected parsed metadata may supply these observations.
    std::fs::rename(
        local(head["files"][0]["path"].as_str().unwrap()),
        root.path().join("unavailable.parquet"),
    )
    .unwrap();
    let mut metadata: Value = serde_json::from_slice(
        &std::fs::read(local(head["metadata_location"].as_str().unwrap())).unwrap(),
    )
    .unwrap();
    let snapshots = metadata["snapshots"].as_array_mut().unwrap();
    assert_eq!(snapshots.len(), 1);
    snapshots[0]["summary"]
        .as_object_mut()
        .unwrap()
        .remove("total-data-files");
    let external = root.path().join("without-optional-summary.metadata.json");
    std::fs::write(&external, serde_json::to_vec(&metadata).unwrap()).unwrap();
    let mut known = selected_input(head);
    known["name"] = json!("known");
    let mut unknown = known.clone();
    unknown["name"] = json!("unknown");
    unknown["metadata_location"] = json!(external);
    let job:JobSpec=serde_json::from_value(json!({"scratch":root.path().join("observe"),"inputs":[known,unknown],"steps":[],"outputs":[],"queries":[]})).unwrap();
    let result = serde_json::to_value(job.run(limits()).await.unwrap()).unwrap();
    assert_eq!(
        result["native_inputs"],
        json!([
            {"name":"known","data_file_count":1,"snapshot_count":1,"manifest_count":null,"planning_seconds":null},
            {"name":"unknown","data_file_count":null,"snapshot_count":1,"manifest_count":null,"planning_seconds":null}
        ])
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

#[tokio::test]
async fn native_selector_admits_observed_metadata_under_a_larger_size_ceiling() {
    let root = tempfile::tempdir().unwrap();
    let created = serde_json::to_value(
        serde_json::from_value::<JobSpec>(spec(root.path()))
            .unwrap()
            .run(limits())
            .await
            .unwrap(),
    )
    .unwrap();
    let read = selected_job(root.path(), selected_input(&created["table_operations"][2]));
    let small = Limits {
        memory_bytes: 32 << 20,
        max_metadata_bytes: 32 << 20,
        ..limits()
    };
    let result = serde_json::from_value::<JobSpec>(read)
        .unwrap()
        .run(small)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(result).unwrap()["queries"][0]["data"],
        json!([{"id":2}])
    );
}

#[tokio::test]
async fn append_identity_refuses_side_effects_with_nonempty_work() {
    for command in ["copy", "ddl", "set"] {
        let root = tempfile::tempdir().unwrap();
        let value = spec(root.path());
        serde_json::from_value::<JobSpec>(value.clone())
            .unwrap()
            .run(limits())
            .await
            .unwrap();
        let catalog = docspec::storage::local_catalog(&root.path().join("catalog"))
            .await
            .unwrap();
        let ident = TableIdent::new(NamespaceIdent::new("archive".into()), "captures".into());
        let before = catalog.load_table(&ident).await.unwrap();
        let target = root.path().join("identity-copy.parquet");
        let identity = match command {
            "copy" => format!(
                "COPY (SELECT id FROM pending) TO '{}' STORED AS PARQUET",
                target.display()
            ),
            "ddl" => "CREATE TABLE injected AS SELECT id FROM pending".into(),
            "set" => "SET datafusion.execution.batch_size = 1".into(),
            _ => unreachable!(),
        };
        let mut next = value.clone();
        next["steps"][0]["sql"] = json!("SELECT cast(3 AS BIGINT) AS id");
        next["table_operations"] = json!([value["table_operations"][2].clone()]);
        next["table_operations"][0]["identity_sql"] = json!(identity);
        let failed = serde_json::from_value::<JobSpec>(next)
            .unwrap()
            .run(limits())
            .await
            .unwrap_err();
        assert_eq!(failed.error.code(), "query", "{command}: {failed}");
        assert!(!target.exists());
        let after = catalog.load_table(&ident).await.unwrap();
        assert_eq!(before.metadata_location(), after.metadata_location());
        assert_eq!(
            before.metadata().current_snapshot_id(),
            after.metadata().current_snapshot_id()
        );
    }
}

#[tokio::test]
async fn create_defaults_data_model_properties_and_uncapped_layout_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut value = spec(root.path());
    value["steps"][0]["sql"] =
        json!("SELECT cast(value AS BIGINT) AS id FROM generate_series(1,3000)");
    value["table_operations"][1]
        .as_object_mut()
        .unwrap()
        .remove("format_version");
    value["table_operations"][1]
        .as_object_mut()
        .unwrap()
        .remove("properties");
    value["table_operations"][2]["layout"] = json!({
        "file_bytes": 128 << 20,
        "row_group_rows": 65536,
        "row_group_bytes": 64 << 20
    });
    let result = serde_json::to_value(
        serde_json::from_value::<JobSpec>(value.clone())
            .unwrap()
            .run(limits())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        result["table_operations"][2]["files"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(result["table_operations"][2]["files"][0]["rows"], 3000);
    let catalog = docspec::storage::local_catalog(&root.path().join("catalog"))
        .await
        .unwrap();
    let ident = TableIdent::new(NamespaceIdent::new("archive".into()), "captures".into());
    let table = catalog.load_table(&ident).await.unwrap();
    assert_eq!(
        table.metadata().format_version(),
        iceberg::spec::FormatVersion::V2
    );
    assert_eq!(table.metadata().properties()["gc.enabled"], "true");
    assert_eq!(
        table.metadata().properties()["commit.retry.num-retries"],
        "0"
    );
    assert_eq!(
        table.metadata().properties()["write.metadata.delete-after-commit.enabled"],
        "false"
    );

    let mut bad_gc = value.clone();
    bad_gc["table_operations"][1]["properties"] = json!({"gc.enabled": "false"});
    let err = serde_json::from_value::<JobSpec>(bad_gc)
        .unwrap()
        .run(limits())
        .await
        .unwrap_err();
    assert_eq!(err.error.code(), "invalid_spec");

    let mut bad_retry = value.clone();
    bad_retry["table_operations"][1]["properties"] = json!({"commit.retry.num-retries": "3"});
    let err = serde_json::from_value::<JobSpec>(bad_retry)
        .unwrap()
        .run(limits())
        .await
        .unwrap_err();
    assert_eq!(err.error.code(), "invalid_spec");

    let no_pk_schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap();
    let mut bad_pk = value;
    bad_pk["table_operations"][1]["schema"] = serde_json::to_value(no_pk_schema).unwrap();
    let err = serde_json::from_value::<JobSpec>(bad_pk)
        .unwrap()
        .run(limits())
        .await
        .unwrap_err();
    assert_eq!(err.error.code(), "invalid_spec");
}

/// One job holding only `operations` against the fixture's local catalog.
fn operations_job(root: &std::path::Path, scratch: &str, operations: Value) -> Value {
    json!({"scratch":root.join(scratch),"catalog_root":root.join("catalog"),"inputs":[],"steps":[],
        "outputs":[],"table_operations":operations})
}
fn add_columns(expected_schema_id: i32, columns: Value) -> Value {
    json!({"kind":"add_columns","name":"add","table":{"namespace":["archive"],"name":"captures"},
        "expected_schema_id":expected_schema_id,"columns":columns})
}
fn note_and_score() -> Value {
    json!([{"name":"note","type":"string","doc":"free text"},{"name":"score","type":"long"}])
}
async fn run_ok(value: Value) -> Value {
    serde_json::to_value(
        serde_json::from_value::<JobSpec>(value)
            .unwrap()
            .run(limits())
            .await
            .unwrap(),
    )
    .unwrap()
}
async fn run_code(value: Value) -> &'static str {
    let failure = serde_json::from_value::<JobSpec>(value)
        .unwrap()
        .run(limits())
        .await
        .unwrap_err();
    failure.error.code()
}
/// Rows of `sql` over the exact native pin a table operation reported.
async fn read_pin(root: &std::path::Path, scratch: &str, head: &Value, sql: &str) -> Value {
    let job = json!({"scratch":root.join(scratch),"inputs":[selected_input(head)],"steps":[],"outputs":[],
        "queries":[{"name":"rows","sql":sql}]});
    run_ok(job).await["queries"][0]["data"].clone()
}
async fn captures_table(root: &std::path::Path) -> iceberg::table::Table {
    docspec::storage::local_catalog(&root.join("catalog"))
        .await
        .unwrap()
        .load_table(&TableIdent::new(
            NamespaceIdent::new("archive".into()),
            "captures".into(),
        ))
        .await
        .unwrap()
}

#[tokio::test]
async fn add_columns_assigns_fresh_ids_then_appends_carry_the_new_columns() {
    let root = tempfile::tempdir().unwrap();
    let created = run_ok(spec(root.path())).await;
    let before = captures_table(root.path()).await;
    let previous = before.metadata().current_schema_id();
    let added = run_ok(operations_job(
        root.path(),
        "add-scratch",
        json!([add_columns(previous, note_and_score())]),
    ))
    .await;
    let report = &added["table_operations"][0];
    let after = captures_table(root.path()).await;
    let schema = after.metadata().current_schema();
    assert_eq!(report["kind"], "add_columns");
    assert_eq!(report["previous_schema_id"], previous);
    assert_eq!(report["schema_id"], schema.schema_id());
    assert_ne!(schema.schema_id(), previous);
    // Fresh ids continue from the table's own last column id; the caller supplies none.
    let last = before.metadata().last_column_id();
    assert_eq!(
        report["added_columns"],
        json!([{"name":"note","field_id":last + 1},{"name":"score","field_id":last + 2}])
    );
    assert_eq!(
        report["metadata_location"],
        after.metadata_location().unwrap()
    );
    assert_eq!(
        report["snapshot"],
        created["table_operations"][2]["snapshot"]
    );
    assert_eq!(report["files"], json!([]));
    let note = schema.field_by_name("note").unwrap();
    assert!(!note.required);
    assert_eq!(*note.field_type, Type::Primitive(PrimitiveType::String));
    assert_eq!(note.doc.as_deref(), Some("free text"));
    assert_eq!(
        schema
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "note", "score"]
    );

    // The engine's append rule is unchanged: writer and table carry the same names.
    let mut without = spec(root.path());
    without["steps"][0]["sql"] = json!("SELECT cast(3 AS BIGINT) AS id");
    without["table_operations"] = json!([without["table_operations"][2].clone()]);
    without["table_operations"][0]["identity_sql"] =
        json!("SELECT 'row-3-without-note' AS identity FROM pending LIMIT 1");
    assert_eq!(run_code(without.clone()).await, "schema_mismatch");

    let mut with = without;
    with["steps"][0]["sql"] =
        json!("SELECT cast(3 AS BIGINT) AS id,'third' AS note,cast(30 AS BIGINT) AS score");
    with["table_operations"][0]["identity_sql"] =
        json!("SELECT 'row-3-with-note' AS identity FROM pending LIMIT 1");
    let appended = run_ok(with).await;
    let head = &appended["table_operations"][0];
    assert_eq!(head["files"][0]["rows"], 1);
    // Results of other operation kinds keep their exact shape.
    assert!(head.get("previous_schema_id").is_none());
    assert!(head.get("added_columns").is_none());
    assert_eq!(
        read_pin(
            root.path(),
            "read-appended",
            head,
            "SELECT id,note,score FROM selected ORDER BY id"
        )
        .await,
        json!([{"id":1,"note":null,"score":null},{"id":2,"note":null,"score":null},
            {"id":3,"note":"third","score":30}])
    );
}

#[tokio::test]
async fn old_pins_keep_their_schema_while_old_rows_read_null_at_the_new_head() {
    let root = tempfile::tempdir().unwrap();
    let created = run_ok(spec(root.path())).await;
    let old = &created["table_operations"][2];
    let first = captures_table(root.path())
        .await
        .metadata()
        .current_schema_id();
    let added = run_ok(operations_job(
        root.path(),
        "add-scratch",
        json!([add_columns(first, note_and_score())]),
    ))
    .await;
    let new = &added["table_operations"][0];
    assert_ne!(new["metadata_location"], old["metadata_location"]);
    let all = "SELECT * FROM selected ORDER BY id";
    // The add writes no snapshot; its own head already reads the columns, as null.
    assert_eq!(
        read_pin(root.path(), "read-added", new, all).await,
        json!([{"id":1,"note":null,"score":null},{"id":2,"note":null,"score":null}])
    );
    assert_eq!(
        read_pin(root.path(), "read-old-before", old, all).await,
        json!([{"id":1},{"id":2}])
    );
    let mut append = spec(root.path());
    append["steps"][0]["sql"] =
        json!("SELECT cast(3 AS BIGINT) AS id,'third' AS note,cast(NULL AS BIGINT) AS score");
    append["table_operations"] = json!([append["table_operations"][2].clone()]);
    // The first append's current table carries the added column too.
    append["table_operations"][0]["sql"] = json!(
        "SELECT incoming.* FROM incoming LEFT ANTI JOIN existing ON incoming.id=existing.id AND existing.note IS NULL"
    );
    append["table_operations"][0]["identity_sql"] =
        json!("SELECT 'row-3-with-note' AS identity FROM pending LIMIT 1");
    let appended = run_ok(append).await;
    assert_eq!(
        read_pin(root.path(), "read-old", old, all).await,
        json!([{"id":1},{"id":2}])
    );
    assert_eq!(
        read_pin(
            root.path(),
            "read-head",
            &appended["table_operations"][0],
            all
        )
        .await,
        json!([{"id":1,"note":null,"score":null},{"id":2,"note":null,"score":null},
            {"id":3,"note":"third","score":null}])
    );
}

#[tokio::test]
async fn add_columns_refuses_required_conflicting_stale_nested_and_empty_requests() {
    let root = tempfile::tempdir().unwrap();
    run_ok(spec(root.path())).await;
    let first = captures_table(root.path())
        .await
        .metadata()
        .current_schema_id();
    // Neither a required flag, a caller field id nor a nested type can be expressed.
    let parses = |column: &Value| {
        let job = operations_job(
            root.path(),
            "parse",
            json!([add_columns(first, json!([column]))]),
        );
        serde_json::from_value::<JobSpec>(job).is_ok()
    };
    assert!(parses(
        &json!({"name":"note","type":"string","doc":"free text"})
    ));
    for column in [
        json!({"name":"note","type":"string","required":true}),
        json!({"name":"note","type":"string","id":7}),
        json!({"name":"note","type":{"type":"struct","fields":[]}}),
    ] {
        assert!(!parses(&column), "{column}");
    }
    for (columns, code) in [
        (json!([]), "invalid_spec"),
        (
            json!([{"name":"parent.child","type":"string"}]),
            "invalid_spec",
        ),
        (json!([{"name":"","type":"string"}]), "invalid_spec"),
        (
            json!([{"name":"note","type":"string"},{"name":"note","type":"string"}]),
            "invalid_spec",
        ),
        // `id` exists with another type, and as a required column of the same type.
        (json!([{"name":"id","type":"string"}]), "schema_mismatch"),
        (json!([{"name":"id","type":"long"}]), "schema_mismatch"),
        (
            json!([{"name":"note","type":"string"},{"name":"id","type":"int"}]),
            "schema_mismatch",
        ),
        // Nanosecond timestamps need format version 3; this table is version 2.
        (
            json!([{"name":"note","type":"string"},{"name":"at","type":"timestamp_ns"}]),
            "schema_mismatch",
        ),
        (
            json!([{"name":"at","type":"timestamptz_ns"}]),
            "schema_mismatch",
        ),
    ] {
        let before = captures_table(root.path()).await;
        let job = operations_job(
            root.path(),
            "refused",
            json!([add_columns(first, columns.clone())]),
        );
        assert_eq!(run_code(job).await, code, "{columns}");
        let after = captures_table(root.path()).await;
        assert_eq!(
            after.metadata_location(),
            before.metadata_location(),
            "{columns}"
        );
    }
    // Another writer moves the schema first; a caller still expecting the first refuses.
    run_ok(operations_job(
        root.path(),
        "other-writer",
        json!([add_columns(first, json!([{"name":"other","type":"int"}]))]),
    ))
    .await;
    let moved = captures_table(root.path()).await;
    assert_ne!(moved.metadata().current_schema_id(), first);
    let stale = operations_job(
        root.path(),
        "stale",
        json!([add_columns(first, note_and_score())]),
    );
    assert_eq!(run_code(stale).await, "stale_authority");
    let after = captures_table(root.path()).await;
    assert_eq!(after.metadata_location(), moved.metadata_location());
    assert!(
        after
            .metadata()
            .current_schema()
            .field_by_name("note")
            .is_none()
    );
}

#[tokio::test]
async fn repeating_add_columns_is_a_reported_noop() {
    let root = tempfile::tempdir().unwrap();
    run_ok(spec(root.path())).await;
    let first = captures_table(root.path())
        .await
        .metadata()
        .current_schema_id();
    let added = run_ok(operations_job(
        root.path(),
        "add",
        json!([add_columns(first, note_and_score())]),
    ))
    .await;
    let added = &added["table_operations"][0];
    let head = captures_table(root.path()).await;
    let current = head.metadata().current_schema_id();
    // The same request again (a retry after a lost response), and one naming the new schema.
    for (scratch, expected) in [("repeat", first), ("current", current)] {
        let repeat = run_ok(operations_job(
            root.path(),
            scratch,
            json!([add_columns(expected, note_and_score())]),
        ))
        .await;
        let report = &repeat["table_operations"][0];
        assert_eq!(report["kind"], "add_columns");
        assert_eq!(report["added_columns"], json!([]), "{scratch}");
        assert_eq!(report["previous_schema_id"], current);
        assert_eq!(report["schema_id"], current);
        assert_eq!(report["metadata_location"], added["metadata_location"]);
        assert_eq!(
            captures_table(root.path()).await.metadata_location(),
            head.metadata_location()
        );
    }
    // Present columns are skipped; only the missing one is added, at the current schema.
    let partial = run_ok(operations_job(
        root.path(),
        "partial",
        json!([add_columns(
            current,
            json!([{"name":"note","type":"string"},{"name":"extra","type":"date"}])
        )]),
    ))
    .await;
    assert_eq!(
        partial["table_operations"][0]["added_columns"],
        json!([{"name":"extra","field_id":head.metadata().last_column_id() + 1}])
    );
}

#[tokio::test]
async fn add_columns_takes_every_primitive_the_table_format_holds() {
    let root = tempfile::tempdir().unwrap();
    run_ok(spec(root.path())).await;
    let first = captures_table(root.path())
        .await
        .metadata()
        .current_schema_id();
    let v2 = [
        "boolean",
        "int",
        "long",
        "float",
        "double",
        "decimal(10,2)",
        "date",
        "time",
        "timestamp",
        "timestamptz",
        "string",
        "uuid",
        "fixed[4]",
        "binary",
    ];
    let columns = v2
        .iter()
        .enumerate()
        .map(|(index, kind)| json!({"name":format!("c{index}"),"type":kind}))
        .collect::<Vec<_>>();
    let added = run_ok(operations_job(
        root.path(),
        "v2",
        json!([add_columns(first, json!(columns))]),
    ))
    .await;
    assert_eq!(
        added["table_operations"][0]["added_columns"]
            .as_array()
            .unwrap()
            .len(),
        v2.len()
    );
    let schema = captures_table(root.path())
        .await
        .metadata()
        .current_schema()
        .clone();
    for (index, kind) in v2.iter().enumerate() {
        let field = schema.field_by_name(&format!("c{index}")).unwrap();
        let parsed: PrimitiveType = serde_json::from_value(json!(kind)).unwrap();
        assert_eq!(*field.field_type, Type::Primitive(parsed));
    }
    // A version 3 table takes the nanosecond timestamps a version 2 table refuses.
    let catalog = docspec::storage::local_catalog(&root.path().join("catalog"))
        .await
        .unwrap();
    let ident = TableIdent::new(NamespaceIdent::new("archive".into()), "events_v3".into());
    let v3 = catalog
        .create_table(
            ident.namespace(),
            iceberg::TableCreation::builder()
                .name(ident.name().into())
                .location(root.path().join("v3").to_string_lossy().into_owned())
                .schema(schema.as_ref().clone())
                .format_version(iceberg::spec::FormatVersion::V3)
                .build(),
        )
        .await
        .unwrap();
    let mut operation = add_columns(
        v3.metadata().current_schema_id(),
        json!([{"name":"at","type":"timestamp_ns"},{"name":"at_utc","type":"timestamptz_ns"}]),
    );
    operation["table"] = json!({"namespace":["archive"],"name":"events_v3"});
    let added = run_ok(operations_job(root.path(), "v3", json!([operation]))).await;
    assert_eq!(
        added["table_operations"][0]["added_columns"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn a_commit_racing_the_add_lands_and_the_add_refuses_before_its_commit() {
    use super::sql_tables::AuthorityCatalog;
    let root = tempfile::tempdir().unwrap();
    run_ok(spec(root.path())).await;
    let first = captures_table(root.path())
        .await
        .metadata()
        .current_schema_id();
    let catalog = docspec::storage::local_catalog(&root.path().join("catalog"))
        .await
        .unwrap();
    // Load 0 resolves the head and passes the schema check; another writer then
    // commits before load 1, the commit's own reload.
    let racing = Arc::new(AuthorityCatalog::racing(catalog, 1));
    let mut job = operations_job(
        root.path(),
        "raced",
        json!([add_columns(first, note_and_score())]),
    );
    job.as_object_mut().unwrap().remove("catalog_root");
    let failure = serde_json::from_value::<JobSpec>(job)
        .unwrap()
        .run_with_catalog(limits(), racing.clone())
        .await
        .unwrap_err();
    assert_eq!(failure.error.code(), "stale_authority", "{failure}");
    assert_eq!(racing.update_count(), 0);
    let schema = captures_table(root.path())
        .await
        .metadata()
        .current_schema()
        .clone();
    assert!(schema.field_by_name("racer").is_some());
    assert!(schema.field_by_name("note").is_none());
    assert!(schema.field_by_name("score").is_none());
}

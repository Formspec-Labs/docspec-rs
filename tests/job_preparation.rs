//! Whole-job schema preparation preserves membership without opening data sources.
// Rust guideline compliant 2026-02-21
use arrow::{
    datatypes::{DataType, Field, Schema},
    ipc::{convert::try_schema_from_ipc_buffer, writer::StreamWriter},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use docspec::{
    budget::Limits,
    job::{PrepareJobSpec, PreparedJob},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, path::Path, process::Command, sync::Arc};

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 128 << 20,
        scratch_bytes: 8 << 20,
        max_metadata_bytes: 1 << 20,
        max_batch_bytes: 1 << 20,
        batch_rows: 16,
    }
}

fn ipc(schema: &Schema) -> String {
    let mut bytes = Vec::new();
    drop(StreamWriter::try_new(&mut bytes, schema).unwrap());
    STANDARD.encode(bytes)
}

fn source_schema() -> Schema {
    Schema::new(vec![
        Field::new("event_id", DataType::Int64, false)
            .with_metadata(HashMap::from([("PARQUET:field_id".into(), "1".into())])),
        Field::new("source", DataType::Utf8View, false)
            .with_metadata(HashMap::from([("PARQUET:field_id".into(), "2".into())])),
    ])
}

fn request(root: &Path, spec: &Value) -> PrepareJobSpec {
    request_with(
        root,
        spec,
        &json!([{"name":"events","schema_ipc_base64":ipc(&source_schema())}]),
    )
}

fn request_with(root: &Path, spec: &Value, inputs: &Value) -> PrepareJobSpec {
    let path = root.join("job.json");
    let bytes = serde_json::to_vec_pretty(spec).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    serde_json::from_value(
        json!({"job":{"path":path,"sha256":format!("{:x}",Sha256::digest(&bytes))},"inputs":inputs}),
    )
    .unwrap()
}

fn specification(root: &Path) -> Value {
    let settings = json!({"profile":"whole","row_group_rows":128,
        "order_by":[{"field":"event_id","direction":"asc","nulls":"nulls_last"}],
        "columns":["source","event_id"],"dictionary_columns":[],"delta_columns":[],
        "key_column":null,"created_by":"authored-schema-example","footer_limit":65536});
    json!({"scratch":root.join("must-not-exist/scratch"),"inputs":[
        {"kind":"native_table","name":"events","metadata_location":"s3://unbound.invalid/raw/metadata.json",
         "table_uuid":"00000000-0000-4000-8000-000000000017","snapshot":17,"schema_id":0}],
        "steps":[{"name":"scored","materialize":true,"sql":"SELECT event_id,source,event_id < 12 AS accepted FROM events"},
          {"name":"routed","sql":"SELECT * FROM scored","release_after":["events"]}],
        "outputs":[
          {"name":"accepted.parquet","path":root.join("must-not-exist/accepted.parquet"),"sql":"SELECT * FROM routed WHERE accepted","settings":settings},
          {"name":"private.rejected.parquet","path":root.join("must-not-exist/rejected.parquet"),"sql":"SELECT * FROM routed WHERE NOT accepted","settings":settings}],
        "queries":[{"name":"partition_invariants","sql":"SELECT count(*) AS routed_rows,CAST(0 AS INT) AS authored_zero FROM routed"}]})
}

fn inferred(prepared: &PreparedJob, digest: &str) -> Schema {
    let bytes = STANDARD.decode(&prepared.schemas[digest]).unwrap();
    assert_eq!(format!("{:x}", Sha256::digest(&bytes)), digest);
    try_schema_from_ipc_buffer(&bytes).unwrap()
}

#[tokio::test]
async fn multiple_scalar_counter_schemas_match_optimized_native_report_ipc_without_input_reads() {
    use arrow::{
        array::{Int64Array, StringViewArray},
        record_batch::RecordBatch,
    };
    use futures::TryStreamExt;
    use parquet::arrow::ArrowWriter;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("events.parquet");
    let sql = "SELECT (SELECT COUNT(*) FROM events WHERE event_id < 0) AS negative_ids, (SELECT COUNT(*) FROM events WHERE source IS NULL) AS missing_sources, (SELECT COUNT(*) FROM events WHERE event_id > 2) AS high_ids";
    let union_sql = "SELECT arrow_cast(source, 'Utf8') AS label FROM events UNION ALL SELECT arrow_cast('x', 'LargeUtf8') AS label";
    let spec = json!({"scratch":root.path().join("must-not-exist"),"inputs":[{"kind":"parquet","name":"events","path":path}],"steps":[],"outputs":[],"queries":[{"name":"counters","sql":sql},{"name":"union_coercion","sql":union_sql}]});
    let prepared = request(root.path(), &spec).prepare(limits()).await.unwrap();
    assert!(!path.exists());
    assert!(!root.path().join("must-not-exist").exists());
    let schema = Arc::new(source_schema());
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema.clone(), None).unwrap();
    writer
        .write(
            &RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(StringViewArray::from(vec!["a", "b"])),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    writer.close().unwrap();
    let job = docspec::job::Job::new(limits(), root.path().join("ordinary")).unwrap();
    job.register_parquet("events", &path).await.unwrap();
    let frame = job.sql(sql).await.unwrap();
    let unoptimized = frame.schema().as_arrow().clone();
    let (state, plan) = frame.clone().into_parts();
    let optimized = state.optimize(&plan).unwrap().schema().as_arrow().clone();
    let mut stream = frame.execute_stream().await.unwrap();
    let actual = stream.schema();
    eprintln!(
        "native_scalar_counter_schema_probe {}",
        json!({"unoptimized":unoptimized,"optimized":optimized,"actual":actual})
    );
    let expected = Schema::new(vec![
        Field::new("negative_ids", DataType::Int64, false),
        Field::new("missing_sources", DataType::Int64, false),
        Field::new("high_ids", DataType::Int64, false),
    ]);
    assert_eq!(actual.as_ref(), &expected);
    assert_eq!(optimized, expected);
    assert_eq!(unoptimized, expected);
    assert_eq!(
        inferred(&prepared, &prepared.queries[0].schema_sha256),
        expected
    );
    // DataFusion 55 still types DataFrame::schema() on UNION ALL from the first
    // branch while optimizing/executing the widened LargeUtf8 branch type;
    // prepare() must record the optimized executed schema rather than pre-optimization.
    let union_frame = job.sql(union_sql).await.unwrap();
    let union_unoptimized = union_frame.schema().as_arrow().clone();
    let (union_state, union_plan) = union_frame.clone().into_parts();
    let union_optimized = union_state
        .optimize(&union_plan)
        .unwrap()
        .schema()
        .as_arrow()
        .clone();
    let union_actual = union_frame.execute_stream().await.unwrap().schema();
    let union_expected = Schema::new(vec![Field::new("label", DataType::LargeUtf8, false)]);
    assert_ne!(
        union_unoptimized, union_expected,
        "the independent fixture must distinguish pre-optimization inference"
    );
    assert_eq!(union_actual.as_ref(), &union_expected);
    assert_eq!(union_optimized, union_expected);
    assert_eq!(
        inferred(&prepared, &prepared.queries[1].schema_sha256),
        union_expected
    );
    let batch = stream.try_next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    for column in batch.columns() {
        assert_eq!(
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
    }
    assert!(stream.try_next().await.unwrap().is_none());
    let mut materialized = spec.clone();
    materialized["steps"] = json!([{"name":"counter_material","sql":sql,"materialize":true}]);
    materialized["outputs"] = json!([{"name":"counter_file","path":root.path().join("counter.parquet"),"sql":"SELECT * FROM counter_material","settings":{"profile":"whole","row_group_rows":128,"order_by":[{"field":"negative_ids"}],"columns":[],"dictionary_columns":[],"delta_columns":[],"key_column":null,"created_by":"independent-scalar-counter-parity","footer_limit":65536}}]);
    materialized["queries"] =
        json!([{"name":"counters","sql":"SELECT * FROM outputs.\"counter_file\""}]);
    let parked = root.path().join("parked.parquet");
    std::fs::rename(&path, &parked).unwrap();
    let description = request(root.path(), &materialized)
        .prepare(limits())
        .await
        .unwrap();
    assert_eq!(
        inferred(&description, &description.steps[0].schema_sha256),
        expected
    );
    assert_eq!(
        inferred(&description, &description.outputs[0].schema_sha256),
        expected
    );
    assert_eq!(
        inferred(&description, &description.queries[0].schema_sha256),
        expected
    );
    std::fs::rename(parked, &path).unwrap();
    let ordinary: docspec::job::JobSpec = serde_json::from_value(materialized).unwrap();
    let result = ordinary.run(limits()).await.unwrap();
    assert_eq!(
        result.queries[0].data,
        json!([{"negative_ids":0,"missing_sources":0,"high_ids":0}])
    );
    let stored = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(root.path().join("counter.parquet")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        stored.schema().as_ref(),
        &expected,
        "ordinary materialization/export preserve the same native nullable facts"
    );
    let standalone:docspec::job::JobPlanSpec = serde_json::from_value(json!({"scratch":root.path().join("standalone"),"inputs":[{"name":"events","schema":source_schema()}],"calculation":{"format":"sql","sql":sql}})).unwrap();
    let standalone = serde_json::to_value(standalone.prepare(limits()).await.unwrap()).unwrap();
    assert_eq!(
        serde_json::from_value::<Schema>(standalone["schema"].clone()).unwrap(),
        expected
    );
}

#[tokio::test]
async fn output_namespace_reports_use_projected_export_schema_without_payload_reads() {
    let root = tempfile::tempdir().unwrap();
    let mut spec = specification(root.path());
    spec["queries"] = json!([{"name":"export_counter","sql":"SELECT COUNT(*) AS exported_rows FROM outputs.\"private.rejected.parquet\""}]);
    let prepared = request(root.path(), &spec).prepare(limits()).await.unwrap();
    assert_eq!(
        inferred(&prepared, &prepared.queries[0].schema_sha256)
            .field(0)
            .data_type(),
        &DataType::Int64
    );
    assert!(!root.path().join("must-not-exist").exists());
    spec["queries"][0]["sql"] = json!("SELECT accepted FROM outputs.\"private.rejected.parquet\"");
    assert!(
        request(root.path(), &spec).prepare(limits()).await.is_err(),
        "reports must not see a field omitted by the actual writer projection"
    );
    assert!(!root.path().join("must-not-exist").exists());
}

#[tokio::test]
async fn all_named_exports_and_reports_are_inferred_with_no_payload_or_scratch() {
    let root = tempfile::tempdir().unwrap();
    let spec = specification(root.path());
    let prepared = request(root.path(), &spec).prepare(limits()).await.unwrap();
    assert_eq!(prepared.status, "inferred");
    assert_eq!(prepared.input_admission, "not_run");
    assert_eq!(prepared.inputs[0].declared_native_schema_id, None);
    assert_eq!(prepared.execution, "not_run");
    assert_eq!(
        prepared
            .outputs
            .iter()
            .map(|r| r.name.as_str())
            .collect::<Vec<_>>(),
        ["accepted.parquet", "private.rejected.parquet"]
    );
    assert_eq!(
        prepared
            .steps
            .iter()
            .map(|r| r.name.as_str())
            .collect::<Vec<_>>(),
        ["scored", "routed"]
    );
    let expected = Schema::new(vec![
        Field::new("source", DataType::Utf8, false)
            .with_metadata(HashMap::from([("PARQUET:field_id".into(), "2".into())])),
        Field::new("event_id", DataType::Int64, false)
            .with_metadata(HashMap::from([("PARQUET:field_id".into(), "1".into())])),
    ]);
    for output in &prepared.outputs {
        assert_eq!(inferred(&prepared, &output.schema_sha256), expected);
    }
    assert_eq!(
        inferred(&prepared, &prepared.steps[0].schema_sha256)
            .field_with_name("source")
            .unwrap()
            .data_type(),
        &DataType::Utf8
    );
    assert_eq!(prepared.queries[0].name, "partition_invariants");
    assert_eq!(
        inferred(&prepared, &prepared.queries[0].schema_sha256),
        Schema::new(vec![
            Field::new("routed_rows", DataType::Int64, false),
            Field::new("authored_zero", DataType::Int32, false)
        ])
    );
    assert_eq!(
        inferred(&prepared, &prepared.inputs[0].schema_sha256),
        source_schema()
    );
    assert_eq!(
        prepared.outputs[0].schema_sha256,
        prepared.outputs[1].schema_sha256
    );
    assert!(!root.path().join("must-not-exist").exists());
}

#[tokio::test]
async fn declarations_and_native_name_lifetime_query_and_export_guards_refuse() {
    let root = tempfile::tempdir().unwrap();
    let spec = specification(root.path());
    for variant in [
        "missing_schema",
        "duplicate_schema",
        "wrong_case",
        "duplicate_step",
        "duplicate_output",
        "duplicate_query",
        "missing_relation",
        "released_input",
        "missing_export_column",
        "volatile_report",
        "invalid_graph",
    ] {
        let mut changed = spec.clone();
        match variant {
            "duplicate_step" => changed["steps"][1]["name"] = json!("SCORED"),
            "duplicate_output" => changed["outputs"][1]["name"] = json!("accepted.parquet"),
            "duplicate_query" => {
                let q = changed["queries"][0].clone();
                changed["queries"].as_array_mut().unwrap().push(q);
            }
            "missing_relation" => {
                changed["outputs"][1]["sql"] = json!("SELECT * FROM absent_private_input")
            }
            "released_input" => changed["queries"][0]["sql"] = json!("SELECT * FROM events"),
            "missing_export_column" => {
                changed["outputs"][1]["settings"]["columns"] = json!(["absent"])
            }
            "volatile_report" => changed["queries"][0]["sql"] = json!("SELECT random() AS unbound"),
            "invalid_graph" => {
                changed["steps"][0] = json!({"name":"scored","components":{"sql":"SELECT event_id,source FROM events","left":"event_id","right":"source","node_limit":3}})
            }
            _ => (),
        }
        let mut req = request(root.path(), &changed);
        match variant {
            "missing_schema" => req.inputs.clear(),
            "duplicate_schema" => req.inputs.push(req.inputs[0].clone()),
            "wrong_case" => req.inputs[0].name = "EVENTS".into(),
            _ => (),
        }
        assert!(req.prepare(limits()).await.is_err(), "{variant} passed");
        assert!(
            !root.path().join("must-not-exist").exists(),
            "{variant} touched job destinations"
        );
    }
}

#[tokio::test]
async fn graph_schema_preserves_fixed_binary_and_recursive_metadata_without_graph_rows() {
    let root = tempfile::tempdir().unwrap();
    let width = DataType::FixedSizeBinary(32);
    let nested = Field::new(
        "occurrences",
        DataType::List(Arc::new(Field::new(
            "element",
            DataType::Struct(
                vec![
                    Field::new("token", width.clone(), true)
                        .with_metadata(HashMap::from([("PARQUET:field_id".into(), "9".into())])),
                ]
                .into(),
            ),
            true,
        ))),
        true,
    );
    let schema = Schema::new_with_metadata(
        vec![
            Field::new("left", width.clone(), false),
            Field::new("right", width.clone(), false),
            nested,
        ],
        HashMap::from([("authored".into(), "nested-declaration".into())]),
    );
    let spec = json!({"scratch":root.path().join("unopened"),"inputs":[{"kind":"parquet","name":"events","path":root.path().join("missing.parquet")}],
        "steps":[{"name":"groups","components":{"sql":"SELECT left,right FROM events","left":"left","right":"right","node_limit":10}},
          {"name":"nested_view","sql":"SELECT occurrences FROM events"}],"outputs":[],"queries":[{"name":"group_types","sql":"SELECT node,component FROM groups"}]});
    let mut req = request(root.path(), &spec);
    req.inputs[0].schema_ipc_base64 = Some(ipc(&schema));
    let prepared = req.prepare(limits()).await.unwrap();
    assert_eq!(
        inferred(&prepared, &prepared.inputs[0].schema_sha256),
        schema
    );
    let expected = Schema::new(vec![
        Field::new("node", width.clone(), false),
        Field::new("component", width, false),
    ]);
    assert_eq!(
        inferred(&prepared, &prepared.steps[0].schema_sha256),
        expected
    );
    assert_eq!(
        inferred(&prepared, &prepared.queries[0].schema_sha256),
        expected
    );
    assert_eq!(
        inferred(&prepared, &prepared.steps[1].schema_sha256).field(0),
        schema.field(2)
    );
    assert!(!root.path().join("unopened").exists());
}

#[tokio::test]
async fn mutated_private_members_effectful_sql_table_operations_and_oversized_metadata_refuse() {
    let root = tempfile::tempdir().unwrap();
    let spec = specification(root.path());
    let req = request(root.path(), &spec);
    let mut missing_private = spec.clone();
    missing_private["outputs"].as_array_mut().unwrap().pop();
    std::fs::write(&req.job.path, serde_json::to_vec(&missing_private).unwrap()).unwrap();
    assert_eq!(
        req.prepare(limits()).await.unwrap_err().code(),
        "job_digest"
    );
    for site in ["step", "graph", "output", "query", "literal"] {
        let mut changed = spec.clone();
        let effect = format!(
            "COPY (SELECT 1 AS id) TO '{}' STORED AS PARQUET",
            root.path().join("forbidden.parquet").display()
        );
        match site {
            "step" => changed["steps"][0]["sql"] = json!(effect),
            "graph" => {
                changed["steps"][0] = json!({"name":"scored","components":{"sql":effect,"left":"a","right":"b","node_limit":1}})
            }
            "output" => changed["outputs"][0]["sql"] = json!(effect),
            "query" => changed["queries"][0]["sql"] = json!(effect),
            "literal" => {
                changed["queries"][0]["sql"] = json!("CREATE TABLE injected AS SELECT 1 AS id")
            }
            _ => unreachable!(),
        }
        assert!(
            request(root.path(), &changed)
                .prepare(limits())
                .await
                .is_err(),
            "{site}"
        );
        assert!(!root.path().join("forbidden.parquet").exists());
    }
    let mut changed = spec.clone();
    changed["table_operations"] =
        json!([{"kind":"create_namespace","name":"effect","namespace":["private"]}]);
    assert_eq!(
        request(root.path(), &changed)
            .prepare(limits())
            .await
            .unwrap_err()
            .code(),
        "unsupported_preparation"
    );
    let mut small = limits();
    small.max_metadata_bytes = 64;
    assert_eq!(
        request(root.path(), &spec)
            .prepare(small)
            .await
            .unwrap_err()
            .code(),
        "metadata_capacity"
    );
    let mut req = request(root.path(), &spec);
    req.inputs[0].schema_ipc_base64 = Some("a".repeat((1 << 20) + 1));
    assert_eq!(
        req.prepare(limits()).await.unwrap_err().code(),
        "metadata_capacity"
    );
}

#[test]
fn cli_returns_complete_schema_inventory_without_initializing_a_workspace() {
    let root = tempfile::tempdir().unwrap();
    let req = request(root.path(), &specification(root.path()));
    let request_path = root.path().join("prepare.json");
    std::fs::write(&request_path, serde_json::to_vec(&req).unwrap()).unwrap();
    let limits_path = root.path().join("limits.json");
    std::fs::write(&limits_path, serde_json::to_vec(&limits()).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .args(["--workspace"])
        .arg(root.path().join("unused-workspace"))
        .arg("--limits")
        .arg(limits_path)
        .args(["prepare-job", "--spec"])
        .arg(request_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["status"], "inferred");
    assert_eq!(result["outputs"].as_array().unwrap().len(), 2);
    assert_eq!(result["outputs"][1]["name"], "private.rejected.parquet");
    assert_eq!(result["queries"][0]["name"], "partition_invariants");
    assert!(!root.path().join("unused-workspace").exists());
    assert!(!root.path().join("must-not-exist").exists());
}

#[test]
fn cli_ordinary_job_exports_to_a_bare_relative_path() {
    let root = tempfile::tempdir().unwrap();
    let spec = json!({"scratch":root.path().join("scratch"),"inputs":[],"steps":[],
        "outputs":[{"name":"values","path":"out.parquet","sql":"SELECT CAST(7 AS BIGINT) AS id","settings":{
            "profile":"whole","row_group_rows":128,"order_by":[{"field":"id"}],"columns":[],"dictionary_columns":[],"delta_columns":[],"key_column":null,"created_by":"relative-output-example","footer_limit":65536
        }}],"queries":[{"name":"observed","sql":"SELECT id FROM outputs.\"values\""}]});
    let path = root.path().join("job.json");
    std::fs::write(&path, serde_json::to_vec(&spec).unwrap()).unwrap();
    let limits_path = root.path().join("limits.json");
    std::fs::write(&limits_path, serde_json::to_vec(&limits()).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .current_dir(root.path())
        .args(["--workspace"])
        .arg(root.path().join("unused-workspace"))
        .arg("--limits")
        .arg(limits_path)
        .args(["job", "--spec"])
        .arg(path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outputs"][0]["rows"], 1);
    assert_eq!(result["queries"][0]["data"], json!([{"id":7}]));
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let reader =
        SerializedFileReader::new(std::fs::File::open(root.path().join("out.parquet")).unwrap())
            .unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 1);
}

#[tokio::test]
async fn generated_input_schema_must_match_its_native_declaration_and_names_bind_without_order() {
    let root = tempfile::tempdir().unwrap();
    let generated = Schema::new(vec![Field::new("allowed", DataType::Boolean, true)]);
    let mut spec = specification(root.path());
    spec["inputs"].as_array_mut().unwrap().push(json!({
        "kind":"json_lines", "name":"generated_policy", "files":[{"key":"authored-v1", "path":root.path().join("unopened-policy.jsonl")}], "schema":generated
    }));
    let mut req = request(root.path(), &spec);
    req.inputs.insert(
        0,
        docspec::job::DeclaredInputSchema {
            name: "generated_policy".into(),
            schema_ipc_base64: Some(ipc(&generated)),
            schema_artifact: None,
        },
    );
    let prepared = req.clone().prepare(limits()).await.unwrap();
    assert_eq!(
        prepared
            .inputs
            .iter()
            .map(|r| r.name.as_str())
            .collect::<Vec<_>>(),
        ["events", "generated_policy"]
    );
    assert_eq!(
        inferred(&prepared, &prepared.inputs[1].declared_schema_sha256),
        generated
    );
    req.inputs[0].schema_ipc_base64 = Some(ipc(&Schema::new(vec![Field::new(
        "allowed",
        DataType::Utf8,
        true,
    )])));
    assert_eq!(
        req.prepare(limits()).await.unwrap_err().code(),
        "invalid_schema"
    );
    assert!(!root.path().join("unopened-policy.jsonl").exists());
    assert!(!root.path().join("must-not-exist").exists());
}

#[tokio::test]
async fn json_lines_source_columns_and_declaration_guards_match_native_provider() {
    let root = tempfile::tempdir().unwrap();
    let schema = Schema::new(vec![
        Field::new("event_id", DataType::Int64, true),
        Field::new("source", DataType::Utf8, true),
    ]);
    let mut spec = specification(root.path());
    spec["inputs"][0] = json!({"name":"events","kind":"json_lines","files":[{"key":"authored-lines","path":root.path().join("never-opened.jsonl")}],"schema":schema});
    spec["steps"][0]["sql"] = json!(
        "SELECT *,json_get_json(__raw_line,'value') AS value_json,event_id < 12 AS accepted FROM events"
    );
    spec["queries"][0]["sql"] =
        json!("SELECT __source_key,__source_line,__raw_line,value_json FROM routed");
    let mut req = request(root.path(), &spec);
    req.inputs[0].schema_ipc_base64 = Some(ipc(&schema));
    let prepared = req.prepare(limits()).await.unwrap();
    assert_eq!(
        inferred(&prepared, &prepared.inputs[0].declared_schema_sha256),
        schema
    );
    let expected = Schema::new(vec![
        Field::new("event_id", DataType::Int64, true),
        Field::new("source", DataType::Utf8, true),
        Field::new("__source_key", DataType::Utf8, false),
        Field::new("__source_line", DataType::Int64, false),
        Field::new("__raw_line", DataType::Utf8, false),
    ]);
    assert_eq!(
        inferred(&prepared, &prepared.inputs[0].schema_sha256),
        expected
    );
    let ordinary = docspec::job::Job::new(limits(), root.path().join("ordinary-schema")).unwrap();
    ordinary
        .register_json_lines(
            "events",
            vec![docspec::job::JsonLinesFile {
                key: "authored-lines".into(),
                path: root.path().join("never-opened.jsonl"),
            }],
            Arc::new(schema.clone()),
        )
        .unwrap();
    let ordinary_frame = ordinary.sql("SELECT * FROM events").await.unwrap();
    assert_eq!(ordinary_frame.schema().as_arrow(), &expected);
    let report = inferred(&prepared, &prepared.queries[0].schema_sha256);
    assert_eq!(&report.fields()[..3], &expected.fields()[2..]);
    assert_eq!(report.field(3).name(), "value_json");
    for bad_schema in [
        Schema::new(vec![Field::new("required", DataType::Int64, false)]),
        Schema::new(vec![Field::new("__reserved", DataType::Utf8, true)]),
        Schema::new(vec![Field::new(
            "nested",
            DataType::Struct(vec![Field::new("required", DataType::Int64, false)].into()),
            true,
        )]),
    ] {
        let mut changed = spec.clone();
        changed["inputs"][0]["schema"] = json!(bad_schema);
        let mut req = request(root.path(), &changed);
        req.inputs[0].schema_ipc_base64 = Some(ipc(&bad_schema));
        let error = req.prepare(limits()).await.unwrap_err();
        assert!(
            error.to_string().contains("JSON fields must be nullable"),
            "{error}"
        );
    }
    assert!(!root.path().join("never-opened.jsonl").exists());
    assert!(!root.path().join("must-not-exist").exists());
}

#[tokio::test]
async fn exact_native_schema_artifacts_keep_declaration_identity_and_refuse_conflicts() {
    let root = tempfile::tempdir().unwrap();
    let spec = specification(root.path());
    let mut req = request(root.path(), &spec);
    let path = root.path().join("native-schema.json");
    let bytes = serde_json::to_vec_pretty(&source_schema()).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    let binding = docspec::job::SchemaArtifact {
        path: path.clone(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        format: docspec::job::SchemaArtifactFormat::ArrowSerdeJson,
    };
    req.inputs[0].schema_ipc_base64 = None;
    req.inputs[0].schema_artifact = Some(binding.clone());
    let prepared = req.clone().prepare(limits()).await.unwrap();
    assert_eq!(
        prepared.inputs[0].declared_schema_format,
        "arrow_serde_json"
    );
    assert_eq!(prepared.inputs[0].declared_schema_size, bytes.len());
    assert_eq!(prepared.inputs[0].declared_schema_sha256, binding.sha256);
    assert_eq!(
        prepared.inputs[0]
            .declared_schema_artifact
            .as_ref()
            .unwrap()
            .path,
        path
    );
    assert_eq!(
        inferred(&prepared, &prepared.inputs[0].schema_sha256),
        source_schema()
    );
    let mut both = req.clone();
    both.inputs[0].schema_ipc_base64 = Some(ipc(&source_schema()));
    assert_eq!(
        both.prepare(limits()).await.unwrap_err().code(),
        "invalid_schema"
    );
    let mut neither = req.clone();
    neither.inputs[0].schema_artifact = None;
    assert_eq!(
        neither.prepare(limits()).await.unwrap_err().code(),
        "invalid_schema"
    );
    let mut changed = req.clone();
    changed.inputs[0].schema_artifact.as_mut().unwrap().sha256 = "0".repeat(64);
    assert_eq!(
        changed.prepare(limits()).await.unwrap_err().code(),
        "schema_digest"
    );
    std::fs::write(&path, b"not schema JSON").unwrap();
    assert_eq!(
        req.clone().prepare(limits()).await.unwrap_err().code(),
        "schema_digest"
    );
    std::fs::File::create(&path)
        .unwrap()
        .set_len((1 << 20) + 1)
        .unwrap();
    assert_eq!(
        req.prepare(limits()).await.unwrap_err().code(),
        "metadata_capacity"
    );
    assert!(!root.path().join("must-not-exist").exists());
}

#[tokio::test]
async fn unbound_parameters_refuse_at_every_nonparameterized_native_sql_site() {
    let root = tempfile::tempdir().unwrap();
    let spec = specification(root.path());
    for site in ["view", "output", "report", "components"] {
        let mut changed = spec.clone();
        match site {
            "view" => {
                changed["steps"][0]["materialize"] = json!(false);
                changed["steps"][0]["sql"] =
                    json!("SELECT event_id,source,event_id < $missing AS accepted FROM events");
            }
            "output" => {
                changed["outputs"][1]["sql"] =
                    json!("SELECT event_id,source,CAST($missing AS BIGINT) AS extra FROM routed")
            }
            "report" => {
                changed["queries"][0]["sql"] = json!("SELECT CAST($missing AS BIGINT) AS count")
            }
            "components" => {
                changed["steps"][0] = json!({"name":"scored","components":{"sql":"SELECT CAST($missing AS VARCHAR) AS a,source AS b FROM events","left":"a","right":"b","node_limit":2}})
            }
            _ => unreachable!(),
        }
        assert_eq!(
            request(root.path(), &changed)
                .prepare(limits())
                .await
                .unwrap_err()
                .code(),
            "invalid_parameters",
            "{site}"
        );
    }
    let mut bound = spec.clone();
    bound["steps"][0]["materialize"] = json!(false);
    bound["steps"][0]["sql"] =
        json!("SELECT event_id,source,event_id < $cutoff AS accepted FROM events");
    bound["steps"][0]["parameters"] = json!({"cutoff":12});
    request(root.path(), &bound)
        .prepare(limits())
        .await
        .unwrap();
    let ordinary =
        docspec::job::Job::new(limits(), root.path().join("ordinary-parameters")).unwrap();
    assert_eq!(
        ordinary
            .sql("SELECT CAST($missing AS BIGINT) AS count")
            .await
            .unwrap_err()
            .code(),
        "invalid_parameters"
    );
    assert!(!root.path().join("must-not-exist").exists());
}

#[tokio::test]
async fn native_iceberg_schema_declarations_use_maintained_ids_and_explicit_format() {
    let root = tempfile::tempdir().unwrap();
    let mut req = request(root.path(), &specification(root.path()));
    let schema = json!({"type":"struct","schema-id":0,"fields":[
        {"id":1,"name":"event_id","required":true,"type":"long"},
        {"id":2,"name":"source","required":true,"type":"string"},
        {"id":7,"name":"occurrences","required":false,"type":{"type":"list","element-id":8,"element-required":false,"element":{"type":"struct","fields":[{"id":9,"name":"token","required":false,"type":"fixed[32]"}]}}}
    ]});
    let bytes = serde_json::to_vec_pretty(&schema).unwrap();
    let path = root.path().join("iceberg-schema.json");
    std::fs::write(&path, &bytes).unwrap();
    req.inputs[0].schema_ipc_base64 = None;
    req.inputs[0].schema_artifact = Some(docspec::job::SchemaArtifact {
        path,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        format: docspec::job::SchemaArtifactFormat::IcebergSchemaJson,
    });
    let prepared = req.clone().prepare(limits()).await.unwrap();
    let inferred = inferred(&prepared, &prepared.inputs[0].schema_sha256);
    assert_eq!(
        prepared.inputs[0].declared_schema_format,
        "iceberg_schema_json"
    );
    assert_eq!(
        prepared.inputs[0].declared_schema_sha256,
        format!("{:x}", Sha256::digest(&bytes))
    );
    assert_eq!(prepared.inputs[0].declared_native_schema_id, Some(0));
    assert_eq!(prepared.input_admission, "not_run");
    assert_eq!(inferred.field(0).data_type(), &DataType::Int64);
    assert_eq!(
        inferred
            .field(0)
            .metadata()
            .get("PARQUET:field_id")
            .map(String::as_str),
        Some("1")
    );
    let DataType::List(element) = inferred.field(2).data_type() else {
        panic!("expected list")
    };
    assert_eq!(
        element
            .metadata()
            .get("PARQUET:field_id")
            .map(String::as_str),
        Some("8")
    );
    let DataType::Struct(fields) = element.data_type() else {
        panic!("expected struct")
    };
    assert_eq!(fields[0].data_type(), &DataType::FixedSizeBinary(32));
    assert_eq!(
        fields[0]
            .metadata()
            .get("PARQUET:field_id")
            .map(String::as_str),
        Some("9")
    );
    let mut crossed = req.clone();
    crossed.inputs[0].schema_artifact.as_mut().unwrap().format =
        docspec::job::SchemaArtifactFormat::ArrowSerdeJson;
    assert!(crossed.prepare(limits()).await.is_err());
    let mut unknown = serde_json::to_value(&req).unwrap();
    unknown["inputs"][0]["schema_artifact"]["format"] = json!("unsupported");
    assert!(serde_json::from_value::<PrepareJobSpec>(unknown).is_err());
    let pinned_root = root.path().join("pinned");
    std::fs::create_dir(&pinned_root).unwrap();
    let mut pinned_job = specification(&pinned_root);
    pinned_job["inputs"][0] = json!({"kind":"iceberg","name":"events","pin":{"table":"authored.events","metadata":{"owner":"authored","path":"unopened/metadata.json","digest":"a".repeat(64),"bytes":1},"snapshot":17,"schema_id":0,"objects":[],"rows":0}});
    let mut exact_pin = request(&pinned_root, &pinned_job);
    exact_pin.inputs[0].schema_ipc_base64 = None;
    exact_pin.inputs[0].schema_artifact = req.inputs[0].schema_artifact.clone();
    let matched = exact_pin.clone().prepare(limits()).await.unwrap();
    assert_eq!(matched.inputs[0].declared_native_schema_id, Some(0));
    assert_eq!(matched.input_admission, "not_run");
    // Only the artifact's native schema ID changes; fields/types remain compatible.
    let mut wrong_schema = schema.clone();
    wrong_schema["schema-id"] = json!(1);
    let wrong_bytes = serde_json::to_vec_pretty(&wrong_schema).unwrap();
    let wrong_path = root.path().join("iceberg-schema-wrong-id.json");
    std::fs::write(&wrong_path, &wrong_bytes).unwrap();
    for mut wrong in [req, exact_pin] {
        let binding = wrong.inputs[0].schema_artifact.as_mut().unwrap();
        binding.path = wrong_path.clone();
        binding.sha256 = format!("{:x}", Sha256::digest(&wrong_bytes));
        let error = wrong.prepare(limits()).await.unwrap_err();
        assert_eq!(error.code(), "invalid_schema");
        assert!(error.to_string().contains("schema ID differs"), "{error}");
    }
    assert!(!root.path().join("must-not-exist").exists());
}

/// The SpicyRegs data-map SQL trial, copied unchanged from spicy-stack `b56c42a`
/// (`datamap/experiments/sql-check`): its JobSpec of six SQL steps, declared
/// inputs, and the two faults DataFusion coerced into silently empty results.
fn trial() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/datamap_sql_trial")
}

fn trial_input(name: &str) -> Value {
    let path = trial().join(format!("inputs/{name}.arrow.json"));
    let sha256 = format!("{:x}", Sha256::digest(std::fs::read(&path).unwrap()));
    json!({"name":name,"schema_artifact":{"path":path,"sha256":sha256,"format":"arrow_serde_json"}})
}

async fn trial_refusal(root: &Path, step: &str, fault: &str) -> String {
    let mut spec: Value =
        serde_json::from_slice(&std::fs::read(trial().join("job.json")).unwrap()).unwrap();
    spec["scratch"] = json!(root.join("must-not-exist/scratch"));
    let inputs = spec["inputs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|input| trial_input(input["name"].as_str().unwrap()))
        .collect();
    if !fault.is_empty() {
        let steps = spec["steps"].as_array_mut().unwrap();
        let step = steps.iter_mut().find(|candidate| candidate["name"] == step);
        step.unwrap()["sql"] = json!(std::fs::read_to_string(trial().join(fault)).unwrap());
    }
    match request_with(root, &spec, &Value::Array(inputs))
        .prepare(limits())
        .await
    {
        Ok(prepared) => {
            assert_eq!(
                (
                    prepared.steps.len(),
                    prepared.outputs.len(),
                    prepared.queries.len()
                ),
                (6, 3, 7)
            );
            String::new()
        }
        Err(error) => {
            assert_eq!(error.code(), "implicit_cast", "{error}");
            error.to_string()
        }
    }
}

#[tokio::test]
async fn implicit_casts_between_type_families_refuse_and_the_trial_baseline_stays_clean() {
    let root = tempfile::tempdir().unwrap();
    assert_eq!(trial_refusal(root.path(), "", "").await, "");
    let f3 = trial_refusal(
        root.path(),
        "member_voting_record",
        "F3-string-integer-join.sql",
    )
    .await;
    assert!(f3.contains("v.roll_call_id (Utf8 to Int64)"), "{f3}");
    let f3b = trial_refusal(root.path(), "rule_history", "F3b-date-integer-compare.sql").await;
    assert!(f3b.contains("Int64(20200101) (Int64 to Date32)"), "{f3b}");

    let inputs = ["member_votes", "roll_calls"].map(|name| {
        let schema = std::fs::read(trial().join(format!("inputs/{name}.arrow.json"))).unwrap();
        json!({"name":name,"schema":serde_json::from_slice::<Value>(&schema).unwrap()})
    });
    let plan = |sql: &str| {
        serde_json::from_value::<docspec::job::JobPlanSpec>(json!({
            "scratch":root.path().join("job-plan"),"inputs":inputs,
            "calculation":{"format":"sql","sql":sql,"parameters":{"1":"P000197"}}}))
        .unwrap()
        .prepare(limits())
    };
    let fault = std::fs::read_to_string(trial().join("F3-string-integer-join.sql")).unwrap();
    assert_eq!(plan(&fault).await.unwrap_err().code(), "implicit_cast");
    plan(&fault.replace("c.number", "c.roll_call_id"))
        .await
        .unwrap();
    assert!(!root.path().join("must-not-exist").exists());
}

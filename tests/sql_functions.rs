//! Generic SQL primitives have no archive identifiers or URL policy.
// Rust guideline compliant 2026-02-21
use arrow::datatypes::{DataType, Field, Schema};
use docspec::{budget::Limits, job::JobSpec};
use flate2::{Compression, write::GzEncoder};
use serde_json::{Value, json};
use std::io::Write;

fn limits() -> Limits {
    Limits {
        compute_threads: 2,
        memory_bytes: 32 << 20,
        max_batch_bytes: 4 << 20,
        batch_rows: 32,
        scratch_bytes: 32 << 20,
        ..Limits::default()
    }
}
async fn scalar(sql: &str) -> Value {
    let root = tempfile::tempdir().unwrap();
    let spec:JobSpec=serde_json::from_value(json!({"scratch":root.path(),"inputs":[],"steps":[],"outputs":[],"queries":[{"name":"value","sql":sql}]})).unwrap();
    spec.run(limits()).await.unwrap().queries.remove(0).data
}

#[tokio::test]
async fn http_dates_are_nullable_utc_values_without_application_retry_policy() {
    let values = scalar(
        "SELECT id,cast(http_date(value) AS BIGINT) AS micros FROM (VALUES
        (1,'Sun, 06 Nov 1994 08:49:37 GMT'),
        (2,'Sunday, 06-Nov-94 08:49:37 GMT'),
        (3,'Sun Nov  6 08:49:37 1994'),
        (4,'Wed, 21 Oct 2026 07:28:00 GMT'),
        (5,cast(NULL AS VARCHAR)),(6,'not a date'),(7,'Thu, 06 Nov 1994 08:49:37 GMT'),
        (8,'Wed, 31 Feb 2026 00:00:00 GMT'),(9,'Wed, 31 Dec 1969 23:59:59 GMT'),
        (10,'Thu, 01 Jan 1970 00:00:00 GMT')) AS v(id,value) ORDER BY id",
    )
    .await;
    assert_eq!(
        values,
        json!([
        {"id":1,"micros":784111777000000i64},{"id":2,"micros":784111777000000i64},
        {"id":3,"micros":784111777000000i64},{"id":4,"micros":1792567680000000i64},
        {"id":5,"micros":null},{"id":6,"micros":null},{"id":7,"micros":null},
        {"id":8,"micros":null},{"id":9,"micros":null},{"id":10,"micros":0}])
    );
    for kind in ["Utf8", "LargeUtf8", "Utf8View"] {
        assert_eq!(scalar(&format!("SELECT cast(http_date(arrow_cast('Sun, 06 Nov 1994 08:49:37 GMT','{kind}')) AS BIGINT) AS micros")).await,
            json!([{"micros":784111777000000i64}]));
    }
    assert_eq!(
        scalar("SELECT arrow_typeof(http_date(cast(NULL AS VARCHAR))) AS kind").await,
        json!([{"kind":"Timestamp(µs, \"UTC\")"}])
    );
}

#[tokio::test]
async fn http_date_output_respects_the_shared_batch_allowance() {
    let root = tempfile::tempdir().unwrap();
    let mut ceiling = limits();
    ceiling.batch_rows = 400;
    ceiling.max_batch_bytes = 1024;
    let spec:JobSpec=serde_json::from_value(json!({"scratch":root.path(),"inputs":[],"steps":[],"outputs":[],
        "queries":[{"name":"values","sql":"SELECT count(http_date(CASE WHEN value%2=0 THEN 'Thu, 01 Jan 1970 00:00:00 GMT' ELSE 'invalid' END)) AS n FROM generate_series(1,400)"}]})).unwrap();
    let error = spec.run(ceiling).await.unwrap_err().to_string();
    assert!(error.contains("function output"), "{error}");
}

#[tokio::test]
async fn compact_json_matches_authored_python_vectors() {
    let vectors: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/json-array.json")).unwrap();
    for vector in vectors {
        let values = scalar(&format!(
            "SELECT {} AS value",
            vector["expression"].as_str().unwrap()
        ))
        .await;
        assert_eq!(
            values[0]["value"], vector["expected"],
            "{}",
            vector["expression"]
        );
    }
    assert_eq!(
        scalar("SELECT json_get_str('{\"a\":\"raw fallback\"}','a') AS value").await[0]["value"],
        "raw fallback"
    );
}

#[tokio::test]
async fn string_and_filename_primitives_follow_their_golden_vectors() {
    let root = tempfile::tempdir().unwrap();
    let vectors: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/url-functions.json")).unwrap();
    let path = root.path().join("vectors.gz");
    let mut out = GzEncoder::new(std::fs::File::create(&path).unwrap(), Compression::fast());
    for (id, row) in vectors.iter().enumerate() {
        let mut row = row.clone();
        row["id"] = json!(id);
        writeln!(out, "{row}").unwrap();
    }
    out.finish().unwrap();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("function", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("base", DataType::Utf8, true),
    ]);
    let spec:JobSpec=serde_json::from_value(json!({"scratch":root.path().join("scratch"),"inputs":[{"kind":"json_lines","name":"vectors","files":[{"key":"golden","path":path}],"schema":schema}],"steps":[],"outputs":[],"queries":[{"name":"values","sql":"SELECT id,CASE function WHEN 'url_resolve' THEN url_resolve(value,base) WHEN 'uri_normalize_encoding' THEN uri_normalize_encoding(value) WHEN 'percent_decode_utf8' THEN percent_decode_utf8(value) WHEN 'url_filename' THEN url_filename(value) WHEN 'disposition_filename' THEN disposition_filename(value) END AS value FROM vectors ORDER BY id"}]})).unwrap();
    let result = spec.run(limits()).await.unwrap();
    for row in result.queries[0].data.as_array().unwrap() {
        let id = row["id"].as_u64().unwrap() as usize;
        assert_eq!(row["value"], vectors[id]["expected"], "{}", vectors[id]);
    }
}

#[tokio::test]
async fn pinned_whatwg_resolver_corpus_has_no_unrecorded_deviation() {
    let root = tempfile::tempdir().unwrap();
    let corpus: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/url/urltestdata.json")).unwrap();
    let cases = corpus
        .into_iter()
        .filter(Value::is_object)
        .collect::<Vec<_>>();
    let path = root.path().join("corpus.gz");
    let mut out = GzEncoder::new(std::fs::File::create(&path).unwrap(), Compression::fast());
    for (id, case) in cases.iter().enumerate() {
        writeln!(
            out,
            "{}",
            json!({"id":id,"input":case["input"],"base":case["base"]})
        )
        .unwrap();
    }
    out.finish().unwrap();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("input", DataType::Utf8, true),
        Field::new("base", DataType::Utf8, true),
    ]);
    let spec:JobSpec=serde_json::from_value(json!({"scratch":root.path().join("scratch"),"inputs":[{"kind":"json_lines","name":"cases","files":[{"key":"WHATWG corpus","path":path}],"schema":schema}],"steps":[],"outputs":[],"queries":[{"name":"resolved","sql":"SELECT id,url_resolve(input,base) AS value FROM cases"}]})).unwrap();
    let rows = spec.run(limits()).await.unwrap().queries.remove(0).data;
    assert_eq!(rows.as_array().unwrap().len(), cases.len());
    let ids = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_u64().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(ids.len(), cases.len());
    let known = include_str!("fixtures/url/expected_failures.txt")
        .lines()
        .collect::<Vec<_>>();
    let mut deviations = Vec::new();
    for row in rows.as_array().unwrap() {
        let case = &cases[row["id"].as_u64().unwrap() as usize];
        let expected = case.get("href").unwrap_or(&Value::Null);
        if &row["value"] != expected {
            let mut name = format!("<{}>", case["input"].as_str().unwrap().escape_default());
            if let Some(base) = case["base"].as_str() {
                name.push_str(&format!(" against <{}>", base.escape_default()));
            }
            assert!(
                known.contains(&name.as_str()),
                "unrecorded resolver deviation {name}: expected{expected},actual{}",
                row["value"]
            );
            deviations.push(name);
        }
    }
    eprintln!(
        "resolver corpus:{} cases; known upstream file-URL deviations:{} {:?}",
        cases.len(),
        deviations.len(),
        deviations
    );
}

#[tokio::test]
async fn json_output_expansion_and_unsupported_values_refuse() {
    let root = tempfile::tempdir().unwrap();
    let mut small = limits();
    small.max_batch_bytes = 16384;
    for sql in [
        "SELECT json_array(repeat('x',32768)) AS value",
        "SELECT json_array(cast('NaN' AS DOUBLE)) AS value",
    ] {
        let spec:JobSpec=serde_json::from_value(json!({"scratch":root.path(),"inputs":[],"steps":[],"outputs":[],"queries":[{"name":"value","sql":sql}]})).unwrap();
        let error = spec.run(small.clone()).await.unwrap_err().to_string();
        assert!(
            error.contains("function output") || error.contains("unsupported JSON"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn non_scalar_url_expansion_admits_retained_capacity_before_parsing() {
    use arrow::{array::StringArray, record_batch::RecordBatch};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("url.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let value = format!("https://example.test/{}", "é".repeat(300000));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![value]))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let spec: JobSpec = serde_json::from_value(json!({
        "scratch":root.path().join("scratch"),
        "inputs":[{"kind":"parquet","name":"urls","path":path}],
        "steps":[],"outputs":[],
        "queries":[{"name":"n","sql":"SELECT length(url_resolve(value,cast(NULL AS VARCHAR))) AS n FROM urls"}]
    })).unwrap();
    let mut small = limits();
    small.memory_bytes = 8 << 20;
    small.max_batch_bytes = 2 << 20;
    let error = spec.run(small).await.unwrap_err().to_string();
    assert!(error.contains("function output"), "{error}");
}

#[tokio::test]
async fn canonical_json_matches_python_sorted_compact_vectors() {
    let vectors: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/json-canonical.json")).unwrap();
    for vector in vectors {
        let literal = vector["input"].as_str().unwrap().replace('\'', "''");
        let actual = scalar(&format!("SELECT json_canonical('{literal}') AS value")).await;
        assert_eq!(
            actual[0]["value"], vector["expected"],
            "{}",
            vector["input"]
        );
    }
    assert_eq!(
        scalar("SELECT json_canonical(cast(NULL AS VARCHAR)) AS value").await[0]["value"],
        Value::Null
    );
    let root = tempfile::tempdir().unwrap();
    for sql in [
        "SELECT json_canonical('{bad') AS value",
        "SELECT json_canonical('[1e999]') AS value",
        "SELECT json_canonical('[NaN]') AS value",
    ] {
        let spec: JobSpec = serde_json::from_value(json!({"scratch":root.path(),"inputs":[],"steps":[],"outputs":[],"queries":[{"name":"value","sql":sql}]})).unwrap();
        assert!(spec.run(limits()).await.is_err(), "{sql}");
    }
    let mut small = limits();
    small.memory_bytes = 1 << 20;
    let spec: JobSpec = serde_json::from_value(json!({"scratch":root.path(),"inputs":[],"steps":[],"outputs":[],"queries":[{"name":"value","sql":"SELECT json_canonical(concat('[',repeat('0,',10000),'0]')) AS value"}]})).unwrap();
    let error = spec.run(small).await.unwrap_err().to_string();
    assert!(error.contains("function output"), "{error}");
}

#[tokio::test]
async fn canonical_cumulative_output_exhaustion_is_a_capacity_refusal() {
    let root = tempfile::tempdir().unwrap();
    let mut ceiling = limits();
    ceiling.batch_rows = 400;
    ceiling.max_batch_bytes = 1024;
    let spec:JobSpec=serde_json::from_value(json!({"scratch":root.path(),"inputs":[],"steps":[],"outputs":[],"queries":[{"name":"value","sql":"SELECT sum(length(json_canonical(concat('1e',cast(value % 2 AS VARCHAR))))) AS n FROM generate_series(1,400)"}]})).unwrap();
    let error = spec.run(ceiling).await.unwrap_err().to_string();
    assert!(error.contains("function output"), "{error}");
}

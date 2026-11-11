//! A lake column's field id outlives a managed step's reopen, as in a plain job.
// Rust guideline compliant 2026-02-21

use super::*;
use parquet::file::reader::{FileReader, SerializedFileReader};

/// Each top-level column's name and Parquet field id, from the footer of the file at `path`.
fn footer_field_ids(path: &Path) -> Vec<(String, Option<i32>)> {
    let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
    let schema = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema();
    schema
        .get_fields()
        .iter()
        .map(|field| {
            let info = field.get_basic_info();
            (field.name().to_owned(), info.has_id().then(|| info.id()))
        })
        .collect()
}

/// The footer of the Parquet file at `path`: the bytes its last eight bytes measure.
fn footer(path: &Path) -> Vec<u8> {
    let bytes = std::fs::read(path).unwrap();
    let end = bytes.len() - 8;
    let length = u32::from_le_bytes(bytes[end..end + 4].try_into().unwrap()) as usize;
    bytes[end - length..end].to_vec()
}

/// The first step: the lake table's columns, then one column per field-identity case.
///
/// `occurrence`, `id` and `amount` are copied, so they carry the lake's ids 1,
/// 2 and 3. After them, def-9ise's cases: `k` unions `id` with `amount`
/// (conflicting ids), `amount_next` is computed (no id), `id_cast` is a
/// `TRY_CAST` to the column's own type, and `same_id` is a correlated scalar
/// subquery, whose decorrelation can change nullability. `amount_or_zero`
/// follows a lake-only incident (W6), where a plan over `coalesce(f.valid,
/// false)` carried the id in its running schema and not in its planned one.
/// The test asserts the ids of the first three columns only; the plain job
/// decides the rest.
const MATERIAL: &str = "SELECT r.occurrence, r.id, r.amount, u.k, \
    r.amount + 1 AS amount_next, TRY_CAST(r.id AS BIGINT) AS id_cast, \
    (SELECT count(*) FROM raw s WHERE s.id = r.id) AS same_id, \
    coalesce(r.amount, 0) AS amount_or_zero \
    FROM raw r JOIN (SELECT occurrence, id AS k FROM raw WHERE occurrence < 3 \
    UNION ALL SELECT occurrence, amount AS k FROM raw WHERE occurrence = 3) u \
    ON u.occurrence = r.occurrence";

/// A managed job's later step and export keep what a plain job keeps.
///
/// `material` ([`MATERIAL`]) copies the lake table's columns, so its file
/// carries their field ids. `later` reads `material` after the managed job has
/// reopened it from that file, and `copied` exports `later`. A plain job keeps
/// its steps as planned, so the same job file run as a plain job is the
/// reference: the managed export must have its footer and its bytes, every
/// column's id, type and nullability included.
#[tokio::test]
async fn a_reopened_step_keeps_the_field_ids_a_plain_job_keeps() {
    let root = tempfile::tempdir().unwrap();
    let ManagedJobCommand::Prepare(mut request) = snapshot_request(root.path(), "field-ids").await
    else {
        unreachable!()
    };
    let reference = root.path().join("plain-copied.parquet");
    let mut job: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&request.job.path).unwrap()).unwrap();
    job["steps"] = json!([
        {"name":"material","sql":MATERIAL,"materialize":true},
        {"name":"later","sql":"SELECT * FROM material","materialize":true}
    ]);
    job["outputs"] = json!([{"name":"copied","path":reference,"sql":"SELECT * FROM later","settings":{
        "profile":"whole","row_group_rows":128,"order_by":[{"field":"occurrence"}],"columns":[],"dictionary_columns":[],"delta_columns":[],"key_column":null,"created_by":"field-id-test","footer_limit":65536
    }}]);
    request.job = artifact(root.path().join("field-ids-job.json"), &job);
    let mut policy: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&request.policy.path).unwrap()).unwrap();
    policy["job_sha256"] = json!(request.job.sha256);
    policy["view_names"]["steps"] = json!(["material", "later"]);
    policy["view_names"]["outputs"] = json!(["copied"]);
    request.policy = artifact(root.path().join("field-ids-policy.json"), &policy);
    let engine = Engine::open(root.path().join("host"), limits())
        .await
        .unwrap();
    let managed = engine
        .managed_job(ManagedJobCommand::Prepare(request))
        .await
        .unwrap();
    assert_eq!(managed.status, "completed");
    let expected = [("occurrence", 1), ("id", 2), ("amount", 3)]
        .map(|(name, id)| (name.to_owned(), Some(id)))
        .to_vec();
    let witness = managed.completion_manifest.as_ref().unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&STANDARD.decode(&witness.bytes_base64).unwrap()).unwrap();
    // The first step writes the lake's ids: they are there for a reopen to lose.
    let written = manifest["steps"][0]["completion"]["path"].as_str().unwrap();
    assert_eq!(footer_field_ids(Path::new(written))[..3], expected[..]);
    // The second step's recorded schema: the one it declared over the reopened first step.
    let sha256 = manifest["steps"][1]["schema_sha256"].as_str().unwrap();
    let encoded = manifest["schemas"][sha256].as_str().unwrap();
    let seen = try_schema_from_ipc_buffer(&STANDARD.decode(encoded).unwrap()).unwrap();
    let seen = seen
        .fields()
        .iter()
        .map(|field| {
            let id = field.metadata().get("PARQUET:field_id");
            (
                field.name().clone(),
                id.map(|id| id.parse::<i32>().unwrap()),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        seen[..3],
        expected[..],
        "the later step lost the reopened step's ids"
    );
    let exported = Path::new(managed.completed_outputs[0]["path"].as_str().unwrap());
    assert_eq!(footer_field_ids(exported)[..3], expected[..]);
    let spec: JobSpec = serde_json::from_value(job).unwrap();
    let plain = spec.run(limits()).await.unwrap();
    assert_eq!(
        footer(exported),
        footer(&reference),
        "the managed export's footer is not the plain job's"
    );
    assert_eq!(
        managed.completed_outputs[0]["sha256"], plain.outputs[0].file.sha256,
        "the managed export's bytes are not the plain job's"
    );
}

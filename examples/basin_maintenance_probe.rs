//! Scratch-only Basin maintenance experiment through maintained Iceberg and DataFusion APIs.
// Rust guideline compliant 2026-02-21

use anyhow::{Context, Result, ensure};
use arrow::{
    array::{Int32Array, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use clap::{Parser, ValueEnum};
use datafusion::{
    execution::{memory_pool::FairSpillPool, runtime_env::RuntimeEnvBuilder},
    prelude::*,
};
use futures::TryStreamExt;
use iceberg::{
    Catalog, NamespaceIdent, TableCreation, TableIdent,
    spec::{DataFile, NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec},
    table::Table,
};
use iceberg_datafusion::table::IcebergStaticTableProvider;
use opendal::{Operator, services::S3};
use serde_json::{Value, json};
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Instant};

#[derive(Clone, Debug, ValueEnum)]
enum Action {
    Create,
    Append,
    Observe,
    Race,
    Purge,
    Decide,
    Projection,
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    namespace: String,
    #[arg(long, value_enum, default_value = "observe")]
    action: Action,
    #[arg(long, default_value = "growth")]
    table: String,
    #[arg(long)]
    allow_scratch_writes: bool,
    #[arg(long)]
    plan: bool,
    #[arg(long, default_value_t = 0)]
    day: i32,
    #[arg(long, default_value_t = 15000)]
    rows_per_day: usize,
    #[arg(long, default_value_t = 1)]
    batches: usize,
    #[arg(long)]
    maintenance_run: Option<String>,
    #[arg(long)]
    previous_observation: Option<PathBuf>,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, num_args=1..)]
    observations: Vec<PathBuf>,
    #[arg(long)]
    projected: bool,
    #[arg(long)]
    projection: Option<PathBuf>,
    #[arg(long)]
    maintenance_evidence: Option<PathBuf>,
    #[arg(long, num_args=1..)]
    write_records: Vec<PathBuf>,
    #[arg(long)]
    cap_trial: bool,
    #[arg(long)]
    scratch: PathBuf,
}

fn validate_namespace(value: &str) -> Result<()> {
    let suffix = value
        .strip_prefix("docspec_m1_")
        .context("scratch namespace required")?;
    ensure!(
        uuid::Uuid::parse_str(suffix)?.simple().to_string() == suffix,
        "canonical scratch UUID required"
    );
    Ok(())
}

fn format_verdict(
    cycles: usize,
    rows_equal: bool,
    projected_scan_ms: u64,
    smallest_runner: bool,
) -> &'static str {
    if cycles < 10 || !smallest_runner {
        "incomplete"
    } else if rows_equal && projected_scan_ms < 60000 {
        "iceberg"
    } else {
        "dated_parquet"
    }
}

fn required(name: &str) -> Result<String> {
    std::env::var(name).map_err(|_| anyhow::anyhow!("missing probe configuration {name}"))
}

fn validate_build_profile(debug_assertions: bool) -> Result<()> {
    ensure!(
        !debug_assertions,
        "remote measurement requires the optimized --release build"
    );
    Ok(())
}

async fn connect() -> Result<(Arc<dyn Catalog>, Operator, String)> {
    let bucket = required("DOCSPEC_M1_BUCKET")?;
    ensure!(
        bucket.starts_with("cx-engine-first-lock-probe-"),
        "dedicated scratch bucket required"
    );
    let uri = required("DOCSPEC_M1_CATALOG_URI")?;
    ensure!(
        uri.trim_end_matches('/').rsplit('/').next() == Some(bucket.as_str()),
        "catalog must target scratch bucket"
    );
    let endpoint = required("DOCSPEC_M1_S3_ENDPOINT")?;
    let catalog = docspec::storage::remote::remote_catalog(
        HashMap::from([
            ("uri".into(), uri),
            ("warehouse".into(), required("DOCSPEC_M1_WAREHOUSE")?),
            ("s3.endpoint".into(), endpoint.clone()),
            ("s3.region".into(), "auto".into()),
            ("s3.path-style-access".into(), "true".into()),
        ]),
        HashMap::from([
            ("token".into(), "DOCSPEC_M1_CATALOG_TOKEN".into()),
            ("s3.access-key-id".into(), "DOCSPEC_M1_ACCESS_KEY_ID".into()),
            (
                "s3.secret-access-key".into(),
                "DOCSPEC_M1_SECRET_ACCESS_KEY".into(),
            ),
        ]),
    )
    .await?;
    let operator = Operator::new(
        S3::default()
            .bucket(&bucket)
            .endpoint(&endpoint)
            .region("auto")
            .access_key_id(&required("DOCSPEC_M1_ACCESS_KEY_ID")?)
            .secret_access_key(&required("DOCSPEC_M1_SECRET_ACCESS_KEY")?)
            .disable_ec2_metadata()
            .disable_config_load(),
    )?
    .finish();
    Ok((catalog, operator, bucket))
}

fn context(args: &Args) -> Result<SessionContext> {
    std::fs::create_dir_all(&args.scratch)?;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(10 << 30)))
        .with_max_temp_directory_size(32 << 30)
        .with_temp_file_path(&args.scratch)
        .build()?;
    Ok(SessionContext::new_with_config_rt(
        SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(8192),
        Arc::new(runtime),
    ))
}

fn batch(table: &Table, day: i32, start: usize, count: usize) -> Result<RecordBatch> {
    let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);
    let payload = (start..start + count)
        .map(|i| format!("{day}/{i}/probe"))
        .collect::<Vec<_>>();
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![day; count])),
            Arc::new(Int64Array::from_iter_values(
                (start..start + count).map(|i| i as i64),
            )),
            Arc::new(StringArray::from(payload)),
        ],
    )?)
}

async fn files(table: &Table, day: i32, count: usize, capped: bool) -> Result<Vec<DataFile>> {
    ensure!(
        count > 0 && count <= i32::MAX as usize,
        "invalid rows per day"
    );
    ensure!(
        table
            .metadata()
            .properties()
            .get("probe.rows-per-day")
            .is_some_and(|v| v == &count.to_string()),
        "writer workload differs from the pinned table"
    );
    let profile = table
        .metadata()
        .properties()
        .get("probe.writer-profile")
        .context("missing pinned writer profile")?;
    ensure!(
        profile
            == if capped {
                "engine-v1-capped"
            } else {
                "engine-v1-uncapped"
            },
        "writer profile mismatch"
    );
    let budget = docspec::budget::Budget::new(docspec::budget::Limits {
        compute_threads: 4,
        memory_bytes: 10 << 30,
        scratch_bytes: 32 << 30,
        batch_rows: 65536,
        max_batch_bytes: 256 << 20,
        max_metadata_bytes: 32 << 20,
    })?;
    let layout = docspec::storage::WriteLayout {
        file_bytes: if capped { 2 << 20 } else { 128 << 20 },
        row_group_rows: if capped { 2048 } else { 65536 },
        row_group_bytes: if capped { 2 << 20 } else { 128 << 20 },
    };
    // Generate fixed-size batches lazily, only after the engine admits writer buffers.
    let batches = futures::stream::iter((0..count).step_by(8192).map(|start| {
        batch(table, day, start, (count - start).min(8192)).map_err(|_| {
            docspec::Error::refusal("probe_batch", "synthetic batch construction failed")
        })
    }));
    Ok(docspec::storage::write_batches_with_layout(table, batches, &budget, layout).await?)
}

async fn append(catalog: &dyn Catalog, table: &Table, files: Vec<DataFile>) -> Result<Table> {
    Ok(docspec::storage::append(table, catalog, files).await?)
}

async fn scalar(ctx: &SessionContext, sql: &str) -> Result<i64> {
    let result = ctx.sql(sql).await?.collect().await?;
    Ok(result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("expected count")?
        .value(0))
}

fn writer_evidence(
    previous: Option<&Value>,
    records: &[Value],
    identity: &Value,
    day: i32,
    rows: usize,
) -> Result<std::collections::BTreeSet<String>> {
    let same_identity = |record: &Value| {
        [
            "namespace",
            "table",
            "table_uuid",
            "writer_profile",
            "binary_blake3",
            "release_build",
        ]
        .iter()
        .all(|key| !identity[key].is_null() && record[key] == identity[key])
    };
    let mut known = std::collections::BTreeSet::new();
    let mut written_days = std::collections::BTreeSet::new();
    if let Some(old) = previous {
        ensure!(
            same_identity(old) && old["rows_per_day"] == rows && old["rows_equal"] == true,
            "previous observation identity or workload mismatch"
        );
        ensure!(
            old["day"]
                .as_i64()
                .is_some_and(|d| d >= 0 && d <= i64::from(day)),
            "previous observation day is invalid"
        );
        for path in old["data_paths"]
            .as_array()
            .context("prior file inventory missing")?
        {
            known.insert(path.as_str().context("prior file path missing")?.to_owned());
        }
    }
    for record in records {
        ensure!(same_identity(record), "writer evidence identity mismatch");
        for batch in record["batches"]
            .as_array()
            .context("append batch evidence required")?
        {
            let files = batch["files"]
                .as_array()
                .context("writer file inventory missing")?;
            if batch["commits"] == 0 {
                ensure!(
                    files.is_empty() && batch["work_list_rows"] == 0,
                    "no-op evidence contains written files"
                );
                continue;
            }
            ensure!(
                batch["commits"] == 1
                    && batch["work_list_rows"] == rows
                    && batch["committed_snapshot"].is_i64(),
                "successful append evidence required"
            );
            let written_day = batch["day"].as_i64().context("write day missing")?;
            ensure!(
                written_day >= 0 && written_day <= i64::from(day),
                "write day outside observation"
            );
            let mut file_rows = 0u64;
            for file in files {
                ensure!(
                    file["bytes"].as_u64().is_some_and(|n| n > 0),
                    "writer file size missing"
                );
                file_rows = file_rows
                    .checked_add(file["rows"].as_u64().context("file row count missing")?)
                    .context("file row count overflow")?;
                ensure!(
                    known.insert(
                        file["path"]
                            .as_str()
                            .context("writer file path missing")?
                            .to_owned()
                    ),
                    "duplicate writer file evidence"
                );
            }
            ensure!(file_rows == rows as u64, "incomplete writer file inventory");
            ensure!(
                written_days.insert(written_day),
                "duplicate successful day evidence"
            );
        }
    }
    if let Some(old) = previous {
        let first = old["day"].as_i64().context("prior day missing")? + 1;
        ensure!(
            (first..=i64::from(day)).all(|d| written_days.contains(&d)),
            "write evidence must cover every newly appended day"
        );
    }
    Ok(known)
}

async fn observe(
    args: &Args,
    table: &Table,
    operator: &Operator,
    bucket: &str,
    binary: &str,
) -> Result<Value> {
    ensure!(
        table
            .metadata()
            .properties()
            .get("probe.rows-per-day")
            .is_some_and(|v| v == &args.rows_per_day.to_string()),
        "observation workload differs from the pinned table"
    );
    let ctx = context(args)?;
    ctx.register_table(
        "probe",
        Arc::new(IcebergStaticTableProvider::try_new_from_table(table.clone()).await?),
    )?;
    let scan_started_at_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos() as u64;
    let start = Instant::now();
    // Consume every column in bounded batches; a metadata-only count is not a full scan.
    let mut stream = ctx
        .sql("SELECT * FROM probe")
        .await?
        .execute_stream()
        .await?;
    let mut rows = 0i64;
    while let Some(batch) = stream.try_next().await? {
        rows = rows
            .checked_add(i64::try_from(batch.num_rows())?)
            .context("scan row count overflow")?;
    }
    let scan_ms = start.elapsed().as_millis() as u64;
    let expected = i64::from(args.day + 1) * args.rows_per_day as i64;
    let bad = scalar(&ctx, &format!("SELECT count(*) FROM probe WHERE day < 0 OR day > {} OR item < 0 OR item >= {} OR payload <> concat(cast(day AS varchar), '/', cast(item AS varchar), '/probe')",args.day,args.rows_per_day)).await?;
    let distinct = scalar(
        &ctx,
        "SELECT count(*) FROM (SELECT day, item FROM probe GROUP BY day, item)",
    )
    .await?;
    let plan = ctx
        .sql(&format!("SELECT item FROM probe WHERE day = {}", args.day))
        .await?
        .create_physical_plan()
        .await?;
    let plan = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    ensure!(
        plan.contains("predicate:[") && !plan.contains("predicate:[]"),
        "partition filter did not reach Iceberg planning"
    );
    let native = table.scan().build()?;
    let tasks = native.plan_files().await?.try_collect::<Vec<_>>().await?;
    let data = tasks
        .iter()
        .map(|t| t.data_file_path.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let snapshot = table
        .metadata()
        .current_snapshot()
        .context("expected snapshot")?;
    let manifests = table.manifest_list_reader(snapshot).load().await?;
    let prefix = table
        .metadata()
        .location()
        .strip_prefix(&format!("s3://{bucket}/"))
        .context("table escaped scratch bucket")?;
    let listing = operator
        .list_with(&format!("{prefix}/"))
        .recursive(true)
        .await?;
    let objects = listing
        .iter()
        .filter(|e| e.metadata().is_file())
        .map(|e| (e.path().to_string(), e.metadata().content_length()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let previous = args
        .previous_observation
        .as_ref()
        .map(|p| {
            std::fs::read(p)
                .and_then(|b| serde_json::from_slice::<Value>(&b).map_err(std::io::Error::other))
        })
        .transpose()?;
    let expired_bytes: u64 = previous
        .as_ref()
        .and_then(|p| p["objects"].as_object())
        .map(|m| {
            m.iter()
                .filter(|(k, _)| !objects.contains_key(*k))
                .map(|(_, v)| v.as_u64().unwrap_or(0))
                .sum()
        })
        .unwrap_or(0);
    let records = args
        .write_records
        .iter()
        .map(|path| Ok(serde_json::from_slice::<Value>(&std::fs::read(path)?)?))
        .collect::<Result<Vec<_>>>()?;
    let identity = json!({"namespace":args.namespace,"table":args.table,
        "table_uuid":table.metadata().uuid(),"binary_blake3":binary,"release_build":!cfg!(debug_assertions),
        "writer_profile":table.metadata().properties().get("probe.writer-profile")});
    let known = writer_evidence(
        previous.as_ref(),
        &records,
        &identity,
        args.day,
        args.rows_per_day,
    )?;
    let compacted = if previous.is_some() && args.maintenance_evidence.is_some() {
        compacted_file(&data, &known)
    } else {
        None
    };
    let selected = compacted
        .as_ref()
        .or_else(|| data.first())
        .context("expected data file")?;
    let first = tasks
        .iter()
        .find(|t| &t.data_file_path == selected)
        .context("selected data file missing")?;
    let mut reader = iceberg::arrow::ArrowFileReader::new(
        iceberg::io::FileMetadata {
            size: first.file_size_in_bytes,
        },
        table
            .file_io()
            .new_input(&first.data_file_path)?
            .reader()
            .await?,
    );
    use parquet::arrow::async_reader::AsyncFileReader;
    let footer = reader.get_metadata(None).await?;
    let codecs = footer
        .row_groups()
        .iter()
        .flat_map(|g| g.columns().iter().map(|c| format!("{:?}", c.compression())))
        .collect::<std::collections::BTreeSet<_>>();
    let maintenance_evidence: Option<Value> = args
        .maintenance_evidence
        .as_ref()
        .map(|p| Ok::<_, anyhow::Error>(serde_json::from_slice(&std::fs::read(p)?)?))
        .transpose()?;
    let projection: Option<Value> = args
        .projection
        .as_ref()
        .map(|p| Ok::<_, anyhow::Error>(serde_json::from_slice(&std::fs::read(p)?)?))
        .transpose()?;
    if args.projected {
        ensure!(
            projection.is_some() && args.table == "projected",
            "projected observation requires its measured-growth plan"
        );
    }
    Ok(
        json!({"namespace":args.namespace,"table":args.table,"maintenance_run":args.maintenance_run,
        "snapshot":snapshot.snapshot_id(),"table_uuid":table.metadata().uuid(),"day":args.day,"rows_per_day":args.rows_per_day,"maintenance_evidence":maintenance_evidence,"projection":projection,"codec_file":selected,"compaction_codec_verified":compacted.is_some(),"cap_trial":args.cap_trial,"rows":rows,"rows_equal":rows == expected && distinct == expected && bad == 0,
        "data_files":data.len(),"data_paths":data,"data_bytes":tasks.iter().map(|t|(&t.data_file_path,t.file_size_in_bytes)).collect::<std::collections::BTreeMap<_,_>>().values().sum::<u64>(),"manifests":manifests.entries().len(),
        "metadata_json_files":objects.keys().filter(|k| k.ends_with(".metadata.json")).count(),
        "expired_bytes":expired_bytes,"objects":objects,"codecs":codecs,"row_group_rows":footer.row_groups().iter().map(|g|g.num_rows()).collect::<Vec<_>>(),"codec_file_bytes":first.file_size_in_bytes,"peak_rss_bytes":process_peak_rss(),"partition_plan":plan,
        "scan_ms":scan_ms,"scan_started_at_ns":scan_started_at_ns,"projected":args.projected,"runner_os":std::env::consts::OS,
        "runner_threads":std::thread::available_parallelism()?.get(),"runner_memory_bytes":runner_memory(),"pool_bytes":10_u64 << 30,
        "scratch_bytes":32_u64 << 30,"writer_profile":table.metadata().properties().get("probe.writer-profile")}),
    )
}

async fn run(args: Args) -> Result<()> {
    validate_namespace(&args.namespace)?;
    ensure!(
        matches!(
            args.table.as_str(),
            "growth" | "capped" | "uncapped" | "projected"
        ),
        "scratch table name required"
    );
    if args.plan {
        std::fs::write(
            &args.output,
            serde_json::to_vec_pretty(&json!({"status":"owner-gated", "namespace":args.namespace,
            "experiments":["guarded race and recomputed work list within three retries", "ten maintenance observations with exact rows and footer codecs", "projected ten-year counts and full scan", "interleaved capped/uncapped arms", "purge and remaining-object listing"],
            "format_rule":"rows unchanged and ten-year full scan under 60000ms on ubuntu-24.04 4cpu/16gb", "network_used":false}))?,
        )?;
        return Ok(());
    }
    if matches!(args.action, Action::Decide | Action::Projection) {
        let reports = args
            .observations
            .iter()
            .map(|p| Ok(serde_json::from_slice::<Value>(&std::fs::read(p)?)?))
            .collect::<Result<Vec<_>>>()?;
        let result = if matches!(args.action, Action::Projection) {
            projection_for(&args.namespace, &reports)?
        } else {
            decision_report(&args.namespace, &reports)?
        };
        std::fs::write(&args.output, serde_json::to_vec_pretty(&result)?)?;
        return Ok(());
    }
    ensure!(
        args.allow_scratch_writes,
        "explicit --allow-scratch-writes required, including observation startup"
    );
    validate_build_profile(cfg!(debug_assertions))?;
    let binary = docspec::storage::digest(&std::env::current_exe()?)?.0;
    let (catalog, operator, bucket) = connect().await?;
    let namespace = NamespaceIdent::new(args.namespace.clone());
    let ident = TableIdent::new(namespace.clone(), args.table.clone());
    let mut result = json!({});
    if matches!(args.action, Action::Create) {
        ensure!(
            !catalog.table_exists(&ident).await?,
            "scratch table already exists"
        );
        if !catalog.namespace_exists(&namespace).await? {
            catalog.create_namespace(&namespace, HashMap::new()).await?;
        }
        let table = create_table(
            catalog.as_ref(),
            &ident,
            &format!("s3://{bucket}/docspec-m1/{}/{}", args.namespace, args.table),
            args.rows_per_day,
        )
        .await?;
        result =
            json!({"created":table.identifier().to_string(),"table_uuid":table.metadata().uuid()});
    } else {
        let table = catalog.load_table(&ident).await?;
        ensure!(
            table
                .metadata()
                .location()
                .starts_with(&format!("s3://{bucket}/docspec-m1/{}/", args.namespace)),
            "table location outside owned scratch scope"
        );
        match args.action {
            Action::Append => {
                let mut batches = Vec::new();
                let mut report_bytes = 0usize;
                for offset in 0..args.batches {
                    let day = args
                        .day
                        .checked_add(i32::try_from(offset)?)
                        .context("day overflow")?;
                    let batch = ingest_day(catalog.as_ref(), &ident, day, &args).await?;
                    report_bytes = report_bytes
                        .checked_add(serde_json::to_vec(&batch)?.len())
                        .context("report size overflow")?;
                    ensure!(
                        report_bytes <= 32 << 20,
                        "append report exceeds metadata bound; split the command"
                    );
                    batches.push(batch);
                }
                result = json!({"batches":batches,"writer_profile":table.metadata().properties().get("probe.writer-profile"),"cap_trial":args.cap_trial,"table_uuid":table.metadata().uuid()});
            }
            Action::Race => {
                result = race(catalog.clone(), table, &args).await?;
            }
            Action::Observe => {
                result = observe(&args, &table, &operator, &bucket, &binary).await?;
            }
            Action::Purge => {
                let current = catalog.load_table(&ident).await?;
                ensure!(
                    current.metadata().uuid() == table.metadata().uuid()
                        && current.metadata_location() == table.metadata_location(),
                    "table changed before purge"
                );
                let prefix = table
                    .metadata()
                    .location()
                    .strip_prefix(&format!("s3://{bucket}/"))
                    .context("scratch scope")?
                    .to_owned();
                catalog.purge_table(&ident).await?;
                let remaining = operator
                    .list_with(&format!("{prefix}/"))
                    .recursive(true)
                    .await?
                    .into_iter()
                    .map(|e| e.path().to_owned())
                    .collect::<Vec<_>>();
                result = json!({"purged":ident.to_string(),"remaining_objects":remaining});
            }
            _ => {}
        }
    }
    result["binary_blake3"] = json!(binary);
    result["release_build"] = json!(!cfg!(debug_assertions));
    result["namespace"] = json!(args.namespace);
    result["table"] = json!(args.table);
    std::fs::write(args.output, serde_json::to_vec_pretty(&result)?)?;
    Ok(())
}

fn failure_report(error: &anyhow::Error, stage: &str) -> Value {
    let (code, message) = if let Some(e) = error.downcast_ref::<docspec::Error>() {
        (
            e.code().to_owned(),
            "engine refused the probe action".to_owned(),
        )
    } else if let Some(e) = error.downcast_ref::<iceberg::Error>() {
        (
            format!("iceberg_{:?}", e.kind()),
            "native catalog or file operation failed".to_owned(),
        )
    } else if let Some(e) = error.downcast_ref::<&'static str>() {
        ("probe_prerequisite".to_owned(), (*e).to_owned())
    } else if error.downcast_ref::<serde_json::Error>().is_some() {
        (
            "invalid_probe_json".to_owned(),
            "measurement JSON is invalid".to_owned(),
        )
    } else if error.downcast_ref::<std::io::Error>().is_some() {
        (
            "probe_io".to_owned(),
            "measurement file or scratch I/O failed".to_owned(),
        )
    } else {
        (
            "probe_failed".to_owned(),
            "probe prerequisite or action failed; no verdict is available".to_owned(),
        )
    };
    json!({"status":"failed","stage":stage,"code":code,"message":message})
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let output = args.output.clone();
    let stage = format!("{:?}", args.action);
    if let Err(error) = run(args).await {
        let report = failure_report(&error, &stage);
        let _ = std::fs::write(output, report.to_string());
        eprintln!("{report}");
        std::process::exit(1);
    }
}

fn process_peak_rss() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()?
                .checked_mul(1024)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_names_and_noncanonical_probe_names_refuse() {
        for name in ["captures", "docspec_m1_captures", "../raw", ""] {
            assert!(validate_namespace(name).is_err());
        }
        assert!(validate_namespace("docspec_m1_71969fc54a0f4f4eaa49d2dd9f4c2272").is_ok());
    }

    #[test]
    fn no_format_verdict_before_ten_complete_maintenance_observations() {
        assert_eq!(format_verdict(9, true, 1000, true), "incomplete");
        assert_eq!(format_verdict(10, true, 59999, true), "iceberg");
        assert_eq!(format_verdict(10, true, 60000, true), "dated_parquet");
        assert_eq!(format_verdict(10, false, 1000, true), "dated_parquet");
        assert_eq!(format_verdict(10, true, 1000, false), "incomplete");
    }
}

async fn create_table(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    location: &str,
    rows_per_day: usize,
) -> Result<Table> {
    ensure!(rows_per_day > 0, "rows per day must be positive");
    let namespace = ident.namespace();
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "day",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::required(
                2,
                "item",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::required(
                3,
                "payload",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .with_identifier_field_ids([1, 2])
        .build()?;
    let partition = UnboundPartitionSpec::builder()
        .add_partition_field(1, "day", Transform::Identity)?
        .build();
    let table = catalog
        .create_table(
            namespace,
            TableCreation::builder()
                .name(ident.name().to_owned())
                .schema(schema)
                .location(location.to_owned())
                .partition_spec(partition)
                .properties([
                    ("probe.rows-per-day".into(), rows_per_day.to_string()),
                    (
                        "probe.writer-profile".into(),
                        if ident.name() == "capped" {
                            "engine-v1-capped".into()
                        } else {
                            "engine-v1-uncapped".into()
                        },
                    ),
                    ("gc.enabled".into(), "true".into()),
                    ("commit.retry.num-retries".into(), "0".into()),
                ])
                .build(),
        )
        .await?;
    Ok(table)
}

fn runner_memory() -> Option<u64> {
    std::fs::read_to_string("/proc/meminfo")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("MemTotal:")?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()?
                .checked_mul(1024)
        })
}

// An external-catalog adapter creates the race after the transaction's final
// load, so the real catalog must enforce the expected-snapshot requirement.
struct RacingCatalog {
    inner: Arc<dyn Catalog>,
    base: Table,
    competing: tokio::sync::Mutex<Option<Vec<DataFile>>>,
    rejected: std::sync::atomic::AtomicBool,
}
impl std::fmt::Debug for RacingCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RacingCatalog").finish_non_exhaustive()
    }
}
#[async_trait::async_trait]
impl Catalog for RacingCatalog {
    async fn update_table(&self, commit: iceberg::TableCommit) -> iceberg::Result<Table> {
        let competing = self.competing.lock().await.take();
        let injected = competing.is_some();
        if let Some(files) = competing {
            docspec::storage::append(&self.base, self.inner.as_ref(), files)
                .await
                .map_err(|_| {
                    iceberg::Error::new(iceberg::ErrorKind::Unexpected, "competing append failed")
                })?;
        }
        let result = self.inner.update_table(commit).await;
        if injected
            && result.as_ref().err().is_some_and(|e| {
                matches!(
                    e.kind(),
                    iceberg::ErrorKind::CatalogCommitConflicts | iceberg::ErrorKind::DataInvalid
                )
            })
        {
            self.rejected
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        result
    }
    async fn list_namespaces(
        &self,
        p: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(p).await
    }
    async fn create_namespace(
        &self,
        n: &NamespaceIdent,
        p: HashMap<String, String>,
    ) -> iceberg::Result<iceberg::Namespace> {
        self.inner.create_namespace(n, p).await
    }
    async fn get_namespace(&self, n: &NamespaceIdent) -> iceberg::Result<iceberg::Namespace> {
        self.inner.get_namespace(n).await
    }
    async fn namespace_exists(&self, n: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(n).await
    }
    async fn update_namespace(
        &self,
        n: &NamespaceIdent,
        p: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(n, p).await
    }
    async fn drop_namespace(&self, n: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(n).await
    }
    async fn list_tables(&self, n: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(n).await
    }
    async fn create_table(&self, n: &NamespaceIdent, c: TableCreation) -> iceberg::Result<Table> {
        self.inner.create_table(n, c).await
    }
    async fn load_table(&self, t: &TableIdent) -> iceberg::Result<Table> {
        self.inner.load_table(t).await
    }
    async fn drop_table(&self, t: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(t).await
    }
    async fn purge_table(&self, t: &TableIdent) -> iceberg::Result<()> {
        self.inner.purge_table(t).await
    }
    async fn table_exists(&self, t: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(t).await
    }
    async fn rename_table(&self, a: &TableIdent, b: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(a, b).await
    }
    async fn register_table(&self, t: &TableIdent, m: String) -> iceberg::Result<Table> {
        self.inner.register_table(t, m).await
    }
}

async fn race(catalog: Arc<dyn Catalog>, table: Table, args: &Args) -> Result<Value> {
    let winner_day = args.day.checked_add(1).context("day overflow")?;
    ensure!(
        day_count(&table, args.day, args.rows_per_day, args).await? == 0
            && day_count(&table, winner_day, args.rows_per_day, args).await? == 0,
        "race days must be empty"
    );
    let a = files(
        &table,
        winner_day,
        args.rows_per_day,
        args.table == "capped",
    )
    .await?;
    let b = files(&table, args.day, args.rows_per_day, args.table == "capped").await?;
    let mut writer_files = a
        .iter()
        .chain(b.iter())
        .map(|f| f.file_path().to_owned())
        .collect::<Vec<_>>();
    writer_files.sort();
    let racing = RacingCatalog {
        inner: catalog.clone(),
        base: table.clone(),
        competing: tokio::sync::Mutex::new(Some(a)),
        rejected: std::sync::atomic::AtomicBool::new(false),
    };
    ensure!(
        append(&racing, &table, b.clone()).await.is_err(),
        "stale append was rebased"
    );
    ensure!(
        racing.rejected.load(std::sync::atomic::Ordering::SeqCst),
        "catalog did not reject the stale snapshot at commit"
    );
    for retry in 1..=3 {
        let current = catalog.load_table(table.identifier()).await?;
        ensure!(
            current.metadata().uuid() == table.metadata().uuid(),
            "table UUID changed"
        );
        let ctx = context(args)?;
        ctx.register_table(
            "current",
            Arc::new(IcebergStaticTableProvider::try_new_from_table(current.clone()).await?),
        )?;
        let remaining = scalar(
            &ctx,
            &format!("SELECT count(*) FROM current WHERE day={}", args.day),
        )
        .await?;
        ensure!(
            remaining == 0,
            "partial or duplicate ingest found after conflict"
        );
        if append(catalog.as_ref(), &current, b.clone()).await.is_err() {
            continue;
        }
        let latest = catalog.load_table(table.identifier()).await?;
        let ctx = context(args)?;
        ctx.register_table(
            "current",
            Arc::new(IcebergStaticTableProvider::try_new_from_table(latest).await?),
        )?;
        let rows = scalar(
            &ctx,
            &format!(
                "SELECT count(*) FROM current WHERE day IN ({},{winner_day})",
                args.day
            ),
        )
        .await?;
        let distinct = scalar(&ctx,&format!("SELECT count(*) FROM (SELECT day,item FROM current WHERE day IN ({},{winner_day}) GROUP BY day,item)",args.day)).await?;
        ensure!(
            rows == 2 * args.rows_per_day as i64 && distinct == rows,
            "conflict retry duplicated or lost a row"
        );
        let before_repeat = catalog.load_table(table.identifier()).await?;
        let repeat = ingest_day(catalog.as_ref(), table.identifier(), args.day, args).await?;
        let after_repeat = catalog.load_table(table.identifier()).await?;
        ensure!(
            repeat["work_list_rows"] == 0
                && repeat["commits"] == 0
                && before_repeat.metadata_location() == after_repeat.metadata_location(),
            "repeated ingest performed work"
        );
        return Ok(
            json!({"guarded_conflict":true,"server_snapshot_conflict":true,
            "recomputed_work_list_rows":args.rows_per_day,"repeat_work_list_rows":repeat["work_list_rows"],"repeat_commits":repeat["commits"],"writer_files":writer_files,"table_uuid":table.metadata().uuid(),"writer_profile":table.metadata().properties().get("probe.writer-profile"),"written_days":[args.day,winner_day],"duplicate_rows":0,"retries":retry}),
        );
    }
    anyhow::bail!("conflict did not settle within three retries")
}

#[cfg(test)]
mod native_tests {
    use super::*;

    #[tokio::test]
    async fn native_writer_commit_race_reuses_files_and_pushes_partition_filter() -> Result<()> {
        let root = tempfile::tempdir()?;
        let catalog = docspec::storage::local_catalog(root.path()).await?;
        let ns = NamespaceIdent::new("docspec_m1_71969fc54a0f4f4eaa49d2dd9f4c2272".into());
        catalog.create_namespace(&ns, HashMap::new()).await?;
        let ident = TableIdent::new(ns, "uncapped".into());
        let table = create_table(
            catalog.as_ref(),
            &ident,
            &root.path().join("table").to_string_lossy(),
            4,
        )
        .await?;
        let table = append(catalog.as_ref(), &table, files(&table, 0, 4, false).await?).await?;
        let args = Args::parse_from([
            "probe",
            "--namespace",
            "docspec_m1_71969fc54a0f4f4eaa49d2dd9f4c2272",
            "--output",
            root.path().join("out.json").to_str().unwrap(),
            "--scratch",
            root.path().join("scratch").to_str().unwrap(),
            "--day",
            "1",
            "--rows-per-day",
            "4",
        ]);
        let result = race(catalog.clone(), table, &args).await?;
        assert_eq!(result["server_snapshot_conflict"], true);
        assert_eq!(result["retries"], 1);
        let before_repeat = catalog.load_table(&ident).await?;
        assert!(
            race(catalog.clone(), before_repeat.clone(), &args)
                .await
                .is_err()
        );
        assert_eq!(
            catalog.load_table(&ident).await?.metadata_location(),
            before_repeat.metadata_location()
        );
        let table = catalog.load_table(&ident).await?;
        let ctx = context(&args)?;
        ctx.register_table(
            "current",
            Arc::new(IcebergStaticTableProvider::try_new_from_table(table).await?),
        )?;
        assert_eq!(
            scalar(
                &ctx,
                "SELECT count(*) FROM current WHERE length(payload) > 0"
            )
            .await?,
            12
        );
        assert_eq!(
            scalar(&ctx, "SELECT count(*) FROM current WHERE day=2").await?,
            4
        );
        let plan = ctx
            .sql("SELECT item FROM current WHERE day=2")
            .await?
            .create_physical_plan()
            .await?;
        let plan = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        assert!(
            plan.contains("predicate:[") && !plan.contains("predicate:[]"),
            "{plan}"
        );
        Ok(())
    }
}

#[cfg(test)]
mod qualification_tests {
    use super::*;
    fn growth() -> Vec<Value> {
        (0..10).map(|day| json!({"namespace":"docspec_m1_71969fc54a0f4f4eaa49d2dd9f4c2272",
            "table":"growth","table_uuid":"fixture-table","day":day,"rows_per_day":4,
            "writer_profile":"engine-v1-uncapped","rows_equal":true,"data_files":day+1,"manifests":day+1,"data_bytes":(day+1)*315000,"binary_blake3":"fixture","release_build":true,
            "maintenance_run":format!("basin-{day}"),"maintenance_evidence":{"source":"basin","completed":true,
            "run_id":format!("basin-{day}"),"table_uuid":"fixture-table"}})).collect()
    }
    #[test]
    fn projected_workload_must_cover_the_measured_growth_targets() -> Result<()> {
        let ns = "docspec_m1_71969fc54a0f4f4eaa49d2dd9f4c2272";
        let mut reports = growth();
        let projection = projection_for(ns, &reports)?;
        assert_eq!(projection["target_files"], 3660);
        assert_eq!(projection["target_manifests"], 3660);
        let mut projected = json!({"namespace":ns,"table":"projected","projected":true,
            "projection":projection,"rows_per_day":4,"rows":14640,"data_bytes":1152900000,"writer_profile":"engine-v1-uncapped","binary_blake3":"fixture","release_build":true,"data_files":1,"manifests":1,"rows_equal":true,
            "runner_os":"linux","runner_threads":4,"runner_memory_bytes":16_u64 << 30,"scan_ms":1000});
        reports.push(projected.clone());
        assert_eq!(
            decision_report(ns, &reports)?["capacity_decision"],
            "incomplete"
        );
        projected["data_files"] = json!(3660);
        projected["manifests"] = json!(3660);
        reports.pop();
        reports.push(projected);
        assert_eq!(
            decision_report(ns, &reports)?["capacity_decision"],
            "iceberg"
        );
        reports.last_mut().unwrap()["release_build"] = json!(false);
        assert_eq!(
            decision_report(ns, &reports)?["capacity_decision"],
            "incomplete"
        );
        reports.last_mut().unwrap()["release_build"] = json!(true);
        reports.last_mut().unwrap()["rows_per_day"] = json!(1);
        assert_eq!(
            decision_report(ns, &reports)?["capacity_decision"],
            "incomplete"
        );
        reports.last_mut().unwrap()["rows_per_day"] = json!(4);
        reports.last_mut().unwrap()["rows"] = json!(4);
        assert_eq!(
            decision_report(ns, &reports)?["capacity_decision"],
            "incomplete"
        );
        reports.last_mut().unwrap()["rows"] = json!(14640);
        reports.last_mut().unwrap()["data_bytes"] = json!(1);
        assert_eq!(
            decision_report(ns, &reports)?["capacity_decision"],
            "incomplete"
        );
        reports.last_mut().unwrap()["data_bytes"] = json!(1152900000);
        reports.last_mut().unwrap()["scan_ms"] = json!(60000);
        assert_eq!(
            decision_report(ns, &reports)?["capacity_decision"],
            "dated_parquet"
        );
        reports[0]["namespace"] = json!("different");
        assert!(projection_for(ns, &reports).is_err());
        Ok(())
    }
    #[test]
    fn compaction_codec_comes_from_a_file_the_writer_did_not_create() {
        let current =
            std::collections::BTreeSet::from(["old.parquet".into(), "compacted.parquet".into()]);
        let writers = std::collections::BTreeSet::from(["old.parquet".into()]);
        assert_eq!(
            compacted_file(&current, &writers).as_deref(),
            Some("compacted.parquet")
        );
        assert!(compacted_file(&writers, &writers).is_none());
    }
}

fn projection_for(namespace: &str, reports: &[Value]) -> Result<Value> {
    let mut growth = reports
        .iter()
        .filter(|r| r["table"] == "growth" && r["maintenance_run"].is_string())
        .collect::<Vec<_>>();
    ensure!(growth.len() >= 10, "ten growth observations required");
    growth.sort_by_key(|r| r["day"].as_u64().unwrap_or(u64::MAX));
    let uuid = growth[0]["table_uuid"]
        .as_str()
        .context("growth table UUID missing")?;
    let rows = growth[0]["rows_per_day"]
        .as_u64()
        .context("growth workload missing")?;
    let mut runs = std::collections::BTreeSet::new();
    let binary = growth[0]["binary_blake3"]
        .as_str()
        .context("growth binary identity missing")?
        .to_owned();
    let mut points = Vec::new();
    let mut volumes = Vec::new();
    for report in growth {
        let evidence = &report["maintenance_evidence"];
        ensure!(
            report["namespace"] == namespace
                && report["table_uuid"] == uuid
                && report["rows_per_day"] == rows
                && report["writer_profile"] == "engine-v1-uncapped"
                && report["release_build"] == true
                && report["binary_blake3"] == binary,
            "growth workload identity changed"
        );
        let run = report["maintenance_run"]
            .as_str()
            .context("maintenance identity missing")?;
        ensure!(
            runs.insert(run.to_owned())
                && evidence["source"] == "basin"
                && evidence["completed"] == true
                && evidence["run_id"] == run
                && evidence["table_uuid"] == uuid,
            "maintenance evidence missing or inconsistent"
        );
        let day = report["day"].as_u64().context("growth day missing")?;
        let files = report["data_files"]
            .as_u64()
            .context("growth file count missing")?;
        let manifests = report["manifests"]
            .as_u64()
            .context("growth manifest count missing")?;
        ensure!(report["rows_equal"].is_boolean(), "row comparison missing");
        volumes.push((
            day,
            report["data_bytes"]
                .as_u64()
                .context("growth byte count missing")?,
        ));
        points.push((day, files, manifests, run.to_owned()));
    }
    let mut files_per_ten_years = 0;
    let mut manifests_per_ten_years = 0;
    for pair in points.windows(2) {
        let days = pair[1]
            .0
            .checked_sub(pair[0].0)
            .filter(|d| *d > 0)
            .context("growth days must advance")?;
        // Precommitted conservative projection: the largest observed positive
        // per-day increase, rounded up, for 3,650 further days plus the last head.
        files_per_ten_years = files_per_ten_years.max(
            pair[1]
                .1
                .saturating_sub(pair[0].1)
                .checked_mul(3650)
                .context("projection overflow")?
                .div_ceil(days),
        );
        manifests_per_ten_years = manifests_per_ten_years.max(
            pair[1]
                .2
                .saturating_sub(pair[0].2)
                .checked_mul(3650)
                .context("projection overflow")?
                .div_ceil(days),
        );
    }
    let last = points.last().context("growth points missing")?;
    let mut byte_growth = 0u64;
    for pair in volumes.windows(2) {
        let days = pair[1].0 - pair[0].0;
        byte_growth = byte_growth.max(
            pair[1]
                .1
                .saturating_sub(pair[0].1)
                .checked_mul(3650)
                .context("projection overflow")?
                .div_ceil(days),
        );
    }
    let target_days = last.0.checked_add(3651).context("projection overflow")?;
    let target_rows = rows
        .checked_mul(target_days)
        .context("projection overflow")?;
    let target_bytes = volumes
        .last()
        .context("growth volume missing")?
        .1
        .checked_add(byte_growth)
        .context("projection overflow")?;
    let basis = blake3::hash(&serde_json::to_vec(&(
        namespace, uuid, rows, &binary, &points, &volumes,
    ))?)
    .to_hex()
    .to_string();
    Ok(
        json!({"namespace":namespace,"basis":basis,"days":3650,"growth_table_uuid":uuid,"rows_per_day":rows,"binary_blake3":binary,"writer_profile":"engine-v1-uncapped","target_days":target_days,"target_rows":target_rows,"target_data_bytes":target_bytes,
        "target_files":last.1.checked_add(files_per_ten_years).context("projection overflow")?,
        "target_manifests":last.2.checked_add(manifests_per_ten_years).context("projection overflow")?,
        "maintenance_runs":runs,"method":"last head plus 3650 times largest observed positive per-day growth, rounded up"}),
    )
}

fn compacted_file(
    current: &std::collections::BTreeSet<String>,
    writer_files: &std::collections::BTreeSet<String>,
) -> Option<String> {
    current.difference(writer_files).next().cloned()
}

fn decision_report(namespace: &str, reports: &[Value]) -> Result<Value> {
    let projection = projection_for(namespace, reports)?;
    let growth = reports
        .iter()
        .filter(|r| r["table"] == "growth" && r["maintenance_run"].is_string())
        .collect::<Vec<_>>();
    let projected = reports.iter().filter(|r| r["projected"] == true).find(|r| {
        r["namespace"] == namespace
            && r["table"] == "projected"
            && r["release_build"] == true
            && r["rows_per_day"] == projection["rows_per_day"]
            && r["writer_profile"] == projection["writer_profile"]
            && r["binary_blake3"] == projection["binary_blake3"]
            && r["rows"]
                .as_u64()
                .is_some_and(|n| n >= projection["target_rows"].as_u64().unwrap_or(u64::MAX))
            && r["data_bytes"]
                .as_u64()
                .is_some_and(|n| n >= projection["target_data_bytes"].as_u64().unwrap_or(u64::MAX))
            && r["projection"] == projection
            && r["data_files"]
                .as_u64()
                .is_some_and(|n| n >= projection["target_files"].as_u64().unwrap_or(u64::MAX))
            && r["manifests"]
                .as_u64()
                .is_some_and(|n| n >= projection["target_manifests"].as_u64().unwrap_or(u64::MAX))
    });
    let smallest = projected.is_some_and(|r| {
        r["runner_os"] == "linux"
            && r["runner_threads"] == 4
            && r["runner_memory_bytes"]
                .as_u64()
                .is_some_and(|n| (14_u64 << 30..=16_u64 << 30).contains(&n))
    });
    let rows_equal = growth.iter().all(|r| r["rows_equal"] == true)
        && projected.is_some_and(|r| r["rows_equal"] == true);
    let capacity = format_verdict(
        growth.len(),
        rows_equal,
        projected
            .and_then(|r| r["scan_ms"].as_u64())
            .unwrap_or(u64::MAX),
        smallest,
    );
    let guarded = reports.iter().any(|r| {
        r["namespace"] == namespace
            && r["server_snapshot_conflict"] == true
            && r["repeat_work_list_rows"] == 0
            && r["repeat_commits"] == 0
            && r["duplicate_rows"] == 0
    });
    let codec = growth
        .iter()
        .any(|r| r["compaction_codec_verified"] == true);
    let cap = cap_decision(namespace, reports)?;
    let complete = guarded && codec && cap["decision"] != "incomplete" && capacity != "incomplete";
    Ok(
        json!({"decision":if complete {capacity} else {"incomplete"},"capacity_decision":capacity,
        "projection":projection,"guarded_append_qualified":guarded,"compaction_codec_qualified":codec,"caps":cap,
        "remaining": {"guarded_race_and_repeat":!guarded,"compaction_codec":!codec,"cap_comparison":cap["decision"] == "incomplete","projected_scan":capacity == "incomplete"}}),
    )
}

fn cap_decision(namespace: &str, reports: &[Value]) -> Result<Value> {
    // Each arm has one immutable table generation. A scan must name the exact
    // snapshot returned by its same-day commit; repeated scans do not add trials.
    let mut scans: [std::collections::BTreeMap<u64, (u64, u64, u64, u64)>; 2] = Default::default();
    let mut writes: [std::collections::BTreeMap<u64, (u64, u64, u64, u64)>; 2] = Default::default();
    let mut generations: [Option<String>; 2] = [None, None];
    let mut binary: Option<String> = None;
    let incomplete = || json!({"decision":"incomplete","reason":"matched table generations, workloads, snapshots and three interleaved write/scan pairs per arm required"});
    for r in reports
        .iter()
        .filter(|r| r["namespace"] == namespace && r["cap_trial"] == true)
    {
        let arm = match r["table"].as_str() {
            Some("capped") => 0,
            Some("uncapped") => 1,
            _ => continue,
        };
        let expected = if arm == 0 {
            "engine-v1-capped"
        } else {
            "engine-v1-uncapped"
        };
        let Some(uuid) = r["table_uuid"].as_str() else {
            return Ok(incomplete());
        };
        let Some(pin) = r["binary_blake3"].as_str() else {
            return Ok(incomplete());
        };
        if r["release_build"] != true
            || r["writer_profile"] != expected
            || generations[arm].as_deref().is_some_and(|id| id != uuid)
            || binary.as_deref().is_some_and(|id| id != pin)
        {
            return Ok(incomplete());
        }
        generations[arm] = Some(uuid.to_owned());
        binary = Some(pin.to_owned());
        if let Some(ms) = r["scan_ms"].as_u64() {
            let (Some(day), Some(rows), Some(total), Some(snapshot), Some(start)) = (
                r["day"].as_u64(),
                r["rows_per_day"].as_u64(),
                r["rows"].as_u64(),
                r["snapshot"].as_u64(),
                r["scan_started_at_ns"].as_u64(),
            ) else {
                return Ok(incomplete());
            };
            if r["rows_equal"] != true
                || rows.checked_mul(day + 1) != Some(total)
                || scans[arm]
                    .insert(day, (rows, snapshot, start, ms))
                    .is_some()
            {
                return Ok(incomplete());
            }
        }
        if let Some(batches) = r["batches"].as_array() {
            for b in batches {
                if b["commits"] != 1 {
                    continue;
                }
                let (Some(day), Some(rows), Some(snapshot), Some(start), Some(ms)) = (
                    b["day"].as_u64(),
                    b["work_list_rows"].as_u64(),
                    b["committed_snapshot"].as_u64(),
                    b["started_at_ns"].as_u64(),
                    b["commit_ms"].as_u64(),
                ) else {
                    return Ok(incomplete());
                };
                if rows == 0
                    || writes[arm]
                        .insert(day, (rows, snapshot, start, ms))
                        .is_some()
                {
                    return Ok(incomplete());
                }
            }
        }
    }
    if scans.iter().any(|a| a.len() < 3)
        || writes.iter().any(|a| a.len() < 3)
        || scans[0].keys().ne(scans[1].keys())
        || writes[0].keys().ne(writes[1].keys())
        || scans[0].keys().ne(writes[0].keys())
    {
        return Ok(incomplete());
    }
    let mut scan_order = Vec::new();
    let mut write_order = Vec::new();
    let mut scan_ms = [Vec::new(), Vec::new()];
    let mut commit_ms = [Vec::new(), Vec::new()];
    for day in scans[0].keys() {
        for arm in 0..2 {
            let scan = scans[arm][day];
            let write = writes[arm][day];
            if scan.0 != write.0
                || scan.1 != write.1
                || scan.0 != scans[1 - arm][day].0
                || write.0 != writes[1 - arm][day].0
            {
                return Ok(incomplete());
            }
            scan_order.push((scan.2, arm));
            write_order.push((write.2, arm));
            scan_ms[arm].push(scan.3);
            commit_ms[arm].push(write.3);
        }
    }
    scan_order.sort_unstable();
    write_order.sort_unstable();
    if scan_order.windows(2).filter(|w| w[0].1 != w[1].1).count() < 5
        || write_order.windows(2).filter(|w| w[0].1 != w[1].1).count() < 5
    {
        return Ok(incomplete());
    }
    let helps = scan_ms[0].iter().max() < scan_ms[1].iter().min()
        || commit_ms[0].iter().max() < commit_ms[1].iter().min();
    Ok(
        json!({"decision":if helps {"keep_caps"} else {"drop_caps"},"scan_ms":scan_ms,"commit_ms":commit_ms,
        "binary_blake3":binary,"table_generations":generations,
        "rule":"non-overlapping capped versus uncapped cost ranges after three matched interleaved trials"}),
    )
}

async fn day_count(table: &Table, day: i32, rows: usize, args: &Args) -> Result<i64> {
    if table.metadata().current_snapshot().is_none() {
        return Ok(0);
    }
    let ctx = context(args)?;
    ctx.register_table(
        "current",
        Arc::new(IcebergStaticTableProvider::try_new_from_table(table.clone()).await?),
    )?;
    let count = scalar(
        &ctx,
        &format!("SELECT count(*) FROM current WHERE day={day}"),
    )
    .await?;
    if count != 0 {
        let distinct = scalar(
            &ctx,
            &format!(
                "SELECT count(*) FROM (SELECT item FROM current WHERE day={day} GROUP BY item)"
            ),
        )
        .await?;
        let bad = scalar(&ctx,&format!("SELECT count(*) FROM current WHERE day={day} AND (item < 0 OR item >= {rows} OR payload <> concat(cast(day AS varchar),'/',cast(item AS varchar),'/probe'))")).await?;
        ensure!(
            count == rows as i64 && distinct == count && bad == 0,
            "partial or duplicate day refuses ingest"
        );
    }
    Ok(count)
}

async fn ingest_day(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    day: i32,
    args: &Args,
) -> Result<Value> {
    let mut written: Option<Vec<DataFile>> = None;
    let mut write_ms = 0;
    let started = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos() as u64;
    for retry in 0..=3 {
        let table = catalog.load_table(ident).await?;
        if day_count(&table, day, args.rows_per_day, args).await? != 0 {
            return Ok(
                json!({"day":day,"work_list_rows":0,"commits":0,"files":[],"write_ms":write_ms,"commit_ms":0,"started_at_ns":started}),
            );
        }
        if written.is_none() {
            let start = Instant::now();
            written = Some(files(&table, day, args.rows_per_day, args.table == "capped").await?);
            write_ms = start.elapsed().as_millis() as u64;
        }
        let start = Instant::now();
        if let Ok(committed) = append(
            catalog,
            &table,
            written.as_ref().context("written files missing")?.clone(),
        )
        .await
        {
            let commit_ms = start.elapsed().as_millis() as u64;
            let inventory = written.as_ref().context("written files missing")?.iter().map(|f|
                json!({"path":f.file_path(),"bytes":f.file_size_in_bytes(),"rows":f.record_count()})).collect::<Vec<_>>();
            return Ok(
                json!({"day":day,"work_list_rows":args.rows_per_day,"commits":1,"files":inventory,
                "write_ms":write_ms,"commit_ms":commit_ms,"retries":retry,"started_at_ns":started,"committed_snapshot":committed.metadata().current_snapshot_id()}),
            );
        }
    }
    anyhow::bail!("append did not settle within three retries")
}

#[cfg(test)]
mod layout_tests {
    use super::*;
    use parquet::arrow::async_reader::AsyncFileReader;

    #[tokio::test]
    async fn both_arms_use_the_engine_writer_and_their_pinned_native_layout() -> Result<()> {
        let root = tempfile::tempdir()?;
        let catalog = docspec::storage::local_catalog(root.path()).await?;
        let ns = NamespaceIdent::new("m1_layout".into());
        catalog.create_namespace(&ns, HashMap::new()).await?;
        for capped in [true, false] {
            let name = if capped { "capped" } else { "uncapped" };
            let ident = TableIdent::new(ns.clone(), name.into());
            let table = create_table(
                catalog.as_ref(),
                &ident,
                &root.path().join(name).to_string_lossy(),
                5000,
            )
            .await?;
            assert!(files(&table, 0, 5000, !capped).await.is_err());
            let written = files(&table, 0, 5000, capped).await?;
            assert!(!written.is_empty());
            let mut groups = Vec::new();
            for file in &written {
                let mut reader = iceberg::arrow::ArrowFileReader::new(
                    iceberg::io::FileMetadata {
                        size: file.file_size_in_bytes(),
                    },
                    table
                        .file_io()
                        .new_input(file.file_path())?
                        .reader()
                        .await?,
                );
                let footer = reader.get_metadata(None).await?;
                groups.extend(footer.row_groups().iter().map(|g| g.num_rows()));
            }
            assert_eq!(groups.iter().sum::<i64>(), 5000);
            if capped {
                assert!(groups.len() > 1 && groups.iter().all(|n| *n <= 2048));
            } else {
                assert_eq!(groups, vec![5000]);
            }
        }
        Ok(())
    }

    #[test]
    fn failure_reports_keep_stage_and_refuse_to_print_dynamic_secrets() {
        let error = anyhow::anyhow!(String::from("Bearer never-report-this-value"));
        let report = failure_report(&error, "Observe");
        assert_eq!(report["stage"], "Observe");
        assert_eq!(report["status"], "failed");
        assert!(!report.to_string().contains("never-report-this-value"));
    }
}

#[cfg(test)]
mod metadata_tests {
    use super::*;
    #[tokio::test]
    async fn explicit_small_row_groups_cannot_undercharge_writer_metadata() -> Result<()> {
        let root = tempfile::tempdir()?;
        let catalog = docspec::storage::local_catalog(root.path()).await?;
        let ns = NamespaceIdent::new("m1_metadata".into());
        catalog.create_namespace(&ns, HashMap::new()).await?;
        let table = create_table(
            catalog.as_ref(),
            &TableIdent::new(ns, "uncapped".into()),
            &root.path().join("table").to_string_lossy(),
            200,
        )
        .await?;
        let budget = docspec::budget::Budget::new(docspec::budget::Limits {
            compute_threads: 1,
            memory_bytes: 1 << 30,
            scratch_bytes: 1 << 20,
            batch_rows: 65536,
            max_batch_bytes: 64 << 20,
            max_metadata_bytes: 32 << 10,
        })?;
        let input = futures::stream::iter([Ok(batch(&table, 0, 0, 200)?)]);
        let error = docspec::storage::write_batches_with_layout(
            &table,
            input,
            &budget,
            docspec::storage::WriteLayout {
                file_bytes: 128 << 20,
                row_group_rows: 1,
                row_group_bytes: 32 << 20,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), "metadata_capacity");
        Ok(())
    }
}

#[cfg(test)]
mod cap_qualification_tests {
    use super::*;
    fn trials() -> Vec<Value> {
        let ns = "docspec_m1_71969fc54a0f4f4eaa49d2dd9f4c2272";
        let mut values = Vec::new();
        for day in 0..3 {
            for arm in 0..2 {
                let table = if arm == 0 { "capped" } else { "uncapped" };
                let profile = if arm == 0 {
                    "engine-v1-capped"
                } else {
                    "engine-v1-uncapped"
                };
                let cost = if arm == 0 { 1 } else { 10 };
                values.push(json!({"namespace":ns,"table":table,"table_uuid":table,"writer_profile":profile,
                "binary_blake3":"fixture","release_build":true,"cap_trial":true,"batches":[{"day":day,"commits":1,"work_list_rows":4,
                    "commit_ms":cost,"started_at_ns":100+day*2+arm,"committed_snapshot":1000+day}]}));
                values.push(json!({"namespace":ns,"table":table,"table_uuid":table,"writer_profile":profile,
                "binary_blake3":"fixture","release_build":true,"cap_trial":true,"day":day,"rows_per_day":4,"rows":(day+1)*4,
                "rows_equal":true,"scan_ms":cost,"scan_started_at_ns":200+day*2+arm,"snapshot":1000+day}));
            }
        }
        values
    }
    #[test]
    fn cap_costs_require_equal_workload_snapshot_and_interleaved_scans() -> Result<()> {
        let ns = "docspec_m1_71969fc54a0f4f4eaa49d2dd9f4c2272";
        let reports = trials();
        assert_eq!(cap_decision(ns, &reports)?["decision"], "keep_caps");
        for field in [
            "rows_per_day",
            "rows",
            "snapshot",
            "table_uuid",
            "binary_blake3",
            "release_build",
        ] {
            let mut bad = reports.clone();
            bad[1][field] = json!(99999);
            assert_eq!(cap_decision(ns, &bad)?["decision"], "incomplete", "{field}");
        }
        let mut bad = reports;
        for (index, row) in bad
            .iter_mut()
            .enumerate()
            .filter(|(_, r)| r["scan_ms"].is_number())
        {
            row["scan_started_at_ns"] = json!(if row["table"] == "capped" {
                index
            } else {
                100 + index
            });
        }
        assert_eq!(cap_decision(ns, &bad)?["decision"], "incomplete");
        Ok(())
    }
}

#[cfg(test)]
mod observation_evidence_tests {
    use super::*;
    #[test]
    fn compactor_attribution_requires_same_generation_and_complete_successful_writes() -> Result<()>
    {
        let identity = json!({"namespace":"ns","table":"growth","table_uuid":"uuid","writer_profile":"engine-v1-uncapped","binary_blake3":"pin","release_build":true});
        let mut previous = identity.clone();
        previous["rows_per_day"] = json!(4);
        previous["rows_equal"] = json!(true);
        previous["day"] = json!(0);
        previous["data_paths"] = json!(["old"]);
        let mut record = identity.clone();
        record["batches"] = json!([{"day":1,"commits":1,"work_list_rows":4,"committed_snapshot":7,"files":[{"path":"new","bytes":8,"rows":4}]}]);
        let known = writer_evidence(Some(&previous), &[record.clone()], &identity, 1, 4)?;
        assert!(known.contains("old") && known.contains("new"));
        for field in [
            "namespace",
            "table",
            "table_uuid",
            "writer_profile",
            "binary_blake3",
            "release_build",
            "rows_per_day",
            "rows_equal",
        ] {
            let mut bad = previous.clone();
            bad[field] = json!("wrong");
            assert!(
                writer_evidence(Some(&bad), &[record.clone()], &identity, 1, 4).is_err(),
                "{field}"
            );
        }
        for field in [
            "namespace",
            "table",
            "table_uuid",
            "writer_profile",
            "binary_blake3",
            "release_build",
        ] {
            let mut bad = record.clone();
            bad[field] = json!("wrong");
            assert!(
                writer_evidence(Some(&previous), &[bad], &identity, 1, 4).is_err(),
                "{field}"
            );
        }
        for files in [
            json!([]),
            json!([{"path":"new","bytes":8,"rows":3}]),
            json!([{"path":"new","bytes":0,"rows":4}]),
        ] {
            let mut bad = record.clone();
            bad["batches"][0]["files"] = files;
            assert!(writer_evidence(Some(&previous), &[bad], &identity, 1, 4).is_err());
        }
        let mut noop = record;
        noop["batches"][0]["commits"] = json!(0);
        noop["batches"][0]["files"] = json!([]);
        noop["batches"][0]["work_list_rows"] = json!(0);
        assert!(writer_evidence(Some(&previous), &[noop], &identity, 1, 4).is_err());
        assert!(writer_evidence(Some(&previous), &[], &identity, -1, 4).is_err());
        Ok(())
    }
}

#[cfg(test)]
mod build_profile_tests {
    use super::*;
    #[test]
    fn debug_build_cannot_start_a_remote_measurement() {
        assert!(validate_build_profile(true).is_err());
        assert!(validate_build_profile(false).is_ok());
    }
}

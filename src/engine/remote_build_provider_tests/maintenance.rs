//! Fresh isolated exact-key deletion and portable catalog-loss restoration.
use super::*;
use crate::export::{ExportPolicy, ExportRows, ExportSpec};
use crate::storage::remote::{RemoteStorage, objects::RemoteRemoval};
use std::{
    io::{Read, Write},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Setup {
    dataset: Id,
    prefix: String,
    generation: uuid::Uuid,
    source: ReleasePin,
    current: crate::export::ExportPin,
    unused_request: ExportSpec,
    unused_object: ObjectRef,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreHandoff {
    point: crate::restore::RestorePointRef,
    prefix: String,
    removal: RemovalStatus,
    source_generation: uuid::Uuid,
    target_generation: uuid::Uuid,
}

fn read_handoff<T: serde::de::DeserializeOwned>(directory: &Path, name: &str) -> Result<T> {
    let mut bytes = Vec::new();
    std::fs::File::open(directory.join(name))?
        .take(1 << 20)
        .read_to_end(&mut bytes)?;
    if bytes.len() == 1 << 20 {
        return Err(Error::refusal("provider_scope", "handoff exceeds bound"));
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn checked<T>(phase: &str, result: Result<T>) -> Result<T> {
    if let Err(error) = &result {
        // Source-free engine refusals carry the literal local reason. Keep only
        // its first line, excluding an optional Error Display backtrace.
        let refusal = std::error::Error::source(error).is_none().then(|| {
            error
                .to_string()
                .lines()
                .next()
                .unwrap_or(error.code())
                .to_owned()
        });
        let mut native = None;
        let mut retryable = None;
        let mut status = None;
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error);
        for _ in 0..8 {
            let Some(current) = cause else {
                break;
            };
            if let Some(error) = current.downcast_ref::<iceberg::Error>() {
                native = Some(error.kind().into_static());
                retryable = Some(error.retryable());
            }
            if let Some(error) = current.downcast_ref::<reqwest_rest::Error>() {
                status = error.status().map(|status| status.as_u16());
            }
            cause = current.source();
        }
        // Literal phase context and stable kind/status only: no upstream message,
        // URL, response body, headers or arbitrary error context enters receipts.
        eprintln!(
            "{}",
            json!({"phase":phase,"code":error.code(),
            "native_kind":native,"retryable":retryable,"http_status":status,"refusal":refusal})
        );
    }
    result
}

fn limits() -> Limits {
    Limits {
        compute_threads: 1,
        memory_bytes: 256 << 20,
        scratch_bytes: 16 << 20,
        batch_rows: 8,
        max_batch_bytes: 1 << 20,
        max_metadata_bytes: 4 << 20,
    }
}

async fn configured(
    prefix: &str,
    generation: uuid::Uuid,
    maintenance: bool,
) -> Result<RemoteStorage> {
    let mut properties = HashMap::from([
        ("uri".into(), env("DOCSPEC_CATALOG_URI")?),
        ("warehouse".into(), env("DOCSPEC_CATALOG_WAREHOUSE")?),
        ("s3.endpoint".into(), env("DOCSPEC_S3_ENDPOINT")?),
        ("s3.region".into(), "auto".into()),
        ("s3.path-style-access".into(), "true".into()),
        (
            "docspec.namespace-generation".into(),
            generation.to_string(),
        ),
    ]);
    if maintenance {
        properties.insert("docspec.maintenance-mode".into(), "quiescent".into());
    }
    storage::remote::remote_storage(
        prefix,
        properties,
        HashMap::from([
            ("token".into(), "DOCSPEC_CATALOG_TOKEN".into()),
            ("s3.access-key-id".into(), "DOCSPEC_S3_ACCESS_KEY_ID".into()),
            (
                "s3.secret-access-key".into(),
                "DOCSPEC_S3_SECRET_ACCESS_KEY".into(),
            ),
        ]),
    )
    .await
}

fn receipt(directory: &Path, name: &str, value: &serde_json::Value) -> Result<()> {
    let mut file = tempfile::NamedTempFile::new_in(directory)?;
    file.write_all(&serde_json::to_vec(value)?)?;
    file.as_file().sync_all()?;
    file.persist_noclobber(directory.join(name))
        .map_err(|error| Error::from(error.error))?;
    std::fs::File::open(directory)?.sync_all()?;
    Ok(())
}

fn now() -> Result<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_secs())
        .map_err(|_| Error::refusal("provider_config", "clock refused"))
}

// Owned subprocess only; credentials travel in pipes, never retained evidence.
struct Issuer(std::process::Child);
impl Drop for Issuer {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}
async fn issuer(
    mode: &str,
    request: serde_json::Value,
    secrets: bool,
) -> Result<serde_json::Value> {
    let python = env("DOCSPEC_QUALIFICATION_PYTHON")?;
    let script = env("DOCSPEC_REMOVAL_ISSUER")?;
    let mut command = Command::new(python);
    command
        .arg(script)
        .arg(mode)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if !secrets {
        command.env_clear();
    }
    command.env_remove("DOCSPEC_CATALOG_TOKEN");
    let mut child = Issuer(command.spawn()?);
    child
        .0
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&request)?)?;
    let start = Instant::now();
    loop {
        if let Some(status) = child.0.try_wait()? {
            if !status.success() {
                return Err(Error::refusal(
                    "provider_authority",
                    "scoped issuer or probe refused",
                ));
            }
            let mut bytes = Vec::new();
            child
                .0
                .stdout
                .take()
                .unwrap()
                .take(16385)
                .read_to_end(&mut bytes)?;
            if bytes.len() > 16384 {
                return Err(Error::refusal(
                    "provider_authority",
                    "issuer output exceeded bound",
                ));
            }
            return Ok(serde_json::from_slice(&bytes)?);
        }
        if start.elapsed() > Duration::from_secs(60) {
            return Err(Error::refusal("provider_authority", "issuer deadline"));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
async fn authority(
    prefix: &str,
    generation: uuid::Uuid,
    objects: Vec<ObjectRef>,
    expires: u64,
) -> Result<(RemoteRemoval, serde_json::Value)> {
    let token = issuer(
        "mint",
        json!({"objects": objects, "expires": expires,
        "bucket": env("DOCSPEC_S3_BUCKET")?, "endpoint": env("DOCSPEC_S3_ENDPOINT")?}),
        true,
    )
    .await?;
    let text = |name: &str| -> Result<String> {
        token[name]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| Error::refusal("provider_authority", "issuer fields refused"))
    };
    let objects_io = storage::remote::remote_objects(
        prefix,
        HashMap::from([
            ("s3.endpoint".into(), env("DOCSPEC_S3_ENDPOINT")?),
            ("s3.region".into(), "auto".into()),
            ("s3.path-style-access".into(), "true".into()),
            ("s3.access-key-id".into(), text("access_key_id")?),
            ("s3.secret-access-key".into(), text("secret_access_key")?),
            ("s3.session-token".into(), text("session_token")?),
        ]),
        HashMap::new(),
    )?;
    Ok((
        RemoteRemoval::new(objects_io, generation, objects, expires)?,
        token,
    ))
}

pub(super) async fn hold_old_commit(directory: &Path, metadata: &str) -> iceberg::Result<()> {
    let result: Result<()> = async {
        std::fs::write(
            directory.join("old.metadata"),
            serde_json::to_vec(metadata)?,
        )?;
        let start = Instant::now();
        while !directory.join("forward-old").is_file() {
            if start.elapsed() > Duration::from_secs(180) {
                return Err(Error::refusal(
                    "provider_evidence",
                    "held old commit deadline",
                ));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    }
    .await;
    result.map_err(|_| {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "held maintenance commit refused",
        )
        .with_retryable(false)
    })
}
pub(super) fn native_outcome(
    directory: &Path,
    outcome: &iceberg::Result<iceberg::table::Table>,
) -> iceberg::Result<()> {
    std::fs::write(
        directory.join("old.native"),
        (if outcome.is_ok() {
            "accepted"
        } else {
            "refused"
        })
        .as_bytes(),
    )
    .map_err(|_| {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "native outcome receipt refused",
        )
    })
}
struct OldChild(std::process::Child);
impl Drop for OldChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

#[tokio::test]
#[ignore = "internal fresh maintenance old-actor child only"]
async fn maintenance_old_actor_child() {
    if let Err(error) = old_actor_child().await {
        eprintln!(
            "{}",
            json!({"phase":"old_actor_failed","code":error.code()})
        );
        panic!("isolated old actor child refused");
    }
}
async fn old_actor_child() -> Result<()> {
    let directory = std::path::PathBuf::from(env("DOCSPEC_MAINTENANCE_CHILD_DIR")?);
    let bytes = std::fs::read(directory.join("old.request"))?;
    if bytes.len() > 16384 {
        return Err(Error::refusal("provider_scope", "child request bound"));
    }
    let request: serde_json::Value = serde_json::from_slice(&bytes)?;
    let dataset: Id = serde_json::from_value(request["dataset"].clone())?;
    let generation: uuid::Uuid = serde_json::from_value(request["generation"].clone())?;
    let prefix = request["prefix"]
        .as_str()
        .ok_or_else(|| Error::refusal("provider_scope", "child prefix"))?;
    let mut remote = configured(prefix, generation, true).await?;
    // Resolve logical lookup, then bind the actual physical native identifier.
    let table = remote
        .catalog
        .load_table(&crate::catalog::lifecycle_ident(&dataset))
        .await?;
    let catalog = publication_race::held_generation_catalog(
        remote.catalog.clone(),
        table.identifier().clone(),
        &directory,
    );
    remote.catalog = catalog.clone();
    let engine = Engine::with_remote(directory.join("old-actor"), remote, limits()).await?;
    catalog.arm();
    let epoch = request
        .get("epoch")
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| Error::refusal("provider_scope", "child epoch differs"))
        })
        .transpose()?
        .unwrap_or(0);
    let outcome = engine.fence_operations(&dataset, epoch).await;
    if outcome.is_ok() {
        return Err(Error::refusal(
            "provider_evidence",
            "old fence unexpectedly accepted",
        ));
    }
    assert_eq!(std::fs::read(directory.join("old.native"))?, b"refused");
    receipt(
        &directory,
        "old.result",
        &json!({"pid":std::process::id(),"outcome":"provider_refused"}),
    )?;
    engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires fresh owned generation, isolated R2/Basin and trusted exact-key temporary issuer"]
async fn exact_remote_removal_and_detached_restore_reconcile_from_fresh_hosts() {
    if let Err(error) = tokio::time::timeout(Duration::from_secs(720), qualify())
        .await
        .unwrap_or_else(|_| {
            Err(Error::refusal(
                "provider_deadline",
                "maintenance qualification deadline",
            ))
        })
    {
        eprintln!(
            "{}",
            json!({"phase":"maintenance_failed", "code":error.code()})
        );
        panic!("isolated maintenance qualification failed; retain recorded references");
    }
}

async fn qualify() -> Result<()> {
    let name = env("DOCSPEC_QUALIFICATION_ID")?;
    let suffix = name
        .strip_prefix("docspec-rs-qualification-")
        .ok_or_else(|| Error::refusal("provider_scope", "fresh qualification UUID required"))?;
    let generation = uuid::Uuid::parse_str(suffix)
        .map_err(|_| Error::refusal("provider_scope", "fresh qualification UUID required"))?;
    if generation.to_string() != suffix {
        return Err(Error::refusal("provider_scope", "canonical UUID required"));
    }
    let dataset = Id::new(&name)?;
    let prefix = format!(
        "s3://{}/__r2_data_catalog/docspec-qualification/{name}/maintenance",
        env("DOCSPEC_S3_BUCKET")?
    );
    if let Some(resume) = std::env::var_os("DOCSPEC_MAINTENANCE_RESTORE_DIR") {
        return restore_saved(std::fs::canonicalize(resume)?, dataset, prefix, generation).await;
    }
    let (directory, selected, current, unused, obsolete) =
        if let Some(resume) = std::env::var_os("DOCSPEC_MAINTENANCE_RESUME_DIR") {
            let directory = std::fs::canonicalize(resume)?;
            let file = std::fs::File::open(directory.join("setup.json"))?;
            let mut bytes = Vec::new();
            file.take(16385).read_to_end(&mut bytes)?;
            if bytes.len() > 16384 {
                return Err(Error::refusal("provider_scope", "setup exceeds bound"));
            }
            let saved: Setup = serde_json::from_slice(&bytes)?;
            if saved.dataset != dataset
                || saved.prefix != prefix
                || saved.generation != generation
                || saved.source.dataset.dataset != dataset
                || saved.current.dataset != dataset
                || saved.current.source != saved.source.version_ref()
                || saved.current.id != Id::new("current")?
                || saved.unused_request.dataset != dataset
                || saved.unused_request.operation != Id::new("unused")?
                || saved.unused_request.source != saved.source.version_ref()
                || saved.unused_request.channel != Id::new("unused")?
                || saved.unused_request.expected.is_some()
                || saved.unused_request.policy != saved.current.policy
                || saved.unused_object.owner != dataset
                || !saved
                    .unused_object
                    .path
                    .starts_with(&format!("{prefix}/{dataset}/"))
                || saved.unused_object.path == saved.current.object.path
            {
                return Err(Error::refusal(
                    "provider_scope",
                    "setup differs from explicit qualification scope",
                ));
            }
            // Resume only retained setup; no input open, Build or export call.
            println!(
                "{}",
                json!({"phase":"maintenance_resume","dataset":dataset,
                "generation":generation,"host_directory":directory})
            );
            (
                directory,
                saved.source,
                saved.current,
                saved.unused_request,
                saved.unused_object,
            )
        } else {
            let raw = connect(&prefix).await?;
            let physical_root =
                iceberg::NamespaceIdent::new(format!("docspec_generation_{}", generation.simple()));
            if raw.catalog.namespace_exists(&physical_root).await? {
                return Err(Error::refusal(
                    "provider_scope",
                    "generation already exists",
                ));
            }
            let directory = tempfile::tempdir()?.keep();
            println!(
                "{}",
                json!({"phase":"maintenance_start", "dataset":dataset, "prefix":prefix,
        "generation":generation, "host_directory":directory})
            );
            let source = directory.join("input.parquet");
            let schema = Arc::new(Schema::new(vec![
                Field::new("text", DataType::Utf8, true),
                Field::new("counter", DataType::Int64, false),
                Field::new("private_canary", DataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![
                        Some("same"),
                        Some("same"),
                        None,
                        Some(""),
                    ])),
                    Arc::new(Int64Array::from(vec![1_i64, 1, 2, 2])),
                    Arc::new(StringArray::from(vec!["synthetic-private-canary"; 4])),
                ],
            )?;
            let mut writer = ArrowWriter::try_new(std::fs::File::create(&source)?, schema, None)?;
            writer.write(&batch)?;
            writer.close()?;
            let data = Engine::with_remote(
                directory.join("data"),
                configured(&prefix, generation, false).await?,
                limits(),
            )
            .await?;
            let selected = data
                .run(&RunSpec {
                    dataset: dataset.clone(),
                    operation: Id::new("initial")?,
                    base: None,
                    relationship: None,
                    input: source.to_string_lossy().into_owned(),
                    expected_digest: storage::digest(&source)?.0,
                    sql: "SELECT * FROM input".into(),
                    definition: "maintenance-v1".into(),
                    partition_by: Vec::new(),
                })
                .await?;
            let spec = ExportSpec {
                dataset: dataset.clone(),
                operation: Id::new("current")?,
                source: selected.version_ref(),
                channel: Id::new("public")?,
                expected: None,
                policy: ExportPolicy {
                    group: "source".into(),
                    rows: ExportRows::All,
                    columns: vec!["counter".into(), "text".into()],
                    ..ExportPolicy::default()
                },
            };
            let current = data.export(&spec).await?;
            let unused = ExportSpec {
                operation: Id::new("unused")?,
                channel: Id::new("unused")?,
                ..spec
            };
            assert_eq!(
                data.export_until(&unused, "transfer")
                    .await
                    .err()
                    .unwrap()
                    .code(),
                "injected_failure"
            );
            let view = data.existing_lifecycle_view(&dataset).await?;
            let material: crate::engine::export::ExportMaterialized = data
                .inner
                .lifecycle
                .get(&view, "export-materialized/unused")
                .await?
                .ok_or_else(|| {
                    Error::refusal("provider_evidence", "unused transferred export missing")
                })?;
            let obsolete = material.object;
            assert_ne!(obsolete.path, current.object.path);
            receipt(
                &directory,
                "setup.json",
                &json!({"dataset":dataset,"prefix":prefix,"generation":generation,
        "source":selected,"current":current,"unused_request":unused,"unused_object":obsolete}),
            )?;
            std::fs::remove_file(&source)?;
            data.shutdown().await?;
            drop(data);
            (directory, selected, current, unused, obsolete)
        };
    let engine = checked(
        "maintenance_open",
        Engine::with_remote(
            directory.join("maintenance"),
            configured(&prefix, generation, true).await?,
            limits(),
        )
        .await,
    )?;
    checked("maintenance_selected_source", engine.latest(&dataset).await)
        .map(|live| assert_eq!(live, selected))?;
    checked(
        "maintenance_selected_export",
        engine.latest_export(&dataset, &Id::new("public")?).await,
    )
    .map(|live| assert_eq!(live, current))?;
    let view = checked(
        "maintenance_retirement_view",
        engine.existing_lifecycle_view(&dataset).await,
    )?;
    let material: crate::engine::export::ExportMaterialized = checked(
        "maintenance_unused_lookup",
        engine
            .inner
            .lifecycle
            .get(&view, "export-materialized/unused")
            .await,
    )?
    .ok_or_else(|| Error::refusal("provider_evidence", "retained unused export missing"))?;
    if material.object != obsolete
        || material.rows != current.rows
        || material.fields != current.fields
    {
        return Err(Error::refusal(
            "provider_scope",
            "saved unused export differs from retained completion",
        ));
    }
    let saved_retirement: Option<Retirement> = checked(
        "maintenance_retirement_lookup",
        engine
            .inner
            .lifecycle
            .get(&view, "retirement/end_unused")
            .await,
    )?;
    let retirement = if let Some(saved) = saved_retirement {
        if saved.plan.dataset != dataset
            || saved.plan.retirement != Id::new("end_unused")?
            || saved.plan.scope
                != (RetirementScope::ExportRecovery {
                    operation: unused.operation.clone(),
                })
        {
            return Err(Error::refusal("provider_scope", "saved retirement differs"));
        }
        saved.plan
    } else {
        checked(
            "maintenance_retirement_plan",
            engine
                .plan_export_retirement(
                    &dataset,
                    &Id::new("end_unused")?,
                    &unused.operation,
                    "unselected export ended",
                )
                .await,
        )?
    };
    checked(
        "maintenance_retirement_execute",
        engine.retire_export(&retirement).await,
    )?;
    let point = checked(
        "maintenance_point_capture",
        engine
            .create_restore_point(&dataset, &Id::new("portable")?)
            .await,
    )?;
    let budget = &engine.inner.lifecycle.budget;
    let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes)?);
    let closure: crate::restore::remote::RemoteRestorePoint = checked(
        "maintenance_point_decode",
        inputs::read_manifest(
            &engine.inner.objects,
            &point.manifest,
            budget,
            &mut 0,
            &memory,
        )
        .await,
    )?;
    drop(memory);
    assert!(closure.objects.contains(&current.object));
    assert!(!closure.objects.contains(&obsolete));
    let source_object = selected.dataset.groups["source"]
        .objects
        .iter()
        .find(|o| o.path.ends_with(".parquet"))
        .ok_or_else(|| Error::refusal("provider_evidence", "source payload missing"))?
        .clone();
    assert!(closure.objects.contains(&source_object));
    let removal = Id::new("one_unused_file")?;
    let preview = checked(
        "maintenance_removal_preview",
        engine
            .plan_removal(
                &dataset,
                &removal,
                &[
                    obsolete.clone(),
                    current.object.clone(),
                    source_object.clone(),
                ],
                "exact unused file",
            )
            .await,
    )?;
    for object in [&current.object, &source_object] {
        assert!(
            preview
                .inspection
                .objects
                .iter()
                .find(|entry| entry.object == *object)
                .unwrap()
                .blocker
                .is_some()
        );
    }
    assert!(
        preview
            .inspection
            .objects
            .iter()
            .find(|entry| entry.object == obsolete)
            .unwrap()
            .blocker
            .is_none()
    );
    let reader = checked(
        "maintenance_reader_open",
        engine.read_export(&current).await,
    )?;
    let other = engine.clone();
    let plan = preview.clone();
    let mut preparation = tokio::spawn(async move { other.prepare_remote_removal(&plan).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut preparation)
            .await
            .is_err()
    );
    drop(reader);
    let saved = tokio::time::timeout(Duration::from_secs(90), preparation)
        .await
        .map_err(|_| Error::refusal("provider_deadline", "removal preparation deadline"))?
        .map_err(|_| Error::refusal("provider_evidence", "removal worker failed"))??;
    let exact = saved
        .intent
        .objects
        .iter()
        .map(|i| saved.intent.plan.inspection.objects[*i].object.clone())
        .collect::<Vec<_>>();
    assert_eq!(exact, vec![obsolete.clone()]);
    receipt(
        &directory,
        "removal.json",
        &json!({"point":point,"status":saved,"dataset":dataset,
        "prefix":prefix,"generation":generation}),
    )?;
    println!(
        "{}",
        json!({"phase":"removal_reserved","point":point,"intent":saved.intent,"current":current,"source":selected})
    );
    let deletion_expires = now()?.saturating_add(120);
    let (actor, _) = checked(
        "maintenance_delete_authority",
        authority(&prefix, generation, exact.clone(), deletion_expires).await,
    )?;
    assert_eq!(
        engine
            .resume_removal_remote_until(&dataset, &removal, actor, "removal-unlink")
            .await
            .err()
            .unwrap()
            .code(),
        "injected_failure"
    );
    engine.shutdown().await?;
    drop(engine);
    let engine = Engine::with_remote(
        directory.join("removal-retry"),
        configured(&prefix, generation, true).await?,
        limits(),
    )
    .await?;
    let (actor, _) = checked(
        "maintenance_delete_authority",
        authority(&prefix, generation, exact.clone(), deletion_expires).await,
    )?;
    let complete = checked(
        "maintenance_delete_resume",
        engine
            .resume_removal_remote(&dataset, &removal, actor)
            .await,
    )?;
    assert!(complete.complete);
    assert_eq!(complete.reconciled_absent_bytes, obsolete.bytes);
    assert_eq!(complete.confirmed_unlinked_bytes, 0);
    let expires = now()?.saturating_add(20);
    let (_, token) = authority(&prefix, generation, exact, expires).await?;
    let probe = issuer(
        "probe",
        json!({"credentials":token,"expires":expires,"bucket":env("DOCSPEC_S3_BUCKET")?,
        "endpoint":env("DOCSPEC_S3_ENDPOINT")?,"allowed":obsolete.path,"wrong":current.object.path,
        "canary":format!("{prefix}/issuer-canary-{}.empty",uuid::Uuid::new_v4())}),
        false,
    )
    .await?;
    println!(
        "{}",
        json!({"phase":"removal_complete","status":complete,"authority_probe":probe})
    );
    // Expiry prevents new requests, not late accepted effects. The only deletion
    // key is already absent, permanently unavailable and outside the live point.
    // Drain both issued sessions before changing the catalog generation.
    while now()? <= deletion_expires.saturating_add(2) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let handoff = RestoreHandoff {
        point,
        prefix,
        removal: complete,
        source_generation: generation,
        target_generation: uuid::Uuid::new_v4(),
    };
    receipt(&directory, "restore.json", &serde_json::to_value(&handoff)?)?;
    finish_restore(&directory, &directory, engine, handoff, selected, current).await
}

async fn restore_saved(
    directory: std::path::PathBuf,
    dataset: Id,
    prefix: String,
    generation: uuid::Uuid,
) -> Result<()> {
    let setup: Setup = read_handoff(&directory, "setup.json")?;
    let saved: RestoreHandoff = read_handoff(&directory, "restore.json")?;
    if setup.dataset != dataset
        || setup.prefix != prefix
        || setup.generation != generation
        || setup.source.dataset.dataset != dataset
        || setup.current.dataset != dataset
        || setup.current.source != setup.source.version_ref()
        || saved.prefix != prefix
        || saved.source_generation != generation
        || saved.target_generation == generation
        || saved.target_generation.is_nil()
        || saved.point.dataset != dataset
        || saved.point.id != Id::new("portable")?
        || saved.point.manifest.owner != dataset
        || !saved
            .point
            .manifest
            .path
            .starts_with(&format!("{prefix}/{dataset}/restore_"))
        || !saved.removal.complete
        || saved.removal.intent.plan.dataset != dataset
        || saved.removal.intent.plan.removal != Id::new("one_unused_file")?
        || saved.removal.intent.objects.len() != 1
        || saved
            .removal
            .intent
            .objects
            .first()
            .and_then(|index| saved.removal.intent.plan.inspection.objects.get(*index))
            .map(|entry| &entry.object)
            != Some(&setup.unused_object)
    {
        return Err(Error::refusal(
            "provider_scope",
            "restore handoff differs from exact completed scope",
        ));
    }
    let control = directory.join(format!("old-restore-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir(&control)?;
    println!(
        "{}",
        json!({"phase":"maintenance_restore_only","source_generation":generation,
        "target_generation":saved.target_generation,"point":saved.point,"control_directory":control})
    );
    let engine = checked(
        "maintenance_restore_source",
        Engine::with_remote(
            control.join("source-host"),
            configured(&prefix, generation, true).await?,
            limits(),
        )
        .await,
    )?;
    let view = checked(
        "maintenance_restore_source_view",
        engine.existing_lifecycle_view(&dataset).await,
    )?;
    let point: Option<crate::restore::RestorePointRef> = checked(
        "maintenance_restore_point_lookup",
        engine
            .inner
            .lifecycle
            .get(&view, "restore-point/portable")
            .await,
    )?;
    if point.as_ref() != Some(&saved.point) {
        return Err(Error::refusal("provider_scope", "live exact point differs"));
    }
    assert_eq!(
        checked(
            "maintenance_restore_source_pin",
            engine.latest(&dataset).await
        )?,
        setup.source
    );
    assert_eq!(
        checked(
            "maintenance_restore_export_pin",
            engine.latest_export(&dataset, &Id::new("public")?).await
        )?,
        setup.current
    );
    assert_eq!(
        checked(
            "maintenance_restore_removal_ack",
            engine
                .inspect_removal(&dataset, &Id::new("one_unused_file")?)
                .await
        )?,
        saved.removal
    );
    // No Build/export, source-input open, retirement, capture, DELETE or mint.
    finish_restore(
        &directory,
        &control,
        engine,
        saved,
        setup.source,
        setup.current,
    )
    .await
}

async fn finish_restore(
    directory: &Path,
    control: &Path,
    engine: Engine,
    handoff: RestoreHandoff,
    selected: ReleasePin,
    current: crate::export::ExportPin,
) -> Result<()> {
    let RestoreHandoff {
        point,
        prefix,
        source_generation: generation,
        target_generation,
        ..
    } = handoff;
    let dataset = point.dataset.clone();
    let view = checked(
        "maintenance_old_actor_epoch_view",
        engine.existing_lifecycle_view(&dataset).await,
    )?;
    let authority: Authority = checked(
        "maintenance_old_actor_epoch",
        engine.inner.lifecycle.get(&view, "authority").await,
    )?
    .ok_or_else(|| Error::refusal("provider_evidence", "source authority missing"))?;
    let old_epoch = authority.epoch;
    receipt(
        control,
        "old.request",
        &json!({"dataset":dataset,"generation":generation,"prefix":prefix,"epoch":old_epoch}),
    )?;
    let log = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(control.join("old.log"))?;
    let mut old = OldChild(
        Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "engine::remote_build_provider_tests::maintenance::maintenance_old_actor_child",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("DOCSPEC_MAINTENANCE_CHILD_DIR", control)
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?,
    );
    let start = Instant::now();
    while !control.join("old.metadata").is_file() {
        if start.elapsed() > Duration::from_secs(60) {
            return Err(Error::refusal(
                "provider_deadline",
                "old commit not captured",
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let target = checked(
        "maintenance_restore_target",
        configured(&prefix, target_generation, true).await,
    )?;
    match engine
        .restore_remote_until(&point, target.clone(), false, "restore-remote-detached")
        .await
    {
        Err(error) if error.code() == "injected_failure" => {}
        Err(error) => {
            return checked("maintenance_restore_detach", Err(error));
        }
        Ok(_) => {
            return Err(Error::refusal(
                "provider_evidence",
                "restore detach stop was not reached",
            ));
        }
    }
    engine.shutdown().await?;
    drop(engine);
    let retry = Engine::with_remote(
        directory.join("restore-retry"),
        configured(&prefix, generation, true).await?,
        limits(),
    )
    .await?;
    let restored = checked(
        "maintenance_restore_resume",
        retry.restore_remote(&point, target.clone(), false).await,
    )?;
    assert_eq!(restored.generation, target_generation);
    std::fs::write(
        control.join("forward-old"),
        b"forward unchanged native commit",
    )?;
    let start = Instant::now();
    loop {
        if let Some(status) = old.0.try_wait()? {
            assert!(status.success());
            break;
        }
        if start.elapsed() > Duration::from_secs(90) {
            return Err(Error::refusal(
                "provider_deadline",
                "old commit forward deadline",
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(std::fs::read(control.join("old.native"))?, b"refused");
    let child: serde_json::Value =
        serde_json::from_slice(&std::fs::read(control.join("old.result"))?)?;
    assert_ne!(
        child["pid"].as_u64().unwrap(),
        u64::from(std::process::id())
    );
    assert_eq!(child["outcome"], "provider_refused");
    drop(old);
    retry.shutdown().await?;
    drop(retry);
    let cold = Engine::with_remote(directory.join("restored-cold"), target, limits()).await?;
    assert_eq!(cold.latest(&dataset).await?, selected);
    assert_eq!(
        cold.latest_export(&dataset, &Id::new("public")?).await?,
        current
    );
    exports::assert_values(&cold, &current).await?;
    let mut stream = cold.read_group(&selected, "source").await?;
    let mut values = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        assert!(values.len() + batch.num_rows() <= 4);
        let count = batch
            .column_by_name("counter")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let text = arrow::compute::cast(batch.column_by_name("text").unwrap(), &DataType::Utf8)?;
        let text = text.as_any().downcast_ref::<StringArray>().unwrap();
        for row in 0..batch.num_rows() {
            values.push((
                count.value(row),
                (!text.is_null(row)).then(|| text.value(row).to_owned()),
            ));
        }
    }
    drop(stream);
    values.sort();
    assert_eq!(
        values,
        vec![
            (1, Some("same".into())),
            (1, Some("same".into())),
            (2, None),
            (2, Some(String::new()))
        ]
    );
    let view = cold.existing_lifecycle_view(&dataset).await?;
    let authority: Authority = cold
        .inner
        .lifecycle
        .get(&view, "authority")
        .await?
        .ok_or_else(|| Error::refusal("provider_evidence", "restored authority missing"))?;
    let next_epoch = authority
        .epoch
        .checked_add(1)
        .ok_or_else(|| Error::refusal("provider_evidence", "restored epoch overflow"))?;
    assert_eq!(
        cold.fence_operations(&dataset, authority.epoch).await?,
        next_epoch
    );
    assert_eq!(cold.latest(&dataset).await?, selected);
    println!(
        "{}",
        json!({"phase":"maintenance_passed","restored":restored,"point":point,
        "old_native_commit":"provider_refused","new_generation_epoch":next_epoch,"rows":values})
    );
    cold.shutdown().await?;
    Ok(())
}

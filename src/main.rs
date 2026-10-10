//! Command-line entry point for the local execution host.

use clap::{Parser, Subcommand};
use docspec::{
    Engine,
    budget::{Budget, Limits},
    engine::{read_evidence, read_group, read_receipts, verify_input_coverage, verify_release_at},
    evidence::EvidenceSpec,
    export::ExportSpec,
    model::{
        BackfillSpec, CompactionSpec, ComparisonIdentity, ComparisonOptions, DependencyCorrection,
        GenerationSeal, Id, InputCoverage, ObjectRef, RefreshSpec, ReleasePin, RemovalPlan,
        RetentionPlan, RunSpec, SchemaSpec, UpdateSpec, VerificationLevel, VersionRef,
    },
    restore::RestorePointRef,
    storage,
};
use futures::TryStreamExt;
use std::{
    fs::File,
    io::{BufReader, BufWriter},
    net::SocketAddr,
    path::PathBuf,
};

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
#[command(
    name = "docspec",
    about = "Local, recoverable Iceberg dataset processing"
)]
struct Cli {
    #[arg(long, default_value = "workspaces/local", global = true)]
    workspace: PathBuf,
    /// Optional JSON resource limits; omitted values use the reference profile.
    #[arg(long, global = true)]
    limits: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Wait after a durable handoff so the qualification harness can kill this process.
    #[cfg(feature = "test-util")]
    #[command(hide = true)]
    PauseAfter {
        spec: PathBuf,
        phase: String,
        #[arg(long, value_enum, default_value = "build")]
        kind: PausedOperation,
    },
    /// Initialize and validate the local catalog.
    Init,
    /// Hash a retained input before selecting it in a run specification.
    Hash { input: PathBuf },
    /// Explain phase reuse without reading payloads.
    Plan { spec: PathBuf },
    /// Run or resume an operation using its stable identity.
    Run { spec: PathBuf },
    /// Resume is the same operation with the same request and retained phases.
    Resume { spec: PathBuf },
    /// Inspect current retained phase and authority records.
    Inspect { dataset: String },
    /// Explain protection for exact objects; this does not authorize removal.
    InspectRetention { objects: PathBuf },
    /// Preview exact owned file removal and its retained protection blockers.
    PlanRemoval {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        removal: String,
        /// JSON array of exact object references.
        #[arg(long)]
        objects: PathBuf,
        #[arg(long)]
        reason: String,
    },
    /// Execute only freshly eligible targets from a saved removal plan.
    RemoveObjects {
        #[arg(long)]
        plan: PathBuf,
    },
    /// Inspect an exact saved removal and its effect acknowledgments.
    InspectRemoval {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        removal: String,
    },
    /// Resume the exact pending effects of a saved removal.
    ResumeRemoval {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        removal: String,
    },
    /// Capture exact phase generations for logical retirement; change no recovery promise.
    PlanPhaseRetirement {
        dataset: String,
        retirement: String,
        /// JSON array of completion keys from inspect.
        keys: PathBuf,
        reason: String,
    },
    /// Retire the phase generations in a saved plan; preserve files and releases.
    RetirePhases { plan: PathBuf },
    /// Capture exact restore points whose recovery promises should end.
    PlanRestorePointRetirement {
        dataset: String,
        retirement: String,
        /// JSON array of restore-point identifiers.
        points: PathBuf,
        reason: String,
    },
    /// Retire saved restore points; preserve their files until separate removal.
    RetireRestorePoints { plan: PathBuf },
    /// Capture exact owner grants that no live consumer binding needs.
    PlanGrantRetirement {
        owner: String,
        retirement: String,
        /// JSON array of exact owner-protection keys from inspect.
        keys: PathBuf,
        reason: String,
    },
    /// Retire unused owner grants from a saved plan; preserve physical files.
    RetireGrants { plan: PathBuf },
    /// Capture a non-current result and all saved readbacks for logical retirement.
    PlanAvailabilityRetirement {
        dataset: String,
        retirement: String,
        operation: String,
        reason: String,
    },
    /// End standalone availability from a saved plan; preserve dependent data and files.
    RetireAvailability { plan: PathBuf },
    /// Capture a terminal operation's recovery records for logical retirement.
    PlanOperationRetirement {
        dataset: String,
        retirement: String,
        operation: String,
        reason: String,
    },
    /// End terminal operation recovery; preserve completed phases and available results.
    RetireOperation { plan: PathBuf },
    /// Stop an unfinished operation; preserve its completed phases and retained files.
    Abandon {
        dataset: String,
        operation: String,
        reason: String,
    },
    /// Resolve the current immutable release pin.
    Latest { dataset: String },
    /// Resolve an immutable historical release by its operation identity.
    Release { dataset: String, version: String },
    /// Create or resume an authorized immutable flat Parquet export.
    Export { spec: PathBuf },
    /// Resolve one export channel to its immutable file pin.
    LatestExport { dataset: String, channel: String },
    /// Resolve a completed historical export by its stable operation identity.
    ExportPin { dataset: String, operation: String },
    /// Rewrite selected files while preserving their exact logical values.
    Compact { spec: PathBuf },
    /// Retain an exact local catalog image and its required immutable files.
    CreateRestorePoint { dataset: String, point: String },
    /// Restore an exact local point while excluding every live workspace actor.
    RestoreLocal {
        point: PathBuf,
        /// Permit a completed backup when current catalog registration is unavailable.
        #[arg(long)]
        allow_unregistered_import: bool,
    },
    /// Prove that target follows prior on accepted history, without reading payloads.
    CheckSuccession { prior: PathBuf, target: PathBuf },
    /// Seal a producer generation's exact accepted versions once.
    SealGeneration { spec: PathBuf },
    /// Capture admissible input versions and the producer seals they require.
    CaptureInputs { spec: PathBuf },
    /// Record omitted source-field dependencies for one exact scalar definition.
    CorrectDependencies { spec: PathBuf },
    /// Verify retained input relationships without opening the catalog or payloads.
    VerifyInputs { coverage: PathBuf },
    /// Explain retained native join work over exact input versions.
    PlanRefresh { spec: PathBuf },
    /// Run or resume a native equality join with contributor evidence.
    Refresh { spec: PathBuf },
    /// Plan complete key replacements and deletions against an exact base.
    PlanUpdate { spec: PathBuf },
    /// Apply or resume an update using its stable operation identity.
    Update { spec: PathBuf },
    /// Plan receipt changes for exact source occurrences.
    PlanEvidence { spec: PathBuf },
    /// Apply or resume receipt changes without recomputing values.
    Evidence { spec: PathBuf },
    /// Plan a schema edit against an exact selected source.
    PlanSchema { spec: PathBuf },
    /// Rename, reorder or hide existing root fields and select the new definition.
    Schema { spec: PathBuf },
    /// Explain completed and pending computed field-group files.
    PlanBackfill { spec: PathBuf },
    /// Compute or resume an independent scalar field group.
    Backfill { spec: PathBuf },
    /// Stream a pinned combined view as Arrow IPC.
    ReadCombined { release: PathBuf, output: PathBuf },
    /// Stream exact contributor pairs as Arrow IPC without the producer catalog.
    ReadEvidence { release: PathBuf, output: PathBuf },
    /// Stream exact receipt values as Arrow IPC without the producer catalog.
    ReadReceipts { release: PathBuf, output: PathBuf },
    /// Stream source values and stable occurrence keys from the local host.
    ReadOccurrences { release: PathBuf, output: PathBuf },
    /// Compare complete pinned results, preserving duplicate row counts.
    Compare {
        before: PathBuf,
        after: PathBuf,
        #[arg(long, default_value = "source")]
        group: String,
        /// Compare stable source occurrences instead of row values.
        #[arg(long)]
        occurrences: bool,
        /// Reconstruct the comparison from the complete endpoints.
        #[arg(long)]
        force_full: bool,
    },
    /// Stream row changes and their exact comparison description as Arrow IPC.
    Changes {
        before: PathBuf,
        after: PathBuf,
        output: PathBuf,
        #[arg(long, default_value = "source")]
        group: String,
        /// Compare row values instead of stable source occurrences.
        #[arg(long)]
        values: bool,
        /// Reconstruct the comparison from the complete endpoints.
        #[arg(long)]
        force_full: bool,
    },
    /// Invalidate pending reuse and preserve the reason.
    Invalidate {
        dataset: String,
        reason: String,
        #[arg(long)]
        definition: Option<String>,
    },
    /// Verify a release pin without opening the producer catalog.
    Verify {
        release: PathBuf,
        /// Hash every payload and reconstruct derived results.
        #[arg(long)]
        deep: bool,
    },
    /// Stream a pinned result as Arrow IPC without the producer catalog.
    Read {
        release: PathBuf,
        output: PathBuf,
        #[arg(long, default_value = "source")]
        group: String,
    },
    /// Serve the local API; remote network binding is refused.
    Serve {
        #[arg(long, default_value = "127.0.0.1:7391")]
        bind: SocketAddr,
    },
}

#[cfg(feature = "test-util")]
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum PausedOperation {
    Build,
    Update,
    Evidence,
    Backfill,
    Refresh,
    Schema,
    Removal,
    Export,
    Compaction,
}

#[tokio::main(worker_threads = 12)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "docspec=info".into()),
        )
        .init();
    let cli = Cli::parse();
    let limits: Limits = cli.limits.map(read_json).transpose()?.unwrap_or_default();
    match cli.command {
        Command::Hash { input } => {
            let (digest, bytes) = storage::digest(&input)?;
            print_json(&serde_json::json!({"digest": digest, "bytes": bytes}))?;
        }
        Command::Verify { release, deep } => {
            let release: ReleasePin = read_json(release)?;
            let level = if deep {
                VerificationLevel::Deep
            } else {
                VerificationLevel::Routine
            };
            let report = verify_release_at(&release, limits, level).await?;
            print_json(
                &serde_json::json!({"verified": true, "release": release.id, "checks": report}),
            )?;
        }
        Command::VerifyInputs { coverage } => {
            verify_input_coverage(&read_json::<InputCoverage>(coverage)?, limits).await?;
            print_json(&serde_json::json!({"relationships_verified": true}))?;
        }
        Command::RestoreLocal {
            point,
            allow_unregistered_import,
        } => {
            print_json(
                &Engine::restore_local(
                    &cli.workspace,
                    &read_json::<RestorePointRef>(point)?,
                    limits,
                    allow_unregistered_import,
                )
                .await?,
            )?;
        }
        command @ (Command::Read { .. }
        | Command::ReadEvidence { .. }
        | Command::ReadReceipts { .. }) => {
            let (release, output, group, receipts) = match command {
                Command::Read {
                    release,
                    output,
                    group,
                } => (release, output, Some(group), false),
                Command::ReadEvidence { release, output } => (release, output, None, false),
                Command::ReadReceipts { release, output } => (release, output, None, true),
                _ => unreachable!(),
            };
            let release: ReleasePin = read_json(release)?;
            let budget = Budget::new(limits)?;
            let mut stream = if let Some(group) = group {
                read_group(&release, &group, &budget).await?
            } else if receipts {
                read_receipts(&release, &budget).await?
            } else {
                read_evidence(&release, &budget).await?
            };
            let file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(output)?;
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(BufWriter::new(file), &stream.schema())?;
            while let Some(batch) = stream.try_next().await? {
                writer.write(&batch)?;
            }
            writer.finish()?;
        }
        command => {
            let engine = Engine::open(&cli.workspace, limits).await?;
            match command {
                #[cfg(feature = "test-util")]
                Command::PauseAfter { spec, phase, kind } => {
                    let result = match kind {
                        PausedOperation::Build => engine
                            .run_until(&read_json::<RunSpec>(spec)?, &phase)
                            .await
                            .map(|_| ()),
                        PausedOperation::Update => engine
                            .update_until(&read_json::<UpdateSpec>(spec)?, &phase)
                            .await
                            .map(|_| ()),
                        PausedOperation::Evidence => engine
                            .evidence_until(&read_json::<EvidenceSpec>(spec)?, &phase)
                            .await
                            .map(|_| ()),
                        PausedOperation::Schema => engine
                            .schema_until(&read_json::<SchemaSpec>(spec)?, &phase)
                            .await
                            .map(|_| ()),
                        PausedOperation::Backfill => engine
                            .backfill_until(&read_json::<BackfillSpec>(spec)?, &phase)
                            .await
                            .map(|_| ()),
                        PausedOperation::Refresh => engine
                            .refresh_until(&read_json::<RefreshSpec>(spec)?, &phase)
                            .await
                            .map(|_| ()),
                        PausedOperation::Removal => engine
                            .removal_until(&read_json::<RemovalPlan>(spec)?, &phase)
                            .await
                            .map(|_| ()),
                        PausedOperation::Export => engine
                            .export_until(&read_json::<ExportSpec>(spec)?, &phase)
                            .await
                            .map(|_| ()),
                        PausedOperation::Compaction => engine
                            .compact_until(&read_json::<CompactionSpec>(spec)?, &phase)
                            .await
                            .map(|_| ()),
                    };
                    match result {
                        Err(error) if error.code() == "injected_failure" => {
                            eprintln!("DOCSPEC_TEST_PAUSED");
                            std::future::pending::<()>().await;
                        }
                        Err(error) => return Err(error.into()),
                        Ok(_) => anyhow::bail!("requested phase did not interrupt"),
                    }
                }
                Command::Init => print_json(
                    &serde_json::json!({"workspace": cli.workspace, "catalog": "iceberg-sql", "lifecycle": "iceberg"}),
                )?,
                Command::Plan { spec } => {
                    print_json(&engine.plan(&read_json::<RunSpec>(spec)?).await?)?
                }
                Command::Run { spec } | Command::Resume { spec } => {
                    print_json(&engine.run(&read_json::<RunSpec>(spec)?).await?)?
                }
                Command::Inspect { dataset } => {
                    print_json(&engine.inspect(&Id::new(dataset)?).await?)?
                }
                Command::InspectRetention { objects } => print_json(
                    &engine
                        .inspect_retention(&read_json::<Vec<ObjectRef>>(objects)?)
                        .await?,
                )?,
                Command::PlanRemoval {
                    dataset,
                    removal,
                    objects,
                    reason,
                } => print_json(
                    &engine
                        .plan_removal(
                            &Id::new(dataset)?,
                            &Id::new(removal)?,
                            &read_json::<Vec<ObjectRef>>(objects)?,
                            &reason,
                        )
                        .await?,
                )?,
                Command::RemoveObjects { plan } => print_json(
                    &engine
                        .remove_objects(&read_json::<RemovalPlan>(plan)?)
                        .await?,
                )?,
                Command::InspectRemoval { dataset, removal } => print_json(
                    &engine
                        .inspect_removal(&Id::new(dataset)?, &Id::new(removal)?)
                        .await?,
                )?,
                Command::ResumeRemoval { dataset, removal } => print_json(
                    &engine
                        .resume_removal(&Id::new(dataset)?, &Id::new(removal)?)
                        .await?,
                )?,
                Command::PlanPhaseRetirement {
                    dataset,
                    retirement,
                    keys,
                    reason,
                } => print_json(
                    &engine
                        .plan_phase_retirement(
                            &Id::new(dataset)?,
                            &Id::new(retirement)?,
                            &read_json::<Vec<String>>(keys)?,
                            &reason,
                        )
                        .await?,
                )?,
                Command::RetirePhases { plan } => print_json(
                    &engine
                        .retire_phases(&read_json::<RetentionPlan>(plan)?)
                        .await?,
                )?,
                Command::PlanRestorePointRetirement {
                    dataset,
                    retirement,
                    points,
                    reason,
                } => print_json(
                    &engine
                        .plan_restore_point_retirement(
                            &Id::new(dataset)?,
                            &Id::new(retirement)?,
                            &read_json::<Vec<Id>>(points)?,
                            &reason,
                        )
                        .await?,
                )?,
                Command::RetireRestorePoints { plan } => print_json(
                    &engine
                        .retire_restore_points(&read_json::<RetentionPlan>(plan)?)
                        .await?,
                )?,
                Command::PlanGrantRetirement {
                    owner,
                    retirement,
                    keys,
                    reason,
                } => print_json(
                    &engine
                        .plan_grant_retirement(
                            &Id::new(owner)?,
                            &Id::new(retirement)?,
                            &read_json::<Vec<String>>(keys)?,
                            &reason,
                        )
                        .await?,
                )?,
                Command::RetireGrants { plan } => print_json(
                    &engine
                        .retire_grants(&read_json::<RetentionPlan>(plan)?)
                        .await?,
                )?,
                Command::PlanAvailabilityRetirement {
                    dataset,
                    retirement,
                    operation,
                    reason,
                } => print_json(
                    &engine
                        .plan_availability_retirement(
                            &Id::new(dataset)?,
                            &Id::new(retirement)?,
                            &Id::new(operation)?,
                            &reason,
                        )
                        .await?,
                )?,
                Command::RetireAvailability { plan } => print_json(
                    &engine
                        .retire_availability(&read_json::<RetentionPlan>(plan)?)
                        .await?,
                )?,
                Command::PlanOperationRetirement {
                    dataset,
                    retirement,
                    operation,
                    reason,
                } => print_json(
                    &engine
                        .plan_operation_retirement(
                            &Id::new(dataset)?,
                            &Id::new(retirement)?,
                            &Id::new(operation)?,
                            &reason,
                        )
                        .await?,
                )?,
                Command::RetireOperation { plan } => print_json(
                    &engine
                        .retire_operation(&read_json::<RetentionPlan>(plan)?)
                        .await?,
                )?,
                Command::Abandon {
                    dataset,
                    operation,
                    reason,
                } => print_json(
                    &engine
                        .abandon(&Id::new(dataset)?, &Id::new(operation)?, &reason)
                        .await?,
                )?,
                Command::PlanUpdate { spec } => {
                    print_json(&engine.plan_update(&read_json::<UpdateSpec>(spec)?).await?)?
                }
                Command::PlanRefresh { spec } => print_json(
                    &engine
                        .plan_refresh(&read_json::<RefreshSpec>(spec)?)
                        .await?,
                )?,
                Command::Refresh { spec } => {
                    print_json(&engine.refresh(&read_json::<RefreshSpec>(spec)?).await?)?
                }
                Command::Update { spec } => {
                    print_json(&engine.update(&read_json::<UpdateSpec>(spec)?).await?)?
                }
                Command::PlanEvidence { spec } => print_json(
                    &engine
                        .plan_evidence(&read_json::<EvidenceSpec>(spec)?)
                        .await?,
                )?,
                Command::Evidence { spec } => print_json(
                    &engine
                        .update_evidence(&read_json::<EvidenceSpec>(spec)?)
                        .await?,
                )?,
                Command::PlanSchema { spec } => {
                    print_json(&engine.plan_schema(&read_json::<SchemaSpec>(spec)?).await?)?
                }
                Command::Schema { spec } => print_json(
                    &engine
                        .evolve_schema(&read_json::<SchemaSpec>(spec)?)
                        .await?,
                )?,
                Command::PlanBackfill { spec } => print_json(
                    &engine
                        .plan_backfill(&read_json::<BackfillSpec>(spec)?)
                        .await?,
                )?,
                Command::Backfill { spec } => {
                    print_json(&engine.backfill(&read_json::<BackfillSpec>(spec)?).await?)?
                }
                command @ (Command::ReadCombined { .. } | Command::ReadOccurrences { .. }) => {
                    let (release, output, occurrences) = match command {
                        Command::ReadCombined { release, output } => (release, output, false),
                        Command::ReadOccurrences { release, output } => (release, output, true),
                        _ => unreachable!(),
                    };
                    let release = read_json::<ReleasePin>(release)?;
                    let mut stream = if occurrences {
                        engine.read_source_occurrences(&release).await?
                    } else {
                        engine.read_combined(&release).await?
                    };
                    let file = std::fs::OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(output)?;
                    let mut writer = arrow::ipc::writer::StreamWriter::try_new(
                        BufWriter::new(file),
                        &stream.schema(),
                    )?;
                    while let Some(batch) = stream.try_next().await? {
                        writer.write(&batch)?;
                    }
                    writer.finish()?;
                }
                Command::Latest { dataset } => {
                    print_json(&engine.latest(&Id::new(dataset)?).await?)?
                }
                Command::Release { dataset, version } => print_json(
                    &engine
                        .release(&Id::new(dataset)?, &Id::new(version)?)
                        .await?,
                )?,
                Command::Export { spec } => {
                    print_json(&engine.export(&read_json::<ExportSpec>(spec)?).await?)?
                }
                Command::LatestExport { dataset, channel } => print_json(
                    &engine
                        .latest_export(&Id::new(dataset)?, &Id::new(channel)?)
                        .await?,
                )?,
                Command::ExportPin { dataset, operation } => print_json(
                    &engine
                        .export_pin(&Id::new(dataset)?, &Id::new(operation)?)
                        .await?,
                )?,
                Command::Compact { spec } => {
                    print_json(&engine.compact(&read_json::<CompactionSpec>(spec)?).await?)?
                }
                Command::CreateRestorePoint { dataset, point } => print_json(
                    &engine
                        .create_restore_point(&Id::new(dataset)?, &Id::new(point)?)
                        .await?,
                )?,
                Command::CheckSuccession { prior, target } => {
                    let prior: ReleasePin = read_json(prior)?;
                    let target: ReleasePin = read_json(target)?;
                    print_json(
                        &engine
                            .prove_succession(&prior.version_ref(), &target.version_ref())
                            .await?,
                    )?;
                }
                Command::SealGeneration { spec } => {
                    print_json(
                        &engine
                            .seal_generation(&read_json::<GenerationSeal>(spec)?)
                            .await?,
                    )?;
                }
                Command::CaptureInputs { spec } => {
                    let inputs: std::collections::BTreeMap<Id, VersionRef> = read_json(spec)?;
                    print_json(&engine.capture_inputs(&inputs).await?)?;
                }
                Command::CorrectDependencies { spec } => {
                    print_json(
                        &engine
                            .correct_dependencies(&read_json::<DependencyCorrection>(spec)?)
                            .await?,
                    )?;
                }
                Command::Compare {
                    before,
                    after,
                    group,
                    occurrences,
                    force_full,
                } => print_json(
                    &engine
                        .compare_with_options(
                            &read_json::<ReleasePin>(before)?,
                            &read_json::<ReleasePin>(after)?,
                            &group,
                            &ComparisonOptions {
                                identity: if occurrences {
                                    ComparisonIdentity::Occurrences
                                } else {
                                    ComparisonIdentity::Values
                                },
                                force_full,
                            },
                        )
                        .await?,
                )?,
                Command::Changes {
                    before,
                    after,
                    output,
                    group,
                    values,
                    force_full,
                } => {
                    let before = read_json::<ReleasePin>(before)?;
                    let after = read_json::<ReleasePin>(after)?;
                    let mut stream = engine
                        .changes(
                            &before,
                            &after,
                            &group,
                            &ComparisonOptions {
                                identity: if values {
                                    ComparisonIdentity::Values
                                } else {
                                    ComparisonIdentity::Occurrences
                                },
                                force_full,
                            },
                        )
                        .await?;
                    let file = std::fs::OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(output)?;
                    let mut writer = arrow::ipc::writer::StreamWriter::try_new(
                        BufWriter::new(file),
                        &stream.schema(),
                    )?;
                    while let Some(batch) = stream.try_next().await? {
                        writer.write(&batch)?;
                    }
                    writer.finish()?;
                }
                Command::Invalidate {
                    dataset,
                    reason,
                    definition,
                } => print_json(
                    &serde_json::json!({"eligibility": engine.invalidate(&Id::new(dataset)?, &reason, definition).await?}),
                )?,
                Command::Serve { bind } => {
                    anyhow::ensure!(
                        bind.ip().is_loopback(),
                        "hosted access is unqualified; bind to loopback"
                    );
                    let listener = tokio::net::TcpListener::bind(bind).await?;
                    let result = axum::serve(listener, docspec::http::router(engine.clone()))
                        .with_graceful_shutdown(async {
                            let _ = tokio::signal::ctrl_c().await;
                        })
                        .await;
                    // Disconnected clients may leave owned service tasks finishing.
                    engine.shutdown().await?;
                    result?;
                }
                _ => unreachable!("independent-reader commands handled before opening workspace"),
            }
        }
    }
    Ok(())
}

fn read_json<T: serde::de::DeserializeOwned>(path: PathBuf) -> anyhow::Result<T> {
    let file = File::open(path)?;
    anyhow::ensure!(
        file.metadata()?.len() <= 32 << 20,
        "metadata input exceeds 32 MiB"
    );
    Ok(serde_json::from_reader(BufReader::new(file))?)
}
fn print_json(value: &impl serde::Serialize) -> anyhow::Result<()> {
    serde_json::to_writer_pretty(std::io::stdout(), value)?;
    println!();
    Ok(())
}

//! Command-line entry point for the local execution host.

use clap::{Parser, Subcommand};
use docspec::{
    Engine,
    budget::{Budget, Limits},
    engine::{read_evidence, read_group, verify_input_coverage, verify_release_at},
    model::{
        BackfillSpec, GenerationSeal, Id, InputCoverage, ObjectRef, RefreshSpec, ReleasePin,
        RemovalPlan, RetentionPlan, RunSpec, SchemaSpec, UpdateSpec, VerificationLevel, VersionRef,
    },
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
    /// Prove that target follows prior on accepted history, without reading payloads.
    CheckSuccession { prior: PathBuf, target: PathBuf },
    /// Seal a producer generation's exact accepted versions once.
    SealGeneration { spec: PathBuf },
    /// Capture admissible input versions and the producer seals they require.
    CaptureInputs { spec: PathBuf },
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
    /// Compare complete pinned results, preserving duplicate row counts.
    Compare {
        before: PathBuf,
        after: PathBuf,
        #[arg(long, default_value = "source")]
        group: String,
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
    Backfill,
    Refresh,
    Schema,
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
        command @ (Command::Read { .. } | Command::ReadEvidence { .. }) => {
            let (release, output, group) = match command {
                Command::Read {
                    release,
                    output,
                    group,
                } => (release, output, Some(group)),
                Command::ReadEvidence { release, output } => (release, output, None),
                _ => unreachable!(),
            };
            let release: ReleasePin = read_json(release)?;
            let budget = Budget::new(limits)?;
            let mut stream = if let Some(group) = group {
                read_group(&release, &group, &budget).await?
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
                        PausedOperation::Build => {
                            engine.run_until(&read_json::<RunSpec>(spec)?, &phase).await
                        }
                        PausedOperation::Update => {
                            engine
                                .update_until(&read_json::<UpdateSpec>(spec)?, &phase)
                                .await
                        }
                        PausedOperation::Schema => {
                            engine
                                .schema_until(&read_json::<SchemaSpec>(spec)?, &phase)
                                .await
                        }
                        PausedOperation::Backfill => {
                            engine
                                .backfill_until(&read_json::<BackfillSpec>(spec)?, &phase)
                                .await
                        }
                        PausedOperation::Refresh => {
                            engine
                                .refresh_until(&read_json::<RefreshSpec>(spec)?, &phase)
                                .await
                        }
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
                Command::ReadCombined { release, output } => {
                    let mut stream = engine
                        .read_combined(&read_json::<ReleasePin>(release)?)
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
                Command::Latest { dataset } => {
                    print_json(&engine.latest(&Id::new(dataset)?).await?)?
                }
                Command::Release { dataset, version } => print_json(
                    &engine
                        .release(&Id::new(dataset)?, &Id::new(version)?)
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
                Command::Compare {
                    before,
                    after,
                    group,
                } => print_json(
                    &engine
                        .compare(
                            &read_json::<ReleasePin>(before)?,
                            &read_json::<ReleasePin>(after)?,
                            &group,
                        )
                        .await?,
                )?,
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

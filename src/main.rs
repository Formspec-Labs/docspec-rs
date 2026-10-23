//! Command-line startup for a local execution host with explicit storage configuration.
// Rust guideline compliant 2026-02-21

use clap::{Args, Parser, Subcommand, ValueEnum};
use docspec::{
    Engine,
    budget::{Budget, Limits},
    engine::{read_evidence, read_group, read_receipts, verify_input_coverage, verify_release_at},
    evidence::EvidenceSpec,
    export::ExportSpec,
    model::{
        BackfillSpec, CompactionSpec, ComparisonIdentity, ComparisonOptions,
        ContinuousBackfillSpec, Id, InputCoverage, ObjectRef, RefreshSpec, ReleasePin, RunSpec,
        SchemaSpec, UpdateSpec, VerificationLevel, VersionRef,
    },
    restore::RestorePointRef,
    storage,
};
use futures::TryStreamExt;
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Write},
    net::SocketAddr,
    path::PathBuf,
};

mod cli_args;
use cli_args::normalize_cli_args;

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
#[command(name = "docspec", about = "Recoverable Iceberg dataset processing")]
struct Cli {
    #[arg(long, default_value = "workspaces/local", global = true)]
    workspace: PathBuf,
    /// Optional JSON resource limits; omitted values use the reference profile.
    #[arg(long, global = true)]
    limits: Option<PathBuf>,
    /// Explicit remote JSON configuration; requires the remote feature.
    #[arg(long, global = true)]
    remote: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum CliComparisonIdentity {
    Values,
    Occurrences,
}

impl From<CliComparisonIdentity> for ComparisonIdentity {
    fn from(value: CliComparisonIdentity) -> Self {
        match value {
            CliComparisonIdentity::Values => Self::Values,
            CliComparisonIdentity::Occurrences => Self::Occurrences,
        }
    }
}

#[derive(Debug, Args)]
struct SpecArg {
    #[arg(long)]
    spec: PathBuf,
}

#[derive(Debug, Args)]
struct PlanArg {
    #[arg(long)]
    plan: PathBuf,
}

#[derive(Debug, Args)]
struct DatasetArg {
    #[arg(long)]
    dataset: String,
}

#[derive(Debug, Args)]
struct DatasetRemovalArgs {
    #[arg(long)]
    dataset: String,
    #[arg(long)]
    removal: String,
}

#[derive(Debug, Args)]
struct OperationRetirementArgs {
    #[arg(long)]
    dataset: String,
    #[arg(long)]
    retirement: String,
    #[arg(long)]
    operation: String,
    #[arg(long)]
    reason: String,
}

impl OperationRetirementArgs {
    fn ids(&self) -> docspec::Result<(Id, Id, Id)> {
        Ok((
            Id::new(&self.dataset)?,
            Id::new(&self.retirement)?,
            Id::new(&self.operation)?,
        ))
    }
}

#[derive(Debug, Args)]
struct ReleaseOutputArgs {
    #[arg(long)]
    release: PathBuf,
    #[arg(long)]
    output: PathBuf,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Wait after a durable handoff so the qualification harness can kill this process.
    #[cfg(feature = "test-util")]
    #[command(hide = true)]
    PauseAfter {
        #[arg(long)]
        spec: PathBuf,
        #[arg(long)]
        phase: String,
        #[arg(long, value_enum, default_value = "build")]
        kind: PausedOperation,
    },
    /// Initialize the execution host with the configured storage.
    Init,
    /// Hash a retained input before selecting it in a run specification.
    Hash {
        #[arg(long)]
        input: PathBuf,
    },
    /// Explain phase reuse without reading payloads.
    Plan(SpecArg),
    /// Run or resume an operation using its stable identity.
    Run(SpecArg),
    /// Deprecated alias for `run`: resume is the same operation with the same request.
    #[command(hide = true)]
    Resume(SpecArg),
    /// Inspect current retained phase and authority records.
    Inspect(DatasetArg),
    /// Fence older operations without changing the selected release.
    FenceOperations {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        expected_epoch: u64,
    },
    /// Inspect optional change evidence for an exact version reference.
    InspectChanges {
        #[arg(long)]
        version: PathBuf,
    },
    /// Explain protection for exact objects; this does not authorize removal.
    InspectRetention {
        #[arg(long)]
        objects: PathBuf,
    },
    /// Preview exact owned file removal and its retained protection blockers.
    PlanRemoval {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        removal: String,
        #[arg(long)]
        objects: PathBuf,
        #[arg(long)]
        reason: String,
    },
    /// Preview one completed remote export's exact original host stage.
    PlanExportStageRemoval {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        removal: String,
        #[arg(long)]
        operation: String,
        #[arg(long)]
        reason: String,
    },
    /// Preview a named created host file after export recovery retirement.
    PlanExportFileRemoval {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        removal: String,
        #[arg(long)]
        operation: String,
        #[arg(long)]
        file: String,
        #[arg(long)]
        reason: String,
    },
    /// Preview exact terminal export recovery retirement.
    PlanExportRetirement(OperationRetirementArgs),
    /// End the exact export recovery in a saved plan, preserving immutable pins.
    RetireExport(PlanArg),
    /// Execute only freshly eligible targets from a saved removal plan.
    RemoveObjects(PlanArg),
    /// Reserve remote keys and detach their native heads without object deletion.
    #[cfg(feature = "remote")]
    PrepareRemoteRemoval(PlanArg),
    /// Resume saved exact remote deletion with named temporary credential bindings.
    #[cfg(feature = "remote")]
    ResumeRemoteRemoval {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        removal: String,
        #[arg(long)]
        authority: PathBuf,
    },
    /// Inspect an exact saved removal and its effect acknowledgments.
    InspectRemoval(DatasetRemovalArgs),
    /// Resume the exact pending effects of a saved removal.
    ResumeRemoval(DatasetRemovalArgs),
    /// Capture exact phase generations for logical retirement; change no recovery promise.
    PlanPhaseRetirement {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        retirement: String,
        #[arg(long)]
        keys: PathBuf,
        #[arg(long)]
        reason: String,
    },
    /// Retire the phase generations in a saved plan; preserve files and releases.
    RetirePhases(PlanArg),
    /// Capture optional change evidence for independent logical retirement.
    PlanChangeRetirement {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        retirement: String,
        #[arg(long)]
        versions: PathBuf,
        #[arg(long)]
        reason: String,
    },
    /// Retire optional change evidence from a saved plan; preserve complete results.
    RetireChanges(PlanArg),
    /// Capture exact restore points whose recovery promises should end.
    PlanRestorePointRetirement {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        retirement: String,
        #[arg(long)]
        points: PathBuf,
        #[arg(long)]
        reason: String,
    },
    /// Retire saved restore points; preserve their files until separate removal.
    RetireRestorePoints(PlanArg),
    /// Capture one producer seal for standalone availability retirement.
    PlanGenerationRetirement {
        #[arg(long)]
        family: String,
        #[arg(long)]
        retirement: String,
        #[arg(long)]
        generation: String,
        #[arg(long)]
        reason: String,
    },
    /// Retire a seal's standalone promise; preserve admitted consumer evidence.
    RetireGeneration(PlanArg),
    /// Capture exact owner grants that no live consumer binding needs.
    PlanGrantRetirement {
        #[arg(long)]
        owner: String,
        #[arg(long)]
        retirement: String,
        #[arg(long)]
        keys: PathBuf,
        #[arg(long)]
        reason: String,
    },
    /// Retire unused owner grants from a saved plan; preserve physical files.
    RetireGrants(PlanArg),
    /// Capture a non-current result and all saved readbacks for logical retirement.
    PlanAvailabilityRetirement(OperationRetirementArgs),
    /// End standalone availability from a saved plan; preserve dependent data and files.
    RetireAvailability(PlanArg),
    /// Capture a terminal operation's recovery records for logical retirement.
    PlanOperationRetirement(OperationRetirementArgs),
    /// End terminal operation recovery; preserve completed phases and available results.
    RetireOperation(PlanArg),
    /// Capture a compaction's recovery records, including interrupted work.
    PlanCompactionRetirement(OperationRetirementArgs),
    /// End compaction recovery; preserve selected representations and physical files.
    RetireCompaction(PlanArg),
    /// Stop an unfinished operation; preserve its completed phases and retained files.
    Abandon {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        operation: String,
        #[arg(long)]
        reason: String,
    },
    /// Resolve the current immutable release pin.
    Latest(DatasetArg),
    /// Resolve an immutable historical release by its operation identity.
    Release {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        version: String,
    },
    /// Create or resume an authorized immutable flat Parquet export.
    Export(SpecArg),
    /// Resolve one export channel to its immutable file pin.
    LatestExport {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        channel: String,
    },
    /// Resolve a completed historical export by its stable operation identity.
    ExportPin {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        operation: String,
    },
    /// Stream an exact retained flat export as Arrow IPC with configured credentials.
    ReadExport {
        #[arg(long)]
        pin: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Rewrite selected files while preserving their exact logical values.
    Compact(SpecArg),
    /// Rewrite superseded lifecycle revisions behind the active protected release.
    CompactLifecycle(DatasetArg),
    /// Retain a configured catalog image and its required immutable objects.
    CreateRestorePoint {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        point: String,
    },
    /// Restore catalog registrations into an explicitly quiescent fresh remote generation.
    #[cfg(feature = "remote")]
    RestoreRemote {
        #[arg(long)]
        point: PathBuf,
        /// Same object scope and fresh generation; credentials are environment references.
        #[arg(long)]
        target: PathBuf,
        #[arg(long)]
        allow_unregistered_import: bool,
    },
    /// Restore an exact local point while excluding every live workspace actor.
    RestoreLocal {
        #[arg(long)]
        point: PathBuf,
        /// Permit a completed backup when current catalog registration is unavailable.
        #[arg(long)]
        allow_unregistered_import: bool,
    },
    /// Prove that target follows prior on accepted history, without reading payloads.
    CheckSuccession {
        #[arg(long)]
        prior: PathBuf,
        #[arg(long)]
        target: PathBuf,
    },
    /// Seal a producer generation's exact accepted versions once.
    SealGeneration(SpecArg),
    /// Capture admissible input versions and the producer seals they require.
    CaptureInputs(SpecArg),
    /// Record omitted source-field dependencies for one exact scalar definition.
    CorrectDependencies(SpecArg),
    /// Verify retained input relationships without opening the catalog or payloads.
    VerifyInputs {
        #[arg(long)]
        coverage: PathBuf,
    },
    /// Explain retained native join work over exact input versions.
    PlanRefresh(SpecArg),
    /// Run or resume a native equality join with contributor evidence.
    Refresh(SpecArg),
    /// Plan complete key replacements and deletions against an exact base.
    PlanUpdate(SpecArg),
    /// Apply or resume an update using its stable operation identity.
    Update(SpecArg),
    /// Plan receipt changes for exact source occurrences.
    PlanEvidence(SpecArg),
    /// Apply or resume receipt changes without recomputing values.
    Evidence(SpecArg),
    /// Plan a schema edit against an exact selected source.
    PlanSchema(SpecArg),
    /// Rename, reorder or hide existing root fields and select the new definition.
    Schema(SpecArg),
    /// Explain completed and pending computed field-group files.
    PlanBackfill(SpecArg),
    /// Compute or resume an independent scalar field group.
    Backfill(SpecArg),
    /// Explain bounded catch-up and cutover work for a continuous field group.
    PlanContinuousBackfill(SpecArg),
    /// Compute or resume a field group while source admissions advance.
    ContinuousBackfill(SpecArg),
    /// Stream a pinned combined view as Arrow IPC.
    ReadCombined(ReleaseOutputArgs),
    /// Stream exact contributor pairs as Arrow IPC without the producer catalog.
    ReadEvidence(ReleaseOutputArgs),
    /// Stream exact receipt values as Arrow IPC without the producer catalog.
    ReadReceipts(ReleaseOutputArgs),
    /// Stream source values and stable occurrence keys from the local host.
    ReadOccurrences(ReleaseOutputArgs),
    /// Stream source rows matching a bounded set of scalar keys as Arrow IPC.
    ReadKeys {
        #[arg(long)]
        release: PathBuf,
        #[arg(long)]
        spec: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Compare complete pinned results, preserving duplicate row counts.
    Compare {
        #[arg(long)]
        before: PathBuf,
        #[arg(long)]
        after: PathBuf,
        #[arg(long, default_value = "source")]
        group: String,
        /// Comparison identity mode (`values` or `occurrences`).
        #[arg(long, value_enum, alias = "mode")]
        identity: Option<CliComparisonIdentity>,
        /// Compare stable source occurrences instead of row values.
        #[arg(long)]
        occurrences: bool,
        /// Reconstruct the comparison from the complete endpoints.
        #[arg(long)]
        force_full: bool,
    },
    /// Stream row changes and their exact comparison description as Arrow IPC.
    Changes {
        #[arg(long)]
        before: PathBuf,
        #[arg(long)]
        after: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value = "source")]
        group: String,
        /// Comparison identity mode (`values` or `occurrences`).
        #[arg(long, value_enum, alias = "mode")]
        identity: Option<CliComparisonIdentity>,
        /// Compare row values instead of stable source occurrences.
        #[arg(long)]
        values: bool,
        /// Reconstruct the comparison from the complete endpoints.
        #[arg(long)]
        force_full: bool,
    },
    /// Invalidate pending reuse and preserve the reason.
    Invalidate {
        #[arg(long)]
        dataset: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        definition: Option<String>,
    },
    /// Verify a release pin without opening the producer catalog.
    Verify {
        #[arg(long)]
        release: PathBuf,
        /// Hash every payload and reconstruct derived results.
        #[arg(long)]
        deep: bool,
    },
    /// Stream a pinned group as Arrow IPC; remote mode uses the configured host.
    Read {
        #[arg(long)]
        release: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value = "source")]
        group: String,
    },
    /// Serve the loopback API over the configured execution host.
    Serve {
        #[arg(long, default_value = "127.0.0.1:7391")]
        bind: SocketAddr,
        /// Create a private readiness record after binding; never overwrite a path.
        #[arg(long)]
        ready_file: Option<PathBuf>,
        /// Stop HTTP intake and drain owned work when the caller closes stdin.
        #[arg(long)]
        shutdown_on_stdin_eof: bool,
    },
}

#[cfg(feature = "test-util")]
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum PausedOperation {
    Build,
    Update,
    Evidence,
    Backfill,
    ContinuousBackfill,
    Refresh,
    Schema,
    Removal,
    Export,
    Compaction,
}

fn main() -> anyhow::Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(12)
        .thread_stack_size(8 * 1024 * 1024)
        .enable_all()
        .build()?
        .block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "docspec=info".into()),
        )
        .init();
    let cli = Cli::parse_from(normalize_cli_args(std::env::args_os()));
    check_startup(&cli)?;
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
            let restored = Engine::restore_local(
                &cli.workspace,
                &read_json::<RestorePointRef>(point)?,
                limits,
                allow_unregistered_import,
            )
            .await?;
            print_json(&restored)?;
        }
        command @ (Command::Read { .. } | Command::ReadEvidence(_) | Command::ReadReceipts(_))
            if cli.remote.is_none() =>
        {
            let (release, output, group, receipts) = match command {
                Command::Read {
                    release,
                    output,
                    group,
                } => (release, output, Some(group), false),
                Command::ReadEvidence(ReleaseOutputArgs { release, output }) => {
                    (release, output, None, false)
                }
                Command::ReadReceipts(ReleaseOutputArgs { release, output }) => {
                    (release, output, None, true)
                }
                _ => unreachable!(),
            };
            let release: ReleasePin = read_json(release)?;
            let budget = Budget::new(limits)?;
            let stream = if let Some(group) = group {
                read_group(&release, &group, &budget).await?
            } else if receipts {
                read_receipts(&release, &budget).await?
            } else {
                read_evidence(&release, &budget).await?
            };
            write_stream(output, stream).await?;
        }
        command => {
            let engine = open_engine(&cli.workspace, limits, cli.remote.as_deref()).await?;
            execute_engine_command(&cli.workspace, cli.remote.is_some(), engine, command).await?;
        }
    }
    Ok(())
}

async fn execute_engine_command(
    workspace: &std::path::Path,
    has_remote: bool,
    engine: Engine,
    command: Command,
) -> anyhow::Result<()> {
    match command {
        #[cfg(feature = "test-util")]
        Command::PauseAfter { spec, phase, kind } => {
            let result = match kind {
                PausedOperation::Build => engine
                    .run_until(&read_json(spec)?, &phase)
                    .await
                    .map(|_| ()),
                PausedOperation::Update => engine
                    .update_until(&read_json(spec)?, &phase)
                    .await
                    .map(|_| ()),
                PausedOperation::Evidence => engine
                    .evidence_until(&read_json(spec)?, &phase)
                    .await
                    .map(|_| ()),
                PausedOperation::Schema => engine
                    .schema_until(&read_json(spec)?, &phase)
                    .await
                    .map(|_| ()),
                PausedOperation::Backfill => engine
                    .backfill_until(&read_json(spec)?, &phase)
                    .await
                    .map(|_| ()),
                PausedOperation::ContinuousBackfill => engine
                    .continuous_backfill_until(&read_json(spec)?, &phase)
                    .await
                    .map(|_| ()),
                PausedOperation::Refresh => engine
                    .refresh_until(&read_json(spec)?, &phase)
                    .await
                    .map(|_| ()),
                PausedOperation::Removal => engine
                    .removal_until(&read_json(spec)?, &phase)
                    .await
                    .map(|_| ()),
                PausedOperation::Export => engine
                    .export_until(&read_json(spec)?, &phase)
                    .await
                    .map(|_| ()),
                PausedOperation::Compaction => engine
                    .compact_until(&read_json(spec)?, &phase)
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
        Command::Init => {
            let catalog = if has_remote {
                "iceberg-rest"
            } else {
                "iceberg-sql"
            };
            print_json(
                &serde_json::json!({"workspace": workspace, "catalog": catalog, "lifecycle": "iceberg"}),
            )?;
        }
        Command::Plan(SpecArg { spec }) => {
            print_json(&engine.plan(&read_json::<RunSpec>(spec)?).await?)?
        }
        Command::Run(SpecArg { spec }) | Command::Resume(SpecArg { spec }) => {
            print_json(&engine.run(&read_json::<RunSpec>(spec)?).await?)?
        }
        Command::Inspect(DatasetArg { dataset }) => {
            print_json(&engine.inspect(&Id::new(dataset)?).await?)?
        }
        Command::InspectChanges { version } => print_json(
            &engine
                .change_certificate(&read_json::<VersionRef>(version)?)
                .await?,
        )?,
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
        } => {
            let plan = engine
                .plan_removal(
                    &Id::new(dataset)?,
                    &Id::new(removal)?,
                    &read_json::<Vec<ObjectRef>>(objects)?,
                    &reason,
                )
                .await?;
            print_json(&plan)?;
        }
        Command::PlanExportStageRemoval {
            dataset,
            removal,
            operation,
            reason,
        } => {
            let plan = engine
                .plan_export_stage_removal(
                    &Id::new(dataset)?,
                    &Id::new(removal)?,
                    &Id::new(operation)?,
                    &reason,
                )
                .await?;
            print_json(&plan)?;
        }
        Command::PlanExportFileRemoval {
            dataset,
            removal,
            operation,
            file,
            reason,
        } => {
            let plan = engine
                .plan_export_file_removal(
                    &Id::new(dataset)?,
                    &Id::new(removal)?,
                    &Id::new(operation)?,
                    &Id::new(file)?,
                    &reason,
                )
                .await?;
            print_json(&plan)?;
        }
        Command::PlanExportRetirement(args) => {
            let (ds, ret, op) = args.ids()?;
            print_json(
                &engine
                    .plan_export_retirement(&ds, &ret, &op, &args.reason)
                    .await?,
            )?;
        }
        Command::RetireExport(PlanArg { plan }) => {
            print_json(&engine.retire_export(&read_json(plan)?).await?)?
        }
        Command::RemoveObjects(PlanArg { plan }) => {
            print_json(&engine.remove_objects(&read_json(plan)?).await?)?
        }
        #[cfg(feature = "remote")]
        Command::PrepareRemoteRemoval(PlanArg { plan }) => {
            print_json(&engine.prepare_remote_removal(&read_json(plan)?).await?)?
        }
        #[cfg(feature = "remote")]
        Command::ResumeRemoteRemoval {
            dataset,
            removal,
            authority,
        } => {
            let cfg =
                read_json::<docspec::storage::remote::objects::RemoteRemovalConfig>(authority)?
                    .resolve()?;
            print_json(
                &engine
                    .resume_removal_remote(&Id::new(dataset)?, &Id::new(removal)?, cfg)
                    .await?,
            )?;
        }
        Command::InspectRemoval(DatasetRemovalArgs { dataset, removal }) => print_json(
            &engine
                .inspect_removal(&Id::new(dataset)?, &Id::new(removal)?)
                .await?,
        )?,
        Command::ResumeRemoval(DatasetRemovalArgs { dataset, removal }) => print_json(
            &engine
                .resume_removal(&Id::new(dataset)?, &Id::new(removal)?)
                .await?,
        )?,
        Command::PlanPhaseRetirement {
            dataset,
            retirement,
            keys,
            reason,
        } => {
            let plan = engine
                .plan_phase_retirement(
                    &Id::new(dataset)?,
                    &Id::new(retirement)?,
                    &read_json::<Vec<String>>(keys)?,
                    &reason,
                )
                .await?;
            print_json(&plan)?;
        }
        Command::RetirePhases(PlanArg { plan }) => {
            print_json(&engine.retire_phases(&read_json(plan)?).await?)?
        }
        Command::PlanChangeRetirement {
            dataset,
            retirement,
            versions,
            reason,
        } => {
            let plan = engine
                .plan_change_retirement(
                    &Id::new(dataset)?,
                    &Id::new(retirement)?,
                    &read_json::<Vec<Id>>(versions)?,
                    &reason,
                )
                .await?;
            print_json(&plan)?;
        }
        Command::RetireChanges(PlanArg { plan }) => {
            print_json(&engine.retire_changes(&read_json(plan)?).await?)?
        }
        Command::PlanRestorePointRetirement {
            dataset,
            retirement,
            points,
            reason,
        } => {
            let plan = engine
                .plan_restore_point_retirement(
                    &Id::new(dataset)?,
                    &Id::new(retirement)?,
                    &read_json::<Vec<Id>>(points)?,
                    &reason,
                )
                .await?;
            print_json(&plan)?;
        }
        Command::RetireRestorePoints(PlanArg { plan }) => {
            print_json(&engine.retire_restore_points(&read_json(plan)?).await?)?
        }
        Command::PlanGenerationRetirement {
            family,
            retirement,
            generation,
            reason,
        } => {
            let plan = engine
                .plan_generation_retirement(
                    &Id::new(family)?,
                    &Id::new(retirement)?,
                    &Id::new(generation)?,
                    &reason,
                )
                .await?;
            print_json(&plan)?;
        }
        Command::RetireGeneration(PlanArg { plan }) => {
            print_json(&engine.retire_generation(&read_json(plan)?).await?)?
        }
        Command::PlanGrantRetirement {
            owner,
            retirement,
            keys,
            reason,
        } => {
            let plan = engine
                .plan_grant_retirement(
                    &Id::new(owner)?,
                    &Id::new(retirement)?,
                    &read_json::<Vec<String>>(keys)?,
                    &reason,
                )
                .await?;
            print_json(&plan)?;
        }
        Command::RetireGrants(PlanArg { plan }) => {
            print_json(&engine.retire_grants(&read_json(plan)?).await?)?
        }
        Command::PlanAvailabilityRetirement(args) => {
            let (ds, ret, op) = args.ids()?;
            print_json(
                &engine
                    .plan_availability_retirement(&ds, &ret, &op, &args.reason)
                    .await?,
            )?;
        }
        Command::RetireAvailability(PlanArg { plan }) => {
            print_json(&engine.retire_availability(&read_json(plan)?).await?)?
        }
        Command::PlanOperationRetirement(args) => {
            let (ds, ret, op) = args.ids()?;
            print_json(
                &engine
                    .plan_operation_retirement(&ds, &ret, &op, &args.reason)
                    .await?,
            )?;
        }
        Command::RetireOperation(PlanArg { plan }) => {
            print_json(&engine.retire_operation(&read_json(plan)?).await?)?
        }
        Command::PlanCompactionRetirement(args) => {
            let (ds, ret, op) = args.ids()?;
            print_json(
                &engine
                    .plan_compaction_retirement(&ds, &ret, &op, &args.reason)
                    .await?,
            )?;
        }
        Command::RetireCompaction(PlanArg { plan }) => {
            print_json(&engine.retire_compaction(&read_json(plan)?).await?)?
        }
        Command::FenceOperations {
            dataset,
            expected_epoch,
        } => print_json(
            &engine
                .fence_operations(&Id::new(dataset)?, expected_epoch)
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
        Command::PlanUpdate(SpecArg { spec }) => {
            print_json(&engine.plan_update(&read_json::<UpdateSpec>(spec)?).await?)?
        }
        Command::PlanRefresh(SpecArg { spec }) => print_json(
            &engine
                .plan_refresh(&read_json::<RefreshSpec>(spec)?)
                .await?,
        )?,
        Command::Refresh(SpecArg { spec }) => {
            print_json(&engine.refresh(&read_json::<RefreshSpec>(spec)?).await?)?
        }
        Command::Update(SpecArg { spec }) => {
            print_json(&engine.update(&read_json::<UpdateSpec>(spec)?).await?)?
        }
        Command::PlanEvidence(SpecArg { spec }) => print_json(
            &engine
                .plan_evidence(&read_json::<EvidenceSpec>(spec)?)
                .await?,
        )?,
        Command::Evidence(SpecArg { spec }) => print_json(
            &engine
                .update_evidence(&read_json::<EvidenceSpec>(spec)?)
                .await?,
        )?,
        Command::PlanSchema(SpecArg { spec }) => {
            print_json(&engine.plan_schema(&read_json::<SchemaSpec>(spec)?).await?)?
        }
        Command::Schema(SpecArg { spec }) => print_json(
            &engine
                .evolve_schema(&read_json::<SchemaSpec>(spec)?)
                .await?,
        )?,
        Command::PlanBackfill(SpecArg { spec }) => print_json(
            &engine
                .plan_backfill(&read_json::<BackfillSpec>(spec)?)
                .await?,
        )?,
        Command::Backfill(SpecArg { spec }) => {
            print_json(&engine.backfill(&read_json::<BackfillSpec>(spec)?).await?)?
        }
        Command::PlanContinuousBackfill(SpecArg { spec }) => print_json(
            &engine
                .plan_continuous_backfill(&read_json::<ContinuousBackfillSpec>(spec)?)
                .await?,
        )?,
        Command::ContinuousBackfill(SpecArg { spec }) => print_json(
            &engine
                .continuous_backfill(&read_json::<ContinuousBackfillSpec>(spec)?)
                .await?,
        )?,
        command @ (Command::ReadCombined(_) | Command::ReadOccurrences(_)) => {
            let (release, output, occurrences) = match command {
                Command::ReadCombined(ReleaseOutputArgs { release, output }) => {
                    (release, output, false)
                }
                Command::ReadOccurrences(ReleaseOutputArgs { release, output }) => {
                    (release, output, true)
                }
                _ => unreachable!(),
            };
            let release = read_json::<ReleasePin>(release)?;
            let stream = if occurrences {
                engine.read_source_occurrences(&release).await?
            } else {
                engine.read_combined(&release).await?
            };
            write_stream(output, stream).await?;
            engine.shutdown().await?;
        }
        Command::Latest(DatasetArg { dataset }) => {
            print_json(&engine.latest(&Id::new(dataset)?).await?)?
        }
        Command::Release { dataset, version } => print_json(
            &engine
                .release(&Id::new(dataset)?, &Id::new(version)?)
                .await?,
        )?,
        Command::Export(SpecArg { spec }) => {
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
        Command::Compact(SpecArg { spec }) => {
            print_json(&engine.compact(&read_json::<CompactionSpec>(spec)?).await?)?
        }
        Command::CompactLifecycle(DatasetArg { dataset }) => {
            print_json(&engine.compact_lifecycle(&Id::new(dataset)?).await?)?
        }
        Command::CreateRestorePoint { dataset, point } => print_json(
            &engine
                .create_restore_point(&Id::new(dataset)?, &Id::new(point)?)
                .await?,
        )?,
        #[cfg(feature = "remote")]
        Command::RestoreRemote {
            point,
            target,
            allow_unregistered_import,
        } => {
            let config = read_remote_config(target)?;
            let remote = storage::remote::remote_storage(
                &config.owned_prefix,
                config.properties,
                config.secret_env,
            )
            .await?;
            print_json(
                &engine
                    .restore_remote(&read_json(point)?, remote, allow_unregistered_import)
                    .await?,
            )?;
        }
        Command::CheckSuccession { prior, target } => {
            let (prior, target): (ReleasePin, ReleasePin) = (read_json(prior)?, read_json(target)?);
            print_json(
                &engine
                    .prove_succession(&prior.version_ref(), &target.version_ref())
                    .await?,
            )?;
        }
        Command::SealGeneration(SpecArg { spec }) => {
            print_json(&engine.seal_generation(&read_json(spec)?).await?)?
        }
        Command::CaptureInputs(SpecArg { spec }) => {
            let inputs: std::collections::BTreeMap<Id, VersionRef> = read_json(spec)?;
            print_json(&engine.capture_inputs(&inputs).await?)?;
        }
        Command::CorrectDependencies(SpecArg { spec }) => {
            print_json(&engine.correct_dependencies(&read_json(spec)?).await?)?
        }
        Command::Compare {
            before,
            after,
            group,
            identity,
            occurrences,
            force_full,
        } => {
            let identity = identity.map_or(
                if occurrences {
                    ComparisonIdentity::Occurrences
                } else {
                    ComparisonIdentity::Values
                },
                ComparisonIdentity::from,
            );
            let options = ComparisonOptions {
                identity,
                force_full,
            };
            print_json(
                &engine
                    .compare_with_options(&read_json(before)?, &read_json(after)?, &group, &options)
                    .await?,
            )?
        }
        Command::Changes {
            before,
            after,
            output,
            group,
            identity,
            values,
            force_full,
        } => {
            let identity = identity.map_or(
                if values {
                    ComparisonIdentity::Values
                } else {
                    ComparisonIdentity::Occurrences
                },
                ComparisonIdentity::from,
            );
            let options = ComparisonOptions {
                identity,
                force_full,
            };
            let stream = engine
                .changes(&read_json(before)?, &read_json(after)?, &group, &options)
                .await?;
            write_stream(output, stream).await?;
            engine.shutdown().await?;
        }
        Command::Invalidate {
            dataset,
            reason,
            definition,
        } => print_json(
            &serde_json::json!({"eligibility": engine.invalidate(&Id::new(dataset)?, &reason, definition).await?}),
        )?,
        Command::Read {
            release,
            output,
            group,
        } => {
            let stream = engine.read_group(&read_json(release)?, &group).await?;
            write_stream(output, stream).await?;
            engine.shutdown().await?;
        }
        Command::ReadKeys {
            release,
            spec,
            output,
        } => {
            let stream = engine
                .read_keys(&read_json(release)?, &read_json(spec)?)
                .await?;
            write_stream(output, stream).await?;
            engine.shutdown().await?;
        }
        Command::ReadExport { pin, output } => {
            let stream = engine.read_export(&read_json(pin)?).await?;
            write_stream(output, stream).await?;
            engine.shutdown().await?;
        }
        Command::Serve {
            bind,
            ready_file,
            shutdown_on_stdin_eof,
        } => {
            serve_http(engine, bind, ready_file, shutdown_on_stdin_eof).await?;
        }
        _ => unreachable!("independent-reader commands handled before opening workspace"),
    }
    Ok(())
}

async fn serve_http(
    engine: Engine,
    bind: SocketAddr,
    ready_file: Option<PathBuf>,
    shutdown_on_stdin_eof: bool,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let stdin_closed = if shutdown_on_stdin_eof {
        let (finished, closed) = tokio::sync::oneshot::channel();
        let _ = std::thread::Builder::new()
            .name("docspec-owner-stdin".into())
            .spawn(move || {
                let result = std::io::copy(&mut std::io::stdin().lock(), &mut std::io::sink());
                let _ = finished.send(result);
            })?;
        Some(closed)
    } else {
        None
    };
    if let Some(path) = ready_file {
        write_ready_file(&path, listener.local_addr()?)?;
    }
    let result = axum::serve(listener, docspec::http::router(engine.clone()))
        .with_graceful_shutdown(async move {
            let owner_closed = async move {
                if let Some(closed) = stdin_closed {
                    if let Ok(Err(error)) = closed.await {
                        tracing::warn!(name: "host.owner_stdin.failed", error = %error, "owner stdin read failed");
                    }
                } else {
                    std::future::pending::<()>().await;
                }
            };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = owner_closed => {},
            }
        })
        .await;
    engine.shutdown().await?;
    result?;
    Ok(())
}

fn check_startup(cli: &Cli) -> docspec::Result<()> {
    #[cfg(feature = "remote")]
    if matches!(&cli.command, Command::RestoreRemote { .. }) && cli.remote.is_none() {
        return Err(docspec::Error::refusal(
            "remote_config",
            "remote restore requires explicit source maintenance configuration",
        ));
    }
    if cli.remote.is_some() {
        #[cfg(not(feature = "remote"))]
        return Err(docspec::Error::refusal(
            "remote_feature",
            "remote startup requires the remote feature",
        ));
        #[cfg(feature = "remote")]
        if matches!(
            &cli.command,
            Command::Hash { .. }
                | Command::Verify { .. }
                | Command::VerifyInputs { .. }
                | Command::RestoreLocal { .. }
                | Command::ReadEvidence(_)
                | Command::ReadReceipts(_)
        ) || matches!(
            &cli.command,
            Command::Read { group, .. }
                | Command::Compare { group, .. }
                | Command::Changes { group, .. }
                if docspec::BackendCapability::for_group(group).local_only()
        ) {
            return Err(docspec::Error::refusal(
                "remote_command_unavailable",
                "this command uses unqualified independent local I/O; selected remote reads support the source group",
            ));
        }
    }
    if let Command::Serve { bind, .. } = &cli.command
        && !bind.ip().is_loopback()
    {
        return Err(docspec::Error::refusal(
            "hosted_access_unqualified",
            "hosted access is unqualified; bind to loopback",
        ));
    }
    Ok(())
}

#[cfg(feature = "remote")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteConfig {
    owned_prefix: String,
    #[serde(default)]
    properties: std::collections::HashMap<String, String>,
    #[serde(default)]
    secret_env: std::collections::HashMap<String, String>,
}

#[cfg(feature = "remote")]
fn read_remote_config(path: PathBuf) -> docspec::Result<RemoteConfig> {
    read_json(path).map_err(|_| {
        docspec::Error::refusal(
            "remote_config",
            "remote startup configuration is unavailable, oversized or invalid",
        )
    })
}

async fn open_engine(
    workspace: &std::path::Path,
    limits: Limits,
    remote: Option<&std::path::Path>,
) -> anyhow::Result<Engine> {
    if let Some(path) = remote {
        #[cfg(feature = "remote")]
        {
            let config = read_remote_config(path.to_owned())?;
            let remote = storage::remote::remote_storage(
                &config.owned_prefix,
                config.properties,
                config.secret_env,
            )
            .await?;
            return Ok(Engine::with_remote(workspace, remote, limits).await?);
        }
        #[cfg(not(feature = "remote"))]
        {
            let _ = path;
            anyhow::bail!("remote_feature: remote startup requires the remote feature");
        }
    }
    Ok(Engine::open(workspace, limits).await?)
}

async fn write_stream(
    output: PathBuf,
    mut stream: datafusion::physical_plan::SendableRecordBatchStream,
) -> anyhow::Result<()> {
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

fn write_ready_file(path: &std::path::Path, address: SocketAddr) -> anyhow::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("ready file needs a private parent directory"))?;
        anyhow::ensure!(
            std::fs::metadata(parent)?.permissions().mode() & 0o077 == 0,
            "ready file needs a private parent directory"
        );
        options.mode(0o600);
    }
    let mut output = BufWriter::new(options.open(path)?);
    serde_json::to_writer(
        &mut output,
        &serde_json::json!({
            "protocol": "docspec-loopback", "version": docspec::http::PROTOCOL_VERSION,
            "capabilities": ["request-deadline", "owned-stdin-eof"],
            "pid": std::process::id(), "address": address,
        }),
    )?;
    output.flush()?;
    Ok(())
}

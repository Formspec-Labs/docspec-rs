//! Command-line entry point for the local execution host.

use clap::{Parser, Subcommand};
use docspec::{
    Engine,
    budget::{Budget, Limits},
    engine::verify_release,
    model::{BackfillSpec, Id, ReleasePin, RunSpec, UpdateSpec},
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
    /// Resolve the current immutable release pin.
    Latest { dataset: String },
    /// Resolve an immutable historical release by its operation identity.
    Release { dataset: String, version: String },
    /// Prove that target follows prior on accepted history, without reading payloads.
    CheckSuccession { prior: PathBuf, target: PathBuf },
    /// Plan complete key replacements and deletions against an exact base.
    PlanUpdate { spec: PathBuf },
    /// Apply or resume an update using its stable operation identity.
    Update { spec: PathBuf },
    /// Explain completed and pending computed field-group files.
    PlanBackfill { spec: PathBuf },
    /// Compute or resume an independent scalar field group.
    Backfill { spec: PathBuf },
    /// Stream a pinned combined view as Arrow IPC.
    ReadCombined { release: PathBuf, output: PathBuf },
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
    Verify { release: PathBuf },
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
        Command::Verify { release } => {
            let release: ReleasePin = read_json(release)?;
            verify_release(&release, limits).await?;
            print_json(&serde_json::json!({"verified": true, "release": release.id}))?;
        }
        Command::Read {
            release,
            output,
            group,
        } => {
            let release: ReleasePin = read_json(release)?;
            verify_release(&release, limits.clone()).await?;
            let pin = release
                .dataset
                .groups
                .get(&group)
                .ok_or_else(|| anyhow::anyhow!("unknown field group"))?;
            let table = storage::open_pin(pin).await?;
            let schema = iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())?;
            let file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(output)?;
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(BufWriter::new(file), &schema)?;
            if pin.snapshot.is_some() {
                let budget = Budget::new(limits)?;
                let stream = storage::read_batches(&table, &budget).await?;
                futures::pin_mut!(stream);
                while let Some(batch) = stream.try_next().await? {
                    writer.write(&batch)?;
                }
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
                        PausedOperation::Backfill => {
                            engine
                                .backfill_until(&read_json::<BackfillSpec>(spec)?, &phase)
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
                Command::PlanUpdate { spec } => {
                    print_json(&engine.plan_update(&read_json::<UpdateSpec>(spec)?).await?)?
                }
                Command::Update { spec } => {
                    print_json(&engine.update(&read_json::<UpdateSpec>(spec)?).await?)?
                }
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
                    axum::serve(listener, docspec::http::router(engine))
                        .with_graceful_shutdown(async {
                            let _ = tokio::signal::ctrl_c().await;
                        })
                        .await?;
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

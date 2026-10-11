//! Bounded child-process rendezvous for final native publication only.
use super::*;
use serde::{Deserialize, Serialize};
use std::{
    fs::OpenOptions,
    io::Write,
    process::{Child, Command, Stdio},
    time::Instant,
};

const CHILD_TEST: &str =
    "engine::remote_build_provider_tests::publication_race::processes::publication_child";
const PROCESS_DEADLINE: Duration = Duration::from_secs(120);

#[derive(Serialize, Deserialize)]
struct Report {
    pid: u32,
    metadata: String,
    outcome: std::result::Result<ExportPin, String>,
}

fn write_new(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut file = tempfile::NamedTempFile::new_in(path.parent().unwrap())?;
    file.write_all(&serde_json::to_vec(value)?)?;
    file.as_file().sync_all()?;
    file.persist_noclobber(path)
        .map_err(|error| Error::from(error.error))?;
    Ok(())
}

pub(super) async fn hold(directory: &Path, side: &str, metadata: &str) -> iceberg::Result<()> {
    let result: Result<()> = async {
        write_new(&directory.join(format!("{side}.metadata")), &metadata)?;
        let start = Instant::now();
        loop {
            let paths = [
                directory.join("left.metadata"),
                directory.join("right.metadata"),
            ];
            if paths.iter().all(|path| path.is_file()) {
                let [left, right] = paths
                    .map(|path| serde_json::from_slice::<String>(&std::fs::read(path).unwrap()));
                assert_eq!(
                    left?, right?,
                    "children captured different catalog metadata"
                );
                return Ok(());
            }
            if start.elapsed() >= HOLD_DEADLINE {
                return Err(Error::refusal(
                    "provider_evidence",
                    "publication child peer deadline",
                ));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    .await;
    result.map_err(|_| {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "independent publication rendezvous refused",
        )
        .with_retryable(false)
    })
}

struct Children(Vec<Child>);
impl Drop for Children {
    fn drop(&mut self) {
        for child in &mut self.0 {
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
            }
            let _ = child.wait();
        }
    }
}

pub(super) async fn run(
    directory: &Path,
    prefix: &str,
    left: &ExportSpec,
    right: &ExportSpec,
) -> Result<(
    std::result::Result<ExportPin, String>,
    std::result::Result<ExportPin, String>,
    Vec<String>,
)> {
    let mut children = Children(Vec::new());
    for (side, spec) in [("left", left), ("right", right)] {
        write_new(&directory.join(format!("{side}.request")), spec)?;
        let log = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join(format!("{side}.log")))?;
        children.0.push(
            Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    CHILD_TEST,
                    "--ignored",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env("DOCSPEC_PUBLICATION_CHILD", directory)
                .env("DOCSPEC_PUBLICATION_SIDE", side)
                .env("DOCSPEC_PUBLICATION_PREFIX", prefix)
                .stdout(Stdio::from(log.try_clone()?))
                .stderr(Stdio::from(log))
                .spawn()?,
        );
    }
    let start = Instant::now();
    loop {
        let statuses = children
            .0
            .iter_mut()
            .map(Child::try_wait)
            .collect::<std::io::Result<Vec<_>>>()?;
        if statuses.iter().all(Option::is_some) {
            assert!(
                statuses.iter().all(|status| status.unwrap().success()),
                "publication child failed; inspect child logs"
            );
            break;
        }
        if start.elapsed() >= PROCESS_DEADLINE {
            return Err(Error::refusal(
                "provider_evidence",
                "publication child process deadline",
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut reports = Vec::new();
    for (index, side) in ["left", "right"].iter().enumerate() {
        let bytes = std::fs::read(directory.join(format!("{side}.result")))?;
        assert!(bytes.len() <= 1 << 20);
        let report: Report = serde_json::from_slice(&bytes)?;
        assert_eq!(report.pid, children.0[index].id());
        reports.push(report);
    }
    assert_ne!(reports[0].pid, reports[1].pid);
    assert_ne!(reports[0].pid, std::process::id());
    assert_ne!(reports[1].pid, std::process::id());
    let right = reports.pop().unwrap();
    let left = reports.pop().unwrap();
    println!(
        "{}",
        json!({"phase":"publication_processes", "pids":[left.pid,right.pid], "parent_pid":std::process::id()})
    );
    Ok((
        left.outcome,
        right.outcome,
        vec![left.metadata, right.metadata],
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "invoked only by the isolated publication parent"]
async fn publication_child() {
    let directory = std::path::PathBuf::from(env("DOCSPEC_PUBLICATION_CHILD").unwrap());
    assert!(directory.is_absolute());
    let side = env("DOCSPEC_PUBLICATION_SIDE").unwrap();
    assert!(matches!(side.as_str(), "left" | "right"));
    let prefix = env("DOCSPEC_PUBLICATION_PREFIX").unwrap();
    let spec: ExportSpec =
        serde_json::from_slice(&std::fs::read(directory.join(format!("{side}.request"))).unwrap())
            .unwrap();
    assert_eq!(spec.operation.as_str(), side);
    let expected_prefix = format!(
        "s3://{}/__r2_data_catalog/docspec-qualification/{}/publication-race",
        env("DOCSPEC_S3_BUCKET").unwrap(),
        env("DOCSPEC_QUALIFICATION_ID").unwrap()
    );
    assert_eq!(prefix, expected_prefix);
    assert_eq!(
        spec.dataset.as_str(),
        env("DOCSPEC_QUALIFICATION_ID").unwrap()
    );
    let mut remote = connect(&prefix).await.unwrap();
    let catalog = Arc::new(RaceCatalog {
        inner: remote.catalog,
        lifecycle: crate::catalog::lifecycle_ident(&spec.dataset),
        armed: AtomicBool::new(false),
        loaded: StdMutex::new(None),
        rendezvous: Arc::new(Rendezvous {
            barrier: tokio::sync::Barrier::new(2),
            metadata: StdMutex::new(Vec::new()),
            files: Some((directory.clone(), side.clone())),
        }),
    });
    remote.catalog = catalog.clone();
    let engine = Engine::with_remote(directory.join(format!("host-{side}")), remote, limits())
        .await
        .unwrap();
    catalog.arm();
    let outcome = engine
        .export(&spec)
        .await
        .map_err(|error| error.code().to_owned());
    // Capture the held pre-commit head, not any later reconciliation load.
    let metadata: String =
        serde_json::from_slice(&std::fs::read(directory.join(format!("{side}.metadata"))).unwrap())
            .unwrap();
    write_new(
        &directory.join(format!("{side}.result")),
        &Report {
            pid: std::process::id(),
            metadata,
            outcome,
        },
    )
    .unwrap();
    engine.shutdown().await.unwrap();
}

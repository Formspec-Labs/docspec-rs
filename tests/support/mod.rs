//! Shared real-process recovery harness; all workspaces belong to the invoking fixture.

use std::{
    io::{BufRead, BufReader},
    path::Path,
    process::{Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

/// Kills the CLI only after it acknowledges the requested durable handoff.
pub fn kill_after(root: &Path, request: &impl serde::Serialize, kind: &str, phase: &str) {
    let spec = root.join("request.json");
    std::fs::write(&spec, serde_json::to_vec(request).unwrap()).unwrap();
    let workspace = root.join("workspace");
    let mut child = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("pause-after")
        .arg(&spec)
        .arg(phase)
        .args(["--kind", kind])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stderr = child.stderr.take().unwrap();
    let (send, receive) = mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        let mut recent = std::collections::VecDeque::new();
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if line == "DOCSPEC_TEST_PAUSED" {
                let _ = send.send(());
                break;
            }
            // Keep bounded diagnostic history when a worker fails before its marker.
            recent.push_back(line.chars().take(4096).collect::<String>());
            if recent.len() > 16 {
                recent.pop_front();
            }
        }
        recent
    });
    // These are small correctness fixtures; a deadline prevents a hung worker
    // from stalling the gate. It is not a performance acceptance threshold.
    let reached = receive.recv_timeout(Duration::from_secs(30));
    let killed = child.kill();
    let status = child.wait().unwrap();
    let recent = reader.join().unwrap();
    assert!(
        reached.is_ok(),
        "{kind}/{phase}: worker did not acknowledge the handoff: {recent:?} ({status})"
    );
    killed.expect("acknowledged worker must still be alive for termination");
    assert!(!status.success());
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            status.signal(),
            Some(9),
            "fixture must actually send SIGKILL"
        );
    }
    eprintln!("killed {kind} worker after {phase}; status={status}");
}

/// Resumes through another CLI process using the request retained by `kill_after`.
pub fn resume(root: &Path, command: &str) -> (docspec::model::ReleasePin, Vec<String>) {
    // File-backed output avoids blocking a large release on an unread pipe.
    let stdout_path = root.join("resumed.json");
    let stderr_path = root.join("resumed.stderr");
    let mut child = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(root.join("workspace"))
        .arg(command)
        .arg(root.join("request.json"))
        .stdout(std::fs::File::create(&stdout_path).unwrap())
        .stderr(std::fs::File::create(&stderr_path).unwrap())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            child.wait().unwrap();
            panic!("{command}: resume exceeded the fixture deadline; worker terminated");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let stderr = std::fs::read_to_string(stderr_path).unwrap();
    assert!(status.success(), "{command}: {stderr}");
    let work = stderr
        .lines()
        .filter_map(|line| line.strip_prefix("DOCSPEC_TEST_WORK "))
        .map(str::to_owned)
        .collect();
    (
        serde_json::from_reader(std::fs::File::open(stdout_path).unwrap()).unwrap(),
        work,
    )
}

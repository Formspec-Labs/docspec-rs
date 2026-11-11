//! What the CLI process reports about itself: its open-file limit and a failed statement.
// Rust guideline compliant 2026-02-21
#![cfg(unix)]

use docspec::budget::Limits;
use serde_json::{Value, json};
use std::process::{Command, Output};

/// The soft limit launchd gives a scheduled job.
const LOW: u64 = 256;

/// The soft limit the CLI is documented to ask for (`OPEN_FILES` in `src/main.rs`).
const STATED: u64 = 65_536;

/// What the CLI asks for once if the kernel refuses that (`OPEN_FILES_FALLBACK`).
const FALLBACK: u64 = 10_240;

/// Runs `script` in `sh` with the CLI as `$0` and `arguments` as `$@`.
fn sh(script: &str, arguments: &[&std::ffi::OsStr]) -> Output {
    Command::new("/bin/sh")
        .args(["-c", script])
        .arg(env!("CARGO_BIN_EXE_docspec"))
        .args(arguments)
        .env("RUST_LOG", "docspec=debug")
        .env("NO_COLOR", "1")
        .output()
        .unwrap()
}

/// Reads `ulimit` output or a `Some(n)`/`None` trace field; `None` is unlimited.
fn limit(text: &str) -> Option<u64> {
    match text.trim() {
        "unlimited" | "None" => None,
        number => Some(
            number
                .trim_start_matches("Some(")
                .trim_end_matches(')')
                .parse()
                .unwrap_or_else(|_| panic!("not a limit: {text:?}")),
        ),
    }
}

/// Starts the CLI after `ulimit_command` and returns the (soft, hard) limits it reports.
fn limits_after(ulimit_command: &str) -> (Option<u64>, Option<u64>) {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("input");
    std::fs::write(&input, b"any bytes").unwrap();
    let output = sh(
        &format!("{ulimit_command} && exec \"$0\" \"$@\""),
        &["hash".as_ref(), "--input".as_ref(), input.as_os_str()],
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    // Raising, or failing to raise, the limit never fails the command.
    assert!(output.status.success(), "{stderr}");
    let line = stderr
        .lines()
        .find(|line| line.contains("open-file limit"))
        .unwrap_or_else(|| panic!("the CLI reported no open-file limit: {stderr}"));
    let field = |name: &str| {
        let value = line.split(name).nth(1).expect(name);
        limit(value.split_whitespace().next().expect(name))
    };
    (field(" soft="), field(" hard="))
}

/// The hard limit this test process hands to its children; `None` is unlimited.
fn inherited_hard_limit() -> Option<u64> {
    limit(&String::from_utf8_lossy(&sh("ulimit -H -n", &[]).stdout))
}

#[test]
fn a_low_soft_limit_is_raised_to_the_stated_number_or_the_hard_limit() {
    let hard = inherited_hard_limit();
    if hard.is_some_and(|hard| hard <= LOW) {
        eprintln!(
            "skipped: the hard limit here is {hard:?}, so nothing above {LOW} can be asked for"
        );
        return;
    }
    let target = hard.map_or(STATED, |hard| hard.min(STATED));
    let (soft, reported_hard) = limits_after(&format!("ulimit -S -n {LOW}"));
    assert_eq!(reported_hard, hard, "the hard limit must not move");
    // Never unlimited, whatever the hard limit is.
    let soft = soft.expect("a soft limit raised from a number is a number");
    // A kernel that refuses the target gets one retry at the fallback; none
    // has been seen to refuse, and either way the limit went up.
    assert!(soft == target || soft == FALLBACK, "{soft}");
    assert!(soft > LOW, "{soft}");
}

#[test]
fn a_hard_limit_below_the_stated_number_is_respected() {
    let hard = inherited_hard_limit();
    // A hard limit under the stated number, or this machine's own if lower.
    let lowered = hard.map_or(4096, |hard| hard.min(4096));
    if lowered > LOW {
        // Lowering the hard limit binds only the child that sh becomes. The soft
        // limit goes first: sh refuses a hard limit under the soft one in force.
        let limits = limits_after(&format!("ulimit -S -n {LOW} && ulimit -H -n {lowered}"));
        assert_eq!(limits, (Some(lowered), Some(lowered)));
    } else {
        eprintln!("skipped the lowered case: the hard limit here is {hard:?}");
    }
    // Without -S or -H, sh sets both limits: nothing is left to raise.
    let floor = hard.map_or(LOW, |hard| hard.min(LOW));
    let limits = limits_after(&format!("ulimit -n {floor}"));
    assert_eq!(limits, (Some(floor), Some(floor)));
}

#[test]
fn a_soft_limit_above_the_stated_number_is_not_lowered() {
    let above = STATED + 1000;
    let hard = inherited_hard_limit();
    if !sh(&format!("ulimit -S -n {above}"), &[]).status.success() {
        eprintln!("skipped: a soft limit of {above} cannot be set here (hard limit {hard:?})");
        return;
    }
    let (soft, _) = limits_after(&format!("ulimit -S -n {above}"));
    assert_eq!(soft, Some(above));
}

#[test]
fn a_failed_statement_is_named_on_stdout_and_stderr() {
    let root = tempfile::tempdir().unwrap();
    let write = |name: &str, value: Value| {
        let path = root.path().join(name);
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        path
    };
    let spec = write(
        "job.json",
        json!({"scratch":root.path().join("scratch"),"inputs":[],"steps":[],"outputs":[],"queries":[
            {"name":"fine","sql":"SELECT 1 AS id"},
            {"name":"broken","sql":"SELECT absent_column"},
        ]}),
    );
    let limits = write(
        "limits.json",
        serde_json::to_value(Limits::default()).unwrap(),
    );
    let output = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--limits")
        .arg(limits)
        .args(["job", "--spec"])
        .arg(spec)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["status"], "failed");
    assert_eq!(report["completed"]["queries"][0]["name"], "fine");
    assert_eq!(report["error"]["code"], "query");
    let named = "query 1 \"broken\": ";
    let message = report["error"]["message"].as_str().unwrap();
    assert!(
        message.starts_with(named) && message.contains("absent_column"),
        "{message}"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&format!("Error: query: {named}")),
        "{stderr}"
    );
}

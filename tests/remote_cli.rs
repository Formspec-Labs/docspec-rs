//! CLI storage selection refuses unsupported local I/O before startup effects.
use std::process::{Command, Output};

fn invoke(root: &std::path::Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_docspec"))
        .current_dir(root)
        .args(["--workspace", "host"])
        .args(args)
        .env("DOCSPEC_TEST_REMOTE_ACCESS", "synthetic-access")
        .env("DOCSPEC_TEST_REMOTE_SECRET", "synthetic-secret")
        .env("DOCSPEC_TEST_REMOTE_TOKEN", "synthetic-token")
        .output()
        .unwrap()
}

#[test]
fn local_default_retains_independent_hash_without_opening_host() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("input"), b"original input").unwrap();
    let output = invoke(root.path(), &["hash", "input"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        value["digest"],
        blake3::hash(b"original input").to_hex().as_str()
    );
    assert_eq!(value["bytes"], 14);
    assert!(!root.path().join("host").exists());
}

#[cfg(not(feature = "remote"))]
#[test]
fn missing_remote_feature_refuses_before_configuration_or_host_open() {
    let root = tempfile::tempdir().unwrap();
    let output = invoke(root.path(), &["--remote", "missing.json", "init"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("remote_feature"));
    assert!(!root.path().join("host").exists());
}

#[cfg(feature = "remote")]
#[test]
fn independent_local_commands_refuse_remote_before_config_limits_and_output_effects() {
    let root = tempfile::tempdir().unwrap();
    for args in [
        vec!["hash", "missing-input"],
        vec!["verify", "missing-release"],
        vec!["verify-inputs", "missing-coverage"],
        vec!["restore-local", "missing-point"],
        vec!["read-evidence", "missing-release", "output.arrow"],
        vec!["read-receipts", "missing-release", "output.arrow"],
        vec![
            "read",
            "missing-release",
            "output.arrow",
            "--group",
            "computed",
        ],
    ] {
        let mut invocation = vec!["--remote", "missing-config", "--limits", "missing-limits"];
        invocation.extend(args);
        let output = invoke(root.path(), &invocation);
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("remote_command_unavailable"),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(!root.path().join("host").exists());
    assert!(!root.path().join("output.arrow").exists());
}

#[cfg(feature = "remote")]
fn scope_refusal_config(root: &std::path::Path) {
    // Invalid owned scope reaches the maintained resolver, which refuses before
    // catalog connection. No fixture service or provider request is needed.
    let config = serde_json::json!({
        "owned_prefix": "s3://bucket/",
        "properties": {
            "uri": "https://catalog.invalid", "warehouse": "synthetic",
            "s3.endpoint": "https://s3.invalid", "s3.region": "auto",
            "s3.path-style-access": "true"
        },
        "secret_env": {
            "token": "DOCSPEC_TEST_REMOTE_TOKEN",
            "s3.access-key-id": "DOCSPEC_TEST_REMOTE_ACCESS",
            "s3.secret-access-key": "DOCSPEC_TEST_REMOTE_SECRET"
        }
    });
    std::fs::write(
        root.join("remote.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
}

#[cfg(feature = "remote")]
#[test]
fn selected_source_read_and_loopback_serve_use_explicit_remote_configuration() {
    let root = tempfile::tempdir().unwrap();
    scope_refusal_config(root.path());
    for args in [
        vec![
            "read",
            "missing-release",
            "output.arrow",
            "--remote",
            "remote.json",
        ],
        vec!["--remote", "remote.json", "serve", "--bind", "127.0.0.1:0"],
    ] {
        let output = invoke(root.path(), &args);
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("remote_scope"), "{stderr}");
        for value in ["synthetic-access", "synthetic-secret", "synthetic-token"] {
            assert!(!stderr.contains(value));
        }
    }
    assert!(!root.path().join("host").exists());
    assert!(!root.path().join("output.arrow").exists());
}

#[cfg(feature = "remote")]
#[test]
fn malformed_remote_config_redacts_parser_values_and_unknown_keys() {
    let root = tempfile::tempdir().unwrap();
    for text in [
        r#"{"owned_prefix":"s3://bucket/owned","properties":{"synthetic-secret-key":"synthetic-secret-value"},"unexpected-sensitive-key":true}"#,
        r#"{"owned_prefix":"synthetic-secret-value","properties":{"token":["synthetic-secret-value"]}}"#,
    ] {
        std::fs::write(root.path().join("remote.json"), text).unwrap();
        let output = invoke(root.path(), &["--remote", "remote.json", "init"]);
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("remote_config"), "{stderr}");
        for value in [
            "synthetic-secret-key",
            "synthetic-secret-value",
            "unexpected-sensitive-key",
        ] {
            assert!(!stderr.contains(value));
        }
    }
    assert!(!root.path().join("host").exists());
}

#[test]
fn non_loopback_serve_refuses_before_host_or_configuration_effects() {
    let root = tempfile::tempdir().unwrap();
    let output = invoke(root.path(), &["serve", "--bind", "0.0.0.0:7391"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("hosted_access_unqualified"));
    assert!(!root.path().join("host").exists());
}

// Rust guideline compliant 2026-02-21

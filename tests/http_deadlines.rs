//! Request deadlines refuse malformed budgets and retain native work after caller timeout.
// Rust guideline compliant 2026-02-21

use super::scalar;
use axum::{
    body::{Body, Bytes, to_bytes},
    http::{Request, StatusCode},
};
use docspec::Engine;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tower::ServiceExt;

#[tokio::test]
async fn malformed_deadlines_refuse_before_polling_request_payload() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), scalar::limits()).await.unwrap();
    let router = docspec::http::router(engine);
    for header in ["0", "", "-1", "1.5", "NaN", "18446744073709551616"] {
        let polled = Arc::new(AtomicBool::new(false));
        let observed = polled.clone();
        let body = Body::from_stream(futures::stream::once(async move {
            observed.store(true, Ordering::SeqCst);
            Ok::<_, std::io::Error>(Bytes::from_static(b"{}"))
        }));
        let response = router
            .clone()
            .oneshot(
                Request::post("/run")
                    .header("content-type", "application/json")
                    .header("docspec-timeout-ms", header)
                    .body(body)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{header}");
        assert!(!polled.load(Ordering::SeqCst));
        let value: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(value["code"], "invalid_request_timeout");
    }
}

#[tokio::test]
async fn deadline_includes_request_payload_before_response_headers() {
    let root = tempfile::tempdir().unwrap();
    let engine = Engine::open(root.path(), scalar::limits()).await.unwrap();
    let body = Body::from_stream(futures::stream::once(async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok::<_, std::io::Error>(Bytes::from_static(b"{}"))
    }));
    let response = docspec::http::router(engine)
        .oneshot(
            Request::post("/run")
                .header("content-type", "application/json")
                .header("docspec-timeout-ms", "10")
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let value: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
    assert_eq!(value["code"], "request_timeout");
}

#[cfg(feature = "test-util")]
#[tokio::test]
async fn timed_out_http_caller_keeps_admitted_native_completion_and_unrelated_reads() {
    let root = tempfile::tempdir().unwrap();
    let (engine, base, _) = scalar::fixture(root.path(), false).await;
    let first = scalar::spec(
        &base,
        "completed-files",
        "stats",
        "length",
        "CAST(length(text) AS BIGINT)",
    );
    assert_eq!(
        engine
            .backfill_until(&first, "files")
            .await
            .unwrap_err()
            .code(),
        "injected_failure"
    );
    let request = scalar::spec(
        &base,
        "timed-out",
        "stats",
        "length",
        "CAST(length(text) AS BIGINT)",
    );
    let (reached, release) = engine
        .pause_scalar_file_reuse(request.dataset.clone(), request.operation.clone())
        .await;
    let router = docspec::http::router(engine.clone());
    let caller = tokio::spawn(
        router.clone().oneshot(
            Request::post("/backfills/run")
                .header("content-type", "application/json")
                .header("docspec-timeout-ms", "1000")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        ),
    );
    tokio::time::timeout(Duration::from_secs(30), reached)
        .await
        .unwrap()
        .unwrap();
    let other = router
        .clone()
        .oneshot(
            Request::get("/datasets/comments/latest")
                .header("docspec-timeout-ms", "1000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(other.status(), StatusCode::OK);
    let response = tokio::time::timeout(Duration::from_secs(5), caller)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(engine.latest(&request.dataset).await.unwrap(), base);
    release.send(()).unwrap();
    let completed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(pin) = engine.release(&request.dataset, &request.operation).await {
                break pin;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let before = engine.inspect(&request.dataset).await.unwrap();
    let response = router
        .oneshot(
            Request::post("/backfills/run")
                .header("content-type", "application/json")
                .header("docspec-timeout-ms", "5000")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let replayed: docspec::model::ReleasePin =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(replayed, completed);
    assert_eq!(engine.inspect(&request.dataset).await.unwrap(), before);
    engine.shutdown().await.unwrap();
}

#[cfg(unix)]
#[test]
fn cli_ready_record_names_the_owned_bound_listener_and_refuses_existing_paths() {
    use std::{
        fs::File,
        io::Read,
        os::unix::fs::PermissionsExt,
        process::{Command, Stdio},
        time::Instant,
    };
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let limits = root.path().join("limits.json");
    std::fs::write(&limits, serde_json::to_vec(&scalar::limits()).unwrap()).unwrap();
    let ready = root.path().join("ready.json");
    let stderr = root.path().join("stderr");
    let mut child = Child(
        Command::new(env!("CARGO_BIN_EXE_docspec"))
            .arg("--workspace")
            .arg(root.path().join("workspace"))
            .arg("--limits")
            .arg(&limits)
            .args(["serve", "--bind", "127.0.0.1:0", "--ready-file"])
            .arg(&ready)
            .stdout(Stdio::null())
            .stderr(File::create(&stderr).unwrap())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    let value: serde_json::Value = loop {
        if let Ok(bytes) = std::fs::read(&ready)
            && let Ok(value) = serde_json::from_slice(&bytes)
        {
            break value;
        }
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "{}",
            std::fs::read_to_string(&stderr).unwrap()
        );
        assert!(
            Instant::now() < deadline,
            "owned host did not publish readiness"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(value["pid"], child.0.id());
    assert_eq!(value["protocol"], "docspec-loopback");
    assert_eq!(value["version"], 1);
    assert_eq!(
        value["capabilities"],
        serde_json::json!(["request-deadline", "owned-stdin-eof"])
    );
    let address: std::net::SocketAddr = value["address"].as_str().unwrap().parse().unwrap();
    assert!(address.ip().is_loopback());
    assert_ne!(address.port(), 0);
    assert_eq!(
        std::fs::metadata(&ready).unwrap().permissions().mode() & 0o077,
        0
    );
    // Use an actual socket: the readiness record must correspond to a serving listener.
    let mut socket = std::net::TcpStream::connect(address).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    std::io::Write::write_all(
        &mut socket,
        b"GET /missing HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .unwrap();
    let mut reply = String::new();
    socket.read_to_string(&mut reply).unwrap();
    assert!(reply.starts_with("HTTP/1.1 404"));
    drop(child);
    let original = std::fs::read(&ready).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_docspec"))
        .arg("--workspace")
        .arg(root.path().join("workspace"))
        .arg("--limits")
        .arg(&limits)
        .args(["serve", "--bind", "127.0.0.1:0", "--ready-file"])
        .arg(&ready)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(std::fs::read(&ready).unwrap(), original);
}

#[cfg(unix)]
#[test]
fn cli_stdin_shutdown_is_opt_in_and_sigint_does_not_wait_for_open_stdin() {
    use std::{
        fs::File,
        io::Write,
        os::unix::fs::PermissionsExt,
        process::{Command, Stdio},
        time::Instant,
    };
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for (owned, close_stdin) in [(true, true), (true, false), (false, true)] {
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let limits = root.path().join("limits.json");
        std::fs::write(&limits, serde_json::to_vec(&scalar::limits()).unwrap()).unwrap();
        let ready = root.path().join("ready.json");
        let stderr = root.path().join("stderr");
        let mut command = Command::new(env!("CARGO_BIN_EXE_docspec"));
        command
            .arg("--workspace")
            .arg(root.path().join("workspace"))
            .arg("--limits")
            .arg(&limits)
            .args(["serve", "--bind", "127.0.0.1:0", "--ready-file"])
            .arg(&ready)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(File::create(&stderr).unwrap());
        if owned {
            command.arg("--shutdown-on-stdin-eof");
        }
        let mut child = Child(command.spawn().unwrap());
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(bytes) = std::fs::read(&ready)
                && serde_json::from_slice::<serde_json::Value>(&bytes).is_ok()
            {
                break;
            }
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "{}",
                std::fs::read_to_string(&stderr).unwrap()
            );
            assert!(Instant::now() < deadline, "host did not publish readiness");
            std::thread::sleep(Duration::from_millis(10));
        }
        child
            .0
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"still-owned")
            .unwrap();
        if close_stdin {
            drop(child.0.stdin.take());
        }
        if !owned || !close_stdin {
            // Manual serve ignores EOF; an owned reader must not block SIGINT exit.
            std::thread::sleep(Duration::from_millis(100));
            assert!(child.0.try_wait().unwrap().is_none());
            assert!(
                Command::new("kill")
                    .args(["-INT", &child.0.id().to_string()])
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let status = loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < deadline, "host shutdown did not finish");
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(
            status.success(),
            "{}",
            std::fs::read_to_string(&stderr).unwrap()
        );
    }
}

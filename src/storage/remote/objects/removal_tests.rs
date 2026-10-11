//! Exact deletion transport without provider access.
// Rust guideline compliant 2026-02-21
use super::*;
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

struct Fixture {
    endpoint: String,
    requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}
impl Fixture {
    fn new(lose_delete: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let shutdown = stop.clone();
        let worker = thread::spawn(move || {
            let mut present = true;
            while !shutdown.load(Ordering::Acquire) {
                let (mut socket, _) = match listener.accept() {
                    Ok(value) => value,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(error) => panic!("accept failed: {error}"),
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut header = Vec::new();
                let mut byte = [0];
                while !header.ends_with(b"\r\n\r\n") && header.len() < 16384 {
                    if socket.read(&mut byte).unwrap_or(0) == 0 {
                        break;
                    }
                    header.push(byte[0]);
                }
                let request = String::from_utf8(header).unwrap();
                let method = request.split_whitespace().next().unwrap().to_owned();
                observed.lock().unwrap().push(method.clone());
                if !present {
                    write!(
                        socket,
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .unwrap();
                } else if method == "DELETE" {
                    present = false;
                    let status = if lose_delete {
                        "500 Internal Server Error"
                    } else {
                        "204 No Content"
                    };
                    write!(
                        socket,
                        "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .unwrap();
                } else if method == "HEAD" {
                    write!(socket, "HTTP/1.1 200 OK\r\nContent-Length: 3\r\nETag: \"fixture\"\r\nConnection: close\r\n\r\n").unwrap();
                } else {
                    assert_eq!(method, "GET");
                    write!(socket, "HTTP/1.1 206 Partial Content\r\nContent-Length: 3\r\nContent-Range: bytes 0-2/3\r\nConnection: close\r\n\r\nabc").unwrap();
                }
            }
        });
        Self {
            endpoint,
            requests,
            stop,
            worker: Some(worker),
        }
    }
    fn objects(&self, temporary: bool) -> RemoteObjects {
        let mut config = HashMap::from([
            ("s3.endpoint".into(), self.endpoint.clone()),
            ("s3.region".into(), "auto".into()),
            ("s3.access-key-id".into(), "synthetic-key".into()),
            ("s3.secret-access-key".into(), "synthetic-secret".into()),
            ("s3.path-style-access".into(), "true".into()),
        ]);
        if temporary {
            config.insert("s3.session-token".into(), "synthetic-temporary".into());
        }
        RemoteObjects::from_resolved(&config, "s3://bucket/owned/").unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}
fn object() -> ObjectRef {
    ObjectRef {
        owner: crate::model::Id::new("owner").unwrap(),
        path: "s3://bucket/owned/object".into(),
        bytes: 3,
        digest: blake3::hash(b"abc").to_hex().to_string(),
    }
}
#[tokio::test]
async fn unknown_delete_reconciles_same_key_without_another_read_or_delete() {
    let fixture = Fixture::new(true);
    let generation = uuid::Uuid::new_v4();
    let expected = object();
    let actor = RemoteRemoval::new(
        fixture.objects(true),
        generation,
        vec![expected.clone()],
        removal_now().unwrap() + 60,
    )
    .unwrap();
    let budget = Budget::new(crate::budget::Limits::default()).unwrap();
    let reader = fixture.objects(false);
    assert_eq!(
        actor
            .remove(&reader, &expected, &budget)
            .await
            .unwrap_err()
            .code(),
        "remote_removal_effect"
    );
    assert_eq!(
        actor.remove(&reader, &expected, &budget).await.unwrap(),
        crate::storage::removal::LocalRemovalOutcome::Absent
    );
    assert!(actor.absent(&expected, &budget).await.unwrap());
    let requests = fixture.requests.lock().unwrap();
    assert_eq!(
        requests.iter().filter(|method| *method == "DELETE").count(),
        1
    );
    assert_eq!(requests.iter().filter(|method| *method == "GET").count(), 1);
}
#[tokio::test]
async fn changed_identity_and_unbound_authority_never_delete() {
    let fixture = Fixture::new(false);
    let generation = uuid::Uuid::new_v4();
    let expected = object();
    assert!(
        RemoteRemoval::new(
            fixture.objects(false),
            generation,
            vec![expected.clone()],
            removal_now().unwrap() + 60
        )
        .is_err()
    );
    let mut wrong = expected.clone();
    wrong.digest = "0".repeat(64);
    let actor = RemoteRemoval::new(
        fixture.objects(true),
        generation,
        vec![wrong.clone()],
        removal_now().unwrap() + 60,
    )
    .unwrap();
    assert!(
        actor
            .check_plan(uuid::Uuid::new_v4(), &[wrong.clone()])
            .is_err()
    );
    assert!(fixture.requests.lock().unwrap().is_empty());
    let budget = Budget::new(crate::budget::Limits::default()).unwrap();
    assert!(
        actor
            .remove(&fixture.objects(false), &wrong, &budget)
            .await
            .is_err()
    );
    assert!(
        !fixture
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|method| method == "DELETE")
    );
    let debug = format!("{actor:?}");
    assert!(!debug.contains("synthetic-secret"));
    assert!(!debug.contains("synthetic-temporary"));
}

//! Actual native S3 writes against a bounded localhost failure boundary.

use super::*;
use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpStream},
    sync::mpsc,
};

const DEADLINE: Duration = Duration::from_secs(15);
const UPLOAD_ID: &str = "owned-upload";

#[tokio::test]
async fn bad_digest_after_part_upload_aborts_only_its_unpublished_upload() {
    // A full part is cached by native MultipartWriter, and WriteGenerator can
    // hold a partial chunk. Two full chunks guarantee initiation/part I/O before
    // the final digest check; the trailing bytes stay unpublished until close.
    let bytes = vec![17; 2 * CHUNK + 31];
    let mut expected = object(&bytes);
    expected.digest = blake3::hash(b"wrong retained digest").to_hex().to_string();
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), bytes).unwrap();
    let fixture = WriteFixture::new(Behavior::AbortMultipart);
    let budget = Budget::new(Limits::default()).unwrap();
    let error = tokio::time::timeout(
        DEADLINE,
        fixture
            .objects()
            .upload_file(&expected, source.path().to_owned(), &budget),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.code(), "object_changed");
    let report = fixture.finish();
    assert_eq!(report.methods("POST"), 1, "no multipart completion request");
    assert!(report.methods("PUT") >= 1, "multipart uploaded real bytes");
    assert_eq!(report.methods("DELETE"), 1, "aborted its own upload");
    assert!(report.published.is_none());
    assert!(report.parts.is_empty());
}

#[tokio::test]
async fn accepted_put_with_lost_response_reconciles_without_another_write() {
    let bytes = b"retained metadata bytes".to_vec();
    let expected = object(&bytes);
    let fixture = WriteFixture::new(Behavior::LosePutResponse);
    let objects = fixture.objects();
    let budget = Budget::new(Limits::default()).unwrap();
    let error = tokio::time::timeout(
        DEADLINE,
        objects.create_bytes(&expected, bytes.clone(), &budget),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.code(), "remote_unconfirmed");
    assert_eq!(
        tokio::time::timeout(DEADLINE, objects.verify(&expected, &budget))
            .await
            .unwrap()
            .unwrap(),
        expected
    );
    let report = fixture.finish();
    assert_eq!(
        report.requests,
        vec![
            ("PUT".into(), None),
            ("HEAD".into(), None),
            (
                "GET".into(),
                Some(format!("bytes=0-{}", expected.bytes - 1))
            ),
            ("HEAD".into(), None),
        ]
    );
    assert_eq!(report.published.as_deref(), Some(bytes.as_slice()));
}

#[tokio::test]
async fn accepted_complete_with_lost_response_reconciles_without_reupload() {
    let bytes = vec![29; 2 * CHUNK + 31];
    let expected = object(&bytes);
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), &bytes).unwrap();
    let fixture = WriteFixture::new(Behavior::LoseCompleteResponse);
    let objects = fixture.objects();
    let budget = Budget::new(Limits::default()).unwrap();
    let error = tokio::time::timeout(
        DEADLINE,
        objects.upload_file(&expected, source.path().to_owned(), &budget),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.code(), "remote_unconfirmed");
    assert_eq!(
        tokio::time::timeout(DEADLINE, objects.verify(&expected, &budget))
            .await
            .unwrap()
            .unwrap(),
        expected
    );
    let report = fixture.finish();
    assert_eq!(
        report.methods("POST"),
        2,
        "one initiation and one completion"
    );
    assert_eq!(report.methods("PUT"), 3, "each original part sent once");
    assert_eq!(report.methods("DELETE"), 0, "unknown close did not abort");
    assert_eq!(report.methods("GET"), 1);
    assert_eq!(report.methods("HEAD"), 2);
    assert_eq!(report.published.as_deref(), Some(bytes.as_slice()));
    assert!(report.parts.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_http_caller_keeps_owned_capacity_until_worker_finishes() {
    let bytes = vec![17; 512];
    let expected = object(&bytes);
    let mut fixture = WriteFixture::new(Behavior::HoldPutResponse);
    let objects = fixture.objects();
    // The vector and payload allowance occupy the whole 1,024-byte pool.
    let budget = Budget::new(Limits {
        compute_threads: 1,
        memory_bytes: 2048,
        ..Limits::default()
    })
    .unwrap();
    let worker_budget = budget.clone();
    let worker_expected = expected.clone();
    let worker_bytes = bytes.clone();
    let caller = tokio::spawn(async move {
        objects
            .create_bytes(&worker_expected, worker_bytes, &worker_budget)
            .await
    });
    // The fixture signals only after receiving the actual complete PUT body.
    tokio::time::timeout(DEADLINE, fixture.started.take().unwrap())
        .await
        .unwrap()
        .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert_eq!(budget.reserve(1).unwrap_err().code(), "capacity");
    fixture.release.take().unwrap().send(()).unwrap();
    // The detached worker has no caller JoinHandle. Bounded admission acquisition
    // observes its completion while yield points keep this runtime driving HTTP.
    let reclaimed = tokio::time::timeout(DEADLINE, async {
        loop {
            if let Ok(permit) = budget.reserve(1024) {
                break permit;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(reclaimed);
    let report = fixture.finish();
    assert_eq!(report.requests, vec![("PUT".into(), None)]);
    assert_eq!(report.published.as_deref(), Some(bytes.as_slice()));
}

#[derive(Clone, Copy)]
enum Behavior {
    AbortMultipart,
    LosePutResponse,
    LoseCompleteResponse,
    HoldPutResponse,
}

#[derive(Default)]
struct WriteReport {
    // Retain only method/range from requests, never signing or other headers.
    requests: Vec<(String, Option<String>)>,
    published: Option<Vec<u8>>,
    parts: BTreeMap<u64, Vec<u8>>,
}

impl WriteReport {
    fn methods(&self, method: &str) -> usize {
        self.requests
            .iter()
            .filter(|(observed, _)| observed == method)
            .count()
    }
}

// This fixture implements only the native writer's S3 interactions under test.
// Blocking accept wakes through a loopback connection on finish; signal waits and
// socket I/O have deadlines, and Drop releases a held response on test failure.
struct WriteFixture {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    server: Option<thread::JoinHandle<WriteReport>>,
    started: Option<tokio::sync::oneshot::Receiver<()>>,
    release: Option<mpsc::Sender<()>>,
}

impl WriteFixture {
    fn new(behavior: Behavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let server_stop = stop.clone();
        let (started, observed) = tokio::sync::oneshot::channel();
        let (release, wait_release) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut report = WriteReport::default();
            let mut started = Some(started);
            loop {
                let (mut socket, _) = listener.accept().unwrap();
                if server_stop.load(Ordering::Acquire) {
                    break;
                }
                socket.set_nonblocking(false).unwrap();
                socket.set_read_timeout(Some(DEADLINE)).unwrap();
                socket.set_write_timeout(Some(DEADLINE)).unwrap();
                let request = Request::read(&mut socket);
                report
                    .requests
                    .push((request.method.clone(), request.range.clone()));
                let (path, query) = request
                    .target
                    .split_once('?')
                    .unwrap_or((&request.target, ""));
                assert_eq!(path, "/bucket/owned/file.bin");
                let upload = query_value(query, "uploadId");
                match request.method.as_str() {
                    "POST"
                        if query
                            .split('&')
                            .any(|item| item == "uploads" || item == "uploads=") =>
                    {
                        assert!(matches!(
                            behavior,
                            Behavior::AbortMultipart | Behavior::LoseCompleteResponse
                        ));
                        let xml = format!(
                            "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>owned/file.bin</Key><UploadId>{UPLOAD_ID}</UploadId></InitiateMultipartUploadResult>"
                        );
                        respond(&mut socket, "200 OK", "", xml.as_bytes());
                    }
                    "PUT" if upload.is_some() => {
                        assert_eq!(upload, Some(UPLOAD_ID));
                        let number = query_value(query, "partNumber")
                            .unwrap()
                            .parse::<u64>()
                            .unwrap();
                        assert!(
                            report.parts.insert(number, request.body).is_none(),
                            "no duplicate part upload"
                        );
                        respond(&mut socket, "200 OK", "ETag: \"fixture-part\"\r\n", b"");
                    }
                    "DELETE" => {
                        assert!(matches!(behavior, Behavior::AbortMultipart));
                        assert_eq!(
                            upload,
                            Some(UPLOAD_ID),
                            "only this unfinished upload can abort"
                        );
                        report.parts.clear();
                        respond(&mut socket, "204 No Content", "", b"");
                    }
                    "POST" => {
                        assert!(matches!(behavior, Behavior::LoseCompleteResponse));
                        assert_eq!(upload, Some(UPLOAD_ID));
                        assert!(request.create_only, "multipart completion is conditional");
                        assert!(report.published.is_none(), "no second completion");
                        report.published = Some(
                            std::mem::take(&mut report.parts)
                                .into_values()
                                .flatten()
                                .collect(),
                        );
                        // Commit in the fixture, then close without any HTTP response.
                    }
                    "PUT" => {
                        assert!(matches!(
                            behavior,
                            Behavior::LosePutResponse | Behavior::HoldPutResponse
                        ));
                        assert!(request.create_only, "single PUT is conditional");
                        assert!(report.published.is_none(), "no automatic retransmission");
                        report.published = Some(request.body);
                        if matches!(behavior, Behavior::HoldPutResponse) {
                            let _ = started.take().unwrap().send(());
                            wait_release.recv_timeout(DEADLINE).unwrap();
                            respond(&mut socket, "200 OK", "ETag: \"fixture-object\"\r\n", b"");
                        }
                        // LosePutResponse closes after storing bytes without a response.
                    }
                    "HEAD" => {
                        let size = report.published.as_ref().unwrap().len();
                        write!(
                            socket,
                            "HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nConnection: close\r\n\r\n"
                        )
                        .unwrap();
                    }
                    "GET" => {
                        let bytes = report.published.as_ref().unwrap();
                        let headers = format!(
                            "Content-Range: bytes 0-{}/{}\r\n",
                            bytes.len() - 1,
                            bytes.len()
                        );
                        respond(&mut socket, "206 Partial Content", &headers, bytes);
                    }
                    _ => panic!("unexpected fixture method"),
                }
            }
            report
        });
        Self {
            address,
            stop,
            server: Some(server),
            started: Some(observed),
            release: Some(release),
        }
    }

    fn objects(&self) -> RemoteObjects {
        let mut config = properties();
        config.insert("s3.endpoint".into(), format!("http://{}", self.address));
        RemoteObjects::from_resolved(&config, "s3://bucket/owned/").unwrap()
    }

    fn shutdown(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect_timeout(&self.address, DEADLINE);
    }

    fn finish(mut self) -> WriteReport {
        self.shutdown();
        self.server.take().unwrap().join().unwrap()
    }
}

impl Drop for WriteFixture {
    fn drop(&mut self) {
        self.shutdown();
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}

// Request parsing is bounded and transient. Signed headers are discarded; only
// the boolean create-only property and explicit range survive header parsing.
struct Request {
    method: String,
    target: String,
    range: Option<String>,
    create_only: bool,
    body: Vec<u8>,
}

impl Request {
    fn read(socket: &mut TcpStream) -> Self {
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            assert_eq!(socket.read(&mut byte).unwrap(), 1);
            header.push(byte[0]);
            assert!(header.len() < 16 << 10);
        }
        let header = String::from_utf8(header).unwrap();
        let mut line = header.lines().next().unwrap().split_whitespace();
        let method = line.next().unwrap().to_owned();
        let target = line.next().unwrap().to_owned();
        let value = |name: &str| {
            header.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case(name).then(|| value.trim())
            })
        };
        let range = value("range").map(str::to_owned);
        let create_only = value("if-none-match") == Some("*");
        let length = value("content-length")
            .map(|value| value.parse::<usize>().unwrap())
            .unwrap_or(0);
        assert!(length <= 3 * CHUNK);
        let mut body = vec![0; length];
        socket.read_exact(&mut body).unwrap();
        Self {
            method,
            target,
            range,
            create_only,
            body,
        }
    }
}

fn query_value<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|item| {
        let (name, value) = item.split_once('=')?;
        (name == key).then_some(value)
    })
}

fn respond(socket: &mut TcpStream, status: &str, headers: &str, body: &[u8]) {
    write!(
        socket,
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n",
        body.len()
    )
    .unwrap();
    socket.write_all(body).unwrap();
}

// Rust guideline compliant 2026-02-21

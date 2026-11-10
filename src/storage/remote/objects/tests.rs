//! Scope and exact identity checks without provider requests.

use super::*;
use crate::{budget::Limits, model::Id};
use std::{
    io::Write,
    net::TcpListener,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

mod flat;
mod job_reads;
pub(crate) mod pinning;
pub(crate) mod writes;

fn properties() -> HashMap<String, String> {
    HashMap::from([
        ("s3.endpoint".into(), "https://s3.invalid".into()),
        ("s3.region".into(), "auto".into()),
        (
            "s3.access-key-id".into(),
            "dummy-secret-must-not-appear".into(),
        ),
        (
            "s3.secret-access-key".into(),
            "dummy-secret-must-not-appear".into(),
        ),
        ("s3.path-style-access".into(), "true".into()),
    ])
}

fn object(bytes: &[u8]) -> ObjectRef {
    ObjectRef {
        owner: Id::new("test-owner").unwrap(),
        path: "s3://bucket/owned/file.bin".into(),
        digest: blake3::hash(bytes).to_hex().to_string(),
        bytes: bytes.len() as u64,
    }
}

#[test]
fn scope_rejects_aliases_and_cross_prefix_destinations() {
    let scope = Scope::new("s3://bucket/owned/").unwrap();
    assert_eq!(
        scope.key("s3://bucket/owned/nested/file.bin").unwrap(),
        "owned/nested/file.bin"
    );
    for uri in [
        "s3://other/owned/file.bin",
        "s3://bucket/owned-other/file.bin",
        "s3://bucket/owned/../file.bin",
        "s3://bucket/owned/./file.bin",
        "s3://bucket/owned//file.bin",
        "s3://bucket/owned/%2Ffile.bin",
        "s3://bucket/owned/%2e%2e/file.bin",
        "s3://bucket/owned/file?x=y",
        "s3://bucket/owned/file#fragment",
        "s3://bucket/owned/file\\suffix",
        "s3://bucket/owned/",
        "s3://bucket/owned",
        "s3://bucket@other/owned/file",
    ] {
        assert_eq!(scope.key(uri).unwrap_err().code(), "remote_scope", "{uri}");
    }
    for uri in [
        "s3://bucket/",
        "s3://bucket",
        "s3://bucket/owned//",
        "s3://bucket/owned/../",
    ] {
        assert!(Scope::new(uri).is_err(), "{uri}");
    }
}

#[test]
fn configuration_errors_and_debug_never_disclose_secret_values() {
    let objects = RemoteObjects::from_resolved(&properties(), "s3://bucket/owned/").unwrap();
    assert!(!format!("{objects:?}").contains("dummy-secret"));
    for key in ["s3.region", "s3.secret-access-key"] {
        let mut config = properties();
        config.remove(key);
        let error = RemoteObjects::from_resolved(&config, "s3://bucket/owned/").unwrap_err();
        assert_eq!(error.code(), "remote_config");
        assert!(!format!("{error:?}").contains("dummy-secret"));
    }
    let mut config = properties();
    config.insert("s3.sse.key".into(), "dummy-secret-must-not-appear".into());
    assert_eq!(
        RemoteObjects::from_resolved(&config, "s3://bucket/owned/")
            .unwrap_err()
            .code(),
        "remote_config"
    );
}

#[tokio::test]
async fn incorrect_or_oversized_metadata_refuses_before_provider_io() {
    let objects = RemoteObjects::from_resolved(&properties(), "s3://bucket/owned/").unwrap();
    let budget = Budget::new(Limits {
        max_metadata_bytes: 3,
        ..Limits::default()
    })
    .unwrap();
    assert_eq!(
        objects
            .create_bytes(&object(b"right"), b"right".to_vec(), &budget)
            .await
            .unwrap_err()
            .code(),
        "capacity"
    );
    let expected = object(b"abc");
    assert_eq!(
        objects
            .create_bytes(&expected, b"bad".to_vec(), &budget)
            .await
            .unwrap_err()
            .code(),
        "object_changed"
    );
}

#[tokio::test]
async fn metadata_spare_capacity_is_admitted_before_provider_io() {
    let objects = RemoteObjects::from_resolved(&properties(), "s3://bucket/owned/").unwrap();
    let budget = Budget::new(Limits {
        memory_bytes: 128 << 10,
        ..Limits::default()
    })
    .unwrap();
    // Length fits easily, but the owned allocation exceeds the admission pool.
    let mut bytes = Vec::with_capacity(128 << 10);
    bytes.extend_from_slice(b"abc");
    assert_eq!(
        objects
            .create_bytes(&object(b"abc"), bytes, &budget)
            .await
            .unwrap_err()
            .code(),
        "capacity"
    );
    // A compact allocation reaches identity checking instead of capacity refusal.
    assert_eq!(
        objects
            .create_bytes(&object(b"abc"), b"bad".to_vec(), &budget)
            .await
            .unwrap_err()
            .code(),
        "object_changed"
    );
}

#[tokio::test]
async fn changed_source_identity_refuses_before_publication() {
    let objects = RemoteObjects::from_resolved(&properties(), "s3://bucket/owned/").unwrap();
    let source = tempfile::NamedTempFile::new().unwrap();
    let budget = Budget::new(Limits::default()).unwrap();
    for bytes in [b"different length".as_slice(), b"bad".as_slice()] {
        std::fs::write(source.path(), bytes).unwrap();
        // A small upload stays in the maintained writer until close. Both a
        // wrong length and a same-length wrong digest must refuse before then.
        assert_eq!(
            objects
                .upload_file(&object(b"abc"), source.path().to_owned(), &budget)
                .await
                .unwrap_err()
                .code(),
            "object_changed"
        );
    }
}

#[tokio::test]
async fn streamed_readback_uses_one_get_and_preserves_retained_identity() {
    let bytes = vec![17; (1 << 20) + 31];
    let expected = object(&bytes);
    let fixture = ReadFixture::new(bytes, [expected.bytes; 2]);
    let objects = fixture.objects();
    let budget = Budget::new(Limits::default()).unwrap();
    assert_eq!(objects.verify(&expected, &budget).await.unwrap(), expected);
    assert_eq!(
        fixture.finish(),
        vec![
            ("HEAD".into(), None),
            (
                "GET".into(),
                Some(format!("bytes=0-{}", expected.bytes - 1))
            ),
            ("HEAD".into(), None),
        ]
    );
}

#[tokio::test]
async fn streamed_readback_refuses_tail_corruption_short_stream_and_size_change() {
    let bytes = vec![17; (1 << 20) + 31];
    let expected = object(&bytes);
    let budget = Budget::new(Limits::default()).unwrap();
    let mut changed = bytes.clone();
    changed[(1 << 20) + 7] ^= 1;
    for (payload, sizes) in [
        (changed, [expected.bytes; 2]),
        (bytes[..7].to_vec(), [expected.bytes; 2]),
        (bytes.clone(), [expected.bytes, expected.bytes + 1]),
    ] {
        let fixture = ReadFixture::new(payload, sizes);
        let error = fixture
            .objects()
            .verify(&expected, &budget)
            .await
            .unwrap_err();
        assert!(matches!(error.code(), "object_changed" | "remote_read"));
        let requests = fixture.finish();
        assert_eq!(
            requests
                .iter()
                .filter(|(method, _)| method == "GET")
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn empty_readback_uses_only_stats_and_scope_refusal_performs_no_io() {
    let empty = object(b"");
    let fixture = ReadFixture::new(Vec::new(), [0; 2]);
    let objects = fixture.objects();
    let budget = Budget::new(Limits::default()).unwrap();
    assert_eq!(objects.verify(&empty, &budget).await.unwrap(), empty);
    assert_eq!(
        fixture.finish(),
        vec![("HEAD".into(), None), ("HEAD".into(), None)]
    );
    let fixture = ReadFixture::new(Vec::new(), [0; 2]);
    let mut outside = empty;
    outside.path = "s3://bucket/elsewhere/file.bin".into();
    assert_eq!(
        fixture
            .objects()
            .verify(&outside, &budget)
            .await
            .unwrap_err()
            .code(),
        "remote_scope"
    );
    assert!(fixture.finish().is_empty());
}

type FixtureInventory = Arc<std::sync::Mutex<HashMap<String, Vec<u8>>>>;

// A tiny read-only S3 HTTP boundary, using the actual configured native operator.
// Records only method/range, never credential-bearing request headers.
pub(crate) struct ReadFixture {
    endpoint: String,
    stop: Arc<AtomicBool>,
    server: thread::JoinHandle<Vec<(String, Option<String>)>>,
    paths: Arc<std::sync::Mutex<Vec<(String, String)>>>,
    contents: Option<FixtureInventory>,
    session_token_observed: Arc<AtomicBool>,
}

#[derive(Clone)]
enum RangeReply {
    Normal,
    IgnoreRange,
    Fragmented,
    Redirect(String),
    HeldError,
}

impl ReadFixture {
    pub(crate) fn new(payload: Vec<u8>, sizes: [u64; 2]) -> Self {
        Self::serve(payload, sizes, None, None, RangeReply::Normal)
    }

    pub(crate) fn inventory(contents: HashMap<String, Vec<u8>>) -> Self {
        Self::serve(Vec::new(), [0; 2], Some(contents), None, RangeReply::Normal)
    }

    pub(crate) fn held(
        payload: Vec<u8>,
    ) -> (
        Self,
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let size = payload.len() as u64;
        let (reached, received) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        (
            Self::serve(
                payload,
                [size; 2],
                None,
                Some((reached, held)),
                RangeReply::Normal,
            ),
            received,
            release,
        )
    }

    // Held flat readers need the same exact URI/range responses as inventory.
    pub(crate) fn held_inventory(
        contents: HashMap<String, Vec<u8>>,
    ) -> (
        Self,
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (reached, received) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        (
            Self::serve(
                Vec::new(),
                [0; 2],
                Some(contents),
                Some((reached, held)),
                RangeReply::Normal,
            ),
            received,
            release,
        )
    }

    fn serve(
        payload: Vec<u8>,
        sizes: [u64; 2],
        contents: Option<HashMap<String, Vec<u8>>>,
        mut hold: Option<(
            tokio::sync::oneshot::Sender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
        reply: RangeReply,
    ) -> Self {
        let contents = contents.map(|value| Arc::new(std::sync::Mutex::new(value)));
        let inventory = contents.clone();
        let paths = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed_paths = paths.clone();
        let session_token_observed = Arc::new(AtomicBool::new(false));
        let observed_token = session_token_observed.clone();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let server_stop = stop.clone();
        let server = thread::spawn(move || {
            let mut requests = Vec::new();
            let mut heads = 0;
            while !server_stop.load(Ordering::Acquire) {
                let (mut socket, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(error) => panic!("fixture accept failed: {error}"),
                };
                // Accepted sockets inherit nonblocking mode on macOS.
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    assert_eq!(socket.read(&mut byte).unwrap(), 1);
                    request.push(byte[0]);
                    assert!(request.len() < 16 << 10);
                }
                let request = String::from_utf8(request).unwrap();
                if request.lines().any(|line| {
                    line.split_once(':')
                        .is_some_and(|(name, _)| name.eq_ignore_ascii_case("x-amz-security-token"))
                }) {
                    observed_token.store(true, Ordering::Release);
                }
                let method = request.split_whitespace().next().unwrap().to_owned();
                let path = request.split_whitespace().nth(1).unwrap();
                let uri = format!("s3://{}", path.strip_prefix('/').unwrap());
                observed_paths
                    .lock()
                    .unwrap()
                    .push((method.clone(), uri.clone()));
                let range = request.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("range")
                        .then(|| value.trim().to_owned())
                });
                requests.push((method.clone(), range.clone()));
                if let RangeReply::Redirect(location) = &reply {
                    write!(
                        socket,
                        "HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .unwrap();
                    continue;
                }
                if matches!(reply, RangeReply::HeldError) {
                    write!(
                        socket,
                        "HTTP/1.1 403 Forbidden\r\nContent-Length: 33554432\r\nConnection: close\r\n\r\n"
                    )
                    .unwrap();
                    let (reached, held) = hold.take().unwrap();
                    reached.send(()).unwrap();
                    held.recv_timeout(Duration::from_secs(5)).unwrap();
                    continue;
                }
                let contents = inventory.as_ref().map(|objects| objects.lock().unwrap());
                let bytes = if let Some(contents) = &contents {
                    let Some(bytes) = contents.get(&uri) else {
                        write!(socket, "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                        continue;
                    };
                    bytes
                } else {
                    &payload
                };
                let reported_size = if contents.is_some() {
                    bytes.len() as u64
                } else {
                    sizes[heads.min(1)]
                };
                if method == "HEAD" {
                    let size = reported_size;
                    heads += 1;
                    write!(
                        socket,
                        "HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nETag: \"fixture\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n"
                    )
                    .unwrap();
                } else {
                    assert_eq!(method, "GET");
                    if let Some((reached, held)) = hold.take() {
                        reached.send(()).unwrap();
                        held.recv_timeout(Duration::from_secs(5)).unwrap();
                    }
                    if contents.is_some() {
                        if range.is_some() && matches!(reply, RangeReply::IgnoreRange) {
                            write!(socket, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"fixture\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n", bytes.len()).unwrap();
                            let _ = socket.write_all(bytes);
                            continue;
                        }
                        let (start, end) = match range.as_deref() {
                            None => (0, bytes.len()),
                            Some(value) => {
                                let (start, end) = value
                                    .strip_prefix("bytes=")
                                    .unwrap()
                                    .split_once('-')
                                    .unwrap();
                                if start.is_empty() {
                                    (
                                        bytes.len().saturating_sub(end.parse::<usize>().unwrap()),
                                        bytes.len(),
                                    )
                                } else {
                                    (
                                        start.parse::<usize>().unwrap(),
                                        if end.is_empty() {
                                            bytes.len()
                                        } else {
                                            (end.parse::<usize>().unwrap() + 1).min(bytes.len())
                                        },
                                    )
                                }
                            }
                        };
                        let selected = &bytes[start..end];
                        let size = selected.len();
                        if range.is_some() {
                            write!(socket, "HTTP/1.1 206 Partial Content\r\nContent-Length: {size}\r\nContent-Range: bytes {start}-{}/{}\r\nETag: \"fixture\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n", end.saturating_sub(1), bytes.len()).unwrap();
                        } else {
                            write!(socket, "HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nETag: \"fixture\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n").unwrap();
                        }
                        if matches!(reply, RangeReply::Fragmented) {
                            let split = selected.len() / 2;
                            socket.write_all(&selected[..split]).unwrap();
                            thread::sleep(Duration::from_millis(20));
                            socket.write_all(&selected[split..]).unwrap();
                        } else {
                            socket.write_all(selected).unwrap();
                        }
                        continue;
                    }
                    let size = bytes.len();
                    let range_size = if contents.is_some() {
                        size as u64
                    } else {
                        sizes[0]
                    };
                    if range.is_some() {
                        write!(socket, "HTTP/1.1 206 Partial Content\r\nContent-Length: {size}\r\nContent-Range: bytes 0-{}/{}\r\nETag: \"fixture\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n", size.saturating_sub(1), range_size).unwrap();
                    } else {
                        write!(socket, "HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nETag: \"fixture\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n").unwrap();
                    }
                    socket.write_all(bytes).unwrap();
                }
            }
            requests
        });
        Self {
            endpoint,
            stop,
            server,
            paths,
            contents,
            session_token_observed,
        }
    }

    pub(crate) fn observed_paths(&self) -> Vec<(String, String)> {
        self.paths.lock().unwrap().clone()
    }

    pub(crate) fn objects(&self) -> RemoteObjects {
        let mut config = properties();
        config.insert("s3.endpoint".into(), self.endpoint.clone());
        RemoteObjects::from_resolved(&config, "s3://bucket/owned/").unwrap()
    }

    pub(crate) fn snapshot_source(&self, token: Option<&str>) -> SnapshotSource {
        let mut config = properties();
        config.insert("s3.endpoint".into(), self.endpoint.clone());
        if let Some(token) = token {
            config.insert("s3.session-token".into(), token.into());
        }
        SnapshotSource::from_resolved(&config, "s3://bucket/owned/").unwrap()
    }

    pub(crate) fn insert(&self, uri: String, bytes: Vec<u8>) {
        self.contents
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .insert(uri, bytes);
    }

    pub(crate) fn session_token_observed(&self) -> bool {
        self.session_token_observed.load(Ordering::Acquire)
    }

    pub(crate) fn finish(self) -> Vec<(String, Option<String>)> {
        self.stop.store(true, Ordering::Release);
        self.server.join().unwrap()
    }
}

#[tokio::test]
async fn snapshot_source_refuses_ignored_ranges_and_fills_fragments() {
    const URI: &str = "s3://bucket/owned/source.bin";
    let bytes = b"0123456789abcdef";
    let budget = Budget::new(Limits::default()).unwrap();
    let _destination = budget.reserve(5).unwrap();
    for reply in [RangeReply::IgnoreRange, RangeReply::Fragmented] {
        let fixture = ReadFixture::serve(
            Vec::new(),
            [0; 2],
            Some(HashMap::from([(URI.into(), bytes.to_vec())])),
            None,
            reply.clone(),
        );
        let source = fixture.snapshot_source(None);
        let mut output = [b'-'; 5];
        let result = source.read_range(URI, 16, 3..8, &mut output, &budget).await;
        match reply {
            RangeReply::IgnoreRange => {
                assert!(result.is_err());
                assert_eq!(&output, b"-----");
            }
            RangeReply::Fragmented => {
                result.unwrap();
                assert_eq!(&output, b"34567");
            }
            _ => unreachable!(),
        }
        assert_eq!(
            fixture.finish(),
            vec![("GET".into(), Some("bytes=3-7".into()))]
        );
    }
}

#[tokio::test]
async fn snapshot_source_refuses_redirects_without_opening_another_placement() {
    const URI: &str = "s3://bucket/owned/source.bin";
    let sink = ReadFixture::new(b"0123456789abcdef".to_vec(), [16; 2]);
    let redirected = ReadFixture::serve(
        Vec::new(),
        [0; 2],
        None,
        None,
        RangeReply::Redirect(format!("{}/bucket/owned/source.bin", sink.endpoint)),
    );
    let source = redirected.snapshot_source(None);
    let budget = Budget::new(Limits::default()).unwrap();
    let _destination = budget.reserve(5).unwrap();
    let mut output = [b'-'; 5];
    assert_eq!(
        source.size(URI, &budget).await.unwrap_err().code(),
        "remote_read"
    );
    assert_eq!(
        source
            .read_range(URI, 16, 3..8, &mut output, &budget)
            .await
            .unwrap_err()
            .code(),
        "remote_read"
    );
    assert_eq!(&output, b"-----");
    assert_eq!(
        redirected.finish(),
        vec![
            ("HEAD".into(), None),
            ("GET".into(), Some("bytes=3-7".into()))
        ]
    );
    assert!(sink.finish().is_empty());
}

#[tokio::test]
async fn snapshot_source_refuses_error_without_reading_its_body() {
    const URI: &str = "s3://bucket/owned/source.bin";
    let (reached, headers_sent) = tokio::sync::oneshot::channel();
    let (release, held) = std::sync::mpsc::channel();
    let fixture = ReadFixture::serve(
        Vec::new(),
        [0; 2],
        None,
        Some((reached, held)),
        RangeReply::HeldError,
    );
    let source = fixture.snapshot_source(None);
    let budget = Budget::new(Limits::default()).unwrap();
    let read = tokio::spawn(async move {
        let _destination = budget.reserve(5).unwrap();
        let mut output = [b'-'; 5];
        let error = source
            .read_range(URI, 16, 3..8, &mut output, &budget)
            .await
            .unwrap_err();
        (error, output)
    });
    tokio::time::timeout(Duration::from_secs(5), headers_sent)
        .await
        .unwrap()
        .unwrap();
    let refused = tokio::time::timeout(Duration::from_secs(2), read).await;
    release.send(()).unwrap();
    let (error, output) = refused.unwrap().unwrap();
    assert_eq!(error.code(), "remote_read");
    assert_eq!(&output, b"-----");
    assert_eq!(
        fixture.finish(),
        vec![("GET".into(), Some("bytes=3-7".into()))]
    );
}

#[tokio::test]
async fn snapshot_source_holds_transport_admission_through_active_read() {
    const URI: &str = "s3://bucket/owned/source.bin";
    let (fixture, reached, release) =
        ReadFixture::held_inventory(HashMap::from([(URI.into(), b"0123456789abcdef".to_vec())]));
    let source = fixture.snapshot_source(None);
    let budget = Budget::new(Limits::default()).unwrap();
    let available = || budget.reserve(0).unwrap().semaphore().available_permits();
    let initial = available();
    let read = tokio::spawn({
        let budget = budget.clone();
        async move {
            let _destination = budget.reserve(5).unwrap();
            let mut bytes = [0; 5];
            source
                .read_range(URI, 16, 3..8, &mut bytes, &budget)
                .await
                .unwrap();
            bytes
        }
    });
    tokio::time::timeout(Duration::from_secs(5), reached)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(available(), initial - READ_ADMISSION - 5);
    release.send(()).unwrap();
    assert_eq!(read.await.unwrap(), *b"34567");
    assert_eq!(available(), initial);
    assert_eq!(
        fixture.finish(),
        vec![("GET".into(), Some("bytes=3-7".into()))]
    );
}

#[test]
fn snapshot_source_does_not_register_ambient_scope() {
    let prefix = "s3://bucket/snapshot-source-only";
    let source = SnapshotSource::from_resolved(&properties(), prefix).unwrap();
    assert_eq!(source.prefix(), prefix);
    assert!(RemoteObjects::for_uri("s3://bucket/snapshot-source-only/member").is_none());
}

#[test]
fn conditional_conflict_and_uncertain_outcome_have_distinct_refusals() {
    for kind in [
        opendal::ErrorKind::AlreadyExists,
        opendal::ErrorKind::ConditionNotMatch,
    ] {
        assert_eq!(
            write_error(opendal::Error::new(kind, "dummy-secret")).code(),
            "remote_conflict"
        );
    }
    let error = write_error(opendal::Error::new(
        opendal::ErrorKind::Unexpected,
        "dummy-secret",
    ));
    assert_eq!(error.code(), "remote_unconfirmed");
    assert!(!format!("{error:?}").contains("dummy-secret"));
}

// Rust guideline compliant 2026-02-21

#[tokio::test]
async fn metadata_reader_hashes_one_get_and_availability_reads_only_metadata() {
    let payload = br#"{"value":42}"#.to_vec();
    let expected = object(&payload);
    let fixture = ReadFixture::new(payload.clone(), [expected.bytes; 2]);
    let objects = crate::storage::Objects::Remote(fixture.objects());
    let budget = Budget::new(Limits::default()).unwrap();
    let worker_objects = objects.clone();
    let worker_expected = expected.clone();
    let memory = budget
        .reserve(payload.len() + objects.metadata_read_allowance())
        .unwrap();
    let handle = tokio::runtime::Handle::current();
    let value = budget
        .blocking(move || {
            let _memory = memory;
            let mut bytes = vec![0; worker_expected.bytes as usize];
            worker_objects.read_metadata_admitted(&worker_expected, &mut bytes, &handle)?;
            worker_objects.check_available_admitted(&worker_expected, &handle)?;
            Ok(serde_json::from_slice::<serde_json::Value>(&bytes)?)
        })
        .await
        .unwrap();
    assert_eq!(value["value"], 42);
    let requests = fixture.finish();
    assert_eq!(
        requests
            .iter()
            .filter(|(method, _)| method == "GET")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|(method, _)| method == "HEAD")
            .count(),
        1
    );
}

#[tokio::test]
async fn metadata_reader_refuses_changed_bytes_lengths_and_scope() {
    let payload = br#"{"value":42}"#.to_vec();
    let expected = object(&payload);
    let mut changed = payload.clone();
    changed[9] ^= 1;
    for (payload, sizes) in [
        (changed, [expected.bytes; 2]),
        (payload[..3].to_vec(), [expected.bytes; 2]),
        // The GET must expose a changed complete size; no later HEAD is used
        // to observe a mutation occurring after this immutable response.
        (payload.clone(), [expected.bytes + 1; 2]),
    ] {
        let fixture = ReadFixture::new(payload, sizes);
        let objects = crate::storage::Objects::Remote(fixture.objects());
        let expected = expected.clone();
        let budget = Budget::new(Limits::default()).unwrap();
        let memory = budget
            .reserve(expected.bytes as usize + objects.metadata_read_allowance())
            .unwrap();
        let handle = tokio::runtime::Handle::current();
        let error = budget
            .blocking(move || {
                let _memory = memory;
                let mut bytes = vec![0; expected.bytes as usize];
                objects.read_metadata_admitted(&expected, &mut bytes, &handle)
            })
            .await
            .unwrap_err();
        assert!(matches!(
            error.code(),
            "corrupt_object" | "object_changed" | "remote_read"
        ));
        let requests = fixture.finish();
        assert_eq!(
            requests
                .iter()
                .filter(|(method, _)| method == "GET")
                .count(),
            1
        );
    }
    let fixture = ReadFixture::new(Vec::new(), [0; 2]);
    let objects = fixture.objects();
    let mut escaped = expected;
    escaped.path = "s3://bucket/outside/file.bin".into();
    let mut bytes = vec![0; escaped.bytes as usize];
    assert_eq!(
        objects
            .read_metadata_admitted(&escaped, &mut bytes)
            .await
            .unwrap_err()
            .code(),
        "remote_scope"
    );
    assert!(fixture.finish().is_empty());
}

#[tokio::test]
async fn empty_metadata_get_and_changed_availability_read_no_payload_bytes() {
    let expected = object(b"");
    let fixture = ReadFixture::new(Vec::new(), [0; 2]);
    let objects = crate::storage::Objects::Remote(fixture.objects());
    let budget = Budget::new(Limits::default()).unwrap();
    let memory = budget.reserve(objects.metadata_read_allowance()).unwrap();
    let handle = tokio::runtime::Handle::current();
    budget
        .blocking(move || {
            let _memory = memory;
            objects.read_metadata_admitted(&expected, &mut [], &handle)?;
            let mut changed = expected;
            changed.bytes = 1;
            assert_eq!(
                objects
                    .check_available_admitted(&changed, &handle)
                    .unwrap_err()
                    .code(),
                "unavailable_phase"
            );
            Ok(())
        })
        .await
        .unwrap();
    let requests = fixture.finish();
    assert_eq!(
        requests
            .iter()
            .filter(|(method, _)| method == "HEAD")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|(method, _)| method == "GET")
            .count(),
        1
    );
}

#[test]
fn restore_placement_ignores_credentials_but_refuses_changed_backend_or_scope() {
    let original = RemoteObjects::from_resolved(&properties(), "s3://bucket/owned/").unwrap();
    let mut rotated = properties();
    rotated.insert("s3.access-key-id".into(), "rotated-dummy".into());
    rotated.insert("s3.secret-access-key".into(), "rotated-dummy".into());
    assert!(
        original
            .same_placement(&RemoteObjects::from_resolved(&rotated, "s3://bucket/owned/").unwrap())
    );
    for (key, value) in [
        ("s3.endpoint", "https://different.invalid"),
        ("s3.region", "other"),
        ("s3.path-style-access", "false"),
    ] {
        let mut changed = properties();
        changed.insert(key.into(), value.into());
        assert!(!original.same_placement(
            &RemoteObjects::from_resolved(&changed, "s3://bucket/owned/").unwrap()
        ));
    }
    assert!(!original.same_placement(
        &RemoteObjects::from_resolved(&properties(), "s3://bucket/other/").unwrap()
    ));
}

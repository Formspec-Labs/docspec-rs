// Rust guideline compliant 2026-02-21
//! Persistent JSON-lines stdio catalog owner worker (`docspec owner-worker`).
//!
//! Serves startup-pinned Parquet row-group pagination (`read_page`) and checked
//! in-process DataFusion SQL recipe queries (`run_query`) in `ddk-exact-json-v1`
//! format without external Python or DuckDB processes.

pub mod exact;
pub mod sql_params;

pub use exact::BridgeError;

use crate::budget::{Budget, Limits};
use exact::ExactBudget;
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, Metadata, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

const MAX_LINE_BYTES: usize = 256 * 1024;
const MAX_DIGEST_CACHE: usize = 4096;
const MAX_SQL_CACHE: usize = 256;
const IO_BUF_BYTES: usize = 64 * 1024;
const DEFAULT_VERIFY_DIGEST_MAX_BYTES: u64 = 256 << 20;
const LOCK_MAX_BYTES: u64 = 16 << 20;
const CATALOG_MAX_BYTES: u64 = 64 << 20;
const SQL_COMPANION_MAX_BYTES: u64 = 16 << 20;
const DEFAULT_QUERY_TIMEOUT_SEC: f64 = 15.0;

/// Default owner resource profile matching `docspec-owner-bridge.py:DEFAULT_DOCSPEC_LIMITS`.
pub fn default_owner_limits() -> Limits {
    Limits {
        batch_rows: 8192,
        compute_threads: 2,
        max_batch_bytes: 8 << 20,
        max_metadata_bytes: 2 << 20,
        memory_bytes: 256 << 20,
        scratch_bytes: 64 << 20,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct StatSig {
    dev: u64,
    ino: u64,
    size: u64,
    mtime_ns: i64,
    ctime_ns: i64,
}

fn stat_signature(meta: &Metadata) -> StatSig {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        StatSig {
            dev: meta.dev(),
            ino: meta.ino(),
            size: meta.len(),
            mtime_ns: meta.mtime_nsec(),
            ctime_ns: meta.ctime_nsec(),
        }
    }
    #[cfg(not(unix))]
    {
        let mtime_ns = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .and_then(|d| i64::try_from(d.as_nanos()).ok())
            .unwrap_or(0);
        StatSig {
            dev: 0,
            ino: 0,
            size: meta.len(),
            mtime_ns,
            ctime_ns: mtime_ns,
        }
    }
}

/// Bounded 4096-entry LRU cache mapping `(dev, ino, size, mtime_ns, ctime_ns) -> sha256`.
#[derive(Debug, Default)]
struct DigestCache {
    tick: u64,
    by_sig: BTreeMap<StatSig, (u64, String)>,
    order: BTreeMap<u64, StatSig>,
}

impl DigestCache {
    fn get(&mut self, sig: &StatSig) -> Option<&str> {
        let (_, digest) = self.by_sig.get(sig)?;
        let digest_cloned = digest.clone();
        self.insert(*sig, digest_cloned);
        self.by_sig.get(sig).map(|(_, d)| d.as_str())
    }

    fn insert(&mut self, sig: StatSig, digest: String) {
        self.tick = self.tick.saturating_add(1);
        let now = self.tick;
        if let Some((old_tick, _)) = self.by_sig.insert(sig, (now, digest)) {
            self.order.remove(&old_tick);
        }
        self.order.insert(now, sig);
        while self.by_sig.len() > MAX_DIGEST_CACHE {
            let Some((&oldest_tick, &oldest_sig)) = self.order.iter().next() else {
                break;
            };
            self.order.remove(&oldest_tick);
            self.by_sig.remove(&oldest_sig);
        }
    }
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn sha256_file(path: &Path) -> Result<String, BridgeError> {
    let mut file = File::open(path).map_err(|err| {
        BridgeError::new(
            "evidence_refused",
            format!("Unable to open file for SHA-256 verification: {err}"),
            404,
        )
    })?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; IO_BUF_BYTES];
    loop {
        let n = file.read(&mut buf).map_err(|err| {
            BridgeError::new(
                "owner_unavailable",
                format!("I/O error while hashing {}: {err}", path.display()),
                503,
            )
        })?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn read_bounded_file(
    path: &Path,
    max_bytes: u64,
    missing_code: &'static str,
    missing_status: u16,
    budget_code: &'static str,
) -> Result<(Metadata, Vec<u8>), BridgeError> {
    let file = File::open(path).map_err(|err| {
        BridgeError::new(
            missing_code,
            format!("Unable to open {}: {err}", path.display()),
            missing_status,
        )
    })?;
    let meta = file.metadata().map_err(|err| {
        BridgeError::new(
            missing_code,
            format!("Unable to stat {}: {err}", path.display()),
            missing_status,
        )
    })?;
    if !meta.is_file() {
        return Err(BridgeError::new(
            missing_code,
            format!("Expected regular file at {}", path.display()),
            missing_status,
        ));
    }
    if meta.len() > max_bytes {
        return Err(BridgeError::new(
            budget_code,
            format!(
                "File {} exceeds byte budget ({max_bytes} bytes).",
                path.display()
            ),
            413,
        ));
    }
    let cap = usize::try_from(meta.len()).unwrap_or(0);
    let mut out = Vec::with_capacity(cap);
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut out)
        .map_err(|err| {
            BridgeError::new(
                "owner_unavailable",
                format!("Failed reading {}: {err}", path.display()),
                503,
            )
        })?;
    if (out.len() as u64) > max_bytes {
        return Err(BridgeError::new(
            budget_code,
            format!(
                "File {} grew past byte budget ({max_bytes} bytes).",
                path.display()
            ),
            413,
        ));
    }
    Ok((meta, out))
}

fn is_within_roots(path: &Path, roots: &[PathBuf]) -> bool {
    roots
        .iter()
        .any(|root| path == root || path.starts_with(root))
}

fn resolve_uri_or_path(raw: &str) -> Result<PathBuf, BridgeError> {
    if raw.starts_with("file://") {
        let parsed = url::Url::parse(raw).map_err(|_| {
            BridgeError::new(
                "evidence_refused",
                "Invalid file:// URI in owner request.",
                409,
            )
        })?;
        if parsed.scheme() != "file" {
            return Err(BridgeError::new(
                "evidence_refused",
                "Only local file:// URIs are permitted.",
                409,
            ));
        }
        match parsed.host_str() {
            None | Some("") | Some("localhost") => {}
            Some(_) => {
                return Err(BridgeError::new(
                    "evidence_refused",
                    "Only local file:// URIs are permitted.",
                    409,
                ));
            }
        }
        return parsed.to_file_path().map_err(|()| {
            BridgeError::new(
                "evidence_refused",
                "Only local file:// URIs are permitted.",
                409,
            )
        });
    }
    Ok(PathBuf::from(raw))
}

fn check_permitted_file(raw: &str, roots: &[PathBuf]) -> Result<(PathBuf, Metadata), BridgeError> {
    let candidate = resolve_uri_or_path(raw)?;
    if !candidate.is_absolute() {
        return Err(BridgeError::new(
            "evidence_refused",
            "Owner file paths must be absolute.",
            409,
        ));
    }
    let resolved = std::fs::canonicalize(&candidate).map_err(|err| {
        BridgeError::new(
            "evidence_refused",
            format!("Permitted file is unavailable: {err}"),
            404,
        )
    })?;
    if !is_within_roots(&resolved, roots) {
        return Err(BridgeError::new(
            "evidence_refused",
            "Target file is outside configured permittedRoots.",
            409,
        ));
    }
    let meta = std::fs::metadata(&resolved).map_err(|err| {
        BridgeError::new(
            "evidence_refused",
            format!("Permitted file metadata is unavailable: {err}"),
            404,
        )
    })?;
    if !meta.is_file() {
        return Err(BridgeError::new(
            "evidence_refused",
            "Target path is not a regular file.",
            409,
        ));
    }
    Ok((resolved, meta))
}

struct ScratchDirGuard {
    path: PathBuf,
}

impl Drop for ScratchDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn create_worker_temp_dir() -> Result<PathBuf, std::io::Error> {
    let base = std::env::temp_dir();
    let candidate = base.join(format!(
        "ddk-owner-worker-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&candidate)?;
    std::fs::canonicalize(&candidate)
}

fn merge_docspec_limits(
    base: &Limits,
    overrides: &Map<String, Value>,
) -> Result<Limits, BridgeError> {
    let mut merged = base.clone();
    for (key, val) in overrides {
        let parse_usize = |field: &str| -> Result<usize, BridgeError> {
            let raw = val.as_u64().ok_or_else(|| {
                BridgeError::new(
                    "invalid_registration",
                    format!("docspecLimits.{field} must be a positive integer."),
                    400,
                )
            })?;
            usize::try_from(raw).map_err(|_| {
                BridgeError::new(
                    "invalid_registration",
                    format!("docspecLimits.{field} overflows host usize."),
                    400,
                )
            })
        };
        match key.as_str() {
            "batch_rows" => merged.batch_rows = parse_usize("batch_rows")?,
            "compute_threads" => merged.compute_threads = parse_usize("compute_threads")?,
            "max_batch_bytes" => merged.max_batch_bytes = parse_usize("max_batch_bytes")?,
            "max_metadata_bytes" => merged.max_metadata_bytes = parse_usize("max_metadata_bytes")?,
            "memory_bytes" => merged.memory_bytes = parse_usize("memory_bytes")?,
            "scratch_bytes" => {
                merged.scratch_bytes = val.as_u64().ok_or_else(|| {
                    BridgeError::new(
                        "invalid_registration",
                        "docspecLimits.scratch_bytes must be a non-negative integer.",
                        400,
                    )
                })?;
            }
            other => {
                return Err(BridgeError::new(
                    "invalid_registration",
                    format!("Unknown docspecLimits key: {other:?}."),
                    400,
                ));
            }
        }
    }
    Budget::new(merged.clone()).map_err(|err| {
        BridgeError::new(
            "invalid_registration",
            format!("Invalid docspecLimits configuration: {err}"),
            400,
        )
    })?;
    Ok(merged)
}

fn map_job_error(err: crate::Error) -> BridgeError {
    if matches!(
        err.code(),
        "capacity" | "metadata_capacity" | "disk_capacity" | "request_timeout"
    ) || err.message().contains("Resources exhausted")
    {
        return BridgeError::new("native_read_unknown", err.to_string(), 408);
    }
    BridgeError::new("native_read_refused", err.to_string(), 409)
}

/// Stateful per-process owner worker handling `init`, `read_page`, `run_query`, and `ping`.
pub struct OwnerWorker {
    initialized: bool,
    base_limits: Limits,
    limits: Limits,
    query_timeout: Duration,
    engine_info: Value,
    catalog_path: Option<PathBuf>,
    catalog_dir: Option<PathBuf>,
    catalog_sha256: Option<String>,
    catalog_doc: Option<Value>,
    lock_path: Option<PathBuf>,
    lock_sha256: Option<String>,
    permitted_roots: Vec<PathBuf>,
    retain_receipts_dir: Option<PathBuf>,
    worker_temp_dir: PathBuf,
    digest_cache: DigestCache,
    sql_cache: BTreeMap<StatSig, Arc<str>>,
    next_query_seq: u64,
    next_receipt_seq: u64,
}

impl Drop for OwnerWorker {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.worker_temp_dir);
    }
}

impl OwnerWorker {
    /// Creates an uninitialized worker with optional CLI `--limits` overrides.
    pub fn new(cli_limits: Option<Limits>) -> Result<Self, std::io::Error> {
        let base_limits = cli_limits.unwrap_or_else(default_owner_limits);
        let worker_temp_dir = create_worker_temp_dir()?;
        Ok(Self {
            initialized: false,
            limits: base_limits.clone(),
            base_limits,
            query_timeout: Duration::from_secs_f64(DEFAULT_QUERY_TIMEOUT_SEC),
            engine_info: Value::Null,
            catalog_path: None,
            catalog_dir: None,
            catalog_sha256: None,
            catalog_doc: None,
            lock_path: None,
            lock_sha256: None,
            permitted_roots: Vec::new(),
            retain_receipts_dir: None,
            worker_temp_dir,
            digest_cache: DigestCache::default(),
            sql_cache: BTreeMap::new(),
            next_query_seq: 0,
            next_receipt_seq: 0,
        })
    }

    fn verify_cached_digest(
        &mut self,
        path: &Path,
        meta: &Metadata,
        expected_sha256: &str,
        max_bytes: u64,
        mismatch_message: &'static str,
    ) -> Result<StatSig, BridgeError> {
        let sig = stat_signature(meta);
        if let Some(cached) = self.digest_cache.get(&sig) {
            if cached != expected_sha256 {
                return Err(BridgeError::new(
                    "integrity_mismatch",
                    mismatch_message,
                    409,
                ));
            }
            return Ok(sig);
        }
        if sig.size > max_bytes {
            return Err(BridgeError::new(
                "adapter_budget",
                "Parquet file exceeds verifyDigestMaxBytes budget.",
                413,
            ));
        }
        let actual = sha256_file(path)?;
        self.digest_cache.insert(sig, actual.clone());
        if actual != expected_sha256 {
            return Err(BridgeError::new(
                "integrity_mismatch",
                mismatch_message,
                409,
            ));
        }
        Ok(sig)
    }

    fn load_verified_sql(
        &mut self,
        sql_path: &Path,
        expected_sha256: &str,
    ) -> Result<Arc<str>, BridgeError> {
        let meta = std::fs::metadata(sql_path).map_err(|_| {
            BridgeError::new(
                "integrity_mismatch",
                "Recipe SQL companion file is missing.",
                409,
            )
        })?;
        if !meta.is_file() {
            return Err(BridgeError::new(
                "integrity_mismatch",
                "Recipe SQL companion file is missing.",
                409,
            ));
        }
        let sig = self.verify_cached_digest(
            sql_path,
            &meta,
            expected_sha256,
            SQL_COMPANION_MAX_BYTES,
            "Recipe SQL companion digest mismatch.",
        )?;
        if let Some(cached_sql) = self.sql_cache.get(&sig) {
            return Ok(Arc::clone(cached_sql));
        }
        let (_, bytes) = read_bounded_file(
            sql_path,
            SQL_COMPANION_MAX_BYTES,
            "integrity_mismatch",
            409,
            "adapter_budget",
        )?;
        if sha256_bytes(&bytes) != expected_sha256 {
            return Err(BridgeError::new(
                "integrity_mismatch",
                "Recipe SQL companion digest mismatch.",
                409,
            ));
        }
        let sql_str = String::from_utf8(bytes).map_err(|_| {
            BridgeError::new(
                "integrity_mismatch",
                "Recipe SQL companion file is not valid UTF-8.",
                409,
            )
        })?;
        let arc_sql: Arc<str> = Arc::from(sql_str);
        if self.sql_cache.len() >= MAX_SQL_CACHE {
            self.sql_cache.clear();
        }
        self.sql_cache.insert(sig, Arc::clone(&arc_sql));
        Ok(arc_sql)
    }

    /// Handles `init(params)`.
    pub fn init(&mut self, params: &Map<String, Value>) -> Result<Value, BridgeError> {
        if self.initialized {
            return Err(BridgeError::new(
                "invalid_registration",
                "Owner worker is already initialized.",
                409,
            ));
        }

        let raw_roots = params
            .get("permittedRoots")
            .and_then(Value::as_array)
            .filter(|arr| !arr.is_empty())
            .ok_or_else(|| {
                BridgeError::new(
                    "invalid_registration",
                    "DocSpecCatalogOwner requires at least one permittedRoot.",
                    400,
                )
            })?;

        let mut resolved_roots = Vec::with_capacity(raw_roots.len());
        for item in raw_roots {
            let raw_root = item.as_str().ok_or_else(|| {
                BridgeError::new(
                    "invalid_registration",
                    "Every permittedRoot must be an absolute directory path.",
                    400,
                )
            })?;
            let candidate = Path::new(raw_root);
            if !candidate.is_absolute() {
                return Err(BridgeError::new(
                    "invalid_registration",
                    "Every permittedRoot must be an absolute directory path.",
                    400,
                ));
            }
            let canon = std::fs::canonicalize(candidate).map_err(|_| {
                BridgeError::new(
                    "invalid_registration",
                    format!("Permitted root does not exist: {raw_root}"),
                    400,
                )
            })?;
            let meta = std::fs::metadata(&canon).map_err(|_| {
                BridgeError::new(
                    "invalid_registration",
                    format!("Permitted root does not exist: {raw_root}"),
                    400,
                )
            })?;
            if !meta.is_dir() {
                return Err(BridgeError::new(
                    "invalid_registration",
                    format!("Permitted root is not a directory: {raw_root}"),
                    400,
                ));
            }
            if !resolved_roots.contains(&canon) {
                resolved_roots.push(canon);
            }
        }

        let catalog_raw = params
            .get("catalogPath")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                BridgeError::new("invalid_registration", "catalogPath is required.", 400)
            })?;
        let lock_raw = params
            .get("lockPath")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                BridgeError::new("invalid_registration", "lockPath is required.", 400)
            })?;

        let catalog_path = std::fs::canonicalize(catalog_raw).map_err(|_| {
            BridgeError::new(
                "invalid_registration",
                "Catalog and lock files must exist.",
                400,
            )
        })?;
        let lock_path = std::fs::canonicalize(lock_raw).map_err(|_| {
            BridgeError::new(
                "invalid_registration",
                "Catalog and lock files must exist.",
                400,
            )
        })?;

        let (lock_meta, lock_bytes) = read_bounded_file(
            &lock_path,
            LOCK_MAX_BYTES,
            "invalid_registration",
            400,
            "adapter_budget",
        )?;
        let actual_lock_sha = sha256_bytes(&lock_bytes);
        if let Some(expected_lock_sha) = params
            .get("expectedLockSha256")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            && actual_lock_sha != expected_lock_sha
        {
            return Err(BridgeError::new(
                "integrity_mismatch",
                "The catalog.lock.json digest differs from lockSha256.",
                409,
            ));
        }

        let (catalog_meta, catalog_bytes) = read_bounded_file(
            &catalog_path,
            CATALOG_MAX_BYTES,
            "invalid_registration",
            400,
            "adapter_budget",
        )?;
        let actual_catalog_sha = sha256_bytes(&catalog_bytes);

        let catalog_doc: Value = serde_json::from_slice(&catalog_bytes).map_err(|err| {
            BridgeError::new(
                "invalid_registration",
                format!("Invalid catalog.ddk.json JSON: {err}"),
                400,
            )
        })?;
        if !catalog_doc.is_object() {
            return Err(BridgeError::new(
                "invalid_registration",
                "catalog.ddk.json root must be a JSON object.",
                400,
            ));
        }
        let lock_doc: Value = serde_json::from_slice(&lock_bytes).map_err(|err| {
            BridgeError::new(
                "invalid_registration",
                format!("Invalid catalog.lock.json JSON: {err}"),
                400,
            )
        })?;
        if !lock_doc.is_object() {
            return Err(BridgeError::new(
                "invalid_registration",
                "catalog.lock.json root must be a JSON object.",
                400,
            ));
        }

        let catalog_dir = catalog_path
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| {
                BridgeError::new(
                    "invalid_registration",
                    "catalogPath must have a parent directory.",
                    400,
                )
            })?;

        self.digest_cache
            .insert(stat_signature(&lock_meta), actual_lock_sha.clone());
        self.digest_cache
            .insert(stat_signature(&catalog_meta), actual_catalog_sha.clone());

        // Seed companion SQL digests and text cache on startup
        if let Some(files) = catalog_doc.get("files").and_then(Value::as_array) {
            for entry in files {
                if entry.get("role").and_then(Value::as_str) == Some("recipe")
                    && let Some(rel) = entry.get("path").and_then(Value::as_str)
                    && let Ok(comp_path) = std::fs::canonicalize(catalog_dir.join(rel))
                    && comp_path.starts_with(&catalog_dir)
                    && let Ok((comp_meta, comp_bytes)) = read_bounded_file(
                        &comp_path,
                        SQL_COMPANION_MAX_BYTES,
                        "invalid_registration",
                        400,
                        "adapter_budget",
                    )
                {
                    let comp_sha = sha256_bytes(&comp_bytes);
                    let comp_sig = stat_signature(&comp_meta);
                    self.digest_cache.insert(comp_sig, comp_sha);
                    if let Ok(comp_str) = String::from_utf8(comp_bytes)
                        && self.sql_cache.len() < MAX_SQL_CACHE
                    {
                        self.sql_cache.insert(comp_sig, Arc::from(comp_str));
                    }
                }
            }
        }

        // Verify own or explicitly supplied DocSpec binary SHA-256 once at startup
        let bin_path = if let Some(raw_bin) = params
            .get("docspecBinary")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            let candidate = Path::new(raw_bin);
            if !candidate.is_absolute() {
                return Err(BridgeError::new(
                    "invalid_registration",
                    "docspecBinary must be an absolute path.",
                    400,
                ));
            }
            let resolved = std::fs::canonicalize(candidate).map_err(|err| {
                BridgeError::new(
                    "owner_unavailable",
                    format!("DocSpec binary is unavailable: {err}"),
                    503,
                )
            })?;
            let meta = std::fs::metadata(&resolved).map_err(|err| {
                BridgeError::new(
                    "owner_unavailable",
                    format!("DocSpec binary is unavailable: {err}"),
                    503,
                )
            })?;
            if !meta.is_file() {
                return Err(BridgeError::new(
                    "owner_unavailable",
                    format!(
                        "DocSpec binary is not a regular file: {}",
                        resolved.display()
                    ),
                    503,
                ));
            }
            resolved
        } else {
            std::env::current_exe()
                .and_then(std::fs::canonicalize)
                .map_err(|err| {
                    BridgeError::new(
                        "owner_unavailable",
                        format!("Unable to resolve current docspec executable: {err}"),
                        503,
                    )
                })?
        };

        let actual_bin_sha = sha256_file(&bin_path)?;
        if let Some(expected_bin_sha) = params
            .get("docspecSha256")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            && actual_bin_sha != expected_bin_sha
        {
            return Err(BridgeError::new(
                "integrity_mismatch",
                "The selected DocSpec binary SHA-256 differs from docspecSha256.",
                409,
            ));
        }

        let merged_limits =
            if let Some(limits_obj) = params.get("docspecLimits").and_then(Value::as_object) {
                merge_docspec_limits(&self.base_limits, limits_obj)?
            } else {
                Budget::new(self.base_limits.clone()).map_err(|err| {
                    BridgeError::new(
                        "invalid_registration",
                        format!("Invalid base limits: {err}"),
                        400,
                    )
                })?;
                self.base_limits.clone()
            };

        let timeout_sec = match params.get("docspecTimeoutSec") {
            None | Some(Value::Null) => DEFAULT_QUERY_TIMEOUT_SEC,
            Some(v) => {
                let secs = v.as_f64().ok_or_else(|| {
                    BridgeError::new(
                        "invalid_registration",
                        "docspecTimeoutSec must be a positive number.",
                        400,
                    )
                })?;
                if !secs.is_finite() || secs <= 0.0 {
                    return Err(BridgeError::new(
                        "invalid_registration",
                        "docspecTimeoutSec must be a positive finite number.",
                        400,
                    ));
                }
                secs
            }
        };

        let retain_receipts_dir = if let Some(raw_retain) = params
            .get("retainReceiptsDir")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            std::fs::create_dir_all(raw_retain).map_err(|err| {
                BridgeError::new(
                    "invalid_registration",
                    format!("Unable to create retainReceiptsDir {raw_retain:?}: {err}"),
                    400,
                )
            })?;
            let canon = std::fs::canonicalize(raw_retain).map_err(|err| {
                BridgeError::new(
                    "invalid_registration",
                    format!("Unable to resolve retainReceiptsDir {raw_retain:?}: {err}"),
                    400,
                )
            })?;
            Some(canon)
        } else {
            None
        };

        let engine_info = json!({
            "id": "docspec",
            "version": env!("CARGO_PKG_VERSION"),
            "binarySha256": actual_bin_sha,
        });

        self.permitted_roots = resolved_roots;
        self.catalog_path = Some(catalog_path);
        self.catalog_dir = Some(catalog_dir);
        self.catalog_sha256 = Some(actual_catalog_sha);
        self.catalog_doc = Some(catalog_doc);
        self.lock_path = Some(lock_path);
        self.lock_sha256 = Some(actual_lock_sha.clone());
        self.limits = merged_limits;
        self.query_timeout = Duration::from_secs_f64(timeout_sec);
        self.retain_receipts_dir = retain_receipts_dir;
        self.engine_info = engine_info.clone();
        self.initialized = true;

        Ok(json!({
            "runtime": "docspec-native-rust",
            "lockSha256": actual_lock_sha,
            "engine": engine_info,
        }))
    }

    /// Handles `read_page(params)` purely via Rust Arrow/Parquet row-group pruning and slicing.
    pub fn read_page(&mut self, params: &Map<String, Value>) -> Result<Value, BridgeError> {
        if !self.initialized {
            return Err(BridgeError::new(
                "owner_unavailable",
                "Owner worker is not initialized.",
                503,
            ));
        }

        let shards = params
            .get("shards")
            .and_then(Value::as_array)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                BridgeError::new(
                    "page_budget",
                    "Invalid Parquet shard pagination parameters.",
                    400,
                )
            })?;

        let offset_u64 = params
            .get("offset")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                BridgeError::new(
                    "page_budget",
                    "Invalid Parquet shard pagination parameters.",
                    400,
                )
            })?;
        let offset = usize::try_from(offset_u64).map_err(|_| {
            BridgeError::new(
                "page_budget",
                "Invalid Parquet shard pagination parameters.",
                400,
            )
        })?;

        let limit_u64 = params.get("limit").and_then(Value::as_u64).ok_or_else(|| {
            BridgeError::new(
                "page_budget",
                "Invalid Parquet shard pagination parameters.",
                400,
            )
        })?;
        if !(1..=100).contains(&limit_u64) {
            return Err(BridgeError::new(
                "page_budget",
                "Invalid Parquet shard pagination parameters.",
                400,
            ));
        }
        let limit = limit_u64 as usize;

        let verify_max = match params.get("verifyDigestMaxBytes") {
            None | Some(Value::Null) => DEFAULT_VERIFY_DIGEST_MAX_BYTES,
            Some(v) => v.as_u64().ok_or_else(|| {
                BridgeError::new(
                    "page_budget",
                    "verifyDigestMaxBytes must be a non-negative integer.",
                    400,
                )
            })?,
        };

        let roots = self.permitted_roots.clone();
        let mut opened: Vec<(ParquetRecordBatchReaderBuilder<File>, usize)> =
            Vec::with_capacity(shards.len());
        let mut expected_cursor = 0usize;

        for shard_val in shards {
            let shard = shard_val.as_object().ok_or_else(|| {
                BridgeError::new(
                    "page_budget",
                    "Each shard specification must be an object.",
                    400,
                )
            })?;
            let raw_path = shard
                .get("path")
                .and_then(Value::as_str)
                .ok_or_else(|| BridgeError::new("page_budget", "Shard path is required.", 400))?;

            let (resolved, meta) = check_permitted_file(raw_path, &roots)?;

            if let Some(byte_size_val) = shard.get("byteSize").filter(|v| !v.is_null()) {
                let expected_size = byte_size_val.as_u64().ok_or_else(|| {
                    BridgeError::new(
                        "integrity_mismatch",
                        "Parquet shard byteSize disagrees with file size.",
                        409,
                    )
                })?;
                if meta.len() != expected_size {
                    return Err(BridgeError::new(
                        "integrity_mismatch",
                        "Parquet shard byteSize disagrees with file size.",
                        409,
                    ));
                }
            }

            if let Some(expected_sha) = shard
                .get("sha256")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                self.verify_cached_digest(
                    &resolved,
                    &meta,
                    expected_sha,
                    verify_max,
                    "Parquet file bytes differ from catalog SHA-256.",
                )?;
            }

            let file = File::open(&resolved).map_err(|err| {
                BridgeError::new(
                    "evidence_refused",
                    format!("Permitted file is unavailable: {err}"),
                    404,
                )
            })?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|err| {
                BridgeError::new(
                    "owner_schema_mismatch",
                    format!("Unable to open Parquet metadata: {err}"),
                    422,
                )
            })?;

            let num_rows_i64 = builder.metadata().file_metadata().num_rows();
            let num_rows = usize::try_from(num_rows_i64).map_err(|_| {
                BridgeError::new(
                    "owner_schema_mismatch",
                    "Parquet metadata reported negative row count.",
                    422,
                )
            })?;

            if let Some(row_count_val) = shard.get("rowCount").filter(|v| !v.is_null()) {
                let expected_rows = row_count_val
                    .as_u64()
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or_else(|| {
                        BridgeError::new(
                            "binding_mismatch",
                            "Parquet shard rowCount disagrees with footer metadata.",
                            409,
                        )
                    })?;
                if num_rows != expected_rows {
                    return Err(BridgeError::new(
                        "binding_mismatch",
                        "Parquet shard rowCount disagrees with footer metadata.",
                        409,
                    ));
                }
            }

            if let Some(range_val) = shard.get("rowRange").filter(|v| !v.is_null()) {
                let range_arr = range_val
                    .as_array()
                    .filter(|a| a.len() == 2)
                    .ok_or_else(|| {
                        BridgeError::new(
                            "binding_mismatch",
                            "Parquet shard rowRange is not contiguous with metadata.",
                            409,
                        )
                    })?;
                let r_start = range_arr[0]
                    .as_u64()
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or_else(|| {
                        BridgeError::new(
                            "binding_mismatch",
                            "Parquet shard rowRange is not contiguous with metadata.",
                            409,
                        )
                    })?;
                let r_end = range_arr[1]
                    .as_u64()
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or_else(|| {
                        BridgeError::new(
                            "binding_mismatch",
                            "Parquet shard rowRange is not contiguous with metadata.",
                            409,
                        )
                    })?;
                if r_start != expected_cursor || r_end < r_start || (r_end - r_start) != num_rows {
                    return Err(BridgeError::new(
                        "binding_mismatch",
                        "Parquet shard rowRange is not contiguous with metadata.",
                        409,
                    ));
                }
            }

            expected_cursor = expected_cursor.saturating_add(num_rows);
            opened.push((builder, num_rows));
        }

        let total = expected_cursor;
        let schema_summary: Vec<Value> = opened[0]
            .0
            .schema()
            .fields()
            .iter()
            .map(|field| {
                json!({
                    "name": field.name(),
                    "type": field.data_type().to_string(),
                    "nullable": field.is_nullable(),
                })
            })
            .collect();

        if offset >= total {
            return Ok(json!({
                "rows": [],
                "rowPositions": [],
                "total": total,
                "hasMore": false,
                "rowOrder": "source-row",
                "schema": schema_summary,
            }));
        }

        let target_start = offset;
        let target_end = offset.saturating_add(limit).min(total);
        let batch_size = (limit * 2).clamp(128, 2048);
        let mut rows: Vec<Value> = Vec::with_capacity(target_end - target_start);
        let mut row_positions: Vec<usize> = Vec::with_capacity(target_end - target_start);
        let mut exact_budget = ExactBudget::default();
        let mut global_cursor = 0usize;

        for (builder, shard_rows) in opened {
            let shard_start = global_cursor;
            let shard_end = shard_start + shard_rows;
            global_cursor = shard_end;
            if shard_end <= target_start || shard_start >= target_end {
                continue;
            }

            let mut rg_cursor = shard_start;
            let mut selected_rgs = Vec::new();
            let mut selected_rg_start = None;
            for (rg_idx, rg_meta) in builder.metadata().row_groups().iter().enumerate() {
                let rg_rows = usize::try_from(rg_meta.num_rows()).map_err(|_| {
                    BridgeError::new(
                        "owner_schema_mismatch",
                        "Parquet row-group reported negative row count.",
                        422,
                    )
                })?;
                let rg_start = rg_cursor;
                let rg_end = rg_start + rg_rows;
                rg_cursor = rg_end;
                if rg_end <= target_start || rg_start >= target_end {
                    continue;
                }
                if selected_rg_start.is_none() {
                    selected_rg_start = Some(rg_start);
                }
                selected_rgs.push(rg_idx);
            }

            let Some(mut batch_cursor) = selected_rg_start else {
                continue;
            };
            let reader = builder
                .with_row_groups(selected_rgs)
                .with_batch_size(batch_size)
                .build()
                .map_err(|err| {
                    BridgeError::new(
                        "owner_schema_mismatch",
                        format!("Unable to build Parquet batch reader: {err}"),
                        422,
                    )
                })?;

            for batch_res in reader {
                let batch = batch_res.map_err(|err| {
                    BridgeError::new(
                        "owner_schema_mismatch",
                        format!("Unable to decode Parquet record batch: {err}"),
                        422,
                    )
                })?;
                let b_len = batch.num_rows();
                let b_start = batch_cursor;
                let b_end = b_start + b_len;
                batch_cursor = b_end;

                if b_end <= target_start {
                    continue;
                }
                if b_start >= target_end {
                    break;
                }

                let local_start = target_start.saturating_sub(b_start);
                let local_end = b_end.min(target_end) - b_start;
                for r_idx in local_start..local_end {
                    let row_val = exact::encode_batch_row(&batch, r_idx, &mut exact_budget)?;
                    rows.push(row_val);
                    row_positions.push(b_start + r_idx);
                }

                if b_end >= target_end {
                    break;
                }
            }
        }

        let has_more = offset + rows.len() < total;
        Ok(json!({
            "rows": rows,
            "rowPositions": row_positions,
            "total": total,
            "hasMore": has_more,
            "rowOrder": "source-row",
            "schema": schema_summary,
        }))
    }

    /// Handles `run_query(params)` completely in-process via `docspec::job::Job` + DataFusion.
    pub async fn run_query(&mut self, params: &Map<String, Value>) -> Result<Value, BridgeError> {
        if !self.initialized {
            return Err(BridgeError::new(
                "owner_unavailable",
                "Owner worker is not initialized.",
                503,
            ));
        }

        let recipe_id = params
            .get("recipeId")
            .and_then(Value::as_str)
            .ok_or_else(|| BridgeError::new("invalid_request", "recipeId is required.", 400))?;
        let expected_sql_sha = params
            .get("expectedSqlSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                BridgeError::new("invalid_request", "expectedSqlSha256 is required.", 400)
            })?;
        let limit_u64 = params.get("limit").and_then(Value::as_u64).ok_or_else(|| {
            BridgeError::new("page_budget", "limit must be a positive integer.", 400)
        })?;
        if limit_u64 == 0 {
            return Err(BridgeError::new(
                "page_budget",
                "limit must be at least 1.",
                400,
            ));
        }
        let limit = usize::try_from(limit_u64)
            .map_err(|_| BridgeError::new("page_budget", "limit overflows usize.", 400))?;
        let wrap_limit = match params.get("wrapLimit") {
            None | Some(Value::Null) => true,
            Some(v) => v.as_bool().unwrap_or(true),
        };
        let release_id = params
            .get("releaseId")
            .filter(|v| !v.is_null())
            .map(|v| {
                v.as_str().ok_or_else(|| {
                    BridgeError::new("invalid_request", "releaseId must be a string.", 400)
                })
            })
            .transpose()?;
        let versions_arg = params
            .get("versions")
            .filter(|v| !v.is_null())
            .map(|v| {
                v.as_object().ok_or_else(|| {
                    BridgeError::new("invalid_request", "versions must be an object.", 400)
                })
            })
            .transpose()?;
        let input_specs = params
            .get("inputs")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                BridgeError::new("invalid_request", "inputs object is required.", 400)
            })?;
        let empty_map = Map::new();
        let raw_parameters = match params.get("parameters") {
            None | Some(Value::Null) => &empty_map,
            Some(v) => v.as_object().ok_or_else(|| {
                BridgeError::new("invalid_parameters", "parameters must be an object.", 400)
            })?,
        };
        let raw_numeric = match params.get("rawNumericParameters") {
            None | Some(Value::Null) => &empty_map,
            Some(v) => v.as_object().ok_or_else(|| {
                BridgeError::new(
                    "invalid_parameters",
                    "rawNumericParameters must be an object.",
                    400,
                )
            })?,
        };
        let verify_max = match params.get("verifyDigestMaxBytes") {
            None | Some(Value::Null) => DEFAULT_VERIFY_DIGEST_MAX_BYTES,
            Some(v) => v.as_u64().ok_or_else(|| {
                BridgeError::new(
                    "page_budget",
                    "verifyDigestMaxBytes must be a non-negative integer.",
                    400,
                )
            })?,
        };

        // Re-verify stat-cached SHA-256 of lockPath and catalogPath
        if let (Some(lock_path), Some(lock_sha)) =
            (self.lock_path.clone(), self.lock_sha256.clone())
        {
            let lock_meta = std::fs::metadata(&lock_path).map_err(|_| {
                BridgeError::new(
                    "integrity_mismatch",
                    "The catalog.lock.json file is missing.",
                    409,
                )
            })?;
            self.verify_cached_digest(
                &lock_path,
                &lock_meta,
                &lock_sha,
                LOCK_MAX_BYTES,
                "The catalog.lock.json digest differs from lockSha256.",
            )?;
        }
        if let (Some(cat_path), Some(cat_sha)) =
            (self.catalog_path.clone(), self.catalog_sha256.clone())
        {
            let cat_meta = std::fs::metadata(&cat_path).map_err(|_| {
                BridgeError::new(
                    "integrity_mismatch",
                    "The catalog.ddk.json file is missing.",
                    409,
                )
            })?;
            self.verify_cached_digest(
                &cat_path,
                &cat_meta,
                &cat_sha,
                CATALOG_MAX_BYTES,
                "The catalog.ddk.json digest differs from startup digest.",
            )?;
        }

        let catalog_dir = self.catalog_dir.clone().ok_or_else(|| {
            BridgeError::new("owner_unavailable", "Catalog directory unavailable.", 503)
        })?;
        let catalog_doc = self.catalog_doc.clone().ok_or_else(|| {
            BridgeError::new("owner_unavailable", "Catalog document unavailable.", 503)
        })?;

        let recipe_doc = catalog_doc
            .get("recipes")
            .and_then(Value::as_array)
            .and_then(|arr| {
                arr.iter()
                    .filter_map(Value::as_object)
                    .find(|r| r.get("id").and_then(Value::as_str) == Some(recipe_id))
            })
            .ok_or_else(|| {
                BridgeError::new(
                    "unknown_recipe",
                    format!("Recipe {recipe_id:?} is absent from catalog."),
                    404,
                )
            })?;

        let sql_rel = recipe_doc
            .get("sql")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                BridgeError::new(
                    "integrity_mismatch",
                    "Recipe SQL companion path is missing.",
                    409,
                )
            })?;

        // Check unique companion entry in catalog.files
        let matching_files: Vec<&Map<String, Value>> = catalog_doc
            .get("files")
            .and_then(Value::as_array)
            .map(|files| {
                files
                    .iter()
                    .filter_map(Value::as_object)
                    .filter(|f| {
                        f.get("role").and_then(Value::as_str) == Some("recipe")
                            && f.get("path").and_then(Value::as_str) == Some(sql_rel)
                    })
                    .collect()
            })
            .unwrap_or_default();
        if matching_files.len() != 1 {
            return Err(BridgeError::new(
                "native_scope_refused",
                "CATALOG-RECIPE-INTEGRITY: The SQL has no unique companion inventory entry.",
                409,
            ));
        }
        let catalog_sql_sha = matching_files[0]
            .get("sha256")
            .and_then(Value::as_str)
            .unwrap_or("");
        if catalog_sql_sha != expected_sql_sha {
            return Err(BridgeError::new(
                "integrity_mismatch",
                "Recipe SQL companion digest mismatch.",
                409,
            ));
        }

        let sql_file = std::fs::canonicalize(catalog_dir.join(sql_rel)).map_err(|_| {
            BridgeError::new(
                "integrity_mismatch",
                "Recipe SQL companion file is missing.",
                409,
            )
        })?;
        if !sql_file.starts_with(&catalog_dir) {
            return Err(BridgeError::new(
                "evidence_refused",
                "Recipe SQL companion escapes catalog directory.",
                409,
            ));
        }
        let raw_sql = self.load_verified_sql(&sql_file, expected_sql_sha)?;

        let started = Instant::now();

        // Validate parameters against recipe JSON Schema
        let (norm_params, norm_lexemes) =
            sql_params::normalize_incoming_parameters(raw_parameters, raw_numeric)?;
        sql_params::validate_parameters_schema(
            &norm_params,
            &norm_lexemes,
            recipe_doc.get("parameters"),
        )?;

        // Validate releaseId vs versions and recipe relationships against catalog_doc
        if release_id.is_some() == versions_arg.is_some() {
            return Err(BridgeError::new(
                "native_scope_refused",
                "CATALOG-BINDING-UNRESOLVED: Supply one exact release, selection, or version mapping.",
                409,
            ));
        }

        let recipe_inputs = recipe_doc
            .get("inputs")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                BridgeError::new(
                    "native_scope_refused",
                    "Recipe inputs mapping is missing.",
                    409,
                )
            })?;
        let mut local_aliases: BTreeMap<String, String> = BTreeMap::new();
        for (input_name, alias_val) in recipe_inputs {
            let Some(alias_str) = alias_val.as_str() else {
                return Err(BridgeError::new(
                    "native_scope_refused",
                    format!(
                        "CATALOG-BINDING-UNRESOLVED: Peer catalog binding for {input_name:?} is unsupported in single-catalog owner."
                    ),
                    409,
                ));
            };
            local_aliases.insert(input_name.clone(), alias_str.to_owned());
        }

        let catalog_versions_arr = catalog_doc
            .get("versions")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                BridgeError::new(
                    "native_scope_refused",
                    "Catalog versions list is missing.",
                    409,
                )
            })?;
        let mut versions_by_id: BTreeMap<&str, &Map<String, Value>> = BTreeMap::new();
        for ver_val in catalog_versions_arr {
            let ver_obj = ver_val.as_object().ok_or_else(|| {
                BridgeError::new(
                    "native_scope_refused",
                    "Invalid catalog version entry.",
                    409,
                )
            })?;
            let ver_id = ver_obj.get("id").and_then(Value::as_str).ok_or_else(|| {
                BridgeError::new(
                    "native_scope_refused",
                    "Catalog version entry is missing id.",
                    409,
                )
            })?;
            if versions_by_id.insert(ver_id, ver_obj).is_some() {
                return Err(BridgeError::new(
                    "native_scope_refused",
                    "CATALOG-BINDING-UNRESOLVED: Dataset version identifiers are ambiguous.",
                    409,
                ));
            }
        }

        let empty_vec = Vec::new();
        let catalog_releases_arr = catalog_doc
            .get("releases")
            .and_then(Value::as_array)
            .unwrap_or(&empty_vec);
        for rel_val in catalog_releases_arr {
            if let Some(members) = rel_val.get("members").and_then(Value::as_array) {
                for m in members {
                    let Some(m_id) = m.as_str() else {
                        return Err(BridgeError::new(
                            "native_scope_refused",
                            "CATALOG-BINDING-UNRESOLVED: Release member must be a string id.",
                            409,
                        ));
                    };
                    if !versions_by_id.contains_key(m_id) {
                        return Err(BridgeError::new(
                            "native_scope_refused",
                            "CATALOG-BINDING-UNRESOLVED: A release includes an unresolved version.",
                            409,
                        ));
                    }
                }
            }
        }

        let mut bound_versions: BTreeMap<String, &Map<String, Value>> = BTreeMap::new();
        if let Some(rel_id) = release_id {
            let matching_releases: Vec<&Map<String, Value>> = catalog_releases_arr
                .iter()
                .filter_map(Value::as_object)
                .filter(|r| r.get("id").and_then(Value::as_str) == Some(rel_id))
                .collect();
            if matching_releases.len() != 1 {
                return Err(BridgeError::new(
                    "native_scope_refused",
                    format!("CATALOG-BINDING-UNRESOLVED: Expected one release for {rel_id:?}."),
                    409,
                ));
            }
            let members = matching_releases[0]
                .get("members")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    BridgeError::new(
                        "native_scope_refused",
                        "CATALOG-BINDING-UNRESOLVED: Release members list is missing.",
                        409,
                    )
                })?;
            for (input_name, ds_alias) in &local_aliases {
                let candidates: Vec<&Map<String, Value>> = members
                    .iter()
                    .filter_map(Value::as_str)
                    .filter_map(|m_id| versions_by_id.get(m_id).copied())
                    .filter(|v| v.get("dataset").and_then(Value::as_str) == Some(ds_alias.as_str()))
                    .collect();
                if candidates.len() != 1 {
                    return Err(BridgeError::new(
                        "native_scope_refused",
                        format!(
                            "CATALOG-BINDING-UNRESOLVED: Release {rel_id:?} needs one version of {ds_alias:?}."
                        ),
                        409,
                    ));
                }
                bound_versions.insert(input_name.clone(), candidates[0]);
            }
        } else if let Some(ver_map) = versions_arg {
            let expected_keys: BTreeSet<&str> = local_aliases.keys().map(String::as_str).collect();
            let supplied_keys: BTreeSet<&str> = ver_map.keys().map(String::as_str).collect();
            if expected_keys != supplied_keys {
                return Err(BridgeError::new(
                    "native_scope_refused",
                    "CATALOG-BINDING-UNRESOLVED: Supply an exact version for every required input.",
                    409,
                ));
            }
            for (input_name, ds_alias) in &local_aliases {
                let ver_id = ver_map
                    .get(input_name)
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        BridgeError::new(
                            "native_scope_refused",
                            "CATALOG-BINDING-UNRESOLVED: Version id must be a string.",
                            409,
                        )
                    })?;
                let ver_doc = versions_by_id.get(ver_id).copied().ok_or_else(|| {
                    BridgeError::new(
                        "native_scope_refused",
                        format!(
                            "CATALOG-BINDING-UNRESOLVED: Expected one dataset version for {ver_id:?}."
                        ),
                        409,
                    )
                })?;
                if ver_doc.get("dataset").and_then(Value::as_str) != Some(ds_alias.as_str()) {
                    return Err(BridgeError::new(
                        "native_scope_refused",
                        format!(
                            "CATALOG-VERSION-MISMATCH: Version {ver_id:?} does not bind {ds_alias:?}."
                        ),
                        409,
                    ));
                }
                bound_versions.insert(input_name.clone(), ver_doc);
            }
        }

        // Enforce recipe relationships
        if let Some(rel_ids) = recipe_doc.get("relationships").and_then(Value::as_array) {
            let catalog_rels = catalog_doc
                .get("relationships")
                .and_then(Value::as_array)
                .unwrap_or(&empty_vec);
            for rel_id_val in rel_ids {
                let rel_id = rel_id_val.as_str().ok_or_else(|| {
                    BridgeError::new(
                        "native_scope_refused",
                        "Invalid relationship id in recipe.",
                        409,
                    )
                })?;
                let matches: Vec<&Map<String, Value>> = catalog_rels
                    .iter()
                    .filter_map(Value::as_object)
                    .filter(|r| r.get("id").and_then(Value::as_str) == Some(rel_id))
                    .collect();
                if matches.len() != 1 {
                    return Err(BridgeError::new(
                        "native_scope_refused",
                        format!(
                            "CATALOG-BINDING-UNRESOLVED: Expected one relationship for {rel_id:?}."
                        ),
                        409,
                    ));
                }
                let rel_doc = matches[0];
                let scope = rel_doc.get("scope").and_then(Value::as_str).unwrap_or("");
                let has_external_target = rel_doc
                    .get("target")
                    .and_then(Value::as_object)
                    .is_some_and(|t| t.contains_key("external"));
                if matches!(scope, "external" | "declared") || has_external_target {
                    return Err(BridgeError::new(
                        "native_scope_refused",
                        "CATALOG-BINDING-UNRESOLVED: The recipe uses a relationship with unsupported scope.",
                        409,
                    ));
                }
                if scope == "same_release" && release_id.is_none() {
                    return Err(BridgeError::new(
                        "native_scope_refused",
                        "CATALOG-RELEASE-SCOPE: The recipe needs one exact release for its relationship.",
                        409,
                    ));
                }
            }
        }

        // Check withheld references on bound versions
        for ver_doc in bound_versions.values() {
            let withheld_flag =
                ver_doc.get("referenceDisclosure").and_then(Value::as_str) == Some("withheld");
            let missing_ref = match ver_doc.get("reference") {
                None | Some(Value::Null) => true,
                Some(_) => false,
            };
            if withheld_flag || missing_ref {
                return Err(BridgeError::new(
                    "native_scope_refused",
                    "CATALOG-VERSION-WITHHELD: The exact native reference is unavailable in this catalog view.",
                    409,
                ));
            }
        }

        // Verify bound_versions match input_specs keys and versionIds
        let bound_keys: BTreeSet<&str> = bound_versions.keys().map(String::as_str).collect();
        let spec_keys: BTreeSet<&str> = input_specs.keys().map(String::as_str).collect();
        if bound_keys != spec_keys {
            return Err(BridgeError::new(
                "recipe_inputs",
                "Resolved recipe inputs differ from requested bindings.",
                409,
            ));
        }

        let roots = self.permitted_roots.clone();
        let mut verified_inputs: Vec<(String, PathBuf)> = Vec::with_capacity(bound_versions.len());
        let mut receipt_inputs: Vec<Value> = Vec::with_capacity(bound_versions.len());

        for (input_name, ver_doc) in &bound_versions {
            let spec = input_specs
                .get(input_name)
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    BridgeError::new(
                        "recipe_inputs",
                        format!("Input specification for {input_name:?} must be an object."),
                        409,
                    )
                })?;
            let resolved_ver_id = ver_doc.get("id").and_then(Value::as_str);
            let spec_ver_id = spec.get("versionId").and_then(Value::as_str);
            if resolved_ver_id != spec_ver_id {
                return Err(BridgeError::new(
                    "binding_mismatch",
                    format!("Input {input_name:?} resolved a different version."),
                    409,
                ));
            }
            let raw_path = spec.get("path").and_then(Value::as_str).ok_or_else(|| {
                BridgeError::new(
                    "evidence_refused",
                    format!("Input {input_name:?} is missing path."),
                    409,
                )
            })?;
            let (file_path, meta) = check_permitted_file(raw_path, &roots)?;
            let maybe_sha = spec
                .get("sha256")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty());
            if let Some(expected_sha) = maybe_sha {
                self.verify_cached_digest(
                    &file_path,
                    &meta,
                    expected_sha,
                    verify_max,
                    "Parquet file bytes differ from catalog SHA-256.",
                )?;
            }

            let mut entry = Map::new();
            entry.insert("name".to_owned(), Value::String(input_name.clone()));
            if let Some(vid) = spec_ver_id {
                entry.insert("versionId".to_owned(), Value::String(vid.to_owned()));
            }
            entry.insert(
                "path".to_owned(),
                Value::String(file_path.to_string_lossy().into_owned()),
            );
            if let Some(sha) = maybe_sha {
                entry.insert("sha256".to_owned(), Value::String(sha.to_owned()));
            }
            receipt_inputs.push(Value::Object(entry));
            verified_inputs.push((input_name.clone(), file_path));
        }

        // Bind SQL parameters and wrap limit
        let parameter_order: Option<Vec<String>> = match recipe_doc.get("parameterOrder") {
            None | Some(Value::Null) => None,
            Some(Value::Array(arr)) => {
                let mut order = Vec::with_capacity(arr.len());
                for item in arr {
                    let s = item.as_str().ok_or_else(|| {
                        BridgeError::new(
                            "invalid_parameters",
                            "recipe.parameterOrder entries must be strings.",
                            400,
                        )
                    })?;
                    order.push(s.to_owned());
                }
                Some(order)
            }
            Some(_) => {
                return Err(BridgeError::new(
                    "invalid_parameters",
                    "recipe.parameterOrder must be an array of strings.",
                    400,
                ));
            }
        };

        let bound_sql = sql_params::bind_sql_parameters(
            &raw_sql,
            &norm_params,
            &norm_lexemes,
            recipe_doc.get("parameters"),
            parameter_order.as_deref(),
        )?;
        let exec_sql = if wrap_limit {
            format!(
                "SELECT * FROM (\n{}\n) AS _ddk_recipe_query LIMIT {}",
                sql_params::strip_trailing_semicolon(&bound_sql),
                limit.saturating_add(1)
            )
        } else {
            bound_sql
        };

        // Execute in-process on tokio using docspec::job::Job
        self.next_query_seq = self.next_query_seq.saturating_add(1);
        let scratch_dir = self
            .worker_temp_dir
            .join(format!("query-scratch-{}", self.next_query_seq));
        std::fs::create_dir_all(&scratch_dir).map_err(|err| {
            BridgeError::new(
                "owner_unavailable",
                format!("Unable to create query scratch directory: {err}"),
                503,
            )
        })?;
        let _scratch_guard = ScratchDirGuard {
            path: scratch_dir.clone(),
        };

        let limits = self.limits.clone();
        let timeout_dur = self.query_timeout;

        let mut encoded_rows = tokio::time::timeout(timeout_dur, async move {
            let job = crate::job::Job::new(limits, scratch_dir).map_err(map_job_error)?;
            for (input_name, file_path) in &verified_inputs {
                job.register_parquet(input_name, file_path)
                    .await
                    .map_err(map_job_error)?;
            }
            let mut stream = job
                .execute_sql_stream(&exec_sql)
                .await
                .map_err(map_job_error)?;

            let mut exact_budget = ExactBudget::default();
            let mut out_rows = Vec::with_capacity(limit.min(1024).saturating_add(1));
            'batches: while let Some(batch) = stream
                .try_next()
                .await
                .map_err(|df_err| map_job_error(crate::Error::from(df_err)))?
            {
                for r_idx in 0..batch.num_rows() {
                    let row_val = exact::encode_batch_row(&batch, r_idx, &mut exact_budget)?;
                    out_rows.push(row_val);
                    if !wrap_limit && out_rows.len() > limit {
                        break 'batches;
                    }
                    if wrap_limit && out_rows.len() > limit {
                        break 'batches;
                    }
                }
            }
            Ok::<Vec<Value>, BridgeError>(out_rows)
        })
        .await
        .map_err(|_| {
            BridgeError::new(
                "native_read_unknown",
                "DocSpec query execution timed out.",
                408,
            )
        })??;

        let has_more = encoded_rows.len() > limit;
        if has_more {
            encoded_rows.truncate(limit);
        }
        let returned_count = encoded_rows.len();
        let row_positions: Vec<usize> = (0..returned_count).collect();
        let elapsed_ms = (started.elapsed().as_secs_f64() * 1000.0 * 100.0).round() / 100.0;

        let retained_record = if let Some(retain_dir) = &self.retain_receipts_dir {
            let receipt_payload = json!({
                "kind": "docspec-owner-query-receipt",
                "recipeId": recipe_id,
                "sqlSha256": expected_sql_sha,
                "inputs": receipt_inputs,
                "returned": returned_count,
                "hasMore": has_more,
                "engine": self.engine_info,
            });
            let mut receipt_bytes = serde_json::to_vec_pretty(&receipt_payload).map_err(|err| {
                BridgeError::new(
                    "owner_unavailable",
                    format!("Failed to serialize query receipt: {err}"),
                    503,
                )
            })?;
            receipt_bytes.push(b'\n');
            let receipt_sha = sha256_bytes(&receipt_bytes);

            let (receipt_path, mut receipt_file) = loop {
                self.next_receipt_seq = self.next_receipt_seq.saturating_add(1);
                let candidate =
                    retain_dir.join(format!("query-receipt-{}.json", self.next_receipt_seq));
                match OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&candidate)
                {
                    Ok(f) => break (candidate, f),
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(err) => {
                        return Err(BridgeError::new(
                            "owner_unavailable",
                            format!("Failed to create retained query receipt: {err}"),
                            503,
                        ));
                    }
                }
            };
            receipt_file.write_all(&receipt_bytes).map_err(|err| {
                BridgeError::new(
                    "owner_unavailable",
                    format!("Failed writing retained query receipt: {err}"),
                    503,
                )
            })?;
            receipt_file.sync_all().map_err(|err| {
                BridgeError::new(
                    "owner_unavailable",
                    format!("Failed syncing retained query receipt: {err}"),
                    503,
                )
            })?;
            drop(receipt_file);
            let canon_receipt = std::fs::canonicalize(&receipt_path).unwrap_or(receipt_path);
            Some(json!({
                "path": canon_receipt,
                "sha256": receipt_sha,
            }))
        } else {
            None
        };

        let mut response = Map::new();
        response.insert("rows".to_owned(), Value::Array(encoded_rows));
        response.insert(
            "rowPositions".to_owned(),
            Value::Array(row_positions.into_iter().map(Value::from).collect()),
        );
        response.insert("hasMore".to_owned(), Value::Bool(has_more));
        if !has_more {
            response.insert("total".to_owned(), Value::from(returned_count));
        }
        response.insert(
            "sqlSha256".to_owned(),
            Value::String(expected_sql_sha.to_owned()),
        );
        response.insert("elapsedMs".to_owned(), json!(elapsed_ms));
        response.insert("engine".to_owned(), self.engine_info.clone());
        if let Some(record_obj) = retained_record {
            response.insert("record".to_owned(), record_obj);
        }
        Ok(Value::Object(response))
    }

    /// Dispatches a single parsed JSON-RPC-like envelope and returns the response envelope.
    pub async fn handle_line(&mut self, line: &[u8]) -> Value {
        let parsed: Value = match serde_json::from_slice(line) {
            Ok(v) => v,
            Err(err) => {
                return json!({
                    "id": Value::Null,
                    "ok": false,
                    "error": {
                        "code": "invalid_request",
                        "message": format!("Malformed JSON request: {err}"),
                        "status": 400,
                    }
                });
            }
        };
        let Some(msg_obj) = parsed.as_object() else {
            return json!({
                "id": Value::Null,
                "ok": false,
                "error": {
                    "code": "invalid_request",
                    "message": "Worker request must be a JSON object.",
                    "status": 400,
                }
            });
        };
        let req_id = msg_obj.get("id").cloned().unwrap_or(Value::Null);
        let method = msg_obj.get("method").and_then(Value::as_str);
        let empty_params = Map::new();
        let params_res = match msg_obj.get("params") {
            None | Some(Value::Null) => Ok(&empty_params),
            Some(v) => v.as_object().ok_or_else(|| {
                BridgeError::new("invalid_request", "Worker params must be an object.", 400)
            }),
        };

        let outcome = match (method, params_res) {
            (_, Err(err)) => Err(err),
            (Some("init"), Ok(params)) => self.init(params),
            (Some("read_page"), Ok(params)) => self.read_page(params),
            (Some("run_query"), Ok(params)) => self.run_query(params).await,
            (Some("ping"), Ok(_)) => Ok(json!({ "ok": true })),
            (Some(other), Ok(_)) => Err(BridgeError::new(
                "invalid_request",
                format!("Unknown worker method: {other:?}"),
                400,
            )),
            (None, Ok(_)) => Err(BridgeError::new(
                "invalid_request",
                "Worker request method is required.",
                400,
            )),
        };

        match outcome {
            Ok(result) => json!({
                "id": req_id,
                "ok": true,
                "result": result,
            }),
            Err(err) => json!({
                "id": req_id,
                "ok": false,
                "error": {
                    "code": err.code,
                    "message": err.message,
                    "status": err.status,
                }
            }),
        }
    }
}

enum BoundedLine {
    Eof,
    Line,
    Oversized,
}

fn read_bounded_line(
    reader: &mut impl BufRead,
    max_bytes: usize,
    out: &mut Vec<u8>,
) -> std::io::Result<BoundedLine> {
    out.clear();
    let mut oversized = false;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if out.is_empty() && !oversized {
                return Ok(BoundedLine::Eof);
            }
            return Ok(if oversized {
                BoundedLine::Oversized
            } else {
                BoundedLine::Line
            });
        }
        if let Some(pos) = available.iter().position(|&b| b == b'\n') {
            let chunk = &available[..=pos];
            if !oversized {
                if out.len() + chunk.len() > max_bytes {
                    oversized = true;
                    out.clear();
                } else {
                    out.extend_from_slice(chunk);
                }
            }
            reader.consume(pos + 1);
            return Ok(if oversized {
                BoundedLine::Oversized
            } else {
                BoundedLine::Line
            });
        }
        let len = available.len();
        if !oversized {
            if out.len() + len > max_bytes {
                oversized = true;
                out.clear();
            } else {
                out.extend_from_slice(available);
            }
        }
        reader.consume(len);
    }
}

fn write_json_line(writer: &mut impl Write, value: &Value) -> std::io::Result<()> {
    serde_json::to_writer(&mut *writer, value).map_err(std::io::Error::other)?;
    writer.write_all(b"\n")?;
    writer.flush()
}

/// Runs the persistent `stdin -> stdout` JSON-lines owner worker loop until `stdin` EOF.
pub async fn serve_stdio(cli_limits: Option<Limits>) -> std::io::Result<()> {
    let mut worker = OwnerWorker::new(cli_limits)?;
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut reader = BufReader::with_capacity(IO_BUF_BYTES, stdin.lock());
    let mut writer = BufWriter::with_capacity(IO_BUF_BYTES, stdout.lock());
    let mut line_buf = Vec::with_capacity(4096);

    loop {
        match read_bounded_line(&mut reader, MAX_LINE_BYTES, &mut line_buf)? {
            BoundedLine::Eof => break,
            BoundedLine::Oversized => {
                let err_resp = json!({
                    "id": Value::Null,
                    "ok": false,
                    "error": {
                        "code": "request_budget",
                        "message": "Worker request exceeded line budget.",
                        "status": 413,
                    }
                });
                write_json_line(&mut writer, &err_resp)?;
            }
            BoundedLine::Line => {
                if line_buf.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                let resp = worker.handle_line(&line_buf).await;
                write_json_line(&mut writer, &resp)?;
            }
        }
    }
    Ok(())
}

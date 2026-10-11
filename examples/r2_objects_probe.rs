//! Opt-in qualification of the actual Rust create-only object adapter.
//!
//! Uses one fresh synthetic prefix, retains remote objects and possible unfinished
//! multipart uploads, and never changes catalogs, policies or existing objects.
//! Exact readback runs once per accepted object after its competing writes.

use docspec::{
    Error, Result,
    budget::{Budget, Limits},
    model::{Id, ObjectRef},
    storage::remote::remote_storage,
};
use serde_json::json;
use std::{collections::HashMap, fs::File, io::Write, path::Path};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        // Upstream provider messages and configuration never enter probe output.
        eprintln!("r2_objects_probe failed: {}", error.code());
        std::process::exit(1);
    }
}

fn env(name: &str) -> Result<String> {
    std::env::var(name)
        .map_err(|_| Error::refusal("remote_config", "required probe configuration is missing"))
}

fn object(owner: &Id, path: String, bytes: &[u8]) -> ObjectRef {
    ObjectRef {
        owner: owner.clone(),
        path,
        digest: blake3::hash(bytes).to_hex().to_string(),
        bytes: bytes.len() as u64,
    }
}

fn fixture(owner: &Id, local: &Path, remote: String, byte: u8) -> Result<ObjectRef> {
    let mut file = File::create(local)?;
    let chunk = vec![byte; 1 << 20];
    let mut digest = blake3::Hasher::new();
    // Cross the adapter's 8 MiB multipart boundary more than once. Generate and
    // hash fixture bytes together; no pre-upload payload hashing pass.
    let size = (17 << 20) + 19;
    let mut remaining = size;
    while remaining > 0 {
        let bytes = &chunk[..remaining.min(chunk.len())];
        file.write_all(bytes)?;
        digest.update(bytes);
        remaining -= bytes.len();
    }
    file.sync_all()?;
    Ok(ObjectRef {
        owner: owner.clone(),
        path: remote,
        digest: digest.finalize().to_hex().to_string(),
        bytes: size as u64,
    })
}

fn conflict(result: Result<ObjectRef>) -> Result<()> {
    match result {
        Err(error) if error.code() == "remote_conflict" => Ok(()),
        _ => Err(Error::refusal(
            "probe_conditional_create",
            "expected the maintained writer to refuse a competing create",
        )),
    }
}

async fn run() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 || args[0] != "--allow-synthetic-writes" {
        return Err(Error::refusal(
            "probe_opt_in",
            "supply --allow-synthetic-writes and a fresh qualification UUID",
        ));
    }
    let suffix = args[1]
        .strip_prefix("docspec-rs-qualification-")
        .ok_or_else(|| Error::refusal("probe_prefix", "qualification prefix required"))?;
    if uuid::Uuid::parse_str(suffix)
        .map(|id| id.to_string())
        .ok()
        .as_deref()
        != Some(suffix)
    {
        return Err(Error::refusal(
            "probe_prefix",
            "canonical qualification UUID required",
        ));
    }
    let owner = Id::new(args[1].clone())?;
    let root = format!(
        "s3://{}/docspec-qualification/{}/rust-objects",
        env("DOCSPEC_S3_BUCKET")?,
        owner
    );
    let storage = remote_storage(
        &root,
        HashMap::from([
            ("uri".into(), env("DOCSPEC_CATALOG_URI")?),
            ("warehouse".into(), env("DOCSPEC_CATALOG_WAREHOUSE")?),
            ("s3.endpoint".into(), env("DOCSPEC_S3_ENDPOINT")?),
            ("s3.region".into(), "auto".into()),
            ("s3.path-style-access".into(), "true".into()),
        ]),
        HashMap::from([
            ("token".into(), "DOCSPEC_CATALOG_TOKEN".into()),
            ("s3.access-key-id".into(), "DOCSPEC_S3_ACCESS_KEY_ID".into()),
            (
                "s3.secret-access-key".into(),
                "DOCSPEC_S3_SECRET_ACCESS_KEY".into(),
            ),
        ]),
    )
    .await?;
    let budget = Budget::new(Limits {
        compute_threads: 4,
        memory_bytes: 128 << 20,
        scratch_bytes: 64 << 20,
        batch_rows: 16,
        max_batch_bytes: 1 << 20,
        max_metadata_bytes: 1 << 20,
    })?;
    let objects = storage.objects;
    let path = format!("{root}/metadata.json");
    if objects.file_io().new_input(&path)?.exists().await? {
        return Err(Error::refusal(
            "probe_existing_prefix",
            "use a fresh qualification UUID",
        ));
    }
    println!(
        "{}",
        json!({"event": "start", "root": root, "scope": "Rust object primitives only; remote objects retained"})
    );
    let a = br#"{"synthetic":"first"}"#.to_vec();
    let b = br#"{"synthetic":"second"}"#.to_vec();
    let first = object(&owner, path.clone(), &a);
    let second = object(&owner, path, &b);
    let (left, right) = tokio::join!(
        objects.create_bytes(&first, a.clone(), &budget),
        objects.create_bytes(&second, b.clone(), &budget),
    );
    let (winner, bytes, loser) = match (left, right) {
        (Ok(pin), other @ Err(_)) => (pin, a, other),
        (other @ Err(_), Ok(pin)) => (pin, b, other),
        _ => {
            return Err(Error::refusal(
                "probe_conditional_create",
                "expected exactly one Rust writer to succeed",
            ));
        }
    };
    conflict(loser)?;
    conflict(objects.create_bytes(&winner, bytes, &budget).await)?;
    objects.verify(&winner, &budget).await?;
    println!(
        "{}",
        json!({"event": "metadata", "winner": winner, "competing_create_refused": true, "repeated_create_refused": true, "exact_readback": true})
    );

    let temporary = tempfile::tempdir()?;
    let path = format!("{root}/multipart.bin");
    let first_local = temporary.path().join("first.bin");
    let second_local = temporary.path().join("second.bin");
    let first = fixture(&owner, &first_local, path.clone(), b'A')?;
    let second = fixture(&owner, &second_local, path, b'B')?;
    // Log expected identity before effects, preserving reconciliation information
    // if the provider response is unknown. No transport loss is injected here.
    println!("{}", json!({"event": "multipart_start", "expected": first}));
    objects.upload_file(&first, first_local, &budget).await?;
    conflict(objects.upload_file(&second, second_local, &budget).await)?;
    objects.verify(&first, &budget).await?;
    println!(
        "{}",
        json!({"event": "complete", "passed": true, "multipart": first, "competing_create_refused": true, "exact_readback": true})
    );
    Ok(())
}

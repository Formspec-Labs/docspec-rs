# R2 catalog foundation

The optional `remote` feature connects the pinned maintained Apache Iceberg REST
catalog to its OpenDAL S3 storage implementation. It adds no signer, Parquet
decoder, catalog database, or remote engine implementation. Default local builds
do not enable these optional dependencies. The S3 storage crate enables only
`opendal-s3`, excluding its default filesystem and memory backends.

`storage::remote::remote_catalog` receives explicit native properties and a map
from native property names to environment-variable names. It resolves secrets
at startup, suppresses ambient S3 credential discovery, and forces REST header
redaction. Its private delegating catalog supplies redacted `Debug` because
upstream `RestCatalog` derives `Debug` over configuration properties. The wrapper
changes no catalog behavior. Configuration is neither serialized nor logged.
Construction is lazy: success does not prove connectivity or native table I/O.

The opt-in `r2_catalog_probe` example requires a new canonical UUID dataset:
`docspec-rs-qualification-UUID`. It refuses an existing derived namespace and
reports its exact table reference before writing. It uses the existing public
`Lifecycle::open`, `commit`, `record`, and `records` APIs. It retains all created
synthetic objects, including candidates from rejected commits. It never drops
tables, purges objects, changes existing tables or bucket policy, or enables
maintenance.

The probe checks these boundaries in order:

1. Commit one small lifecycle row through the maintained native writer.
2. Inject a competing lifecycle commit after the captured authority check and
   immediately before the original REST update. Require the REST adapter's
   `CatalogCommitConflicts`, which represents server HTTP 409, and check that
   only the competing row became selected. This exercises the actual native
   expected-snapshot requirement rather than an application-only stale check.
3. Commit another row remotely, then discard its successful response through
   the probe-only catalog wrapper. Recreate the REST client and lifecycle cache
   and recover the exact retained row identity without repeating the write.
4. Compare every selected lifecycle key/value and the expected revision through
   the fresh native reader. Emit non-secret table, metadata, snapshot, and row
   references.

Response loss is injected after a successful remote write. It is not a measured
network failure, process death, or server-side uncertain outcome. The complete
remote engine, publication, cleanup, failover, and performance remain unqualified
even if this probe passes.

## Execution

The parent task owns module registration, lockfile resolution, formatting,
builds, tests, and remote execution. This implementation task made no cloud
calls and ran no Cargo commands. The parent must retain build and runtime
evidence; the source alone is not a passing qualification.

From `docspec-rs/`, after resolving the lockfile:

```sh
cargo run --locked --features remote --example r2_catalog_probe -- \
  --allow-synthetic-writes "docspec-rs-qualification-$(uuidgen | tr '[:upper:]' '[:lower:]')"
```

Load sibling `spicy-regs/.env` only inside the launching subprocess. Map the
verified provider's credentials to these variables without printing secrets or
placing their values in command arguments:

| Probe variable | Meaning |
| --- | --- |
| `DOCSPEC_CATALOG_URI` | `https://catalog.cloudflarestorage.com/174055408ff1560e60601c4d12c561c4/spicy-regs` |
| `DOCSPEC_CATALOG_WAREHOUSE` | `174055408ff1560e60601c4d12c561c4_spicy-regs` |
| `DOCSPEC_S3_ENDPOINT` | `https://174055408ff1560e60601c4d12c561c4.r2.cloudflarestorage.com` |
| `DOCSPEC_CATALOG_TOKEN` | Existing provider's catalog bearer token |
| `DOCSPEC_S3_ACCESS_KEY_ID` | Existing provider's R2 access key ID |
| `DOCSPEC_S3_SECRET_ACCESS_KEY` | Existing provider's R2 secret access key |

The probe sets S3 region `auto` and path-style addressing explicitly. First-time
creation assumes this isolated qualification UUID has a single creator, as the
public lifecycle API documents.

## Remaining remote blockers

`Engine::with_catalog` is an injection seam, not whole-engine qualification.
Source custody and object identity/verification still use local paths and file
metadata. Export materialization and independent delivery, exact-object removal
and reader fencing, and catalog backup/restore use filesystem operations. Each
must use maintained remote I/O where appropriate and qualify the selected
provider's actual authority and failure boundaries. A synthetic lifecycle-table
probe cannot establish these guarantees or full-data resource targets.

Sources: the pinned [REST adapter](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/catalog/rest/src/catalog.rs),
[OpenDAL storage guide](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/storage/opendal/README.md),
and [native transactions](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/transaction/mod.rs).

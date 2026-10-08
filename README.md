# Rust DocSpec

A local Rust implementation of recoverable dataset processing over Iceberg and
Parquet. It admits an exact Parquet input, executes a repeatable SQL query, retains
completed phases, selects an immutable release, and reads that release without
the producing catalog.

This repository implements the local foundation and a recoverable Parquet path
from the [accepted greenfield plan](docs/PLAN.md). **The complete dataset lifecycle and the
10-million-Comments performance target remain unfinished.** See
[implementation status](docs/STATUS.md) for the requirement and qualification map.

## Run locally

The pinned toolchain installs through rustup. Build the binary:

```sh
cargo build --locked
target/debug/docspec --workspace workspaces/comments init
target/debug/docspec hash /absolute/path/comments.parquet
```

Save a request as `comments.json`, using the BLAKE3 digest from `hash`:

```json
{
  "dataset": "comments",
  "operation": "initial",
  "input": "/absolute/path/comments.parquet",
  "expected_digest": "<64-character BLAKE3 digest>",
  "sql": "SELECT * FROM input",
  "definition": "comments-v1"
}
```

```sh
target/debug/docspec --workspace workspaces/comments plan comments.json
target/debug/docspec --workspace workspaces/comments run comments.json > release.json
target/debug/docspec --workspace workspaces/comments inspect comments
target/debug/docspec --workspace workspaces/comments resume comments.json
target/debug/docspec verify release.json
target/debug/docspec read release.json comments.arrow
```

`read` writes an Arrow IPC stream. Its result and `verify` need the retained files,
but do not need the catalog. Files referenced by a release remain local absolute
paths; the release is not yet a portable or public distribution.

Repeat the same operation to recover its original release. Use a new operation ID
for another captured request. Unchanged admitted input and processing reuse their
original files. Changing the query can reuse byte admission. Changing an input
currently rebuilds the complete result. The input path is a locator; its trusted
expected digest determines the selected bytes. Original bytes, request SQL,
processing revision and runtime dependency-lock digest are retained.

For changes against a selected version, save an update request:

```json
{
  "dataset": "comments",
  "operation": "update-001",
  "definition": "comments-v1",
  "base": "initial",
  "keys": ["comment_id"],
  "replacements": {
    "path": "/absolute/path/replacement-comments.parquet",
    "digest": "<64-character BLAKE3 digest>"
  },
  "deletions": {
    "path": "/absolute/path/deleted-comment-keys.parquet",
    "digest": "<64-character BLAKE3 digest>"
  }
}
```

```sh
target/debug/docspec --workspace workspaces/comments plan-update update.json
target/debug/docspec --workspace workspaces/comments update update.json > updated-release.json
```

Replacement rows must contain the complete existing schema. Each supplied key
replaces every old row with that key, allowing duplicate and one-to-many results.
Deletion files contain only the named key columns; omit `deletions` when unused.
Composite keys match as tuples, and null key components match nulls. Overlapping
replacement and deletion keys refuse. A changed selected base refuses the update;
prepare a new operation against the new version instead of retrying a stale decision.
Repeat the same request to resume or recover its already selected release.

Updates retain typed removed and added rows with exact before/after native
metadata, the base release and stable native key field IDs. Unaffected source files
are shared; affected files supply both kept and removed rows in one joined read.
The independently verified release and old versions remain readable without the
catalog. This supports source updates; dependent field groups and general
incremental SQL remain unfinished. Release format 2 is the current greenfield
format; there is no reader migration for earlier experiment outputs.

```sh
target/debug/docspec --workspace workspaces/comments latest comments
target/debug/docspec --workspace workspaces/comments release comments initial
target/debug/docspec --workspace workspaces/comments compare old.json new.json
target/debug/docspec --workspace workspaces/comments invalidate comments "dependency correction"
target/debug/docspec --workspace workspaces/comments serve
```

`compare` performs a full, duplicate-aware value comparison, not a write journal.
`invalidate` fences unfinished work and future processing reuse while retaining
the old selected release. Pass `--definition comments-v2` to activate a replacement
definition explicitly. A new operation is required after invalidation.

The loopback HTTP API exposes `POST /plan`, `POST /run`, `POST /updates/plan`,
`POST /updates/run`, `GET /datasets/{dataset}`,
`GET /datasets/{dataset}/latest`, and `GET /datasets/{dataset}/versions/{version}`.
CLI and HTTP call the same application services. HTTP accepts bounded JSON;
the CLI streams bulk reads. One process owns a workspace; use HTTP to submit
concurrent requests to that process.

Update inspection includes observed file scans, reused files/bytes, newly written
data files/bytes and removed/added row counts. Writes include temporary prepared
keys and change tables; metadata and verification I/O are not included in these
write counters. Distinct affected keys are prepared once using bounded native
writers and DataFusion execution. Discovery still probes every source file, and
verification still reads the complete result and retained inputs. These limits
prevent a claim that update time is independent of dataset size. Disk preflight
uses conservative compressed-file estimates, not an output-size guarantee.

## Small implementation, native storage

One crate contains the model, resource limits, Iceberg catalog adapter, local
storage, application services, CLI and HTTP interface. Apache Iceberg handles
native table commits and Parquet metadata. DataFusion handles relational execution
and spill. Arrow carries batches. The maintained Iceberg SQL catalog uses SQLite
for its own catalog bookkeeping; DocSpec's authority, phase, request, protection
and release records are Iceberg rows. There is no separate application SQL ledger.

Each dataset has one lifecycle table. A guarded native commit selects exact data,
schema, input and release pins. The adapter rejects a changed authority boundary
before Iceberg's transaction retry can refresh stale application decisions.
Data preparation occurs outside the short selection lock. Concurrent retries
adopt an already completed phase or release instead of replacing its generation.
One bounded, disposable cache holds rows from an exact immutable lifecycle snapshot.
Native catalog reads and guarded commits still decide authority. The cache removed
repeated metadata scans after the history probe demonstrated their cost; it adds
no durable index or second source of truth. Warm commits and cold replay enforce
the same history budget; new work refuses before that bound makes acknowledged
records unreadable. History compaction remains required for long-running use.

Owned output directories are protected before writes. Automatic expiration and
metadata deletion are disabled. Everything remains retained; application-level removal, compaction,
cross-dataset sharing and restore are not implemented. A native storage probe
qualifies sharing unchanged Parquet files across candidate snapshots while
preserving their original owner and exact read dependencies. This conservative
local behavior avoids promising a cleanup or recovery mechanism that has not been
qualified.

`--limits limits.json` accepts a partial resource profile. Defaults come from the
proposal: 12 compute partitions, 24 GiB memory, 32 GiB scratch, bounded batches and
metadata. DataFusion and application buffers share separate portions of the
allowance, and the native spill manager enforces its scratch limit. These are
admission controls, **not a qualified whole-process memory ceiling**. Native reads
use serial file decoding and retain admission through stream consumption. Bulk
copying and hashing use bounded blocking workers; canceling a caller keeps the
reservation until its worker finishes. Decoders can
allocate before a batch-size refusal; Q1 remains open.

SQL is limited to a read-only query over retained `input`. Clock and random
functions, limits, offsets, window functions and order-sensitive aggregates are
refused until their repeatability is qualified. `EXCEPT ALL` and `INTERSECT ALL` are refused because the
pinned engine's implementation does not preserve the required multiplicity. SQL
relations have no promised physical row order. Per-value evidence, source
occurrence identities and broader transformation semantics still need qualification.

## Verification

```sh
./scripts/check
```

[Integration tests](tests/lifecycle.rs) exercise native guarded commits, exact
values and duplicates, null versus empty strings, empty outputs, catalog-free
reads, historical comparisons, same-size corruption, stale dependencies,
nonregressing coverage, competing retries, HTTP behavior and real process kills
after durable handoffs. Fault controls compile only with `test-util`.

[Update tests](tests/updates.rs) compare applied results and change rows with
independent reconstruction. They cover duplicate and null keys, quoted composite
keys, nested values, deletion to an empty table, empty updates, stale-base races,
competing retries, corrupted base files, altered change declarations, HTTP routes,
retained-phase recovery and failure while the paired writers are active.

The [retained-data experiment](experiments/local-reuse/protocol.md) uses a separate
PyArrow reader and preserves raw commands, outputs, timing and memory measurements.
The [history probe](experiments/local-reuse/history-protocol.md) measures the cost
of retaining more lifecycle operations before accepting a metadata layout.
See the [measured results](experiments/local-reuse/REPORT.md).

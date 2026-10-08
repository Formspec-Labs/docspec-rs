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
catalog. An update automatically refreshes every selected scalar field group,
reusing computations for unchanged source files. It selects the new source and
all groups together after independent verification. General incremental SQL
remains unfinished. Release format 10 is the current greenfield
format; there is no reader migration for earlier experiment outputs.

## Change a source schema

Use `plan-schema` and `schema` to rename, reorder or hide existing root fields.
The request lists native field IDs from the selected source's Iceberg schema;
these IDs are independent of column position and computed groups' logical IDs.
For a source with native fields `id` (1) and `text` (2), save `schema.json`:

```json
{
  "dataset": "comments",
  "operation": "rename-001",
  "base": "update-001",
  "definition": "comments-v1",
  "next_definition": "comments-v2",
  "fields": [
    {"id": 2, "name": "body"},
    {"id": 1, "name": "comment_id"}
  ]
}
```

```sh
target/debug/docspec --workspace workspaces/comments plan-schema schema.json
target/debug/docspec --workspace workspaces/comments schema schema.json > renamed-release.json
```

The field list defines the complete new order and must retain at least one field.
Omitted fields stay available in retained historical versions. Removing a field
consumed by a selected computed group refuses. Computed expressions keep their
original input IDs after a rename;
new expressions use the current names. Repeat the same request to resume or recover
its selected result. HTTP exposes `/schemas/plan` and `/schemas/run`.

Finish pending source admission before starting a schema change. Selection
atomically installs the schema and `next_definition`. New writes use
that definition; pending old-definition writes refuse. A changed selected base or
later source admission also refuses cutover and preserves completed work. The
operation shares existing value files, while verification still reads their values.
Optional/default additions, type or nested edits, and schema edits of related
join/summary outputs are not yet supported.

## Add a computed field group

Save a request as `backfill.json`:

```json
{
  "dataset": "comments",
  "operation": "text-stats-001",
  "base": "update-001",
  "definition": "comments-v1",
  "group": "text_stats",
  "group_definition": "text-stats-v1",
  "fields": [
    {"id": 10, "name": "character_count", "expression": "CAST(length(comment) AS BIGINT)"}
  ]
}
```

```sh
target/debug/docspec --workspace workspaces/comments plan-backfill backfill.json
target/debug/docspec --workspace workspaces/comments backfill backfill.json > enriched-release.json
target/debug/docspec --workspace workspaces/comments read-combined enriched-release.json comments.arrow
```

Fields have stable logical IDs within their group and explicit mappings to native
Iceberg field IDs. Expressions use source columns and immutable scalar functions.
Aggregates, windows, subqueries and nested computed outputs refuse until qualified.
Each source occurrence receives one output row, including successful null or empty
values. Empty sources retain the declared schema. A failed file prevents selection
of the group; inspection distinguishes it from pending work and successful nulls.

Backfill preserves the source files and retains completed file computations,
validation and independent readback. Retrying the request resumes those handoffs.
The plan reports completed and pending files, failures and any reason selection
would refuse. If ingestion advances before selection, a new request against the
new source can reuse unchanged file computations. Independent groups merge into
the latest selection; competing replacements of one group reject a stale target.
Combined reads join group values to exact source-file row positions.

The same `update` request works after group selection; `plan-update` includes group
preparation. A no-op update preserves the exact source and group table pins while
recording its new input coverage and empty source changes. Metadata-only schema
changes, computed input groups and logical row identity across compaction remain
unfinished. Full independent
release verification still reads retained inputs and data; file reuse does not
establish population-independent backfill latency.

```sh
target/debug/docspec --workspace workspaces/comments latest comments
target/debug/docspec --workspace workspaces/comments release comments initial
target/debug/docspec --workspace workspaces/comments compare old.json new.json
target/debug/docspec --workspace workspaces/comments check-succession old.json new.json
target/debug/docspec --workspace workspaces/comments invalidate comments "dependency correction"
target/debug/docspec --workspace workspaces/comments serve
```

`compare` performs a full, duplicate-aware value comparison, not a write journal.
`invalidate` fences unfinished work and future processing reuse while retaining
the old selected release. Pass `--definition comments-v2` to activate a replacement
definition explicitly. A new operation is required after invalidation. Incremental
updates refuse an invalidated source; a full rebuild requalifies its source query
and every selected scalar group. Historical releases remain readable.

To rebuild an enriched selection, use the normal run request with `"base"` set to
the exact selected version, a new `"operation"`, and the complete replacement
input/query. Keep the active definition, or activate a changed definition first.
The engine preserves every selected group definition and computes it over the
new source. Missing fields or incompatible expressions leave the prior release
selected. The replacement input supplies this version's evidence; the exact parent
manifest retains its relationship to history.

`plan`, `run`, `resume` and the corresponding HTTP routes handle rebuilds directly.
A changed selected base or a later source admission fences a rebuild's cutover;
submit a new request against the current selection. Completed source and group
work remains retained. Rebase with the same input, query, definition and eligibility
reuses qualified source work. Changing dependency eligibility recomputes it.
Builds without an explicit base still refuse to replace selected computed groups.

`check-succession` proves that the target follows the prior version on the observed
accepted history. Each selection records its actual predecessor in the same native
commit as the result and selected head. The check uses exact manifest identities
and retained lifecycle records; optional change files and payload reads are not
needed. It reports accepted selections advanced and behind the observed head.
These counts describe selection history, not elapsed time or upstream acquisition
completeness. Missing, malformed, backward or orphaned history refuses. The check
does not establish payload availability; use `verify` for that.

## Qualify related input generations

A producer can add a relationship declaration to a run request:

```json
"relationship": {"kind": "coupled", "family": "regulations", "generation": "g1"}
```

An omitted declaration inherits the exact `base`, or makes a new source
independent. Rebuilding a coupled source or changing a selected relationship
requires that selected base. Updates and backfills inherit the relationship.
An explicit `{"kind": "independent"}` with a base removes the requirement for
that new producer version; consumers cannot override another version's rule.

After producing all related versions, save a seal request with `family`,
`generation`, and `members`. Each member key is its dataset ID; its value contains
`dataset`, `version`, and the exact `manifest` object from the release pin.
The first seal fixes the family's dataset membership. Later generations use the
same members; an intentional membership change requires a new family ID.

```sh
target/debug/docspec --workspace workspaces/comments seal-generation generation.json
target/debug/docspec --workspace workspaces/comments capture-inputs inputs.json > coverage.json
target/debug/docspec verify-inputs coverage.json
```

`inputs.json` is a dataset-to-version map with the same shape as `members`.
Capture checks accepted history, current producer eligibility, and exact sealed
membership. A current Comments version requiring Dockets D2 refuses Dockets D1,
even when D1's older declaration was independent. Consumers may read a subset of
a family. Every member they do consume must match the seal.

Seals are immutable: an identical retry returns the original; different members
under the same generation refuse. Backfill creates a new version, so it cannot
silently replace an already sealed member. Use a based rebuild to declare a new
generation and seal its final versions. Qualified unchanged source and scalar
files remain reusable across that association change.

Capture and `verify-inputs` read bounded manifests, not data payloads. The latter
checks retained relationships without a catalog; it does not establish current
eligibility, payload availability or who issued a supplied coverage record.
These captured rules also govern native join refresh and its final selection.

## Refresh a native join

A `RefreshSpec` uses `transform.kind: "join"` and exact `transform.left` and
`transform.right` version references, using the same
`dataset`, `version`, `manifest` shape as generation members. Set `dataset` to the
output dataset, `operation` to a stable retry identity, `definition` to the active
recipe version, and `base` to the selected output version. `base: null` requires
an absent output. The `transform.join` portion declares every key and output field:

```json
{
  "keys": [{"left": "docket_id", "right": "docket_id"}],
  "kind": "left",
  "nulls": "never",
  "cardinality": "many_to_one",
  "fields": [
    {"side": "left", "field": "comment", "name": "comment"},
    {"side": "right", "field": "title", "name": "docket_title"}
  ]
}
```

```sh
target/debug/docspec --workspace workspaces/comments plan-refresh refresh.json
target/debug/docspec --workspace workspaces/comments refresh refresh.json > joined-release.json
target/debug/docspec verify joined-release.json
target/debug/docspec read-evidence joined-release.json contributors.arrow
```

Supported joins are `inner`, `left`, and `full`, with `never` or `equal` null-key
matching. Keys require identical scalar types. Projection copies declared source
fields without coercion; reading computed input groups remains unfinished. Cardinality is `many_to_many`, `many_to_one`, or `one_to_one`.
Matching duplicate keys that violate the declared partner limit refuse before
writing the joined output. Unmatched duplicates remain valid occurrences.

Processing retains exact left/right file and row positions alongside the values.
A separate evidence table retains contributor pairs and unmatched sides. Ordinary
combined reads hide the position fields; native source reads retain them. These
positions bind to the exact input pins, not stable logical IDs across rewrites.
Independent verification reconstructs values and pair multiplicities from those
inputs, traversing shared semantic dependencies once per verification request.

`policy: "captured"` accepts complete forward input coverage even if newer inputs
are selected during processing. `policy: "latest"` additionally requires each
immediate input to remain selected at this operation's final commit. Inspect
`input-progress/<operation>` for observed heads, revisions and selections behind.
Latest is an operation policy; it is not a continuing freshness promise for later
backfills. Every new selection checks current producer eligibility throughout its
semantic dependency graph. Historical offline reads check retained semantics,
without claiming current eligibility or accepted catalog history.

A guarded commit selects the complete output and progress together. Backward
coverage, role changes, stale output bases and invalidated dependencies refuse.
Admission, matching, evidence, validation and readback have separate recovery
checkpoints. Retry `refresh` with the same request; a completed operation reconciles
its original result even after later source movement. Scalar backfills preserve
related bindings, and based refresh preserves selected scalar group definitions.
Use refresh to change a related output; raw build/update replacement refuses.

Refresh defaults to `"mode": "full"`. Set `"mode": "incremental"` to replace
complete match groups affected by old and new keys in changed native input files.
This includes newly possible matches, removed partners, duplicate fanout and
partial-null keys. Rewritten input files have new occurrence identities, so their
complete key groups are recomputed. Source and evidence files outside the affected
scope remain shared; identical input files under new versions adopt the exact
prior output and evidence pins while advancing coverage.

Incremental mode requires a selected related base with unchanged join definition,
input roles, native schemas and output eligibility. Unsupported changes return
`full_refresh_required`; submit an explicit full refresh. Reusable incremental
phases bind the exact base manifest. Applied changes use contributor tuple field
IDs and retain actual removed/added rows, including remove-and-add of the same
unchanged pair when its group is recomputed. Independent verification checks
removed-row membership and reconstructs the new multiset from the old one.

Computed input groups, general evidence-only changes and
complete-path performance remain open. Deliberate many-to-many fanout can be large;
there is no output-size upper bound or large-workload performance claim.

## Summarize comments or joined results

Use the same `RefreshSpec` and `refresh` command with `transform.kind: "aggregate"`.
Set `transform.input` to one exact version reference and `transform.grouping` to:

```json
{
  "keys": ["docket_id"],
  "aggregates": [
    {"name": "comment_rows", "function": {"kind": "count_rows"}},
    {"name": "known_lengths", "function": {"kind": "count", "field": "length"}},
    {"name": "total_length", "function": {"kind": "sum", "field": "length"}}
  ]
}
```

The input is its native source group, which may itself be a joined result.
`count_rows` counts occurrences: join fanout can make this differ from distinct
comments. `count` counts non-null values. `sum`, `min` and `max` accept signed
Int32/Int64 fields. Sums return Decimal128(38,0), with operands widened before
addition; verified input rows must not exceed `i64::MAX`. No floating-point
aggregation is qualified. Keys support Boolean, Int32, Int64 and Utf8, with nulls
forming one group. Field names and aliases must be distinct and outside the
reserved `__docspec_` prefix.

An empty key list produces one global result, including over empty input: counts
are zero and sum/min/max are null. An empty grouped input produces no rows.
Contributor evidence retains typed group keys and exact input file/row positions;
it contains no rows for empty input. For joined inputs, those positions identify
joined rows whose retained tuples identify the original contributors.

`mode: "incremental"` replaces complete groups reached by old/new keys in changed
native files. It handles moved keys, deleted final members and equal aggregate
values with different contributors. Any file change affects a global summary.
Unchanged native inputs adopt exact source/evidence pins while advancing coverage.
Use a full refresh to change the recipe or native input schema. Both modes use
the same recovery, eligibility, captured/latest selection and scalar-group refresh.
Combined reads hide the global key; native reads expose it for evidence matching.

Discovery, contributor processing and independent verification still scan retained
relations. File sharing reduces demonstrated rewrites; complete-path speed and
10M Comments performance remain unmeasured.

## HTTP access

The loopback HTTP API exposes `POST /plan`, `POST /run`, `POST /updates/plan`,
`POST /updates/run`, `POST /backfills/plan`, `POST /backfills/run`, `POST /succession`, `GET /datasets/{dataset}`,
`GET /datasets/{dataset}/latest`, and `GET /datasets/{dataset}/versions/{version}`.
`POST /generations/seal`, `POST /inputs/capture`, `POST /refreshes/plan`, and
`POST /refreshes/run` accept the same metadata as the corresponding CLI commands.
CLI and HTTP call the same application services. HTTP accepts bounded JSON and
streams Arrow IPC for bulk reads. One process owns a workspace; use HTTP to submit
concurrent requests to that process.

With `serve` running, read an exact version through either route:

```sh
curl --fail -o comments.arrow http://127.0.0.1:7391/datasets/comments/versions/initial/arrow/source
curl --fail -o enriched.arrow http://127.0.0.1:7391/datasets/comments/versions/text-stats-001/arrow
```

The `/arrow/{group}` route reads a named physical field group; `/arrow` combines
source fields with all selected scalar groups. Related results also expose their
contributor pairs at `/datasets/{dataset}/versions/{version}/evidence`. Responses identify the dataset,
version and manifest digest in `docspec-*` headers. Each read verifies the complete
retained release before sending its schema, so first-byte latency still depends
on release size. Batches then follow client demand without a background producer
or whole-result buffer. Missing versions/groups and initial verification failures
return structured JSON errors. A later failure aborts the response: clients must
require successful HTTP completion and discard partial output. Empty results keep
their schema. This local codec currently refuses Arrow dictionary array schemas.

Update inspection includes observed file scans, reused files/bytes, newly written
data files/bytes and removed/added row counts. Writes include temporary prepared
keys and change tables; metadata and verification I/O are not included in these
write counters. Incremental join counters also omit scoped input scans, tuple
discovery and cardinality preflights; they are partial writer/discovery counters,
not total file decodes or complete-path I/O. Distinct affected keys are prepared once using bounded native
writers and DataFusion execution. Discovery still probes every source file, and
verification still reads the complete result and retained inputs. These limits
prevent a claim that update time is independent of dataset size. Disk preflight
uses compressed-file estimates where available. Join fanout has no input-byte
upper bound: it preflights affected rewrites and scratch, then checks free space
as replacement batches reach the admitted writer. Native flushes and concurrent
disk users can still exhaust space; completed phases remain retained.

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
An accepted-history check retains that loaded immutable state and its memory
reservation throughout traversal, so unrelated dataset activity cannot trigger
repeated scans. It yields cooperatively while walking the bounded history.

Full comparisons connect DataFusion's maintained `StreamingTable` to the same
native Iceberg reader used for independent verification. This small adapter
preserves reader admission through completion or cancellation and enforces the
configured batch size. DataFusion applies query filters and column selection above
that stream; this path deliberately makes no source-pushdown performance claim.
Native capacity refusals keep their structured codes through DataFusion errors.

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
allowance. All contexts cloned from a Budget share one execution pool and native
spill manager, including catalog-free join verification. The pinned DataFusion
spill manager checks disk usage after writes. Its quota-error accounting can
remain charged after files are deleted; reopen the workspace/runtime before retrying
a spill-quota failure. The capability probe is retained in `src/budget.rs`. These are
admission controls, **not a qualified whole-process memory ceiling**. Native reads
use serial file decoding and retain admission through stream consumption. Bulk
copying and hashing use bounded blocking workers; canceling a caller keeps the
reservation until its worker finishes. Decoders can
allocate before a batch-size refusal; Q1 remains open.

SQL is limited to a read-only query over retained `input`. Clock and random
functions, limits, offsets, window functions and order-sensitive aggregates are
refused until their repeatability is qualified. `EXCEPT ALL` and `INTERSECT ALL` are refused because the
pinned engine's implementation does not preserve the required multiplicity. SQL
relations have no promised physical row order. Source-file row positions are
verified; durable logical identities, per-value evidence and broader transformation
semantics still need qualification.

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

[Backfill tests](tests/backfill.rs) check independent value reconstruction,
duplicate and null occurrences, empty schemas, interrupted handoffs, unchanged-file
reuse, concurrent group selection, stale source and target refusal, malformed
coverage, missing cached objects, failure versus successful nulls, combined-reader
capacity and HTTP behavior.

[Refresh tests](tests/refresh.rs) check complete source/group selection, every
computed column, unchanged-file evaluation counts, no-op and delete-all outcomes,
exact inherited siblings, stale definitions and source boundaries, and retained
work after partial group completion. This reuses the scalar preparation path;
there is no separate refresh store or scheduler.

Build, update and backfill recovery tests also terminate real CLI workers after
acknowledged handoffs and resume in fresh processes without the external inputs.
Test-only work-entry observations assert that only missing phases execute.
These cases cover the named completed boundaries; machine power loss and arbitrary
mid-write failures remain unqualified.

The [retained-data experiment](experiments/local-reuse/protocol.md) uses a separate
PyArrow reader and preserves raw commands, outputs, timing and memory measurements.
The [history probe](experiments/local-reuse/history-protocol.md) measures the cost
of retaining more lifecycle operations before accepting a metadata layout.
See the [measured results](experiments/local-reuse/REPORT.md).

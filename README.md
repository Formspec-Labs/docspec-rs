# Rust DocSpec

A local Rust implementation of recoverable dataset processing over Iceberg and
Parquet. It admits an exact Parquet input, executes a repeatable SQL query, retains
completed phases, selects an immutable release, and reads that release without
the producing catalog.

This repository implements the local foundation and a recoverable Parquet path
from the [accepted greenfield plan](docs/PLAN.md). **The complete dataset lifecycle
remains unfinished; the defined full local 10-million-Comments gate passes.** See
[implementation status](docs/STATUS.md) for the requirement and qualification map.

## Local exports, compaction and restore

`export export.json` creates an independently readable Parquet file containing
only the explicitly selected columns and all active rows of one retained group.
The request uses [ExportSpec](src/export.rs): an exact source `VersionRef`, a stable
operation ID, a channel, its expected previous export ID, and an explicit policy.
The policy names the group, sets `rows` to `"all"`, and lists columns in output
order. Row restrictions must already be applied upstream. The caller supplies
this authorization; the loopback HTTP service does not implement user accounts.
Use `latest-export DATASET CHANNEL` or `export-pin DATASET OPERATION` to resolve
the immutable file. Repeating an operation reuses completed work. Remote upload
and public hosting remain unfinished.

`compact compaction.json` accepts [CompactionSpec](src/model.rs): dataset,
operation, version, group, exact file paths (an empty list selects all active
files), and `target_file_bytes`. It preserves the logical version and selects an
equivalent physical representation for managed reads. Source occurrence IDs
survive rewrites so retained computed groups remain readable. Target file size
is a preference; metadata limits may close a file sooner. Old exact pins remain
retained. Source updates and incremental joins/summaries capture and reuse eligible
compacted output files, including on resume; later compaction cannot change an
admitted write. Compaction itself keeps old storage. Explicit recovery retirement
below can make obsolete intermediate files eligible for the existing removal
service while preserving selected layouts and independently retained pins.

Create a local restore point before maintenance, then restore it only after
closing the engine and all reader streams:

```sh
target/debug/docspec --workspace workspaces/comments create-restore-point comments before-maintenance > restore-point.json
target/debug/docspec --workspace workspaces/comments restore-local restore-point.json
```

Restore points retain native catalog state and its required immutable objects in
the same canonical Unix workspace. They protect against later unlink; hard links
are not a second physical copy against disk failure or external modification.
Restoring selects a fresh catalog file after fencing old local actors and checking
backup identities. `--allow-unregistered-import` is an explicit catalog-loss
procedure for a completed artifact; known retired points remain refused.
Use `plan-restore-point-retirement` and `retire-restore-points` to end selected
restore promises. Retirement and physical deletion remain separate operations.

## Schedule operations with Dagster

The [Dagster adapter](adapters/dagster/README.md) submits bounded native requests
to one local Rust HTTP host. It preserves operation IDs across uncertain retries
and leaves lifecycle decisions in Rust. Local tests cover completion-response
loss, Python worker death and cold Rust-host recovery after retained processing.
Wider route and provider qualification remain unfinished.

## Qualify remote storage

The optional `remote` feature supplies maintained Iceberg REST and S3 adapters.
The [synthetic probe](examples/r2_catalog_probe.rs) checks native commit conflicts
and lost-response readback in a fresh `docspec-rs-qualification-<UUID>` namespace.
It requires `--allow-synthetic-writes`, retains its test objects and does not
change existing tables or maintenance policies. Configuration uses the
`DOCSPEC_CATALOG_URI`, `DOCSPEC_CATALOG_WAREHOUSE`, `DOCSPEC_S3_ENDPOINT`,
`DOCSPEC_CATALOG_TOKEN`, `DOCSPEC_S3_ACCESS_KEY_ID` and
`DOCSPEC_S3_SECRET_ACCESS_KEY` environment variables. Keep secrets out of arguments.

```sh
cargo run --locked --features remote --example r2_catalog_probe -- --allow-synthetic-writes "docspec-rs-qualification-<UUID>"
```

The [object probe](examples/r2_objects_probe.rs) uses the same configuration plus
`DOCSPEC_S3_BUCKET`. It checks create-only metadata and multipart uploads, then
streams one exact readback of each winner. Use a fresh UUID; it retains synthetic
objects and possible unfinished multipart parts under its isolated prefix.

```sh
cargo run --locked --features remote --example r2_objects_probe -- --allow-synthetic-writes "docspec-rs-qualification-<UUID>"
```

The [candidate probe](examples/r2_candidate_probe.rs) exercises the shared
local/remote candidate serializer and conditional remote writer. It checks empty
native metadata with preserved field IDs, followed by fresh catalog and
catalog-free readback. Basin requires its metadata beneath `__r2_data_catalog`;
the probe writes only to a fresh UUID child there and retains the result.

```sh
cargo run --locked --features remote --example r2_candidate_probe -- --allow-synthetic-writes "docspec-rs-qualification-<UUID>"
```

The Rust library can open a local execution host with the same explicitly resolved
catalog and object configuration. Initial independent Build, recovery, selection and selected
source-group streaming are qualified on the configured R2/Basin path. Remote
Build supports a dataset's first selection and recovery of that same operation;
a new Build after selection refuses before source or lifecycle mutation. Other remote
mutations refuse before lifecycle effects; remote export, maintenance, computed
and related groups remain unqualified. CLI and loopback HTTP startup accept the
same explicit remote configuration. See [current qualification](docs/STATUS.md).

```rust,ignore
use docspec::{budget::Limits, engine::Engine, storage::remote::remote_storage};
use std::collections::HashMap;

let remote = remote_storage(
    "s3://my-bucket/__r2_data_catalog/docspec/my-project",
    HashMap::from([
        ("uri".into(), "https://catalog.example/v1".into()),
        ("warehouse".into(), "my-warehouse".into()),
        ("s3.endpoint".into(), "https://account.r2.cloudflarestorage.com".into()),
        ("s3.region".into(), "auto".into()),
        ("s3.path-style-access".into(), "true".into()),
    ]),
    HashMap::from([
        ("token".into(), "DOCSPEC_CATALOG_TOKEN".into()),
        ("s3.access-key-id".into(), "DOCSPEC_S3_ACCESS_KEY_ID".into()),
        ("s3.secret-access-key".into(), "DOCSPEC_S3_SECRET_ACCESS_KEY".into()),
    ]),
).await?;
let engine = Engine::with_remote("workspaces/my-project", remote, Limits::default()).await?;
// engine.run(&initial_build).await?; // RunSpec::base must be None.
// Resolve engine.latest(&dataset), then stream engine.read_group(&release, "source").
engine.shutdown().await?;
```

## Start CLI or loopback HTTP with remote storage

Build with `--features remote`, then pass `--remote remote.json`. The JSON file
contains the owned object prefix, explicit native properties and secret environment
references. There is no ambient configuration discovery. For example:

```json
{
  "owned_prefix": "s3://my-bucket/__r2_data_catalog/docspec/my-project",
  "properties": {
    "uri": "https://catalog.example/v1",
    "warehouse": "my-warehouse",
    "s3.endpoint": "https://account.r2.cloudflarestorage.com",
    "s3.region": "auto",
    "s3.path-style-access": "true"
  },
  "secret_env": {
    "token": "DOCSPEC_CATALOG_TOKEN",
    "s3.access-key-id": "DOCSPEC_S3_ACCESS_KEY_ID",
    "s3.secret-access-key": "DOCSPEC_S3_SECRET_ACCESS_KEY"
  }
}
```

Keep secret values in the referenced environment variables. `--workspace` still
names the local execution host, custody and scratch directory; retained objects
use `owned_prefix`, which must fit the catalog's storage profile.

```sh
target/debug/docspec --workspace workspaces/my-project --remote remote.json run initial.json
target/debug/docspec --workspace workspaces/my-project --remote remote.json latest my-dataset > release.json
target/debug/docspec --workspace workspaces/my-project --remote remote.json read release.json source.arrow
target/debug/docspec --workspace workspaces/my-project --remote remote.json serve
```

Remote `read` uses the configured Engine and its exact retained source pin. The
local default keeps the independent reader. Remote startup supports a dataset's
first independent Build and recovery of that same operation, inspect/latest/release
and source streaming. Other Engine operations retain their existing refusal gates.
Standalone local verification, input hashing, restore and independent evidence or
receipt readers refuse `--remote` before effects. Hash a producer file without
`--remote` when preparing its Build request.

`serve` injects the same configured Engine into the existing HTTP routes and still
requires a loopback bind. This adds backend startup configuration, not hosted
network access, user authentication or broader operation qualification.

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

Normal reads resolve the immutable release and stream only the requested data.
They do not revalidate retained inputs, other field groups or dependency history.
Creation checks new output shape, field IDs/types, sizes, footer row counts and a
small decoded sample once. Readback confirms the serialized selection. Reuse
trusts the retained completion; final selection still checks current authority.

Use `verify release.json` when checking whether retained data is usable. It reports
the check scope and samples at most three files with up to eight rows each across
the dependency graph, with the configured byte allowance per batch (at most three
batch allowances in total). Footer counts
come from metadata, not a full independent row count. The sample is deterministic,
not a statistical guarantee. Investigate suspected corruption or incorrect derived
values with `verify release.json --deep`; that explicitly hashes payloads and
reconstructs derived values and evidence. The library's `verify_release` remains
an explicit deep diagnostic for qualification callers; `verify_release_at` selects
routine or deep scope. Neither diagnostic is a read prerequisite.

Repeat the same operation to recover its original release. Use a new operation ID
for another captured request. Unchanged admitted input and processing reuse their
original files. Changing the query can reuse byte admission. Changing an input
currently rebuilds the complete result. The input path is a locator; its trusted
expected digest determines the selected bytes. Original bytes, request SQL,
processing revision and runtime dependency-lock digest are retained.

New completed phases also retain the exact input copies and schema interpretation
they consumed. Reuse keeps those references even if a later admission stores an
equivalent copy elsewhere. This records small metadata references during existing
work, without another payload read. Older completions remain resumable; missing
dependency information does not authorize cleanup. Explicit local removal requires
qualified identities and fresh protection checks; broader maintenance remains unfinished.

When investigating retained disk use, `inspect-retention objects.json` accepts a
JSON array of exact object references already present in release pins. It reports
one protecting record per object, observed catalog boundaries and deduplicated
logical byte totals. The library method `Engine::inspect_retention` and HTTP
`POST /retention/inspect` provide the same inspection. Complete retained pins
supply their existing file inventories; other dependencies require only bounded
metadata reads. Use the known references without hashing the payloads again.

This inspection currently supports the owned local catalog. Unknown dependencies
refuse inspection, including older completion records that did not capture them.
An unmatched object is not permission to delete it: concurrent work, catalog
recovery and worker ownership still require the separate retention execution
boundary. Inspection changes no availability promise and removes no files.

To retire specific completed recovery phases, save their exact completion keys
from `inspect` as a JSON array in `phase-keys.json`, then capture and apply a plan:

```sh
target/debug/docspec --workspace workspaces/comments plan-phase-retirement comments retired-preparation phase-keys.json "preparation recovery no longer needed" > phase-retirement.json
target/debug/docspec --workspace workspaces/comments retire-phases phase-retirement.json
```

HTTP uses `POST /retention/phases/plan` with JSON fields `dataset`, `retirement`,
`keys` and `reason`; `POST /retention/phases/retire` accepts the returned plan.
These call `Engine::plan_phase_retirement` and `Engine::retire_phases`. Execution
retires only the captured completion revisions, and identical retries return the
same durable decision. A new completion at the same key remains independently
reusable. Retirement changes recovery promises: it preserves files, attempt
protections, issued readbacks and selected releases. It does not delete objects
or establish reclaimable space.

To end a generation seal's standalone availability, capture its exact generation
and apply that plan. This preserves its identity and family membership:

```sh
target/debug/docspec --workspace workspaces/comments plan-generation-retirement regulations end-g1 g1 "generation no longer offered for new capture" > generation-retirement.json
target/debug/docspec --workspace workspaces/comments retire-generation generation-retirement.json
```

HTTP uses `POST /retention/generations/plan` with `family`, `retirement`,
`generation` and `reason`; `/retention/generations/retire` accepts the plan.
These call `Engine::plan_generation_retirement` and `Engine::retire_generation`.
New capture and resealing return `generation_retired`. Previously admitted
consumers may finish, and their retained coverage still protects required member
bytes. Unused owner grants require separate retirement; exact removal remains a
separate operation. This local maintenance service does not delete seal records
or change their immutable member identities.

To end unused owner grants, save their exact `owner-protection/` keys from
`inspect` as a JSON array in `grant-keys.json`, then capture a plan in the owning
dataset:

```sh
target/debug/docspec --workspace workspaces/comments plan-grant-retirement comments retired-grant grant-keys.json "consumer no longer needs this grant" > grant-retirement.json
target/debug/docspec --workspace workspaces/comments retire-grants grant-retirement.json
```

The planner accepts the owner, retirement ID, keys file and reason.
HTTP uses `POST /retention/grants/plan` with `dataset` (the owner), `retirement`,
`keys` and `reason`; `POST /retention/grants/retire` accepts the returned plan.
These call `Engine::plan_grant_retirement` and `Engine::retire_grants` with the
shared `RetentionPlan` and its `owner_grants` scope. Planning and execution refuse
with `grant_in_use` (HTTP 409) while a live consumer binding references an exact
grant generation. Fresh planning of a retired generation returns `grant_retired`
(HTTP 409). Identical execution retries return the retained decision, including
after reopening the workspace. Grant retirement removes no physical files and
does not establish reclaimable space.

To stop offering an old result through catalog lookup, retire its standalone
availability together with every saved readback for that operation:

```sh
target/debug/docspec --workspace workspaces/comments plan-availability-retirement comments retired-version old-operation "historical lookup no longer needed" > availability-retirement.json
target/debug/docspec --workspace workspaces/comments retire-availability availability-retirement.json
```

HTTP uses `POST /retention/availability/plan` with `dataset`, `retirement`,
`operation` and `reason`, then `POST /retention/availability/retire` with the
returned plan. The library methods are `Engine::plan_availability_retirement`
and `Engine::retire_availability`. The current selection cannot retire. An
unfinished operation must first be abandoned. Execution checks that the complete
saved result/readback set still matches the plan; it never expands a stale plan.

Historical lookup, replay under that operation ID and fresh dependency admission
then return `availability_retired` (HTTP 410). Already admitted consumers retain
their inputs and can resume; succession receipts and completed phases remain.
Existing immutable pins still refer to the same objects. This is logical
availability retirement, not access revocation or permission to delete bytes.
All retirement services use one scoped `RetentionPlan` and durable decision.

To end recovery for a completed or explicitly abandoned operation, capture and
apply its recovery retirement plan:

```sh
target/debug/docspec --workspace workspaces/comments plan-operation-retirement comments retired-recovery old-operation "operation recovery no longer needed" > operation-retirement.json
target/debug/docspec --workspace workspaces/comments retire-operation operation-retirement.json
```

HTTP uses `POST /retention/operations/plan` with `dataset`, `retirement`,
`operation` and `reason`, then `POST /retention/operations/retire` with the
returned plan. These call `Engine::plan_operation_retirement` and
`Engine::retire_operation`. An unfinished active operation refuses with
`operation_active` (HTTP 409). Execution retires the captured operation, request,
dependency and backfill-capture recovery records and owned attempt protections.
Completed phases and available results remain independently retained and readable;
retiring operation recovery does not delete objects or establish reclaimable space.
Identical execution retries return the original durable decision, including after
reopening the workspace. Fresh operation-retirement planning returns
`recovery_retired` (HTTP 409). Available completed results still support lookup
and run/plan retries. Abandoned operations still refuse with `operation_abandoned`;
retirement ends their recovery protections without changing their terminal state.

To end retry/recovery for a compaction, including an interrupted rewrite, capture
and apply its exact recovery plan:

```sh
target/debug/docspec --workspace workspaces/comments plan-compaction-retirement comments retired-layout-recovery old-compaction "rewrite recovery no longer needed" > compaction-retirement.json
target/debug/docspec --workspace workspaces/comments retire-compaction compaction-retirement.json
```

HTTP uses `POST /retention/compactions/plan` with `dataset`, `retirement`,
`operation` and `reason`, then `POST /retention/compactions/retire` with the
returned plan. These call `Engine::plan_compaction_retirement` and
`Engine::retire_compaction`. Execution checks the exact request, completed
rewrite/verification/result records and owned compaction attempts. New work since
preview requires a fresh plan. Identical execution retries return the same
decision; reusing the compaction ID returns `compaction_retired` (HTTP 409).

The selected equivalent layout remains readable. For successive rewrites A then
B, A's exclusive files can become removable after both recovery promises end:
B's recovery still needs A until then. Retained phases, readers, original pins,
shared files and owner grants remain independent protections. Retirement keeps
metadata and bytes; use the existing removal preview and execution for eligible
objects, and explicit owner-grant retirement where needed. It does not retire
the selected `representation/` authority or automatically expire history.

Catalog work keeps running after a caller disconnects or drops its future. The
owned task retains workspace ownership, shared maintenance admission and its
request/task memory reservation until the existing operation finishes. Unrelated
datasets can still run concurrently; exhausted admission refuses more work.
Use the same operation ID to retrieve or resume its retained result.

Embedded callers must await `Engine::shutdown()` before stopping their Tokio
runtime, then drop all engine clones to release the workspace. Shutdown stops new
catalog work and drains admitted operations. The HTTP server does this after its
graceful stop. If a private task panics or the runtime stops before private work
finishes, that host stays unavailable until process exit because underlying
blocking work may still finish.
Engine group, evidence and combined reads resolve this host's exact available
result or completed readback and hold shared maintenance admission until the
stream reaches EOF or is dropped. Comparison holds the same admission through
completion. Shutdown waits for these readers; consume or drop streams before
awaiting shutdown. New Engine
reads of a retired result refuse with `availability_retired`.

Standalone `read`, `read-evidence` and `verify` remain catalog-free. Their callers
must keep the files retained for the operation or rely on an applicable published
availability window. A saved JSON pin identifies exact content; it creates no
lease and does not extend a retired availability promise. Logical retirement
preserves the physical files, so independent reads can still use those files.
Library callers can also use the shared catalog-free `engine::read_combined`
function with a `Budget`; the same availability responsibility applies.
Explicit local removal and restore-point capture use the same maintenance admission.
Automatic expiration remains disabled.

To reclaim explicitly retired local objects, save their exact `ObjectRef` values
as a JSON array and capture a removal plan:

```sh
docspec --workspace ./workspace plan-removal --dataset comments \
  --removal cleanup-1 --objects retired-objects.json --reason 'End old output retention' > removal-plan.json
docspec --workspace ./workspace remove-objects --plan removal-plan.json
docspec --workspace ./workspace inspect-removal --dataset comments --removal cleanup-1
docspec --workspace ./workspace resume-removal --dataset comments --removal cleanup-1
```

Preview reports protections without hashing payloads. Execution removes only
previewed objects that remain eligible. It durably reserves those exact targets
and prevents new references before detaching affected native table registrations
and removing files. A failed or interrupted operation resumes that saved scope;
completed retries return saved outcomes without revisiting payloads. Protected
objects stay in place, and removal never retires their promises implicitly.

This path supports the owned local Unix workspace. It checks each present file's
digest once at the irreversible removal boundary, rejects symlinks and changed
files, and synchronizes its parent directory. Concurrent external changes to the
owned filesystem are unsupported. Automatic sweeping and native purge remain
disabled. Reported unlinked bytes are logical file sizes; reconciled absence
does not claim another unlink or freed disk blocks.

HTTP exposes the same services through `POST /retention/removal/plan`,
`POST /retention/removal/execute`, `GET /datasets/{dataset}/removals/{removal}`,
and `POST /datasets/{dataset}/removals/{removal}/resume`.

To stop an unfinished operation, use:

```sh
target/debug/docspec --workspace workspaces/comments abandon comments initial "input selection withdrawn"
```

The library's `Engine::abandon` and HTTP
`POST /datasets/comments/operations/initial/abandon` (JSON `{"reason":"input selection withdrawn"}`)
record the same decision. Identical retries return it without another commit.
That operation can no longer reserve work, acknowledge a completion or select a
result. Already-started I/O may finish; its protection stays retained. Another
operation ID can reuse phases completed before abandonment. Selected results
cannot be abandoned, and this action removes no files or recovery phases.

Abandonment preserves the admission counter. A schema change can proceed once
every unselected source admission has been explicitly abandoned, preserving the
selected source coverage. Backfills can prepare while source work is pending and
select once those captured admissions are abandoned. An admission after capture
still requires a fresh operation; abandoning it does not revive the old capture.

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
The selected release and old versions remain readable without the
catalog. An update automatically refreshes every selected scalar field group,
reusing computations for unchanged source files. It selects the new source and
all groups together after creation checks. General incremental SQL
remains unfinished. New releases use format 11, which separates source coverage
from the source-admission guard for schema changes. Retained format-10 releases
and interrupted operations keep their original interpretation. Earlier experiment
formats remain unsupported.

## Change a source schema

Use `plan-schema` and `schema` to rename, reorder or hide existing root fields,
add nullable scalar fields, or change defaults for future omitted values.
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

The field list defines the complete order of retained roots; additions follow it.
The resulting schema must contain at least one logical field.
Omitted fields stay available in retained historical versions. Removing a field
consumed by a selected computed group refuses. Computed expressions keep their
original input IDs after a rename;
new expressions use the current names. Repeat the same request to resume or recover
its selected result. HTTP exposes `/schemas/plan` and `/schemas/run`.

Finish pending source admission before starting a schema change. Selection
atomically installs the schema and `next_definition`. New writes use
that definition; pending old-definition writes refuse. A changed selected base or
later source admission also refuses cutover and preserves completed work. The
operation shares existing value files. Creation checks the metadata composition
and reuses completed checks for inherited files. Explicit deep verification reads
values when requested.

For a nullable addition, include this property in the same request:

```json
"additions": [
  {"name": "reviewed", "data_type": "boolean", "default": false},
  {"name": "note", "data_type": "string"}
]
```

Supported types are `boolean`, `long` (signed 64-bit integer), `double` and
`string`. Omitting `default` means null. Old files receive the initial default
through Iceberg v3 read rules; the operation writes no replacement data files.
Updates and based rebuilds apply the current default to newly omitted values.
An explicitly supplied null stays null, and a supplied value equal to the default
still counts as supplied. Change a future default with `"write_defaults": {"3": 9}`
using the selected logical field ID; null clears it. This never changes the
interpretation of existing rows.

Raw source-group reads and scalar expressions expose one required Boolean
`__docspec_supplied_<field-ID>` column per addition. False means the value was
omitted; true means supplied, including explicit nulls. Combined reads hide these
columns. For mixed omission within an incoming column, supply that Boolean column
alongside the value. Without it, a present column counts as supplied for every row.
Update keys must be explicitly supplied; false key flags refuse before defining
the replacement scope. Marker fields cannot be edited as public source fields.

After adding fields, full builds require an exact `base` and preserve native field
IDs and schema history, even after the added fields are hidden. Identical rebuilds
reuse completed output when input, SQL, definition and material schema rules match.
New writes retain the supplied flags alongside values; old files remain shared.
Large defaults must fit the configured batch allowance. Type changes, nested
additions, and schema edits of related join/summary outputs remain unsupported.

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
An unfinished plan reports completed and pending files, failures and any reason
selection would refuse. A completed plan returns saved row metadata immediately;
its file counters are `null` because reporting completion requires no historical
file scan. If ingestion advances before selection, a new request against the
new source can reuse unchanged file computations. Independent groups merge into
the latest selection; competing replacements of one group reject a stale target.
Combined reads join group values to exact source-file row positions.

Scalar computation selects its input columns through the native reader when
captured file metadata establishes a physical dependency. Other cases keep the
existing full-batch memory check before selecting columns. This preserves field
identity, defaults and occurrence addresses without an extra payload pass.

For a single resumable request while source intake continues, wrap that request
in `continuous-backfill.json`:

```json
{
  "backfill": {
    "dataset": "comments", "operation": "text-stats-continuous-001",
    "base": "update-001", "definition": "comments-v1",
    "group": "text_stats", "group_definition": "text-stats-v1",
    "fields": [{"id": 10, "name": "character_count", "expression": "CAST(length(comment) AS BIGINT)"}]
  },
  "optimistic_rounds": 2,
  "fence_millis": 30000
}
```

```sh
target/debug/docspec --workspace workspaces/comments plan-continuous-backfill continuous-backfill.json
target/debug/docspec --workspace workspaces/comments continuous-backfill continuous-backfill.json > enriched-release.json
```

Repeat the same parent request to resume. Each captured source has an exact child
operation; completed file work is shared across captures. The returned release
identifies the selected child, and parent retries return that same release after
later selections. Competing changes to the target group refuse. Authored source
field IDs must retain their meaning; a rebuild that renumbers them requires a new
intent against the new schema.

After the configured optimistic attempts, the engine briefly refuses new source
admissions with `source_fenced` while already accepted requests finish. An
unresolved accepted request remains visible in the plan. Expiry returns
`source_fence_lost`, allows intake again and retains completed work for retry.
`optimistic_rounds` accepts 1–16 and `fence_millis` accepts 1–300000. These are local
host limits; a stalled native commit can hold its dataset lock beyond the lease.
Use the existing `abandon` operation with the parent ID to stop an unfinished
intent. This does not delete completed work.

HTTP exposes `POST /continuous-backfill` and `POST /plan-continuous-backfill`
with the same JSON. Ordinary backfill entry points refuse owned child IDs;
operate through the parent. See [current qualification](docs/STATUS.md#one-recoverable-scalar-backfill-across-advancing-source-versions)
for the supported local behavior and remaining lifecycle work.

Before schema or backfill selection, every accepted source request must have
entered the selected history or been explicitly abandoned. A later full build
does not resolve an earlier unfinished request. Such a request produces
`pending_source`; completed backfill/schema work remains reusable after resolution.
This check reads retained metadata, including receipts for retired historical
results. It does not reopen historical payloads or run during ordinary reads.

The same `update` request works after group selection; `plan-update` includes group
preparation. A no-op update preserves the exact source and group table pins while
recording its new input coverage and empty source changes. Metadata-only schema
changes, computed input groups and logical row identity across compaction remain
unfinished. Explicit deep verification reads retained inputs and data; normal reads
stream the requested result. Complete-path backfill latency remains unmeasured.

```sh
target/debug/docspec --workspace workspaces/comments latest comments
target/debug/docspec --workspace workspaces/comments release comments initial
target/debug/docspec --workspace workspaces/comments compare old.json new.json
target/debug/docspec --workspace workspaces/comments changes old.json new.json changes.arrow
target/debug/docspec --workspace workspaces/comments check-succession old.json new.json
target/debug/docspec --workspace workspaces/comments invalidate comments "dependency correction"
target/debug/docspec --workspace workspaces/comments serve
```

`compare` returns duplicate-aware value counts and a description of the compared
endpoints. `--occurrences` also distinguishes replacement by an equal value with
a new occurrence identity. `changes` streams the actual before/after members as
Arrow IPC; it defaults to occurrence identity, with `--values` for value-only
comparison. Both accept `--group` and `--force-full`. When consuming the row
stream, derive counts from those rows instead of running a second comparison.

Each change row contains `side`, `file`, `row`, and typed `values`. The address
identifies an occurrence in that endpoint; after compaction it may name its
original location. Schema metadata `docspec.change` records exact endpoints,
identity, path and metadata effects. In value mode, addresses are deterministic
representatives of surplus values. Read through successful end of stream before
treating the result as complete. Default-only schema changes can report a schema
effect with zero value differences; incompatible fields refuse streamed comparison.

HTTP `POST /compare` and `POST /changes` accept `before` and `after` release pins,
an optional `group` (default `source`), and optional `options` containing `identity`
(`values` or `occurrences`) and `force_full`. Omitted options follow the respective
CLI defaults. Identical field-group pins skip payload reads unless forced.
Compatible local source histories use `forward_changes` to read retained applied
rows. Missing or retired history, unsupported comparisons and history limits use
full endpoint comparison with an explicit reason; invalid present history refuses.
`--force-full` bypasses history. See [optional change history](docs/optional-change-history.md)
for eligibility, bounds and independent retirement. Local dependent refresh uses
the same qualified history; portable certificate trust remains unfinished.

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
complete match groups affected by old and new keys. Qualified local change history
supplies those keys from applied removed/added rows, including same-key value
edits. Missing or retired history uses conservative changed-file discovery;
present corrupt history refuses. This includes newly possible matches, removed
partners, duplicate fanout and partial-null keys. Unaffected output occurrences
and source/evidence files remain shared. Unchanged logical source identities
under newer coverage adopt the exact prior output and evidence pins.

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

`mode: "incremental"` replaces complete groups reached by raw old/new keys from
qualified changes, with changed native files as fallback. It handles moved keys,
deleted final members and equal aggregate values with different contributors.
Any applied source rows affect a global summary; key discovery needs no payload
values for that global case.
Unchanged native inputs adopt exact source/evidence pins while advancing coverage.
Use a full refresh to change the recipe or native input schema. Both modes use
the same recovery, eligibility, captured/latest selection and scalar-group refresh.
Combined reads hide the global key; native reads expose it for evidence matching.

History narrows discovery, while scoped computation and contributor processing
still scan retained relations. Long, high-churn histories can stage more keys than
the fallback. Peak additional disk, retained storage and complete-path incremental
performance remain unmeasured; the separate full-refresh Comments gate does not
qualify these costs.

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
writers and DataFusion execution. Qualified history narrows join/summary key
reads; fallback still inventories endpoints and reads changed files. Required
inventory checks and explicit deep verification retain whole-state costs. These
limits prevent a claim that update time is independent of dataset size. Disk preflight
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

Owned output directories are protected before writes. Shared objects preserve
their original owners and exact dependencies. Explicit local removal uses the
common protection inventory and durably reserves outcome capacity in the owner's
lifecycle table. Automatic expiration and native metadata cleanup remain disabled.
Explicit compaction and restore use the shared lifecycle services. Lifecycle/catalog
recovery files are outside the object
removal namespace; retained history compaction remains unfinished.

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

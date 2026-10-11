# Greenfield Rust DocSpec: local-first, Iceberg-only implementation

This is the implementation scope accepted on 2026-10-07. It preserves R1–R7 and
the research acceptance cases in [requirements](requirements/). The user's
Iceberg-only revision supersedes the older proposal's Lance/database comparison.
Implementation progress and unqualified behavior belong in [STATUS.md](STATUS.md),
not in a reduced definition of completion.

## Architecture

Build one embedded Rust engine with thin CLI and HTTP interfaces. Keep domain
decisions independent of storage, catalog, execution and transport; inject
maintained dependencies at startup. Add abstractions only when a concrete adapter
or test requires them. Use Iceberg/Parquet for data and lifecycle records, Arrow
for batches, and DataFusion for relational processing. A separate DocSpec SQL
database is outside v1. SQL used internally by the embedded Iceberg catalog holds
only that catalog's bookkeeping.

Qualify filesystem storage and the maintained embedded catalog first. Add R2
object storage with its managed REST catalog afterward; qualify the actual
client/server combination before claiming hosted support.

Separate source values, independently computed field groups, evidence
relationships and bulk changes/coverage. Retain original bytes immutably. Share
unchanged physical files through snapshots and avoid per-comment lifecycle
transactions. One dataset-scoped lifecycle table selects the exact consistent
combination of completed results, definitions, versions, releases and retention
state through a guarded commit. Unrelated datasets prepare and commit independently.

Native table heads do not independently determine a complete DocSpec result.
Prepare and protect required snapshots, then select their exact combination.
Use maintained expected-snapshot checks; after conflicts, re-evaluate application
eligibility rather than retrying stale decisions.

Public types include `DatasetPin`, `OperationId`, `PhaseResult`, `ReleasePin` and
`RetentionPlan`. Pins bind exact snapshots, schemas and evidence. Operations have
stable retry identities and structured refusal reasons. Shared services expose
plan, run, inspect, resume, read, verify, compare, backfill, refresh, publish,
compact, retention preview/execute and restore-point create/restore. CLI and HTTP
exchange bounded metadata; bulk reads stream batches.

## Performance and user-value rules

Do work once and retain its useful result. Repeat a read, validation, transfer or
computation only when a concrete correctness boundary requires it, or avoiding it
would add more complexity than it removes. Favor immutable selected versions and
small commit-time authority checks. Ordinary reads resolve a version and stream
the requested data; they do not revalidate historical inputs or dependency graphs.

Validate new outputs at their creation boundary using schema/field IDs/types,
file sizes, footer row counts and a bounded native row sample. Final assembly
checks that the already validated components belong together; readback checks the
serialized selection, without repeating payload validation. Reuse completed
validation on resume. Initial admission hashes while copying; a newly written
durable native file currently needs one digest pass because the maintained writer
does not return a checksum. Retain that exception until measurement justifies a
writer adapter.

Diagnostics must serve a real decision: establish that a retained result is
usable, investigate a reported discrepancy, or explain a failed operation. The
explicit `verify` command checks routine structure and a small sample;
`verify --deep` checks full integrity and reconstructs derived results. Deep
checks and exact independent comparisons remain in qualification tests. Do not
turn optional diagnostics or comprehensive reports into a normal-path dependency.
Sampling does not establish exact unsampled values or statistical completeness.

## Lifecycle

Record completion at useful boundaries: admission/transfer, processing, evidence
preparation, validation, export/upload and independent readback. Each completion
binds exact inputs, material dependencies, outputs, verification and retention
protection. Resolve qualified completed work before opening large payloads.
Worker loss, cancellation, later validation failure and report-rendering failure
must preserve completed phases. Keep full runtime provenance separate from the
dependencies that decide reuse.

Produce typed changes from actual applied writes: inserts, replacements,
deletions, evidence-only changes, stable field renames and independently backfilled
groups. Initial incremental refresh supports equality joins, one-to-many
replacement and complete affected-group recomputation. Discover affected results
using old and new keys, including newly possible matches. Unsupported transforms
use an explicit full rebuild.

Routine refresh selects complete captured inputs and reports lag without
regressing coverage. Schema cutover additionally fences the active definition and
exact source boundary. Pending work, successful nulls, failures and deletions
remain distinct. Preserve later accepted inputs during backfill and cutover.

Publish immutable authorized objects, independently verify them, then select the
release through the catalog. The HTTP API resolves latest from that selection
and returns an immutable reference. Parquet downloads and evidence remain
independently readable. There is no second mutable latest object in R2. A lost
response reconciles its existing operation without repeating completed uploads or
verification. Public files contain only authorized bytes.

Maintain one logical protection inventory for selected versions, completed
phases, readers, releases, unfinished operations and restore points. Map snapshot
protection to native Iceberg references where supported and verified. Disable
automatic expiration until protection tests pass. Removal requires fresh
eligibility, durable reservation, prevention of new references and reconciliation
of exact deletion outcomes. Default to explicit retirement until a retention
policy exists.

Restore points bind verified metadata and the required object closure. Restore
into a fresh catalog with old destructive actors fenced at the actual storage
boundary. Do not promise recovery beyond the last completed restore point.

## Delivery and completion checklist

- [x] **Local foundation and probes:** pinned compatible dependencies, guarded
  commits, lost-response reconciliation, snapshot protection, local durability,
  bounded readers/writers; early memory, concurrency and cleanup qualifications.
- [x] **Recoverable Comments path:** retained input → deterministic shaping →
  data/evidence → validation → immutable local release → independent reader.
  Inspect/resume and failure after each expensive phase must reuse qualified work.
  Complete the first R1/R2/R6/R7 slices.
- [x] **Incremental updates and backfill:** R3–R5 exact changes, docket
  joins/summaries, evidence-only refresh, independent field additions, concurrent
  ingestion/backfill and guarded cutover. Compare optimized results with
  independent full reconstruction.
- [x] **History and maintenance:** R6 pinned history, compaction, retention
  preview/execution, phase retirement and explicit restore points. Preserve every
  promised value and evidence relationship through maintenance.
- [x] **R2 and Dagster:** remote publication/readback through the same engine;
  thin Python adapter passes operation IDs and bounded status while Rust owns
  lifecycle decisions. Qualify worker loss, concurrent hosts, credentials,
  retention and restore against the actual provider.

Use maintained catalog, decoder, writer, scheduler and backup facilities. When a
required primitive fails qualification, document the concrete gap and repair or
replace that adapter without weakening the lifecycle guarantee. Commit coherent
checkpoints and close actionable independent semi-formal review findings.

## Performance and acceptance

Freeze a digest-verified, representative selection of **10 million retained
Comments**, preserving full text, nested values, duplicates and evidence. Reuse
verified local assets before downloading missing data.

Time admission, deterministic shaping, docket joins/summaries, evidence retention,
native and flat output production, release selection and independent readback.
Government API acquisition and optional OCR/model inference are separate workloads.
The acceptance target is **1,800 seconds for each of three fresh-workspace full
runs**, not an established capability.

Use the reference workstation's 14 CPU cores and 48 GiB memory. Start with
configurable ceilings of 12 compute threads, 24 GiB aggregate process memory and
32 GiB scratch. Preflight retained-output and scratch needs against current disk
availability; do not assume the historical free-disk observation remains current.
Record local and R2-inclusive timings separately.

Clear application reuse state between full runs and record operating-system cache
conditions/background load. Measure unchanged, small-update, backfill and
interrupted/resumed paths separately. Freeze protocols through test-hypotheses
before collection and retain failures/retries. Run
[Q1–Q8](requirements/07-operations-qualification.md#adversarial-qualification-cases),
adapting publication-authority tests to the single catalog selection; real-provider
destructive maintenance and restore tests remain required.

Verify exact values, types, null/empty distinctions, membership, meaningful
ordering, schema interpretation, input coverage and evidence with an independent
reader. Hold update size fixed while independently increasing population, file
count, retained versions, phase history and evidence depth. Report elapsed time,
payload/metadata requests, bytes read/written/transferred, retries, peak memory,
scratch and retained storage.

Optimize measured repeated scans/transfers first, unnecessary rewrites second,
metadata operations third and execution overhead afterward. Reconsider an
additional SQL backend only when lifecycle metadata prevents the target and a
controlled comparison proves a material complete-path improvement with identical
guarantees. Earlier speculative overhead estimates are not acceptance results.

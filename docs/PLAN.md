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

The retained whole-job extension uses the same native lifecycle owner. Resolve
per-member custody and sealed material dependencies before caller payload reads;
include expression subqueries and the exact originating runtime in reuse keys.
Keep input/step payload replay under the existing conservative result-availability
promise until explicit retirement ends every applicable root. Admit retained
files and native spills together, preserve host-wide disk concurrency bounds,
and reclaim private crash scratch only under exclusive workspace ownership.
End admission authority-read snapshots after their guarded commit, before
execution. End computation providers after their last consumer while retaining
independent schema, report, witness, disk and protection accounting.
Avoid unused representation capacity at known-size lifecycle writes. Admit the
existing conservative export metadata units as row groups become necessary,
before writing, while retaining the same ceiling and owner lifetime. Keep private
spill layout independent of compute-batch rows and preserve native byte limits.
Qualify these boundaries with tiny successor, interruption, actual-spill and
footer/cache lifetime probes before the complete repository gate.

Extend retained input admission to exact native snapshots through selected-read
closure custody. Preserve original metadata, manifest, data and supported delete
URIs; use maintained native I/O over protected exact copies rather than rewriting
metadata or exporting rows. Seal useful members before later transfers and reuse
them before reopening original objects. Version native custody receipts explicitly
so file-only readers cannot silently accept stronger claims. Keep historical
metadata for interpretation and ancestry without promising historical payloads.
Native producer-definition adoption and accepted population prerequisites must
precede application model execution; hashes and custody alone do not prove them.
First adopt exact independently established definition metadata through the
existing local dataset authority. Guard the current definition, epoch, selection
and adoption; change definition and adoption in the same commit without selecting.
Retain one current byte witness and immutable action identity, so lost-response
retries return history without reopening artifacts or restoring an old adoption.
Keep new managed execution and acceptance in adopted scopes refused until exact
static proof and accepted population prerequisite admission are wired. An
adoption records the owner's interpretation; it does not prove writer execution,
row equality, extraction readiness or publication.
Before population work, capture the exact adopted definition and original
producer record bytes with proof admission. Check original head pins, adopted
producer facts and selected native append ancestry through existing protected
metadata reads. Accept exact native required counters under the adopted proof
policy. Admit the dependent model only with its exact accepted same-scope proof,
original model file member pins and inherited native input custody. Preserve
the grant independently of donor retirement, with proof and model evidence
bound to each completed view set.
Transfer same-owner custody through a guarded receiving operation, binding the
exact lifecycle table incarnation and original input generation. Commit inherited
objects and original-copy provenance as roots before later seals. Keep that new
root independent of donor retirement, and retain provenance under independent
result availability. Use source-only material keys; physical copy locations and
custody-reference history must not invalidate computation.

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

## Whole-job schema preparation

Prepare one complete captured `JobSpec` from explicit named Arrow IPC schema
messages or exact bounded native Arrow-serde/Iceberg schema artifacts with explicit
format tags and maintained native conversion. Reuse the job's native name/lifetime checks, guarded SQL/function and
parameter planning, export projection/portable schema rules, and connected-component
endpoint/type rules. Return every named input, step, export and report schema,
including private outputs, tied to the exact captured artifact and declared schema
bytes. Distinguish declared input schema bytes from the native provider relation
schema, including JSON Lines source columns and declaration restrictions. When
both an Iceberg declaration and an input pin supply native schema IDs, require
exact equality before registration; retain the declared ID without claiming
observed admission. Arrow-only declarations remain unobserved assumptions.
Schema-only providers refuse scans. Preparation does not create job scratch,
open data sources/catalogs, materialize graph rows, export rows or execute reports.
Reject nonempty `table_operations` in this profile.

Input schemas remain declarations, including original raw pins. They do not prove
observed schema, admission, custody, consumed coverage or protection. Use standard
Arrow IPC schema messages for maintained Python consumption without another type
language. Bound artifact decoding, declared schemas and returned schema metadata
under the execution host's existing budget. Guarded literal expansion charges
actual conservative estimates before invocation, preserves cumulative limits and
holds admission through maintained consumer lifetimes; empty contexts consume no
unused literal allowance. Prove unchanged small-host consumer capacity as well as
expansion refusal and stream ownership.

Ordinary whole-job execution, retained multi-output completion/reopen hooks,
remote custody, complete candidate qualification and guarded selection require
separate evidence. Retained execution must reuse native durable phase completions
for expensive materializations, exports and reports; wrapping `JobSpec::run` is
insufficient. The initial managed whole-job family must also reject table operations.

## Local external quality gates

The local `managed-build` command supports independent source-only Build and
based Update operations. `prepare_update` carries the existing `UpdateSpec` and
an immutable qualification policy. Reuse the native admission, retained phases,
exact protected reads, evidence acceptance and guarded selection; do not add
another lifecycle or treat ordinary SQL jobs as managed operations.

The first Update profile requires a selected independent source-only base. Keep
complete-key replacement, explicit deletion, declared column merge rules and
unchanged-row inheritance distinct. Qualify the exact candidate before selection.
A changed selection or authority refuses promotion; this profile does not compose
a new candidate from a newer sibling state. Rule-only changes reuse computed
values. Completed retries return the original historical result without requiring
it to remain current or selecting it over a successor.

An exact retry of accepted evidence resolves native retained acceptance before
opening caller artifacts again. Compare the complete submitted evidence, active
policy, candidate and retained acceptance identity. New acceptances retain the
original resolved-rule bytes; older records do not gain a byte-custody claim.
Keep the exact managed request/result through checkpoint compaction until native
recovery or availability retirement ends that promise. Quality control metadata
does not add a separate payload lifetime.

Refresh, Backfill, computed/related views, multi-view qualification and publication
need their own supported selection paths and exact candidate evidence. This local
profile does not qualify those extensions. Its focused tests, DDK wrapper tests,
repository gate and independent review remain separate evidence.

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
# Active retained whole-job increment

Implement the existing native whole `JobSpec` through durable phase completion
and reopen hooks. Keep one authority for dataset selection and use the existing
protection, inventory, phase recovery, operation recovery and result availability
scopes. Preserve original request bytes for unknown submission reconciliation.
Native SQL qualification resolves the original completed candidate, full named
view inventory, original SQL digests, exact Arrow types and one complete integer
zero row per declared blocking report; metadata reports remain ordinary results.

The first local file profile needs independent interruption/reopen, changed-rule
material reuse, null/empty/nonzero/truncated refusal, historical selection,
retirement/resource and actual DDK consumer proof before a full repository gate.
An explicit native adoption of original producer-definition manifests, exact
origin/append chains and native population prerequisites must close Congress Gate5
before its application guards can be removed. Remote raw snapshot custody and
publisher intent remain unavailable until implemented and qualified.

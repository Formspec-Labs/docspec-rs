# Implementation and qualification status

## Exact dependencies for reusable work

New processing completions retain the exact admitted copies they consumed.
Release assembly uses those captured references when reusing processing, so a
later admission under the same material key cannot silently replace the original
input provenance. Validation inherits those dependencies. Older completions
remain resumable through the existing admission lookup, with missing dependencies
explicitly unknown for future reclamation.

Schema interpretation and consumed table snapshots have distinct references.
An evolved source build retains the native schema/default metadata it actually
read. Full-join evidence retains the processing snapshot it consumed, and
validation retains the evidence snapshot. Scalar file completions retain their
original source schema/default metadata across valid reuse after a schema change.
Existing change and coverage records continue to supply their typed dependencies.

These references are captured from metadata already available during processing.
They add no payload read, hash, verification pass, lifecycle transaction or
per-file inventory copy to the general phase path. Material reuse identities stay
unchanged. This is a correctness prerequisite for retention, not a measured
throughput improvement or permission to delete objects.

Focused tests cover admission-key rebinding for builds and updates, actual legacy
resume, source defaults, scalar completion reuse after schema reorder, and exact
join/evidence snapshot references. See the [input cases](../tests/phase_dependencies.rs),
[expanded focused log](reviews/phase-dependencies-inputs-focused.log),
[snapshot case log](reviews/phase-dependencies-snapshot-focused.log) and
[independent review](reviews/phase-dependencies-review.md).
The full [repository gate](reviews/phase-dependencies-check.log) passed, including
formatting, strict all-target linting and the complete tests with normal stack
limits. No large-data benchmark was rerun for this metadata change.

The [retention design review](reviews/retention-inventory-design.md) identifies
the remaining work: complete typed roots and native dependency expansion,
independently interpretable prepared/validation backfill dependencies, retirement
of exact completion generations, worker exclusion, durable removal intents and
exact deletion outcomes. Phase retirement, physical cleanup, compaction and
restore remain unfinished. No expiration or deletion is enabled here.

## Explicit operation abandonment

`Engine::abandon`, the CLI `abandon` command and HTTP operation endpoint record
an explicit decision against an admitted, unfinished operation. The retained
record binds its identity, original request digest and admission revision to the
reason. Existing per-dataset locking and guarded native commits decide races
with selection. Identical retries read that decision without another commit;
already selected results refuse abandonment.

All source, refresh, schema and scalar-group reservation/completion paths check
the operation's abandonment key, as does the common selection path. These checks
reuse the exact lifecycle snapshot cache and test key presence without decoding
large results or reasons. They add no payload scan or ordinary-read validation.
Completed phases remain reusable under their material identities by another
operation ID. Readback releases, historical results and attempt protections all
remain retained. Already-started native or blocking I/O may finish; abandonment
is a logical fence, not worker termination or deletion authority.

Source admission counters remain monotonic. A withdrawn admission does not become
consumed input. The following cutover change separates selected source coverage
from the guard against newly admitted work.

The focused tests and full repository gate passed. The fixtures cover reopen/retry, shared
phase and file-completion reuse, unchanged protections, late native source/group
commits, lost abandonment/selection responses, CLI/HTTP, and source-boundary
refusal. See [tests](../tests/abandonment.rs),
[concurrency cases](../tests/rebuild_completion.rs) and the independent
[code review](reviews/operation-abandonment-review.md),
[focused log](reviews/operation-abandonment-focused.log) and
[full gate](reviews/operation-abandonment-check.log). The
[architecture review](reviews/retention-architecture-review.md) identifies the
remaining typed root inventory, worker protection and exact deletion boundaries.
No storage reclamation, phase expiry, release expiry, compaction or restore is
claimed by this checkpoint.

The [development resource record](reviews/operation-abandonment-resources.json)
separates regenerated debug-cache storage from volume-wide free space, which also
reflects other workstation activity. The cache remains available for continued
implementation; retained experiment data and failure evidence were not removed.

## Cutover after abandoned input

New schema operations preserve the selected source boundary and capture the
current admission counter separately. Admission proves that every entry between
those boundaries was explicitly abandoned, checking its original request,
revision and abandonment identity. Missing, duplicate or inconsistent entries
refuse cutover. The schema admission does not rewrite the unchanged authority row.

Backfills can prepare while source work is pending. Final selection checks the
exact source, target group, writer definition and captured admission counter,
then proves abandonment of any captured source gap from the same locked catalog
view. This lets a completed backfill select after pending input is withdrawn.
Any later admission invalidates that capture even if subsequently abandoned;
a fresh operation may reuse completed computation.

New releases use format 11. Readers also accept format 10; its schema changes
retain their original incremented boundary rule. An interrupted operation keeps
its admitted result format, including serialized readback identity and the older
strict backfill rule. Material processing identities remain unchanged, preserving
reuse across this metadata correction.

The gap check visits retained operation metadata only when a gap exists and keeps
a bounded set of qualified admission numbers. It adds no payload scan or ordinary
read validation. Work scales with retained operation records and gap size; large
history performance remains unqualified. No gap index or stored certificate was
added. The [public cases](../tests/abandonment.rs),
[retained-format cases](../tests/admission_formats.rs) and malformed-history unit
tests passed in the [focused](reviews/admission-fence-focused.log) and
[history](reviews/admission-fence-history.log) runs. Format compatibility fixtures
reconstruct retained serialized state; they do not run a historical binary.

The first full gate exposed a debug-stack overflow during native update metadata
reading. Moving construction of the existing boxed `prepare_and_select` future
into a regular function reduced its caller's measured poll frame from 69,952 to
41,888 bytes, including saved registers. The work and allocation count stay the
same; the inner frame remains 629,808 bytes. This is a debug stack measurement,
not a throughput or whole-process memory result. The
[failed gate](reviews/admission-fence-check-stack-failed.log),
[crash trace](reviews/admission-fence-stack-failed.json),
[before](reviews/admission-fence-frame-before.log) and
[after](reviews/admission-fence-frame-after.log) prologues remain retained.
The [defaults retry](reviews/admission-fence-defaults-retry.log) and complete
[repository gate](reviews/admission-fence-check-retry.log) passed with normal stack
limits. The [independent review](reviews/admission-fence-review.md) records the
correction and qualification. The complete R1–R7 scope remains unfinished.
The [resource snapshot](reviews/admission-fence-resources.json) records development
cache storage separately; this checkpoint ran small fixtures and retained the
existing large-data experiment evidence.

## Comments performance and disk checkpoint

The resumed source-processing slice over the frozen 10-million-Comments input
passed in 41.35 seconds through selection and independent readback, or 49.29 seconds
including its original failed attempt. It used the maintained default Zstandard
writer and a measured 768 MiB batch allowance; the generic default stays unchanged.
The first attempt exceeded its 64 MiB batch allowance and remains a retained failure.

The final workspace retains 5.45 GB, **2.04 times the encoded input**, including
admission, selected output, failed output and metadata. The frozen input and codec
diagnostics outside that workspace bring the listed task-owned data to 8.22 GB,
before build artifacts and the shared source corpus. Observed peak process-tree
memory was 3.31 GB; no scratch was observed. Unchanged processing reused the same
input/native objects and added 99.7 KB of metadata; its exact retry changed no
retained files. Physical reuse does not measure absence of cached reads.

This is a potentially warm, resumed `SELECT *` source build. The complete lifecycle
performance target remains unqualified. The [pilot report](../experiments/comments-input/REPORT.md)
records raw results, disk accounting, sampling limits and the independent reviews.
Live experiment stops are not a production disk quota. Repeated history and
maintenance capacity remain open, and no automatic reclamation is claimed.

## Nullable source fields and defaults

`SchemaSpec.additions` supports nullable Boolean, signed 64-bit integer, Double
and String roots. The same operation allocates stable native IDs and selects
Iceberg v3 initial/write defaults plus a required Boolean supplied flag. It shares
every existing source payload file. There is no separate schema registry or
backfill scan for a literal addition.

Initial defaults interpret old physical absence. Future omission defaults can
change by field ID without reinterpreting existing rows or recomputing scalar
groups that only consume their unchanged values. New input normalization applies
the selected rule once; internal kept/removed-row rewrites retain stored values
and flags. Explicit null, explicit equal-default values and omission stay distinct
for every supported type. Combined reads hide flags; raw source-group reads and
scalar expressions expose `__docspec_supplied_<field-ID>`.

Based rebuilds of these sources preserve the retained schema history and native
allocation high-water mark, including after all added fields are hidden. Reuse
depends on the bounded material schema identity rather than a physical metadata
location or snapshot. Verification checks that relationship using metadata.
Tracked update keys cannot omit their values: explicit per-row key flags receive
one streaming probe before replacement scope is derived. This probe can read all
replacement flags in the all-supplied case; it avoids a custom execution operator
and does not scan the existing population solely to validate presence.

Default expansion receives a conservative pre-decode batch check. This is not a
whole-process memory quota. Writer batches retain native nullability without
copying value buffers. Adding fields saves an immediate source rewrite; this
checkpoint does not measure schema-operation latency, marker storage overhead,
history growth or maintenance reclamation on the Comments workload.

The [focused tests](../tests/defaults.rs) exercise exact values/types, preserved
historical reads, partial-file sharing, hidden-field ID history, default changes,
rebuild reuse without original input, omitted-key refusal and forged metadata.
The [independent review](reviews/schema-defaults-review.md) retains findings and
their resolution; [gate output](reviews/schema-defaults-check-retry.log) records the
repository check. Earlier test failures and the native inspection stack trace
remain under `reviews/schema-defaults-*`. A stack failure led to separating the
existing source-admission/processing block from later result assembly. A regular
function constructs its boxed future, so native source state is not embedded in
the assembly poll frame. Bounded debug disassembly measured that frame decreasing
from 903,344 to 607,504 bytes, including its saved registers; this is a debug-frame
measurement, not a release throughput or total-memory result. Phase order,
cancellation, selection guards and payload work stay the same. An attempted
expression-worker change and inspection boxing were removed; normal stack limits
remain in use.

Nested/type evolution, related-output edits, global source IDs across legacy
non-evolved rebuilds, continuous-ingestion catch-up and the broader R3/performance
gates remain open.

The full repository gate passed with normal stack limits. After its process
finished, an owned-reader check found no active debug executable or build in this
repository. `cargo clean --profile dev` removed **5.4 GiB** of rebuildable artifacts;
the release binary's size and modification time stayed unchanged. Free volume
space measured 20.59 GB afterward, including concurrent workstation effects. The
[cleanup log](reviews/schema-defaults-build-cleanup.log) separates this development
cost from retained dataset, historical-result and failure evidence.

## Current verification policy

User steering on 2026-10-08 supersedes earlier notes below that describe automatic
full verification. Creation now checks schema/field IDs/types, file sizes, footer
counts and bounded samples. Output validation runs once at its retained phase;
final assembly checks metadata composition, and readback checks the serialized
manifest. Ordinary reads resolve that manifest and stream the requested data,
without inspecting historical payloads or replaying the dependency graph. Refresh
admission trusts accepted captured versions. Newly created durable objects retain
one initial digest; inherited objects reuse their retained digest.

`verify` is an explicit routine diagnostic. It reports its sample scope (at most
three native files, eight rows each, and one configured decoded batch-byte
allowance per file, at most three allowances across its graph). It reads each live file's footer; work still scales with file
and metadata count. Native readers may fetch/decode compressed pages or row groups
larger than the returned sample. Neither footer admission nor decoded sample limits
establish a whole-process memory or physical-I/O bound. Same-size unsampled
corruption may pass. `verify --deep` and qualification callers retain payload
hashes and complete reconstruction. Neither is a prerequisite for serving data.

The first suite exposed an outdated HTTP expectation that historical input damage
must prevent reading an already completed empty result. The updated test requires
the read to succeed and the explicit deep diagnostic to identify that damage.
The 10-million-Comments performance target remains **unqualified**; removed scans
are established by the call paths and tests, not a measured throughput claim.

Creation now trusts physical files from the exact completed base and shares one
bounded check inventory across source, changes, evidence and coordinated scalar
groups. A newly written file shared by source and change tables is inspected once.
Reuse requires the same object identity, native row count and compatible physical
schema history. Each table still checks its own metadata, declared counts and
references. Explicit routine verification starts without trusted files and deduplicates
physical checks within that diagnostic. File-size/stat checks and metadata traversal
can still repeat; this does not establish update latency independent of population.

Review exposed a sample-budget refusal for individually valid wide batches; the
shared allowance now covers at most three per-batch limits. A regression fixture
also encountered retained input row-group buffers exceeding a small batch allowance
during processing. Smaller input row groups isolate the sampling test, whose retry
passes; the original failure remains in the review logs. It does not establish a
general bound on DataFusion's backing-buffer memory.

The corrected full repository gate passed; see
[the gate log](reviews/routine-verification-final-check-retry.log) and
[independent review](reviews/routine-verification-consistency.md). Process tests
show creation inspection once, no inspection during ordinary read/resume, no
duplicate source/change file inspection, and no inherited-file footer inspection
during an update. Explicit deep corruption/reconstruction tests remain enabled.


This repository follows the user's local-first, Iceberg-only revision of the
[greenfield proposal](PLAN.md).
The revision selects Iceberg rather than the older document's Lance-first
comparison and keeps DocSpec lifecycle records in Iceberg. There is no migration
or compatibility layer for legacy DocSpec.

## Requirements

| Requirement | Implemented locally | Required work remaining |
| --- | --- | --- |
| R1 Durable data and commits | Retained input bytes; native tables and complete snapshot read inventories; shared-file source updates; guarded selection; stable retry identity; completed phase reuse; process-kill recovery after acknowledged build, update and scalar-backfill handoffs | Broader durability failures; complete occurrence/value identity and evidence semantics |
| R2 Incremental publication | Immutable local manifest; native independent readback before catalog selection; retry reconciliation; original completed release reuse | Authorized public exports; R2 uploads and independent remote readback; transfer reuse and provider qualification |
| R3 Schema evolution and backfill | Typed pinned schemas; logical field IDs mapped to native IDs; public root rename/reorder/hide and nullable scalar additions/defaults with atomic writer-definition cutover; durable supplied/omitted values; scalar computed groups; exact scalar input bindings and value verification; complete source-occurrence coverage; per-file progress and failures; retained computation, validation and readback; unchanged-file reuse keyed to consumed source fields; coordinated source/scalar-group updates and full rebuilds; source requalification; guarded source/target selection; combined reads | Nested/type evolution; related-output schema edits; global source IDs across non-evolved full rebuilds; nested computed values; broader selective dependencies; continuous-ingestion catch-up and cutover |
| R4 Verified changes | Full comparison preserving duplicate counts, schemas and input identity; typed removed/added tables from applied source, join and summary writes; exact endpoints and key field IDs; independent removed-row membership and multiset reconstruction; accepted-version succession independent of optional change files | Evidence-only changes; change-history composition, lineage certificates and explicit unavailable-history fallback |
| R5 Dependent refresh | Repeatable DataFusion query execution over one retained Parquet input; coverage regression refusal; bounded accepted-history proof; producer-declared coupled/independent inputs; fixed family membership and create-once exact generation seals; metadata-only related-input capture; native full equality joins with contributor evidence, captured/latest guarded selection, transitive eligibility checks and catalog-free reconstruction; complete-key source updates with coordinated scalar-group refresh; old/new-key affected-group join replacement; unchanged source/evidence file sharing and exact no-op adoption; typed grouped/global summaries with exact contributors and complete affected-group replacement | General evidence-only changes; computed input groups; broader aggregate functions and key types |
| R6 Maintenance and retention | Retained native history; catalog-free pins; protection before preparation; explicit logical operation abandonment; exact dependency capture for new general phases and scalar file completions; all cleanup disabled | Complete shared-owner roots and dependency expansion; reader/release/phase retirement; compaction; retention preview/reservation/deletion reconciliation; complete restore points and fenced fresh-catalog restore |
| R7 Operations and qualification | Shared services; CLI; loopback HTTP; plan, run, inspect, resume, read, verify, compare, schema edits, backfill and native join/summary refresh; exact-version native and combined HTTP Arrow streams; structured refusals; configured budgets; fault harness; raw local experiments | Complete counters; aggregate resource and real-network qualification; broader fault tests; representative Comments corpus; final local and remote performance gates; Dagster adapter |

The CLI exposes implemented operations only. Source updates support complete key
replacement and deletion against an exact base; unsupported transformations require
an explicit full query. Independent scalar backfill and coordinated source/scalar
updates and explicit full rebuilds are available. Incremental reuse of a globally
invalidated source refuses until a full rebuild requalifies it. General incremental
SQL remains unfinished. A retained source file establishes whole-input provenance, not
per-value evidence relationships or complete DocSpec conformance.

## Component qualifications

| Component | Evidence and boundary |
| --- | --- |
| Iceberg and Iceberg SQL catalog 0.10.1 | Native reads/writes and expected-snapshot commits execute in integration tests. A narrow catalog guard prevents stale transaction refresh. Local ownership uses an OS lock. Remote multi-host use is unqualified. |
| Arrow/Parquet 58 and DataFusion 53.1.0 | Exact retained flat and nested inputs pass an independent PyArrow comparison. The duplicate-removal regression demonstrates why component qualification is necessary. General SQL correctness and all source types are not established. |
| Filesystem and SQLite | Process termination after acknowledged handoffs is exercised. Machine power loss, storage faults and host loss are not qualified. |
| Resource controls | The application reserves reader/writer capacity; Budget owns the shared DataFusion execution/spill runtime. A native probe confirms leaked disk accounting after spill-quota failure; reopen is required until a maintained fix is qualified. Whole-process memory includes allocations outside these controls. |
| Append-only lifecycle layout | Deliberately provisional. The bounded history probe exposed repeated scans; one disposable exact-snapshot cache reduced that cost. There is no durable auxiliary index. Cold lookup and retained metadata still grow with history. Warm admission and cold replay now enforce the same bound; history growth refuses new commits before acknowledged records become unreadable. |

## Delivery gates

The local foundation and retained Parquet path are implemented and tested. The
first milestone's wider capacity and durability qualifications are still open;
the Comments milestone still needs its complete evidence and consumer path beyond
the measured retained source-processing slice.

The required sequence continues with incremental updates and backfill, maintenance
and restore, then R2 and Dagster. Provider credentials and actual deployment behavior
must be qualified before claiming hosted support. Public-data authorization is not
inferred from a readable local file.

The full performance gate remains **not run**: a frozen representative selection
of 10 million retained Comments, deterministic shaping, docket joins/summaries,
evidence, native and flat outputs, release selection and independent readback in
1,800 seconds, passing on each required fresh-workspace repetition. Neither small
fixtures nor repeated local metadata lookups establish this target.

Q1–Q8 remain the acceptance framework. Current tests cover bounded worker recovery,
stale dependency refusal, concurrent independent field-group selection and independent
reading. Coordinated scalar refresh has source-admission/backfill race tests;
the full ingestion and compaction matrix, shared cleanup, related-input selection,
old-actor restore, the full metadata-growth
matrix and the complete consumer path remain unqualified. See the original
[qualification plan](requirements/07-operations-qualification.md).

## Shared-file storage checkpoint

The maintained Iceberg append operation can create a fresh candidate table that
references unchanged Parquet files alongside replacement files. The native probe
checks unchanged bytes and modification time, retained old membership, exact new
membership including duplicates/nulls, preserved inherited ownership, and reads
without the original catalog. Unknown external objects, conflicting inherited
identities and missing inventory references refuse. Native statistics sidecars
participate in ownership and inventory checks; interpreting their contents is not
qualified by the reference-only test.

Inventories traverse the snapshots named by the exact native metadata and decode
each shared manifest once. They describe snapshot read dependencies, not a catalog
backup or full restore point: previous metadata-log files, catalog bookkeeping and
provider authority still require the R6 restore implementation. Delete-bearing
current snapshots explicitly refuse this candidate-sharing path until delete
semantics are qualified. The source-update checkpoint below adds affected-key
discovery and guarded selection. Shared retention remains unfinished; independent
scalar backfill is described below.

## Source-update checkpoint

`UpdateSpec` binds a selected base, typed replacement rows and optional deletion
keys. A key replaces all matching old rows; composite keys use null-safe equality.
Overlap refuses. The application prepares distinct keys once, shares unaffected
native files, and sends kept/removed rows from each affected file to bounded
writers. Added rows share their files with the selected source table. Stable
native field IDs are preserved, including nested values. No schema change is
accepted through this operation.

The common phase path retains admission, processing, validation and readback.
Every phase and final selection checks the captured base. Competing attempts of
the same operation reconcile its durable result. Changes bind the base manifest,
exact before/after metadata, key IDs, typed removed/added tables and row counts.
Creation checks the change-table shape and count arithmetic. Explicit deep
verification checks removed-row membership and exact multiset reconstruction.
Fixtures additionally compare the applied source values against request semantics.

See [update tests](../tests/updates.rs), the independent
[consistency review](reviews/updates-consistency.md) and
[resource review](reviews/updates-resources.md). The tests include failure while
paired writers are active and resume without the original input files. Source
updates now have actual process-termination cases at acknowledged handoffs, as
described below. The full Q1–Q8 matrix remains open.

Physical file reuse is asserted, but end-to-end update performance is unqualified.
Discovery still reads source files to find affected keys. Native pin creation,
validation and readback still hash/read full result inventories and retained
inputs. Write counters exclude metadata and verification I/O. File-count/history
scaling and complete-path performance measurements remain required; no speedup is
claimed for this checkpoint.

## Native query-reader checkpoint

Full comparisons now use [the shared native-reader adapter](../src/query.rs)
through maintained DataFusion streaming interfaces. The former Iceberg/DataFusion
provider ignored the configured execution context when opening its native scan.
The adapter reuses the existing serial reader, byte admission and batch-row limit,
and preserves native capacity refusal codes through DataFusion context wrappers.
The unused provider dependency is removed; DataFusion's prior default feature set
is now enabled directly rather than through dependency feature unification.

Targeted tests verify live-stream admission, competing-reader refusal, release on
drop/end/error, configured batch rows, duplicate/null values, retained selection,
filters and empty schemas. See the [consistency review](reviews/query-reader-consistency.md),
[resource review](reviews/query-reader-resources.md) and [gate output](reviews/query-reader-check.log).
This closes a reader-control gap. It establishes neither a throughput improvement
nor a whole-process memory ceiling. Query filters and column selection are applied
above the native stream. The later pinned-schema reader checkpoint qualifies a limited set of native
schema changes; public lifecycle schema evolution remains unfinished.

## Source occurrence checkpoint

`storage::read_occurrences` adds an exact native file path and zero-based physical
row position before relational processing. Consumers bind that tuple to the
verified immutable objects in their source pin. Complete coverage requires
successful consumption through the end of the stream. These locations do not
promise stable logical identity after rewriting or compaction.

The pinned native reader declares `_pos` but fails to read it; the original
[capability failure](reviews/native-position-probe.log) is retained. The adapter
uses the maintained native whole-file reader, visits every live file exactly
once, and checks every decoded row count. Partial, filtered, delete-bearing,
missing and repeated tasks refuse. Reserved names refuse. The later pinned-schema reader checkpoint qualifies
limited top-level schema differences while retaining occurrence correspondence.

An independent Parquet reader confirms file-relative correspondence across row
groups and batch sizes, including duplicate/null values and byte-identical files
at different paths. Tests cover malformed tasks, incorrect counts, empty input,
oversized batches and reader admission release. See the
[consistency review](reviews/occurrences-consistency.md),
[resource review](reviews/occurrences-resources.md) and
[check output](reviews/occurrences-check.log).

This supplies the source locations used by the backfill checkpoint below. Durable
logical occurrence identity and compaction are still unfinished. Native manifest
planning may continue briefly after cancellation;
wrapper permit tests do not establish upstream task quiescence or aggregate RSS.
No throughput improvement is claimed.

## Independent field-group checkpoint

`BackfillSpec` binds an exact selected source, group definition and scalar field
expressions. Logical field IDs map explicitly to the native IDs assigned by
Iceberg. The native table schema and mapping are independently checked. Expressions
must be immutable and row-local; nested computed outputs are not yet supported.

The engine looks up completed work before opening source payloads. Missing work
reads one verified source file through the native occurrence adapter and evaluates
maintained DataFusion expressions on Arrow batches. Each completion retains the
exact source object, definition, output schema, output objects and native Avro
file descriptors. Acknowledgments are bounded bundles; aggregate descriptor
assembly and final combined batches have explicit byte limits. The engine creates
one final group table from retained outputs. It does not create a cumulative group
snapshot or copy the entire source pin for each input file.

Validation checks every file/row location for complete, unique coverage. Null and
empty values remain successful outputs; failed files remain distinct from pending
files. Validation and independent readback are durable handoffs. Resumption checks
that retained objects are still present before adopting completed work. Fixture
reconstruction separately checks computed values. Malformed native group fixtures
exercise duplicates, gaps, unknown files, invalid positions and altered field maps.

Selection checks current eligibility, the exact source and its admission boundary,
and the expected target group. Backfill does not advance source admission. A newer
source blocks selection while preserving completed file work; a new request can
reuse exact unchanged source objects. Compatible sibling groups merge into the
latest selection and receive a fresh independent readback. A competing replacement
of the same group refuses its stale target. Original operation retries still return
their original recorded result.

The later coordinated-refresh checkpoint below permits source updates after group
selection. The full-rebuild checkpoint below adds source requalification after
invalidating an enriched selection; historical reads continue to work. Reuse currently depends on
the complete source schema, not only referenced columns. Physical source locations
do not supply logical identities across compaction. The current release format is
described below; no migration of older development outputs is supplied.

See [backfill tests](../tests/backfill.rs), the
[consistency review](reviews/backfill-consistency.md),
[resource review](reviews/backfill-resources.md) and
[gate output](reviews/backfill-check.log). A default-stack test failure is retained
in [the original log](reviews/backfill-check-failed-stack.log); placing the large
native preparation future on the heap fixes the overflow without raising stack
limits. Tests cover reopen/resume, concurrent attempts, independent groups, stale
targets, missing retained objects, file failures and combined-read capacity.

This is an R3 implementation slice, not completion of R3 or Q2. Complete release
verification still reads whole retained inputs and inventories. Native allocations,
arbitrary in-flight crash behavior and complete-path scaling remain unqualified; no
throughput improvement or whole-process memory ceiling is claimed.

## Process recovery checkpoint

The shared test harness terminates the actual CLI with `SIGKILL` after an
acknowledged handoff, removes external input files and resumes through a new CLI
process. Source builds and updates cover admission, processing, validation,
readback and selection. Scalar backfills cover completed file work, assembled
groups, validation, readback and selection. Tests verify values against independent
Rust reconstruction, retained source/change/group identities, historical reads,
and original-result reconciliation after a lost selection response.

Test-only work-entry observations check that resume executes exactly the missing
phases. They do not infer reuse merely from unchanged completion records. The
file-completion fixture explicitly proves all source files were acknowledged
before termination. Both handoff and resume waits are bounded; timeout terminates
and reaps the fixture worker. A shared harness replaces the build-only subprocess
code without adding a production scheduler or recovery store.

See [the semi-formal review](reviews/process-recovery-review.md) and
[gate output](reviews/process-recovery-check.log). These tests establish recovery
from worker loss at the named acknowledged boundaries. They do not establish
power-loss durability, arbitrary mid-write or catalog-commit failures, remote
recovery, or the full Q1–Q8 acceptance matrix.

## Coordinated scalar-refresh checkpoint

`update` now prepares its source changes and refreshes every group selected by its
exact base. The same scalar preparation helper serves standalone backfill and
coordinated updates. An unchanged immutable source file reuses its prior computation;
a rewritten file receives complete new scalar output. Removed source files supply
no rows to the new group. The old complete selection remains readable until one
guarded lifecycle commit selects the new source and all groups together.

Applied source changes remain in `ChangePin`; per-value derived changes and broader
history composition remain unfinished. Complete source processing, each group's
files/assembly/validation, and final readback remain separately recoverable. Planning
reports whether group preparation remains. Routine refresh can select its complete
captured source while newer work is admitted; strict backfill still refuses when
its source admission boundary advances. Both paths retain completed work on conflict.

`GroupDefinition` separates material scalar logic from the operation establishing
complete coverage. Each group retains that actual request and exact source binding;
reused files retain their original object identities. Release format 4 adds the
exact composition-parent manifest. Independent verification checks immediate parent
membership, recipes, input references, source boundary and inherited bindings/table
pins. It does not walk the entire history or reconstruct an inherited update's
historical applied-write proof; that R4 lineage work remains open. The verifier
retains one already-checked base manifest across groups sharing it, avoiding repeated
reads of the common update base. Aggregate release metadata remains bounded.

No-op detection uses the closed writers' actual added/removed row counts. When both
are zero, the exact source pin and prepared group tables remain unchanged. Tests
assert those identities and observe no scalar-file evaluation. This removes native
candidate append/pin work for that case; full source discovery and required release
verification remain, so no end-to-end speedup is claimed. The original failing
identity assertion is retained in [the no-op log](reviews/refresh-noop-failed.log).

An invalidated base source refuses before admission. Recomputing its scalar fields
does not requalify the original shaping query. The later full-rebuild checkpoint
adds that path; later related-refresh checkpoints add joins and summaries. Nested
computed values and complete cutover under continuous ingestion remain unfinished. Material-identical retries may supply
new input locators; release history preserves the original admitted request and
reuses completed readback.

See [refresh tests](../tests/refresh.rs), the
[consistency review](reviews/refresh-consistency.md),
[resource review](reviews/refresh-resources.md) and
[gate output](reviews/refresh-check.log). This is a bounded R3/R5 slice, not full
Q2 or Q5 qualification and not the large Comments performance gate.

## Full source-rebuild checkpoint

An optional exact `RunSpec.base` selects the group definitions to preserve during
a complete source rebuild. The existing plan/run/resume services prepare the new
source and every scalar group through the shared preparation path, verify them,
then select one complete release. No additional command, dependency, scheduler or
rebuild service is introduced. An unbased build still refuses to replace selected
groups; an explicit rebuild refuses missing source fields or incompatible expressions.

Full rebuilds accept the complete replacement input. The new release retains that
input and an exact composition parent; historical input evidence remains with old
releases. Independent verification checks that parent, exact group definitions,
fresh operation/source bindings, complete occurrence coverage, advancing source
boundary and nonregressing eligibility. A changed definition requires increased
eligibility. After invalidation, rebuilding recomputes the source and groups under
the new qualification before incremental updates or further backfills can continue.

Explicit rebuilds fence both the selected base and the current source admission
boundary. Later ingestion prevents a stale cutover. Operation identity includes
the base, while source computation reuse depends on input bytes, query, definition,
processor and eligibility. A newly based request can therefore reuse identical
source work and prepared scalar groups. Qualified completed work retains its
original provenance even if eligibility or selection changes before acknowledgment;
the current execution epoch still fences recording by obsolete actors. This does
not qualify restore-time storage fencing, which remains R6 work.

Tests compare full values, duplicate counts and null/empty distinctions against
independent Rust reconstruction. They cover changed shaping/definition, following
updates and backfills, malformed release claims, retries, stale admission, failed
groups and real process loss after source, partial group and release handoffs.
The existing shared fixture and scalar preparation helpers avoid parallel
implementations. This establishes reuse and recovery behavior, not throughput.
Broader schema evolution, continuous-ingestion catch-up, related-input dependencies,
destructive maintenance, provider qualification and the full performance gate remain
open. See [rebuild tests](../tests/rebuild.rs), the
[in-flight completion race](../tests/rebuild_completion.rs),
[consistency review](reviews/rebuild-consistency.md),
[resource review](reviews/rebuild-resources.md) and
[full gate output](reviews/rebuild-check.log).

The first regression run exposed transient `workspace_busy` failures during
fixture close/reopen; [its output is retained](reviews/rebuild-regressions-failed.log).
A deterministic duplicated-descriptor test reproduced an OS-lock lifetime gap.
Workspace ownership now explicitly unlocks before closing its descriptor. The
full gate passes with that fix. Descriptor inheritance is a plausible explanation
for the transient failures, not a traced cause of that particular run. See the
[lock review and failed regression](reviews/workspace-release.md).

## Accepted-history checkpoint

Successful selection now commits a `Succession` row alongside the result and
authority. Its predecessor is the actual selected version under the same guard,
including when an independent group merges with a sibling selected after its
request was captured. Admission order and composition parents do not determine
accepted order. Retrying a completed operation leaves the recorded link unchanged.

`prove_succession`, CLI `check-succession` and HTTP `POST /succession` inspect one
captured lifecycle snapshot. The traversal follows the observed head through the
target to the prior version, requiring exact manifest identities, matching native
commit revisions for each result/link pair, and strictly decreasing predecessor
revisions. The authority retains its exact selection revision across later
admissions and invalidations. Both proof and selection require the head link to
match that revision and its recorded source boundary and eligibility. A rewritten
head pair or an authority predeclaring a future commit therefore refuses. Both
endpoints must belong to that selected chain, even when equal.
Missing, malformed, backward, rebound or orphaned history refuses. Proofs report
selection counts, not elapsed time or completeness of external acquisition.

The proof reads no payloads and does not depend on optional change tables. It is
an ordering prerequisite for related-input refresh, not an availability check or
a complete change-history certificate. Existing experimental workspaces without
these records refuse; ordering is never inferred from operation names or source
admission numbers. Payload verification, retention and restore remain separate.

Traversal retains the existing immutable row cache and its memory reservation
through cooperative yields. A reviewer identified that reentering the single cache
for every link could replay the entire lifecycle after unrelated dataset activity.
The corrected path uses a retained handle without adding another cache or index.
A regression removes the old native directory after cache replacement and checks
both readable retained rows and reservation release. Serialized-byte and link
bounds limit history traversal; full cold replay still grows with retained history.

See [succession tests](../tests/succession.rs), the
[consistency review](reviews/succession-consistency.md),
[resource review](reviews/succession-resources.md), and
[gate output](reviews/succession-check.log). Related-generation declarations are
described below, followed by joins, guarded output selection and summaries.

The first full gate stopped when a recovery fixture inherited the production
32 GiB scratch profile while available disk approached that size. The
[failed output is retained](reviews/succession-disk-failed.log). The shared
process-kill harness now writes an explicit small-fixture resource profile and
passes it to both the interrupted worker and resume. Production resource limits
are unchanged; these fixtures do not qualify the production performance target.

## HTTP bulk-read checkpoint

Exact-version routes stream a native field group or the combined source/scalar
view as Arrow IPC. The shared group reader supplies both HTTP and catalog-free
CLI reads, removing the CLI's separate verification/reader setup. The catalog
resolver now checks that the returned dataset and both version identities match
the requested path and describe a verified result; another valid release cannot
satisfy a rebound catalog key. Response headers identify the captured version and
manifest digest. No live-head lookup changes a read midway through transmission.

The maintained Arrow stream writer emits the schema, successive batches and a
successful end marker only after source exhaustion. Each body poll requests at
most one source batch. There is no application producer queue or detached encoder.
The capped output sink reserves capacity before growth, and each outgoing
`Bytes` owner retains its reservation until its last clone drops. Cancellation
drops the native stream and encoder; the response retains local workspace ownership
while in flight. Engine-native cancellation and network buffering outside this
adapter remain subject to their existing qualification limits.

The codec rejects top-level and nested Arrow dictionary schemas. Review found
that the native writer can retain old dictionary buffers across batches, which
would require additional accounting beyond this implementation's fixed encoder
admission. The maintained schema traversal detects those types before streaming;
each actual batch must also match the declared schema. Ordinary native Iceberg
output remains supported. Arrow constructs temporary encoded data before the sink
checks its wire size. These reservations are admission controls, not an established
whole-process memory ceiling or hard bound on upstream allocations.

The response starts only after full retained-release verification. Streaming
removes whole-result transport buffering; it does not make verification or time
to first byte independent of population. Initial failures return JSON refusals.
After headers, source or encoding failures abort the body without a successful
end marker. Consumers must require successful HTTP completion; a parser accepting
a truncated Arrow prefix is insufficient. Empty results still carry their schema.

Tests exercise exact historical and computed-group values, duplicates/nulls,
empty schemas, catalog-free CLI reading, rebound identities, demand-driven source
polling, canceled responses, cloned-byte reservation custody and failed bodies.
The [initial fixture failure](reviews/http-read-first-failed.log) assumed a 64-bit
length without declaring that result type; the fixture now explicitly casts it.
See [HTTP tests](../tests/http_read.rs), [encoder tests](../src/http/ipc.rs), the
[consistency review](reviews/http-read-consistency.md),
[resource review](reviews/http-read-resources.md) and
[gate output](reviews/http-read-check.log). Tests use the Axum router/body and
native readers; real-network transport performance and the Comments gate remain open.

## Related-input generation checkpoint

Source manifests retain a producer-owned `SourceRelationship`. Omission inherits
a specified base; new unbased sources default to Independent. Rebuilding a coupled
source or changing a selected relationship requires the exact selected base.
Updates and backfills preserve the declaration. Independent release verification
checks that the declaration agrees with the request and inherited parent.
The greenfield manifest/processor format advances to 5; earlier experimental
outputs need rebuilding rather than a compatibility layer.

`seal_generation` commits exact accepted versions under one family/generation.
The first seal also fixes the family's member dataset set in the same commit;
later seals cannot silently drop a member. New membership requires a new family.
Identical retries reconcile the original before opening manifests or payloads,
including after source invalidation. Fresh sealing and capture require current
producer eligibility. Source and family locks are acquired in one sorted distinct
set; this is a local ownership guarantee, not a qualified multi-host transaction.

`capture_inputs` returns exact version references, family membership and required
seals. Every consumed member intersecting a seal must match it, even when an older
manifest declared itself independent or belonged to another family. A subset is
allowed; additional unrelated independent sources remain independent. Captured
coverage can become stale afterward and does not itself select a derived result.
The CLI and HTTP invoke these shared operations. Catalog-free `verify-inputs`
hashes the captured manifests and applies the same relationship rules; it does
not verify payloads, current eligibility, acquisition completeness or authorship
of user-supplied coverage metadata.

Backfill and updates create new version identities, even when source bytes match.
They cannot replace a member of a sealed generation. A based rebuild can associate
unchanged qualified source/scalar work with a new generation, followed by sealing
the final exact versions. Tests assert the same source table and scalar Parquet
objects survive that association change. No timing improvement is claimed.

Review caught a shrinking-family consistency gap and an unbuffered manifest read,
then resource custody and size-bound issues. The corrected reader admits a fixed
buffer, checks actual size, reads the declared bytes plus an overflow probe, and
hashes/parses those same bytes. The blocking worker retains both byte and metadata
reservations after caller cancellation. Metadata charges are cumulative across
the input vector and seals. Existing cold lifecycle replay still grows with
history; these controls do not establish a whole-process memory ceiling.

See [generation tests](../tests/input_generations.rs), the cancellation/lock cases
in [the input module](../src/engine/inputs.rs),
[consistency review](reviews/input-generations-consistency.md),
[resource review](reviews/input-generations-resources.md), and
[gate output](reviews/input-generations-check.log). This implements an R5/Q5
prerequisite. The following checkpoints add native joins, contributor relations,
captured/latest output guards and incremental discovery. Full Q5 publication remains
unfinished. Cleanup stays
disabled; retention must account for seals and captured dependencies before it
can retire their objects. No remote or Comments performance claim follows.


## Native join refresh checkpoint

`RefreshSpec` consumes exact left/right native source versions with explicit typed
equality keys, null matching, inner/left/full membership, projected fields and
partner cardinality. Processing uses the shared native occurrence reader and
DataFusion; a semi join checks declared cardinality before writing a potentially
multiplicative result. The full source result retains contributor positions, and a
separate native evidence table retains their exact multiplicities. Combined reads
hide these internal fields; source and evidence readers remain available through
Rust, CLI and HTTP. Format 6 adds the related binding and contributor relation.

Every expensive phase retains its result separately. Final selection takes the
sorted output, source and family locks; checks exact output base, definition,
eligibility, seals and equal/forward accepted input succession; then commits the
release, succession and observed input progress together. Captured mode permits
newer producer heads and reports lag. Latest additionally fences immediate heads
for that refresh operation. Later backfill preserves exact related bindings and
checks current dependencies under Captured semantics. It does not inherit a
permanent Latest promise. A no-op refresh advances the source boundary and fences
an older backfill even when its source table is reused.

Independent readback uses an iterative, bounded semantic dependency graph. Exact
identity conflicts and cycles refuse; shared dependencies are verified once per
request. Composition history retains its existing shallow checks. Verification
compares complete typed rows, including contributor positions, using signed grouped
counts; a second comparison checks the evidence relation. Cached readback checks
the dependency closure's object availability. Final selection retains that same
bounded graph while checking ancestor eligibility, so invalidating an indirect
source also fences new derived selection. Offline verification checks retained
semantics and relationship declarations, not present eligibility, catalog acceptance
or current heads.

The implementation reuses the existing phase, selection, writer, native reader,
backfill and HTTP stream services. The DataFusion runtime moved into the shared
Budget so catalog-free relational verification uses the same finite pools. Native
probes establish shared memory admission and normal spill-file accounting/custody.
They also reproduce a DataFusion 53.1 quota-overrun bug: the global disk charge is
updated before the per-file charge, so dropping a failed spill leaves a phantom
charge. Reopening the runtime restores capacity. Automatic in-process recovery and
a hard instantaneous scratch ceiling remain unqualified; no custom spill manager
was introduced to mask that dependency gap.

Review corrections include parenthesized composite null predicates, reserved
verification aliases, symmetric offline parent rules, early cardinality refusal,
transitive availability/eligibility and shared runtime ownership. Blocking object
checks now share one helper and retain their copied inventory's admission until
the worker exits, including after cancellation. A controlled queue/cancel test
checks both availability and hash-verification workers.

The initial test and later rebuild regressions hit default-stack overflows.
Boxing the optional related binding keeps it out of ordinary pins' inline layout;
the native [before](reviews/native-refresh-layout-before.log) and
[after](reviews/native-refresh-layout-after.log) probe measured `DatasetPin` shrinking
from 1,848 to 1,160 bytes and `Authority` from 3,872 to 2,496 bytes on this target.
JSON shape stays unchanged. Large preparation, verification and selection futures
also use heap storage at application boundaries. See the retained
[initial failure](reviews/native-refresh-first-failed.log),
[rebuild regression](reviews/native-refresh-regressions-failed.log),
[catalog-race regression](reviews/native-refresh-race-stack-failed.log),
[join fixtures](../tests/related_refresh.rs),
[consistency review](reviews/native-refresh-consistency.md),
[resource review](reviews/native-refresh-resources.md), and
[gate output](reviews/native-refresh-check.log). Fixtures include actual CLI
process termination after each acknowledged refresh phase and assertions that
resume runs only missing work. The direct Rust matching oracle checks projected
values and exact pairs independently of the generated SQL.

This full-join reference also qualifies the incremental checkpoint below.
The summary checkpoint below adds aggregation. Computed input groups, general
evidence-only changes,
logical identity across compaction, full maintenance/restore and remote publication
remain open. Deliberate many-to-many output can still exceed local storage; there is
no output-size bound. Verification scans full inputs/results and may execute the
join repeatedly across required phases. No timing gain or 10M acceptance result is
claimed. Cleanup remains disabled and must preserve these cross-dataset references.


## Incremental equality-join checkpoint

`RefreshMode::Incremental` keeps the same lifecycle services and adds one explicit
work choice. It requires an exact selected related base and unchanged join recipe,
producer roles, native source schemas and output qualification. The offline reader
checks the same provenance rules, including equality of the composition parent and
change base manifest. Unsupported changes require an explicit full refresh.

Changed-file discovery captures each old/new native inventory once and streams
keys from removed and added files. It materializes the union of those keys, expands
them across both old and new inputs, and prepares old output contributor tuples
once. The shared replacement writer then shares unaffected output files and emits
the actual removed/added rows while computing the scoped join once. Evidence uses
the same replacement writer and applied tuples. New source versions with identical
files retain exact output/evidence pins and empty changes while advancing coverage.

This deliberately treats rewritten files as changed occurrences. It has no durable
index or general incremental SQL compiler. Discovery and verification still scan
broadly; file sharing proves fewer rewrites in the fixtures, not delta-proportional
elapsed time. Counters cover direct changed-file extraction and replacement writer
work; they omit scoped relational scans, tuple discovery, cardinality checks,
metadata and independent verification. They are not total I/O measurements.

The shared change reader now verifies removed-row membership and the duplicate-aware
equation `after = before - removed + added`. It reads the prior source shallowly,
without recursively replaying historical deltas. Preparation checks retained base
manifests and old source bytes without reconstructing the old join. Final independent
verification still reconstructs the complete target join.

The new source-intent oracle exposed a pinned DataFusion 53.1 defect: dynamic join
filters use ordinary min/max and membership predicates for null-safe joins, which
can discard null matches during Parquet reading. The timing-dependent failure also
left stale right-only rows in a derived result; verification refused selection.
The shared query configuration disables only join dynamic-filter pushdown. Other
pruning remains enabled. Re-enabling this optimization requires an upstream fix
and qualification; its performance cost has not been measured. See the retained
[source oracle failure](reviews/incremental-refresh-source-oracle.log) and
[tuple diagnostic](reviews/incremental-refresh-empty-side-diagnostic.log).

[Incremental fixtures](../tests/related_refresh/incremental.rs) compare source-update
intent, joined values and contributor pairs against direct Rust reconstruction.
They exercise first/last partners, simultaneous producer changes, moved keys,
duplicates, partial nulls, empty inputs, exact no-op adoption and unchanged-file
bytes/modification times. The [computed-group fixture](reviews/incremental-refresh-computed-groups.log)
also checks that selected scalar groups preserve unchanged files and exact no-op
pins across incremental joins. Real killed workers resume processing/evidence/readback
handoffs; rebased work cannot reuse a prior base's delta. Rehashed forgeries test
balanced foreign removals, unsupported incremental provenance and rebound manifests.
Disk refusal preserves admission for a retry under a smaller scratch profile.

See the [consistency review](reviews/incremental-refresh-consistency.md),
[resource review](reviews/incremental-refresh-resources.md) and
[gate output](reviews/incremental-refresh-check.log). The full R1–R7 lifecycle,
10M Comments gate, maintenance/restore, provider publication and Dagster remain open.


## Grouped and global summary checkpoint

`RefreshTransform` distinguishes equality joins from aggregation while retaining one
refresh request, lifecycle and transport path. The manifest/processor format is now
8; earlier greenfield artifacts require rebuilding. Summary inputs name one exact
native source group, including a joined source. Group keys support Boolean, Int32,
Int64 and Utf8. Null keys group together. Fixed-state functions provide occurrence
count, non-null count, and signed Int32/Int64 sum/min/max.

DataFusion 53.1 SUM uses wrapping addition even for Decimal128. This implementation
casts signed operands to Decimal128(38,0) before summing and limits verified input
rows to `i64::MAX`. Every partial sum is therefore below decimal precision 38 in
magnitude. Global empty input has one zero-count/null-measure result; grouped empty
input has no results. Floats and variable-width min/max remain unqualified.

Incremental summaries reuse changed-file discovery and the existing replacement
writer. They replace every complete affected group, even if its values remain
equal, then replace contributor evidence using both sides of the applied change.
The evidence table stores group keys and exact input occurrences, never growing
arrays inside a row. For global summaries, file membership determines scope without
payload key extraction; unchanged input does not fabricate an empty-group replacement.
New coverage with unchanged files retains exact source/evidence pins. Selected
scalar groups reuse unchanged source-file computations.

Independent verification reconstructs complete typed values and the exact
contributor multiset from pinned inputs. A summary over a join retains its ancestor
join verification cost. The catalog-free fixture resolves summary contributors to
joined occurrences and then original input tuples; there is no general lineage
traversal API yet. Count over a join includes fanout, not distinct source comments.

Contributor replacement streams once, but its removal/addition key view can be read
again during the replacement semi join. These are summary-change rows. Input reads,
independent verification, metadata and other relational scans still exceed the
partial writer counters. No delta-proportional latency or complete-path performance
claim follows from file reuse. The temporary discovery inventory is released before
replacement to avoid retaining duplicate metadata reservations.

See [summary fixtures](../tests/related_refresh/aggregation.rs),
[consistency review](reviews/summary-consistency.md),
[resource review](reviews/summary-resources.md), and
[gate output](reviews/summary-check.log). The initial module-path failure and default
stack overflow are retained with the qualification logs. Join-specific preparation now lives outside the shared incremental dispatcher,
removing its stack cost from summaries. Transform futures use heap storage like
existing native refresh boundaries; tests keep the default stack.
The full lifecycle, maintenance/restore, real-provider publication, Dagster and
10M Comments gate remain open.

## Pinned-schema reader checkpoint

All shared native reads now select the current schema from the exact retained
metadata, including ordinary batches, occurrence reads and selected-file reads.
A native regression demonstrated that Iceberg 0.10.1's name-only transformation
can relabel equal-typed columns after a reorder without moving their values.
The adapter requests Iceberg's transient `_file` column, which forces its
field-ID-aware transformation, validates that column's ID and type, and removes
it before application processing. It uses the maintained reader and shares the
original value buffers; there is no custom decoder or durable schema index.

The adapter qualifies top-level rename, reorder, removal and nullable primitive
addition. Type, requiredness, default interpretation and nested structural
changes refuse. The native metadata-fork probe preserves original file bytes,
field IDs and old reads, then appends new rows with explicit null, zero and empty
values. Other probes cover same-name replacement with a new ID, all selected
fields absent, equal-typed reorder through DataFusion, occurrence correspondence,
selected-file reads, malformed virtual columns and transient batch accounting.

The transient native column participates in the existing byte check. Native
batch rows are limited to `i32::MAX`, matching its run-end representation. When
all selected fields are absent from a physical file, the maintained reader still
decodes all physical columns before producing the new values. The output check
does not bound that temporary decoding memory or establish column pruning.
No performance or whole-process memory result is claimed.

This is a reader prerequisite for R3. The following checkpoint adds ID-preserving
replacement candidates. The scalar binding checkpoint below adds ID-bound computed
dependencies. Public schema operations, durable absence semantics and guarded
schema cutover remain unfinished. Native
metadata changes alone do not establish that lifecycle behavior. See the
implementation reviews and raw qualification logs under [reviews](reviews/).

## Schema-preserving writer checkpoint

Replacement candidates and their removed/added change tables now retain the exact
native schema, including sparse and nested field IDs, schema history and the highest
allocated field ID. Dropping a field cannot make its ID available again. Candidates
receive new table identities and protected output directories; inherited snapshots,
references and statistics are cleared. Native metadata-log entries remain because
the maintained builder has no removal operation. They do not select candidate
results or add snapshots to its read dependencies.

The replacement writer consumes one checked file inventory. Discovery reads only
the requested key columns; affected files then use the same native adapter to read
complete values. Unchanged file descriptors and bytes are shared. These value-only
reads avoid occurrence-locator allocation and accept ordinary source fields named
like internal locator columns. The native all-requested-fields-absent fallback
described above still applies. Verification and hashing remain broader than the
changed rows, so this is not a claim of update-size-proportional latency.
The selected-file provider does not preserve the previous raw Parquet provider's
predicate/statistics pushdown. Column skipping is verified; the complete-path
performance tradeoff has not been measured. Additional provider machinery should
follow a measured need.

Candidate metadata encoding stops at the declared byte limit before creating a
metadata object. Admission spans preparation, durable writing and native catalog
registration; a canceled blocking worker retains its allowance until it exits.
These checks bound application admission and encoded output, not every allocation
inside native metadata cloning or parsing.

Fixtures compare exact values and change membership against independent Rust
expectations after rename/reorder, same-name replacement with a new ID, null keys,
empty results and no-op updates. They check stable nested IDs, allocation beyond
retired IDs, unchanged file bytes and modification times, key-only reads with a
corrupted unselected payload, reader cancellation and metadata capacity refusal.
The writer remains an R3 prerequisite; public schema evolution and its lifecycle
cutover are still open. See the [consistency review](reviews/schema-write-consistency.md),
[resource review](reviews/schema-write-resources.md) and
[qualification output](reviews/schema-write-check.log). The full check exposed a
paired-writer fixture reaching a later readback limit; its replacement uses an
oversized affected value under the same allowance. All earlier suites passed, and
the corrected updates suite, formatting, Clippy and doc tests passed afterward.

## Scalar input-binding checkpoint

Computed groups now retain the native field IDs selected by their original SQL
input names. Renaming or reordering a consumed source field preserves its meaning;
reusing the old name for a different ID does not redirect an inherited expression.
Freshly authored SQL resolves against the current names. Full source rebuilds
explicitly map prior IDs through the prior current names into the new schema;
missing names refuse. This correspondence does not yet preserve global source
IDs across full rebuilds.

Per-file reuse depends on exact source bytes, the expression and output definition,
resolved input IDs, and consumed native field types, requiredness and defaults.
Unrelated fields, root display names and ordinary documentation changes no longer
invalidate those computations. Expressions using DataFusion's `arrow_metadata`
also retain documentation as a material dependency. Nested schema details remain
conservative dependencies. Preparation keys additionally bind the exact source
metadata and resolved inputs, preventing different interpretations of the same SQL
from sharing a prepared group. Output-field presentation changes and equivalent SQL
rewrites do not yet share computation.

Independent verification reconstructs scalar values from the exact source and
compares them by physical occurrence with null-safe equality. It also proves each
binding through actual owning selections to the original authored backfill.
Rehashed intermediate records cannot switch input fields or invent a different
operation kind. Cached readback still requires the binding manifests and origin
schemas. Release format 9 records the binding; earlier experiment outputs have no
migration path.

One request-local cache shares exact ancestor manifests across groups, enforces a
combined serialized-byte limit, rejects conflicting references, and releases its
admission when the request ends. Native schema metadata still uses the existing
`open_pin` path and can be reread for each group/history step outside that manifest
allowance. This is not a bound on total ancestry I/O or process memory. The new value
check reads complete current source/group values and joins them; computation reuse
does not eliminate that verification cost. No end-to-end speed claim is made.

Fixtures establish actual native file-completion reuse after rename/reorder,
different results for newly authored same-name SQL, full rebuild correspondence,
metadata-dependent expressions, opposite Boolean errors, redirected history and
missing dependencies during cached resume. The full repository gate passed. A
subsequent test-only cache/admission probe was checked with formatting, all-target
Clippy and the library suite. See the [consistency review](reviews/scalar-binding-consistency.md),
[resource review](reviews/scalar-binding-resources.md),
[full gate](reviews/scalar-binding-check.log) and
[final library gate](reviews/scalar-binding-final-check.log).

The following checkpoint adds public root schema edits. Durable absence/default
interpretation, continuous-ingestion catch-up and the full Comments performance
gate remain open.

## Public schema-operation checkpoint

`SchemaSpec` selects existing source root IDs and their names in the desired order.
Omitted fields disappear from the new view and remain in retained old versions.
The operation refuses missing IDs, consumed-field removal, computed-name collisions,
reserved occurrence names and invalid writer-definition changes before admission.
New schema admission also refuses while unresolved source work remains beyond the
selected boundary. Completing that input or explicitly abandoning every pending
admission permits a fresh schema request.
It currently supports source datasets, including their selected scalar groups;
related join/summary outputs need additional contributor/schema mapping.

The existing run service handles admission, protected preparation, validation,
group catch-up, readback and selection. CLI `plan-schema`/`schema` and HTTP
`/schemas/plan`/`schemas/run` call those same services. The exact base manifest,
active writer definition and source admission boundary guard promotion. One native
lifecycle commit selects the new schema, result and writer definition. Old workers
refuse after cutover. Later input admission prevents stale cutover; that input and
completed schema phases remain retained for explicit recovery. This does not yet
implement continuous-ingestion catch-up or a bounded write fence.

Native metadata retains snapshots, field IDs, historical schemas and the allocation
high-water mark. A fresh candidate UUID/location isolates its metadata; source and
computed Parquet files keep their exact references, owners, bytes and modification
times. Retained SQL keeps its original input IDs; new Backfill and Update operations
use the selected names and writer definition. No separate schema registry, operation
journal or query framework was added.
The typed schema request and exact parent manifest describe this change; it does
not fabricate removed/added value rows for a presentation-only edit.

Independent verification reconstructs the permitted native metadata edit and uses
native equality to include private snapshot references and other native state.
Only the generated UUID/location and update timestamp may differ. The maintained
builder has no timestamp setter, so one bounded serialization roundtrip normalizes
that clock for comparison. Metadata comparison is admitted before allocating its
borrowed inventory maps; the blocking worker retains its reservation after caller
cancellation. This admission does not bound every allocation in native metadata
opening. Cached related readback also checks the schema input's original metadata,
even when that input has no computed groups.

The source writes are metadata-only. Pinning now preserves inherited digests;
creation uses routine checks and serialized readback. Explicit deep verification
retains complete value checks. Rename latency independent of population, total I/O
savings and whole-process memory bounds have not been measured. Native metadata
handles now share the maintained `Arc<TableMetadata>` before creating the one
candidate copy, avoiding redundant owned clones. A diagnostic debug-build probe
measured smaller schema-transition futures; it is not a process-memory result.

Full gates exposed stack overflows while parsing generated compound-key join SQL
in summary replacement and generated scalar comparison SQL in refresh. These
operations now use maintained DataFusion expressions with literal column names,
explicit source column order and the same null-safe comparisons. This removes
recursive parsing from file matching and scalar-value comparison, using one shared
qualified-column helper. The tests retain normal stack limits. Earlier unsuccessful
debugging attempts and both extracted failure stacks remain in the review evidence.

Fixtures cover exact old/new values, duplicates/nulls/empty sources, file sharing,
renamed computed inputs, hidden fields followed by new writes, unchanged allocation
history, CLI/HTTP retry identity, forged native properties/references/high-water,
definition and admission races (including earlier unresolved source input),
missing schema history and SIGKILL at durable handoffs. The full repository gate
passed with normal stack limits, including the final cutover guards, scalar-value
forgery cases and shared replacement paths. See the
[full gate](reviews/schema-operation-check.log),
[consistency review](reviews/schema-operation-consistency.md) and
[resource and recovery review](reviews/schema-operation-resources.md).

Format 10 adds schema requests and distinguishes expected from selected definitions.
Optional/default additions, durable absence interpretation, nested evolution,
global IDs across full rebuilds and the broader R3/R7 performance gates remain open.

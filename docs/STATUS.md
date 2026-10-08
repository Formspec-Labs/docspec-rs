# Implementation and qualification status

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
| R3 Schema evolution and backfill | Typed pinned schemas; logical field IDs mapped to native IDs; scalar computed groups; complete source-occurrence coverage; per-file progress and failures; retained computation, validation and readback; unchanged-file catch-up reuse; coordinated source/scalar-group updates and full rebuilds; source requalification; guarded source/target selection; combined reads | Metadata-only evolution; nested computed values; selective material dependencies; full concurrent-ingestion cutover |
| R4 Verified changes | Full comparison preserving duplicate counts, schemas and input identity; typed removed/added tables from applied source writes; exact endpoints and key field IDs | Evidence-only changes; history composition, lineage certificates and explicit unavailable-history fallback |
| R5 Dependent refresh | Repeatable DataFusion query execution over one retained Parquet input; complete captured input selection; coverage regression refusal; complete-key source updates with coordinated scalar-group refresh and unchanged-file computation reuse | Multiple pinned related inputs; old/new-key affected-result discovery for joins; joined and aggregate-group refresh against full reconstruction |
| R6 Maintenance and retention | Retained native history; catalog-free pins; protection before preparation; all cleanup disabled | Reader/release/phase retirement; shared-owner roots; compaction; retention preview/reservation/deletion reconciliation; complete restore points and fenced fresh-catalog restore |
| R7 Operations and qualification | Shared services; CLI; loopback HTTP; plan, run, inspect, resume, read, verify, compare and backfill; combined CLI reads; structured refusals; configured budgets; fault harness; raw local experiments | Complete counters; aggregate resource qualification; broader fault tests; HTTP bulk streaming; representative Comments corpus; final local and remote performance gates; Dagster adapter |

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
| Resource controls | The application reserves reader/writer capacity; the shared DataFusion runtime bounds execution memory and spill. Whole-process memory includes allocations outside these controls. |
| Append-only lifecycle layout | Deliberately provisional. The bounded history probe exposed repeated scans; one disposable exact-snapshot cache reduced that cost. There is no durable auxiliary index. Cold lookup and retained metadata still grow with history. Warm admission and cold replay now enforce the same bound; history growth refuses new commits before acknowledged records become unreadable. |

## Delivery gates

The local foundation and retained Parquet path are implemented and tested. The
first milestone's wider capacity and durability qualifications are still open;
the Comments milestone also needs actual Comments inputs and evidence semantics.

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
Base bytes are verified before rewriting; validation and independent readback
verify the change tables. These checks establish retained integrity, while fixture
reconstruction separately checks applied value semantics.

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
above the native stream. Metadata-only schema evolution still needs its own
qualification; it is not established by these same-schema reader tests.

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
missing and repeated tasks refuse. Reserved names and a difference between the
snapshot schema and current schema also refuse pending separate qualification.

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
adds that path. Related-input joins, aggregate refresh, nested computed values and complete cutover
under continuous ingestion remain unfinished. Material-identical retries may supply
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

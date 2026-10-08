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
| R4 Verified changes | Full comparison preserving duplicate counts, schemas and input identity; typed removed/added tables from applied source, join and summary writes; exact endpoints and key field IDs; independent removed-row membership and multiset reconstruction; accepted-version succession independent of optional change files | Evidence-only changes; change-history composition, lineage certificates and explicit unavailable-history fallback |
| R5 Dependent refresh | Repeatable DataFusion query execution over one retained Parquet input; coverage regression refusal; bounded accepted-history proof; producer-declared coupled/independent inputs; fixed family membership and create-once exact generation seals; metadata-only related-input capture; native full equality joins with contributor evidence, captured/latest guarded selection, transitive eligibility checks and catalog-free reconstruction; complete-key source updates with coordinated scalar-group refresh; old/new-key affected-group join replacement; unchanged source/evidence file sharing and exact no-op adoption; typed grouped/global summaries with exact contributors and complete affected-group replacement | General evidence-only changes; computed input groups; broader aggregate functions and key types |
| R6 Maintenance and retention | Retained native history; catalog-free pins; protection before preparation; all cleanup disabled | Reader/release/phase retirement; shared-owner roots; compaction; retention preview/reservation/deletion reconciliation; complete restore points and fenced fresh-catalog restore |
| R7 Operations and qualification | Shared services; CLI; loopback HTTP; plan, run, inspect, resume, read, verify, compare, backfill and native join/summary refresh; exact-version native and combined HTTP Arrow streams; structured refusals; configured budgets; fault harness; raw local experiments | Complete counters; aggregate resource and real-network qualification; broader fault tests; representative Comments corpus; final local and remote performance gates; Dagster adapter |

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
verify the change tables, removed-row membership and exact multiset reconstruction.
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
replacement candidates. Public schema operations, ID-bound computed dependencies,
durable absence semantics and guarded schema cutover remain unfinished. Native
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

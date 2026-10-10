# Implementation and qualification status

## Plan completion

The full [implementation plan](PLAN.md) remains **incomplete**. Local processing,
recovery, incremental joins/summaries, scalar backfills, schema operations and
historical reads are implemented. The requirements table below records their
specific limits. Remaining delivery includes dependency-correction semantics and
continuous-ingestion cutover; change-history composition; remaining protection
lifetimes, maintenance integration and broader failure qualification; remote
publication and R2/Basin; broader Dagster worker recovery; and complete Q1–Q8 and
10-million-Comments qualification. Native row statistics now let the full
Comments-to-Dockets join complete, and larger Parquet groups now let both flat
exports finish. The next full attempt stopped in the independent exact oracle at
its 32 GiB scratch limit; the 1,800-second complete-path target remains unqualified.

Earlier checkpoint notes retain the limitations known at their completion. Use
this summary and the requirements table for the current overall status.

## Native query planning

Native scans now expose the selected pin's row count through maintained
`DataSourceExec` planning. Captured-file queries use their existing file index;
no payload scan is added to obtain statistics. Decoded byte sizes remain unknown,
and the existing native reader retains schema, occurrence and admission rules.
Column pushdown and cardinality-query changes remain separate work.

The [baseline plan](reviews/native-join-plan-baseline.log) builds the main join
from Comments; the [corrected plan](reviews/native-join-plan-corrected.log)
builds it from Dockets. The cardinality probe builds Dockets in both. These early
captures did not verify the enclosing manifest identity. The [final capture](reviews/native-join-plan-verified.log)
reuses production manifest admission and confirms Dockets as the build side with
279,719 retained rows, probing 10 million Comments. Physical plans establish
optimizer choice, not successful execution or lower memory use.

The [design review](reviews/native-statistics-design-review.md) identified an
upstream filter/offset/count defect. The [initial runtime check](reviews/native-statistics-initial-check.log)
reproduced zero where four rows should remain. The shared session now omits only
`aggregate_statistics`, preserving maintained join selection. This sacrifices
metadata-only aggregate shortcuts in affected sessions to preserve results.
There is no compatible published 53.x fix or narrower general-purpose setting
identified by the official-source follow-up. The native and flat-Parquet
regressions cover this path.

The [repository gate](reviews/native-statistics-full-check.log) passed before the
final review corrections. Those corrections replace a repeated file-inventory
walk with a map lookup and bind diagnostic manifests through the production
reader. Post-correction [strict lint](reviews/native-statistics-review-fixes-check.log),
[library checks](reviews/native-statistics-review-fixes-tests.log), and verified
plan capture pass. The [implementation review](reviews/native-scan-implementation-review.md)
approves the final static change.

The [new acceptance attempt](../experiments/comments-e2e/acceptance-native-statistics-20261010.json)
uses the [captured `881eb8c` release](../experiments/comments-e2e/release-build-native-statistics-20261010.json).
Its complete join/evidence/validation stage finished in 47.58 seconds, followed by
the docket summary in 18.94 seconds. Run 1 stopped after 116.53 seconds when flat
export's conservative accumulated-footer accounting exceeded the 32 MiB metadata
allowance. This is an admission estimate, not a measured serialized footer size.
The independent oracle and remaining repetitions did not run.

Sampled aggregate memory peaked at 5.06 GB, sampled scratch remained zero, and
retained logical storage reached 9.62 GB (3.50× the 2.74 GB input). This includes
partial export and completed native results; it is not final-output amplification.
The original hash-build failure is cleared for these inputs, while complete-path
acceptance remains unqualified. The next investigation targets output row-group
layout under the unchanged memory and metadata allowances.

## Flat-export row-group layout

The export writer now separates reader batches from Parquet row groups. It keeps
ArrowWriter's maintained row cap and flushes when estimated writer memory reaches
the existing batch allowance. The writer reservation, conservative accumulated
footer check and independent serialized-footer check remain unchanged. The final
pending group is charged before `finish`, which consumes the writer's completed
group metadata. No new tuning option or writer implementation is added.

The [design review](reviews/export-layout-design-review.md) ties the earlier
refusal to a deterministic group-count estimate. The small-batch fixture now
exports exact values in one group. The first [focused run](reviews/export-layout-focused-check.log)
also found an input-buffer allowance problem in the new refusal fixture; its
corrected [refusal/retry check](reviews/export-layout-refusal-check.log) reaches
the intended export guard, preserves the selected release and resumes exactly.
The [complete repository gate](reviews/export-layout-full-check.log) passes.
The [implementation review](reviews/export-layout-implementation-review.md) approves
the change, noting that final-pending-group-only refusal lacks an isolated fixture.
The [frozen comparison](../experiments/comments-e2e/export-layout-plan.md) retains
the failed baseline and resource limits. A writer memory estimate checked between
batches is not a whole-process memory bound.

The [captured `e40be01` attempt](../experiments/comments-e2e/acceptance-export-layout-20261010.json)
completed engine processing, evidence, summaries, exports and selection lookups in
about 135 seconds. The 10-million-row flat export completed in 32.67 seconds, with
sampled process-tree memory peaking at 2.00 GB. Its [retained layout](../experiments/comments-e2e/export-layout-observation-20261010.json)
is 10 row groups, a 30,192-byte serialized footer and a 2.47 GB file. Summary export
also completed. The earlier obsolete-source build was canceled before measurement
and retained separately; this attempt's captured executable matches the successful
[corrected build receipt](../experiments/comments-e2e/release-build-export-layout-corrected-20261010.json).

The full attempt failed after 239.29 seconds in DuckDB's independent exact native
join comparison. DuckDB refused another temporary block at its 32 GiB scratch
limit. Before that failure, full shaped Comments and nested Dockets/receipt values
passed; the native join comparison and later checks remain incomplete. Subsequent
fresh runs did not start. Sampled aggregate memory peaked at 23.38 GB and scratch
at 32.27 GB (30.05 GiB); sampling missed the higher instantaneous spill usage
reported by DuckDB. Retained run data is 10.59 GB (3.86× input), with peak additional
logical disk about 42.86 GB including temporary work. This clears the export
blocker, not the complete acceptance gate. The next investigation targets a more
efficient exact oracle over retained completed outputs, preserving all checks and
resource limits.

## Receipt changes without value recomputation

The [receipt interface](evidence-updates.md) replaces complete receipt groups for
stable source occurrences. It retains original input bytes, nested values,
duplicates and nulls, records actual removed/added rows, and adopts existing
source, computed and contributor tables. Source corrections preserve surviving
occurrences and prune replaced/deleted associations in a durable phase. Scalar
backfills incorporate intervening receipt selections without repeating completed
computation. Value-only dependent refresh retains its tables while advancing exact
input coverage. Opaque receipt URIs do not promise custody of external bodies.

The [focused checks](reviews/evidence-updates-focused-final.log) pass for these
behaviors, invalid keys/conflicting retries, exact receipt-phase retirement,
restore-point capture, and actual process loss after receipt processing, pruning
and validation. The many-file fixture uses managed compaction, then holds one
receipt correction and the memory allowance fixed. Projected origin reads use one
sequential reader, avoiding buffer admission proportional to the file count.
This establishes bounded reader admission in the fixture, not measured peak memory
or a complete update-cost result. Writer metrics omit source-association reads.

The [consistency](reviews/evidence-consistency-review.md) and
[efficiency](reviews/evidence-efficiency-review.md) reviews led to the sequential
reader, accurate no-work planning and pruning recovery tests. Runtime checks then
found native string-view compatibility and missing receipt-phase recognition;
both are corrected. Default-stack failures also exposed large embedded async state
and inline receipt metadata. Shared boxed futures and boxing receipt metadata like
related results reduce that state without changing serialized data or resource
limits. The measured `DatasetPin` size fell from 2,432 to 1,328 bytes; failed runs
and [type measurements](reviews/evidence-type-sizes-boxed.log) remain retained.
The subsequent full gate exposed the same stack pressure in an existing
incremental join. Isolating the new validation futures fixes the
[original regression](reviews/evidence-related-stack-verified.log) under the
default stack. The disk-refusal fixture now exercises a real input change;
unchanged values correctly reuse their tables without reserving payload scratch.
The [final repository gate](reviews/evidence-updates-final-check.log) passes
formatting, strict linting, and every library, integration and documentation test.
It includes the receipt suite after the final future correction and the existing
incremental oracle. The separate [related-refresh suite](reviews/evidence-related-final.log)
also passes. Independent reviews approve the static implementation; the primary
agent executed these runtime checks without increasing stack or resource limits.
These fixture checks do not establish throughput, remote support, the Q4 discovered
dependency-omission case, or the complete Comments acceptance target.

## Complete Comments qualification harness

The [frozen full-path protocol](../experiments/comments-e2e/protocol.md) and runner
now cover full-width Comments shaping, real docket joins, summaries, nested docket
receipts, contributor relations, native/flat selection and independent exact
readback. They share one deadline and account for aggregate memory, combined
scratch, retained output and peak additional disk. Each series captures its
executable and oracle sources before timing. Pilot results cannot pass acceptance.

The [synthetic complete-path and adversarial checks](reviews/comments-e2e-harness-check.log)
pass against a real Rust executable. They include duplicates, nulls, Unicode,
nested receipt order, unmatched rows, missing/corrupt retained input and incorrect
exported field IDs. The [independent review](reviews/comments-e2e-review.md) approves
controlled collection. A DuckDB initialization error found during qualification
was corrected before this pass.

The [input preparation](../experiments/comments-e2e/preparation-20261009.json) reuses
the frozen Comments identity and verifies the smaller Dockets/receipt members in
one digest pass each. The [first acceptance attempt](../experiments/comments-e2e/acceptance-20261010.json)
used the separately [built and captured `ce3ad6b` executable](../experiments/comments-e2e/release-build-20261010.json).
Run 1 failed after 48.76 seconds: Comments admission/shaping completed in
30.50 seconds, while the Comments-to-Dockets join exhausted DataFusion's hash-build
memory pool. The protocol retained that failure and did not start the remaining
runs. The log does not distinguish the cardinality probe from the result join;
physical-plan inspection is required before assigning the exact cause.

Sampled aggregate resident memory peaked at 9.54 GB, sampled scratch was zero,
and retained logical bytes were 5.48 GB against 2.74 GB of input (2.00×). Original
admitted files and native data account for almost all that retained size. This is
storage at failure, not final-output amplification: joins, summaries, exports and
independent reconstruction did not complete. Raw logs and workspaces remain
retained; the resource limits and acceptance criteria are unchanged.

The [documentation research](datafusion-performance-research-2026-10-10.md)
prioritizes physical-plan statistics, native column projection, avoidable
cardinality scans and output row-group layout. Statistics are now integrated as
described above; the remaining opportunities and full-path benefit are unqualified.

## Local Dagster scheduling

The [thin adapter](../adapters/dagster/README.md) submits an unchanged native JSON
request to a loopback Rust HTTP host. Stable IDs, explicit native refusals and
bounded uncertain-outcome retries preserve Rust's recovery decisions. Python
keeps no lifecycle database and returns bounded identity/status metadata.

The [recovery suite](reviews/dagster-recovery-check.log) passes with Python 3.13,
Dagster 1.13.16 and the `ce3ad6b` Rust executable. A relay drops a real native
completion response; native Dagster retry and a separate replacement Python
process both reconcile the unchanged request. Exact native pins, lifecycle rows,
retained file hashes, sizes and modification times remain unchanged. The
[independent review](reviews/dagster-recovery-review.md) approves this local scope.
The receipt route also forwards a structured native refusal. Successful receipt
materialization through Dagster, worker loss during processing, Rust host crashes
and hosted credentials/provider behavior remain separate qualification work.

## Local exports, compaction and restore points

Local exports now produce independently readable Parquet using an explicit
all-active-rows policy and an ordered list of authorized primitive columns.
The writer projects those columns, removes source metadata, hashes as it writes,
and retains materialization and bounded validation separately. A guarded catalog
commit selects the channel; completed retries return the original immutable file
without moving an advanced channel backward. Library, CLI and HTTP share these
services. Nested/combined-group disclosure, row filtering, remote upload and public
hosting remain outside this implemented slice.

Compaction rewrites selected native files and verifies exact typed values,
occurrence identities, multiplicity and order before selecting a physical
representation. Selection uses the logical source identity, so an independent
backfill can advance the dataset version without losing the shared representation.
Existing exact pins stay unchanged. Managed group/combined readers resolve the
representation through the same host admission as other reads. Source updates
capture and protect an eligible physical representation at admission, share its
unaffected files, and retain that capture across retries. Their change endpoints
still identify the canonical logical base. Compaction does not reclaim old
storage or retire obsolete representation recovery records.

The maintained native writer now bounds footer accumulation through periodic
writer closure and separately charges retained file descriptors. A very large
requested target can close earlier at the metadata bound; a target creating too
many files refuses before exhausting the declared allowance. Compaction checks
free space during streaming, including declared scratch and writer headroom.
These are conservative admission checks, not measured peak memory or storage
bounds. Exact equivalence requires a source/output comparison at this physical
replacement boundary; ordinary reads do not repeat it. The fixture assertions
establish file sharing, preservation and bounded refusal, not throughput gains.

Restore points capture the native catalog and every registered native table's
required files, including unpromised registrations that a later cleanup could
remove. Backup paths reuse existing immutable objects across checkpoints and
owners; temporary reuse dependencies remain protected until the completed point
takes over. Hard links avoid payload copies on the same filesystem. Recovery
checks each physical inode once, restores missing original names, fences old
local actors and selects a fresh catalog file. This is same-workspace Unix
recovery, not portable or off-device disaster recovery.

Capture retries use one stable reserved attempt. An incomplete attempt can
rebuild only its validated, unpublished flat files; a completed descriptor
reconciles without copying again. Explicit point retirement ends its recovery
promise. Failed shared preparation can be retired through the existing phase
API, after which the same point ID refuses further capture. Its own attempt
prefix remains conservatively protected; dedicated abandoned-prefix disposal is
still unfinished. Restore execution interruption and reuse of fresh candidate
catalog generations need further qualification.

Independent semi-formal reviews of [compaction](reviews/compaction-review.md),
[exports](reviews/local-export-review.md) and [restore](reviews/local-restore-review.md)
record the initial findings and subsequent static corrections. Review findings
led to bounded writer metadata, nonquadratic file lookup, recovery independent
of retired historical availability, complete native backup closure, stable failed
attempts and shared backup references. The expanded primitive fixture also found
UTC-label incompatibility in native writes and the missing `LargeBinary` export
case; both now preserve native types, epoch values, bytes and nulls.

The focused receipts cover typed and nested compaction, backfill races, phase recovery, primitive
export disclosure, footer refusal/retry, process-killed export handoffs, native
registration recovery, repeated/shared checkpoints, interrupted capture and
explicit retirement. These use temporary fixtures. No corpus was deleted and no
10-million-row or remote performance acceptance run occurred.

A further [independent review](reviews/compacted-updates-review.md) covers source
updates that consume compaction. It found a reuse mismatch when repeated
compactions changed physical layout after a scalar group was first computed.
Group completions now retain their logical source identity alongside the exact
physical snapshot; justified reuse survives an equivalent layout change. Missing
old correspondence proof refuses cross-layout reuse. The [focused tests](reviews/compacted-updates-focused.log)
cover the original counterexample, partial-file sharing, stable surviving
occurrences, native defaults and captured-update recovery after later compaction.
The first real post-compaction update can still recompute scalar groups across
the population because per-file progress is tied to physical input names; this
change does not establish a measured speedup.
The [complete repository gate](reviews/compacted-updates-check.log) passes for
this isolated increment, including existing schema, backfill, refresh, retention
and restore behavior.

The expanded writer exposed a default-stack regression in incremental summaries.
The [backtrace](reviews/native-writer-stack-backtrace.log) and
[disassembly](reviews/native-writer-stack-disassembly.log) identified large nested
application future frames. A shared boxed writer boundary preserves writer and
budget behavior while moving its state off callers' stacks. The original
[regression now passes](reviews/native-writer-stack-corrected.log) without changing
thread limits; an [independent review](reviews/native-writer-stack-review.md)
checks the focused change. The complete gate also exposed a duplicated inventory
and encoding reservation in physical removal; the corrected handoff keeps the
intent covered without charging moved inventory twice. No resource limit was
raised. The [complete repository gate](reviews/local-lifecycle-check.log) now
passes formatting, strict linting and all library, integration and documentation
tests. The failures remain retained with their corrections.

## Explicit local physical removal

The library, CLI and HTTP interface now share preview, execute, inspect and resume
for an explicit bounded set of owned files. Preview batches creation proofs and
uses the existing protection inventory without reading payloads. Execution holds
exclusive maintenance ownership, intersects the preview with fresh eligibility,
and records exact intent and owner unavailability in one guarded commit. It does
not end availability, phase recovery or any other protection by inference.

Affected native registrations are captured by identifier, UUID and metadata head,
including provisional tables with no completed result. The maintained catalog's
registration-only drop runs before file effects; native purge remains disabled.
Shared selected files survive when an obsolete registration also names an eligible
file. New or changed native references refuse without expanding the submitted
plan. Saved registration validation excludes lifecycle and unrelated tables.

Each still-present target gets a final bounded digest and Unix file-identity check
before exact unlink and parent-directory synchronization. This is an irreversible
effect boundary, not a hash on preview or ordinary reads. Symlink ancestry,
nonregular files and changed bytes refuse. Arbitrary simultaneous external
filesystem mutation remains outside the cooperative local workspace model.

Removal progress durably reserves enough lifecycle history for its bounded next
report and terminal reconciliation. Every owner commit and cold replay honors
that reservation. A failed report keeps terminal capacity; already-absent targets
can finish without obtaining more room. New effects require their reporting room
first. Outcome records distinguish acknowledged unlink, reconciled absence, failure
and unresolved effects; logical byte totals do not claim freed allocated blocks.

The initial execution reuses the proof captured under the same exclusive guard.
Nonterminal retries recheck current protections and native heads against their
saved exact scope. Completed retries return saved outcomes without a root walk,
hash or unlink. Typed unavailable markers remain permanent, and inspection refuses
malformed or orphaned removal records. No filesystem sweep, reverse-reference
index, separate SQL ledger or automatic maintenance was added.

The [focused service and file checks](reviews/physical-removal-internal-corrected.log)
pass for actual retired-output deletion, exact selected replacement reads after
reopen, protected previews, new owner grants, provisional native heads, reader
exclusion, corruption refusal, permanent unavailability and completed retries.
The [fault checks](reviews/physical-removal-faults.log) pass cold resume after
intent, registration removal and unlink, lost native intent/outcome replies, and
terminal reconciliation at the metadata limit. These use injected interruptions
and fresh Engine instances. Subsequent [process checks](reviews/physical-removal-process.log)
send actual SIGKILL after the acknowledged intent, registration and unlink
handoffs, then inspect and resume through fresh CLI processes. They preserve the
exact replacement values, including duplicates, nulls and Unicode, and distinguish
confirmed unlink from reconciled absence. Completed retries leave replacement
bytes at an old path untouched. The pause follows an injected service return;
these tests do not kill an in-flight native commit, hash or system call.

The [first process attempt](reviews/physical-removal-process-capacity-failure.log)
exposed overlapping maximum metadata reservations under the existing 512 MiB
fixture profile. Proof decoding, inventory capture and intent encoding now hold
their allowances only during their respective phases. Saved retries charge the
exact intent/state records; fresh inspection has its own allowance for namespace
growth. The unchanged-profile process test now passes. This corrects admission
and allocation lifetimes; it does not measure peak resident memory or throughput.

The [filesystem checks](reviews/physical-removal-filesystem.log) inject failed
unlink and failed parent synchronization through the actual adapter. Failed unlink
preserves bytes; failed synchronization after unlink returns uncertainty, and a
retry acknowledges absence only after synchronization succeeds. The HTTP fixture
also exercises successful execute, inspect and resume for an empty protected
intersection. Power loss, arbitrary in-flight failures and real-provider behavior
remain unqualified. The initial
[focused run](reviews/physical-removal-internal-initial.log),
[test compilation failure](reviews/physical-removal-internal-expanded.log),
[lint correction](reviews/physical-removal-check-initial.log) and
[fixture lint correction](reviews/physical-removal-check-fixture-lint.log) remain
retained. The corrected [complete repository gate](reviews/physical-removal-check.log)
passes formatting, strict linting and all library, integration and documentation
tests. It includes exact data-and-metadata batch removal, malformed saved native
registration refusal and shared CLI/HTTP preview behavior. The
[resource snapshot](reviews/physical-removal-resources.json) separates allocated
build-cache space from shared-volume free space; it does not measure peak usage,
dataset storage, throughput or reclaimed blocks.

The [independent review](reviews/physical-removal-review.md) identified the missing
resume check and terminal-capacity problem. Both were corrected and tested. Review
agents then hit their usage limit. A subsequent
[independent review](reviews/physical-removal-final-review.md) approves the local
implementation and the process, filesystem, transport and memory-lifetime
follow-up. The [complete follow-up repository gate](reviews/physical-removal-qualified-check.log)
passes formatting, strict linting and all library, integration and documentation
tests. The [new resource snapshot](reviews/physical-removal-qualified-resources.json)
again separates allocated build cache from shared-volume free space. Native catalog
listing allocation and the workspace-wide maintenance pause remain scaling
limitations to measure under Q7. The [compaction analysis](reviews/compaction-prerequisites.md)
records the stable-identity and representation-selection work required before
rewriting source files can preserve backfills, evidence and history.
This implementation supports explicit local Unix file removal. Automatic native
cleanup, directory sweeping, lifecycle history compaction, restore, remaining
history/protection lifetimes, remote deletion and the complete R1–R7 qualification
remain unfinished. Only temporary fixture files were removed; no retained corpus
cleanup or performance experiment occurred.

## Reader lifetime during maintenance

Engine group, evidence and combined reads now hold shared maintenance admission
from opening through stream EOF or drop. Their opening task retains custody if
the caller disconnects. Comparisons run as owned finite work under the same
admission. Maintenance and shutdown wait for these readers; internal processing
uses already-admitted helpers rather than nesting locks behind a queued writer.

Engine read admission resolves an exact available result or independently
completed readback from this host before opening the immutable selection. A
mismatched pin refuses, and new reads of a retired result return
`availability_retired`. There is no dependency-graph walk,
payload hash or full validation on this path. The existing stream and worker
mechanisms own the lifetime; no reader registry, durable lease or background
scheduler was introduced.

Standalone read and verification functions remain catalog-free. Their callers
must keep objects retained for the operation or rely on the applicable published
availability window. Saved pins identify exact content; they do not create an
indefinite availability promise. Logical retirement still preserves physical
files. Existing detached native read-only work has no retained-publication or
file-mutation authority; dropping a reader does not drain unread payloads.

The [focused checks](reviews/protected-readers-internal-complete.log) pass for
opening cancellation, EOF/drop, workspace ownership, shutdown, exact host/pin
identity and all Engine read kinds. An actual retirement race waits through the
last returned batch until EOF, then permits retirement and refuses new Engine
reads and comparison. A standalone reader still returns exact values from the
retained file. The earlier [focused run](reviews/protected-readers-internal.log)
is retained as well.

The [initial full gate](reviews/protected-readers-check-initial.log) exposed an
overly narrow result-only check: a completed readback must remain readable after
its parent is abandoned before selection. The correction uses exact result and
readback keys, including the existing Backfill manifest-digest key, rather than
scanning phase history. It also avoids creating a lifecycle table when the pin
belongs to a different host. The
[corrected abandonment checks](reviews/protected-readers-abandonment-corrected.log)
pass; independently completed work retains its original availability promise.

The [next gate](reviews/protected-readers-check-batch-failure.log) reached a
payload-size test whose deliberately tiny batch limit refused the new lifecycle
lookup before reaching the joined output. The unchanged combined-reader body now
has the same catalog-free form as group and evidence readers. Engine reads call
that shared body under reader admission; the payload test exercises it directly
with the tiny `Budget` and still requires the specific joined-output refusal.
No alternate join implementation or relaxed limit was introduced.

The corrected [complete repository gate](reviews/protected-readers-check.log)
passes formatting, strict linting and all library, integration and documentation
tests. This includes both independently completed Backfill compositions before
and after retirement, wrong-host non-mutation, and the shared combined-reader
output limit. The [independent review](reviews/protected-readers-review.md)
approves the corrected checkpoint. Both earlier full-gate failures remain
recorded above. The [resource snapshot](reviews/protected-readers-resources.json)
separates allocated build-cache space from shared-volume free space; it does not
measure peak usage, dataset storage or reclaimed bytes.

The parallel [consistency](reviews/physical-cleanup-consistency.md) and
[resource/native-metadata](reviews/physical-cleanup-resources.md) reviews define
the remaining deletion boundary: fresh bounded eligibility, exact durable intent
and owner unavailability, registration-only native table removal, exact file
effects, durable outcomes and recovery capacity. This reader prerequisite does
not enable deletion. Those effects, their fault qualification, compaction and
restore remain required for R6. No reclaimed bytes or throughput gain are claimed.

## Owner-grant retirement

An operator can now retire an exact owner-grant generation after its named
consumer has ended every binding that still uses it. Planning groups grants by
consumer and reads each consumer's lifecycle metadata once. Execution waits for
admitted workers, repeats the eligibility check under exclusive maintenance
ownership, and uses the existing retirement commit and retry reconciliation.
No dataset enumeration, external dependency expansion or payload read is needed
for this proof. Ordinary data reads and local protection propagation gain no
owner-history scan.

Live phases, results, readbacks, admitted requests and generation seals block
retirement independently. A failed handoff with no consumer table can retire
without creating that table. Reacquiring the same exact object scope creates a
new native grant generation and reuses its qualified creation evidence. Old
references refuse, and retrying an older retirement decision cannot retire the
new generation. Original grant records remain creation evidence.

Shared retirement-membership and decoded-claim helpers remove duplicated
validation. Temporary metadata reservations cover the actual binding, claim and
any retained base result, separately from bounded retained sets. These are code
and admission improvements; no throughput or whole-process memory gain has been
measured.

The [corrected retention checks](reviews/grant-retirement-internal-corrected.log)
and [transport checks](reviews/grant-retirement-transport.log) pass. They cover
fresh bindings after planning, independent phase and seal blockers, stale grant
references, lost replies, concurrent/cold retries, absent consumers, malformed
bindings and bounded request refusal. A complete owner/consumer case ends their
old promises, makes the old object unblocked, preserves its file, and reads exact
replacement values after reopening. The initial
[fixture failure](reviews/grant-retirement-internal.log) is retained.

The repository gate exposed an existing disk-test race: observed free space plus
1 MiB could become available before the real preflight. The test now requests
more than total volume capacity while retaining its admission/recovery
assertions; production capacity checks are unchanged. The
[failed gate](reviews/grant-retirement-check-disk-failure.log),
[focused correction](reviews/grant-retirement-disk-corrected.log) and earlier
[strict-lint failure](reviews/grant-retirement-check-initial.log) remain recorded.

The corrected [complete repository gate](reviews/grant-retirement-check.log)
passes formatting, strict linting and all library, integration and documentation
tests, including the disk refusal under concurrent execution. The
[resource snapshot](reviews/grant-retirement-resources.json) separates reusable
build-cache allocation from shared-volume free space; it does not measure peak
usage or dataset storage.

The [independent review](reviews/grant-retirement-review.md) approves this bounded
increment. Historical observation bindings remain conservative blockers, and
generation-seal and descendant dependency lifetimes still need explicit handling.
Physical deletion, reader protection, compaction, restore and the full R1–R7
qualification remain unfinished. No reclaimed bytes are claimed.

## Terminal-operation recovery retirement

A completed or explicitly abandoned operation can now end its recovery roots and
owned attempt-directory protections through the shared scoped retirement service.
Planning captures exact operation, request, dependency, Backfill-capture and
attempt generations. Execution waits for admitted workers, checks the full set
again under dataset ownership, and appends one durable decision. Changed sets
refuse; uncertain responses and identical retries reconcile that same decision.
Original receipts and their native revisions remain available as identity and
creation evidence.

Results and completed phases retain independent availability. An available
completed result still supports lookup and exact run/plan retries; an abandoned
operation remains abandoned. Retired recovery bindings cannot provide protection
to new work. Saved retirement validation visits its named records rather than
rescanning every attempt for each historical decision. Decoding reservations
cover the original request, result and captured attempts, including retry paths.

Completed Backfill planning now returns saved result metadata before opening old
bases, native snapshots or source files. Historical file counters are `null` for
a completed plan; unfinished plans retain exact recovery counters. Identity and
availability checks remain enforced. This removes unnecessary recovery reads;
it is not a measured throughput claim.

The [focused internal checks](reviews/operation-recovery-retirement-internal-corrected.log)
and [integration/transport checks](reviews/operation-recovery-retirement-integration.log)
pass. They cover mismatched requests, canonical attempt ownership, stale plans,
lost responses, concurrent/cold retry, unavailable recovery inputs, exact selected
values, and completed-phase reuse after parent retirement. The positive retention
case now makes an actually unused old output unblocked after its explicitly
chosen promises retire, while the independent replacement stays protected and
readable. No files are deleted and no reclaimed bytes are claimed.

The [independent review](reviews/operation-recovery-retirement-review.md) approves
this scope after corrections to terminal-request identity, Backfill completion
checks and decoded-metadata accounting. The [complete repository gate](reviews/operation-recovery-retirement-check.log)
passes formatting, strict linting and all library, integration and documentation
tests. The [initial test-build failure](reviews/operation-recovery-retirement-internal.log)
is retained alongside the corrected run. The [resource snapshot](reviews/operation-recovery-retirement-resources.json)
separates the rebuilt development cache from shared-volume free space; it is not
a peak measurement or dataset-storage result. Owner grants, seals, physical
removal and the rest of the full plan remain unfinished.

## Explicit historical availability retirement

An operator can now retire a non-current result together with all saved readbacks
for its operation. Planning captures exact native generations; execution checks
the complete set again under local maintenance ownership. A changed set refuses
without expanding the plan. Unfinished work must first be abandoned, and the
current selection cannot retire. CLI, HTTP and library calls share the existing
retirement commit and lost-response reconciliation service.

After retirement, catalog lookup, own-operation replay and fresh dependency
admission refuse with `availability_retired` (HTTP 410). An independently admitted
consumer can still resume from its retained inputs. Result and succession receipts
keep their original values and native revisions, so ordering remains provable.
Retiring availability does not revoke already held immutable pins. Completed
phases, owner grants and other dependent roots remain protected; physical objects
are unchanged.

One shared scoped `RetentionPlan` now represents phase recovery or result
availability. Its generation records, decision receipts and markers replace the
earlier phase-only greenfield representation; no released-format compatibility or
migration is claimed. Ordinary available operations check a cached metadata key
inside admitted work. Retirement checks its pointer, receipt and markers only
when that pointer exists; ordinary payload reads gain no validation scans.
Retention inspection validates each decision once, reuses its exact generation
and operation sets, and ignores only the retired independent roots and bindings.

The [independent review](reviews/availability-retirement-review.md) approves the
scoped implementation. Its authority-identity finding led to one shared check
that the current selection matches its immutable result and original revision.
An otherwise consistent alias cannot select retired availability. The initial
[internal run](reviews/availability-retirement-internal.log) exposed a marker
failure that depended on key ordering; marker membership now validates before
active roots. The [corrected run](reviews/availability-retirement-internal-corrected.log)
and [integration/transport checks](reviews/availability-retirement-integration.log)
passed, covering cold dependent recovery, full Backfill readback membership,
concurrent retries, uncertain commits, malformed records and unchanged files.
The initial [repository gate](reviews/availability-retirement-check.log) caught a
fresh-dataset race introduced by the entry check: simultaneous retries could open
the native namespace before taking the dataset lock. Both run paths now use one
shared locked entry check. The existing [concurrent-retry test](reviews/availability-retirement-concurrency.log)
passed after correction. The [corrected complete repository gate](reviews/availability-retirement-check-corrected.log)
passed formatting, strict linting and all library, integration and documentation
tests.

Unbased Build admission also stops acquiring the previous selection's payload
solely to record ordering. Explicit bases keep their existing protections. This
removes unnecessary dependency traversal and new retained state; it is not a
measured throughput or storage-reclamation result.

Owner-grant and operation/attempt retirement, physical removal, compaction,
restore, hosted adapters and full performance qualification remain unfinished.
The [next-step review](reviews/retention-next-step-review.md) identifies terminal
operation recovery and attempt closure as the next necessary extension, while
preserving independent phases and results. The [resource snapshot](reviews/availability-retirement-resources.json)
separates reusable build-cache allocation from shared-volume free space; it is
not a peak measurement. This checkpoint adds metadata and ends one availability
promise; it reclaims no bytes and does not establish the 10M Comments acceptance
target.

## Ordering evidence and retained content have separate lifetimes

Selection retains succession and input-progress as catalog evidence without
creating another payload-retention obligation for their endpoints. Actual result,
phase and captured-input protection remains in force. This removes the previous
succession scope union and rejects the proposed observed-head acquisition loop:
recording order or lag does not require retaining every observed version's bytes.
The [design correction](reviews/history-metadata-design.md) supersedes those
parts of the earlier selection-protection guidance.

Retention inspection checks succession against its original selection receipt and
progress against the result's exact captured inputs. It uses narrow metadata
decoding and never opens payloads merely to establish order or report lag.
Mutable authority now checks its exact immutable result and original selection
revision, including after later admission or invalidation. It stores no duplicate
binding, and unknown older result protection remains unknown.

The [phase/restart checks](reviews/history-metadata-phases.log),
[captured-refresh check](reviews/history-metadata-refresh.log), and
[inventory checks](reviews/history-metadata-inventory.log) passed. They cover
unavailable old or unconsumed manifests, cold restart, exact retry, malformed
receipts, retained captured inputs, and continued enforcement of older grants.
The [independent review](reviews/history-metadata-review.md) approves this scope;
the [complete repository gate](reviews/history-metadata-check.log) passed
formatting, strict linting, and the regression suite. The
[superseded candidate review](reviews/progress-protection-review.md) preserves why
the discarded acquisition design and its narrow tests were insufficient.

Existing owner grants remain protected until explicitly retired. No objects were
deleted and no reclaimed bytes or throughput improvement are claimed. The
[resource snapshot](reviews/history-metadata-resources.json) separates reusable
build-cache allocation from shared-volume free space; it is not a peak measurement.
Explicit result/readback availability retirement, grant retirement, physical
removal, compaction, restore, hosted adapters and full performance qualification
remain unfinished.

## Protection follows completed work

New source, processing, evidence and validation completions carry their input
protections in the same native commit. Scalar file, prepared-group, validation,
coordinated-group and readback handoffs use the same rule. Reused work keeps the
binding of the actual winning generation, including work originally completed by
another operation. Combining outputs combines their existing scopes; it does not
reacquire owners, walk historical dependencies or rehash payloads.

Unknown older provenance stays unknown downstream. When a bound completion is
explicitly retired and an unbound replacement appears, its old binding cannot
qualify the replacement. Readers and inventory require retirement of that exact
native generation before treating the obsolete binding as absent. Other revision
mismatches refuse. Scalar acknowledgment bundles account for copied bindings as
well as completion records under the existing metadata and memory limits.

The [focused recovery checks](reviews/phase-protection-focused.log) passed for
identical input bytes retained by different owners, exact completion reuse and
unknown replacement generations. The [complete repository gate](reviews/phase-protection-check-complete.log)
passed formatting, strict linting, and all library and integration checks, including
scalar retries/reopen, refresh evidence, schema admission and coordinated groups. The
[independent review](reviews/phase-protection-review.md) approves the scoped change.

The [initial gate](reviews/phase-protection-check-final.log) exposed fixture
problems. Static reservation accounting showed that the pre-existing scalar path
already fills the 256 MiB profile's 128 MiB shared pool before request overhead.
The combined schema/backfill fixture now uses the established 512 MiB scalar test
profile; other small-memory tests and production limits are unchanged. The
[diagnostic log](reviews/phase-protection-capacity-diagnosis.log) retains the failure.
The legacy dependency fixture now explicitly retires its bound predecessor before
installing an unknown replacement, preserving the production generation checks.
The same correction applies to the integration fixtures that replace phase and
scalar completion records. Their [targeted suites](reviews/phase-protection-legacy-fixtures.log)
passed with the original reuse and output assertions intact; the
[earlier integration failure](reviews/phase-protection-check-repaired.log) remains retained.
The intermediate [retention run](reviews/phase-protection-retention.log) and
[lint failure](reviews/phase-protection-check.log) remain retained. No corpus
benchmark, payload revalidation or cleanup was added.
The [post-gate resource snapshot](reviews/phase-protection-resources.json) records
allocated debug-cache blocks and shared-volume free space separately from dataset
storage. It is not a peak measurement; the build outputs remain reusable.

The later selection checkpoint adds immutable result bindings. The current
checkpoint above keeps those bindings, checks authority through its exact result,
and treats succession and observed lag as ordering metadata. Older unbound results
still leave dependent backfill readbacks unbound. Grants remain permanent and
physical removal stays disabled. These checkpoints do not complete retention,
restore, hosted adapters or the full R1–R7 and performance plan.

## Protected request admission and managed input reuse

New Build, Update, Refresh, Schema and Backfill requests acquire protection for
their exact dependencies before acknowledging the request. This includes the
selected predecessor, exact backfill base, captured refresh inputs and every
member of a captured producer seal. Owner acquisition runs outside consumer
locks. The consumer then rechecks its metadata boundary and eligibility before
committing the request, dependency capture and protection bindings together.
Interrupted handoffs retain the owner grants; retries reuse their exact identities.
If another compatible operation changes the consumer during preparation, admission
restarts from fresh state and rechecks its guards. Independent backfills can still
proceed; incompatible source or definition changes continue to refuse.

Inputs already retained by this workspace resolve through the owner's recorded
creation evidence. Admission preserves the original owner and file reference,
without another input copy or digest pass. Provisional files refuse before request
acknowledgment. External files retain the existing copy-and-hash path when that
work is actually missing. Refresh reuses its admitted producer coverage. The disk
preflight excludes the input-copy allowance for reused managed files; retained
source bytes still exist and count toward total storage.

Bindings can combine distinct grants from the same owner. Inspection shares work
only for identical consumer/token sets and requires exact object coverage from
that set. Later local propagation can reuse immutable committed references without
reopening owners. Acquiring a new scope still requires an owner commit first.

Review and the existing small-memory fixture caught duplicate maximum metadata
reservations. Cached rows now supply decoded-record charges from their serialized
sizes; explicit-root traversal reserves graph state separately from each known-size
external decode. Capture transfers its existing allowance to admission. This keeps
the existing 256 MiB fixture usable for based reuse and Refresh without raising its
limits. These are admission-accounting and tested code-path improvements, not
measured whole-process memory or throughput results.

The [focused admission checks](reviews/admission-protection-focused.log) and
[final engine checks](reviews/admission-protection-unit-final.log) passed. They
cover native atomic bindings, lost request responses, restart/reuse, Backfill,
complete producer seals, same-owner grant unions and missing-scope refusal. The
earlier [failed resource check](reviews/admission-protection-unit-focused.log)
and [initial gate failure](reviews/admission-protection-check.log) remain retained.
The latter caught concurrent backfills rejecting one another at admission; the
[targeted concurrency suite](reviews/admission-protection-concurrency.log) passed
after the fresh-decision correction. The [independent review](reviews/admission-protection-review.md)
approves this increment with all findings closed. The
[final repository gate](reviews/admission-protection-check-final.log) passed,
including formatting, strict all-target linting and the complete test suite.
The [resource snapshot](reviews/admission-protection-resources.json) separates
development-cache allocation from shared-volume free space; it is not a peak.

This checkpoint covers initial request admission; the section above records later
phase integration. Selected sibling composition, actual selection predecessors
and observed input heads still need binding integration. Grants remain permanently active;
legacy histories still require conservative inventory. Physical cleanup, token
retirement, compaction, fenced restore, hosted qualification and the remaining
R1–R7 work remain unfinished. No corpus benchmark or cleanup was performed here.

## Durable owner protection and generation seals

Generation sealing now acquires immutable protection from each actual file owner
before acknowledging the seal. The owner commits one batch of exact object
references through its guarded lifecycle table; the family then commits its seal
and exact token bindings together. Acquisition holds no consumer lock. Existing
member/family locks still guard final eligibility. A failed handoff leaves extra
protection, and retry reuses the committed owner records. A family that also owns
a member needs no foreign token for its own files.

The shared retention traversal collects the supplied releases' dependencies,
including history, evidence, changes and sealed siblings, without enumerating the
catalog or reading data payloads. Complete native inventories avoid reopening
native metadata. Compact paths without retained owner/digest evidence refuse.
Owner identity requires the original production reservation and exact typed
creation evidence; merely putting a file in a reserved directory is insufficient.
Protection survives retirement of the original creation phase.

The independent review caught an avoidable memory-admission floor and repeated
owner-history reads. Decoded collections now keep size-based reservations after
their larger temporary allowance is released. Inspection groups obligations by
owner/token and deduplicates direct required roots. It makes at most one additional
history read per referenced owner at the same reported metadata boundary, avoiding
both repeated reads per consumer binding and retention of every owner's full
history in memory. A changed boundary refuses the inspection. No throughput or
whole-process memory improvement is claimed from these structural changes.

The [independent review](reviews/owner-protection-review.md) approves this scoped
increment and retains the original findings and their dispositions. The
[focused checks](reviews/owner-protection-focused-ready.log) cover owner recovery,
exact ownership, provisional-file refusal and interrupted handoffs. The
[repository gate](reviews/owner-protection-check.log) passed. Subsequent
[focused inspection checks](reviews/owner-protection-final-focused.log) cover the
last review-requested assertions without repeating the full integration suite. The
[resource snapshot](reviews/owner-protection-resources.json) separates allocated
development cache from shared-volume availability; it is not a peak measurement.
Original failed compilation and capacity-test logs remain beside the passing logs.

This was the acquisition primitive and its generation-seal integration. The
request-admission follow-up is described above. Independently retained
phases/readbacks and selection progress still need protection integration. Tokens
remain permanently active here. Retry checks validate tokens and direct member
manifests, without reconstructing the complete transitive closure each time.
Token retirement needs separate complete claim-lifetime rules. Legacy histories
remain protected by inventory; missing old bindings never establish removal
eligibility. Physical removal, expiration, compaction, fenced restore, hosted
qualification and complete fresh Comments acceptance remain unfinished. No corpus
benchmark, payload copy or data deletion was added for this metadata increment.

## Exact-generation phase retirement

Library, CLI and HTTP now preview and explicitly retire specific completion
revisions. Execution waits for admitted work, records the markers and decision
in one guarded Iceberg commit, and reconciles identical retries after a lost
response. Later completion generations at the same material key remain reusable.
Retirement preserves files, attempt protections, issued readbacks and releases;
it does not establish reclaimable space or enable deletion.

Recovery resolves surviving processing or readback before upstream preparation.
Captured input references keep their original provenance. A retained column
backfill can reuse its target and compose with a newly selected sibling. Active
validation and evidence refuse a different physical processing generation until
their own completion is explicitly retired. Plans report the work actually still
needed. These changes add bounded metadata decisions, without new payload scans,
a separate state store, or an ordinary-read validation step.

The independent review corrected receipt membership, retry-marker integrity and
legacy admission provenance. Inventory validates each retirement batch once and
keeps a charged set of exact generations; ordinary reuse looks up its exact marker
and referenced receipt. Both paths reject unknown retirement fields. Receipt and
marker history still accumulates within the existing metadata allowance; large
history performance is unqualified.

The [focused recovery and transport checks](reviews/phase-retirement-focused.log)
and [adversarial unit checks](reviews/phase-retirement-unit-focused.log) passed.
The final [repository gate](reviews/phase-retirement-check.log) also covers canceled
exclusive maintenance, lost native retirement replies, concurrent retry and reopen.
See the [independent review](reviews/phase-retirement-review.md) for dispositions
and the [design note](reviews/retention-execution-next.md) for the next boundary.
The full gate passed with normal stack limits, including formatting, strict
all-target linting and the complete tests. The [resource snapshot](reviews/phase-retirement-resources.json)
separates allocated development cache from shared-volume availability; neither is
a measured peak for a corpus run.

Physical cleanup still requires durable owner protections and removal intents,
reservation closure, fresh eligibility and exact deletion reconciliation.
Compaction, fenced restore, the remaining R1–R7 work and the complete fresh
Comments performance acceptance remain open. No corpus benchmark was rerun for
this metadata increment.

## Owned worker lifetime and graceful shutdown

Catalog-using Engine services now execute in private owned Tokio tasks. A caller
that disconnects or drops its future stops waiting; the existing operation retains
its Engine, OS workspace ownership and shared maintenance guard through native
I/O, lifecycle decisions and error reconciliation. Public release and input-capture
entry points have already-admitted internal bodies, preventing nested read-lock
acquisition behind a queued maintenance writer. Unrelated ordinary operations
share admission; no global execution mutex or second worker registry was added.

Owned request metadata is sized and charged to the existing shared Budget before
cloning. The private task also retains a reservation for its inline future/closure
and task allowance. These permits bound detached queued-work admission; they are
not an exact heap or whole-process memory measurement. This adds one task and
small request metadata work per service, without another payload scan, hash,
verification or lifecycle record. Ordinary immutable readers remain catalog-free.

`Engine::shutdown` stops new catalog admission and waits for existing owned work.
The HTTP server drains it after graceful listener shutdown. Local initialization
also owns its host lock independently of the caller. Private panic or runtime
termination during unfinished private work marks the host uncertain and retains
only its OS lock until process exit; subsequent work/reopen refuses. Normal returned errors finish
custody. This does not claim that arbitrary injected adapters or detached native
readers have been drained, or establish a remote storage fence.

The [focused worker tests](reviews/worker-custody-unit-focused.log) passed for
canceled blocking work, concurrent admission, bounded detached requests, permit
recovery, resumable shutdown, actual Build/Refresh calls behind a queued writer,
and private panic. The [native catalog and initialization tests](reviews/worker-custody-catalog-focused.log)
passed for disconnected committed writes, lost acknowledgment, original-operation
retry, caller-independent setup and forced unpolled setup termination. The full
[repository gate](reviews/worker-custody-check.log) passed with normal stack limits.
See the [independent review](reviews/worker-custody-review.md) for the corrected
request-memory finding and residual qualification limits. Initial compilation
errors in private method visibility and a slice-bound preflight were corrected;
the original log remains with the passing evidence.

The [resource snapshot](reviews/worker-custody-resources.json) separates development
cache allocation from shared-volume availability. Existing corpus inputs, selected
outputs and failed attempts were unchanged. No throughput, full-process memory or
ordinary-call overhead result is claimed from these small correctness fixtures.

Physical removal, durable unavailability/removal intent, exact deletion
reconciliation, compaction and fenced restore remain unfinished. Exact-generation
phase retirement is described above. No deletion or expiration is enabled. The full R1–R7 scope and
complete Comments performance acceptance remain open.

## Shared protection inspection

`Engine::inspect_retention`, CLI `inspect-retention` and HTTP
`POST /retention/inspect` accept exact candidate object references and report a
first protecting record, the lifecycle snapshots observed, and deduplicated
logical protected/unmatched bytes. This answers why known objects remain retained
without scanning their payloads. It is an explicit maintenance diagnostic;
ordinary reads and processing do not call it.

The owned embedded catalog is enumerated once. Each dataset or family lifecycle
view is visited separately through the existing bounded snapshot cache. Typed
records cover selected/history/readback releases, general and scalar completions,
operation/request captures, related inputs, generation seals, succession,
coordinated groups and attempt reservations. Complete table/release pins supply
their retained inventories directly. Compact dependencies expand bounded immutable
release or native metadata when that inventory is not already available.

New prepared and validated backfill records retain exact source/output snapshots,
including empty inputs without file completions. Schema admission retains its
base release, and coordinated-group completion retains its source snapshot.
Legacy records still resume without rewriting their provenance; missing dependency
information refuses retention inspection. Unknown record families, discarded
fields, unsupported phase kinds and conflicting identities also refuse. Native
schema/history collections match their stable IDs when the maintained library
reorders them during decoding/serialization.

The inspection holds bounded candidate, visited-metadata and first-blocker state.
It uses a single namespace listing rather than the adapter's repeated full query
per namespace. Known pins avoid redundant native expansion; the complete-pin
fixture asserts zero external metadata reads. These are verified call-path and
fixture results, not large-history performance measurements. The maintained
catalog allocates its listing before application admission, native manifest reads
can repeat across distinct metadata generations, and the metadata-object counter
does not count all underlying requests or bytes. Whole-process memory and scaling
remain unqualified. Strict field preservation adds bounded metadata decoding and
serialization work only during this explicit inspection.

Focused tests passed for isolated phase roots, empty-source worker loss/reopen,
legacy reuse, schema interpretation, active refresh inputs, shared external
release dependencies, missing payloads, unknown metadata, native binding mismatch,
capacity refusal, and matching CLI/HTTP results. See the
[inventory cases](../tests/retention_inspection.rs),
[backfill recovery cases](../tests/backfill_dependencies.rs),
[focused result](reviews/retention-inspection-focused.log) and
[independent review](reviews/retention-inspection-review.md).
The full [repository gate](reviews/retention-inspection-check.log) passed with
normal stack limits. Early test compilation mistakes and a missing native runtime
were corrected; their focused logs remain alongside the final evidence. The first
full gate stopped at a test-only lint, corrected before the passing complete run.
The [resource snapshot](reviews/retention-inspection-resources.json) separates
development cache allocation from shared-volume capacity; existing corpus and
benchmark artifacts were unchanged.

This report is read-only and observes separate table boundaries, not an atomic
catalog-wide snapshot. Unmatched objects are not certified reclaimable. Injected
catalogs remain unqualified. All issued releases and unfinished reservations stay
protected; catalog-internal recovery state is not a removal candidate service.
Exact-generation retirement, worker exclusion, fresh plan intersection, durable
unavailability/removal intents, per-object reconciliation, compaction and fenced
restore remain required. No expiration or deletion is enabled.
The [worker-boundary review](reviews/retention-worker-design.md) supplies the next
implementation step: retain engine and maintenance ownership in an admitted task
through caller cancellation, then qualify nested calls and queued catalog effects.
The owned service boundary above implements that next step; destructive execution
still requires its separate durable rules and qualification.

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

The [retention design review](reviews/retention-inventory-design.md) established
the typed inventory and backfill dependencies implemented by the inspection work
above. Retirement of exact completion generations, worker
exclusion, durable removal intents and exact deletion outcomes remain required.
Phase retirement, physical cleanup, compaction and restore remain unfinished.
No expiration or deletion is enabled here.

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
| R2 Incremental publication | Immutable local manifest; native independent readback before catalog selection; retry reconciliation; original completed release reuse; authorized all-row primitive-column Parquet export with streaming digest, retained materialization/validation, guarded channel selection and CLI/HTTP | Nested/combined-group disclosure and row filtering; R2 uploads and independent remote readback; transfer reuse and provider qualification |
| R3 Schema evolution and backfill | Typed pinned schemas; logical field IDs mapped to native IDs; public root rename/reorder/hide and nullable scalar additions/defaults with atomic writer-definition cutover; durable supplied/omitted values; scalar computed groups; exact scalar input bindings and value verification; complete source-occurrence coverage; per-file progress and failures; retained computation, validation and readback; unchanged-file reuse keyed to consumed source fields; coordinated source/scalar-group updates and full rebuilds; source requalification; guarded source/target selection; combined reads | Nested/type evolution; related-output schema edits; global source IDs across non-evolved full rebuilds; nested computed values; broader selective dependencies and Q4 discovered-dependency correction; continuous-ingestion catch-up and cutover |
| R4 Verified changes | Full comparison preserving duplicate counts, schemas and input identity; typed removed/added tables from applied source, join and summary writes; exact endpoints and key field IDs; independent removed-row membership and multiset reconstruction; accepted-version succession independent of optional change files; occurrence-keyed receipt replacement/deletion with exact native changes | Change-history composition, lineage certificates and explicit unavailable-history fallback |
| R5 Dependent refresh | Repeatable DataFusion query execution over one retained Parquet input; coverage regression refusal; bounded accepted-history proof; producer-declared coupled/independent inputs; fixed family membership and create-once exact generation seals; metadata-only related-input capture; native full equality joins with contributor evidence, captured/latest guarded selection, transitive eligibility checks and catalog-free reconstruction; complete-key source updates with coordinated scalar-group refresh; old/new-key affected-group join replacement; unchanged source/evidence file sharing and exact no-op adoption; typed grouped/global summaries with exact contributors and complete affected-group replacement; receipt-only input succession with exact value/contributor reuse and new coverage | Evidence-dependent policy requalification and dependency correction; computed input groups; broader aggregate functions and key types |
| R6 Maintenance and retention | Retained native history; catalog-free pins; owner protection before reference admission and preparation; exact request/phase/result protection bindings; logical operation abandonment; exact-generation phase, result/readback availability, terminal-operation recovery and unused owner-grant retirement; shared-root inspection with bounded native expansion; owned caller-independent worker lifetime; Engine stream/compare admission and shutdown drain; explicit local Unix removal preview, durable reservation, native registration detachment, exact file effects and cold reconciliation; SIGKILL at acknowledged removal handoffs, controlled unlink/sync failures and independent local review; stable source occurrence identities and guarded equivalent-representation compaction for managed reads; same-workspace catalog restore points with exact native registration closure, backup reuse, explicit retirement and fenced fresh-catalog restore | Historical observation bindings, seal and descendant dependency lifetimes; compaction adoption by incremental derived writes and obsolete representation retirement; broader reader/removal and interrupted-restore process qualification; abandoned restore-prefix disposal; remote provider qualification |
| R7 Operations and qualification | Shared services; CLI; loopback HTTP; plan, run, inspect, resume, read, verify, compare, schema edits, backfill and native join/summary refresh; exact-version native and combined HTTP Arrow streams; structured refusals; configured budgets; fault harness; raw local experiments; bounded local Dagster scheduling with native completion-response loss and replacement Python worker reconciliation | Complete counters; aggregate resource and real-network qualification; broader fault tests; final local and remote performance gates; Dagster mid-operation worker loss, host crashes and wider route qualification |

The CLI exposes implemented operations only. Source updates support complete key
replacement and deletion against an exact base; unsupported transformations require
an explicit full query. Independent scalar backfill and coordinated source/scalar
updates and explicit full rebuilds are available. Incremental reuse of a globally
invalidated source refuses until a full rebuild requalifies it. General incremental
SQL remains unfinished. A retained source file establishes whole-input provenance. Explicit receipt
associations add occurrence-level evidence; neither alone establishes complete
DocSpec conformance.

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

The full performance gate remains **unqualified** after the first attempt's
hash-join memory failure; see the collection evidence above. Its scope remains a
frozen representative selection of 10 million retained Comments, deterministic
shaping, docket joins/summaries, evidence, native and flat outputs, release
selection and independent readback in 1,800 seconds on each required fresh run.
Neither a resumed diagnostic nor a smaller fixture can replace that gate.

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

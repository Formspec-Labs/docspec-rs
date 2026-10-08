# Backfill resource, custody, and simplicity review

**VERDICT: APPROVE the scoped row-local backfill checkpoint.** BR1–BR5 below
are resolved. Targeted tests cover the fixes; no unresolved implementation defect
remains in this static review. Whole-process resource and complete-path
performance qualification remain separate work.

Static review of the working tree relative to `64666e1` on 2026-10-08, applying
the semi-formal-code-review and relevant Rust guidelines. Scope: backfill
application code, expression compilation, independent coverage verification,
selective occurrence scans, combined queries, and CLI/HTTP entry points. The
reviewer did not execute tests or change implementation files. Line references
describe the final source inspected for this checkpoint. README and status claims
were also checked against the implementation and its explicit qualification limits.

## Findings

### BR1 — Unsupported source work advanced its boundary before refusing

**Severity: BLOCKER. Category: correctness / recovery. Status: resolved.**

The initial source-operation path advanced the authority boundary before rejecting
an update or build against selected computed groups. The selected source then
lagged the boundary, and later backfills could not pass boundary equality.
Scenario: select A, try an unsupported source update, then add B; B became stuck
although source selection had not changed.

The repaired path checks selected computed groups under the admission lock before
changing the boundary (`src/engine.rs:525–539`); the final selection guard remains.
The regression exercises both unsupported update and build, asserts the entire
authority is unchanged, and then successfully adds B (`tests/backfill.rs:422–466`).
The fence for genuinely admitted, supported source work remains intact.

### BR2 — Individually bounded inputs did not bound joined output

**Severity: WARNING. Category: resource control. Status: resolved.**

The initial combined reader returned joined DataFusion batches directly. Several
individually admissible inputs can combine into an output above `max_batch_bytes`.
The CLI would then write that batch without checking it.

`read_combined` now routes its final stream through `query::bounded`
(`src/engine/backfill.rs:114`; `src/query.rs:87`). The wrapper preserves schema,
owns a consumer reservation with the stream, and checks the combined footprint.
It emits typed `oversized_batch`; exhaustion, error, or drop releases its state
(`src/query.rs:91–109`). The regression measures every individual native input and
the located source, reopens at their maximum allowance, and asserts that the
failure specifically identifies combined output (`tests/backfill.rs:559`).

### BR3 — Aggregate native descriptors lacked continuing admission

**Severity: WARNING. Category: resource control. Status: resolved.**

Initially every native `DataFile` and inherited reference accumulated while the
individual descriptor reservation ended on return from `read_completion`.
Individually admissible descriptors could grow past aggregate admission before
final append or pinning rejected them.

The loop now reserves two metadata allowances through assembly, cumulatively
charges descriptors, references and per-output overhead, and refuses before
extending the vectors (`src/engine/backfill.rs:387–455`). Pending completed files
are acknowledged before the refusal. The reservation spans append and pinning
(`:469–477`); pinning retains its own inventory reservation inside its blocking
worker (`src/storage.rs:456–474`). The targeted unit test admits one descriptor
and refuses a second whose cumulative charge crosses the allowance
(`src/engine/backfill.rs:935`). This establishes the admission rule, not decoded
native descriptor RSS or a large-manifest stress result.

### BR4 — Each immutable completion lookup reloaded the catalog

**Severity: OBSERVATION. Category: performance / simplicity. Status: resolved.**

The original loop called `lifecycle.open` for each file, which checks namespace
and table existence and loads the table (`src/catalog.rs:68–75`). The loop now
captures one completion view and refreshes it after acknowledgment
(`src/engine/backfill.rs:391`, `:463`). Fresh promotion remains separate.
Concurrent work absent from that view can be duplicated; acknowledgment already
preserves the durable winner. This removes per-file catalog polling without a
new cache or index. No latency improvement is measured here.

### BR5 — Reconciliation exposed a default-stack failure

**Severity: BLOCKER. Category: resource control. Status: resolved.**

The owner's initial full gate aborted with a stack overflow in the sibling-group
integration case after reconciliation added another preparation-future wrapper.
The failure remains at `docs/reviews/backfill-check-failed-stack.log`.
`backfill_internal` now heap-pins preparation with `Box::pin`
(`src/engine/backfill.rs:248–250`). The subsequent owner-run integration suite
passes under the default stack, including the failing case and identical
concurrent operations; the successful full gate is retained at
`docs/reviews/backfill-check.log`. No larger configured
stack is required. This is observed regression closure; no exact future stack
size was measured by this reviewer.

## Patch summary

The patch adds independently stored row-local field groups. It captures an exact
source release, compiles immutable scalar expressions, reuses completion records
by source-file identity and material expression dependencies, computes remaining
files, assembles a native group, verifies exact physical occurrence coverage, and
selects a fresh composition of compatible sibling groups. CLI and HTTP handlers
call the same application methods.

The design uses the existing lifecycle authority, native file reader/writer,
maintained DataFusion physical expressions, and native Avro `DataFile` encoding.
It introduces no row-index database, persisted `FileScanTask`, generic plugin
framework, or second scheduler. The small separate preparation guard is justified:
completed computation may remain reusable after a source boundary becomes stale,
while selection has stricter guards.

## Function trace

| Function / method | File:line | Inputs → outputs and verified behavior |
| --- | --- | --- |
| `validate` | `src/engine/backfill.rs:43` | Request → structural validation; requires distinct group/operation, nonempty definitions, and distinct field identities/names. |
| `read_combined` | `src/engine/backfill.rs:77` | Exact release → streamed source plus computed fields; fully verifies release before joining on file and position. Final output passes through the consumer byte check. |
| `plan_backfill` | `src/engine/backfill.rs:128` | Request → bounded progress; opens native metadata and completion records without decoding source payloads. Checks retry identity and compiles expressions. |
| `backfill`, `backfill_until` | `src/engine/backfill.rs:228`, `:237` | Normal/test entry points → one implementation with optional durable-phase interruption. |
| `prepare_backfill` | `src/engine/backfill.rs:273` | Captured request → retained file completions, prepared table, verified release and guarded selection. Per-file cache lookup precedes `compute_file`; final composition rechecks current siblings. |
| `reserve_backfill` | `src/engine/backfill.rs:635` | Operation/purpose → durable protected directory; checks execution epoch under the dataset lock before creating storage. |
| `acknowledge_files` | `src/engine/backfill.rs:673` | Pending file records → lifecycle bundle; checks preparation authority, preserves already committed winners and clears pending records only after success. |
| `compute_file` | `src/engine/backfill.rs:701` | One immutable source object → native output descriptors and digested retained outputs; estimates disk, admits expression work, selectively decodes, writes native batches, and retains the Avro descriptor. |
| `file_key` | `src/engine/backfill.rs:831` | Processor, source object, schema, group definition and fields → eligibility-scoped reuse key. Exact source release binding stays separate, allowing unchanged-file reuse across source snapshots. |
| `read_completion` | `src/engine/backfill.rs:851` | Retained completion → maintained native descriptors; caps descriptor size, verifies output/descriptor digests, and checks unique paths, sizes and total rows. |
| `promotion` | `src/engine/backfill.rs:776` | Fresh authority plus captured source/target → accept/refuse; checks epoch, definition/eligibility, exact source and boundary, and expected same-group version. |
| `check_preparation`, `source_objects`, `check_names`, `quote`, `invalid_group` | `src/engine/backfill.rs:766`, `:823`, `:902`, `:898`, `:891` | Shared epoch guard, exact source-object lookup, cross-group name checks, SQL identifier escaping, and stable refusal construction. |
| `Expressions::compile` | `src/engine/backfill/expressions.rs:26` | Source schema and fields → physical expressions plus native schema and logical/native ID mapping; allows row-local nodes and immutable functions, rejects other expression kinds and nested computed results. |
| `Expressions::evaluate` | `src/engine/backfill/expressions.rs:117` | Located source batch → one derived row per input; preserves locator arrays, evaluates/casts values, and checks the derived batch footprint. |
| `OccurrenceScan::open` | `src/storage/occurrences.rs:209` | Native table → bounded captured native tasks; verifies the complete task set without opening data payloads. Reuses the existing whole-file reader. |
| `OccurrenceScan::files`, `read_file` | `src/storage/occurrences.rs:253`, `:257` | Captured task inventory → enumeration or one admitted source stream. The shared metadata permit survives either owner's drop. |
| `verify_groups`, `verify_group` | `src/engine/backfill/verification.rs:6`, `:64` | Release bindings → exact schema and occurrence-coverage verification using independent native reads. |
| `read_base` | `src/engine/backfill/verification.rs:45` | Retained manifest reference → size/digest-checked source binding. |
| `FileCoverage::insert`, `complete` | `src/engine/backfill/verification.rs:164`, `:189` | Decoded ranges → gap/overlap-aware completeness; coalesces adjacent intervals regardless of scan order and caps retained segment count. |
| `query::provider`, `NativePartition::execute` | `src/query.rs:26`, `:59` | Native table → finite maintained streaming provider; chooses plain or occurrence reader lazily and preserves typed errors. |
| `backfill_internal` | `src/engine/backfill.rs:241` | Request → heap-pinned preparation or reconciled exact-operation result; an ordinary loser error cannot hide concurrent durable success. |
| `charge_assembly` | `src/engine/backfill.rs:917` | Previous charge/completion → bounded cumulative charge before vector extension. |
| `query::bounded` | `src/query.rs:87` | Joined stream → schema-preserving, admitted, byte-checked consumer stream. |
| `storage::check_available` | `src/storage.rs:114` | Retained references → bounded-worker file/type/size checks before cached-phase reuse; this checks availability rather than hashing content again. |
| CLI and HTTP callers | `src/main.rs:183–215`; `src/http.rs:108–118` | Bounded request metadata → shared plan/run methods; CLI combined output streams Arrow IPC. |

Pinned native implementations were read in the local Cargo registry:
`iceberg-0.10.1/src/spec/manifest/data_file.rs:293–343` supplies the Avro descriptor
writer/reader; these accept unpartitioned native descriptors directly. Task
serialization is not used. The existing serial `ArrowReader` path and whole-file
guards remain in force. DataFusion's parsed expression tree is traversed before
physical compilation; the current allowlist includes harmless aliases while
still visiting their children (`expressions.rs:58–71`).

Native new-table creation explicitly reassigns field IDs
(`iceberg-0.10.1/src/spec/table_metadata_builder.rs:77–89`, `:1275–1286`). The
implementation therefore retains logical IDs separately and maps them to the
native schema (`src/engine/backfill/expressions.rs:91–107`). Independent
verification checks the mapping as well as native schema equality
(`src/engine/backfill/verification.rs:87–93`). This accommodates the observed
native behavior without custom native metadata mutation.

## Data flow and invariants

| State | Creation / mutation / use | Invariant and boundary |
| --- | --- | --- |
| Source file identity | Exact source pin → object lookup → completion key and locator path | Full source pin is an adoption guard, not the per-file cache key. Distinct paths remain distinct occurrences even if bytes match. |
| Selective tasks | Native inventory → task map → individual source stream | Payload opens only after a missing completion is identified. Task metadata remains owned by an `Arc` permit; each active payload stream acquires separate batch admission. |
| File completions | Computed output + native Avro descriptor + object digests → bounded acknowledgment bundle | No per-row lifecycle writes. A crash may repeat unacknowledged files; acknowledged outputs remain protected by their attempt directories. |
| Aggregate descriptors | Verified completions → file/reference vectors → one native append | The aggregate cap applies before vector extension and holds admission through assembly. |
| Group coverage | Independent derived rows → per-source-file interval maps | Null positions, unknown paths, negative/out-of-range positions, overlaps and incomplete ranges refuse. Equal row totals alone cannot pass. |
| Cached handoffs | Prepared native metadata and serialized-composition digest → retained validation/readback | Reuse checks availability before adoption. Previously verified immutable bytes are the premise for reuse; availability checking is not a new full-content verification. |
| Sibling selection | Fresh authority → merged candidate → readback → fresh authority | A changed sibling causes composition to retry; an altered same-group target or source/boundary refuses. Expensive file work is retained. |
| Output custody | Durable directory protection precedes file creation; native pin imports exact output `ObjectRef`s | Reused output files keep their original owners and content identities. Pinning verifies inherited bytes rather than relabeling ownership. |
| Runtime capacity | Shared budget for native work and shared DataFusion runtime for joins | No independent execution pool is introduced. Cancellation drops ordinary stream state; detached hash workers retain their existing blocking-job permits. |

Review hypotheses: immutable per-file reuse can avoid source decoding (confirmed
by call order); coverage must not depend on native file ordering (confirmed by
range merging); descriptor limits might release admission before aggregate use
(confirmed initially, repaired in source); and reader limits might automatically
cover joined output (refuted, BR2).

## Tests and execution evidence

Tests were read, not executed by this reviewer:

| Test / location | Inspected assertions and interpretation |
| --- | --- |
| `combined_values_preserve_duplicate_null_empty_and_unicode_occurrences`, `tests/backfill.rs:162` | Compares with direct Rust character counts, preserves source/boundary, checks logical/native mapping, retry result and conflicting request identity. |
| Empty source, `:210` | Checks typed empty native output and combined consumption without fake rows. |
| Retained handoffs, `:228` | Interrupts file, processing, validation, readback and selection boundaries; reopens without external input and preserves retained completion records. Controlled interruption is not process-kill qualification. |
| Stale source and catch-up, `:282` | Requires stale-source refusal, reports mixed reused/pending files, and compares catch-up with independent reconstruction. |
| Sibling composition and independent concurrency, `:338`, `:495` | Preserves a newly selected sibling after old readback, refuses competing same-group replacement, and keeps both concurrent independent groups. |
| Unsupported source admission, `:422` | Exercises BR1 and stale eligibility refusal while preserving the selected result. |
| Cached object loss, `:510` | Deletes retained group data or input after readback; requires `unavailable_phase`, unchanged selection, and no completed result. |
| Joined output limit, `:559` | Isolates BR2 after all independently read inputs fit. |
| Malformed native groups, `:625` | Tests duplicate, unknown, out-of-range and null locations, plus incorrect field mapping. Non-null coverage cases assert matching native schema before reaching occurrence validation. |
| Identical concurrent attempts, `:746` | Both attempts reconcile one recorded result. |
| Failed files versus successful nulls, `:759` | Fails a later file, preserves completed work, reports failure separately, then corrects source and verifies one parsed value with successful nulls elsewhere. |
| HTTP routes, `:831` | Exercises actual plan/run responses and stale-group conflict through the router. |
| Nonlocal/volatile expressions, `:879` | Refuses aggregate, volatile, window and subquery forms without selecting a replacement. |
| Selective native stream, `src/storage/occurrences/tests.rs:214` | Removes an unrequested payload, drops the scan, reads the chosen file, and verifies continuing/released admission. |
| Aggregate admission, `src/engine/backfill.rs:935` | Individually admissible descriptors cross the cumulative limit and produce `metadata_capacity`. |
| Coverage intervals, `src/engine/backfill/verification.rs:202` | Checks coalescing, gaps, overlap and bounds. |

The reviewer read the owner's completed `docs/reviews/backfill-check.log`:
the build/lint checks, unit, backfill, lifecycle, update, and documentation test
sections pass under the default stack. The earlier stack failure remains at
`docs/reviews/backfill-check-failed-stack.log`. This is inspected owner execution
evidence, not execution performed by this reviewer.

New CLI parsing/output has no dedicated process-level test in the inspected
suite; its application and stream paths are exercised by library and HTTP tests.
This is a remaining transport test observation, not a blocker for the small
statically traced delegation. Native cancellation timing and aggregate resource
measurement remain outside the checkpoint.

## Performance and scope limits

This checkpoint avoids decoding completed source files during computation and
batches lifecycle acknowledgments. Fresh release readback still fully verifies
retained sources and group output bytes; combined reads perform maintained joins
after verification.
Retries resolve retained computation, validation and readback, with cheap
availability checks before reusing those completed phases. It therefore does not
establish complete-path incremental latency, metadata-history scaling, or the
Comments performance target.

The retained lifecycle map still has a finite cap and copies on each commit.
Bundling reduces commit frequency; it does not change that asymptotic history
cost. Native Avro decode and scalar functions may allocate before post-operation
byte checks. The disk preflight is based on compressed source size, so expanding
expressions can exceed its estimate. These are qualification boundaries, not
evidence of a strict process-memory or output-size guarantee.

## Conclusion

**VERDICT: APPROVE.** BR1–BR5 are closed. The scoped implementation resolves reuse
before source computation, retains completed work, and preserves exact coverage,
field interpretation and custody with small adapters around maintained components.
Changed-path coverage is **ADEQUATE** for this checkpoint; confidence is **HIGH**
in the static ownership and authority traces. The owner's full gate passes.
This is an R3 slice; coordinated source/group refresh and broader schema evolution
remain unfinished. Timing, aggregate resident-memory limits, destructive
maintenance, hosted operation and the full lifecycle acceptance gate remain
outside this approval.

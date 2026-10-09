# Compacted source adoption by updates: independent review

**Current independent static verdict: APPROVE after correction.** See
"Independent follow-up review" at the end. The original review and counterexample
below are retained as the record of the issue. Runtime validation remains pending.

**Verdict: REQUEST CHANGES.** Static review found one reproducible-by-trace
correctness blocker in repeated compaction with no-op updates and a computed
group. The main admission, applied-change and occurrence-preservation paths are
otherwise consistent in the inspected code. No Cargo, build, formatting or test
commands were run.

Scope: the unstaged Rust adoption delta inspected on 2026-10-09, with the staged
compaction/export/restore work as its baseline. Unrelated experiment changes are
outside this review. The later staged removal-adapter change is reviewed only in
the separate appendix below. Requirements consulted:
`docs/PLAN.md`, R3 stable identities/reuse, R4 exact change endpoints, and R6
equivalent representations and retained recovery roots.

## Findings

### 1. BLOCKER — A second equivalent representation can strand a no-op update

Category: correctness/recovery. Entry point: `src/engine/write.rs:197`, which now
passes the captured representation to `replace_keys`; the no-applied-rows branch
at `src/engine/write.rs:585` returns that pin unchanged, including its
`logical_metadata`.

Concrete sequence, all using supported operations:

1. Build source S with no computed groups; compact it to C1.
2. Run an empty update. Its selected source is C1 with logical metadata S.
3. Create a scalar backfill. `prepare_backfill` consumes selected C1
   (`src/engine/backfill.rs:584`, `:632`). `prepare_group` keys its completion by S
   (`src/engine/backfill/preparation.rs:32`), but records the consumed native
   source metadata C1 in `GroupDependencies` (`:196`).
4. Compact that source again to C2. Both representations retain logical metadata
   S (`src/engine/compaction.rs:468`).
5. Run another empty update. It adopts C2 and reaches inherited group preparation
   (`src/engine.rs:1391`, `:1405`). The same logical completion key finds the C1
   completion. `check_group_dependencies` accepts only exact C2 or a prior source
   whose physical metadata is S (`src/engine/backfill/preparation.rs:451-459`).
   C1 satisfies neither, so the update refuses with `phase_generation_conflict`.

No eligibility change occurs in this sequence. Retrying keeps the same captured
C2 and fails again. The existing tests author their group before the first
compaction, which stores S directly and does not exercise this case
(`tests/compaction.rs:135`, `:179`).

**Fix:** retain enough verified logical-source identity in group dependencies to
recognize equivalent captured representations, while preserving the actual
consumed physical snapshot for protection and recovery. Do not remove the
generation check or retire the valid earlier computation merely to retry. Add
the sequence above as a regression, asserting successful second no-op selection,
unchanged occurrence tuples, reuse of the computed table, and deep verification.
This follows R3's original-generation reuse rule and R6's preserved reuse history
(`docs/requirements/03-schema-backfill.md:33`,
`docs/requirements/06-maintenance-retention.md:15`).

### 2. WARNING — First real update after compaction can recompute unchanged groups

Category: performance/reuse; nonblocking limitation, not a measured regression.
After an actual update creates a new source metadata identity
(`src/engine/write.rs:588-589`), the whole-group cache key changes
(`src/engine/backfill/preparation.rs:32-40`). Per-file reuse then depends on the
physical `ObjectRef` (`src/engine/backfill.rs:1032-1036`). New compacted files have
no matching old file completions, so even shared, logically unchanged compacted
files take `compute_file` (`src/engine/backfill/preparation.rs:75-99`).

Thus a fixed-size update after full compaction can reevaluate an existing scalar
group over the full source population once. The partial-compaction test proves
source-file sharing and correct values, but does not assert computed-file reuse
or count evaluated rows (`tests/compaction.rs:224`, `:328`). Record this limit and
measure evaluated rows, bytes read/written and retained storage before claiming
small-update savings. If material, reuse retained group values by the preserved
occurrence relation rather than treating new physical files as new computation.

## Patch summary and function trace

The patch captures a selected source representation at update admission, carries
it through durable dependencies, adopts it for source writes, and permits hidden
origin columns without changing logical fields or canonical change endpoints.
Public update callers are `Engine::update` and `update_until`
(`src/engine.rs:746`, `:757`); both enter the shared run service.

| Function | Location | Inputs → output; verified behavior |
| --- | --- | --- |
| `prepare_and_select` | `src/engine.rs:1032` | Request/authority → admitted operation; resolves representation under locks, reacquires protection outside them, and rechecks lifecycle boundary. |
| `protect_admission` | `src/engine/admission.rs:146` | Base and captured source → protected dependencies; includes representation objects in owner protection. |
| `admitted_dependencies` | `src/engine/admission.rs:88` | Retained rows → capture; checks request/revision/binding and restricts `update_source` to same-dataset updates. |
| `prepare_source` processing branch | `src/engine.rs:941` | Saved capture → prepared source; records its native snapshot in phase dependencies and retains request protections. |
| `process_update` | `src/engine/write.rs:182` | Canonical base/captured representation → applied source and changes; checks logical identity, rows and schema; normalizes fresh origin columns. |
| `replace_keys` | `src/engine/write.rs:362` | Source/key scope/replacements → source, removed, added; shares unaffected files and preserves stored origins on affected-file reads. No-op branch causes finding 1. |
| `verify_changes` | `src/engine/changes.rs:6` | Request/source/change pins → validation; binds canonical endpoints, permits only identity-column schema additions, and deep-checks occurrence-aware reconstruction. |
| `preserves_schema` | `src/storage/identity.rs:70` | Before/after schemas → bool/refusal; compares all logical fields and IDs, identifier sets and any preexisting origin fields. |
| `verify_binding_history` | `src/engine/backfill/verification.rs:289` | Group history → validation; uses the same logical-schema rule for update and incremental-refresh transitions. |
| `prepare_backfill` | `src/engine/backfill.rs:464` | Backfill request → protected selection; passes no update representation and otherwise retains existing behavior. |
| `Inventory::record` | `src/engine/retention/inventory.rs:961` | Request dependency row → object/native roots; includes the entire captured update table at `:1232`. |
| `compare_admitted` | `src/engine.rs:536` | Two releases/group → multiset comparison; validates origin-column shapes, then compares public schemas and values. |
| `prepare_group` / `check_group_dependencies` | `src/engine/backfill/preparation.rs:10`, `:443` | Logical reuse key/physical dependency → retained group; incomplete equivalence handling causes finding 1. |

## Data flow and invariants

- `update_source` is created from the selected verified representation
  (`src/engine.rs:1122`), serialized atomically with request dependencies and
  protection bindings (`src/engine/admission.rs:48-64`), reloaded for processing
  (`src/engine.rs:951`), and retained as both a request table root and a phase
  native-snapshot dependency (`src/engine/retention/inventory.rs:1232`,
  `src/engine.rs:1009`). Later compaction does not recapture it on retry.
- Canonical endpoints remain `base.manifest`, the base source metadata and the
  resulting source metadata (`src/engine/write.rs:345-352`). Verification checks
  those against the retained base manifest (`src/engine/changes.rs:44-72`).
- Old files acquire fallback birth locations before filtering; existing stored
  origins survive (`src/storage/occurrences.rs:133-163`). Fresh replacements get
  null origins through normalization and writer-generated identities
  (`src/engine/schema.rs:283-286`, `src/storage.rs:245-260`). Added change files are
  shared with the resulting source (`src/engine/write.rs:538`, `:580-595`).
- Protection reuses the common object collector and native snapshot references;
  no separate registry or new bulk-copy path was introduced. Retention's direct
  table walker checks metadata membership and tracks the native snapshot
  (`src/engine/retention/inventory.rs:861-881`).

## Tests and limits

Static traces are consistent with these tests succeeding; execution remains the
main thread's responsibility:

- `updates_share_partial_compaction_and_preserve_surviving_occurrences`
  (`tests/compaction.rs:179`) checks sharing, canonical endpoints, removed/added
  tuples, surviving identities, public schema comparison, computed values and
  deep verification across original-file and compacted-file replacements.
- `admitted_updates_resume_the_captured_representation_after_later_compaction`
  (`tests/compaction.rs:335`) checks admission/processing interruptions, a cold
  reopen after deleting the external input, exact retained dependencies and
  phases, isolated request-root protection, and use of C1 rather than later C2.
- `schema_defaults_and_supplied_state_remain_native_and_exact`
  (`tests/compaction.rs:555`) now checks omission/default versus explicit null
  after compacted-source adoption and independently verifies the result.

The new matrix does not cover finding 1, compacted-source deletion-only or
delete-all updates, or falsification of stored origins in change evidence.
Existing ordinary update tests cover deletion, no-op and endpoint corruption
without compaction (`tests/updates.rs:344`, `:384`, `:659`); they do not establish
the combined paths. No throughput, memory, I/O or storage benefit was measured
by this review. Source-file sharing is supported by code and test assertions,
not by performance evidence.

## Conclusion

**VERDICT: REQUEST CHANGES.** The intended capture, endpoint and protection
behavior is supported by the inspected paths, but an ordinary repeated-layout
sequence can reject a valid no-op update. Changed-path coverage is insufficient
for that case. Confidence: high in the static counterexample; runtime outcome
awaits a focused regression executed by the main thread.

## Separate appendix: removal inventory permit handoff

**Static verdict: APPROVE** for the staged change in
`src/engine/retention/removal.rs:374-431`. The caller `remove_admitted` preserves
the original plan validation, fresh retention intersection, registration checks,
dataset lock, unavailable-object reservation and guarded commit
(`src/engine/retention/removal.rs:338-357`, `:377-407`, `:443-471`).

The `4 * max_metadata_bytes` permit covers inventory construction. The new
`8 * max_metadata_bytes` encoding permit is acquired before constructing the
intent (`:410`). At the new drop (`:431`), candidate copies were dropped (`:376`),
selected copies were dropped (`:394`), the native-registration vector was consumed
(`:396`), and the inspection boundaries/objects and chosen indices were moved or
consumed into the intent (`:414-425`). The encoding permit remains in scope
through serialization, commit and reconciliation. I found no interval where the
retained intent lacks the replacement admission charge and no expansion of
deletion authority.

The direct improvement is removal of an overlapping inventory reservation during
catalog commit. This review did not measure resident memory or reproduce the
reported capacity failure; the main thread must retain the targeted process-test
result at the unchanged budget. This appendix does not change the adoption
verdict above.

## Implementation follow-up: logical identity of saved scalar work

The follow-up records `GroupDependencies.source_logical_metadata` when a scalar
assembly or validation consumes a physical rewrite. Its `source` still records
the exact consumed native snapshot. Reuse across layouts requires the same logical
source and exact output snapshot; when physical metadata matches, source snapshot
and schema IDs must also match. Retention keeps both the consumed snapshot and the logical
metadata object. Material reuse keys remain unchanged.

The added regression builds S, compacts to C1, adopts C1 through an empty update,
first computes a scalar group on C1, compacts to C2, and adopts C2 through another
empty update. It asserts unchanged occurrence tuples, exact computed-table reuse,
unchanged saved file/assembly/validation records, canonical change endpoints, and
deep verification. This implementation follow-up has not run that test or the
repository gate; execution remains with the main thread.

Ordinary historical dependencies whose native source is the logical source
remain reusable. An unreleased interim completion that consumed C1 but omitted
its logical S identity still refuses reuse across C1 and C2 with
`phase_generation_conflict`. The fix does not infer missing proof or migrate
those interim records.

Finding 2 remains a bounded, unmeasured limitation: the first real update after
compaction can recompute scalar values for shared compacted files that have no
saved per-file completion, potentially covering the full source after a full
compaction. Later work can reuse those completions while their files and
dependencies remain unchanged. No evaluated-row, I/O, throughput, or storage
benefit is claimed for this path; an occurrence-based reuse index is outside this
fix.

## Independent follow-up review

**VERDICT: APPROVE** for the corrected adoption delta by static analysis. Finding
1 is resolved for newly recorded completions. Finding 2 remains a disclosed,
nonblocking resource limitation. No builds, formatting or tests were executed by
this reviewer; this verdict is not a passing-gate or performance claim.

I independently read the new `GroupDependencies` field, both producers, the reuse
predicate, both retention branches and the regression. The author's preceding
implementation note was not treated as verification evidence.

| Function/data | Current location | Independently verified behavior |
| --- | --- | --- |
| `GroupDependencies` | `src/engine/backfill.rs:51-57` | Adds an optional logical metadata reference without replacing the consumed native snapshot. Omitted fields deserialize as `None`. |
| `prepare_group` assembly | `src/engine/backfill/preparation.rs:196-211` | Records both `source.into()` and `source.logical_metadata.clone()` before committing the completion with its protection binding. |
| `prepare_group` validation | `src/engine/backfill/preparation.rs:251-266` | Records the same logical identity independently for completed validation, preserving its exact input and output snapshots. |
| `check_group_dependencies` | `src/engine/backfill/preparation.rs:445-470` | Equal physical metadata still requires equal source snapshot/schema and any recorded logical identity. Different physical metadata requires equal retained logical identity. Every path still requires exact output metadata/snapshot/schema. |
| `check_validation` | `src/engine/backfill/preparation.rs:474-483` | Checks the validated output and then applies the same dependency rule. |
| `Inventory::record` | `src/engine/retention/inventory.rs:1164-1187` | Both completion kinds retain the actual source snapshot, the optional logical metadata object and the actual output snapshot. Logical identity does not replace physical recovery roots. |

For the original sequence, both saved records now contain physical source C1 and
logical source S. The second no-op update consumes C2, whose logical source is
also S; the cross-layout comparison at
`src/engine/backfill/preparation.rs:460-461` succeeds. The output dependency still
matches the original computed table, so both prepared work and validation can be
reused. The change adds one small metadata reference per saved dependency rather
than a new payload scan, correspondence index or rewritten result.

The regression
`repeated_compaction_noop_updates_reuse_groups_first_computed_on_a_representation`
(`tests/compaction.rs:179-225`) reproduces the exact order: first adopt C1 without
groups, then compute the group, compact to C2, and issue the second no-op update.
It verifies the recorded physical/logical pair (`:204-207`), exact computed-table
reuse and unchanged saved file/assembly/validation records (`:215-216`), original
occurrence tuples and values (`:217-218`), empty canonical changes (`:219-223`),
and deep release verification (`:224`). Before the correction this sequence
reaches `phase_generation_conflict`; with the correction, the inspected predicates
admit it. Execution remains with the main thread.

Compatibility is conservative: an older ordinary completion whose physical
source is S can still match a representation of S. An interim C1 completion that
omitted logical identity cannot cross to C2 because its fallback identity is C1;
same-physical-source reuse remains available
(`src/engine/backfill/preparation.rs:454-461`). This does not infer missing proof.

Remaining coverage limits are nonblocking: the regression does not isolate each
new logical-metadata retention root or falsify logical identity/source snapshot
fields to exercise every refusal branch. Those paths are supported here by the
shared collector and explicit predicates, not by new test execution. The existing
deletion-only and stored-origin falsification gaps from the first review also
remain. Confidence: high in resolution of the reported counterexample; runtime
qualification awaits the targeted regression and repository gate.

## Author-run qualification of the final increment

The [focused compaction suite](compacted-updates-focused.log) and
[complete repository gate](compacted-updates-check.log) pass on the isolated
source increment, including the repeated-compaction counterexample retained
above. The gate covers formatting, strict linting and library, integration and
documentation tests. Parent compilation used the fixed snapshot; concurrent
evidence-only development was excluded. These are author-run results, distinct
from the independent static review. No corpus benchmark or throughput claim is
included.

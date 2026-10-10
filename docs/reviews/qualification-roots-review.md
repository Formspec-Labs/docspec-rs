# Standalone qualification root coverage

The added `tests/qualification_roots.rs` isolates the qualification inventory and original-owner fallback through maintained public APIs. It changes no production code. These are synthetic catalog isolation fixtures built from a successful correction flow; they do not establish a legal transition that discards other lifecycle promises. Builds, tests, formatting, benchmarks and corpus reads were not performed by this reviewer. Execution and the repository gate remain with the implementing agent.

## Behavior checked

The fixture creates a small native source, computes a scalar group, records an omitted-field correction, and obtains the actual successful supplemental proof through sibling backfill. `Lifecycle::commit` then places that proof in a fresh catalog with an object-free authority record. It preserves the real first-correction revision. Assertions exclude file, prepared, validation, result, readback and earlier grant records that could mask the dispatch being tested.

- `sole_qualification_root_protects_original_creations_and_target_native_closure` expects inspection to name the exact qualification key as the blocker for its origins, original declaration, descriptor, output objects and native target metadata. A second synthetic proof points at the sibling native table, whose metadata and payload are not already reachable through those origins. This checks the qualification target expansion separately from origins expansion. It tests inventory behavior only; the changed target does not claim successful semantic correspondence. Exact protected byte accounting and zero unmatched bytes are asserted.
- `qualification_only_owner_acquires_original_creations_without_direct_phase_rows` retains only the proof and existing path reservations in a fresh injected catalog for each requested object. Reservations establish the actual producer path; they contain no completed creation identities. Foreign admission must recover each original declaration, descriptor and output through the qualification origins, bind the exact original input reference, and create an exact owner grant. A mismatched digest must refuse before creating any owner grant. Stopping at admission prevents attempts to execute metadata as Parquet. A separate catalog per object prevents a previously acquired grant masking the fallback.

The production trace is `Inventory::record` → `qualification` → `expand` in `src/engine/retention/inventory.rs`, and managed input admission → `qualified_object` → `visit_original_creation` plus owner acquisition → `confirm_creation` in `src/engine/retention/owners.rs`. The fixture uses those implementations directly rather than replicating their algorithms. It reuses the existing scalar fixture and local catalog APIs and adds no test adapter or production abstraction.

## Remaining qualification

F1 in `dependency-correction-retention-review.md` is narrowed by these assertions only after they pass. This file is an implementation/coverage note, not executed evidence or a declaration that F1 is fully closed.

The following remain distinct gaps:

- Standalone qualification through supported retirement/replacement transitions, including original acknowledgment and unknown-provenance refusal cases. Synthetic omission of overlapping records does not authorize removal of any promise.
- Same-workspace standalone restore capture and restoration of its original and target-only native dependencies. The isolated inventory fixture references real metadata outside its fresh workspace; restore requires owned paths and native catalog registration closure. The owner fixture uses an injected catalog, for which retention and restore are deliberately unqualified. Existing selected-inline-proof restore coverage remains useful but does not establish this standalone case.
- A live qualification binding with inherited foreign owner witnesses must keep that grant in use until its own promise ends. The local original executions in these fixtures do not have foreign owner witnesses, so their new consumer grants do not establish that case.
- Broader failure, resource-scaling and remote behavior. The source has a small fixed row set and uses configured budgets; no throughput, peak-memory or streaming-scale claim follows from it.

## Execution evidence

The parent implementing agent ran the fixtures as part of the
[complete repository gate](native-column-selection-full-check-3.log), which
passed formatting, strict Clippy and all-feature tests. Both isolated-root tests
passed. The [independent static review](qualification-roots-independent-review.md)
approves their stated scope. This execution narrows F1 to the remaining cases
above; it does not qualify supported retirement transitions or standalone restore.

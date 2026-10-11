# Concurrent candidate reconciliation review

**Verdict: approve, subject to the unchanged concurrency regression and parent-owned gate.** Reviewed patch SHA `1f6bcbe186270acc2435d7361937e965e29666c5dbda20242622b60b21961bd7` against the frozen shared source on 2026-10-11. Static review only; no source edits, builds or provider activity.

## Problem and exact change

`tests/lifecycle.rs:659` starts two identical operations and requires the same immutable release. The new retained initial candidate is shared by exact retries. One writer can commit its native append while the other reaches `GuardedCatalog` with the earlier empty state; the loser correctly refuses stale append. At that point `Engine::run_admitted` (`src/engine.rs:837–861`) can find no final result yet, so final-result-only reconciliation returns the loser error.

The patch changes `process_initial_build` at `src/engine/write/build.rs:220` to share its existing completed-native-output recovery logic and call it once after a preparation/preflight/write failure. It reloads only the retained `pending.identifier`; it never creates a replacement attempt, loops, re-executes SQL or appends again.

## Verified trace and invariants

1. `existing_candidate`/`reserve_candidate` bind exact request, operation revision, processing generation, admitted input and protection before this path. The existing retained metadata permit remains live through reconciliation.
2. `GuardedCatalog::load_table` and `update_table` (`src/catalog.rs:575–601`) reject adoption of changed native metadata. `storage::append` (`src/storage.rs:449`) still uses that guard and maintained expected-snapshot updates. No guard is weakened by the patch.
3. On failure, the new branch loads the retained identifier once and rebinds its native I/O through configured `Objects`. Failed query/writer work has left the inner async scope before recovery begins.
4. `recover_initial_output` first calls the existing `check_candidate` (`build.rs:492` before this patch): UUID, root location, schema, properties, partition/sort layout and absence of statistics must match the original. An accepted result must contain exactly one parentless Append snapshot. Empty state must equal the retained initial metadata and URI.
5. A valid completed append supplies its existing summary counts and passes through `pin_output` (`src/engine/write.rs:104`) and bounded configured inventory/identity checks. Later normal phase verification/selection remains unchanged. A pending intent alone never becomes qualified creation evidence.
6. An exact empty candidate preserves the original preparation error. Reload, binding or validation failures remain failures; absence is not converted into completion. An error during pinning can cause one repeated identity attempt because no durable completed phase exists; completed phase lookup still bypasses this native preparation path.

The error branch intentionally reconciles all preparation errors, not just one string-coded conflict. A sibling's already committed exact attempt can satisfy the operation even when this attempt failed earlier. It cannot adopt an unrelated table or a later multi-append state. This avoids coupling recovery to unstable provider error text and adds no lease, lock or retry framework.

## Test boundary

The unchanged simultaneous-retry fixture is the concrete regression target; parent runs it and the relevant suite. Existing controlled append-gap recovery and stale native-append tests remain relevant. This patch adds no deterministic lost-append-response injection and performs no new actual-provider qualification; those stronger execution claims must not be inferred from the static branch trace. No actionable finding remains.

# Explicit local physical removal review

## Findings and current disposition

This is an in-progress static review of the removal slice against baseline
`2bf55c8fe5393c34232b85a325910ad643db57a7`. The implementation is still changing;
the findings below are not a final approval. The reviewer applies the Rust and
semi-formal code review skills and has not run Cargo, formatting, tests, cleanup,
commits, or physical deletion.

1. **BLOCKER, correctness — pending resume must recheck current roots and native
   registrations.** The first `finish_removal` implementation resumed captured
   registration drops and file effects without a fresh root/native check. A new
   native head introduced after an interrupted intent could retain a target that
   resume would then unlink. Recheck the exact unfinished set under owned
   maintenance, refuse new protections or new/changed native effects, and preserve
   the saved scope. Completed retries should return before those scans. The author
   accepted the finding and is implementing the correction.
2. **BLOCKER, recovery — terminal reconciliation must fit the reserved floor.**
   The first implementation always topped up to two outcome allowances before
   checking whether effects had already happened. If unlink succeeded but parent
   synchronization failed, a failed report retained one terminal allowance. After
   ordinary owner writes consumed unreserved capacity, resume could not top up and
   could not acknowledge the file's durable absence. Perform confined read-only
   reconciliation before any top-up; allow a complete terminal report to consume
   its reserved floor. Require additional capacity before new destructive effects.
   The author accepted the finding and is implementing the correction.

The earlier integration concern about scalar-only history accounting is resolved
in the current catalog adapter: current and changed removal states share typed
`RemovalIntent`/`RemovalProgress` validation, and every intent requires a state.
The final review will recheck that validation and its tests after integration.

## Scope and requirements

The slice enables an operator to reclaim explicitly selected managed files after
all applicable promises end. It covers owner-scoped preview, fresh eligibility,
durable exact intent and unavailability, registration-only native detachment,
exact local effects, durable bounded outcomes, inspect, and resume.

The governing behavior is R6's separation of retirement from deletion, fresh
intersection with the submitted plan, unavailable-before-effect rule, exact
outcomes, and deferred orphan sweeping (`docs/requirements/06-maintenance-retention.md:23`).
Q3 requires preserved readers/shared roots and protection races
(`docs/requirements/07-operations-qualification.md:101`). The local scope does not
qualify Q6 remote stale actors or restored catalogs
(`docs/requirements/07-operations-qualification.md:125`). Compaction, restore,
hosted adapters, independent retention policy windows, broader history lifetimes,
and the full R1–R7/Comments acceptance remain open.

## Evidence and function trace

| Function or seam | Inputs and output | Verified behavior |
| --- | --- | --- |
| `owners::confirm_creation` (`src/engine/retention/owners.rs:768`) | Owner rows and exact candidate batch → qualification or refusal | One traversal matches full `ObjectRef` identity and rejects conflicting or missing creation evidence. Retired typed evidence remains usable as creation proof. |
| `Engine::check_object_owner` (`src/engine/retention/owners.rs:715`) | Owner, object and retained reservation → ownership proof | Requires the object's owned attempt namespace and original `Protection` reservation; directory membership alone is insufficient. |
| `owners::available` (`src/engine/retention/owners.rs:401`) | Owner rows and object → availability or refusal | Permanent unavailable-path markers block new owner acquisition, including an otherwise matching existing grant. |
| Managed input admission (`src/engine/admission.rs:216`) | Input locator/digest → exact managed dependency | Resolves aliases, calls `qualified_object`, and retains the observed owner boundary for later admission. |
| `Engine::register_output` (`src/engine/write.rs:67`) | Dataset namespace and native metadata preparation → registered table | Uses a generated `data_<uuid>` name in the owning lifecycle namespace. |
| `storage::register_candidate` (`src/storage/tables.rs:81`) | Exact native metadata → durable metadata file and registration | A registered head can exist before a completion `TablePin`; deletion proof must inspect native registrations independently of completed results. |
| `storage::snapshot_files` (`src/storage.rs:370`) | Exact native head → snapshot paths and current data descriptors | Includes current and historical snapshots, manifest lists, manifests and statistics; does not infer restore promises from metadata-log entries. |
| `Engine::inspect_retention_admitted` (`src/engine/retention/inventory.rs:94`) | Candidate objects → common-root blockers and boundaries | Uses the existing typed global root walker, exact retirement records and bounded metadata expansion without payload scans. |
| `Engine::maintain_work` (`src/engine/worker.rs:136`) | Owned service future → result | Holds exclusive maintenance and host custody through actual worker completion, independently of the public caller's lifetime. |
| `storage::removal::remove_owned_file` (`src/storage/removal.rs:29`) | Owned exact object → removed, absent or refusal | Confines the path, rejects symlink ancestry/nonregular files, hashes the current file once, checks Unix file identity, unlinks only that file and syncs its parent. Concurrent external mutation is explicitly unsupported. |
| Maintained SQL catalog `drop_table` | Exact registration → registration removed | Installed `iceberg-catalog-sql-0.10.1/src/catalog.rs:750` deletes the catalog registration; `purge_table` at line 778 also deletes physical data and must remain unused. |

## Data flow and invariants to verify in final integration

- Preview carries sorted deduplicated exact objects, blockers, lifecycle boundaries,
  owner table identity and exact native registration heads. Execute may shrink
  that candidate set but cannot add objects or registration effects.
- The owner lifecycle commits the immutable intent, current reservation state and
  exact unavailable markers together. Lost intent responses are reconciled against
  that exact state before effects.
- Unavailable markers point to exact intent membership and the same native revision.
  Inventory rejects orphan or conflicting markers and treats validated removal
  records as outcome metadata, never as new retained payload roots.
- A registration's namespace, generated name, owned metadata location, UUID and
  current head establish its supported local scope. All recorded registration
  effects finish before file effects. No native purge or directory deletion is
  permitted.
- `history_bytes + reserved_future_bytes` is checked against the same lifecycle
  bound on cold replay and before native writes. Current reservations count once;
  old appended reports remain charged. Failed outcomes preserve the terminal floor.
- Completed outcomes return without hashing, unlinking or root traversal. Pending
  outcomes remain unavailable across restart; retries use the same immutable set.
- `confirmed_unlinked_bytes` and `reconciled_absent_bytes` describe logical file
  sizes. Neither establishes reclaimed allocated blocks or volume free space.

## Test review status

The existing old/new retirement fixture proves old objects can become unblocked
while the selected replacement stays readable (`tests/operation_retirement.rs:93`);
its baseline still preserves the old file. The new service fixtures must extend
that observable result to exact removal and cold retry.

The new local-effect tests cover exact unlink with a retained neighbor, absent
retry, same-size changed bytes, escaped/wrong-owner paths, directory targets, leaf
symlinks and ancestor symlinks (`src/storage/removal.rs:181`). The current transport
test covers equivalent CLI/HTTP protected previews, bounded bodies, rejected unknown
fields and missing inspect/resume identities (`tests/physical_removal_transport.rs:25`).
These tests have been read statically; this reviewer has not executed them.

Final adversarial review remains pending for the service tests, fresh resume
correction, terminal-capacity reconciliation, interruption and lost responses,
native-head changes, corruption refusal, cancellation and the final author-run
gate evidence.

## Conclusion

VERDICT: REQUEST CHANGES — the accepted recovery findings above remain under
implementation. Final verdict and evidence will replace this interim disposition.

Coverage of changed paths: INSUFFICIENT until the service and recovery fixtures
are reviewed. Confidence is high in the concrete failure traces, with execution
qualification deliberately left to the author.

## Author follow-up after reviewer interruption

The reviewer subsequently traced both corrections and reported them structurally
sound. The final review turn and capacity-test implementation agent then stopped
at their provider usage limit. This section records the implementation owner's
follow-up; it is not independent approval or a completed review certificate.

`finish_removal` now rechecks current protections and native heads on nonterminal
resume, without expanding the saved scope. Initial execution reuses the proof
already captured under exclusive maintenance. `reconcile_absent_file` permits
terminal reconciliation before a required capacity top-up; completed retries skip
both paths. The saved registration validator additionally rejects lifecycle names,
foreign metadata namespaces and still-present registrations unrelated to the
exact candidates.

The owner-run `physical-removal-internal-corrected.log` and
`physical-removal-faults.log` pass. Fixtures exercise actual unlink, retained shared
values, active readers, newly introduced native heads, corruption, cold recovery,
lost replies and terminal completion when no unreserved history room remains.
The owner-run complete `physical-removal-check.log` passes formatting, strict
linting and all tests, including the additional malformed-registration and
metadata-batch fixtures. The two earlier lint failures remain retained. Full
process-kill/provider qualification, compaction, restore and final independent
review remain open. The reviewer did not inspect these final additions or the
completed gate; this is owner-run evidence.

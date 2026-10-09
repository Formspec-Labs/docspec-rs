# Explicit local object removal: final independent review

No actionable blocker remains in commit
`ecb31f1cbdfa0e79fb6a1d06eee269421bc17f13` plus the reviewed working-tree
follow-up for its cooperative local Unix workspace scope. Expanded process
qualification exposed one real capacity defect in the original commit; the
follow-up fixes its reservation lifetimes and passes that unchanged-limit fixture.
The code implements fresh eligibility, exact durable intent, unavailability before
effects, registration-only detachment, exact file removal, and bounded retained
outcomes. This approves the reviewed implementation; it does not qualify the full
R6 plan or production-scale maintenance.

Review date: 2026-10-09. This was a blind static review using `ms-rust` and
`semi-formal-code-review`, followed by inspection of the earlier review and saved
gate logs. The parent then extended the review to the working-tree resource,
process, transport, and filesystem-failure additions. No Cargo, build, formatting,
test, cleanup, or implementation command was run by this reviewer. All line
references below refer to `ecb31f1` unless explicitly identified as working-tree
follow-up references.

## Findings

### F0 — Overlapping maximum reservations refused a tiny removal

Severity: BLOCKER in `ecb31f1`; resolved in the reviewed follow-up.
Category: correctness / resource admission.

Location: original `src/engine/retention/removal.rs:320`,
`src/engine/retention/inventory.rs:110`, `src/budget.rs:83`.

The original execution retains `12 * max_metadata_bytes` while the common
inventory requests another `5 * max_metadata_bytes`. With the existing process
harness's 512 MiB configured memory and 16 MiB metadata maximum, those two
reservations alone require 272 MiB from the 256 MiB application pool. A tiny
valid removal therefore refuses before durable intent, even though its live
metadata fits. This was reproduced in
`docs/reviews/physical-removal-process-capacity-failure.log:20`; the limit profile
is at `tests/support/mod.rs:20`.

The follow-up separates proof decoding, inventory work, and encoding lifetimes.
It scopes the proof's eight-maximum allowance before inventory, keeps four
maxima for returned collections, and restores the other eight only after native
inventory releases its readers. It also releases unused row/candidate copies and
charges saved-intent decoding by its actual serialized size. See the follow-up
trace below. No configured limit or fixture size was increased. The unchanged
process fixture subsequently passes in `docs/reviews/physical-removal-process.log`.
This establishes removal of the reproduced admission failure; it does not
measure a reduction in peak resident memory.

### F1 — Original failure-domain gaps now have focused follow-up coverage

Severity: WARNING in `ecb31f1`; addressed for the named local handoffs by the
reviewed follow-up. Category: test-coverage.

Location: `src/engine/retention/removal/fault_tests.rs:23`,
`src/engine/retention/removal/fault_tests.rs:38`,
`src/storage/removal.rs:165`.

The committed fault tests return an injected error after the intent,
registration effects, or an unlink; they then drop and reopen an Engine. They
establish cold metadata replay and unknown-effect reconciliation, but do not
exercise abrupt process death while a private task owns the workspace lock.
The committed file tests also do not cause unlink or directory synchronization
to fail. These are concrete untested branches because synchronization can fail
after the unlink has succeeded, leaving an unresolved effect.

The follow-up adds actual process-kill/reopen cases at the acknowledged removal
handoffs and controlled unlink/sync failures. Keep power loss, provider behavior,
and restored-catalog fencing separate; those are required
by `docs/requirements/06-maintenance-retention.md:31` and
`docs/requirements/07-operations-qualification.md:125` but are not supplied by
local handoff tests. The test-only CLI waits after the service returns its
injected failure, so the new SIGKILL cases do not interrupt an in-flight hash,
native commit, or system call. This boundary is explicit in the follow-up review.

### F2 — Application accounting does not bound native enumeration allocation

Severity: WARNING. Category: performance.

Location: `src/engine/retention/removal.rs:849`,
`src/engine/retention/inventory.rs:116`, `src/engine/worker.rs:153`.

The application caps candidate metadata, retained inventory, individual native
snapshot inventories, and lifecycle replay history. However, `list_tables`
returns its full vector before the loop charges table identities, and the common
inventory similarly receives all namespaces before charging them. The pinned
SQL catalog uses `fetch_rows`, builds a `HashSet`, then returns a vector
(`iceberg-catalog-sql-0.10.1/src/catalog.rs:392` and `:684`, checked in the installed
Cargo source; pinned by `Cargo.lock:2544`). A large catalog can therefore allocate
more than the application reservation before receiving `metadata_capacity`.

In addition, execution and pending resume enumerate all relevant native heads
and hold the workspace-wide maintenance write guard while hashing selected files.
A small deletion can pause unrelated Engine reads and writes for work that grows
with retained catalog history. This is a conservative local consistency tradeoff,
not a demonstrated throughput or whole-process memory bound.

Retain the current scope statement and measure catalog size, metadata I/O, peak
memory, and foreground wait under Q7 before claiming scale. If the measured limit
requires a change, prefer a maintained paged enumeration facility or a qualified
bounded catalog adapter. A new reverse index is not justified by this review alone.

### F3 — Transport follow-up covers routes; eligible HTTP deletion remains untested

Severity: OBSERVATION after the follow-up. Category: test-coverage.

Location: `tests/physical_removal_transport.rs:29`, `src/http.rs:118`,
`src/main.rs:350`.

The original transport fixture verifies equivalent protected previews, oversized/unknown
request refusal, and missing inspect/resume identities. It never invokes
`POST /retention/removal/execute`, a successful HTTP resume, or the CLI's actual
removal command. The handlers directly call the reviewed service, so this is not
an observed deletion defect.

The reviewed follow-up exercises successful HTTP execute, inspect, and resume
for a fully protected preview whose fresh intersection is empty, and the new
process fixture exercises actual CLI deletion/resume. Route serialization and
saved-result behavior now have direct coverage. An eligible-file HTTP deletion
fixture would extend end-to-end coverage, but the existing thin dispatcher and
shared service tests make this a remaining boundary, not a blocker.

## Patch summary

The patch adds `RemovalPlan`, `RemovalIntent`, `RemovalProgress`, and
`RemovalStatus` (`src/model.rs:567`). An operator supplies exact owned objects and
a reason; preview returns their blockers and native heads. Execution selects only
objects eligible in both preview and fresh inventory, atomically records their
intent and unavailable markers, detaches captured native registrations, and
checks/unlinks each exact regular file. Inspection and resume use the retained
identity instead of constructing a new removal.

The CLI dispatches to the same service at `src/main.rs:333`; HTTP registers and
dispatches the removal routes at `src/http.rs:43` and `:101`. The catalog change
reserves lifecycle history capacity for outcomes on both warm commits and cold
replay (`src/catalog.rs:254`, `:367`). Inventory validates saved removal records
before excluding them from payload roots (`src/engine/retention/inventory.rs:147`).

The governing requirements are fresh intersection and exact deletion authority
(`docs/requirements/06-maintenance-retention.md:29`), unavailable-before-effect
and truthful recovery (`:31`), reader/shared-reference protection (`:25`), and
disabled unsupported cleanup (`docs/requirements/07-operations-qualification.md:103`).
The plan keeps remote adapters, compaction, restore, and full qualification in
scope for later delivery (`docs/PLAN.md:96`, `:104`, `:121`).

## Function trace

`removal.rs` in this table means `src/engine/retention/removal.rs`.

| Function / method | File:line | Inputs → output | Verified behavior |
| --- | --- | --- | --- |
| `plan_removal`, `removal_plan_admitted` | `removal.rs:160`, `:180` | Owner, ID, objects, reason → plan | Admits before cloning; sorts/deduplicates; validates creation/ownership; collects blockers and native heads; checks serialized size. |
| `qualify_removal_objects` | `removal.rs:226` | Owner rows and exact objects → proof/refusal | Validates every identity, unavailable marker, original attempt ownership, and batched typed creation evidence. |
| `removal_catalog`, `existing_removal_view` | `removal.rs:125`, `:136` | Engine or owner → supported catalog/view | Requires owned embedded catalog and Unix; inspection of missing intent does not create a lifecycle table. |
| `remove_objects`, `remove_admitted` | `removal.rs:252`, `:309` | Plan → saved status | Owns exclusive maintenance and owner lock; exact retry compares saved plan; fresh execution shrinks eligibility and refuses new native heads. |
| `inspect_removal`, `resume_removal` | `removal.rs:265`, `:287` | Owner and removal ID → saved/reconciled status | Inspection reads metadata; resume owns maintenance and the owner lock before validating saved authority. |
| `validate_removal_plan` | `removal.rs:438` | Plan and current view → validity | Requires owner UUID, valid owner boundary, nonempty sorted object scope, sorted registrations, reason, and metadata bound. |
| `saved_removal`, `validate_removal_intent` | `removal.rs:465`, `:515` | Saved rows/view/ID → exact intent and state | Checks identity, revisions, sorted in-range indices, original blockers, owned paths, and every exact unavailable marker. |
| `validate_removal_records` | `removal.rs:482` | Lifecycle rows → authorized marker set | Rejects duplicate marker membership and orphan state; inventory separately rejects orphan unavailable markers. |
| `finish_removal` | `removal.rs:566` | Exact intent/state → reconciled state | Completed retry returns early; pending retry checks current protections/native heads; registrations finish before file effects; terminal absence can use the reserved floor. |
| `save_removal_progress` | `removal.rs:723` | Exact intent and next state → committed view | Validates state, commits, then reconciles an unknown reply against the same saved intent and exact state. |
| `detach_registration` | `removal.rs:756` | Exact native registration → removed/absent/failure | Checks generated owned identity, current UUID/head; calls `drop_table`; an uncertain drop checks catalog absence. No purge call. |
| `removal_registration_ident` | `removal.rs:786` | Saved registration → native identifier | Rejects lifecycle/arbitrary names and mismatched owned metadata namespaces. |
| `removal_registrations` | `removal.rs:826` | Candidate paths and lifecycle boundaries → sorted native heads | Walks maintained snapshot inventories; includes provisional registrations; validates owner reservation and unchanged owner boundary. |
| `validate_progress`, `outcome_charge`, `finished` | `removal.rs:40`, `:79`, `:104` | Intent/state → validity, worst-case row charge, completion | Checks vector sizes, bounded error codes, registration-before-file ordering, and terminal reserve rules. |
| `indices_valid`, `failure`, `status` | `removal.rs:938`, `:112`, `:942` | Indices/errors/outcomes → validated indices, bounded failure, logical byte totals | Rejects out-of-range/duplicate scope; distinguishes confirmed unlink from reconciled absence with checked totals. |
| `intent_key`, `state_key`, `invalid`, `conflict` | `removal.rs:16`, `:20`, `:24`, `:31` | ID or no inputs → key/fixed refusal | Use the existing validated `Id` format and stable refusal codes. |
| `Lifecycle::rows` | `src/catalog.rs:203` | Native lifecycle snapshot → cached current rows | Counts all replayed history, validates current reservations, and includes reserved future bytes in the same limit. |
| `Lifecycle::commit` | `src/catalog.rs:288` | Captured view and changes → guarded next view | Makes intents immutable; adjusts changed state reservations; checks history plus reserves before native writes. |
| Catalog reservation helpers | `src/catalog.rs:430`, `:435`, `:449`, `:455`, `:493`, `:497` | Encoded rows → charges/validation | Validate IDs and exact intent links; use checked arithmetic; ordinary commits cannot consume reserved outcome space. |
| `inspect_retention_admitted` | `src/engine/retention/inventory.rs:94` | Exact candidates → blockers/boundaries | Uses the common retained-root inventory; validates removal records before skipping outcome-only rows. |
| `check_object_owner`, `confirm_creation`, `available` | `src/engine/retention/owners.rs:715`, `:768`, `:401` | Owner records and identities → authority/refusal | Owned attempt path plus reservation is required; exact typed identity must exist; unavailable paths block new grants before old-grant reuse. |
| `maintain_work`, `spawn_work`, `admit_stream` | `src/engine/worker.rs:136`, `:203`, `:159` | Service/stream future → owned work | Global write admission excludes active Engine workers/read streams; caller cancellation does not release the private worker's custody. |
| `remove_owned_file`, `reconcile_absent_file`, `owned_worker` | `src/storage/removal.rs:29`, `:42`, `:50` | Root, exact object, budget → effect/absence | Retain object and blocking-worker reservations through completion; absence reconciliation does not hash present payloads. |
| `checked_path`, `check_ancestors`, `check_regular` | `src/storage/removal.rs:79`, `:170`, `:182` | Path/metadata → safe local target/refusal | Confine to owned object namespace; reject parent traversal, symlink ancestry, and nonregular leaf objects. |
| `remove_file`, `check_identity`, `sync_parent` | `src/storage/removal.rs:123`, `:193`, `:217` | Exact object → deleted/absent/refusal | One MiB hashing buffer; size/digest and Unix device/inode/time checks; one unlink followed by parent sync. |
| Storage refusal helpers | `src/storage/removal.rs:223`, `:230`, `:237` | No inputs → fixed refusal | Retained outcomes use fixed path, identity, and I/O codes without copying arbitrary error text. |
| `snapshot_files` | `src/storage.rs:370` | Exact native metadata → physical inventory | Includes all referenced snapshots, manifests, manifest lists, statistics, and live file paths; sequential bounded native metadata reads. |
| HTTP removal handlers and `HttpError::into_response` | `src/http.rs:101`, `:117`, `:124`, `:135`, `:269` | JSON/path parameters → service JSON or refusal | Thin dispatch; missing ID is 404; conflict is 409; shared request body cap remains in force. |
| CLI `main` removal arms | `src/main.rs:333` | Parsed command/JSON → service JSON | Uses the shared services and existing bounded JSON-file reader. No alternate removal implementation. |

The pinned SQL catalog implementation was inspected directly:
`drop_table` at its `src/catalog.rs:750` deletes only the catalog row;
`purge_table` at `:778` additionally invokes physical data deletion. The patch
calls only the former. This evidence is specific to the installed dependency
version, not an assumption from method names.

## Data flow and invariants

1. **Preview is a bounded candidate set, not deletion authority.** The planner
   creates exact objects and native head identities. Execution recomputes blockers
   and uses the old/new eligible intersection (`removal.rs:345`). Current native
   effects must already occur in the submitted plan (`:364`). A changed catalog
   UUID refuses instead of interpreting an old plan against a new owner.
2. **Reservation precedes effects.** Intent, progress reserve, and each unavailable
   marker share one guarded lifecycle commit (`removal.rs:403`). Unknown commit
   responses are reconciled before proceeding (`:418`). New owner grants consult
   availability before returning even a matching old grant
   (`src/engine/retention/owners.rs:661`). This invariant depends on the cooperative
   owned host; direct external catalog/filesystem mutation is not fenced here.
3. **Pending retry cannot add effects.** Saved indices and markers are validated;
   a fresh root or unreserved native head refuses (`removal.rs:581`). A saved
   registration that still exists but is no longer related to targets also
   refuses (`:609`). Native registration absence is reconciled; all registration
   outcomes must finish before any file effect (`:688`).
4. **Unknown effects remain inspectable.** The saved outcome may remain unresolved
   after a successful unlink and failed report. Resume checks absence and reports
   `Absent`, not another confirmed unlink (`removal.rs:649`, `:701`). Completed
   state returns before traversal or effects (`:576`), including when unrelated
   bytes later appear at the old path.
5. **Final reporting capacity survives restart.** Intent initially reserves two
   worst-case outcome-row charges (`removal.rs:398`). Changed incomplete reports
   retain one terminal floor (`:712`); ordinary commits and cold replay charge
   this room. If every effect is already absent, reconciliation writes the final
   report without a top-up (`:625`). New effects require additional reporting room
   first (`:668`). This reserves replay-accounting bytes, not free disk space or
   a complete process-memory guarantee.
6. **Physical scope stays exact.** The adapter rejects path/type/identity changes,
   hashes only pending present targets, and unlinks no directory. A final path
   check followed by unlink is not an atomic conditional unlink against a hostile
   external writer; the code explicitly excludes that model
   (`src/storage/removal.rs:20`). A Unix hard link or open external descriptor can
   retain allocated blocks, so status correctly reports logical file sizes only
   (`src/model.rs:636`, `removal.rs:955`).

Preview and pending execution use the existing root walker rather than inventing
a second retention ledger. Payload work is proportional to present selected file
bytes; the hashing buffer is fixed and files are handled sequentially. Metadata
work grows with catalog tables, retained lifecycle history, and referenced native
snapshots. Completed retries avoid that work. These are static complexity and
reuse observations; no speed, peak-memory, or disk-reclamation measurement was
performed in this review.

## Test behavior and edge cases

The following assertions were read in source. “Expected pass” is a static
conclusion from the traced branches, not a new execution result. The retained
author gate reports them passing (`docs/reviews/physical-removal-check.log:14`,
`:84`, `:134`, `:347`); earlier failed compile/lint logs remain available. The
resource JSON is a point-in-time build-cache/free-volume observation and supplies
no maintenance performance evidence.

| Test and source line | Expected behavior and evidence |
| --- | --- |
| `exact_deletion_preserves_selected_values_and_completed_cold_retries_skip_effects`, `removal/tests.rs:162` | Expected pass: old target disappears; replacement is exactly `[8, 9]`; reopened completed retries preserve newly written unrelated bytes at the old path. |
| `fresh_owner_protection_shrinks_a_stale_preview_without_deletion`, `removal/tests.rs:215` | Expected pass: later owner grant yields empty chosen scope, retained blocker, and unchanged file. |
| `same_size_corruption_retains_failure_and_unavailability_until_exact_resume`, `removal/tests.rs:242` | Expected pass: hash mismatch persists failure and prevents a new grant; restoring exact bytes permits same-intent resume. |
| `already_absent_target_reports_absence_without_newly_unlinked_bytes`, `removal/tests.rs:294` | Expected pass: missing file becomes `Absent`; logical confirmed-unlink total stays zero across reopen. |
| `exact_batch_removes_retired_data_and_native_metadata_without_requiring_them_on_cold_retry`, `removal/tests.rs:318` | Expected pass: native registration detaches before metadata/data deletion; completed replay requires only lifecycle records; replacement values remain exact. |
| `new_native_heads_block_preview_execution_and_pending_resume_without_expanding_effects`, `removal/native_tests.rs:40` | Expected pass: alias added before execute or after reserved intent refuses; removing alias permits exact saved resume. |
| `provisional_native_head_detaches_while_selected_shared_data_remains_readable`, `removal/native_tests.rs:106` | Expected pass: unselected mixed registration drops while selected shared files remain. Its row assertion is a count; the separate service fixture checks exact replacement values. |
| `active_engine_reader_delays_physical_removal_until_eof`, `removal/native_tests.rs:171` | Expected pass: active stream owns read admission; deletion waits beyond the returned batch until EOF. |
| `malformed_reserved_marker_refuses_inventory_and_resume_before_effects`, `removal/native_tests.rs:194` | Expected pass: marker identity/revision mismatch refuses before deletion. |
| `forged_saved_registration_cannot_drop_lifecycle_or_unrelated_selected_native_table`, `removal/native_tests.rs:245` | Expected pass: lifecycle name fails structural validation; unrelated existing native table fails fresh membership check. |
| `cold_resume_reconciles_each_durable_effect_boundary_without_inventing_unlinks`, `removal/fault_tests.rs:38` | Expected pass: injected-error/reopen at intent, registration, and unlink boundaries preserves intent; post-unlink recovery reports absence. Does not kill a process. |
| `lost_intent_and_outcome_replies_reconcile_the_same_saved_operation`, `removal/fault_tests.rs:77` | Expected pass: post-commit lost-response hook is reconciled; retry does not add another lifecycle revision. |
| `terminal_absence_reconciliation_uses_reserved_floor_when_no_topup_can_fit`, `removal/fault_tests.rs:114` | Expected pass: ordinary commit refuses at history limit; already-absent target finishes using the reserved final row. |
| `cold_and_warm_reservations_count_only_current_states`, `src/catalog.rs:703` | Expected pass: cold/warm current reservations agree; obsolete state history remains charged separately. |
| `ordinary_commit_preserves_exact_terminal_headroom`, `src/catalog.rs:749` | Expected pass: ordinary write cannot consume final reserve; terminal state can consume it. |
| `invalid_removal_references_refuse_before_native_commit`, `src/catalog.rs:795` | Expected pass: malformed state, wrong intent revisions, orphan records, duplicate intent, and underreserved progress leave head unchanged. |
| `cold_reservation_validation_rejects_malformed_and_dangling_states`, `src/catalog.rs:902` | Expected pass: malformed JSON, dangling intent, invalid IDs, and overflow refuse in shared cold-validation helpers. |
| `absence_reconciliation_skips_present_bytes_and_rejects_unsafe_types`, `src/storage/removal.rs:258` | Expected pass: changed present regular bytes are untouched; missing file reconciles; directory/symlink refuses. |
| `deletes_exact_file_and_reconciles_absence`, `src/storage/removal.rs:296` | Expected pass: target removed, neighbor retained, second adapter call reports absence. |
| `refuses_same_size_changed_bytes`, `src/storage/removal.rs:313` | Expected pass: digest mismatch retains changed bytes. |
| `refuses_escaped_paths_and_wrong_owner`, `src/storage/removal.rs:327` | Expected pass: escape, dot components, and wrong owner refuse before effects. |
| `refuses_directory_and_leaf_symlink`, `src/storage/removal.rs:349` | Expected pass: nonregular leaf refuses; external symlink target remains untouched. |
| `refuses_ancestor_symlink`, `src/storage/removal.rs:375` | Expected pass: moved attempt directory behind a symlink refuses without touching external bytes. |
| `cli_http_preview_agrees_and_keeps_protected_bytes`, `tests/physical_removal_transport.rs:29` | Expected pass: equal protected plans, body/schema rejection, missing-operation 404, unchanged selection. Successful execute/resume is absent from this fixture. |

The directory shorthand `removal/` in the table means
`src/engine/retention/removal/`.

## Reviewed working-tree follow-up

The additional source scope is `src/engine/retention/removal.rs`, `src/main.rs`,
`src/storage/removal.rs`, `tests/physical_removal_process.rs`,
`tests/physical_removal_transport.rs`, and `tests/support/mod.rs`. The following
references name working-tree lines after the resource correction.

### Resource admission correction

| Function / seam | File:line | Inputs → output | Verified behavior |
| --- | --- | --- | --- |
| `removal_plan_admitted` proof/inventory handoff | `src/engine/retention/removal.rs:205` | Owned candidates and owner records → inspection/native scope | Eight maxima cover historical creation decoding only; owner rows drop before four maxima cover returned inventory collections. |
| `remove_admitted` saved branch | `src/engine/retention/removal.rs:344` | Saved intent/state → existing execution | `reserve_decoded` retains eight times the actual encoded records plus fixed overhead through `finish_removal`, replacing the unconditional twelve-maxima reservation. |
| `remove_admitted` new intent preparation | `src/engine/retention/removal.rs:364` | Fresh plan → eligible intent | Proof eight-maxima allowance ends before working four-maxima allowance and nested inventory; unused candidates and selected clones drop before encoding. |
| `remove_admitted` encoding/commit | `src/engine/retention/removal.rs:408` | Captured fresh scope → serialized records/commit | Additional eight-maxima allowance starts only after native enumeration returns; combined encoding allowance remains the original twelve maxima. |
| `finish_removal` pending eligibility | `src/engine/retention/removal.rs:616` | Saved intent → fresh root/native checks | Scoped four-maxima allowance covers newly grown namespace/boundary collections that may exceed the old intent's size; it ends before effect reporting. |

This changes resource lifetimes, not eligibility, intent identity, deletion order,
or durable history reserves. The request permit already covers owned request/plan
copies (`src/engine/worker.rs:91`). Inventory individually bounds its candidate,
boundary, and selected collections; native registration enumeration owns its
transient allowance. The correction keeps each original proof/encoding allowance
when it is needed and avoids holding it during an independent bounded scan.
Saved decoding remains charged across retries, and the new pending-resume
allowance covers growth independent of the saved request. No correctness boundary
was removed and no capacity limit was raised.

### Filesystem failure adapter

At the parent's explicit request, the additional storage-only diff was reviewed
after the commit review. It introduces `remove_file_with` at working-tree
`src/storage/removal.rs:129`; production `remove_file` passes the same standard
unlink and `sync_parent` functions at `:123`. Path, digest, identity, and effect
order remain unchanged. The injection is narrow and does not create a second
filesystem implementation or public API.

`failed_unlink_preserves_bytes_and_does_not_synchronize` at working-tree `:324`
injects permission denial and verifies retained bytes and no sync call, then
performs the normal deletion. `failed_directory_sync_reports_uncertainty_then_reconciles_absence`
at `:342` injects failure after a real unlink, verifies that absence alone still
refuses while sync fails, and then reconciles `Absent` through the production
helper. Both tests are expected to pass by static trace. They exercise the exact
branches missing from the original file tests; the reviewed author-run
`docs/reviews/physical-removal-filesystem.log:7` and `:10` report both passing.
They do not simulate power loss.

### Process and transport qualification

`Engine::removal_until` at working-tree `src/engine/retention/removal.rs:274`
uses the same request/maintenance ownership and `remove_admitted` with the
existing phase hook; it is compiled only under `test-util`. The CLI's
`PausedOperation::Removal` branch at `src/main.rs:304` accepts `RemovalPlan` and
normalizes success to `()` just as the other paused operations now do. No ordinary
CLI path or recovery result changes. `PauseAfter` emits its marker and waits only
after the service reports `injected_failure` (`:309`).

`tests/support/mod.rs:95` extracts the existing fresh-process runner into generic
`run_cli<T>` so it can decode `RemovalStatus` as well as `ReleasePin`. The existing
`resume` wrapper passes the same command and request path as before. Output stays
file-backed, and the same bounded wait terminates a hung child. `kill_after` checks
the acknowledged marker and actual SIGKILL signal before proceeding (`:64`).

`sigkill_at_each_removal_boundary_resumes_exact_effects_in_a_fresh_cli` at
`tests/physical_removal_process.rs:108` covers the intent, native-registration,
and post-unlink handoffs. Fresh CLI inspection must expose unresolved objects and
the same saved plan; fresh resume must preserve exact intent, distinguish removed
from absent registration/file outcomes, and release its future-history reserve.
The selected-result oracle compares complete values including duplicates, nulls,
Unicode, and empty text (`:80`). Completed CLI execution/resume retries must leave
unrelated new bytes at the old path untouched (`:200`). The reviewed author-run
`physical-removal-process.log` reports this fixture passing with each actual
SIGKILL and unchanged limits. The failed first attempt remains retained.

The HTTP fixture's added branch at working-tree
`tests/physical_removal_transport.rs:92` calls execute, inspects its complete empty
intersection, then requires the same status from GET inspection and POST resume.
It verifies that selected bytes survive. This directly covers the successful
transport paths missing in the original test without making a new file-removal
authority path. F3 records the narrower eligible-file HTTP gap.

No blocker was found in these follow-up source changes. They qualify named
handoffs and branches; they do not establish arbitrary mid-system-call crashes,
power loss, filesystem/provider behavior beyond the controlled errors, or Q6
restore fencing. The author-run complete `./scripts/check` exited successfully.
The reviewed `docs/reviews/physical-removal-qualified-check.log` records the
build/lint stages at `:1`, passing filesystem failure cases at `:135` and `:137`,
passing process recovery at `:352`, passing transport coverage at `:359`, and
successful completion through documentation tests at `:515`. This is retained
author execution evidence; the reviewer did not run the gate.

## Conclusion

VERDICT: APPROVE for the explicit local removal implementation in `ecb31f1`
together with the reviewed resource, filesystem, process, and transport follow-up.
The original commit alone retains F0's reproduced capacity defect.

Justification:

- The patch achieves its local intent: it preserves current roots, reserves exact
  authority before effects, refuses changed scope, and reconciles uncertain
  outcomes without inventing confirmed unlinks.
- Coverage of changed core service paths: ADEQUATE for this static checkpoint,
  including reviewed author evidence for SIGKILL at acknowledged handoffs.
  Broader failure-domain, resource-scaling, restore, and remote qualification
  remain INSUFFICIENT for the full accepted plan.
- Confidence: HIGH in the traced local control flow; MEDIUM in operational
  qualification because this review ran no tests and the fixtures do not cover
  all failure domains. The complete author-run follow-up gate passed.

Compaction, explicit restore points and old-deleter fencing, public/remote
availability policies, full Q3/Q6/Q7 qualification, and the complete Comments
performance gate remain required by the plan. This review neither enables
automatic cleanup nor authorizes deletion of any retained corpus.

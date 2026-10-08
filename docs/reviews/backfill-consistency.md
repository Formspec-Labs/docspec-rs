# Row-local backfill consistency review

Review scope: the uncommitted checkpoint after `64666e1`, inspected statically on
2026-10-08. This review applies the semi-formal code-review and Microsoft Rust
guidance. No tests, benchmarks, or implementation edits were performed by this
reviewer. Line references in initial findings refer to the inspected pre-fix
working tree; closure evidence, when present, supersedes them.

## Findings

All findings are closed in the final static inspection. The initial findings
below retain the original failure sequences so the corrections remain
reviewable. The final verdict and qualification limits appear after closure
evidence.

### F1 — Globally invalidated selections can enter a new backfill release

- **Severity:** BLOCKER; correctness.
- **Location:** `src/engine/backfill.rs:438`, `:645-658`;
  `src/engine.rs:369-420`.
- **Evidence chain:** `invalidate` advances eligibility without changing the
  selected output. A new backfill captures the new authority eligibility, but
  promotion compares only authority and operation eligibility. It never requires
  the selected source/result to have that qualification. The new result clones
  the entire old selected dataset, including its old `eligibility` and existing
  sibling groups.
- **Failure sequence:** Select source and group A at eligibility 0; record a
  dependency correction using `invalidate(reason, None)`; start group B at
  eligibility 1. Its own computation uses new file-phase keys, but promotion can
  select the old source and A in a newly acknowledged release at eligibility 0.
  Historical readability does not authorize new adoption of invalidated work.
- **Required correction:** Refuse new backfill adoption when its selected/base
  qualification differs from the captured operation. Preserve historical result
  reconciliation. Broader coordinated requalification can remain outside this
  checkpoint. Add an unchanged-definition invalidation fixture containing an
  already selected group.

### F2 — A refused source operation can block every later group selection

- **Severity:** BLOCKER; correctness.
- **Location:** `src/engine.rs:518-554`, `:749-761`;
  `src/engine/write.rs:90-96`; `src/engine/backfill.rs:654-662`.
- **Evidence chain:** Source registration advances `authority.boundary` before
  processing. An update against selected groups refuses in `process_update`; a
  full build refuses before final selection. Neither advances the selected
  source boundary. Backfill requires the authority admission boundary to equal
  the selected source boundary.
- **Failure sequence:** Select group A; attempt an unsupported source update or
  build; receive `dependent_groups`; request independent group B. B now refuses
  `stale_source_boundary`. A source operation cannot repair the boundary because
  selected groups still prohibit source selection. A rejected request has
  changed the dataset's ability to accept otherwise supported work.
- **Required correction:** Check the known dependent-group refusal under the
  initial source registration lock before advancing admission. Preserve the
  final guard for races. Test both update and build refusal followed by a
  successful sibling-group addition, with unchanged source coverage/admission.

### F3 — Validation and release readback lack durable recovery boundaries

- **Severity:** BLOCKER; recovery correctness and avoidable repeated work.
- **Location:** `src/engine/backfill.rs:419-420`, `:450-466`;
  `tests/backfill.rs:231-263`.
- **Evidence chain:** Prepared file and group results are durable, but
  `verify_group` returns directly to the injected `validation` failure without
  persisting its outcome. The composed manifest and full release readback are
  likewise unacknowledged until final selection. Retry repeats those completed
  phases. The recovery test compares only retained file-completion records, so
  it cannot establish that validation was reused.
- **Required correction:** Retain a bounded validation completion for the exact
  source/group/binding and a readback completion for the exact composed result.
  Resolve eligible completions before repeating the phase. A sibling selection
  may require a new combined readback; it need not discard qualified file or
  group validation. Add recovery assertions at each actual durable boundary.
  This follows `docs/PLAN.md:Recovery and reuse` and the proposal's explicit
  completed-validation requirement.
- **Closure follow-up:** The new exact validation and composition-specific
  readback records address persistence. Reuse must also check retained-object
  availability under the managed immutable-object trust boundary. Otherwise,
  deleting a group Parquet after the `readback` stop lets retry select an
  unavailable result without even the availability/size check used by existing
  `Engine::phase`. A cheap exact-reference check is sufficient; repeating native
  decoding is unnecessary.

### F4 — The plan cannot distinguish retained progress from promotion eligibility

- **Severity:** WARNING; API semantics and recovery visibility.
- **Location:** `src/engine/backfill.rs:32-40`, `:124-181`.
- **Evidence chain:** Planning counts completion-key presence but neither checks
  current definition/source/target guards nor returns an eligibility result.
  After source advancement or invalidation it can report all files completed
  while execution must refuse. A new plan with an inactive definition can also
  succeed although execution refuses before preparation.
- **Suggested correction:** Keep the useful retained-progress view and add a
  bounded promotion refusal/status. Mirror the run guards for new plans, while
  making stale-source retained work visible for catch-up. Planning need not hash
  every payload merely to report catalog state.

## Patch summary

The checkpoint adds independently stored scalar field groups over an exact
retained source, file-level recovery, combined reads, guarded group replacement,
and CLI/HTTP entry points. It uses maintained native readers and DataFusion
expressions. It explicitly defers ongoing source updates after group selection,
metadata-only renames, stable occurrence identity across compaction, and full R3.

The physical file/ordinal locator remains bound to an immutable source object;
it is not claimed as a permanent business identity. This is consistent with the
preceding design review's narrow native-reader remedy.

## Function trace

| Function or type | Location | Inputs and outputs | Inspected behavior |
| --- | --- | --- | --- |
| `BackfillSpec`, `ComputedField`, `ComputedGroup` | `src/model.rs:170-205` | Source version, group definition, stable native field IDs; serialized source binding | Distinguish target group, exact base manifest, and source metadata. |
| `validate` | `src/engine/backfill.rs:42` | Spec → result | Rejects empty/duplicate/reserved field definitions and invalid operation/group identity. |
| `plan_backfill` | `src/engine/backfill.rs:124` | Spec → bounded progress | Parses expressions, captures native file metadata, resolves exact file completion keys; F4 covers missing promotion status. |
| `backfill`, `backfill_until`, `backfill_internal` | `src/engine/backfill.rs:188-201` | Spec and optional failure point → release | Stable request reconciliation precedes work; records capture, resolves prepared output, builds missing files, validates and attempts merged selection. |
| `reserve_backfill` | `src/engine/backfill.rs:497` | Operation and purpose → protected directory | Checks execution epoch; records protection before writing outputs. |
| `acknowledge_files` | `src/engine/backfill.rs:535` | Pending completions → lifecycle records | Preserves first acknowledged completion; uses the preparation epoch guard rather than a mutable source-head guard. |
| `compute_file` | `src/engine/backfill.rs:563` | Captured native file and expressions → completion | Hashes the source, reads its full native task, computes scalar values, writes native files and retained Avro descriptors, checks row count. |
| `check_preparation`, `promotion` | `src/engine/backfill.rs:628-676` | Authority, captured operation/source/target → result | Separates persistence from promotion; promotion checks source, covered boundary, admission fence, definition, and same-group selection. F1 concerns inherited eligibility. |
| `file_key`, `read_completion` | `src/engine/backfill.rs:687-744` | Exact object/native schema/definition/eligibility → retained files | Includes owner/path/digest/size, native interpretation, group fields and processor; independently verifies descriptors and output identities. |
| `Expressions::compile`, `evaluate` | `src/engine/backfill/expressions.rs:25`, `:114` | Source schema/spec and located batches → typed group batches | Accepts row-local immutable expressions, preserves locators, sets explicit native field IDs, rejects nested computed output, bounds returned batch bytes. |
| `verify_groups`, `read_base`, `verify_group` | `src/engine/backfill/verification.rs:6`, `:45`, `:64` | Dataset/source/group pins → verification | Checks exact request/source/schema bindings, hashes native objects, reads group rows and proves locator coverage. It does not recompute every expression value. |
| `FileCoverage::insert`, `complete` | `src/engine/backfill/verification.rs:159`, `:184` | Source-file intervals → exact coverage | Rejects overlap, gaps, unknown/out-of-range locators; merges adjacent intervals; empty files need empty coverage. |
| `OccurrenceScan::open`, `files`, `read_file` | `src/storage/occurrences.rs:207-278` | Exact native table/file → located batches | Captures each allowed full native task once; refuses unknown/repeated/omitted files; retains metadata and batch permits for selected file reads. |
| `read_combined` | `src/engine/backfill.rs:76` | Verified release → Arrow stream | Verifies coverage before joining each group on physical file and ordinal; quotes user field names; preserves duplicate/null business keys. |
| `query::provider`, `NativePartition::execute` | `src/query.rs:26`, `:59` | Native table and reader kind → maintained streaming provider | Uses the existing admitted native stream or the located variant; maps embedded DocSpec errors through DataFusion. |
| `prepare_and_select`, `verify_release_with_budget` | `src/engine.rs:494`, `:1156` | Source operation/release → selection or verification | Source selection preserves dependent groups by refusal; format-3 verification now delegates group coverage. F2 covers the earlier admission mutation. |
| `OperationRequest` helpers and `fingerprint` | `src/model.rs:207-262`, `src/engine.rs:940-959` | Typed request → IDs/material digest | Backfill uses the common operation namespace; changing material under one operation refuses. |
| HTTP handlers | `src/http.rs:22-26`, `:108-119` | JSON spec → shared plan/run service | Thin bounded metadata endpoints; stale source/group conflicts map to HTTP 409. |
| CLI dispatch | `src/main.rs:60-69`, `:183-206` | Spec/release files → service results or Arrow IPC | Uses the same services; combined output uses create-new and streaming batches. |

## Data flow and invariants

| State or invariant | Creation, mutation, use and escape | Assessment |
| --- | --- | --- |
| Exact source and target capture | Captured under dataset lock; retained in operation/capture records; compared again during promotion | Source equality includes the complete `TablePin`; same-group comparison uses immutable metadata identity. |
| File reuse identity | `file_key` binds exact `ObjectRef`, native schema, processor, group and scalar definition plus eligibility | Addresses the design review's digest-only reuse concern; identical bytes at different paths remain separate observations. |
| Persistence versus selection | File/prepared completion needs epoch authority; selection additionally needs definition, source, target and admission guards | Source advancement retains reusable work. Invalidation must still prevent new adoption of old selected results: F1. |
| One row per physical occurrence | Reader assigns locators before expression evaluation; scalar compiler cannot filter/join/aggregate; coverage verifier checks exact intervals | Equal counts alone are not accepted as coverage. Successful null is a completed row. |
| Sibling preservation | Promotion loads current selection, replaces one group, verifies that exact combination, then rechecks the current selection under lock | A compatible sibling change repeats composition instead of committing an obsolete whole-result map. |
| Stable completed-operation retry | Result lookup occurs before preparation and again around promotion | A completed operation returns its original release instead of republishing into latest. |
| Source admission versus covered source | Source work increments admission; backfill leaves it unchanged and requires equality at promotion | Correct conservative fence, but rejected unsupported source requests must not advance it: F2. |
| Durable expensive-phase reuse | File and prepared-table records exist; validation/readback initially lacked records | F3 remains necessary for the advertised recovery semantics. |
| Protected references | Attempt directory protection precedes output creation; group pins inherit reused file owners; base manifests remain explicit bindings | Cleanup is disabled; this is not qualification of retirement, restore, or permanent logical identity. |

## Test behavior and edge cases

The following source assertions were inspected; execution is the implementing
agent's responsibility.

| Test | Location | Static expectation and limits |
| --- | --- | --- |
| `combined_values_preserve_duplicate_null_empty_and_unicode_occurrences` | `tests/backfill.rs:165` | Direct Rust reconstruction compares full combined values, checks unchanged source/boundary, stable native field ID and operation conflict. Expected to pass on the inspected happy path. |
| `empty_source_has_typed_complete_coverage_without_fake_rows` | `tests/backfill.rs:213` | Empty native source/group and combined output remain typed and empty. Expected to pass. |
| `retained_file_handoffs_resume_after_reopen_and_external_input_loss` | `tests/backfill.rs:231` | Checks file records survive reopen and original-input loss. It does not establish validation/readback reuse; F3 requests that stronger assertion. |
| `stale_source_retains_files_and_catchup_reuses_unchanged_objects` | `tests/backfill.rs:267` | Prepared work survives source update, old promotion refuses, catch-up reports both reused and missing files, and combined values match direct reconstruction. |
| `nonlocal_and_volatile_expressions_refuse_before_selection` | `tests/backfill.rs:313` | Aggregate, volatile, window and subquery expressions refuse in plan/run and leave latest unchanged. |
| `equal_counts_do_not_prove_coverage` | `src/engine/backfill/verification.rs:197` | Directly demonstrates holes, overlaps, adjacency, duplicates and out-of-range intervals; independent malformed native group fixtures are still needed to exercise integration. |

Pending coverage at initial inspection: sibling selection races, same-target
conflict, unchanged-definition invalidation, refusal without admission mutation,
full validation/readback reuse, native malformed coverage, selective-file reader
boundaries, and HTTP backfill dispatch. The implementing agent is adding fixtures;
their actual assertions must be inspected before closure.

## Initial conclusion

**VERDICT: REQUEST CHANGES** at initial inspection.

The exact occurrence and sibling-merge structure follows the agreed narrow
design. F1 and F2 permit invalid lifecycle transitions, and F3 misses a required
recovery boundary. Coverage of the initial happy path is useful but currently
insufficient for those branches. Static confidence is high for the concrete
failure sequences; no runtime or performance claim is made.

## Closure inspection

- **F1 corrected:** New operations require captured-base eligibility to match
  current authority (`src/engine/backfill.rs:304`); promotion also requires the
  selected result's eligibility to match the captured operation (`:791`). The
  fixture at `tests/backfill.rs:422` invalidates an already enriched selection
  without changing its definition, then asserts no new release can be selected.
- **F2 corrected:** `prepare_and_select` checks selected computed groups under
  its registration lock before changing admission (`src/engine.rs:525`). The same
  fixture checks both update and full-build refusal, unchanged authority, and a
  successful independent group afterward.
- **F3 corrected:** Validation records bind request material/eligibility
  to prepared native metadata; readback records include a digest of the exact
  composed dataset (`src/engine/backfill.rs:496-600`). Recovery now checks that
  those records survive all supported stops (`tests/backfill.rs:228`). A
  compatible sibling changes the composed digest, so old readback cannot select
  a result that drops it (`tests/backfill.rs:338`). Prepared results and cached
  readbacks check exact retained-reference availability and size before adoption
  (`src/engine/backfill.rs:371`, `:554-575`; `src/storage.rs:114-134`). The focused
  fixture deletes either computed data or retained input after readback and
  verifies refusal without selection (`tests/backfill.rs:510`). This uses the
  existing managed immutable-object trust boundary; it does not claim detection
  of arbitrary concurrent external same-size mutation without full verification.
- **F4 corrected:** `BackfillPlan::promotion_refusal` preserves retained progress
  while reporting the same definition/source/target guards used for promotion
  (`src/engine/backfill.rs:158-191`). Stale-source, stale-target and stale-
  eligibility fixtures inspect the returned refusal codes.
- **Native field identity adjustment reviewed:** Iceberg's maintained
  `TableMetadataBuilder::new` explicitly reassigns field IDs for new tables
  (`iceberg-0.10.1/src/spec/table_metadata_builder.rs:77-89`). The implementation
  now retains group-scoped logical IDs and an exact logical-to-native mapping
  (`src/model.rs:170-202`, `src/engine/backfill/expressions.rs:91-111`). Independent
  verification rejects mapping/schema disagreement
  (`src/engine/backfill/verification.rs:87-94`). This avoids custom catalog
  mutation and does not claim completed rename/evolution support.
- **Concurrent paths inspected:** A suspended old readback is recomposed after a
  sibling wins; two replacements of the same target yield one selected winner;
  exact completed-operation retry leaves latest unchanged
  (`tests/backfill.rs:338`). The parallel independent-backfill fixture checks both
  operations finish and both groups survive (`:495`). These are source-level
  observations; this reviewer did not execute them.
- **Independent malformed native results inspected:** The fixture at
  `tests/backfill.rs:625` preserves row count while introducing duplicate/missing
  locators, unknown files, out-of-range locations, null identity or a false native
  field mapping. It rebuilds immutable native tables and serialized manifests so
  object-integrity rejection does not substitute for semantic verification.
  Duplicate/unknown/range cases explicitly assert identical native schemas before
  verification, demonstrating that schema refusal cannot mask the locator checks.
- **Additional behavior inspected:** Identical concurrent attempts return one
  recorded result (`tests/backfill.rs:746`), supported by error-path reconciliation
  in `src/engine/backfill.rs:241-269`. Failed files remain distinct from successful
  nulls; completed work survives until an exact source correction enables catch-up
  (`tests/backfill.rs:758`). The HTTP fixture exercises shared plan/run routes and
  stale-target HTTP 409 behavior (`tests/backfill.rs:831`).
- **Consumer output admission inspected:** The final combined stream now has its
  own batch admission after joins (`src/query.rs:86-109`). The fixture at
  `tests/backfill.rs:559` establishes a limit accepted by every individual input,
  then checks that the wider joined output refuses with `oversized_batch`. This
  does not establish a whole-process memory bound.

## Final conclusion

**VERDICT: APPROVE** for the narrow executable row-local backfill checkpoint.

- The corrected implementation retains completed work, prevents stale source,
  target and eligibility adoption, preserves independently selected siblings, and
  verifies exact physical occurrence coverage. No remaining correctness blocker
  was found in the inspected paths.
- **Coverage of changed paths: ADEQUATE** for static acceptance of this checkpoint.
  The implementing agent completed `./scripts/check` with exit 0; the retained
  [gate output](backfill-check.log) records successful execution of every backfill
  fixture above, including identical attempts, failure outcomes and HTTP routes,
  together with the existing unit, lifecycle and update suites. The inspected
  `scripts/check` runs formatting verification, locked Clippy with warnings denied,
  and locked tests with all features. This reviewer inspected the script and saved
  output but executed no tests. The final heap-pinned preparation future preserves
  the same awaited reconciliation path (`src/engine/backfill.rs:248-250`);
  the retained gate qualifies its default-stack execution.
- **Confidence: HIGH** for the local consistency paths reviewed. This is not
  qualification of full R3, native schema-only evolution, coordinated dependent
  refresh, cleanup/restore, hosted operation, or the large Comments performance
  target. Invalidation of an already enriched dataset conservatively blocks new
  adoption until coordinated requalification exists; historical reads remain
  available. This operating limit is now explicit in `README.md:152-156` and
  `docs/STATUS.md:193-198`. The status document also preserves the unqualified
  process-kill recovery, complete-path scaling and whole-process memory limits
  (`docs/STATUS.md:209-212`).

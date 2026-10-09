# History metadata and authority review

**Disposition: APPROVE for the scoped, non-destructive checkpoint.** No material
finding remains open in the reviewed source. A malformed progress-key case found
during review was corrected and added to the test cases. The separate
[superseded review](progress-protection-review.md) remains retained; its candidate
and runtime evidence do not qualify this replacement.

This is an independent static review of the current changes against `7e94506`.
It applies the repository instructions, Rust guidance, semi-formal review method,
and R4/R5/R6 requirements. The reviewer did not run Cargo, format Rust, or commit.
Test assertions were read and traced; author-run execution and the final gate
remain separate evidence.

## 1. Patch summary

New selections keep `succession` and `input-progress` as inline catalog evidence.
They no longer acquire or inherit full payload protection merely to establish
ordering or report lag. The actual `result` still receives the candidate's
existing protection in the same selection commit
(`src/engine/history.rs:219`). This follows R4's minimal accepted-succession rule,
R5's exact coverage/nonregression requirement, and R6's distinct history/content
lifetimes (`docs/requirements/04-change-tracking.md:27`,
`docs/requirements/05-dependent-refresh.md:29`,
`docs/requirements/06-maintenance-retention.md:9`).

The inventory checks succession receipts against their co-committed result
identity and checks progress receipts against the result's captured inputs. It
does not expand the manifests named only as ordering endpoints
(`src/engine/retention/inventory.rs:983`). Actual result and phase roots still
expand their content; existing owner grants remain independently protected
(`src/engine/retention/inventory.rs:170`, `:874`).

Mutable authority now requires an exact result value and original selection
revision (`src/engine/retention/inventory.rs:879`). Admission and invalidation
can advance authority metadata without moving that selection revision. No second
authority binding is introduced, and an unbound result remains unbound.

## 2. Function trace

| Function or path | Location | Inputs → output | Verified behavior |
| --- | --- | --- | --- |
| `select_with_progress` | `src/engine/history.rs:160` | Guarded authority, retained candidate, optional proof map → atomic selection | Checks abandonment and actual predecessor; commits authority, bound result, plain succession, and plain progress together. No new owner acquisition. |
| `select` | `src/engine/history.rs:142` | Candidate with related inputs → selection | Generates fresh captured-input proofs before the common selection commit. |
| `select_refresh` | `src/engine/refresh.rs:222` | Refresh candidate and live guards → selection | Existing output/input/family locking, authority checks, ancestor checks, and Captured/Latest policy remain in place. The abandoned acquisition loop is absent. |
| Backfill selection | `src/engine/backfill.rs:724` | Retained composition → selection | Existing promotion, sibling composition, input guards, and completion reuse remain unchanged from the base production path. |
| `read_link` | `src/engine/history.rs:297` | Snapshot, observed revision, exact expected version → link and serialized-byte charge | Checks the expected identity and allowed revision, delegates receipt consistency, and returns both link and result byte charges. |
| `check_link_receipt` | `src/engine/history.rs:321` | Snapshot, native link revision, link → result byte charge | Requires a co-committed verified result with exact version/manifest identity, boundary, eligibility, and valid predecessor dataset/owner identity. It performs no external reads. |
| `follows` | `src/engine/history.rs:44` | Exact endpoints and captured producer state → succession proof | Still traverses the accepted head's chain, checks decreasing revisions and coverage, charges cumulative metadata, and refuses backward/unproven endpoints. |
| Authority inventory branch | `src/engine/retention/inventory.rs:879` | Authority and same-snapshot rows → exact selected roots or refusal | Checks selected/release equality, dataset authority, verified status, positive nonfuture selection revision, exact retained result equality, and result revision equality. |
| Succession inventory branch | `src/engine/retention/inventory.rs:983` | Inline link plus co-committed result → no additional physical roots | Checks canonical key/table, shared receipt rules, and inline object shape. It does not open predecessor or selected manifests solely for ordering. |
| Progress inventory branch / `ProgressResult` | `src/engine/retention/inventory.rs:996`, `:326` | Inline proof map and narrow result receipt → no additional physical roots | Checks canonical operation identity, same consumer revision, exact captured target vector, endpoint owners/identities, positive producer revision, and zero/nonzero distance consistency. |
| `validate_binding` / `Inventory::check_bindings` | `src/engine/retention/owners.rs:233`, `src/engine/retention/inventory.rs:495` | Existing binding and immutable grant references → validation/refusal | Legacy bindings still check claim digest/revision and every referenced owner token. Missing or altered tokens refuse even when the typed ordering claim has no physical roots. |
| `Lifecycle::rows` / `commit` | `src/catalog.rs:204`, `:257` | Immutable lifecycle state / guarded changes → bounded snapshot or next commit | Existing history/cache admission and expected-state commit rules remain unchanged. |

Important selection callers remain `select_prepared` (`src/engine.rs:1514`) and
the Refresh/Backfill paths above. The patch changes neither the lock order nor the
owned-work lifetime. No new callback, scheduler, cache, or external transaction
is introduced.

## 3. Data flow and invariants

**H1 — removing payload roots must not weaken accepted-order proof. Confirmed.**
The receipt check was extracted from `read_link`, rather than replaced by a
second ordering algorithm. `read_link` retains its observed-revision and expected
endpoint checks (`src/engine/history.rs:313`). `follows` retains chain membership,
monotone revision/boundary/eligibility checks, endpoint matching, and forward-order
checks (`src/engine/history.rs:79`, `:87`, `:95`, `:110`). The extracted helper adds
owner/dataset identity checks. Catalog receipts must remain available; unavailable
external payloads need not be reopened to prove order.

**H2 — metadata accounting must survive the helper extraction. Confirmed for
the changed traversal.** `check_link_receipt` returns the result's serialized
size; `read_link` adds it to the link's size (`src/engine/history.rs:316`), and
`follows` adds each charge before continuing (`src/engine/history.rs:83`). The
existing step bound and metadata reservation remain (`:55`, `:76`). The narrow
`SelectedRelease` decoder continues to avoid materializing group/file inventories
(`src/engine/history.rs:238`).

Progress inspection initially decoded a whole `ReleasePin`. During review it was
narrowed to `ProgressResult` and nested identity/coverage fields
(`src/engine/retention/inventory.rs:323`). This removes unnecessary duplicate
file-inventory, recipe, and evidence allocations. The bounded lifecycle snapshot
still contains the serialized result; this is not a claim that parsing skips its
bytes or that peak memory has been measured. Inventory retains its existing
aggregate decode allowance (`src/engine/retention/inventory.rs:110`), and the
catalog still bounds replay and cached records (`src/catalog.rs:228`, `:242`).

**H3 — legacy promises must not disappear when new receipts omit bindings.
Confirmed.** The writer stops creating new succession bindings; it does not
delete old binding/grant rows. The inventory visits every owner grant as an
independent object root (`src/engine/retention/inventory.rs:170`). It still checks
legacy bindings through the existing digest/revision and owner-token path
(`:179`, `src/engine/retention/owners.rs:249`). A legacy binding with no direct
ordering-object requirement does not authorize its referenced tokens to vanish.

**H4 — unknown result scope must not become a known empty scope. Confirmed.**
Selection passes the candidate's original optional protection only to its result
(`src/engine/history.rs:220`). Neither plain ordering records nor authority
inspection manufactures `ReferenceProtections::empty`. The existing unknown
predecessor test continues to assert that the older result is unbound and the new
result retains only its own known scope
(`src/engine/retention/owners/phase_tests.rs:443`).

Producer `observed_revision` belongs to a different lifecycle table from the
consumer receipt. Requiring positivity is valid; comparing it numerically with
the consumer commit revision would be wrong. The new fixture deliberately uses a
producer observation revision above the consumer's revision
(`src/engine/retention/inventory/history_tests.rs:121`).

Inventory receipt validation is a structural check of retained evidence. It does
not rerun every historical producer proof or certify a complete graph of retained
ordering records. Live promotion still obtains that proof through `follows`.
Actual result retention, not a lag observation, remains responsible for captured
input content. Future metadata compaction/retirement must preserve the required
receipt identities and original revisions.

## 4. Test behavior and edge cases

All dispositions below are static expectations from the assertions and traced
paths. They are not reviewer-executed results.

| Test | Location | Expected behavior |
| --- | --- | --- |
| `history_receipts_do_not_expand_unavailable_endpoints_but_results_keep_captured_inputs` | `src/engine/retention/inventory/history_tests.rs:156` | Succession/progress yield no physical roots despite unavailable endpoint manifests; actual result roots still protect captured input metadata and payload references. Inspection opens only the captured producer manifest. |
| `succession_roots_refuse_unanchored_or_malformed_ordering_receipts` | `src/engine/retention/inventory/history_tests.rs:220` | Missing or differently committed result receipts, wrong identity/boundary/eligibility, foreign/self predecessors, wrong key/table, unverified result, and invalid inline path refuse. |
| `progress_roots_refuse_wrong_receipts_endpoints_and_coverage` | `src/engine/retention/inventory/history_tests.rs:288` | Result/coverage mismatch, invalid revisions or owners, inconsistent distances, repeated key prefix, and one version ID naming different manifests refuse. |
| `existing_succession_bindings_and_owner_grants_remain_enforced` | `src/engine/retention/inventory/history_tests.rs:375` | A legacy binding remains valid and its grant protects bytes; replacing its token with a missing token refuses both binding validation and inventory. |
| `succession_keeps_actual_predecessor_without_inheriting_its_payload_scope` | `src/engine/retention/owners/phase_tests.rs:302` | Overlapping unbased builds retain the actual predecessor and candidate result scope without a succession binding. Removing old manifest/payload files in the private fixture and reopening still permits the metadata proof; owner revisions do not change. |
| `unknown_predecessor_scope_does_not_erase_the_selected_results_known_scope` | `src/engine/retention/owners/phase_tests.rs:386` | An unknown old result remains unknown; the selected result retains its known scope; inline succession still records exact endpoints. |
| `scalar_completions_keep_foreign_scope_across_stops_reuse_and_reopen` | `src/engine/backfill/protection_tests.rs:116` | The adjusted assertions retain result/readback scope and co-committed exact succession after sibling recomposition, while requiring no new succession binding. |
| `captured_selection_records_lag_latest_refuses_and_coverage_cannot_regress` | `tests/related_refresh.rs:536` | With the newer observed-head manifest hidden, cold reopening still selects older captured coverage and reconciles a retry; Latest refuses; actual forward coverage and regression checks remain. |
| Authority alias tests | `src/engine/retention/inventory/collector_tests.rs:115`, `:163`, `:247` | Exact known/unbound results are preserved; malformed, missing, incomplete, and rebound authority identities refuse; consistent empty authority adds no roots. |
| `completed_and_interrupted_roots_protect_without_opening_payloads` | `tests/retention_inspection.rs:37` | Admission/invalidation authority rewrites retain the original exact result and physical blockers without reopening absent payloads. |

Existing `tests/succession.rs:322` exercises missing, malformed, rebound, and
cyclic links; `tests/succession.rs:501` covers the small history limit and shared
transport path. The extraction preserves their refusal conditions and cumulative
metadata charging. Changed selection assertions also retain equal native
revisions for result and succession, immutable retry results, and owner-grant
stability.

The reviewer also read the author's successful
[inventory test log](history-metadata-inventory.log), including the new receipt,
legacy-grant, and malformed-prefix cases; the
[phase-protection log](history-metadata-phases.log); and the
[captured-refresh log](history-metadata-refresh.log). These are author-run
results, not independent re-execution. The complete repository gate was not
finished when this review was written.

The missing-file fixtures establish metadata independence. They do not constitute
authorized retirement or deletion tests: the fixture intentionally removes/hides
private test content while catalog result-retention promises still exist.

## 5. Findings and disposition

**Resolved WARNING — canonical progress key.** The first reviewed replacement
used `trim_start_matches("input-progress/")`, which accepted repeated prefixes as
the same operation key. It now strips exactly one prefix
(`src/engine/retention/inventory.rs:996`), and the malformed repeated-prefix case
is present in `progress_roots_refuse_wrong_receipts_endpoints_and_coverage`.
No equivalent alias remains in that path.

**Resolved resource observation — avoid full result copies for progress.**
The private `ProgressResult` decoder now reads only identity and captured inputs
(`src/engine/retention/inventory.rs:326`). No general cache or additional
durable representation was needed.

No material correctness, recovery, or maintainability finding remains open.
The concrete simplification is removal of predecessor payload-scope loading,
scope merging, and succession binding writes from selection, together with reuse
of one receipt checker. This is a verified reduction in work and state; no speed,
peak-memory, or retained-byte improvement was measured by this review.

## 6. Conclusion

VERDICT: APPROVE for the reviewed history-metadata and authority-alias change.

- The replacement preserves exact accepted ordering and actual result protection
  while removing the unintended new content promise from ordering-only records.
- Coverage of changed paths: ADEQUATE in the inspected tests for this checkpoint;
  runtime qualification depends on the author's targeted checks and repository
  gate.
- Confidence: HIGH in the static state, identity, and byte-accounting traces;
  MEDIUM in resource behavior without measurement.

Legacy grants and all presently retained result records remain roots. The patch
does not retire those promises, implement exact-object deletion, prove native
cleanup races, qualify compaction/restore, or complete R6/R1–R7 and the stated
performance acceptance plan (`docs/requirements/06-maintenance-retention.md:46`,
`:47`, `:49`; `docs/PLAN.md:121`, `:134`).

# Cutover admission prerequisite: implementation review

**Verdict: APPROVE the corrected prerequisite. No material finding remains open.** The patch prevents field/schema cutover from overlooking an unresolved source request below the selected admission boundary. It preserves the existing prepared work and reads lifecycle metadata for the new proof. The implementation owner's focused tests passed; the repository gate was still running when this report was finalized.

Reviewed on 2026-10-10 against `8566235f32abc90ea21cfbd4b75f610766cfdc2d`, including the live compatibility corrections in `src/engine.rs`, `src/engine/backfill.rs`, `src/engine/history.rs`, `src/engine/history/admissions.rs`, `src/engine/history/admissions/tests.rs`, `src/engine/retention.rs`, `tests/admission_formats.rs`, `tests/cutover_admissions.rs`, and suite registration in `tests/integration.rs`. The reviewer performed static analysis only: no builds, tests, benchmarks, or corpus reads. Runtime results belong to the implementation owner's retained evidence. The unrelated change-chain implementation review was neither read nor changed.

This is the accepted-input prerequisite identified by F2 in the [consistency design review](continuous-backfill-design-consistency-2026-10-10.md) and F1 in the [resource design review](continuous-backfill-design-resources-2026-10-10.md). It does not implement durable catch-up attempts, target lineage across recaptures, or a bounded admission fence. R3 remains broader than this patch (`docs/requirements/03-schema-backfill.md:37–59`; `docs/STATUS.md:5–12`).

## Findings and their disposition

### F1 — BLOCKER, correctness: old-format schema recovery initially blocked itself — RESOLVED

The initial patch called the guard with no distinction for the schema being selected. The reader correctly treats a format-10 schema as a source admission (`src/engine/history/admissions.rs:135–141`). An interrupted schema has an operation/request and possibly a complete readback, but no selected result. The reader therefore returns its own ID as pending (`:163–179`). This made the previously supported cold resume in `tests/admission_formats.rs` refuse forever.

The corrected caller passes only its already checked schema ID immediately before the same guarded selection (`src/engine.rs:1679–1708`). The private wrapper excludes precisely that ID, while the pure history reader still reports the complete pending set (`src/engine/retention.rs:21–42`). This means the schema is being resolved by the impending selection; it is not incorrectly recorded as already selected or abandoned. The retained format and source-boundary interpretation remain unchanged.

The revised `original_admissions_resume_and_mixed_history_stays_readable` fixture deliberately includes an unrelated earlier source request. It expects the old schema to refuse while that request remains pending, preserve the readback and active writer, and resume the exact readback after explicit abandonment (`tests/admission_formats.rs:83–197`). Static tracing supports those assertions. This also guards against an overly broad exemption of old schemas or all pending work.

### F2 — BLOCKER, correctness: old-format backfill initially bypassed the new proof — RESOLVED

The initial patch retained a `result_format == FORMAT` condition around both backfill checks. A format-10 backfill could capture selected boundary 3 while source request 2 remained unresolved. Its existing guard compared the selected boundary, current admission boundary, and capture boundary; all could equal 3. It therefore passed without accounting for request 2 (`src/engine/backfill.rs:1038–1047`).

The corrected plan and selection paths call the all-admissions guard for every otherwise eligible backfill, with no excluded operation (`src/engine/backfill.rs:389–394,880–890`). The old format's stricter above-boundary check remains in `promotion`; adding this proof does not relax its historical interpretation. The new `format_10_backfill_refuses_hidden_admission_and_reuses_exact_completion` fixture reconstructs the retained format-10 admission shape, checks plan/run refusal, cold retry, exact retained completion, unchanged output timestamps, and success after abandonment (`tests/cutover_admissions.rs:180–252`).

### O1 — OBSERVATION, performance: the proof is bounded history work

The reader traverses the accepted selection chain once and scans the operation prefix once (`src/engine/history/admissions.rs:76–111`). Its metadata reads, decoding and hashing grow with retained history and serialized request/result size. Tree lookup and insertion add logarithmic CPU factors. This is not a constant-time cutover check.

The added work is deliberately bounded: cumulative charges must fit `max_metadata_bytes` before the relevant decodes (`:29–35,61,81–82,112–118,164`). Each loop includes a cooperative yield (`:106,182`). Exhaustion refuses rather than dropping history or interpreting an incomplete scan as success (`:184–187`). The resource design review explicitly accepts bounded full-history accounting for this prerequisite (`continuous-backfill-design-resources-2026-10-10.md:47–52`). No runtime or memory improvement is claimed.

## Patch summary

Input is a captured dataset lifecycle view. The new reader validates the selected succession chain, then binds every retained operation to its co-committed request. Each source admission must have a qualified selected result on that chain or a matching explicit abandonment. Remaining source IDs block field/schema cutover, including IDs numerically below the selected boundary. The schema being committed can resolve its own historical source admission in that same selection.

The implementation reuses `read_head`, `read_link`, `check_selected_link`, `fingerprint`, and the native lifecycle snapshot. It adds no payload ledger or second source of selection authority. The backfill caller replaces the interval-only proof; the existing interval helper remains for schema admission (`src/engine/schema.rs:85–88`). Final selection is still the common `select` path (`src/engine/history.rs:143–247`).

## Function trace

| Function or method | Location | Inputs → outputs | Verified behavior |
| --- | --- | --- | --- |
| `charge` | `src/engine/history/admissions.rs:29` | Cumulative bytes, serialized size, limit → success/refusal | Uses checked arithmetic and refuses before the associated decode. Charges include a documented conservative multiplier and fixed overhead. |
| `pending_source_operations` | `src/engine/history/admissions.rs:48` | Exact `View`, dataset → pending source IDs | Checks dataset identity, accepted-chain receipts, operation/request binding, source boundaries, and abandonment. No source payload opens or writes. |
| `check_cutover_inputs` | `src/engine/retention.rs:27` | View, dataset, optional exact selecting schema ID → success or `pending_source` | Requires every other source admission to be resolved. Its only production `Some` caller is schema selection. |
| `plan_backfill_admitted` | `src/engine/backfill.rs:265` | Immutable `BackfillSpec` → `BackfillPlan` | Existing eligibility checks run first; otherwise the new proof supplies `promotion_refusal` for both supported result formats (`:375–394`). |
| `prepare_backfill` | `src/engine/backfill.rs:505` | Immutable request and optional stop phase → release/refusal | Keeps durable preparation/readback, locks the final input set, captures a fresh view, checks promotion and the new proof, then selects (`:833–890`). |
| `select_prepared` | `src/engine.rs:1657` | Request, admitted operation, retained release → release/refusal | Returns an existing result on retry. Otherwise it checks current authority and applies the new proof only for schema, under the final dataset/input guards (`:1679–1708`). |
| `read_head` | `src/engine/history.rs:283` | Snapshot rows, observed revision → optional selected release | Requires consistent selected/release identity, verified result, and a valid selection revision. |
| `read_link` / `check_link_receipt` | `src/engine/history.rs:310,334` | Expected version and snapshot → exact succession receipt | Requires exact selected identity and a co-committed, verified result with matching boundary and eligibility. Rejects invalid predecessor identity. |
| `check_selected_link` | `src/engine/history.rs:369` | Selected head and first chain link → success/refusal | Binds the selected head to its actual succession commit. |
| `select` / `select_with_progress` | `src/engine/history.rs:144,162` | Same view, authority, qualified release → committed selection | Rechecks abandonment/corrections and atomically writes authority, result, succession, and applicable change/progress records. Schema selection changes the active definition in that assembly. |
| `fingerprint` | `src/engine.rs:2044` | Typed request → material identity | The new scan reuses the same request-kind-specific fingerprint used for admission; explicit dataset/operation comparisons cover the identity fields outside that fingerprint. |
| `Lifecycle::rows` / `SnapshotRows::prefix` | `src/catalog.rs:208,63` | Captured view/prefix → retained snapshot/borrowed iterator | Holds one immutable metadata state and its reservation; iterates matching records without cloning the map. |
| `Lifecycle::commit` | `src/catalog.rs:293` | Captured view and records → next view | Assigns one revision and uses `GuardedCatalog` with the expected table before native append (`:311–346,402–408`). |
| `original_admission` | `tests/admission_formats.rs:54` | Fixture workspace, operation ID, schema flag → reconstructed old admission | Removes newer format fields and adjusts old schema coverage while keeping operation/request in one commit. Newly shared by the old-backfill fixture. |
| Integration fixture helpers | `tests/cutover_admissions.rs:19,33,55,81,94,112,118,184` | Source rows / retained releases / saved records → requests and assertions | Create real stopped admissions, retain metadata and file timestamps, and compare exact combined values including duplicates, nulls, empty text, and Unicode. |
| Unit fixture helpers | `src/engine/history/admissions/tests.rs:7,11,31,58,83,93,120` | Small metadata records → native lifecycle views | Construct source admission, selection, and abandonment independently; absent source paths ensure the reader cannot rely on payload availability. |

## Data flow and invariants

| State | Creation and mutation | Use and lifetime | Required property |
| --- | --- | --- | --- |
| Captured `rows` | Obtained once at `admissions.rs:59` | Used across both loops; the `Arc<SnapshotRows>` and its reservation survive yields | The decision cannot mix cache generations when another dataset replaces the shared cache (`catalog.rs:208–220,278–286`). |
| `selected` | Filled from the current head's predecessor chain (`admissions.rs:67–105`) | Entries are removed when matching admitted operations are checked (`:147`); nonempty remainder refuses (`:184`) | An orphan result or selected history without an admission cannot qualify a source request. |
| Source `boundaries` | Source-only positive numbers inserted at `:135–143` | Final cardinality must equal the authority counter (`:184`) | Together with the upper bound at `:130`, uniqueness and cardinality prove complete admission numbers through the counter. A high selected number alone does not. |
| `pending` | Appended only without a selected receipt or qualified abandonment (`:163–179`) | Returned to the private wrapper, which immediately checks it | Prepared, failed, stale, or readback-only work remains unresolved. Composition-only operations do not create source backlog. |
| Selected result binding | Checked by shared link receipt logic and again against the admission (`:147–159`) | Metadata-only qualification | Selection must follow admission and preserve its boundary, eligibility, result format, and exact request. A simultaneous abandonment refuses. |
| Abandonment binding | Decoded only for an unselected source (`:164–175`) | Resolves precisely its admission | Dataset, operation, digest, original admission revision, later terminal revision, and nonempty reason must agree. |
| `selecting` | Supplied only by the already checked schema caller (`engine.rs:1704–1706`) | Used by the private wrapper; selection follows on the same locked view | Only that schema's own admission may be resolved by the impending commit. Other pending requests still refuse. |
| Completed work | Retained before the new final proof (`backfill.rs:833–862`; `engine.rs:1628–1653`) | Remains durable after a proof refusal | The new reader and wrapper make no deletion, retirement, or recomputation decision. |

The accepted chain remains valid after logical result/recovery retirement. The reader uses retained inline selection receipts rather than the availability-checking read API. The real retirement integration fixture exercises this distinction (`tests/cutover_admissions.rs:312–357`); the unit fixture also uses absent payload paths (`src/engine/history/admissions/tests.rs:195–243`).

The new proof adds no data-payload I/O. `Lifecycle::rows` can still read lifecycle Parquet metadata on a cold cache (`src/catalog.rs:233–258`). Existing backfill planning/preparation and release checks retain their own I/O costs; this review does not characterize those whole paths as payload-free.

## Test behavior and edge cases

The following trace assessment comes from inspected assertions. The reviewer also read the implementation owner's passing [metadata-unit log](cutover-admissions-unit-2-2026-10-10.log), [cutover integration log](cutover-admissions-integration-2026-10-10.log), and [format compatibility log](cutover-admissions-formats-2026-10-10.log). Those logs confirm the focused rows below; they do not establish the broader repository gate or performance qualification. The ordinary integration suite includes `cutover_admissions` (`tests/integration.rs:23`).

| Test | Location | Predicted behavior and trace |
| --- | --- | --- |
| `later_full_selection_does_not_hide_earlier_preparation_or_stale_request` | `src/engine/history/admissions/tests.rs:151` | Pass: the earlier request remains pending despite preparation/readback and later selection; exact abandonment clears it. |
| `selected_chain_remains_covered_with_retired_unavailable_payloads` | Same file, `:195` | Pass: retained succession/results suffice despite absent payloads and a retirement marker. |
| `rejects_orphan_result_and_forged_selected_receipt` | Same file, `:247` | Pass: a result outside the head's chain refuses; selected format disagreement refuses. |
| `rejects_missing_and_duplicate_source_boundaries` | Same file, `:279` | Pass: positive unique boundary insertion plus final cardinality detects both omissions and duplicates. |
| `rejects_abandonment_not_bound_to_original_admission` | Same file, `:302` | Pass: the abandonment's recorded admission revision differs. |
| `legacy_schema_counts_as_source_while_current_schema_does_not` | Same file, `:318` | Pass: the pure helper reports format-10 schema as pending and excludes format-11 composition. No self exemption leaks into this helper. |
| `refuses_metadata_before_decoding_an_oversized_request` | Same file, `:355` | Pass: the request's cumulative charge refuses before typed decoding. This checks application accounting, not measured heap use. |
| `backfill_refuses_hidden_admission_and_reuses_completed_work_after_abandonment` | `tests/cutover_admissions.rs:173` | Pass expected: admission-, processing-, and readback-stopped source requests remain blockers; cold retry retains work; explicit abandonment permits exact selected completion. |
| `format_10_backfill_refuses_hidden_admission_and_reuses_exact_completion` | Same file, `:180` | Pass expected after F2: old format now runs the same proof while retaining its original completion format and the stricter historical promotion rule. |
| `schema_refuses_hidden_admission_and_keeps_old_writer_until_exact_retry` | Same file, `:256` | Pass expected: the new proof executes before definition selection, so refusal preserves the active writer and readback; explicit abandonment permits the exact retry. |
| `retired_selected_history_does_not_become_a_source_backlog` | Same file, `:312` | Pass expected: real result and recovery retirement do not remove the inline accepted-order evidence used by the new proof. |
| `original_admissions_resume_and_mixed_history_stays_readable` | `tests/admission_formats.rs:83` | Pass expected after F1: old schema excludes only itself, still refuses an unrelated hidden request, then resumes its exact format-10 readback after abandonment. |
| `original_backfill_does_not_silently_adopt_new_gap_rules` | Same file, `:202` | Existing behavior retained: the old stricter boundary check refuses before the new proof; a new-format operation can use the explicitly abandoned gap. |
| Shared succession corruption tests | `tests/succession.rs:326,446` | Existing shared helper assertions cover missing/malformed/rebound/cyclic links and inconsistent selected heads. The new proof calls those maintained helpers; these tests are not substitutes for the new whole-admission tests above. |

The reviewer did not execute the checks. The retained focused logs establish compilation and passing assertions for this increment; the full repository gate remained in progress. Process-memory peaks and cutover latency were not measured. The bounded full-history approach is suitable for this prerequisite; larger-history cost remains a qualification matter, as already required by the resource design review.

## Conclusion

VERDICT: APPROVE

Justification:

- The corrected patch achieves the scoped intent by proving the fate of every source admission, including hidden earlier requests, before field/schema selection.
- Both supported result formats now receive the guard, and old schema recovery resolves only its own admission in the same selection.
- The proof reuses retained metadata and accepted-order checks, preserves completed work, and refuses incomplete/corrupt or over-budget history.
- Coverage of changed paths: ADEQUATE by static inspection and the implementation owner's retained focused test results; the required full gate remains outstanding at report finalization.
- Confidence: HIGH in the traced control flow and identity bindings. Focused runtime results support the review; resource measurements remain unqualified.

## Follow-up: existing refresh/backfill race fixture

**APPROVE the corrected test-only follow-up.** Static review covered `refresh_and_backfill_races_preserve_captured_source_and_selected_groups` in `tests/refresh.rs:475`, after the implementation owner's full gate exposed its previous implicit-resolution assumption. No execution was performed for this follow-up; its focused rerun and remaining gate results belong to the implementation owner.

The fixture admits `later` against the old base, selects `first`, and verifies that retrying `later` refuses with `stale_base` (`tests/refresh.rs:503–517`). It then selects a separate `catchup` operation carrying the same replacement values against the new base (`:525–526`). That makes the data current but does not select or abandon the identity of `later`. Expecting field cutover to refuse until the fixture explicitly abandons `later` is therefore the intended safety behavior. The unchanged assertions still establish routine refresh selection despite later admission, exact selected values, and final group composition/release verification (`:497–524`; the end of the same test).

An initial test-edit draft placed the original `files_completed > 0` assertion after a refused `backfill(new_group)` call. That would weaken the reuse evidence: this call can prepare and retain the missing files before the final selection guard refuses (`src/engine/backfill.rs:625–862,885`). The corrected fixture captures `initial_plan` and asserts both `pending_source` and existing completed files **before** executing the new backfill (`tests/refresh.rs:528–530`). Planning counts pre-existing file completions without computing them (`src/engine/backfill.rs:412–436`). It then checks actual refusal, preserves `latest == caught_up`, explicitly abandons only `later`, and retains the original final success/verification checks. The assertion-order concern is resolved; the test preserves its original reuse property while adding the required admission proof.

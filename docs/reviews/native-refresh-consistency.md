# Native related-input refresh consistency review

Static review of the uncommitted checkpoint against `b3c8e1f`, dated 2026-10-08. The reviewer applied the semi-formal code-review and Microsoft Rust guidance, read implementation, callers, tests and retained owner logs, and executed no tests. Only this report is reviewer-owned. Initial finding locations below refer to the first reviewed diff; closure locations refer to the corrected implementation.

## Findings first

### F1 — Composite null-safe join predicates need component parentheses — CLOSED

- **Severity:** BLOCKER; correctness.
- **Location:** `src/engine/refresh.rs:465–477`.
- **Evidence chain:** `join_frame` joins multiple `IS NOT DISTINCT FROM` expressions with bare `AND`. The pinned parser has already required parentheses for the equivalent source-update expression; `src/engine/write.rs:418–424` contains that correction. Processing (`refresh.rs:86`) and independent value comparison (`:522`) both call this builder, so agreement between those paths does not qualify intended composite-key semantics.
- **Failure sequence:** A two-component, null-safe join uses partially null keys. The generated expression groups differently from independent component equality, producing a planning/type error or incorrect matching. Replaying the same builder cannot independently expose the latter case.
- **Correction:** Parenthesize each equality component. Add a fixture with an independent Rust expected-result calculation covering nonmatches and partial nulls on both components.
- **Closure trace:** `refresh.rs:488` now parenthesizes each component. `tests/related_refresh.rs:200–234,299–322` derives expected pairs directly in Rust across join/null modes, including partial nulls, duplicate keys and empty sources. Assertions and successful final owner-run gate evidence have been inspected.

### F2 — Internal multiset aliases collide with valid visible fields — CLOSED

- **Severity:** WARNING; correctness.
- **Location:** `src/engine/refresh.rs:265–281,579–587`.
- **Evidence chain:** Validation reserves `__docspec_` names, but permits visible fields named `__weight` and `__total`. `equal_multisets` introduces those names alongside the requested columns and then refers to them without a distinct qualification. Such a result can fail verification through ambiguous/duplicate columns despite being a valid typed join.
- **Correction:** Use internal aliases in the already-reserved `__docspec_` namespace. Exercise both formerly colliding names as legitimate visible output fields.
- **Closure trace:** `refresh.rs:642–643` now uses `__docspec_weight` and `__docspec_total`, which request validation reserves. `tests/related_refresh.rs:665–677` builds and independently verifies a result with both formerly colliding names and a quoted/spaced name.

### F3 — Offline based-Build validation does not mirror the related-parent refusal — CLOSED

- **Severity:** WARNING; correctness/verification consistency.
- **Location:** `src/engine/backfill/verification.rs:18–35`; runtime rule at `src/engine/refresh.rs:296–309`.
- **Evidence chain:** Runtime refuses Build and Update over a selected related output. The Update verifier rejects `base.related.is_some()`, but the based-Build verifier checks only the child's related field and the parent's ordinary identity, boundary, eligibility and group membership.
- **Failure sequence:** A separately rehashed manifest declares a based Build, clears the child related binding, retains a genuine related parent and satisfies the ordinary source/group checks. Independent structural verification can accept a transition that the engine refuses to perform.
- **Correction:** Reject a related parent in the based-Build branch too. Use a rehashed fixture that reaches the semantic check rather than failing only on the old digest.
- **Closure trace:** `backfill/verification.rs:22` now rejects a related Build parent. `tests/related_refresh.rs:692–737` checks both runtime raw-write refusals and a freshly hashed based-Build manifest that reaches the parent consistency check.

### F4 — Offline based-Refresh validation omits input-role continuity — CLOSED

- **Severity:** WARNING; correctness/verification consistency.
- **Location:** `src/engine/backfill/verification.rs:126–140`; runtime rule at `src/engine/refresh.rs:171–180`.
- **Evidence chain:** Selection refuses replacing input datasets or swapping roles relative to an existing related output. Offline parent checks do not compare those roles. The full join verifies the child's newly declared recipe, so a correctly recomputed child with different role datasets can pass despite the forbidden transition.
- **Correction:** Mirror the inexpensive role-dataset continuity check against the exact related parent. This does not require offline accepted-history replay.
- **Closure trace:** `backfill/verification.rs:138–141` now compares each role's dataset with the exact related parent. `tests/related_refresh.rs:738–759` first creates valid swapped-role tables, then forges only their claimed output succession and rehashes the manifest. The structural parent check supplies the refusal.

### F5 — Enlarged futures overflow an existing ordinary worker stack — CLOSED

- **Severity:** BLOCKER; runtime regression/qualification.
- **Evidence:** The initial related-refresh run aborted on stack overflow (`docs/reviews/native-refresh-first-failed.log`). Later failures in existing rebuild and multithreaded deterministic catalog-race fixtures are retained in `native-refresh-regressions-failed.log` and `native-refresh-race-stack-failed.log`.
- **Correction:** Production heap boundaries cover the shared preparation future and new refresh/verification futures. `DatasetPin.related` and backfill's captured related binding use `Option<Box<RelatedResult>>` (`src/model.rs:83`; `src/engine/backfill.rs:24`). The retained layout probe (`examples/layout_probe.rs`, `docs/reviews/native-refresh-layout-before.log`, `native-refresh-layout-after.log`) measures `DatasetPin` at 1848→1160 bytes and `Authority` at 3872→2496 bytes on this build. This reduces inline state size without changing the serialized record shape. These are native layout measurements, not whole-process memory or throughput results.
- **Disposition:** The final `native-refresh-check.log` shows the complete gate passing, including both previously failing rebuild paths and the related-refresh suite under ordinary stack settings. The reviewer inspected that evidence and the separate successful no-default-feature check. No test-only stack increase supplies closure.

## Patch summary and requirement trace

The patch adds a typed two-input equality join with explicit inner/left/full behavior, null matching, matching-partner cardinality and named source fields. It persists exact input versions and generation seals, writes physical contributor tuples into the source result and a separate evidence table, and selects only after independent value/evidence checks. Format and processor identity advance together to v6.

This addresses the full-reconstruction path and common promotion/recovery requirements in `docs/requirements/05-dependent-refresh.md:13–39`. It does not itself implement old/new-key affected-result discovery or aggregate contributor refresh. The existing source tables supply fields; computed input-group fields are not part of this join's input schema (`src/engine/refresh.rs:402–430`).

Exploration began with four hypotheses: exact-base and source-eligibility guards remain atomic; backfill cannot replace related coverage; evidence checks include visible values; and semantic input verification avoids an ancestor-history scan. The corrected implementation and inspected fixtures support those hypotheses. SQL expression construction required separate inspection because processing and verification share the builder.

## Function trace

| Function / type | Location | Inputs → outputs | Verified behavior |
| --- | --- | --- | --- |
| Join types, `RefreshSpec`, `RelatedResult` | `src/model.rs:144–251` | Typed input roles/rules → request and retained binding | Exact VersionRefs remain separate left/right roles; coverage deduplicates datasets; null/cardinality/policy choices are explicit. |
| `OperationRequest` accessors | `src/model.rs:427–499` | Request variant → shared dataset/base/identity | Refresh enters shared phase/select machinery and declares no raw-file inputs. |
| `plan_refresh`, `refresh`, `refresh_until` | `src/engine/refresh.rs:19,28,38` | RefreshSpec → plan/result | Thin shared-service entry points; fault hook is feature-gated. |
| `validate`, `check_rebuild` | `src/engine/refresh.rs:256,304` | Request/current selection → refusal or admission | Refuses missing keys/fields, output-as-input, different versions of one self-joined dataset, reserved names, an unbased replacement, and raw writes to related results. |
| `plan_request`, `run_internal`, `prepare_and_select` | `src/engine.rs:155,519,551` | Stable request → retained operation and phases | Request conflict and completed-result reconciliation precede new authority mutation. Admission advances the output boundary. Adds evidence to the refresh plan. |
| `fingerprint`, refresh `phase_key` | `src/engine.rs:1178`; `src/engine/refresh.rs:330` | Retry/material inputs → identities | Retry identity binds base, roles, recipe, policy and declaration. Material phase keys bind exact roles, recipe/definition, processor and output eligibility while allowing composition/policy reassociation. |
| `admit_refresh` | `src/engine/refresh.rs:46` | Exact inputs → retained admission | Captures producer relationships, loads exact manifests, independently verifies input releases, and completes a phase without selecting output. |
| `process_refresh`, `join_frame` | `src/engine/refresh.rs:81,406` | Admitted native sources → typed DataFrame | Checks exact input vector, uses maintained occurrence readers, requires equal scalar key types, quotes identifiers, and adds the four immediate-input occurrence columns. |
| `write_frame` | `src/engine/write.rs:59` | Maintained DataFrame → native TablePin | Extracted existing source writer; preserves native schema conversion, admitted batches, append and exact pin creation. No new writer implementation. |
| `prepare_evidence` | `src/engine/refresh.rs:103` | Retained processing → independently retained tuple table | Reuses completed evidence first; otherwise selects tuple fields from the acknowledged processing table and commits its own phase. |
| Dataset/group construction and readback | `src/engine.rs:750–885` | Source/evidence/base recipes → exact release | Refresh retains no old raw source inputs; based refresh recomputes all inherited scalar recipes and binds actual request/base. Readback compares the exact candidate before reuse. |
| `select_refresh`, `guard_ancestors`, `guard_inputs` | `src/engine/refresh.rs:148,198,211` | Completed candidate + current catalogs → selected result and progress | Locks complete sorted semantic output/input/family set; rechecks output authority, previous role datasets, accepted succession, current producer eligibility, exact seals and immediate Latest heads. |
| `history::select`, `select_with_progress` | `src/engine/history.rs:128,148` | Guarded release/progress → one lifecycle commit | Backfill rechecks related input eligibility/relationships. Authority, result, succession and observed progress are committed together. |
| Backfill capture/promotion/selection | `src/engine/backfill.rs:24,201,359,475,506` | Captured source/target/related binding → compatible composition | Captures exact related binding, compares it during promotion, takes input/family locks before selection, and retains sibling-compatible merge/re-readback behavior. |
| `read_combined` | `src/engine/backfill.rs:87` | Verified release → consumer stream | Hides the four match fields only for related results; raw source reads retain them. Scalar fields remain joined by physical output occurrence. |
| `verify_groups` | `src/engine/backfill/verification.rs:6` | Dataset/parents/groups → structural qualification | Checks exact related inheritance for Backfill, raw Update refusal, Refresh request/coverage equality, new input semantics and inherited scalar recipes. Findings F3/F4 identify missing parent checks. |
| `verify_all`, `available_graph`, `walk` | `src/engine/verification.rs:64,72,79` | Root releases/shared Budget → dependency qualification or availability graph | Iterative postorder traversal of semantic inputs only; exact identity conflict/cycle detection; per-call memoization; aggregate metadata charging and cooperative yields. Composition parents keep shallow checks. |
| `verify_local_release`, `verify_related_with_inputs` | `src/engine.rs:1462`; `src/engine/refresh.rs:557` | Exact release plus verified dependencies → data/evidence checks | Verifies request identities, native inventories/rows and groups; compares expected full typed join rows and exact evidence tuple multiplicities, then applies per-occurrence cardinality. |
| `equal_multisets`, `has_rows` | `src/engine/refresh.rs:636,650` | Two native relations → difference/refusal | Maintained grouped weighted comparison preserves duplicate multiplicity and includes visible values. Reserved internal aliases close F2. |
| `load_inputs`, `read_release`, shared `read_manifest` | `src/engine/refresh.rs:361,383`; `src/engine/inputs.rs:288` | Exact ObjectRefs → bounded verified manifest bodies | Reads/hash-checks the same admitted bytes; verifies dataset/version/owner/format; caller retains the metadata reservation. |
| `Budget::create/context`, `Engine::context` | `src/budget.rs:63,98`; `src/engine.rs:133` | Limits/shared runtime → query contexts | Moves the existing maintained execution/spill pool into Budget so offline and engine queries share it within the operation. Resource review owns detailed accounting. |
| CLI and HTTP handlers | `src/main.rs:76,229`; `src/http.rs:34,163` | Typed bounded JSON → application methods | Plan/run and test-only paused refresh route to the same services. Latest conflicts map to HTTP 409. Shared evidence IPC reading uses exact releases. |
| `check_objects` and callers | `src/storage.rs:103,107,123` | Owned inventory/shared Budget → verification or availability | Shared worker loop retains inventory admission across cancellation; full verification hashes objects, availability checks exact path/type/size. New cancellation fixture exercises both callers. |

Behavior-critical existing callees read: `history::follows` (`src/engine/history.rs:30`), producer qualification and relationship checks (`src/engine/inputs.rs:148,193`), native occurrence schema/task/count handling (`src/storage/occurrences.rs:30,96,132`), the maintained DataFusion bridge (`src/query.rs:26,59`), and source/group expression validation. The existing catalog commit still owns the durable transaction; local mutexes hold participating source authorities stable during output selection.

## Data flow and invariants

1. **Exact source roles:** RefreshSpec creates the left/right identities; admission retains the deduplicated vector and seal bodies; processing and independent verification compare that vector with the request. Distinct roles may share only the same exact dataset version.
2. **No premature progress:** Processing, evidence, validation and readback are retained independently. Only the final output commit advances accepted output succession and persists the input-progress observation.
3. **Captured versus Latest:** Both require canonical equal/forward accepted coverage and current producer eligibility. Captured may trail a newer accepted head; Latest requires the target to equal that exact head. Progress stores the observed head/revision rather than implying acquisition completeness.
4. **Output authority:** Initial refresh requires absence; based refresh requires the selected base. Output definition/eligibility/epoch remain guarded. Completed exact operation retries retain historical reconciliation behavior.
5. **Composition:** Backfill copies the current sibling-inclusive composition but requires exact captured related coverage, source pin, boundary and target binding. Fresh related guards execute while the full lock set is held.
6. **Evidence semantics:** Match tuples refer to file/ordinal occurrences in fixed exact input pins. Comparing complete rows against the expected join checks visible values and null/unmatched membership, then a separate tuple comparison checks the retained evidence table. The shared builder requires independently authored fixture oracles.
7. **Offline boundary:** Dependency verification establishes retained bytes, typed values, lineage membership and producer-declared relationship consistency. It does not establish current eligibility, Latest status, selected acceptance or source acquisition completeness without the catalogs. Accepted-succession observations live beside selection, not inside the immutable release.
8. **Bounded reuse:** Material phases may be adopted across a new base/policy/declaration while exact retry identities remain distinct. Selection always rechecks the current policy. Graph memoization avoids repeated verification of a shared semantic dependency within one call; it does not walk all output history.

Final source additions strengthen the same invariants. `DependencyGraph::datasets` (`src/engine/verification.rs:24`) includes the output, every semantic input dataset and every retained family. `available_graph` (`:72`) verifies exact manifest identities and the dependency closure's object availability, and retains its aggregate reservation while selection acquires sorted locks. `guard_ancestors` (`src/engine/refresh.rs:198`) rechecks every ancestor input coverage under Captured semantics before the immediate operation policy. Both Refresh (`:148–192`) and related Backfill (`src/engine/backfill.rs:475–500`) use that same graph and lock set. An indirect producer invalidation therefore fences a new selection without changing historical offline-read semantics.

`Engine::compare` (`src/engine.rs:339–385`) now includes related coverage in `same_inputs` and omits only the four reserved tuple fields from visible source-value comparisons when both releases are related. Equal displayed values can therefore coexist with changed input coverage. `read_evidence` (`src/engine/read.rs:23,54`) verifies the full release before streaming its exact native contributor table; CLI and HTTP reuse that service and the existing native/IPC stream code. No new detached stream producer is introduced.

## Test behavior and edge cases

The following dedicated assertions and final successful owner-run evidence have been inspected. The reviewer executed no tests. Earlier default-stack failures remain retained as described in F5; the complete corrected gate passes without changing the ordinary test stack.

| Test | Location | Inspected behavior |
| --- | --- | --- |
| `all_join_kinds_preserve_duplicate_and_partial_null_occurrences` | `tests/related_refresh.rs:299` | Direct Rust matching checks all supported join/null modes, duplicate occurrence pairs, visible null/empty values, evidence pair multiplicities, hidden combined columns, one empty input and both empty inputs. The oracle shares native occurrence decoding but not the SQL join builder. |
| `cardinality_guards_reject_fanout_and_self_joins_keep_exact_roles` | `:325` | Declared many-to-one/one-to-one fanout refuses without selection or native output files; a same-version self-join preserves distinct roles; output-as-input planning refuses. |
| `offline_verifier_reconstructs_values_contributors_and_exact_input_graph` | `:382` | Substitutes valid native source/evidence tables from another recipe and rewrites roles/field side under fresh manifest hashes; checks refusal, aggregate metadata capacity and successful catalog-hidden verification. |
| `captured_selection_records_lag_latest_refuses_and_coverage_cannot_regress` | `:445` | Advances a source after retained readback; Captured records exact newer observed head and lag while Latest refuses. Forward refresh retains equal visible values but different coverage; backward input coverage refuses. |
| `completed_phases_resume_and_original_selection_reconciles_after_invalidation` | `:500` | Uses the shared real child-process kill/resume harness at every acknowledged refresh handoff; asserts the exact remaining work-marker list, exact prior phase-record preservation, expected values, and original-result reconciliation after producer invalidation. |
| `output_base_and_producer_eligibility_fence_selection_without_losing_work` | `:557` | A competing initial result fences expected-absent work; producer invalidation after readback refuses promotion while retaining evidence and the old selected output. |
| `backfill_preserves_related_binding_and_noop_refresh_fences_old_cutover` | `:604` | Backfill retains the exact related binding. A same-input refresh reuses the source TablePin, advances boundary and preserves scalar groups; old pending backfill refuses. |
| `quoted_helper_names_and_unmatched_duplicate_keys_are_supported` | `:665` | Validates formerly colliding/internal-looking and quoted output names; unmatched duplicate keys remain permissible under one-to-one matching cardinality. |
| `offline_parent_rules_reject_related_conversion_and_role_rebinding` | `:692` | Runtime Build/Update conversion refuses; freshly hashed parent forgeries target the exact F3/F4 structural checks. |
| `cached_readback_checks_missing_transitive_objects_and_live_ancestor_eligibility` | `:776` | Builds a derived input, retains nested readback, hides an indirect source data file and proves no selection; restores bytes, invalidates that ancestor and expects stale eligibility. Historical immutable verification still succeeds. |
| `http_refresh_and_catalog_free_cli_verification_use_shared_services` | `:817` | HTTP plan includes evidence; HTTP run matches the Rust value/pair oracle; evidence IPC has the expected schema/row count; real CLI verify and evidence read work with the catalog hidden and do not recreate it. Shared Rust evidence values are checked by the ordinary fixture reader. |
| `coupled_refresh_refuses_mixed_generations_then_advances_exact_coverage` | `:917` | Creates/seals both producer generations, selects the first exact pair, refuses a mixed pair while preserving that result, then selects the complete second generation and checks exact values/pairs. This exercises the shared relationship rule through real refresh, rather than only metadata capture. |

The cardinality preflight in `refresh.rs:500–539` counts keys only after a maintained semi join. This preserves each candidate partner once and rejects matching fanout before writing the Cartesian output. Unmatched duplicate keys are excluded, matching the declared per-occurrence partner semantics. The independent post-write cardinality check remains in place.

Final execution evidence is retained in `docs/reviews/native-refresh-check.log` and `docs/reviews/native-refresh-no-default.log`. The reviewer inspected successful formatting/Clippy gate progression, all feature and existing regression suites through final doc tests, the actual coupled refresh case, real child-process recovery assertions, and the no-default-feature compile result. The owner confirmed the combined run exited 0. `scripts/check` establishes the gate commands: formatting check, locked Clippy over all targets/features with warnings denied, and locked all-feature tests.

The qualification remains local and fixture-based. Full reconstruction repeats join/verification work across required phases; changed VersionRefs currently require reconstruction even for identical source bytes. Deliberate many-to-many fanout has no output-size estimate. Scratch-overrun behavior of the pinned DataFusion dependency is explicitly retained as a resource limitation in `docs/STATUS.md:510–520`. These are documented limits, not measured performance or complete incremental-R5 claims.

## Conclusion

**VERDICT: APPROVE**

The corrected patch satisfies its declared native full-join, retained evidence, guarded selection and recovery scope. F1–F5 are closed by source changes, focused assertions and the inspected successful final gate. Coverage of changed paths is **ADEQUATE** for this checkpoint; confidence is **HIGH** in the static consistency review and retained local execution evidence. The operation-scoped Latest boundary and full-reconstruction limits are explicit (`README.md:229–293`; `docs/STATUS.md:479–539`). No full incremental R5, remote durability or performance qualification is inferred.

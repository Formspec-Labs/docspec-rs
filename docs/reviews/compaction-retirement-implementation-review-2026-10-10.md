# Compaction recovery retirement: implementation review

## Findings

No blocking correctness finding in the reviewed implementation. The new scope can end an obsolete or interrupted compaction's recovery promise while the existing inventory and removal service preserve independently retained data.

**Finding OBS-1 — qualification limits, narrowed by the final test addition.** Severity: OBSERVATION. Category: test-coverage. Location: `src/engine/retention/compaction_recovery/tests.rs:150`. A direct helper fixture now uses real native pins and an actual compaction to assert exact grants for foreign consumers: recovery includes the original payload and release manifest; selection includes replacement objects and the original logical metadata while excluding the obsolete payload and manifest. This addresses the earlier absence of a direct grant-inventory fixture. It does not bind or select a foreign consumer representation, and both logical and replacement objects in this fixture have the same owner. A complete workflow with different physical/logical owners remains outside its scope. Evidence chain: `src/engine/retention/compaction_recovery/tests.rs:157`, `:169`, `:173`, `:195`, `:204`; `src/engine/compaction.rs:16`, `:350`; `src/engine/retention/owners.rs:234`; `src/engine/retention/inventory.rs:670`. I inspected the assertions and trace without executing the new test. Preserve these limits in qualification claims.

This is a static review against `a3d695f`, including the new untracked compaction-retirement source and tests. I read the parent and repository instructions, applicable Rust guidance, the semi-formal review skill, `docs/PLAN.md`, `docs/STATUS.md`, and the maintenance requirements. I did not run builds, tests, benchmarks, or corpus work, and do not claim the author's execution results. The source changed during review: I reread the final protection helper and added unit/integration fixtures, then refreshed references after formatting. The final follow-up also reviewed the helper's `pub(super)` visibility and direct grant-inventory fixture. This assessment covers that final working-tree snapshot, not an immutable commit.

## Patch summary

The accepted need is explicit retirement followed by separately authorized exact-object removal, preserving selected versions, completed work, active readers, and immutable history (`docs/requirements/06-maintenance-retention.md:17`, `:25`, `:27`, `:29`, `:31`; `docs/PLAN.md:44`, `:69`, `:96`).

The patch adds `RetirementScope::CompactionRecovery` and thin CLI/HTTP entry points. Planning captures the exact request, preparation, verification, result, and attempt-protection generations for one operation. Execution runs through the existing exclusive maintenance path and atomically records retirement markers plus one receipt. Future compaction retries refuse that operation identity. Selected representation records remain active, but their retention contribution is reduced to the replacement and logical schema. Broad predecessor dependencies remain on recovery records until explicit retirement.

The concrete improvement is that an intermediate layout in a compaction chain can become eligible for the existing removal service after every recovery or independent dependency on it ends. No second deletion mechanism, lifecycle database, scheduler, or reachability index is added. Physical storage savings depend on actual removal and independent blockers; this review measures neither speed nor bytes reclaimed.

## Exploration record

| Hypothesis before tracing | Evidence and result |
| --- | --- |
| H1: a caller could omit a late completion or include another attempt in a plan. | Refuted for normal execution: fresh `collect` output must equal the captured list before markers are created (`retirement.rs:583`). Saved receipts also verify fixed-record membership and each captured attempt (`compaction_recovery.rs:165`). |
| H2: narrowing representation retention might release selected data. | Refuted in the typed trace: `Inventory::table` retains the replacement's complete declared object set and logical metadata (`inventory.rs:886`); only the predecessor table is omitted for `representation/` (`inventory.rs:1227`). |
| H3: an admitted worker could publish after retirement. | Refuted for the owned local host: compaction holds shared admission, retirement takes exclusive admission, and the worker retains its guard through cancellation (`worker.rs:111`, `:136`, `:67`). |
| H4: retiring recovery could erase evidence needed to qualify deletion. | Refuted: retirement adds markers without rewriting source records (`retirement.rs:617`); owner creation lookup still reads `compaction-prepared/` (`owners.rs:997`). |
| H5: ordinary operation retirement could capture a compaction with the same operation ID. | Refuted: collection excludes the exact `/compaction` purpose and saved-attempt validation rejects it (`recovery.rs:181`, `:221`). |
| H6: ordinary reads would repeatedly validate predecessor payloads. | Refuted: `representation_pin` resolves retained metadata and returns the replacement; no retirement traversal or payload-equivalence scan was added there (`compaction.rs:72`). |

## Function trace

Paths in this table are under `src/` unless prefixed with `tests/`.

| Function / method | File:line | Inputs | Output and verified behavior |
| --- | --- | --- | --- |
| `keys`, `recovery_key`, `scope_key` | `engine/retention/compaction_recovery.rs:18`, `:22`, `:27` | Operation ID or record key | Identifies the compaction record families and possible attempt keys. Specific attempt ownership is checked separately. |
| `fixed` | `engine/retention/compaction_recovery.rs:35` | Immutable rows, dataset view, operation, boundary | Requires admission identity, an unbroken completion chain, expected verification flags, matching source/request, and captured native generations. Allows identical concurrent verification acknowledgments to arrive after result selection. |
| `check_attempt` | `engine/retention/compaction_recovery.rs:91` | Attempt record and generation | Requires exact key, dataset, operation purpose, owned directory, revision interval, and an active attempt record. |
| `collect` | `engine/retention/compaction_recovery.rs:117` | Captured rows and operation | Returns a sorted exact-generation list. Scans attempt metadata once and charges list/decoder storage. |
| `validate_saved` | `engine/retention/compaction_recovery.rs:165` | Saved receipt and current rows | Checks fixed membership and only the attempts named by the receipt; it does not rescan all attempts per receipt. |
| `capacity` | `engine/retention/compaction_recovery.rs:205` | None | Returns the existing `metadata_capacity` refusal class. |
| `plan_compaction_retirement` | `engine/retention/compaction_recovery.rs:221` | Dataset, retirement ID, compaction ID, reason | Admits request memory, locks the dataset, checks retired admission, and returns the shared `RetentionPlan`. |
| `retire_compaction` | `engine/retention/compaction_recovery.rs:280` | Plan | Rejects another scope and delegates to shared retirement. |
| `retire_retained`, `retire_retained_admitted` | `engine/retention/retirement.rs:506`, `:518` | Plan | Takes exclusive maintenance admission; validates native boundary, fresh exact scope, and generations; commits markers/receipt; reconciles an uncertain commit by exact receipt identity. |
| `scope_key`, `retained_key` | `engine/retention/retirement.rs:44`, `:68` | Scope/key | Integrates the new record families without making selected representation keys retireable. |
| `record_retired`, `validate_markers`, `retired_records` | `engine/retention/retirement.rs:96`, `:157`, `:213` | Rows and receipt/record identity | Validates marker/receipt membership; full inventory validation additionally invokes the new scope validator and rejects orphan or duplicate markers. |
| `protect_compaction_table` | `engine/compaction.rs:16` | Dataset, table pin, optional manifest | Charges the temporary clone; protects table objects, logical metadata, and optional recovery manifest through the maintained owner-grant service. |
| `compact`, `compact_admitted` | `engine/compaction.rs:44`, `:107` | Compaction request | Holds shared worker admission. Checks retired request before completed retry. Recovery uses broad capture protection; selection uses replacement-only protection. |
| `representation_pin` | `engine/compaction.rs:72` | Logical pin and group | Resolves selected equivalent layout by retained metadata without rereading predecessor payloads. |
| `check_result` | `engine/compaction.rs:652` | Capture/result | Checks request, source, logical metadata, and complete row-count identity. Used by both compaction and retirement. |
| `Inventory::record`, `Inventory::table` | `engine/retention/inventory.rs:1054`, `:886` | Typed retained record/table | Recovery roots retain both sides; selected layout roots retain replacement objects and logical metadata. |
| `collect_inventory` | `engine/retention/inventory.rs:175` | Catalog roots and candidates | Validates retirements before omitting retired roots. Independent records, owner grants, and current representation roots remain active. |
| Ordinary recovery `check_attempt`, `collect` | `engine/retention/recovery.rs:162`, `:193` | Ordinary request/attempts | Prevents same-ID compaction attempt retirement in both new plans and saved receipts. |
| `completion_records`, `ReferenceProtections::binding` | `engine.rs:111`, `engine/retention/owners.rs:149` | Completion and protection scope | Commits the selected record and its digest-bound owner references together. |
| `protect_objects`, `validate_binding` | `engine/retention/owners.rs:559`, `:234` | Exact objects / bound retained claim | Deduplicates identities, acquires foreign owner grants, and uses the same typed root walker to validate required foreign objects. |
| `plan_removal`, `remove_admitted` | `engine/retention/removal.rs:160`, `:326` | Exact candidate objects / saved removal plan | Reuses retained creation evidence; executes only old and fresh eligibility intersection, then reserves unavailable identities before physical effects. |
| HTTP planning/execution and error mapping | `http.rs:430`, `:446`, `:458` | Existing typed request / plan | Calls the same engine methods; missing compactions map to 404 and retired recovery to 409. |
| CLI dispatch | `main.rs:606`, `:621` | IDs/reason or JSON plan | Calls the same planner/executor. No transport-specific retirement logic. |

Test-only visibility changes expose existing fixtures and helpers rather than introducing another fixture implementation (`src/engine/retention/recovery/tests.rs:27`; `tests/compaction.rs:15`, `:26`, `:43`). New integration suites are registered in `tests/integration.rs:19`.

## Data flow and invariants

`CompactionSpec` enters the existing compaction service. Admission creates `Capture`; preparation and verification produce the same `CompactionResult` apart from the verification flag. Planning reads those retained rows and creates only `(key, revision)` references. The plan escapes to CLI/HTTP or the caller. Retirement re-collects the current set under exclusive admission, rejects changed membership/generations, and stores immutable evidence of that decision (`compaction_recovery.rs:35`, `:117`, `:221`; `retirement.rs:583`, `:617`).

The selected `representation/` row is never in the new retirement scope. Its replacement table retains native metadata, logical metadata, and all declared physical objects. Historical result pins and captured update/phase pins still enter the same inventory independently. Thus ending one recovery promise cannot cancel another promise that reaches the same object (`compaction_recovery.rs:11`, `:27`; `inventory.rs:886`, `:1182`, `:1230`).

The broad recovery protection and narrow selected protection are created through one helper and bound to different retained keys. Owner-grant retirement checks all active consumer bindings; it cannot release a token still required by the selected layout (`compaction.rs:16`, `:196`, `:350`, `:390`; `grants.rs:121`, `:140`, `:149`). Local retirement requires the owned catalog, whose process lock and shared/exclusive admission prevent another qualified local writer from publishing through retirement (`retirement.rs:656`; `budget.rs:183`; `worker.rs:130`, `:153`).

Malformed stored retirement data fails before the inventory drops roots: receipt/key/scope/native identity, per-record markers, complete fixed-chain membership, and attempt ownership all participate in validation. Missing or extra markers cannot silently shrink protection (`retirement.rs:126`, `:157`, `:213`; `compaction_recovery.rs:165`). Omitting an unrelated attempt from a forged receipt leaves that attempt active; it cannot authorize broader removal.

Retirement neither rewrites nor deletes the original compaction rows. Creation evidence therefore remains usable after source payload removal, and an exact retirement retry does not reopen old data. Physical removal still qualifies the candidate against creation records and records unavailability before effects (`retirement.rs:617`, `:638`; `owners.rs:997`; `removal.rs:234`, `:441`).

## Test behavior and edge cases

These are predictions from the assertions and implementation, not execution results.

| Test | Location | Static expectation and evidence |
| --- | --- | --- |
| `retired_chain_reclaims_intermediate_data_and_metadata_without_changing_selected_layout` | `tests/compaction_retirement.rs:83` | Expected to pass: A remains protected by B recovery; after both retire, A data/native metadata become removable while B and canonical objects remain protected. The test removes files/registration, reopens, checks the identical selected row, schema/values, and occurrences. |
| `captured_update_keeps_intermediate_layout_until_both_operation_and_phase_recovery_end` | `tests/compaction_retirement.rs:207` | Expected to pass: captured update dependencies protect A independently; abandonment and ordinary recovery retirement leave a completed phase blocker; explicit phase retirement releases it. |
| `interrupted_bundles_retire_exactly_after_reopen_and_later_completion_invalidates_preview` | `tests/compaction_retirement.rs:285` | Expected to pass: rewrite/verification interruptions preserve records and bytes; explicit retirement prevents retry after reopen. A later completion changes the exact list, producing `stale_retirement`. |
| `identical_operation_ids_keep_normal_and_compaction_attempt_retirement_separate` | `tests/compaction_retirement.rs:387` | Expected to pass in either order: exact attempt purpose separates the two scopes; untouched markers remain absent. |
| `concurrent_identical_compactions_leave_one_exact_retirable_recovery_bundle` | `tests/compaction_retirement.rs:457` | Expected to pass: workers share one winning preparation; later identical verification acknowledgments need not sort before selection. The fixed validator checks contents and exact generations rather than a false total order. |
| `lost_retirement_reply_and_concurrent_retry_keep_one_decision_and_current_layout` | `src/engine/retention/compaction_recovery/tests.rs:35` | Expected to pass: exclusive maintenance serializes retirements; uncertain commit reconciliation returns the saved identical receipt; changed reason conflicts; selection remains unchanged. |
| `forged_compaction_receipt_cannot_end_another_attempt_or_omit_completed_work` | `src/engine/retention/compaction_recovery/tests.rs:84` | Expected to pass: execution rejects a changed list; direct forged stored receipts fail scope-specific inventory validation before cleanup. |
| `compaction_helper_grants_logical_metadata_without_obsolete_payload_in_selection_scope` | `src/engine/retention/compaction_recovery/tests.rs:150` | Expected to pass: actual compaction supplies a replacement whose object inventory excludes the original logical metadata and input files. Direct helper calls acquire owner grants for separate foreign consumers. Exact grant maps assert broad recovery protection and replacement-plus-logical-metadata selection protection; the selection map excludes old input payload and the release manifest. This tests the helper, not a complete consumer selection/retirement/removal workflow or different physical/logical owners. No execution result is claimed. |
| `cli_http_compaction_retirement_preserves_reads_and_reconciles_exact_retry` | `tests/compaction_retirement_transport.rs:56` | Expected to pass: CLI and HTTP share planning/execution; planning preserves retry; saved execution retry returns the same receipt after reopen; retired compaction returns HTTP 409 and selected values remain readable. |
| `canceled_caller_keeps_blocking_work_gate_and_host_until_real_exit` | `src/engine/worker/tests.rs:35` | Existing shared-service fixture: canceled callers leave the worker's guard and host lock alive until work exits. |
| `stream_retains_maintenance_admission_and_host_until_eof_or_drop` | `src/engine/worker/tests.rs:181` | Existing shared-service fixture: an admitted stream blocks exclusive maintenance through its final batch; EOF/drop releases custody. |
| `active_engine_reader_delays_physical_removal_until_eof` | `src/engine/retention/removal/native_tests.rs:171` | Existing removal fixture checks no physical effect while an engine reader remains active. |

The new fixtures cover the changed local behavior, saved-decision failure paths, and exact helper grant inventories for foreign consumers. They do not establish a complete mixed-owner physical/logical workflow, remote storage safety, large-history scaling, process RSS, sustained storage amplification, or full R1–R7 completion. This review does not substitute ordinary source snapshots for tests at every native deletion fault boundary; those remain the existing removal service's responsibility.

## Resource use and maintained-component reuse

Planning scans the current dataset's `protection/` metadata once and holds a bounded generation list. Saved receipt validation reads only that receipt's fixed chain and named attempts, avoiding a complete attempt scan for each retirement. Decoder reservations precede allocation, and the table-protection helper charges its temporary object copy (`compaction_recovery.rs:43`, `:124`, `:132`, `:165`; `compaction.rs:16`; `catalog.rs:78`).

The new retry check shares the lifecycle's immutable snapshot cache with existing lookups; it does not add a payload scan (`catalog.rs:186`, `:208`; `compaction.rs:129`). Retired receipts still retain metadata, and lifecycle cold replay still scales with retained history (`catalog.rs:233`). Full compaction results also retain file-reference inventories. This change enables physical reclamation; it does not solve all metadata-growth costs.

The patch reuses the existing plan/receipt format, native lifecycle commit, exact-generation markers, typed root walker, owner-grant service, workspace admission, creation proofs, and removal service. The helper unifies admission/selection table-object protection while expressing their different manifest needs. I found no new parallel mechanism or abstraction without a concrete caller.

## Conclusion

VERDICT: APPROVE

Justification:

- The patch achieves the scoped implementation intent: explicitly end exact compaction recovery, retain selected/independent dependencies, and make unneeded intermediate objects eligible for existing guarded removal.
- Coverage of changed local paths: ADEQUATE by static inspection, including the final direct grant-inventory fixture, with the complete mixed-owner workflow and broader operational/scale qualification limits stated above.
- Confidence: HIGH in the traced local control flow; runtime results and performance remain outside this review.

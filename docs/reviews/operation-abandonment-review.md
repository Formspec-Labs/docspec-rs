# Logical operation abandonment review

## Findings and disposition

No material implementation defect found. **APPROVE the narrowed logical-abandonment design and implementation**. This is an independent static review of the uncommitted change against `0b31c06`, using `AGENTS.md`, R6, the Rust guidance and the semi-formal code-review workflow.

The operation loses permission to record further preparation or select a result. Its completed phases, retained pins, reservations and files remain available. This does not qualify phase retirement, worker quiescence, physical cleanup, source-gap evidence, remote execution or complete R6 acceptance.

The author-owned focused run and full `scripts/check` both completed successfully. The reviewer inspected `operation-abandonment-focused.log` and the terminal `operation-abandonment-check.log`: formatting, strict all-target/all-feature Clippy, library and integration suites, and doc-tests passed, including the new related-refresh case. The author confirmed terminal exit 0 and no source changes since that gate started. The reviewer did not run Cargo or inspect large payloads.

## Patch and function trace

The patch stores one explicit decision at `abandoned/<operation>` and checks its presence at the existing durable boundaries. CLI and HTTP call the same application service.

| Function or type | Code reference | Inputs and output | Verified behavior |
| --- | --- | --- | --- |
| `Abandonment` | `src/model.rs:52` | Dataset, operation, request digest, original admission revision and reason | Records the identity and reason without changing the admitted operation. |
| `Engine::abandon` | `src/engine/retention.rs:19` | Dataset, operation and reason → retained decision | Requires a bounded, nonblank reason and an admitted operation; identical replay returns the existing record; conflicting replay or selected result refuses; only the abandonment record is committed. |
| `SnapshotRows::contains_key` | `src/catalog.rs:41` | Key → Boolean | Uses the existing snapshot map without decoding a potentially large result or reason. |
| `check_not_abandoned` | `src/engine/retention.rs:74` | Exact lifecycle view and requesting operation → permission/refusal | Presence of the decision refuses with `operation_abandoned`. |
| `plan_request` | `src/engine.rs:165` | Build, update, schema or refresh request → plan | Checks abandonment before resolving reusable phases. Public plan methods converge here. |
| `prepare_and_select` | `src/engine.rs:704` | Request and optional test stop → release | Checks abandonment before new/resumed work; group and readback acknowledgment use the completion guard; final source selection uses both the ordinary guard and common selection helper. |
| `check` / `check_completion` | `src/engine.rs:1046`, `src/engine.rs:1063` | View, captured operation and caller's ID → authority/permission | Adds the operation fence while preserving the prior distinction between selection eligibility and acknowledgment of completed work. |
| `phase`, `reserve_attempt`, `complete` | `src/engine.rs:1081`, `src/engine.rs:1118`, `src/engine.rs:1153` | Request, phase and captured operation → reusable work/reservation/completion | Fences lookup, protection reservation and acknowledgment with the requesting ID, independently of the original phase producer. Reservation commits before directory creation. |
| `plan_backfill` / `prepare_backfill` | `src/engine/backfill.rs:149`, `src/engine/backfill.rs:300` | Scalar-group request → plan/release | Refuses abandoned retries; preserves shared prepared groups and file completions; guards composition and readback acknowledgment. |
| `prepare_group` | `src/engine/backfill/preparation.rs:10` | Source pin, definition and requesting operation → prepared group | Checks before resolving preparation; fences failure records, prepared-table acknowledgment and validation acknowledgment. |
| `reserve_group` / `acknowledge_files` | `src/engine/backfill/preparation.rs:190`, `src/engine/backfill/preparation.rs:228` | Request and protection/file completions → durable records | Uses the common completion guard against the same view used by the commit. Existing reservations remain unretired. |
| `select` / `select_with_progress` | `src/engine/history.rs:128`, `src/engine/history.rs:146` | View, authority and release → atomic selection | The common helper fences `release.id` before committing authority, result, succession and optional input progress. This includes standalone backfill selection. |
| `select_refresh` | `src/engine/refresh.rs:177` | Refresh request and retained release → selected release | Checks the requesting operation and then uses the common selection helper. |
| HTTP handler and error mapping | `src/http.rs:42`, `src/http.rs:70`, `src/http.rs:163` | POST path IDs and JSON reason → decision/error | Calls `Engine::abandon`; missing operation maps to 404, conflicting/completed/abandoned operations to 409; request size remains bounded by the existing body limit. |
| CLI `Abandon` | `src/main.rs:64`, `src/main.rs:257` | Dataset, operation and reason → JSON decision | Calls the same service; output failure cannot undo the retained decision. |

The important unchanged callees were also read: `Lifecycle::rows` and `commit`, `GuardedCatalog`, source admission/processing, scalar-file preparation, `run_internal` and `backfill_internal` response reconciliation, schema admission guards, and backfill promotion.

## Data flow and invariants

1. **One durable outcome wins.** Abandonment and selection inspect the same immutable lifecycle view that their guarded commit uses. The dataset mutex serializes local decisions; `Lifecycle::commit` and `GuardedCatalog` reject a stale native head (`src/catalog.rs:210`, `src/catalog.rs:361`). If selection wins, abandonment sees `result/<operation>` and refuses. If abandonment wins, selection cannot commit from its old view, and a fresh decision sees the marker. Completed-result fast paths therefore remain valid; this patch does not create a reachable result-plus-abandonment state.
2. **A lost response is replayable.** The stored request digest and admission revision come from the immutable operation record. The reason remains exact text. Retry reads the same decision without another commit (`src/engine/retention.rs:42–69`). The existing selected-result reconciliation remains intact (`src/engine.rs:550`; `src/engine/backfill.rs:268`).
3. **Shared completed work survives.** The fence applies to the caller's operation ID, not `PhaseResult.operation`. Abandonment writes no phase tombstone, changes no reuse key, and retires no protection. A new operation may reuse completed admission, processing or scalar work under the existing material-dependency checks (`src/engine.rs:1081–1180`; `src/engine/backfill/preparation.rs:10–181`).
4. **Late native writes remain protected but cannot be acknowledged.** Already reserved workers can finish native I/O. Their subsequent file, table, phase, validation or readback lifecycle writes check the marker before committing. There is no cancellation barrier, filesystem deletion or native table expiration in this slice. Existing detached pins remain readable.
5. **Source admission stays monotonic.** Abandonment does not alter authority. Schema admission still requires the selected boundary to match the admitted boundary (`src/engine/schema.rs:80`); scalar promotion still requires both captured and selected source boundaries to match authority (`src/engine/backfill.rs:548–580`). A new explicitly selected source is required to reconcile an abandoned source gap. The implementation does not fabricate coverage for the withdrawn input. The architecture addendum recommends separating source coverage from the admission fence for a future schema-gap design; that proposal does not change this implementation or this review's scope.
6. **The added work is bounded metadata work.** Presence checks reuse the existing snapshot cache and perform a map lookup rather than scanning payloads or decoding large retained records. The decision adds one lifecycle record; reason, commit and total replay/cache limits remain enforced. Cold lifecycle loading and append-history growth remain existing costs. No throughput or storage-reclamation improvement has been measured or claimed.

## Test behavior and concrete edges

The following assertions were read. The focused log records success for the abandonment and rebuild-completion suites; the full gate records these and the related-refresh case passing.

| Test | Static trace and expected result |
| --- | --- |
| `abandonment_survives_reopen_and_preserves_shared_phases_and_readback` (`tests/abandonment.rs:31`) | Stops admission/processing/readback; compares all lifecycle records except the new marker; reopens; rejects old plan/run; reads a retained readback pin; removes original input before fresh-operation reuse; refuses abandonment after selection. Expected to pass through the guarded paths above. |
| `abandoned_backfill_preserves_file_reuse_and_refuses_every_retry` (`tests/abandonment.rs:134`) | Refuses old backfill plan/run, then selects a fresh operation with no pending files and unchanged retained scalar completion/prepared records. Expected to preserve shared work. |
| `abandonment_fences_updates_and_schema_without_rewinding_source_admission` (`tests/abandonment.rs:174`) | Checks unchanged authority and latest release, refusal of update/schema retries, the pending-source schema guard, and a new selected source resolving the gap. Expected to preserve the monotonic boundary. |
| `http_and_cli_share_the_durable_decision` (`tests/abandonment.rs:248`) | Exercises missing operation, blank reason, success/conflict status, then reopens through the CLI and compares the exact decision. Expected to use the same retained identity. |
| `abandoned_workers_cannot_acknowledge_late_native_source_or_group_commits` (`tests/rebuild_completion.rs:175`) | Pauses a real native source/group commit response, commits abandonment, releases the worker, and requires `operation_abandoned`, unchanged lifecycle state, unchanged selected release, existing native metadata and unretired protection. This directly covers delayed completion rather than only a retry after a test stop. |
| `lost_abandonment_response_reconciles_without_another_native_commit` (`tests/rebuild_completion.rs:255`) | Aborts after the real abandonment commit but before its response; replay must preserve the native sequence number and fence the old operation. |
| `selection_wins_over_abandonment_even_when_its_response_is_lost` (`tests/rebuild_completion.rs:311`) | Aborts after the real selection commit; abandonment must refuse, retry must return the selected pin, and no abandonment key may exist. |
| `abandoned_refresh_cannot_select_but_new_operation_reuses_its_result` (`tests/related_refresh.rs:161`) | Stops a related refresh at readback; old operation refuses, new operation reuses exact group and evidence pins plus coverage while retaining its own request identity, and producer selections remain unchanged. |

The generic stale-lifecycle-view test (`tests/lifecycle.rs:337`) and unchanged stale-source backfill assertions (`tests/backfill.rs:302–311`) support the catalog and boundary reasoning. The abandonment fixture directly tests the schema gap; the backfill gap is traced statically through the unchanged promotion guard. The new pause tests exercise a local maintained catalog, not simultaneous independent remote engines. No physical reclamation test is represented as passing, and no large-data performance qualification was performed.

## Conclusion

**VERDICT: APPROVE**, for the stated logical-fence scope. Static path coverage is adequate and no material finding remains. Confidence is high in the traced local lifecycle behavior; focused execution and the full repository gate passed. Broader retirement, cleanup and distributed-provider guarantees remain outside this certificate.

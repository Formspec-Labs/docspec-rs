# Export attempt retirement review

VERDICT: APPROVE for the narrow original-host cleanup scope. No actionable production safety finding. Static review of frozen patch SHA256 d37382055a23ca6a122803a2b049e3df29c5ce34caeeafd3e039e1ee925db6ca and `/tmp/docspec-export-attempt-retirement/new`; root subsequently made a borrowed-prefix comparison and equivalent clippy let-chain repair. Root reports the three focused tests passed. Reviewer ran no Cargo, tests or provider requests.

## Findings and limits

- OBSERVATION: original frozen host::validate moved `prefix` into `[prefix]`, causing a compile failure; root's borrowed comparison preserves the intended exact prefix check. Root also collapsed a nested conditional for clippy. Neither repair changes cleanup authority.
- OBSERVATION: custody is committed after synced empty-file creation and before ArrowWriter construction/payload writing (export.rs:580–603). Death before custody may leave an unrecorded empty file; this patch deliberately cannot adopt or sweep it. Legacy unrecorded children remain outside this cleanup scope.
- OBSERVATION: the losing-file fixture invokes the actual native writer sequentially to create the same shape as a losing completion (host/tests.rs:63–75). It proves exact losing-file cleanup and winner preservation, not a new concurrent publication qualification.
- NIT: export_recovery.rs:113 lists foreign host custody among retirement-plan errors, but retirement itself intentionally validates portable saved proof without checking the current host root; physical removal alone requires original host custody. Remove that error-list phrase when touching these docs. No safety dependency rests on it.

## Change and caller trace

| Function / path | Proposed source | Verified behavior |
| --- | --- | --- |
| materialize_export → retain_host_file | engine/export.rs:509,580; export/host.rs:52 | Keeps writer memory and open file across admitted custody commit; no payload before saved native identity. Closed child gets its own materialized proof at export.rs:686. |
| host::validate / remote::attempt_protection | export/host.rs:23; export/remote.rs | Exact operation revision, dataset, protection generation, canonical child and configured remote attempt proof; metadata-only validation survives removed files. |
| export_admitted / check_export_authority / retain_export_record | export.rs:223,418,479 | Completed immutable result returns before retired recovery refusal. Unfinished processing and new custody/closed records cannot resume after terminal retirement. |
| collect → plan_export_retirement → retire_export | retention/export_recovery.rs:17,114,133; retirement.rs:534 | Captures actual operation/phase/child/protection generations; existing exclusive maintenance waits for admitted work. Fresh exact record comparison prevents retiring a stale subset. Public result/channel keys are excluded. |
| inventory / owner creation discovery | retention/inventory.rs:1347; owners.rs:993 | Incomplete children remain roots; completed remote winning stage is not resurrected by new creation proof. Exact closed child can establish original creation ownership. |
| plan_export_file_removal / validate_export_file_plan | retention/removal/export_file.rs:63,18 | One named child, retired creation proof, same operation/native binding, original host, no registration drop or prefix sweep. Historical public export result and channel references independently block deletion. |
| check_stage_grants / owners::available | removal/export_stage.rs:94; export_file.rs:87 | Valid exact active owner grants and existing removal reservations remain stronger promises. Retired grants require native retirement marker/receipt validation. |
| remove_objects → finish_removal → remove_created_file | retention/removal.rs:636,781; storage/removal.rs:81,183 | Reuses existing saved intent/outcome protocol and maintenance exclusion. Unresolved effects recheck saved proof/configured host/grants; payload hash plus original device/inode and stable path/file metadata precede unlink; absence and parent-sync reconciliation remain existing behavior. |
| HTTP / CLI | http.rs:64,230; main.rs:95,535 | Thin plan/retire transport; execution uses existing removal service. No remote object deletion or whole remote catalog-maintenance capability added. |

## Invariants and data flow

HostFile is created from the original open create_new file and saved under operation/native revision/child ID. It is never overwritten by cleanup. The original protection is older than custody, and closed proof is newer than custody. Partial bytes get one planning hash into the existing ObjectRef removal intent; closed bytes reuse their retained hash. Removal checks the original inode as well as the intent's bytes, so a replacement with identical bytes does not inherit custody (storage/removal.rs:200–230). The one MiB streaming hash buffer uses the existing Budget::blocking allowance; no whole-file buffering was introduced.

Retirement markers end recovery roots without editing original operation, protection, child or completion JSON. Exact public pins retain their separate payload promise. Owner grants are not implicitly retired. Native catalog commits still provide the existing table-bound conflict check. Host removal remains Unix-only and assumes the existing exclusive host custody model; this is not a conditional filesystem unlink guarantee against arbitrary external processes.

## Tests and evidence

- host/tests.rs:33 `partial_host_bytes_reclaim_after_exact_terminal_retirement_and_intent_retry`: actual native partial payload; refusal while recovery lives, explicit retirement, injected saved-intent gap, exact unlink and replay/cold outcome; original rows unchanged.
- host/tests.rs:63 `closed_losing_file_reclaims_without_ending_immutable_winning_pin`: actual second completion, selected-channel refusal including HTTP, successor channel, loser-only unlink, historical winning-pin refusal, completed acknowledgment and literal duplicate/null reads preserved.
- host/tests.rs:101 `partial_same_bytes_replacement_cannot_acquire_original_creation_identity`: retains old inode, copies identical bytes to original path, gets removal_changed and zero confirmed unlink.
- Existing storage/removal.rs tests retain absence, changed bytes, symlink, unlink and directory-sync failure coverage because the original no-extra-identity path delegates through the same executor.

Coverage is adequate for this bounded change. New tests are Local; common native writer custody and original-host executor are shared with configured Remote staging. Existing remote winning-stage tests cover configured-reader exclusion/grants. No new provider, power-loss, concurrent-loser or arbitrary remote payload deletion claim follows from this review. Added cost is two bounded lifecycle writes per successfully closed child, plus a planning stream hash only for previously unclosed bytes; no throughput claim.

Conclusion: the patch gives users a supported way to explicitly end unfinished export recovery and reclaim one original partial or losing host child while preserving every independently retained public pin or grant. Confidence HIGH in the traced authority/identity boundaries; root owns full-gate qualification.

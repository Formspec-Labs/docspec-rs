# R6 based-Build ancestry review

VERDICT: APPROVE (static; runtime gates pending). No actionable correctness finding in the reviewed scope.

Artifact: `/tmp/docspec-build-ancestry.patch`, SHA256 `502fc336d5f587d276e3894ea6fc57a6cb0a9c129f40531acf7e6805b6b9c7ca`. All changed files read, plus production verification, retirement, availability and removal callers. No builds/provider/shared edits.

## Patch and trace

| Function | Evidence | Verified behavior |
| --- | --- | --- |
| Inventory.enqueue | staged `src/engine/retention/inventory.rs:805–887` | Exact ObjectRef/kind/native binding/change binding must agree. Release strength upgrades monotonically from interpretation to payloads; stronger roots queue expansion when necessary. State stays budget charged. |
| Inventory.computed / qualification | `:947–970` | Current origins remain full source/output dependencies. Inline qualification keeps origin closure and target snapshot. Only computed base manifest uses selected history strength. |
| Inventory.dataset | `:1039–1078` | Narrowing requires an explicitly based Build, Independent relationship, no related result and no evidence. Current groups and inputs remain fully rooted. Other modes retain full parent closure. |
| Inventory.interpretation | `:1081–1101` | Keeps every historical native group metadata and logical metadata, source metadata, recursive manifests; preserves historical inline qualification payload closure. It does not promise old ordinary values. |
| Inventory.expand / release_pin | `:1104–1125,1655–1722` | Original exact manifest decoding/owner check remains. Decode uses current strongest visited state; completed direct result remains full. Pending duplicates skip after strongest expansion. Weak-already-decoded to full needs one explicit bounded manifest reread, covered by count assertions. |
| collect_inventory | shared `src/engine/retention/inventory.rs:275–366` | Exact retirement markers suppress only matching generations. Current/other retained roots still win. No automatic retirement or unlink added. |
| verify_binding_history / verify_groups | shared `src/engine/backfill/verification.rs:409–523,45–74,240–267` | Historical ordinary bindings require schema/selection history; current data verification reads selected source/group. Historical inline qualifications explicitly check old originals, matching retained full qualification edges. |
| remove_objects / remove_owned_file | shared `retention/removal.rs:326–458,730–749`; `storage/removal.rs:85–171` | Existing owned catalog, creation proof, fresh graph, maintenance exclusion, exact file/hash/path and durable outcome checks remain. |

## Data flow / invariants

H1: weaker ancestry can never overrule a stronger live availability/recovery root. Confirmed by `release_payloads` monotonic upgrade and exact metadata identity check (`enqueue:819–845`), plus expand selecting visited strength rather than the queued weaker value (`:1714–1720`). Tests cover full first, history first after decode, and both queued before decode.

H2: based Build has a complete independent current population. Narrowing is restricted to that actual operation/relationship/evidence shape (`dataset:1045–1049`); current groups, origins and input ObjectRefs remain full (`:1050–1064`). Related/evidence/update/schema/backfill sources are not silently narrowed.

H3: interpreted ancestors suffice for current deep verification. The production verifier walks owning manifests and native schemas, then verifies current selected data. It separately executes historical inline qualification checks; `interpretation:1096–1099` keeps that explicit closure. Missing original payload is therefore never authorized where an inline proof still needs it.

## Tests / concrete boundary

`tests/retention_build_ancestry.rs:45–157` uses actual public Engine guards. Before any deletion it ends old availability, all matching phase recovery, old terminal operations, and finally the new terminal rebuild recovery (which legitimately captured the full base). It asserts old files remain blocked until that last root ends. It then uses existing preview/removal, asserts exact removed byte sum and actual missing payload files, preserves manifest/native schema metadata, cold-opens, reads exact duplicate/null computed values and independently verifies the selected result. Completed-result retry after OperationRecovery is legitimate: `check_operation_entry` checks standalone availability retirement, not ended terminal recovery (`availability.rs:244–252,315–328`), and the new selected result remains available.

The historical-inline-proof branch (`:129–137`) retires the independent qualification completion but confirms selected ancestry still protects the original payloads, then cold reads and verifies. This protects the real verifier dependency rather than pretending a full rebuild erased immutable historical proof.

Functional upgrade unit test `inventory.rs:2348–2420` verifies stronger roots in both encounter orders and explicit metadata-read counts. `tests/integration.rs` registers the new suite. These are meaningful runtime checks to execute; this review does not assert they passed.

## Limits

This is local owned-catalog retention qualification for independent based full Builds. Remote maintenance remains unqualified. Historical inline proof bytes, current origins, active results/recovery and cross-owner grants may still block reclamation. No total retained-storage, performance or full R6 closure claim follows from this patch.

## Final artifact closure

Final digest independently confirmed: `502fc336d5f587d276e3894ea6fc57a6cb0a9c129f40531acf7e6805b6b9c7ca`. Removing exactly the four added producer-removal fixture lines and restoring the new-file hunk length reconstructs the previously reviewed digest `1cad346cc92b810cd09125ea20252902e23b03cb2086e937111108207424ee44`; no production or other test delta is present.

`tests/retention_build_ancestry.rs:151–154` first asserts all authoritative admitted object paths differ from the external request input, then removes only that synthetic producer before dropping/reopening Engine. Cold selected read/deep verification and completed request retry follow at `:155–161`. This strengthens the no-source-reexecution check without removing current authoritative input or changing production semantics. Final APPROVE remains static; no builds or shared edits were performed.

## Runtime fixture correction

Parent's focused run passed unqualified reclamation but rejected the original historical-proof fixture at the based Build with `correction_mismatch`; retained failure evidence is `docs/reviews/stable-build-ancestry-focused-2026-10-11.log`. The production guard correctly refuses fresh origins under the acknowledged deficient definition. The earlier static review did not identify this fixture assumption.

Supplemental test-only artifact `/tmp/docspec-ancestry-qualified-fixture.patch`, SHA256 `258fa683d232102a5f45acdc2578d8dd2917b62625259edf8b385dc70ffa0cb3`, preserves the qualified sibling selection in history, selects an explicit ID-aware replacement definition through public Backfill, then performs the based Build. Exact input field IDs and independent duplicate/null values are asserted. This follows the existing corrected-definition path in `tests/dependency_corrections.rs:184–207`; exact-definition correction scope in `src/engine/corrections/records.rs:21–27` and original-context guard in `src/engine/corrections.rs:120–137` stay unchanged. Runtime confirmation of this supplemental fixture remains required.

## Executed qualification

The first combined focused run retained the `correction_mismatch` failure in
`stable-build-ancestry-focused-2026-10-11.log`. Rebuilding under a deficient
definition is correctly refused. The test-only follow-up
`/tmp/docspec-ancestry-qualified-fixture.patch` (SHA256
`258fa683d232102a5f45acdc2578d8dd2917b62625259edf8b385dc70ffa0cb3`)
uses an explicit ID-aware replacement definition before the full Build. The old
inline proof remains in its exact historical selection. No production guard changed.

`stable-build-ancestry-followup-2026-10-11.log` passes both reclamation and retained
proof cases plus current/legacy admission and cutover checks. The reclaimed case
removes exact obsolete raw/native payloads, retains historical schema metadata,
cold-reads literal current values, deep-verifies and reconciles completed retries.
`stable-build-ancestry-unit-2026-10-11.log` passes both root encounter orders and
the bounded reread case when a stronger root arrives after history expansion.
The full repository gate remains pending at this note's creation.

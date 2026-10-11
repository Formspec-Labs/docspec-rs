# Independent generic validator identity review

VERDICT: APPROVE for parent-owned execution. No unresolved static blocker.

Artifact: /tmp/docspec-core-validator-final.patch, SHA256 f8dc0cc4da8e111c010f24ec9c2a8731d870d53ba372d234d0bd50d2c29fe340. Reviewed all six touched files in /tmp/docspec-core-validator/new, the immutable original core, cumulative85e737, and final current-readback supplement cc2508c3. No execution, builds, provider calls or shared edits by reviewer. Confidence: medium-high static; compilation/runtime remain unverified here.

## Findings first

Resolved correctness finding: original core trusted captured-base native files before every validation lookup. An unknown/older base checker could therefore seed the current checker with prior footer approval, and validation hits unnecessarily expanded inventory. Final engine.rs1634 calls trust_current_readback only inside actual validation miss; validation.rs67–84 requires exact ObjectRef manifest plus current typed stamp before Checks::trust. Missing/old/scalar-only proof falls back to current bounded target inspection. The base fixture tests actual file inspections Some(0) with current proof versus Some(1) with unknown proof.

Resolved coverage finding: initial fixture suite lacked current generic readback after upstream phase retirement with native revision assertions. Final validation/tests.rs216 captures every phase row revision and raw JSON, retires upstream through the real service, removes only external producer, cold reopens, resumes readback/selection and requires unchanged readback revision, zero current checks/SQL, and identical entire phase map. It can catch validation resurrection that unchanged public PhaseResult values would miss.

Resolved fixture mismatch: new private stamp changes raw row shape while public ReleasePin stays unchanged. lifecycle.rs937/1020 and updates.rs1013 now decode the typed pin; updates.rs1012 retains the separate raw-row equality check. No preservation assertion was removed.

## Patch summary and function trace

| Function | Proposed source | Inputs/output | Verified behavior |
|---|---|---|---|
| phase_key/material_phase_key | engine.rs2244/2251; validation.rs10 | existing material phase key → check key | Only validation wraps key with built-in VALIDATOR identity; processor, admission, processing, evidence and receipt keys remain unchanged. |
| ValidatedReadback codec/stamp/current | validation.rs18–63 | public pin + optional private string | Maintained JSON Value decoder preserves native integer field-map keys. Absence/older string is unknown; nonstring refuses. Serialized stamp borrows pin rather than cloning inventories. |
| prepare_and_select retained path | engine.rs1492–1524 | exact operation/readback/protection → selection | Exact admitted request/format/eligibility/boundary checks remain. Current proof bypasses current-check upgrade; unknown proof enters revalidate_readback. |
| revalidate_readback | validation.rs87–139 | retained exact pin/protections → same pin | Fresh configured routine Checks, same immutable manifest equality, required inputs and source/groups/evidence verified. Related graph uses existing shared verifier. Guarded reload checks exact public pin before co-committing stamp and original protection references. No SQL or output rewrite. |
| trust_current_readback | validation.rs67–84 | captured base + request-local Checks | Compact typed header; exact manifest/current string gates native trust. No trust work on current validation hits. |
| readback creation/reconcile | engine.rs1910/1969/1983 | validated components → stamped readback | Existing winner must carry current proof; fresh readback stamps current check with existing atomic completion/protection helper. |
| strict inventory | inventory.rs1506/1898/2584 | generic readback JSON → exact release roots | New private field recognized only generic readback type; result and scalar rows remain strict. Legacy dataset normalization scoped to this wrapper. |

Callers/callees inspected: phase(), complete(), completion_records(), retained_protections()/reference binding validation; Checks::routine/table/trust; verify_local_release and backfill::verify_groups (including Update independent state checks); evidence::verify; verification::verify_all/walk; check_read_release; plan_request; select_prepared; existing phase-retirement regression.

## Data flow and invariants

Built-in check identity is separate from computation identity and immutable public manifest. The versioned validation key acknowledges current checks for one exact physical/dependency generation. Current readback independently carries that proof after upstream recovery retirement. Requalification writes the same pin with one private identity and refreshed native claim/binding revision; historic native revisions remain retained. A concurrent changed public pin refuses phase_generation_conflict before stamp; completion/authority guards precede write and selection. Existing exact dependency/generation comparisons in complete() remain.

Completed operation acknowledgment returns from engine.rs1312 before readback/preparation. Ordinary host reads still parse public ReleasePin and resolve exact retained selection, without consulting current validator or inspecting payloads anew. Unknown check identity affects unfinished work, not an already acknowledged result. Explicit deep verification remains explicit; routine upgrade is not deep value reconstruction or a provider qualification.

## Tests and concrete edge cases

- validation/tests.rs62: compatibility-shaped old validation record; actual retained processing; producer absent; current check once; material key/native revision unchanged; current validation hit does not repeat; old check explicitly retires.
- :105: absent and older-string readback after upstream retirement/cold reopen; one upgrade, exact same public pin/manifest, no SQL, current stamp once; completed operation after older stamp remains exact acknowledged result.
- :145: missing authoritative check input refuses io, preserves old stamp revision/material, leaves no selected result.
- :159: real related summary upgrades without rewriting source/contributors/material/evidence phases; literal nullable-source count3; completed replay performs no further check.
- :198: actual native file inspection with exact current versus unknown captured-base proof.
- :216: current readback after all upstream retirement, native revision/raw-row preservation and zero inspection entry/computation after cold reopen.
- inventory.rs2584: legacy/current wrapper decode including native numeric IDs; stamp forbidden on public result/scalar type; unknown top-level/nested dependencies, nonstring stamp refuse; narrowly supported legacy dataset fields normalize.

These fixtures exercise the current built-in validator using compatibility-shaped prior records with real native pins/protection bindings. They do not implement a selectable external validator, claim every future checker change is tested, or qualify provider faults. No additional framework is required for this scope.

## Conclusion

The final patch achieves validation-only invalidation while retaining computation, exact completed acknowledgment, independent readback lifetime and strict retention semantics. The concrete unknown-base trust correction improves correctness and removes unnecessary trust/inventory work on validation hits; no speed claim is made. Coverage of this bounded change is adequate statically, pending parent's focused tests and full gate.

## Parent-owned first runtime outcome and fixture correction

Parent reports original85e737 focused run: three passed, two failed. The static review missed two test assumptions: missing authoritative input returns existing `unavailable_phase` from storage.rs220 rather than `io`; global summary column0 is the reserved synthetic grouping column rather than the named count. Author supplied test-only correction e0bd56b0125edb696a97c869fae247fc740ac2b20f0cfe5cd1aa907f9a1a6d6d: exact unavailable_phase refusal and explicit `column_by_name("rows")` count oracle. Reviewer read the two hunks; neither changes production behavior, exact pins/revisions or refusal preservation assertions. The final current-readback supplement was not in that original runtime. Corrected full focused and broad checks remain parent-owned; prior static approval is not a runtime pass.

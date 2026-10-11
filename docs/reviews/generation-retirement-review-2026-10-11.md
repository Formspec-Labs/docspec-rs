# Generation-seal retirement review

**Verdict: approve the bounded Local retirement slice, subject to the focused lifetime/transport cases and repository gate.** No remaining actionable static finding. Reviewed `/tmp/docspec-seal-retirement.patch`, final SHA `6a1188882fcc55bf937f2992aff84599bd43595f21779da0232e0fa717896126`, against the frozen shared source on 2026-10-11. No shared edits, builds, formatting or provider calls by this reviewer.

## Scope and requirement

R6 requires separate history/availability lifetimes, shared-reference protection and exact reclamation after the final promise ends; Q3 requires both preservation and eventual eligibility. The patch ends one producer seal's standalone availability while retaining its immutable member identities. It reuses existing retirement plans, exact-generation markers, guarded commits, grant retirement and removal. It does not add a new deletion path or weaken an admitted consumer's whole-input promise.

## Function trace and invariants

| Boundary | Reviewed behavior |
|---|---|
| New `retention/seals.rs:98`, `plan_generation_retirement` | Existing request admission, owned-catalog gate and family lock precede lookup. The exact native record revision is captured in the existing `RetentionPlan`. No object is removed or grant retired by planning. |
| `seals.rs:8–58`, record/plan validation | Admits seal, binding and family-members records before decoding. Checks generation, owning family table, exact membership and binding/claim native revision. Existing `owners::validate_binding` checks digest, consumer and required owner references. Plan records must equal precisely that one generation. |
| `seals.rs:144` → shared `retire_retained` | Rejects other scopes. Existing exclusive maintenance ownership drains admitted workers; the family lock and guarded catalog commit serialize the decision. Fresh validation rejects changed or already retired generations; exact prior receipts reconcile retries and lost responses. |
| `inputs.rs:147,221` | Identical resealing of a retired generation refuses. Fresh capture checks retirement while holding source/family locks; `require_available=false` remains limited to the existing admitted legacy recovery path. |
| `admission.rs:175` / `engine.rs:1285` / `refresh.rs:326` | New consumer admission uses availability-checked capture and independent owner protection. Existing admitted coverage is subsequently guarded by exact seal identity and source eligibility, not by a new availability promise. A retirement cannot interleave within an admitted operation on this owned host because the shared maintenance gate is exclusive. |
| `retirement.rs` additions → `inventory.rs:304–359` | `generation/<id>` becomes an explicitly retireable root. Inventory still validates its retained binding, but skips that exact retired claim and binding as active roots. The seal's immutable record remains. |
| `inventory.rs:909–923` / `grants.rs:45–165` | Consumer coverage continues expanding all exact seal member references. Family owner grants remain protected until separate unused-grant retirement proves no active bound claim needs them. Consumer grants and other selected/recovery roots independently block removal. |
| CLI/HTTP handlers | Thin calls to the same public services; plan and execution stay distinct. `generation_retired` maps to HTTP conflict. Owned-catalog refusal still applies remotely before family lifecycle effects. |

The new validation is bounded metadata work: one seal/member/binding decode allowance and the existing shallow reference walker, with no payload reads or separate reachability graph. Retirement receipts do not require removed payloads to be reopened, so cold reconciliation remains possible after exact removal. This is admission accounting, not a measured memory/performance result.

## Tests and review disposition

The real lifetime fixtures in staged `retention/grants/tests.rs:649` create coupled native sources and seal them through public services. They reject altered scope/revision without changed records, reconcile a controlled lost retirement response, retain the identical seal record, and refuse fresh capture/resealing. They supersede the sources, end their independent recovery/availability promises, retire unused grants, and use the real removal service on an exclusive old native payload. Cold reopen checks exact receipt/removal reuse and current values.

The consumer variant admits a real join before retirement, finishes it afterward, and proves that its independent references still block source removal. Only after ending those separate consumer promises and grants may the old payload be removed. This correctly preserves the whole exact input closure; it does not imply subset-level source reclamation.

The transport fixture at staged `tests/input_generations.rs:699` compares Engine/HTTP/CLI plans, HTTP execution/refusal and a cold CLI execution retry. Existing active-seal grant-retirement tests remain necessary regressions. These fixtures were inspected but not run by this reviewer; parent-owned results establish their execution status. They do not qualify multi-host/provider maintenance, power-loss durability, all cleanup interleavings or reclamation of every old object.

The one documentation finding was closed: `seal_generation` now says explicitly retired seals refuse resealing and removes the obsolete claim that cleanup is disabled. No source-level blocker remains.

## Runtime follow-up: fixture assumptions require correction

Parent-owned execution rejected both lifetime fixtures at their unbased coupled replacement with `relationship_base`. That is the correct production guard (`inputs::check_rebuild`): changing an existing coupled source requires its selected base. The initial static review did not establish fixture executability, and the new lifetime/removal claims remain unqualified until corrected cases pass.

A valid end-of-promise sequence uses an explicit based transition from coupled g1 to Independent g2, verifies that g2's retained parent still protects g1, then creates a genuinely new-material unbased Independent g3. End both g1 and intermediate g2 phase/result/operation promises and unused grants before testing removal. This proves retirement after ending that coupled lifetime; it does not qualify reclamation beneath a still-selected coupled descendant. No relationship/base or historical evidence guard should change.

Two further fixture details must reflect actual reuse: Build phase keys are material-addressed rather than `phase/<operation>/`, and identical input/SQL/definition can intentionally reuse native output across operation or relationship changes. Use exact serialized phase ownership/bindings when ending promises, tolerate no newly created phase for an operation, and give the final replacement genuinely changed material with independent expected values. A new operation ID alone cannot justify a forced rewrite. Reviewer sent these corrections to the author; parent owns execution of the revised fixture.


## Corrected source lifetime and remaining consumer fixture limitation

Reviewed the applied fixture-only correction `4eda64ad8689da8e70c3afffd331a92bf47fbc7869f75e5c495a903e9d578d36`. The coupled-to-Independent based bridge keeps and asserts its parent; the later independent rebuild uses genuinely changed `[3,4]` input bytes, asserts no parent and no old candidate, and ends both old/bridge promises through real services. Exact phase selection now uses serialized `PhaseResult.operation` under `phase/` and tolerates reused phases. No production guard changed. Parent-owned execution reports the non-consumer exact removal and cold-read case passed.

The selected-consumer variant then correctly failed with `related_source`: raw Build cannot replace a related result. A full Refresh still requires the selected base (`refresh.rs:432–453`), and release construction retains `parent_manifest` (`engine.rs:1755`), so that route does not prove reclamation beneath a selected related descendant. The review does not approve weakening either guard or calling that lifetime complete. A smaller valid fixture can finish the already admitted consumer through an unselected readback, prove its values remain readable after seal retirement, then explicitly abandon and retire its separate readback/recovery/grant promises. Otherwise keep a selected-consumer preservation-only case and record descendant-lifetime reclamation as an unresolved limit. Revised consumer fixture and parent-owned execution remain pending.


## Final selected-consumer preservation correction

Reviewed `/tmp/docspec-seal-consumer-correction.patch`, SHA `b06a93f4f70f121d7494909542be796416e871c6222549941521910e2ba806b6`. **Approve the corrected bounded scope**, subject to parent-owned tests. The selected-consumer case now ends with an exact removal preview that reports all candidate bytes protected, and cold reopen proves the same selected consumer still returns `[1,2]` while current independent producers return `[3,4]`. The old payload remains present and protected. It no longer attempts unsupported raw replacement or claims to end the consumer's required lineage. The non-consumer case separately proves exact reclamation after every promise in its supported independent replacement sequence ends.

A small test robustness recommendation was sent: sort collected values before comparing the join result, whose ordering is unspecified. This preserves exact multiplicity/value assertions without inventing an ordering guarantee. No further production change is requested. Parent reports the final pair is running; this static review does not turn pending execution into a pass.

## Parent execution follow-up

The final two lifetime cases pass in `generation-retirement-lifetimes-2026-10-11.log`; the generation/transport suite passes in `generation-retirement-transport-2026-10-11.log`. The fixture now compares unordered native results as multisets. Failed attempts remain retained. The combined repository gate is recorded separately.

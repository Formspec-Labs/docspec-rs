# RunPlan final-selection review

**Verdict: approve the narrow R7 correction, subject to parent-owned execution checks.** No actionable finding. Static review of `/tmp/docspec-plan-selection.patch` against the frozen repository on 2026-10-11; no source edits, builds, formatting or provider requests.

## Requirement and change

R7 requires truthful remaining work and the next supported action (`docs/requirements/07-operations-qualification.md:27–35`, UR-03). Current `plan_request_admitted` can report `completed=false` with no required work after independent readback. Execution still must select the release. The patch adds `selection` to incomplete plans and treats a retained result as completed even when another result is currently selected. It adds no record, field, ledger, payload phase or telemetry pass.

## Trace and invariants

| Path | Evidence | Result |
|---|---|---|
| `Engine::plan` / shared planner | `src/engine.rs:392–419` | Existing request admission and dataset serialization remain unchanged. |
| Request and authority checks | `engine.rs:420–454` | Backend, request identity, availability, abandonment and incomplete-operation eligibility checks still precede the new return. The patch does not grant selection authority or bypass a refusal. |
| Retained result/readback | Patch to `engine.rs:455` | A result means no remaining work for this operation. Readback without a result means reusable readback plus required guarded selection. Neither interpretation relies on the current selected head. |
| Other incomplete plans | Patch to `engine.rs:558` | Existing phase-discovery and retained-processing logic remain intact; one bounded `selection` label is appended after the existing missing phases. `PHASES` and execution work markers are unchanged. |
| Actual recovery | `engine.rs:1358–1400`, `select_prepared` | Execution already resolves retained readback, checks identity/protection/availability, and performs guarded selection. The plan now describes that existing behavior. A required selection can still refuse on a concurrent eligibility change. |
| Transports | `src/main.rs:451`, `src/http.rs:539`, `adapters/dagster/src/docspec_dagster/client.py:235–237` | CLI/HTTP serialize `RunPlan`; Dagster validates dataset, operation and completion without imposing a phase enum. No caller requiring the previous incomplete-empty list was found. |

The completed-result branch avoids obsolete phase/base reconstruction when a retained result remains available. Existing availability and abandonment checks still run. This is metadata-only planning; it neither verifies payloads nor changes which completion records authorize reuse. Planning a never-seen dataset may initialize its lifecycle table as before; the patch does not newly promise globally side-effect-free planning.

## Tests and boundaries

The added lifecycle fixtures check: a missing producer file still permits an initial plan listing all handoffs; an actual CLI process killed after acknowledged readback is planned without changing durable records, then resumes with no payload-work markers and unchanged phase/readback records; a completed older operation remains complete without reselecting it after a later selection. The process test uses the existing SIGKILL/CLI harness in `tests/support/mod.rs`, whose bounded output and work-marker collection were inspected. Existing completed-plan assertions remain valid.

These tests are meaningful execution checks, not tests merely duplicating the new conditional. They were not run by this reviewer. This small change resolves a specific misleading plan result; it does not complete R7 consumed-input explanations, rerun reasons, live observations, resource-pressure reporting or the wider qualification matrix.

## Execution follow-up

The focused lifecycle suite passes in `plan-selection-focused-2026-10-11.log`.
The combined repository gate found an older retirement test still expecting no
work before selection. Its assertion now expects only `selection`; its corrected
run passes in `qualification-lifetime-selection-2026-10-11-2.log`. That invocation
also contains a separate, unsuccessful qualification-lifetime fixture; it is not
a passing combined gate. No production change was needed for this assertion.

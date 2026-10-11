# Required phase reasons review

**Verdict: APPROVE.** Static review of `/tmp/docspec-plan-reasons.patch` (`48353eb9cd639bf70003f5d217138435ebdbe65a9f04e5826012a4865bf7c204`) against the current checkout. No source edits, Cargo, or provider calls by reviewer. No actionable finding.

The patch exposes the reason already known by the existing completion lookup: no exact retained completion, or explicit retirement of that completion's recovery promise. This implements a bounded R7-T01/UR-03 operator improvement without claiming failure state, active progress, changed input, or full R7 completion (`docs/requirements/07-operations-qualification.md:45`, `RESEARCH.md:28`).

| Function/type | Proposed location | Verified behavior |
|---|---|---|
| `PlanPhaseReason` and `RunPlan` | `/tmp/docspec-plan-reasons/new/src/engine.rs:173` | Adds serialized snake_case reason enum and a phase-keyed map. No durable schema or ledger. |
| `completion_with_reason` | `new/src/engine/retention/retirement.rs:339` | Same exact row lookup and native-generation retirement validation previously used by execution. Returns absent/retired reason, otherwise unchanged decoded completion. No payload I/O or second retirement lookup. |
| `active_completion` | `new/src/engine/retention/retirement.rs:331` | Delegates and discards only the explanation; execution retains the original Option/error behavior. |
| `plan_request_admitted` | `new/src/engine.rs:435` | Populates reasons only for required durable phases. Readback/result short-circuit keeps empty reasons. Final selection remains a separate named handoff. Retained processing removes obsolete admission from both required lists. Receipt adoption still skips unnecessary receipt work. |

Data flow: the reason is created during existing exact phase lookup, inserted only if that phase is required, removed if durable processing makes admission unnecessary, and returned as bounded metadata. It never affects eligibility, work execution, retirement, or phase identities. Growth is bounded by the already fixed phase list.

Tests extend the initial no-input planning case; the actual explicit-retirement scenario temporarily moves original input before planning and expects `recovery_retired` for processing, absent completion for validation, and no reason for retained admission or final selection. The existing recomputation assertions remain and ensure an old marker does not hide a new native generation. The processing-retained/admission-retired case checks no spurious reason survives. Runtime remains parent-owned/pending; no reason to add another payload or broad test pass beyond the planned gate. Coverage adequate; confidence high.

## Parent-owned execution

The combined `./scripts/check` passes in
`remote-export-plan-full-check-2026-10-11.log`. It includes the relevant library
and integration cases, strict all-feature Clippy and formatting, and the restore
and worker-initialization suites. Opt-in provider and application checks have
separate retained evidence; this gate does not claim they ran automatically.

# R7 reusable phase origins review

VERDICT: APPROVE (static only). Artifact `/tmp/docspec-plan-origins.patch`, SHA256 `a3ab6ad26a180e15d6fa7ac8903e1d67d3c83cafaf350cbdb71556f793d17970`. All changed production/test hunks read. No Cargo/provider/shared edits.

`RunPlan.reusable_phase_origins` contains exact existing retained key/native revision, rather than inferred operation identity or new durable state. `completion_with_reason` returns the native revision with the already-decoded completion; `active_completion` preserves its previous Option value behavior. Exact retirement checks occur before exposing that revision. Planning consumes the same completion to decide dependency/adopted-receipt reuse, so it does not add another phase parse. The readback fast path resolves once from the same immutable view and carries its exact revision; the later missing-readback branch does not query again. Completed older operations preserve their acknowledgment independently of current authority.

Tests check cross-operation original processing identity and exact admitted input after request locator removal, metadata unchanged by planning, retired generation exclusion and later revision replacement, and readback/older completed plans. Source references locate evidence through inspect; they grant no protection or promise future availability. Public CLI/HTTP serialization follows the existing service naturally. This does not qualify all consumed-input/rerun observations or close R7.

Pending R3 effective schema-key changes must preserve this one-lookup origin path; this review does not approve an admission-only schema-key shortcut across generation replacement.

## Executed checks

`plan-origins-focused-2026-10-11.log` passes the phase dependency, retirement and
lifecycle suites, including the actual killed-worker cases. This is correctness
and recovery evidence, not a new performance or complete R7 qualification.

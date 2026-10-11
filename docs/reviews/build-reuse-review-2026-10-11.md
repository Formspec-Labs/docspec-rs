# R3 exact first-based Build reuse review

VERDICT: APPROVE (static only). Final artifact `/tmp/docspec-build-reuse.patch`, SHA256 `38bd11ed22b580688eb9429ab97169aedf5de5d39fd1837d31982a76e914e7ff`, independently confirmed. Initial reviewed digest was `1b075bf9ca6a7e14ced46335065d7a0894d3dea68f4968e40e88c8b79d03c200`. All changed hunks read; current schema normalization, phase lookup/completion, exact-generation retirement and planning paths traced. No builds/provider/shared edits.

`schema_key` qualifies only the active schema-less processing completion whose full TablePin equals the exact selected source. On a miss, candidate decode and admission are dropped before the native schema hash; changed interpretation still uses full schema/history key. The chosen value is saved in existing Operation, without aliases or copied completion/protection generations. Original unchanged-source fixture remains an exact TablePin reuse test with external locator removed.

Admission identity alone cannot guarantee later generation identity. The shared check_reused_build_schema resolves immutable result/<request.base>, not latest authority, for every format12 based schema-less completion exposed by plan or actual runtime phase lookup. Exact pin equality avoids native reads. Otherwise existing format12 native schema/history comparison accepts equivalent history in a new physical table and refuses changed field identity before reported processing reuse or downstream work. Retired generations remain excluded by the existing completion lookup; legacy formats keep prior interpretation behavior. Selection retains its independent routine verifier.

The phase's original key, native revision, dependencies and retained protections remain authoritative. R7 origin lookup still reports the actual generation returned by the one planning completion read. The capture check adds a bounded cached completion decode and the compatibility check adds a bounded base-result decode; author explicitly documents this metadata cost instead of carrying a large preparation object through admission. No payload scan or SQL/native write occurs on common exact-source reuse. This is an acceptable bounded tradeoff for avoiding complete repeated processing; no measured performance claim follows.

Fixture models an explicitly retired slot followed by a real maintained native replacement. Compatible metadata UUID change must qualify; field42 replacing field1 must refuse in plan and runtime, preserving selected original and lifecycle rows. It models the native completion boundary, not a scheduler race or actual old binary execution. The handwritten unreserved output path is being replaced with existing reserve_attempt before native effects to preserve standard protected creation evidence.

R7 origins review already closed independently at /tmp/docspec-plan-origins-review.md for a3ab6ad26a180e15d6fa7ac8903e1d67d3c83cafaf350cbdb71556f793d17970. No remaining origins finding; this supplement preserves its native generation semantics.

## Final closure

Final fixture uses reserve_attempt before native creation, and replacement completion operation/request_digest agree with its captured operation (`admission/tests.rs:819,831–834`). The existing `phase_generation_conflict` refusal is reused in both plan/run assertions, preserving the established HTTP conflict mapping. No other production guarantee changed. No actionable finding remains. Parent should run the admission suite, schema/rebuild/phase-retirement/origins suites and full gate; this review reports no executed tests or complete R3 qualification.

## Executed checks

Both admission reuse cases pass in `reuse-stage-focused-2026-10-11.log`. That
invocation failed only in the separate held-reader fixture, retained and corrected
in `held-stage-reader-fixed-2026-10-11.log`. Phase dependencies, retirement and
lifecycle checks pass in `plan-origins-focused-2026-10-11.log`.

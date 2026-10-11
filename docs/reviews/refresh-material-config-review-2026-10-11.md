# Material join configuration review

Verdict: APPROVE for parent-owned execution. Artifact `/tmp/docspec-refresh-material-config.patch`, SHA256 `985379ec847462052f12b0e4c8aafb5e8c92e01fe0b7e3bb58b5a82db52111c2`. No builds, formatter, provider calls or shared edits by reviewer.

Requirements: R3 material configuration changes invalidate affected values/checks (docs/requirements/03-schema-backfill.md); R5 matching dependent material and invalidation recovery (docs/requirements/05-dependent-refresh.md). This is one meaningful built-in transform change, not a new arbitrary invalidation framework.

The fixture changes NullMatching::Equal to Never for the same consumer, exact producer captures, definition and selection policy. The null/partial-null rows in the existing fixture make this materially observable: the direct Rust occurrence-pair oracle produces different values/contributors, the changed result has seven rows and excludes both named null-match contributors. The oracle loops over exact native source occurrences and implements equality/null handling itself; it does not call production join_plan. check() compares selected source values and contributor file+row pairs to that independent oracle, including duplicates. Deep verification is additional corroboration rather than the sole oracle.

src/engine/refresh.rs:458–482 keys Full admission by processor plus captured inputs, and subsequent material phases by processor plus complete transform/definition. The tested boundary therefore should reuse admission while recomputing processing/evidence/validation/readback. The test observes actual killed CLI worker receipts, original native processing revision through public reusable_phase_origins, unchanged old record and exact reference binding, new output TablePin, and unchanged exact coverage. Plan calls are also asserted read-only. Changing operation ID alone is not the evidence: the transform changes, processing reuse is refused, affected work is observed, and the output differs under an independent oracle. Original processing remains reusable under its original transform afterward.

Process supervision uses the existing support::kill_after_with_work helper already reviewed separately. Removing only external producer locators preserves authoritative native inputs. No production change, synthetic catalog replacement, gate weakening or remote mutation is introduced. The fixture does not measure all configuration/processor/validator boundaries or throughput. Actual execution and exact phase receipts remain parent-owned gates.

No actionable static finding.

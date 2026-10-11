# Same-consumer refresh policy reuse review

**APPROVE for parent-owned execution.** No actionable static findings in `/tmp/docspec-refresh-policy-reuse.patch`, SHA256 `9a8177111200150177d7ca849553b5b7f32307840dd2169acebf4c2f66b56f92`. No builds, provider calls or shared edits performed.

## Patch and trace

Two test-only files add one real-process Captured→Latest same-consumer case and expose actual work-start receipts from the existing bounded SIGKILL harness. Existing kill_after callers retain their behavior.

| Boundary | Evidence | Verified static behavior |
|---|---|---|
| Material reuse | `src/engine/refresh.rs:458–480`; `src/engine.rs:1980–2026` | Full-refresh phase keys bind processor, exact input/transform and definition independently of selection policy; active completion availability and protection bindings remain checked. |
| Current promotion policy | `src/engine/refresh.rs:264–308,326–352` | Fresh selection validates captured coverage/succession and the new operation's policy. Latest rejects observed live heads differing from captured targets with stale_input. |
| Explicit new operation | private `tests/related_refresh.rs:635–667` | Policy change uses a new operation; request and coverage remain distinct, original processing generation/protection are unchanged. This does not mutate the old request or resume it under another policy. |
| Real recovery/no recompute | private test `:615–617,643–645,687–691`; private support `:58–98` | Real workers are killed after durable phase handoffs; work-start receipts assert admission+processing initially, then only evidence+validation+readback for Latest, and readback for Captured completion. Native phase generation and typed table identity assertions independently reinforce work receipts. |
| Latest refusal | private test `:668–684` | Producer advances after retained Latest readback, then stale_input refuses with unchanged consumer records and no selected result. No valid guard is weakened. |
| Values/contributors | existing `tests/related_refresh.rs:277–376`; private test `:692–703` | Independent Rust matching oracle includes exact native occurrence file/row pairs, duplicates and nulls. Shared check reads source values and evidence contributor pairs; deep verifier separately reconstructs the release. |

Accepted scope is `03-schema-backfill.md:17,19,59` and `05-dependent-refresh.md:15,21,33,37,49`: material work can remain qualified across a policy change while the affected promotion decision executes. This test proves one supported policy boundary, not all material or validator invalidation.

## Invariants and limits

Original producer files alone are removed (`private test:611–613`); authoritative retained bytes remain. Both operations use the same consumer and material inputs before selection, avoiding the unsupported unbased-refresh-on-selected-output transition. After Captured selects, the pending unbased Latest operation would also face stale_base (`refresh.rs:435–440`); the test correctly proves stale_input before that separate fence and does not claim subsequent refusal precedence.

The helper retains a finite fixture's actual work receipts and keeps the existing 30-second deadline, kill/wait, and SIGKILL assertion. Runtime success and emitted phase order remain parent-owned qualification.

## Validator assessment (separate next implementation)

Current scalar validation key uses prepared/material digest (`backfill/preparation.rs:376`); ValidationCompletion lacks independent validator identity (`backfill.rs:85–97`). Key-only versioning would miss unselected readback reuse: `backfill.rs:654–680,942–1027` adopts a retained target before preparation/validation lookup. Completed results return first (`:535–539`) and must retain exact acknowledgment.

The smallest coherent next slice is one built-in validator identity and one current acknowledgment helper shared by prepared-target reuse and unselected-readback adoption. Bind exact source/output, original origins and field interpretation; preserve material generations/protections and original records. Missing/legacy validator identity is unknown. Missing current acknowledgment runs the existing routine verifier before promotion, without recomputation. Routine checks native schema/field mapping and admitted table inspection (`backfill/verification.rs:530–559`); full payload reconstruction is deep-only and should not be claimed for this slice.

## Executed evidence

The same-consumer policy case passes in
`schema-policy-progress-focused-2026-10-11.log`, including both real killed workers
and the final exact values/contributors. That invocation failed only in the
separate comparison fixture's expected refusal-code assertion.

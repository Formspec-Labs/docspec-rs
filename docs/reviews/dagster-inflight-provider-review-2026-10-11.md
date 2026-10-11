# Dagster in-flight provider recovery review

VERDICT: APPROVE for root-owned opt-in execution. Static review only; no builds, provider requests, secret resolution or test execution. Frozen patch SHA256 41d5d20ca412d7ab344d825bac95ed6897d5a96f13700283818670e6bdd8c0ce; proposed tree `/tmp/docspec-dagster-inflight-provider/new`.

No actionable findings. The runner tests an unfinished acknowledged processing handoff, rather than disguising a completed acknowledgment as recovery. All observations below are code traces; runtime remains pending.

## Function trace

| Function | Proposed file:line | Input/output and checked behavior |
| --- | --- | --- |
| main preflight | experiments/dagster-inflight-provider.py:52,66,175 | Requires prior passed retained provider receipt, exact synthetic namespace/prefix, selected update-two manifest, source-only independent shape and absence of fixed new operation/request/readback/result before new effects. |
| http / records / plan | same:36,137,140 | Loopback-only requests, finite transport timeout,32MiB metadata response bound; actual Update plan supplies exact reusable key/revision. |
| host | same:119 | Owns two explicit Rust processes and logs; test pause env only on first; replacement uses fresh distinct local host directory and same resolved config/executable. |
| ObservedResource.submit | adapters/dagster/tests/native_worker.py:19 | Real Dagster submission records waiting before HTTP call; uncertain retries0; atomic report updates permit distinguishing waiting death from learned/completed response. |
| processing capture / kill | runner:224,239 | Exact operation/phase, positive native revision, removed/added3/3, keyID2, retained old base; no readback/result and old authority remain. Waiting Python report/PID and both live processes precede exact SIGKILL/reap. |
| _worker | adapters/dagster/tests/test_rust_integration.py:464 | Existing native worker path with default60 preserved and opt-in240 transport timeout; catches timeout or BaseException (including outer alarm), kills/reaps owned child. |
| recovery | runner:252,258,266 | Same plan origin and raw PhaseResult before resubmit; exact request digest and completed summary after new Python worker; same processing native pin/record and parent manifest selected without original producer. |
| read_exact | runner:143 | Exact old/new HTTP source routes once each; byte/row bound; independent literal five-row duplicate/null/empty/private-canary oracle; field order/native IDs1/2/3/types/nullability and exact version/manifest response headers. |
| final work / cleanup | runner:277,289 | First log processing1/no validation/readback; second no admission/processing, validation1/readback1. Only owned processes stopped/reaped; replacement graceful shutdown required. Retains failure receipt/logs/local roots/all remote objects. |

## Safety and state reasoning

The fixed operation cannot silently rekey after unknown outcome. The old selection must match the prior exact manifest before writes. The sole external file is a freshly generated sub64KiB producer within the new evidence directory; its exact local path/no-symlink guard precedes unlink. Remote admitted input/native outputs remain intact. Killing the waiting Python request first cannot release the native validation pause; the marker is not released until cleanup after the deliberately killed Rust process. The replacement worker sees identical request JSON with the removed locator, so success plus unchanged processing origin/pin demonstrates actual retained recovery rather than producer reopen.

The existing work_started pause blocks before the validation work marker; hence the first log's no-validation assertion is meaningful. Actual native RunPlan records expose key/revision, and routine completion uses the processing TablePin, making the final exact table comparison valid. Current generic validator keying does not change the captured processing material key.

Credentials are passed only by configured environment references to Rust. Worker environments remove all DOCSPEC_* names; saved remote config contains names, not values. Failure receipt stores exception type rather than raw potentially sensitive response content. This does not assert secrets in arbitrary inherited non-DOCSPEC environments were comprehensively audited.

## Bounds and qualifications

300second outer alarm,180second processing wait,20second paused capture,240second worker submission,30second worker-exit allowance, bounded HTTP buffers and five decoded rows are explicit. Cleanup has additional bounded waits after alarm cancellation. Local helper default timeout remains60; adding optional arguments does not narrow existing arbitrary request fixture shapes.

No new production API, fake lifecycle row, alternate writer, import or cleanup framework is introduced. Report accurately excludes request/whole-prefix-byte counts, throughput, power loss, unknown native-commit outcomes, two computers and remote destructive maintenance/restore. Provider execution remains a required independent qualification; static approval does not establish it.

## Runtime evidence and limits

The original static approval missed a Python name-shadowing defect and the fact
that inspection waits behind the paused native operation. Retain both failures:
`dagster-inflight-provider-2026-10-11/receipt.json` (preflight), and
`dagster-inflight-provider-fixed-2026-10-11/receipt.json` (capture timeout). The
second run's cleanup killed the actual Python worker and Rust host; readback was
retained before the host ended. Fresh inspection found `update-two` selected,
`readback/dagster-inflight-update` present and its `result/` absent. An initial
inspection helper import mistake also remains in the reconciliation stderr log.

The explicit follow-up `dagster-provider-recovered-2026-10-11/receipt.json` passes:
fresh host directory, identical request through a new real Dagster worker, original
input removed, unchanged retained phases/readback, no repeated phase-work events,
and exact independent Arrow values/types/field IDs/nullability for old/new versions.
This qualifies actual worker/host loss after retained readback before selection.
It does not qualify the initially intended earlier boundary, machine power loss,
concurrent processes, remote destructive maintenance, or remote throughput.

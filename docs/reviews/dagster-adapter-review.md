# Dagster adapter: independent static review

VERDICT: APPROVE for the initial local scheduling adapter. No blocking correctness
defect was found. This verdict does not qualify complete R7 operations, native
worker-loss recovery, hosted deployment, or performance.

Reviewed on 2026-10-09 against workspace HEAD
`d57042e00a43b821b9730ad688d94ad6d2e011f4` and the uncommitted
`adapters/dagster/` sources. The review used the semi-formal code-review and Rust
skills, repository instructions, the accepted plan, and R1/R7 recovery rules.
It was independent of author test outcomes. No implementation edits, tests,
builds, Cargo commands, formatting commands, or commits were performed.

## Findings

### F1 — Rejected responses should expose uncertainty consistently

- **Severity:** WARNING; nonblocking usability issue.
- **Category:** maintainability.
- **Location:** `adapters/dagster/src/docspec_dagster/definitions.py:54`;
  `adapters/dagster/src/docspec_dagster/client.py:230`.
- **Evidence:** Exhausted transport retries emit `outcome: unknown`
  (`definitions.py:49–53`). Unusable responses instead enter the generic
  `AdapterError` handler and emit only a description (`definitions.py:54–55`).
  Oversize/framing messages explicitly preserve uncertainty, but a mismatched
  plan/result acknowledgment says only that identity does not match
  (`client.py:234–242`), and a malformed refusal says to inspect before retrying
  (`client.py:192–193`). A response-validation failure after POST cannot establish
  that the native operation failed. The implementation does not claim that it
  failed, but downstream event consumers must infer the distinction from prose.
- **Suggestion:** Catch `ResponseError` separately and attach
  `metadata={"outcome": "unknown"}` with the same original-request recovery
  instruction. Preserve terminal automatic-retry behavior. Extend the native
  Dagster response-limit or mismatch test to check this metadata. This needs no
  new lifecycle state or Rust decision.

### F2 — Native failure recovery remains outside the written integration test

- **Severity:** OBSERVATION; qualification limitation, not a release blocker for
  the documented local adapter scope.
- **Category:** test-coverage.
- **Location:** `adapters/dagster/tests/test_rust_integration.py:79–95`;
  `adapters/dagster/tests/test_dagster.py:39–44`.
- **Evidence:** The actual Rust-host test executes `/run` successfully, then
  executes the same request again and compares the summary, selected pin,
  lifecycle records, and retained native/object hashes. The connection-loss
  retry test uses the HTTP fixture. No adapter test combines a lost native
  response or replacement Dagster worker with an actual Rust completion. No
  actual-host adapter test covers `/plan`, `/updates/run`, `/schemas/run`,
  `/backfills/run`, `/refreshes/run`, or `/exports/run`.
- **Meaning:** The tests are well separated: protocol assertions do not pretend
  to implement Rust recovery, and the native test checks real retained state.
  Passing them would establish the documented local build/replay slice, not
  R7-T05 worker-loss or provider qualification. The README already preserves this
  distinction (`adapters/dagster/README.md:98–121`). Keep that limitation in status
  updates; add a controlled native lost-response test when claiming that broader
  behavior.

### F3 — The plan acknowledgment is intentionally smaller than R7 status

- **Severity:** OBSERVATION; scope limitation.
- **Category:** maintainability.
- **Location:** `adapters/dagster/src/docspec_dagster/client.py:233–236`.
- **Evidence:** `/plan` returns only identity, `outcome: planned`, and `completed`.
  Rust also returns `reusable_phases`, `required_phases`, and `limits`
  (`src/engine.rs:142–151`). The adapter does not expose ongoing phase progress or
  the complete inspect/resume surface. R7 asks for reusable phases, remaining
  work, and resource conditions (`docs/requirements/07-operations-qualification.md:27–39`).
- **Meaning:** The adapter correctly distinguishes planning from execution and
  never invents phase status. Its small acknowledgment should not be described
  as completing R7 status. Operators still use the native plan/inspect interfaces
  for the detailed recovery explanation.

## Patch summary

The package adds a standard-library HTTP client, a native Dagster resource/op/job,
transport tests, Dagster execution tests, an opt-in Rust integration test, and
operating instructions. The Python side sends a caller-supplied native request
unchanged to an existing loopback host, schedules bounded uncertain-outcome
retries, and returns a small identity acknowledgment. It neither starts the
production host nor stores operation or phase truth.

Important callers are `DocSpecResource.submit` and `docspec_operation`
(`adapters/dagster/src/docspec_dagster/definitions.py:18–57`), the native job
(`definitions.py:60–63`), the README usage example
(`adapters/dagster/README.md:17–40`), and the reviewed tests. No other production
callers were found within the adapter.

This implements the boundary in `docs/PLAN.md:124–127`: Python carries identities
and bounded status while Rust owns lifecycle decisions. It also follows R7's
maintained-executor rule (`docs/requirements/07-operations-qualification.md:39`).

## Function trace

Paths in this table are relative to `adapters/dagster/` unless prefixed `Rust:`.

| Function or type | File:line | Inputs and outputs | Verified behavior |
| --- | --- | --- | --- |
| `AdapterError`, `ResponseError`, `ResponseTooLarge`, `UncertainOutcome` | `src/docspec_dagster/client.py:33–46` | Exception classification | Separates local/response rejection from retryable transport uncertainty. Response errors expressly allow prior native completion. |
| `RemoteRefusal.__init__` | `src/docspec_dagster/client.py:52` | HTTP status and bounded JSON object → exception | Retains complete refusal for direct callers; exception text contains only bounded code/status. |
| `_unique_object` | `src/docspec_dagster/client.py:60` | JSON member pairs → object | Rejects duplicate members at every parsed object level. |
| `_invalid_constant`, `_finite_float` | `src/docspec_dagster/client.py:69–77` | Numeric token → refusal or finite float | Rejects nonstandard constants and overflow to infinity. |
| `_json_object` | `src/docspec_dagster/client.py:80` | Bounded bytes → object | Requires UTF-8, unique members, finite numbers, and a top-level object. |
| `_identifier` | `src/docspec_dagster/client.py:92` | Any value → boolean | Matches Rust's ASCII identifier bounds (`Rust: src/model.rs:17–28`). |
| `DocSpecClient.__post_init__` | `src/docspec_dagster/client.py:109` | Connection/response configuration → validation | Requires a positive finite timeout and an integer response allowance from 1 byte through 32 MiB; rejects booleans for these direct-client settings. |
| `DocSpecClient._endpoint` | `src/docspec_dagster/client.py:125` | Base URL → literal loopback host/port | Rejects DNS names, non-HTTP schemes, credentials, non-root paths, queries, fragments, scopes, whitespace, and invalid ports. |
| `DocSpecClient.submit` | `src/docspec_dagster/client.py:149` | Allowlisted route and original JSON → small summary | Bounds/validates metadata, sends the exact encoded string through one HTTP connection, rejects redirects, classifies response/errors, and always closes the connection. It makes no internal retry. |
| `DocSpecClient._read_bounded` | `src/docspec_dagster/client.py:204` | HTTP response → at most configured body bytes | Checks encoding and framing, refuses declared oversize before reading, reads at most limit + 1 bytes, and detects oversize and short declared bodies. |
| `DocSpecClient._summary` | `src/docspec_dagster/client.py:230` | Route, submitted IDs, decoded body → acknowledgment | Checks returned IDs. `/plan` requires an actual boolean and remains `planned`; run routes identify the completed result. Full pins and inventories do not escape. |
| `DocSpecResource.submit` | `src/docspec_dagster/definitions.py:18` | Dagster resource settings and request → acknowledgment | Constructs the transport client without creating engine or lifecycle state. |
| `OperationConfig` | `src/docspec_dagster/definitions.py:22` | Dagster route/request configuration | Keeps caller-selected route and native request separate from Dagster run and attempt identities. |
| `docspec_operation` | `src/docspec_dagster/definitions.py:30` | Context/config/resource → metadata and output | Explicit refusals and adapter errors stop retries; uncertain outcomes request a bounded Dagster retry; exhausted uncertainty remains unknown. |
| `docspec_job` | `src/docspec_dagster/definitions.py:61` | Injected resource/config → one native op | Adds no scheduling state or lifecycle decisions. |
| HTTP `plan`, `run`, `update`, `evolve_schema`, `backfill`, `refresh`, `export` | `Rust: src/http.rs:377–394,449–465,515–533` | Native request types → `RunPlan`, `ReleasePin`, or `ExportPin` | Delegates to the corresponding engine method without a second Python translation. |
| `Engine::plan_request_admitted` | `Rust: src/engine.rs:277` | Native request → actual `RunPlan` | Validates request identity and native availability; reports retained completion and required/reusable phases. |
| `Engine::run_admitted`, `prepare_and_select` | `Rust: src/engine.rs:776,1032` | Native request → release | Fingerprints requests, reconciles matching committed results, and looks up existing results before preparation. Conflicting identity refuses. |
| `Engine::backfill_admitted`, `export_admitted` | `Rust: src/engine/backfill.rs:430`; `Rust: src/engine/export.rs:141` | Corresponding request → native pin | Retains native identity/reconciliation and phase reuse. Export returns its original result before new materialization. |
| `Engine::admit_work`, `spawn_work` | `Rust: src/engine/worker.rs:111,203` | Owned engine work → join result | Rust owns admitted work in a spawned task. Dropping the HTTP caller does not constitute operation abandonment. |
| `reply`, `server` test helpers | `tests/http_fixture.py:15,22` | Prescribed response sequence → loopback peer and captured requests | Emits length/chunked/EOF/disconnected responses. It does not simulate lifecycle decisions. |
| `DagsterTests.execute` | `tests/test_dagster.py:21` | HTTP peer and retry/bounds choices → native execution result | Uses actual `execute_in_process`, resource injection, and configured caller retry policy. |
| Integration `_wait_ready`, `_get`, `_retained_files`, `_tail` | `tests/test_rust_integration.py:107,119,132,150` | Real host/workspace → readiness, bounded reads, hashes, diagnostic tail | Operates only within the opted-in temporary fixture. Native/object hashes provide evidence beyond equality of returned summaries. |

## Data flow and invariants

1. **Native identity originates with the caller.** `request_json` is encoded once
   per attempt, decoded only for shape and IDs, and the original `payload` is
   sent (`client.py:157–177`). The Python code does not reserialize it, substitute
   a Dagster run ID, calculate a new base, or synthesize a request after failure.
   Dagster re-enters the op with its configured string
   (`definitions.py:22–33,45–48`). Rust owns request fingerprint comparison
   (`src/engine.rs:1038–1060`; `src/engine/export.rs:536–542`).

2. **An uncertain request is retried without deciding eligibility.** Network and
   HTTP transport exceptions become `UncertainOutcome` (`client.py:198–200`).
   Truncated replies and unstructured server errors use the same category
   (`client.py:186–195,226–227`). Native structured refusals, including HTTP 503
   capacity responses, become terminal `RemoteRefusal` (`client.py:189–196`;
   `src/http.rs:319–359`). Rust rechecks mutable eligibility on the next admitted
   request; the adapter does not alter the request to make it pass.

3. **Dagster retry policy cannot convert an explicit refusal into automatic
   retry.** `Failure(allow_retries=False)` is used on every terminal op branch
   (`definitions.py:35–55`). The installed pinned Dagster 1.13.16 implementation
   specifically rethrows this failure before applying a caller retry policy.
   Explicit `RetryRequested` is also handled before that policy.

4. **The successful summary fits the current Rust response shape.** `RunPlan`
   has flat `dataset`, `operation`, and boolean `completed`
   (`src/engine.rs:142–151`). `ReleasePin` carries the operation as `id` and a
   nested `DatasetPin` (`src/model.rs:167`; `src/engine.rs:1562–1567`;
   `src/engine/backfill.rs:754–763`). `ExportPin` carries flat `id` and `dataset`
   (`src/export.rs:74–78`; `src/engine/export.rs:191–195`). No nonexistent
   `RunPlan.result` field is assumed.

5. **Only bounded metadata crosses the adapter boundary.** Requests have a
   1 MiB character precheck and UTF-8 byte check, matching the Rust router's body
   limit (`client.py:157–162`; `src/http.rs:105`). Responses default to 1 MiB and
   have a configurable ceiling of 32 MiB (`client.py:15–18,118–123,204–228`).
   Success output uses validated IDs and constant keys/values only. Refusal event
   messages are capped at 2,048 characters with a truncation flag
   (`definitions.py:37–42`). These are serialized metadata limits; parsing and
   Python object allocation add memory overhead. No whole-process memory or
   elapsed-operation bound is established.

6. **Underlying exception detail does not bypass the intended event summary.**
   The op raises Dagster failures/retries `from None` (`definitions.py:44,48,53,55`).
   Pinned Dagster's error serializer checks `__suppress_context__` before including
   exception context. Therefore the transport cause chain is suppressed at the
   Dagster boundary. Direct client callers still receive the chained cause, which
   is consistent with the distinction between direct diagnostics and event output.

7. **Dataset truth remains in Rust.** There is no adapter database, stored phase
   ledger, input selection, publication pointer, deletion route, or result cache.
   The allowlist excludes retention/removal/restore (`client.py:19–28`). Rust
   retains phases and owns admitted work (`src/engine.rs:823–863`;
   `src/engine/worker.rs:128–133,217–228`). An op/report failure cannot delete or
   abandon completed native work through this adapter.

## Test behavior and concrete edge cases

These are static expectations from the assertions and implementation, not test
execution results. All test functions were read. Their expected behavior is
consistent with the reviewed paths; no test failure is predicted by this review.

| Tests and location | Trace and expected assertion | Evidence limit |
| --- | --- | --- |
| `test_preserves_exact_native_request_and_returns_only_small_identity_summary`, `test_plan_and_export_acknowledgments_keep_the_same_identity`, `test_wrong_acknowledgment_refuses_without_claiming_completion` — `tests/test_client.py:20–42` | Captured bytes remain exact; plan/export branch output is small; mismatched operation or dataset raises `ResponseError`. | The peer returns protocol fixtures, not retained results. |
| `test_bounded_success_with_length_chunked_and_no_length`, `test_excess_response_refuses_for_every_framing_mode`, `test_large_declared_length_refuses_before_waiting_for_payload` — `tests/test_client.py:44–59` | The same body limit applies with length, chunking, and EOF framing; declared excess refuses before body read. | Serialized response bounds, not peak process memory. |
| `test_partial_response_and_disconnect_have_unknown_outcome`, `test_unstructured_server_failure_is_uncertain` — `tests/test_client.py:61–66,78–81` | Short or missing replies and a non-JSON 502 become `UncertainOutcome`. | No native completion is involved. |
| `test_explicit_refusals_preserve_code_message_and_status_even_for_503`, `test_malformed_refusal_is_terminal_even_when_server_status_is_500` — `tests/test_client.py:68–86` | Structured 409/503 bodies retain refusal fields; malformed codes remain terminal response errors. | Does not claim a refused attempt erased earlier phases. |
| `test_redirect_is_never_followed`, `test_ambiguous_framing_and_content_encoding_refuse`, `test_response_json_must_be_unique_finite_and_an_object` — `tests/test_client.py:88–108` | No redirect follow-up; duplicate length, encoding/framing ambiguity, duplicate JSON members, non-finite numbers, arrays, and invalid JSON refuse. | The actual Rust router normally emits well-formed JSON. |
| `test_url_scope_is_literal_loopback_without_dns_credentials_or_paths`, `test_invalid_requests_never_reach_http`, `test_invalid_capacity_and_timeout_refuse_at_construction` — `tests/test_client.py:110–140` | Invalid endpoints/requests/settings fail before HTTP construction; UTF-8 byte excess is caught. | IPv6 endpoint syntax is accepted statically; no IPv6 listener test is provided. |
| `test_native_execution_emits_only_identity_metadata` — `tests/test_dagster.py:28–37` | Actual Dagster output metadata has the same small keys as returned output; unrelated response inventory is absent. | Uses the protocol peer. |
| `test_uncertain_response_retries_exactly_the_original_request` — `tests/test_dagster.py:39–44` | A disconnect triggers one retry event and byte-identical resubmission before success. | Does not drop a real Rust response; see F2. |
| `test_explicit_refusal_overrides_the_callers_retry_policy`, `test_exhausted_unknown_outcome_does_not_become_a_claim_of_rust_failure` — `tests/test_dagster.py:46–62` | Native Dagster failure handling overrides the caller policy and preserves unknown outcome at retry exhaustion. | Runtime execution still belongs to the parent task. |
| `test_large_refusal_message_has_an_explicit_event_truncation_flag`, `test_response_limit_is_terminal_even_with_a_caller_retry_policy` — `tests/test_dagster.py:64–76` | Event messages cap at 2,048 characters; oversize responses produce a single terminal request. | The latter does not assert unknown-outcome metadata; see F1. |
| `test_same_native_request_reconciles_without_more_retained_writes` — `tests/test_rust_integration.py:23–105` | Native Dagster executes against an actual Rust host twice; results, lifecycle state, selected pin, and retained file hashes must remain unchanged; shutdown must complete without forced kill. | Opt-in, clean `/run` replay only; skipped without both supplied paths or without Dagster. No result was inferred from the test's existence. |

The adapter adds little mechanism beyond request validation, response bounds,
small acknowledgments, and Dagster's own retry controls. Reusing `http.client`
avoids another HTTP dependency and bypasses proxy environment settings by using a
literal loopback connection (`client.py:170–177`). The transport tests remain
usable without Dagster, while native tests use the actual pinned executor.
These are simplicity observations, not measured speed or resource improvements.

## Framework evidence and review identity

Installed source inspected, without running the interpreter:

- Environment:
  `/Users/mikewolfd/.cache/uv/archive-v0/h1RKfXWViF4CHEhlYTQUF/lib/python3.13/site-packages/`.
- `dagster-1.13.16.dist-info/METADATA:3` identifies the pinned version.
- `dagster/_core/execution/plan/utils.py:66–84` confirms explicit retry handling
  and `Failure.allow_retries` precedence.
- `dagster_shared/error.py:75–90` confirms suppressed-context serialization.

SHA-256 of the reviewed production package inputs:

| File | SHA-256 |
| --- | --- |
| `src/docspec_dagster/__init__.py` | `feac56575d20fc95f1c69308250f3b1d79e776c3df4704ae9ee4a3670e726e2d` |
| `src/docspec_dagster/client.py` | `e954f4ff9abf8874938e27b329e9755a355f2e5b22cd2c1f48b757a69809c5d8` |
| `src/docspec_dagster/definitions.py` | `c04c30d1e52100d908468c4ca098ab27985b6ef3df6de23e38fdfe075c370cb0` |
| `pyproject.toml` | `45bdcdb17448c397e0309847814702aa3d9ca035c0b4eb1bb1c4d8ee2138c478` |

## Conclusion

VERDICT: APPROVE.

- The patch achieves its stated thin, local scheduling intent: exact request
  bytes and native identities survive retries; explicit refusals stop them;
  response handling and emitted metadata have explicit size limits; current
  Rust response types match; lifecycle decisions stay in Rust.
- Coverage of changed adapter paths: ADEQUATE for the stated transport and
  scheduling slice by static inspection. Coverage of native interrupted recovery
  and the complete R7 acceptance scope: INSUFFICIENT, explicitly outside this
  verdict. The parent task owns test execution and its reported results.
- Confidence: HIGH in the reviewed source-level identity and boundary behavior;
  MEDIUM in runtime qualification until the parent records actual native test
  outcomes. F1 is a small, nonblocking improvement to operational clarity.

## Author follow-up and runtime evidence

The nonblocking F1 improvement is implemented: `ResponseError` now produces
`outcome: unknown` and original-request recovery guidance without automatic retry.
The native Dagster tests cover both oversized and mismatched responses. The
[focused suite](dagster-adapter-check.log) passes with Python 3.13 and Dagster
1.13.16, including the real Rust host build/replay fixture. This is author-run
evidence, separate from the independent static review above. F2/F3 remain the
documented qualification and status limits.

# Dagster completion-response recovery review

**Result: APPROVE. Local qualification is ready for the reviewed scope.** No
actionable correctness defect was found. Independent static tracing supports the
fault boundary and assertions; the parent-collected focused run includes the new
native cases and ends successfully (`dagster-recovery-check.log:672-761`). This
review does not claim the repository gate or broader provider recovery passed.

Reviewed the working diff against `ce3ad6b` under `adapters/dagster/`, including
the untracked `tests/native_worker.py`. The reviewer performed static reads and
wrote this report only: no tests, Rust commands, hashes, installation or commits.
The implementation explanation was read after deriving the request/recovery
trace. Repository requirements remain authoritative: completed work survives
worker replacement, uncertain completion reconciles its stable identity, and
Rust owns lifecycle decisions (`docs/requirements/01-durable-storage.md:7-9`,
`docs/requirements/01-durable-storage.md:36-42`, `docs/PLAN.md:124-127`).

## 1. Patch summary

The production change adds `/receipts/run` to the existing allowlist
(`adapters/dagster/src/docspec_dagster/client.py:19-29`). It reuses the same bounded
transport, request identity checks and release acknowledgment. The test changes
extend the existing temporary native-host fixture with a lost-response relay and
a separate Python worker helper. The README states the resulting local failure
boundary and its limits (`adapters/dagster/README.md:114-154`).

Important callers are the Dagster op through `DocSpecResource.submit`
(`adapters/dagster/src/docspec_dagster/definitions.py:18-19,30-33`), native test
entry points (`adapters/dagster/tests/test_rust_integration.py:27-36`), and the new
worker process (`adapters/dagster/tests/native_worker.py:11-24`). No Python
lifecycle store, native processing implementation, or production retry mechanism
is added. This preserves the small adapter and reuses maintained Dagster and
standard-library transport facilities.

## 2. Function trace

Paths in this table are repository-relative. `integration` abbreviates
`adapters/dagster/tests/test_rust_integration.py`.

| Function / method | File:line | Inputs | Output | Verified behavior |
| --- | --- | --- | --- | --- |
| `DocSpecClient.submit` | `adapters/dagster/src/docspec_dagster/client.py:150` | Allowlisted route, original JSON string | Small summary or typed failure | Sends the original UTF-8 bytes; disconnect becomes `UncertainOutcome`; structured refusal remains terminal. |
| `_read_bounded` | `adapters/dagster/src/docspec_dagster/client.py:205` | Native HTTP response | Bounded bytes | Enforces response allowance, framing and completeness before parsing. |
| `_summary` | `adapters/dagster/src/docspec_dagster/client.py:232` | Route, original IDs, native response | Identity acknowledgment | Receipt results use the existing nested `ReleasePin.dataset.dataset` and result ID checks. |
| `DocSpecResource.submit` | `adapters/dagster/src/docspec_dagster/definitions.py:18` | Route, JSON, configured bounds | Client result | Constructs the same client for every attempt. |
| `docspec_operation` / `docspec_job` | `adapters/dagster/src/docspec_dagster/definitions.py:30,67` | Dagster configuration/resource | Result or Dagster retry/failure | Retries unchanged configuration only for uncertainty; exhaustion records `outcome: unknown`. |
| Native test entry points | `integration:27,30,33` | Replay, retry or replacement mode | Test assertions | Share one real-host setup and state comparison. |
| `_qualify` | `integration:36` | Explicit binary and tiny input paths | Native recovery qualification | Starts the host, performs the selected scenario, asserts exact requests/results/state, probes the new route, and requires graceful shutdown. |
| `_state` | `integration:161` | Native port and workspace | Latest pin, lifecycle records, files | Reads native authority and retained artifacts independently of the adapter summary. |
| `_worker` | `integration:165` | Original configuration path, relay URL | Bounded report | Starts a new interpreter, requires exit success and a report; kills and fails on deadline. |
| Worker entry point | `adapters/dagster/tests/native_worker.py:11` | Configuration, URL, report path | Success summary or unknown outcome plus PID | Runs the real Dagster job with zero retries; reports Dagster failure metadata without inventing native failure. |
| `_wait_ready` | `integration:184` | Host process and port | Ready or test failure | Checks that the process remains alive and binds within its deadline. |
| `_get` | `integration:196` | Readback path | Bounded parsed JSON | Requires HTTP 200 and closes each connection. |
| `_retained_files` | `integration:209` | Native workspace | Path-to-digest/size/mtime map | Compares retained native/object files and rejects an oversized fixture. |
| `_tail` | `integration:231` | Log path | Diagnostic tail | Bounds returned diagnostics; used on failed assertions and cleanup. |
| `lost_response_relay` / `Handler.do_POST` | `integration:238,241` | Native port, capture callback, original request | Dropped first reply, real second reply | Forwards bytes, reads a real Rust 200 response, captures state before the drop, and permits at most two requests. |
| `Handler.log_message` | `integration:273` | HTTP log arguments | None | Suppresses routine fixture request logging only. |
| Native `run` / `update_evidence` handlers | `src/http.rs:394,472` | `RunSpec` / `EvidenceSpec` | `Json<ReleasePin>` | Await the corresponding engine method; receipt route is registered at `src/http.rs:38`. |
| `Engine::run` / `update_evidence` | `src/engine.rs:745`, `src/engine/evidence.rs:201` | Native typed request | Retained release | Both enter the shared Rust execution path. |
| `run_internal` / `run_admitted` | `src/engine.rs:783,796` | Request and admission | Release or refusal | Run owned work, check operation entry, and reconcile a competing completed result when appropriate. |
| `prepare_and_select` | `src/engine.rs:1056` | Stable native request | Existing or newly selected result | Refuses a changed request identity, then returns a retained result before preparation (`1074-1085`). |
| `select_prepared` | `src/engine.rs:1718` | Prepared result | Selected result | Performs native selection before returning success (`1765-1767`). |
| Native inspect/latest | `src/engine.rs:430,450` | Dataset ID | Lifecycle records / selected release | Read the guarded native state; do not create an adapter recovery record. |

The installed Dagster source was inspected directly under
`/Users/mikewolfd/.cache/uv/archive-v0/h1RKfXWViF4CHEhlYTQUF/lib/python3.13/site-packages/`.
Its `_core/execution/plan/utils.py:66-84` preserves explicit `RetryRequested` and
honors `Failure(allow_retries=False)`. Its
`_core/execution/plan/execute_plan.py:173-179,245-303` carries user failure metadata
and emits the retry event used by the assertions. No library behavior was inferred
from the exception names alone.

## 3. Data flow and invariants

- **Exact replay:** `_qualify` creates `request_json` once and places that string
  inside the Dagster configuration (`integration:81-89`). The worker configuration
  file preserves that string; the helper reads it unchanged (`integration:116-124`,
  `native_worker.py:12-16`). The relay records bytes actually received, and the test
  compares both attempts with the original bytes (`integration:125-128`). Changing
  the request or generating a new operation ID would fail these assertions.
- **Native completion precedes response loss:** the relay forwards to Rust, reads
  its successful response, stores the full decoded pin, captures native state,
  and only then closes the Python connection (`integration:252-262`). The second
  completion must equal the first full pin, while native latest, records and files
  must equal the captured baseline (`integration:128-136`). A fabricated adapter
  success alone cannot satisfy this path.
- **Uncertainty remains distinct from native failure:** the client converts the
  disconnected response into `UncertainOutcome` (`client.py:199-203`); the op either
  requests another attempt or emits unknown outcome (`definitions.py:45-53`). The
  first worker must fail with that explicit outcome before a separate successful
  process is accepted (`integration:118-124`).
- **Rust owns reuse:** `prepare_and_select` checks the retained request digest and
  returns `result/<operation>` before opening the processing path
  (`src/engine.rs:1074-1085`). Fixture configuration, PID reports and inventories
  remain temporary test evidence, not lifecycle authority.
- **Receipt route matches native schema:** `/receipts/run` accepts `EvidenceSpec`
  and returns `ReleasePin` (`src/http.rs:38,472-477`; `src/evidence.rs:26-33`). Its
  validation accepts the request's identity/digest shape before `check_base`
  refuses the absent selection (`src/engine/evidence.rs:17-31`,
  `src/engine.rs:1102-1104,2087-2097`). HTTP maps that refusal to 409
  (`src/http.rs:361-369`). There is no route or acknowledgment-shape mismatch.

Initial hypotheses were that the relay must prove real completion before dropping
the reply, replacement must use a new process with the original request, and the
new route must reach the existing native evidence service. All were confirmed by
the traces above before the author explanation was read.

## 4. Test behavior and edge cases

The reviewer did not execute tests. The parent-collected
[focused run](dagster-recovery-check.log) confirms the following cases ran and
passed, with no skips in that log; its terminal result is at lines 758-761.

| Test | Source | Assertion path and observed result |
| --- | --- | --- |
| `test_native_completed_response_loss_retries_without_retained_rewrites` | `integration:30` | Real completion is dropped, one actual Dagster retry occurs, exact bytes/full pins/state match. Passed at `dagster-recovery-check.log:672-732`. |
| `test_replacement_python_worker_reconciles_native_completed_response_loss` | `integration:33` | First process reports unknown and exits; another PID loads the same configuration and recovers the original result. Passed at `dagster-recovery-check.log:733`. |
| `test_same_native_request_reconciles_without_more_retained_writes` | `integration:27` | Existing direct replay also compares lifecycle/file state and runs the native receipt refusal probe. Passed at `dagster-recovery-check.log:734-756`. |
| `test_plan_and_export_acknowledgments_keep_the_same_identity` | `adapters/dagster/tests/test_client.py:27` | Added receipt response reaches the shared summary path and returns completed outcome. Passed at `dagster-recovery-check.log:10`. The assertion is transport coverage, not native receipt materialization. |

Relevant existing tests cover wrong result/dataset identities, disconnects,
structured refusals, oversized responses and exhausted uncertainty
(`adapters/dagster/tests/test_client.py:39-109`,
`adapters/dagster/tests/test_dagster.py:39-87`). Successful receipt processing and
its native recovery remain distinct Rust tests: they assert preserved source and
computed pins, exact receipt contents, and resume without repeating admission or
processing (`tests/evidence_updates.rs:229-303,675-696`). This review read those
assertions but did not run or requalify that Rust suite.

Concrete limits and failure behavior:

- Invalid relay status, excessive response, short request or state-capture error
  records a relay error; the parent requires an empty error list and two matching
  requests (`integration:125-128,245-269`). These paths cannot silently count as a
  successful qualification.
- Worker expiry kills the process and fails; host cleanup sends SIGINT and requires
  exit zero, with forced shutdown failing (`integration:150-159,173-181`). Relay
  teardown closes the server and checks thread exit (`integration:283-288`).
- The input/request, response and retained-file bounds are explicit fixture failure
  bounds (`integration:46,202-203,220-227,245-256`). Socket timeouts are per operation;
  in-process Dagster has no aggregate deadline, as the README explicitly states
  (`adapters/dagster/README.md:133-141`).
- File hashes, sizes and modification times detect retained output changes. They
  do not measure repeated input reads, transient scratch work, CPU cost or writes
  to SQLite runtime files (`integration:209-227`). The README limits its claims
  accordingly (`adapters/dagster/README.md:121-124`).
- The replacement test covers a worker that exits after failing to learn native
  completion. It does not kill the worker mid-operation, restart Rust, test multiple
  hosts, or qualify a remote provider (`integration:118-124,150-159`;
  `adapters/dagster/README.md:126-131,154`).

## 5. Findings

No actionable BLOCKER, WARNING or NIT was found within this patch's stated scope.

**OBSERVATION — qualification boundary.** The actual run supports post-completion
response loss, native Dagster retry and replacement Python reconciliation against
one surviving local Rust host. Successful receipt materialization through Dagster,
mid-operation worker killing, Rust-host loss and provider recovery remain separate
work. Evidence: the native scenarios and receipt refusal assertions at
`integration:104-149`; the explicit limits at `adapters/dagster/README.md:126-154`.
Preserve this distinction in status/release wording; no production-code change is
required by the observation.

## 6. Conclusion

**VERDICT: APPROVE**

- The patch achieves its scoped intent: it exercises actual native completion
  followed by an unknown Python outcome, recovers the exact request/result, and
  retains Rust as the sole lifecycle authority.
- Coverage of changed paths: **ADEQUATE for this local scope**, supported by the
  parent-collected execution log and independently traced assertions.
- Confidence: **HIGH** for the scoped behavior. Broader recovery and performance
  claims remain unqualified; no repository-wide gate result is implied.
- Qualification status: **READY** for the reviewed local completion-response and
  replacement-worker cases. The earlier pending status is superseded by the
  successful focused run, not by static review alone.

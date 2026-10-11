# Dagster in-flight recovery: blind static review

**Verdict after repair review: APPROVE.** F1 and F2 are closed. The native pause and host-kill ordering establish an unfinished Rust operation after retained processing; the repaired Python observation and live-host completion barrier make the intended interruption sequence explicit. No material findings remain in this scoped patch.

Reviewed the uncommitted changes to `src/engine.rs`, `adapters/dagster/tests/native_worker.py`, `adapters/dagster/tests/test_rust_integration.py`, and `adapters/dagster/README.md` against HEAD `42ab0c3fe705db45da41634cdf2460a7089abc8a`. Initial scoped diff SHA-256: `0e84c47b07c8301e0835e062902393db93ada710ebec612cd7b77f90ca1ffd2b`; repaired scoped diff SHA-256: `e0b2f133da1e0ac3f63acef4e3cd4523e5f214699ddd9ba7d100c852cf12da03`. No author implementation report was read before the initial findings. The reviewer ran no builds or tests. Concurrent comparison changes, committed backfill changes, and the excluded change-chain review were not reviewed.

## Repair review and closure

**F1 closed:** `ObservedResource.submit` wraps the actual maintained submission method. It atomically replaces the report with `waiting` and a monotonic timestamp before submission, and writes `failed` or `returned` before handing control back to Dagster (`native_worker.py:18-35`). This removes the earlier interval covering Dagster's failure handling. The worker's socket timeout is now 60 seconds (`52`). The parent includes startup, native inspection and inventory hashing in its 30-second interruption deadline, requires a recent `submitted_at`, and compares the unchanged waiting report immediately before SIGKILL and after reaping the killed process (`test_rust_integration.py:206-238`). The native marker proves this submission reached Rust's unfinished validation boundary. Together these checks address the original timeout/finalization false-positive path.

**F2 closed:** after releasing the pause in the surviving-host case, the parent waits for `result/stable_build` in the native dataset records before launching replacement (`test_rust_integration.py:262-271`). The original request has therefore retained its result before replay can begin, so the exactly-one phase assertions no longer depend on Python startup timing. The host-kill branch remains separate: old host SIGKILL is reaped before resume is touched (`240-253`), then a fresh host opens the same workspace without pause configuration (`255-261`). This preserves cold recovery coverage.

| Repaired function/path | Current evidence | Verified behavior |
| --- | --- | --- |
| `ObservedResource.submit` and its local `record` helper | `native_worker.py:18-35` | Original submission arguments pass unchanged to `super().submit`; atomic state writes expose submission completion/failure before Dagster handling. |
| Worker main | `native_worker.py:38-61` | Original identity/digest retained; observed resource uses a longer socket timeout; final report is consumed only after process exit. |
| `_recover_interrupted` | `test_rust_integration.py:193-291` | Deadline and waiting-state checks precede actual SIGKILL; surviving host completes before replacement; killed host resumes cold. |
| Preservation/replay assertions | `test_rust_integration.py:272-290` | Unchanged config and request digest; exact processing records, existing file identities, completed pin/lifecycle/inventory, and single processing/validation/readback entries remain required. |

The retained parent-run output reports both actual-kill tests as `ok` at `docs/reviews/dagster-inflight-tests-2026-10-10.log:672-673`, with the suite ending in `OK` at line 763. I inspected that evidence; I did not independently execute it or establish the executable's build provenance from this log. The static review and supplied execution evidence support these named local interruption boundaries. They do not establish arbitrary-instruction crashes, power loss, hosted providers, or concurrent active replacement execution.

## Initial review record — findings resolved above

The initial findings, function trace, and line references below describe the initial diff snapshot and are retained to explain the repairs. They are not outstanding findings against the repaired snapshot.

### F1 — P2 / WARNING / test-coverage: “started” does not establish a waiting HTTP call

**Location:** `adapters/dagster/tests/native_worker.py:23-35`; `adapters/dagster/tests/test_rust_integration.py:213-231`.

The worker writes `started: true` before `execute_in_process` and replaces that report only after the whole call returns. The parent equates an unchanged report plus a live process with still awaiting the native response. Those observations also fit a worker whose HTTP request has already timed out but which has not yet completed Dagster failure handling and returned to the reporting code.

The socket timeout is 30 seconds (`native_worker.py:26`; `client.py:172-201`). After reaching the native pause, the parent performs a separate HTTP inspection with a 30-second timeout and hashes the retained inventory before killing the worker (`test_rust_integration.py:217-228,312-343`). There is no elapsed-time check covering that interval. `UncertainOutcome` is handled inside the Dagster op before `execute_in_process` returns (`definitions.py:45-53`). A slow or descheduled worker can therefore have learned an uncertain outcome while its report still says `started`; killing it then can satisfy the current assertions. The native pause still proves Rust was unfinished, but the README's stronger “while it still waits for the response” statement (`README.md:133-136`) is not established.

**Fix:** Give the interrupted worker a request timeout longer than a single enforced parent deadline covering pause, inspection, inventory, and kill. Also record request completion/failure at the submission boundary, before Dagster teardown, and reject a terminal submission observation. Keep the parent deadline below the native pause watchdog. Alternatively, narrow the claim explicitly to killing a live Python process before its overall job report completes; that is weaker qualification.

### F2 — P2 / WARNING / correctness: live-host single-pass assertion depends on scheduling

**Location:** `adapters/dagster/tests/test_rust_integration.py:240-255,271-274`.

For `workerkill`, the `finally` block releases the existing Rust worker and the parent then launches a replacement Python worker immediately. No barrier establishes that the original Rust operation has completed. Its owned task survives HTTP caller loss (`src/engine/worker.rs:120-142,212-237`), so the two native requests can overlap.

An overlapping replacement can observe no validation completion at `src/engine.rs:1305-1318`, enter validation at line 1320, and likewise enter readback before the other request retains its readback (`1580-1603`). The existing engine deliberately permits competing attempts and preserves the first completed generation (`1854-1890`); it does not serialize the full operation. The marker only pauses its first creator (`2279-2296`). Thus a correct competing-attempt execution can produce two validation or readback work entries, failing the new exactly-one assertion. Python startup will often give the first worker enough time to finish, but it is not a synchronization guarantee.

**Fix:** If this case is intended to qualify caller death followed by reconciliation of the surviving host's result, release the pause and wait with a bounded poll for that exact operation's completion before starting the replacement. Retain the separate host-kill case to establish cold recovery from processing. If replacement during active native work is intended, make the overlap deterministic and use explicitly defined competing-attempt expectations; do not implicitly require new execution deduplication from this fixture.

## Patch summary and function trace

The patch adds a feature-gated pause before a named native work phase, a Python process identity/request digest report, and real-host cases for Python SIGKILL and combined Python/Rust SIGKILL. The intended user result is recovery of the unchanged operation without losing or repeating completed processing.

| Function or path | Evidence | Inputs → outputs and verified behavior |
| --- | --- | --- |
| New test entry points | `test_rust_integration.py:37-41` | Select `workerkill` or `hostkill` in the shared fixture. |
| `_qualify` | `test_rust_integration.py:43-184` | Bounded fixture input → isolated workspace, private removable source, configured host, exact `/run` request, recovered state and cleanup. Existing modes clear pause variables. |
| `_host` | `test_rust_integration.py:186-191` | Exact executable/workspace/limits/port/environment → tracked Rust child with file-backed logs. |
| `_recover_interrupted` | `test_rust_integration.py:193-275` | Stable saved config → paused native state, actual killed Python child, optional killed/reopened host, replacement and completed replay checks. Findings F1/F2 limit its evidence. |
| Python worker main | `native_worker.py:12-35` | Saved config → PID, route, request digest and final Dagster outcome; the initial report spans the entire job call. |
| `work_started` | `src/engine.rs:2270-2301` | Phase/environment → first-creator pause, bounded wait, then phase-entry trace. With no matching pause variable, only the existing trace remains. |
| HTTP/run admission | `src/http.rs:545-549`; `src/engine.rs:664-667,702-712`; `src/engine/worker.rs:212-237` | `/run` → owned native task → release pin. Caller cancellation does not own the spawned operation's lifetime. |
| Processing/composition | `src/engine.rs:762-800,880-969,1225-1230,1305-1373` | Reuse retained processing or create and durably complete it; only then enter the validation pause. |
| Completed replay | `src/engine.rs:986-1010` | Fingerprint match plus existing result → exact retained result before source access or work-phase entry. |
| State/inventory helpers | `test_rust_integration.py:277-350` | Native latest/records plus retained files → exact pin, lifecycle and hash/size/mtime comparison, with explicit metadata/file bounds. |

## Data flow and invariants

- The request is constructed once, saved once, hashed by both workers, and compared with its original config bytes after replacement (`test_rust_integration.py:101-109,194-196,255-260`). The adapter submits the same UTF-8 request bytes (`client.py:161-178`); the engine checks operation identity against its fingerprint (`src/engine.rs:986-1004`). No second lifecycle store is introduced.
- Processing is committed before the pause (`src/engine.rs:937-967,1225-1230,1320`). The parent captures processing records, absence of a selected result, and retained files before SIGKILL, then requires exact record and file preservation afterward (`test_rust_integration.py:217-239,261-265`). Deleting only the private original locator tests independence from that locator (`60-65,232`).
- Host death precedes resume-marker creation: `kill` → bounded `wait` → assert SIGKILL → `finally` touches resume (`233-246`). The new host opens the same workspace with pause variables removed (`248-254`). This ordering cannot release validation on the old host before the intended kill.
- Completed replay compares the full native pin, lifecycle records and retained-file inventory, not just Dagster's identity summary (`261-274,277-279`). Work traces independently detect phase re-entry. They do not measure all reads, CPU work, power-loss durability, or arbitrary-instruction crashes.
- The pause and its call sites are under `test-util`; Cargo has no default feature enabling it (`Cargo.toml:31-32`; `src/engine.rs:1319-1320,2270`). The CLI's Tokio runtime has other worker threads available while this test hook blocks one (`src/main.rs:309`). These are source facts, not a production-build or runtime verification result.

## Test behavior, failure paths, and limits

The host-kill test should reach cold processing reuse and then validation/readback under the traced engine paths; only execution can confirm that. The live-host case has the scheduling-dependent failure in F2. Both new cases have the waiting-state evidence gap in F1. Existing replay/response-loss modes run without pause configuration and retain their original completion assertions.

The new worker is killed/reaped in `finally` on assertion failure, and the pause is released so outer shutdown can drain native work (`test_rust_integration.py:240-246`). The last tracked host receives bounded graceful shutdown with forced kill on expiry (`174-184`); replacement workers have bounded waits (`281-298`). Readbacks, fixture source size, retained inventory and native pause have explicit bounds (`53,312-343`; `src/engine.rs:2285-2293`). These are fixture failure bounds, not aggregate process-memory or performance results. If restart itself fails before a replacement host is tracked, the outer zero-exit assertion on the intentionally killed old host can mask that failure's primary message; this is a diagnostic limitation, not a success path or retained-process leak established here.

No runtime failure frequency is inferred for F1/F2. Dagster internals were not available in the inspected local adapter paths; F1 follows from the visible placement of the report writes and timeout handling, without relying on a particular teardown duration. Hook misconfiguration/watchdog paths are not exercised by the new tests; they fail rather than establish recovery.

## Conclusion

**VERDICT: APPROVE after repair review.** F1 and F2 are resolved by explicit submission observations, interruption deadline checks, and a native completion barrier. Native kill ordering, stable operation identity, completed-processing preservation, feature gating, and bounded normal cleanup remain supported by the code traces. Coverage is **ADEQUATE for the named local processing-boundary cases**. Confidence: **HIGH** in the scoped review; execution evidence is the inspected parent-run log rather than a reviewer rerun. The README's pending-execution/review status paragraph can now be refreshed to point to that evidence and this closure.

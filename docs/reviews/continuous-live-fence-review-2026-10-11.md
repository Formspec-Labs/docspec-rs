# Live continuous-fence worker death — static certificate

**Verdict: APPROVE. No actionable findings.** Reviewed patch SHA256 `77d48f7d2328499cd5df394d8d4672f400b2fab82825da55fa000199040c68e2` and the complete proposed test/controller sources. No runtime, Cargo, formatting, provider calls or shared edits performed.

| Boundary | Source trace | Finding |
| --- | --- | --- |
| Durable fence before interruption | `backfill/continuous.rs:270–276` changed hook; `intake.rs:147–161` | Feature-gated pause executes only after native fence commit succeeds and before pending-source drain. Normal builds contain no new hook. |
| Reachable pending work | proposed `continuous_backfill_process.rs:373–399`; `backfill.rs:974–1005`; `continuous.rs:263–290` | Source Update has acknowledged processing but no selection. First optimistic child may retain readback, then final cutover refuses pending source; next round acquires the fence. Fixture asserts that actual child/readback exists. |
| Actual process death | proposed test:401–425 | Fresh CLI holds exclusive workspace custody; marker wait is bounded to30s within300s lease. Competing Engine must return workspace_busy, then SIGKILL status must be9. RAII child guard prevents a test failure leaving a worker alive. |
| No stale live authority | proposed test:426–439; `intake.rs:35–43,70–80`; `worker.rs:25` | Fresh host has a new instance identity. Saved fence can remain closed=false but cannot fence that host. Pending source and prior selected sibling remain explicit. |
| Recover required work | proposed test:440–454; `tests/support/mod.rs:102–147` | Producer files are removed only after admission. Fresh CLI finishes accepted source with no admission/processing events, then continuous catch-up selects a different exact child tied to the new source/boundary. CLI waits remain bounded. |
| Exact values and retained history | proposed test:455–478; existing `assert_output`:44–137 | Literal duplicate/null/Unicode expectations, exact source/computed occurrence coverage, sibling values, deep verification, saved completion JSON and native phase generations are checked. |
| Completed retry | proposed test:479–482 | Fresh CLI returns exact selected pin with no work events. |

The hook is absent unless test-util is enabled and adds no retention record or production controller branch. Existing exact-work assertions in earlier process tests do not cross this new fence hook: their resumed source is already settled before first optimistic cutover. The new test is registered through the existing integration module.

This closes one local Q2/UR-01 boundary if it passes: actual host death while its durable intake fence is still live, followed by fresh-process recovery. It does not establish power-loss durability, deadline-expiry behavior, distributed/provider fences, or zero recomputation for every unacknowledged attempt. Equality of retained generations supports preserved work; the explicit no-work claims are limited to source admission/processing replay and the final completed retry. Runtime result remains parent-owned; confidence medium until executed.

## Parent runtime closure

The actual SIGKILL/cold-recovery fixture passes in `continuous-live-fence-focused-2026-10-11.log` under its bounded resource wrapper. Original source/child completions, native generations and final literal values remain exact. The complete repository gate is recorded separately.

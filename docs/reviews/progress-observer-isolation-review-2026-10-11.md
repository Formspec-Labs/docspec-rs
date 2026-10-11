# Independent progress observer review

**VERDICT: APPROVE (static).** No actionable finding. Artifact /tmp/docspec-progress-isolation.patch SHA256 a005786471c003d19ecbc9e1193edbefb284d2270728f95c67de5ef15b960378. No builds or shared edits performed.

## Scope and trace

Only src/progress/tests.rs changes. Proposed source /tmp/docspec-progress-isolation-tests.rs:124–153 introduces a bounded current-executable child wrapper; callers at157 and251 are the two existing subscriber-sensitive observer tests.

| Function | Inputs/output | Verified behavior |
| --- | --- | --- |
| run_isolated124 | Exact full test name→bool | Parent reexecutes only that test; child receives exact marker and bypasses wrapper. Parent requires successful exit before returning true. |
| ChildGuard Drop130 | Owned Child | Kill+wait on normal return, panic or timeout; already reaped child causes harmless ignored errors. |
| held_writer…156 | Actual native writer and held stream | Existing row/byte counters, early progress, failure/cancellation/closed distinctions and secret-safe output assertions195–245 remain unchanged in child. |
| writer_close…250 | Actual native writer and quiet catalog writer | Actual files/rows, no native snapshot commit and observer close totals263–276, quiet catalog output283–288 remain unchanged. |

## Isolation/recovery invariants

The child is a separate process, not another thread or global subscriber in the same process. current_exe plus --exact full module-qualified name selects the real existing test. CHILD marker126–127 prevents self-recursion; parent cannot report success on failed/panicking child because exit status147 is asserted. Thirty-second deadline144–151 bounds the wrapper; RAII guard retains ownership through every failure. Inherited stdout/stderr retain child diagnostics and stdin is null. No production event, writer or subscriber implementation changes.

## Evidence and limits

Read pinned tracing-core0.1.36 callsite.rs540–570: when one retained dispatcher exists, Rebuilder::JustOne visits dispatcher::get_default on the current emitting thread. compute_interest490–506 caches that enabledness. The proposed separate process avoids unrelated parallel tests initializing the same observer callsites. This is a grounded explanation for the intermittent missing close observation, not a reproduced proof of its exact interleaving. Existing notes retain the full-gate observer failure; no production writer failure is inferred.

Test behavior: parent runs the original test body only through the selected child; child keeps both existing held-writer cases and all assertions. Coverage is adequate for this test-only isolation change by static trace. Parent must run focused/full gates; no runtime success claimed here. Confidence HIGH for recursion, lifetime and assertion preservation; MEDIUM for the unmeasured original race mechanism.

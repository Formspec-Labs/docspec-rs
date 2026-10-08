# Comments performance harness review

**Disposition: approve the corrected harness for the bounded pilot.** No remaining
material harness issue was found. This does not qualify the engine's performance,
payload-read avoidance, full acceptance target, or unfinished lifecycle behavior.

Reviewed on 2026-10-08 against repository `AGENTS.md`, the frozen Comments protocol,
and `experiments/comments-input/run.py`. The review used the semi-formal code-review
and test-hypotheses workflows. Repository HEAD at final inspection was
`0da368bf4a9d36e2162a4da2801f0d56d85c5195`.

Reviewed artifact SHA-256 identities:

- `run.py`: `23ae53a0c855aa55541bbd1090ec257ca6999b3462b2936695b5041d6fdaba23`
- `protocol.md`: `0709403e5072b9ac94393d3c94afc39c1522ec4fa19763a47eb2f9ebea56ec0d`

## Findings and resolution

| Original material finding | Correction verified in the final code |
| --- | --- |
| Recursive workspace scans blocked the one-second RSS/free-space safety loop. | `command` runs one inventory worker in the background; the main loop polls process-tree RSS and volume free space independently (`run.py:119–160`). Inventory duration and sampled peaks remain explicit. |
| Waiting for the `/usr/bin/time` wrapper after TERM could leave a TERM-ignoring child running when the wrapper exited first. | Final cleanup kills the private process group even after the wrapper exits (`run.py:161–177`). The tiny process fixture below confirmed termination. |
| Iterating every Parquet object in the returned pin did not independently establish current-snapshot membership. | `independent_readback` uses PyIceberg's snapshot scan planner, rejects delete files for this source-build probe, compares exact live-file membership and descriptor sizes, and reconciles native record counts with PyArrow footers (`run.py:217–257`). |
| Stable file identities were being used to support a broader read-avoidance claim than the measurements establish. | The pre-collection protocol amendment explicitly leaves payload-read avoidance unmeasured. Results distinguish `bounded_checks_passed`, `payload_read_avoidance_measured=false`, and `full_performance_acceptance_passed=false` (`run.py:285–286`). |

The retained protocol preserves its earlier criteria and explains each amendment.
The harness records before/after inventories, logical sizes, allocated blocks,
volume free space, raw commands/output/timing, source and executable identities,
and retained failure results. Workspace limits and free-space stops match the
bounded pilot scope. Input preparation, build caches, prior history, and unfinished
retirement remain separate from timed workspace growth.

## Tiny fixture evidence

The reviewer did not run Cargo or the large benchmark. Temporary fixtures imported
the harness with a minimal `psutil` stub and injected a synthetic RSS threshold.
They used the real `/usr/bin/time`, a private process group, and a Python child
that ignored TERM. Fixture files were temporary; any surviving child was explicitly
killed. These are control-flow tests, not real RSS or filesystem benchmarks.

Before correction, the stop path returned while the child remained alive:

```json
{"term_ignoring_child_survived_harness_stop":true,"wrapper_exit_code":-15,"stop_reason":"sampled process-tree RSS exceeded 24 GiB"}
```

After correction, a simulated two-second inventory scan left polling responsive,
the child terminated, and metrics were retained:

```json
{"rss_probe_interval_seconds":1.01,"simulated_inventory_seconds":2.0,"term_ignoring_child_still_running":false,"child_status":"gone","metrics_retained":true}
```

## Evidence and interpretation limits

The reviewer statically traced fresh creation, committed selection, independent
readback, unchanged operation, exact retry, and their failure paths. The independent
PyIceberg/PyArrow readback was not executed by the reviewer. Its checks cover selected
membership, schemas, footer counts, and bounded decoded shape; they do not establish
full value equality. Stable identities establish absence of the checked rewrites,
not absence of cached reads or hashes.

Sampled resource peaks are lower bounds. Scan duration can extend the nominal
inventory interval; short-lived peaks can be missed. Allocated blocks on APFS do
not prove unique physical extents, and whole-volume free-space changes include
unrelated activity. The protocol and emitted result fields disclose these limits.

During final review, the parent collected the first pilot. The reviewer inspected
its retained `run-20261008T103609/results.json`, `fresh.metrics.json`, and
`fresh.stderr.log`. Fresh execution failed before readback because a decoded batch
used 97,641,135 bytes against a 67,108,864-byte allowance. The failure retained
2,802,772,903 logical workspace bytes after 7.948 seconds; `/usr/bin/time` reported
1,483,898,880 maximum RSS bytes while sampled RSS missed that peak. The result
correctly leaves all passing/acceptance claims false. This failed pilot supplies
diagnostic evidence, not a performance pass; subsequent engine changes or revised
experiment criteria require their own review and retained evidence.

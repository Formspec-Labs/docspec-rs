# Exact oracle scratch comparison

Frozen before diagnostic collection on 2026-10-10. This investigates the
independent checker failure, not engine performance acceptance.

**Decision:** Can a maintained DuckDB setting make the complete exact checker
fit its existing resource limits before changing comparison logic?

**Observed failure:** The captured `e40be01` engine completed processing and both
exports. Its DuckDB 1.4.4 oracle failed at the signed full-value native join
comparison, refusing another temporary block at 31.9/32 GiB. Earlier shaped
Comments and nested Dockets/receipt checks passed. Source: the retained
`acceptance-export-layout-20261010.json` and referenced run logs.

**Hypothesis:** Fewer concurrent aggregation workers may reduce simultaneous
hash state and spill pressure enough to complete this unchanged checker. Failure
at the same bound weakens this explanation. It does not establish that every
spill cost comes from concurrency; wide repeated strings remain an alternative.

**Arms:** Historical original oracle, 12 threads, versus an exact captured copy
with only `SET threads=12` changed to `SET threads=4`. Preserve 18 GiB DuckDB memory,
24 GiB observed aggregate process RSS, 32 GiB scratch and all equality/type/
coverage/evidence checks. Capture the source difference and package versions.

**Cases:** All checks over the retained completed full 10-million-row workspace
from `comments-e2e-export-layout-20261010/20261010T103539-acceptance-13ca1824/run-01`.
Copy only bounded request/result/input metadata into a fresh diagnostic directory;
use a workspace symlink to the immutable retained outputs. Keep new scratch,
receipts, stdout/stderr and measurements separate. Do not run the engine again.
Repeating earlier checks avoids adding a partial-check/reuse mechanism solely for
this diagnosis and verifies the whole proposed checker behavior together.

**Held constant:** Input/output identities, SQL comparisons, DuckDB version and
all resource limits except worker count. One diagnostic, 600-second deadline,
32 GiB additional scratch plus 256 MiB metadata allowance, 2 GiB free-space reserve.
Use the existing process/disk monitor. Record workstation load and cache conditions;
no root tests/builds or other observed tests/builds/benchmarks during collection.
Read-only agent reviews may proceed. No OS cache clearing. Existing output size
is recorded separately because the monitor does not traverse the workspace symlink.

**Decision rule:** All exact checks must complete within diagnostic bounds before
adopting the smaller thread setting for a newly captured three-run acceptance
series. A timeout, mismatch, memory or spill failure is a failed diagnostic. A
pass is a bounded resource result over retained outputs, not fresh acceptance or
an isolated speedup. Keep every failure. If it fails, consider a second frozen
experiment using maintained exact dictionary encoding of repeated contributor
paths; do not raise budgets or substitute hashes/samples for equality.

DuckDB documents tuning thread count and choosing types for memory efficiency:
[official workload guidance](https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads),
[official performance checklist](https://duckdb.org/docs/current/guides/performance/my_workload_is_slow).
These support the experiment; they do not predict this workload's outcome.

Pre-launch correction: the first diagnostic driver tried to hash its imported
`__pycache__` directory and stopped before launching DuckDB. Retain its directory
and `oracle-threads4-diagnostic-20261010.log`; restrict capture hashes to files
and disable bytecode writes on the corrected launch. No workload ran and no
comparison result is attributed to this setup failure.

Scope clarification: current catalog selection checks ran in the historical
full engine attempt. This diagnostic rechecks the full oracle over exact retained
pins; it does not rerun catalog selection or claim a fresh full lifecycle run.

## Result and adoption decision

The corrected diagnostic passed every exact oracle check in **164.19 seconds**.
It checked 10,000,000 joined Comments: 9,988,138 matched and 11,862 unmatched.
The previously failing native join comparison completed in 60.27 seconds; full
flat joined-output equality completed in 51.92 seconds. Sampled aggregate RSS
peaked at 23,345,152,000 bytes (21.74 GiB), and scratch at 32,681,856,400 bytes
(30.44 GiB). No bound or mismatch fired. Retained new diagnostic files are small;
the original 10.59 GB output workspace was reused. Its engine scratch was empty.

See `oracle-threads4-20261010.json`, the corrected raw log and its retained run.
The baseline was historical; this is a sufficiency result, not an isolated
speedup or a claim that every internal allocation has been explained. Scratch
still approaches the cap. **Adopt four active oracle threads** and retain the
exact existing comparison. Defer ENUM/keyed alternatives. Current catalog
selection remains evidence from the original full engine attempt, and full
performance acceptance still needs three newly captured fresh complete runs.

For the next acceptance series, use the unchanged `e40be01` Rust executable and
its successful corrected build receipt; only the captured Python checker and
its recorded thread setting changed. Rebuilding identical Rust is unnecessary.
Predeclare **12 GiB retained data per run** from the observed 10.59 GB, retaining
32 GiB shared scratch and 2 GiB free-space reserve. The three-run preflight needs
70 GiB free; fail admission if current free space cannot cover it. This tightens
the planning allowance and changes no data, equality, RSS, scratch or time limit.

Adoption validation: the synthetic Python harness and complete CLI/readback
fixture passed (`oracle-threads4-check-20261010.log`), and the repository
format/lint/test gate passed (`oracle-threads4-full-check-20261010.log`). The
independent implementation follow-up approves the exact two-line change: active
threads and its environment record. Rust source and executable are unchanged.

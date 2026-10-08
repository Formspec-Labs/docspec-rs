# Local retained-data reuse probe

Frozen before measurements on 2026-10-07. This is an implementation probe,
not the 10-million-Comments acceptance test or a comparison with Python DocSpec.

Decision: Does retained phase reuse avoid rewriting admitted inputs and native
tables, preserve values under an independent reader, and reduce complete-run time?

Hypothesis: A new operation over unchanged admitted inputs reuses their exact
object paths and the original native snapshot. A repeated completed operation
returns the same release without writing lifecycle or payload files. Unchanged
runs should be faster than fresh runs; otherwise investigate metadata/readback.

Arms: A fresh workspace; a new operation in that workspace over the same request;
an exact retry of that new operation. Same binary, SQL, limits and input bytes.
Each new operation still completes its own release readback. Exact retries reuse
that completed readback. No generation or original phase is relabeled.

Cases: The existing Bill Actions sample and its nested receipt table under
`../experiments/docspec-spicyregs-performance-20261007/inputs/bill-actions/`.
Check each source against its retained members manifest using SHA-256 before use.
These are retained small samples, not a representative Comments corpus. Run
`SELECT * FROM input`; compare output field types, nulls, values and multiplicity
with PyArrow reading the original Parquet. Row order is not promised by this query.

Held constant: Three fresh-workspace repetitions per case; Rust debug binary,
pinned Cargo.lock, local filesystem, default engine limits. Record OS, compiler,
binary/lock/protocol/harness digests, source identities, raw stdout/stderr, elapsed
time, process memory from macOS time, and retained-file inventories. Record failure
and stop that case rather than replacing it with an easier input. Inputs may be in
the OS cache; do not call these cold-cache results. Bound each command to 120 seconds.

Decision rule: Any incorrect field, missing duplicate, changed source, repeated
payload preparation, or different release on an exact retry is NOT SOLVED. Report
a bounded latency improvement only when all three paired unchanged runs are faster
and the median improves by at least 20%. Otherwise report NO MEASURED IMPROVEMENT
or TRADEOFF and use the phase/file evidence to locate the cost. Whole-process memory
must stay below 24 GiB. Do not extrapolate timings to 10 million rows.

The independent PyArrow comparison runs outside the Rust command timing and is
reported separately. The Rust run timing includes its native independent readback.
This probe does not qualify per-value evidence, meaningful ordering, all source
types, process-kill recovery, shared-memory limits under concurrency, maintenance,
remote publication, or full lifecycle conformance.

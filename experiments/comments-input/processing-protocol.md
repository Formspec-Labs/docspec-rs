# Comments batch and encoding diagnosis

Frozen after the retained capacity failure and before collecting these diagnostic
arms. This supplements, and does not replace, the failed full-path pilot.

**Decision:** Choose the smallest maintained reader/writer configuration that can
process the unchanged Comments input within its existing 64 MiB batch allowance
and avoid unnecessary disk expansion. Do not add a buffer-copy layer or raise
the global memory ceiling without evidence that configuration cannot solve it.

**Competing explanations:** An individual row may exceed the allowance; many
ordinary rows may exceed it in aggregate; or Arrow string views may retain much
larger buffers than the values in a returned batch. Their distinguishing evidence
is maximum batch allocation, string-value bytes and maximum row-value bytes in
the largest observed batches, under independently varied batch and view settings.

**Scan arms:** Use the same release-built DataFusion version and retained 10M
input, `SELECT * FROM input`, 12 execution partitions and the same memory pool.
Read the immutable object already qualified by completed admission in the failed
workspace. Resolve its retained phase record and require its digest/size to match
the frozen input identity; retain that metadata inspection. This reuses completed
input qualification rather than hashing a mutable external path for every arm.
Scan once for each combination of 4,096/512 rows and forced Parquet string views
enabled/disabled. Consume bounded batches without writing data. Record all rows,
batches, maximum reported allocation, oversized-batch counts, and diagnostic
value lengths for every oversized batch and each new allocation maximum. Retain
summary maxima for oversized batches; no text values are logged.
These are read diagnostics, not completed lifecycle measurements.

**Encoding arms:** Use the completed physical data file retained by the failed
pilot, selected through the failure's inventory and footer diagnosis. Qualify its
SHA-256 once before the codec arms; the inventory alone records size/mtime, not a
digest. Compare the maintained
Parquet writer with uncompressed, Snappy and default Zstandard settings, one run
each, holding row groups and batches at 4,096 rows. Write separate output files,
retain all arms, and compare schemas, row counts and exact ordered batch values
with the input. Readback is outside encoding time and recorded separately. This
bounded codec comparison can justify the next full-path candidate but not an
end-to-end speed claim.

Use the existing monitor and its 24 GiB RSS, 2 GiB free-space and 1,800-second
command stop bounds. Diagnostic workspaces hold only these bounded codec outputs,
not another copy of the 10M input. Record source, executable, harness and protocol
identities, cache conditions, time, RSS, sampled disk peaks and retained bytes.
Run no Cargo builds or unrelated benchmarks alongside collection. Inputs may be
warm. Keep failures and partial results.

**Decision rule:** A scan configuration is a candidate only if all 10M rows are
consumed with every batch below the unchanged byte allowance. Prefer changing
one maintained setting over adding project-owned splitting or copying. If only a
combination works, retain its complete-path tradeoff as a question for the next
pilot. A codec candidate must preserve exact values and substantially reduce the
observed uncompressed output footprint; report CPU/time tradeoffs. Qualify any
adopted production change through the repository gate, independent review and a
new explicitly identified full-path run or resume. Never relabel the earlier
failure or these diagnostics as a passed complete acceptance gate.

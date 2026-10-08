# Retained Comments pilot

The first 10-million-Comments pilot **failed during processing**. It does not
qualify the performance target. The engine stopped after 7.95 seconds when a
decoded batch used 97,641,135 bytes against its 67,108,864-byte allowance. Admission
had completed; native output, validation, selection, independent readback and
reuse had not completed. No fresh-run or reuse result passed.

See the [frozen protocol](protocol.md), [raw attempt](run-20261008T103609/),
[command log](pilot-20261008.log) and
[independent harness review](../../docs/reviews/comments-performance-harness.md).
The review approves the corrected measurement harness, not the engine's result.

## Disk cost of the failed attempt

| Measurement | Bytes | Meaning |
| --- | ---: | --- |
| Frozen input, outside the engine workspace | 2,670,490,619 | Prepared once; retained for subsequent diagnostics and qualification |
| Admitted input in the failed workspace | 2,670,490,619 | Required retained input copy produced by this attempt |
| Partial native output | 132,213,327 | Unfinished output, retained with the failed attempt |
| Total retained workspace | 2,802,772,903 | Includes input, partial output, catalog and lifecycle metadata |
| Sampled peak allocated workspace blocks | 2,809,331,712 | Filesystem-reported allocation; shared APFS extents are not distinguished |
| Sampled scratch peak | 0 | No scratch was observed; brief unsampled peaks remain possible |

The workspace retained **1.050 times the encoded input size before processing
finished**. This is the cost of a failed partial attempt, not completed-dataset
storage amplification. The prepared input is outside that total, as is the shared
original capture of 6,680,266,997 bytes. Failed attempts and repeated qualification
workspaces consume additional space until their lifecycle permits retirement.
Maintenance is unfinished; this experiment credits no automatic reclamation.

Live volume free space remained above the experiment's 2 GiB stop threshold.
Volume changes also include other workstation activity, so they are not used as
DocSpec's disk-write total. The pilot sampled workspace metadata asynchronously
every five seconds and free space every second. Its inventory samples took about
0.005 seconds in total. Directory inventories and raw samples remain in the
attempt directory; sampled peaks are lower bounds.

Before measurement, `cargo clean --profile dev` reclaimed 7.0 GiB of rebuildable
debug artifacts. The release build remained available. Build-cache space is a
development cost outside the timed engine workspace; the cleanup log is retained
under [reviews](../../docs/reviews/comments-build-cache-cleanup.log).

## Diagnosis and next decision

The macOS timing tool reported a maximum resident set of 1,483,898,880 bytes. The
one-second process-tree sampler missed that brief peak, recording 30,752,768 bytes.
Use the timing tool's higher measurement here; neither value proves a continuously
enforced aggregate process-memory ceiling. Zero reported block I/O operations on
this cached local run do not prove that payloads were not read.

A [footer-only follow-up](row-group-diagnosis-20261008.json) found input row groups
up to 325,203,265 uncompressed bytes despite the 4,096-row layout. Row count alone
therefore does not bound decoded batch memory. That footer observation alone did
not distinguish a large individual value from retained backing buffers or shared
buffer accounting. The following scans resolve the batch-setting decision without
adding buffer-copy code.

The original native writer configured row-group size but left Parquet
compression at its default. A [partial-output footer inspection](run-20261008T103609/partial-output-diagnosis.json)
confirmed `UNCOMPRESSED` encoding in the completed physical output file. The next
file was unfinished and lacked its final footer; neither file belonged to a
selected completed result. The input footer reports about 15.37 GB of total
uncompressed row-group data versus 2.67 GB on disk. This makes output encoding
an immediate capacity question once processing can proceed. It is a diagnosis
priority, not a measured prediction of completed output size or an established
codec speedup. Compare a maintained compression setting with identical values
and complete-path time/storage evidence before choosing a new default.

Any follow-up must record its changed batch or writer setting before collection,
reuse the frozen input and completed admission when appropriate, preserve this
failure, and include old/new coexistence in capacity planning. Raising limits or
discarding the failure cannot turn this pilot into a pass. The complete local and
remote lifecycle gates remain open.

## Measured batch and codec follow-up

The [frozen diagnostic protocol](processing-protocol.md) and
[retained results](processing-20261008T105859/results.json) cover the same immutable
input already qualified by completed admission. Every scan consumed all 10 million
rows. Each oversized batch was inspected for row/value lengths; no literal text
was logged. These are streaming read diagnostics, not completed lifecycle runs.

| Batch rows | Forced string views | Oversized batches | Maximum batch allocation, bytes | Maximum observed row string bytes in oversized batches |
| ---: | --- | ---: | ---: | ---: |
| 4,096 | Enabled | 25 | 326,634,835 | 92,992,878 |
| 512 | Enabled | 36 | 238,716,254 | 92,992,878 |
| 4,096 | Disabled | 32 | 606,894,640 | 92,992,878 |
| 512 | Disabled | 16 | 243,052,095 | 92,992,878 |

One row's string values alone exceed the original 64 MiB allowance. Changing row
count or view representation cannot make that allowance sufficient. Keep the
maintained reader configuration and use a **768 MiB per-batch workload profile**
for the next Comments run, covering both observed view and plain-string allocation.
The 24 GiB process ceiling stays unchanged. This profile also raises existing
writer/read reservations and the rolling-file target; the next run must measure
their effect. Generic defaults remain unchanged.

The codec probe re-encoded the completed physical output from the failed attempt:

| Codec | Output bytes | Read/decode/write/sync seconds | Exact ordered values and schema |
| --- | ---: | ---: | --- |
| Uncompressed | 67,884,310 | 0.152 | Equal |
| Snappy | 19,918,409 | 0.201 | Equal |
| Zstandard, maintained default level 1 | 10,759,625 | 0.196 | Equal |

Zstandard used **84.2% less disk than uncompressed output** in this bounded fixture.
It also used less disk than Snappy, with similar observed re-encoding time. These
single, potentially cached observations do not establish a stable speed difference
or complete-path improvement. Exact comparison retained field metadata, types,
null/empty values, ordering and multiplicity. All arms and their readbacks remain
in the diagnostic directory; they added 98,562,344 logical output bytes.

The production candidate selects the maintained default Zstandard level in the
existing shared writer. It adds no codec framework, buffer-copy layer, new public
option or format change. Already completed objects remain reusable. The next
[resume protocol](resume-protocol.md) preserves original failed time/storage and
checks the changed profile through processing, selection, independent readback,
unchanged work and exact retry. The independent
[diagnostic review](../../docs/reviews/comments-processing-diagnostics.md) and
[writer review](../../docs/reviews/zstd-writer-review.md) retain findings and scope.

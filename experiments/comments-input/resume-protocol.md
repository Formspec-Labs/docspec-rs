# Comments resume with qualified encoding and a workload-sized batch allowance

Frozen after [processing diagnostics](processing-20261008T105859/results.json)
and before collecting the resumed lifecycle run. Preserve the failed original
pilot and every diagnostic arm.

**Decision:** Can the unchanged retained 10M Comments input finish the implemented
local flow with bounded memory and disk after correcting its actual batch-capacity
and output-encoding problems? Does an unchanged operation then reuse the input and
native files, and does its exact retry preserve retained files?

All scan arms consumed 10M rows. Every tested row/view combination still produced
oversized batches; an observed individual row held 92,992,878 string-value bytes.
Keep the existing 4,096-row/view reader configuration. Set this workload's existing
`max_batch_bytes` option to **768 MiB**, covering the observed 606,894,640-byte
allocation in the plain-string scan as well as the smaller view allocation. This
is a workload profile, not a change to the generic default or evidence of a
continuous process-memory bound. It also changes the existing writer's allowance
and rolling-file target; record resulting file count, memory and disk costs.

Keep 12 compute threads, the **24 GiB process ceiling**, 8 GiB scratch ceiling,
32 MiB metadata allowance, the same full-width input and `SELECT * FROM input`.
Use the maintained Parquet writer's default Zstandard level 1. In the bounded
fixture it retained 10,759,625 bytes versus 67,884,310 uncompressed and preserved
exact ordered values, types, field metadata, nulls and duplicates. Its 0.196-second
read/decode/write/sync observation versus 0.152 seconds uncompressed is a single
diagnostic, not a stable speedup or complete-path result. Existing completed
objects remain reusable regardless of their codec; no rewriting solely to adopt
compression and no new format or processor identity are needed.

Resume operation `fresh` in `run-20261008T103609/workspace` so its completed
admission and failed partial output remain accounted for. Record all new command
arguments, limits, binary/source/lock/protocol identities, raw output, timings and
before/after inventories. The 768 MiB limit belongs to this experiment's limits
record; the operation's original admission record still describes its first
execution. This is a resumed run, not one of the required fresh-workspace runs.

Use the existing independent Iceberg/PyArrow readback and catalog selection check,
then unchanged and exact-retry arms. Reuse the existing monitor's time, RSS,
scratch and volume-free-space stop bounds. The workspace bound remains three
frozen input sizes plus declared scratch, including the earlier failed output.
Record the marginal and cumulative retained storage, and the total time including
the failed original attempt, resumed processing and independent readback. Keep
diagnostic/build costs separate and visible.

Success permits only a bounded result for this implemented slice and profile.
The defined joins/summaries, complete evidence lifecycle, exports, maintenance,
hosted adapters and three fresh full-flow runs remain required. If this attempt
fails, retain it and diagnose the actual boundary before changing the protocol.

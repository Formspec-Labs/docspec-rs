# Flat-export row groups

Frozen 2026-10-10 before collecting corrected writer evidence.

**Decision:** decouple output row groups from reader batch size when the
maintained writer can retain bounded batches without exhausting footer admission.

**Observation:** the `881eb8c` full attempt completed join, contributor evidence
and summary, then failed after 116.53 seconds during flat export. The error was
DocSpec's conservative accumulated-footer admission, not a measured serialized
footer overrun. With 32 fields, each group costs at least 33,792 accounting bytes;
10 million rows at no more than 4,096 per group require at least 2,442 groups,
exceeding the 32 MiB allowance. `export.rs` also flushes every reader batch.
The failed attempt and partial objects remain retained.

**Hypothesis:** larger output groups chosen under the existing writer-memory
allowance reduce group metadata enough to complete this export, while preserving
exact values, field IDs, ordering within the input stream and disclosure policy.
An added encoded-byte tuning setting is unnecessary for this first correction.

**Arms:** the retained `881eb8c` failure versus a separately captured release
which keeps ArrowWriter's maintained default row cap, removes per-batch flush,
and flushes at accepted-batch boundaries when `memory_size()` reaches the existing
`max_batch_bytes`. Keep the 4× batch reservation and metadata reservation, with
conservative footer accounting including a final pending group before finish.
`memory_size()` is an estimate, and one batch can cross its threshold; neither it
nor the reservation is a numeric process-RSS proof. Retain measured process memory.
Do not change the native writer, scan projection, SQL, cardinality rules, codec,
resource limits, input selection or independent exact oracle in this increment.

**Cases:** small reader batches spanning a complete independently read export;
primitive types, nulls and duplicate occurrences; oversized/footer admission and
retained selection/retry behavior where a proportionate real fixture applies;
the original complete Comments workload. Keep creation/validation/retry semantics.
Check the final pending group before finish because maintained finish consumes
its internal completed-group metadata. Keep the independent serialized-footer
check after writing.

**Held constant:** full workload and protocol, pinned dependency versions,
12 threads, 24 GiB process memory, 32 GiB scratch, 4,096-row reads, 768 MiB batch
allowance and 32 MiB metadata allowance. Build separately. Keep failures and do
not benchmark during builds/tests. Follow the existing protocol's disk preflight:
predeclare 22 GiB retained bytes per fresh run for the next series (a tighter
allowance than the prior 24 GiB). The observed incomplete run retains 9.62 GB;
22 GiB leaves space for complete exports and uncertainty but is not an output-size
prediction. All three runs plus scratch and reserve must fit before starting.
Do not delete old attempts or silently shrink scope if preflight refuses.

**Decision rule:** adopt only if writer resource/metadata guards and exact output
checks pass, independent review approves the scoped change, and the actual export
can progress within its unchanged limits. Complete performance acceptance still
requires every stage and exact independent check in each of three fresh runs
under 1,800 seconds. A resumed failure diagnosis or one pilot is not acceptance.
Record export time, actual row groups/footer length for completed files, sampled
peak process memory, scratch, retained bytes and amplification. Stop and retain
any failure. Do not credit this layout change with join-statistics improvement.

## Disk admission revision before corrected collection

Available disk fell to about 100 GiB while prior failures and build artifacts
remain retained. Predeclare 18 GiB per run for the next series, tightening the
stop allowance; three runs plus 32 GiB scratch and 2 GiB reserve need 88 GiB at
preflight. The prior run retained 9.62 GB including a partial export; its complete
native tables account for 5.37 GB and original admitted inputs for 2.74 GB. This
supports testing within the smaller allowance with headroom, not a guarantee of
final size. Keep the original 22 GiB planning note above as history. No corrected
writer performance has been collected, no acceptance limit has been relaxed,
and the harness still refuses or stops if actual storage cannot fit.

## Correctness qualification before the full run

The first focused suite passed existing export behaviors and the new tiny-batch
success case; the new refusal fixture failed during source preparation because
an Arrow view retained about 8 MiB behind small slices under a 4 MiB allowance.
The fixture now admits that input under 16 MiB and writes small native groups,
then runs the intended export under 32 KiB batches and 256 KiB metadata. It reaches
the specific estimated-footer refusal, preserves the selected release, retains
no completed materialization/result, and retries with exact independent values.
That focused check and the final full repository gate pass. Both reviews are
retained. The implementation review approves with a nonblocking coverage limit:
no fixture isolates refusal caused only by the final pending group; the guarded
branch was statically traced against maintained finalization.

These are correctness and fixture-layout observations. They do not establish
full-scale export memory, output size, throughput or performance acceptance.

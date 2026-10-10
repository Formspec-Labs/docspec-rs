# Native efficiency follow-up: independent source review

Date: 2026-10-10. Scope: `src/query.rs`, `src/storage.rs`,
`src/storage/occurrences.rs`, `src/budget.rs`, their immediate callers and tests,
and the exact crate versions in `Cargo.lock`.

The initial findings below were established without reading
`docs/datafusion-performance-research-2026-10-10.md`. This was static analysis:
no code changes, builds, tests, benchmarks, or corpus payload reads. The review
used the Rust and semi-formal code-review skills and local cached crate source,
with upstream release sources as references. A later documentation cross-check
is recorded separately below.

## Findings, in priority order

### Reader working memory needs separate accounting

`storage.rs:90–107` reserves twice `max_batch_bytes`, sets output batch rows,
then checks decoded batch size after polling the native stream.
`storage/occurrences.rs:105–119,133–139` follows the same pattern. These checks
bound admitted output batches and reader multiplicity; they do not establish a
bound on the native reader's own working set.

Iceberg 0.10.1's `arrow/reader/pipeline.rs:210–236,260–261,394–405` builds a
Parquet async stream with a field projection and output batch size. Parquet
58.4.0's `arrow/async_reader/mod.rs:695–706` says that this stream buffers all
selected data pages of a row group during decoding. Coalesced range requests
can also retain intervening bytes: Iceberg `arrow/reader/file_reader.rs:74–104`
fetches merged ranges and returns shared slices of those buffers.

This is a static accounting gap, not evidence of an observed memory failure.
Project-written native files currently have short row groups
(`storage.rs:227`), which mitigates the usual case. That does not make output
batch size a general reader-memory limit, especially across different write/read
budgets. Routine inspection checks footer bytes, schema, size and counts, but
does not admit selected row-group working bytes (`storage/inspection.rs:126–155`).
Its small sample also uses this async reader (`:171–180`).

Qualify selected compressed row-group bytes, decoding allocations and observed
process memory before increasing simultaneous readers or native row-group size.
Prefer retained layout facts and maintained APIs; a new decoder is not justified
by this review alone. [Parquet release source](https://raw.githubusercontent.com/apache/arrow-rs/58.4.0/parquet/src/arrow/async_reader/mod.rs)

### Push requested columns into the maintained native reader

`query.rs:143–165` creates a full native scan and applies `ProjectionExec` above
it. `query.rs:250–255` declines physical projection pushdown, while
`storage/schema.rs:81–100` selects every current root field. A narrow key or
count query can therefore decode unrelated text and nested values.

The existing `OccurrenceScan::read_projected_file` already selects field IDs
(`storage/occurrences.rs:327–353`); `engine/evidence.rs:816–823` uses it for an
origins-only read. Extend this path rather than creating a second decoder.
Preserve field-ID interpretation, defaults, stored origins and virtual-file
handling. In pinned Iceberg, an empty physical projection or requested fields
all absent from an old file falls back to reading all physical columns; do not
claim zero-column counts become metadata-only automatically.
[Iceberg projection source](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/arrow/reader/projection.rs)

### Reuse captured file tasks within a selected operation

Each table-source execution starts the reader again (`query.rs:263–275`). An
occurrence reader walks snapshot inventories (`storage/occurrences.rs:88–90`),
then separately plans files (`:112`); `storage.rs:494–554` includes retained
snapshot manifests in the inventory walk. Repeated relational consumers can
repeat this metadata work even when using the same immutable pin.

`OccurrenceScan` already captures tasks once (`storage/occurrences.rs:208–215,
252–293`). Reuse that bounded state when the exact selected pin and schema are
unchanged. Preserve the complete-file and count checks rather than substituting
directory listings or mutable table heads. This is verified repeated work, not
a measured dominant cost.

### Source parallelism is a candidate after reader-memory qualification

The adapter exposes one source partition (`query.rs:197–205,218–220`), and both
reader paths set data-file concurrency to one (`storage.rs:96`,
`storage/occurrences.rs:115`). Captured multi-file reads also flatten serially
(`query.rs:304–312`). Iceberg's pinned single-concurrency path uses
`and_then(...).try_flatten()`; the alternative uses unordered buffered streams
(`arrow/reader/pipeline.rs:64–85`).

Raising `compute_threads` only changes DataFusion target partitions
(`budget.rs:110–112`); downstream repartitioning cannot turn that source decoder
into multiple decoders. Raising only Iceberg file concurrency can overlap I/O,
but its unordered stream combinators do not themselves spawn parallel CPU decode
tasks. Multiple independently executed source partitions are a separate option.
Measure either change under a shared reader-memory allowance, preserving each
file's row positions and exact-once task assignment.
[Iceberg reader source](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/arrow/reader/pipeline.rs)

### Retain the maintained scan counters before further tuning

`storage.rs:99–100` and `storage/occurrences.rs:194–195` immediately consume
`ScanResult` into its stream. The pinned API provides a cloneable `ScanMetrics`
handle with `bytes_read()`. Keeping that handle is a small way to measure reader
work without inventing an I/O wrapper. Its counter increments requested range
length before the underlying read, so document it as requested storage bytes;
it is not unique bytes, successful-transfer bytes, or observed memory. It also
does not cover the separate manifest planning paths.
[Iceberg scan metrics source](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/arrow/scan_metrics.rs)

### Separate execution batches from persisted native layout

`storage.rs:227` ties row-group rows to `batch_rows`, and the ordinary writer's
target file bytes derive from `max_batch_bytes` (`storage.rs:175–183`). These
couplings make reader/execution tuning change persisted file layout. The writer
already bounds accumulated footer descriptors (`:276–309`) and uses Zstandard
(`:230`); retain those protections. Compare a separate native layout setting
against current behavior for complete-path time, compressed output, footer bytes,
peak memory and coexistence disk use. No faster or smaller outcome is established
by inspecting these settings.

## Function trace and streaming interpretation

| Path | Verified behavior |
| --- | --- |
| `NativeTable::scan`, `query.rs:143` | Residual filters; projection and optional limit above source. |
| `NativePartition::execute`, `query.rs:263` | Opens a native stream lazily, then yields batches. |
| `read_batches`, `storage.rs:86` | One in-flight file; byte check after native batch decode. |
| `Occurrences::next`, `storage/occurrences.rs:133` | Consumes one whole file at a time; checks decoded row counts at EOF. |
| `read_projected_file`, `storage/occurrences.rs:317` | Reuses captured task; selects native field IDs for values-only reads. |
| `Budget::context`, `budget.rs:100` | Shared DataFusion memory/spill runtime, target partitions, correctness guards. |
| `backfill` read, `engine/backfill.rs:239–246` | Executes a stream and applies final batch admission. |

`execute_stream()` does not collect the full result or force the entire query
onto one CPU. DataFusion 53.1's `physical-plan/src/execution_plan.rs:1225–1233`
coalesces multi-partition output; `coalesce_partitions.rs:212–222` starts a task
per input partition. Blocking joins, sorts or aggregates still retain state
before yielding. The collection in `engine.rs:636–647` is only a final scalar
aggregate, and `engine/write.rs:690–696` collects an at-most-one-row probe;
neither is evidence of full-result application materialization.
[DataFusion execution source](https://raw.githubusercontent.com/apache/datafusion/53.1.0/datafusion/physical-plan/src/execution_plan.rs)

## Preserved invariants and unsafe shortcuts

- A batch-byte check after decoding is not a whole-process memory limit. The
  shared pool in `budget.rs:85–95` and native reservations cover different scopes.
- Occurrence paths require complete, unfiltered, undeleted file tasks
  (`storage/occurrences.rs:374–387`). Filtering before deriving physical row
  positions would change stable identities; pushing projection alone need not.
- Keep the pinned aggregate-statistics and null-safe dynamic-join guards
  (`budget.rs:101–118`) until an upstream fix is independently qualified.
- Keep the virtual file-column workaround that forces field-ID interpretation
  (`storage/schema.rs:87–91`); matching column names alone is insufficient.

## Tests and review limits

The inspected tests cover reservation lifetime, cancellation/drop and output row
limits (`query.rs:433–472`), post-decode oversized batches (`:532–555`), and
occurrence reservation release (`storage/occurrences/tests.rs:156–184`). The
projected-file test (`engine/write/tests.rs:511–544`) succeeds on a narrow key
projection while the full read exceeds its decoded-batch limit. Those assertions
support the existing behavior; they do not measure reader working memory,
physical bytes saved, or multi-file CPU scaling. No tests were executed.

Initial verdict: **NEEDS DISCUSSION for tuning, not a code rejection.** The
highest-confidence next efficiency change is native column selection with
maintained reader metrics. Reader-working-set qualification should precede more
concurrency or larger native row groups. Runtime gains and resource ceilings
remain workload measurements, not source-analysis conclusions.

## Documentation cross-check

After retaining the independent findings, reviewed the current changes to
`docs/datafusion-performance-research-2026-10-10.md` and `docs/STATUS.md`.

**VERDICT: APPROVE the documentation changes.** The new reader-buffer,
source-parallelism and scan-metric sections match the pinned source. They
correctly separate static accounting gaps from observed failures and proposed
tuning from measured gains. The priorities are proportional: narrow reads and
reused work first; memory qualification before larger row groups or additional
readers; no new scheduler or decoder without a measured reason.

The added upgrade guidance is also appropriately qualified. On this review date,
the [hash-join spilling issue](https://github.com/apache/datafusion/issues/24768)
is open and the [sort-merge fallback PR](https://github.com/apache/datafusion/pull/26015)
is a draft. `Cargo.toml:24,42–43` and `src/main.rs:26–27` confirm mimalloc and
release debug information; [Cargo's profile documentation](https://doc.rust-lang.org/cargo/reference/profiles.html#lto)
supports treating cross-crate thin LTO as a build-time versus runtime tradeoff,
without a promised workload speedup.

No correction is required for the reviewed additions. Coverage is adequate for
the documentation claims; confidence is high for the pinned code paths and
medium for workload priority until measured. This review did not rerun the
historical performance gate or independently audit its retained measurements.

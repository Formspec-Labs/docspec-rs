# Native SQL column selection: design review

**Decision: implement column selection in the existing native adapter.** Pass
DataFusion's requested source columns to Iceberg as stable field IDs, then return
the requested Arrow columns in their exact order. Extend the captured-file reader
to do this for occurrence views as well as value views. Keep native decoding,
current schema interpretation, serial file reading and existing admission.

Date: 2026-10-10. Static review against `5508a1144d872e4f72ed636eab02dc0eedc0146c`
and the inspected working files. Another feature has concurrent working changes;
this document plans the next performance slice and does not expand that feature's
commit. This review changed no Rust and ran no builds, tests, formatting,
benchmarks or corpus-payload reads. The MS Rust, semi-formal architecture review and
test-hypotheses workflows informed the analysis. Proposed checks below have not run.

## Findings and disposition

| Priority | Finding and evidence | Disposition |
| --- | --- | --- |
| Required design change | SQL selection currently sits above full-width decoding: `NativeTable::scan` creates `ProjectionExec`, while table execution calls the unselected readers ([query.rs:143](../../src/query.rs#L143), [query.rs:263](../../src/query.rs#L263)). | **KEEP** the maintained reader; move the column list into the native source. |
| Correctness requirement | Captured providers already have a source-relative selection, but SQL indices refer to the provider's advertised schema. Located reads currently refuse selection ([query.rs:101](../../src/query.rs#L101), [occurrences.rs:317](../../src/storage/occurrences.rs#L317)). | **RESHAPE** selection as index composition followed by stable-ID translation. Keep requested output order and duplicates separate from unique native dependencies. |
| Correctness requirement | Stored origins can be absent physically and need paired validation/fallback; their meaning differs from the current physical filename ([identity.rs:128](../../src/storage/identity.rs#L128), [occurrences.rs:392](../../src/storage/occurrences.rs#L392)). | **KEEP** complete, unfiltered file traversal. Read the origin pair whenever the occurrence result depends on either member. |
| Qualified limitation | Empty or entirely absent physical field selections can read all physical columns in pinned Iceberg ([projection.rs:118](https://github.com/apache/iceberg-rust/blob/v0.10.1/crates/iceberg/src/arrow/reader/projection.rs#L118), [projection.rs:211](https://github.com/apache/iceberg-rust/blob/v0.10.1/crates/iceberg/src/arrow/reader/projection.rs#L211)). | **DEFER** a cheaper count/default-only path. Preserve correct rows/defaults and explicitly measure the fallback. |
| Resource limitation | Reader permits cover admitted batches, not all selected row-group pages; the async decoder buffers selected pages ([storage.rs:90](../../src/storage.rs#L90), [Parquet reader:695](https://github.com/apache/arrow-rs/blob/58.4.0/parquet/src/arrow/async_reader/mod.rs#L695)). | **KEEP** current concurrency and limits. Do not turn this optimization into a whole-process memory claim. |

## Artifact, lineage and ownership

This is an execution-efficiency change with qualification support. Its beneficiary
is a consumer running a narrow query, key check, summary or evidence read over a
wide retained table. The intended outcome is fewer unused values decoded without
changing values, schemas, multiplicity, defaults or source occurrences. It adds no
public dataset format, lifecycle operation or storage authority.

| Prior artifact | Relationship |
| --- | --- |
| [PLAN: Architecture and performance rules](../PLAN.md#architecture) | Requires maintained Iceberg/Parquet, Arrow and DataFusion, exact selected versions and ordinary streamed reads. |
| [R3: Schema and identity design](../requirements/03-schema-backfill.md#schema-and-identity-design) | Makes field identity independent of name/position; distinguishes absent, explicit null and default values. |
| [R7: Qualification](../requirements/07-operations-qualification.md#performance-acceptance) | Requires exact independent expectations and measured cost before efficiency claims. |
| [Current efficiency decisions](../datafusion-performance-research-2026-10-10.md#column-selection-is-the-smallest-useful-reader-change) | Selects this as the next reader increment; inventory reuse, cardinality changes, layout and concurrency are separate decisions. |
| [Independent source review](native-efficiency-followup-review-2026-10-10.md#push-requested-columns-into-the-maintained-native-reader) | Identifies the same reader gap and its admission, metrics and fallback limits. |

| Component / named seam | Owner and responsibility | Immediate consumers |
| --- | --- | --- |
| `TableProvider::scan` → `NativePartition` | `query.rs`: interpret indices against the advertised schema; retain row statistics and normal DataFusion filters/limits. | [Join input registration](../../src/engine/refresh.rs#L503), ordinary native query consumers. |
| Native field selection → `FileScanTask` | `storage/schema.rs`: select stable root IDs from the retained current schema, preserving the transient `_file` workaround. | [Ordinary native reads](../../src/storage.rs#L86), occurrence readers and inspection. |
| Captured task → streamed file | `storage/occurrences.rs`: preserve admitted inventory, full-file/count checks and origin semantics. | [Replacement key reads](../../src/engine/write.rs#L451), [origin membership reads](../../src/engine/evidence.rs#L816). |
| Iceberg reader → Arrow batches | Maintained Iceberg 0.10.1 and Arrow/Parquet 58.4.0: field-ID mapping, decoding and absent-field interpretation. | Existing DocSpec reader adapters. |

All seams already exist. The plan refines their input data rather than adding an
executor or provider family. The dependency versions were checked in
[Cargo.lock](../../Cargo.lock); the compatible pinned DataFusion API is 53.1.0.

## Verified execution trace

| Function | Input → output | Behavior to retain |
| --- | --- | --- |
| [`NativeTable::scan`](../../src/query.rs#L143) | Provider-relative indices → execution plan | DataFusion owns residual filtering; optional `GlobalLimitExec` stays above the source. |
| [`projected_files`](../../src/query.rs#L86) | Captured schema plus optional requested names → provider | Names resolve once to captured-schema indices. File membership and row hints come from the captured inventory. |
| [`NativePartition::execute`](../../src/query.rs#L263) | Immutable source plus budget → lazy Arrow stream | Values and located tables keep their distinct native paths; captured files open sequentially. |
| [`schema::plan_files`](../../src/storage/schema.rs#L78) | Retained table → selected tasks | Overrides tasks with the retained current schema, not the older snapshot's schema; appends reserved `_file` by ID. |
| [`read_batches`](../../src/storage.rs#L86) | Table → ordinary value batches | Default-expansion admission, serial native reader, transient-column check and stream-owned permit. |
| [`OccurrenceScan::read_projected_file`](../../src/storage/occurrences.rs#L317) | Captured file plus optional indices → checked stream | Reuses one captured task/schema; holds its inventory permit through stream lifetime. |
| [`Occurrences::next`](../../src/storage/occurrences.rs#L133) | Whole-file native batches → values/occurrences | Advances file positions by decoded row count; validates expected counts at EOF; handles stored origins. |

The pinned [DataFusion table interface](https://github.com/apache/datafusion/blob/53.1.0/datafusion/catalog/src/table.rs#L93)
defines selection indices relative to the provider schema and requires that order.
Its [projection helper](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-expr/src/projection.rs#L185)
explicitly permits repeated indices. Iceberg's
[reader pipeline](https://github.com/apache/iceberg-rust/blob/v0.10.1/crates/iceberg/src/arrow/reader/pipeline.rs#L214)
uses field IDs to choose Parquet columns and its
[transformer](https://github.com/apache/iceberg-rust/blob/v0.10.1/crates/iceberg/src/arrow/record_batch_transformer.rs#L347)
constructs output fields in the requested ID order. These are maintained facilities
the adapter can connect; none requires an owned decoder.

## Smallest implementable change

### 1. Compose selection at the existing query boundary

Give table sources the same optional source-index mapping already held by captured
file sources. A projected `NativePartition` owns its projected output schema and
the mapping back to its immutable source schema. In `NativeTable::scan`, validate
indices with Arrow's schema projection, compose them with any existing mapping,
and construct `DataSourceExec` over that narrowed partition.

For example, an existing file provider selecting source indices `[4, 1]` followed
by SQL selection `[1, 0, 1]` must read source indices `[1, 4, 1]`, with that output
order. A SQL index must never be reused directly as an Iceberg field ID or as an
index into the original schema. Keep `None` meaning all provider columns and
`Some([])` meaning zero output columns; never merge those cases. Validate before
indexing so an invalid request returns an error rather than panicking.

This removes the adapter-created post-read `ProjectionExec` for the index-only
scan request. DataFusion can still keep its own projection for aliases,
expressions and final output. Keep `try_swapping_with_projection` returning `None`
for this slice: logical SQL selection already reaches `TableProvider::scan`, and
absorbing arbitrary physical expressions is unnecessary. Capture physical plans
to check this premise. If a real target query lacks logical pushdown, add only a
column-reference case using the same composition helper and preserving aliases;
do not silently absorb expressions. The relevant maintained extension is
[`DataSource::try_swapping_with_projection`](https://github.com/apache/datafusion/blob/53.1.0/datafusion/datasource/src/source.rs#L167).

Retain the exact existing row hint after column selection. Produce statistics
for the narrowed schema; leave decoded byte statistics unknown and counts
`Inexact`. Preserve the [aggregate-statistics and dynamic-join guards](../../src/budget.rs#L100).

### 2. Share one small field-selection description in storage

Add a private helper beside the existing native schema code. Its inputs are the
qualified current schema, reader mode and requested source indices. Its outputs
are the unique native root IDs to decode, necessary origin dependencies, the
ordered result mapping, and the expected output schema. Reuse it from ordinary
table, occurrence-table and captured-file reads. Full reads remain wrappers using
the all-columns selection, so independent/full verification keeps its behavior.

Build unique native dependencies in a deterministic order, such as current root
order. Translate indices with the retained `NestedField.id`. A selected nested
root selects the complete root; nested-leaf pruning is outside this increment.
Append `RESERVED_FIELD_ID_FILE` exactly once, last, including empty selections.
Keep [schema qualification](../../src/storage/schema.rs#L17) and
[`strip_file_column`](../../src/storage/schema.rs#L106). A user field named
`_file` remains an ordinary field with its own ID; names cannot identify the
transient field.

Native input dependencies and result positions serve different purposes. Decode
an ID once, then use Arrow array references to return repeated columns. This
also avoids independently expanding the same virtual default for each output
alias. Preserve field metadata, nullability and schema metadata in the final
advertised schema. Arrow's
[`RecordBatch::project`](https://github.com/apache/arrow-rs/blob/58.4.0/arrow-array/src/record_batch.rs#L454)
shares array handles and preserves row count for an empty projection. If final
assembly needs a supplied schema, use the maintained constructor with an explicit
row count; `try_new` with no arrays cannot infer the nonzero count.

Do not replace ordinary `read_batches` with a captured occurrence scan. That
would add a retained all-file inventory and occurrence-specific delete/task
restrictions to a value-reader path. Add an internal selected form of each
existing reader and let both use the shared selection description. Likewise,
extend `read_projected_file` instead of creating a second file-reader path.

### 3. Keep origin dependencies and output locators distinct

For located reads, classify requested indices against the original native root
count before handling appended `__docspec_file` and `__docspec_row`. These are
synthetic occurrence fields, not native IDs. Preserve the existing reserved-name
check even if the query omits the conflicting field.

If either requested occurrence field depends on stored origins, include both
`__docspec_origin_file` and `__docspec_origin_row` in the native dependency list.
Keep paired-null validation, nonempty origin/nonnegative position validation,
and the existing null-pair fallback in `identity::materialize`. This applies also
when a captured values-only reader requests just one stored origin: projecting
away its partner before materialization would change existing semantics.
Ordinary table value reads retain their current raw native-value semantics;
do not introduce origin materialization there as an incidental change.

When the source has no stored origins, derive requested locators from the checked
task path and original file offset. Generate only requested locator arrays: a
row-only request need not allocate repeated path strings. A value-only selection
from an occurrence provider need not allocate either synthetic locator. Stored
origin dependencies may remain internal and disappear in the final selection.

Advance offsets over every native row, including zero-column batches, before
DataFusion filters or limits consume the result. Keep `check_task` unchanged:
complete Parquet files, exact sizes/counts, no native predicates or delete tasks.
Do not substitute Iceberg `_pos`; the pinned dependency probe explicitly finds it
unavailable ([query.rs:655](../../src/query.rs#L655)). EOF proves complete-file
coverage; early stream drop remains only a partial read.

### 4. Preserve defaults, admission and measurement lifetime

Retain the current full-schema `defaults::check_expansion` admission first. It is
conservative for a narrow read and may refuse due to an unselected large default,
but keeping it avoids mixing default-admission relaxation into this change.
If that blocks a measured target case, separately qualify a selected-ID version;
never remove the pre-allocation check. Read defaults through the maintained
transformer. Keep initial defaults distinct from write defaults and preserve
requested supplied-state fields as ordinary stable-ID dependencies.

Retain the stream-owned `2 × max_batch_bytes` reservation, `batch_rows` cap,
transient `_file` byte check, and checks after origin/default/output assembly.
Do not discount the logical output size simply because repeated columns share
buffers. Preserve error/drop/EOF release behavior and one active file reader per
source. Reuse the captured `Arc` and its metadata permit; do not clone its full
task map. Bound newly retained selection/map allocations with the existing
metadata allowance before copying request vectors, and keep that allowance with
their owner. This is application admission, not a new allocator-level bound.

Capture `ScanResult::metrics().clone()` before consuming `.stream()`. Expose it
only through qualification or requested diagnostics, using the same small hook
in both native paths. Captured reads create a result per file: sum each handle's
final value once and release it, rather than retaining a metrics list proportional
to file count. Keep partial counts when a stream errors or is canceled. The
maintained [counter](https://github.com/apache/iceberg-rust/blob/v0.10.1/crates/iceberg/src/arrow/scan_metrics.rs#L42)
adds requested range lengths before I/O; label the measure **requested scan
bytes**, not physical disk traffic, unique bytes or complete metadata traffic.

## Invariants, limits and reuse boundary

| Invariant | Status in the proposed change | Failure caught by qualification |
| --- | --- | --- |
| Exact retained snapshot and current schema | **PRESERVED**: use the `Table`/captured tasks already tied to the pin; [open_pin](../../src/storage.rs#L677) checks both bindings. | Older schema or name-based lookup misreads reorder/rename/same-name replacement. |
| Exact output order, duplicates and row count | **NEW explicit mapping**, preserving existing results. | Deduplication leaks into output; a second selection indexes the wrong source; empty batches lose rows. |
| Stable occurrence meaning | **PRESERVED**: paired stored origins or unfiltered physical offsets. | Compacted rows acquire new file identities, or filtered rows are renumbered. |
| Default and supplied-state interpretation | **PRESERVED** through native field-ID transformation and existing admission. | Absent fields become the wrong null/default; explicit null is treated as omission. |
| Bounded admitted readers and metadata lifetime | **RELIED UPON**, with selected mapping lifetime included. | Early release admits a competing reader while the first still retains buffers. |
| Full verification remains explicit | **PRESERVED**: full-read wrappers and verification continue selecting all required data. | A key-only success is incorrectly reported as integrity proof for unselected values. |

Projection intentionally changes whether unrelated payload corruption is visited:
a narrow admitted read can succeed while a full integrity check fails. That is
already the captured-reader behavior tested in
[write/tests.rs:569](../../src/engine/write/tests.rs#L569) and matches
[ordinary-read policy](../PLAN.md#performance-and-user-value-rules).

This slice reuses existing captured metadata in place. It does not promise fewer
inventory walks across separate queries. Table occurrence execution currently
calls `snapshot_files`, which walks snapshots retained in that metadata
([occurrences.rs:88](../../src/storage/occurrences.rs#L88),
[storage.rs:494](../../src/storage.rs#L494)); cardinality probes and the main join
execute separately ([refresh.rs:532](../../src/engine/refresh.rs#L532)). Sharing
one `Arc<OccurrenceScan>` for the exact immutable selection within an operation
remains the next separately measured change. Keep complete retention inventories
at lifecycle boundaries. Do not add a global cache, key by table name, reopen a
mutable catalog head, or rehash payloads to select columns.

Reader working memory also remains a disclosed limitation. Parquet buffers
selected row-group pages, and Iceberg's
[range coalescing](https://github.com/apache/iceberg-rust/blob/v0.10.1/crates/iceberg/src/arrow/reader/file_reader.rs#L74)
can retain intervening bytes. Existing creation inspection parses a footer but
retains schema/count facts, not a per-row-group reader allowance
([inspection.rs:126](../../src/storage/inspection.rs#L126)). Before increasing
row groups or concurrent readers, qualify selected-page plus decode allocations
using available footer information and a large-row-group/small-batch fixture.
Do not reread every footer merely to create an unmeasured new cache in this slice.

## Qualification needed before implementation acceptance

The existing tests establish prerequisites, not the proposed SQL pushdown. In
particular, `query_provider_preserves_pins_filters_duplicates_and_empty_schema`
checks duplicate **rows**, not repeated selected columns.

| Test family to extend | Required assertions |
| --- | --- |
| [Query semantics](../../src/query.rs#L475) | Direct provider `scan` and SQL: identity, reordered `[2,0]`, repeated `[2,0,2]`, `None`, `Some([])`, aliases/expressions, invalid indices, empty source and multiple batches. Use independent expected arrays, not the new selection helper as oracle. |
| Captured provider composition | Start with a reordered/repeated `query::file`/`files` selection, then select/repeat again through SQL. Cover single and multi-file providers, values and located views. Ensure unchanged captured tasks can serve a later full read. |
| [Schema evolution](../../src/storage/occurrences/tests/schema.rs#L17) and [equal-type reorder](../../src/storage/occurrences/tests/schema.rs#L210) | Sparse IDs, rename/reorder, same-name replacement with another ID, nested root, field metadata, original retained reads, user `_file` and reserved occurrence names. Narrow DataFusion output must match direct independent expected values. |
| [Native defaults](../../src/storage/occurrences/tests/defaults.rs#L136) and [zero native fields](../../src/storage/occurrences/tests/schema.rs#L549) | Defaults-only, all selected IDs absent physically, mixed old/new files, nullable addition, explicit null/zero/empty/false versus omission and selected supplied marker. Assert exact rows and types; record the full-column fallback rather than asserting pruning for these cases. |
| [Occurrence correspondence](../../src/storage/occurrences/tests.rs#L101) | File-only, row-only, both, reordered/repeated locators and values-only from a located provider, across files/row groups/batch sizes. Include compacted stored origins, mixed stored/null origin pairs, malformed pairs and SQL filtering after skipped rows. |
| [Task/count refusals](../../src/storage/occurrences/tests.rs#L240) | Existing incomplete/repeated/deleted/predicated-task and short/long decoded-count failures still occur with narrow and empty selections. No shortcut may manufacture successful coverage from metadata counts. |
| [Admission](../../src/query.rs#L433), [locator limits](../../src/storage/occurrences/tests.rs#L188) and [captured reader lifetime](../../src/engine/write/tests.rs#L546) | Partial read, cancellation/drop, EOF and error release permits. Competing readers refuse; selected values, transient metadata, origins and final duplicate outputs respect their byte checks. Preserve the tiny-budget `COUNT(*)` refusal fixture. |
| [Actual column skipping](../../src/engine/write/tests.rs#L569) | Extend the existing corrupt-unselected-page probe to `query::table`, `query::occurrences` and captured SQL paths. A key query must return exact values; selecting the damaged field must fail. This proves unused value decoding was skipped, independently of elapsed time. |
| [Count guard](../../src/query.rs#L559) and [join statistics](../../src/query.rs#L604) | `COUNT(*)`, constant expressions, filtered counts, `LIMIT/OFFSET`, nullable keys and narrow joins retain results with/without row hints; smaller-build planning and existing null-safe join guards remain intact. |

After focused tests and an independent static implementation review, run the
repository gate once for the actual implementation. A failure requires a targeted
repair/recheck; this design review does not claim that gate passed.

## Controlled small experiment before a throughput claim

Use the test-hypotheses workflow to freeze the following protocol before collecting
results. It can be executed after the current feature gate and the new code gate;
do not benchmark concurrently with builds, tests or another acceptance run.

| Protocol item | Fixed choice |
| --- | --- |
| Decision | Whether forwarding SQL columns reduces native reader work enough to retain the implementation and justify a bounded scan-performance statement. |
| Hypothesis | An actually present key plus required origins reads/decodes less than the same query over full-width native batches. Alternatives: selection failed to reach tasks; footer/range overhead dominates; all-absent fallback prevents savings; DataFusion/sink work dominates time. |
| Arms | A: baseline adapter. B: selected-column adapter. Pin both code revisions and one lockfile; add the same metrics hook to both. Hold input pins, physical layout and SQL identical. |
| Cases | A fixed complete-file slice from retained qualified native Comments data, with manifest/object identities recorded before execution; one narrow key query, one key-plus-occurrences query, and one full-width control. Use a deterministic synthetic fixture separately for empty/default-only/origin edge cases. Never corrupt retained data for the decoding probe. |
| Size bound | Select whole files totaling at most 256 MiB compressed using their inventory before opening payloads. If no eligible retained slice exists, use a labeled deterministic wide fixture within that cap; do not download or rewrite the full corpus. This cap is a probe budget, not a representative-workload claim. |
| Held constant | Release build settings, resource profile, serial reader, source partition count, batch rows, predicates, output sink, snapshot/schema IDs and file order. Record actual CPU/runtime settings. No writer-layout, inventory-reuse or thread change. |
| Repetition/cache | One labeled warm-up per arm, then three interleaved measured pairs in a preregistered order. Use fresh query contexts/processes consistently, retain the shared operating-system cache, and call this a warm-cache probe. Do not claim a cold-cache result without controlled cache state. |
| Stop bound | Ten minutes total measured execution, five minutes per invocation, 24 GiB process-memory ceiling, 1 GiB new probe outputs/scratch, and the configured stricter limit where applicable. Check live disk first; retain every refusal, timeout and partial metric. Do not expand the slice after seeing results. |
| Observations | Requested native field IDs and physical plan, native requested scan bytes, rows/batches, output/working Arrow byte observations, wall time, process CPU time and sampled peak resident memory. Record scratch and extra retained bytes. Keep inventory/planning time separate from streaming time; manifest requests are not included in `ScanMetrics`. |
| Exactness | Compare narrow outputs and occurrences against independent fixture expectations or the baseline full reader restricted only after decoding. Consume all rows; no `LIMIT`, metadata-only result or slow formatted output may substitute for the same scan work. Keep equality checking outside timed streaming, or include the identical checker in both arms and label it. |
| Decision rule | Accept semantic changes only with exact results and all refusal/lifetime checks. Claim column skipping only when the physical/task evidence and corrupt-page test agree. Claim lower requested read bytes only from counters. Claim faster scanning only when every measured narrow pair improves elapsed time with no higher paired peak memory, and the full-width control stays within the preregistered 10% regression guard. Otherwise report reduced work, tradeoff or unresolved timing as observed. |

The numerical guards above are proposed experiment limits, not existing product
requirements. Freeze them with command lines, binaries, fixture identities and
environment before the first measurement. Preserve all raw results and explain
cache variability. A small scan result cannot establish faster complete lifecycle
processing. Use the existing frozen Comments workflow only when making that
broader claim; keep its independent checker and acceptance workload intact.

## User value, counterfactuals and verdict

The improvement removes unused native value decoding where requested physical
fields exist. The small mapping helper pays down duplicated selection logic across
existing read paths. It does not promise less row scanning, cheap zero-column
reads, lower whole-process memory, or faster complete lifecycle processing.

Keeping the current post-read projection would preserve behavior but continue the
avoidable wide decode. Replacing the provider/decoder would add schema, identity
and admission integration work already handled here. Operation-wide task reuse is
a complementary metadata optimization, not a substitute for this value-read
change. If this slice were removed, consumers would remain correct and lose only
the qualified efficiency benefit.

Kill or reshape this design if exact field/default/occurrence results cannot be
preserved, reader permits cease to cover stream lifetime, or source instrumentation
shows no reduction in the targeted work. If an all-absent fallback dominates the
real workload, a maintained-reader fix or separately qualified count path is the
next decision; multiplying custom read cases is not the answer. A future reviewer
should be able to locate a mechanism proof and bounded measurements, rather than
infer speed or memory safety from a projected schema.

**VERDICT: APPROVE this design for the next isolated implementation slice.**
Intent and shape match the accepted reader priority. Existing commitments are
honored, maintained APIs cover decoding, and the added logic stays within the
current adapter. Confidence is high for the static seam and medium for performance
value until the frozen experiment runs. Implementation acceptance and all new
performance claims remain unqualified.

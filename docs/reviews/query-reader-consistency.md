# Native query reader: consistency review

**Final static verdict: APPROVE for the comparison-reader checkpoint.** No row,
snapshot, filter or dependency-feature regression was found in the reviewed
bridge. The stable error-code issue found during review is closed in the revised
source. Initial findings remain below with a closure trace.

This is a static semi-formal review of the uncommitted checkpoint against
`f85c74f`, reviewed on 2026-10-08. The reviewer read the changed files, their
callers/callees, new and existing assertions, and the pinned DataFusion/Iceberg
implementations. No tests or benchmarks were executed by this reviewer.

Scope is the native reader used for full comparisons. Update-file discovery,
schema evolution, destructive maintenance and general selective query performance
are outside this checkpoint.

## Findings

### F1 — Native reader refusal codes disappear across the query boundary

- **Severity:** WARNING
- **Category:** correctness
- **Location:** initial `src/query.rs:49-60`, `src/engine.rs:339`, `src/error.rs:70`.
- **Evidence chain:** `storage::read_batches` returns a DocSpec `capacity` or
  `oversized_batch` refusal (`src/storage.rs:72`, `87-91`). The bridge preserves it
  as `DataFusionError::External`. When comparison collection returns the error,
  the blanket `From<DataFusionError>` implementation makes the outer DocSpec code
  `query`, even though the original stable code remains buried in the cause chain.
- **Concrete sequence:** Admission succeeds for the preceding sequential release
  verification, but another reader uses capacity when the comparison stream
  begins. The new bridge correctly refuses that stream; `Engine::compare` returns
  a generic query failure instead of `capacity`. A caller checking `Error::code()`
  cannot distinguish retryable admission refusal from invalid query execution.
- **Suggestion:** Preserve the embedded DocSpec code when converting a maintained
  query error back into the application error, including DataFusion context
  wrappers. Keep the original query error as cause if needed. A targeted assertion
  should check the resulting code, rather than only its displayed text.
- **Test evidence:** Initial tests at `src/query.rs:137` and `:232` check refusal
  message substrings. They do not exercise the conversion back to DocSpec's public
  error type.

### O1 — Full native scans are an explicit scope tradeoff

- **Severity:** OBSERVATION
- **Category:** performance
- **Location:** `src/query.rs:18-30`, `src/storage.rs:74-80`.
- **Evidence:** The bridge exposes one partition backed by the serial admitted
  native reader. It does not send column selection or predicates into the native
  scan. The former Iceberg provider accepted these through its scan implementation
  (`iceberg-datafusion-0.10.1/src/table/mod.rs:316-339`).
- **Disposition:** Acceptable for the sole production caller: `Engine::compare`
  already needs every result field and row (`src/engine.rs:315-335`). This does not
  establish parity for a future selective query API or a throughput improvement.
  Keep that boundary explicit when reusing this provider.

## Patch summary

`src/query.rs` connects the existing admitted native reader to maintained
DataFusion `StreamingTable` and `PartitionStream`. `Engine::compare` now creates
those providers from exact static native tables and the workspace's shared budget
(`src/engine.rs:290-313`). The comparison's signed occurrence-count query remains
unchanged, including its handling of duplicate multiplicity and null values.

`src/lib.rs:12` registers the module privately. `Cargo.toml:19-23` removes
`iceberg-datafusion` and directly enables the pinned DataFusion defaults. This
preserves the previously unified feature set: the removed crate's pinned manifest
declares `datafusion = "53.1.0"` without disabling defaults
(`iceberg-datafusion-0.10.1/Cargo.toml:54-55`). DataFusion defaults include the
previously explicit `parquet` and `unicode_expressions` features, plus the features
already enabled transitively (`datafusion-53.1.0/Cargo.toml:70-83`). The lockfile
diff removes the provider package and its direct DocSpec reference without
changing pinned DataFusion or native reader versions.

## Function trace

| Function / method | Location | Inputs | Output and verified behavior |
| --- | --- | --- | --- |
| `Engine::compare` | `src/engine.rs:272` | Two release pins and group name | Verifies both releases, opens exact pins, reports schema differences before querying, and otherwise computes added/removed multiplicities. Uses the new reader only at table registration. |
| CLI `Compare` dispatch | `src/main.rs:185-196` | Serialized releases and group | Calls `Engine::compare`; query errors escape through the application error conversion. No HTTP comparison handler was found. |
| `query::table` | `src/query.rs:18` | Native static table and shared budget | Converts the native current schema, stores one native partition and returns maintained `StreamingTable`. Does not resolve a catalog head. |
| `NativePartition::schema` | `src/query.rs:41` | Stored partition | Returns its retained native Arrow schema. |
| `NativePartition::execute` | `src/query.rs:45` | Execution context | Clones the retained table/budget into a lazy future; prepares a fresh reader on polling and adapts its stream without collecting batches. |
| `external` | `src/query.rs:59` | DocSpec error | Wraps the original error as DataFusion's external cause; F1 concerns the later conversion back. |
| `storage::open_pin` | `src/storage.rs:480` | Table pin | Hashes native metadata, opens static metadata and requires current snapshot/schema IDs to equal the pin. |
| `storage::read_batches` | `src/storage.rs:68` | Exact table and budget | Reserves capacity, builds serial native scan with configured row batches and retains its permit in stream state. Refuses batches exceeding the byte allowance. |
| `StreamingTable::scan` | `datafusion-catalog-53.1.0/src/streaming.rs:96` | Query column selection and limit | Creates maintained `StreamingTableExec`; filters are unsupported for pushdown under the default trait implementation. |
| `StreamingTableExec::execute` | `datafusion-physical-plan-53.1.0/src/streaming.rs:262` | Partition and task context | Starts the partition, applies requested column selection to each batch, cooperatively polls and applies any fetch limit. |

## Data flow and invariants

1. **Pin stability:** Comparison opens verified static metadata before constructing
   the provider (`src/engine.rs:290-313`). `NativePartition` owns that `Table`
   (`src/query.rs:34-37`) and clones it for every scan (`:46`). Selection changes
   elsewhere cannot change the stored metadata. The bridge has no catalog lookup.
2. **Schema interpretation:** The provider and partition receive the same schema
   derived by the maintained native conversion (`src/query.rs:19-29`). The
   maintained execution plan checks partition-schema equality before running
   (`datafusion-physical-plan-53.1.0/src/streaming.rs:84-97`). Native field metadata
   is retained; this patch does not implement evolving-schema reads.
3. **Filters are not silently skipped:** `StreamingTable` does not override filter
   support. The default returns `Unsupported`
   (`datafusion-catalog-53.1.0/src/table.rs:285-292`), so DataFusion keeps residual
   filters above the stream. Ignoring `_filters` in the maintained provider is
   consistent with that declaration. Column selection occurs after native decode.
4. **Finite, replayable input:** `StreamingTable::try_new` defaults to finite with
   no asserted sort order (`datafusion-catalog-53.1.0/src/streaming.rs:61-66`). Each
   `execute` constructs a new reader rather than sharing a consumed cursor
   (`src/query.rs:45-55`). The comparison does not rely on row order.
5. **Admission lifetime:** The preparation future captures a cloned shared budget.
   Once polled, `read_batches` owns the permit inside its unfold state
   (`src/storage.rs:72-95`). The adapter retains that stream. Completion, refusal
   or dropping the stream releases its state; there is no detached producer in
   this bridge. No claim is made that the reservation measures all process memory.
6. **Dependency equivalence:** The old transitive default-feature activation is
   now a direct dependency choice. Removing the adapter does not also remove SQL,
   string, nested-expression or compression support used elsewhere. No feature
   reduction is claimed.

## Test behavior and edge cases

These are assertions inspected statically, not reviewer execution results.

| Test | Location | Expected behavior and assertion trace |
| --- | --- | --- |
| `native_query_stream_keeps_admission_until_drop_and_enforces_batch_rows` | `src/query.rs:137` | First polling admits a native scan and returns the configured row-batch size. A competing reader fails while capacity is retained. Dropping the failed reader does not release the successful reader's permit; dropping the successful reader does. A fresh complete scan returns every fixture row and releases capacity. |
| `query_provider_preserves_pins_filters_duplicates_and_empty_schema` | `src/query.rs:176` | Registers the old pin, selects a new empty result, then queries the old provider twice. Both queries retain null and duplicate matching rows while excluding other rows. The empty provider keeps its field schema and returns no values. |
| `query_reader_refuses_oversized_native_batches_and_releases_capacity` | `src/query.rs:232` | An intentionally tiny byte bound fails during collection with the native reader's message; full admission is available after the failure. Initial assertion does not establish public error-code preservation (F1). |
| `comparison_preserves_duplicates_and_historical_release` | `tests/lifecycle.rs:599` | Calls the actual comparison path after building a deduplicated result and removing the original submitted input. Requires unchanged schema/input identity, no added occurrence and exactly one removed occurrence. |

No new direct multi-column reordering fixture was found; the bridge delegates
column selection to the maintained implementation whose `RecordBatch::project`
call was inspected. Full comparisons select every original field. No test in this
checkpoint establishes selective-read pushdown, general type coverage, schema
evolution or improved timing, and none is required for the stated change.

## Exploration notes

- **H1:** Replacing the provider could silently drop query filters. **Refuted:**
  unsupported pushdown and the residual-filter fixture preserve filtering.
- **H2:** A provider might follow a later catalog selection. **Refuted:** the
  captured static table is retained and repeated reads use it.
- **H3:** Removing `iceberg-datafusion` could disable SQL/expression features.
  **Refuted for this diff:** direct defaults preserve the former unified features.
- **H4:** Native refusal identities survive the adapter unchanged. **Refuted at
  the public application boundary:** F1 loses the outer stable code.
- **H5:** Reader cancellation releases owned admission. **Supported by the owned
  stream-state trace and direct drop/error assertions.** No execution claim is made.

## Closure inspection

**F1 is closed.** Revised `src/error.rs:72-87` traverses the retained error cause
chain. When it finds an embedded DocSpec error, it uses that error's stable code
and retains the original DataFusion error as the cause. A query error with no
embedded DocSpec refusal keeps the existing `query` code. The pinned DataFusion
implementation exposes both `External` and `Context` contents through `source()`
(`datafusion-common-53.1.0/src/error.rs:387-410`), so the inspected conversion covers
the actual bridge and context-wrapped failures without parsing error text.

The revised admission test converts a real competing-reader refusal through an
added DataFusion context and asserts `Error::code() == "capacity"` while retaining
the context message (`src/query.rs:152-159`). The oversized-reader test now
converts the collected query failure and asserts `"oversized_batch"`, retains the
original native-reader message, and checks released admission
(`src/query.rs:244-258`). These assertions cover the previously missing public
boundary. They were inspected but not executed by this reviewer.

One future schema-evolution boundary is explicit: the bridge advertises native
`current_schema`, as did the replaced provider (`src/query.rs:19-21`;
`iceberg-datafusion-0.10.1/src/table/mod.rs:266-271`), while the native scan chooses
the current snapshot's schema (`iceberg-0.10.1/src/scan/mod.rs:190-221`). Currently
emitted pins use matching schemas, and an empty scan retains the provider's
declared schema. A future schema-only change must qualify this relationship before
claiming R3 support. This checkpoint introduces no such change or support claim.

## Conclusion

**VERDICT: APPROVE** for the comparison-reader checkpoint.

- The native-read bridge achieves its stated row/pin and shared-admission design.
  Revised error conversion preserves the stable refusal code through DataFusion.
- **Coverage of changed paths: ADEQUATE** for values, filtering, repeatability and
  reader lifetime, including the added public-code assertions.
- **Confidence: HIGH** in the reviewed static traces. No benchmark, full process
  resource qualification or wider lifecycle approval is implied.

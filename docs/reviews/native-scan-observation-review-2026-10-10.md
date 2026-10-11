# Native scan observation: independent implementation review

VERDICT: APPROVE for the scoped implementation. No unresolved correctness or
resource finding remains. The error-lifetime test gap identified during review
was corrected and re-read. This is a static review, not a test result or a
performance qualification; the implementation owner owns execution.

Reviewed October 10, 2026 against HEAD
`967a72c1205eae08558d156c6fdbba1e1cd3483b` and the uncommitted changes in
`src/storage.rs`, `src/storage/occurrences.rs`,
`src/storage/occurrences/tests.rs`, and `src/storage/scan_metrics.rs`.
The Rust and semi-formal code-review skills informed the review. No Cargo
command, build, test, benchmark, or corpus read was run. Only this report was
written. Author design/research conclusions were not used to derive findings.

## Findings

### Resolved warning: observer state after a stream error lacked a regression

- Category: test coverage.
- Original gap: the new observer promised post-error sampling
  (`src/storage/scan_metrics.rs:15-17`), while the initial added fixture exercised
  successful consumption and partial stream drop. The existing failure test
  used the unobserved reader (`src/storage/occurrences/tests.rs:157-184`).
- Correction reviewed: `observed_reader_error_keeps_attempts_sampleable_without_new_ranges`
  now forces decoded-batch admission to fail after a real native read, checks
  positive retained bytes and one started scan, drops the stream, reacquires the
  reader budget, removes the temporary payloads, and samples the retained and
  cloned observer again (`src/storage/occurrences/tests.rs:188-222`).
- Result: the added case exercises the observer-bearing error path without
  introducing timing-dependent cancellation. Static trace predicts the stated
  assertions; execution is not claimed here.

### Observation: counters preserve the maintained reader's meaning

The metric represents requested native byte ranges after native range merging,
including attempts that fail or cancel. It does not establish successful bytes,
backend retry traffic, network bytes, manifest-planning cost, or complete
execution after an early stop. The API states those limits
(`src/storage/scan_metrics.rs:6-17,30-37`). Its scan counter counts calls to
`ArrowReader::read`, so one values scan can cover multiple files while the
occurrence reader starts one scan per file (`src/storage.rs:174-183`;
`src/storage/occurrences.rs:273-280`). This distinction is intentional and tested.

## Patch summary

The patch adds feature-gated observation entry points around the existing
readers. They return the same batch stream plus a separately retained metric
handle. Ordinary entry points pass `None`; observed entry points allocate one
observer and pass a clone into the same implementation
(`src/storage.rs:119-154`; `src/storage/occurrences.rs:65-108`).

The concrete improvement is access to the maintained Iceberg counter during the
actual qualification read, with constant additional retained state rather than
a second payload pass or per-file metric collection. This is a source-level
allocation/work property, not a measured throughput or process-memory result.

Important production callers remain the query partition's table and file paths
(`src/query.rs:321-349`) and scalar backfill processing
(`src/engine/backfill/preparation.rs:613-628`). The observed scalar helper is
unit-test-only (`src/storage/occurrences.rs:444-459`). Public observation exports
and entry points use `cfg(any(test, feature = "test-util"))`
(`src/storage.rs:13-24,134`; `src/storage/occurrences.rs:80`);
`test-util` is an existing feature (`Cargo.toml:31-33`).

## Function trace

| Function or method | Evidence | Inputs and output | Verified behavior |
| --- | --- | --- | --- |
| `read_batches` / `read_projected_batches` | `src/storage.rs:112-125` | Table, budget, optional selection → stream | Ordinary reads enter the shared implementation with no observer. |
| `read_batches_observed` | `src/storage.rs:135-146` | Same inputs → stream and observer | Constructs one observer, retains a caller clone, propagates setup errors. |
| `read_projected_batches_inner` | `src/storage.rs:149-200` | Optional observer plus original inputs → stream | Preserves expansion/admission/planning/selection, attaches once before extracting the native stream, finishes only at successful EOF. |
| `read_occurrences` / `read_projected_occurrences` | `src/storage/occurrences.rs:58-70` | Table, budget, optional selection → stream | Ordinary reads pass no observer. |
| `read_occurrences_observed` / inner helper | `src/storage/occurrences.rs:81-108` | Same inputs → stream and observer | Captures the same inventory and selection, then puts the observer into the stream state. |
| `Occurrences::capture_projected` | `src/storage/occurrences.rs:148-207` | Table, budget, inventory, location flag, selection → state | Keeps original batch and metadata reservations, serial reader options, and initializes observation to absent. |
| `Occurrences::next` | `src/storage/occurrences.rs:210-289` | Mutable state → batch, EOF, or error | Checks decoded counts and task coverage; finalizes a native counter only after successful per-file EOF, then attaches the next file's counter. |
| `OccurrenceScan::read_projected_file` | `src/storage/occurrences.rs:404-425` | Captured path, budget, selection → stream | Keeps original selection construction and passes no observer. |
| `read_scalar_file` / observed helper / inner helper | `src/storage/occurrences.rs:435-494` | Captured path, budget, scalar selection → stream and selection flag | Both variants share the existing primitive-column evidence check and full-read fallback. Only the test helper supplies an observer. |
| `read_selection_file` | `src/storage/occurrences.rs:497-526` | Captured task/file, selection, optional observer → stream | Transfers the observer into the same admitted one-file state; payloads remain lazy. |
| `NativeScanObservation::attach` | `src/storage/scan_metrics.rs:58-63` | Native result → active metric handle | Asserts serial attachment, clones only the native metric, increments reader-execution count. |
| `NativeScanObservation::finish` | `src/storage/scan_metrics.rs:65-70` | Active metric → completed byte total | Takes and accumulates the handle once; a second call cannot count it again. |
| `NativeScanObservation::sample` | `src/storage/scan_metrics.rs:48-55` | Observer → copied sample | Reads completed plus active counters under a short mutex; does no I/O and retains no batches. |

## Dependency evidence and data flow

`Cargo.lock:2695-2698` pins Iceberg 0.10.1;
`Cargo.lock:2348-2351` pins futures-util 0.3.34. The cached sources inspected are
under `/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/`.
The following dependency references are relative to that directory.

1. Native `ScanMetrics` owns only `Arc<AtomicU64>`; cloning a metric does not keep
   a reader, batch, task inventory, or payload alive
   (`iceberg-0.10.1/src/arrow/scan_metrics.rs:52-72`). `ScanResult::stream` moves
   out the stream; cloning its metrics first preserves access to the same atomic
   counter (`iceberg-0.10.1/src/arrow/scan_metrics.rs:75-95`).
2. `CountingFileRead::read` increments the requested range length before awaiting
   the underlying read (`iceberg-0.10.1/src/arrow/scan_metrics.rs:42-49`). The
   Arrow adapter combines nearby ranges before issuing them
   (`iceberg-0.10.1/src/arrow/reader/file_reader.rs:70-105`). Footer metadata uses
   this reader too (`iceberg-0.10.1/src/arrow/reader/pipeline.rs:412-449`).
3. `ArrowReader::read` creates one metric and passes clones to the data and delete
   readers. With data-file concurrency one it uses the serial `and_then` and
   `try_flatten` path (`iceberg-0.10.1/src/arrow/reader/pipeline.rs:48-88`). The
   existing DocSpec readers already selected that concurrency; this patch does
   not add native scans or enable extra reads.
4. Delete loading uses the same metric when opening a delete Parquet file
   (`iceberg-0.10.1/src/arrow/delete_file_loader.rs:65-91`). The native loader can
   spawn work that outlives a failed data-read future
   (`iceberg-0.10.1/src/arrow/caching_delete_file_loader.rs:168-232`). Successful
   batch production awaits the delete-loader result and equality predicate
   (`iceberg-0.10.1/src/arrow/reader/pipeline.rs:264-265`); loaded delete content is
   parsed before the loader sends success
   (`iceberg-0.10.1/src/arrow/caching_delete_file_loader.rs:295-325`). Thus
   successful native EOF is an appropriate point to freeze its counter, while
   error/drop must keep the live handle. Occurrence scans already reject delete
   tasks (`src/storage/occurrences.rs:530-545`).
5. `try_unfold` moves its state into each polling future, clears that future on
   completion, and preserves state only for `Ok(Some(...))`. On error, owned
   reader state and reservations drop while the returned observer's `Arc`
   survives (`futures-util-0.3.34/src/stream/try_stream/try_unfold.rs:95-118`).

The observer's mutable state consists of completed bytes, started scans, and at
most one active metric (`src/storage/scan_metrics.rs:23-28`). `sample`, `attach`,
and `finish` serialize transitions without awaiting while holding the mutex.
At a file boundary, `finish` runs before the next `attach`
(`src/storage/occurrences.rs:244-278`). On errors, that active handle remains in
the caller-owned state. Once it is inactive, its value is folded into a scalar,
so retained observer memory is O(1) in files and rows. Existing inventories and
native processing retain their existing bounds; this review does not establish
a new bound on native decoder or background-worker memory.

The relevant invariants are therefore: one active native scan per observer;
finish only after native EOF; count each completed handle once; preserve the
active handle on cancellation/error; and keep ordinary call paths observer-free.
The first invariant is asserted, and the public API does not accept a caller's
observer for reuse. Breaking the EOF rule would lose late native increments;
adding an active-handle collection would lose the constant-state property.

## Test behavior and edge cases

All predictions below are static. No pass result is inferred from the test code.

| Test or scenario | Evidence and trace | Assessment |
| --- | --- | --- |
| Full versus selected values, occurrences, and scalar file reads | `observed_projection_requests_fewer_ranges_with_exact_values_and_occurrences`, `src/storage/occurrences/tests.rs:383-622`, creates deterministic wide payloads, checks exact generated values, uses an independent synchronous Parquet reader for positions, checks schemas and scan counts, and compares requested ranges. | Expected to preserve values and reduce requested bytes for the narrow selection; actual numeric reduction requires execution. |
| Partial occurrence-stream drop | Same test, `src/storage/occurrences/tests.rs:610-621`: initial zero sample, real batch, positive bytes, drop, nondecreasing sample and unchanged scan count. | Exercises the retained active handle; it correctly does not demand an exact canceled-I/O total. |
| Observer after real-read admission error | `src/storage/occurrences/tests.rs:188-222`: tiny batch allowance reaches `strip_file_column` refusal, then retains and clones the observer after drop and payload removal. | Expected to preserve attempts and release the original reservations. Closes the review finding. |
| Ordinary read reservations and failure | `src/storage/occurrences/tests.rs:157-184`: partial/drop, EOF, and oversized-batch paths reacquire budget. | Existing behavior remains in the shared implementation; observer code is absent through `None`. |
| Task/count refusals | `src/storage/occurrences/tests.rs:313-341`: omitted/repeated tasks and lower/higher expected counts. | Coverage checks remain before successful completion; observer cannot turn failure into EOF. |
| Query column selection | `src/storage/occurrences/tests/selection.rs:324-379`: corrupts an unselected column and reads `id` through located/unlocated and captured/table providers. | Existing projection behavior remains shared with observed paths. |
| Setup failure before a stream returns | `src/storage.rs:155-179`; `src/storage/occurrences.rs:102-104`. | No observer is returned; local setup state drops. No new metric promise exists for failed construction. |
| Empty input | Values reader attaches one native execution; occurrence reader encounters task EOF before attachment (`src/storage.rs:174-191`; `src/storage/occurrences.rs:255-278`). | Counters follow reader executions, so the different scan counts are valid. Existing empty occurrence test is at `src/storage/occurrences/tests.rs:143-153`; no new observed-empty assertion was added. Low risk. |
| Concurrent sampling during detached native failure work | Native atomic plus observer mutex; no locks span await. | Source-supported behavior, not directly exercised by the deterministic fixtures. Samples remain live observations, not cancellation completion. |

## Review reasoning and conclusion

- H1: factoring ordinary and observed entry points changes read selection or
  admission. Refuted by the preserved setup, reader options, selection, and
  refusal paths; only the optional observer state is added.
- H2: retaining metrics retains each completed file or payload. Refuted by the
  observer state and the native metric's single atomic ownership.
- H3: errors or early drop lose observation access or falsely freeze in-flight
  work. Refuted by the independent observer clone and absence of `finish` on
  either path; the corrected error fixture now exercises this ownership path.
- H4: successful EOF can fold an incomplete native total. Refuted for the traced
  serial reader by awaited data/delete processing before native EOF. Counter
  semantics still exclude planning/backend traffic as documented.

VERDICT: APPROVE

- The patch exposes maintained native requested-range observations while sharing
  the ordinary reader behavior and retaining constant additional observer state.
- Coverage of changed paths: ADEQUATE for this scoped checkpoint after the
  error-lifetime fixture was added. Native background cancellation and hosted
  transport behavior remain outside the executed-evidence scope of this review.
- Confidence: HIGH for the static ownership, control-flow, and dependency
  conclusions. Build/test and performance outcomes require the owner's logs.

Reviewed-file SHA-256 values:

```text
18f4294f2878629a5eba227f76cd90e77cf2312247642e18a9c5a1fd0dd63d15  src/storage.rs
3196562df8c323f149aa6b26b3dc94845748e5d83634fce296fe6acb30d755a9  src/storage/occurrences.rs
e92baa58560c4fd125f0547407a50907030abf7750da61ed26e9847b0b2173f8  src/storage/occurrences/tests.rs
3b4a4053f4289a9cfde8ebb3cdcdfb2813a9ff3d868d5da1a216a017c96f58a3  src/storage/scan_metrics.rs
```

# Query reader review: admission and resource lifetime

**VERDICT: APPROVE the scoped query-reader checkpoint.** One actionable finding
was repaired during review. The final source preserves reader admission and
refusal codes without introducing a separate execution system.

Static review on 2026-10-08 of uncommitted changes relative to `f85c74f`, applying
the semi-formal-code-review and relevant Rust guidelines. Scope: `src/query.rs`,
the comparison caller, error conversion, module registration, dependency changes,
and the corresponding README/status updates. The reviewer did not execute tests
or change implementation files.

## Findings

### QR1 — DataFusion wrapping hides stable reader refusal codes

**Severity: WARNING. Category: correctness / operations. Status: resolved.**

The initial adapter maps reader failures to
`DataFusionError::External(Box<crate::Error>)` (`src/query.rs:59–61`). The existing
generic `From<DataFusionError>` conversion assigned every such error the code
`query`. `Engine::compare` applies that conversion to `collect().await?`
(`src/engine.rs:339`), so new admission and batch failures lost their actionable
`capacity` and `oversized_batch` codes at the application boundary. Tests that
only examined display text did not establish the structured behavior.

The repaired conversion walks the error-source chain, copies the first embedded
DocSpec refusal code, and retains the complete original DataFusion error as the
cause (`src/error.rs:72–87`). Errors without a DocSpec cause still use `query`.
This also handles DataFusion context wrappers without parsing error messages.

The adapter tests now check `capacity` after wrapping the real competing-reader
failure in `DataFusionError::Context`, preserving the context text
(`src/query.rs:152–159`). A real oversized native batch passes through DataFusion
collection and the ordinary conversion, then asserts `oversized_batch`
(`:244–258`). These close the reported path.

No other actionable defect was found in the inspected checkpoint.

## Patch summary

`Engine::compare` now connects an exact native snapshot to maintained DataFusion
`StreamingTable` and `PartitionStream` interfaces through the existing
`storage::read_batches` function (`src/engine.rs:309–314`). This makes comparison
use the same configured batch size, serial native decoding and shared admission
as independent reads. The adapter stays private (`src/lib.rs:12`).

The removed `iceberg-datafusion` provider ignored the supplied task context and
built default native scans. The new adapter also does not derive limits from
`TaskContext`, but explicitly carries the engine's shared `Budget`; those are the
limits it enforces. It remains a finite full-table reader.

Removing `iceberg-datafusion` also removes a source of Cargo feature unification.
The prior dependency enabled DataFusion's default features implicitly
(`iceberg-datafusion-0.10.1/Cargo.toml:54–55`). The direct dependency now enables
those defaults at the same pinned DataFusion version (`Cargo.toml:19`), preserving
that effective feature selection. The lockfile removes the unused adapter and
does not change dependency versions. This is a resource-control repair and a
dependency simplification; it is not a measured speed or binary-size improvement.

## Function trace

| Function / type | File:line | Input → output | Verified behavior |
| --- | --- | --- | --- |
| `Engine::compare` | `src/engine.rs:272` | Two release pins and group name → scalar duplicate-aware comparison | Verifies both releases, opens their exact native metadata, checks schemas, registers the new providers, and aggregates complete occurrence counts. |
| CLI comparison caller | `src/main.rs:185` | Bounded release JSON → `Engine::compare` | Uses the same application method; no separate CLI reader or comparison logic. |
| `query::table` | `src/query.rs:18` | Owned native table and shared budget → `TableProvider` | Converts the exact native schema and creates one finite streaming partition. No payload is collected or catalog head refreshed here. |
| `NativePartition::schema` | `src/query.rs:41` | Partition → shared schema | Supplies the same schema used to construct the maintained provider. |
| `NativePartition::execute` | `src/query.rs:45` | Task invocation → lazy record-batch stream | Clones the immutable native table and shared budget; prepares a reader when polled; owns its stream through `once(...).try_flatten()`. |
| `external` | `src/query.rs:59` | DocSpec error → DataFusion error | Preserves the original typed cause rather than reducing it to a string. |
| `From<DataFusionError> for Error` | `src/error.rs:72` | Wrapped query error → application error | Recovers embedded DocSpec refusal codes and retains original context; unrelated query errors keep their existing code. |
| `storage::read_batches` | `src/storage.rs:68` | Exact table and shared budget → bounded native stream | Reserves two batch allowances before scan preparation, sets native concurrency to one and explicit batch rows, checks each decoded batch, and retains its permit in stream state. |
| `Budget::reserve` | `src/budget.rs:101` | Requested admission → owned semaphore permit/refusal | Refuses immediately when shared capacity is unavailable; it does not wait while another reader holds capacity. |

Pinned dependency implementations were read in the local Cargo registry under
`/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/`:

- `datafusion-catalog-53.1.0/src/streaming.rs:46–66` constructs a finite table.
  Its `scan` creates the maintained execution plan (`:96–134`).
- `datafusion-physical-plan-53.1.0/src/streaming.rs:262–285` calls the supplied
  partition stream, applies column selection after reading, wraps cooperative
  polling, and applies a limit when present.
- `datafusion-catalog-53.1.0/src/table.rs:285–300` defaults to unsupported filter
  pushdown and no provider statistics. The streaming provider does not override
  these defaults.
- `iceberg-0.10.1/src/scan/mod.rs:484–498` forwards the configured batch size and
  file concurrency into the maintained Arrow reader. Its serial branch uses one
  file stream at a time (`src/arrow/reader/pipeline.rs:64–74`).
- `futures-util-0.3.32/src/stream/try_stream/try_flatten.rs` stores the active inner
  stream as an owned field and removes it at exhaustion; dropping the outer
  stream also drops that field.

## Data flow and invariants

| State / resource | Creation, modification and use | Invariant / boundary |
| --- | --- | --- |
| Native pin | Verified release → static native table → owned partition (`src/engine.rs:290–313`; `src/query.rs:22–26`) | Later selection changes do not change the table read by an existing provider. |
| Schema | Native schema conversion → partition and `StreamingTable` share one `Arc` (`src/query.rs:19–29`) | Empty results retain their typed schema; the adapter does not infer types from payload rows. |
| Reader lifetime | Lazy preparation future → admitted native stream → flattened adapter stream (`:45–55`) | Before polling, no reader permit has been acquired. During preparation and consumption, the future or returned stream owns admission. Drop cancels that owned work and releases its permit. |
| Reader capacity | `Budget` clones share the same semaphore (`src/budget.rs:40–43`; `src/storage.rs:72`) | Each executed scan acquires its own admission. Registering another provider does not create a new independent capacity pool in production. |
| End or failure | `read_batches` state returns `None` or an error (`src/storage.rs:81–94`) | The state and reservation drop on exhaustion or failure; keeping the provider registered does not retain an active-reader permit. |
| Query execution | One serial source partition → maintained DataFusion operators (`src/query.rs:27–30`; `src/engine.rs:328–339`) | Downstream aggregation uses the engine's existing shared runtime. The native scan is not made parallel by DataFusion's target partition count. |
| Refusal identity | Native/application refusal → boxed external error → source-chain conversion (`src/query.rs:59`; `src/error.rs:72`) | Resource codes remain machine-readable while query context remains available. |

The adapter introduces no detached tasks, collector, queue or manual cancellation
protocol. Native Iceberg planning has its own spawned work
(`iceberg-0.10.1/src/scan/mod.rs:401–477`); this review does not establish that every
upstream I/O operation has stopped at the instant the application stream is
dropped. It establishes ownership of the application permit and source stream.
Native cleanup timing and aggregate resident memory remain broader qualification
work, as they were for the reused reader.

Review hypotheses: the provider might accidentally be infinite (refuted by the
maintained constructor); adapter wrapping might release admission after creation
(refuted by owned stream state); filters might silently be skipped (refuted by
unsupported pushdown leaving DataFusion responsible for filtering); and all
reader failures might preserve stable codes (initially refuted, then repaired).

## Test behavior and concrete edge cases

- `native_query_stream_keeps_admission_until_drop_and_enforces_batch_rows`
  (`src/query.rs:137`) configures admission for exactly one reader. The first
  decoded batch respects the row limit, a competing scan refuses, dropping only
  the competitor does not free the first reservation, dropping the first does,
  and a new full scan returns all occurrences and frees capacity at exhaustion.
  The context-wrapped refusal assertion also exercises QR1.
- `query_provider_preserves_pins_filters_duplicates_and_empty_schema` (`:175`)
  registers the old native table, selects a new empty result, then queries the
  retained provider twice. It preserves nulls and duplicate values after filtering
  and aliasing, and keeps the empty provider's column schema. This exercises the
  maintained provider path rather than only the partition implementation.
- `query_reader_refuses_oversized_native_batches_and_releases_capacity` (`:231`)
  forces a decoded-batch refusal through normal query collection, checks the
  recovered code and message, and reacquires the full reader allowance afterward.
- `comparison_preserves_duplicates_and_historical_release`
  (`tests/lifecycle.rs:599`) reaches the changed production caller and checks the
  expected duplicate-count difference between exact historical results.

These assertions match the traced behavior. The reviewer read the implementation
owner's gate log, now retained at `docs/reviews/query-reader-check.log`: the build
checks and unit, lifecycle, update and documentation test sections completed
successfully. This is observed
execution evidence from the owner, not execution performed by this reviewer.

## Performance and scope limits

Column selection occurs after native decoding. Filters run in DataFusion, and the
adapter supplies no native predicate pushdown or provider statistics. A narrow
future query may therefore decode more data than a specialized provider. The
current production caller compares all fields of complete results, so this
checkpoint does not claim a selective-query optimization.

Native file decoding is serial per reader. Comparison can run admitted before
and after readers concurrently, while downstream DataFusion operators retain
their configured execution behavior. This makes reader capacity explicit; it
does not prove faster comparison. Both releases are still fully verified before
the comparison scans (`src/engine.rs:290–291`).

The decoded-byte check occurs after decoding. Reader admission is not a bound on
all Parquet buffers, metadata allocations or process memory. This review makes no
new resident-memory, request-count or timing claim, and does not close the full
Comments performance gate.

## Conclusion

**VERDICT: APPROVE.** The patch achieves its stated resource-control intent with
a small adapter around maintained interfaces. QR1 is repaired and covered.
Coverage is **ADEQUATE** for this checkpoint; confidence is **HIGH** in the static
lifetime and dependency traces. General selective querying, native cancellation
timing, aggregate resource bounds and complete lifecycle performance remain
outside this approval.

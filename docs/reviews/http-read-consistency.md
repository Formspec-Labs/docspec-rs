# Exact-version HTTP read consistency review

Review date: 2026-10-08. Scope: uncommitted read changes relative to `221ed8f`. This is a static semi-formal review; the reviewer did not execute Cargo, tests, or benchmarks. Resource accounting has a separate reviewer.

## Findings first

### F1 — Requested identity was not checked against the resolved release — CLOSED

- **Severity:** BLOCKER; correctness.
- **Location:** `src/http.rs:173`, calling `src/engine.rs:284`.
- **Initial evidence:** `Engine::release` read `result/{version}` from the requested dataset's lifecycle table and returned its decoded value. It did not check the returned dataset or version against those lookup arguments. Independent verification checks the returned release against its own immutable manifest (`src/engine.rs:1393–1418`), which does not establish that it is the release requested by the URL.
- **Failure sequence:** Retain two valid releases A and B; rebind `result/A` to B's complete release value. `GET /datasets/D/versions/A/arrow` returns B's independently valid bytes and B's response headers with status 200. Rebinding to a valid release from another dataset has the same issue. Correctly generated catalogs do not produce this state, but the read path promises exact identity and otherwise refuses malformed retained state.
- **Required closure:** Check `release.dataset.dataset == dataset`, `release.id == version`, and `release.dataset.version == version` in the shared getter. Add rebound-result fixtures covering wrong version and wrong dataset, and require a structured refusal before starting an IPC body.
- **Closure:** `src/engine.rs:294–302` now refuses a mismatched dataset, either mismatched version identity, or an unverified result. The guard is in the shared resolver before either HTTP reader starts. `rebound_catalog_results_cannot_change_an_exact_version_url` (`tests/http_read.rs:318`) writes a distinct valid release under the old key, then separately changes dataset identity, verification state and the dataset version; both the shared getter and HTTP route must return `release_mismatch`. Owner-run targeted tests pass. **Status: CLOSED.**

### O1 — Physical-group and catalog-free extraction coverage — CLOSED

- **Severity:** OBSERVATION; test coverage.
- **Evidence:** Initial HTTP fixtures exercise historical `/source` and the combined view (`tests/http_read.rs:36`). The shared reader is also used for physical computed groups and CLI extraction (`src/engine/read.rs:30`, `src/main.rs:135`).
- **Closure:** The HTTP value test now decodes `/arrow/stats`, checks the exact physical field names and computed values (`tests/http_read.rs:107–154`). `catalog_free_cli_uses_the_shared_group_reader` (`tests/http_read.rs:269`) hides the producer catalog, invokes the actual CLI, verifies no catalog was recreated and compares independently decoded IPC rows with the Rust fixture. Owner-run targeted tests pass. **Status: CLOSED.**

## Patch summary

The patch adds exact-version Arrow IPC endpoints for the combined logical view and individual physical groups. It extracts the CLI's catalog-free group read into one reusable application function, keeps the host's shared budget on HTTP reads, and encodes batches on body demand through maintained Arrow and Axum components. It adds no publication selection, lifecycle mutation, alternate mutable head, or producer queue.

The implementation reduces duplicate CLI/group reader setup. This review makes no throughput or peak-memory improvement claim.

## Function trace

| Function or path | Location | Inputs → outputs | Verified behavior |
| --- | --- | --- | --- |
| `router` | `src/http.rs:23` | Engine → Router | Installs separate combined and physical-group GET routes under a dataset/version path. Existing bounded metadata routes remain shared. |
| `read_combined`, `read_group` handlers | `src/http.rs:153`, `src/http.rs:160` | Extracted path → response | Both delegate to one read handler; group names only index the pinned group map. |
| HTTP `read` | `src/http.rs:167` | Dataset/version/group → streaming body | Resolves once, independently verifies and constructs a read stream, then adds release identity headers. Captured Engine keeps workspace ownership until the response stream drops. The shared resolver now binds both returned version fields and dataset identity to the lookup arguments. |
| `Engine::release` | `src/engine.rs:284` | Dataset/version → retained ReleasePin | Uses the per-dataset lock and exact lifecycle result key; no latest lookup. Refuses result identity mismatch and unverified records before returning a pin. |
| Engine group wrapper / `read_budget` | `src/engine/read.rs:11`, `src/engine/read.rs:19` | Host and release → stream/budget clone | Reuses host admission rather than creating an independent HTTP allowance. |
| Free `read_group` | `src/engine/read.rs:30` | Release/group/shared Budget → schema-carrying native stream | Refuses missing group, verifies complete release, opens static native metadata, preserves native schema and wraps native errors with their source. Does not open a catalog. |
| `verify_release_with_budget`, `verify_table` | `src/engine.rs:1385`, `src/engine.rs:1325` | Exact pins → verification/refusal | Checks manifest equality, request/input binding, object hashes, native inventory and row counts, change endpoints, and computed coverage before returning a stream. |
| Existing `Engine::read_combined` | `src/engine/backfill.rs:87` | Verified release → bounded combined batches | Opens pinned source/groups and joins by exact file/ordinal occurrence. Business-key duplicates and nulls do not select field values. Projects user columns without physical locators. |
| `open_pin`, `read_batches` | `src/storage.rs:507`, `src/storage.rs:73` | Native pin/table → maintained scan | Opens immutable metadata without a catalog; checks current snapshot/schema against the pin; serial native reads retain admission and refuse oversized batches. |
| `encode` | `src/http/ipc.rs:11` | Schema/batch stream/Budget → IPC byte chunks | Writes schema first, requests one batch on demand, refuses dictionary schemas and changed batch schemas, emits the success terminator only after source exhaustion, and propagates source/encoding errors. |
| `Chunk::as_ref`, `Sink::write`, `Sink::flush` | `src/http/ipc.rs:92`, `src/http/ipc.rs:103`, `src/http/ipc.rs:120` | Owned bytes or writer input → bytes/write result | Chunks own transferred buffers and reservations. Sink checks length before extension and emits no partial successful chunk when encoding fails. Flush is empty because there is no internal sink buffering. |
| CLI `Command::Read` | `src/main.rs:135` | Serialized release/group/output → IPC file | Calls the free verified reader before creating an output file, writes its schema and batches, and finishes only after successful exhaustion. It still uses `create_new` and does not open Engine. |

Dependency behavior inspected locally: Arrow `58.4.0` `StreamWriter::try_new`, `write`, `finish`, and `get_mut` in `arrow-ipc/src/writer.rs:1400–1490`; Axum Core `0.5.6` `Body::from_stream` and `StreamBody::poll_frame` in `src/body.rs:59–70,209–221`; Futures Util `0.3.34` `TryUnfold::poll_next` in `src/stream/try_stream/try_unfold.rs:95–121`. Arrow's stream writer is unbuffered and writes its terminator only when explicitly finished. The HTTP codec now refuses dictionary schemas, including nested fields, using maintained Arrow schema traversal (`arrow-schema/src/schema.rs:364` and `src/field.rs:688–708`). The separate resource review identified why persistent dictionary state was outside the fixed admission assumption. Axum exposes a stream failure as a body error; `try_unfold` drops the failed state and has no later success state.

## Data flow and invariants

1. **Exact release selection:** URL identifiers enter the lifecycle result lookup once; the returned pin drives verification, native reads, and headers. A later selection cannot retarget those objects. The shared resolver enforces the comparison between URL identity and returned pin (`src/engine.rs:294–302`).
2. **Independent retained evidence:** Verification precedes successful response construction. A valid table alone is insufficient: retained inputs, source objects, changes, computed coverage and manifest bindings must agree. This deliberately performs complete verification before first-byte streaming; the patch is not a metadata-only read path.
3. **Schema and values:** The physical reader carries native schema even with no data batches. The combined reader derives its schema from the maintained query plan. Current emitted source and group pins use matching snapshot/current schemas; broader schema-only evolution remains outside this checkpoint.
4. **IPC continuity:** One writer persists across chunks. Taking the sink buffer leaves an empty destination for the next schema/batch/terminator message. Dictionary schemas refuse before response construction, and every actual batch must match the declared schema before encoding. There is no custom Arrow framing or serialized-row conversion.
5. **Failure is not success:** A failed input read, oversized batch or writer failure returns an error before transferring that chunk and before `finish`. A client must check HTTP body completion; Arrow readers may accept EOF without requiring the optional IPC terminator, so decoding a prefix alone is not successful HTTP completion.
6. **Ownership and cancellation:** The response stream captures Engine, native scan state and writer state. Drop stops demand for additional batches and releases owned state; already transferred chunk clones retain their own byte reservation. This is a local stream-lifetime property, not a claim that every upstream blocking operation is immediately interruptible.
7. **No lifecycle side effect:** None of the new read or encoder paths select, admit, invalidate, or update a dataset. HTTP resolves exact retained results rather than maintaining a second current pointer.

## Test behavior and edge cases

| Test | Location | Assertion and static trace |
| --- | --- | --- |
| `http_reads_exact_historical_source_and_combined_values_in_bounded_frames` | `tests/http_read.rs:22` | Reads historical source after enrichment, then combined values. Checks exact header identity, independently decodes IPC, compares duplicate/null/empty/Unicode rows to Rust expectations, and checks the computed length. Separate schema/data/end frames are asserted. Physical computed-group schema and values are also decoded and checked. |
| `empty_streams_keep_schema_and_known_failures_return_structured_errors` | `tests/http_read.rs:162` | Both source and combined empty outputs retain schema with zero rows. Missing group/version returns structured 404. Corrupted retained input refuses before streaming. |
| `in_flight_response_keeps_workspace_ownership_until_canceled` | `tests/http_read.rs:239` | Drops router ownership while retaining an unconsumed response; another workspace owner is refused. Dropping the response permits reopening the same selected result. |
| `demand_controls_batch_reads_and_chunks_keep_admission_until_last_clone_drops` | `src/http/ipc.rs:158` | Poll counter proves schema emission does not consume a batch; one data demand consumes one batch; dropping the stream does not poll another; a retained chunk clone keeps admission. |
| `encoding_preserves_strings_nulls_duplicates_and_complete_empty_streams` | `src/http/ipc.rs:192` | Independent Arrow decoding equals expected batches, including changed strings, duplicate values and nulls. Empty case preserves schema. Successful output ends in the explicit IPC terminator. |
| `failures_abort_the_body_without_emitting_a_successful_end` | `src/http/ipc.rs:259` | A failure after a successful batch makes Axum body collection fail. Oversized batches produce the stable refusal after schema emission. Dropping failed state restores admission. |
| `dictionaries_and_changing_batch_schemas_refuse` | `src/http/ipc.rs:225` | Top-level and nested dictionaries refuse before polling any batch; a batch with a different field schema refuses with `invalid_schema` after the schema frame. |
| `catalog_free_cli_uses_the_shared_group_reader` | `tests/http_read.rs:269` | Actual CLI extraction works with catalog hidden, does not recreate it, and returns exact duplicate/null/empty/Unicode rows. |
| `rebound_catalog_results_cannot_change_an_exact_version_url` | `tests/http_read.rs:318` | Distinct valid release replacement and individual identity/verification corruptions refuse in the shared getter and HTTP route before streaming. |
| `encoded_chunks_refuse_before_growing_beyond_their_admission` | `src/http/ipc.rs:292` | A write crossing the sink limit leaves the previous bytes intact; an exact-limit write succeeds. |

Owner-run targeted results were inspected in `/tmp/docspec-http-ipc-tests-2.log` and `/tmp/docspec-http-read-tests-3.log`: encoder and HTTP tests pass, including the new identity, physical-group and catalog-hidden CLI cases. The initial fixture type failure is retained in `docs/reviews/http-read-first-failed.log`; the fixture now explicitly declares BIGINT rather than assuming the return type of `length`. The final `./scripts/check` and `cargo check --locked --no-default-features` completed successfully (owner-reported exit 0). The retained `docs/reviews/http-read-check.log` was inspected through the final test suites, doc tests and no-default-features build; it includes the new encoder, HTTP and catalog-free CLI cases. The later source change only clarifies the public resolver's error documentation. The reviewer did not execute tests. README and STATUS describe complete pre-stream verification, dictionary refusal, failed-body handling, and the limits of local adapter qualification (`README.md:200–208`, `docs/STATUS.md:380–424`). Socket/proxy behavior, remote authentication/publication, production throughput, and an aggregate process-memory guarantee are not qualified by this review.

## Conclusion

**VERDICT: APPROVE**

- The shared resolver now enforces the requested identity; independently verified source, combined and physical-group values reach one maintained IPC writer without lifecycle mutation. F1 and O1 are closed.
- Coverage of changed paths: **ADEQUATE** for this local read checkpoint.
- Confidence: **HIGH** in static consistency and inspected targeted/full-gate results. This verdict does not claim remote or performance qualification.

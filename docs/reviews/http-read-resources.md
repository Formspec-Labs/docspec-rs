# HTTP bulk-read resource review

No open actionable findings remain. The dictionary-retention issue identified during review is closed by an explicit schema refusal and actual-batch schema checks. **VERDICT: APPROVE** for this local HTTP adapter checkpoint.

This is an independent static review of the uncommitted change against `221ed8f`. The reviewer read implementation, relevant callers and callees, pinned dependency source, tests, documentation, and owner-produced gate output. The reviewer did not execute tests or modify implementation. The review applies the semi-formal-code-review workflow and relevant Microsoft Rust performance, resilience, interoperability, and library guidance.

## Findings and closure

### HR1 — Persistent dictionary buffers could outlive the fixed encoder allowance — CLOSED

- **Severity/category:** WARNING / performance and resource correctness.
- **Location:** `src/http/ipc.rs:15`, `src/http/ipc.rs:54`; pinned `arrow-ipc-58.4.0/src/writer.rs:931`.
- **Evidence chain:** Arrow's dictionary tracker retains an earlier dictionary's `ArrayData` when a later dictionary has equal values (`writer.rs:937–956`). With several dictionary columns, successive batches could give different columns large backing buffers and later provide equivalent compact dictionaries. Each current batch could fit the configured batch allowance while the tracker retained large earlier buffers from multiple columns. A single fixed encoder reservation did not establish a bound on that accumulated state.
- **Resolution:** Before allocating the writer, `encode` uses maintained `Schema::flattened_fields` to reject top-level and nested dictionary types. Pinned Arrow schema traversal follows nested fields (`arrow-schema-58.4.0/src/schema.rs:364`, `src/field.rs:688–708`). Each actual batch must equal the declared schema before encoding, preventing a later batch from bypassing the initial check. `dictionaries_and_changing_batch_schemas_refuse` verifies both dictionary cases and an inconsistent batch schema (`src/http/ipc.rs:225`). This is a smaller qualified boundary than introducing dictionary-state accounting that native Iceberg output does not require.

### Resource and performance limits — documented observations

- **Eager verification remains population-dependent.** Both reader services verify the complete retained release before returning a source stream (`src/engine/read.rs:38`, `src/engine/backfill.rs:92`, `src/engine.rs:1385`). A group-only request also verifies other retained release dependencies. Streaming removes whole-result HTTP accumulation; it does not remove those scans or establish fast time to first byte. README states this at `README.md:200–208`.
- **Reservations are admission controls, not exact process-memory accounting.** Arrow builds temporary `EncodedData` and FlatBuffer buffers before it calls the capped sink (`arrow-ipc-58.4.0/src/writer.rs:470–495`, `520–606`, `1436–1457`). `Sink::write` caps logical outgoing bytes before appending, but that check does not bound prior upstream allocations or allocator rounding (`src/http/ipc.rs:103–118`). `docs/STATUS.md:399–406` states the limitation. No throughput or aggregate resident-memory qualification is claimed.
- **Cancellation stops demand at this adapter.** Dropping the body drops its owned stream and encoder, but already running upstream native or blocking work may have its own cancellation lifetime. Combined reads retain the existing DataFusion joins and memory controls (`src/engine/backfill.rs:103–130`). This patch introduces no encoder producer task or queue; it does not establish that every upstream task becomes quiescent immediately. The distinction is explicit in `docs/STATUS.md:389–396`.

## Patch summary

The patch adds exact-version HTTP Arrow IPC reads for a physical field group and the combined source/scalar result. It extracts the CLI's verified group reader into an injected-budget application function, then uses the same function from HTTP. The shared release getter additionally checks that the returned dataset and both version identities match the requested lookup and that the result is verified.

The new codec uses maintained Arrow framing and Axum bodies. It emits schema, batches, and completion separately on consumer demand. A small capped sink owns the current output buffer, and transferred `Bytes` owns its admission permit. This removes duplicate CLI reader setup without adding a separate query engine, durable index, producer queue, or custom IPC format. The source diff demonstrates the reduction in duplicated setup; no measured speed improvement is asserted.

## Function trace

| Function / method | File:line | Inputs | Output | Verified behavior |
| --- | --- | --- | --- | --- |
| `router` | `src/http.rs:22` | Shared Engine | Axum Router | Registers exact-version combined and physical-group routes on the existing host. |
| `HttpError::into_response` | `src/http.rs:53` | Application error | Status and JSON | Adds missing-group 404 handling; existing capacity mapping remains shared. |
| HTTP `read_combined`, `read_group` | `src/http.rs:153`, `src/http.rs:160` | Path and Engine | Response or initial refusal | Thin wrappers supply the optional group to one handler. |
| HTTP `read` | `src/http.rs:167` | Dataset, version, optional group | Arrow body and identity headers | Resolves exact version, constructs a verified stream, uses shared admission, and captures Engine for body lifetime. |
| `Engine::release` | `src/engine.rs:284` | Dataset and version IDs | Verified `ReleasePin` metadata | Reads one captured lifecycle view and refuses rebound dataset/version identities or an unverified result before reader setup. |
| `Engine::read_group`, `read_budget` | `src/engine/read.rs:11`, `src/engine/read.rs:19` | Host and release/group | Stream or shared Budget clone | Reuses the host budget rather than granting each HTTP request a new independent allowance. |
| Free `read_group` | `src/engine/read.rs:30` | Release, group, injected Budget | Schema-carrying native batch stream | Refuses missing group, verifies release, opens pinned metadata, and delegates native scanning. No producing catalog is needed. |
| CLI `Read` branch | `src/main.rs:135` | Release JSON, group, output | IPC file | Calls the same free reader before creating output, streams through the maintained writer, and preserves catalog-free execution. |
| `Engine::read_combined` | `src/engine/backfill.rs:87` | Exact release | Bounded combined stream | Existing service verifies coverage, joins by immutable file/ordinal identity, and bounds the final combined batch. |
| `verify_release_with_budget`, `verify_table` | `src/engine.rs:1385`, `src/engine.rs:1325` | Exact pins and Budget | Verified data or refusal | Existing eager verification checks retained object hashes, manifest binding, native inventory, row counts, changes, and groups. |
| `storage::read_batches` | `src/storage.rs:73` | Pinned native table and Budget | Native batch stream | Holds reader admission, sets native concurrency to one and configured batch rows, and refuses an oversized decoded batch. |
| `query::bounded` | `src/query.rs:87` | Combined stream and Budget | Consumer-bounded stream | Keeps the final reader reservation with the stream and checks the combined batch footprint. |
| `ipc::encode` | `src/http/ipc.rs:11` | Schema-carrying batches and Budget | Demand-driven IPC chunks | Refuses dictionaries; reserves encoder and initial output capacity; polls at most one source batch per data chunk; checks schema and batch bytes; finishes only on clean EOF. |
| `Chunk::as_ref` | `src/http/ipc.rs:93` | Owned bytes and permit | Borrowed byte slice | Exposes encoded bytes while leaving reservation custody with their owner. |
| `Sink::write`, `flush` | `src/http/ipc.rs:103`, `src/http/ipc.rs:120` | Encoded fragments | Appended bytes or I/O error | Uses checked length arithmetic and refuses above-limit append before growth. Flush is a no-op for this in-memory destination. |
| `Budget::reserve` | `src/budget.rs:101` | Requested bytes | Owned permit or refusal | Existing immediate shared-capacity admission avoids waiting while holding other permits. Pressure can abort a stream; it does not permit unbounded buffering. |

Dependency behavior was checked in locally pinned source: `arrow-ipc 58.4.0` `StreamWriter::try_new`, `write`, and `finish` (`src/writer.rs:1400`, `1436`, `1462`); `bytes 1.12.1` `Bytes::from_owner` and owned drop (`src/bytes.rs:254–289`, `1144–1159`); and `futures-util 0.3.34` `TryUnfold::poll_next` (`src/stream/try_stream/try_unfold.rs:95–118`). The Arrow writer is unbuffered; it creates temporary encoded messages but does not accumulate completed output. Bytes ownership is reference-counted, so the final clone releases `Chunk`. The unfolding stream owns either its next state or its active step future and does not maintain an application queue.

## Data flow and invariants

1. **One exact version per response.** HTTP captures the `ReleasePin` once, before reader construction (`src/http.rs:173–180`). That pin supplies both retained reads and identity headers. The stream does not re-resolve latest between batches. The getter checks lookup identity before returning (`src/engine.rs:294–302`).
2. **Shared resource custody.** The Engine wrapper supplies the host's Budget (`src/engine/read.rs:16`). The native or combined reader owns its reservation; the codec state owns its temporary-encoding reservation (`src/http/ipc.rs:28`, `42–44`). The initial output is reserved before writer construction, and each later output is reserved before polling source data (`src/http/ipc.rs:34–41`, `52–53`). No newly granted per-request pool bypasses host admission.
3. **Transferred bytes retain their reservation.** The sink buffer moves into `Chunk` via `mem::take`, leaving an empty destination for the next message (`src/http/ipc.rs:73–76`). `Bytes::from_owner` lets chunks escape the body safely: the last clone, rather than body drop, releases that output's permit. Consumers can copy bytes into separately managed memory, but that is outside this adapter's allocation control.
4. **Demand controls encoding.** Writer construction generates only the schema. The first poll emits it without polling a batch. Each later poll reserves output capacity and requests at most one batch (`src/http/ipc.rs:49–70`). There is no detached producer that continues filling an application queue after cancellation.
5. **Errors cannot become successful completion.** A source error, mismatched schema, oversized batch, or sink error exits before `writer.finish`. Only clean source EOF emits the completion marker (`src/http/ipc.rs:53–69`). Axum propagates a later error as a failed body; clients must require successful HTTP completion, because an Arrow parser accepting a prefix alone does not establish a complete download (`README.md:206–208`).
6. **Workspace ownership follows the in-flight response.** The mapped stream captures Engine (`src/http.rs:181–186`). Dropping the router alone cannot release its workspace while the body can still poll native files. Dropping the response releases this ownership when no other Engine owner remains.
7. **No hidden historical accumulation in the qualified codec.** Dictionary schemas refuse before writer allocation; actual batch schema equality preserves that boundary (`src/http/ipc.rs:15–24`, `54–59`). Ordinary previous batches are not retained by an application queue. Temporary encoding allocations remain the stated upstream qualification limit.

## Test behavior and edge cases

All tests below were read, including their assertions. Their passing owner-produced results are retained in `docs/reviews/http-read-check.log`; the reviewer did not run them.

| Test and location | Trace and assertions | Disposition |
| --- | --- | --- |
| `demand_controls_batch_reads_and_chunks_keep_admission_until_last_clone_drops`, `src/http/ipc.rs:158` | A source poll counter stays zero through schema emission, becomes one for one data request, and stays there after cancellation. Keeping a chunk clone prevents full budget reacquisition until its final drop. | Pass; exercises demand and ownership rather than only output equality. |
| `encoding_preserves_strings_nulls_duplicates_and_complete_empty_streams`, `src/http/ipc.rs:192` | Encodes changing ordinary strings and empty input; independently decodes schema and exact batches, including null/empty/duplicate distinctions; requires the explicit completion marker. | Pass. |
| `dictionaries_and_changing_batch_schemas_refuse`, `src/http/ipc.rs:225` | Top-level and list-nested dictionaries refuse before writer creation. An ordinary actual batch differing from the advertised schema fails after schema output. | Pass; closes HR1 and protects the preflight assumption. |
| `failures_abort_the_body_without_emitting_a_successful_end`, `src/http/ipc.rs:259` | A good batch followed by a source error makes Axum body collection fail. An oversized decoded batch returns the typed refusal; reservations are recoverable after termination. | Pass; code trace additionally establishes that neither branch invokes `finish`. |
| `encoded_chunks_refuse_before_growing_beyond_their_admission`, `src/http/ipc.rs:292` | An over-limit append fails with earlier bytes unchanged; the exact-limit append succeeds. | Pass; checks the sink's boundary. |
| `http_reads_exact_historical_source_and_combined_values_in_bounded_frames`, `tests/http_read.rs:22` | Requests old source and enriched combined versions, checks identity headers and bounded separate frames, decodes typed values with an independent Rust oracle, then verifies physical computed-group schema and values. Latest remains unchanged. | Pass; preserves nulls, duplicates, empty text, and Unicode. |
| `empty_streams_keep_schema_and_known_failures_return_structured_errors`, `tests/http_read.rs:162` | Empty source and combined streams retain schema; missing group/version returns structured 404; corrupt retained input refuses before successful response construction. | Pass. |
| `in_flight_response_keeps_workspace_ownership_until_canceled`, `tests/http_read.rs:239` | Drops router ownership while retaining the unread response; a replacement host is refused until response drop. | Pass; exercises response lifetime separately from the chunk-custody test. |
| `catalog_free_cli_uses_the_shared_group_reader`, `tests/http_read.rs:269` | Hides the catalog, invokes the real CLI, checks no catalog was recreated, and independently decodes expected rows. | Pass; covers the refactored caller. |
| `rebound_catalog_results_cannot_change_an_exact_version_url`, `tests/http_read.rs:318` | Rebinds the lifecycle result to another valid release or changes identity/verification fields. Both the getter and HTTP route return `release_mismatch`. | Pass; protects exact-version routing before payload streaming. |

The initial HTTP attempt is preserved in `docs/reviews/http-read-first-failed.log`. The fixture assumed a 64-bit length without declaring it; it now requests `CAST(length(text) AS BIGINT)` explicitly (`tests/http_read.rs:31`). The final retained gate output shows the IPC/HTTP cases and existing suites passing through doc tests and the no-default-features build. The owner reported successful `./scripts/check` and `cargo check --locked --no-default-features` completion; only a release-getter error-documentation clarification followed the gate.

Actual sockets, proxies, remote storage, transport throughput, the representative Comments workload, and aggregate process memory remain unqualified. The patch's documentation says so (`docs/STATUS.md:380–424`). Those are scope limits, not failed acceptance claims for this local adapter.

## Conclusion

VERDICT: APPROVE

Justification:

- The patch achieves exact-version local HTTP streaming through shared verified readers, with consumer-driven encoding and chunk-owned admission. HR1 is closed by a maintained schema check rather than additional state-accounting machinery.
- Coverage of changed paths: ADEQUATE. Assertions cover historical and empty values, physical/combined results, catalog-free CLI reuse, identity refusals, cancellation, failed bodies, schema boundaries, and reservation lifetime.
- Confidence: HIGH for the reviewed local adapter behavior. The evidence does not establish whole-process memory, immediate upstream quiescence, or production network performance; those limits remain explicit.

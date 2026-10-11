# Shared native footer reader and provider primitive probe

No material correctness or isolation defect was found. **APPROVE** the shared-footer prerequisite and the bounded provider probe for their stated scope. This static review does not qualify remote Engine behavior, provider behavior before execution, transport-loss recovery, or throughput.

Reviewed the working changes against `967a72c1205eae08558d156c6fdbba1e1cd3483b` on 2026-10-10: `src/storage/inspection.rs`, its new tests, the `bytes` development dependency and lockfile entry, and `experiments/r2_conditional_create.py`. The Rust and semi-formal code-review skills informed this review. The reviewer ran no builds, tests, network operations, or payload reads and changed only this report. Execution evidence belongs to the implementation owner. Unrelated working changes and the separate change-chain review were excluded.

## Findings

### Observation 1: serialized footer bounds are distinct from total memory and immutable identity

- **Category:** performance / correctness qualification.
- **Location:** `src/storage/inspection.rs:11-35`, `src/storage/inspection.rs:170-175`.
- **Evidence:** the reader admits the declared serialized-footer allowance, checks object size before and after its reads, and hands retained ranges to the maintained decoder. The same decoder can allocate decoded metadata beyond serialized size. Equal-size replacement is not detected by these size checks. These limits are already stated in the helper documentation; routine inspection relies on previously established immutable identities.
- **Disposition:** no required patch change. Keep decoder allocation and same-size mutation outside the claimed guarantees. The existing routine/deep distinction explicitly allows routine checks to trust retained identities (`tests/lifecycle.rs:75-101`).

### Observation 2: execution and failure-injection coverage have explicit limits

- **Category:** test-coverage.
- **Location:** `src/storage/inspection.rs:24-29`, `src/storage/inspection/tests.rs:32-75`, `experiments/r2_conditional_create.py:110-138`.
- **Evidence:** the new fixtures cover filesystem and memory adapters, initial size mismatch, malformed metadata, exact ranges, short reads and capacity refusals. They do not inject a size change specifically between the two metadata calls or execute the remote FileIO adapter. The provider probe races conditional PUTs and delays a competing multipart completion; it does not retry completion of the winning upload or inject lost transport responses.
- **Disposition:** adequate for this prerequisite and narrow provider experiment. These are qualification limits, not proof of remote Engine completion or the full recovery requirements in `docs/requirements/02-incremental-publication.md:27-40`.

## Patch summary

`inspect_table` replaces direct `std::fs::File` footer reads with the table's maintained `FileIO` (`src/storage/inspection.rs:172-208`). `read_footer` checks size, invokes a shared range helper, rechecks size, and decodes (`src/storage/inspection.rs:15-35`). `footer_decoder` obtains only the tail and declared metadata body, refuses invalid bounds, and disables page-index reads (`src/storage/inspection.rs:40-87`). Routine schema, row-count, known-file reuse and sample decisions remain in the same caller (`src/storage/inspection.rs:181-235`).

The concrete improvement is a single injected native I/O path for local and future remote callers, plus removal of the second tail read previously performed by `ParquetMetaDataReader::parse_and_finish`. The new counting fixture asserts the requested ranges. This is a source-level I/O improvement, not a throughput or peak-memory measurement. `bytes` is used only by the test reader's required trait return type (`Cargo.toml:66-69`, `Cargo.lock:2033-2042`, `src/storage/inspection/tests.rs:132-145`).

The Python probe operates only beneath a canonical synthetic UUID prefix, refuses an existing attempt, tests provider create-only conditions, performs exact readback, and retains objects and unfinished uploads (`experiments/r2_conditional_create.py:28-61`, `:86-138`).

## Function trace

| Function | Evidence | Inputs and outputs | Verified behavior |
| --- | --- | --- | --- |
| Verification context `table` | `src/engine/verification.rs:199-235` | Selected table pin and shared budget to updated report/known files | Admits the known-file inventory, calls routine inspection, then retains returned knowledge. |
| `inspect_table` | `src/storage/inspection.rs:143-277` | Pin, sampling bounds, known identities and admission to inspection/known files | Preserves native inventory checks; moves temporary footer admission and retained-inventory admission into the blocking worker; uses the table's FileIO. |
| `read_footer` | `src/storage/inspection.rs:15-35` | InputFile, exact declared size and allowance to native metadata or refusal | Checks size on both sides of range reads; accepts only a complete native decode. |
| `footer_decoder` | `src/storage/inspection.rs:40-87` | Injected FileRead, size and allowance to populated native decoder | Reads tail once, validates magic/length, checks subtraction, reads body once, rejects short responses, and skips page indexes. |
| Budget `blocking` / `blocking_admitted` | `src/budget.rs:130-165` | Owned closure to result | The worker owns byte and job permits until it exits; dropping its awaiting caller does not release them early. |
| `parquet_bytes` / `store` | `src/storage/inspection/tests.rs:13-29` | Small Arrow fixture to Parquet bytes and native storage | Uses maintained writer/output APIs to supply the same physical fixture to both adapters. |
| CountingReader `read` | `src/storage/inspection/tests.rs:132-145` | Requested range to native bytes, optionally shortened | Records ranges before awaiting; releases its mutex before I/O; shortens only the selected nonempty fixture read. |
| Probe `run` / `record` | `experiments/r2_conditional_create.py:28-61` | CLI paths and explicit synthetic UUID to retained JSONL evidence | Creates evidence exclusively, constructs a fixed prefix, records scope/version, and refuses any prior object/upload before writes. |
| Probe `call` | `experiments/r2_conditional_create.py:63-71` | Selected SDK operation to status/code and response | Retains success status or structured ClientError status/code, omitting headers, bodies and configuration. |
| Probe `put` | `experiments/r2_conditional_create.py:86-108` | Writer index to outcome | Synchronizes two distinct bodies; requires one 200 and one 412, repeats the winner conditionally, and requires another 412. |
| Probe `readback` | `experiments/r2_conditional_create.py:73-84` | Key and expected bytes to exact/not-exact result | Streams once, compares byte length and SHA-256, records the result, and closes the response body. |
| Multipart block | `experiments/r2_conditional_create.py:110-138` | Two uploads, each with a 5 MiB first part and short final part | Starts both before completion, requires completion statuses 200 then 412, and verifies the original winner's exact bytes afterward. |

## Data flow and invariants

- **Ranges:** for file size `N` and declared footer length `L`, requests are `[N-8, N)` followed by `[N-8-L, N-8)`. `N < 8`, invalid tail magic, `L > limit`, and `L > N-8` refuse before requesting a body. Exact returned lengths are checked before bytes enter the native decoder (`src/storage/inspection.rs:46-86`). This checked subtraction also protects the native push decoder's unchecked metadata-start subtraction.
- **Decoder:** tail and body move into `ParquetMetaDataPushDecoder`; neither payload data nor page indexes are requested. Cached Parquet 58.4.0 source confirms `Skip` applies to both index policies and decode returns metadata without an index request (`parquet-58.4.0/src/file/metadata/push_decoder.rs:285-288`, `:389-421`). The old and new readers use the maintained metadata parser (`parquet-58.4.0/src/file/metadata/reader.rs:562-585`; `parser.rs:186-196`, `:229-233`).
- **Storage:** InputFile delegates metadata and reader creation to injected storage (`iceberg-0.10.1/src/io/file_io.rs:289-305`). LocalFs allocates the requested range and uses `read_exact`; memory returns a bounded slice; OpenDAL forwards the same range to its maintained reader (`iceberg-0.10.1/src/io/storage/local_fs.rs:237-262`; `memory.rs:270-287`; `iceberg-storage-opendal-0.10.1/src/lib.rs:602-607`). These sources were inspected locally; remote behavior was not executed.
- **Admission and cancellation:** FileIO and runtime handle move into the admitted worker. Footer and known-file permits live inside that worker; native reads and decoding finish there. The returned metadata is consumed for physical schema and row checks within the file loop (`src/storage/inspection.rs:170-235`). No new unbounded task fanout, global cache or storage abstraction is introduced.
- **Provider isolation:** object names derive only from the validated UUID beneath `docspec-qualification/`; the probe has no delete, policy, catalog or production-prefix operation. Object and unfinished-upload listings have bounded page sizes and need only establish presence (`experiments/r2_conditional_create.py:28-61`). These checks do not provide a distributed lock; the fresh UUID is an experiment identity.
- **Provider custody and secrecy:** evidence opens with exclusive creation before remote effects; each upload ID is recorded before sending parts; automatic SDK retries are disabled. Explicit output consists of bucket/prefix, keys, IDs, versions, status/error codes and readback digests. Credentials go only to client configuration; the outer handler prints only the exception type (`experiments/r2_conditional_create.py:36-56`, `:63-84`, `:116-124`, `:147-152`). Failure may leave an incomplete report and retained remote attempt, which must remain unqualified.
- **Provider pass rule:** conditional PUT and both completion calls carry `IfNoneMatch="*"`. A final pass requires the stated status pattern and a full size-plus-digest match after the competing writes. A provider accepting the delayed multipart overwrite cannot produce a successful report (`experiments/r2_conditional_create.py:92-108`, `:127-138`).

## Test behavior and edge cases

These are predictions from assertions and traced code, not reviewer execution results.

| Test | Evidence | Expected behavior under this patch |
| --- | --- | --- |
| `local_and_memory_use_the_same_bounded_footer_decoder` | `src/storage/inspection/tests.rs:33-75` | Both adapters decode matching rows/schema through the blocking-worker/runtime bridge; an allowance one byte below the footer refuses, and an initial size mismatch refuses. |
| `malformed_tail_and_declared_lengths_refuse_before_body_decode` | `src/storage/inspection/tests.rs:80-122` | Empty/tiny files, invalid magic, impossible lengths, malformed body and empty/encrypted footer refuse with the asserted error categories. |
| `ranges_are_disjoint_and_short_reads_refuse_without_retry` | `src/storage/inspection/tests.rs:150-191` | Valid metadata decodes from exactly the expected ranges; short tail prevents a body request, short body causes refusal, and capacity failure requests only the tail. |
| `impossible_footer_lengths_do_not_request_a_body` | `src/storage/inspection/tests.rs:195-225` | A footer length beyond the file requests only its tail; a sub-tail file requests nothing. |
| `routine_checks_reject_damaged_footer_and_declared_counts` | `tests/lifecycle.rs:121-158` | Forged counts still refuse; corrupted tail magic reaches the native decoder check and returns `parquet`. Registered in `tests/integration.rs:37`. |
| `routine_verification_inspects_required_state_files_once` | `tests/updates.rs:465-487` | Known-file reuse and file-count reporting stay unchanged; registration remains in `tests/integration.rs:56`. |
| Existing blocking cancellation fixtures | `src/budget.rs:359-431` | Caller cancellation keeps worker admission until actual completion. The footer helper does not add a dedicated stalled-remote-read fixture. |

The Python script is itself the provider qualification experiment. It has no separate mocked test suite in this scope, and its actual result must be read from retained execution evidence. Multipart testing covers sequential competing completions after both uploads have begun; it does not establish simultaneous completion-race behavior or response-loss recovery.

## Review reasoning and conclusion

The review started with three hypotheses: the native I/O replacement preserves footer-only reads; cancellation retains admission through actual worker completion; and the Python experiment can detect overwritten winner bytes without touching existing production objects. Source tracing and the inspected assertions support all three within the scope above. Unresolved questions are execution results, remote Engine integration, remote read interruption behavior, and decoder peak allocation.

**VERDICT: APPROVE**

- The patch achieves its stated prerequisite and experiment-design intent through maintained storage/decoder reuse, bounded explicit ranges, preserved worker ownership, conditional writes and exact readback.
- Coverage of changed paths: **ADEQUATE** for this scoped static checkpoint, with the explicit gaps above.
- Confidence: **HIGH** for source-level bounds and probe isolation; provider behavior and full remote qualification remain **UNESTABLISHED** by this review.

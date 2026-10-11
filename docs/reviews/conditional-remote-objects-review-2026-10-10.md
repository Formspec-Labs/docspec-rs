# Conditional remote objects: independent source review

**VERDICT: APPROVE this object-adapter checkpoint. The fresh synthetic Rust/R2
provider probe passed.** The metadata admission and read-request findings below are resolved in
the reviewed source. No material safety finding remains open. Recovery after
provider response loss and actual remote-worker cancellation remains unqualified;
this is not approval of complete remote engine support.

Reviewed October 10, 2026 against working changes over
`e669987873eac3daa5d2ec277ad89c28bf0df8b3`, including the subsequent allocation and
streaming-reader fixes. Scope: `Cargo.toml`, `Cargo.lock`,
`src/storage/remote.rs`, `src/storage/remote/objects.rs`, its tests, and
`examples/r2_objects_probe.rs`. The Rust and semi-formal code-review skills,
repository guidance, `docs/PLAN.md`, and publication requirements governed this
review. No build, test, network request, corpus read, or implementation mutation
was performed by this reviewer. Only this report was written. The unrelated
change-chain review was not read. Root performed execution; this reviewer inspected its retained
focused-test and provider evidence and verified the current source hashes.

## Findings and resolution

### F1 — Metadata allocation accounting: resolved

The initial implementation reserved twice `Vec::len()` even though its worker
owned the vector's entire capacity. A small valid value in a vector with large
spare capacity could pass the metadata length limit and retain substantially more
memory than its reservation through cancellation and the provider request.

The allocation is not reduced by conversion: OpenDAL's `Buffer::from(Vec<u8>)`
forwards to `Bytes` (`opendal-core-0.57.0/src/types/buffer.rs:419-422`), and locked
`bytes-1.12.1/src/bytes.rs:947-965` preserves spare vector capacity.

The final implementation reserves checked `capacity + len`
(`src/storage/remote/objects.rs:142-148`). The new
`metadata_spare_capacity_is_admitted_before_provider_io` test gives a three-byte
value a 128-KiB allocation against a 64-KiB admission pool and expects `capacity`;
a compact wrong value reaches the distinct hash refusal
(`objects/tests.rs:120-148`). This is a meaningful regression test, not an
assertion of the arithmetic alone. It passed in root's retained corrected-fixture run, inspected below.

### F2 — One-MiB verification requests: resolved

The initial verification loop issued a separate FileIO range read per MiB.
Iceberg's wrapper calls OpenDAL `Reader::read` and coalesces the result
(`iceberg-storage-opendal-0.10.1/src/lib.rs:598-607`); that API collects a new stream
for every supplied range (`opendal-core-0.57.0/src/types/read/reader.rs:117-123`).
For a one-GiB object the old successful path therefore required 1,024 serial
payload GETs, in addition to metadata reads. This is source-derived request
arithmetic, not a measured timing.

The final implementation uses the configured OpenDAL operator's
`reader(key).into_stream(0..expected.bytes)` with default streaming options. It
hashes each returned buffer's slices before polling the next buffer, checks exact
byte count, and preserves the before/after size checks
(`objects.rs:273-325`). Empty files skip the payload GET. This removes the
per-MiB request amplification without a whole-file buffer, second payload pass,
or project-owned transport framework.

The localhost S3 fixture now asserts the exact `HEAD, GET, HEAD` sequence and full
range for a payload exceeding one MiB (`objects/tests.rs:170-189`), refuses tail
corruption, short content and changed final size (`191-219`), and checks empty
objects and out-of-scope refusal (`221-245`). These tests use the actual configured
operator. They passed in root's retained corrected-fixture run, inspected below.

### F3 — Remote write failure and cancellation qualification: open scope limit

**WARNING — test coverage.** Local tests cover refusal before an actual write,
streamed reads, and error-kind mapping; they do not exercise the remote writer
after a source has begun transferring (`objects/tests.rs:93-168,333-350`). The
example injects no lost response (`r2_objects_probe.rs:171-176`).

The final test addition covers a small same-length wrong file digest before
single-part publication (`objects/tests.rs:150-168`); its passing result appears
in root's retained corrected-fixture log. Before claiming recovery qualification, exercise a
wrong digest after multipart transfer begins, an acknowledged effect whose
response is lost, and cancellation while remote I/O is active. Check destination identity,
absence of a second automatic write, and retained budget admission until actual
worker completion. A maintained local HTTP boundary or transport hook can support
these checks; no new transfer framework is required. This limit does not block
the present primitive checkpoint or bounded functional provider probe.

## Patch summary and function trace

The patch adds create-only objects through the already pinned OpenDAL release,
shares resolved startup properties with the catalog, and exposes explicit
streamed full-object readback. The example is the only non-test caller found;
these primitives are not wired into engine publication. The optional dependency
is gated by `remote` and matches locked OpenDAL 0.57.0
(`Cargo.toml:31-50`, `Cargo.lock:3569-3585`).

| Function | Source | Input → output and verified behavior |
| --- | --- | --- |
| `remote_storage` | `remote.rs:41` | Prefix/properties/secret references → catalog and objects; resolves credentials once for both. |
| `remote_objects` | `remote.rs:58` | Same object configuration → object adapter through the shared resolver. |
| `remote_catalog` | `remote.rs:83` | Catalog properties → catalog; preserves the previous entry point. |
| `resolve_properties` | `remote.rs:90` | Properties and environment references → resolved map; refuses absent/empty secrets and forces redaction/discovery switches. |
| `catalog_from_resolved` | `remote.rs:107` | Resolved map → wrapped native catalog; validates URI/warehouse, bounds HTTP, sanitizes initialization errors. |
| `RemoteObjects::from_resolved` | `objects.rs:40` | Prefix and resolved map → operator/FileIO; checks supported S3 options and shared addressing/credentials. |
| `file_io` | `objects.rs:114` | Adapter → native FileIO escape hatch, explicitly unrestricted and credential-bearing. |
| `create_bytes` | `objects.rs:129` | Expected reference/owned bytes/budget → same reference; scope, length, allocation and hash checks precede conditional write. |
| `upload_file` | `objects.rs:180` | Expected reference/local path/budget → same reference; streams and hashes, aborts copy failures, publishes only after identity matches. |
| `verify` | `objects.rs:273` | Expected reference/budget → same reference; fresh streamed read, exact byte count/digest and observed sizes. |
| `Scope::new`, `Scope::key`, `split_s3` | `objects.rs:339,347,356` | Strict URI → owned key or refusal; rejects cross-bucket/prefix and normalization aliases. |
| Refusal helpers and `write_error` | `objects.rs:387-426` | Failure/provider error → stable code; separates conflict/unconfirmed result without provider text. |
| `Budget::blocking_admitted`, `reserve` | `budget.rs:147,171` | Owned work/allocation → worker result/permit; fail-fast admission remains captured through completion. |
| Probe `run` | `r2_objects_probe.rs:72` | Opt-in/canonical UUID → synthetic probe events; metadata race/repeat and multipart repeat conflicts, then exact readback. |
| Probe `main`, `env`, `object`, `fixture`, `conflict` | `r2_objects_probe.rs:16,25,30,39,62` | Configuration/synthetic bytes/results → expected identity or sanitized failure; fixture generation hashes once and top-level errors print only codes. |
| `ReadFixture::new`, `objects`, `finish` | `objects/tests.rs:256,321,327` | Payload/observed sizes → loopback S3 reads and method/range observations; holds no production credentials and performs no write endpoint behavior. |

## Data flow and verified invariants

**Destination scope.** Private `Scope` retains bucket and slash-delimited prefix
(`objects.rs:332-353`). Each public write/read validates the destination before
provider I/O (`135`, `186`, `274`). The appended slash prevents `owned-other`
matching `owned`; strict parsing rejects traversal, percent escapes, queries,
fragments, whitespace and controls (`356-384`). `ObjectRef::owner` remains
caller-supplied metadata, not a remote authorization check. FileIO is an explicit
unrestricted escape hatch (`110-115`); callers still own authority decisions. The
probe uses that handle only for the fresh-prefix existence check.

**Publication follows source identity.** Metadata hashes before its write
(`155-162`). Uploads hash the same owned chunks submitted to OpenDAL and check
total length, final source length, and digest before close (`212-253`). OpenDAL's
S3 backend chooses `MultipartWriter` for non-append writes
(`opendal-service-s3-0.57.0/src/backend.rs:1097-1108`). It caches the first buffer;
later writes only upload unfinished parts; close calls either `write_once` or
`complete_part` (`opendal-core-0.57.0/src/raw/oio/write/multipart_write.rs:211-295`).
The outer chunk writer flushes its final buffer before delegating close and clears
it on abort (`types/context/write.rs:186-203`). Empty and short files therefore
retain the same pre-publication hash boundary.

**Conditions reach both publication requests.** S3 PUT includes
`If-None-Match: *` (`opendal-service-s3-0.57.0/src/core.rs:323-329`), as does
multipart completion (`1065-1071`). Completion also detects errors embedded in
HTTP 200 (`writer.rs:190-207`). This proves request construction. The retained
Rust/provider run below additionally exercises conditional creation and exact
readback against the configured provider.

**Unknown effects remain unknown.** The write operator has no retry layer
(`objects.rs:89-97`). Native part concurrency is one (`209`), which takes the
executor's direct branch and returns errors without scheduling automatic
replacement (`opendal-core-0.57.0/src/raw/futures_util.rs:229-239`). S3 HTTP 409/412
maps to `ConditionNotMatch` (`opendal-service-s3-0.57.0/src/error.rs:42-48`), then
`remote_conflict`; other provider errors become `remote_unconfirmed`
(`objects.rs:414-426`). A close failure neither deletes nor retransmits the
object. Copy failure attempts to abort only the unpublished upload ID; failed
abort or failed close can leave provider parts (`247-253`). There is no durable
partial-upload recovery inside these primitives. The example declares that its
remote objects and possible unfinished multipart parts remain retained
(`r2_objects_probe.rs:1-5`).

**Cancellation retains admission while the runtime stays active.** Memory permits
move into each blocking worker (`objects.rs:148-154,193-199,275-281`), and shared
`blocking_admitted` moves its worker permit too (`budget.rs:151-164`). Dropping the
awaiting task does not drop either worker-owned permit. The shared cancellation
test aborts the caller, observes continued capacity refusal, releases the worker,
then observes restored admission (`budget.rs:393-431`). Its worker is synchronous;
actual remote transport/runtime cancellation remains F3. Public documentation
correctly requires the runtime to continue driving I/O.

**Resource growth.** Upload chunks remain eight MiB, part concurrency one, and
preflight limits expected files to 10,000 parts (`objects.rs:14-23,187-209`). Native
part metadata grows with part count and has an explicit allowance. Submitted
buffers are owned independently of the next input chunk. Metadata admission now
covers spare allocation capacity. Readback processes response buffers immediately
and uses one payload request per nonempty object. Its two-MiB reservation is
explicitly an allowance, not a configured transport-buffer or measured RSS cap
(`objects.rs:15-17,260-268`). No throughput or peak-memory result is claimed here.

**Shared configuration and redaction.** Unknown `s3.*` keys and `client.region`
refuse (`objects.rs:47-79`), avoiding silent differences between the explicit
writer and FileIO parsing. Addressing defaults and the supported explicit
credentials/session token match maintained Iceberg S3 parsing
(`iceberg-storage-opendal-0.10.1/src/s3.rs:40-68,114-122`). Public wrapper Debug
omits fields and object errors omit provider text. FileIO and native catalog
method errors are not a general secret-safe diagnostic surface; the probe prints
neither those handles nor full errors (`r2_objects_probe.rs:17-21`).

## Maintained streaming API evidence

Iceberg 0.10.1 `FileRead` exposes only `read(Range<u64>)`; `InputFile::read` loads
the entire object (`iceberg-0.10.1/src/io/file_io.rs:245-305`). The OpenDAL reader
wrapper is private (`iceberg-storage-opendal-0.10.1/src/lib.rs:598-607`), so this
version provides no public FileIO-to-stream conversion.

The final adapter reuses OpenDAL directly. Its defaults are `chunk=None`,
concurrency one and prefetch zero (`opendal-core-0.57.0/src/raw/ops.rs:446-454`).
`BufferStream` chooses `StreamingReader` without a chunk size
(`types/read/buffer_stream.rs:279-288`). `ReadGenerator` opens the supplied whole
range once (`types/context/read.rs:159-197`), and `StreamingReader` yields body
buffers as they arrive (`types/read/buffer_stream.rs:73-91`). The HTTP body wraps
reqwest `bytes_stream` (`raw/http_util/client.rs:255-266`,
`raw/http_util/body.rs:131-141`). The final code does not collect these buffers,
configure range chunking, or run another payload pass.

This establishes the successful one-GET path statically. The new test asserts its
observable request sequence. The transport still determines response-buffer size,
so whole-process memory and provider wire-request counts remain unmeasured. The
retained provider run below confirms successful integrity readback, not HTTP
request telemetry.
FileIO remains available for native table interoperability; integrity readback
through OpenDAL does not by itself qualify a consumer's Iceberg table path.

## Test behavior and edge cases

The initial author run failed in the loopback fixture: its accepted socket
retained nonblocking behavior and `write_all` returned `WouldBlock`. The failed
[run log](remote-objects-streaming-tests-2026-10-10.log) remains retained. Root
changed only the fixture to set accepted sockets to blocking mode and bound both
read and write waits (`objects/tests.rs:274-281`). This is appropriate because the
fixture performs synchronous socket I/O on its dedicated thread; it leaves the
listener nonblocking so the stop flag still works.

I inspected the subsequent [passing run log](remote-objects-streaming-tests-2026-10-10-2.log).
It confirms the focused remote library suite passed, including allocation,
single-part bad-digest refusal, exact GET count, tail corruption, short stream,
final size change, empty object, and scope cases. The table explains their source
behavior. Execution was performed by root, not this reviewer.

| Test/case | Source | Expected behavior and limit |
| --- | --- | --- |
| Scope aliases and cross-prefix destinations | `objects/tests.rs:38-70` | Accepts nested owned key; strict parser rejects aliases, foreign bucket/prefix and empty destinations. |
| Configuration/redaction | `objects/tests.rs:72-91` | Missing required/unsupported S3 options refuse; Debug/refusal text omit dummy secrets. |
| Metadata size and digest refusal | `objects/tests.rs:93-118` | Limit or incorrect digest refuses before provider write. |
| Spare metadata capacity | `objects/tests.rs:120-148` | Oversized owned allocation refuses before I/O; compact invalid bytes reach digest checking. |
| Changed source identity | `objects/tests.rs:150-168` | Wrong initial length refuses before writer construction; the added small same-length bad digest remains buffered and refuses before close. Passed in root's retained focused run. |
| Streamed read and request count | `objects/tests.rs:170-189` | Preserves identity and asserts one GET between two HEADs for more than one MiB. |
| Tail corruption, short stream, changed final size | `objects/tests.rs:191-219` | Refuses content/length mismatch or transport failure; checks exactly one GET in each case. |
| Empty and out-of-scope reads | `objects/tests.rs:221-245` | Empty digest/size needs only two HEADs; wrong prefix generates no requests. |
| Conflict versus unknown error mapping | `objects/tests.rs:333-350` | Directly constructed provider kinds get distinct safe codes; no real lost response is injected. |
| Catalog redaction/configuration | `remote.rs:208-241` | Existing behavior continues through the shared resolver. |
| Shared canceled worker | `budget.rs:393-431` | Admission persists after caller cancellation until the worker exits; remote transport not exercised. |
| Real-provider example | `r2_objects_probe.rs:72-179` | Fresh UUID/opt-in, one metadata winner, refused losing/repeated create, multipart repeat refusal, exact readback once per winner; does not race multipart completions or inject failures. |
| File grows/shrinks or has wrong digest | `objects.rs:187-253` | Preflight catches initial length/size bounds; final mismatch aborts before publication. Post-start writer cases remain F3. |

## Actual Rust/provider evidence

Root ran the built `r2_objects_probe` under the fresh owner
`docspec-rs-qualification-bcf2167f-5773-4533-913a-c5d38a10bae4` in
`s3://spicy-regs/docspec-qualification/<owner>/rust-objects`. The retained
[run manifest](r2-rust-objects-provider-2026-10-10.json) records completion and exit
code zero between `2026-10-11T01:12:52Z` and `2026-10-11T01:13:01Z` (October 10
locally). Its [event log](r2-rust-objects-provider-2026-10-10.jsonl) contains the
expected start, metadata, multipart-start and completion events. The
[stderr log](r2-rust-objects-provider-2026-10-10.stderr.log) is empty.

I independently recalculated SHA-256 for the recorded `Cargo.toml`, `Cargo.lock`,
`remote.rs`, `objects.rs` and probe source and matched every value in the manifest.
The manifest also records the executed binary's SHA-256; I did not rehash or
execute that binary. I checked event order, prefix/owner equality, object paths,
expected multipart size, result flags, and equality of the pre-effect multipart
reference and final readback reference. No provider calls or object payload reads
were made during this evidence review.

| Observed result | What the current probe requires before emitting it |
| --- | --- |
| Metadata winner is 22 bytes, digest `b0cc4b61158510de5e9d12d992488d89672ba7aaae64ed450089be6fd593e79f` | Concurrent distinct creates return exactly one success; losing and repeated winning creates both return `remote_conflict`; a fresh streamed read matches the winning exact size/digest (`r2_objects_probe.rs:139-162`). |
| Multipart winner is 17,825,811 bytes, digest `5a31a20b8f1fdf97e913996ded350ff191d5a3fe3ab0ac826466b9e8cd5cb47e` | The first file uploads successfully; a different file targeting the same key returns `remote_conflict`; a fresh streamed read still matches the first expected size/digest (`r2_objects_probe.rs:165-179`). |
| Final `passed: true` and exit zero | Both preceding checks completed; errors would print only their code and exit one (`r2_objects_probe.rs:16-22,177-181`). |

This qualifies conditional metadata creation, repeated-create refusal,
multipart create/competing replacement refusal, and exact streamed readback
through this pinned Rust adapter against the configured provider. The logged
refusals are adapter codes; this evidence does not retain raw HTTP status or
provider wire-request counts. Multipart attempts run sequentially, so this is not
a race between two pending multipart completions. Provider objects and possible
unfinished parts remain retained; no cleanup or exact incomplete-part inventory
is claimed.

F3 remains open for multipart source mismatch after transfer starts, lost close
responses and active remote-worker cancellation. The run does not qualify durable
transfer resume/reuse, native Iceberg table consumers, catalog release selection,
authority fencing, remote retention/restore, throughput or peak memory. The full
repository gate was still running when this evidence update was written; no full
gate result is inferred from this probe.

## Conclusion

**VERDICT: APPROVE.** The final source supports the intended narrow conditional
object primitive and explicit full-object integrity readback. F1 and F2 are
resolved, and the new tests check observable admission and HTTP-read behavior.
Coverage is **adequate for this bounded checkpoint**, and **insufficient for remote
failure/recovery qualification**. Confidence in source behavior is **high**;
the focused tests passed in root's retained run after the fixture correction.
The actual Rust/provider probe also passed within the scope recorded above.
Process/resource measurements and broader remote failure qualification remain
outstanding. Full engine admission, lifecycle selection,
fencing, transfer reuse, retention, and restore remain outside this review.

Dependency references are relative to the local published crate cache at
`/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/`.

## Lead execution follow-up

After the independent source review, the lead completed the
[focused checks](remote-objects-streaming-tests-2026-10-10-2.log) and
[full repository gate](remote-related-full-check-2026-10-10.log), both with exit
status zero. Formatting, strict all-target/all-feature Clippy, library/integration,
restore, worker-initialization and documentation checks pass. The existing
external Comments physical-plan probe remains explicitly ignored. This closes
the pending execution check for this patch, not the wider lifecycle or resource
qualification limits recorded above.

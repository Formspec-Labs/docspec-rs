# Native remote-write fault tests: independent review

**VERDICT: APPROVE.** No material finding blocks this test-only checkpoint. The
tests exercise real localhost HTTP through the maintained S3 client and add
evidence for multipart source refusal, lost write responses, and cancellation of
an awaiting caller. They do not qualify those failures against R2 or prove
recovery after process death.

Scope: the pending delta over `559f4aa908405d579f205a3f2b0d5d7b12c262ff` in
`src/storage/remote/objects/tests.rs` and its new `tests/writes.rs` child. The only
parent-file change is registering `mod writes` at line 13. No production code
changed. Review used the previously loaded Rust and semi-formal review guidance.
This reviewer ran no builds, tests, provider calls or payload reads and changed
only this report. The unrelated change-chain review was not read.

## Findings and scope limits

**No blockers or required changes.** One coverage observation remains: the HTTP
cancellation test asserts retained byte admission, but does not directly try to
acquire the occupied worker slot (`writes.rs:125-161`). The shared budget test
already checks worker-slot retention (`src/budget.rs:393-431`). Describe the new
test as proof of retained memory admission during active HTTP, or add the same
small `blocking_admitted` refusal assertion if claiming both permits here.

The fixture implements only the S3 interactions needed for these failure cases.
It checks conditional headers and exact path/upload identity, but it is not a
provider simulator: multipart completion assembles recorded parts without parsing
the submitted completion XML or independently validating its ETags
(`writes.rs:226-313`). That is sufficient for the tested response-loss boundary;
actual provider protocol qualification remains separate.

## Function trace and test evidence

Paths below are under `src/storage/remote/objects/tests/` unless otherwise shown.
I inspected root's retained [focused run](remote-write-fault-tests-2026-10-10.log),
which passed every new fault test. The following traces explain what those results
establish.

| Function | Source | Input, observed behavior and output |
| --- | --- | --- |
| `bad_digest_after_part_upload_aborts_only_its_unpublished_upload` | `writes.rs:14` | A file larger than two upload chunks has the right size but a wrong retained digest. Native initiation/part I/O occurs, then `object_changed`; assertions require initiation without completion, real part PUT, exactly one abort, no published object and cleared fixture parts. |
| `accepted_put_with_lost_response_reconciles_without_another_write` | `writes.rs:44` | The fixture stores a conditional PUT then closes its socket without a response. Native code returns `remote_unconfirmed`; explicit `verify` succeeds. Exact requests are PUT, HEAD, GET, HEAD; no second write occurs. |
| `accepted_complete_with_lost_response_reconciles_without_reupload` | `writes.rs:82` | Native multipart completion is stored before response loss. `remote_unconfirmed` is followed by explicit `verify`; assertions require one initiation/completion, each original part once, no abort, one GET/two HEADs, and byte-identical stored output. |
| `canceled_http_caller_keeps_owned_capacity_until_worker_finishes` | `writes.rs:120` | After the fixture receives the full PUT and holds its response, cancel the awaiting task. Byte admission stays exhausted until the response is released and the worker completes; only one PUT is observed. |
| `WriteReport::methods` | `writes.rs:184` | Counts recorded HTTP methods for the assertions above. |
| `WriteFixture::new` | `writes.rs:204` | Creates one loopback listener and dedicated server thread. Its behavior enum chooses abort, response loss or held response; only expected path/upload methods are accepted. |
| `WriteFixture::objects` | `writes.rs:328` | Reuses the production constructor with dummy credentials and the loopback endpoint. |
| `shutdown`, `finish`, `Drop` | `writes.rs:334,342,349` | Release held response, set stop, wake blocking accept, and join. Explicit finish propagates server panic; Drop preserves failure cleanup without a second panic. |
| `Request::read`, `query_value`, `respond` | `writes.rs:368,404,411` | Bounded header/body parsing, fixed query extraction, and synchronous HTTP responses. Only method/range and necessary booleans survive header parsing; signing headers are not reported. |

The production callees are unchanged: metadata identity is checked before PUT
(`src/storage/remote/objects.rs:142-162`); uploads hash transmitted chunks and abort
the unfinished upload on mismatch before close (`212-253`); explicit verification
only issues stat/read calls (`273-325`). `Budget::blocking_admitted` captures its
worker permit inside `spawn_blocking`, while the operation closure owns its
memory permit (`src/budget.rs:147-165`). The pinned OpenDAL `MultipartWriter`
caches its first buffer, uploads parts on subsequent writes, and publishes only
at close, as traced in the preceding conditional-object review. The wrong-digest
test uses two full chunks plus a tail specifically to cross that native buffering
boundary before the digest check.

## State, signals and failure robustness

`WriteReport.parts` records received part bodies and rejects duplicate part
numbers. Abort accepts only the fixture's exact upload ID and clears those
unpublished parts. A completed object moves into `published` before the socket is
closed, so lost-response tests exercise an actual transport failure after a
fixture-side effect, rather than returning a fabricated application error
(`writes.rs:247-295`). Readback uses those stored bytes; final comparisons bind
them to the original fixture input. No test invokes create/upload a second time
to reconcile an unknown result (`44-117`). This proves explicit read-only identity
reconciliation in one live process, not durable lifecycle reuse.

The cancellation start signal is emitted only after the request body has arrived
and `published` is set (`289-292`); there is no sleep-based guess about the worker's
state. The caller is aborted while its runtime remains active, and byte admission
is checked before the server response is released (`140-161`). This covers
cancellation after remote acceptance and before acknowledgment. It does not cover
a killed process, a stopped runtime, cancellation halfway through a request body,
or interrupted multipart transfer.

Sockets have read/write timeouts, held-response waits have a timeout, and test
futures have a timeout (`10,25,50,59,90,99,141,151,220-221,292`). Drop releases the held
response and wakes the blocking listener before joining (`334-354`). The checks
avoid indefinite signal waits on ordinary failure paths. These are per-operation
bounds, not a guarantee that the whole test terminates within one `DEADLINE`:
cleanup and an already-started blocking worker can outlive a canceled test future.
Request headers and bodies have explicit size limits (`369-393`); loopback scope,
dummy credentials and fixed object identity keep the fixture separate from real
storage.

## Conclusion

The preceding F3 gaps now have passing **local HTTP** evidence for a bad digest
after a part has transferred, accepted PUT/completion with a lost response, and
retained byte admission after caller cancellation. The tests are focused and
reuse the actual adapter. They add no production abstraction or transfer
framework. Coverage is **adequate for these named local failure boundaries**;
confidence is **high** from source traces and the retained focused run.

**VERDICT: APPROVE.** R2 fault injection, process-crash recovery, durable
reconciliation/resume, and whole-engine publication remain unqualified by this
delta. Prior passing R2 functional creation/readback evidence does not extend to
these newly tested failure scenarios.

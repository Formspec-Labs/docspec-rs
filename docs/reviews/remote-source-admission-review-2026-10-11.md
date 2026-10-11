# Remote source admission — independent follow-up review

**VERDICT: APPROVE for the scoped internal source-admission change, subject to the repository gate.** Original F1 and F2 are addressed. No new blocking finding was found. This is static review; the root agent owns compilation/test execution and provider work.

Reviewed current source against `ff190bf`, including the applied core, shared typed-validator cleanup, and formatted actual Engine tests. Reference exports: `/tmp/docspec-remote-build-location-core-v2.patch` SHA-256 `6964f23117edaf4e8c3abc20aa9eefe7295c7478117ce0204694442cc107e8b5`; `/tmp/docspec-remote-build-location-tests-v2.patch` SHA-256 `17eeaccb69b651d86f84a620e1a7f47a8b9f87bafe178bb45419470f7fb26e37`. The original findings remain in `/tmp/docspec-remote-build-location-review.md`; the accepted recovery decision is `/tmp/docspec-source-attempt-recovery-decision.md`. Applied repository guidance, ms-rust, and semi-formal-code-review. No source edits, builds, formatting, or provider calls by this reviewer.

All source references below are relative to `/Users/mikewolfd/Work/spicy-stack/docspec-rs/`.

## Original findings and final disposition

**F1 — permanent pending-intent refusal: CLOSED.** `SourceAttempt` now binds operation ID and the original native operation-row revision, with material digest/eligibility retained as validation fields (`src/engine/locations.rs:90–98`, `314–316`). `reserve_source_attempt` loads the same intent before source metadata access, rechecks current authority, and reuses the saved destinations (`:102–118`). Intent and protection commit together; an uncertain reservation response reconciles a fresh native view (`:154–189`). The validator requires matching operation/request revisions and digest, eligibility, consumer, exact input names/digests/paths, and protection committed in the same revision (`:261–310`). The engine-specific check also enforces the configured exact retained root (`:192–210`).

`Objects::admit_source` reconciles an existing intent before opening the original file. Matching retained bytes return immediately; only a definite initial NotFound permits one conditional upload at the same URI. A conflict or uncertain write receives one reconciliation pass, without an upload loop (`src/storage/object_io.rs:30–74`). Another operation with identical material input now receives a distinct operation/generation key instead of inheriting a permanent refusal. Existing completed-phase lookup remains ahead of this path (`src/engine.rs:773–813`, `1831–1869`).

The intent is recognized as operation recovery metadata (`src/engine/retention/recovery.rs:16–29`, `220–233`; `src/engine/retention/retirement.rs:68–85`). Inventory validates it without turning it into creation evidence (`src/engine/retention/inventory.rs:1514–1516`). Remote destructive maintenance remains unsupported through the existing injected-catalog gate and local attempt interpretation; this patch does not enable it.

**F2 — disconnected tests: CLOSED for this implementation slice.** The new tests privately compose an Engine with the maintained local test catalog and actual native HTTP RemoteObjects, then invoke public `run`/`run_until` through `prepare_source` (`src/engine/locations/remote_tests.rs:25–131`). They now connect lifecycle state, protection, transport effects, and phase reuse. They stop at admission or the explicit `remote_build_unavailable` processing boundary, so they do not claim a working remote Engine.

## Function traces and invariants

| Path | Evidence | Verified behavior |
|---|---|---|
| Operation → saved intent | `locations.rs:102–189` | Original revision identifies the intent; a retained intent is consulted before original-source stat; fresh protection precedes PUT. |
| Intent → authority | `locations.rs:192–210`, `261–310` | Saved bytes must match request meaning and same-commit reservation, and remain within the explicitly configured retained root. |
| Resume → remote object | `storage/object_io.rs:52–72` | Existing exact bytes are reused; absent destination receives one create-only attempt; conditional conflict alone is not success. |
| Native absence → result | `storage/remote/objects.rs:314–347` | Only initial `opendal::ErrorKind::NotFound` yields None. Auth/transport/read failures and mismatch remain refusals. The existing streamed hash core is reused after the initial stat, avoiding a duplicate initial HEAD. |
| Qualified phase → retry | `engine.rs:1831–1869` | Availability checks only; no payload GET or upload. |
| Intent → retention | `retention/recovery.rs:220–233`; `retention/inventory.rs:1514–1533` | Pending metadata and its protection remain distinct from typed creation evidence. Operation recovery can identify the record; physical remote removal is not enabled. |
| Local path | `storage/object_io.rs:38–49`; `engine/locations.rs:213–248` | Existing copy/hash remains the callee; protection still commits before creating the local directory. |

The implementation deliberately permits bounded duplicate uncommitted transfer in concurrent/ambiguous races. A missing HEAD does not establish that an older writer stopped. Existing conditional PUT/multipart completion protects the immutable destination; Engine custody retains worker/admission lifetime after caller cancellation. There is no new lock/lease framework and no claim of exactly-once cross-host transfer or verification. Durable completion is the boundary that prevents repeat work.

## Test behavior inspected

| Test | Source | What its assertions establish |
|---|---|---|
| `reserved_remote_admission_resumes_same_destination_then_reuses_completed_phase` | `remote_tests.rs:134` | Stop after reservation before PUT; resume via native 404 at the same saved URI; qualified completion survives source removal. PUT observer reads committed protection; total PUT/GET remain one each. |
| `transferred_remote_admission_recovers_without_original_source_or_second_put` | `:189` | Stop after transfer/readback before phase acknowledgment; remove source; recover retained bytes with one necessary new verification and no second PUT. |
| `lost_remote_put_response_reconciles_exact_intent_and_completes_admission` | `:237` | Native fixture accepts bytes and drops response; Engine reconciles exact bytes and completes admission; later reuse does not repeat transfer/readback. |
| `saved_remote_intent_does_not_turn_auth_failure_into_absence` | `:268` | Native 403 gives `remote_read`, retains the intent, and sends no PUT/GET. |
| `pending_remote_intent_does_not_poison_another_operation_with_same_material_request` | `:292` | Equal material digest, different operation/revision/prefix; new operation completes admission. |
| `readable_in_scope_remote_input_has_no_ownership_without_creation_evidence` | `:329` | Explicitly readable in-scope bytes still fail owner qualification before operation admission. |
| `present_remote_destination_with_wrong_digest_never_triggers_upload` | `:381` | Same-sized wrong bytes fail digest verification even with original source absent; no Engine upload or deletion occurs. |

The fixture reuses the existing native S3 HTTP boundary with a test-only PUT observer and explicit HEAD 404/403 responses; it does not substitute fake success for object I/O. Tests were inspected, not executed by this reviewer. Dedicated tests for catalog lost-reservation response, operation-retirement execution, and concurrent multipart conflict are not added here; the relevant branches were statically traced, and this approval must not be promoted into broad provider/recovery qualification.

## Conclusion

The change now supplies a recoverable, retained remote source to the actual shared Build admission service. Completed work is resolved first; pending work can progress; readable prefixes do not grant ownership. The shared typed validator removes the initial serialize/parse roundtrip. Verification work remains proportional: fresh upload gets one readback, ambiguous unacknowledged work may be reread, and completed phases receive only availability checks. No throughput or resource-performance claim was measured.

Coverage of the changed source-admission path is **ADEQUATE for this slice**, pending execution. Confidence is **HIGH**. Native remote processing, public remote startup, publication/selection, provider concurrency, remote maintenance, and full lifecycle acceptance remain outside this approval.

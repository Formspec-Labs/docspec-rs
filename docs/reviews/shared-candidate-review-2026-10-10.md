# Shared remote candidate review — 2026-10-10

Root execution follow-up: the full repository gate subsequently passed in
`shared-candidate-full-check-2026-10-10.log`, with resource observations in
`shared-candidate-check-resources-2026-10-10.json`. After user-requested cleanup of
inactive Cargo artifacts, `shared-candidate-provider-build-2026-10-10.log` rebuilt
the all-feature example. The fresh actual-provider run in
`r2-shared-candidate-provider-2026-10-10.json` and its `.jsonl` readback passes
through this shared helper, retaining a 775-byte metadata object and its synthetic
namespace. It preserves native meaning and the exact URI through registration,
fresh catalog load and catalog-free opening. These later execution results do not
extend the static review to payload publication, process crashes or remote Engine
operation; earlier pending-evidence statements below describe review time.

VERDICT: APPROVE for the reviewed source and local fixture scope. The retained-destination, compression, assertion, and registration-error coverage findings are resolved. Root's focused rerun passed; the new helper's real-provider qualification and full repository gate remain separate.

## Scope and findings

Independent static review of the candidate-writer changes against `07fd332`, including their callers, native dependencies, tests, and the changed call in `examples/r2_candidate_probe.rs`. Read repository instructions, `docs/PLAN.md`, publication/operations requirements, and the Rust and semi-formal review skills. No builds, tests, provider calls, or Rust edits were performed by this reviewer. Root owns execution. Author/design-review conclusions and the unrelated change-chain review were excluded.

The original `/tmp/docspec-shared-candidate.patch` had SHA-256 `94d9d26661f81dc3e32fe9b77c0432bc1f2a5588b4b09a1e00c5583cc9bce789`; its initial findings remain in `/tmp/docspec-shared-candidate-review.md`. This report covers the subsequent retained-location/compression correction in current source.

### F1 — Resolved: exact destination now precedes remote effects

The initial worker generated a new UUID internally, hiding the URI on uncertain writes and making independently generated fixture paths differ. Pinned Iceberg creates a fresh UUID on every `MetadataLocation::new_with_metadata` call (`iceberg-0.10.1/src/catalog/metadata_location.rs:63–69`). The corrected public method accepts a caller-retained `MetadataLocation` (`src/storage/remote.rs:51–65`). `CandidateWriter::metadata_path` validates and returns that exact native destination (`src/storage/tables.rs:101–117`); it does not generate another remote name. The fixtures now pass the same retained location they expect on the wire (`writes.rs:469–497,613–641`). Local location generation remains native and unchanged.

The caller can now reconcile its retained identifier and URI after a lost object or catalog response. The API documentation explicitly distinguishes cancellation during object creation from an uncertain catalog registration (`remote.rs:31–45`). This is a low-level prerequisite for recovery, not a complete durable recovery workflow or automatic retry.

### F2 — Resolved: plain JSON refuses incompatible compression

`CandidateWriter::metadata_path` uses the maintained `metadata_compression_codec()` parser and refuses non-none compression before serialization/write (`tables.rs:89–95`). It also refuses a compressed retained destination (`:108–115`). Native Rust reads detect gzip by magic bytes and therefore could hide a misleading `.gz` suffix; native writes explicitly validate/compress (`iceberg-0.10.1/src/spec/table_metadata.rs:450–506`). The new refusal keeps this helper's implementation consistent with its plain-JSON scope.

The shared check also newly refuses compressed/invalid metadata on the local branch. Existing local production preparation omits or removes that property (`tables.rs:11–34,63–68`), so supported local preparation remains unchanged. The new remote refusal test covers mismatched table location, compressed metadata, compressed destination, and a destination outside owned scope, asserting no request or registration (`writes.rs:673–764`). Invalid codec parsing follows the maintained error path; it is not a separately executed case here.

### F3 — Resolved: native metadata assertion corrected

Category: test correctness. Location: `src/storage/remote/objects/tests/writes.rs:514`.

`Table::metadata()` already returns `&TableMetadata`; the unnecessary `.as_ref()` is now removed. Root's retained `docs/reviews/shared-candidate-tests-2026-10-10.log` preserves compiler error E0599 from the first run. This reviewer read the complete `shared-candidate-tests-2026-10-10-2.log`: compilation completed, and the focused run passed all eight write fixtures with no failures in 0.80 seconds. Root reports process exit 0. This is focused local HTTP/test-double evidence, not provider evidence.

### F4 — Resolved: acknowledged PUT followed by catalog error is exercised

Category: test coverage. Location: `writes.rs:532–595,826–846`; `tables.rs:162`.

`CandidateCatalog::register_table` now has a failure mode that records the identifier/path and returns a native Iceberg error. The new test permits the real localhost PUT to acknowledge, then invokes that mode. It asserts the propagated `iceberg` error, exactly one registration attempt for the retained URI, exactly one PUT with no read/delete/retry, and the original expected bytes still retained (`writes.rs:571–594`). A transport failure cannot satisfy these assertions because it would return a different error and never reach the catalog. This uses the existing fixture without another framework.

The success fixture's returned metadata equality assertion remains stub-derived; captured PUT bytes supply the meaningful serialization check. The new error is injected at the catalog trait boundary. It is not a real REST response lost after catalog commit, and it does not exercise reconciliation itself or provider crash recovery.

## Function trace and patch summary

The change adds a concrete local/remote writer choice to the existing admitted candidate worker. It shares one bounded serializer and the existing native conditional PUT core; it does not introduce remote Engine selection.

| Function | Source | Input → output and verified behavior |
| --- | --- | --- |
| `Engine::create_output`, `replacement_output`, `register_output` | `src/engine/write.rs:71,86,101` | Existing schema/base preparation → shared worker with explicit `Local`. |
| `fresh_metadata`, `replacement_metadata` | `src/storage/tables.rs:11,37` | Existing native ID-preserving preparation; no new schema reconstruction. |
| `RemoteStorage::register_candidate` | `src/storage/remote.rs:51` | Identifier, retained native location, metadata, budget → table/error; moves metadata into shared worker. |
| `CandidateWriter::metadata_path` | `src/storage/tables.rs:89` | Metadata → validated destination; local generation or exact retained remote URI; rejects incompatible encoding/layout. |
| `CandidateWriter::create_bytes` | `src/storage/tables.rs:122` | Owned bytes → local create-new or native conditional PUT, driven by the captured runtime handle within the same worker. |
| shared `register_candidate` | `src/storage/tables.rs:138` | Reserves allowance, prepares/serializes once, creates immutable bytes, then registers the successful URI; errors do not trigger cleanup or retry. |
| `bounded_json`, `BoundedJson::write` | `src/storage/tables.rs:165,188` | Native serialization → vector with checked length/growth limit; capacity refusal precedes PUT. |
| `RemoteObjects::create_bytes` | `src/storage/remote/objects.rs:129` | Existing public identity-checked upload; scope, capacity+length admission, length and digest checks remain intact. |
| `create_bytes_admitted`, `create_bytes_by_key` | `src/storage/remote/objects.rs:169,179` | Storage-private admitted vector → one scoped `write_with(...).if_not_exists(true)`; no nested worker/reservation. |
| `Scope::key`, `write_error` | `src/storage/remote/objects.rs:367,434` | Same strict bucket/prefix validation and redacted conflict/unknown-outcome error mapping. |
| `Budget::blocking`, `blocking_admitted` | `src/budget.rs:130,147` | Memory allowance and worker permit enter `spawn_blocking` and remain until that worker finishes. |
| remote catalog registration | `src/storage/remote.rs:237`; native `iceberg-catalog-rest-0.10.1/src/catalog.rs:940–1002` | Delegates to native REST registration with `overwrite=false`; builds returned table from catalog response. |
| `WriteFixture::for_path` / PUT branch | `writes.rs:210,288` | Real native HTTP request to localhost; captures bytes and checks condition/path, then acknowledges, rejects, or loses response. |
| `CandidateCatalog::register_table` | `writes.rs:826` | Records exact identifier/path, then injected native error or stub-derived table; failure mode performs no object operation. |
| changed provider-probe call | `examples/r2_candidate_probe.rs:157–161,208–220` | Retains planned URI and independent expected bytes/digest, then invokes the helper. Root must rebuild/run before claiming new provider evidence. |

The crate-private re-export is at `src/storage.rs:28`. Other touched callers (`src/engine/backfill.rs:1503`, `src/engine/write/tests.rs:308,731`, `src/storage/occurrences/tests/defaults.rs:33`) select `Local` without changing preparation.

## Data flow and invariants

- **Native identity:** The worker serializes supplied `TableMetadata` directly, preserving its native schema IDs, history, defaults and references. No second `TableCreation` builder reassigns fields in the new path. Preservation does not establish Engine support for every supplied layout or dependency.
- **Owned object scope:** The retained URI must round-trip as a canonical native metadata location directly beneath the supplied table location (`tables.rs:102–115`). `Scope::key` subsequently rejects other buckets/prefixes and unsafe path forms before PUT. Scope covers this object, not referenced manifests/data; caller authority and dependency protection remain explicit (`remote.rs:31–45`).
- **Work once:** One bounded serialization feeds one create-only call. The helper adds no preliminary HEAD/GET, extra hash pass, or copy of value files. Catalog response parsing remains native required work. The independent expected serialization in the provider probe is qualification work, not a new production pass.
- **Admission lifetime:** `tables.rs:145–160` shares the `2 * max_metadata_bytes` reservation between caller and worker. `Budget::blocking` adds its existing 1 MiB allowance and worker slot. While PUT is pending, cancellation drops the waiting caller but not worker-owned admission. Normal completion keeps caller admission during registration.
- **Memory limit:** These are modeled allowances, not measured bounds on the caller's preconstructed metadata graph, HTTP/library allocation, allocator overhead, or whole-process RSS (`src/budget.rs:47`). The caller allocates metadata before this API can admit it. Neither test nor static review establishes peak RSS.
- **Cancellation and uncertainty:** A held-PUT cancellation does not start registration afterward. Once registration has begun, dropping the caller or losing its response may leave it committed; retained identifier/URI reconciliation is required. A worker surviving task cancellation is not qualification for runtime shutdown or process crash.
- **Catalog result:** Native REST registration returns the catalog's metadata/location. The helper does not prove the catalog kept the caller's original URI or byte representation; the provider probe checks returned meaning, captures any returned immutable identity, and opens that exact pin. These observations require the new probe run.
- **Public limits:** The helper does not create namespaces, protect attempts, validate source admission, qualify referenced files, select a DocSpec release, or clean retained objects. No credential values or new diagnostic dumps are introduced.

## Tests and evidence limits

| Test / edge | Actual assertion scope |
| --- | --- |
| `remote_candidate_creation_sends_one_conditional_put_before_registration` (`writes.rs:441`) | Passed: planned URI and captured bytes, conditional request, no GET/HEAD/retry, registration only after acknowledged PUT. |
| `remote_candidate_keeps_created_metadata_when_catalog_registration_fails` (`writes.rs:532`) | Passed: acknowledged localhost PUT precedes injected catalog error; exact registration attempt, one PUT, original bytes, no cleanup/retry. |
| `canceled_remote_candidate_keeps_original_admission_until_put_finishes` (`writes.rs:598`) | Signal follows full PUT receipt while response is held; abort caller, confirm byte-pool refusal, release response, recover admission, assert no registration and one PUT. Does not directly assert worker-slot refusal. |
| `remote_candidate_refuses_mismatched_location_and_compression_without_effects` (`writes.rs:673`) | Refusal codes and empty HTTP/registration reports for the specified invalid inputs. |
| Existing serializer test (`tables.rs:225`) | Exact serialized limit, overflow refusal, bounded growth; unchanged implementation. |
| Existing local cancellation test (`tables.rs:262`) | Stalled preparation retains admission through local creation; only adds explicit local writer. |
| Catalog lost response / reconciliation | Not exercised by injected catalog error; remains a separate qualification boundary. |
| Fixture failure robustness | Existing socket/signal deadlines, release-on-drop, listener wakeup and join remain (`writes.rs:194–365`). These bound individual waits, not complete process/runtime teardown. |
| New real-provider helper path | Probe source now invokes helper; preceding provider receipts concern the old direct-primitives path and cannot qualify this call. |

## Conclusion

VERDICT: APPROVE for this bounded low-level implementation. All actionable review findings are resolved, and the focused local fixtures pass for the intended success/refusal/failure/task-cancellation branches. Confidence is HIGH for the reviewed code and assertion traces. Real-provider execution and the full repository gate remain root-owned and pending; this verdict does not claim those gates passed.

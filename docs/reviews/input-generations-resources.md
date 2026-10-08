# Related-input generation resource review

No open actionable finding remains. The manifest reader closes the unbuffered-I/O, cancellation-custody, and actual-byte-bound issues identified during review. **VERDICT: APPROVE** for this local metadata qualification checkpoint.

This is a static review of the uncommitted checkpoint against `9565e87`, using the semi-formal-code-review workflow and applicable Microsoft Rust performance, resilience, library, and universal guidance. The reviewer inspected source and pinned dependencies; the reviewer did not execute tests or change implementation. Scope is metadata qualification of accepted related inputs, not joined computation or refresh selection.

## Findings and closure

### IG-R1 — Unbuffered parsing made manifest scans unnecessarily expensive — CLOSED

- **Severity/category:** WARNING / performance.
- **Location:** `src/engine/inputs.rs:270` (`read_relationship`); initial implementation passed a raw File to `serde_json::from_reader` after hashing it.
- **Evidence:** Pinned `serde_json 1.0.151` explicitly does not buffer `from_reader` input (`src/de.rs:2518–2537`), and `IoRead::new` uses `reader.bytes()` (`src/read.rs:189–205`). A large manifest therefore caused byte-at-a-time file reads during parsing. Sealing held every member and family lock throughout that work.
- **Resolution:** The final reader reads a bounded, admitted byte buffer once, hashes that buffer, and calls `from_slice` on the same bytes (`src/engine/inputs.rs:281–309`). It avoids the unbuffered parsing pass and the earlier separate hash/read passes. This is a statically established reduction in file reads; no elapsed-time improvement is claimed without measurement.

### IG-R2 — Caller cancellation released metadata admission before its parser exited — CLOSED

- **Severity/category:** WARNING / resource correctness.
- **Location:** `src/engine/inputs.rs:279–288`; `src/budget.rs:73–95`.
- **Evidence:** Initially only the async caller retained the metadata permit. `Budget::blocking` retained its worker slot and fixed hash/copy allowance, but not that metadata permit. Cancellation could release the larger allowance while serde continued parsing. Even an invalid identifier can allocate its string before `Id::new` enforces its length (`src/model.rs:43–45`; pinned serde JSON `src/read.rs:218–244`).
- **Resolution:** Callers now share their metadata permit through an Arc. The blocking closure explicitly owns that Arc and a separately admitted manifest buffer until completion. Canceling the awaiting caller cannot drop either worker-owned reservation (`src/engine/inputs.rs:280–288`). The existing worker slot and I/O allowance also remain owned by the worker (`src/budget.rs:90–94`).

### IG-R3 — A small claimed manifest size did not bound actual-file I/O — CLOSED

- **Severity/category:** WARNING / resource correctness and performance.
- **Location:** `src/engine/inputs.rs:296–309`.
- **Evidence:** The initial path charged `ObjectRef.bytes` but called `verify_object`, which hashes a file to EOF before checking its declared size (`src/storage.rs:235–241`). A large file with a small claimed size could therefore cause a much larger scan than the metadata allowance indicated.
- **Resolution:** The new reader checks size on the opened file, reads exactly the admitted size, and probes one additional byte. A file that grows after the size check cannot induce an unbounded scan. Hashing and decoding use the same retained buffer, also removing the possibility of parsing different bytes on a second open. Short reads or size/digest disagreement fail before relationship acceptance.

## Patch summary

The change adds immutable producer relationship declarations, create-once generation seals, and capture of exact accepted related inputs. The first seal fixes a family's dataset membership; subsequent seals select exact versions for that membership. Captures retain member sets and seals for independent relationship checks. CLI and HTTP are thin wrappers over the same Engine methods; independent verification needs manifests but no producing catalog or payload scan.

Build request identity includes the declaration, while source-computation reuse remains keyed by material inputs. Rebuilds inherit the captured base declaration unless explicitly changed; unbased replacement of a coupled source or an explicit relationship change refuses. Updates and backfills preserve relationship metadata. The format and processor version advance, with no legacy migration promise.

The implementation reuses existing lifecycle commits, accepted-history traversal, shared admission, locking, and blocking I/O capacity. It introduces no SQL index, dependency, detached metadata producer, or mutable policy engine. Metadata reads are serial and conservatively bounded by cumulative manifest and retained-record sizes; this favors a small qualified implementation over an unmeasured parallel metadata pipeline.

## Function trace

| Function / method | File:line | Inputs | Output | Verified behavior |
| --- | --- | --- | --- | --- |
| `Engine::seal_generation` | `src/engine/inputs.rs:17` | Exact seal | Original or newly committed seal | Admits metadata, validates identities, locks members/family in sorted order, reconciles an existing seal before manifest reads, qualifies new members, and commits seal plus first family membership together. |
| `Engine::capture_inputs` | `src/engine/inputs.rs:79` | Dataset-to-version map | Retained coverage | Reads immutable headers to determine all locks, locks the complete set, checks accepted history/current eligibility, reads required seals/member sets, and checks exact relationships without selecting output. |
| `lock_inputs` | `src/engine/inputs.rs:139` | Sorted distinct dataset IDs | Owned guards | Acquires in BTreeSet order; previously acquired guards drop if acquisition is canceled. |
| `Engine::lock` | `src/engine.rs:135` | Dataset ID | Shared mutex | Releases the lock-map mutex before callers await an individual dataset mutex; no global mutex spans dataset lock acquisition. |
| `qualify_input` | `src/engine/inputs.rs:148` | Exact VersionRef | Source relationship | Uses one captured View, proves accepted history membership, checks exact release identity/format, and compares retained versus current eligibility. Caller owns dataset lock. |
| `follows` | `src/engine/history.rs:30` | Captured View and equal endpoints | Accepted-chain proof | Existing bounded traversal anchors the selected native commit and retains one SnapshotRows Arc across cooperative yields. It reads lifecycle metadata, not payloads. |
| `resolve_release` | `src/engine.rs:295` | View, dataset, version | Release or refusal | Shared private resolver avoids reacquiring a held dataset lock while preserving lookup-identity/verification checks. Public `release` remains a locking wrapper. |
| `verify_input_coverage` | `src/engine/inputs.rs:178` | Retained coverage and Limits | Success/refusal | Reads bounded manifest headers and verifies retained relationship structure without opening producer catalogs or certifying current eligibility. |
| `check_relationships` | `src/engine/inputs.rs:193` | Coverage and manifest declarations | Success/refusal | Requires one matching seal per family, exact fixed membership, exact consumed members, and no extra family/seal entries. A current seal also constrains an older consumed member declaring another relationship. |
| `validate_seal`, `validate_members` | `src/engine/inputs.rs:245`, `249` | Member maps | Validated identities | Refuses empty sets, mismatched map keys, and manifest-owner disagreement before lock/read work. |
| `read_relationship` | `src/engine/inputs.rs:270` | Version, shared budget, cumulative byte count, permit Arc | Minimal header relationship | Charges declared bytes first; worker owns reservations; opened-file size, exact read, overflow probe, digest, format, and identity checks precede acceptance. |
| `relationship` | `src/engine/inputs.rs:321` | Operation and optional base | Exact declaration | Explicit Build declaration wins; otherwise inherits base or defaults to Independent. |
| `check_rebuild` | `src/engine/inputs.rs:334` | Authority and request | Eligibility/refusal | Requires an explicit selected base to rebuild coupled input or explicitly change an existing relationship. |
| `metadata_size`, `check_size` | `src/engine/inputs.rs:360`, `380` | Serializable metadata and byte allowance | Charged size/refusal | Counting writer refuses before exceeding its allowance without allocating a second serialized copy; cumulative charging uses saturating arithmetic. |
| `fingerprint`, `phase_key` | `src/engine.rs:1144`, `1026` | Operation material | Retry and phase keys | Relationship changes alter operation identity; the existing source-computation key excludes composition-only metadata. |
| `verify_groups` | `src/engine/backfill/verification.rs:6` | Retained dataset and Budget | Independent acceptance/refusal | Checks Build derivation and Update/Backfill inheritance of the retained declaration against exact base metadata. |
| HTTP handlers and CLI commands | `src/http.rs:81`, `src/main.rs:144` | Bounded JSON or local metadata files | JSON results | Delegate to the Engine/free verifier; independent CLI verification remains before Engine creation. |

## Data flow and invariants

1. **Lock order is global and complete.** Seal locking begins with its member keys plus family; capture first reads immutable manifests to add family IDs. Both use the same BTreeSet-order helper (`src/engine/inputs.rs:26–28`, `86–96`, `139–145`). Family-equals-member is deduplicated. Existing single-dataset operations hold no second dataset lock, and `qualify_input` calls the nonlocking release resolver. This avoids nested lock reacquisition and inverted family/member ordering.
2. **Accepted state cannot change between qualification and sealing/capture.** Guards remain in scope while reading accepted history, current eligibility, and family metadata, then through seal commit or coverage construction. Guards are local owned values and release on refusal or cancellation. This is a one-host serialization guarantee using the existing workspace ownership model, not a newly qualified multi-host protocol.
3. **Retries resolve durable metadata before expensive work.** Seal equality and fixed membership are checked before qualifying member histories or opening manifests (`src/engine/inputs.rs:31–42`). A lost successful response can reconcile the existing seal even after later eligibility changes. A conflicting request returns `generation_conflict`; it does not overwrite. Creating a new seal still requires eligible accepted inputs.
4. **Hash and parse share exact bytes.** The final worker opens once, checks size, reads only the admitted bytes plus an overflow probe, hashes them, and decodes the same buffer (`src/engine/inputs.rs:289–309`). Payload inventories are ignored by the minimal header type. This proves the retained declaration's byte binding, not source completeness or authenticity of an externally supplied seal.
5. **Cancellation does not release live-worker admission.** The outer operation and current blocking parser share the metadata permit. The worker separately owns buffer admission and the existing blocking-job/I/O permits. Drop of the async task releases locks and caller state while the worker retains the resources it still uses. No new worker is spawned per manifest concurrently; reads are awaited serially.
6. **Capture is descriptive metadata.** InputCoverage escapes to its caller after local reservations drop, as other bounded metadata results do. It is not a reader lease or output-selection guard. Public documentation explicitly requires a fresh guard at later selection and excludes payload availability, acquisition completeness, and current eligibility certification from independent verification (`src/engine/inputs.rs:70–78`, `169–177`). Cleanup remains disabled.
7. **Finite metadata limits are conservative.** Request/coverage serialization, manifests, seals, and fixed membership consume the combined allowance. Existing lifecycle history and accepted-chain traversal apply their own finite caps. The operation remains dependent on member count, manifest bytes, and accepted-history length; it is not a constant-time lookup and does not qualify aggregate process RSS. Full ReleasePin and Authority deserialization in qualification remains bounded by existing lifecycle limits and is a potential future optimization if measurement identifies it as significant.

## Test behavior and edge cases

The following assertions were inspected, along with their passing owner-produced results in `docs/reviews/input-generations-check.log`. No reviewer execution was performed.

| Test and location | Assertion trace | Disposition |
| --- | --- | --- |
| `independent_and_coupled_inputs_follow_the_entire_consumed_member_map`, `tests/input_generations.rs:79` | Independent versions combine without a seal; coupled inputs refuse before sealing. Complete historical/current generations verify, mixed generations and a mismatching old independent/foreign-family member refuse. A valid next-generation source cannot shrink established family membership. A subset or unrelated independent input remains allowed. | Pass; exercises entire consumed-map checks rather than only the current head. |
| `omitted_relationship_inherits_and_explicit_transitions_require_the_selected_base`, `tests/input_generations.rs:173` | Unbased changes refuse before operation admission; JSON omission with an exact base inherits; source TablePin remains exact; the same operation ID with a changed declaration conflicts; explicit based Independent transition verifies. | Pass; covers request identity and source-work reuse. |
| `updates_and_backfills_inherit_but_resealing_requires_exact_new_members`, `tests/input_generations.rs:242` | Backfill and Update inherit relationship metadata and independently verify, but their distinct versions do not replace an already sealed member. An explicit next-generation rebuild reuses exact source and derived Parquet files before a new seal. | Pass; distinguishes reusable values from exact generation membership. |
| `stable_family_membership_and_exact_retry_survive_restart_and_invalidation`, `tests/input_generations.rs:331` | Family equals a member; competing full/partial first seals run under a timeout and exactly one wins. Later membership changes refuse. Invalidation blocks a new capture while offline prior coverage remains verifiable. After restart and manifest removal, the exact winner reseal succeeds. | Pass; directly covers deduplication, concurrent reconciliation, and retry before manifest reads. |
| `offline_relationship_checks_need_only_captured_manifests_and_detect_tampering`, `tests/input_generations.rs:398` | Hides the catalog and removes retained Parquet payloads; metadata-only verification succeeds. Missing/mutated seals or families refuse. A one-byte-short cumulative allowance returns metadata_capacity, false declared size returns corrupt_object, and altered manifest bytes fail their digest check. | Pass; covers metadata-only scope and IG-R3 refusal behavior. |
| `invalid_seals_and_rebound_input_keys_refuse`, `tests/input_generations.rs:503` | Independent members cannot form a coupled seal; empty sets and wrong dataset/owner bindings refuse. | Pass; validates identities before lock acquisition and reads. |
| `independent_release_verification_rejects_rewritten_producer_relationships`, `tests/input_generations.rs:551` | Changes a retained declaration and recomputes the manifest digest for new, explicitly coupled, inherited, and backfilled releases. Independent full verification still refuses the request/base disagreement. | Pass; the hash alone cannot legitimize an inconsistent declaration. |
| `cli_and_http_use_the_same_generation_guards`, `tests/input_generations.rs:603` | HTTP seal/capture responses retain exact members; real CLI seal and capture commands succeed. After catalog hiding, CLI verify-inputs succeeds without recreating it. | Pass; covers shared interfaces and the catalog-free branch. |
| `canceled_manifest_read_keeps_admission_until_its_blocking_work_exits`, `src/engine/inputs.rs:399` | Queues the actual manifest reader behind a controlled blocking job, aborts its async caller, and asserts that the worker alone retains the metadata Arc. Exact remaining-capacity assertions include both the buffer and fixed I/O allowance; full capacity returns only after worker completion. | Pass; directly exercises IG-R2 without a production test hook. |
| `canceled_partial_input_lock_acquisition_releases_prior_locks`, `src/engine/inputs.rs:457` | Holds the second sorted lock, waits until the task owns the first, aborts that task, and reacquires the first and then second lock. | Pass; verifies cancellation unwinds partially acquired guard sets. |

Existing fixture edits supply the new optional `RunSpec.relationship` field as None and preserve their prior intent. Actual joins, refresh promotion against captured inputs, retention protections, and multi-host execution are outside this checkpoint and are not inferred from these tests.

The retained gate output was inspected through the new tests, existing regression suites, doc tests, and final no-default-features build. It shows successful formatting/Clippy progression and passing tests for the owner's `./scripts/check` run, followed by successful `cargo check --locked --no-default-features`. README documents the exact member rules and unfinished processing scope (`README.md:186–228`). STATUS documents worker custody, cumulative metadata accounting, existing lifecycle replay costs, disabled cleanup, and the absence of production-memory or Comments performance qualification (`docs/STATUS.md:426–475`).

## Conclusion

VERDICT: APPROVE

Justification:

- The implementation closes the concrete resource findings and retains one consistent sorted locking discipline. Exact retries reconcile before manifest reads; new qualification remains bounded and metadata-only.
- Coverage of changed paths: ADEQUATE. The actual reader, cancellation ownership, partial locks, concurrent seals, missing-manifest retries, cumulative limits, interfaces, and independently verified declarations are exercised.
- Confidence: HIGH for this local metadata checkpoint. No joined processing, distributed selection, retention, aggregate RSS, or full-workload performance claim follows.

# Canonical scalar completion: consistency and recovery review

**Verdict: APPROVE for this increment.** No required correctness changes found. The returned completion, native generation and retained protection come from the same winning record, and assembly now consumes that winner. The coverage observation below is nonblocking. Persistent original-execution records and Q4 dependency correction remain future work.

Reviewed on 2026-10-10 against `9c818a0976c15ea4177e72f44b04ba93ad7436b3`. Scope: the uncommitted `src/engine/backfill/preparation.rs` changes and new `src/engine/backfill/file_completion_tests.rs`. This was an independent static review under the semi-formal code-review and Rust skills, including the universal and performance guidelines. No builds, tests, benchmarks, formatting, commits or source edits were performed. Test outcomes below are source-derived expectations, not execution results.

## Findings

### F1 — Direct mixed-bundle and distinct-owner-scope assertions would strengthen coverage

- **Severity:** OBSERVATION
- **Category:** test-coverage
- **Location:** `src/engine/backfill/file_completion_tests.rs:124`, `src/engine/backfill/file_completion_tests.rs:146`, `src/engine/backfill/preparation.rs:467`.
- **Description:** Both new tests acknowledge one key at a time. The concurrent fixture distinguishes unknown protection from a known empty scope, but does not give the competing completions different nonempty owner-grant sets. No direct test was found for a returned bundle containing both an existing generation and newly committed generations, or for flushing pending new completions when an existing descriptor cannot be read.
- **Evidence:** `pending` creates a singleton map; the competing scope is created with `ReferenceProtections::empty`; the revision fixup separately updates only keys in `new_keys`. Existing foreign-owner recovery asserts a real token set throughout the scalar path (`src/engine/backfill/protection_tests.rs:60`, `:115`), and existing failed-file recovery checks that prior successes survive (`tests/backfill.rs:794`). Those cover the constituent mechanisms, but do not directly combine these particular branches.
- **Suggestion:** In a later focused test, mix a retained key with a new key and assert their different native revisions; use distinct retained nonempty scopes to assert the winning tokens, and force the existing-descriptor error after one new candidate. This is additional regression protection, not evidence of a current implementation defect.

## Patch summary

`AcknowledgedFile` packages the canonical `FileCompletion`, its exact `RetainedGeneration`, and its retained owner-protection scope (`src/engine/backfill/preparation.rs:9`). `acknowledge_files` resolves a concurrent winner while holding the dataset lock, commits only missing or retired keys, then returns the winning records (`:386`). `FileAssembly::append` reads the returned descriptor, adopts its output identities, and inherits its scope (`:24`). This removes the former possibility that assembly used one worker's physical outputs while the durable completion key named another worker's output generation.

`prepare_group` uses the same assembly helper for already retained files and newly acknowledged batches (`src/engine/backfill/preparation.rs:121`, `:208`, `:223`). Important callers are backfill preparation (`src/engine/backfill.rs:642`) and automatic maintenance of existing computed groups during a source operation (`src/engine.rs:1518`). Public backfill enters admitted work at `src/engine/backfill.rs:426`.

The concrete simplification is one path for descriptor validation, output identity adoption, and scope inheritance. New completions in one bundle share one acquired `ReferenceProtections` instance through `Arc`, while each serialized binding is still counted independently (`src/engine/backfill/preparation.rs:422`). No speed or memory improvement was measured.

## Function trace

| Function or method | Source | Inputs | Output | Verified behavior |
| --- | --- | --- | --- | --- |
| `prepare_group` | `src/engine/backfill/preparation.rs:54` | Captured source, operation, group definition, source protection, stop point | `PreparedGroup` | Resolves retained preparation first; otherwise assembles canonical file work, saves preparation with its protection, and validates the exact prepared output. |
| `FileAssembly::append` | `src/engine/backfill/preparation.rs:24` | Acknowledged records, compiled expressions, eligibility, budget | Updated descriptors, inherited output references and scope | Checks material key, charges descriptor accumulation, validates/reads the winner descriptor, then adopts its output identities and scope. |
| `acknowledge_files` | `src/engine/backfill/preparation.rs:386` | Pending candidates and acquired source scope | Canonical records with exact revisions | Locks dataset; checks operation authority; chooses active winner or prepares new claim plus binding; commits once; sets only new revisions from the successful commit; clears pending only on success. |
| `compute_file` | `src/engine/backfill/preparation.rs:479` | Source file, occurrence scan, expressions, output table | Candidate `FileCompletion` | Streams bounded computation, checks row total, writes a bounded descriptor, and records output object identities. The candidate is not yet a durable completion. |
| `reserve_group` | `src/engine/backfill/preparation.rs:348` | Operation and processing purpose | Owned output directory | Saves attempt protection under the dataset lock before creating the directory. Losing physical files remain within retained attempt protection. |
| `active_completion` | `src/engine/retention/retirement.rs:288` | Immutable lifecycle view and material key | Active typed record or none | Uses the native row revision and rejects that exact retired generation. |
| `retained_protections` / `local_binding` | `src/engine/retention/owners.rs:436`, `:454` | Same lifecycle view and claim key | Known scope or unknown legacy scope | Checks claim/binding revision and digest through `validate_binding`; a retired or mismatched binding cannot qualify current work. |
| `completion_records` / `ReferenceProtections::binding` | `src/engine.rs:96`; `src/engine/retention/owners.rs:146` | Completion and acquired scope | Claim and optional binding records | Writes claim and binding in the same lifecycle commit; binding names `view.revision + 1` and the exact claim digest. |
| Lifecycle `commit` | `src/catalog.rs:288` | Captured view and bounded records | Committed native view | Rejects duplicate keys or excessive metadata, uses guarded native append, and exposes its next immutable cache only after success. |
| `file_key` | `src/engine/backfill.rs:1062` | Exact source object, compiled dependencies, eligibility | Material completion key | Binds source object and expression dependency digest, with eligibility in the key prefix. |
| `charge_assembly` / `read_completion` | `src/engine/backfill.rs:1135`, `:1070` | Completion and native output schema | Charged metadata / `DataFile` list | Limits aggregate descriptors; checks output availability, descriptor digest, distinct paths, sizes and row totals. |
| Storage `append` / `pin` | `src/storage.rs:354`, `:584` | Winner descriptors and winner output references | Exact native output pin | Appends selected files; verifies native row count; reuses inherited object identities and rejects conflicting identities for the same path. |
| `ReferenceProtections::inherit` / `merge` | `src/engine/retention/owners.rs:80`, `:104` | Current and consumed scopes | Conservative union or unknown | Unknown remains unknown; known scopes are bounded, merged and deduplicated without acquiring new owner grants. |
| `admit_work` / `maintain_work` | `src/engine/worker.rs:111`, `:136` | Full worker future | Owned execution under shared/exclusive guard | Ordinary work holds the shared maintenance guard; retirement holds the exclusive guard. |
| `retire_phases` / `retire_retained_admitted` | `src/engine/retention/retirement.rs:462`, `:478` | Exact retirement plan | Durable retirement receipt | Waits for admitted work, locks the dataset, and rejects replaced/retired planned generations before recording retirement. |
| New test `fixture` | `src/engine/backfill/file_completion_tests.rs:27` | Temporary engine | Two candidates for one source/key | Computes physically different outputs with matching material inputs. |
| New test `pending` / `assembly` | `src/engine/backfill/file_completion_tests.rs:124`, `:128` | Candidate / engine | Singleton pending map / empty assembly | Constructs bounded test state; the assembly begins with a known empty scope. |

## Data flow and invariants

1. **A durable winner and its provenance stay together.** Pending candidates originate at `compute_file` and enter the map at `preparation.rs:203`. Under the lock, an existing winner replaces the candidate value together with its native row revision and retained scope (`:410–420`). For a new winner, claim and binding are queued together and the returned generation is filled from the committed view (`:431–472`). The candidate is never appended before this decision. The invariant would break if a future caller combined `candidate.outputs` with an acknowledgment's scope or revision.

2. **Every reused record is read from one immutable view.** The fast path uses `completion_view` for completion, revision and protection (`preparation.rs:124–140`). Acknowledgment likewise uses one locked `view` (`:399–420`). `validate_binding` requires binding and claim revisions to match, and checks the claim digest (`owners.rs:233–266`). New commits at other keys can advance the catalog without changing the captured record.

3. **Retirement cannot replace a consumed generation mid-assembly through the supported service path.** Backfill runs inside `admit_work` (`backfill.rs:434`); the shared guard lasts for the owned future (`worker.rs:130–133`). Phase retirement and owner-grant retirement use exclusive maintenance (`retirement.rs:469–475`; `grants.rs:44–46`). Thus the stale read view does not permit a service-level retire/replacement race during this admitted operation. This is a local hosted execution guarantee, not multi-host qualification.

4. **The final pin uses the same physical files whose scope was inherited.** `FileAssembly` reads `file.completion`, extends inherited references from that completion, and merges only that record's scope (`preparation.rs:35–43`). `storage::pin` receives those references (`:237–244`) and rejects conflicting shared identities (`storage.rs:608–618`). `PreparedCompletion` and its binding are saved together (`preparation.rs:264–279`). A concurrent prepared-group winner also returns its own table and scope (`:249–261`).

5. **Unknown provenance does not become qualified through reuse.** `None` on any consumed scope clears the assembly's qualified scope; later known scopes cannot restore it (`owners.rs:80–91`). This includes an unknown legacy completion winning over a newly computed candidate with known source protection. Starting assembly with the current source's protection remains necessary because its prepared dependency record also captures that source (`preparation.rs:112`, `:265`).

6. **The durability boundary precedes fallible assembly.** Newly computed bundles are committed before the `files` interruption and descriptor assembly (`preparation.rs:208–219`, `:223–234`). A later descriptor, metadata, pin or validation error does not undo those completion records. On a compute error, pending successes are acknowledged before the failure record (`:177–197`). On an existing-descriptor assembly error, pending successes are acknowledged before returning the error (`:142–158`). A failed acknowledgment leaves its input map uncleared (`:467–476`); a lost response can be reconciled by a later fresh lookup.

7. **Recovery remains batched and bounded.** `COMPLETION_BUNDLE` retains the existing batch limit and rationale (`backfill.rs:16–18`); the pending map also flushes at the existing metadata fraction (`preparation.rs:203–207`). Acknowledgment independently bounds serialized claims/bindings and returned completion metadata (`:438–457`). Assembly retains the aggregate descriptor bound and request reservation (`:117–119`; `backfill.rs:1135`). No payload population or per-row lifecycle journal was introduced. This establishes checked bounds in source, not measured peak memory or throughput.

The generation revision escapes acknowledgment in `AcknowledgedFile`, but is not persisted as an execution-origin edge in the prepared group. `FileAssembly::append` checks the key, then consumes the completion and scope; its revision is not used afterward (`preparation.rs:31–43`). `GroupDependencies` still stores source/output native snapshots (`backfill.rs:49–58`). Persistent original execution and dependency-correction records are explicitly outside this increment; the patch should not be described as completing those guarantees.

## Test behavior and concrete edge cases

All expectations here are static. The author must supply execution evidence separately.

| Test | Source | Expected behavior and supporting trace |
| --- | --- | --- |
| `concurrent_acknowledgments_assemble_one_winner_with_its_own_scope` | `src/engine/backfill/file_completion_tests.rs:140` | Expected to pass: the dataset lock serializes acknowledgments; both receive the first committed generation and its scope. The test deletes the losing descriptor before assembling both results, so reading either loser's descriptor would fail. It asserts output identities, descriptor, generation and known/unknown scope consistency, then checks native row revision and binding. |
| `retired_completion_gets_a_new_generation_and_retry_keeps_that_winner` | `src/engine/backfill/file_completion_tests.rs:229` | Expected to pass: explicit retirement makes the first key inactive; replacement commits with a later revision and new scope. A cold reopen and retry with the old candidate receive the replacement's generation, files and scope. Its direct `stop_after` call confirms the error code; full path stop placement is covered by the existing tests below. |
| `scalar_completions_keep_foreign_scope_across_stops_reuse_and_reopen` | `src/engine/backfill/protection_tests.rs:116` | Expected unchanged: actual foreign-owner tokens remain bound to file/prepared/validation claims across repeated stops and reopen; payload identity and owner revision remain unchanged. The new assembly merges the same retained scopes. |
| `retained_file_handoffs_resume_after_reopen_and_external_input_loss` | `tests/backfill.rs:122` | Expected unchanged: each durable stop leaves saved completions; reopening without the external input resumes and preserves their serialized values. |
| `killed_backfill_workers_resume_retained_files_validation_and_readback` | `tests/backfill.rs:176` | Expected unchanged: file acknowledgment still precedes the `files` stop; resumed work names exclude already acknowledged file computation. Exact combined values and retained records are asserted. |
| `stale_source_retains_files_and_catchup_reuses_unchanged_objects` | `tests/backfill.rs:278` | Expected unchanged: stale promotion refuses while a new source operation reuses unchanged file completions and computes changed files; combined values match the independent expected rows. |
| `failed_files_remain_distinct_from_successful_nulls_and_completed_files_survive` | `tests/backfill.rs:794` | Expected unchanged: pending successful work is acknowledged before recording a later computation failure; successful nulls, incomplete files and failures remain distinct; corrected input can finish. |
| `scalar_preparation_cannot_reuse_validation_from_a_retired_generation` | `tests/phase_retirement.rs:382` | Expected unchanged: retained file work survives prepared-group retirement, but old validation refuses the new table generation until explicitly retired. Historical/current result protection remains independent. |
| `native_rename_reuses_file_completions_but_name_reuse_changes_input_meaning` | `src/engine/backfill.rs:1289` | Expected unchanged: a stable field-ID binding reuses the original files; reusing the old display name for a different field ID changes the material key and produces distinct values. |
| `aggregate_completion_descriptors_cannot_exceed_assembly_allowance` | `src/engine/backfill.rs:1477` | Expected unchanged: a later descriptor exceeding the cumulative allowance returns `metadata_capacity`; the new helper uses the same charge function. |
| `canceled_caller_keeps_blocking_work_gate_and_host_until_real_exit` | `src/engine/worker/tests.rs:35` | Expected unchanged: aborting the caller does not release the worker's maintenance guard or host; exclusive maintenance remains pending until real work exits. |
| `a_new_claim_blocks_stale_execution_until_every_exact_claim_retires` | `src/engine/retention/grants/tests.rs:112` | Expected unchanged: fresh live consumer bindings block grant retirement; ending one phase promise does not retire protection required by another. |

Concrete unexecuted cases traced in source:

- **Existing and new winners in one bundle:** existing revisions remain unchanged; `new_keys` alone receives the new committed revision. Low residual risk; F1 records missing direct coverage.
- **Assembly fails after durable acknowledgment:** acknowledgment survives; partial in-memory assembly is discarded, and retry can use retained file work. Existing stop/reopen tests cover the durable boundary; a forced descriptor-error case is not directly asserted.
- **Retirement before a new admitted attempt:** exact retired file generation is absent to reuse, so a replacement completion receives a new revision. Covered by the new retirement test.
- **Retirement requested while an admitted worker runs:** exclusive maintenance waits; a later stale plan cannot retire a different generation. Shared worker and retirement code provide the trace; the new unit fixture does not directly exercise this service-level race.
- **Empty source:** no candidates are acknowledged; append on the empty vector is a no-op, and storage append preserves the empty output table (`preparation.rs:223–244`; `storage.rs:359`). Existing empty-source behavior remains unchanged.
- **Lost commit response:** `commit` can return an error after native success (`catalog.rs:403–412`); no acknowledged vector escapes on that failed call. A retry's fresh active lookup finds the durable value/revision/binding. No new direct scalar lost-native-reply test was found; existing cold-retry tests cover retained lookup, and this patch does not change native response reconciliation.

## Review hypotheses and resolution

- **H1:** A concurrent loser can still feed its own descriptor to final assembly. **Refuted** for the changed path: candidates are acknowledged before append, and the returned retained winner replaces value and scope together (`preparation.rs:208–219`, `:410–420`).
- **H2:** A retained binding can refer to a different completion revision. **Refuted** for valid lifecycle data: both are loaded from one immutable view, binding validation checks revision/digest, and new claim/binding records share one guarded commit (`owners.rs:249–266`; `engine.rs:103–109`).
- **H3:** Logical retirement can invalidate a winning scope after acknowledgment but before prepared completion. **Refuted** within the supported admitted host path by shared/exclusive maintenance exclusion (`worker.rs:130`, `:153`).
- **H4:** Delaying descriptor assembly loses completed work on a later failure. **Refuted** for the changed durable boundary: completion acknowledgment now precedes descriptor assembly and the injected stop (`preparation.rs:208–234`). Unacknowledged candidates remain subject to the existing bounded-bundle recovery limit (`backfill.rs:16–18`).
- **H5:** Returning a revision establishes durable original-execution history. **Refined:** it establishes an exact transient canonical handoff; persistent origin history remains outside this patch (`preparation.rs:31–43`; `backfill.rs:49–58`).

## Reviewed artifact identity and conclusion

The source files did not change between the initial read and the final reread. SHA-256:

- `src/engine/backfill/preparation.rs`: `af87528c9d7eea8099e019558224fdcdc73f5d80dd0a3ccede6cdc97cc48a12c`
- `src/engine/backfill/file_completion_tests.rs`: `e3e41c902e654a5e877b51f69b847fc53ae1966f1dc03aab846a812f82d1df2f`

**VERDICT: APPROVE**

Justification:

- The patch achieves the scoped canonical handoff: assembled output references, returned exact generation and propagated retained protection identify the same winning file completion.
- Coverage of changed paths: **ADEQUATE for this increment**, with the direct mixed-bundle, descriptor-error and distinct-token observations above. Runtime results are outside this static review.
- Confidence: **HIGH in the static consistency/recovery trace**. This review does not qualify measured resource use, multi-host execution, persistent original-execution descriptors, dependency correction or full Q4 behavior.

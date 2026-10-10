# Canonical scalar completion: efficiency and simplicity review

VERDICT: APPROVE for this increment.

No blocking correctness or resource-bound regression found. Assembly now consumes
the acknowledged winner with its matching native generation and protection. The
change keeps bundled commits and introduces no additional source/output payload
scan. This is a static review, not a throughput or peak-memory measurement.

## Findings

**OBSERVATION — test coverage:** The mixed acknowledgment bundle lacks a focused
test. `acknowledge_files` can return an existing winner alongside a newly committed
key, updating revisions only for `new_keys`
(`src/engine/backfill/preparation.rs:409-475`). The new fixtures submit one key at
a time (`src/engine/backfill/file_completion_tests.rs:124-125,144-158,236-283`).
The implementation trace is correct, so this is nonblocking. A useful follow-up
would submit both kinds together, assert the old and new revisions separately,
and exercise a small combined metadata allowance without partial acknowledgment.
The existing assembly-cap test covers `charge_assembly`, not that combination
(`src/engine/backfill.rs:1477-1497`).

## Patch summary and review boundary

Reviewed the uncommitted preparation changes against `9c818a0` and the complete
new test file. Final reread matched these Git blob hashes:

- `src/engine/backfill/preparation.rs`: `f433c1c84a1c308ce05e437ceddcfe4d2934f7fe`
- `src/engine/backfill/file_completion_tests.rs`: `c5f3ce347f9bb078770d23f0d90d9bbe02436d12`

The accepted requirement is canonical scalar completion plus its exact native
generation/protection under concurrency, with bounded acknowledgment and cheap
recovery. Full Q4 dependency correction and persistent original-execution
descriptors remain future scope. Read workspace/repository instructions,
`docs/PLAN.md`, applicable R3 requirements, current status, the semi-formal review
skill, and the ms-rust performance/universal guidance. No builds, tests,
benchmarks, formatting, commits, or Rust edits were performed by this reviewer.

Exploration hypotheses: (H1) assembly waits for the canonical winner—confirmed;
(H2) added metadata remains bounded without scanning values—confirmed at the
code/admission level, not as a measured heap bound; (H3) shared protections remain
valid through assembly and failures—confirmed through owned scope values and
the existing admitted-worker/maintenance exclusion.

## Function trace

| Function | Evidence | Input → output; verified behavior |
| --- | --- | --- |
| `backfill_internal` / `prepare_backfill` | `src/engine/backfill.rs:426-436,637-661` | Admitted request and source protections → `prepare_group`; the caller keeps the source scope alive across the await. |
| `prepare_group` | `src/engine/backfill/preparation.rs:54-160,203-280` | Pinned source, expression definition, scope → prepared table and scope. Retained hits bypass compute; new candidates enter a bounded pending bundle; only acknowledged values enter assembly. Prepared-result reuse remains ahead of file processing. |
| `AcknowledgedFile` / `FileAssembly::append` | `src/engine/backfill/preparation.rs:9-45` | Completion, native key/revision, matching scope → checked descriptors, moved output references, merged protection. The record stays owned through descriptor reads; a failed read cannot publish the partial assembly. |
| `acknowledge_files` | `src/engine/backfill/preparation.rs:386-476` | Pending candidates → canonical records. One dataset lock and immutable view select existing winners or commit missing claims and bindings; only new keys receive the returned commit revision. Pending clears only after success. |
| `compute_file` | `src/engine/backfill/preparation.rs:479-542` | One source file → written output, Avro descriptor, object references. Existing bounded batch processing and creation-time hashing are unchanged. |
| `file_key` / `charge_assembly` | `src/engine/backfill.rs:1062-1067,1135-1146` | Source/material dependencies → reuse identity; descriptors/output references → cumulative assembly-cap check before decode. |
| `read_completion` | `src/engine/backfill.rs:1070-1106` | Canonical completion → native `DataFile` descriptors. Checks output availability, verifies and decodes the descriptor, and checks distinct paths, sizes and row totals. |
| `active_completion` / `Lifecycle::rows` | `src/engine/retention/retirement.rs:288-303`; `src/catalog.rs:203-281` | Exact view/key → active typed value and cached native revision. Retired generations are excluded; repeated same-view reads normally return an `Arc` cache hit. |
| `completion_records` / `Lifecycle::commit` | `src/engine.rs:96-109`; `src/catalog.rs:288-425` | Claim and scope → claim/binding records, then one guarded native append. Binding and claim share the native revision; failed or lost responses return before pending clears. |
| `retained_protections` / `local_binding` | `src/engine/retention/owners.rs:436-494` | Exact view/claim → scope after local revision/digest/retirement checks. Does not reopen owners. |
| `ReferenceProtections::{duplicate,inherit,merge}` | `src/engine/retention/owners.rs:60-143` | Accounted scope → owned copy/merged scope. Unknown scope remains unknown; merged references deduplicate and release transient charges. |
| `storage::{check_available,append,pin}` | `src/storage.rs:117-167,354-368,584-645` | Output refs/descriptors → filesystem size checks and native metadata assembly. Inherited data files reuse known identities instead of being rehashed by `pin`. |
| `admit_work` / `maintain_work` / retirement | `src/engine/worker.rs:111-155`; `src/engine/retention/retirement.rs:462-475` | Public work holds shared maintenance admission; phase retirement requires exclusive admission and waits for it. A retained revision need not be reopened after each descriptor read. |

## Data flow, allocations and invariants

- **Canonical choice:** Before acknowledgment, `pending` owns candidate metadata.
  Under the dataset lock, an existing active completion replaces that candidate
  in the returned value, together with the revision and binding read from the
  same view. New records acquire their final revision after native commit
  (`preparation.rs:396-475`). Assembly validates the material key and consumes
  that value (`preparation.rs:31-43`); it never reads the losing descriptor.
- **Bounded bundle:** `COMPLETION_BUNDLE` remains 16; the pending byte trigger
  remains one eighth of the metadata allowance
  (`src/engine/backfill.rs:16-18`; `preparation.rs:203-206`). Serialized claims
  plus bindings are capped together, and returned completion metadata receives
  a separate cumulative cap (`preparation.rs:438-457`). `metadata_size` counts
  through a writer without allocating serialized bytes
  (`src/engine/inputs.rs:484-503`). No per-row lifecycle transaction was added.
- **Actual copies:** A new completion is cloned into the returned vector while
  `pending` remains available for failed acknowledgment; JSON claim/binding values
  also coexist until commit (`preparation.rs:431-465`). These are metadata, not
  Arrow payloads. New entries share one `Arc<ReferenceProtections>` instead of
  acquiring a scope copy per entry (`preparation.rs:422-430`). Assembly moves
  `completion.outputs` rather than cloning that vector (`preparation.rs:42`).
  Protection merging still creates transient owned copies and deduplicates them
  (`owners.rs:80-143`). This establishes a concrete allocation simplification,
  not a measured speedup or total-resident-memory ceiling.
- **Metadata work:** The additional revision lookups use `Lifecycle::rows` and
  the existing cache, followed by a map lookup (`preparation.rs:130-135,402-419`;
  `src/catalog.rs:53-54,203-215`). The cache is shared and holds one snapshot, so
  another dataset can still cause a lifecycle-metadata cache miss; this patch
  does not remove that existing limitation. Assembly also repeats the small
  material-key hash as an identity check (`preparation.rs:32`).
- **Descriptor/payload work:** Reuse still performs output filesystem metadata
  checks plus descriptor digest/decode (`src/engine/backfill.rs:1081-1106`;
  `src/storage.rs:113-134`). No value reread or output digest pass was added.
  Newly computed losing output can remain retained under the attempt's existing
  protection, as before; this increment does not reclaim it
  (`preparation.rs:348-381`).
- **Capacity-refusal timing:** Assembly charges now occur after canonical
  acknowledgment. Therefore a capacity refusal can follow computation of the
  remaining bounded bundle, whereas the prior loop checked each candidate
  immediately. That bounded extra work is retained for recovery; charging the
  canonical result avoids refusing solely on a losing candidate's descriptor.
  Aggregate decoded assembly remains capped before each descriptor is opened
  (`preparation.rs:35-41,203-234`).
- **Scope lifetime:** The returned `Arc` owns the protection reservation through
  descriptor consumption; assembly keeps its merged owned scope through prepared
  completion commit (`preparation.rs:31-43,272-280`). `None` remains unknown,
  including when mixed with known scope (`owners.rs:79-91`). Runtime generation
  identity is returned with the completion; this patch does not add persistent
  original-execution metadata.

## Tests and failure branches

| Test/scenario | Static result and evidence |
| --- | --- |
| `concurrent_acknowledgments_assemble_one_winner_with_its_own_scope` | Expected to pass: both candidates exist before acknowledgment; the dataset lock serializes choice; returned revision, descriptor, outputs and scope presence agree. The loser descriptor is removed before both assemblies, so a losing-descriptor read would fail (`file_completion_tests.rs:140-225`). |
| `retired_completion_gets_a_new_generation_and_retry_keeps_that_winner` | Expected to pass: explicit retirement excludes the old generation; replacement gets a later revision; reopening and retrying with the old candidate still returns and assembles the replacement (`file_completion_tests.rs:229-298`). |
| `scalar_completions_keep_foreign_scope_across_stops_reuse_and_reopen` | Expected behavior preserved: exercises actual foreign protection tokens, claim/binding revision identity, unchanged payload metadata and owner revision over stops/reopen (`src/engine/backfill/protection_tests.rs:60-88,116-255`). |
| `killed_backfill_workers_resume_retained_files_validation_and_readback` | Expected behavior preserved: stop after durable file acknowledgment resumes assembly/validation/readback without scalar recomputation; final values and retained records are checked (`tests/backfill.rs:176-268`). |
| `aggregate_completion_descriptors_cannot_exceed_assembly_allowance` | Existing helper-level bound still applies through `FileAssembly::append`; expected refusal remains `metadata_capacity` (`src/engine/backfill.rs:1477-1497`). |
| Compute failure | Earlier pending work is acknowledged before failure recording and return (`preparation.rs:177-197`); same recovery boundary as baseline. |
| Reused descriptor or assembly failure | Pending fresh work is acknowledged before returning an error on the retained-hit path; new-bundle work was already committed before append (`preparation.rs:142-158,208-234`). The partial assembly remains local. |
| Commit conflict, capacity refusal, lost response | No acknowledged vector escapes and `pending.clear()` is not reached on failure (`preparation.rs:438-475`). A later retry reopens retained state; native lost-response injection precedes cache publication (`src/catalog.rs:403-421`). No new focused lost-response or mixed-bundle-cap test was found. |

## Conclusion

VERDICT: APPROVE.

- The patch achieves the scoped winner/generation/protection requirement and
  preserves bounded file acknowledgment and recovery.
- Coverage of changed paths: ADEQUATE for the narrow invariant, with the
  nonblocking mixed-bundle/resource-fault gap recorded above.
- Confidence: HIGH in the static identity/lifetime trace; performance and actual
  memory consumption remain unmeasured by this review.

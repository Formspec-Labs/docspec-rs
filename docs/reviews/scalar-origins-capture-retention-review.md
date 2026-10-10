# Scalar origins capture and retention review

Current static disposition: **APPROVE**. The final disposition below closes both
findings retained here as review history; execution evidence belongs to the
implementation checkpoint.

Review date: 2026-10-10. Static review of the working diff against
`8cb752574288e9830252c4f8cb7d439ae7311548`. The implementation was being integrated
concurrently. This review covers capture, acknowledgment, retention, owner
qualification and the supplied tests. The concurrently authored origin verifier
is outside this review. No build, formatting, test, benchmark or corpus command
was run.

## Findings

### P2 — New owner grants lose creation evidence after same-key phase replacement

**Severity:** WARNING. **Category:** correctness.
**Location:** `src/engine/retention/owners.rs:824-847` and
`src/engine/retention/owners.rs:851-859`.

A retained scalar result can become unusable as a new foreign dataset input
after its file-completion generations are retired and replaced. Its original
objects remain protected, but the owner cannot qualify a new grant for their
complete identities.

Concrete trace:

1. Complete scalar result A. Its origins descriptor contains original file
   completions, including their declaration and Avro descriptor. Retain A.
2. Retire the scalar file/prepared/validation generations. Compute the same
   source and definition with a new operation, replacing the same cache keys.
   `acknowledge_files` writes a new generation after retirement
   (`src/engine/backfill/preparation.rs:457-520`); `SnapshotRows::prefix` exposes
   only the current value of each key (`src/catalog.rs:57-65`). The old result
   remains independently available.
3. Admit A into a new foreign consumer. `protect_releases` collects its complete
   object closure before acquiring grants (`src/engine/retention/owners.rs:545-554`).
   The inventory expands A's origins and includes the old declaration and Avro
   descriptor (`src/engine/retention/inventory.rs:918-935,1436-1443`).
4. `confirm_creation` scans current lifecycle rows. The replacement file phases
   expose only their new declarations and descriptors. The old result exposes
   its origins object through `release_objects`, but that helper does not expose
   the origins object's children (`src/engine.rs:2413-2418`). `visit_creation`
   does not open the descriptor. Consequently, the old declaration and Avro
   descriptor remain in `missing`, and admission returns
   `unqualified_owner_reference` (`src/engine/retention/owners.rs:774-790`).

This requires no corruption, object loss or unavailable historical result. A
fresh consumer with no earlier grant is sufficient. It is a refusal regression,
not evidence of data deletion. The new restore fixture retires phases without
replacing their keys, so current creation records still mask this failure
(`tests/restore.rs:507`). The existing nested-owner fixture uses ordinary and
joined releases, without scalar origins (`src/engine/retention/owners/tests.rs:165`).

**Requested correction:** qualify exact original objects through an immutable
origins descriptor anchored by retained creation evidence, using bounded,
deduplicated metadata reads; alternatively retain directly addressable immutable
creation evidence for replaced generations. Do not accept arbitrary references
solely because their paths are reserved. Add a test that retains A, retires its
scalar phases, replaces the same keys, and successfully acquires a new foreign
grant for A's exact old declaration and descriptor. Also verify that forged
identities still refuse.

## Patch summary

The patch records each new file execution's operation and shared declaration,
then embeds the canonical acknowledged completion and native revision in one
prepared-group origins descriptor. Prepared, validation and computed-group
records carry the same descriptor reference. Retention follows that descriptor
and the declaration references, and owner creation scanning recognizes the new
direct references.

The relevant requirements are the original declaration/execution records and
unchanged-history rules in `docs/dependency-corrections.md:38-56`, the canonical
winner rule at `docs/dependency-corrections.md:65-69`, and the evidence-retention
step at `docs/dependency-corrections.md:116-121`. This increment does not implement
the complete scoped correction service or qualify Q4.

Important callers are scalar backfill assembly
(`src/engine/backfill.rs:654-674`), coordinated group preparation
(`src/engine.rs:1534-1544`), foreign admission
(`src/engine/admission.rs:280`), and restore dependency collection
(`src/engine/retention/inventory.rs:139-171`).

## Function trace

| Function or type | File and line | Inputs and outputs | Verified behavior |
| --- | --- | --- | --- |
| `OriginalDeclaration`, `OriginalExecution`, `FileOrigin`, `ScalarOrigins` | `src/engine/backfill/origins.rs:8-40` | Compiled declaration, operation, completion and retained generation → typed metadata | Original file records are embedded directly, with no adoption-chain lookup. |
| `origins::write` | `src/engine/backfill/origins.rs:47` | Owner, path, typed value, budget → `ObjectRef` | Counts before serialization, reserves bytes, writes once, hashes the serialized buffer. |
| `storage::write_new` | `src/storage.rs:416` | Destination and bytes → durable file | Uses `create_new`, syncs file and parent directory. |
| `FileAssembly::append` | `src/engine/backfill/preparation.rs:25` | Canonical acknowledgments → assembled files and origins | Checks the file key, charges metadata, reads the winning descriptor and preserves the winning generation, completion and protection. |
| `Engine::prepare_group` | `src/engine/backfill/preparation.rs:63` | Exact source and definition → `PreparedGroup` | Reuses retained prepared origins; new assembly writes origins only after acknowledgment and retains the same reference in validation. |
| `Engine::reserve_group` | `src/engine/backfill/preparation.rs:399` | Operation and purpose → fresh directory | Commits prefix protection before creating files. |
| `Engine::acknowledge_files` | `src/engine/backfill/preparation.rs:437` | Pending bundle → canonical acknowledgments | Existing winner supplies its completion, revision and protection; new records receive the committed revision. |
| `read_completion` | `src/engine/backfill.rs:1083` | Completion and native schema → data files | Reads a bounded buffer, hashes the same bytes it decodes, checks unique output identities and checked row-count sum. |
| `charge_assembly` | `src/engine/backfill.rs:1174` | Current charge and file completion → bounded charge | Includes the full completion, descriptor bytes and per-output allocation allowance. |
| `check_validation` | `src/engine/backfill/preparation.rs:632` | Validation, source, output, origins → acceptance/refusal | Requires the retained origins reference to match alongside output and source dependencies. |
| `ComputedGroup` | `src/model.rs:830` | Serialized group binding → retained origins reference | Missing origins deserialize to `None`; no inference of historical execution is added. |
| `Inventory::computed`, `file_completion`, `group_dependencies` | `src/engine/retention/inventory.rs:910-949` | Typed references → protection roots | Retains original schema metadata as a leaf, plus original source, descriptor, outputs, declarations and assembled source/output snapshots. |
| `Inventory::enqueue`, `expand` | `src/engine/retention/inventory.rs:794,1413` | Typed metadata roots → deduplicated closure | Rejects conflicting interpretations, bounds metadata, reads declarations/origins once, and does not read scalar payloads. |
| `reference_roots` | `src/engine/retention/inventory.rs:438` | One retained claim → direct references | Shallow propagation recognizes new references without opening external metadata. |
| `release_objects`, `visit_creation` | `src/engine.rs:2386`; `src/engine/retention/owners.rs:795` | Retained records → direct created objects | Includes origins and current file declarations, but loses original child witnesses after same-key replacement as described above. |

## Data flow and invariants

- **Canonical identity:** file capture originates at computation; acknowledgment
  can substitute a retained winner; assembly stores that returned completion and
  generation. The mixed catch-up and competing-winner tests explicitly compare
  these identities. A later adoption operation is not substituted for the file's
  original execution.
- **Durability:** declaration files reside under a prefix reserved before the
  write. File acknowledgment happens before origins assembly; an origins-write
  failure therefore leaves completed file work recoverable. Prepared and
  validation retries use the retained descriptor instead of replacing it.
- **Unknown history:** older optional fields stay absent or `None`; the new
  descriptor may explicitly contain an older completion with unknown execution
  and source metadata. Retention fails closed on missing source metadata rather
  than inventing a historical schema (`inventory.rs:922-926`).
- **Protection closure:** selected/prepared roots independently retain originals
  through typed metadata expansion. The same complete closure is used for
  restore. New foreign grants additionally require an owner creation witness;
  that second invariant is the finding above.
- **Resources and simplicity:** buffering grows with file metadata, not source
  rows. Capture reuses existing `RetainedGeneration`, `ObjectRef`, acknowledgment,
  bounded JSON and inventory mechanisms. This is a structural observation; no
  time, memory or storage improvement was measured. The shared JSON reader
  verifies byte length and digest before decoding (`src/engine/inputs.rs:399-440`).

## Test behavior and edge cases

All outcomes below are static expectations, not executed results.

| Test | Expected behavior supported by the trace |
| --- | --- |
| `concurrent_acknowledgments_assemble_one_winner_with_its_own_scope`, `src/engine/backfill/file_completion_tests.rs:183` | Both assemblies retain the same winner's generation, execution and outputs even after deleting the losing descriptor. |
| `retired_completion_gets_a_new_generation_and_retry_keeps_that_winner`, same file:279 | A replacement receives a newer revision; reopen/retry retains that replacement. It does not test a foreign consumer of the older result. |
| `completion_descriptors_refuse_duplicate_outputs_and_changed_bytes`, same file:155 | Duplicate outputs refuse; changed descriptor size/digest produces `corrupt_object`. |
| `stale_source_retains_files_and_catchup_reuses_unchanged_objects`, `tests/backfill.rs:314` | A mixed group preserves old and new operation/source identities, then keeps the descriptor unchanged across reopen, validation and replay. |
| Recovery helper and empty/legacy cases, `tests/backfill_dependencies.rs:41,167,175` | Process interruption preserves prepared/validation references; old record shapes remain readable without fabricated origins. |
| `legacy_file_reuse_keeps_original_execution_unknown`, `tests/backfill_dependencies.rs:183` | Reused legacy file records carry null unknowns and leave the retained legacy records unchanged. |
| `selected_and_prepared_roots_retain_original_executions_without_old_phases`, `tests/retention_inspection.rs:131` | Isolated selected and prepared roots discover the original objects; payload removal does not cause an origin-metadata scan to open values. |
| `scalar_origins_restore_after_file_prepared_and_validation_retirement`, `tests/restore.rs:507` | A restore point includes origins, declarations, original descriptors and source/output metadata after phase retirement; restore reconstructs deleted original metadata. |
| `scalar_origins_tests`, `src/engine/retention/inventory.rs:1628` | Typed expansion deduplicates descriptor reads; shallow discovery does no external reads; strict decoding preserves legacy unknowns and rejects unknown dependency-bearing fields. |

The concrete uncovered edge is retirement followed by **replacement** of the
original phase keys and then first-time foreign acquisition of the older result.
The tests exercise those boundaries separately, leaving their composition
unchecked.

## Conclusion

**VERDICT: REQUEST CHANGES**

The capture and local retention paths preserve original identity and have
meaningful static test coverage. Owner qualification does not yet preserve that
guarantee across same-key phase replacement. Fix and exercise that boundary before
approving this slice. Coverage is insufficient for this composition; confidence
in the static finding is high. Execution qualification remains with the root
agent, and the new verifier requires its separate review.

## Owner lookup fix: independent disposition

The fallback introduced in `src/engine/retention/owners.rs:784-934` resolves the
original finding for retained prepared groups and results/readbacks. When direct
creation records miss an object, it reads their immutable origins descriptors
and recognizes the original declaration, Avro descriptor and output identities.
`confirm_creation` checks the complete requested identity, while
`qualified_object` checks owner and digest before applying object-shape,
reservation and availability checks (`owners.rs:641-662,806-819`).

The new regression
`foreign_seal_protects_old_scalar_origins_after_cache_keys_are_recomputed`
(`tests/grant_retirement_transport.rs:210`) exercises the reported sequence. It
retires and replaces all scalar cache keys, seals the older release for a new
foreign consumer, checks the old metadata identities in the owner grant, checks
that the grant remains in use, and checks unchanged replay. This is an appropriate
test for the original finding; it was read but not executed by this reviewer.

The fix keeps these boundaries:

- Anchors come from typed retained records, and their owner must match the
  lifecycle being inspected (`owners.rs:838-870`). Retired records may still
  establish creation, consistent with the existing direct creation scan;
  retirement of recovery is distinct from object unavailability.
- Anchors reserved for removal are skipped. Requested objects still pass their
  own availability and reserved-path checks before owner acquisition
  (`owners.rs:683-685,877-880`). The fallback does not revive removing objects.
- Only declarations, Avro descriptors and produced outputs count as creations.
  Source/schema/assembled dependency references do not (`owners.rs:913-930`).
- Anchor identities are deduplicated and charged; conflicting identities refuse.
  Decoded metadata has both individual and cumulative bounds. The fallback opens
  hash-checked JSON through `origins::read`, never scalar payloads
  (`owners.rs:870-912`). A successful direct lookup avoids these reads.

### Remaining P2 — Include independently retained validation origins

**Severity:** WARNING. **Category:** correctness.
**Location:** `src/engine/retention/owners.rs:840-863`.

The anchor filter omits `backfill-validation/`, even though validation
completions independently retain and expand their origins
(`src/engine/retention/inventory.rs:1216-1225`). The same creation-witness failure
therefore remains when validation is the only surviving record for old origins.

Concrete trace: stop a backfill after validation, retire its file/prepared
generations, and run another operation that replaces those keys. The replacement
prepared group commits before encountering the retained old validation mismatch
(`src/engine/backfill/preparation.rs:324-358`). No result/readback need exist.
The old validation still protects its original outputs, but all file and prepared
creation records now refer to the replacement. A new managed-input admission
resolving an old output through `qualified_object` cannot find the only remaining
origins anchor and returns `unqualified_owner_reference`
(`src/engine/admission.rs:226-246`; `owners.rs:641-658`).

**Requested correction:** include `ValidationCompletion.origins` in the same
bounded anchor collection, and cover the independently retained validation case.
The foreign-seal regression requires a completed result, so it does not reach
this case.

**Updated verdict: REQUEST CHANGES.** The original completed-result failure is
resolved statically. The narrower validation-only root omission remains. No
additional ownership, retirement or payload-read regression was found. Build and
test qualification remain with the root agent.

## Final disposition: validation-only origin lookup

The remaining P2 is resolved. `visit_original_creation` now accepts
`backfill-validation/` and decodes `ValidationCompletion.origins` into the same
anchor collection (`src/engine/retention/owners.rs:841-860`). It retains the
existing ownership filter, unavailable-object exclusion, identity deduplication,
aggregate metadata allowance and hash-checked JSON reader
(`owners.rs:870-917`). The narrow change adds no payload read or new creation
category; only declarations, Avro descriptors and outputs can establish an
original creation (`owners.rs:918-935`).

The regression
`validation_only_origins_qualify_original_creations_after_cache_replacement`
(`src/engine/retention/owners/tests.rs:990`) reaches the exact missing boundary:
it stops the original operation after validation, retires only file/prepared
records, and replaces those keys while leaving validation unchanged. It then
asserts that no scalar result/readback and no direct creation record can mask the
fallback. It independently resolves the original declaration, descriptor and
output through `qualified_object`, then acquires a fresh foreign owner grant and
checks its exact retained identities. The test stops the second operation after
processing, so it does not depend on proceeding through a conflicting validation
binding.

**Final verdict: APPROVE for the reviewed capture/retention/owner-lookup scope.**
Both reported correctness findings are resolved by the inspected code and have
targeted regression coverage. No additional actionable finding remains in this
scope. Confidence is high in the static trace; this reviewer ran no compilation,
tests or performance checks. The root agent owns execution results and the full
gate, and the separately authored verifier remains outside this review.

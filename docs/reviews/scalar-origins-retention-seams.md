# Scalar origins: retention and verification seams

Static implementation preparation against `9c818a0` and the concurrently edited
`preparation.rs`, reviewed 2026-10-10. No code edits, tests, builds or benchmarks.
This reviews [the correction design](../dependency-corrections.md); it does not
qualify Q4 or physical retention. Line references describe the inspected tree.

The existing object reader, creation reservation, completion acknowledgment and
typed inventory are sufficient. Add typed descriptor expansion to those paths;
do not create a second provenance store or scan values to recover missing history.

## Findings and counterexamples

1. **An object reference alone does not retain its contents' references.**
   `Inventory::object` only records that exact object
   (`src/engine/retention/inventory.rs:765`). `computed` currently retains source
   metadata and the base release (`:901`); the file/prepared branches explicitly
   list their known references (`:1121`, `:1171`). If a selected group only adds a
   reference to an origins JSON object, retiring its old file completions can
   leave the original declaration and source interpretation outside the selected
   result's closure. Add an origins expansion kind to the existing queue, then
   visit the exact declaration, source, source metadata, output descriptor and
   output objects from each retained execution. Keep schema-only metadata as an
   object; expanding its entire old native table would retain unrelated files.

2. **A key/revision is not independently readable original execution evidence.**
   `RetainedGeneration` contains only key and revision (`src/model.rs:541`), and
   `active_completion` resolves the current generation for that key
   (`src/engine/retention/retirement.rs:288`). A retired/replaced key or fresh
   restored catalog cannot reconstruct old content from those two values alone.
   The immutable origins descriptor must carry the original retained file record
   alongside its scoped generation, not a pointer requiring a latest-key lookup.
   Current `FileCompletion` also lacks the original operation and declaration
   (`src/engine/backfill.rs:38`); capture those when writing new files. Absence on
   old completions remains unknown. The original operation belongs to the file
   execution, not the shared per-definition declaration.

3. **Cached and concurrent winners must supply the descriptor as well as values.**
   The working `AcknowledgedFile` returns a canonical completion, actual native
   generation and its protection (`preparation.rs:9`, `:376`). Extend
   `FileAssembly::append` before it moves the outputs away (`:24`) to retain that
   exact tuple. Both prepared-cache branches (`:88`, `:234`) must return the
   winner's origins descriptor, including `None` for an older winner. Building a
   new descriptor from the losing attempt or current request would misattribute
   reused values. No fallback lookup after acknowledgment can repair that.

4. **Presence needs semantic verification; derived deserialization is insufficient.**
   `verify_binding_history` currently verifies authored/inherited SQL bindings,
   but has no per-file execution reference to check
   (`src/engine/backfill/verification.rs:380`). A descriptor pointing to a valid
   but unrelated declaration/output would otherwise be accepted. Verify descriptor
   identity, processor/definition/input bindings, source-file identity and the
   complete output association when creating the new prepared/binding metadata.
   Reuse the retained outcome; do not introduce this traversal into ordinary
   reads (`src/engine.rs:2447`). The future supplemental evaluator must distinguish
   absent origins from an empty, known execution set for an empty source.

## Minimal integration map

| Seam | Existing implementation | Required addition |
| --- | --- | --- |
| Original declaration | `backfill/expressions.rs:133–157` checks exact SQL bindings and hashes normalized interpretations | Retain one immutable descriptor with the processor, original `GroupDefinition` and bindings; share its reference across files. Preserve the existing material key. |
| File creation | `backfill/preparation.rs:333` reserves an owned prefix before creating files; `:453` creates the completion | Write the declaration inside a protected prefix, attach it and the original operation before bundled acknowledgment. New references must already exist durably. |
| Metadata writing/reading | `storage.rs:416` writes a new file and syncs file/directory; `engine/inputs.rs:399` bounds, reads, hashes and decodes one buffer | Reuse these helpers, counting serialized bytes and decoded assembly state. Build `ObjectRef` from the same known bytes where practical. No payload reread is needed. |
| Prepared handoff | `backfill.rs:21`, `:62`; `preparation.rs:249`, `:325` | Carry optional origins through `PreparedCompletion` and `PreparedGroup`, including both cache-winner branches. Share the descriptor through validation/readback; do not regenerate it per phase. |
| Selected binding | `model.rs:830`; constructors `backfill.rs:653`, `engine.rs:1536` | Add the optional reference and pass the prepared value in both direct backfill and refreshed-group construction. Inherited bindings already clone/compare the complete struct. |
| Inventory closure | `retention/inventory.rs:451`, `:463`, `:787`, `:901`, `:1121`, `:1171`, `:1381` | Add the typed expansion and metadata-kind tag; reuse identity conflict checks, queue deduplication, bounded reader and strict decoding. A declaration containing no object references is a leaf; origins are not. |
| Owner creation evidence | `retention/owners.rs:795`, especially `:824`, `:840`; `engine.rs:2385` | Observe the declaration created with a file completion and origins descriptor created with a prepared completion. Include selected origins in the lightweight direct release object list; full closure still belongs to inventory. Do not label reused inputs as newly created objects. |
| Owner scope propagation | `engine.rs:96`; `retention/owners.rs:79`, `:103`, `:436` | Preserve each canonical file's scope in assembly and bind it to the prepared/selected claim. Missing scope stays unknown. No per-file owner reacquisition is needed for already qualified scope. |
| Verification | `backfill/verification.rs:7`, `:21`, `:332`, `:380` | Add a shared bounded descriptor checker at creation/explicit verification, with request-local identity caching. Keep SQL bindings distinct from future semantic supplementation. |
| Restore/removal | `retention/inventory.rs:136`, `:275`; `engine/restore.rs:181` | Correct inventory expansion supplies both restore copies and removal blockers. No separate backup implementation is necessary. |

Owner checks deliberately use a shallow typed-root walk
(`inventory.rs:436`, `owners.rs:273`); that is not a substitute for qualifying a
new descriptor's transitive references at creation. Preserve the already acquired
file scopes when introducing the descriptor. Grant retirement checks every token
named by the surviving claim (`retention/grants.rs:125–165`), even when a token is
not a direct root. This avoids reopening origins merely to propagate protection.

## Result-format decision

**The additive origins-only step does not require a result-format bump.** Add
optional fields with serde defaults; absence remains unknown, and keep originals
unchanged. The current engine accepts formats 10 and 11 (`engine.rs:156–168`).
`FileCompletion`, flattened `PreparedCompletion` and `ComputedGroup` currently
allow additive fields (`backfill.rs:37–69`, `model.rs:829`), so older ordinary
readers can still resolve the immutable values.

Cleanup is deliberately stricter: `Strict<T>` round-trips every original field,
including flattened and nested fields (`inventory.rs:1470–1534`). Older cleanup
therefore refuses newly added dependency fields instead of treating them as
unprotected. New cleanup understands present descriptors; it need not invent them
for old records. Preserve compatible null/omission serialization and test it.

This decision applies to retained provenance only. It does **not** establish that
old writers can safely ignore future scoped-correction adoption rules. Review
writer eligibility/version fencing when implementing that semantic change; a
result-format number alone does not guard mutable lifecycle decisions.

## Qualification needed for the addition

Existing tests demonstrate relevant boundaries, not the new feature:
`tests/retention_inspection.rs:130` covers independent prepared/validation roots
and unknown flattened fields; `:407` refuses unknown families/fields;
`tests/backfill_dependencies.rs:110` preserves legacy completion records during
resume; `retention/inventory/collector_tests.rs:269` verifies metadata closure
without payload reads and retained admission.

Extend these with: selected-only and prepared-only roots retaining every origin
object after old recovery retirement; duplicate descriptor references decoded
once; conflicting/dangling origins refusing; mixed original generations and
unknown legacy files; empty source; current and old serialization; exact restore
closure; and canonical concurrent-winner attribution. Keep these as small
metadata/recovery fixtures. Full Q4 supplementation and cutover tests come later.

**Conclusion:** approve this integration direction. The reference expansion,
original execution capture, canonical winner propagation and descriptor checks
are required before claiming the new metadata is trustworthy or independently
retained. Confidence is high for the inspected seams; implementation and runtime
qualification remain outstanding.

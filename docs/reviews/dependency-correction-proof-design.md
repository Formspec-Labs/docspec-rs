# Supplemental scalar proof representation

Static design review, 2026-10-10. **APPROVE option B:** embed a small immutable
qualification in `ComputedGroup` and retain the same value through the existing
completion service. No Rust changes, builds, tests, benchmarks, or payload scans
were performed. The review inspected the working correction records and the
existing origins, verification, selection, and retention implementations.

## Decision and findings

1. **Keep the existing original-executions object.** It already binds the
   original declaration, per-file completion generations, source interpretations,
   and assembled output (`src/engine/backfill/origins.rs:25-43`). Copying that
   history into the proof would add no evidence.
2. **Use inline qualification.** The selected manifest already binds every
   `ComputedGroup` by content identity, and ordinary resolution checks that
   serialized selection (`src/model.rs:854-866`, `src/engine.rs:2455-2464`). A
   bounded inline proof remains independently readable without the old catalog.
   The existing completion helpers accept typed records; qualification does not
   need to become a generic `PhaseResult` (`src/engine/backfill/preparation.rs:324-332`,
   `src/engine/retention/retirement.rs:288-303`).
3. **Do not omit completion retirement and owner recovery integration.**
   `active_completion` rejects unknown completion prefixes. Add
   `dependency-qualification/` to `completion_key` and the typed inventory;
   include qualification-only origins in `visit_original_creation` so an
   independently retained qualification can supply creation evidence after
   original cache keys are replaced (`retirement.rs:20-35,288-303`,
   `owners.rs:824-868`, paths under `src/engine/retention/`).
4. **Do not relax inherited binding equality broadly.** History currently
   requires exact `ComputedGroup` equality for inherited evidence and sibling
   backfills (`src/engine/backfill/verification.rs:423-432,457-466`). Compare all
   original fields exactly and allow only a separately checked qualification
   change. Check the new proof at its owning manifest before walking the old
   parent; do not attribute the supplement to the historical execution.

## Small data schema

`ComputedGroup.qualification: Option<Box<ScalarQualification>>`, defaulting to
absent for older records. `ScalarQualification` contains:

| Field | Meaning |
| --- | --- |
| `checker: String` | Versioned semantics of this correspondence check. |
| `scope: String` | Hash of the scalar processor and exact `GroupDefinition`. |
| `fields: BTreeSet<i32>` | Normalized cumulative required source-field IDs. |
| `first_revision: i64` | First correction revision for this scope. |
| `origins: ObjectRef` | Exact existing `ScalarOrigins` descriptor. |
| `target: NativeSnapshotRef` | Exact target metadata, snapshot and schema. |

The lifecycle completion key is `dependency-qualification/<hash(proof)>`; its
typed payload is the same proof. Store and reconcile it with
`completion_records`, `active_completion`, and retained protections. Return the
canonical retained value after races or lost responses. The proof contains no
operation-specific alias, copied file list, copied definition, or mutable
correction-history list.

`revision` and `latest_decision` remain in current `Requirements`; the proof
does not need them. A repeated correction that adds no fields does not change
the correspondence obligation. Final selection compares the current normalized
`scope`, cumulative `fields`, and stable `first_revision`. A new field or checker
requires a different proof. This uses the cumulative union and preserved first
revision already implemented in `src/engine/corrections/records.rs:67-93`.

## Lineage, seams, and invariants

| Requirement or seam | Preservation rule and evidence |
| --- | --- |
| Q4 original history | Keep SQL input bindings, original operations and native generations unchanged; supplementation belongs to a new association (`docs/requirements/07-operations-qualification.md:109-115`). |
| Fresh deficient computation | Every included original file must have `0 < generation.revision < first_revision`. Check canonical winners and all mixed origins. A newly acknowledged deficient execution cannot qualify itself merely because its language now matches. The cutoff concerns durable acknowledgment, not CPU start time. |
| Exact association | Require proof origins equal binding origins; target equals the selected source's `NativeSnapshotRef`; scope matches the binding's processor/definition; owners agree with the dataset. The existing origins check binds output and complete file coverage, so another output reference is redundant (`origins/checks.rs:221-241,277-310,335-415`). |
| Supported correspondence | Compare each required primitive root field by stable ID and semantic read interpretation for each original source file. Missing declaration/schema/field remains unknown. Exact immutable source-file identity is required. Share field normalization from `expressions.rs:136-155`; retain initial defaults and material metadata. |
| Metadata reuse | Extend the request-local origins checker, which already caches descriptors, declarations, schemas and compiled facts (`origins/checks.rs:25-38,70-167`). Do not create another cache or re-open each schema per field. Validate caller-specific proof associations even when parsed facts are cached. |
| Offline verification | `verify_binding_dependencies` and `verify_groups` check an embedded proof against its embedded immutable requirements and retained metadata. They never query current corrections (`src/engine/backfill/verification.rs:7-31,231-255`). Ordinary reads keep immutable resolution. |
| Current authority | Before a new manifest, qualify included affected groups, including inherited siblings. At `select_with_progress`, resolve current requirements from its supplied `View` and require a matching proof. The existing guarded commit defeats a later correction (`src/engine/history.rs:160-234`; `docs/PLAN.md:23-33`). |
| Immutable readback | Retained readback cannot gain a different proof in place. Refuse selection with a new-operation instruction; retain completed values and old bytes (`docs/dependency-corrections.md:109-118`). Completed-result replay remains historical replay. |
| Retention and restore | Add a shared typed proof-root collector: enqueue origins with existing `Expansion::Origins`, retain target with existing native-reference handling, and carry protections into consuming completions/results. Use it from inline bindings and qualification completion records. The common inventory also supplies restore dependencies (`src/engine/retention/inventory.rs:139-171,910-948,1435-1443`). No new expansion kind or object creation branch is required. |

References abbreviated to `origins/checks.rs` and `expressions.rs` above are
under `src/engine/backfill/`.

New affected-value exports have their own admission and final selection boundary;
use the same evaluator for the consumed group, as required by
`docs/dependency-corrections.md:95-107`. Source-only consumers must not inherit
corrections to unconsumed computed groups.

## Alternatives, value, and verdict

Option A is safe if its immutable JSON proof object receives a hash-bound reader,
creation protection, explicit retention expansion, owner creation recognition,
availability checks, and restore closure. Its additional indirection is useful
only if proof size or reuse across many manifests makes the shared object
materially cheaper. No such need is established here: the expensive file history
is already shared. Option B removes those extra seams while retaining the same
bounded proof and completion semantics; this is a structural simplification, not
a measured speed or storage claim.

This is proof infrastructure serving Q4's operator outcome: correct unsafe reuse,
preserve original history, and retain unrelated work. The existing lifecycle and
origins machinery substantially subsumes the work. Reject either design if an
affected stale candidate selects, a fresh deficient execution becomes its own
historical baseline, or a manifest cannot verify after its qualification cache
record is retired and the old catalog is unavailable. A retention/restore test
must exercise a qualification-only root as well as a selected manifest.

**Verdict: APPROVE B.** Intent and shape match; commitments remain honored with
the named checks; conceptual debt is lower than A. Confidence is high in this
static representation decision. Runtime correctness and resource measurements
remain the implementation qualification work.

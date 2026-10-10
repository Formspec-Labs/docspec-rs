# Dependency correction adoption seams

Static audit, 2026-10-10, against `89c2eafb76b8bd92b3aa9d58fe8efd309cf601e7`.
This maps integration work before its implementation; it is not an approval of
Q4. No builds, tests, payload reads, or Rust changes were performed. Sources are
the repository instructions, `docs/dependency-corrections.md`, relevant R1/R3/R6/R7
requirements, and the implementations and fixture assertions cited below. The
Rust and semi-formal review guidance informed the audit.

The smallest safe design has a shared scalar evaluator, an immutable proof check,
and explicit calls at existing adoption boundaries. Final dataset selection is
already shared. Export selection is separate. Do not put mutable correction
lookups in the ordinary reader or the catalog-free verification graph.

## Integration risks that need explicit decisions

1. **Readback bypasses preparation.** Generic operations resolve
   `readback/<operation>` before `prepare_source` or scalar preparation
   (`src/engine.rs:1247-1288`). Backfill similarly resolves retained readbacks
   before `prepare_group` (`src/engine/backfill.rs:617-681`). An evaluator confined
   to file or prepared lookup misses both. A final check at
   `select_with_progress` must require already-retained proof current for every
   computed group in the candidate.
2. **Inherited bindings currently require exact equality.** A supplemental
   reference added to an inherited group would fail existing composition and
   binding-history checks (`src/engine/backfill/verification.rs:140-145`,
   `215-227`, `308-313`, `423-432`, `457-466`). The narrow permitted change is
   described below; weakening whole-group or source equality is unnecessary.
3. **Export never calls dataset selection.** Its own admission and channel commit
   need current requirements for `policy.group` (`src/engine/export.rs:237-301`,
   `203-225`). Rejecting every corrected group in its source release would also
   block unrelated source-only exports.
4. **An apparently older definition can execute after correction.** Without a
   retained cutoff, a new execution of the deficient text-only definition could
   become its own comparison baseline on a later operation. The proposed
   `Requirements.first_revision` closes this case only if every original file
   generation predates that first correction, including canonical winners and
   mixed-origin prepared groups. This is an acknowledgment cutoff, not evidence
   of when CPU execution occurred.
5. **New retained keys and objects require typed closure.** Unknown lifecycle
   prefixes currently refuse inventory (`src/engine/retention/inventory.rs:1403-1408`).
   A qualification reachable only through the current catalog would also fail
   the catalog-free release verification and restore promises.

## Early reuse and binding construction

| Function and boundary | Current behavior | Small integration point |
| --- | --- | --- |
| `Engine::prepare_group`, `src/engine/backfill/preparation.rs:67-99` | Compiles SQL bindings, derives a source/definition prepared key, looks up active completion. | Resolve the exact processor/definition requirement state before opening payloads. Keep SQL `input_fields` unchanged. The prepared key and unrelated value-cache keys need no correction revision. |
| Prepared hit, `preparation.rs:100-112` | Checks dependencies and output availability, adopts table/origins/protection. | Evaluate retained originals against target source before accepting the candidate. Reuse the same immutable descriptor. |
| File hit, `preparation.rs:148-188` | Resolves canonical completion and native revision, appends it, continues without computation. | Evaluate the actual `AcknowledgedFile`; add the Q4 pause after this real hit while no dataset mutex is held. Retain original completion/generation even when later adoption refuses. |
| `FileAssembly::append`, `preparation.rs:25-57` | All direct hits and acknowledged bundles converge here; it reads the canonical descriptor, collects exact output references, original generations and protection. | A shared per-file check here can cover direct reuse and bundle winners if supplied target/requirement context. Do not interpret new post-correction executions as qualifying originals. |
| `acknowledge_files`, `preparation.rs:463-525` | A concurrent winner may replace the caller's attempt; the winner's completion, native revision and protection are returned. | Check eligibility on the returned canonical winner, never only the attempted outputs. Keep completed useful computation retained even if adoption fails. |
| Prepared winner reconciliation, `preparation.rs:279-345` | A prepared completion that won during assembly replaces the local candidate. Both branches converge on `scalar_origins`. | Evaluate the final selected origins, not only the initial miss/hit branch. |
| Validation reuse, `preparation.rs:347-415` | Both initial and concurrent hits bind source/output/origins; no value verification rerun on a hit. | Qualification must happen before this bypass or at the common post-preparation boundary. A validation acknowledgment is not proof of current supplemental requirements. |
| `PreparedGroup` to `ComputedGroup`, `src/engine/backfill.rs:660-679`; coordinated operations, `src/engine.rs:1518-1544` | Writes the new association using retained origins and original SQL bindings. | Carry the supplemental qualification reference separately at both constructors. It is association evidence, not an added SQL dependency or rewritten original declaration. |
| Backfill composition, `src/engine/backfill.rs:720-739` | Clones current dataset, replaces the target group, inherits every sibling. | Before new serialization, qualify included inherited groups as well as target. Preserve the composed source/output pins and original bindings. |
| Evidence-only composition, `src/engine.rs:1497-1502` | Copies all previous groups and computed bindings directly; never calls `prepare_group`. | Requires the same pre-readback all-group association pass. |
| Generic final composition, `src/engine.rs:1598-1626` | Assembles build/update/schema/refresh/evidence candidate, checks groups, then serializes readback. | A common pre-readback pass here covers new associations and inherited evidence bindings. It does not cover a retained-readback shortcut. |

`Expressions::compile` currently rejects extra declared SQL bindings and hashes
the actual referenced fields (`src/engine/backfill/expressions.rs:133-157`). Its
field interpretation removes display name and write default, retaining initial
default and relevant metadata (`136-147`). Share that normalization with the
supplemental evaluator; do not replace it with the physical schema identity.
The file key hashes immutable source identity and those material dependencies
(`src/engine/backfill.rs:1066-1071`).

The request-local originals checker already caches origins, declarations,
schemas, compiled dependencies and caller associations
(`src/engine/backfill/origins/checks.rs:25-38`, `70-167`). Reuse these facts or the
same bounded metadata reader rather than opening each original schema again per
field. Its existing positive native revision check (`361-371`) is not a
correction cutoff; it needs the scoped requirement state for that decision.

Planning has a distinct read-only path: `plan_backfill_admitted` counts file
completions and reports promotion refusals (`src/engine/backfill.rs:263-424`).
Integrate an affected-group reason here without turning planning into proof
publication. Preserve its completed-operation early return (`280-308`).

## Final adoption and immutable readback

All dataset selections converge on `Engine::select_with_progress`
(`src/engine/history.rs:160-234`):

- `select_prepared` handles generic operations, under the output/dependency locks
  (`src/engine.rs:1716-1765`), through `select` (`history.rs:142-157`).
- Backfill rechecks fresh composition under its output/dependency locks and calls
  `select` (`src/engine/backfill.rs:831-866`).
- Refresh calls `select_with_progress` directly after input guards
  (`src/engine/refresh.rs:226-270`).

Evaluate **all** `release.dataset.computed` bindings in the supplied lifecycle
`View` before authority/result/succession records are assembled. This must be a
check of retained proof, with no new qualification commit or lock acquisition.
The callers already hold the locks, and committing qualification would invalidate
the very view selected against. `Lifecycle::commit` uses that view's expected
metadata and disables stale native retries (`src/catalog.rs:337-345`, `403`,
`555-583`). A concurrent correction must cause fresh evaluation or refusal,
never blind retry with the old result of the check.

Existing completed results return before these guards
(`src/engine.rs:1741-1745`, `src/engine/backfill.rs:512-516`,
`src/engine/refresh.rs:238-242`). Keep that reconciliation ordering.

Once a readback has retained a binding, an operation requiring a different
qualification binding must report the need for a new operation ID. Generic
readback has one key (`src/engine.rs:1624-1635`). Backfill permits sibling
recomposition under manifest-digest keys, but explicitly forbids changing its
target table or binding across those readbacks
(`src/engine/backfill.rs:939-945`). Its more flexible key is not permission to
rewrite that target after correction. `tests/backfill.rs:479-515` and
`src/engine/backfill/protection_tests.rs:257-299` verify the legitimate sibling
case and preservation of the earlier readback.

### Narrow inherited-binding exception

Compare every existing `ComputedGroup` field exactly while allowing only the new
supplemental reference to differ: `origins`, `definition`, `input_fields`,
`request`, `native_field_ids`, `source_metadata` and `base_manifest`
(`src/model.rs:850-864`). Preserve source and output `TablePin` equality.

Use this comparison at the exact-equality sites listed in risk 2. A changed
supplemental reference requires a valid immutable proof for the same original
executions, definition, source and output. Do not permit `Some` to become `None`
or permit a weaker requirement set. Validate a changed proof at each owning
selection walked by `verify_binding_history` (`verification.rs:404-503`), not
only at the outer release. Otherwise an intermediate inherited step can carry
unchecked proof metadata. Offline verification checks embedded immutable
requirements; online adoption separately checks the current requirements.

## Exports, reads and source-only consumers

`admit_export` has matching-operation replay before new admission work
(`src/engine/export.rs:243-251`). New admission must check the selected
`policy.group` in its fresh view before the operation acknowledgment
(`274-301`). Resumed unfinished exports require the same check at the common
pre-materialization authority check (`155-157`) and final selection (`213-225`).
The source's computed binding is the input; source and unaffected computed-group
exports must remain admissible. The output materializer reads only the requested
group (`374-390`).

Completed export replay returns at `148-154` and `206-211`, before current
authority checks. `lookup_export` is a historical identity check (`97-136`).
Neither should become a current-correction admission check.

`check_read_release` establishes exact retained result/readback identity and
availability (`src/engine/read.rs:53-104`); `read_group` then opens its requested
pin (`13-29`). Preserve these historical reads. `resolve_release` deliberately
decodes only immutable selection (`src/engine.rs:2455-2464`).

Likewise, `verification::walk` visits semantic producer manifests and performs
local immutable verification (`src/engine/verification.rs:294-409`). It underlies
catalog-free verification and related-input checks. Adding current correction
lookups here would incorrectly reject source-only consumers: joins register only
the producer `source` group (`src/engine/refresh.rs:503-520`), and summaries use
the same source accessor (`src/engine/aggregation.rs:65-85`). Leave
`qualify_input`/`guard_inputs` scoped to their existing producer authority and
coverage rules. The destination's own included scalar groups still pass the final
dataset guard.

## Retention and catalog-free qualification

Register new current-requirement, correction-decision and qualification lifecycle
records in `Inventory::record` with strict typed identity checks. If correction
decisions contain no object references, retain their lifecycle identity without
inventing payload dependencies. Qualification objects must expand their actual
references: original executions, original declaration/schema metadata, exact
target source and output, and immutable requirement evidence used by the checker.

Connect a binding's supplemental reference through `Inventory::computed`
(`src/engine/retention/inventory.rs:910-915`), which is reused by releases and
captures (`964-993`, `1227-1243`). Reuse the existing bounded, identity-deduplicated
`enqueue`/`expand` traversal (`794-851`, `1413-1459`). Standalone qualification
completions also need roots; a result must not depend on its old cache entries
remaining active.

The same inventory supplies `reference_objects` and owner protection
(`inventory.rs:30-65`; `src/engine/retention/owners.rs:548-557`) and restore capture
(`inventory.rs:139-171`; `src/engine/restore.rs:179-181`). New qualification
creation evidence also needs recognition in `visit_creation`
(`owners.rs:942-1013`); references to inputs must not be relabeled as objects the
qualification created. Preserve foreign protections before acknowledgment.

Register direct qualification references in `release_objects`
(`src/engine.rs:2378-2433`) and validate immutable proof in scalar binding
verification (`src/engine/backfill/verification.rs:7-30`, `231-265`).
`verify_release_at` receives a pin and limits, with no live catalog
(`src/engine.rs:2367-2375`). It must be able to check the retained proof from those
objects after correction state advances or the original catalog is gone. This
checks what the release claimed then; it must not claim current reuse authority.

## Cutoff assessment

Blocking fresh computation under a corrected deficient definition, while
allowing qualified originals whose retained revisions precede the first scoped
correction, is a defensible conservative first-case rule. Preserve that first
revision when later decisions add fields. Compare revisions only within the
same dataset lifecycle, require a positive original revision, and check every
file origin, including mixed-origin groups and winners returned by
`acknowledge_files`.

The deliberate loss is a file computed before discovery but first acknowledged
afterward, or an otherwise identical completion whose only available generation
was recreated after retirement. There is no retained pre-correction execution
proof for that new generation. Existing older origins remain usable if retained;
do not replace their revisions with the adopting prepared/validation revision.
Handle empty origin sets explicitly rather than allowing a vacuous per-file
test to authorize new deficient computation. The cutoff supplements, rather
than replaces, exact field interpretation/file correspondence. An explicit
corrected definition naturally has a different material definition; changing
only an operation ID must not evade the corrected scope.

## Qualification fixtures and hooks

No existing test read for this audit establishes correction behavior. These are
usable fixtures and concrete additions, not results from executed tests.

| Existing fixture or hook | Established assertion and extension |
| --- | --- |
| `tests/backfill.rs:173-221` | Stops at files/processing/validation/readback/selection, reopens, and compares retained records unchanged. Extend each unfinished resume with a scoped correction; selected replay remains exact. |
| `tests/backfill.rs:329-463`, `canonical_file_records` at `28-75` | Reads actual native completion revisions and proves mixed old/new origins plus unchanged descriptors. Use these witnesses for cutoff and original-generation assertions. |
| `src/engine/backfill/file_completion_tests.rs:201-295` | Concurrent acknowledgment returns the same canonical winner, outputs, revision and protection; loser descriptor is removed before assembly. Add correction timing around winner adoption. |
| `tests/defaults.rs:30-100` | Public `SchemaSpec` additions/defaults and exact native field capture provide the language-default setup. Compare full source `ObjectRef` identities, not just the helper's path set. |
| `tests/backfill_dependencies.rs:192-264` | Retires/replaces private-fixture completions with absent original facts and verifies unknowns survive reuse. Add affected refusal while preserving unaffected legacy readability. |
| `tests/scalar_bindings.rs:271-321` | Cached readback refuses when original schema/binding evidence is unavailable and preserves the old head. Add correction-specific readback refusal and a new-ID metadata-only success case. |
| `tests/exports.rs:719-768`, `773-827` | Resumed export avoids reopening withheld source metadata; stale authority/channel preserves bytes without promotion. Add computed-group correction admission/final-selection checks and unaffected source export controls. |
| `tests/retention_inspection.rs:113-192` | Isolated roots prove originals remain protected without masking file phases/grants; payload removal proves traversal stays metadata-only. Add standalone proof, selected proof, and inherited proof roots. |
| `tests/restore.rs:507-621` | Retires scalar recovery phases, captures restore, removes original metadata, restores exact bytes, checks selected pin and catalog-free routine verification. Extend with qualification and cumulative requirement state. |
| `tests/rebuild_completion.rs:33-125` | `PausedCatalog` pauses after a real native commit and can lose its response. Useful for acknowledgment reconciliation, but not itself an actual file-cache-hit pause. |
| `tests/support/mod.rs:11-83`; `src/engine.rs:2300-2315` | Process harness kills after durable handoff; `stop_after` returns an error and work markers observe actual computation. Neither pauses a still-live operation at reuse lookup. |

Add a small `test-util` one-shot, bounded pause at the actual cache-hit branch
(`preparation.rs:153-188`), outside the dataset lock. It should identify the
operation/key/generation and resume the same future. Q4 must stop the original
operation after real file lookup, commit only the scoped correction, and resume
it. A stop/restart test alone does not exercise that race. Keep a separate
prepared-hit test so successful prepared reuse cannot mask the intended file
lookup. For expected-view fencing, a native catalog wrapper must pause before
the guarded selection effect; the existing post-commit wrapper cannot establish
that particular ordering by itself.

Required counterexamples are: same-name replacement language ID with unchanged
text/source files; missing historical language; adequate same-ID evidence;
second cumulative correction; fresh computation racing the first correction;
mixed original generations; cached readback; an affected inherited sibling;
unaffected text-length work and source-only related consumers; corrected
definition recomputation; completed replay; export correction after retained
materialization/validation; and retention/restore without active old scalar
phases. Assert the correction-specific refusal and unchanged source head,
definition, broad eligibility, original bytes and native generations in the
decisive race. Assertions about work avoided must use actual work observations,
not only unchanged create-once records.

## Audit conclusion

The existing selection, metadata-reader, original-execution and retention seams
support a small implementation. The adoption scope is not complete until early
reuse, retained-readback shortcuts, inherited associations, policy-scoped exports,
and independent retained proof all agree. The first-correction revision rule is
reasonable with its stated conservative boundary. Qualification remains pending
implementation and execution of the required counterexamples.

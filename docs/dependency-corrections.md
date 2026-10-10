# Scoped dependency correction

Status: the local scalar correspondence path is implemented and covered by the
[repository gate](reviews/dependency-correction-full-check-2.log). The
[status and remaining limits](STATUS.md#scoped-scalar-dependency-correction)
distinguish this case from broader qualification. This follows R1/R3 and
[Q4](requirements/07-operations-qualification.md#q4-discovered-dependency-omission).
The independent [consistency](reviews/dependency-correction-consistency-design.md)
and [efficiency](reviews/dependency-correction-efficiency-design.md) reviews supply
the failure cases and code traces.

An operator who discovers an omitted material dependency needs to stop unsafe
reuse, preserve the original execution history and keep unrelated completed work.
The existing global `invalidate` safely refuses adoption but also discards reuse
eligibility for unrelated work. Scoped corrections use the existing Iceberg
lifecycle. Global invalidation remains available for explicitly broad invalidations.

## Supported first case

Start with scalar field groups and omitted primitive source-field dependencies.
SQL input bindings remain exactly the fields inferred by DataFusion. A material
applicability condition, such as the language assumed by a text classifier, is a
separate statement; it must not claim the SQL read a field it did not read.

Accept supplemental correspondence only for the same immutable source files and
the same stable field IDs and read interpretation. Compare type, requiredness,
initial defaults and material metadata. Share the semantic per-field normalization
used by `Expressions::compile`. The physical `storage::schema_identity` hash drops
read defaults and cannot establish this correspondence. Names, equal row counts,
samples and business keys do not establish this
correspondence. A removed/replaced field, missing historical declaration or
unknown historical interpretation refuses reuse. Richer correspondence across
rewritten files is outside this first implementation.

This proves correspondence with the original execution context; it does not
invent evidence that an arbitrary classifier was correct. A known mismatch needs
an explicitly corrected group definition that consumes or validates the required
input. Automatically rerunning the deficient text-only expression does not fix
its missing applicability condition.

## Retained facts and authority

Use bounded typed records and existing immutable JSON descriptors:

| Fact | Representation and purpose |
| --- | --- |
| Correction | Stable request ID, dataset, exact scalar processor and `GroupDefinition`, added source-field IDs and reason. Retain each decision unchanged. Identical retries reconcile; changed content under the same ID refuses. |
| Current requirements | A directly addressed lifecycle record for that processor/definition, containing the bounded cumulative field set and current revision/latest decision reference. Union fields and update it atomically with the new decision under the dataset lock and native guarded commit. Later corrections cannot forget earlier requirements. |
| Original declaration | One immutable descriptor per compiled material definition containing processor, original `GroupDefinition` and exact SQL field bindings. File completions reference it and retain their original operation and source metadata. Missing fields on older records remain unknown. |
| Original file executions | One immutable descriptor for each prepared group, listing the actual file-completion generations and their original retained records. Include the information needed to resolve them without the old catalog. Reference their declaration/output descriptors and original source metadata directly; do not build an adoption chain. |
| Supplemental qualification | A small `DependencyQualification` embedded in the hash-bound `ComputedGroup`, separate from its original SQL declaration. The same value is retained as a directly addressed lifecycle completion for recovery. It binds original executions, exact target source, normalized requirements and the correspondence checker. No separate proof object or proof-object read is needed. |

Reuse `RetainedGeneration`, `ObjectRef`, existing completion acknowledgments and
owner protection. The prepared-group descriptor carries per-file origins because
one group can reuse files from different executions. Do not attribute them all
to the later assembly operation. Share declaration objects across files rather
than repeating their full contents in every completion. Reuse the exact original-
executions descriptor across validation, readback, qualification and retries when
the origin set is unchanged. Use the existing bounded, hash-bound JSON reader.

Keep immutable correction decisions independently addressable. Do not append the
whole correction-reference history with every current-state update: that retains
quadratic history as corrections accumulate. Qualification binds the normalized
requirement state and checker. An explanation can retrieve the scoped decisions
on demand. The cumulative field set itself remains bounded by the accepted
declaration size and metadata budget; its retained versions still consume space.

Bundle acknowledgment now returns the canonical retained completion and its
actual native generation. Assembly uses those returned completions after the
bundle commits. If another worker already retained a winner, it uses that
winner's outputs and protection. This prerequisite fixes the earlier mismatch
between an attempt's physical outputs and the retained winner's generation.
Bundled acknowledgment and bounded descriptor buffering remain; see the
[implementation checkpoint](STATUS.md#canonical-scalar-file-completions).
Those original generations now persist in shared immutable descriptors, with
original declarations and source-schema references retained through prepared,
validation and result records. See the
[original execution checkpoint](STATUS.md#original-scalar-execution-metadata).
The correction service, supplemental evaluator and adoption guards below are
implemented; [STATUS](STATUS.md) records their verification and remaining limits.

The current requirement key is derived from the exact deficient processor and
group definition, independently of source writer definitions or adoption IDs.
It must apply to every reuse of that same deficient definition. Correction IDs
and qualification identities are separate from the existing broad eligibility
epoch; no unrelated value-cache key changes.

Every original file acknowledgment must predate the first correction for that
scope. This cutoff uses retained lifecycle revisions, not a claim about when
computation began. Fresh execution of a known deficient definition, including a
recreated retired file completion, cannot establish an original baseline.
Precomputed but late-acknowledged work also refuses conservatively. A new explicit
definition can consume or validate the omitted field. An empty or incomplete
original-execution history remains unknown.

The inline proof contains the cumulative field set and first-correction revision;
it does not copy the latest decision or whole correction history. Another decision
that adds no new requirement therefore does not invalidate identical evidence.
The shared typed inventory follows the proof's original executions and exact
target snapshot for protection and restore. Recovery completions may retire;
immutable correction decisions and current requirements do not retire as phases.

## Decision path

1. Record a correction even if sufficient historical evidence is unavailable.
   Its durable acknowledgment does not promise successful qualification.
2. Resolve applicable requirements before opening payloads. For an affected
   candidate, use the retained original-file descriptor and target schema to
   establish supported correspondence. Store that result once. Reuse it on
   retries by exact original generation, target, requirement state and checker.
3. Preserve useful completed computation if qualification or later selection
   refuses. Distinguish qualification required, known mismatch and unknown
   history in the existing structured refusal/status output.
4. At `select_with_progress`, evaluate every included computed group against the
   current requirements in the same lifecycle view used for selection. Include
   inherited groups. A correction committed after lookup must defeat an old
   decision through the existing expected-snapshot guard and fresh evaluation.
5. Apply the same rule to new exports of affected values at admission and final
   selection. Existing completed-operation replay and pinned historical reads
   retain their original meaning. Current related-input processing consumes
   source groups only; a correction to an unconsumed computed group must not
   invalidate those source-only consumers.

Use one shared evaluator for early reuse decisions and final adoption. Do not
make ordinary historical reads consult current corrections. A routine result
read continues to resolve an immutable pin and stream the requested data.

The public entry points are `Engine::correct_dependencies`, CLI
`correct-dependencies <request.json>` and HTTP `POST /dependencies/corrections`.
The request names the dataset, stable correction ID, exact group definition,
omitted stable source-field IDs and reason. `plan_backfill` exposes a pending
`correction_required` refusal when qualification is needed; `inspect` retains
the decision and normalized requirement state for investigation.

Prepared, validation and readback caches must retain original execution identity
and cannot bypass the final rule. Before immutable readback exists, supplemental
evidence can qualify the pending association and its new manifest. Once readback
has been retained, a correction requiring a different binding refuses that
operation's selection and directs the caller to a **new operation ID**. The new
operation reuses eligible file/prepared outputs and writes only its qualification
and association/manifest metadata, with composition checks. This fits the existing
single readback identity instead of adding readback generations. Preserve the old
acknowledgment and bytes; never overwrite them or rerun values just to obtain a
new manifest. Completed-operation replay still returns its historical result.
The readback commit also compares its target against already retained readbacks
in the same locked lifecycle view. This covers a correction racing a cache hit,
a sibling-composition retry and another worker retaining the operation first.

## Implementation order and checks

1. **Original identity and shared records.** Add the optional original-declaration
   reference to new file completions, carry original generations through prepared
   groups and bindings, and register the new evidence roots in the typed retention
   inventory. Protect every reference before acknowledgment. Keep older records
   readable without interpreting missing facts as proof.
2. **Correction and qualification.** Implement the bounded service and shared
   evaluator using existing lifecycle/metadata helpers. Add thin CLI/HTTP routes.
   Make plan/inspect report only the affected group, correction and next action.
3. **Adoption integration.** Cover file, prepared, validation and readback reuse,
   shared selection, inherited groups and affected export paths. Preserve exact
   completed replay and unaffected source-only consumers.
4. **Qualification and review.** Run the fixtures below and the repository gate.
   Obtain independent consistency and efficiency reviews of the implementation,
   then update STATUS only for the paths actually qualified.

The decisive Q4 fixture uses production operations:

- Build text, then add virtual `language = en` through schema evolution.
- Compute an English-specific text-only classification through `files`, leaving
  that group unselected. This preserves a real reusable file completion.
- Hide the old language field and add a new same-name field with default `fr`
  and a fresh ID, preserving the source files and text field.
- Admit a new backfill using the current source definition and original group
  definition. Pause the live operation immediately after its actual file-cache
  hit. Commit the scoped correction, then release the pause.
- Require a correction-specific refusal with no intervening source/head/global
  eligibility change. A no-correction control must reach selection. Preserve the
  original completion and outputs exactly.

Selecting the original group before schema evolution creates a prepared cache
entry; test that separately rather than calling it a file-cache hit. Other
required cases are adequate same-field supplementation, unknown original
language, a second correction, cached readback, an affected inherited sibling,
unaffected text-length work, restart/lost responses, conflicting retry IDs,
explicit corrected recomputation, and retention/restore evidence closure.

Metadata work may grow with candidate files, affected declarations and actual
correction decisions; it must not grow per source row or eagerly walk descendants.
Reuse schema reads within qualification and retain its outcome. Measure payload
work avoided, cold/warm metadata work and retained bytes with fixed correction
size before claiming an efficiency gain. The existing bounded lifecycle history
still has cold-read and commit costs; this design does not create a second index,
database, scheduler or general computation/adoption framework.

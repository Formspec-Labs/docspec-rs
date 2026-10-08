# R4 — Retain exact changes without another event system

Status: greenfield implementation plan. [Shared decisions](../PLAN.md) govern identity, commits, and evidence. No existing revision format or reader API must be preserved.

## User outcome and requirements

“A small update tells consumers exactly which members and material fields changed, without repeatedly comparing the whole dataset.”

A change relation identifies exact base and result versions, logical keys, before/after occurrence or value references, and changed field groups where established. It includes inserts, removals, replacements, and one-to-many shrinkage. Incomplete or uncertain field evidence must broaden affected work, never silently omit it.

## Design

Produce a bounded typed change relation from the same validated write relations that construct the new snapshot. Prefer maintained engine change facilities when they provide the required completeness, delete semantics, and identity; otherwise add a narrow adapter to those write relations. Do not build an independent global event log or per-row catalog graph.

A versioned descriptor binds endpoints, schema/identity rules, operation, physical reference, digest, and completeness claim. The catalog atomically commits the result and trusted descriptor link. A caller-supplied file is not certified merely because its fields look correct. Verify endpoint membership, unique keys, before/after addresses, and all implicit removals.

Comparison and admission consume R1's exact selected-input/read context. Reuse qualified decoding, member resolution, and same-scope verification within an operation; do not create a separate reader/cache per comparison consumer. Check known immutable identities and completed comparison evidence before payload staging when their trust and dependencies permit it. Independent test oracles and required external admission remain separate. Bound reader lifetime and retained native relations rather than accumulating all decoded rows.

Distinguish row/value changes, receipt/evidence changes, schema changes, definition changes, and coverage-only results. Adding a schema mapping or independent enrichment does not imply that every base occurrence changed. A processing change can require re-evaluation even if output bytes eventually match. A receipt-only change must remain visible to evidence-dependent consumers even when value comparison is empty. Preserve definition/result evidence independently from value-change discovery.

Compose a forward chain by logical key, taking the first old and final new references and verifying continuity. Drop net cancellations only when the selected identity comparison establishes equality; equal displayed values may represent distinct source occurrences. Changing field groups can conservatively overstate net affected fields, but cannot understate them. Spill or partition long chains within declared limits.

Use one comparison service for engine-provided and adapter-produced evidence. Its public result includes exact endpoints, before/after addresses, path, and fallback reason. Full comparison remains the independent oracle and fallback for missing optional history, unsupported accelerators, reverse comparisons, or unrelated states. Invalid present evidence refuses rather than returning an empty result.

Change acceleration is independently retained. A required consumer binding protects its chosen evidence, but a complete state must not depend on keeping all historical accelerators. Missing optional changes can slow comparison without losing state readability.

Accepted source succession is a separate, minimal catalog relationship used by R5 nonregression checks. Retain enough ordering evidence for selected coverage; do not infer order from an optional change file, wall clock, or equal rows. Physical compaction can replace representations without changing endpoint meaning or producing logical edits.

## Implementation tasks

| Task | Owner and dependencies | Deliverable and acceptance |
| --- | --- | --- |
| **R4-T01** | Change/identity semantics; R1/R3 shared design and first-path measurements | Define typed descriptors, field-group and evidence-only change meaning, trusted admission, endpoint comparison, and independence from source-order evidence. Compare maintained facilities before an adapter. Examples cover insert/remove/replacement, no-op, reversion, receipt/schema/enrichment-only change, and contradictory endpoints. |
| **R4-T02** | Native write/change production; T01, R1 commit interface | Retain exact before/after changes from actual writes and deletes in bounded native storage. Atomically commit descriptor links with the result. Segment shrink and adopted values are correct; retries recover identical committed evidence; independent full comparisons match. |
| **R4-T03** | Admission and reference protection; T01–T02, R1 shared read/phase interface | Verify certificates through maintained decoding and shared exact-input admission; protect files before cleanup exposure. Repeated same-scope checks reuse qualified results. Forged links, truncated changes, conflicting identities, missing deletes, changed validators, or altered references cannot inherit prior approval. Keep bulk keys outside catalog rows. |
| **R4-T04** | Comparison service; T03 | Implement bounded chain composition and full-comparison fallback. Validate continuity, net reversions, exact endpoints, and conservative field impact. No unsupported branch order is inferred. Report the comparison path through normal operation status. |
| **R4-T05** | Retention and physical maintenance; T03–T04, R6 | Remove optional history while endpoints remain readable and comparison falls back correctly. Required consumer and source-order evidence remain protected. Equivalent compaction reports zero logical changes and allows obsolete physical representations to retire safely. |
| **R4-T06** | Qualification; early probes with T01/R7, optimization after first-path evidence, full checks after T02–T05 | Validate independent membership/field/evidence changes, corruption, concurrency, and interruption. Count selection, decoding and verification passes; compare cold and warm discovery, index construction, unchanged reruns, and resumed work. Include write and recovery overhead in the complete consumer path. Include the independent growth axes in [Q7 metadata and evidence scaling](07-operations-qualification.md#q7-metadata-and-evidence-scaling). |

## Resource behavior and rollout

Change discovery does not guarantee delta-proportional writes. The chosen engine may still scan keys, load manifests, validate inherited files, or rewrite physical partitions. Measure these separately before adding an index. Prefer maintained indexing/layout mechanisms justified by actual access patterns; there is no universal index requirement.

Where ordering, population, identity, and policy have been verified, qualify bounded ordered streaming as an alternative to constructing a full-payload lookup index. Reordered, missing, added, duplicated in a different position, or differently classified records must take an explicitly correct path; an ordering mismatch cannot silently inherit evidence by position. Compare initial index construction, cold admission/download, temporary storage, invalidation, and recovery with the complete consumer benefit. A faster warm lookup alone does not justify an index.

Preregister fixed-size updates over increasing population and history, long chains, hot keys, wide rows, field/evidence-only changes, and high-churn controls. Include unchanged and interrupted/resumed runs with cold/warm read and index states. Record exact before/after equality, relevant order and duplicate preservation, selection/decoding/validation passes, bytes/files read and written, construction/comparison time, update overhead, whole-process memory, and spill. Set component budgets from R7 lifecycle targets before results; do not adopt a warm lookup or relative speedup as proof of user value.

Enable an accelerated path only after it agrees with the oracle across supported cases. Keep a force-full-comparison control. Qualify cross-workspace certificate trust separately; a producer's local optimization is not automatically trusted public evidence.

Open decisions are supported change/field granularity, engine change facilities, long-chain bounds, optional history duration, and portable certificate admission. Source-succession retention must remain explicit even when acceleration history expires.

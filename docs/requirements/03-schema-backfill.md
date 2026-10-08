# R3 — Evolve schemas without rebuilding unrelated data

Status: greenfield implementation plan. [Shared decisions](../PLAN.md) govern identity, commits, coverage, and publication. Current whole-row schema/definition hashing is not a design constraint.

## User outcome and requirements

“I can add or improve fields without reprocessing unrelated data, and readers can distinguish complete results from pending work.”

A change has explicit meaning: metadata-only evolution, a new computed field group, a transformation of existing values, or a key/relationship change. Historical schema versions remain readable while retained. Completion distinguishes pending, failed, successfully null, successfully empty, and deleted inputs.

## Schema and identity design

Give fields stable IDs independent of display name, position, and physical file. Version the schema, type interpretation, defaults, field presence rules, and reader mapping in the same catalog as dataset versions. Do not create a second schema authority. Use maintained table-engine evolution features where their semantics satisfy these requirements.

Separate business key, source observation/occurrence, immutable value or field-group identity, processing definition, and dataset version. A whole presentation row may change when its schema changes while unchanged source occurrences and value components remain reusable. Never keep an earlier occurrence ID while mutating the value it identifies. Do not salt every unchanged value with a global schema or processor version merely to record a new definition; record execution and result bindings explicitly.

For reuse, distinguish material dependencies from full runtime provenance. Record actual source/subject pins, receipt/evidence pins, processor/configuration, schema, policy, decoder, and validator versions consumed by each phase. An unrelated package or presentation change does not automatically invalidate all computed values. A relevant code, configuration, policy, schema, or validator change invalidates the checks or values it can affect; uncertain dependency adequacy requires conservative recomputation or revalidation. Never declare a changed implementation irrelevant without a justified dependency/version rule.

Receipt-only changes remain observable even when values are identical. Preserve the new evidence/result association and rerun evidence-dependent qualification; reuse value computation only when its material inputs still correspond. Do not treat identical row hashes as proof that provenance, policy, or acceptance stayed unchanged. R1 owns the common dependency and phase-reuse representation, so schema evolution does not add another cache-key scheme.

| Change | Default behavior | Required check |
| --- | --- | --- |
| Rename/reorder with stable field meaning | New schema mapping; reuse value storage | Existing IDs, types, and old-version reads remain exact. |
| Add optional field or literal default | Versioned read rule can avoid rewriting old files | Preserve absent versus explicit null/default semantics; no claim that historical computation occurred. |
| Add computed field group | New derived values and coverage over pinned inputs | Pending differs from a computed null; only affected processing runs. |
| Change processing logic | Re-evaluate affected dependencies; adopt prior values only with justified correspondence | New definition and actual execution/reuse evidence remain explicit. |
| Change type or meaning | Explicit lossless mapping if proved; otherwise transform affected values | Preserve exact old values, new interpretation, and failure outcomes. |
| Change key/relationship | Explicit re-key and collision/multiplicity handling | No silent merge or loss of distinct members. |
| Hide/drop field in new view | New schema selection | Retained old states and required field bytes remain available. |

A metadata-only addition is a schema operation, not completed backfill of a computed field. A virtual value must resolve deterministically from retained rules and inputs. Qualify supported evolution cases rather than claiming automatic arbitrary type widening.

Group independently computed fields by shared processing and update behavior, such as a comment-classification group. Version that group separately from base comments; do not require one physical table per attribute. Bind outcomes to exact source occurrences and definitions. When a newer source differs only outside adequate declared dependencies, a new result may explicitly adopt an older value with verified correspondence. Preserve its original generation and record the new input binding; stable business-key equality alone is insufficient.

Consumers receive a pinned combined view or R2 materialization. This composition has explicit keys, coverage, schema, and input-version relationships. Compare its read cost with the avoided wide-table rewrite before fixing the grouping.

## Backfill and cutover

Prepare a candidate under the new definition while readers use the selected version. Retain the accepted source sequence, pinned scope, schema, definition, batch identities, output references, and bulk coverage. Process bounded batches; atomically commit each output and its matching coverage through R1. Resume verified committed batches without re-executing them. Bulk progress belongs in bounded native relations, not a per-row catalog journal.

Use the shared selected-input/read context and retain costly restoration, computation, and validation completions independently of final cutover. A validation or reporting failure must not discard completed candidate data. Coverage advances only after the required output and processing outcome are committed; a transfer completion cannot substitute for computed coverage. Status and resume explain which dependencies changed and which completed phases remain reusable.

Catch up from the candidate's covered source boundary using R4 changes or complete comparison. Replay inserts, corrections, removals, and one-to-many replacements. The candidate must equal independent processing of the target pinned inputs.

Promotion requires complete declared coverage, with accepted null/empty outcomes explicit and unresolved failures blocking. The shared catalog transaction checks expected target, active writer definition, all required live source heads, and the cutover boundary, then selects the candidate and new definition together. Old-definition workers cannot commit afterward. If continuous input prevents convergence, use a bounded write fence or durable queue boundary; retain all accepted later input for processing under the new definition. No hidden indefinite lock or dropped accepted work.

Routine R5 refresh uses captured-version promotion and does not inherit this strict latest-head check. Share result resolution, coverage validation, and promotion mechanics, while keeping backfill batching and catch-up in a focused service. Rollback after new-definition writes requires explicit reconciliation; blindly selecting an older state can lose updates.

## Implementation tasks

| Task | Owner and dependencies | Deliverable and acceptance |
| --- | --- | --- |
| **R3-T01** | Schema/identity semantics; foundational slice before first Comments path, with R1/R4/R7 | Define stable field IDs, value/group identity, material dependency/version rules, and evolution classification. Include receipt-only changes, unrelated integration edits, and relevant policy/validator changes. Required checks invalidate precisely; unrelated values keep identity. Foundational rules do not require full schema-evolution delivery. Define reuse behavior for [Q4 discovered dependency omissions](07-operations-qualification.md#q4-discovered-dependency-omission). |
| **R3-T02** | Candidate/coverage model; T01 | Define bounded backfill descriptions, progress, definition selection, and shared promotion inputs. Represent pending/null/empty/failed/deleted distinctly. Catalog metadata names native coverage tables; empty candidates and complete empty outcomes need no fake rows. |
| **R3-T03** | Batch execution and commits; T02, R1 phase/read interface | Implement bounded processing, shared input selection, exact retries, resource accounting, and atomic output/coverage commits. Retain costly intermediate phase completions. Kill before/after commit, validation, response, and report rendering; reuse qualified work, refuse conflicting reuse, and preserve uncertainty. |
| **R3-T04** | Catch-up and dependent-field selection; T03, R4 fast path | Apply complete insert/change/delete scope using certified changes or the oracle. Recompute only semantically affected work; preserve prerequisite and field-group reuse where justified. Concurrent edits, reversions, and segment shrink match full reconstruction. |
| **R3-T05** | Catalog promotion/definition fencing; shared primitive can follow T01/T02, full cutover needs T04 | Implement strict cutover alongside R5's captured policy. Verify complete coverage, source boundary, expected target, and active definition atomically. Race workers and cutover; no accepted input disappears, old-definition commits refuse, and retries preserve identity. |
| **R3-T06** | API/CLI and reader integration; T03–T05, R2/R6 | Expose plan, inspect coverage, resume, validate, and promote through focused APIs without legacy signatures. Support schema-only and enrichment cases as distinct operations. Operators see pending work, definition, source boundary, read representation, local commit, and public release separately. |
| **R3-T07** | Qualification; early identity/dependency probes with T01, full evolution after first Comments path and T06 | Qualify historical reads, native types, empty states, key collisions, precise invalidation, continuous updates, and phase recovery. Receipt-only changes preserve evidence without unnecessary value recomputation; relevant validator changes rerun affected checks. Compare enrichment composition with equivalent wide-table processing and verify exact provenance. Pass [Q2 concurrent backfill](07-operations-qualification.md#q2-concurrent-field-group-updates) and [Q4 dependency-omission](07-operations-qualification.md#q4-discovered-dependency-omission) cases. |

## Measurement and remaining decisions

Measure source/receipt bytes read, selection/decoding/validation passes, reused phases, values recomputed, files rewritten, catalog/progress cost, retained bytes, catch-up and foreground latency, and consumer read/materialization cost. Include resumed backfills and dependency-only changes. Hold changed fields/rows fixed while increasing table width, population, and history. A schema rename should not trigger payload rewrite; computed additions still pay for required historical computation. Actual gains require measurements.

Set supported evolution/type rules, field-group boundaries, reader capabilities, backfill lag, and cutover-fence limits before qualification. Partial computed-field promotion remains a separately specified product capability. Complete coverage is the default; it does not force an eager rewrite for a valid metadata-only schema operation.

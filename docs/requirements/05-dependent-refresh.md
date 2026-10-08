# R5 — Refresh complete captured versions while inputs advance

Status: greenfield implementation plan. [Shared decisions](../PLAN.md) govern commits, identity, and policy. Existing schedulers, result classes, and API names are not constraints.

## User outcome and requirements

“Corrections update all affected joins and summaries, and results keep advancing while new data arrives.”

A comment moving dockets refreshes both groups. A newly available docket creates previously impossible matches. Removal retracts contributions. New field groups can be processed independently, and unchanged outputs can gain newer verified input coverage.

## Design

A bounded refresh plan freezes prior output, prior covered inputs, target input vector, processing definition and dependencies, join/group rules, and promotion policy. Retain output and coverage together as a completed result. The selected result is the authority for consumer progress; do not add a second mutable cursor.

Resolve each exact subject and receipt/evidence selection once through R1's shared read context. Consumers share maintained decoding/member resolution and qualified same-scope validation, with bounded streams/native relations and explicit reader lifetime. Check retained phase/results before restoring or staging an unchanged input. Unknown identities or changed validation dependencies still require admission; output equality alone cannot hide new receipts or policy effects.

Use a maintained relational engine for joins, grouping, and bulk selection. Scope the first supported incremental rules explicitly: equality joins with declared null/cardinality/unmatched behavior, one-to-many replacement, and affected-group recomputation. Unsupported rules use a complete rebuild or explicit refusal. Do not develop a general incremental SQL compiler to support this lifecycle.

Candidate discovery unions old lineage dependencies, rows matching old and new join keys on every changed side, newly added rows, and old/new group membership. R4 before/after and field-group evidence narrows work only when dependency declarations justify it. Arbitrary processors with unknown dependencies require conservative recomputation. Use typed relations and supported expressions rather than interpolating source values into SQL.

Plan evidence-dependent work separately from value recomputation. A receipt-only change produces an updated evidence/result association and reruns affected qualification even when values are adopted. R3's material dependency/version rules prevent unrelated integration changes from invalidating all work and require relevant processor, policy, or validator changes to invalidate the affected phases. Keep full execution/package provenance available without using one broad hash as the sole reuse decision.

Recompute complete affected groups by default. Arithmetic delta maintenance is a later operation-specific optimization requiring exact equivalence and meaningful savings. Preserve contributor evidence in bounded retained group-input relations, not unbounded occurrence arrays or truncated lineage. A hot group may legitimately require most of a table.

Independent enrichments follow R3's grouping. Bind a value to its exact source occurrence and definition. If a new source differs only outside adequate material dependencies, retain an explicit correspondence result associating the new input with the adopted prior value. Keep its original generation. Never attach stale enrichment to a corrected comment through business-key equality alone. A combined view or materialization must satisfy declared complete coverage; pending computation differs from successful null.

## Shared promotion and recovery

The catalog transaction atomically validates a completed candidate, expected output, active definition, input relationships, and promotion policy. Routine captured-version policy permits live sources to advance while computation runs. For each input, candidate coverage must equal or follow the selected result's coverage along retained accepted source history. A backward or incomparable vector, a branch, or missing ordering evidence requires explicit reconciliation. Timestamps and equal content do not prove order. R4's optional accelerators are separate from this required succession evidence.

After a successful captured promotion, report exact covered inputs and observed lag, then schedule catch-up from that result. Do not claim the current source universe was completely acquired. Stable inputs must eventually converge; sustained load beyond capacity must expose backlog and backpressure.

An explicit latest-input policy also compares required live heads. R3 schema cutover always uses its strict boundary and definition fencing. Both policies refuse an unexpected output or stale definition. If output bytes are unchanged, commit a new completed result/coverage binding that adopts prior values; do not invent a new generation of those values. R2 publishes that completed result explicitly.

Use the same result resolution, reference admission, coverage checks, and conditional commit mechanics for backfill and refresh. Keep their discovery and batching logic separate. A maintained executor may schedule parallel work and retry attempts, but committed dataset state remains catalog-owned.

Resume only a checkpoint matching the plan, definition, and policy. Recover committed results by stable identity; rerun uncertain uncommitted work with visible attempt history. Lost old values needed for departing keys require qualified full reconstruction, not assumed absence. Malformed evidence or missing required new inputs refuses.

Commit costly restoration, matching/processing, evidence construction, and validation phases independently using R1's ordinary result representation. A later admission, report-rendering, or publication failure must not discard qualified matching output or its exact selected inputs. Resume through the supported API/CLI, which explains remaining work and reused phases. No intermediate phase advances final output coverage or freshness before the required complete-result checks pass.

## Implementation tasks

| Task | Owner and dependencies | Deliverable and acceptance |
| --- | --- | --- |
| **R5-T01** | Refresh semantics; coordinate R1/R3/R4 | Define supported joins/groups, output keys, field dependencies, lineage bounds, complete empty results, input consistency, accepted succession, and captured/latest policies. Examples include simultaneous input changes and branch reconciliation. Full rebuilding alone cannot repair missing ordering evidence. Specify coupled and independent source cases from [Q5 related inputs](07-operations-qualification.md#q5-related-input-consistency). |
| **R5-T02** | Planning, shared input reuse, and retained coverage; T01, R1 phase/read rules | Create bounded plans with exact subject/evidence pins, material dependencies, shared selection/read context, and ordering protection. Check qualified completed work before restoring/staging payloads. Use certified changes or full comparison; conflicting reuse refuses; bulk selections remain native. No independent cursor or cache. |
| **R5-T03** | Relational candidate discovery; T02 | Discover a complete deduplicated affected scope using old/new keys and newly possible matches. Test docket moves, additions, removals, null transitions, duplicate matches, many-to-many fanout, field-only updates, and input changes in either order. Conservative supersets are acceptable; omissions are not. |
| **R5-T04** | Processors, phase results, and evidence construction; full refresh follows T03; empty-state support can follow T01 | Execute justified affected work; retain costly processing/matching output and evidence as independently recoverable phases. Support complete one-to-many replacement, removals, bounded contributor evidence, and empty results. Receipt-only changes update evidence without unnecessary value recomputation; independent content/identity checks agree. |
| **R5-T05** | Catalog promotion and phase recovery; T02–T04, R1/R3 shared primitive | Commit complete output/coverage under guards while retaining qualified phase outputs across later failures. Captured results advance under source movement; incomparable coverage requires reconciliation. Test no-change/evidence-only adoption, late validation/report failures, dependency invalidation, lost responses, output races, and stale-definition workers. Pass [Q5 related-input selection](07-operations-qualification.md#q5-related-input-consistency). |
| **R5-T06** | Execution integration and qualification; T05, early workload design with R7 | Run identical frozen work directly and through any selected maintained executor. Qualify Comments/docket joins, summaries, and enrichment reuse under failures and continuous ingestion. Document throughput, backlog, read/materialization tradeoffs, and full-rebuild fallback. |

## Measurements and decisions

Compare selective refresh with full reconstruction for equivalent pinned inputs. Hold changed rows/fields fixed while increasing population, width, and history; include fixed-rate, hot-group, evidence-only, unchanged-repeat, and interrupted/resumed controls. Measure selection/decoding/validation passes, cold restoration/index cost, discovery scans, compute, reuse checks, phase commits, requests/bytes, whole-process and engine memory, spill, retries, and consumer join/materialization cost. Optimize confirmed repeated work from the first Comments path before speculative indexes. Include remote durability and independent publication reading in R7's final result.

Test sources moving faster than one refresh completes: captured results must still make valid progress, expose lag, and converge when inputs stabilize. Also test explicit latest-policy refusal, source reversion, lost succession evidence, and schema cutover. No throughput guarantee is claimed for overload.

Open decisions are supported rules, dependency granularity, bounded group-evidence representation, lag/backpressure targets, and measured layout/index needs. Default to captured versions with complete coverage; strict latest-input policy is an explicit product choice. General incremental SQL and universal indexes are deferred without measured need.

# Implementation and qualification status

This repository follows the user's local-first, Iceberg-only revision of the
[greenfield proposal](PLAN.md).
The revision selects Iceberg rather than the older document's Lance-first
comparison and keeps DocSpec lifecycle records in Iceberg. There is no migration
or compatibility layer for legacy DocSpec.

## Requirements

| Requirement | Implemented locally | Required work remaining |
| --- | --- | --- |
| R1 Durable data and commits | Retained input bytes; native tables; guarded selection; stable retry identity; completed phase reuse; process-kill recovery | Broader durability failures; partial applied updates; complete occurrence/value identity and evidence semantics |
| R2 Incremental publication | Immutable local manifest; native independent readback before catalog selection; retry reconciliation; original completed release reuse | Authorized public exports; R2 uploads and independent remote readback; transfer reuse and provider qualification |
| R3 Schema evolution and backfill | Typed pinned schemas; full-query replacement; explicit definition invalidation; historical schema reads | Stable field identities; independently computed groups; pending/null/failure outcomes; backfill and exact-boundary cutover |
| R4 Verified changes | Full comparison preserving duplicate counts, schemas and input identity | Typed changes from actual writes; inserts, replacements, deletions and evidence-only change records |
| R5 Dependent refresh | Repeatable DataFusion query execution over one retained Parquet input; complete captured input selection; coverage regression refusal | Multiple pinned related inputs; old/new-key affected-result discovery; joins and affected-group refresh against full reconstruction |
| R6 Maintenance and retention | Retained native history; catalog-free pins; protection before preparation; all cleanup disabled | Reader/release/phase retirement; shared-owner roots; compaction; retention preview/reservation/deletion reconciliation; complete restore points and fenced fresh-catalog restore |
| R7 Operations and qualification | Shared services; CLI; loopback HTTP; plan, run, inspect, resume, read, verify and compare; structured refusals; configured budgets; fault harness; raw local experiments | Complete counters; aggregate resource qualification; broader fault tests; representative Comments corpus; final local and remote performance gates; Dagster adapter |

The CLI exposes implemented operations only. Unsupported updates currently require
an explicit full query; there is no claim of general incremental SQL or independent
column backfill. A retained source file establishes whole-input provenance, not
per-value evidence relationships or complete DocSpec conformance.

## Component qualifications

| Component | Evidence and boundary |
| --- | --- |
| Iceberg and Iceberg SQL catalog 0.10.1 | Native reads/writes and expected-snapshot commits execute in integration tests. A narrow catalog guard prevents stale transaction refresh. Local ownership uses an OS lock. Remote multi-host use is unqualified. |
| Arrow/Parquet 58 and DataFusion 53.1.0 | Exact retained flat and nested inputs pass an independent PyArrow comparison. The duplicate-removal regression demonstrates why component qualification is necessary. General SQL correctness and all source types are not established. |
| Filesystem and SQLite | Process termination after acknowledged handoffs is exercised. Machine power loss, storage faults and host loss are not qualified. |
| Resource controls | The application reserves reader/writer capacity; the shared DataFusion runtime bounds execution memory and spill. Whole-process memory includes allocations outside these controls. |
| Append-only lifecycle layout | Deliberately provisional. The bounded history probe exposed repeated scans; one disposable exact-snapshot cache reduced that cost. There is no durable auxiliary index. Cold lookup and retained metadata still grow with history. Warm admission and cold replay now enforce the same bound; history growth refuses new commits before acknowledged records become unreadable. |

## Delivery gates

The local foundation and retained Parquet path are implemented and tested. The
first milestone's wider capacity and durability qualifications are still open;
the Comments milestone also needs actual Comments inputs and evidence semantics.

The required sequence continues with incremental updates and backfill, maintenance
and restore, then R2 and Dagster. Provider credentials and actual deployment behavior
must be qualified before claiming hosted support. Public-data authorization is not
inferred from a readable local file.

The full performance gate remains **not run**: a frozen representative selection
of 10 million retained Comments, deterministic shaping, docket joins/summaries,
evidence, native and flat outputs, release selection and independent readback in
1,800 seconds, passing on each required fresh-workspace repetition. Neither small
fixtures nor repeated local metadata lookups establish this target.

Q1–Q8 remain the acceptance framework. Current tests cover bounded worker recovery,
stale dependency refusal and independent reading. Concurrent field-group updates,
shared cleanup, related-input selection, old-actor restore, the full metadata-growth
matrix and the complete consumer path remain unqualified. See the original
[qualification plan](requirements/07-operations-qualification.md).

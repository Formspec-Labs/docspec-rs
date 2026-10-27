---
id: def-l154
status: closed
deps: []
links: []
created: 2026-10-27T10:53:30Z
type: task
priority: 2
assignee: mikewolfd
tags: [engine-first, C1]
---
# C1: bounded multi-input SQL job service

## Acceptance Criteria

Local SQL joins declared Parquet tables and raw-preserving nullable receipt rows. Named steps reserve memory or spill to scratch. Runner tests cover conflict, line identity, join, resources and refusal. Iceberg provider qualification remains M1 owner-gated.


## Notes

**2026-10-27T11:18:32Z**

Implemented shared-budget multi-input SQL with exact pinned Iceberg static providers, Parquet inputs, raw-preserving nullable gzip JSON, memory-owned named results and shared managed scratch spill. Six focused tests pass, including two independent tables/file/receipt, partition predicate, type conflict, safe exact-line errors, sparse schemas, shared quota and frame lifetime. Full locked offline cargo gate:385lib/354integration/15restore/2worker/11probe pass,17ignored; fmt and all-target/all-feature Clippy pass. M1 live Basin format/performance evidence remains owner-blocked separately; this closes the local C1 capability, not track C.

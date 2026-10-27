---
id: def-l154
status: in_progress
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


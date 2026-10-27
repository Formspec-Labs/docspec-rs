---
id: def-acon
status: in_progress
deps: []
links: []
created: 2026-10-27T19:00:59Z
type: task
priority: 1
assignee: mikewolfd
tags: [engine-first, C2]
---
# Align compatible nested Arrow fields with native table schema

## Acceptance Criteria

Native writer uses maintained recursive Arrow casts and existing compaction for list/struct field metadata and nullability, preserves field IDs, refuses actual nulls in required children and incompatible child names/types, and accepts slices with unused null backing. D3 nested append reproducer passes.


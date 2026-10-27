---
id: def-acon
status: closed
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


## Notes

**2026-10-27T19:17:34Z**

Nested value fixtures3passed and storage group100passed after review resource fix. CODE sole concern reproduced393b0e7 (10MB list charged13968B), fixed actual retained-byte admission; ARCH APPROVE. Native fields, strict nulls/type/name checks, compacted slices, sameBudget and maintained Arrow casts. All-target/all-feature Clippy passed before the small admission correction. D3 full adapter verification follows shared snapshot.

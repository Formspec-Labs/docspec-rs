---
id: dec-xn81
status: closed
deps: []
links: []
created: 2026-10-28T14:35:27Z
type: bug
priority: 0
assignee: mikewolfd
---
# Preserve DISTINCT cardinality across equality joins


## Notes

**2026-10-28T15:06:34Z**

Fixed in 729eb8d after regression commits 2b99ad7 and eb74869. Exact real 49-/50-row seed Parquets now return all 48 matching document IDs under DISTINCT, matching the plain query; 885e8c3 and c9d518b returned one. Stock DataFusion 53.1 reproduces the fault in replace_distinct_aggregate wildcard USING-column exclusion. Guarded native aggregation preserves the complete selected schema; alias/ON, partial grouping, duplicate, null, empty-input and DISTINCT ON controls pass. Independent review APPROVE with no material findings. Final scripts/check EXIT 0: 440 library, 435 integration, 15 restore, 3 process memory, 2 worker, 11 probe and 31 Dagster tests; 17 existing ignored tests unchanged. Ordinary remote binary and additive final qualification sent to engine. Upstream issue draft prepared locally per network-write rule. Broad production impact inventory is a separate retained diagnostic; scoped seed/deploy paths already cleared with exact885 metadata plans and a failing positive control.

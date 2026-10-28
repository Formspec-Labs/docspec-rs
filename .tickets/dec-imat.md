---
id: dec-imat
status: in_progress
deps: []
links: []
created: 2026-10-28T10:19:25Z
type: feature
priority: 1
assignee: mikewolfd
tags: [engine-first, D1]
---
# Accept per-row JSON path lists in one-pass extraction

Lead giant-lines finding: preserve one native JSON parser traversal for all caller-supplied targets of a retained record. Extend existing json_get_many with per-row path definitions while preserving literal specialization, Arrow string representations, path validation, output positions and all admission limits. Raw owns SQL/grain and pointer selection.

## Acceptance Criteria

Contract-first dynamic path/representation/null/invalid/byte-fitting checks pass; parser and builders reused with pre-allocation admission and no growing definition cache; focused checks/review/ordinary executable delivered. No application field names or parsing policy.


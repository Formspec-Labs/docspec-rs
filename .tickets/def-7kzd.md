---
id: def-7kzd
status: in_progress
deps: []
links: []
created: 2026-10-27T19:09:29Z
type: task
priority: 1
assignee: mikewolfd
tags: [engine-first, C1]
---
# C1: plain and gzip JSON lines through one native reader

## Acceptance Criteria

One native JSON-lines path detects gzip by magic, reads plain provenance JSONL as well as compressed receipts, preserves raw-line bytes/source keys/ordinals/type-conflict nulling/admission, and refuses corrupt gzip without a plain fallback.


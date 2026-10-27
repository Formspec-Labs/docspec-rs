---
id: def-7kzd
status: closed
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


## Notes

**2026-10-27T19:17:34Z**

Plain/gzip equivalence and truncated-gzip refusal fixture passes; Clippy passes; CODE and ARCH APPROVE81f21b0. Existing single reader retains raw UTF8 lines, source ordinals and nullable conflict handling; no fallback after gzip error. Line-count correction for81f21b0 body: job.rs211 before/211after; docs/sql-jobs.md125before/125after; json_lines.rs296before/309after as recorded.

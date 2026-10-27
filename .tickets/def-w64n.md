---
id: def-w64n
status: in_progress
deps: []
links: []
created: 2026-10-27T14:16:10Z
type: task
priority: 1
assignee: mikewolfd
tags: [engine-first, C6]
---
# C6: compact logical Arrow slices before admission

## Acceptance Criteria

Bounded materialization/export compact oversized retained backing buffers without undercharging them. Small scalar and nested slices pass within their logical-byte allowance; genuinely oversized values and repeated view expansion still refuse. Existing SQL job/export fixtures remain equal.


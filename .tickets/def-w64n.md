---
id: def-w64n
status: closed
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


## Notes

**2026-10-27T15:11:54Z**

Done: six bounded slice regressions pass; post-fix eight export and six CLI contracts pass, including last-use release validation; prior22SQL checks cover final-owner scratch cleanup. All-target/all-feature Clippy and remote-only binary build pass. Code and architecture reviews APPROVE the confirmed resource fixes. Whole C6 model-build/RSS acceptance remains separately tracked.

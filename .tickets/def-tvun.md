---
id: def-tvun
status: in_progress
deps: []
links: []
created: 2026-10-27T11:06:57Z
type: task
priority: 2
assignee: mikewolfd
tags: [engine-first, C3]
---
# C3: deterministic SQL result export with explicit writer profiles

## Acceptance Criteria

Native export applies caller-supplied order and per-file writer settings, writes fixed row groups and required indexes, hashes SHA256 and MD5 during writing, and gives byte-equal reruns/input permutations on pinned limits. Cross-runner acceptance remains phase2; range request evidence follows reader binding.


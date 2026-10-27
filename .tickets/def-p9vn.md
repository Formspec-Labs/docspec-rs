---
id: def-p9vn
status: in_progress
deps: []
links: []
created: 2026-10-27T13:57:56Z
type: task
priority: 1
assignee: mikewolfd
tags: [engine-first, C0]
---
# C0: remove production-only lifecycle shortcuts

## Acceptance Criteria

Ordinary and test builds use the same guarded lifecycle commit and native pin read semantics; no dataset-name shortcut skips remote admission. Archive jobs remain independent of retained lifecycle fault machinery. Focused recovery and native job tests plus a remote-only build pass; M1-dependent caps and cross-platform digests remain explicit gates.


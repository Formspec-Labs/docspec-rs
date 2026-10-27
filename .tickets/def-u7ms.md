---
id: def-u7ms
status: in_progress
deps: []
links: []
created: 2026-10-27T18:36:53Z
type: task
priority: 1
assignee: mikewolfd
tags: [engine-first, C7]
---
# C7: native table operations in declarative jobs

## Acceptance Criteria

Existing job CLI/library executes explicit local or injected-catalog create/append/purge operations, recomputes materialized work against current authority after conflicts using C2, validates identity/empty work, preserves completed operation reports, and opens no lifecycle workspace. CLI/local fixtures pass.


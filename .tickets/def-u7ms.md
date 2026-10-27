---
id: def-u7ms
status: closed
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


## Notes

**2026-10-27T18:44:48Z**

C7 local/injected service composition implemented38d53e7. Four table-job contracts and all42 SQL checks pass; all-target/all-feature Clippy passes. CODE and ARCH APPROVE. Existing CLI exports remain unchanged; table failures retain completed authority. New immutable snapshot engine-38d53e7 shared with raw. Direct injected-catalog fixtures will accompany C5 startup binding, and M1/live acceptance remains gated.

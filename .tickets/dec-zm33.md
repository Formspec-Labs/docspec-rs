---
id: dec-zm33
status: closed
deps: []
links: []
created: 2026-10-28T02:34:42Z
type: bug
priority: 1
assignee: mikewolfd
---
# Retain scalar output admission with Arrow buffer ownership

## Acceptance Criteria

Shared buffer ownership across generic functions and json_get_many; clone/slice/child lifetime checks; reviewed.


## Notes

**2026-10-28T03:10:25Z**

Closed after independent review APPROVE, Arrow clone/slice/child buffer lifetime tests, generic SQL-function checks, and complete scripts/check pass. Whole-process bounds and DataFusion scalar-copy allocations remain separate from buffer ownership.

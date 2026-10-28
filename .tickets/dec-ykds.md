---
id: dec-ykds
status: in_progress
deps: []
links: []
created: 2026-10-28T12:13:56Z
type: bug
priority: 2
assignee: mikewolfd
---
# Expose retained SQL step estimates to native join selection

Lead121138 requests retained steps publish inexact row and byte statistics through ExecutionPlan::partition_statistics. Reuse row/byte counts collected during materialization, without another scan, so native JoinSelection can compare retained inputs. No application SQL or seed ordering changes.

## Acceptance Criteria

Contract first binds memory, scratch, projection, limit and empty estimates; native plan observes inexact rows and bytes; focused checks and one independent review pass.


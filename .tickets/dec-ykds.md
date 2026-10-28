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


## Notes

**2026-10-28T12:46:31Z**

Owner123456 explicitly pauses off-path statistics for critical JSON reader. Full sql_jobs focused lane found existing wide_view_batches_fit test regression at a010: physical wrapper prevents native byte-batch preservation; native projection delegation alone did not resolve it. Preserve assertion unchanged. Defer feature/contracts in new commit restoring ad5 materialization path for the reader prefix; source and contracts retained in92d0/a010 plus scratch projection WIP. Do not claim statistics qualification or closure.

---
id: def-7lka
status: closed
deps: []
links: []
created: 2026-10-28T02:25:46Z
type: task
priority: 1
assignee: mikewolfd
---
# Reuse materialized row counts for native append admission

Owner cheap C7 concern: materialization already counted every row and exposes Exact statistics for retained memory and scratch providers. Replace frame.count emptiness scan with this metadata; reject unexpected statistics without rescanning. Preserve create/append/no-op and spill behavior through existing native tests.


## Notes

**2026-10-28T05:58:44Z**

Closed as covered by core dec-58l0 and integrated code9b59263/71cc6eb: materialized native row counts are reused for append admission, with focused table/no-op tests and coherent419library/418integration fullgate PASS. No new rescan path was added; existing append semantics retained.

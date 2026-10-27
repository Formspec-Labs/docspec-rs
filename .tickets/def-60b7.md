---
id: def-60b7
status: closed
deps: []
links: []
created: 2026-10-27T21:34:48Z
type: task
priority: 2
assignee: mikewolfd
---
# Align incoming columns to native table field order

Native Iceberg ParquetWriter writes arrays positionally even when statistics use name matching. Correct generic batch alignment with a zero-copy native-order projection and exact name validation. Preserve view buffers, field IDs, nested semantics and every refusal; unit and actual local native append/readback cover reordered same-typed columns and dates before binding.


## Notes

**2026-10-27T22:44:40Z**

Native order slice complete: 4 alignment and 6 C7 table-job checks, all-target/all-feature Clippy, CODE and ARCH APPROVE. Full same-head composite gate: 402 library/401 integration/15 restore/2 worker/11 maintenance-probe pass, 17 ignored. Broad run had the unchanged 20ms export timing assertion fail under host load; one quiet isolated retry passed (cx_engine_export_timing_quiet.log). No assertion changed.

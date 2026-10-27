---
id: def-60b7
status: in_progress
deps: []
links: []
created: 2026-10-27T21:34:48Z
type: task
priority: 2
assignee: mikewolfd
---
# Align incoming columns to native table field order

Native Iceberg ParquetWriter writes arrays positionally even when statistics use name matching. Correct generic batch alignment with a zero-copy native-order projection and exact name validation. Preserve view buffers, field IDs, nested semantics and every refusal; unit and actual local native append/readback cover reordered same-typed columns and dates before binding.


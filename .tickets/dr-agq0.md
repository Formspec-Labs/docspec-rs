---
id: dr-agq0
status: open
deps: []
links: []
created: 2026-11-13T15:23:34Z
type: task
priority: 3
assignee: mikewolfd
parent: dr-dlb1
tags: [owner-worker, review]
---
# owner-worker: page-level RowSelection and exact decimal parameter literals

SDK4 final review P3-2/P3-3. read_page prunes row groups then slices decoded batches, so a third-party file with one huge row group decodes from row 0 to offset+limit; build a Parquet RowSelection instead (src/owner_worker.rs read_single_parquet_slice). sql_params.rs lowers number parameters with <=15 significant digits to CAST(x AS DOUBLE), which turns Decimal128 + param arithmetic into Float64; lower fixed-point lexemes to DECIMAL(p,s).

## Acceptance Criteria

Mid-file read on a single-row-group file decodes only the selected pages (measured); 0.10 bound to a decimal column keeps Decimal128 type in results.


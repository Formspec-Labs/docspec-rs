---
id: dr-agq0
status: open
deps: []
links: [dr-rtdp]
created: 2026-11-13T15:23:34Z
type: bug
priority: 1
assignee: mikewolfd
parent: dr-dlb1
tags: [owner-worker, review]
---
# Decode only selected Parquet pages for bounded owner reads

Owner role: DocSpec owner-worker reader owner.

User outcome: A mid-file page does not decode all preceding rows in a large row group.

SDK4 review P3-2: use maintained Parquet RowSelection or an equivalent supported narrow reader. Keep this separate from whole-file digest admission in [ddk-eu0k](/Users/mikewolfd/Work/data-design-kit/.tickets/ddk-eu0k.md) and exact decimal parameters in [dr-rtdp](dr-rtdp.md).

Sources: [Cold-read integration](/Users/mikewolfd/Work/data-design-kit/.tickets/ddk-eu0k.md).

Greenfield scope, priorities and evidence: [work backlog](/Users/mikewolfd/Work/data-design-kit/docs/work-backlog.md#greenfield-delivery).

## Acceptance Criteria

- Measure decoded pages/bytes and memory for a mid-file read of a large single-row-group file against independently expected rows.
- Exercise row-group boundaries, multiple shards, nested/exact values, empty pages, cancellation and bounded response sizes.
- Verify actual consumer results and report the measured reduction; returned LIMIT alone is not proof of bounded I/O.

## Reconciliation

November 13, 2026: Decimal scope moved to dr-rtdp; original finding retained below.

### Original finding

# owner-worker: page-level RowSelection and exact decimal parameter literals

SDK4 final review P3-2/P3-3. read_page prunes row groups then slices decoded batches, so a third-party file with one huge row group decodes from row 0 to offset+limit; build a Parquet RowSelection instead (src/owner_worker.rs read_single_parquet_slice). sql_params.rs lowers number parameters with <=15 significant digits to CAST(x AS DOUBLE), which turns Decimal128 + param arithmetic into Float64; lower fixed-point lexemes to DECIMAL(p,s).

## Acceptance Criteria

Mid-file read on a single-row-group file decodes only the selected pages (measured); 0.10 bound to a decimal column keeps Decimal128 type in results.

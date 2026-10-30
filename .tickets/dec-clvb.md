---
id: dec-clvb
status: closed
deps: []
links: []
created: 2026-10-28T03:30:01Z
type: bug
priority: 0
assignee: mikewolfd
---
# Fit portable materialization and export batches to byte allowances

## Acceptance Criteria

Contract first: wide/repeated-view batches split and compact before conversion, preserve exact values/order, each emitted batch fits; single oversized row refuses; unchanged limits; focused checks and independent review.


## Notes

**2026-10-28T04:14:32Z**

Contractsabb3a47/b5a26e0/e62f90e before behavior. Native UNNEST256x1KiB and22-column/null fixture fit unchanged allowances with exact values; exports from3-row/32-row source batches are byte-identical. Portable6unit,10SQLjobs,8exports and all-targetClippy pass. Linear online byte sizing, maintained Arrow slice/compact/cast and bounded canonical coalescing; ignored null view headers cleared before native cast, owned slack shrunk. Independent review next; scalar output contract remains intentionally red pending next fix.

**2026-10-30T20:16:23Z**

Validated on main (4ac305a / ce75e1b): PortableBatches and portable_stream slice batches to fit batch_byte_allowance before write_materialized_batches and export_parquet, refusing single-row oversize with capacity; verified in tests/sql_jobs.rs.

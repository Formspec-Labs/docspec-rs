---
id: dec-eog7
status: closed
deps: []
links: []
created: 2026-10-28T18:30:15Z
type: bug
priority: 1
assignee: mikewolfd
---
# Bound retained SQL memory so native operators can execute

## Acceptance Criteria

192 MiB raw-shaped retention/window/aggregate fixtures pass unchanged limits; retained bytes have separate documented allowance; spill reservations scale; full engine gate and reachable dependent tests pass; independent review resolved.


## Notes

**2026-10-30T20:17:52Z**

Commits cce1de4 and e7f9cfc landed retention_pool (src/budget.rs:114), scaled sort_spill_reservation_bytes (src/budget.rs:172-177), and released retained step memory on last reader completion (src/job/materialized.rs:285-293), passing raw_shaped_retention_leaves_operator_headroom_within_192_mib (2,048 rows) and releasing_dead_materialized_steps_reclaims_memory_and_scratch_before_downstream_work. Kept in_progress solely because retained_wide_steps_fit_192_mib_at_four_times_rows (tests/sql_jobs.rs:104-107, 8,192 rows) still exceeds the 256 MiB scratch ceiling.

**2026-10-30T20:33:55Z**

Added ZSTD(3) compression on materialized scratch spill Parquet writers (src/job/materialized.rs:174), bringing 4x wide-row spills well under the 256 MiB scratch ceiling; retained_wide_steps_fit_192_mib_at_four_times_rows (tests/sql_jobs.rs:79-108) now passes with unchanged 192 MiB memory / 256 MiB scratch limits.

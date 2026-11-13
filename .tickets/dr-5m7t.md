---
id: dr-5m7t
status: closed
deps: [dr-kg49, dr-297r]
links: []
created: 2026-11-09T19:38:50Z
type: task
priority: 2
assignee: mikewolfd
parent: dr-dlb1
tags: [planner, ddk]
---
# Expose job-plan plus an empty-input run as the catalog's SQL gate

User decision 2026-11-09: a DDK recipe or job step passes only when both the sqlglot binder and DocSpec's planner pass it (DDK-CATALOG-011; DDK WP-15 item 3). The trial showed planning alone is not enough: both planners passed a job the optimizer then refused at run time, so the gate is job-plan (per recipe) or prepare-job (whole job) plus one run on empty typed inputs (0.4 s in the trial). DDK already calls job-plan and prepare-job; about 42 lines of glue on its side.

## Acceptance Criteria

A documented, stable CLI contract for job-plan, prepare-job and an empty-input run that DDK can call offline with a pinned binary; diagnostics are machine-readable with codes; the six data-map recipes and the trial's 14 fault cases give the expected results, recorded.


## Notes

**2026-11-13T16:06:11Z**

2026-11-13: done. job-plan, prepare-job and a 0-row typed run are the DDK SQL gate (DDK-CATALOG-011/012, data-design-kit b0299b79 docspec_recipe_gate.py; sqlglot binder retired). Diagnostics carry codes (implicit_cast, invalid_sql, query). The six SpicyRegs recipes plan and the 14 trial fault cases plus dr-7uke gave the expected results on the prototype (6b1b8f1, tagged ddk-sdk4-prototype-6b1b8f1); DDK SDK4 suite passes on main's DataFusion 55 build (1,174 passed). Contract documented in docs/sql-jobs.md.

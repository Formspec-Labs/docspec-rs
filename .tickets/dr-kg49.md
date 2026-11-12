---
id: dr-kg49
status: closed
deps: [dr-z0vd]
links: []
created: 2026-11-09T19:38:50Z
type: task
priority: 1
assignee: mikewolfd
parent: dr-dlb1
tags: [planner, ddk]
---
# Refuse implicit casts in job-plan and prepare-job

Branch feat/implicit-cast-check-20261109 (built by a follow-up session, unreviewed). Makes the planner refuse implicit conversions between text, numbers, dates and booleans. The 2026-11-08 SQL trial (spicy-stack codex/datamap-sql-trial-20261108, datamap/experiments/sql-check/README.md) showed these are the only faults DDK's sqlglot binder caught that the planner missed: an integer=string join silently turned 17,534 rows into 0.

## Acceptance Criteria

Trial cases F3 and F3b fail planning with a clear diagnostic; the six real map recipes still plan; reviewed with greybeard, etl-workflow and evolutionary-refactoring; merged after step A.


## Notes

**2026-11-09T20:19:44Z**

Implemented on feat/implicit-cast-check-20261109 at 3cc56c8 (local, unmerged): job-plan and prepare-job refuse implicit casts between text, numbers, dates and booleans with a new implicit_cast error; trial cases F3 and F3b now fail planning; the six data-map baseline files still plan clean. Remaining: independent review, then merge after step A (dr-z0vd).

**2026-11-12T01:18:00Z**

Independently blind-reviewed with `greybeard`, `etl-workflow`, and `evolutionary-refactoring`; resolved all findings in `ae608e3` (`LogicalPlan::Join` left/right schema typing for semi/anti/`USING` joins, pre-coercion `TryCast` marking for written casts wrapping coerced subqueries and `date + int` math, `time_to_nanos` handling for `DATE`/`TIME` arithmetic, `Job::refusing_implicit_casts` visibility, and `VALUES` documentation) with `ship` across all three lenses, and merged `--no-ff` into `dx/one-line`.

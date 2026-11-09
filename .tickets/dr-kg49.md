---
id: dr-kg49
status: open
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


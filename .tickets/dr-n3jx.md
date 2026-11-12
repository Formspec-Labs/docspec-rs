---
id: dr-n3jx
status: open
deps: [dr-z0vd]
links: []
created: 2026-11-09T19:38:50Z
type: task
priority: 2
assignee: mikewolfd
parent: dr-dlb1
tags: [datafusion, ci]
---
# Step B: upgrade to DataFusion 55 and drop the vendored crate

fix/datafusion-55 (c314014 holds the json_strict.rs and CI workflow fixes from the docspec-df55 worktree). Dry-run merge onto step A conflicts in 6 files; the real work is design: DDK's scratch-file tracking is built on a component DataFusion 55 replaces. Blocked on congressional-tech's test lane (spicy-stack ticket ss-x8fh: 21 failures after the union-type fixes). Step B can change job-plan's inferred UNION types; rerun the SQL gate cases.

## Acceptance Criteria

scripts/check passes; congressional-tech's lane passes on the merged engine; the vendored DataFusion crate is removed; ticket ss-x8fh closed or superseded.


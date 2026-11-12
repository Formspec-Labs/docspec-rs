---
id: dr-z0vd
status: open
deps: []
links: []
created: 2026-11-09T19:38:50Z
type: task
priority: 0
assignee: mikewolfd
parent: dr-dlb1
tags: [engine, ci]
---
# Step A: merge the DDK engine line and the spill fix into main

Branch fix/docspec-one-line-20261109 (f803ee6) = main 5a7fa9f fast-forwarded to codex/ddk-docspec-runtime then merged with fix/spill-view-gc (b0342a7, congressional-tech's pin). CI run 42633258779 failed: 3 engine failures, 33 suspected environment failures. Every consumer's pinned commit ends up inside main, so no app repins for this step. Merge only; never rebase or squash (apps and DDK evidence cite the commits).

## Acceptance Criteria

The 3 engine failures are fixed on the branch; the 33 others are each shown to be environment or fixed; scripts/check passes on the pinned Rust 1.94.0 (not Homebrew's 1.99.1 on PATH); CI green; local main fast-forwarded; push of main approved by the owner separately.


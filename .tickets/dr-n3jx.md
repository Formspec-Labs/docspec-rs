---
id: dr-n3jx
status: closed
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


## Notes

**2026-11-12T22:48:52Z**

2026-11-12 DF55 qualification (no merge, no push): docspec-rs feat/datafusion-55-unified-20261112 be5cf34 (fix/datafusion-55 + main 4963d58) scripts/check green, 1,105 Rust + 31 adapter tests; DDK SDK4 1,174 passed; two-stack slice + viewer passed. CT feat/ct-df55-qualify-20261112 0f2bbbb8 full production-offline lane: 12,987 passed, 7 failed, all one difference: captures receipt_key/receipt_line nullability (DF55 sees through the captures.py:661 locator so the direct path writes not null). Fix needs captures.py (byte-hashed; compatible-versions refuses captures) -> owner decision. Evidence: congressional-tech/.cache/archive-release/df55-qualification-20261112/SUMMARY.md

**2026-11-13T16:06:11Z**

2026-11-13: local main fast-forwarded to 98a2d54 (DataFusion 55.2, Arrow/Parquet 59, datafusion-iceberg 726e4208; vendor/datafusion-physical-plan removed). scripts/check exit 0 at 98a2d54: 1,106 Rust tests passed, 19 ignored, clippy -D warnings clean, 31 Dagster tests OK (peak 8.54 GiB). congressional-tech main pins 98a2d54 (darwin 4c4a94c8, linux ae75bb55) and passed its lane (ct-7t5p closed); hosted linux-release 43367715137 failed one CT test (test_phase2_release_pruning) only because CI checked out CT 6cf02972, fixed on CT main in a2ff03a7. ss-x8fh closed. Push of main is pending owner approval.

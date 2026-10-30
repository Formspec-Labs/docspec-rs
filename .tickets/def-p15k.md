---
id: def-p15k
status: in_progress
deps: []
links: []
created: 2026-10-27T04:40:15Z
type: task
priority: 2
assignee: mikewolfd
tags: [engine-first, M1]
---
# M1: implement the scratch-table Basin measurement probe

## Acceptance Criteria

Probe runs through maintained Iceberg/DataFusion interfaces, local contracts pass, and owner can execute guarded races, repeated maintenance observations, projected scan, cap arms and purge without touching production tables.


## Notes

**2026-10-27T06:04:53Z**

Local implementation qualified: full locked offline cargo test passes (385 lib,348 integration,15 process,2 layout,10 probe;17 ignored live/qualification tests), fmt/clippy and binary/example builds pass. Independent probe/storage reviews approve frozen blobs. Two metadata-accounting regressions found by full gate were corrected and the gate rerun. Safe --plan makes no connection. Live Basin measurement remains owner-gated; CT protocol records exact commands and precommitted decisions.

**2026-10-27T08:57:00Z**

Build-profile contract 97b86f6 and binding df52da3 qualify locally: the full locked offline cargo test passes 385 library, 348 integration, 15 process, two layout and eleven probe tests; 17 remain ignored. Formatting, clippy, builds and the offline plan pass. Independent review cx_m1_build_profile_review.md approves blob 61c924d4. CT protocol f576fc23 requires optimized --release and refuses debug before configuration or I/O. Live Basin remains owner-gated.

**2026-10-30T20:17:52Z**

All local M1 probe contracts and uncapped R2 file-size layout changes (b368968 / 0167510) are on main. Remains in_progress solely for owner-gated live Basin/R2 probe execution.

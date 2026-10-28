---
id: dec-hor2
status: closed
deps: []
links: []
created: 2026-10-28T09:40:58Z
type: task
priority: 2
assignee: mikewolfd
tags: [engine-first, C3]
---
# Expose bounded native scan planning observations

Engine requested per-table file and manifest counts plus scan planning observations for ARCH8.6 maintenance decisions. Reuse native Iceberg facts or actual task streams without another planning pass; unavailable values stay nullable. Do not substitute first-batch latency for planning time.

## Acceptance Criteria

Actual consumer can log truthful available counts/timing with exact native semantics, bounded memory and no duplicate data or plan scan; tests bind public output first; qualify or record precise locked-dependency gap.


## Notes

**2026-10-28T09:45:05Z**

Locked Iceberg0.10.1 to_arrow privately consumes plan_files and exposes only data bytes via ScanMetrics; no maintained planning/manifest observation callback. Timing task-stream consumption includes reader backpressure, so pure planning time remains unavailable. Snapshot summary can supply qualified total-data-files without a second plan. Need exact metadata-only facts or compatible maintained task/planner callback; do not replace native reader configuration for telemetry.

**2026-10-28T11:14:44Z**

Owner updated phase1 scope: truthful selected-metadata data-file and retained-snapshot counts; manifest_count/planning_seconds stayNULL until maintainedhook, manual/inactive maintenance policy belongs engine. Contract3c2ee93RED0.40s; restored ee31fc8/ef5d074 provesold/current exactpins1vs2,empty0,missingoptionalcounterNULL andunavailablepayload. Parsed-provider hook now19tablejobsPASS0.53s/ClippyPASS8.70s; no newHEAD/plan/manifest/dataread andnogrowingobservationregistry. CODEreview/ordinarypinpending. Native planner timing/manifests exactdependency remains explicit.

**2026-10-28T14:26:23Z**

Owner phase1 scope qualified in ad5e4e8: selected-metadata data-file and retained-snapshot counts, manifest_count/planning_secondsNULL with precise lockedIceberg callback dependency.19tablejobs/Clippy/CODEAPPROVE; engine actual three exact native inputs returned1file/1snapshot each in0.260s with unavailable fieldsNULL, no data query or extra planning. Currentc9fullscripts/checkEXIT0 requalifies combined source. Ordinary ad5 binary and normal integration merge/snapshot delivered. Close truthful phase1 observations; maintained planning/manifests hook remains a future dependency, not a fabricated zero/timing.

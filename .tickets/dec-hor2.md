---
id: dec-hor2
status: in_progress
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

---
id: def-j9sr
status: in_progress
deps: []
links: []
created: 2026-10-27T11:23:27Z
type: task
priority: 2
assignee: mikewolfd
tags: [engine-first, C7]
---
# C7: declarative SQL job CLI and library runner

## Acceptance Criteria

One typed job spec declares inputs, SQL steps, exports and scratch; library and CLI execute the same path and return machine-readable results. Local fixture runs SQL service/export contracts without opening lifecycle workspace. Basin M2 workload remains owner-gated by M1.


## Notes

**2026-10-27T12:08:26Z**

Typed JobSpec and local CLI share one bounded service; explicit limits required, no lifecycle workspace. Five CLI/library checks pass, including native JSON schema serde, output identity preservation on later failure/remaining-output retry, immutable views and bounded metadata results. All19SQL-related checks pass. Full locked offline gate385lib/366integration/15restore/2worker/11probe pass,17ignored; fmt andall-target/all-featureClippy pass (cx_c7_*). Remaining C7 plan acceptance: full M2/model export as spec and C5remote job adapter; M1Basin/smallest-runner evidence remains owner-blocked. Local interface is built, ticket remains open for that acceptance.

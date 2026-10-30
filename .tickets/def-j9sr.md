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

**2026-10-28T05:13:18Z**

Ownership transferred to core by owner051000queue decision. Engine integration retains merges/snapshot packaging only; core owns one-context native table/Parquet inputs, named materialized steps/graphs/metadata queries/guarded deterministic multi-file exports and any needed job fixes. Existing C7 spec shapes retained; actual native pins model regression sent with exact job.

**2026-10-28T06:05:29Z**

Owner/enginefreshhead requirement: contract-first readonly resolve operation; batch multiple names in onejob, fresh exactmetadata/UUID/snapshot/schema, typedabsence distinct fromerrors, nativecataloggenerationwrapper preserved, noemptyappend/catalogwrites. Reuse existing catalog authorityfixture with mutation denial, readonlySQLopen forresolve-onlylocal jobs.

**2026-10-30T20:17:52Z**

All Rust CLI/library code and local full-corpus F0 verification of the 7-table congressional-tech model export spec (docs/measurements/engine-first-20261027/f0.md, site/seed-first-full.md, site/final-f0.md) are complete on main (b368968 / 0167510). Remains in_progress solely for owner-gated live Basin M1/M2 workload execution.

---
id: dr-dlb1
status: open
deps: []
links: []
created: 2026-11-09T19:38:50Z
type: epic
priority: 1
assignee: mikewolfd
tags: [ddk, engine]
---
# One engine line, and DDK reads DocSpec jobs as build records

Umbrella for the DocSpec work created by the 2026-11-08/09 DDK decisions (data-design-kit ADR-015: DDK is the evidence-bound catalog; transformations are SQL files run as DocSpec job steps). Four engine lines exist and congressional-tech pins a commit on none of them; DDK needs job identity and run binding to show build records; DocSpec's planner becomes the SQL gate. Plan: docs/line-reconciliation-20261108.md on codex/docspec-line-reconciliation-20261108. Not DocSpec's: incremental rebuilds, query logging, entity matching.

## Acceptance Criteria

Local and origin main hold one line that every consumer pins; DDK attaches run evidence to a SpicyRegs dataset through a DocSpec job; the one-vote proof passes against that engine.


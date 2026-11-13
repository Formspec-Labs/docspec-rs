---
id: dr-dlb1
status: closed
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


## Notes

**2026-11-13T16:06:11Z**

2026-11-13: one line: local main = 98a2d54 contains every consumer pin (CT 98a2d54; DDK re-pinned to a main build, ss-co08). Remaining for this epic: push main (owner approval) and rerun the one-vote proof against it.

**2026-11-13T16:26:26Z**

2026-11-13: done. origin/main = 6a45723 (DataFusion 55.2 line at 98a2d54 plus tickets); congressional-tech pins 98a2d54 and DDK pins a content-addressed main build (b3a5d49d), so every consumer is on one line. DDK attaches DocSpec job run evidence to SpicyRegs OCC datasets (vertical slice, kind job productions, DDK-CATALOG-012). One-vote proof, engine half: the roll-99 / P000197 job on retained generation e7cdd80d returns Pelosi No -> nay, 214/210/8, one source row, tallies reconcile, broken filter and broken normalization caught, duplicate-key export refused - identical on b0342a7 and 98a2d54 (artifacts/one-vote-proof-20261113/PROOF.json). The receipt-to-capture pointer is spicy-regs work: ss-3u5d.

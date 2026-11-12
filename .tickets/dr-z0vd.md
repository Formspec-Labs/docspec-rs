---
id: dr-z0vd
status: closed
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

## Notes

**2026-11-12T01:18:00Z**

Consolidated onto `dx/one-line` via `--no-ff` merges preserving every consumer-pinned commit (`5a7fa9f`, `501503c`, `b0342a7`, `b1270b2`, `d96e001`, `255c184`, `62265f9`, `f803ee6`, `1718b49`, `ae12bbd`, `8db7ebb`, `bafd5c4`, `3cc56c8`, `ae608e3`, `1d1dc2b`):
- Hosted CI run `42994498288` on `ae12bbd` (`fix/docspec-dx-one-line-20261111`) passed `build` + 6/6 `production-offline` shards against Congressional Tech `main` (`c87b3d5a`), and CT repinned live to `ae12bbd` (`21fc049940bb`).
- Merged `dx/managed-field-ids` (`8db7ebb`, Stage 1a `ct-exlx`), `dx/add-columns` (`bafd5c4`, `X9` verified 10/10 X1 and 28/28 W12 byte-identical to `ae12bbd`), `feat/implicit-cast-check-20261109` (`ae608e3`, `dr-kg49`), `codex/docspec-line-reconciliation-20261108` (`1d1dc2b`), and `main` (`501503c`).
- Owner approved `scripts/check` + hosted CI qualification (`fix/docspec-one-line-20261111-final`) + fast-forwarding `main` and pushing `origin/main`.


---
id: dec-imat
status: in_progress
deps: []
links: []
created: 2026-10-28T10:19:25Z
type: feature
priority: 1
assignee: mikewolfd
tags: [engine-first, D1]
---
# Accept per-row JSON path lists in one-pass extraction

Lead giant-lines finding: preserve one native JSON parser traversal for all caller-supplied targets of a retained record. Extend existing json_get_many with per-row path definitions while preserving literal specialization, Arrow string representations, path validation, output positions and all admission limits. Raw owns SQL/grain and pointer selection.

## Acceptance Criteria

Contract-first dynamic path/representation/null/invalid/byte-fitting checks pass; parser and builders reused with pre-allocation admission and no growing definition cache; focused checks/review/ordinary executable delivered. No application field names or parsing policy.


## Notes

**2026-10-28T10:43:22Z**

Contracts33b7646 andc3f7283 fail on literal-only restriction before implementation; isolated CLI-allocator process contract2648634 precedes behavior. Final20functionsPASS3.86s/ClippyPASS25.88s. Dynamicpath >2GiBlogical/roughly1MiB-backed regressionPASS7.74s peakRSS55017472B/physical9667112B underunchanged128MiB+64MiB overhead. Existing literal specialization, path validation/Jiter traversal, representation signature and output-buffer admission retained. Independent review and ordinary pin follow.

**2026-10-28T11:03:11Z**

Review F1 reproduced90f9296/5d34c62 RED0.04s: preceding output/workspace prevents a later row path trie reservation even both rows fit separately. Retryable multi-row reservation errors now produce typed fit hints; one-row capacity and invalid-definition errors remain refusals. Final3JSON unitsPASS0.01s/20functionsPASS4.48s/ClippyPASS24.27s. Observation WIP preserved aside and its independent red contracts reverted in9e6dd1a to qualify a clean rebuild-only prefix; restore after runtime pin.

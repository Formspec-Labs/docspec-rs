---
id: def-p9vn
status: closed
deps: []
links: []
created: 2026-10-27T13:57:56Z
type: task
priority: 1
assignee: mikewolfd
tags: [engine-first, C0]
---
# C0: remove production-only lifecycle shortcuts

## Acceptance Criteria

Ordinary and test builds use the same guarded lifecycle commit and native pin read semantics; no dataset-name shortcut skips remote admission. Archive jobs remain independent of retained lifecycle fault machinery. Focused recovery and native job tests plus a remote-only build pass; M1-dependent caps and cross-platform digests remain explicit gates.


## Notes

**2026-10-27T20:42:31Z**

C0 archive-facing slice complete a120e00: ordinary remote-only and all-feature six-table groups pass; redcontractc29fca8 load1vs2 reproduced beforefix. Native pin metadata path alreadyuniformfromC5. CODE and ARCH APPROVE cx_c0_guard_finish_*. Hoistedoneexactprecommitcheckremovesfeaturebranch; retained lifecycle-only caching/fault hooks serveotherconsumers perBIND. M1caps/server-CAS/Linuxformat qualificationremainowner gated, notclaimed.

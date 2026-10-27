---
id: def-mpjb
status: closed
deps: []
links: []
created: 2026-10-27T20:53:08Z
type: task
priority: 1
assignee: mikewolfd
---
# Parse nullable HTTP dates in native SQL

Generic C8 http_date(text) uses maintained locked parser and existing budget plumbing. UTCmicrosecond output, null invalid/unsupported, supported3HTTPformats and documented1970..9999range. No application retry policy. Done after native vectors/capacity checks and code/architecture reviews.


## Notes

**2026-10-27T21:19:31Z**

Nine native SQL-function checks pass, including three HTTP wire forms, nullable invalid/range values and all Arrow text representations. Clippy all-target/all-feature passes. CODE and ARCH APPROVE in codex-impl/cx_http_date_{review,arch_review}/review.md. Generic parsing only; D1 application policy and adoption owned by raw. Snapshot engine-3746f22 shared.

---
id: def-mpjb
status: in_progress
deps: []
links: []
created: 2026-10-27T20:53:08Z
type: task
priority: 1
assignee: mikewolfd
---
# Parse nullable HTTP dates in native SQL

Generic C8 http_date(text) uses maintained locked parser and existing budget plumbing. UTCmicrosecond output, null invalid/unsupported, supported3HTTPformats and documented1970..9999range. No application retry policy. Done after native vectors/capacity checks and code/architecture reviews.


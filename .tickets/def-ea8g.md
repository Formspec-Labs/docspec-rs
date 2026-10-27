---
id: def-ea8g
status: in_progress
deps: []
links: []
created: 2026-10-27T22:46:14Z
type: task
priority: 2
assignee: mikewolfd
---
# Read exact native table selectors returned by jobs

C1/C7 seam: add a generic immutable native_table JobInput from metadata URI, UUID, snapshot and schema. Read metadata once with caps and explicit FileIO, reuse the native static provider and retain metadata admission through frames/plans/streams. No lifecycle inventory, catalog lookup, domain rules or network writes. Contracts before binding; local and fake remote checks plus static reviews.


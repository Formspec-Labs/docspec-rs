---
id: def-sg14
status: closed
deps: []
links: []
created: 2026-10-27T19:24:09Z
type: task
priority: 1
assignee: mikewolfd
tags: [engine-first, C5]
---
# C5: single create-only release-file PUT with native MD5

## Acceptance Criteria

Known local file identities upload with one create-only PUT and Content-MD5, without multipart/HEAD/GET/readback; whole-buffer and cancellation memory are admitted before allocation. 412 returns already_exists without verification claim, changed source and capacity refuse. Generic library plus thin CLI reuse one path; existing multipart consumers stay intact.


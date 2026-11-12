---
id: dr-zlec
status: open
deps: [dr-z0vd, dr-n3jx]
links: []
created: 2026-11-09T19:38:51Z
type: task
priority: 2
assignee: mikewolfd
parent: dr-dlb1
tags: [release]
---
# Publish the accepted engine build for congressional-tech's cutover

Repinning congressional-tech is a cutover, not a file edit: a saved publication intent refuses a different engine, and the capture scheduler refuses until a model publish has run on the new engine. DocSpec's part: an accepted main commit with its Linux and macOS binaries, hashes and a release note; the repin itself is a congressional-tech ticket to file against model/docspec-runtime-pin.json, scheduled between nightlies (see memory: CT Linux engine refresh).

## Acceptance Criteria

Accepted commit, binaries and SHA-256s published; congressional-tech ticket filed with the cutover steps; no pin changed from here.


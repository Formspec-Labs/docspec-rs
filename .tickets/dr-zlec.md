---
id: dr-zlec
status: closed
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

## Notes

**2026-11-12T15:21:37Z**

Closed: unified Wave 1 + Wave 2 engine commit `e3d9092a6393e654efe221d662a76daf8fd56ca5` passed `linux-release.yml` 7/7 (`43238636122`) and fast-forwarded onto `Formspec-Labs/docspec-rs` `origin/main`, with production binaries (`--features remote`, no `test-util`) verified for `linux-amd64` (`3e7e1b7412eb214e69b9b8007b8b021aa1ec84a5a10279723f900e9b12e9b92c`) and `darwin-arm64` (`86255b5f851d53992e6ae81465ed575bee266da4557389c69aedc3abc52715cc`) and handed off to `congressional-tech` Wave 2 (`claude/ct-wave-2-pin2`).


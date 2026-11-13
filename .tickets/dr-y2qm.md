---
id: dr-y2qm
status: open
deps: []
links: []
created: 2026-11-13T15:23:34Z
type: task
priority: 3
assignee: mikewolfd
parent: dr-dlb1
tags: [engine, managed-job]
---
# Pass large whole-job completion witnesses by object reference

Whole-job completion witnesses are base64-nested inside JSON envelopes; large jobs approach the 32 MiB stdout cap (RG-7 / F8). Return an ObjectRef the caller reads instead.

## Acceptance Criteria

A job whose witness exceeds the stdout cap completes and the caller verifies the witness by sha256 through the reference.


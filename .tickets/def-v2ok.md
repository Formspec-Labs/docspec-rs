---
id: def-v2ok
status: closed
deps: []
links: []
created: 2026-10-27T14:51:22Z
type: task
priority: 1
assignee: mikewolfd
tags: [engine-first, C8]
---
# C8: generic URL, filename and compact JSON functions

## Acceptance Criteria

SQL jobs expose WHATWG URL resolution, percent normalization/strict decoding, RFC6266 download filename and compact json_array with maintained JSON getters. Pinned upstream resolver corpus and existing/normative filename and Python serialization vectors pass or identify exact upstream deviations; output expansion is bounded and application rules stay outside the crate.


## Notes

**2026-10-27T16:28:19Z**

Generic primitive acceptance is met: five focused tests pass, including the full 819-case resolver corpus with only the 36 named upstream file-URL deviations and the non-scalar capacity refusal. All-target/all-feature Clippy passes. CODE and ARCH reviews approve after the two allocation findings were fixed. PLAN full-model/M2 throughput acceptance remains outside this scoped ticket.

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


## Notes

**2026-10-27T20:10:07Z**

Closed locally:20454f9/4232caa/4aa75aa contracts, c394517 binding;12transfer/8CLI native fixtures pass, CT12client fixtures pass and Clippy passes. CODE and ARCH APPROVE cx_c5_upload_*. Whole-file transfer is admitted, create-only nativeMD5, no readback or multipart; whole-process RSS/largest-release check and publisher integration remain plan-level work.

**2026-10-27T20:56:06Z**

Largest-transfer extension: existing9MiB case retained; added50,331,665B case above measured F0largest48,923,028B. One conditionalMD5PUT, exactbytes, zero multipart/GET/HEAD in eachcase. Warm cargo-native /usr/bin/time runner receipt cx_c5_largest_memory.log: maxRSS215,089,152B, peakfootprint190,284,520B with256MiBconfiguredBudget; includesfixture/sourcebytes, no model/live/runner-throughput claim. Defaultotherfixturebodycap remains3CHUNK.

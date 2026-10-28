---
id: def-tvun
status: in_progress
deps: []
links: []
created: 2026-10-27T11:06:57Z
type: task
priority: 2
assignee: mikewolfd
tags: [engine-first, C3]
---
# C3: deterministic SQL result export with explicit writer profiles

## Acceptance Criteria

Native export applies caller-supplied order and per-file writer settings, writes fixed row groups and required indexes, hashes SHA256 and MD5 during writing, and gives byte-equal reruns/input permutations on pinned limits. Cross-runner acceptance remains phase2; range request evidence follows reader binding.


## Notes

**2026-10-27T11:35:42Z**

Native writer implemented;7export contracts pass, including ties across fixed batches, volatile-query refusal, hidden ordering fields, independent nested leaf encodings, managed output quota, streaming hashes and byte equality.14existing native-storage tests and6SQL job tests pass; all-target/all-featureClippy and fmt pass. Reused existing immutable-function and writer-metadata accounting helpers. Range-cache round-trip consumer acceptance remains for track A; no cross-runner or live-storage claim.

**2026-10-27T13:07:06Z**

F0 exposed logical/physical string-view drift. Contract8e64a70 first failed on exported Utf8View; shared portable conversion normalizes view arrays before materialization/coalescing, accounts repeated logical value lengths before allocation, and replaces old duplicated scalar view mapping.8writer/6job/5CLI/5existing-export tests andClippy pass (cx_c3_portable_*).

**2026-10-28T05:13:18Z**

Ownership transferred to core by owner051000queue decision. Engine integration retains merges/snapshot packaging only; core owns further deterministic model export fixes and exact native-table slice acceptance. Current same-model Parquet sliceac866passes all public bytes; native table slice fails at source_annotations materialization with executed batch type changed, exact job shared in codex-impl/cx_vertical_real_v2/model/job.json.

**2026-10-28T07:17:49Z**

Native-pin slice/D2 replay exposed Binary versus FixedSizeBinary(32) and nested list child-nullability drift. Contract-first tests require exact values and declared types, strict invalid-width/NULL/unrelated-type refusal, and target-byte fitting for null-heavy fixed storage plus variable offsets. Current portable unit lane: 7 pass, 3 fail at existing type guard; evidence cxc_run4_type_contract_red2.log. No application SQL cast or allowance increase.

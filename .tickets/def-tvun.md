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

**2026-10-28T08:07:03Z**

b7576e6/4df787b contracts first; strict maintained Arrow conversion and declared-target byte sizing pass final 11 portable checks, all-target/all-feature Clippy, and exact retained D2 replay. Initial 69 SQL checks pass before minor no-op cleanup. Native model completes source_annotations and all later named steps, then fails first export on DataFusion physical/logical PARQUET:field_id metadata drift; no accepted model exports. Type investigation reached 45 minutes; prioritize the independent full D1 physical-footprint bottleneck, then return to the exact metadata failure.

**2026-10-28T08:58:28Z**

Metadata contract first: two real local native tables with field IDs 1 and 2, plus NULL branches, retain exact row multisets and declared batch schemas. Current focused lane fails both tests: aggregate physical/logical PARQUET metadata disagreement and fabricated union field identity. Upstream PR21127 fixes only empty metadata; the retained daily fixture has actual conflicting IDs (captures SHA16 versus readings SHA1), requiring metadata reconciliation without stripping input IDs or disabling validation.

**2026-10-28T09:37:52Z**

Metadata contracts 7e13d61/215e35c precede 30255bf. Missing/conflicting native UNION field IDs and TRY_CAST source-tag leakage now pass seven native selectors/contracts, three zero-copy/schema units and all-target/all-feature Clippy. Native model replay with rebuilt CLI and independent review remain pending; no final acceptance claim.

**2026-10-28T09:39:38Z**

30255bf metadata behavior plus C1 fixtures48ec2f2: rebuilt ordinary remote-only CLI completes25 native-table model outputs, list29, all invariant/completeness guards zero; 4.93s, peak RSS418349056B, footprint301417600B. Exact cxc_run4_native_model_meta2 evidence retained. Candidate48ec2f2 SHA427b772aabb62cccafb3b5005a46a900b7678d31062981883da82a6fe97a4f52 handed engine/raw/release; independent metadata review and final full gate active. Native consumer qualification remains separate.

**2026-10-28T09:52:42Z**

Candidate48 full scripts/check PASS:429lib/423integration/15restore/1processmemory/2worker/11probe/31Dagster,17existing skips. Seven actual daily cases expose stale ancestor schemas after child UNION metadata changes; contract59af50e reproduces physical{} versus logical fieldID2. Native rewrite_with_subqueries and selective native recompute_schema now pass73 SQL checks/8native matrix cases and all-target Clippy; actual daily rebuilt replay and follow-up review pending.

**2026-10-28T10:02:22Z**

Review F1 confirmed by27a4511: native TypeCoercion creates merged UNION tags inside a scalar subquery, and unchanged outer relational input leaves stale outer metadata. Observe direct child and embedded subquery schemas through native apply_subqueries before selective reconstruction. Final4metadata unitsPASS0.03s,74SQLPASS21.45s and ClippyPASS; e812 rebuilt retained daily fixture yields1workitem/0admissions/allconflictguards0. Final CLI/review/fullgate follow.

---
id: def-ea8g
status: closed
deps: []
links: []
created: 2026-10-27T22:46:14Z
type: task
priority: 2
assignee: mikewolfd
---
# Read exact native table selectors returned by jobs

C1/C7 seam: add a generic immutable native_table JobInput from metadata URI, UUID, snapshot and schema. Read metadata once with caps and explicit FileIO, reuse the native static provider and retain metadata admission through frames/plans/streams. No lifecycle inventory, catalog lookup, domain rules or network writes. Contracts before binding; local and fake remote checks plus static reviews.


## Notes

**2026-10-27T22:57:35Z**

Exact C7 selector slice complete0500cc1/01afa5d/0c5545a. Nine C7 job fixtures,20selected library checks incl populated remote/strict caps,1provider-frame-plan-stream admission fixture and all-target/all-feature Clippy pass. CODE/ARCH APPROVE no blockers/concerns. Error/cancel and malformed remote response branches traced but not directly new-fixture exercised; no whole RSS/live qualification. Ordinary snapshot engine0c5545a shared; preceding full composite gate retained separately.

**2026-10-27T23:57:53Z**

Full gate exposed1newsmall-pool failure after407library/403integration passes. Reopened pending final full rerun. Observed plain admissionb6516df and gzip/catalog correction51f952b resolve focusedregression;8SQLjobs/10C7/7metadata pass and finalCODE/ARCHAPPROVE. No assertion or limit changed; final full engine gate will run with current C8group.

**2026-10-28T00:38:37Z**

Final source48636e5 gate:409library passed/15ignored (cx_engine_final_json_many_full.log before model-capacity interruption); restarted integration411passed/2ignored (cx_engine_json_many_integration_resume.log);15restore/2worker/11probe pass (cx_engine_json_many_remaining.log); fmt and prior all-target/all-featureClippy(cx_json_many_depth_clippy_final.log) pass. Ordinary remote-only snapshot48636e5 built and shared; no corpus/Linux claim. C6RSSescape tracked separatelydef-h6g9 by owner decision.

---
id: dec-e67y
status: closed
deps: []
links: []
created: 2026-10-28T02:56:16Z
type: bug
priority: 0
assignee: mikewolfd
---
# Preserve string comparison types across once-worklist execution

D2 synthetic retained repro fails Utf8 == Utf8View during output worklist query. Trace generic native planning/reading type consistency; fix smallest shared engine cause without application casts. Done when cheap generic regression and exact synthetic repro pass with one independent review.


## Notes

**2026-10-28T03:06:07Z**

Generic regression now reproduces Utf8 == LargeUtf8 in0.19s: named UNION begins with Utf8, later input widens it; outer aggregate filter plus correlated NOT EXISTS over an inner join produces inferred literal filters with stale outer-reference types. Exact D2 fake job independently reproduces Utf8 == Utf8View. Simpler cases retained as controls.

**2026-10-28T03:08:27Z**

Maintained TypeCoercion after native decorrelation fixes generic36-query matrix0.22s and exact retained D2 fake worklist0.39s (1work row/1admission row). Focused SQLjobs8PASS21.43s; all-targetClippy/format pass. No optimizer disabled and no application SQL/policy/limit changed. Independent review and final gate pending.

**2026-10-28T03:22:03Z**

Closed after independent semi-formal review APPROVE, generic36-query mixed-string/null matrix, exact retained D2 fake worklist on all-feature and ordinary remote-only builds, SQLjobs8PASS, and final scripts/check PASS:417lib/412integration/15restore/1processmemory/2worker/11probe plus31Dagster,17existing skips. Source code pinned9b59263; later commits are ticket metadata only. No application workaround or optimizer disable.

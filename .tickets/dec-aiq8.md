---
id: dec-aiq8
status: in_progress
deps: []
links: []
created: 2026-10-28T02:34:42Z
type: bug
priority: 1
assignee: mikewolfd
---
# Guard scratch writes before physical I/O

## Acceptance Criteria

Migration, ordinary and footer writes refuse before crossing remaining capacity; cleanup releases accounting; reviewed.


## Notes

**2026-10-28T02:45:51Z**

Review confirmed concurrent outer accounting update gap; removed in this follow-up. Original204b967 body had materialized.rs229 after; actual232 before this repair, now228.

**2026-10-28T03:14:06Z**

Independent review-storage/review.md REQUEST CHANGES: DocSpec guarded writes are correct in isolation, but native DataFusion spills bypass mutex and use post-write subtract-then-add accounting; mixed concurrent writes can observe an understated counter and exceed total cap. Global pre-write scratch guarantee remains OPEN. Blocked: owner must assign/authorize common native spill pre-write/accounting primitive or qualified independent allocation design before closure. Exact interleaving and dependency source paths sent engine. Do not treat existing native-spill-overrun fixture as this proof.

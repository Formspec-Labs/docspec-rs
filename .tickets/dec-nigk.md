---
id: dec-nigk
status: closed
deps: []
links: []
created: 2026-10-28T02:32:05Z
type: bug
priority: 0
assignee: mikewolfd
---
# Prevent Arrow byte-offset overflow in SQL JSON evaluation

## Acceptance Criteria

Cheap logical expansion regression; narrow D1 reproducer completes or reports bounded capacity without panic; independent review.


## Notes

**2026-10-28T02:49:23Z**

Regression3423d08 now passes: 2048 repeated views (>2GiB logical, <2MiB backing) narrow without Utf8 cast; 15 SQL-function integration tests pass. Exact combinations preserve all mixed Utf8/LargeUtf8/Utf8View inputs before admission. Real audit and independent review next.

**2026-10-28T02:55:52Z**

Independent review found untyped NULL companion fallback. Regression28e8fc2 failed on injected Utf8 cast; include exact Null signatures and handle NullArray explicitly. Guardtests3PASS4.78s and SQLfunctions15PASS4.01s. Original official audit now bounded refusal63.22s/maxRSS7105298432B, no Arrow panic, no acceptedcount.

**2026-10-28T03:10:26Z**

Closed after independent string review and NULL follow-up APPROVE, >2GiB logical view and all representation/NULL regressions, SQLfunctions15PASS, complete scripts/check pass, and official audit proper capacity refusal without panic at7.11GB under14GiB. No accepted official source-line count claimed.

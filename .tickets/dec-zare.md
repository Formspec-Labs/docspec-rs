---
id: dec-zare
status: closed
deps: []
links: []
created: 2026-10-28T02:33:00Z
type: bug
priority: 1
assignee: mikewolfd
---
# Preserve conditional PUT conflicts as failures

## Acceptance Criteria

409 fails; 412 already_exists; each one PUT without probes; semi-formal review.


## Notes

**2026-10-28T03:10:25Z**

Closed after static semi-formal review APPROVE including HTTP304 follow-up, focused socket409/412/304 fixtures, and complete scripts/check pass. Exactly one conditional PUT per tested outcome; no probes or network writes during qualification.

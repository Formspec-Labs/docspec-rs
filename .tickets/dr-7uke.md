---
id: dr-7uke
status: closed
deps: [dr-n3jx]
links: []
created: 2026-11-09T19:38:50Z
type: bug
priority: 2
assignee: mikewolfd
parent: dr-dlb1
tags: [datafusion]
---
# Reproduce and fix the push_down_leaf_projections failure on JSON-derived view columns

DataFusion 53.1: selecting a json_get_str-derived column of a view under a filter (SELECT fr_document_number FROM rulemaking_timeline WHERE rulemaking_id = 'x') fails in rule push_down_leaf_projections with 'ambiguous __datafusion_extracted_7'. SELECT * and count(*) work; materializing the view avoids it. Both planners pass the job; only execution fails. A follow-up session may hold a reproduction (reproduce-docspec-push-d).

## Acceptance Criteria

A DocSpec test reproduces it on 53.1; the test passes on the DataFusion 55 branch or a workaround is applied; the data map's timeline view no longer needs materialization for this reason.


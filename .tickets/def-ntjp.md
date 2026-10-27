---
id: def-ntjp
status: in_progress
deps: []
links: []
created: 2026-10-27T18:28:51Z
type: task
priority: 1
assignee: mikewolfd
tags: [engine-first, C8]
---
# C8: canonical sorted JSON text for generic SQL identities

## Acceptance Criteria

json_canonical(text) recursively sorts object keys by UTF-8 bytes and emits Python-compatible compact UTF-8 JSON, preserves integer precision, refuses nonfinite/malformed input and preadmits parsing/output. Golden vectors and capacity refusal pass; record selection and hashing policy remain in application SQL.


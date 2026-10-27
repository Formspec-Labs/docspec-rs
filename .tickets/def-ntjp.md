---
id: def-ntjp
status: closed
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


## Notes

**2026-10-27T18:38:09Z**

Seven focused SQL-function checks pass, including Python canonical numeric/private-key vectors and valid cumulative-output capacity refusal. All-target/all-feature Clippy passed before the small error-category fix. ARCH APPROVE; CODE sole concern reproduced by committed fixture and fixed with retained OutOfMemory classification. Maintained Jiter/Serde/Ryu, no application record/hash policy. Full D1 corpus/RSS remains separate.

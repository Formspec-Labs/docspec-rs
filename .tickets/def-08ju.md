---
id: def-08ju
status: in_progress
deps: []
links: []
created: 2026-10-27T23:18:16Z
type: task
priority: 1
assignee: mikewolfd
---
# Project disjoint JSON fragments in one native pass

C8 critical D1 seam: immutable json_get_many(raw,literal_paths_json), positional raw JSON fragments equal to independent json_get_json. Compile literal paths once, reuse maintained Jiter and native arrays, bounded admission, no application metadata rules or second parser. Prefix-disjoint paths and duplicates supported; overlapping ancestors refuse explicitly. Contract fixtures before binding; raw downstream typed metadata remains application SQL.


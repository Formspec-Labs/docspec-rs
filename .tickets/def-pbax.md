---
id: def-pbax
status: in_progress
deps: []
links: []
created: 2026-10-27T21:28:52Z
type: task
priority: 2
assignee: mikewolfd
---
# Use blocking accepted sockets in the removal transport fixture

Full and isolated native gates reproduce an empty HTTP request on macOS because accepted sockets inherit nonblocking mode. Match the neighboring established transport fixture and preserve all method-count, refusal and removal assertions. Done when the unchanged removal group and full Cargo gate pass.


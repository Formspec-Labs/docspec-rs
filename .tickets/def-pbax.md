---
id: def-pbax
status: closed
deps: []
links: []
created: 2026-10-27T21:28:52Z
type: task
priority: 2
assignee: mikewolfd
---
# Use blocking accepted sockets in the removal transport fixture

Full and isolated native gates reproduce an empty HTTP request on macOS because accepted sockets inherit nonblocking mode. Match the neighboring established transport fixture and preserve all method-count, refusal and removal assertions. Done when the unchanged removal group and full Cargo gate pass.


## Notes

**2026-10-27T22:44:40Z**

Two focused removal cases pass with unchanged assertions; CODE APPROVE. Same-head full composite gate completed 402 library/401 integration/15 restore/2 worker/11 probe,17 ignored; broad gate timing-only unrelated export failure passed once isolated at the natural quiet boundary. Socket fix does not affect production.

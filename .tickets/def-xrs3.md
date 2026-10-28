---
id: def-xrs3
status: in_progress
deps: []
links: []
created: 2026-10-28T01:20:05Z
type: bug
priority: 0
assignee: mikewolfd
---
# Restrict every SQL job entry before side effects

Owner review B2: reuse Job::sql with maintained SQLOptions disabling DDL, DML and statements, then route materialization through that one entry. COPY, CREATE and SET must refuse before file/table/session effects, including append identity_sql with nonempty work; relational SELECT remains valid. Contract tests precede binding; focused gate and CODE/ARCH reviews required.


---
id: def-xrs3
status: closed
deps: []
links: []
created: 2026-10-28T01:20:05Z
type: bug
priority: 0
assignee: mikewolfd
---
# Restrict every SQL job entry before side effects

Owner review B2: reuse Job::sql with maintained SQLOptions disabling DDL, DML and statements, then route materialization through that one entry. COPY, CREATE and SET must refuse before file/table/session effects, including append identity_sql with nonempty work; relational SELECT remains valid. Contract tests precede binding; focused gate and CODE/ARCH reviews required.


## Notes

**2026-10-28T01:37:02Z**

B2 fixed88c81c7/bace462. COPY direct/materialization no-effect unit and append identity regression were red beforefix. Corrected unit passes;61focusedSQLintegrationcasespass25.55s inclnonemptyappendidentity; all-target/all-featureClippy22.92spass. CODEAPPROVE(cx_b2_sql_code/review.md),ARCHAPPROVE(cx_b2_sql_arch/review.md), nofindings. Fullnativegatewillrunafterremainingengineconcerns, previoussource486gate848passed/17ignored.

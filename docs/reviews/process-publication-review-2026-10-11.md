# Independent process-publication review

**Static verdict: approve** `/tmp/docspec-process-publication.patch`, SHA256 `7c0284f7fb8836cc5c5cfb2da6cfcaa62281b492b1ddcf47c8b2cccc84d1c41f`. No actionable finding. No build, provider request or runtime performed for this review.

The parent still creates the tiny real source/update and exact durable export materialized/validation records before racing final selection. Each child opens a separate host and configured catalog client, reuses those completions, and arms its catalog wrapper only after startup. The wrapper captures the maintained lifecycle load's metadata location, holds the original TableCommit at update_table, then forwards that unchanged commit including native requirements. It does not manufacture a success/conflict or rewrite the expected catalog head. Both immutable metadata rendezvous records must match before either commit advances; atomic persist_noclobber prevents a partial control record from passing the peer. Returned reports independently bind child PID and captured head; parent asserts distinct processes, exactly one success, exact winning prior materialized/validation pin, co-committed result/channel native revision, absent losing result, unchanged retained completions, completed winning retry and stale losing export_conflict.

Child execution is the exact ignored child test in current_exe, with explicit fresh UUID-derived prefix and dataset checks; normal provider test cannot recursively run. Parent bounds process lifetime to120s, peer hold has its existing bounded deadline, and RAII kills/reaps still-running children on failure. Control files/logs remain retained. No secret/header/body is intentionally recorded by the new rendezvous/report, and signing is unchanged.

Scope: same-head final native catalog publication by independent OS processes with already-completed immutable output; cold readback and existing retry assertions remain. This does not establish old-worker credential fencing, provider authority handoff, upload cancellation/retransmission counts, or a destructive restore route. Same-head equality proves the wrapper's captured native load boundary, not every earlier read or a universal provider isolation property. Runtime/provider evidence remains root-owned.

## Actual provider qualification

The [provider execution](process-publication-provider-2026-10-11.stdout.log) passed
with two independently configured child OS processes holding the same native
lifecycle head. Exactly one selected its completed export; the other retained
its exact materialization and verification. Completed winning retry, stale losing
retry, exact cold old/new/losing output reads and shared channel/result native
revision assertions passed. The tiny control records and child logs are retained
under `process-publication-provider-2026-10-11/processes/`. The unchanged local
control passed as well. This is one computer with independent process/host
workspaces, not physical machine loss or destructive authority qualification.

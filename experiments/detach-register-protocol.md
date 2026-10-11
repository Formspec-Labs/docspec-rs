# Empty-table detach/register capability probe

Decision: after Basin rejected simultaneous registration of one UUID, does maintained non-purging detachment allow exact metadata registration under a new name, and does the old identifier reject a captured native commit? This is a necessary capability for explicit quiescent restore, not a full restore or obsolete-credential qualification.

Runner `/tmp/docspec-detach-register-probe.py`, SHA256 `fed633f9080e8b7d3c1fb99422d61584220b009c45a0e32ca9417370425427c7`. Inspected against the existing sibling environment's PyIceberg 0.12.0 source; Python AST parsed only, no execution by author. Public maintained REST methods handle configuration, auth, request serialization, registration and commit. Public Tenacity `retry_with(stop_after_attempt(1))` bounds decorated API calls to one attempt; transaction retry is not used. No production adapter is changed.

Root execution: after active measurements stop, pass the existing catalog URI/warehouse/token environment to the installed `../spicy-regs/.venv/bin/python`, with a fresh absolute evidence directory and fresh canonical UUID:

```text
../spicy-regs/.venv/bin/python /tmp/docspec-detach-register-probe.py /absolute/fresh/evidence <fresh-canonical-uuid>
```

The runner creates two fresh `docspec_detach_probe_<UUID>` namespaces and exactly one empty V2 table in its explicit `s3://spicy-regs/__r2_data_catalog/docspec-qualification/docspec-rs-qualification-<UUID>/detach-probe` location. The schema has nullable `counter` and `text`, no snapshots or data files. Table properties disable native GC, previous metadata deletion and commit retry. No prior namespace/object/evidence is changed. The provider writes a small native metadata object as part of this new table creation; do not call this a zero-write run.

The runner captures exact native URI, UUID, location, field identities and canonical metadata digest. It calls `drop_table(..., purge_requested=False)` only for the fresh candidate, requires old registration404, registers the exact captured URI at the other fresh namespace once, then calls native `commit_table` with the old Table handle, `AssertTableUUID` and one property update. Only HTTP404 qualifies rejection. A fresh maintained catalog client loads the restored registration; native identity and full parsed metadata digest must remain exact. No S3 object DELETE, append, prefix list or cleanup occurs. Original registration is deliberately detached; the new registration and all bytes remain.

Every call is recorded before it begins. Each step has a 25-second deadline; total workflow budget is 240 seconds (root can supervise at 260). PyIceberg controls response decoding; the runner checks decoded native metadata size at 1 MiB and does not claim a pre-decode wire-memory cap. Fixed empty-table scope provides bounded intended work; no throughput/resource qualification is claimed. Only status, exception class, identifiers, schema and metadata identities are persisted. Raw provider messages, config and credentials are not logged.

Stop after any unexpected failure or ambiguous response. Do not retry a mutation or choose another identity. Root reconciles the exact retained names/metadata from the receipt with read-only calls. Keep all failed evidence and synthetic resources. If this passes, next Engine work may use detach/register during explicit quiescence with no reuse of old physical names; it does not authorize restarting obsolete hosts, online restore, object deletion, or shared credential changes.

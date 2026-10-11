# Basin duplicate-UUID registration probe

Decision: can a fresh catalog namespace register an existing exact Iceberg metadata URI while the old registration remains? This is the first capability needed for quiescent restore into a new physical namespace generation. It does not qualify restore, destructive fencing, backup, or native commits after restore.

Frozen runner: `/tmp/docspec-duplicate-uuid-probe.py`, SHA256 `4784a34b71b98375ad3579402f5be488ef58c915038afb3915eccc0892a93d65`. Static Python AST parsing only; no provider execution by the author.

Input: the already registered empty synthetic candidate from `docs/reviews/r2-candidate-profile-provider-2026-10-10.{json,jsonl}`. Exact UUID `01a1289a-93cb-74fb-bc0d-2e80965a79d1`, schema field IDs `[7,42]`, and native metadata URI are constants in the runner. The previous run created 775 bytes and retained the registration/object with native automatic cleanup disabled. This standalone candidate is retained evidence, not a claim that it has an Engine lifecycle protection record. Both registrations and their shared original metadata remain; no object is uploaded, downloaded, changed, or deleted by this probe. Basin may internally read the metadata to register it; that provider operation is necessary.

Root execution, after active measurements stop: resolve the existing `DOCSPEC_CATALOG_URI`, `DOCSPEC_CATALOG_WAREHOUSE`, and `DOCSPEC_CATALOG_TOKEN` environment from the existing authorized configuration; do not paste secrets. Choose one fresh canonical UUID and a fresh absolute evidence directory, then run an existing Python interpreter:

```text
python /tmp/docspec-duplicate-uuid-probe.py /absolute/fresh/evidence-directory <fresh-canonical-uuid>
```

No Python package installation is needed. The diagnostic uses standard-library HTTP and the documented Iceberg REST endpoints; it is not a second production catalog adapter. Redirects and automatic request retries are disabled. Each HTTP response is capped at 4 MiB and each request at 15 seconds; the process has a 150-second deadline. The receipt is persisted before each attempted mutation. Root may additionally supervise with a 170-second process deadline.

Sequence: load catalog configuration; load and check the exact retained source; require the unique destination namespace to be definitively absent; create that namespace; POST the existing metadata URI once to its registration endpoint; read the destination and source through fresh HTTP requests. Compare exact selected URI, UUID, table location, sparse field IDs, empty snapshot state and a canonical digest of all native metadata fields. Catalog response credential/config fields are not persisted. Persist bounded status and structured error type/code, never raw upstream text or secret properties.

Success: destination and source both retain the exact same native identity and selected metadata URI; source remains unchanged. A 400/409 with destination absent records rejection, not success or an access blocker. Other failures are inconclusive. A transport timeout during a mutation is an unknown outcome; stop without reposting or choosing another identity and let root reconcile the exact saved namespace/table with read-only requests. Keep all failed evidence and registrations. There is no cleanup step and no payload, throughput, process-recovery or full-restore claim.

Execution follow-up: the initial urllib config403 preceded any mutation. The
same frozen semantic probe switched only its transport to maintained httpx,
with redirects/retries disabled and bounded chunks, using the same still-unused
namespace identity. The resulting409 and unchanged-original reconciliation are
retained separately; no mutation retry followed that definitive response.

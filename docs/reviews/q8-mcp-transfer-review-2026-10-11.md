# Retained export → resumed transfer → trusted MCP review

Verdict: APPROVE frozen patch `505d6427894fbe26cf5a7490dc858c5b37ce76bda5063fc9c8d60278b998d2f8` for root-owned execution. Static review only: no Python run, build, provider or credential access.

Reviewed `/tmp/docspec-q8-mcp/new/experiments/r2-export-mcp.py` against the actual sibling `src/spicy_regs/docspec_exports.py`, `mcp_server.py`, and `tests/test_mcp_query_results.py` plus the existing Rust-host readiness helper. The canonical retained pin's literal fields match the new preflight assertion: IDs 2/1, Int64/Utf8, required counter and nullable text.

Closed findings:
- Cleanup formerly raced child exit between `poll()` and `psutil.Process(...).children()`, allowing `finally` to stop before host cleanup and receipt saving. Lines 195–210 now handle process disappearance while still waiting for each owned `Popen`.
- The protocol's literal field-ID/nullability claim formerly relied only on equality with a loaded expected pin and a names/types DESCRIBE. The new preflight assertion checks the exact field meanings independently before provider I/O.

The transfer is narrowly scoped to the retained 824-byte export in the approved namespace. Prefix and tail requests are conditional on one HEAD's ETag and assert exact 206 ranges, lengths and EOF. The actual SDK send hook counts HEAD=1, GET=2; the first owned Python child is killed only after its exact completed 412-byte prefix marker. The second child fetches only the tail. No catalog or object mutations occur remotely. The complete local file appears through an exclusive hard link; the consumer's maintained native hash checks its full immutable identity once.

The actual MCP TestClient route exercises canonical export lookup and source-release lookup through a fresh Rust host, one native object hash, repeated query/refresh with unchanged binding, exact literal values including duplicate/null/empty values, native consumer types, private-field exclusion and the source-release evidence reference. The wrappers count the real `_record`/`_hash` calls and preserve their implementation. The file/source metadata is reused between repeated reads.

Remaining execution limits are explicit: this is the frozen qualification harness's partial-transfer recovery, not a new public download service; it proves the existing retained export and consumer path, not an additional evidence update, row-receipt mapping, new remote export, multi-host recovery or large-scale throughput. The `/usr/bin/time` child observations and complete-path wall time are measurements only once actually collected. The strict failed-run record must remain if runtime assumptions fail.

## Actual provider execution

The [retained result](r2-export-mcp-2026-10-11/receipt.json) passed: the exact remote
export's first412 bytes survived an owned worker kill, and one conditional tail
request transferred only its missing412 bytes. Actual SDK counts were HEAD1/GET2;
no prefix bytes repeated. The actual MCP reader performed canonical metadata
admission twice and one native hash, then reused that admission across queries.
Transfer plus interruption/resume, host startup, admission and exact consumer
read took5.090 seconds; body-transfer time was0.267 seconds. This tiny retained
export observation excludes the prior already-qualified source update/export
computation and makes no throughput claim. Its canonical staged local path was
absent before collection; remote bytes and new local output remain retained.

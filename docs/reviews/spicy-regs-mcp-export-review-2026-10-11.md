# Actual MCP consumer qualification review

**Verdict: approve the bounded test for execution.** No actionable static blocker. Reviewed `/tmp/docspec-consumer-qualification.patch` and staged test sources against actual sibling `spicy-regs` reader code on 2026-10-11. No builds, provider calls, shared-source edits or test execution by this reviewer.

## Evidence trace

- `tests/exports/mcp.rs:18–48` independently verifies each exact immutable export, gives it a table name through a local hard link, and starts the explicitly configured existing Python environment with only temporary local data/home settings. No production directory or remote credentials are supplied. `Drop` kills/reaps failed helper children; the helper has a 120-second alarm and each MCP query caps output at 50 rows. All fixture payloads are tiny and fixed.
- `tests/qualification/spicy_regs_mcp.py:15–35` imports the actual sibling application and sends real stateless HTTP `tools/call` requests through `TestClient(build_app())`. Request headers and SSE parsing match `spicy-regs/tests/test_mcp_query_results.py:23–31`; `mcp_server.py:2506–2517` supplies the stateless app. No connection or SQL behavior is mocked.
- `spicy-regs/src/spicy_regs/mcp_server.py:650–675,727–739` selects the explicit local data path, creates real DuckDB Parquet views and skips remote publication acquisition and HTTP-extension loading. `local_data.py:84–142` discovers only Parquet files in that temporary directory when no managed selection is supplied. No native state/download pointer is synthesized.
- The real `query_sql` path (`mcp_server.py:2200–2277`) applies its read-only checks, executes against the held connection, bounds rows/cells and emits its actual publication metadata. The helper checks both the values query and `DESCRIBE`, plus explicit `local_unversioned` status.
- `tests/exports/mcp.rs:111–181` uses independent expected constants for duplicate/null/empty/Unicode values, complete-key correction and deletion. It holds the first actual application process across later exports, opens new processes on new immutable exports, then rechecks both. Evidence-only updates preserve source values while changing DocSpec source identity and exact receipt value. The separate DocSpec receipt assertions are explicit.

## Qualification limits

This is a useful actual-consumer slice of R2/Q8: DocSpec immutable local export bytes are queryable through SpicyRegs MCP with the checked values, `BIGINT`/`VARCHAR` types and authorized column shape. It tests held-file reader continuity and completed export identity reuse.

It does **not** qualify SpicyRegs managed-release selection, application receipt compatibility, a mutable current-pointer refresh, remote transfer/readback, every supported type, interrupted publication, or consumer retention/expiry. The helper correctly asserts `local_unversioned`; evidence is verified by DocSpec separately rather than claimed as MCP evidence support. Existing export metadata/disclosure tests provide the broader artifact checks; this fixture checks returned content and schema.

Parent-owned execution should retain the sibling checkout revision/dirty state and configured Python environment/version alongside the DocSpec source/test evidence. The test imports live sibling source, so a DocSpec revision alone cannot reproduce that consumer qualification.


## Runtime finding and narrow correction

The first parent-owned run exposed a fixture assumption missed by the static trace: `_available_tables` (`mcp_server.py:1651–1669`) lists declared/managed/selected names, so an unmanaged custom `qualification_comments` view could be queried but had no entry in publication metadata. The helper's `local_unversioned` assertion failed. That unsuccessful attempt must remain in the evidence.

Reviewed the current correction: use only the declared `comments` name for the isolated hard link, SELECT and DESCRIBE. No consumer behavior is mocked, no assertion is weakened, and immutable export bytes remain identical. The fixture checks a two-column synthetic subset; naming it `comments` does not establish full Comments schema or dictionary compatibility.

Parent reports the corrected actual MCP run passed (`docs/reviews/spicy-regs-mcp-export-qualification-2026-10-11-2.log`) and retained the consumer checkout/interpreter receipt. This establishes the bounded local MCP value/type/readback scope above; no provider rerun or broader application release/evidence claim follows.

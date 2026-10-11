# Opt-in SpicyRegs MCP application-values qualification

Patch: `/tmp/docspec-consumer-qualification.patch`.
Source copies: `/tmp/docspec-consumer-qualification/new/`.

No producer/adaptor change, new dependency, Cargo target, sibling change or production configuration. The existing integration suite registers `exports`; the patch adds one nested ignored test, so default gates do not depend on the sibling checkout or Python packages.

## Exact run command after root applies the patch

From `/Users/mikewolfd/Work/spicy-stack/docspec-rs`:

```sh
DOCSPEC_SPICY_REGS_PYTHON=/Users/mikewolfd/Work/spicy-stack/spicy-regs/.venv/bin/python DOCSPEC_SPICY_REGS_REPO=/Users/mikewolfd/Work/spicy-stack/spicy-regs cargo test --locked --all-features --test integration exports::mcp::actual_spicy_regs_mcp_reads_immutable_exports_across_updates -- --ignored --exact --nocapture
```

The sibling environment and checkout exist locally. They must already contain their maintained dependencies; no download/install step is included. The Python interpreter path is absolute because child environments clear all inherited configuration. Source is imported explicitly from the named checkout; Python packages come from the named interpreter. Three tiny application processes use explicit local file roots, an explicit memory ceiling and bounded lifetimes; Starlette TestClient exercises MCP HTTP in process. Local mode takes the actual reader's local branch, without HTTP extension loading/provider requests. Each child has a 120-second alarm and Rust kills/waits it on fixture failure.

Root should run its normal integration gate separately; this ignored qualification is discoverable but requires the explicit command above. No Cargo or runtime qualification has been run by the author. Python syntax was parsed with the standard AST parser only.

## What the fixture checks

- Real immutable exports of initial rows, corrected/deleted rows, and an evidence-only release through the existing Engine/export path; explicit authorized `[public,id]` schema.
- Actual sibling `build_app()`/`query_sql` over the real cold `_get_connection` and explicit local Parquet views; no query/connection mocking or direct DuckDB/PyArrow substitute.
- Initial child is read before updates, held through both subsequent selections, and reread afterward. Independent children cold-read later versions.
- Exact equal duplicate occurrences, BIGINT/VARCHAR interpretation, null key, null text versus empty text, Unicode text, correction and deletion; expected JSON rows are literal expectations rather than derived from a producer result.
- Exact DocSpec source occurrence survives the correction; original and changed receipts are checked separately by retained release; evidence-only release preserves native value groups while export binds its new canonical VersionRef.
- The isolated fixture uses declared `comments.parquet` / `comments` so the real application includes its `local_unversioned` status in query publication metadata. Private canary and hidden/evidence fields do not enter the selected schema or application reply. Completed export retry returns the exact artifact.

## Limits

This establishes a small local application **values** path if it passes. Only Int64/text are qualified here; broader primitive/nested types still require the named consumer's meaning. The script expressly checks the actual application label `local_unversioned`; DocSpec release/evidence binding has not been added to MCP. The application reads a named hard link to verified immutable export bytes in each separate fixture directory; this is test configuration, not public distribution/admission semantics. Evidence proof remains a separate DocSpec reader assertion, not MCP receipt lookup.

No remote upload, public HTTP hosting/authentication, browser/search consumer, application publication pointer, availability-window policy, interrupted/resumed application transfer, complete-path performance, or wider Q8 closure is claimed. TestClient exercises the real MCP HTTP handler without a listening network service.

The audit `/tmp/docspec-consumer-matrix-audit.md` now correctly states that current STATUS records the passing scoped full local 10M Comments acceptance gate. That gate and this separate application handoff have distinct scope.

Root execution: corrected declared-table fixture passed; source/interpreter receipt is `docs/reviews/spicy-regs-mcp-export-source-2026-10-11.json`, corrected run log is `docs/reviews/spicy-regs-mcp-export-qualification-2026-10-11-2.log`. The original custom-table publication-map failure remains retained. The scoped result is BIGINT/VARCHAR values continuity plus separate DocSpec evidence checks; elapsed test time is not a performance gate. The earlier exported patch is an author snapshot; the applied root correction is the current implementation.

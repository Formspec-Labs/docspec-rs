# Blind trusted MCP export admission review

VERDICT: REQUEST CHANGES for one narrow do-once defect; no SQL disclosure or release-identity blocker found. Static only: no tests, Cargo, provider calls or shared edits. Author notes were not read. Reviewed frozen SpicyRegs patch067ffc302a6801c91184aeccbbff9815b5e61dc8203bbdad36d25ecd367724d4 and DocSpec patchaf48702da80a77aa0eed98ac7c6f6b35bfcdeb327ab1b2be848bbf9d939a61d1, exact `/tmp/docspec-mcp-admission/{repo}/new` trees. Parent workspace and DocSpec instructions apply; SpicyRegs has no AGENTS.md/CLAUDE.md in the inspected checkout.

## Finding

F1 WARNING, performance/do-once: SpicyRegs `src/spicy_regs/docspec_exports.py:115–136` permits two named application tables selecting the same direct local file and exact canonical ExportPin. Each iteration repeats both canonical metadata requests and the complete native hash. Neither the signature map nor unchanged TTL fast path prevents repeated initial payload reads. Hardlinked duplicate filenames also select the same physical bytes. The trusted configuration format does not prohibit this case.

Smallest remedy: explicitly refuse duplicate physical member bindings before metadata/hash effects if one-table-per-file is the intended supported subset. Otherwise reuse those exact per-admission identities. Add a focused same-file/hardlink alias refusal or exact request/hash count test. Author agrees to the refusal option; final repaired artifact has not yet been reviewed here.

## Patch and function trace

| Path | Proposed source | Observed behavior |
| --- | --- | --- |
| local_selection → local_exports | local_data.py:99; docspec_exports.py:98 | Explicit absolute regular trusted config; requires separate local directory and prevents managed/native selection mixing. Only named direct Parquet children enter serving. |
| _document / _metadata_url / _record | docspec_exports.py:21,44,58 | Bounded JSON, duplicate-key rejection; literal loopback HTTP port, no redirects/proxy/ambient credentials; bounded canonical records. |
| canonical export/source identity | docspec_exports.py:126–135 | Exact whole ExportPin JSON equality against configured canonical endpoint, then exact source dataset/version/manifest equality against retained release. No inferred lineage from matching values. |
| _hash / member guard | docspec_exports.py:76,136–144; local_data.py:208,255 | Existing absolute native standalone hash executable with bounded time/response, no Engine startup; native digest+bytes equality and pre/post ordinary dev/inode/size/time signature checks. These signatures are explicitly not cryptographic replacement protection against arbitrary trusted local mutation. |
| _build_connection | mcp_server.py:641–750 | Only selected named local files form views. Native/publication metadata tables hold trusted references; schema read is maintained DuckDB behavior. Existing locked allowed_paths and disabled external access remain authoritative. |
| _publication_status / _reply_pins | mcp_server.py:996,1275 | Exact pin plus release-manifest reference; no invented row-receipt mapping. Query/describe publication coverage is None for this new status. Static dictionary descriptions remain descriptive legacy metadata. |
| _refreshed / _get_connection | mcp_server.py:1325,1352 | Exact configuration identity plus member signatures reuses connection, avoiding repeated canonical/hash work across TTL. Changed config builds a replacement; failed refresh keeps original captured connection. |
| _statement_timeout / request cursors | mcp_server.py:1380,2041+ | Existing per-cursor pre/post member checks and timeout apply. Old connection is not closed under live cursors. Member mutation refuses queries; old immutable selection is not silently promoted. |
| Consumer::open / MetadataHost | DocSpec tests/exports/mcp.rs:19,220 | Real hardlinked export bytes, real native hash and real Engine HTTP canonical metadata. Two-thread runtime permits router service while synchronous consumer pipe waits; host task aborted on fixture drop. |
| qualification helper | tests/qualification/spicy_regs_mcp.py:19–79 | Wraps actual metadata/hash calls for counts; real MCP route, private-canary exclusion, exact BIGINT/VARCHAR schema and literal rows. TTL0 forces repeated refresh checks without rehash. |

## Data flow and safety invariants

Caller-trusted configuration chooses executable, loopback producer, canonical expected pin and disclosed references; it is not imported untrusted metadata or authentication authority. Canonical release JSON is transient: the saved reply holds the exact export and manifest reference, not private release/native payloads or fabricated receipt joins. Only authorized flat member paths reach DuckDB's allowlist (mcp_server.py:577–587); source manifest, config and executable paths are not added. Existing SQL write/dynamic-query restrictions remain unchanged. References themselves are explicitly disclosed by caller choice, so revealing their locators is intentional; SQL content access to them remains refused.

Admitted local bytes are guarded once per initial selection and around later statements. The unchanged config fast path does not imply producer availability or a lasting foreign owner grant; README correctly requires callers to retain referenced evidence separately. Evidence-only source changes keep different exact release references even when exported row multisets coincide. Existing immutable ExportPin/public result promises retain the producer output independently.

## Tests and limitations

SpicyRegs tests/test_docspec_exports.py:61 proves expected full pin/release reference, formatting-only config edits, same cached connection at TTL0, two metadata calls/one hash and SQL private-manifest refusal. Parametrized cold failures at:88 cover wrong pin, wrong release, changed bytes and external metadata URL; unmanaged mode remains unversioned. Hash/canonical boundaries are mocked there and cannot qualify native Engine behavior.

Opt-in DocSpec reads_across_updates at tests/exports/mcp.rs:226 uses actual initial/corrected/deleted/evidence-only releases and independent row oracles. New trusted variant checks full exact export/reference and real admission counts; old readers preserve prior rows/pins. Its new schema qualification is BIGINT/VARCHAR only and local same-host distribution; no remote consumer or all-type claim. Existing default unbound variant remains separate.

No new warm config-change/held-cursor failure fixture is added; existing managed-local signature/cursor code is reused unchanged. New alias case F1 currently lacks coverage. Provider qualification and runtime test results remain parent-owned.

Confidence HIGH in identity/disclosure/cursor traces. Re-review the small alias repair before final approval; no importer, trust framework or public Rust API is needed.

## Final alias repair — static APPROVE

Reviewed final SpicyRegs patch `87bd7df4e8b7913f3aeca2b0043439578341a2134408eaddf2755127ffd42856`; DocSpec remains `af48702da80a77aa0eed98ac7c6f6b35bfcdeb327ab1b2be848bbf9d939a61d1`. The intermediate filename-only repair42041903 did not cover hardlinked names. Final `docspec_exports.py:114–132` preflights every direct member, rejects repeated filenames and `(device,inode)` identities via the existing regular-file signature before creating the HTTP client. The parametrized test at `tests/test_docspec_exports.py:119–130` exercises same filename and an actual hardlink with both metadata and hash observations empty. Distinct copied files remain separate admitted members; no new cache/content deduplication is needed. F1 is closed.

Final verdict: APPROVE for bounded execution. No remaining actionable finding. This is static review only; parent owns Python/native runtime and actual consumer qualification. Existing trusted-local mutation assumptions and evidence retention responsibility remain as stated above.

## Parent-owned Python runtime follow-up

Parent reports the combined slice at `docs/reviews/mcp-admission-python-2026-10-11.log` passed110 and failed two existing generation dictionary tests; new admission tests passed. Both failures originate in unchanged `tests/test_generation_mcp.py:214`: it patches shared `duckdb.connect` to its Connection adapter, which lacks `register`. The expected_schemas → contract_schemas → contract_types.described_schema lazy import captures that adapter through unchanged `native_types.py:8` (`from duckdb import connect as _connect`), then `native_types.py:37` requires native register. Neither fixture nor native_types is changed by this admission patch. This is a preexisting import-order-dependent mock mismatch by source trace; baseline runtime was not executed independently. A fixture-only native_types import before monkeypatching, or maintained register delegation, preserves the assertions. Native Engine qualification remains pending parent execution.

## Root qualification

Actual native Rust producer → local immutable export → SpicyRegs MCP admission
and HTTP queries pass in `mcp-admission-native-2026-10-11.log`. This covers initial
duplicates/null/empty/Unicode values, corrections/deletions, evidence-only release
changes and held old readers. Each newly admitted connection uses exactly one
native file hash and two canonical metadata reads; stable refresh performs none.

The Python boundary/regression slice had110passes and two failures from an
existing import-order-dependent mock: the generation fixture patched shared
`duckdb.connect` before `native_types` captured its native connection. Importing
`native_types` before that patch fixes the fixture; both failed cases pass in
`mcp-admission-python-fixture-fixed-2026-10-11.log`. The original log remains.
Ruff passes for the new adapter/tests and corrected fixture.

This is explicit local admission, not hosted deployment or interrupted remote
distribution qualification. Exact release references do not invent row-level
receipt mappings.

# Remote export provider fixture review

**Verdict: APPROVE for one isolated opt-in execution.** Static review of patch `45caba34fc3b280468f1f309ced79945e0a06744e66fc3b2889230600e0a1bda` and applied/formatted `src/engine/remote_build_provider_tests/exports.rs`, plus `/tmp/docspec-engine-export-provider.py`. No execution by this reviewer.

The earlier fixed-name stage guard was incorrect because production writes `stage-<UUID>.parquet`. The final guard now binds the saved attempt ID and host prefix to the canonical temporary host, exact dataset/attempt directory, and canonical unique stage filename before deleting that single synthetic file (`exports.rs:229-264`). No provider deletion or cleanup exists.

| Trace | Verified behavior |
|---|---|
| `qualify_export`, `exports.rs:92` → existing `connect`/`compose` | Requires canonical fresh qualification UUID and absent native lifecycle namespace. Uses scoped R2 prefix and bounded existing Engine configuration. |
| Source creation and Build, `exports.rs:129-166` | Tiny literal source has duplicate/null/empty/public and private-canary values. Native Build retains input, filters one row, then original producer file is removed. |
| Transfer interruption, `exports.rs:179-228` | Stops after verified transfer before materialized/validation/result completion. Exact stage and validation identities/rows, protected destination, and absent completion pair are asserted. |
| Resume, `exports.rs:263` onward | Deletes only the qualified synthetic stage, resumes same operation, requires exact destination identity, unchanged stage/check/attempt records, and no recreated stage. Later channel selection remains selected after old completed retry. |
| `assert_values`, `exports.rs:37` | Configured reader checks literal schema, field IDs, types/nullability and metadata, bounded row count, duplicate multiplicity, null and empty distinctions. Fresh host reads the old exact pin; changed caller pin refuses. |
| Wrapper | Runs one exact ignored test with fresh UUID and timeout; captures source and executed binary hashes, safe stdout/stderr, and terminal result. Configuration values are passed only through environment; the wrapper does not print secrets. |

The expected schema is justified by the actual native path: `src/engine/write/build.rs:187` auto-assigns IDs starting at one in SELECT * schema order; `src/storage/schema.rs:68` uses maintained Iceberg conversion, whose String output is Utf8. Therefore text ID1 and counter ID2, reordered by export policy to counter/text, is a literal independent expectation. DataFusion internal Utf8View does not change that native read schema. `authorized_schema` (`src/engine/export.rs:776`) strips arbitrary metadata and preserves those IDs/types/nullability.

Passing proves this tiny actual-provider Engine source export, transfer-gap recovery with missing original host stage, immutable older selection, and fresh-host configured read. It does not measure transfer request counts, exercise HTTP/CLI/public-constructor composition, qualify arbitrary types, authorize automatic stage cleanup, prove cross-host unfinished recovery, or establish resource/performance acceptance. The fixture explicitly reports request counts unmeasured. Confidence: high for execution scope, provider result pending.

## Parent-owned execution

`r2-engine-export-provider-2026-10-11.json` records a passing isolated run against
the configured SpicyRegs R2/Basin provider. Raw stdout and stderr are retained
next to it, along with source/executable hashes. The first and later exports each
contain 810 bytes and four rows. Same-host missing-stage recovery, completed
original retry after channel advancement, and fresh-host exact authorized
readback pass. The fixture records one initial query and zero fresh-host queries;
transfer request counts remain unmeasured. No remote objects were removed.

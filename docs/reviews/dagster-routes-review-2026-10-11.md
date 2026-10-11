# Native Dagster advertised-route review

## Finding

**NIT, performance/simplicity — duplicate producer deep check**: proposed `adapters/dagster/tests/test_rust_integration.py:289–295` verifies enriched separately and then verifies summary. `src/engine/verification.rs:387–390,401–412` already visits summary's exact enriched semantic input and deeply checks that release. Keep only the summary subprocess; coverage equality at proposed line283 already binds the expected enriched version. Sent to author for the small correction. No request-shape, guard, or scheduling-path blocker found.

## Patch summary and trace

Reviewed incremental artifact `32c927da93eb4a5edf880a28f3f64a56b8576893a399bb9c34d38325b9ec4d5d` atop the already applied export-response-loss patch. Source references below use `/tmp/docspec-dagster-routes.py` as proposed `adapters/dagster/tests/test_rust_integration.py`. This is a test-only addition; no adapter or engine production behavior changes.

| Function / boundary | Evidence | Verified behavior |
| --- | --- | --- |
| New route test / `_qualify("routes")` | proposed lines32–33,109–128 | Uses the real existing Rust host and Dagster job. A literal native SQL row avoids assumptions about the supplied tiny Parquet schema or cardinality. Original fixture file stays unchanged. |
| `submit` | proposed lines201–215 | Each request calls real `docspec_job.execute_in_process`, checks success and exact route/dataset/operation acknowledgment, then obtains the canonical native result using its correct release or export lookup route. |
| Dagster op and native HTTP | `definitions.py:29–63,66–69`; `src/http.rs:610–627,682–710,760–790` | The job calls the resource/client; native handlers deserialize the corresponding request and invoke Engine services. This is actual in-process Dagster execution, not a fabricated HTTP peer or a scheduler deployment. |
| `/plan` | proposed lines232–242; `client.py:232–237` | Same completed native RunSpec produces exact planned/completed acknowledgment. Retained state equality checks planning did not create new native state. |
| Receipt input and `/receipts/run` | proposed lines220–254; `evidence.rs:39–51,104–108` | Native Build/export produces exact source_file/source_row/evidence shape. Seed has one data file and one row; row0 association is valid. File literal escapes apostrophes. Attached source pin stays unchanged. |
| `/updates/run` | proposed lines255–261; `evidence.rs:114–132` | Adds new key2 to existing key1. Removed rows are empty, so existing receipt table can be adopted unchanged; the assertion matches the actual production rule. |
| `/schemas/run` and `/backfills/run` | proposed lines262–275; `model.rs:873–876,906–942` | Rename uses the established single root ID1 and definition cutover; computed ID10 is group-local. Backfill retains exact source pin and covers two rows. Native request field names match deserializers. |
| `/refreshes/run` | proposed lines276–286; `model.rs:306–325,336–377,406–418` | Captured/full aggregate request, empty grouping keys and count_rows representation are exact native shapes. Coverage binds enriched VersionRef and evidence covers two contributors. Selected heads are checked separately. |
| Independent final verification | proposed lines287–295; `main.rs:348–357`; `verification.rs:316–323,337–412` | Existing catalog-free deep verifier checks final summary and its semantic input closure. A single summary invocation suffices. |

## Invariants and scope

The adapter README allowlist contains `/plan`, `/run`, `/updates/run`, `/schemas/run`, `/receipts/run`, `/backfills/run`, `/refreshes/run`, `/exports/run`; the new sequence actually dispatches each through the Dagster job. It keeps native operation IDs and metadata-only requests, does not add a Python lifecycle ledger, and uses maintained native export to create valid tiny input files. Generic response-loss/retry qualification remains in the already passed separate cases, without fault duplication per route.

The fixture's exact assertions cover acknowledgment identity, row coverage, receipt preservation, source sharing, active definition, semantic input references and selected heads. Existing native deep verification supplies reconstruction checks. This does not independently qualify every operation variant, all source values against a separately implemented oracle, sustained scheduling, real-provider scheduling/authentication, every route's interruption boundary, or performance. Optional environment absence still skips this native test honestly.

No builds, Python tests, provider calls, shared-source changes, or formatter execution performed by this reviewer. Runtime result remains parent-owned.

## Verdict

**APPROVE**, with the small duplicate-check simplification above. Static coverage is adequate for the advertised local success-path boundary; confidence medium pending native execution. No production guarantees should be inferred before the new test passes.

## Root closure

Applied final patch `54fb910315f849d2053b4712981dd3f02e0a0b1a164a78269482e81c209da5df`: one summary deep-verification subprocess retains its exact producer closure check. The duplicate producer scan was removed as requested. Runtime evidence is recorded separately.

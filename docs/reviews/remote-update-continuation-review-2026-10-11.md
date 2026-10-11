# Retained remote Update continuation review

Verdict: **APPROVE for the scoped continuation**, static only. No additional reachable Local-storage fallback found in the permitted source-only path. The first provider run failed and remains failed evidence; this review does not establish successful continuation.

## Findings and disposition

- The original review missed a real backend-injection defect: `evidence::verify` reads the parent even when the new result has no receipts, because it must reject dropped prior receipts. `src/engine/evidence.rs:594–619` now passes `checks.objects()` to the same bounded manifest reader. Parent identity and receipt-loss checks are unchanged. Later Local receipt readers (`:637–638`) require actual evidence and remain outside the remote capability.
- No material continuation issue found. `updates.rs:454–485` requires a canonical qualification UUID and makes existing namespace presence agree with explicit retained mode. `:494–511` requires selected `initial`, resolves the actual saved `request/update-one`, and captures its completed processing before calling the scenario. `:515–518` asserts the entire retained processing revision/PhaseResult is unchanged. Execution must use the exact qualification UUID in the retained failed receipt; the generic test intentionally does not hard-code a particular run ID.
- `prepare` regenerates only tiny producer files in a fresh temporary directory (`updates.rs:135–175`). `fingerprint` binds Build/Update content digests and material parameters rather than producer locator (`engine.rs:2369–2375`), so saved requests retain their material identity; normal engine guards refuse changed content. Existing processing recovery precedes fresh payload work (`engine.rs:2068–2115`). Literal removed/added reads are explicit fixture checks; this is not a claim of zero payload reads for the qualification itself.

## Post-validation trace

| Boundary | Evidence | Result |
| --- | --- | --- |
| Composition and source history | `engine.rs:1871–1881`; `backfill/verification.rs:81–111,375–390`; `write/verification.rs:19–40` | Configured parent/native metadata, routine source-update schema checks; empty computed set skips scalar paths. |
| Corrections and receipt inheritance | `corrections.rs:145–146`; `evidence.rs:594–619` | Empty computed set returns; configured receipt-parent check returns after proving both sides lack evidence. |
| Immutable release creation/readback | `locations/release.rs:98–210` | Same configured object adapter for creation and one-buffer identity/decode readback. |
| Change certification and final selection | `changes/certificates.rs:369–398`; `engine.rs:1988–2027` | Configured descriptor read; related dependency traversal skipped for source-only result; backend/authority guards remain. |
| Completed retry availability | `engine.rs:2738–2752`; `schema.rs:670–684`; `backfill/verification.rs:7–15` | Configured object availability. Local schema read is Schema-only; scalar history returns immediately with no computed groups. |
| Exact source read and export | `read.rs:13–32,147–158`; `export.rs:492–518` | Configured native opens and scan; local file writing later in export is intentional bounded host staging, not a remote URI fallback. |
| Parent retention expansion | `retention/inventory.rs:1711–1725` | Complete native inventories bypass expansion; unsupported remote expansion families refuse before their Local readers. Strict complete DatasetPin parents use configured reader. |

## Evidence and limits

The failed provider receipt `docs/reviews/r2-engine-update-provider-2026-10-11.json` records exit 101 at qualification `docspec-rs-qualification-0e38b494-34a4-47e4-a96e-f8ae27822f00`. Its stderr shows initial work/readback and first Update admission/processing/validation before `io`. The continuation specifically tests retained processing and then second Update/export/cold read. It does not inject an unknown native commit outcome, measure all request counts, establish throughput, or qualify remote receipt/computed/maintenance paths. Completed validation reuse can be checked from retained work-event evidence; this fixture's explicit exact-generation comparison covers processing.

No builds, provider calls, or shared-source edits performed in this review.

Reviewed source digests:
- `src/engine/evidence.rs`: `6da6548050303c7b08ecf7950885805d57bae5cedd4a0449424e2551e0e824aa`
- `src/engine/remote_build_provider_tests/updates.rs`: `a7242febfd271567d174d8156996618ca1f068a540961a914d6b4cd40e51942d`

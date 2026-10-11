# Two-client publication qualification

Decision: whether the configured provider path preserves one mutable export-channel selection when two independent Engine/catalog clients publish from the same captured catalog version.

Hypothesis: native expected-snapshot requirements accept exactly one publication; the losing completed export remains exact and available, a stale retry refuses without rerunning its materialization/validation, and old immutable pins remain readable after the channel advances.

Cases and arms: one constructed scenario runs first against two separate embedded-catalog client instances, then once against two separately configured R2/Basin clients. Both use the real Engine Build, source Update, export preparation/selection and configured reader. Local is an adapter/control check, not evidence about provider concurrency. Both clients are in one OS process with separate execution-host directories, budgets, catalog instances and Engine locks. This is not a separate-process or geographically distributed qualification.

The source has three small literal rows. A source Update changes one key into duplicate rows, producing four rows with duplicates, null and empty strings. An old export is selected first. Two distinct export operations prepare and validate output from the updated source, both expecting the old channel pin. Only final publication overlaps: a test-only forwarding Catalog adapter holds each client's next lifecycle update after the existing guarded native preflight. It records the already-observed metadata URI and waits at a two-party barrier for at most 60 seconds. It forwards the original maintained TableCommit without changing requirements, returning synthetic successes, adding provider retries or taking new metadata reads. Both captured metadata URIs must be equal.

Pass criteria, fixed before execution:

- Exactly two held commits reach the same immutable metadata URI and exactly one export call succeeds.
- The winner equals its prepared object/validation/source, and immutable-result/channel records share one committed native revision.
- The losing result is absent. Its materialization and validation records, native revisions and exact object stay unchanged and available. Retrying its original stale expected-channel request returns `export_conflict`; it is not silently republished or given a changed request identity.
- Winner retry returns its original pin. Both clients resolve the same winner.
- A fresh Engine/catalog client reads the old and winning export pins with exact literal values and fixed field IDs/types/nullability. Current source selection remains the already accepted Update.

Boundaries: one race, no automatic repetitions or live-provider cleanup. Use a 600-second outer process deadline for preparation, the held race, reconciliation and cold reads. Each Engine has the existing small fixture profile (one compute thread, 256 MiB admitted memory, 16 MiB scratch, 1 MiB batches, 4 MiB metadata); two active preparation clients therefore admit up to 512 MiB memory and 32 MiB scratch in aggregate. Returned reads refuse beyond four rows. The test logs safe synthetic identities, bounded error codes, observed boundary and the already-known retained export byte sizes. Catalog/native metadata, failed-commit objects and other prefix bytes are excluded explicitly; no listing or whole-prefix storage claim.

Remote scope: a fresh canonical qualification UUID, absent dataset namespace and `s3://<bucket>/__r2_data_catalog/docspec-qualification/<uuid>/publication-race` prefix only. Retain all remote objects, including the losing prepared export and any failed native catalog-commit objects. This fixture does not inject network loss, worker crashes, same-operation races, source-update conflicts, or destructive maintenance; it does not measure throughput, total request counts or process RSS. Such results must not be inferred from this case.

Artifact is test-only and depends on the connected remote Update patch. No source correctness fix was needed during static tracing. Author performed no builds, formatting, provider calls or shared edits; root owns execution.

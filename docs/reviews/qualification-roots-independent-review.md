# Independent review of qualification root tests

No material findings. The new fixtures isolate qualification-driven inventory and original-owner fallback against `05ea3a1996c56a470f9777e6db35a22ebdff8572`. Their accompanying note accurately limits the evidence: synthetic catalog isolation does not qualify a supported retirement, replacement, or standalone restore sequence.

This is a static review of `tests/qualification_roots.rs` and `docs/reviews/qualification-roots-review.md`. I read the applicable workspace and repository instructions and applied the Rust and semi-formal code-review skills. I traced the relevant committed production implementations; those files had no working-tree differences from the reviewed commit. I did not review the concurrent native column-selection changes or run builds, tests, formatting, benchmarks, or corpus work. Execution remains unverified here.

## Patch summary

The patch adds public-API integration fixtures and a coverage note, with no production changes. `qualified` creates a real corrected scalar group and returns its successful proof plus original creation references (`tests/qualification_roots.rs:33`). `retain_only` installs that proof into a fresh catalog while preserving a sufficient lifecycle revision (`tests/qualification_roots.rs:111`). The test entry points are at `tests/qualification_roots.rs:143` and `tests/qualification_roots.rs:198`; there are no ordinary application callers.

The tests use the existing scalar fixture and maintained catalog APIs. Their improvement is narrower, attributable regression coverage: overlapping file phases, results, readbacks, or earlier owner grants cannot satisfy the assertions. No speed or resource improvement is claimed.

## Function trace

| Function or method | Location | Inputs and output | Verified behavior |
| --- | --- | --- | --- |
| `limits` | `tests/qualification_roots.rs:23` | Shared scalar limits → fixture limits | Sets a fixed metadata and memory allowance; establishes no measured memory bound. |
| `qualified` | `tests/qualification_roots.rs:33` | Temporary root → proof, sibling target, original creations, reservations | Runs the source and original scalar backfill, adds a correction, then selects a sibling backfill carrying the successful inherited proof. Reads declaration, descriptor, and output references from the immutable origins. |
| `retain_only` | `tests/qualification_roots.rs:111` | Fresh lifecycle and proof → qualification key | Advances revision using empty authority records, commits the exact hash-keyed proof, and asserts that the catalog contains only its qualification and authority keys. |
| Inventory test | `tests/qualification_roots.rs:143` | Real proof and synthetic target variant → assertions | Uses a fresh owned catalog per variant; requires exact qualification blockers, zero unmatched bytes, and exact protected-byte accounting. |
| Owner test | `tests/qualification_roots.rs:198` | Each original creation → admission assertions | Uses a fresh injected catalog per object, verifies mismatch refusal without a grant, then checks the exact admitted object and sole new owner grant. |
| `Engine::with_catalog` | `src/engine.rs:209` | Workspace plus injected catalog → engine | Passes the supplied catalog directly into `compose`; it does not reload the original workspace catalog. |
| `inspect_retention_admitted` / `collect_inventory` | `src/engine/retention/inventory.rs:97`, `:174` | Candidate objects and owned catalog → blockers and accounting | Enumerates the injected lifecycle's catalog only. Ordinary inspection does not add the restore-only native catalog roots. |
| `Inventory::record` qualification branch | `src/engine/retention/inventory.rs:1078` | Qualification row → queued dependencies | Checks its key, owning lifecycle table, and first revision, then dispatches to `qualification`. |
| `qualification` / `expand` | `src/engine/retention/inventory.rs:921`, `:1482` | Origins and target snapshot → referenced objects and native paths | Expands both origins and target. Origins recover original completions; native expansion reads the exact snapshot metadata and its inventory. |
| `protect_admission` managed-input branch | `src/engine/admission.rs:226` | Managed path and expected digest → exact input dependency | Resolves the object through `qualified_object`, then calls `protect_objects` for the foreign consumer. |
| `qualified_object` | `src/engine/retention/owners.rs:611` | Owner, path, digest → original `ObjectRef` | Tries direct creations first, then original completions through retained origins. Mismatching identities refuse before owner acquisition. |
| `acquire_owner` / `confirm_creation` | `src/engine/retention/owners.rs:665`, `:784` | Foreign consumer and exact objects → owner grant | With no prior grant, independently requires completed creation evidence; missing direct creations trigger the origins fallback again. |
| `visit_original_creation` / `visit_creation` | `src/engine/retention/owners.rs:828`, `:958` | Lifecycle rows → observed creation identities | The former follows qualification origins and recognizes actual declarations, descriptors, and outputs. The latter recognizes neither reservations nor standalone qualifications as direct creations. |
| `run_until` / admission completion | `src/engine.rs:817`, `:1020` | Build request and `admission` stop → injected failure | Stops after durable admission, before processing attempts to parse the input as Parquet. |

## Data flow and invariants

**H1: Inventory cannot pass through an overlapping lifecycle root. Confirmed.** `retain_only` asserts the full current key set (`tests/qualification_roots.rs:132`). The inventory test uses an entirely separate workspace and catalog (`:155`), while production inventory lists only that catalog (`src/engine/retention/inventory.rs:186`). No copied reservations or results are present. Every inspected candidate must identify the exact standalone qualification key as its blocker (`tests/qualification_roots.rs:183`).

The original proof points at the source snapshot (`src/engine/corrections.rs:71`). Its origins record the original group's source and output snapshots when prepared (`src/engine/backfill/preparation.rs:318`). The sibling `next` group is created afterward by a different expression (`tests/qualification_roots.rs:61`, `:74`). The second variant substitutes that sibling's native snapshot and adds its payload to the candidates (`:163`, `:177`). This gives target expansion a dependency that the earlier original origins cannot supply. The altered proof is hash-keyed again; the test does not reuse a key for different proof contents.

The mutated target is intentionally synthetic. Its successful inventory inspection would establish dependency traversal, not semantic correspondence between those original executions and the substituted target. The test comment and coverage note state this limit (`tests/qualification_roots.rs:152`; `docs/reviews/qualification-roots-review.md:9`). Candidate assertions cover origins, original creations, target metadata, and a target-only payload; they do not independently enumerate every possible native metadata artifact.

**H2: Reservations or earlier grants cannot mask original-owner fallback. Confirmed.** Each object starts in a new catalog containing only authority, qualification, and copied path reservations (`tests/qualification_roots.rs:201`). `visit_creation` has no reservation or qualification branch (`src/engine/retention/owners.rs:958`). Its initial pass therefore misses both in `qualified_object` and in `confirm_creation`. The only eligible fallback anchor is the qualification's immutable origins (`src/engine/retention/owners.rs:856`), whose actual completion identities are enumerated at `:934`.

The shared physical workspace is necessary for managed-path recognition and reservation validation; it does not expose the original lifecycle records because `with_catalog` uses the supplied fresh catalog (`src/engine.rs:209`). The valid admission must retain the original `ObjectRef` exactly, rather than copy the bytes into a new owner identity (`tests/qualification_roots.rs:255`). Creating a fresh catalog per object and requiring exactly one grant prevent one iteration's grant from qualifying a later iteration (`:271`).

## Test behavior and edge cases

Static traces support the expected assertions in both tests. Actual pass/fail status requires execution with `test-util` enabled, as the entire test file is feature-gated (`tests/qualification_roots.rs:3`).

The wrong-digest case reaches `qualified_object` before `protect_objects`; the retained original path with a different digest produces `invalid_owner_protection` (`src/engine/retention/owners.rs:643`). The subsequent no-grant assertion distinguishes identity refusal from partial owner acquisition (`tests/qualification_roots.rs:228`). The valid request deliberately stops after admission, so declaration and descriptor metadata are legitimate identity fixtures without claiming to be executable Parquet (`:244`; `src/engine.rs:1020`).

Supported retirement/replacement, unknown original provenance, standalone restore, and inherited foreign grant lifetime remain uncovered by these new tests. These are explicit limits, not hidden alternate paths through the assertions. In particular, inventory accepts references outside the isolated workspace, while restore additionally requires owned paths and native catalog closure; injected catalogs are not qualified for retention inspection (`src/engine/retention/inventory.rs:101`, `:139`, `:216`). The coverage note identifies these gaps and says the earlier F1 is narrowed only after execution succeeds (`docs/reviews/qualification-roots-review.md:16`, `:20`).

## Findings and conclusion

No actionable correctness, isolation, or claim-scope finding in the reviewed patch. The remaining lifecycle and restore coverage is already described accurately and should remain open.

VERDICT: APPROVE for the stated isolated inventory and owner-fallback coverage.

Coverage of the stated new paths: ADEQUATE by static trace. Coverage of the broader standalone lifecycle/restore requirement: INSUFFICIENT, explicitly acknowledged by the patch. Confidence: HIGH in the isolation analysis; runtime execution was not performed by this reviewer.

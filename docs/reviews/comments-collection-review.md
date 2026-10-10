# Comments collection readiness review

**VERDICT: APPROVE for controlled collection.** No new blocking defect was found
in the frozen full-workload entry point, execution capture, resource monitor, or
independent oracle. This approves collecting evidence; it does not establish a
performance pass or complete the wider implementation plan.

Static review on 2026-10-10 of `ce3ad6b88ed748bdcc56cf33ce0da43a78ac2752`, using
the semi-formal code-review and required Rust guidance. The reviewed harness and
Rust boundary files have no working-tree diff against that commit. Unrelated
Dagster changes were excluded. No tests, builds, benchmarks, payload hashes,
downloads, or input changes were performed by this reviewer. This follows the
approved harness review rather than reopening its resolved findings
(`comments-e2e-review.md:367-444`).

## Findings and collection obligations

**O1 — Build identity remains an explicit evidence obligation.**
The existing `experiments/comments-e2e/release-build-20261009.json:2-4,461-469`
describes a successful build of `a903c750`, not the reviewed commit. Supply the
new exact-commit build receipt and compare its binary byte count/SHA-256 with
`execution-capture.json` before interpreting results. The runner preserves the
receipt verbatim and deliberately does not validate its schema
(`run.py:173-213`); this obligation already belongs to the frozen protocol
(`protocol.md:170-179`). The parent is compiling the reviewed commit separately.
This review does not certify the pending binary or its eventual capture.

**O2 — Disk admission fits only if current capacity still meets the declared plan.**
The proposed 24 GiB retained allowance per run requires **106 GiB** free for three
runs: `3 × 24 + 32 scratch + 2 reserve`. The runner measures available space after
execution capture and refuses below that requirement (`run.py:337-346`). A
read-only `df -k .` during this review reported 142,525,896 KiB available, about
136 GiB. This is a point-in-time observation during separate compilation, not
launch capacity or measured output size; use the runner's final preflight.
The allowance remains an estimate, and an over-budget attempt remains a failure.

**O3 — Resource observations and test coverage retain their stated limits.**
The monitor observes aggregate runner/descendant RSS and free space, samples both
engine and oracle scratch, checks stage-boundary inventories, and rejects observed
overruns (`comments-input/run.py:104-228`). It cannot prove an unsampled peak stayed
under the ceiling. `/usr/bin/time -l` adds child peak RSS, not continuous aggregate
RSS. The existing synthetic evidence does not inject every deadline, termination,
resource-refusal, or full `execute`/series failure; that known coverage limit remains
documented in `comments-e2e-review.md:429-433`. Neither point blocks the already
defined, sampled-resource collection.

## Scope and function trace

The measured input remains the frozen 10 million full-width Comments, real Dockets,
and complete nested docket receipts. Shaping adds byte lengths; the left join
preserves unmatched Comments and duplicate occurrences; summaries retain exact
Decimal128 sums; native contributor tables and both flat exports are checked.
The source-only pilot and synthetic fixtures cannot satisfy this gate
(`protocol.md:17-71`; `workload.py:14-42,50-99`).

| Function / boundary | Inputs and output | Verified behavior |
| --- | --- | --- |
| `validated_inputs`, `execute` — `run.py:154-170,322-362` | Prepared identities, executable, receipt and allowance → retained series results | Fixes population/input identities; requires three fresh runs in acceptance mode; stops after the first failed run; only three successful full runs set acceptance. |
| `freeze_execution` — `run.py:173-213`; `capture_bytes` / `copy_frozen_file` — `integrity.py:17-54` | Loaded sources and selected files → read-only copies and identities | Hashes during capture, refuses observed mutation/replacement, retains failed copies, and preserves the build receipt and dirty diff. |
| `run_one` / nested `request` / `command` — `run.py:240-318` | Frozen executable, complete request builders and fresh workspace → per-run result | Includes source admission, generation seal, join, summary, exports, selection lookups and oracle under one deadline. Checks retained size before passing. |
| `main` CLI dispatch — `src/main.rs:270,340,397-398,555-556,620-626,650-655` | Serialized requests and complete limits JSON → shared engine calls | Uses the actual `run`, `refresh`, `export`, selection and seal APIs; no test-only processing path is invoked. |
| `Engine::open` / `compose`, `Budget::create` / `context` — `src/engine.rs:175-221`; `src/budget.rs:66-111` | Workspace and limits → owned host, shared memory/spill runtime | Routes engine spill to `workspace/scratch`, splits engine memory admission with DataFusion, and supplies batch size and target partitions. These controls are distinct from observed process RSS. |
| Engine operation entries — `src/engine.rs:745-748`; `src/engine/refresh.rs:29-36`; `src/engine/export.rs:56-76`; `src/engine/inputs.rs:17-34` | Actual request types → admitted operations / release or seal | Preserves shared application admission and capacity checks; the harness calls maintained production operations. |
| Monitor `command` — `comments-input/run.py:104-228` | Command, shared deadline, process root, scratch prefixes and disk bound → raw logs / metrics | Polls RSS/free space independently of directory scans; checks reported child peak RSS; terminates the private process group on failure/cancellation; preserves command evidence. |
| `Oracle.input`, `native`, `exact_equal`, `run_checks` — `oracle.py:97-106,134-226,242-346` | Retained admitted copies, exact native pins and exports → exact readback result | Verifies admitted BLAKE3 identities, plans exact Iceberg snapshots, checks schemas/membership/field IDs, and compares every value and multiplicity including nested order, contributor relations and exports. |

## Data flow and invariants

- **Hypothesis H1: synthetic substitutions cannot reach production collection.**
  Confirmed: `execute` supplies the captured oracle explicitly, the CLI exposes no
  row-count or oracle override, and production `ROWS` stays 10,000,000
  (`run.py:347-350,376-386`; `workload.py:16`). The five-row override is confined to
  the explicit integration-test wrapper (`test_integration.py:53-68`).
- **Hypothesis H2: ordinary worktree edits cannot change an active series.**
  Confirmed within the stated ownership boundary: loaded helpers retain their
  source bytes, later engine commands use the copied executable, and the copied
  oracle imports its copied siblings (`run.py:27-50,182-194,247,288`). Keep the
  Python environment and experiment-owned copies unchanged; the read-only files
  are not a defense against arbitrary writers (`protocol.md:162-179`).
- **Hypothesis H3: failures cannot become an acceptance result.** Confirmed for
  the traced paths: source/refusal errors, monitor stops, oracle mismatch,
  deadline failure and retained-size failure all leave the run false; collection
  stops and retains it (`run.py:271-318,347-360`). Prior pilot success or resuming
  failed work cannot replace a required fresh run.
- Limits are explicit: the full harness supplies 12 compute threads, 24 GiB
  memory, 32 GiB scratch, 4,096-row batches, 768 MiB batch allowance and 32 MiB
  metadata (`workload.py:38-39`; `run.py:246-262`). It does not accidentally inherit
  the reused pilot monitor's 8 GiB scratch/64 MiB batch defaults. DuckDB uses
  18 GiB internally while the aggregate monitor remains responsible for the
  24 GiB process observation (`oracle.py:115-122`).
- Each run's peak additional logical allowance is 24 + 32 GiB above the existing
  series baseline; scratch prefixes cover every planned run. Retained run bytes
  are checked separately at completion (`run.py:248-251,291-292`). All results,
  logs, previous runs and failed work remain on disk; no cleanup makes a pass fit.

## Tests, evidence and reporting limits

Inspected the relevant synthetic test implementations and existing retained
`comments-e2e-harness-check.log:43-49`, which ends in `OK`. These tests assert
captured-code stability and capture refusal (`test_workload.py:25-68`), retained
input custody (`70-117`), exported field interpretation (`119-135`), duplicate,
null/empty and nested-order comparison (`137-161`), combined scratch (`163-172`),
and the complete real-CLI-to-oracle fixture (`test_integration.py:20-68`). This
review did not rerun them or infer corpus performance from them.

Keep compilation/preparation/capture costs separate and stop unrelated builds,
tests and benchmarks before timing (`protocol.md:95-108`). Report each run's
elapsed time and exact oracle result, observed RSS/scratch, peak additional and
retained logical/allocated bytes, storage amplification, and matched/unmatched
rows from the retained outputs. Payload request/read-byte counters remain unknown;
APFS allocated blocks do not establish unique extents, and volume change includes
unrelated work (`protocol.md:117-133`; `run.py:302-317`; `oracle.py:339-346`).

Evidence remains whole-input Comments provenance, generation-associated nested
docket receipts and exact local contributors. Unavailable original Comments ETL
receipts, attachment bodies, per-docket receipt decoding and statistical coverage
of every agency are not supplied by this selection (`protocol.md:25-57`). A pass
must retain these limits and does not qualify remote publication, maintenance,
restore, or separate unchanged/update/backfill/interrupted cases
(`protocol.md:3-6`; `docs/STATUS.md:3-13`).

**Conclusion:** the scoped runner can collect an honest result under the frozen
decision rule. Coverage is **adequate for controlled collection**, with the explicit
fault-injection gaps above. Confidence is **high in the static trace**, while the
actual full-run time/resource outcome and new build/capture identity remain
unmeasured by this review. No acceptance criterion was changed.

# DocSpec Rust guidance

This file records the user's decisions for this repository. Apply the parent
workspace instructions as well. Read [the implementation plan](docs/PLAN.md),
[requirements](docs/requirements/) and [current status](docs/STATUS.md) for scope
and evidence. Status notes describe implemented behavior; they do not redefine
completion. Later direct user instructions take precedence over older plans.

## Start with user value

- Build capabilities to solve an actual user problem. Trace requirements and
  implementation tasks to the result the user needs, preserving R1–R7 and the
  research acceptance cases. Consult the retained research references before
  inventing new workflows.
- Give people what they need when they need it. A request, report, diagnostic,
  configuration option or approval step must support a concrete decision or
  resolve an observed problem. Do not create them for hypothetical completeness.
- Keep routine output focused on the result and actionable failures. Make deeper
  inspection available when someone is debugging a failure, reviewing a result
  or investigating a discrepancy. Diagnostic work must not become a prerequisite
  for ordinary use.

## Design simply, for distributed scale

- Treat this as greenfield development. Preserve the intended behavior and data
  guarantees, without carrying forward legacy code, APIs or work solely because
  they already exist. Question earlier choices using their rationale and evidence.
- Prefer an elegant, small design that remains efficient across machines and
  object storage. Consider bytes moved, scans, rewrites, network requests,
  metadata growth, concurrency, recovery and retained storage alongside CPU time.
- **Do something once—read, hash, verify, compute, transfer or write—unless a
  concrete correctness boundary requires repetition, or avoiding it would add
  more complexity than it removes.** Reuse the durable result. Explain material
  exceptions and optimize them when measurements justify the change.
- Ordinary reads resolve a committed immutable version and stream the requested
  data. Do not revalidate the dataset, historical inputs or dependency graph on
  every read. Validate new material once at admission or creation. At publication,
  check the assembled metadata and current authority; check newly transferred
  objects without repeating completed checks on existing objects.
- Resolve reusable work before opening large payloads. Process batches, share
  unchanged files, and keep metadata separate from bulk values. Avoid per-row
  lifecycle transactions and full-population work for small updates when a
  qualified incremental path exists.
- Prefer streaming and bounded, columnar batches for processing and transfers.
  Push column selection and filtering toward the reader, avoid unnecessary
  materialization/copies/serialization, and use bounded concurrency and
  backpressure. Choose the fastest resource-efficient path supported by the
  workload; streaming is a means, not a rule that overrides a measured better
  approach or required recovery boundary.
- Think about Big O where growth materially affects time, I/O, requests or memory.
  Identify the relevant dimensions: rows, changed rows, files, metadata/history,
  join fanout and output size. Notice repeated linear scans and multiplicative
  work in hot paths. Optimize those costs when they matter; do not complicate a
  cheap bounded operation to improve its theoretical complexity. Use measurements
  to account for constants, locality, parallelism and external storage costs.
- Reuse maintained components before building infrastructure. Keep project-owned
  adapters thin, dependencies injected at application startup, and domain rules
  independent of storage, catalog, execution and transport. Introduce an
  abstraction only for a concrete adapter, shared behavior or test need. Avoid
  speculative plugin systems, caches, schedulers and parallel implementations.
- Each refactor must make affected code simpler, cleaner, faster or more
  resource-efficient, or explain the constraint preventing an improvement.
  Measure speed/resource claims; removing a call is not a throughput result.

## Architecture and lifecycle decisions

- Build an embedded Rust engine with thin CLI and HTTP interfaces using the same
  application services. Use Iceberg/Parquet for data and lifecycle records, Arrow
  for batches, and DataFusion for relational work. Qualify local execution before
  R2/Basin and Dagster adapters. Rust owns lifecycle decisions.
- Keep v1 Iceberg-only. A maintained catalog may use SQL internally for its own
  bookkeeping. A separate DocSpec SQL database is outside v1; any proposal to
  change that scope needs a measured complete-path problem and a controlled
  comparison with equivalent guarantees.
- A dataset-scoped guarded lifecycle commit selects the exact combination of
  snapshots, schemas, evidence, inputs and definitions. Independent native table
  heads do not define a complete result. Unrelated datasets prepare independently.
- Use stable operation identities and immutable selected versions. Reconcile lost
  responses with retained state. Recheck mutable eligibility after commit conflicts;
  do not blindly retry stale decisions or repeat completed uploads/computation.
- Preserve expensive completed phases across cancellation, worker loss, later
  validation failures and report failures. Keep material reuse dependencies
  separate from full runtime provenance. Keep pending work, successful nulls,
  failures and deletions distinct.
- Preserve original source bytes, field identity, input coverage, evidence and
  historical interpretation. Derive change records from applied writes. Unsupported
  incremental transformations must explicitly rebuild, never silently guess.
- Publish immutable authorized objects, then select their release through the
  catalog. Do not create a second mutable "latest" authority in object storage.
- Follow the plan's protection, retirement and restore rules. Keep expiration
  disabled until protection tests qualify it. A performance optimization must not
  delete required evidence, weaken selection guards or overstate recovery.

## Verification proportional to the need

- Routine creation checks cover shape, field IDs, types/nullability, file sizes,
  readable footers, metadata row counts, exact references and a bounded decoded
  sample. Validate new output once and retain the completion for retries.
- Validate the combination of already checked components through metadata rules.
  Readback checks the serialized selection without repeating the payload scans.
  Keep constraints needed to produce valid output, such as declared join
  cardinality, in processing rather than repeatedly reconstructing them afterward.
- Compute hashes during admission or writing when the maintained component makes
  that straightforward. Preserve known identities for reused immutable objects.
  A separate initial digest pass is acceptable when removing it requires a more
  complicated writer; do not repeat it during normal reads or retries.
- Use explicit deep verification and independent reconstruction for qualification
  or a specific integrity/correctness investigation. A routine completion must not
  satisfy a requested deep check. State the actual scope; sampling does not prove
  every value, complete coverage or statistical representativeness.
- Bound sampling by files, rows and decoded bytes across the request. Sample
  native files directly; a `LIMIT` above a join or aggregate can still scan all
  inputs. Distinguish returned sample size from actual compressed I/O and memory.
- Do not build a validation framework or extra user workflow merely to support
  optional checks. Prefer one shared implementation with explicit scope.

## Performance target and evidence

- Preserve the target in [the plan](docs/PLAN.md): **10 million representative
  retained Comments through the defined full local flow and independent readback
  within 1,800 seconds on each of three fresh-workspace runs**. This is an
  acceptance target until measured, not an established capability.
- The initial reference is 14 CPU cores and 48 GiB memory, with configurable
  ceilings of 12 compute threads, 24 GiB aggregate process memory and 32 GiB
  scratch. Check actual free disk and preflight retained output plus scratch;
  never treat an old free-space observation as current capacity.
- Keep local and remote timings separate. Keep government API acquisition and
  optional OCR/model inference outside the defined local workload. Preserve the
  complete acceptance scope; a fast subset or synthetic fixture does not pass it.
- Use the test-hypotheses workflow for performance experiments: freeze the
  decision, hypothesis, workload, arms, resource/cache conditions and success
  criteria before collecting results. Retain failed attempts and raw evidence.
  Do not benchmark while unrelated builds or tests distort the measurement.
- Measure unchanged, small-update, backfill and interrupted/resumed paths as well
  as full runs. Hold update size fixed while growing population, file count,
  retained history and evidence depth. Record time, requests, bytes moved,
  retries, peak memory, scratch and retained storage as relevant to the decision.
- Treat ballooning disk use as a performance problem. Measure peak additional
  disk space during execution and retained space afterward, including input
  copies, intermediate files, spills, outputs, metadata, history and failed
  attempts. Report storage amplification against a stated input or selected-output
  size; distinguish logical file sizes, allocated blocks and whole-volume free
  space. Attribute build caches and unrelated workstation growth separately.
  Include space needed while old and new versions coexist and while maintenance
  runs. Sample live free space and stop bounded experiments before exhausting it;
  a fast run that cannot fit the storage budget does not pass. Reuse immutable
  files, avoid unnecessary materialization, and retire disposable work through
  the appropriate lifecycle rules without deleting retained evidence.
- Optimize measured bottlenecks: repeated scans/transfers, unnecessary rewrites,
  metadata operations, then execution overhead. Prefer narrow reads and maintained
  execution primitives. Avoid optimizing a microbenchmark at the expense of the
  complete lifecycle or adding complexity without a demonstrated benefit.

## Implementation, reviews and repository work

- Make concrete progress through the full accepted plan. Do not substitute an
  easier subset for its scope or declare completion from a narrow passing test.
  Keep implemented, measured, unqualified and unfinished work distinct.
- Read the applicable Rust skill before modifying or reviewing Rust code. Preserve unrelated
  workspace work and keep this repository's history separate from sibling repos.
- Commit coherent, reviewable increments as work proceeds. Use independent
  subagents for blind semi-formal reviews at meaningful implementation checkpoints.
  Give them the artifact, requirements and review question
  without coaching them toward the author's conclusions.
- Review through consistency/recovery, performance/resource use, simplicity/reuse,
  flexibility and operations lenses as appropriate. Require actionable findings
  grounded in code paths and failure cases. Resolve material findings and retain
  the review result; scale ceremony to the risk of the change.
- Run [the repository gate](scripts/check) for implementation changes and targeted
  checks that exercise the changed behavior. Keep tests meaningful and proportional;
  avoid tests that merely mirror code or repeated broad runs without new evidence.
- Register ordinary integration suites in `tests/integration.rs`; Cargo does not
  discover them automatically. Share its fixture modules, and keep suites needing
  process isolation as explicit targets in `Cargo.toml`. Target an ordinary suite
  with `cargo test --locked --all-features --test integration <suite>::`.
- Reuse local corpora, caches and retained task outputs before downloading.
  Establish the exact version through manifests/digests, and preserve shared
  evidence. Download only the missing or unavailable portion.
- Keep changing inventories in their source of truth, not hard-coded prose counts.
  Keep measured bounds and acceptance numbers with their context. Local commits
  do not imply authorization to push, publish or deploy.

## Communication

Speak like a solutions architect explaining the work to a technically literate
program manager. Lead with the result or practical meaning, then give the evidence
needed to assess it. Use plain English, active voice and concrete wording. Explain
what goes in, what happens, what comes out and how it is checked when useful.

Distinguish verified facts, interpretations, plans and blockers. State performance
and verification limits plainly. Keep progress updates short; avoid jargon,
unnecessary warnings, invented workflows and requests for approval already given.
When asking for a necessary decision, make the proposed result concrete and
reviewable first. Explain what changed, why, how it was checked and what remains.

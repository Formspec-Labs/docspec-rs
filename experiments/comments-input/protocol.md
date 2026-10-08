# Retained Comments performance probe

Frozen before collecting execution timings on 2026-10-08. This probe chooses the
next performance intervention in the implemented path. It is not the complete
R1–R7 acceptance gate; missing lifecycle features remain in `docs/STATUS.md`.

**Decision:** Can the current admission → shaping → native output → routine
validation → immutable selection path handle 10 million full-width Comments
within its reference time/memory budget, and does reuse avoid repeating payload
work? Use failures and measured stage costs to choose the next change.

**Hypothesis:** Removing automatic deep reconstruction makes completion and normal
reads depend mainly on required data work. Unchanged operations reuse exact native
files; retries return the original release. A refusal, missed time budget,
population-sized reuse scan or excessive memory weakens that hypothesis.

**Cases:** Reuse the digest-qualified capture in `local-asset-check.json` without
downloading. Select exactly 10,000,000 physical occurrences uniformly across its
26,418,078 rows: retain row `i` when
`floor((i + 1) * 10000000 / 26418078) > floor(i * 10000000 / 26418078)`.
Keep every column and literal value, including nulls, empty text, duplicate
occurrences and evidence-related JSON. Do not deduplicate or shorten text.
This systematic selection spans the capture; it is not a claim that every
regulatory population or evidence dependency is represented.

Prepare this immutable input once using bounded Arrow batches and 65,536-row
Parquet groups, recording schema, physical selection rule, source identity,
output digest/size and preparation failures. Preparation is outside the engine
timing. The original capture's unusually small groups produce about 132 MB of
metadata; input preparation avoids carrying that incidental layout into the
engine probe. Keep the original capture unchanged.

**Arms:** One fresh workspace, one new operation with unchanged input/query, and
one exact retry of that operation. Use `SELECT * FROM input` and the same release
binary, lockfile and limits. Check native file identity across reuse/retry.
This is an initial bottleneck probe, not a historical binary speed comparison.

**Held constant:** Release build; 12 compute threads; 24 GiB configured aggregate
memory; 8 GiB scratch ceiling to fit current local disk; default batch/metadata
limits unless the frozen run records a refusal. Preflight input, retained output
and scratch together. Record actual free disk, machine/OS, binary/source/lock and
harness/protocol identities, cache conditions and background load. Do not run
measured commands alongside Cargo or another benchmark. Inputs may be cached;
do not call these cold-cache timings.

**Readback:** Use independent PyArrow footer/type/count checks and a bounded row
sample of selected output files. Verify that release selection matches the returned
native snapshot. Measure this separately and include it in the reported local
path total. Do not write a full intermediate Arrow export merely to verify it.
Existing exact reconstruction tests remain correctness evidence for the
implementation, not evidence that every value in this probe was rechecked.

**Bounds and decision rule:** Retain one pilot for each arm. Stop a command at
1,800 seconds or a measured process resident set above 24 GiB; preserve its logs,
partial workspace and exit status. Record wall time, process memory, disk/I/O
statistics exposed by macOS, retained bytes/files and reuse identities. A failure
is not replaced by a narrower input. Diagnose it before revising this protocol.
If the fresh path plus readback fits the budget and checks pass, report only a
bounded result for this implemented slice. If unchanged/retry work rewrites
payloads or performs deep verification, treat reuse as not solved. Do not infer a
stable speedup from one pilot.

The full gate still needs the defined joins/summaries, evidence handling, native
and flat outputs and complete lifecycle behavior, with three fresh runs within
1,800 seconds each. Remote publication and readback need separate measurements.
Any protocol revision must retain this version's criteria and explain the change.

## Pre-collection amendment

Before input preparation or measured runs, the wide-value verification fixture
showed DataFusion batches retaining buffers for a complete input row group
(`docs/reviews/routine-verification-wide-samples.log`). Use **4,096-row input
groups** instead of the originally proposed 65,536, matching the reference batch
size and reducing that known source of oversized backing buffers. The selection,
columns, values and engine limits remain unchanged. This still consolidates the
capture's roughly 1,000-row groups; record the actual footer size after preparation.

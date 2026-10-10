# Exact oracle scratch failure: narrow options

Research date: 2026-10-10. Scope: source, retained metadata/logs, and official
documentation only. No corpus payload was read or hashed, no benchmark or test
was run, and no implementation was changed for this note.

The next diagnostic should run the existing oracle over the completed retained
outputs with only `threads=12` changed to `threads=4`. This is the smallest
maintained intervention and preserves the comparison algorithm. Its resource
benefit remains unmeasured. If it fails, shared strict DuckDB `ENUM` types can
encode contributor paths without dropping values or occurrences. A keyed exact
join is a later fallback with stronger identity obligations. None of these
retained-output diagnostics qualifies the complete acceptance flow.

## Observed failure and unchanged decision

The evidence root is
[`20261010T103539-acceptance-13ca1824`](../../workspaces/qualification/comments-e2e-export-layout-20261010/20261010T103539-acceptance-13ca1824/).
The saved [`environment.json`](../../workspaces/qualification/comments-e2e-export-layout-20261010/20261010T103539-acceptance-13ca1824/environment.json)
records source `e40be018b2a1c12c2e1067ba4fe552de3a6711f6`, DuckDB **1.4.4**,
PyArrow **23.0.0**, PyIceberg **0.12.0**, and Python **3.12.9**. The current
`oracle.py`, `workload.py`, and `protocol.md` equal the retained frozen source
bytes at research time. This comparison read source files, not data files.

The saved [`run-01/oracle.stderr.log`](../../workspaces/qualification/comments-e2e-export-layout-20261010/20261010T103539-acceptance-13ca1824/run-01/oracle.stderr.log)
shows that admitted byte identity, native membership/types, complete shaped
Comment values, Dockets values, and nested receipt values passed. Shaped Comment
equality took 43.765 seconds. The next check, `exact native join`, failed while
offloading a 96 KiB block with 31.9 GiB of the 32 GiB DuckDB temporary allowance
used. This is a spill-capacity failure, not a reported value mismatch.

[`oracle.metrics.json`](../../workspaces/qualification/comments-e2e-export-layout-20261010/20261010T103539-acceptance-13ca1824/run-01/oracle.metrics.json)
records 104.336 seconds, exit 1, sampled process-tree RSS 23,380,492,288 bytes
(about 21.77 GiB), child peak RSS 23,567,630,336 bytes, and sampled scratch
32,271,175,008 bytes. The sampled scratch figure missed the terminal peak shown
by DuckDB. A null monitor `stop_reason` does not turn the engine failure into a
pass. The complete failed run took 239.294 seconds; approximately 135 seconds
preceded the oracle. Native processing, evidence, exports and selection completed,
but independent verification of the join and subsequent outputs did not.

Preserve the [protocol](../../experiments/comments-e2e/protocol.md): every field,
null/empty distinction, duplicate occurrence, nested receipt/list order, original
literal JSON string, contributor relation, field ID, export mapping, generation,
and serialized selection remains in scope. Acceptance still requires the full
10-million-row flow within **1,800 seconds on each of three fresh runs**, at most
**12 compute threads**, **24 GiB aggregate process RSS**, and **32 GiB combined
engine/oracle scratch**. Retained-output diagnostics cannot become those runs.

## What the current comparison costs

[`exact_equal`](../../experiments/comments-e2e/oracle.py) groups the signed
`UNION ALL` by every selected value and rejects any nonzero multiplicity
difference. That is an exact bag comparison after the separate schema checks.
The failing invocation includes the complete Comment/docket values and all four
contributor fields. The expected relation itself contains a left join.

DuckDB documents grouping, joining, sorting and windowing as blocking work, and
warns that combinations can exhaust resources even with external execution.
`EXISTS` above this grouping does not make a successful equality check a bounded
sample. [DuckDB 1.4 LTS workload tuning](https://duckdb.org/docs/lts/guides/performance/how_to_tune_workloads)

The retained native schema metadata confirms that Comments and Dockets have no
stored `__docspec_origin_*` fields in this run. Their unique complete physical
files therefore define the source identities used by the oracle. The selected
Comments paths are each 264 characters; the Dockets path is 263. This is a
metadata-derived reason to investigate repeated path storage. It is not a
measurement of DuckDB's internal string copies or an attribution of the entire
spill to those columns; no operator-level profile was retained for the failure.

## Prioritized options

### 1. Change one maintained setting: fewer active threads

The failure message recommends reducing threads. DuckDB's OOM guidance also
recommends it and explains that engine memory limits do not bound every process
allocation. The current oracle already disables insertion-order preservation
and uses an 18 GiB engine limit. Raising memory is especially unattractive with
observed RSS already near 22 GiB under the separate 24 GiB ceiling.
[DuckDB OOM guidance](https://www.duckdb.org/docs/current/guides/performance/oom)

A single `threads=4` arm, with query, 18 GiB memory setting and 32 GiB scratch
unchanged, can test whether concurrent operator state drives the peak. It remains
below the 12-thread ceiling but changes the protocol's frozen active setting;
record that as an explicit intervention before collection. A lower peak could
trade throughput for capacity. It does not isolate which blocking operator
caused the original peak. Do not conduct a thread-count sweep.

Record actual settings with `duckdb_settings()` or `current_setting()`.
`max_temp_directory_size` applies to DuckDB's selected directory; the parent
monitor must continue accounting for combined scratch and aggregate RSS.
[DuckDB configuration](https://duckdb.org/docs/lts/configuration/overview)

Do not initially change caches, prefetching, allocators, compression settings,
DuckDB versions, or connection lifetimes. No retained measurement currently
identifies them as the cause. The current-version OOM page supplies general
guidance; the recorded failure itself confirms thread tuning is offered by the
pinned 1.4.4 runtime.

### 2. Keep the exact bag comparison; encode known file paths losslessly

DuckDB enums store dictionary references, distinguish case, preserve nullable
column values, and reject an explicitly cast string absent from the enum.
Comparisons between different enum types can convert back to strings. These
are documented in the **1.4 LTS** API, matching the recorded major/minor version.
[DuckDB enum type](https://duckdb.org/docs/lts/sql/data_types/enum)

Create one named enum from the validated Comments source-path list and one from
the validated Dockets source-path list. In both arms of this specific comparison,
explicitly cast `__docspec_left_file` to the first type and
`__docspec_right_file` to the second. Keep both row numbers and every user value
unchanged; keep the signed union, full grouping, and all original schema checks.
Use the same named type on both sides. Do not use `TRY_CAST`, truncate paths,
extract basenames, hash paths, or create a dictionary separately per side.

The reasoning is exact: a shared injective dictionary maps equal strings to the
same value and different strings to different values. Null stays null. Applying
that mapping to each row preserves equality classes and their signed counts.
Uniqueness of business IDs or source occurrences is unnecessary for this proof.
Unexpected paths must fail conversion; they must never disappear in an inner
dictionary join or collapse into null. The allowed domain comes from independent
source membership, not from trusting the actual joined rows.

This needs no new payload scan: enum values come from already resolved metadata,
and conversion occurs inside the existing comparison. Confirm the union and
group keys retain the enum types in the pinned runtime; an implicit conversion
back to `VARCHAR` would defeat the proposed representation benefit. Keep a
strict applicability condition for inputs with stored origin paths: current
physical membership alone may not contain their historical origins. For those
inputs, use the unchanged comparison unless an independently complete origin
domain is already available. Do not add a corpus scan merely to build one.

The same narrow representation can later apply to contributor-only comparisons
using the appropriate source dictionary. Do not modify all checks preemptively.
The plain flat export excludes these fields, so this option makes no claim about
that later check. The expected improvement is smaller grouping/spill state; only
the bounded retained-output run can establish whether it is enough.

### 3. If grouping remains too large, compare rows by proven occurrence identity

For the native join only, define `K = (left_file, left_row)`. The independent
Comments source has one row per K because metadata qualification accepts each
complete physical file once and `file_row_number` identifies a physical row.
The existing stored-origin branch additionally rejects invalid or duplicate
resolved origin pairs. A business `comment_id` is not a substitute.
[DuckDB Parquet location columns](https://duckdb.org/docs/lts/data/parquet/overview)

Before treating K as a key of the expected join, independently check Dockets:
every non-null `docket_id` must have at most one occurrence. Null IDs may repeat
because the frozen equality join never matches nulls. This check reads only that
small key column and must refuse duplicates rather than deduplicate them. The
result is exactly one expected row per Comments K, including unmatched rows.
Also check actual joined K values are non-null and unique. A declaration of
`many_to_one` or equal total row counts alone proves neither property.

Then perform a full outer join of expected and actual on K, reject either absent
side using explicit non-null presence markers, and compare **every remaining
field**, including right contributor identity, with `IS DISTINCT FROM`. A full
outer join preserves missing rows on either side; ordinary `<>` is unsuitable
because null comparisons yield unknown.
[DuckDB outer joins](https://duckdb.org/docs/lts/sql/query_syntax/from),
[DuckDB null-safe comparisons](https://duckdb.org/docs/lts/sql/expressions/comparison_operators)

Proof: unique keys give a one-to-one pairing. No absent side establishes equal
key sets; exact null-safe comparison establishes equal typed values per key.
Identical user rows at different K remain separate occurrences. The narrow key
checks read no wide text, and the value comparison reads each wide relation once.
This replaces full-row grouping with key lookup; a hash join may still retain
wide payload and spill. It is a hypothesis, not a memory guarantee.

If that join is still too wide, disjoint **column groups**, each paired with K,
are a conditional fallback after uniqueness is established. They can read each
value column once using Parquet projection pushdown; repeated reads are limited
to the narrow keys needed to preserve row association. Verify the physical plan
actually projects disjoint columns. Do not apply this to unkeyed source or flat
outputs: independent column bags lose cross-column relationships.
[DuckDB Parquet projection and filter pushdown](https://duckdb.org/docs/lts/data/parquet/overview)

### Arrow batch equality is a useful primitive, not a replacement row identity

PyArrow provides `ParquetFile.iter_batches` with selected columns/row groups and
`RecordBatch.equals`. The 23.0 documentation currently identifies itself as
23.0.1; this run used 23.0.0, so qualify the pinned runtime before adoption.
[Arrow ParquetFile](https://arrow.apache.org/docs/23.0/python/generated/pyarrow.parquet.ParquetFile.html),
[Arrow RecordBatch](https://arrow.apache.org/docs/23.0/python/generated/pyarrow.RecordBatch.html)

Direct lockstep batches are unsafe here: neither the workload nor joins promise
global output order, and batch boundaries can differ. Sorting both sides by a
proven unique key could make an Arrow comparison exact, with small batch-boundary
slices and simultaneous end-of-stream checks. But sorting remains blocking, and
both sort producers count toward the same RSS/scratch limits. Small returned
batches do not prove bounded upstream work. For flat output, which excludes K,
an exact canonical sort would need every user column or an independently proven
key; the current source provides no such key proof. This is a lower-priority
option, not grounds to add a custom sorter or comparison framework.
[DuckDB order preservation](https://duckdb.org/docs/lts/sql/dialect/order_preservation)

## Counterexamples a candidate must retain

| Case | Required result / implication |
| --- | --- |
| Two equal user rows at different source positions | Both survive; changing multiplicity fails. |
| Drop one K and duplicate another, leaving total count unchanged | Fails; total row count is insufficient. |
| Equal K appears twice on both sides | Generic bag comparison may pass; a keyed path must refuse its uniqueness precondition. |
| Swap one column between K values | Fails; independent unkeyed column comparisons could miss this. |
| Null text versus empty text, or null versus zero length | Fails; use typed null-safe equality. |
| Unmatched Comment with a fabricated right path/row | Fails, even if all docket user fields are null. |
| Unknown path, same basename in a different directory, or case change | Strict full-path encoding refuses; no fallback-to-null. |
| Duplicate non-null docket key | Keyed path refuses cardinality; null docket keys must still never join. |
| Row permutation or changed record-batch boundaries | Does not change bag equality; lockstep Arrow comparison alone is invalid. |
| Nested list order, null list versus empty list, or literal JSON spacing changes | Existing complete source checks still fail the difference. |
| Same values but changed field IDs/export mappings | Existing metadata checks still fail. |

Plain `EXCEPT` loses duplicates. Symmetric `EXCEPT ALL` preserves them but is not
an evidenced improvement over the current one-union check, and can repeat scans.
Digest equality, sample equality, `LIMIT` above blocking work, business-key
assumptions, and repeated full-payload scans per hash bucket are excluded.
[DuckDB bag/set semantics](https://duckdb.org/docs/lts/sql/query_syntax/setops)

## Bounded diagnostic plan for the execution owner

**Decision:** Can the unchanged exact oracle complete all checks on the retained
outputs with fewer active threads, inside the existing resource ceilings?

**Hypothesis:** Less concurrent blocking-operator state reduces peak spill enough
to finish. Completion within the bound supports this intervention; another
capacity failure weakens it. This experiment does not identify the internal
allocation class responsible for the original failure.

**Arms:** The saved 12-thread failing run is a historical control. The selected
first arm changes only active DuckDB threads to four and invokes the entire
original oracle once in a fresh process. No implementation adoption is implied.
Save an exact diff and captured candidate source. ENUM encoding remains a
successor candidate only if another resource failure warrants it.

**Held constant:** Exact retained input/native pins, all fields and expected
transformations, pinned runtime, 18 GiB DuckDB memory, 24 GiB process-tree RSS,
32 GiB combined scratch, and 12-thread ceiling. The diagnostic cap is **600
seconds for the full oracle**, not a fresh allowance for each query. Use a new
experiment-owned directory, symlink its workspace to the completed retained
workspace, and copy only the bounded metadata required by the original oracle.
Write new scratch/logs/results under the diagnostic directory; leave the failed
run's files unchanged. Check current free space, concurrent work and selected
metadata before starting. Do not rebuild or rerun native processing/exports.

1. Record SQL, effective settings, source/candidate identities, the retained
   output pins and the original failure link. Metadata-only `EXPLAIN` can show
   operator shape and expected fanout without a data-query execution.
   `EXPLAIN ANALYZE` executes the query and must count as the diagnostic, not as
   free preparation. [DuckDB profiling](https://duckdb.org/docs/lts/sql/statements/profiling)
2. Reuse the existing process-tree and disk monitor. Verify its inventory sees
   the new oracle scratch directory and accounts for any existing engine scratch
   in the linked workspace. Preserve the combined ceilings; do not infer a bound
   from DuckDB's engine setting or a symlink directory's apparent size.
3. Run once. The unchanged full oracle deliberately repeats the admitted-byte
   custody check and earlier comparisons: this establishes that the existing
   whole oracle can finish in one fresh process against the retained artifacts.
   This is a concrete verification boundary for this diagnostic, not a general
   reason to repeat native processing or to add full scans to normal reads.
4. Capture all incremental check receipts, overall/per-check elapsed time,
   process-tree RSS, scratch/free space and failures. If query profiling is
   enabled, capture it during the intended execution rather than replaying
   payload queries afterward. Treat profiling overhead as part of the observed
   diagnostic. Unknown physical read-byte counts remain unknown. Report the
   shared-workstation/cache conditions and label the saved control historical.
5. A passing diagnostic establishes exact readback of those retained outputs
   under the observed conditions. It does not repair the earlier acceptance
   record or supply a fresh-workspace run. Append the intervention to the
   existing protocol before any new acceptance series. The original complete
   scope, 1,800-second limit and three fresh runs still apply.
6. If four threads fail, retain that result and select one successor before
   another payload run. Prefer strict path enums as the next narrow change;
   first verify the shared enum types, origin-domain applicability and the
   adversarial cases with the existing synthetic harness. A keyed comparison
   needs all uniqueness and membership proofs above. Avoid a settings sweep,
   combined interventions, new comparison framework, or unbounded retries.

**Decision rule:** A mismatch, refusal, resource-limit failure or timeout remains
failure. Adopt only after the full exact diagnostic fits and relevant existing
regressions pass. A faster partial path is not acceptance. If the narrow changes
do not fit, report that wide exact comparison state remains unresolved; do not
increase budgets or replace equality with probabilistic evidence.

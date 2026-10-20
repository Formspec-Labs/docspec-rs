# Avoid unused native key-table registration

Decision: Determine whether removing the unused registered key table reduces
Update metadata work while preserving the existing bounded key-file and applied
change paths. This is ct-xl0y; application adoption remains undecided in ct-eipa.

Hypothesis: `replace_keys` consumes prepared distinct-key Parquet files directly
through DataFusion and never appends, pins, or selects their table. Building its
writer metadata in memory with the maintained Iceberg `Table::builder` should
remove one catalog registration and one retained metadata JSON per replacement,
without changing native data reads/writes or materialized key files. A mismatch in
schema, ownership, old/new values, changes or retry behavior rejects the change.

Arms: native 91eb44f baseline binary versus this one correction; keep the original
native primary binary unchanged and compile only into a separate target cache.

Cases: existing exact renamed/reordered fields, duplicate/null/composite keys,
correction/deletion/append/empty updates; failed processing after key discovery,
retained key ownership and resume without original producer files. Then run the
existing application 48-case leaf comparison with the baseline and candidate CLI
on fresh local workspaces, same schemas, finite limits and command sequence.

Held constant: no production/provider configuration or calls; same input schemas,
16-row changed leaf and one/ten unrelated leaves, three repeats and exact literal
oracles. Source FileIO and the original protected attempt directory remain the
writer's owner. Stop before exhausting disk; parent coordinates quiet native
build/test/measurement windows. Grouped timing/cache conditions remain explicitly
limited as in the retained application baseline. Do not change selection policy.

Decision rule: adopt only after the structural reduction is observed, all relevant
preservation/failure checks pass, repository gate and independent review pass, and
matched elapsed/retained bytes are reported without claiming an unmeasured
throughput gain. A successful metadata reduction alone does not settle whether
the congressional application should use Update or an immutable full-leaf Build.

Selection finding: existing Update intentionally prepares and selects one native
dataset result; its public method does not expose a prepare-only branch operation.
The exact selected application-pin/unselected-native-candidate regression is
retained in congressional commit74cff4c9. Native `check_base` correctly refuses an
old selected base. Public preparation/selection composition is a separate design
question and is not changed by this metadata optimization.

## Scale revision before native candidate measurements

The user requested population, layout and history measurements rather than an
adoption judgment from tiny leaves. Preserve the small comparison above as
historical evidence; it is insufficient to decide the engine's value at scale.

Proposed scale matrix:

- Population: 1,000 / 8,192 / 10,000 / 100,000 generated rows, with one-row append,
  correction and deletion held fixed. Add 1,000,000 only if the 100,000-row run's
  elapsed time and storage high water fit the declared whole-experiment bounds.
- Files: hold 100,000 rows and change size fixed, vary initial file fragmentation
  through existing seed-only native limits, then reopen all timed arms with the
  same standard limits. Record actual file counts, not requested approximations.
- History: hold 8,192 rows and the exact source table pin fixed; prepare 0 / 8 / 32
  empty native Updates to grow lifecycle history without changing source files.
  Assert exact source-pin identity after each history step before timed changes.

The application currently bounds a state leaf at 8,192 rows and 16 MiB decoded
bytes. Cases above that bound test native dataset layout; they do not describe a
permitted application leaf. Keep application leaf and engine scale conclusions
separate.

Arms are the frozen baseline independent Build, baseline Update and optimized
Update. Begin one screening repetition, then use three matched repetitions near
any crossover or material tradeoff. Interleave arm order between repetitions.
Include producer preparation as a separately timed stage, then native operation
time and independent correctness readback separately. Generate Parquet through
bounded batches, preserve nested duplicates/null/empty binary values and maximum
unsigned integers, and never acquire real source records.

Stop at 30 minutes for the whole experiment, including generation, history and
readback, or 4 GiB additional retained fixture data,
and before free space drops below 8 GiB plus declared native scratch. Record
per-operation sampled process-tree RSS, fixture logical-size/free-space high
water, temporary file observations, retained bytes, command counts, actual native
scan/write/reuse counters and catalog table registrations. Distinguish native
requested-range observations (where exposed) from unavailable full physical
disk/network bytes and catalog/backend request totals. Report failures and their
costs. Parent coordinates an exclusive quiet window after tests/builds; results
from overlapping background workloads do not qualify timing.

Decision rule revision: the minimal engine metadata correction can be judged on
preservation and measured removed metadata work. Application Update adoption
additionally requires the realistic leaf measurements and a separate resolution
of application/native selection composition. Neither a tiny relative loss nor a
larger-dataset relative gain alone settles that application decision.

## Structural and preservation checkpoint

The production change retains the existing native writer, source FileIO, schema
field IDs, protected attempt path, distinct-key files and selection checks. It
constructs the writer's Iceberg metadata in memory rather than registering an
unused catalog table. Metadata serialization still receives the original bounded
size check and two-times metadata memory reservation.

The new structural assertion failed on baseline because `keys/metadata` existed.
It passes with the correction: a direct replacement creates only result, removed
and added tables. A full two-row CLI correction creates four native tables instead
of five and zero key metadata JSON files instead of one; both versions report
identical four written data files, 23,111 written bytes and two source-file scans.
The CLI smoke validates exact old/new schema, values, ordinals and old object
hashes; its timings overlap other work and do not qualify performance claims.

Local native verification:

- Focused writer tests: seven passed. Update/evidence-update integration tests:
  thirty passed.
- Repository `scripts/check`: formatting and all-target/all-feature Clippy pass;
  346 library, 313 integration, 15 restore and two worker tests pass. Existing
  qualification-only tests remain ignored.
- The failure/resume test now proves that an interrupted processing attempt owns
  its unregistered key files through the original unretired protection record.
  Those files remain byte-identical after retry, and admitted inputs support retry
  after original producers disappear. This does not claim that failed-attempt key
  files are reused: retry may regenerate them in a new protected attempt, as
  before this correction.
- A supplemental ignored query-metrics probe passes separately, followed by final
  formatting and all-target/all-feature Clippy. It repeats the exact prepared-key
  view/join pattern against an in-memory left input, isolating key-side reads:
  1/10/100 independently executed probes report 47/470/4,700 DataFusion
  `bytes_scanned`. These are key-payload query metrics, not complete filesystem,
  catalog or backend request counts.

Evidence is retained in `output/update-efficiency-20261020/` under this worktree.
Frozen candidate SHA-256 is
`20a9fd8202c009696c9b2b9455bf54b5634cb9b6e9843f042081b72740993529`.
The protected baseline remains
`817831a55c4273d79b8a9653a0a0248caa3648242b7f5cbb6200164c62864dbb`.

## Algorithm assessment before scale timing

Let R be source rows, F source files, C files containing affected keys and K
distinct affected keys. The current implementation probes every source file's
key columns and then scans changed files again for complete replacement. It
materializes DISTINCT once, but the prepared Parquet relation is read by each of
the F probes and C rewrites. Work is approximately O(R key values + (F+C)K +
changed-file payload), plus retained lifecycle work. Unchanged payload files are
shared; all-file key discovery remains. Wider payloads can therefore benefit
from reuse even when discovery still grows with source rows.

On this implemented path, redundant persistent metadata improves from C to B:
one demonstrably unused registration/file is removed, while necessary result and
change metadata remain. Key-discovery algorithm stays C: this patch does not
change its growth rate. Local interruption preservation stays A-minus: exact
ownership and retry checks pass, but this is local qualification. Speed and
application-adoption grades remain pending the frozen scale comparison and
independent review.

A later conservative file-bounds planner could reject only known-impossible
files before key probes. Missing, truncated, ambiguous or unsupported bounds must
fall back to scanning. Iceberg 0.10.1's metrics evaluator is private; its public
filtered scan planner may help, but occurrence readers must retain original
whole-file descriptors to preserve physical row locators. Large IN predicates
also stop pruning conservatively beyond the dependency's 200-value threshold.
No pruning or selection-policy change belongs in this metadata correction.

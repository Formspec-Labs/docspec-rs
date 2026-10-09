# Full local Comments acceptance

Frozen before collection. No measurements from the source-only pilot count toward
this gate. This protocol exercises the full local flow in `docs/PLAN.md`; it does
not establish remote publication, source acquisition, maintenance, restore, or the
separate unchanged/update/backfill/interrupted acceptance cases.

**Decision:** Can the current local engine preserve the complete frozen Comments
population and evidence through shaping, real docket joins, summaries, native and
flat output, selection and exact independent readback within the accepted resource
and time limits?

**Hypothesis:** Retained phase boundaries, maintained columnar processing and
native contributor relations make the complete consumer path fit those limits
without reducing values, multiplicity, evidence, or independent verification.

**Arms:** The current release binary in three distinct fresh workspaces. Each run
must finish within 1,800 seconds, including every admission, shaping, seal, join,
summary, evidence write, export, selection lookup and independent oracle. The
global deadline does not restart for each command. A `pilot` executes the identical
full workload once and is explicitly incapable of passing acceptance. No automatic
retry replaces a failed observation; the series stops after a failure and retains
its outputs, with later repetitions reported as not run.

**Cases:** Reuse `../comments-input/preparation-20261008.json` and its exact retained
10-million-row Comments file. Preserve all original full-width strings, literal
JSON, nulls, empty values and duplicate occurrences. Add nullable signed 64-bit
UTF-8 byte lengths of `comment` and `text_content`; do not replace either value.
The systematic source selection is inherited from the preparation receipt. It is
not a claim of statistical representativeness for every agency or future input.

Use the Dockets generation pinned in `workload.py`, with its complete original
`dockets.parquet` and `etl_receipts.parquet`. Explicit preparation verifies the
small member payloads against the retained SHA-256 manifest and computes their
DocSpec BLAKE3 identities together in one bounded read. During preparation, the
frozen Comments identity is reused without an extra payload hash. Each timed
fresh admission must establish its admitted copy's identity. Independent readback
also hashes that admitted copy once against the frozen BLAKE3 identity, then uses
the retained copy for all expected reconstruction. Preparation and release
compilation are separately recorded and outside the engine measurement. Source
acquisition is outside this workload.

**Evidence scope:** Preserve every nested docket receipt field as a separate
native dataset, including the observed receipt as well as accepted receipts.
Declare Dockets and receipts as coupled members and seal their exact versions.
The join consumes the exact Dockets release and retains that generation seal.
This is an explicit generation-level association. It does not claim that
`record_id` is `docket_id`, decode per-docket receipt identities, or claim a
per-docket receipt link. Receipt IDs use the producer's frozen identity encoding.

Comments provenance binds the retained whole input, source snapshot and physical
selection receipt. Preserve its evidence-related JSON strings byte for byte as
values. Separate original Comments ETL receipts and referenced attachment/source
bodies are not supplied by this frozen input and are not claimed as retained or
verified. Full join and summary contributor relations are generated and checked
independently over every native occurrence; these are additional evidence of the
local processing, not substitutes for unavailable upstream bodies.

**Processing and outputs:** Join Comments to real Dockets using a left equality
join on `docket_id`, with nulls never matching and many-to-one cardinality. Carry
all Comment fields and lengths, plus every docket field with a `docket_` prefix.
Preserve unmatched Comments and report matched/unmatched counts; at least one
match is required, and output cardinality must remain exactly 10 million. A
cardinality refusal remains a failure; do not silently deduplicate Dockets.

Summarize the joined occurrences by Comment `docket_id`: count occurrences,
matched dockets and known Comment text; sum both byte lengths exactly in
Decimal128(38,0); report largest Comment byte length. Null docket IDs form a
group. Emit native results and complete contributor tables. Export every user
column of the joined and summary results to flat Parquet in the declared order.
Keep nested receipts in their native table because the current flat exporter
supports primitive fields. Export selection excludes internal occurrence fields.

**Independent readback:** PyIceberg resolves exact snapshots with `plan_files`;
PyArrow checks file footers, schemas, field IDs and membership. Unexpected delete
files or partial-file tasks refuse rather than bypassing Iceberg semantics. Never
call `to_arrow()` over the full population. DuckDB reconstructs shaping, join,
summary and contributor relations independently and compares a signed `UNION ALL`
grouped by every value column, requiring zero summed multiplicity difference.
These comparisons preserve duplicates and null/empty distinctions. Exact
schema checks precede comparison so SQL coercion cannot hide a type change. All
nested receipt values are compared, including list order. Neither sampled rows
nor 64-bit checksums establish equality. No global row ordering is promised by
the workload; typed nested order and literal JSON are preserved.

Compare the native source values with the verified admitted input, native joins with the
independent join, summaries with independent aggregates, each evidence table with
its full expected contributor relation, and flat exports with their exact native
columns. Exported Parquet field IDs and every `ExportPin.fields` entry must match
the selected native fields in the declared order, including name, type and
nullability. Verify serialized release manifests, generation members, input coverage
and the current catalog's native/export pins. SQL processing failures or memory
exhaustion remain failed attempts; no smaller comparison is substituted.

**Held constant:** Release binary and source/lockfile identities; full inputs and
requests; 12 compute threads; 24 GiB aggregate process-tree RSS ceiling; 32 GiB
combined engine/oracle scratch ceiling; 4,096-row batches; 768 MiB batch allowance;
32 MiB metadata allowance. The batch allowance retains the earlier diagnosed
profile for unusually large individual Comment values. DuckDB's own memory
setting is 18 GiB, leaving headroom under the separately observed process limit.
Its spill directory is included with engine scratch in the global monitor.

Record the operating system, interpreter/package versions, machine memory,
process list, load, source/worktree/binary/lock/protocol/harness identities and
operator background-load note. Stop unrelated builds, tests and benchmarks
before collection. Do not clear OS caches or call these cold-cache runs. Fresh
workspaces clear application reuse only. Record preparation and build costs
separately; retained source and previous failed results are not deleted.

**Disk admission and accounting:** Before starting, declare
`--retained-bytes-per-run` from current capacity planning. This is an allowance
with uncertainty, not measured output size. Require available disk for all planned
retained runs, 32 GiB scratch and 2 GiB reserve. Account for old/new coexistence;
stop if the declared run allowance or peak additional allowance is exceeded.
The harness never cleans a failed or completed workspace to make the result fit.

Reuse the source pilot monitor through optional parameters that preserve its old
default limits. Sample process-tree RSS and free space each second, and directory
logical/allocated bytes every five seconds plus stage boundaries. Include the
runner and all descendants. Retain `/usr/bin/time -l` peak child RSS as a second
observation. Short-lived aggregate memory or disk peaks may escape sampling;
these are observed bounds, not proof of continuously enforced limits. Monitor
inventory cost is recorded. Directory allocation on APFS does not prove unique
physical extents; volume free-space change includes unrelated workstation work.

Keep each command's arguments, exact specification, stdout, stderr, exit/stop
reason, measurements and before/after inventories. Keep incremental oracle check
receipts so an interrupted comparison has a known last completed check. Report
global/stage elapsed time, RSS, sampled scratch/peak additional disk, retained
logical and allocated bytes, matched/unmatched rows and storage amplification
against the frozen input total. Payload request/read-byte counters unavailable
from this instrumentation remain unknown; filesystem block counts are not a
claim of avoided cached reads.

**Decision rule:** Accept only if all three fresh runs complete every stage and
exact check within 1,800 seconds each, with observed resources and retention
inside declared allowances. Any missing stage, type/value/evidence mismatch,
population reduction, refusal, timeout, out-of-memory failure, or exceeded bound
means the full gate did not pass. Keep original criteria and append any later
revision with its reason before collecting revised results.

Pre-collection implementation choice: use one signed union/group comparison
instead of the initially proposed symmetric `EXCEPT ALL`, avoiding two reads of
each relation per check. The synthetic harness cases cover unequal duplicate
counts, null versus empty text, and nested list ordering. For initial native
files without stored origin columns, unique file paths and physical row numbers
establish occurrence identity without an additional full-population grouping.
No measured result or acceptance criterion changed with these choices.

Pre-collection oracle clarification: declared admission digests and an available
external fixture cannot establish that original bytes remain retained. The timed
oracle now checks admitted file presence/size, computes its BLAKE3 once using the
maintained Python package, and reconstructs expectations from that verified file.
This independent custody check is a required trust boundary; it is distinct from
normal reads. Small-input preparation computes SHA-256 and BLAKE3 in one pass.
Flat-output verification additionally checks Parquet field IDs and the complete
serialized field mapping; equal decoded values/types alone do not suffice.
Synthetic regressions retain external fixtures while removing or altering the
admitted copy, and change flat field IDs without changing values/types. These
checks were added before collection; no result has been reclassified.

Pre-collection execution clarification: each series captures one executable and
the oracle with its `workload.py` and `integrity.py` siblings in a new read-only
`frozen/` directory before any timed run. Every engine subprocess uses that
executable; every readback subprocess uses that oracle. The runner compiles its
local workload, integrity helper and monitor from captured source bytes and keeps
those exact bytes, plus its own source captured at load, as evidence. The oracle
does not use the workload module's repository-relative preparation paths.

Capture hashes the executable during its one copy, records original and captured
file identity/statistics, and refuses observed source replacement or mutation
during copying. Loaded source bytes are retained without rereading the worktree.
There is no per-stage executable hash. Treat the experiment-owned read-only
directory as immutable for the series; this protects against ordinary worktree
edits, not hostile writers. Keep the dirty Git diff, source commit/status, lockfile,
protocol, prepared input record and the producer's build receipt alongside these
copies. Preserve the build receipt verbatim and match its binary identity to
`execution-capture.json` before interpreting the series; the harness does not
invent or validate a build-attestation schema. Capture cost is separately recorded.
Synthetic cases replace the original executable and Python sources after capture
and require the saved subprocesses to retain their original behavior; observed
mutation during capture must refuse and retain the failed copy.

## Commands

Use a Python environment containing `pyarrow`, `duckdb`, `pyiceberg`, `psutil` and
the maintained `blake3` package. Execution additionally needs an already-built
current release binary. The runner installs and builds nothing. Preparation
performs the explicit small-input verification without invoking the engine:

```sh
python experiments/comments-e2e/run.py prepare --output experiments/comments-e2e/prepared.json
```

Run the full gate with a capacity allowance chosen before collection (bytes):

```sh
python experiments/comments-e2e/run.py run \
  --inputs experiments/comments-e2e/prepared.json --mode acceptance \
  --build-receipt "$RELEASE_BUILD_RECEIPT" \
  --retained-bytes-per-run "$RETAINED_RUN_ALLOWANCE_BYTES" \
  --background-load-note "Unrelated builds, tests and benchmarks stopped."
```

Use `--mode pilot` only for one identical full-workload diagnostic. It cannot pass
acceptance. `test_workload.py` contains synthetic harness checks; those do not
qualify the corpus or establish resource/performance results.

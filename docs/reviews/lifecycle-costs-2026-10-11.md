# Bounded local lifecycle cost observations

The fixed and growth runs passed their exact value/type and retained-phase
checks. These are small local diagnostic observations, separate from the accepted
10-million-Comments gate. They do not establish a general performance limit or
complete Q1/Q2/Q7 qualification.

Evidence is retained under `experiments/lifecycle-costs/`:
`results-fixed-2026-10-11/`, `growth-2026-10-11/` and
`overlap-2026-10-11/` and `q1-2026-10-11/`. Each fixed/growth `preparation.json` binds the executable,
runner, protocol and input hashes; `completion.json`, per-case `result.json`,
metrics, samples and exact native observations retain outcomes. The original
`results-2026-10-11/` failure remains: the harness received Darwin `EPERM` during
paused-child cleanup. The corrected driver kills the exact native child and
leaves the time wrapper alive to reap it; that failure was not an Engine refusal.

## Fixed-path costs

At 256 and 4096 rows, three independent workspaces per population ran fresh
Build, unchanged Build, completed retry, interrupted/resumed Update and scalar
backfill, with exact readback. Resume followed process termination after durable
processing; original request files were removed and retained phase key, revision
and full JSON stayed unchanged. Configured limits were 512 MiB memory, 64 MiB
scratch, two compute threads, 4096-row batches and 8 MiB batch/metadata bounds.

Median elapsed seconds across each population's three workspaces:

| Path | 256 rows | 4096 rows |
| --- | ---: | ---: |
| Fresh Build | 0.323 | 0.316 |
| Unchanged Build | 0.164 | 0.196 |
| Completed unchanged retry | 0.054 | 0.059 |
| Update resume | 0.310 | 0.274 |
| Update interruption + resume + read | 1.384 | 1.349 |
| Scalar resume | 0.269 | 0.268 |
| Scalar interruption + resume + read | 1.416 | 1.463 |

Interruption totals include the deliberate approximately one-second pause and
its detection. Fresh 256-row Build ranged from 0.308 to 2.511 seconds; retain
that outlier rather than infer a scaling improvement from these medians.

## Fixed-data growth and complete endpoints

The growth run held 4096 logical rows fixed, used 64-row batches, and otherwise
kept the same limits. Public operations produced these actual retained counts:

| Arm | Small endpoint | Larger endpoint |
| --- | --- | --- |
| Selected physical files | 1 | 64; canonical source inventory remains 1 |
| Retained results | 2, processing 1 | 9, processing 1 |
| Retained processing | 2, results 1 | 9, results 1 |
| Evidence-bearing results | 1 of 2 results | 8 of 9 results |
| Receipt-free selection control | 2 results, processing 1 | 9 results, processing 1 |

Requested history/phase/evidence depth was 1 versus 8. Evidence also creates
versions and processing records; its matched receipt-free control separates
selection growth, not every record family. Equivalent SQL created unselected
processing phases through real interrupted operations; no synthetic catalog
rows were inserted.

At the larger endpoints, fixed two-row Updates took 1.112 seconds for version
growth, 1.194 for phase growth, 1.730 for evidence growth and 0.950 for its control;
independent exact reads added 0.148–0.274 seconds. A separate connected sequence
measured scalar backfill (0.409), combined read (0.100), primitive export (0.315),
flat read (0.099), compaction (0.314), retention inspection (0.094), availability
retirement planning (0.088) and retirement (0.148). These are single observations,
not steady-state throughput or sustained overlap measurements.

## Resource accounting and limits

| Observation | Fixed runs | Growth run |
| --- | ---: | ---: |
| Sum of measured CLI elapsed seconds | 22.621 | 44.574 |
| Maximum time-wrapper RSS, bytes | 133,218,304 | 147,587,072 |
| Maximum sampled process-tree RSS, bytes | 134,529,024 | 142,245,888 |
| Final Engine workspaces, logical bytes | 6,500,754 | 19,615,731 |
| Final Engine workspaces, allocated bytes | 8,208,384 | 22,470,656 |
| Final entire evidence directory, logical bytes | 315,236,623 | 439,209,812 |
| Final entire evidence directory, allocated bytes | 318,611,456 | 445,485,056 |

The roughly 302 MB frozen executable/assets dominate fixed-run storage. Growth
also retains repeated diagnostic inventory output outside Engine workspaces;
that is measurement storage, not retained application payload. Sampled scratch
was zero in both runs; sampling cannot exclude short spill peaks. Minimum sampled
volume free space was 3,431,870,464 and 3,682,979,840 bytes respectively. RSS maxima
are separate observations and may occur in different commands. OS caches were
not cleared and unrelated host activity was present. Physical I/O bytes, request
counts and retry traffic remain unknown; work events do not supply those totals.

The additional actual correction/sibling-backfill/compaction overlap test passed
in 3.6474 seconds, preserving its 516-row literal oracle and retained completion
checks. Time RSS was 106,512,384 bytes; sampled tree RSS was 105,250,816 bytes;
no resource stop occurred. Its Engine reservation ceiling remained the default
24 GiB, while process observation used a 512 MiB stop. Runtime was shorter than
the five-second disk-inventory cadence and temporary directories were removed:
sampled zero disk/scratch is an **unknown peak**, not proof of zero spill.

The connected Q1 pressure case also passed: a real shared Budget, a wide Arrow
slice retaining its parent buffer, receipt mutation and a held conditional PUT
preserved exact selection, refusals and worker-lifetime admission. Elapsed time
was 5.5773 seconds including two seconds of injected occupancy; reported RSS was
175,767,552 bytes and sampled tree RSS 172,081,152 bytes. Engine memory and process
observation were both 512 MiB; Engine scratch was configured to zero. Sampled
private temporary-directory bytes were 24,710,995 logical / 24,952,832 allocated.
The monitor calls this directory scratch, but it aggregates transient input and
output files and does **not** establish Engine spill. `Q1_OBSERVATION` in exact
stderr binds the admission assertions. Retain the initial compile-failure log
and its narrow owned-Schema correction alongside the passed collection.

These runs close the bounded required local observations represented by their
protocols. Sustained Q2 input/backfill/compaction overlap, related join/summary
costs, wider independent data growth and physical I/O where available remain.
They do not establish whole-provider resource or universal capacity promises.

## Retained evidence

The [archive inventory](../../experiments/lifecycle-costs/evidence-archives.json)
binds lossless compressed copies of every raw observation, including the failed
first attempt, frozen protocols/drivers/input assets, read outputs and build
identity. It excludes rebuildable executable bytes and Engine workspace trees;
all original directories remain locally available. Archives are copies of the
original evidence, with no rerun. This keeps diagnostic inventories out of the
loose repository tree and distinguishes measurement storage from application
storage.

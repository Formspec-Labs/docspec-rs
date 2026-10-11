# Original Python DocSpec versus Rust: bounded native-table comparison

Frozen before collection, 2026-10-11.

Decision: measure the cost of turning the same retained table into a persistent,
queryable native state, retrying unchanged work, and reading it again. This is a
bounded table comparison, not the full Comments acceptance workload or a language
microbenchmark. Production code will not change.

Hypothesis: Rust's columnar creation and metadata-only retry reduce initial and
repeated work. A slower equivalent path or increased retained storage weakens the
claim. Report each measure separately; no universal speed claim or adoption gate.

Arms:
- Original Python DocSpec 168079e53474c403030d93a70c8a5f6093574f9d, installed
  offline from its locked project into an isolated experiment environment.
- Optimized Rust e40be018b2a1c12c2e1067ba4fe552de3a6711f6, the existing immutable
  executable from the accepted Comments series. This is an older revision, not
  current HEAD; later local optimizations and correctness changes exist.
- Current Rust 9d01704983a1400ef80c40ae3341ee60a61af291 debug executable, diagnostic
  only. Do not call its timing production performance or attribute differences
  from the older executable solely to source changes.

Cases: real retained Bill Actions (20,000 rows) and 25 evenly spaced complete row
 groups from the retained 10M Comments selection (nominally 102,400 rows). Preserve
all selected user values and add a benchmark-only unique text row key to both arms
so duplicates do not violate the Python keyed-state model. This is a deterministic
coverage sample, not a statistical population sample. Preparation writes one
shared zstd Parquet input and seals a new explicitly experimental Rulespec
single-table generation. No claim of full producer-family or upstream receipt
coverage. Both arms bind these exact input bytes; Python also retains the small
required generation documents. Inputs and binaries are hashed once in preparation;
large pre-existing Comments identity comes from the retained receipt and file
metadata, not a repeated full scan.

Path: original uses CoreWorkspace.admit_generation then open_state().table();
Rust uses run with SELECT * FROM input, then read. Both stream all selected columns
to Arrow IPC. Measure initial native admission, first native read, same-request
retry, and close/reopen pinned read. Python reuses its live workspace for retry;
Rust uses a fresh CLI process per call. Report that startup asymmetry. Python
creates row membership and identity indexes and registers Parquet without rewriting;
Rust keeps physical occurrence identity and writes a native table. These are their
real implementations of the shared user outcome, not identical internal work.
Different portable export formats, joins, updates, history, recovery and remote
publication are outside this comparison.

Held constant: 3 sequential repetitions per case, fresh isolated workspaces,
rotating arm order, identical frozen inputs, 4,096-row read batches, 6 GiB engine
memory setting, 12 GiB sampled process-tree ceiling, 2 GiB minimum free disk,
512 MiB per-run retained/scratch allowance, 180 seconds per worker. Python's
maintained writer uses one thread for correctness; Rust uses its reference 12
threads. This compares complete implementations/configurations. Cap auxiliary
Arrow/BLAS threads to one. Do not clear OS caches or call runs cold-cache. Capture
background load, native package versions, exact binary/source pins and commands.
No builds or other benchmarks during collection. Environment setup is excluded.

Checks: independently decode the shared input once and each returned Arrow stream,
require exact column order/type/nullability and every row/value after sorting by
the benchmark key. This retains duplicate user rows and null/empty distinctions.
Oracle time is separate from service latency and also reported in total worker
elapsed. Unchanged retry must return the same immutable state/groups. One read of
each output is sufficient; do not rehash/read the original source after every run.

Resources: reuse the existing Comments command monitor (process RSS/free-space at
1 second, directory logical/allocated bytes at 5 seconds and boundaries), plus
/usr/bin/time -l. Run workspaces include Arrow handoffs and temporary directories;
report retained engine bytes separately from readback artifacts. Sampled peaks
may miss short spikes. APFS allocated blocks do not establish unique physical
extents and volume free-space changes include unrelated workstation activity.
Record cold whole-worker elapsed including imports separately from timed phases.
No physical read/write byte or network-request claim without instrumentation.

Decision rule: publish medians and ranges for passing repetitions with exact
output equality, and ratios only between the Python and optimized Rust arms.
Retain every failed/refused/timed-out attempt. A failure is not a slow valid result.
If capacity or correctness prevents collection, report the bounded result and the
specific missing comparison. Stop at the declared case/repetition/time/disk bound.

Pre-collection storage clarification: Arrow IPC handoffs are disposable read
outputs. After each handoff passes exact independent comparison, record its size
and remove that experiment-owned handoff. Preserve admitted inputs, native states,
requests, selection pins, checks and all failure output. Decode the shared input
once per worker for both checks. This avoids retaining duplicate decoded copies
and leaves all selected results reproducible from the retained native workspace.
Sample boundary sizes before removal; include the handoff in peak disk accounting.

Pre-collection review correction: reuse a private copy of the existing command
monitor with its unrelated workload entry points removed. The only behavioral
change handles Darwin EPERM when signaling an already reaped process group:
accept it only after enumerating processes and confirming the group is empty.
A live group or inspection failure remains an error. Freeze this monitor with
the harness. This changes supervisor cleanup, not measured engine behavior.

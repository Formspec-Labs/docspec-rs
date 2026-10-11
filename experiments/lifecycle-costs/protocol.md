# Frozen bounded complete-path cost protocol

Decision: detect unnecessary computation or retained disk growth in supported Local unchanged Build, fixed two-row source Update, scalar backfill, and acknowledged processing interruption/resume. This qualifies complete-path diagnostic costs on constructed inputs. It does not establish universal product latency, independent Q7 metadata/history/evidence axes, or a new Comments acceptance result.

Hypothesis: unchanged material reuses the exact native groups/input; completed acknowledgment preserves exact native phase revisions; Update and scalar resume after durable processing do not recompute acknowledged processing. A violation of exact output, phase identity or observed work falsifies reuse. Timing alone cannot prove it.

Cases and order: populations 256 and 4096, each three fresh workspaces. Every population is generated from the literal formula in runner.population: duplicate Int64 keys, two null keys, null text, empty text, ASCII strings. Each workspace executes initial Build → unchanged Build → completed unchanged acknowledgment → exact read → Update replacing key1 with literal `(1,'fixed'),(1,null)` → kill at acknowledged processing → remove only external input/replacement producer files → exact same Update resume → independent Arrow source read → scalar `length(text)` → kill at acknowledged processing → same Backfill resume → independent combined read. Full old/new/history state remains retained. No catalog rows are fabricated, no retirement is used to improve disk results, no cleanup occurs. Small populations may have one native file, so the protocol does not require or claim unaffected-file sharing.

Correctness: compare exact literal row multisets, duplicate and null multiplicity, field names, declared Int64/string/nullable source schema and scalar numeric type/null/length. Scalar lengths are independently calculated by Python over the literal reconstructed source. Reconstruct Update by excluding all prior key1 occurrences and adding the two literal replacement rows, independently of engine change records. Native lifecycle key/revision/raw JSON snapshots come from the read-only maintained SQLite catalog schema and maintained PyIceberg table reader; all pre-resume phase rows must remain exactly equal. Fresh Update must report processing; fresh scalar must report backfill-file. Their resumes must report no corresponding processing/file/assembly events. Completed unchanged acknowledgment must preserve all native phase records and report no work. Keep raw stderr events; these are work events, not file-read/network-byte counts.

Held constant: exact supplied build receipt commit/features/binary SHA256; prepared byte-frozen executable, runner, protocol and sampler; fixture SHA256 and BLAKE3; codec; current filesystem; SQL; stable field IDs; runtime settings. Configure 2 threads,512MiB memory,64MiB scratch,4096 batch rows,8MiB max batch and8MiB metadata. The reduced batch/metadata limits deliberately fit explicit writer admission within this diagnostic memory profile; they replace the earlier draft's default64MiB/32MiB values, which could require a larger peak permit. No claimed product bound changes. Default scalar physical length may be Int32 or Int64; the literal numeric/null oracle is independent of either.

Bounds: 120seconds per CLI command,900seconds whole run,1GiB series logical retained bytes,2GiB live volume free-space floor and3GiB preparation preflight. The existing sampler owns TERM/KILL/reaping of private process groups. Import the frozen sampler normally. Its maintained `command(..., json_output=True)` default preserves existing JSON callers; pass `False` only for Arrow `read`/`read-combined` commands whose results are written to the requested file. No source-text rewriting occurs. RSS refusal messages report the configured memory bound in bytes, including this profile's 512 MiB limit. A tiny wrapper around its existing RSS observation detects DOCSPEC_TEST_PAUSED and sends SIGKILL only to the unique native child whose executable matches the frozen binary; the time wrapper reaps it and retains resource evidence. The first collection killed the whole group and encountered Darwin EPERM during repeated group cleanup; its failed evidence remains. Missing marker, other process error or ceiling violation fails and stops the series; no fresh retry discards a failure. One-second marker detection includes observation delay and paused occupancy in the interrupted attempt cost; this is not a measured exact commit latency or unknown native commit gap. Keep failed attempts/logs/outcomes.

Measurement: reuse experiments/comments-input/run.py::command/inventory/totals (the sampler already imported by comments-e2e). Retain argv,stdout,stderr,/usr/bin/time -l maximum RSS,process-tree sampled RSS,volume free space,logical/allocated disk,peak scratch,scan overhead,before/after inventories,all sample streams and exact request/release/native phase records. Sampling runs against each whole case directory, covering external producer copies, Arrow readback artifacts and host files; additionally retain host-specific lifecycle/native/admitted/object metadata categories. Series high-water estimate adds already retained other cases/frozen inputs to the current case peak. Report peaks as sampled lower bounds. APFS allocated block sums count directory entries separately; hardlinks/shared extents are not deduplicated, so allocated sums are not exclusive physical volume ownership. Volume free-space change is separate.

Record initial input bytes and selected source Parquet/all-object bytes, old/new coexistence, metadata/history categories and final retained case/series inventories. Update/scalar complete cost is interrupted attempt + resume + independent read command; Python oracle decode and read-only native snapshot observation overhead is outside these CLI totals and must be reported as harness overhead, not attributed to engine wall time. Do not call resume-only time complete Update/backfill cost. No network transport is involved, but missing native file-read/byte counters remain unknown.

Decision rule: exact oracle/phase/work failures stop correctness qualification. Resource ceiling failure stops with evidence. Successful cases permit reporting observed distributions/spread and retained disk ratios; there is no speedup or latency acceptance threshold. Two population levels isolate only source population under fixed two-row replacement; native file/history/phase/evidence counts naturally co-vary. Q7 independent axes and product targets remain unresolved. No10M gate is repeated.

Collection gate: root runs only after Cargo/provider jobs are idle and records background activity/load/free disk. Baseline collection environment includes process names/IDs and load; caches are not cleared, CLI process/catalog is cold but OS cache is uncontrolled. Background VM/server activity prevents a clean performance claim unless stopped or stable and explicitly labeled. Do not hide contamination.

Invocation (root-owned, no collection performed during preparation of this artifact):

```sh
python3 experiments/lifecycle-costs/run.py prepare --repo /Users/mikewolfd/Work/spicy-stack/docspec-rs --output /absolute/fresh/series --binary /absolute/current/docspec --build-receipt /absolute/build-receipt.json
python3 /absolute/fresh/series/frozen/runner.py run --repo /Users/mikewolfd/Work/spicy-stack/docspec-rs --output /absolute/fresh/series
```

Build receipt minimum JSON fields: `commit` exact git HEAD, `features` including `test-util`, `binary_sha256`. Supply a verified root build receipt; the driver performs no build. Runtime dependencies are existing pyarrow,blake3,psutil,pyiceberg. Preparation generates only deterministic tiny Parquet, captures sources and hashes once; it does not run engine commands or measurements. No download/provider call. `run` is explicit and terminal: completion record, including failures, prevents rerunning the same series. Runner syntax checked with Python AST only, not executed.

## Fixed-population growth and layout-cost supplement

Use a separate fresh series with `--growth-only` on both prepare and run. This
skips the six baseline cases and freezes one 4096-row profile; it reuses the same
binary, literal formula, ceilings, sampler, process ownership and terminal receipt.
The profile uses 64 batch rows in every arm so maintained rolling writes can
construct different physical fragment counts. Whole-profile deadline is 1800
seconds; per-command 120 seconds, series storage 1GiB and free-space floors stay
unchanged. No build or provider work occurs in this driver.

Fresh independent workspaces construct these arms through public CLI services:

- Fragment contrast: equivalent source compaction at 16MiB versus 512-byte target,
  same logical version/rows. Require observed output-file counts to differ; record
  actual counts rather than equating target size with a file count. Read through
  `read-combined` to exercise selected representation. Measure construction,
  verification and exact compaction recovery retirement.
- Retained versions: eight unchanged independent Builds with identical source and
  SQL. Their exact material phases remain reusable while result selections grow.
- Retained processing: eight distinct equivalent SQL texts paused after durable
  processing without selecting a new result. Kill only the owned native child;
  retain exact handoffs, then abandon pending operations before new source intake.
- Evidence depth: eight real receipt replacements on one exact source occurrence,
  compared with eight receipt-free identical-value selections. The paired control
  matches selection count, not all metadata shape. Receipt versions necessarily
  add parent/execution metadata; report this causal difference rather than claiming
  every native record family is independently fixed.

At depths1 and8 measure inspect/explanation, latest opening, reuse plan, exact
configured source read, and retention preview. Record actual current source rows,
canonical source file count, retained result and processing count, evidence-bearing
results, and exact native row/JSON byte counts; separately record selected
representation fragments. Retired-record counts use exact existing markers.
All growth arms then measure the same two-row key1 replacement (same values,
new occurrence identity), independent readback and exact processing retirement.
Their construction/retirement costs remain separate from lookup costs.

One additional fixed-source workspace measures scalar group creation, combined
read, authorized primitive flat export, exact read-export, source compaction,
retention preview and old standalone availability retirement. The oracle compares
literal duplicate/null/string values and independently calculated lengths. Receipt
keys come from the configured `read-occurrences` service; read-receipts verifies
exact final key/value at each evidence endpoint. Decoder/oracle and native catalog
snapshot overhead is outside CLI time and recorded separately.

The supplement measures supported Local construction and ongoing costs. It does
not simulate simultaneous updates, wide backing-buffer overlap, provider upload,
remote maintenance or Q6 fencing. Existing continuous overlap correctness tests
remain evidence; their aggregate resource observation can use the same sampler
against the already-built exact test executable. Q1 needs a separate genuinely
connected wide/receipt/slow-output case, not a larger progress-only batch.
Payload/metadata request and byte counters not exposed by the services remain
unknown; observed output JSON bytes and native record bytes are not physical I/O
counters. Peaks remain sampled lower bounds. No universal latency target or full
Q7 support follows from one1/8 contrast; report all failures and actual limits.

Invocation: add `--growth-only` to both invocation lines above and choose a new
output directory. Never replace or continue the earlier baseline collection.

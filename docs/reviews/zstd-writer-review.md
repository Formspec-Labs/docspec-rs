# Zstandard writer and resumed Comments profile review

**Disposition: approve for the frozen resumed experiment.** No material code,
compatibility, or experiment-design issue remains from this review. The corrected
fixture, full gate retry, and fresh release build passed. The measured resumed
run and end-to-end performance qualification remain pending.

Reviewed on 2026-10-08 using `AGENTS.md`, the applicable Microsoft Rust guidelines,
and the semi-formal review workflow. Scope: `src/storage.rs`,
`experiments/comments-input/resume-protocol.md`, `resume.py`, and the optional
readback-limit change in `run.py`, plus the later correction to
`tests/support/scalar.rs` and `tests/refresh.rs`. The reviewer ran no Cargo command
or large dataset job.

## Writer behavior and reuse

`storage::write_batches` adds only the maintained Parquet Zstandard setting to
existing writer properties (`src/storage.rs:178–185`). The installed Parquet
58.4.0 implementation defines its default Zstandard level as 1; Iceberg 0.10.1
passes the properties to `AsyncArrowWriter`. Schema/field matching, row groups,
byte admission, rolling, and error propagation retain their existing paths.

Completed work should remain reusable. Admission keys bind exact input digests,
and build phase keys bind the processor, input, SQL, definition, and eligibility
(`src/engine.rs:1158–1183`). Encoding changes physical bytes without changing
these processing semantics. Existing immutable pins already identify their exact
objects. Keeping the processor identity avoids needless recomputation or rewriting
valid uncompressed results. A failed processing attempt has no completion to reuse;
the engine creates a new protected attempt while retaining its completed admission
and earlier partial outputs (`src/engine.rs:710–758`, `reserve_attempt`).

## Resource and evidence scope

The retained diagnostics consumed the entire input under each frozen scan setting.
They observed a row with 92,992,878 string-value bytes and a maximum reported
plain-string batch allocation of 606,894,640 bytes. The 768 MiB allowance therefore
has a concrete basis for this workload. The generic 64 MiB default stays unchanged.
The larger allowance also changes the rolling target, reserves 3 GiB per writer,
and reserves 1.5 GiB for native reads/sampling. These reservations fit the existing
12 GiB application half, but do not establish actual aggregate memory use or
successful execution. The unchanged 24 GiB process monitor remains necessary.

The codec fixture's exact readbacks and lower retained Zstandard size support this
candidate. They do not establish corpus-wide compression ratios, stable timing,
or complete-path performance. The protocol preserves that distinction and retains
the failed original attempt and all diagnostic arms.

The driver checks the original unselected authority and completed admission,
resumes the same operation, verifies exact admitted-input reuse, checks committed
selection, and invokes independent readback with the revised limits. Unchanged
and exact-retry arms check stable selected identities and retained files. It reports
original failed time plus resumed processing/selection/readback time, marginal
and cumulative retained space, and keeps full acceptance and payload-read-avoidance
claims false. The original durable runtime record describes the initial attempt;
the resumed binary and limits are separately retained in this experiment's evidence.
This remains a resumed implemented-slice run, not a fresh full acceptance run.

## Fixture correction and validation closure

`docs/reviews/zstd-writer-check.log` reported a failure at `tests/refresh.rs:117`:
the test expected an old physical source file to remain selected. Its compressed
fixture produced two source files, and changed keys occur near both the beginning
and null-bearing tail. The final fixture preserves its IDs, row population, nulls,
duplicate cases, batch size, and byte allowance. It extends each generated text
value with a second distinct deterministic hash block, supplying enough compressed
data for an untouched middle file. Expected text lengths continue to derive from
the original Rust values, rather than a hard-coded former length.

The refresh test now requires at least three starting physical files. Its original
assertions still require an old source path to survive, new files to appear,
computed work only for the new files, no repeated admission/processing, exact
result values, and unchanged retained completions. This preserves the test's
meaning rather than weakening reuse coverage. The reviewer inspected the passing
`docs/reviews/zstd-refresh-fixture-retry.log`, which records three initial files
and a successful targeted test.

The earlier attempt to lower the fixture allowance to 8 KiB failed with a
14,120-byte decoded batch and was reverted; its evidence remains in
`docs/reviews/zstd-refresh-fixture.log`.

The reviewer inspected the completed `docs/reviews/zstd-writer-check-retry.log`:
the gate reaches its final doc tests with all reported suites passing, including
the refresh suite. The separate `docs/reviews/zstd-release-build.log` records a
successful optimized release build. The main task reported exit status 0 for both
commands. Recomputed hashes for every reviewed source, driver, protocol, and test
file still match the identities below. This closes the validation item without
claiming a measured resumed-run result. Debug-cache reclamation has its separate
`docs/reviews/zstd-debug-cache-cleanup.log`; it is not dataset storage savings.

Reviewed SHA-256 identities:

- `src/storage.rs`: `8c24bfa10bc89b8fbce7440da1fd9678155973948296ea2380402d9685dcabb6`
- `resume.py`: `ce96a0a03045e5af43bd3f617339129c31dc21e7ab84d7f7365c2adf22b58ac3`
- `resume-protocol.md`: `b59f8c0eaab4f43d79ef8bc61a0bc0e48be4db3e2d33fa212e9d7270ccf08e19`
- `run.py`: `c888995572964b3dc2b59cdf841b5e142f984e2974b30de034ab83885995ddd2`
- `tests/support/scalar.rs`: `23bfccf1cc288d6d721d404a6e0492e12f5e1551b2bf3201d9753db468969960`
- `tests/refresh.rs`: `bf44831977d33f9e71ab0295e16400d2ae219309640dbf619a17730723d5974d`

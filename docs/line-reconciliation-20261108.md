# DocSpec engine: back to one line

Status: proposal, 2026-11-08. Investigation and in-memory dry runs only. Nothing
was merged, pushed or re-pinned, and no consumer pin changed.

## Bottom line

- The engine has four committed lines and one uncommitted one. Every committed
  line forks from `main` at `5a7fa9f`, and none is behind `main`. The shape is a
  star, not a tangle.
- **Stage A, now:** fast-forward `main` to the DDK line, then merge the spill
  backport. The dry run merges with no conflicts, and the lock file resolves
  offline with the pinned toolchain. Every commit any consumer pins becomes an
  ancestor of `main`, so **no consumer has to re-pin**. `job-plan` stays.
- **Stage B, after ticket ss-x8fh qualifies:** merge DataFusion 55 into `main`.
  The dry run conflicts in 6 files. One of them is real design work: DDK's
  retained-scratch custody sits on the scratch writer that DataFusion 55
  replaces. Stage B also removes the vendored DataFusion crate.
- Use merges only. A rebase, squash or cherry-pick would orphan pinned commits.

## 1. Inventory

All pairwise merge bases are `5a7fa9f` (main, 2026-11-02). `5a7fa9f` differs
from the release base `5dae611` only in `.github/workflows/linux-release.yml`.

| Line | Tip | Ahead of main | Adds | On origin | Pinned by |
| --- | --- | --- | --- | --- | --- |
| `main` | `5a7fa9f` | 0 | (base) | yes | no one directly |
| `fix/spill-view-gc` = `release/5dae611-spill`, tag `docspec-5dae611-spill` | `b0342a7` | 2 | Vendored DataFusion 53.1.0 `datafusion-physical-plan` (byte-identical to crates.io), then the apache/datafusion#21633 spill compaction backport with an Arrow 58 guard for arrays of 2 GiB or more. Test `budget::tests::native_spills_write_view_columns_by_referenced_bytes`. 122 files, +90,856 lines, almost all `vendor/`. | yes | congressional-tech `main`; Congress DDK replacement branch |
| `fix/datafusion-55` | `fcb5bf2` | 1 | DataFusion 55.2, Arrow/Parquet 59, iceberg-rust git `5b82ab99`, datafusion-iceberg git `726e4208`, OpenDAL 0.58.2, Rust 1.95.0. Replaces `ScratchWriter` with DataFusion's `SpillFile` accounting. Keeps the spill test. 48 files. | yes | no one |
| `codex/ddk-docspec-runtime`, local tag `ddk-v0.1.0-dev.2` at `b1270b2` | `62265f9` | 18 | Guarded SQL, saved Substrait plans, managed quality, `job-plan` / `prepare-job` (schema-only planning), retained native jobs, snapshot custody, producer adoption and proof, publication intents, S3 snapshot source. 87 files, +21,990 lines. Code at `62265f9` equals `255c184`; the last three commits only change `docs/STATUS.md`. | **no: the only copy** | DDK releases; SpicyRegs and Congress DDK replacements |
| Uncommitted, worktree `docspec-df55` (Nov 2) | on `fcb5bf2` | n/a | `src/job/json_strict.rs` (210 lines) keeps the typed JSON getters strict as in functions-json 0.53. It covers the MODS descriptor difference in ss-x8fh. Also `fetch-depth: 0` in `linux-release.yml` for congressional-tech's gate-5 producer-proof tests. | **no: not committed** | no one |

### Consumers

| Consumer | Pins | Recorded in | How the binary arrives |
| --- | --- | --- | --- |
| congressional-tech `main` | `b0342a7`; darwin `fed9ea73…`, linux `9e5cc34f…`; needs `request-deadline`, `owned-stdin-eof` | `model/docspec-runtime-pin.json` (commit `7743247e`) | Local: shared read-only store `<main>/.cache/docspec-runtime/<platform>/<sha256>/docspec`. Hosted: the `DOCSPEC_RUNTIME_ARTIFACT` variable names a private-bucket descriptor. |
| Congress DDK replacement (`codex/ddk-congress-replacement-20261106`, on hold) | Repository pin `b0342a7`. Its DDK jobs ran on `255c184` / `e2ed2646`. | Gap G06 in `docs/replacement-ddk/runtime-gaps.md` names this split as an open obligation | same as above |
| DDK (data-design-kit) | dev1–dev4 `b1270b2` (`b89b43e2…`); dev5 `d96e001`; integrated-2 and -3 (SDK2, SDK3) `255c184` (`e2ed2646…`) | `docs/local-release*.md` | Executable path plus SHA-256 given to the adapter. Local release only. |
| SpicyRegs DDK replacement (`codex/ddk-spicyregs-app-20261106`, on hold) | SDK2, so `255c184` / `e2ed2646` | stop handoff; `SPICY_REGS_DOCSPEC_EXECUTABLE` and `_SHA256` | Environment variables |
| SpicyRegs `main` | No revision pin. Calls only `docspec hash` | operator-supplied binding JSON (`docspec_exports.py`) | Absolute path |
| spicy-docs, spicyengine, spicysearch | None. spicyengine vendors the separate Python DocSpec 0.12.4 wheel | n/a | n/a |

### congressional-tech's Linux pin flow

1. docspec-rs `.github/workflows/linux-release.yml` runs on a push to `main` or
   `fix/**`, or by manual dispatch. It does not run for `codex/**`, so the DDK
   line has never had this CI. It builds `--release --features remote` in
   `rust:<toolchain>-bookworm`, strips the binary and refuses 1 GiB or more. It
   uploads artifact `docspec-linux-amd64-<sha>`. Its qualify job then runs
   congressional-tech's `production-offline` lane in 6 shards with that binary.
2. The darwin binary is built and stripped by hand on this Mac. For example,
   `.cache/runtime-b0342a7/darwin-arm64/stripped/docspec`.
3. A re-pin edits `model/docspec-runtime-pin.json`: the source revision and both
   SHA-256 values. Both capabilities are still advertised in `src/main.rs` on
   every line.
4. `scripts/dev.py write-local-runtime … --binary <docspec>` adds the binary to
   the shared store. It refuses bytes the pin does not name. Store entries are
   never replaced.
5. `scripts/publication/publish-docspec-runtime.py --bucket
   congressional-tech-runtime` writes the descriptor. The lead uploads with
   `--upload --expected-descriptor`. Never use the public `congressional-tech-raw`
   bucket.
6. The lead sets `DOCSPEC_RUNTIME_ARTIFACT`, keeping the old value for rollback.

## 2. Why the lines split

- **Spill backport (`b0342a7`).** congressional-tech's `capture_dup` GROUP BY
  spilled 53.4 GB, because Arrow IPC rewrote shared view buffers for every
  spilled slice. The fix exists only in DataFusion 54 and later, and
  iceberg-datafusion 0.10.1 capped DataFusion at ^53.1. On 2026-11-02 the owner
  chose to "backport for release, upgrade after" (ss-vuwa). The backport went
  on a release branch so `main` could take the upgrade instead of carrying a
  vendored crate (commit message of `fcb5bf2`). After it, spill fell to 383 MB.
- **DataFusion 55 (`fcb5bf2`).** This is the "upgrade after" half. It stays off
  `main` because congressional-tech's lane failed on it: 122 tests in CI run
  41519693446. DataFusion 55 coerces UNION types differently
  (apache/datafusion#27023). In a local lane, the union casts on
  congressional-tech branch `df55-types` `2ec631b9` cut 90 failures to 21: list-leaf and writer-column names, the MODS outcome, a review
  diagnostic and publish-job behavior. ss-x8fh (open, P1) owns them, and keeps
  `b0342a7` as the runtime until the upgrade qualifies.
- **DDK runtime (`62265f9`).** It was built from 2026-11-05 to 11-07 for the DDK
  replacement program. It branched from `main` because `main` was the
  documented line; that `main` had no spill fix. It was never merged: DDK
  qualifies exact local commits with local tags, and ss-qkiw (P3, deferred)
  tracks it as "unpushed, only copy". `codex/**` is outside CI, so nothing ever
  checked it against congressional-tech.
- **Uncommitted DataFusion 55 work.** This is in-progress stage-B work from
  2026-11-02 that was never committed.

Each split had a sound local reason. The common cause is that `main` stopped
moving: the release fix and the DDK work each landed on a side branch, waiting
for an upgrade that is still blocked.

## 3. Proposed single line

The target branch is `main`. Use merge commits or fast-forwards only, so every
pinned commit and tag stays reachable. Nothing moves until you approve.

### Stage A: collect (now)

| Step | Action | Dry-run result |
| --- | --- | --- |
| A0 | Push `codex/ddk-docspec-runtime` and tag `ddk-v0.1.0-dev.2` to origin as a backup. Commit the `docspec-df55` work on `fix/datafusion-55`. | not run (needs approval) |
| A1 | New worktree and branch from `62265f9`. This is a fast-forward of main's content, because the DDK line is 0 behind. | n/a |
| A2 | `git merge --no-ff fix/spill-view-gc` | **No conflicts.** Cargo.toml, Cargo.lock and `src/budget.rs` merge automatically. `cargo metadata --locked --offline --all-features` on Rust 1.94.0 succeeds, with one `datafusion-physical-plan` (vendored). `datafusion-substrait` 53.1 uses it through `datafusion`. |
| A3 | Run the checks in section 4, then push as `fix/one-line-20261108` so `linux-release` builds it and runs congressional-tech's lane. | not run |
| A4 | Fast-forward `main` to the A2 merge and push. Keep the tags and `release/5dae611-spill`. Other branch refs may go once `main` contains them. | not run |

Pin updates in stage A: **none required.** `b0342a7`, `b1270b2`, `d96e001` and
`255c184` all become ancestors of `main`. When a consumer chooses to move to the
new tip, it gets DDK features plus the spill fix. That closes Congress gap G06
with a single tuple. congressional-tech uses the flow in section 1; DDK
re-qualifies its release.

### Stage B: upgrade (after ss-x8fh acceptance)

Merge `fix/datafusion-55`, including the committed `json_strict` work, into
`main` with `--no-ff`. Dry run on top of the stage-A merge:

| File | Conflict |
| --- | --- |
| `Cargo.toml` | 2 hunks: DataFusion 53.1 plus DDK's Substrait dependencies, against 55.2; `typetag` became required on DDK and optional on 55 |
| `Cargo.lock` | 2 hunks; regenerate it after deciding `Cargo.toml` |
| `src/budget.rs` | 2 hunks: DDK's `RetainedFiles` and `retained_existing_bytes`, against 55's removal of `scratch_writes` |
| `src/budget/scratch.rs` | 55 deletes it; DDK changed it |
| `src/job/export.rs` | 1 hunk: DDK's `retain_temporary` and `sync_parent`, against 55's `Budget::scratch_path` hard link |
| `src/job/materialized.rs` | 1 hunk: DDK's `read_retained`, against 55's direct Parquet read with `schema_force_view_types = false` |

Nine more shared files merge automatically. Work no text merge will show:

- Delete `vendor/datafusion-physical-plan` and `[patch.crates-io]`. They survive
  the merge, but a 53.1 patch cannot apply to 55.2.
- Move `datafusion-substrait` from 53.1.0 to the 55.2-compatible release, with
  its `substrait` and `prost`. That release is not in the local cargo cache, so
  this step needs the network.
- Port DDK's retained temporary-file custody onto 55's `SpillFile` and
  `scratch_path` accounting. This covers `retain_temporary`,
  `with_retained_disk`, `scratch_writer` and `RefCountedTempFile`: about 16 added
  lines plus `scratch.rs`. It is design work.
- Remove `as_any` from 4 DataFusion trait implementations DDK added: two
  `FileMetadata` impls in `src/budget/metadata_cache.rs` (one is a test),
  `SchemaOnlyTable` (`job-plan`), and the `Admitted` scalar function. Also check
  that module's `CacheAccessor` and `FileMetadataCache` impls against 55's cache
  API; this was not checked. The 12 other `as_any()` calls are Arrow array
  downcasts, which stay.
- Rust 1.95.0. `fcb5bf2` already updates `rust-toolchain.toml` and the CI image.

Pin updates in stage B:

- congressional-tech re-pins with the section 1 flow. It merges `df55-types`
  first and needs ss-x8fh acceptance.
- DDK re-qualifies `job-plan` and saved plans, then records a new local release.
- The replacement branches adopt the new tip when they resume.

### Rejected

- **DataFusion 55 first.** `main` would become unpinnable for
  congressional-tech: the 21 lane failures are unresolved.
- **Rebase or cherry-pick DDK onto 55.** This rewrites pinned commits
  (`b1270b2`, `d96e001`, `255c184`) that DDK's evidence cites.
- **Keep the spill fix off `main` until 55 lands.** This honors the
  2026-11-02 intent, but leaves two lines open with no date. congressional-tech
  could not reach DDK features without losing the spill fix.

## 4. Checks per step

Run every gate through `scripts/check`, which runs fmt, clippy `-D warnings`,
the tests, the build and the 31 Dagster adapter tests. Never use a bare
`cargo`, `rustc` or `uv` from PATH. On this Mac all three resolve wrongly:

- PATH `cargo` is Homebrew 1.99.1, which ignores `rust-toolchain.toml`.
- The main checkout has a rustup directory override to 1.94.1, which beats the
  pinned 1.94.0.
- PATH `uv` is the pyenv shim 0.6.10.

Run the gate from a worktree, which has no override:

```sh
cd <worktree>
export PATH="$HOME/.cargo/bin:/opt/homebrew/bin:$PATH"
test "$(cargo --version | cut -d' ' -f2)" = "$(sed -n 's/^channel = "\(.*\)"/\1/p' rust-toolchain.toml)"
uv --version        # expect 0.13.x
./scripts/check
```

| Step | Check |
| --- | --- |
| A2 | Lock: `cargo metadata --locked --offline --all-features` (done). Targeted: `cargo test --locked --all-features --lib budget::tests::native_spills_write_view_columns_by_referenced_bytes` and `--lib job::retained`; DDK's retained-scratch tests were qualified without the backport. Then the full `scripts/check`, which recorded 1,011 Rust and 31 Dagster tests at `255c184`. |
| A3 | `linux-release` on the pushed `fix/**` branch: stripped binary under 1 GiB, plus congressional-tech `production-offline` 6/6 shards. This may need the uncommitted `fetch-depth: 0` workflow fix. Optional: the `capture_dup` spill repro on a stripped darwin build, which measured about 383 MB and 0.8 s on `b0342a7`. |
| B | Full `scripts/check` on Rust 1.95.0, and the spill test, which must pass without the vendored crate. Also ss-x8fh's exact-output comparisons through congressional-tech's lane, DDK's installed qualification including `job-plan` type inference, and the CI qualify job. |
| Re-pin (any stage) | congressional-tech `doctor` and the full `production-offline` lane with the new pin. |

## 5. Risks

1. **Only copy.** `codex/ddk-docspec-runtime`, tag `ddk-v0.1.0-dev.2` and the
   `docspec-df55` edits exist only on this disk.
2. **Untested combination.** DDK's 22k lines have never run in docspec-rs CI or
   against congressional-tech's lane. The backport changes spill sizes for view
   columns under DDK's new retained-scratch accounting. Gate A3 covers both
   before `main` moves. Consumers still on `b0342a7` or `255c184` are
   unaffected.
3. **congressional-tech runs and evidence.** A re-pin is a cutover, not a file
   swap.
   - A retained publication intent refuses a changed engine ("Native runtime
     changed since the retained intent", `archive_controller.py`).
   - The capture scheduler refuses a selected release whose
     `capture_profile.json` names another `engine_revision`, until a model
     publish runs on the new engine. This was read in code, not exercised.
   - Re-pin only with no nightly running and no intent in flight.
   - Producer `parser_version`s hash code, not the engine, so readings stay
     valid.
   - Keep the `b0342a7` store entries and `.cache/runtime-b0342a7/` for rollback.
4. **Stage B and retained data.** DataFusion 55 changes outputs, which ss-x8fh
   lists. It also rejects an initial default in Iceberg v2 metadata, so check
   retained congressional-tech and DDK tables for one before re-pinning.
   Substrait plans saved under 53.1 / substrait 0.62.2 may not load
   identically on 55.2. `job-plan` reads types from the optimized plan, so its
   inferred UNION types can change. Builds depend on two unreleased upstream git
   revisions.
5. **Replacement branches (on hold).** Stage A changes nothing for them. On
   resume, each should pick one tuple on `main` rather than its current mix of
   `b0342a7` and `255c184`.
6. **Disk.** About 31 GiB free. Run one full gate at a time, after any running
   DocSpec build or nightly. Remove scratch target directories afterwards.

## 6. Decisions for the user

1. Approve stage A, including putting the vendored crate on `main` until
   stage B. This reverses the 2026-11-02 "main moves forward instead" intent.
2. Push the DDK branch and its tag as a backup now (A0).
3. Have the `docspec-df55` edits committed on `fix/datafusion-55`.
4. Push `fix/one-line-20261108` to run CI before `main` moves (A3).
5. When, or whether, congressional-tech and DDK move to the new tip after
   stage A. Nothing requires it.

## Reproduce the dry run

```sh
git clone -q --shared --no-checkout <docspec-rs> /tmp/dryrun && cd /tmp/dryrun
for b in codex/ddk-docspec-runtime fix/datafusion-55 fix/spill-view-gc; do git branch -f $b origin/$b; done
git checkout -q -b stage1 codex/ddk-docspec-runtime && git merge --no-ff --no-edit fix/spill-view-gc
PATH="$HOME/.cargo/bin:$PATH" cargo metadata --locked --offline --all-features --format-version 1 >/dev/null
git checkout -q -b stage2 stage1 && git merge --no-ff --no-edit fix/datafusion-55   # 6 conflicted files
```

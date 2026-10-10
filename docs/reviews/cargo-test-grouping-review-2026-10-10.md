# Cargo integration-test grouping review

**No actionable regression found. APPROVE the inspected grouping and shared-helper imports, subject to the implementation owner's build/test results.** The patch preserves existing test bodies and feature gates, keeps the process-sensitive suites separate, and continues to exercise the real CLI. This review does not claim compilation or execution passed.

Static review on 2026-10-10 against baseline `2a5ae513cb0aa7c40f771a534474956ca92364c8`. Scope: the current `Cargo.toml` diff, new `tests/integration.rs`, and changed test helper declarations/imports. Read the complete scoped diff and relevant helper, subprocess, feature-gating and registration code. No builds, tests, formatting, benchmarks, cleanup, or source changes were performed. No other reviewers' reports or gate logs were read. Only this review file was written.

## 1. Patch summary

`Cargo.toml:7-29` disables automatic integration-test target discovery and explicitly declares `integration`, `worker_initialization`, and `restore`. It also disables the CLI's empty unit-test harness. `tests/integration.rs:4-49` registers the ordinary suites as Rust modules and compiles `scalar`, `enriched`, and feature-gated process support once per ordinary test executable. Consumers replace repeated module definitions with `use super::...`; `tests/support/enriched.rs:2` resolves the same shared scalar helper through its parent.

The scoped diff changes declarations/imports and removes redundant module-specific dead-code expectations. It changes no test body, assertion, test attribute, fixture value, or fault-injection branch. Comparing the current root test-file inventory with `tests/integration.rs:11-49` and `Cargo.toml:21-29` found no omitted suite and no suite registered twice. Existing child suites remain declared by their parents (`tests/updates.rs:6-12`; `tests/evidence_updates.rs:977-979`; `tests/related_refresh.rs:1098-1105`).

The important caller is the unchanged repository gate, `scripts/check:4-6`, which formats, lints all targets/features, then runs `cargo test --locked --all-features`. A scoped search of current scripts, experiments, adapters, README and non-review documentation found no live `--test <former-suite>` invocation requiring migration. Historical review logs were excluded.

## 2. Function and configuration trace

| Function / configuration | Source | Inputs and outputs | Verified behavior |
| --- | --- | --- | --- |
| Explicit test target registration | `Cargo.toml:7-29`; `tests/integration.rs:4-49` | Root suite files → Cargo test targets and nested test modules | Every existing root suite remains reachable; process-sensitive exceptions retain their own target. |
| Shared scalar `fixture`, `input`, `spec` | `tests/support/scalar.rs:29-142` | Caller-owned root and values → local Parquet input, engine and immutable release | Functions contain no shared mutable fixture state; paths derive from the caller's root. |
| Shared `enriched` | `tests/support/enriched.rs:15-31` | Caller root → source plus computed groups | Calls the same `fixture` and `spec` implementation; changing `crate::scalar` to `super::scalar` resolves the root helper from this module. |
| Shared `check_values` | `tests/support/enriched.rs:34-76` | Engine/release/expected rows → assertion | Independent Rust expected rows and readback assertions are unchanged. |
| `kill_after` | `tests/support/mod.rs:12-83` | Caller root, request, kind and phase → terminated CLI child | Writes files beneath the caller's root, starts `CARGO_BIN_EXE_docspec`, waits for its handoff marker, and asserts actual SIGKILL on Unix. |
| `resume` / `run_cli` | `tests/support/mod.rs:87-133` | Caller root and command → parsed result plus phase messages | Starts another real CLI child; bounded wait and output files remain scoped to that root. |
| `delayed_writer_child` and its parent | `tests/restore.rs:742-765,847-916` | Child-only environment root and exact test name → independent writer | `restore` stays a separate root target, so `--exact delayed_writer_child` still selects the intended function. |
| Forced initialization shutdown | `tests/worker_initialization.rs:36-59` | Unique temp root and replacement runtime → `workspace_busy` assertion | Deliberately retained host ownership remains confined to its separate process. |

Cargo's locally installed reference explicitly recommends one integration target with module suites and states that binary targets are built for integration tests with `CARGO_BIN_EXE_<name>` available. It also documents that `autotests = false` disables automatic target discovery: `/Users/mikewolfd/.rustup/toolchains/stable-aarch64-apple-darwin/share/doc/rust/html/cargo/reference/cargo-targets.html:274-284,428-432`. The executable name remains `docspec`, and source inspection found no unit-test functions in `src/main.rs` that the new `test = false` would suppress.

## 3. Data flow and invariants

**Test coverage remains registered.** Rust now discovers ordinary tests through the module declarations in `tests/integration.rs`. Source-file names become test-name prefixes, while test functions and inner modules remain unchanged. Existing file-level `#![cfg(...)]` attributes apply to the included module. The child-suite declarations stay in their original source files, preserving their relative paths.

**Helpers share code, not fixture instances.** `scalar` and `enriched` functions remain ordinary functions; they hold no static data or global mutable cache. Their `pub(super)` visibility now admits the integration crate's descendants. Callers still supply independent temporary roots, including the process-loss fixture (`tests/physical_removal_process.rs:101-142`) and scalar refresh (`tests/refresh.rs:72-85`). No environment mutation, current-directory mutation, global logger/panic hook installation, or new process-global fixture state was found in the grouped tests. The mutable pause state in `rebuild_completion` remains an instance field (`tests/rebuild_completion.rs:43,199`).

**Feature and platform boundaries remain intact.** `tests/integration.rs:8-9` gates process support on `test-util`. Partial-feature consumers retain matching gated imports (`tests/backfill.rs:3-4`; `tests/refresh.rs:5-6`; `tests/schema.rs:6-7`). Whole feature-only suites retain their existing inner gates, for example `tests/abandonment.rs:2` and `tests/backfill_dependencies.rs:2`. Physical removal retains `all(feature = "test-util", unix)` at `tests/physical_removal_process.rs:4`; restore catalog retains `unix` at `tests/restore_catalog.rs:2`. This supports default-feature compilation structurally; no default-feature build was executed by this reviewer.

**CLI process coverage remains real.** Grouping the test host does not replace child processes with library calls. `CARGO_BIN_EXE_docspec` references and their result assertions remain intact. The only discovered self-execution with an exact root test name is the separately retained restore target.

## 4. Test behavior and edge cases

| Test / scenario | Source | Expected effect of grouping and evidence |
| --- | --- | --- |
| CLI change stream round trip | `tests/change_transport.rs:213-263` | Test name gains its module prefix. It still launches the CLI, checks process success, IPC end marker, schema and exact sorted values. |
| Routine versus deep CLI validation | `tests/lifecycle.rs:211-275` | Preserves the `test-util` gate and independent CLI executions; counters still assert no inspection on read/resume and separate deep verification. |
| SIGKILL during physical removal | `tests/physical_removal_process.rs:101-223` | Continues through shared process support. Asserts durable intent, resumed removal outcomes, selected values and idempotent retry after unrelated bytes occupy the old path. |
| SIGKILL during summary refresh | `tests/related_refresh/aggregation.rs:387-419` | Parent module's support import remains accessible to the child module; exact remaining-phase and summary assertions are unchanged. |
| Delayed independent restore writer | `tests/restore.rs:847-919` | Remains a separate executable. Exact child test name, environment handoff, workspace fence assertion and child success check retain their meaning. |
| Unpolled initialization quarantine | `tests/worker_initialization.rs:36-59` | Keeps its independent test process and deliberate host-descriptor lifetime. |
| Default feature selection | `tests/integration.rs:8-9`; representative gated imports above | Feature-only support is excluded with its callers. Shared ordinary scalar/enriched helpers remain available. Runtime/compile results remain the owner's responsibility. |
| Concurrent tests from different former suites | `tests/integration.rs:11-49`; shared fixture traces above | Ordinary suites can now overlap within one libtest process. No shared test-global state was found, and fixture paths are caller-owned. Scheduling and peak resource behavior still require the owner's actual gate result. |
| Targeted commands | `Cargo.toml:17-29`; `tests/integration.rs:14,31,49` | Former ordinary target names intentionally become module filters, e.g. `cargo test --locked --all-features --test integration backfill::`. Current live scripts have no old target-specific invocation. The separate restore/worker commands retain their names. |

No test result is predicted as a demonstrated pass. Static traces establish that coverage paths and assertions remain present; they do not establish successful linking, runtime scheduling or storage savings.

## 5. Findings

No BLOCKER, WARNING, or actionable NIT found in the inspected patch.

**Observation — explicit registration is now part of adding a suite.** With `autotests = false`, a future ordinary root test file must be declared in `tests/integration.rs`, or explicitly added as a separate target when isolation is required. The existing registration list and Cargo comments make the present arrangement clear. No current suite is omitted, so this is an ongoing convention rather than a regression.

**Observation — all-features execution does not establish default-feature compilation.** The unchanged gate selects all features (`scripts/check:6`). The owner should retain a bounded default-feature compile check alongside the grouping gate if it is not already covered by their current verification. The inspected gates/imports show no concrete default-feature defect; no additional test framework is needed.

**Observation — linked-output savings are measurable, not inferred throughput.** Sharing the ordinary test executable removes repeated linking of their common dependency graph by construction, and centralizing helpers removes duplicate modules. Actual elapsed build time, executable disk use and runtime resource behavior must come from the owner's retained measurements. This review supplies no performance result.

## 6. Conclusion

**VERDICT: APPROVE.**

- The patch achieves its structural intent through Cargo's native test-target/module mechanism and removes repeated helper module definitions.
- Existing suites, assertions, feature/Unix gates, CLI subprocesses and the identified process-sensitive isolation are preserved in the inspected source.
- Coverage of changed paths: ADEQUATE at the source level; owner-run gate and default-feature compilation remain the execution evidence.
- Confidence: HIGH for registration, import resolution and subprocess semantics; runtime and resource results were not independently executed.

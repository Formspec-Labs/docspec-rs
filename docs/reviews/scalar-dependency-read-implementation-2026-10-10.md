# Scalar dependency reads

Scalar preparation now requests only the distinct root fields consumed by its SQL expressions, plus occurrence locations, through `OccurrenceScan::read_projected_file`. It uses the existing Iceberg field selection and DataFusion physical-expression compiler. No second reader, provider, cache, payload check or retained artifact was added.

`Expressions::compile` still parses authored names against the original binding and resolves names through stable native IDs. After collecting dependencies, it projects the native Arrow schema in native field order and compiles each physical expression once against that schema. Field metadata remains attached to the selected fields. Logical expressions, output schemas, input bindings and material dependency fingerprints retain their existing meaning. The preparation stream evaluates selected batches directly and retains its existing batch limits, row-count checks, write path and completion recovery.

The existing selected reader supplies stored origins when occurrence locations are requested, materializes fallback locations where required, preserves native defaults and nulls, and checks complete file counts. Literal-only expressions select the occurrence locations and compile against an empty source schema. The native library can still decode physical columns when no selected value field exists in a particular file; this change does not replace that maintained fallback or claim a zero-payload read.

Changed implementation: `src/engine/backfill/expressions.rs` and `src/engine/backfill/preparation.rs`. Updated `src/engine/backfill/expressions/tests.rs` to evaluate projected batches and retain independent expected values for stable-ID renames, reorder/name reuse, metadata-sensitive functions and literal nulls. The metadata observer now consumes a field moved past an unrelated field. Added `tests/scalar_dependency_reads.rs` with direct Rust reconstruction of exact values and occurrence addresses, a wide unused source field that exceeds a narrower full-read batch allowance, repeated dependencies, successful nulls, and literal-only evaluation over stored origins after compaction.

The new integration suite needs `mod scalar_dependency_reads;` in `tests/integration.rs`; the parent task owns registration. This assignment ran no builds, tests, benchmarks, formatting or commits. The work has only static review evidence; no speed, requested-byte, memory or disk improvement was measured. The concrete simplification is one dependency selection passed to the existing reader, with one physical compilation per expression. Existing completion/protection rules and retained evidence are unchanged.

Parent verification commands, from the repository directory:

```sh
cargo test --locked --all-features --lib engine::backfill::expressions::tests::
cargo test --locked --all-features --test integration scalar_dependency_reads::
cargo test --locked --all-features --test integration defaults::
cargo test --locked --all-features --test integration backfill::
cargo test --locked --all-features --test integration scalar_bindings::
cargo test --locked --all-features --test integration continuous_backfill::
cargo test --locked --all-features --test integration compaction::
./scripts/check
```

Representative native requested-byte measurements against the full-width path remain qualification work. Existing metadata/task admission, default-expansion checks and streaming batch bounds still apply; projecting fields does not promise a whole-process memory bound beyond the existing budget design.

## Resource-admission correction

Addressed the independent review's F1 in `OccurrenceScan::read_scalar_file` and scalar preparation. The new scalar entry point constructs the existing `Selection` once, including required stored-origin IDs, and looks up the exact captured task and `DataFile` in the existing path-keyed map. It authorizes native projection only when `column_sizes` contains a selected primitive root ID. A single evidenced physical field suffices even when other selected fields are absent. Missing metric keys and nested-only evidence take the conservative full read through the shared `read_selection_file` implementation. No new footer, metadata or payload pass was added.

The returned flag tells scalar preparation whether its input is already projected. For the conservative path, the existing full source stream checks batch bytes before preparation applies `RecordBatch::project` in the physical expressions' compiled column order. This preserves the previous scalar batch-admission behavior while retaining dependency projection when physical selection is established. The metric's numeric size is unused: compressed storage bytes do not estimate decoded memory. Positive evidence relies on the existing admitted immutable native descriptor; this correction does not independently qualify forged or stale metrics.

The scalar integration fixture now shares its wide native source between the original physical-dependency test and new regressions. The uncompacted literal regression and the optional-null/defaulted dependency regression each expect `oversized_batch` with a 64 KiB allowance, then compare exact Rust expected values and original occurrence addresses under the ordinary adequate allowance. The absent-dependency case also confirms both added value IDs are absent from captured physical metrics. The existing wide physical-dependency success and literal stored-origin compaction cases remain.

This correction ran no formatting, builds, tests, benchmarks or commits; the parent task owns verification. It restores the prior admission boundary, not a general raw-decoder or whole-process memory guarantee. The preexisting full reader can itself transform away old physical columns when the entire selected schema contains absent fields. Actual requested-byte, speed and process-memory improvements remain unmeasured.

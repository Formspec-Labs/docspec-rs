# Scalar dependency reads: independent static review

**Current decision: APPROVE after the correction reviewed below.** F1 is resolved within the scalar path's existing admission and immutable-metadata trust boundaries. The initial finding and review are retained, followed by the current static follow-up.

**Initial decision: request changes for one resource-admission gap.** The field selection preserves the scalar expression, field-ID binding, output schema, defaults and stable occurrence mapping on the traced paths. When no requested field exists physically, however, the maintained reader decodes every column and removes them before DocSpec checks the batch allowance. Routing scalar work through that path can silently admit a wide native batch that the previous full read rejected.

Reviewed the uncommitted scalar change against `d6f39b313b6a95e69bb4c0565b19bc87aac46d98` on 2026-10-10. Scope: `src/engine/backfill/preparation.rs`, `src/engine/backfill/expressions.rs`, `src/engine/backfill/expressions/tests.rs`, `tests/scalar_dependency_reads.rs`, and necessary callers/readers/tests. The integration-suite registration was checked. Other continuous-process and retention work was excluded. No author handoff or implementation-review notes were read. This review used the Rust and semi-formal code-review skills, `docs/PLAN.md`, R3 and R7. No build, test, benchmark or payload read was run; only this report was written.

## Findings

### F1 — Keep batch admission effective when the native projection falls back to all columns

- **Severity:** WARNING (P2); **category:** performance/resource correctness.
- **Changed location:** `src/engine/backfill/preparation.rs:616`; empty dependency selection is constructed at `src/engine/backfill/expressions.rs:136–155`.
- **Scenario:** Use the uncompacted wide source already constructed in `tests/scalar_dependency_reads.rs:58–98`, but compute `CAST(17 AS BIGINT)` with the same narrow budget. Another applicable case consumes only a newly added defaulted field that is absent from each old file.
- **Evidence chain:**
  1. Literal SQL produces no input IDs, so the projection contains only the two occurrence positions (`expressions.rs:136–155`). `Selection::new` adds stored origins only if the source schema has them, and always adds the virtual Iceberg file ID (`src/storage/selection.rs:64–87`). An uncompacted original source has no stored origins.
  2. Iceberg removes metadata IDs before constructing its Parquet projection (`iceberg-0.10.1/src/arrow/reader/pipeline.rs:214–236`). With no IDs it selects all physical columns (`reader/projection.rs:118–120`). If IDs are nonempty but all absent, it also selects all physical columns (`reader/projection.rs:202–216`).
  3. The Parquet stream decodes that full batch, then `RecordBatchTransformer` retains only requested fields before returning it (`reader/pipeline.rs:392–403`; `record_batch_transformer.rs:302–335,481–493`). DocSpec's first byte check receives the transformed, narrow batch (`src/storage/occurrences.rs:176–180`; `src/storage/schema.rs:114–141`). The wide unrequested arrays are already gone, so they escape that check.
  4. The reader still reserves only twice the configured batch allowance (`src/storage/occurrences.rs:397`), and scalar execution reserves twice that allowance separately (`preparation.rs:613`). These reservations do not inspect or charge the discarded full decoded batch. The old full source read retained the wide columns through `strip_file_column` and returned `oversized_batch`; the existing wide fixture establishes that expected old behavior at `tests/scalar_dependency_reads.rs:147–153`.
- **Practical effect:** The new path can continue decoding full-width batches under a budget that appears sufficient for only the narrow result. This is a statically established admission blind spot, not a measured out-of-memory incident. It also prevents claiming dependency-only physical reads for literal-only or all-absent dependency cases.
- **Suggested correction:** Keep the shared reader and maintained decoder, but handle their no-physical-column fallback explicitly. Preserve the prior full-batch refusal in that fallback, or qualify a maintained/thin reader path that avoids decoding unused payloads while preserving native row counts, defaults and occurrence addresses. A post-transform size check alone cannot enforce this boundary.
- **Minimum regression expectation:** Add an uncompacted wide literal case and an old-file/default-only case. Each must either avoid the unrelated payload decode with evidence from the read boundary, or conservatively refuse when its actual decoded batch exceeds the allowance. Narrow output equality or success under a small `max_batch_bytes` alone is insufficient. Keep the compacted-origin literal case, since it exercises a different physical selection.

Upstream citations above refer to the locally cached, pinned crate under `/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/iceberg-0.10.1/src/arrow/`. These implementations were read directly.

### Proposed correction assessed after reporting F1

The implementation owner proposed selecting native columns only when the exact captured `DataFile.column_sizes()` proves a requested primitive root or required origin ID is physically present. Otherwise, run the existing full source read once, then Arrow-project the checked batch before scalar evaluation. The proposal is a suitable conservative repair within this scope: it adds no payload/footer pass, reuses both established read paths, and can use the existing path-keyed captured task lookup. Missing metrics and nested-root-only evidence should take the full-read path. A physical dependency keeps Iceberg's mask nonempty even when other selected fields need defaults. This paragraph assesses the proposal, not an implemented correction.

The metric has a bounded trust meaning. Iceberg's `column_sizes` is optional manifest metadata describing compressed on-disk column regions (`spec/manifest/data_file.rs:64–72`); it is not a decoded-memory estimate. The pinned maintained writer derives its keys from actual Parquet chunks (`writer/file_writer/parquet_writer.rs:392–413`) and retains them in the generated `DataFile` (`434–445`); DocSpec uses that writer (`src/storage.rs:264–286`). DocSpec's routine footer inspection checks physical schema membership and rows but does not independently compare `column_sizes` with the footer (`src/storage/inspection.rs:124–145`). Therefore the proposed positive test relies on the existing admitted, immutable native-metadata trust boundary. It does not protect against fabricated or stale metrics. Absent/unavailable metrics must not authorize the narrow path.

Finally, a full-read fallback restores prior scalar admission behavior, not a general raw-decoder allocation bound. If the entire current schema contains absent fields because old physical columns were dropped, the preexisting full read can itself transform away those old columns before the byte check. That inherited limitation does not require broadening this scalar repair, but the disposition must not claim to have removed it.

## Patch summary

The patch extracts unique consumed root field IDs, orders them as the current native schema, and compiles the physical expressions against that smaller Arrow schema. It appends occurrence columns to the read projection and sends the projection through the existing captured-file reader (`expressions.rs:136–155`, `preparation.rs:613–618`). It adds assertions for projection shape, reordered metadata, successful literal nulls, a wide source and stored origins after compaction.

The intended benefit is less unrelated source decoding and smaller batches for scalar field groups. This follows R3's stable field identity and bounded backfill requirements (`docs/requirements/03-schema-backfill.md:13–17,39–43,55–59`). The resource caveat above matters under R7's admission and retained-buffer requirements (`docs/requirements/07-operations-qualification.md:23,85–91`). Actual speed, bytes read and process-memory improvements remain unmeasured, as required by R3:63 and R7:151–159.

## Function trace

| Function | Evidence | Input and output | Verified behavior |
| --- | --- | --- | --- |
| `Expressions::compile` | `src/engine/backfill/expressions.rs:30–204` | Native schema, group definition, optional authored-name binding → logical/physical expressions, output schema, input IDs, projection | Resolves authored names to stable root IDs before choosing native-order columns. Compiles physical indices against the same order the projected reader returns. |
| `Expressions::evaluate` | `expressions.rs:207–228` | Projected batch → occurrence columns and computed columns | Reads occurrence columns after the selected source count, broadcasts scalar results to row count, casts to retained output types, and checks computed output bytes. |
| `field_interpretation` / `root_index` | `expressions.rs:233–256` | Native root ID → material field meaning / root position | Keeps type, requiredness and initial default; removes display name and write default. Keeps documentation when `arrow_metadata` is used. Selection does not change the reuse digest inputs. |
| `Engine::prepare_group` | `src/engine/backfill/preparation.rs:67–159,215–224` | Exact source pin and definition → prepared group | Compiles against the pinned schema, resolves prepared/file completions first, then invokes computation only for missing files. |
| `Engine::compute_file` | `preparation.rs:590–654` | Captured source file and compiled expressions → retained file completion | Streams the selected read through evaluation into the existing writer, and checks the total output row count against native metadata. |
| `OccurrenceScan::open` / `capture` | `src/storage/occurrences.rs:270–338` | Native table and budget → captured file tasks | Captures unfiltered complete-file tasks and immutable schema; no new payload scan is introduced by this patch. |
| `OccurrenceScan::read_projected_file` | `occurrences.rs:362–402` | Captured path and projection → native batch stream | Creates one admitted selection, replaces native field IDs in a cloned task and retains complete-file row-count checks. |
| `Selection::new` / `apply` | `src/storage/selection.rs:30–149` | Requested positions → native IDs and exact output positions | Deduplicates native dependencies, adds required origin pairs, shares Arrow fields/arrays, and preserves requested output order and row count. |
| `Occurrences::next` / `location_columns` | `occurrences.rs:174–240,443–485` | Native batches → selected stable occurrences | Validates decoded row totals. Uses stored origin pairs when present; otherwise generates original file/row addresses. |
| `identity::materialize` | `src/storage/identity.rs:129–202` | Stored origin arrays and physical fallback → validated complete origins | Validates paired nullness, nonempty origin and nonnegative row; preserves existing origins and fills only missing pairs. |
| `verify_group_data` | `src/engine/backfill/verification.rs:522–553,617–686` | Source/output pins, binding and output-ID mapping → verification | Recompiles the output schema and mapping. Deep mode rebuilds values through logical SQL and compares complete occurrence membership; it does not use the changed physical-expression evaluator. |

## Data flow and invariants

- **Bindings:** `input_fields` is collected from parsed columns, then logical column names are rewritten from those stable IDs (`expressions.rs:80–127`). Its exact equality check against a retained binding remains intact (`130–132`). The new `BTreeSet` changes read selection only; it does not change the persisted binding or digest (`136,156–162`). Repeated references and references shared by output fields read one native source column.
- **Ordering:** The projection is built in native root order (`137–143`), and physical expressions are created against `source.project(...)` (`144–150`). `Selection` requests native columns in that order and reconstructs exact output positions (`selection.rs:78–105,129–142`). No physical expression retains an index from the original full schema.
- **Schema and metadata:** Arrow projection retains the original field handles. The runtime projected schema is built from the same pinned native interpretation (`storage/schema.rs:17–68`, `selection.rs:55`). The pinned `arrow_metadata` implementation reads argument field metadata (`datafusion-functions-53.1.0/src/core/arrow_metadata.rs:124–137`); the reordered documentation test checks it explicitly. Native output ID allocation and logical-to-native mapping remain unchanged (`expressions.rs:163–193`).
- **Defaults and nulls:** The shared read path continues qualifying native evolution (`storage/schema.rs:19–65`). The maintained transformer looks up physical IDs before falling back to an initial default or null (`record_batch_transformer.rs:528–563`); explicit physical nulls pass through. The full-schema default-expansion check is retained before scan capture (`occurrences.rs:120`, `storage/defaults.rs:112–140`), so this patch does not promise that unrelated large defaults stop affecting admission.
- **Occurrence identity:** The two appended indices refer to the full native schema's appended occurrence fields, while `source_fields` records only the number of selected source fields (`expressions.rs:152–155`). Internal origin arrays are reader dependencies, not extra returned source columns. This keeps evaluation offsets correct after compaction (`selection.rs:64–76`; `occurrences.rs:453–460`).
- **Recovery and completion:** The new projection is transient in `Expressions`; persisted declarations, dependency hashes, file keys, completions and source/output pins are unchanged (`preparation.rs:95–106,143–159,225–233`). Existing reuse bypasses computation as before. A reader/evaluation failure follows the existing acknowledgment and failure-record path (`234–254`).
- **Work and admission:** The implementation reuses the existing reader, ID-aware native transformer, Arrow schema projection, DataFusion physical expressions and writer. It adds small selection metadata, not a parallel decoder or an additional payload pass. The shared selection reserves mapping/schema metadata before allocating it (`selection.rs:45–55,153–195`). The full/default-only native fallback is the unresolved exception in F1.

## Test behavior and edge cases

These are static expectations from inspected assertions, not execution results. The new suite is registered at `tests/integration.rs:53`; Cargo has `autotests = false` and an explicit integration target (`Cargo.toml:7,16–19`).

| Test / scenario | Evidence and expected behavior |
| --- | --- |
| Rename, reorder, reused display name | `expressions/tests.rs:49–109` checks projections `[0,2,3]` and `[1,3,4]`, exact old-ID value `10`, fresh-name value `101`, and missing/extra binding refusal. Expected to preserve behavior. |
| Metadata-sensitive scalar function | `expressions/tests.rs:113–157` moves the consumed field after an unused field, changes its documentation, and expects `arrow_metadata` to return `before` / `after` with distinct dependencies. Ordinary arithmetic ignores the documentation change. Expected to pass by projected field metadata preservation. |
| Type change and literal null | `expressions/tests.rs:160–185` checks type-sensitive dependencies and a successful null output with occurrence-only projection. Expected to preserve output type and row count. This synthetic batch test does not exercise native decoding. |
| Wide unused payload, multiple inputs and repeated references | `tests/scalar_dependency_reads.rs:58–166` supplies independent Rust arithmetic/null expectations keyed by exact occurrences. It first expects the full read to exceed the bound, then expects a two-input backfill to match exact results. Expected to pass because both selected input fields physically exist. It does not measure compressed I/O or peak memory. |
| Literal-only after compaction | `tests/scalar_dependency_reads.rs:170` captures prior occurrences, adopts the compacted source, computes `17`, compares exact addresses and confirms an empty logical input binding. Expected to pass because `Selection` includes the stored origins. This does not exercise the uncompacted metadata-only native fallback. |
| Empty source / retained work | `tests/backfill_dependencies.rs:40–179` asserts no file-work completion for empty input and exact retained prepared/validation generations on resume. The changed computation path is skipped when there are no files or a reusable completion. |
| Default/supplied-state dependency | `tests/defaults.rs:295–338,429–450` backfills a presence marker absent from old files, updates explicit-null/default/omitted inputs, and checks the final supplied count `2`. This exercises defaulted dependency selection semantically, but not its native decoding budget. |
| Shared-reader physical selection | `src/storage/occurrences/tests/selection.rs:281–380` damages an unselected payload page, expects selected IDs to remain readable, and expects selecting the damaged payload to fail. It supports the present-column reader route reused here; it does not cover an empty/all-absent mask. |
| Shared-reader occurrence integrity | `selection.rs:18–120,151–276` checks empty selections, original offsets, exact final row counts, stored-origin fallback and malformed origin refusal. These support the unchanged address semantics. |
| Uncompacted literal / all-absent default with a wide unrelated payload | Not covered by the scalar budget tests. The source trace predicts the admission bypass in F1. |

## Exploration record

| Hypothesis before the relevant read | Observation and disposition |
| --- | --- |
| H1: physical column indices may still refer to the full schema after dependency selection. | Refuted by physical compilation against the projected schema and native-order selection (`expressions.rs:136–150`, `selection.rs:78–105`). |
| H2: selecting only logical inputs could lose stored occurrence origins after compaction. | Refuted by the reader adding both origin fields whenever occurrence locations are requested (`selection.rs:64–76`) and using them for returned addresses (`occurrences.rs:453–460`). |
| H3: metadata-sensitive functions or defaults might change under projection. | Value/schema behavior preserved by retained field metadata and ID-aware default resolution; tests cover reordered documentation and presence defaults. No new semantic defect found. |
| H4: a small returned batch proves that unused payloads were not decoded. | Refuted by the pinned reader's empty/all-absent projection fallback and its transform-before-return ordering; this yields F1. |

## Conclusion

**VERDICT: REQUEST CHANGES**

- The patch preserves the traced scalar value/schema/identity semantics and uses maintained shared components. No other actionable correctness defect was found within scope.
- Resolve F1 or narrow the optimization so that fallback execution preserves the previous admission behavior before treating the change as complete.
- Coverage of changed semantic paths: adequate for this bounded change. Coverage of the new resource-admission behavior: insufficient for empty/all-absent physical dependencies.
- Confidence: high in the source-level trace; execution and full-path resource qualification remain with the implementation owner. This review demonstrates neither runtime success nor measured savings.

## Static follow-up after the admission correction

**Current verdict: APPROVE. F1 is resolved for the new scalar path.** This follow-up inspected the final working-tree `src/storage/occurrences.rs`, `src/engine/backfill/preparation.rs`, `tests/scalar_dependency_reads.rs`, and the expression changes. It ran no build, test, formatting command or benchmark. The earlier line references describe the initial reviewed checkpoint; the references below describe the corrected source.

### Correction trace

| Function / state | Final source evidence | Verified behavior |
| --- | --- | --- |
| `OccurrenceScan::read_scalar_file` | `src/storage/occurrences.rs:393–420` | Looks up the exact captured task and `DataFile` by path, constructs the existing admitted `Selection`, and chooses a narrow native read only when a requested primitive root ID occurs in that file's `column_sizes` map. |
| Physical-presence decision | `occurrences.rs:410–419` | Includes the origin IDs added by `Selection`; excludes nested roots as proof; treats missing metrics or absent selected IDs conservatively. A true decision establishes at least one real leaf for the pinned native mask under the existing admitted-metadata trust boundary. No compressed byte value is used as a memory estimate. |
| `OccurrenceScan::read_selection_file` | `occurrences.rs:423–450` | Shares the preexisting reader-state construction. `Some(selection)` replaces task field IDs; `None` keeps the original full task. It preserves the exact captured descriptor, row-count checks and two-batch reservation. |
| `Engine::compute_file` | `src/engine/backfill/preparation.rs:613–628` | Receives the read mode with the stream. A fallback batch must successfully emerge from the full `Occurrences` path before Arrow projection and expression evaluation; an oversized full batch therefore fails before it can be reduced. |
| Physical expression input | `src/engine/backfill/expressions.rs:136–159,211–232` | Both reader modes reach evaluation in the same compiled column order, with occurrence columns immediately after selected source fields. Output types, null broadcasting and output admission remain unchanged. |

The complete fallback order is: original native task → maintained decoder → `strip_file_column` byte check → origin validation/materialization → appended occurrence columns and their byte check → caller's Arrow projection → physical scalar evaluation. The shared reader implements the first checks at `src/storage/occurrences.rs:174–206` and `472–488`; the caller projects only after `batch?` succeeds (`preparation.rs:617–626`). This restores the refusal lost in F1 without another source read.

The narrow path still uses `Selection` to read required origins even for literal-only SQL. For a compacted file with stored origins, their primitive IDs provide the physical-presence evidence and `location_columns` returns the preserved addresses (`occurrences.rs:491–508`; `src/storage/selection.rs:64–76`). One physical ID is sufficient when other selected inputs are absent: Iceberg's native mask stays nonempty, and the unchanged transformer supplies their initial defaults/nulls. With no such ID, the fallback retains the former full-read semantics.

The correction performs one path-keyed lookup, examines the current schema and selection, and uses the already captured metrics. It adds no inventory-wide lookup for each file, footer read, payload pass or alternative decoder. The projected `Selection` retains its existing metadata permit; choosing the full path drops that temporary selection. Arrow projection shares field/array handles and does not copy source values.

### Regression assertions checked

- `uncompacted_wide_literal_preserves_full_batch_admission` (`tests/scalar_dependency_reads.rs:177`) exercises a literal with no physical dependencies. `all_absent_optional_and_default_dependencies_preserve_full_batch_admission` (`:182`) adds an optional null field and a field defaulted to `7` without rewriting the original files. Both call `fallback_admission` (`:186`).
- The fixture uses the pinned current snapshot's public manifest reader and live `DataFile` entries (`:228–243`). Its independent expected keys are each retained file path and row position, with values `17` or `[null, 7]` (`:244–262`). The default case explicitly confirms both added IDs are absent from every file's column-size metrics (`:263–278`).
- Under two rows per batch and a 64 KiB allowance, both scenarios assert the exact `oversized_batch` refusal (`:295–308`). With the ordinary larger allowance and a fresh operation, both assert exact computed values and occurrence addresses and request release verification (`:310–318`). These assertions would reject the originally reviewed unconditional projection behavior.
- The existing wide present-column case still asserts that the full read fails while the selected arithmetic result equals independent Rust expectations (`:98–173`). The stored-origin literal case still asserts exact pre-compaction occurrence addresses and an empty logical input binding (`:322–354`). The expression unit tests still check metadata-sensitive values, ID/name reuse and successful literal nulls.

The first owner-run full gate caught an integration-test visibility error: the draft fixture referenced crate-private `storage::OccurrenceScan`. That failure is retained in `continuous-scalar-full-check-2026-10-10.log:2–11`. The reviewer initially missed this access error. The final fixture replaces it with public `Table::manifest_list_reader`, `ManifestListReader::load`, `ManifestFile::load_manifest`, `Manifest::entries`, `ManifestEntry::is_alive`, and `ManifestEntry::data_file` APIs. Their pinned definitions were checked directly. The current source no longer refers to the internal scan type. This is a static correction assessment, not a claim that the restarted gate passed.

### Disposition and limits

- **F1: resolved.** The new fallback preserves previous scalar admission behavior whenever no requested physical primitive/origin field is evidenced. The targeted refusal and exact-value tests cover both reported triggers.
- **No additional actionable production defect found.** Field-ID binding, metadata-sensitive expressions, default/null interpretation, literal row counts and stored occurrence addresses remain consistent across both modes.
- **Metric trust is unchanged.** `column_sizes` remains optional admitted native metadata generated from the maintained writer's physical chunks, not an independently revalidated field-presence certificate or a decoded-byte budget. The earlier detailed trust analysis still applies.
- **Resource qualification remains bounded.** Missing metrics and nested-root-only selections can forgo the optimization. The preexisting case where all current-schema fields are physically absent can still hide old dropped fields inside the native decoder's full-read transform. The patch does not claim a universal raw-decoder/process-memory bound, measured I/O reduction or speedup.
- Coverage is adequate for the bounded semantic change and the identified regression. Confidence is high in the static source trace. Runtime and full-gate results belong to the implementation owner's separately retained evidence.

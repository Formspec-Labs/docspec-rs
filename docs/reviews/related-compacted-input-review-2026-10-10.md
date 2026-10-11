# Related refresh with selected compacted producer layouts

Static, independent review of the pending local patch over `e669987873eac3daa5d2ec277ad89c28bf0df8b3`, including the legacy-recovery correction made during review. No builds, formatting, tests, network operations, or Rust edits were performed by this reviewer. Line references describe the reviewed working tree and may move during formatting.

## Findings

### F1 — Older aggregate processing completions could not resume evidence preparation — resolved in code

- Severity: WARNING / P2.
- Category: correctness, compatibility.
- Locations: `src/engine/refresh.rs:210`, `src/engine/aggregation.rs:328`.
- The initial patch used `processing.dependencies.as_ref().ok_or_else(invalid)?` in both aggregate evidence paths. `PhaseResult.dependencies` remains optional specifically for older records (`src/model.rs:490`), and `prepare_source` deliberately resumes a dependency-free processing completion when its original admission still exists and predates processing (`src/engine.rs:762`). An interrupted older full summary, or an incremental summary with nonempty applied changes, therefore reached the new requirement and failed despite having recoverable processing and canonical producer inputs.
- Both sites now use an empty map when the dependency block is absent, preserving canonical inputs. This matches the existing fallbacks in processing and final related-result construction. The added `legacy_summary_processing_resumes_canonical_inputs_without_recomputation` fixture (`tests/related_refresh/compaction.rs:802`) covers full and incremental summaries: it installs the older processing shape through explicit phase retirement, preserves the original admission, makes a later producer representation unreadable temporarily, and asserts that processing and contributor evidence still use the canonical inputs.
- Resolution: static trace confirms the correction. The fixture was read but not executed by this reviewer; execution belongs to the lead agent.

### F2 — Previous captured producer layouts needed a subsequent incremental-refresh fixture — resolved in test code

- Severity: WARNING / P2.
- Category: test-coverage.
- Locations: `src/engine/incremental.rs:65`, `tests/related_refresh/compaction.rs:582`, `tests/related_refresh/compaction.rs:684`.
- `process_incremental` now loads the previous producer tables from the base result's `RelatedResult.input_sources`. That branch matters when the consumer was created using a compacted producer and a later producer correction changes old/new join or grouping keys. The new full-refresh fixture stops at creation/verification; the processing-reuse fixture starts from `prepared()`, whose consumer was created before either producer was compacted (`tests/related_refresh/compaction.rs:188`). Neither uses a consumer with a nonempty previous-input map as the base for a subsequent incremental refresh.
- The existing key-move fixtures (`tests/related_refresh/incremental.rs:125`, `tests/related_refresh/aggregation.rs:174`) exercise canonical inputs. They do not establish behavior when `load_inputs` replaces the previous source table while `changed_keys` separately carries canonical endpoint metadata into `change_chain`.
- The added `subsequent_incremental_refresh_uses_old_and_new_captured_producer_layouts` fixture (`tests/related_refresh/compaction.rs:864`) now creates a consumer after producer compaction and asserts its nonempty map. It moves a duplicate old key to a new key, changes a null-key group, compacts the new input, and incrementally refreshes both join and grouped summary. It repeats after explicitly retiring the producer delta, asserts the certificate's absence, and checks complete value/contributor oracles, full-versus-forward consumer changes for both value and occurrence identities, and deep verification. `local_certificate` returns no chain entry after retirement (`src/engine/changes/certificates.rs:172`), establishing the fallback branch.
- Resolution: the missing scenario is now covered in test code. The fixture was read but not executed by this reviewer; no incorrect result was established by the static trace.

## Patch summary

The patch lets related refresh operations read a producer's selected equivalent `source` representation. Immutable `VersionRef` coverage remains the logical input identity. A new optional `input_sources` map records the actual physical source pins in request dependencies, phase dependencies, and `RelatedResult`.

Admission resolves the representation under the producer lock and protects its objects before retaining the consumer request. Processing and contributor preparation read the captured pins. Incremental history checks receive the canonical endpoint pins separately. Deep verification reconstructs from canonical producers and independently compares captured producer values and occurrences. Retention and availability traversal include the new physical references.

This follows R6's separation of physical representation from logical version and its independently retained phase roots (`docs/requirements/06-maintenance-retention.md:13`, `:17`, `:25`, `:27`). No measured speed or memory improvement is claimed. The concrete structural improvement is reuse of the existing representation selector, captured-source qualification, occurrence reader, and root inventory rather than a second producer-reading implementation.

## Function trace

| Function or type | Location | Input → output | Verified behavior |
| --- | --- | --- | --- |
| `RelatedResult`, `PhaseDependencies` | `src/model.rs:434`, `:498` | Serialized records → optional physical input map | Missing maps deserialize empty; empty maps are omitted on serialization. Logical coverage fields are unchanged. |
| `AdmissionDependencies` | `src/engine/admission.rs:13` | Retained request dependencies | Missing map defaults empty despite `deny_unknown_fields`; new nonempty maps bind exact `TablePin`s. |
| `admitted_dependencies` | `src/engine/admission.rs:97` | Lifecycle request/dependency records → checked dependencies | Requires matching request revision and coverage; rejects foreign map keys/metadata owners; requires the existing protection binding. |
| `protect_admission` | `src/engine/admission.rs:163` | Request and exact logical producer releases → protected capture | Resolves selected source while producer is locked; stores only a noncanonical representation; protects those objects before returning. |
| `PreparedAdmission::records` | `src/engine/admission.rs:51` | Prepared capture → lifecycle records | Binds operation, request and request-dependencies claims in the guarded commit. |
| `representation_pin` | `src/engine/compaction.rs:72` | Canonical source → selected physical pin | Requires verified result, group, original logical metadata identity and row count. |
| `admit_refresh` | `src/engine/refresh.rs:59` | Protected capture → admission phase | Loads captured inputs and retains the map with unchanged logical coverage and inherited request protections. |
| `load_inputs`, `canonical_source` | `src/engine/refresh.rs:501`, `:487` | Coverage and captured pins → physical releases plus canonical source map | Reads exact manifest identity, checks owner/key/schema/logical identity, keeps the old source pin before replacing the in-memory source group. |
| `open_captured_source` | `src/engine/write.rs:43` | Canonical/physical pins → native physical table | Requires equal logical metadata and row count; opens exact metadata and checks public schema preservation. |
| `process_refresh` | `src/engine/refresh.rs:118` | Admission phase → prepared source | Full processing reads the retained map; incremental mode delegates with the captured output source. |
| `prepare_source` | `src/engine.rs:753` | Request and operation → retained processing | Reuses processing before considering later admission; newly completed processing copies the admission map and inherits request plus admission protections. |
| `process_incremental` | `src/engine/incremental.rs:40` | Base/current coverage and captured layouts → applied source changes | Previous values use the base related map; current values use admission map. Logical equality preserves the output/evidence with empty changes. |
| `process_incremental_join` | `src/engine/incremental.rs:147` | Old/current physical inputs → complete affected-key result | Uses physical pins for occurrence reads; passes separate canonical pins to key discovery. |
| `changed_keys` | `src/engine/incremental.rs:350` | Endpoint versions, canonical pins and key names → key table | Logical equality avoids physical-rewrite keys; qualified history uses canonical endpoints; fallback scans physical files present on only one side. |
| `history_keys` | `src/engine/incremental/history.rs:8` | Canonical endpoints plus physical source schema → projected old/new keys | Calls shared chain validation with canonical pins and projects both removals and additions. |
| `change_chain` | `src/engine/changes/chain.rs:44` | Version identities/canonical pins → qualified delta tables or fallback | Checks canonical physical and logical endpoint identities at lines 76 and 156; validates public schema against the physical source at line 164. |
| `process_aggregation` | `src/engine/aggregation.rs:184` | Previous/current inputs → affected-group replacement | Reads current captured values and passes canonical endpoint pins to shared key discovery. |
| `prepare_evidence` | `src/engine/refresh.rs:162` | Retained processing → retained related result | Joins derive contributor pairs from processing; summaries reload processing's map. Reused evidence must match dependencies, coverage and changes. Final result copies processing's map. |
| `aggregate_evidence` | `src/engine/aggregation.rs:307` | Applied group changes and processing capture → contributor replacement | Unions old/new affected groups and reads the processing capture; older missing dependencies now use canonical inputs. |
| `phase_key`, `complete` | `src/engine/refresh.rs:456`, `src/engine.rs:1860` | Logical material inputs → shared completion identity | Physical layout does not change phase identity. Reuse returns the existing phase; evidence/validation reject incompatible dependency generations. |
| `release_objects` | `src/engine.rs:2370` | Related result → availability objects | Includes captured physical source objects alongside logical coverage and evidence. |
| `verify_related`, `verify_related_with_inputs` | `src/engine/refresh.rs:821`, `:831` | Serialized result and canonical releases → verification | Does not resolve current catalog representations; checks each captured source and deep-compares all occurrence columns against canonical producers before reconstructing output/evidence. |
| `Inventory::related`, `table`, record traversal | `src/engine/retention/inventory.rs:980`, `:886`, `:1330`, `:1408` | Result/phase/admission roots → exact objects | All new maps traverse complete table pins, including physical metadata, logical metadata and retained objects. |

## Data flow and invariants

1. **Logical input identity remains immutable.** `load_inputs` changes only an in-memory release's source group; `ReleasePin::version_ref` still returns dataset, version and the original manifest (`src/model.rs:445`). Neither the transform's version vector nor retained coverage advances due to compaction.
2. **Captured physical inputs survive phase reuse.** The map first appears in protected admission (`src/engine/admission.rs:229`), is copied into admission/processing, and is propagated from processing into evidence and the final result (`src/engine/refresh.rs:249`). A later operation may acquire a newer representation, but reused processing and its original protections take precedence (`src/engine.rs:762`, `:968`, `:1395`).
3. **Physical rewrite does not generate new occurrences.** The shared occurrence reader uses retained origin-file/origin-row columns when present (`src/storage/occurrences.rs:567`); otherwise it emits the original physical location. Join tuples and summary contributors use that shared reader (`src/engine/refresh.rs:580`, `src/engine/aggregation.rs:77`, `:172`).
4. **Canonical change certificates remain independently checked.** `canonical_sources` keeps the original manifest-selected pins while scans use replacements (`src/engine/refresh.rs:516`). This prevents `change_chain` from rejecting valid history merely because its endpoint data files were compacted. Fallback scans preserve conservative old/new key discovery; equivalence is established by the selected compaction before admission.
5. **Protection is attached to retained claims.** Request roots receive owner grants before their guarded commit (`src/engine/admission.rs:51`, `:321`). Processing inherits both current request and reused admission references; evidence inherits processing; selection inherits processing, validation and evidence (`src/engine.rs:968`, `:1395`). Inventory visits the map independently, so a completed phase can protect its producer replacement without a producer selection record.
6. **Cold verification cannot accept a changed source from a claimed logical label alone.** The manifest graph yields canonical releases (`src/engine/verification.rs:360`, `:419`). Deep verification checks captured payloads and exact occurrence multisets, then reconstructs the output and evidence from canonical inputs (`src/engine/refresh.rs:855`). Routine verification performs schema/identity checks without claiming full payload equivalence.
7. **Older serialized records retain their meaning.** Empty new fields are omitted and missing fields default empty (`src/model.rs:436`, `:500`, `src/engine/admission.rs:21`). The corrected aggregate fallbacks retain the explicitly supported older `dependencies: None` behavior. No down-version-reader compatibility promise was established.

## Test behavior and concrete edge cases

Tests below were inspected, not executed. Expected behavior follows the code trace, not a runtime result.

| Fixture | Expected behavior and checked assertion | Remaining limit |
| --- | --- | --- |
| `new_refresh_reads_selected_producer_layout_and_preserves_exact_contributors` (`tests/related_refresh/compaction.rs:582`) | Both full join and grouped summary should reach processing while a replaced canonical producer payload is unreadable; retained maps equal selected replacements; value/contributor oracles and deep verification agree; a changed producer relabeled as equivalent is rejected. | Does not use the resulting capture as a previous incremental input. |
| `completed_refresh_keeps_its_producer_layout_after_later_compaction_and_new_admission` (`tests/related_refresh/compaction.rs:684`) | Original processing is retained, a later admission captures a new layout, and reopen/retry returns the original processing table/map. An isolated phase record protects replacement objects without producer catalog state. | The base's previous producer map is empty; only the current input map is exercised. |
| `legacy_summary_processing_resumes_canonical_inputs_without_recomputation` (`tests/related_refresh/compaction.rs:802`) | Full and incremental aggregate evidence should resume from canonical inputs after processing dependencies are omitted; later compacted payload damage should not affect that resume; original processing table and record remain unchanged. | New fixture still needs lead-owned execution. |
| `subsequent_incremental_refresh_uses_old_and_new_captured_producer_layouts` (`tests/related_refresh/compaction.rs:864`) | Previous/current captured input maps survive duplicate-key moves and null-group replacement for joins and summaries. Retiring the producer delta forces conservative physical discovery; complete value/evidence oracles and full-versus-forward consumer changes still agree. | New fixture still needs lead-owned execution. |
| `partial_compaction_is_shared_by_disjoint_refresh_and_noop_advances_coverage` (`tests/related_refresh/compaction.rs:262`) | Existing compacted output files remain shared through disjoint input corrections; no-op advances logical coverage and records empty changes. | Exercises output compaction, not a prior nonempty producer map. |
| Existing join/summary oracle helpers (`tests/related_refresh.rs:281`, `:317`; `tests/related_refresh/aggregation.rs:89`) | Compare complete sorted contributor occurrence tuples and output values against independently iterated canonical producer rows; summary uses an integer accumulator. | Fixtures are synthetic, not representative cost measurements. |

The related suite is registered through `tests/integration.rs:49` and `tests/related_refresh.rs:1118`. The collector/history fixture literal changes merely initialize an empty map; they do not independently exercise nonempty producer capture traversal.

Concrete scenarios traced: both sides of a join use a compacted source; one source appears in both roles; same logical producer acquires another physical layout; processing survives operation abandonment and later admission; false equivalence preserves row count but changes values; older processing has no dependency block; a previous consumer already has captured producer sources, with qualified-history and retired-history discovery branches.

## Review reasoning record

- H1: Physical source substitution might change version identity or certificate endpoints. Refuted by `version_ref`, the separate canonical source map, and canonical endpoint checks in `change_chain`.
- H2: Reusing processing after later compaction might adopt the newer admission's producer files or lose old protections. Refuted by processing-first reuse, map propagation from processing, inherited bindings and the isolated-phase fixture.
- H3: An attacker or corrupted record could claim a logical metadata identity while changing producer values. Deep verification refutes this by exact canonical/captured occurrence multiset comparison. Routine verification has its intentionally narrower scope.
- H4: New maps might prevent older records from decoding or resuming. Empty-map serialization is compatible; the initial aggregate evidence requirement did break resume. The corrected fallback resolves that concrete defect; the regression fixture is now present.
- H5: Initial tests covered old and new captured layouts during a key move. Initially refuted; the subsequent-refresh fixture added during review now supplies this coverage and closes F2 in test code.

## Conclusion

VERDICT: APPROVE

The compatibility defect is corrected on static inspection and the subsequent-refresh coverage gap is addressed in test code. No unresolved actionable finding remains in the selected producer-layout implementation. Run the lead-owned targeted checks and repository gate before closing this checkpoint; this static approval is not execution evidence.

- Coverage of changed paths: ADEQUATE in inspected test code for this bounded change; test execution remains pending with the lead.
- Confidence: HIGH for the traced identity, occurrence, retention and compatibility behavior; execution remains unverified by this review.
- Scope exclusions: separate remote-object adapter work, broad retirement/compaction redesign, representative performance qualification, and automatic cleanup enablement.

## Lead execution follow-up

After the independent source review, the lead completed the
[focused checks](related-compacted-input-tests-2026-10-10.log) and
[full repository gate](remote-related-full-check-2026-10-10.log), both with exit
status zero. Formatting, strict all-target/all-feature Clippy, library/integration,
restore, worker-initialization and documentation checks pass. The existing
external Comments physical-plan probe remains explicitly ignored. This closes
the pending execution check for this patch, not the wider lifecycle or resource
qualification limits recorded above.

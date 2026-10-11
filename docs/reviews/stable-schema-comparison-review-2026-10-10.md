# Stable-schema comparison review — 2026-10-10

**Current verdict: APPROVE after correction, by static review.** F1 and F2 below are resolved in the final inspected source. Their history and the original verdict are preserved; see the final disposition at the end. This is not a build or runtime qualification result.

## Findings and initial verdict

**REQUEST CHANGES.** The proposed alignment handles a real metadata-only rename or reorder correctly, but its initial admission rule also treats independently allocated native field numbers as stable correspondence. That can report an empty value difference for different fields. F1 must be resolved before accepting the change.

This is an independent static review of the uncommitted changes to `src/engine/comparison.rs` and `tests/comparison.rs`, against HEAD `42ab0c3fe705db45da41634cdf2460a7089abc8a`. Necessary schema, write, storage, query, and change-chain callers were read. The engine test hook, adapter work, and unrelated lifecycle changes are excluded. No implementation handoff or unrelated change-chain review was read. No build, test, formatter, benchmark, or implementation edit was run by this reviewer. Line references below describe the initial reviewed patch; any correction is assessed separately at the end.

### F1 — Require evidence of shared field history before relaxing names and order

- **Severity:** BLOCKER (P2)
- **Category:** correctness
- **Location:** `src/engine/comparison.rs:281–287`, `src/engine/comparison.rs:546–559`
- **Description:** Every pair of non-computed source schemas enters `aligned_fields`. It matches root fields by their integer IDs and accepts different names when types and nullability agree. Neither the caller nor the helper establishes that those numbers belong to a preserved schema history. Native IDs are allocated afresh for unrelated sources and for ordinary full rebuilds. Numeric equality alone therefore does not establish the field identity asserted by `ChangeDescriptor.comparable_values`.

Evidence chain:

1. Comparison admits both endpoint releases independently, with no same-dataset requirement (`src/engine/comparison.rs:214–235`). Existing cross-dataset type-change tests exercise that public surface (`tests/comparison.rs:473–505`).
2. `process_build` preserves a base schema only when evolved defaults or stored occurrence origins require it; other builds, including replacements with a base release, call `write_frame` (`src/engine/write.rs:132–164`). `verify_rebuild` also returns immediately for a base without evolved defaults (`src/engine/schema.rs:232–245`).
3. `write_frame` calls `arrow_schema_to_schema_auto_assign_ids` and creates fresh metadata (`src/engine/write.rs:167–177`). The pinned Iceberg 0.10.1 implementation starts at field ID 1 and deliberately reassigns IDs rather than taking Arrow metadata IDs (`iceberg-0.10.1/src/arrow/schema.rs:226–237,296–306`; pin: `Cargo.lock:2487–2488`). Fresh metadata retains only the supplied current schema (`src/storage/tables.rs:11–34`).
4. The new comparison rule equates those reused numbers, reads the after field under its new name, and aliases it to the same positional `cN` as the before field (`src/engine/comparison.rs:546–559,564–582`). Value counts and streams then cancel equal values under this unproved mapping (`src/engine/comparison.rs:112–129,585–649`).
5. The existing continuous-source check explicitly recognizes this distinction: native auto-numbering is not evidence of unchanged field meaning (`src/engine/backfill/continuous.rs:965–976`). R3 requires stable meaning across rename/reorder, and R4 permits cancellation only after the selected identity comparison establishes equality (`docs/requirements/03-schema-backfill.md:13–23`; `docs/requirements/04-change-tracking.md:19–23`).

A concrete reachable case is two independently built single-column sources named `temperature_c` and `weight_kg`, each with native ID 1, the same nullable integer type, and the value 10. The patch reports comparable values and zero value additions/removals, and produces an empty value stream. The previous strict schema rule returns unknown counts and refuses the stream. A same-dataset ordinary rebuild can likewise reallocate the same numeric IDs to changed names or order, so adding only a dataset-equality check does not close this defect. Occurrence mode still sees different birth addresses after a rebuild, but incorrectly presents the after field under the before field identity; the qualification defect affects both modes. `force_full` uses the same alignment and does not repair it.

**Suggested correction:** Keep the existing strict compatibility rule when sufficient. Qualify the relaxed rename/reorder rule using retained native schema history or another existing authoritative correspondence. Refuse relaxed alignment when that evidence is unavailable. A shared dataset name, matching schema ID, or matching ID set alone is insufficient. Do not infer identity from native table UUID equality: valid replacement metadata changes the UUID while preserving schema history (`src/storage/tables.rs:55–78`).

**Minimum regression expectations:** Exercise unrelated datasets with equal integer IDs and types but different field names, plus an ordinary same-dataset `RunSpec` rebuild that reallocates those IDs. Verify both directions, automatic and forced full comparison, unknown count results with `comparable_values=false`, and `comparison_schema_changed` from streaming, for both identities. Keep the legitimate `SchemaSpec` rename/reorder tests passing. A retained-history qualifier should also have direct negative coverage for matching schema numbers without equal schema contents.

## Patch summary

The patch makes source value comparison independent of root display names and root order when native IDs and types match. `comparison_plan` builds an after-field list in before-field order and passes each endpoint's own names to the existing signed relation builder (`src/engine/comparison.rs:279–348,424–438`). Output field names, native IDs, and metadata remain those of the before endpoint. Computed groups retain strict Arrow schema equality. `unchanged` additionally requires unchanged native public schema rules (`src/engine/comparison.rs:364`).

The desired behavior follows R3's metadata-only rename/reorder model and R4's separation of schema effects from value differences. The initial patch's problem is the qualification of field correspondence, rather than the signed difference algorithm.

## Function trace

| Function | Location | Inputs and output | Verified behavior |
| --- | --- | --- | --- |
| `compare` | `src/engine/comparison.rs:64` | Endpoint releases and group → counts | Selects value identity and automatic path. |
| `compare_with_options` | `src/engine/comparison.rs:90` | Endpoints, group, identity/options → `Comparison` | Uses the shared plan; incompatible fields yield unknown counts; compatible fields aggregate signed multiplicity by values, adding addresses for occurrence identity. |
| `changes` | `src/engine/comparison.rs:183` | Same arguments → protected typed stream | Uses the same plan with addresses enabled, then `comparison_stream`. |
| `comparison_plan` | `src/engine/comparison.rs:206` | Exact releases and options → descriptor, fields, relation, reservation | Resolves retained endpoint representations, validates origins/group addresses, determines compatibility and schema effects, selects unchanged/forward/full, and aligns the full endpoint relations. |
| `aligned_fields` | `src/engine/comparison.rs:522` | Native and Arrow schemas → optional after fields | Requires equal root membership by numeric ID, native type, requiredness, Arrow type, and Arrow nullability; returns after fields in before order. Initial version has no history qualification. |
| `signed_frame` | `src/engine/comparison.rs:564` | DataFrame, endpoint fields, address flag, sign → selected DataFrame | Reads each field by literal name and aliases values to positional names; appends original address columns independently. |
| `identity_columns` | `src/engine/comparison.rs:656` | Plan → grouping keys | Uses values only for value identity; adds file and row for occurrence identity. |
| `change_members` / `value_members` | `src/engine/comparison.rs:585,621` | Signed relation → changed members | Cancels exact occurrences first; occurrence identity retains exact residuals; value identity ranks surplus duplicates using maintained DataFusion window/aggregate operations. |
| `comparison_stream` | `src/engine/comparison.rs:451` | Plan → output stream | Refuses incompatible schemas, admits the physical plan, enforces output batch limit, and emits before-schema typed values plus endpoint addresses and descriptor. |
| `public_schema` | `src/storage/identity.rs:19` | Native table → Arrow schema | Uses maintained Iceberg conversion and excludes only stored origin fields. |
| `process_schema` | `src/engine/schema.rs:160` | Accepted `SchemaSpec` → prepared source | Evolves the admitted base metadata and retains `schema_input`; this is actual ID-preserving rename evidence. |
| `process_build` / `write_frame` | `src/engine/write.rs:116,167` | Build inputs/SQL → prepared source | Preserves schema for evolved/origin-bearing bases, otherwise assigns fresh IDs. This defeats unconditional stable-ID inference. |

## Data flow and invariants

- **Alignment:** `aligned` is created from endpoint schemas, determines `comparable`, and supplies `after_fields`. It does not escape the plan builder. The after list is paired with before fields by position and filtered using the same before-field visibility predicate (`src/engine/comparison.rs:279–348`). This keeps corresponding values in the same signed-relation column once correspondence has been established. F1 identifies the missing prerequisite.
- **Schema and value distinctions:** `same_schema` still compares native public field definitions, order, names, defaults, and sorted key IDs (`src/engine/comparison.rs:294–318`). A compatible rename therefore reports a schema effect even with an empty value difference. Root default/documentation changes are not treated as value-type changes; endpoint readers supply the actual values. Native nested type equality remains strict, including nested IDs and rules (`src/engine/comparison.rs:550–555`).
- **Metadata:** Pinned Iceberg's root Arrow field conversion emits the native field ID and optional documentation, and derives nullability from requiredness (`iceberg-0.10.1/src/arrow/schema.rs:518–536`). The patch explicitly checks the ID, type, and nullability and preserves before metadata in the emitted struct. Ignoring root documentation for value equality does not introduce a discovered type interpretation gap.
- **Hidden fields and group kind:** Native stored origins are excluded consistently from both public schemas and the after-ID map (`src/storage/identity.rs:15–29`; `src/engine/comparison.rs:528–534`). Computed address fields and source contributor fields retain their existing visibility rules; the same mask filters both selected lists (`src/engine/comparison.rs:319–348`). Computed/non-computed kind mismatch remains incompatible, and each computed endpoint's address schema is validated (`src/engine/comparison.rs:271–277,709–722`). No separate defect was found here.
- **Forward versus full:** Changed schema rules force the full relation; forward-chain qualification still requires unchanged schema rules (`src/engine/comparison.rs:393–415`). Both automatic fallback and `force_full` use the same after-field ordering. Thus full comparison is not an independent oracle for the identity-admission mistake in F1.
- **Occurrence addresses:** Alignment affects value columns only. Located source reads use the maintained occurrence provider, and `signed_frame` separately carries its file/row fields (`src/engine/comparison.rs:391,428–438,575–579`). Count grouping and streamed members continue to distinguish occurrence and value semantics. The schema rename itself does not create new birth addresses.
- **Resources and reuse:** The patch adds a root-ID map and field-reference vectors, proportional to schema width, under the existing metadata reservation (`src/engine/comparison.rs:237–238,528–559`). It reuses the existing providers, signed aggregates, window admission, and stream limits. No added payload verification pass appears in the changed code. Renamed-schema comparison becomes a full read where the old behavior refused; no measured speed or memory saving is established or claimed by this review.

## Test behavior and edge cases

These are assertion and execution-path inspections, not reported test results.

| Test | Location | Static expectation and limits |
| --- | --- | --- |
| `field_alignment_refuses_changed_ids_types_nullability_and_membership` | `src/engine/comparison.rs:732` | The helper rejects changed IDs, requiredness, primitive type, and membership in both directions. It cannot detect coincident IDs with changed meaning. |
| `stable_source_ids_compare_renames_reorders_and_reused_display_names` | `tests/comparison.rs:252` | Expected to accept actual `SchemaSpec` operations, including quoted display names and names reused by a different ID. Both directions, identities, and force modes assert full path, schema effects, output IDs/names, and empty counts/stream. |
| `stable_source_ids_compare_actual_changes_after_rename_and_reorder` | `tests/comparison.rs:283` | Expected to align updated values through name reuse and reorder, including null and duplicate removal, and match an independent signed value bag. |
| `streaming_values_and_occurrences_match_independent_bags_in_both_directions` | `tests/comparison.rs:328` | Existing coverage checks endpoint membership, addresses, duplicates, null/empty values, equal-value replacements, and the distinction between identities. Its schemas are unchanged. |
| `unchanged_pins_skip_payloads_and_force_full_preserves_read_failures` | `tests/comparison.rs:441` | Exact same-schema pins still skip corrupt payloads; forced full still reads and fails. |
| `schema_changes_are_explicit_in_counts_and_refused_for_streams` | `tests/comparison.rs:473` | Type incompatibility remains unknown counts plus stream refusal, including unrelated datasets. Same-type coincident IDs were not covered in the initial patch. |
| `computed_groups_use_retained_addresses_and_report_added_definitions` | `tests/comparison.rs:508` | Strict computed-group compatibility and retained addresses remain on their existing path. |
| `default_only_changes_report_schema_rules_without_inventing_value_changes` | `tests/comparison.rs:597` | Write-default-only changes report schema effects with no invented value changes; the source value rule remains compatible. |
| `returned_change_stream_holds_maintenance_admission_until_drop` | `tests/comparison.rs:656` | The patch does not alter stream-lifetime admission ownership. |
| Forward comparison checks | `tests/comparison/forward.rs:29,121,220` | Existing helpers compare full/forward output and exact endpoint occurrence bags, including compacted updates and evidence-only hops. Rename cases intentionally do not take this path. |

The new `assert_schema_difference` helper checks stream descriptor, before field names, signed value bag, and emitted counts (`tests/comparison.rs:146–249`). It does not read the output file/row columns. Its changed-values fixture happens to yield the same counts for both identities. As a non-blocking coverage improvement, include an equal-value replacement followed by rename, then assert endpoint membership and exact occurrence differences after alignment. That distinguishes identity modes on the changed path and directly checks the address promise. F1's missing negative cases remain required.

## Hypothesis disposition

| Hypothesis | Disposition | Evidence |
| --- | --- | --- |
| Stable root correspondence makes rename/reorder equivalent to selecting original fields. | Confirmed conditionally. | After fields are selected by native ID and mapped to before order; original endpoint names are used literally. |
| Equal numeric native IDs always establish that correspondence. | Refuted. | Ordinary `write_frame` reallocates IDs; neither initial comparison caller nor helper qualifies history. |
| Schema effects can remain visible when value changes are empty. | Confirmed. | Native rules determine `same_schema`; alignment separately determines value compatibility. |
| Renames accidentally change occurrence addresses or use the forward path. | Refuted for the traced code. | Address columns are independent and changed schemas force full comparison. Runtime renamed-stream address coverage remains limited as noted above. |
| The change establishes a performance improvement. | Not established. | No execution or measurement was authorized; this is a behavior extension using maintained comparison components. |

## Initial conclusion

**VERDICT: REQUEST CHANGES**

- The intended metadata-only rename/reorder behavior is implemented, but the initial qualification admits independently reused IDs and can hide differences.
- Coverage of changed paths: **INSUFFICIENT** until the coincident-ID negative cases are added and F1 is corrected. Existing positive cases are meaningful.
- Confidence: **HIGH** in the static finding and path trace. Runtime correctness and performance were not evaluated by this reviewer.

## Correction review

The first correction resolves F1's semantic issue. Identical Arrow schemas retain the prior compatibility rule. Different source schemas qualify only within the same dataset and when one endpoint's complete current native schema is present in the opposite endpoint's retained schema history (`src/engine/comparison.rs:281–314,543–551`, first correction). The check uses canonical logical endpoint metadata so a physical representation's origin fields cannot invent or remove the logical history evidence.

I independently checked the maintained semantics behind this rule. Iceberg 0.10.1 `Schema::eq` compares the complete root structure, schema ID, and identifier-field set (`iceberg-0.10.1/src/spec/schema/mod.rs:76–81`). DocSpec's schema evolution clones the admitted base metadata before adding the current schema (`src/engine/schema.rs:519–539`), and schema-preserving replacements clone native history (`src/storage/tables.rs:55–78`). Fresh builds remove unused provisional schemas (`src/storage/tables.rs:25–34`). Thus a same-dataset fresh schema with coincident field numbers and changed names no longer qualifies.

This is a bounded compatibility proof, not a universal proof of semantic provenance: a fresh build can recreate an exactly equal schema. The previous strict comparison already accepted that equality boundary. Requiring exact retained native schema equality before extending it through maintained evolution does not introduce the unqualified name relaxation in F1. No lifecycle ancestry walk is required to resolve this regression.

### F2 — Admit additional canonical metadata before opening it

- **Severity:** WARNING (P2)
- **Category:** resource admission
- **Location:** First correction, `src/engine/comparison.rs:287–296`
- **Description:** When both endpoints use compacted representations, the new history check can open two additional canonical native metadata objects while both chosen representation tables remain live. The existing reservation is unchanged at three metadata allowances, and its per-pin size check applies only to the chosen representation pins (`src/engine/comparison.rs:237–265`). `storage::open_pin` verifies and decodes a pin but does not apply a budget or metadata-size limit (`src/storage.rs:716–734`). The new canonical reads therefore need explicit size admission and additional temporary reservation.
- **Suggestion:** Before either additional open, check the canonical pin's declared metadata size and reserve capacity for the extra metadata objects for the duration of the history check. Reuse already-open tables when the pins match and release temporary capacity after the boolean result is obtained.
- **Minimum regression expectation:** A comparison needing canonical history must refuse with the appropriate capacity error when its extra metadata cannot be admitted. At minimum, static ordering must show size checks and reservation before opens, retained until the decoded objects are dropped. The ordinary unchanged-schema path should incur no extra canonical reads or reservation.

The new negative integration fixture explicitly asserts that both ordinary builds allocate native IDs `[1, 2]`, then compares different display names for unrelated and same-dataset fresh builds in both directions and force modes (`tests/comparison.rs:283–349`, first correction). It asserts unknown counts, full-path schema fallback, and `comparison_schema_changed` from streaming. At this checkpoint it covered value identity only; occurrence coverage was requested because compatibility admission is shared by both identities.

## Final disposition

**F1 resolved.** The final source keeps the exact-schema compatibility branch and qualifies relaxed alignment with the same-dataset and retained canonical schema checks described above (`src/engine/comparison.rs:281–331,560–573`). The added regression now loops over both `Values` and `Occurrences`, both endpoint directions, and both force modes; each combination checks unknown counts and stream refusal for coincident IDs in unrelated and freshly rebuilt sources (`tests/comparison.rs:283–351`). The existing legitimate rename/reorder and actual-change cases remain in the patch. This is an assertion inspection, not a claim those tests were executed by this reviewer.

**F2 resolved.** Before opening canonical metadata, the final code counts the extra objects, checks each canonical pin against `max_metadata_bytes`, and obtains a temporary reservation for that count of metadata allowances (`src/engine/comparison.rs:287–303`). It then opens only differing pins and reuses existing tables otherwise (`src/engine/comparison.rs:304–314`). The temporary reservation is declared before the canonical tables, so their local owners are dropped before the reservation is released. `Budget::reserve` refuses exhausted shared capacity without waiting (`src/budget.rs:154–169`). The unchanged-schema branch never enters this extra-read block. No dedicated compacted-rename capacity regression was inspected; the required ordering and lifetime are established by the static trace.

The corrected path can read additional canonical metadata after compaction; this is bounded, admitted qualification work. It does not add a source-payload pass, and it does not establish any measured speedup. Full Schema equality is deliberately conservative: unavailable history or an unproved correspondence yields the existing unknown-count/refused-stream behavior. The non-blocking suggestion to assert renamed-stream occurrence addresses after an equal-value replacement remains useful, but no address defect was found in the traced implementation.

**VERDICT: APPROVE**

- The final patch supports qualified source rename/reorder comparison while preserving the previous strict compatibility behavior elsewhere.
- F1's identity qualification and F2's additional metadata admission are corrected. No remaining actionable defect was found in the requested scope.
- Coverage of changed behavior: **ADEQUATE for static approval**, with the explicit runtime and optional edge-case gaps above. The root agent owns formatting, build, and test results.
- Confidence: **HIGH** in the traced logic and finding dispositions; execution and performance remain outside this review.

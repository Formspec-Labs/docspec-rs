# R3 based-Build stable schema review

VERDICT: APPROVE (static; runtime gates pending). No actionable correctness finding in the final artifact.

Artifact: `/tmp/docspec-stable-build-schema.patch`, SHA256 `88b272da128a43fd6dd326e798b37cbe09d22be4740d281d73e76f81591a651d`. All changed production/test files read; actual schema/default normalization, replacement metadata, phase-key construction, binding verifier and admission/cutover branches traced. No builds/provider/shared edits.

## Patch / function trace

| Function | Evidence | Verified behavior |
| --- | --- | --- |
| schema_key | staged `src/engine/schema.rs:10–29` | Every new explicitly based Build captures native schema/history identity, including non-evolved schemas. Existing saved operations retain their captured key. |
| process_build | staged `src/engine/write.rs:158–197` | Based Build always normalizes query result against exact selected base schema, then uses native replacement metadata. Unbased authoring remains the separate existing path. |
| normalize | staged `schema.rs:248–388` | Rejects unknown/hidden supplied fields, changed type/requiredness, missing ordinary fields; preserves retained order/IDs and existing tracked omission/default rules. Hidden identity fields are generated, not supplied from untrusted query columns. |
| replacement_output | staged `write.rs:124–155`, shared `storage/tables.rs:41–68` | Maintained replacement metadata retains schemas/current schema/high-water; native table/snapshot identity is fresh. No second schema reconstruction introduced. |
| schema_identity / verify_rebuild | staged `schema.rs:199–245` | Exact identity includes native format,current schema ID,last column ID,all schemas. Identifier field IDs normalized as unordered set; field order remains material. Format12 requires exact identity; legacy10/11 retain prior non-evolved interpretation behavior. |
| verify_groups | staged `backfill/verification.rs:45–79` | Actual based-Build parent/request/boundary/eligibility and group composition checks run before format-aware schema comparison. Verification is not merely comparing independent table heads. |
| supported_format / pending_source_operations | staged `engine.rs:200–213`; `retention.rs:93–111` | Formats10/11 remain readable. Schema operations11/12 remain composition-only for pending source coverage; only legacy10 schema admissions preserve old boundary semantics. |
| phase_key / prepare_source | shared `engine.rs:2096–2140,902–938,1949–1985` | New schema identity is a material processing/validation key input, not broad processor/package hash. Retained phase lookup occurs before payload. Saved old operation keys continue to describe original retained work. |

## Data flow / invariants

H1: SQL column order does not reallocate retained IDs. Confirmed by normalization selecting retained native order and replacement metadata copying original schemas/history (`write.rs:180–193`, `schema.rs:369–388`).

H2: schema-history changes invalidate materially incompatible work without hiding reusable work when history is stable. Captured schema identity enters phase keys. The updated rebase fixture first establishes hidden origin fields, so the later value-only update keeps exact history; the separate first-origin-update fixture expects recomputation. This matches actual production key behavior rather than weakening assertions.

H3: the format bump cannot retroactively make accepted old renumbered layouts unreadable. Format11 old non-evolved schema returns through the explicit legacy branch; format12 compares exact schema identity (`verify_rebuild:234–242`). Supported-format and schema-boundary callers treat11 independently of12 where needed. No unsupported remote based-Build startup is claimed.

## Tests / assumptions checked

- `tests/rebuild.rs:43–115`: existing computed expressions retain source IDs across reordered based SQL; combined literal values remain independently checked; forged binding redirects to a genuinely different retained field and refuses.
- `tests/rebuild.rs:280–370`: establishes hidden origins before reusable source capture, then proves later value-only rebase reuses exact processing/validation and prepared scalar output while source input is unavailable. Old fixture's first-origin transition could not legitimately reuse exact schema history; final artifact fixes that setup.
- `tests/rebuild.rs:373–406`: missing-field rebuild now fails before retained processing; admission remains reusable and required processing/groups stay truthful. It does not retain an invalid candidate merely to satisfy the former test.
- New renamed/hidden/default fixture (`:528–610`): authored IDs2/1 survive query reorder and rename, hidden historical ID3 is not reused, score allocates4 and supplied-marker5, histories/current schema/high-water equal, omitted nullable/default score produces independent expected7 for all rows; wrong names/missing/type/extra fields refuse without changing latest selection.
- Legacy fixture (`:613–635`) authors an unbased reordered layout then hashes a format11 based interpretation with exact parent. Existing old interpretation validates; same layout labeled format12 rejects `invalid_schema`. It represents the legacy decoder boundary, not a new production bypass.
- First hidden-origin history change (`:640–672`) retains admission but requires new processing; exact output native schema/high-water matches the new base and complete values verify.
- `tests/continuous_backfill.rs:793–863`: unbased authoring is explicit before continuous intent. Later **based** query-order change keeps authored native identity; original continuous work remains valid and independently verifies. It does not ask based Build to silently author different fields.
- `src/engine/retention.rs` adds format11 composition-only schema to the actual cutover coverage fixture.

No test execution was performed. Root should run rebuild, continuous_backfill, defaults/schema and full gates to catch transport/runtime/native integration regressions. Qualification remains bounded to stable based source interpretation, not complete R3 or general nested schema evolution.

## Current-format assertion supplement

Reviewed `/tmp/docspec-format12-assertions.patch`, SHA256 `e247c43735066e6ca6311d2410098995e23c2f0f8bf3570b708b1ab278009b66`. Fresh-result format assertions become12; cutover preserves explicit legacy10; strict legacy-member support matrix includes10,11 and current `FORMAT`. Malformed format11 legacy fixtures and the unrelated last-column-ID literal11 remain intact. No production change or actionable static finding. Parent reports focused R3/rebuild/schema/continuous checks passed; full gate remains parent-owned.

## Executed qualification

The schema/rebuild/continuous-backfill cases in
`stable-build-ancestry-focused-2026-10-11.log` pass; that overall invocation failed
on the separate historical-qualification fixture, whose failed attempt is retained.
`stable-build-ancestry-followup-2026-10-11.log` passes current/legacy admissions and
cutover. Runtime fixtures confirm exact field meaning and unchanged-history reuse;
they do not establish a performance result. The full repository gate is pending.

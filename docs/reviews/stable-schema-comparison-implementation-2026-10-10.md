# Stable source-field comparison

Source rename/reorder comparisons now align endpoint columns by native field ID
before using the existing signed multiset comparison. A root field keeps its
meaning when its display name or position changes. Reusing another field's old
display name does not switch identities. The typed change stream retains the
before endpoint's field names, order and native IDs.

This implements the bounded R3 rename/reorder comparison gap and preserves R4's
distinction between value changes and schema/evidence effects. It does not complete
the broader R3/R4 requirements. Execution and independent review remain with the
implementation owner; this note records source inspection and added test cases,
not passing build or runtime evidence.

## Shared comparison and conservative paths

`src/engine/comparison.rs::comparison_plan` admits and resolves both exact release
pins and opens their selected native representations. The prior strict identical
Arrow-schema case remains supported. Relaxing names/order additionally requires
the same dataset and an exact full native schema from one canonical logical
endpoint retained in the other endpoint's native schema history. This includes
names, nested identities and rules; matching numeric IDs or schema IDs alone is
insufficient. `schema::evolve` and `storage::replacement_metadata` clone that
history, while `storage::fresh_metadata` removes unused provisional schemas and
keeps only its new schema. Physical compaction representations do not become the
schema-history authority. When canonical metadata differs from the already open
representation, its declared bytes are checked before opening and a scoped
additional metadata reservation covers its qualification lifetime.

Once that boundary is established, source compatibility requires the same public
root ID set, matching native field types and required
rules, and matching Arrow types/nullability. Native type equality also preserves
nested IDs and rules; nested schema edits remain outside this alignment. Added,
removed, renumbered, type-changed or nullability-changed fields remain incompatible.
Default-only rule changes retain the existing behavior: compare visible decoded
values and report the schema-rule effect separately.

The existing schema-rule comparison still sets `schema_changed`. Schema edits
cannot select `Unchanged`, and the existing `same_schema` gate rejects forward
change acceleration. Compatible renamed/reordered endpoints use the full native
reader and DataFusion column selection; `signed_frame` already aliases each
endpoint's selected columns to the same comparison positions. The change adds no
reader, dependency, cache, payload copy or retained relation. Alignment does
metadata work proportional to schema size with a native-ID map; no throughput or
memory improvement is claimed.

`compare_with_options` and `changes` both call this shared plan. Counts aggregate
the same signed values that the stream uses for deterministic surplus members or
exact occurrence differences. The CLI paths in `src/main.rs` and the HTTP handlers
in `src/http.rs` call those engine methods, so they receive the same qualification,
fallback reason and field ordering.

Computed groups retain their previous strict Arrow-schema compatibility rule.
Their authored logical IDs and native-ID mapping need separate qualification
before rename/reorder alignment can be extended to them. This change does not
infer that mapping from display names.

## Added checks

The registered `tests/comparison.rs` suite adds source rename-only, reorder-only,
and display-name reuse cases with zero expected differences. Another case updates
values before swapping names and order: independent Rust bags expect two added
members and three removed members, including duplicate removal and a new explicit
null. Both directions, value and occurrence identities, default and forced-full
options check counts, exact emitted values, field IDs, typed field names/order,
schema effects and full-comparison fallback. The existing incompatible-type and
default-only tests remain applicable.

Independent review identified that ordinary full builds automatically reallocate
numeric IDs (`engine/write.rs::write_frame`), including inside the same dataset.
The added qualification fixes this gap. A regression gives an unrelated dataset
and a fresh same-dataset rebuild coincident numeric IDs, types and values under
different field names. Both directions and both value/occurrence identities return
unknown counts and refuse the stream, with default and forced-full options. This does not establish global stable field
identity across arbitrary full rebuilds; unproved changed schemas retain the prior
refusal.

A private unit case refuses renamed identities under identical display names,
changed native types, changed required/null rules, and membership differences in
both directions. No new integration-suite registration is needed.

The task owner will run formatting, the focused comparison suite and repository
checks. No build, formatting command, test command or commit was run in this
implementation subtask.

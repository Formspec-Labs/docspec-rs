# Scalar input bindings: consistency review

**Static verdict: APPROVE for this binding prerequisite.** The concrete comparison,
provenance and cached-readback defects identified during review are closed in the
current source. Final gate evidence is recorded separately below.

Reviewed against `59cc143` on 2026-10-08 using the semi-formal code-review and Rust
skills. This reviewer inspected implementation, callers, maintained dependency
source, test assertions and owner-generated logs. The reviewer did not execute
Cargo, modify implementation or create commits.

## Findings and closure

### F1 — Boolean comparison could hide incorrect scalar values — closed

- **Severity/category:** BLOCKER, correctness.
- **Evidence:** The initial value verifier joined unparenthesized
  `IS DISTINCT FROM` terms with `OR`. Pinned sqlparser 0.61.0 parses the right
  side of that comparison with `parse_expr()` (`src/parser/mod.rs:3848`), which
  consumes the following Boolean expression. Expected `(true, false)` and actual
  `(false, true)` can therefore produce a false mismatch predicate; mixed types
  can instead cause a planning error.
- **Closure:** Each comparison is now parenthesized
  (`src/engine/backfill/verification.rs:478`). The independently rehashed opposite
  Boolean fixture requires `invalid_group_values` while the correct release still
  verifies (`tests/scalar_bindings.rs:121`).

### F2 — A rewritten parent could redirect an inherited input — closed

- **Severity/category:** BLOCKER, correctness and provenance.
- **Evidence:** A one-step comparison trusts a forged parent input map. Merely
  following each binding's declared request is also insufficient: an intermediate
  full Build that renumbered fields can falsely claim to be an Update, inherit the
  wrong integer ID, and permit a descendant recomputed from another same-typed
  field. The original Backfill SQL can remain unchanged throughout that forgery.
- **Closure:** `verify_binding_history` walks actual owning selections
  (`verification.rs:272`). Source-producing bindings must match their owner's
  exact request and parent. Update/incremental Refresh hops preserve the exact
  native schema. Sibling Backfills inherit the exact binding, value table and
  source. Only the actual producing Backfill terminates the walk by freshly
  deriving bindings from its captured source. The adversarial fixture changes
  both parent metadata and current values, including the false operation-kind
  case (`tests/scalar_bindings.rs:177`).

### F3 — Cached readback omitted newly required ancestors — closed

- **Severity/category:** BLOCKER, recovery correctness.
- **Evidence:** `release_objects` lists immediate composition references. Once
  binding verification needs older manifests and origin schemas, removing one
  after readback could leave those immediate objects intact. A retry could then
  select a release that independent verification could no longer read.
- **Closure:** Cached local availability now calls the metadata-only binding
  checker (`src/engine.rs:1499`), as does each non-full related dependency node
  (`src/engine/verification.rs:145`). It reuses the same history proof without
  scalar-value or historical-payload execution. The readback fixture removes
  either an older manifest or the original schema metadata, requires retry to
  refuse with the prior selection unchanged, restores the bytes and resumes
  successfully (`tests/scalar_bindings.rs:268`).

## Patch summary and function trace

`ComputedGroup.input_fields` records SQL input names and the root native IDs they
mean in this source interpretation (`src/model.rs:504`). The SQL recipe and
logical output IDs remain in `GroupDefinition`. Fresh Backfill resolves current
names; automatic continuation preserves the recorded meaning. Full source
reconstruction derives explicit correspondence by prior current names because its
writer still assigns new native numbering. Format and processor versions advance
for this greenfield representation (`src/engine.rs:81`).

| Function | Location | Inputs/output and verified behavior |
| --- | --- | --- |
| `Expressions::compile` | `src/engine/backfill/expressions.rs:29` | Definition, exact native schema and optional binding → physical/logical expressions, output schema and material signature. Parses retained SQL against its alias schema, traverses maintained `Expr` nodes, rewrites column references to current names by root ID and requires the exact used binding set. |
| `Expressions::evaluate` | `expressions.rs:196` | Located source batch → computed values with the same occurrence columns. Physical column positions come from the current schema; unrelated additions/reordering cannot move the locator boundary unnoticed. |
| `inherited_inputs` | `src/engine/backfill.rs:570` | Request, prior binding and before/after pins → current binding. Update/incremental Refresh retain IDs; full Build/Refresh map prior ID through its prior current name to the new current root ID. Missing fields refuse. |
| `file_key` | `backfill.rs:613` | Exact source object, compiled material dependencies and eligibility → retained completion key. The global source schema is no longer a material salt. |
| `prepare_group` | `src/engine/backfill/preparation.rs:6` | Exact source, definition and optional bindings → prepared table plus mappings. Both whole-table preparation/validation keys and per-file keys include input meaning; file completion ownership and coverage checks remain. |
| `plan_backfill` / Backfill preparation | `backfill.rs:146`, `:392` | Fresh authored requests resolve current names consistently in planning and execution. Persisted `ComputedGroup` receives the actual prepared mapping. |
| Source/group assembly | `src/engine.rs:793` | A based source operation derives bindings before preparing every inherited group and records them beside its exact request/source/base. Selection guards remain unchanged. |
| `History::read` / `base` | `verification.rs:241`, `:259` | Exact object reference → shared decoded manifest. Uses the existing bounded hash-and-parse worker, rejects conflicting references for one path, shares ancestors across groups, and checks declared base identity. |
| `verify_binding_history` | `verification.rs:272` | Current owning selection and group → bounded metadata-only origin proof. Checks ownership, composition, remapping and identity-preserving schemas; rejects cycles/missing history and reaches a fresh authored binding. |
| `verify_group_data` | `verification.rs:355` | Source/group pins and mappings → schema, coverage and value verification. Exact non-overlapping locator coverage establishes one row per source occurrence; a separately executed relational plan reconstructs scalar values and compares them null-safely. |
| `verify_binding_dependencies` | `verification.rs:7` | Cached result → metadata availability/provenance check. Empty computed sets return immediately. Local cached readback and related dependency traversal both invoke it. |

## Data flow and invariants

1. **Names locate newly authored inputs; IDs preserve existing meaning.** Fresh
   compilation obtains IDs from the exact current native roots. Bound compilation
   creates an alias schema before parsing, so removal or reuse of an old display
   name cannot capture a different field. Missing/extra binding entries refuse
   (`expressions.rs:41-52`, `:130`). The implementation uses maintained expression
   traversal, not SQL text substitution or serialized query-engine objects.
2. **Material keys and result provenance serve different purposes.** Per-file
   dependencies include consumed native fields and the exact alias map, definition
   and processor (`expressions.rs:133-153`). Root names and ordinary documentation
   changes are excluded; types, requiredness and default interpretation remain.
   `arrow_metadata` makes documentation observable, so that function conservatively
   retains it. Whole preparation/validation keys also include source metadata and
   the map (`preparation.rs:24`, `:151`), preventing fresh-name and inherited-name
   results from colliding on the same current schema.
3. **Full reconstruction is an explicit correspondence boundary.** The mapping
   uses the old field's current name, not the SQL alias or equality of integer IDs.
   This keeps existing source-phase reuse independent of composition bases while
   making the new mapping reviewable and independently derivable. It does not
   preserve global source IDs/high-water across full reconstruction.
4. **The owning history authorizes inheritance.** The proof follows immutable
   composition parents, validates each owner's dataset/version/request, and checks
   exact sibling inheritance or source-operation mapping. A self-declared binding
   cannot switch operation kinds to avoid a remap. The request-local cache retains
   admitted manifests and shares their reads across groups; traversal yields
   cooperatively (`verification.rs:226-352`). This is an input-binding proof, not
   full re-execution of every historical operation.
5. **Value verification does not trust prepared output.** Coverage rejects missing,
   duplicate, unknown, null or out-of-range locations before comparison. Therefore
   joining expected and actual rows on location establishes a one-to-one comparison,
   including duplicate business rows. The expected values use a separate DataFusion
   plan over pinned native source occurrences, with the same explicit output type
   conversion; scalar values, nulls and Boolean differences are then checked.
6. **Recovery retains work without skipping new metadata dependencies.** Prepared
   files and tables remain reusable, while cached readback checks its binding
   history before selection. A lost ancestor produces a refusal without rerunning
   scalar payload processing or changing the selected result.

## Test behavior and edge cases

The reviewer inspected these assertions; execution was performed by the owner.

| Fixture | Location | Evidence supplied |
| --- | --- | --- |
| Rename, reorder and name reuse | `src/engine/backfill/expressions/tests.rs:46` | A quoted alias continues consuming its original ID after rename/reorder/unrelated addition. Fresh name resolution consumes the new ID and produces a different value/signature. Missing and extra bindings refuse. |
| Constants, types and metadata observers | `expressions/tests.rs:108` | Literal-null expressions have no input binding; unrelated schemas preserve their signature. Consumed type changes invalidate. Ordinary docs changes preserve value dependencies, while `arrow_metadata` observes different values and invalidates them. |
| Native completion reuse and collision | `src/engine/backfill.rs:706` | A real metadata rename shares exact completion records and computed Parquet objects while assembling new metadata. Fresh resolution of the reused name produces different files/null values, proving both per-file and whole-preparation isolation. |
| Reordered full rebuild | `tests/rebuild.rs:45` | Swapped native input numbering produces correct text lengths and numeric results against a Rust oracle. Replacing current mappings with the old IDs in a rehashed release refuses. |
| Malformed group/value cases | `tests/backfill.rs:630` | Native wrong-value tables with preserved schema/locations now fail `invalid_group_values`; wrong bindings and existing coverage/mapping corruptions also refuse. |
| Opposite Boolean values | `tests/scalar_bindings.rs:121` | F1 regression with independently rewritten native values and a rehashed release. |
| Rehashed parent redirection | `tests/scalar_bindings.rs:177` | F2 regressions for changed parent mappings and a full Build falsely labeled as Update, with wrong values recomputed to match the forged map. |
| Ancestor loss after readback | `tests/scalar_bindings.rs:268` | F3 refusal, unchanged authority, restoration and successful resume for both missing manifest and origin schema metadata. |
| Shared history admission | `src/engine/backfill/verification.rs:541` | Exact cache hits reuse the same decoded manifest without rereading or charging twice. Conflicting references refuse; cumulative capacity refuses before opening a missing next file; dropping the history releases admission. |

The existing rebuild, refresh, Backfill and related-result suites exercise the
shared callers, sibling composition, empty inputs, duplicates/nulls, history and
interrupted work. The new direct metadata-fork fixture qualifies a prerequisite;
it does not expose a public schema-evolution operation. The new history fixture
directly checks cumulative byte capacity and cache ownership. A direct binding-history
cycle fixture was not added; its refusal is supported by the inspected visited-set
guard, not claimed as a newly executed adversarial case.

## Execution evidence and conclusion

Inspected successful targeted logs: `scalar-binding-expressions.log`,
`scalar-binding-reuse.log`, `scalar-binding-backfill.log`,
`scalar-binding-rebuild.log` and `scalar-binding-history.log`. The last was
collected before the final owner-anchored request-kind regression. Earlier fixture
compile failures and the intermediate Clippy argument-count failure remain in
the corresponding `*-failed.log` files. The redundant wrapper was removed rather
than adding a lint exception.

The final `scalar-binding-check.log` records successful formatting, all-target,
all-feature Clippy and the complete test suite through doc tests; the owner
confirmed `./scripts/check` exited 0. This run includes the final owning-history
implementation and the false operation-kind regression. After that run, only a
test-only history cache/admission fixture and a module documentation line changed.
Formatting passed, and `scalar-binding-final-check.log` records successful
all-target, all-feature Clippy and the library suite including that fixture; the
owner confirmed exit 0. No production logic changed after the full gate. A
no-default-features check was not performed for this checkpoint.

**VERDICT: APPROVE for the scalar binding prerequisite.** No unresolved source
correctness finding remains. Inspected assertions and completed owner-run gates
provide adequate qualification for this scope. Confidence is high in the inspected
meaning, reuse-key and recovery paths. README and STATUS describe format 9 and
preserve the unfinished schema-evolution and performance boundaries.

This patch avoids unrelated-schema cache invalidation and adds proof of scalar
values. It also adds metadata-history work and complete current-value comparison;
their complete-path performance has not been measured. Public schema operations,
global stable source identity across full rebuilds, durable absence/default
semantics and broader cutover remain unfinished R3 work.

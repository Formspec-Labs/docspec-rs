# Selected-result protection review

## Findings

No material defect was established in this checkpoint. The implementation keeps
the candidate's protection attached to its immutable result and separately
protects the actual predecessor named by accepted history. It reuses the existing
selection transaction and protection types.

The final author-run repository gate passes. Its retained log was inspected
through the documentation-test tail; the author reported exit status 0. No Rust
changes followed the reviewed source before that gate.

Reviewed statically against `fdba90a`, using the semi-formal code-review and Rust
skills, repository guidance, `docs/PLAN.md` and the retention requirement. The
reviewer read the production diff, callers, scope helpers, catalog commit path,
inventory readers and changed tests. The reviewer did not run Cargo, formatting,
tests or benchmarks, and changed only this report.

### O1 — Protection remains deliberately incomplete outside immutable selections

- **Severity/category:** OBSERVATION; scope and correctness boundary.
- **Evidence:** `src/engine/history.rs:243` writes ordinary mutable authority;
  `src/engine/history.rs:258` writes input progress without a binding. The new
  bindings cover `result` and `succession` only. `docs/STATUS.md:29` explicitly
  records the unfinished checked authority-to-result link, progress protection
  and disabled physical removal. The proposed authority link reuses the immutable
  result's binding; it is future work, not a second stored binding in this patch.
- **Disposition:** Correctly scoped. This approval does not qualify grant
  retirement, removal, automatic expiration, restore or hosted coordination.
  Those operations must not infer complete protection from this checkpoint.

### O2 — Metadata work is bounded, but not constant with retained state

- **Severity/category:** OBSERVATION; performance and memory.
- **Evidence:** The added path reads the immediate predecessor's result and
  binding from the existing `SnapshotRows`, reserves decoded memory, and copies
  admitted protection references (`history.rs:199`, `:206`, `:208`). It does not
  reopen owners, traverse ancestors or inspect payloads. Scope copies acquire
  permits before cloning (`retention/owners.rs:60`); merging deduplicates and
  enforces the metadata limit (`retention/owners.rs:104`). The catalog still
  clones its bounded current record map and charges append history before commit
  (`src/catalog.rs:313`).
- **Disposition:** Appropriate for this small propagation change. Result scopes
  do not inherit unrelated predecessor scopes, avoiding artificial accumulation
  across independent builds. Succession carries the two endpoint scopes. Large
  result inventories, many grants and retained lifecycle history can still reach
  the existing capacity refusal; no throughput or large-history claim was tested.

## Patch summary

The source preparation path previously discarded the readback's protection before
selection. `select_prepared` now receives `Retained<ReleasePin>` for both fresh
and resumed readbacks (`src/engine.rs:1133`, `:1514`). Refresh passes the same
wrapper through its existing input guards (`src/engine/refresh.rs:222`).

`select_with_progress` constructs the result binding from that saved scope. It
builds a separate succession scope from the candidate and the actual preceding
selection, obtained under the selection lock and from the same native view
(`src/engine/history.rs:179`, `:199`). The existing backfill composition path can
therefore inherit selected sibling protection instead of treating every result
as unknown (`src/engine/backfill.rs:583`).

## Function trace

| Function or seam | Location | Inputs → output and verified behavior |
| --- | --- | --- |
| Saved readback branch | `src/engine.rs:1133` | Saved release and binding from one view → `Retained`; verifies operation identity before selection and retains its original scope. |
| Fresh readback / `select_prepared` | `src/engine.rs:1487`, `:1514` | Retained readback → selected release. Completed operation reconciliation precedes a new selection; eligibility, coverage and base guards remain. |
| `select_refresh` | `src/engine/refresh.rs:222` | Retained release → selected release after existing input locks, ancestor and coverage guards. It passes the wrapper intact to the shared selection routine. |
| `select` | `src/engine/history.rs:142` | Guarded authority plus retained release → optional input progress and common selection. Backfill calls this same function. |
| `select_with_progress` | `src/engine/history.rs:160` | Current authority and candidate scope → one native commit containing authority, immutable result/succession, their known bindings and optional progress. |
| `read_head` / `read_link` | `src/engine/history.rs:298`, `:325` | Cached rows → exact selected revision and predecessor link. Their existing identity/revision checks precede protection propagation. |
| `completion_records` | `src/engine.rs:90` | Claim plus optional scope → claim/binding records for the same next native revision. Unknown scope produces no binding. |
| `retained_protections` / `local_binding` | `src/engine/retention/owners.rs:403`, `:421` | Exact claim in one view → admitted immutable references or unknown. Validate consumer, claim digest and revision without owner reacquisition. |
| `duplicate` / `inherit` | `src/engine/retention/owners.rs:60`, `:80` | Known scopes → admitted copies/union. Unknown on either side keeps the dependent scope unknown. |
| Backfill composition / promotion | `src/engine/backfill.rs:583`, `:599`, `:732` | Prepared target plus current selected composition → new retained readback. Uses the selected result's exact scope and retries composition if the selection changed before promotion. |
| `Lifecycle::commit` | `src/catalog.rs:257` | All selection records → guarded native append. Budget or stale-view failure cannot acknowledge only a binding or only its claim. |
| Inventory roots | `src/engine/retention/inventory.rs:847`, `:926` | Result/succession records → exact release roots; succession retains both endpoints. The new bindings use the existing typed inventory path. |

## Data flow and invariants

1. **Protection follows the saved bytes actually consumed.** Material reuse may
   adopt an earlier owner's input. Both resumed and fresh paths pass the saved
   readback's scope, rather than reconstructing it from the latest request.
   `Retained` does not escape through the public API; the selected `ReleasePin`
   remains the public return value (`engine.rs:1552`).
2. **The predecessor is selected at the selection boundary.** Two unbased builds
   can finish preparation before either selects. The later selection uses the
   guarded authority's actual predecessor, not its admitted request or optional
   `parent_manifest` (`history.rs:179`, `:204`). When the predecessor has a known
   scope, its full retained result must also equal authority before lending that
   scope (`history.rs:207`).
3. **Known does not replace unknown.** A known candidate after an unknown
   predecessor still receives its known result binding. Only the succession
   scope becomes unknown. A known predecessor likewise cannot upgrade an unknown
   candidate. This follows the shared `inherit` truth table and the distinct
   scopes passed at `history.rs:249` and `:256`.
4. **Claim and binding are one acknowledgment.** The two bindings use the same
   captured next revision as their claims and commit together (`history.rs:244`).
   Existing result reconciliation returns the saved result after an acknowledged
   selection instead of appending another selection (`engine.rs:1531`).
5. **Sibling recomposition preserves completed work.** Backfill merges prepared
   target protection with the currently selected result, writes a new composition
   readback when necessary, and leaves the earlier target readback intact. A
   changed current selection causes recomposition before commit
   (`backfill.rs:599`, `:734`).

## Test behavior and execution evidence

The test assertions were read; execution was supplied by the author.

| Test | Location | Relevant assertion and observed result |
| --- | --- | --- |
| `material_reuse_carries_original_owner_scope_into_validation_and_readback` | `src/engine/retention/owners/phase_tests.rs:152` | Material-identical inputs under different owners retain the original phase scope through readback, result and succession. Repeated selection leaves native revision and payload metadata unchanged; owner grants are not reacquired. Passes in the source log. |
| `succession_inherits_the_actual_predecessor_after_overlapping_unbased_builds` | `phase_tests.rs:287` | Both builds stop at readback. After A then B selects, result B contains only B's scope while succession B contains A and B, names A as predecessor, and reconciles unchanged on retry. Passes in the source log. |
| `unknown_predecessor_scope_does_not_erase_the_selected_results_known_scope` | `phase_tests.rs:353` | An explicitly retired/replaced unbound generation remains unknown. B's result keeps its known scope; succession following A remains unbound. Passes in the source log. |
| `scalar_completions_keep_foreign_scope_across_stops_reuse_and_reopen` | `src/engine/backfill/protection_tests.rs:116` | Stops/retries and engine reopen retain exact scalar claims/bindings. A sibling selects before target promotion; recomposition adopts the sibling and the original target files, preserves the old readback, and writes matching result/succession bindings without changing owner revisions or scalar payloads. Passes in the focused log. |

The helpers also resolve the referenced owner grants and require each claim's
revision to equal its binding's revision (`phase_tests.rs:134`,
`backfill/protection_tests.rs:301`). The source log includes the existing exact
retirement, refresh-evidence and schema/group protection regressions passing.

Inspected evidence:

- [Compile log](selection-protection-compile.log): successful development check.
- [Focused log](selection-protection-focused.log): the scalar protection test and
  owner-handoff/forged-identity regressions pass.
- [Source protection log](selection-protection-source.log): all named source
  protection regressions above pass.
- [Complete repository gate](selection-protection-check.log): formatting,
  all-target/all-feature Clippy with warnings denied, and the complete
  all-feature test run pass through the documentation-test tail. The log includes
  the new selection/sibling assertions and the existing refresh, schema,
  succession, retirement and update regression suites. The author reports exit
  status 0.

There is no dedicated new test that injects a lost native commit response
specifically between the new
selection records and cache publication, nor one that forges only the full
predecessor body. Their handling is supported by the unchanged atomic catalog
commit/reconciliation path and the explicit new equality guard; these are static
coverage boundaries, not claimed new failure-injection results. No memory peak,
large-history or throughput measurement was performed.

## Conclusion

**VERDICT: APPROVE for the scoped immutable selection change.**

- The implementation preserves candidate and actual predecessor protection in the
  existing guarded transaction without new owner acquisition or payload work.
- Coverage of changed behavior: **ADEQUATE** for this checkpoint. Focused author
  execution and the complete repository gate pass.
- Confidence: **HIGH** in the reviewed propagation and conservative fallback;
  this verdict does not extend to unfinished destructive maintenance.

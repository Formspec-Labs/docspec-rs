# Accepted succession consistency review

Scope: the uncommitted checkpoint after `e7f64a2`, reviewed statically on
2026-10-08 under the semi-formal code-review and Microsoft Rust guidance. The
reviewer read the implementation, selection callers, retained catalog state,
transports and fixtures, and executed no tests. Only this report was edited.
The scope is accepted-version ordering for R5, not related-input selection,
join maintenance, payload availability or complete change-history certification.

## Findings

### F1 — Anchor the current head to its actual acceptance commit

- **Severity:** BLOCKER; consistency. **Status: CLOSED.**
- **Initial location:** `src/engine/history.rs:45-48`, `:207-243` in the initial
  implementation inspected.
- **Evidence chain:** `read_link` initially required result and succession rows
  to share a native revision, but traversal discarded the authority row's
  revision and kept no durable selection revision. Strictly decreasing revisions
  detected a rewritten older pair only when a newer child was visited first.
  The current head had no such child anchor.
- **Failure sequence:** Select A, B and C. Rewrite C's result and succession in
  one later lifecycle commit, changing its predecessor to A while retaining C's
  exact manifest identity. Authority still names C. The original traversal
  accepted the rewritten pair and could report the wrong accepted distance;
  equal-C endpoints also succeeded immediately. Merely checking the authority
  row's current revision is insufficient because later admission or invalidation
  changes that revision without selecting a new release.
- **Required correction:** Record `Authority.selection_revision` in the same
  commit as authority/result/succession. Preserve it through admission and
  invalidation. Require the head pair's native revision to equal that anchor,
  with `0 < selection_revision <= authority_revision <= observed_revision`.
  Check selected/release identities and boundary/eligibility consistently in
  both traversal and new selection. A corrupted current head must not become
  the predecessor of a newly accepted result.
- **Closure:** `Authority.selection_revision` is assigned only by selection.
  `read_head` (`src/engine/history.rs:221`) validates the selected/release
  identities and the native authority revision bounds. `check_selected_link`
  (`:287`) validates the acceptance revision, boundary and eligibility in both
  `follows` (`:65-67`) and `select` (`:159-160`). The final fixtures reject a
  rewritten current pair, equal-head claims, rewritten heads followed by later
  invalidation, future-declared acceptance and mismatched selected metadata.
  Rewritten heads also cannot become the predecessor of another selection.

### O1 — Ordering proof and payload verification remain separate

- **Severity:** OBSERVATION; scope.
- **Location:** `src/engine/history.rs:7-15`; `tests/succession.rs:159`.
- **Assessment:** The proof checks retained accepted identities and metadata.
  It does not open release manifests or source/change payloads. This is deliberate:
  missing optional change acceleration must not erase accepted source order.
  Successful proof does not establish present payload availability, a valid
  derived-result computation, complete upstream acquisition or a standalone
  cryptographic certificate. The README and STATUS preserve these limits.

## Patch summary

Both existing selection paths now call one `select` helper. It derives the actual
previously selected release, then commits authority, immutable result identity and
succession together using the existing guarded native lifecycle commit. The edge
is independent of optional changes and of the request's composition base.

`prove_succession` and its CLI/HTTP interfaces capture one lifecycle view and walk
from the selected head through target to prior. Exact manifest references identify
the endpoints. The walk retains an existing `Arc<SnapshotRows>` across cooperative
yields, checks native row revisions and bounded metadata, and returns observed
selection distances. No new durable index, scheduler or cursor is added.

## Function trace

| Function/type | Location | Inputs and outputs | Inspected behavior |
| --- | --- | --- | --- |
| `VersionRef`, `ReleasePin::version_ref`, `Succession`, `SuccessionProof` | `src/model.rs:100-138` | Release → bounded identity, accepted edge and observation | Carries dataset/version/exact manifest; predecessor is another exact reference. Distances describe accepted selections, including composition-only changes. |
| `Authority.selection_revision` | `src/model.rs:165` | Selection commit → retained acceptance anchor | Set only by selection; existing admission and invalidation clone/update authority while preserving it. |
| Source selection caller | `src/engine.rs:843-865` | Completed readback and captured operation → guarded selection | Completed retry returns first. Existing eligibility, definition, base, strict rebuild boundary and coverage checks precede shared selection. |
| Backfill selection caller | `src/engine/backfill.rs:469-484` | Verified composed release and current authority → selection or recomposition | Existing target/source/eligibility checks remain; a changed compatible composition retries before selecting. |
| `Engine::select` | `src/engine/history.rs:128` | Captured view, authority and release → one native commit | Refuses replacing an accepted edge; derives predecessor from actual authority; checks its retained evidence; commits result, edge and updated authority together. F1 adds the durable head anchor. |
| `Engine::prove_succession`, `follows` | `src/engine/history.rs:16`, `:30` | Exact prior/target → proof or structured refusal | Captures one source view; verifies head → target → prior membership, including equal endpoints; rejects backward coverage, missing identities and malformed history. |
| `read_head`, `read_link`, `check_selected_link`, partial metadata structs | `src/engine/history.rs:192`, `:221`, `:248`, `:287` | Retained rows → exact head/link and native revision | Deserializes only needed result/head fields, avoiding native payload inventories. Matches result/link revisions, result verification flag, version identity and descriptive metadata. |
| `SnapshotRows::get_record` | `src/catalog.rs:41` | Key → native revision, serialized size and decoded value | Uses the retained immutable state, exposing the revision necessary for ordering checks without a new scan. |
| `Lifecycle::rows`, `get` | `src/catalog.rs:140`, `:118` | Captured view → retained state/value | Exact metadata identity selects the cached state; an owned `Arc` preserves both rows and reservation across another dataset's cache replacement. Ordinary get retains its prior value semantics. |
| `Lifecycle::commit`, native guard | `src/catalog.rs:205`, `:363` | Captured view and rows → atomic next view | Assigns all rows the same `view.revision + 1`; rejects duplicate keys, resource overflow and stale native state; publishes the next cache only after successful commit. |
| CLI `CheckSuccession` | `src/main.rs:245` | Two release JSON files → proof JSON | Extracts bounded references and delegates to the shared engine service. |
| HTTP `succession`, error mapping | `src/http.rs:130`, `:41` | Bounded JSON endpoint pair → proof/refusal | Calls the same service. Backward/unproven coverage returns conflict; metadata capacity returns unavailable. |

## Data flow and invariants

| Invariant | Creation, mutation and consumption | Assessment |
| --- | --- | --- |
| Acceptance alone creates an edge | A candidate completes phases/readback first; `select` writes the edge only with final authority/result | Prepared or failed candidates cannot establish accepted coverage. Lost-response retries return the original result without another edge. |
| Actual predecessor is authoritative | `select` reads `authority.release` while the caller holds its dataset lock and guarded view | An unbased full Build links to the previous selection; independent group B links to selected sibling A even when B captured an older base. |
| Head proof starts at genuine acceptance | Authority preserves the acceptance revision separately from later mutable updates | F1 closes the unanchored current-head rewrite. Later authority revisions do not redefine when a result was accepted. |
| Every compared endpoint belongs to one observed chain | Walk starts at captured head and searches both exact references | Forward/equal/backward results depend on chain positions. A plausible orphan pair cannot pass, even when prior and target are identical. |
| Native revisions establish edge direction | Each result/link pair has one positive revision; visited predecessor revisions strictly decrease | Rewriting an older pair after its child or visiting a cyclic/nondecreasing chain refuses. Boundary/eligibility checks validate descriptions, not ancestry. |
| One immutable observation remains one observation | The walk holds one retained row state and its memory permit throughout | Cache replacement elsewhere cannot silently change its head or force a per-link replay. The reported revision remains the captured lifecycle revision. |
| Metadata work is finite | Existing lifecycle replay bound, traversal byte charge, link limit and cooperative budget consumption apply | No collection of all release payload inventories or growing path vector is built. This is not a measured whole-process memory or latency guarantee. |
| Required order is independent of optional payloads | Succession resolves native lifecycle records only | Removed optional change files or unavailable original input bytes do not fabricate unknown order; missing succession itself explicitly refuses. |
| Existing selection guards remain authoritative | Common helper is reached only after operation-specific guards | Succession does not grant stale work new promotion authority or change captured-versus-strict cutover policy. |

## Test behavior and edge cases

| Fixture | Location | Inspected assertion and trace |
| --- | --- | --- |
| `every_selection_records_actual_predecessor_in_the_same_native_commit` | `tests/succession.rs:72` | Selects initial, unbased Build, independent sibling groups, Update and explicit rebuild. Checks every actual predecessor and native result/link row revision, including final authority revision; checks forward/equal/backward proofs, distances and unchanged records after retries. Removing input/change payloads breaks ordinary payload verification while succession still succeeds. |
| `admission_and_prepared_releases_do_not_create_accepted_history` | `tests/succession.rs:184` | A read-back but unselected candidate has no edge. Newer selected work remains unchanged on retry; old work cannot select or prove acceptance. |
| `identity_mismatch_and_orphan_branches_cannot_establish_coverage` | `tests/succession.rs:239` | Alters digest or dataset, then injects a plausible orphan result/link pair outside the selected head's chain. Both forward and equal orphan claims refuse; legitimate history still proves. |
| `missing_malformed_rebound_and_cyclic_history_refuses_without_payload_reads` | `tests/succession.rs:321` | Separates missing links, malformed records, mismatched pair revisions, older-pair rewrites, current-head pair rewrites, later authority changes, boundary mismatch and self-cycle. Synthetic malformed-history cases update their synthetic anchor where needed so they reach the intended traversal guard rather than failing F1 first. |
| `selected_head_cannot_predeclare_a_future_commit_or_disagree_with_its_release` | `tests/succession.rs:441` | Checks dataset/version/boundary/eligibility disagreement, false verification status and an authority row that predeclares a later result/link commit. Even equal endpoint proof refuses these malformed heads. |
| `small_history_budget_refuses_and_transports_share_the_proof_service` | `tests/succession.rs:500` | HTTP produces the same proof/refusal semantics; CLI after reopen matches the successful HTTP proof. A small lifecycle budget refuses explicitly. This does not isolate a long-history traversal throughput bound. |
| `retained_snapshot_survives_cache_replacement_and_holds_its_reservation` | `src/catalog.rs:442` | Retains dataset A's rows, replaces the shared cache using B, hides A's native directory, then successfully reads retained A. Checks that its reservation remains charged until the last retained owner drops. |

The final child-process helper adjustment in `tests/support/mod.rs:15` writes a
small explicit correctness-fixture resource profile and passes that same file to
both killed and resumed workers. It avoids requiring the production scratch
allowance for tiny fixtures; it does not change production limits or establish
resource-performance results. The retained `succession-disk-failed.log` preserves
the earlier failure rather than treating it as a passing run.

Final execution evidence is owner-supplied. The implementing agent reports
`./scripts/check` and `cargo check --locked --no-default-features` completed with
exit 0. The reviewer inspected `docs/reviews/succession-check.log`, including the
accepted-history, final head-corruption, retained-cache and existing regression
suites. Formatting and Clippy are part of the repository gate. This reviewer
executed no tests or benchmarks.

## Conclusion

**VERDICT: APPROVE.** F1 is closed, and the final repository gate and
no-default-features check passed. No unresolved consistency blocker remains for
this checkpoint.

The implementation follows the intended small design: one accepted edge at
selection, a durable acceptance anchor, one captured metadata view, exact endpoint
references and explicit bounded refusal. The shared selection and head-validation
helpers keep both source and backfill behavior aligned without another stateful
subsystem.

**Coverage: ADEQUATE** for the scoped prerequisite, including actual predecessors,
native commit revisions, retries, candidate exclusion, exact/equal endpoints,
orphan branches, malformed and rewritten history, transport parity and retained
snapshot lifetime. **Confidence: HIGH** in the static assessment. This does not
complete multi-input consistency, joins, affected-group maintenance, retention,
restore or the Comments performance gate.

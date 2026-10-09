# Availability retirement implementation review

**Disposition: APPROVE the corrected scoped implementation on static review;
full-gate rerun pending.** The current-selection finding and the first-run
namespace-creation race are corrected in the inspected source. The race was found
by the author's full gate after the initial static verdict; it was missed by that
review. Both findings, the marker-order failure, and their corrections remain
recorded below. This review does not authorize physical removal or claim
completion of R6.

This independent review compares the uncommitted implementation with `4fd1ca6`.
It applies the repository instructions, Rust guidance, semi-formal review method,
and R4/R5/R6 requirements. The reviewer read production changes, their callers,
and the available tests, but did not edit Rust, run Cargo or formatting, or commit.
The shared type and record-key rename is an intentional greenfield API decision;
no released compatibility promise is inferred.

## Patch summary

The patch moves phase retirement into one shared service with a closed scope:
phase recovery or one operation's standalone result availability. Availability
retirement captures its result and every saved readback generation, excludes the
current selection, and requires abandonment for unfinished operations. Execution
compares the complete fresh set before appending one receipt, exact-generation
markers, and an operation-level pointer.

Original result, readback, succession, and progress records remain unchanged.
Public lookup and replay of the retired operation refuse; independently retained
consumers can still resolve its original receipt. This implements the distinct
content/history/recovery lifetimes in R6 (`docs/requirements/06-maintenance-retention.md:9`,
`:23`, `:27`) and preserves R4 accepted ordering and R5 captured coverage.

## Function and data-flow trace

| Path | Inputs and resulting behavior | Evidence |
| --- | --- | --- |
| Shared scope and generations | A closed scope restricts keys; sorted exact native generations identify what expires. Phase and availability APIs reject the other scope. | `src/model.rs:496`; `src/engine/retention/retirement.rs:30`, `:280`; `src/engine/retention/availability.rs:315` |
| Availability planning / `collect` | Operation and request identity, authority, abandonment when needed, and bounded saved identities produce the complete generation set. No payload or external manifest is opened. | `src/engine/retention/availability.rs:108`, `:262` |
| Shared execution | Exclusive owned-host worker admission, a fresh locked view, native boundary checks, exact-set comparison, one atomic append, and durable response reconciliation. | `src/engine/retention/retirement.rs:287`, `:296`, `:333`, `:380` |
| Receipt and marker validation | Checks dataset/table, scope keys, positive ordered revisions, sorted unique keys, marker membership, and same-commit pointer identity. | `src/engine/retention/retirement.rs:60`, `:80`, `:111`, `:136`; `src/engine/retention/availability.rs:36` |
| Operation availability | Pointer and receipt are decoded under reservations; scope/operation and native revisions must agree; all markers and pointer validate before refusal. | `src/engine/retention/availability.rs:59`, `:322` |
| Serialized operation entry | Takes the dataset lock before opening or creating its lifecycle, checks availability, and releases the lock before preparation. | `src/engine/retention/availability.rs:243`; `src/engine.rs:761`; `src/engine/backfill.rs:365` |
| Public run / plan | Build, Update, Schema, and Refresh share the guarded admitted entry. The run check precedes preparation and its success reconciliation. | `src/engine.rs:280`, `:753`; wrappers in `src/engine/schema.rs:36`, `:46`, and `src/engine/refresh.rs:16` |
| Backfill run / plan | Own-operation retirement is checked before base reads or preparation. Fresh admission also checks the historical base; an existing admitted operation keeps raw base resolution. | `src/engine/backfill.rs:195`, `:359`, `:418` |
| Public historical result | The new available resolver checks retirement under the dataset lock, then uses the existing exact release decoder. Internal processing retains the raw resolver. | `src/engine.rs:455`, `:465`, `:472` |
| Fresh input capture and seals | Fresh capture and admission require availability; new seal construction uses available result lookup. Existing seals reconcile their separately retained promise first. | `src/engine/inputs.rs:37`, `:66`, `:153`, `:169`; `src/engine/admission.rs:147`, `:186` |
| Existing dependency recovery | Admitted Refresh uses retained capture; legacy capture keeps its previous conservative behavior. Ordering and ancestor guards continue using raw receipt qualification. | `src/engine/refresh.rs:64`, `:70`, `:271`, `:300`; `src/engine/inputs.rs:261` |
| Inventory | Validates each retirement receipt once, reuses exact retired record/operation sets, filters matching independent roots and bindings, and rejects orphan pointers/markers. | `src/engine/retention/inventory.rs:147`, `:167`, `:196`, `:235`, `:248` |
| Selected authority | One shared validator checks full authority/result identity, original selection revision, version ID, owner, table, and absence of a retirement pointer on the selection. | `src/engine/retention/inventory.rs:287`; `src/engine/retention/availability.rs:150` |
| HTTP and CLI | Thin availability plan/execute bindings use the shared types. Retired availability maps to HTTP Gone; selected/active refusals map to conflict. | `src/http.rs:45`, `:124`, `:151`; `src/main.rs:76`, `:304` |

## Invariants and recovery reasoning

**A1 — retirement cannot race past a successful entry check. Confirmed within
the owned local host.** Every affected service entry runs inside `admit_work`,
which acquires the maintenance read guard before spawning the owned worker. That
guard remains held through preparation, selection, and reconciliation
(`src/engine/worker.rs:129`, `:172`). Retirement acquires exclusive ownership
before its dataset lock (`:152`, `retirement.rs:299`). It cannot appear partway
through an admitted operation. The own-operation checks are outside the error
reconciliation blocks, so a raw retained result cannot turn the initial refusal
back into success. Cancellation does not discard a spawned worker's ownership.
Injected external catalogs remain refused by the retirement service.

The corrected `check_operation_entry` also serializes first-time native creation
under the same dataset lock used by admission (`availability.rs:243`). It acquires
that lock after worker read ownership and releases it before preparation acquires
consumer/input locks. It performs no foreign-owner acquisition or nested admitted
call. Caller cancellation still leaves the owned worker and its maintenance
guard alive; forced worker termination retains the existing host quarantine
behavior (`src/engine/worker.rs:55`, `:172`). The helper therefore closes the
creation race without changing maintenance lock order or cancellation custody.

**A2 — a stale plan cannot expand or partially retire the operation. Confirmed.**
`collect` includes the exact result/readback keys plus every canonical Backfill
composition key. Execution freshly repeats collection and requires equality with
the saved sorted generation list before producing markers
(`retirement.rs:335`). The shared generation loop also checks each current native
revision. A new or changed readback causes refusal; it is not silently added.
Current selections refuse after checked authority identity, and readbacks without
a result require exact durable abandonment (`availability.rs:167`).

**A3 — durable retry preserves the same decision. Confirmed by trace.** A receipt
with the same retirement ID must match the full plan and native revision; its
markers and pointer validate before return. After an uncertain append, only an
identical durable receipt returns success. A different receipt refuses. All
changes use the existing guarded native commit, so no independent pointer write
can expose half a decision (`retirement.rs:303`, `:380`).

**A4 — availability is distinct from phase reuse and retained dependency use.
Confirmed.** Phase retirement still makes that generation absent to
`active_completion`, permitting later recomputation at the same material key.
An availability pointer instead refuses the retired operation ID. Internal
result/readback fast paths remain raw because the admitted entry has already
checked that operation. Processing of a different admitted consumer can still
read its retained inputs and pass captured ordering/ancestor guards. A fresh
consumer must acquire available inputs first. Unknown legacy capture or missing
protection scope does not become a known empty scope.

**A5 — filtering one promise does not erase shared protection. Confirmed.**
Inventory filters only the exact independent generation and its matching
binding. It still validates a binding's local identity before filtering it.
Operation bases, captured request dependencies, retained phases, and descendant
manifests still expand raw referenced content. Owner grants are still separately
validated and rooted (`inventory.rs:187`); no grant is retired by this API.
Succession/progress keep the original result receipts and revisions. Logical
availability retirement therefore does not imply that bytes become reclaimable,
and it is not an access-control revocation for already held immutable pins.

**A6 — bounded metadata and shared mechanics. Confirmed for inspected paths;
not measured.** Discovery reserves a metadata allowance and charges key/vector
growth before pushing. Per-record decoding is bounded and extracts only readback
identity. Operation/request/authority/current-result decoding receives separate
known-size reservations. The shared executor retains request memory before
cloning its plan. Inventory charges its retired-record and retired-operation sets
and validates each receipt once; pointer scanning reuses those checked identities.
Catalog history/cache and commit budgets remain in force. Markers add bounded
history; this is not metadata compaction or a Q7 scaling result.

**A7 — unbased Build no longer retains old payload merely for ordering.
Confirmed.** Admission passes a selected release to protection acquisition only
when the request has an explicit base (`src/engine.rs:1052`). Source processing
consumes old source/schema content only for that explicit base
(`src/engine/write.rs:102`). Existing evolved-schema, computed-group,
relationship, and related-source guards remain. Selection obtains its actual
predecessor from inline history (`src/engine/history.rs:179`). Existing grants
remain intact. The improvement is removal of unnecessary new closure traversal
and retained dependency state; no runtime or byte-saving measurement is claimed.

## Findings

**Resolved material finding — partial current-selection identity.** The first
implementation excluded only `authority.release.id == operation` after comparing
selected dataset values. A malformed release ID could disagree with
`authority.selected.version`, allowing retirement of the selected version while
`latest` still returned it. The author extracted `inventory::validate_selection`
and calls it from both inventory and retirement collection. It now requires the
release ID to equal the selected version, the manifest owner/table to match, the
full immutable result to equal authority, and the original result revision to
equal `selection_revision`. A selected availability pointer also refuses. The
reviewer inspected this correction and the author's successful corrected internal
test log; that execution remains author evidence.

**Resolved material finding from the full gate — unguarded first-run namespace
creation.** The initial availability check in `run_admitted` called
`Lifecycle::open` before taking the dataset lock. `open` can create the native
namespace/table and explicitly requires callers to serialize first-time creation
(`src/catalog.rs:127`). Concurrent first-time retries raced that creation. The
existing `simultaneous_retries_reconcile_to_one_immutable_release` test failed
with a SQL unique-constraint violation in `iceberg_namespace_properties`, retained
in [the first full-gate log](availability-retirement-check.log).

The author introduced the shared locked `check_operation_entry` and uses it from
both run and Backfill before preparation and outside error reconciliation. The
reviewer inspected the helper's lock lifetime and worker ownership as described
in A1. The existing focused concurrency test then passed in
[the concurrency log](availability-retirement-concurrency.log). That focused pass
does not replace the full-gate rerun. The initial static review did not detect
that the new early catalog read also performed creation.

**Resolved test failure — marker checks depended on lexical order.** The initial
internal run reported `invalid_json` instead of the expected `invalid_retirement`
for the existing forged-marker fixture. Renaming `phase-retired/` to `retired/`
moved markers after active phase rows in lexical order, exposing the fixture's
intentionally undecodable phase before its unauthorized marker. Both paths
refused, but detection depended on root ordering. Inventory now checks all marker
membership in a prefix pass before interpreting active roots (`inventory.rs:167`).
The existing expectation was preserved, and the corrected author run passed.

No additional material correctness, recovery, lock-order, or maintainability
finding remains. The common service removes duplicated commit/reconciliation
mechanics while retaining scope-specific eligibility and lookup behavior.

## Tests inspected and qualification boundary

The following are static expectations from source assertions, not reviewer runs:

| Test | Expected behavior |
| --- | --- |
| `tests/availability_retirement.rs:142` | Current selection refuses; a historical Build captures result plus readback; incomplete plans refuse; retirement and retry survive reopen; own run/plan and historical lookup refuse; files and original receipts remain; succession still proves forward order. |
| `tests/availability_retirement.rs:267` | Backfill retains distinct saved compositions; active unfinished work refuses retirement; after abandonment all compositions retire together; retry performs no work and keeps the sibling selection. |
| `tests/availability_retirement.rs:369` | A protected dependent Refresh resumes from completed processing after producer retirement; fresh capture and a new consumer refuse; exact aggregate values and original completed phases remain. |
| `src/engine/retention/retirement/tests.rs:25` | The shared phase path reconciles a lost native response and concurrent identical retry into one durable retirement, including reopen. |
| `src/engine/retention/retirement/tests.rs:69` | Missing markers and markers outside receipt membership refuse both reuse and inventory. |
| `src/engine/retention/retirement/tests.rs:145` | Unknown legacy processing cannot adopt a replacement admission after an older generation retires. |
| `src/engine/retention/availability/tests.rs:58` | Lost response and concurrent identical retirement retries preserve one native decision; independent retained work still protects the old manifest. |
| `src/engine/retention/availability/tests.rs:100` | Wrong authority operation ID, owner, selection revision, and immutable receipt identity refuse at plan, execution, and inventory. Even an otherwise consistent historical alias cannot become current after retirement. |
| `src/engine/retention/availability/tests.rs:193` | Missing pointer, receipt, or markers and wrong pointer identity refuse reconciliation and inventory; present malformed pointers also refuse operation replay. |
| `tests/phase_retirement_transport.rs:27` | CLI and HTTP produce the same availability plan, current selection conflicts, historical lookup returns Gone after retirement, latest remains exact, and cold CLI retry returns the same decision. |
| `tests/lifecycle.rs:655` | Concurrent first-time retries return one identical release and its selected value passes verification. The first full gate exposed the new creation race; the existing test passes after the locked-entry correction. |
| Existing phase and transport tests | Mechanical shared-type/key updates retain exact generation retry, independent completed-phase recovery, and common CLI/HTTP behavior. |

The reviewer read the failed author
[internal run](availability-retirement-internal.log) and the successful
[corrected internal run](availability-retirement-internal-corrected.log), which
includes the availability corruption/retry cases, shared retirement cases,
inventory, and owner-protection checks. The earlier
`availability-retirement-compile-corrected.log` predates the authority extraction
and is not qualification of that later change. The reviewer subsequently read
the successful [integration and transport log](availability-retirement-integration.log),
the failed [first full gate](availability-retirement-check.log), and the successful
focused [concurrency correction log](availability-retirement-concurrency.log).
The full-gate rerun after the locked-entry correction remains pending and must be
reported from its own completed receipt.

Static confidence is high in the owned-host entry-check, commit, root filtering,
and dependency-recovery traces. Qualification is limited to explicit logical
retirement. Physical deletion, owner-grant retirement, automatic expiration,
public/reader windows, restore fencing, and full R6/R1–R7 acceptance remain open.

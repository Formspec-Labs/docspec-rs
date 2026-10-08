# R6 retention architecture review

**Verdict: APPROVE the narrowed logical-abandonment design; physical deletion is not ready.** Stop an exact operation from resuming or committing, preserve every completed phase/release and attempt reservation, and delete nothing. Existing dataset locks and guarded commits suffice for this metadata-only action; a workspace-wide gate adds no necessary protection yet. The findings below identify prerequisites for the eventual retention/deletion boundary.

Reviewed commit: `0b31c06`. This is an independent static architecture review, using the semi-formal architecture-review and ms-rust guidance. No builds, tests, payload scans, downloads or destructive actions were performed. Recommendations below are design requirements, not qualified implementation claims.

## Immediate implementation decision

Add one durable abandonment record tied to the dataset, operation and original request identity, with a retained reason. Check it under the existing decision lock at admission/resume, new reservations, completion acknowledgments and selection. Reconcile identical abandonment retries. Refuse abandonment after accepted selection, so a concurrent selection and abandonment cannot both succeed. All result-producing paths use the existing metadata-location compare-and-swap; a stale delayed commit must not retry against a new view without checking abandonment.

This ends the operation's right to advance, not the independent recovery promise of its completed phases. Other operation IDs may reuse those material-key completions. Already-started writes may finish; their reservations remain protected. No quiescence, reclaimed-byte or physical-retirement claim follows.

The implementation audit must include direct `check_preparation` callers in backfill: prepared/validation commits, file acknowledgments, group reservations, readback and sibling composition (`src/engine/backfill/preparation.rs:142`, `:173`, `:202`, `:236`; `src/engine/backfill.rs:417`, `:499`). Checking only `Engine::check` misses those paths. Also cover initial admission, success-after-error reconciliation and shared selection (`src/engine.rs:561`, `:711`; `src/engine/backfill.rs:281`; `src/engine/history.rs:146`). Decide whether late failure diagnostics remain appendable; they do not authorize completion.

Keep admitted source boundaries monotonic. Abandonment alone does not clear the existing schema/backfill pending-source fences (`src/engine/schema.rs:82`, `src/engine/backfill.rs:569`). Document that a new selected source operation must reconcile such a gap; do not decrement the boundary or silently loosen cutover semantics.

## Findings first

### RA1 — BLOCKER: directory reservations are not a deletion inventory

`Protection` contains an owner, purpose and directory prefix, and reservations are created before output (`src/model.rs:432`, `src/engine.rs:1101`). Nothing retires them. `ObjectRef.owner` is a dataset identity, not an exclusive physical-file owner. Candidate tables deliberately inherit exact objects from earlier tables (`src/storage.rs:495`). A phase's files can therefore remain required by another phase, historical result or related dataset.

**RESHAPE:** retirement ends a named recovery promise; it does not authorize deleting its directory. Resolve candidates against roots across the entire owned catalog. Deduplicate by exact physical identity, refuse conflicting identities at one path, and show the roots blocking reclamation. Use typed record/reference traversal, not a JSON search for fields named `path`. Unknown root formats or incomplete catalog enumeration must refuse execution. Directory listings can enumerate an explicitly closed, owned attempt; age and an unreferenced filename cannot establish abandonment. This follows R6's exact-inventory rule (`docs/requirements/06-maintenance-retention.md:25`).

### RA2 — BLOCKER for release retirement: detached readers have no protection lifetime

Catalog-free readers take a release plus `Budget`, and return a lazy stream (`src/engine/read.rs:38`). CLI `Read` bypasses `Engine::open` (`src/main.rs:172`). Engine read methods also return streams without retaining the engine (`src/engine/read.rs:11`, `src/engine/backfill.rs:94`). HTTP retains the engine in its body, but that only retains the workspace host lock; it does not exclude maintenance running inside that host (`src/http.rs:250`). Thus the existing lock is not an active-reader inventory.

**RESHAPE:** keep every issued result/readback release and its required closure protected in this checkpoint. That preserves detached readers without adding catalog traffic to ordinary reads. If release retirement is included, it requires an explicit availability promise or a real reader guard covering stream lifetime and every authority owning its files. An in-memory engine counter cannot cover detached processes. Anonymous access cannot silently create leases (R6:35). Preserve the distinction between an immutable pin and a promise that its bytes remain available.

### RA3 — BLOCKER: shared phase identities and delayed work can resurrect retirement

Admission/build phase keys are material identities reused across operations (`src/engine.rs:1178`); scalar file completions use source/dependency keys (`src/engine/backfill.rs:646`). Reuse currently treats presence of those records as sufficient, while completion checks deliberately permit stale selection eligibility so finished work survives (`src/engine.rs:1050`, `src/engine.rs:1136`). Backfill has additional prepared/file/validation records outside `PhaseResult` (`src/engine/backfill/preparation.rs:37`). Retiring by `PhaseResult.operation`, or only adding `Protection.retired`, misses these consumers.

**RESHAPE:** bind retirement to exact record keys and observed revisions. Define which operation is abandoned and which shared phase promises remain. Every phase lookup, prepared-group lookup, completion acknowledgment and result selection must respect retirement/removal state. Preserve original provenance; a replacement computation is a new attempt. Capture active work before allowing maintenance, and ensure any uncancelable worker retains that protection until it actually exits. `Budget::blocking` already demonstrates worker-owned resource custody but currently carries only resource permits (`src/budget.rs:115`). A canceled caller is not proof that a writer stopped.

### RA4 — BLOCKER: removal needs a durable barrier against new references

Dataset locks cover individual decisions, not whole computations (`src/engine.rs:548`); related-input locks span sorted datasets only during qualification (`src/engine/inputs.rs:139`). They cannot make a global root scan followed by deletion safe. A new consumer could retain a candidate after preview, or an old worker could publish after deletion.

**RESHAPE:** for the qualified local host, use one shared/exclusive maintenance boundary: normal operations remain concurrent; explicit destructive execution drains relevant work, captures fresh catalog state, commits retirement plus exact removal intent, then performs effects. Acquire this boundary before dataset locks and retain custody through blocking/native work. New protections between preview and execution shrink its candidates. Persistent removal state prevents reuse after restart; reconcile pending intents before admitting conflicting work. The existing guarded lifecycle commit can store the intent and outcomes—no second SQL database, job ledger or consensus service is needed. Cross-host/custom-catalog deletion remains unqualified until its actual authority boundary is enforced.

### RA5 — CONCERN: existing graphs and native deletion helpers have different purposes

`release_objects` is shallow (`src/engine.rs:1588`); the verification graph intentionally traverses semantic inputs only (`src/engine/verification.rs:278`). Scalar binding proofs follow owning manifests (`src/engine/backfill/verification.rs:287`), schema checks open parent metadata (`src/engine/schema.rs:577`), and succession checks require the original result/link commit revisions (`src/engine/history.rs:276`). None is alone a complete retention traversal. `TablePin.objects` describes snapshot dependencies, not a restore point (`docs/STATUS.md:207`).

Pinned Iceberg `0.10.1` also has materially different cleanup APIs:

- `catalog/utils.rs:39`, `drop_table_data`, removes shared manifests, previous metadata and statistics even with `gc.enabled=false`; that property suppresses data-file deletion only.
- `iceberg-catalog-sql-0.10.1/src/catalog.rs:750`, `drop_table`, unregisters catalog bookkeeping without deleting files.
- `transaction/expire_snapshots.rs:34` documents metadata-only expiry. Explicit snapshot IDs still combine with default age-based expiry unless constrained (`:72`).

These source paths are under the installed Cargo registry; they were inspected locally. **KEEP** native automatic cleanup disabled (`src/storage.rs:67`). **RESHAPE** one purpose-aware retention traversal using existing model types and bounded metadata helpers. Preserve required history and schema metadata; do not recursively turn every provenance link into a permanent payload root. Keep optional change files retained until the explicit missing-history fallback exists—current update validation expects them (`src/engine/backfill/verification.rs:71`). Native expiry/drop is not a substitute for exact-object removal intent.

### RA6 — CONCERN: retirement must remain possible within the metadata budget

Lifecycle replay and warm commits charge cumulative appended history, including superseded rows (`src/catalog.rs:263`). Appending tombstones/outcomes cannot itself reduce that history. `Lifecycle::records` also materializes a parsed copy (`src/catalog.rs:130`). A full-catalog inventory that retains every dataset copy multiplies memory; a per-object outcome transaction multiplies small native files and replay cost.

**RESHAPE:** admit plan/candidate metadata before work, process dataset views incrementally, and bound candidate identities and blocker details. For a small retirement set, keep its candidate map and mark references while scanning retained roots, rather than build a permanent global index. Batch outcomes without losing each object's status. Preflight durable intent/reconciliation headroom before deletion; refuse if the existing append budget cannot support it. Lifecycle metadata compaction remains a separate required capability, not a claimed consequence of phase retirement.

## Artifact summary and lineage

The artifact is the next R6 implementation boundary, not an existing deletion implementation. Its user outcome is lower retained disk use without repeating valuable work or breaking a promised result. It is operational product functionality. Current status correctly lists deletion, shared-root retirement and restore as unfinished (`docs/STATUS.md:149`).

| Prior artifact | Relationship |
| --- | --- |
| `AGENTS.md`, Architecture and lifecycle decisions | Requires explicit retirement, one lifecycle authority and preserved evidence; no speculative infrastructure. |
| `docs/PLAN.md:96` | Governs the common protection inventory and durable removal barriers. |
| `docs/requirements/RESEARCH.md:26` and practitioner `FINDINGS.md`, Concrete incidents | Completed expensive work survives later failure; cleanup must not recreate the reported recovery failures. |
| R1, Shared phase completion and reuse | Phase completion is independent of parent-job success; R6 ends its recovery promise explicitly. |
| R3, Schema and identity design | Retained schema/default/supplied-state meaning survives cleanup. |
| R4:25–27 and R5, Shared promotion and recovery | Optional changes differ from required accepted succession and consumed inputs. |
| R6:21–37; R7 Q3/Q6/Q7 | Declare retention, deletion, reader, restore and scaling obligations. |

| Seam | Owner and dependency | Current readiness |
| --- | --- | --- |
| Result/phase retention → objects | Engine lifecycle, native pins, exact `ObjectRef`s | Data exists; retirement and complete root resolution missing. |
| Derived output → source/evidence/schema history | R3/R4/R5 typed records | Several focused traversals exist; unify reference enumeration without executing validation. |
| Consumer → availability | Read services, HTTP, detached CLI | Immutable interpretation exists; release retirement/reader promise unresolved. |
| Removal intent → filesystem effects | Local storage and guarded lifecycle | New bounded adapter required; native bulk deletion is unsuitable. |
| Restore → destructive authority | R6 storage/provider boundary | Unqualified; ordinary pins must not be advertised as restore points. |

## Invariants and commitments

| Invariant | Status and failure if ignored |
| --- | --- |
| Parent failure does not abandon completed phases (R6:27) | **RELIED UPON.** Otherwise cleanup converts a resumable failure into repeated expensive work. |
| Every promised reference protects the exact object (R6:25) | **NEW ENFORCEMENT REQUIRED.** Owner-based deletion breaks shared files and cross-dataset evidence. |
| Preview is an upper bound; new protections win (R6:29) | **NEW ENFORCEMENT REQUIRED.** Stale plans must not grow or bypass fresh roots. |
| Intent and unavailability precede effects (R6:31) | **NEW ENFORCEMENT REQUIRED.** Crash/retry otherwise resurrects unavailable outputs. |
| Reads resolve once and stream (`AGENTS.md`) | **PRESERVE.** Root analysis belongs in explicit maintenance, not every read. |
| Succession, schema meaning and evidence outlive optional accelerators (R3/R4/R5) | **RELIED UPON.** A table can remain readable while later refresh or diagnosis becomes invalid. |
| Advertised restore points protect their objects (R6:33) | **UNQUALIFIED.** Do not imply old catalog images can restore deleted bytes. |

## Smallest useful shape and performance

For eventual physical retirement, expose shared engine operations to preview retirement, execute that exact plan, and inspect/resume its result. The bounded plan identifies dataset/phase keys and revisions, object identities, reasons, blockers, unique logical bytes and inventory boundaries. A fresh execution may remove fewer objects, never more. Keep the final removed/absent/failed outcomes durable and distinguish them from estimated reclaimable space.

Enumerate existing lifecycle tables using maintained `Catalog::list_namespaces(None)` and `load_table`, without `Lifecycle::open` creating state. Current namespaces are flat (`src/catalog.rs:74`); table identities need not be reversed into dataset names. Read one retained `SnapshotRows` view at a time and record table identity/revision. The maintained list API returns a `Vec`, so an application limit does not prove an upstream allocation ceiling. Unknown lifecycle formats or enumeration failures block deletion.

Retain all historical and readback releases, generation seals, required succession/schema metadata, active operations and nonretired completions. A completion retirement may reclaim nothing because another root still needs every byte; report that plainly. Explicitly closed attempts may contain disposable intermediate files absent from completed pins. Inventory only that known owned scope after writers stop, identify exact candidates, and use the same intent mechanism. Never delete original external input paths or infer an orphan from a warehouse listing.

The initial cost should be proportional to inspected lifecycle records and object references, plus metadata for identified attempts—not retained rows or payload bytes. Reuse stored digests and sizes; no sampling, reconstruction or repeated payload hashing is needed merely to prove reachability. Check filesystem ownership/identity at the effect boundary under the qualified local immutable-path model; refuse changed or out-of-scope targets. Batch deletion/outcomes within declared limits. Do not add a reference-count database, persistent graph cache, scheduler or per-row records without measurement.

Measure metadata requests/bytes, inventory time, unique reclaimable bytes, actual logical/allocated bytes removed, temporary maintenance space and retained bytes afterward. If protected releases dominate disk, phase retirement cannot solve that cost; the required next decision is an explicit history/availability policy. This review establishes no speed or reclamation amount.

## Counterfactuals and qualification requirements

Deleting whole owner directories is simpler but breaks shared references. Keeping cleanup disabled preserves safety but cannot address accumulated abandoned work. A global reference-count subsystem adds transactional edges and is not justified by current evidence. The existing lifecycle and exact pins partially subsume the needed machinery; the missing pieces are promise retirement, complete root accounting and guarded effects.

The design is invalidated by any retained consumer losing bytes, any retry binding a removing object, or maintenance requiring full payload scans merely to discover liveness. Six-month failure signals are nominal retirement with no explainable reclamation, growing per-object lifecycle traffic, or readers slowed by mandatory retention scans.

Before enabling deletion, qualify:

- One physical object shared by historical/current results, two phases and a dependent dataset; retire each promise separately and reclaim only after the final applicable one ends.
- A stale preview followed by a new seal, derived result, completion or input capture; execution shrinks/refuses and preserves the new root.
- A paused writer, canceled blocking worker and delayed completion; no write or acknowledgment can escape the maintenance boundary.
- Lost responses/crashes before intent, after intent, after some deletions and before outcome commit; restart exposes pending work and reconciles exact identities.
- Detached CLI and engine/HTTP streams held across retirement; all retained releases remain readable, including unread later files and supplied/default schema state.
- Foreign paths, symlinks, conflicting same-path identities, unknown records, partial enumeration and metadata exhaustion; no deletion follows an incomplete proof.
- Fixed candidate size while dataset count, file count, phase history and evidence depth grow (R7 Q7); include failed attempts and maintenance's own metadata/storage cost.

## Verdict

**APPROVE** logical operation abandonment with the immediate invariants above, subject to implementation review and race tests. **RECONSIDER** enabling physical cleanup using current protections. **KEEP** the one-catalog, immutable-file architecture and disabled native automatic cleanup.

Intent and current status match; the abandonment decision gives users an explicit way to stop a retained operation from advancing without discarding reusable work. The broader retention value is supported by retained-disk and recovery needs, but this checkpoint saves no disk. Existing guarantees are honored while cleanup is disabled, but are not yet enforced against deletion. The shape adds necessary lifecycle state and reuses existing infrastructure; it does not establish complete R6, release expiry, compaction, restore, remote fencing or distributed-reader support. Confidence is high in the identified seams and medium in implementation cost until the relevant race cases execute.

## Addendum: schema cutover after abandoned source admissions

**APPROVE separating source coverage from the captured admission fence for a later implementation. REVERSE the earlier suggestion to retain an abandonment-gap certificate in every schema result.** That certificate would preserve an implementation-specific counter rule rather than a normative requirement. The current logical-abandonment checkpoint can retain its existing behavior; this addendum changes the recommended subsequent design, not its implementation verdict.

R3 requires exact pinned inputs, qualified schema interpretation, complete declared coverage and atomic checks of source heads, expected target and active definition (`docs/requirements/03-schema-backfill.md:31`, `:39–45`). R5 distinguishes those strict cutover checks from routine captured-input refresh (`docs/requirements/05-dependent-refresh.md:29–35`). Neither requires schema-only work to admit new source observations or advance a counter by one. The current increment in `prepare_and_select` and adjacent-boundary check in `schema::verify_transition` are implementation choices (`src/engine.rs`, operation creation; `src/engine/schema.rs:531`). Backfill already separates its selected source boundary from `Capture.admission_fence`; accepted succession already permits equal boundaries and orders selections by retained predecessor/commit links (`src/engine/backfill.rs`, `Capture` and `promotion`; `src/engine/history.rs:65–75`).

The smallest sound change is:

1. Source-producing Build/Update/Refresh admissions continue advancing the monotonic authority counter. Schema composition does not. A schema result, like a backfill result, preserves its exact parent's selected source boundary.
2. A new schema/backfill operation captures the current admission counter separately from that source boundary. If a gap exists, inspect one exact retained lifecycle view under the existing dataset lock. Require every intervening source admission to be explicitly abandoned with matching operation/request identity. Missing, duplicate, malformed or unresolved admissions refuse; finding only some abandoned rows does not prove the interval closed. Composition operations are not source admissions.
3. Persist that fence in the existing operation/capture record. At selection, require the exact captured counter, source pin, target and definition conditions still to hold. Any later source admission makes the old capture stale, even if that new admission is subsequently abandoned. A new request may capture the newly resolved state; an existing worker cannot adopt it silently.
4. Independent schema interpretation checks equal source boundaries, exact parent/inputs/membership and the qualified native schema transformation. It does not replay producer scheduling history. Abandonment records remain producer lifecycle evidence; they are not payload dependencies needed to read unchanged source observations. Preserve versioned interpretation for any already-issued schema format that used the old counter meaning.

This retains the real race protections: an active admission in the gap blocks preparation; an admission after capture blocks selection; a competing schema selection changes the exact base/definition; compatible sibling backfills retain their existing merge rule; abandoned workers cannot acknowledge or select. No unresolved input is silently treated as applied. The captured fence is an admission-order guard, not evidence that abandoned bytes entered the dataset.

| Alternative | Decision and cost |
| --- | --- |
| Force a new source result solely to close an abandoned gap | Reject: adds a result and possible processing without new source facts. |
| Preserve adjacent counter arithmetic with portable gap certificates | Reject: duplicates lifecycle history in manifests and increases retention/verification obligations. |
| Separate source boundary and admission fence; inspect existing records when needed | Prefer: reuses backfill's distinction, keeps payload reads unchanged and adds no index. |
| Add a resolved-boundary index or pending-operation subsystem | Defer unless the bounded metadata scan is measured to dominate. |

The gap check costs O(lifecycle records inspected), with storage proportional to the checked interval if sorting is necessary. Use one retained `SnapshotRows` view, bounded selective decoding and cooperative yields; do not clone the entire JSON record map or scan payloads. Repeated new cutover requests can repeat this metadata check; that is a fresh-authority check, not justification for a cache by itself.

Qualification must cover a valid abandoned gap, one live/missing admission within it, a later admission before selection, later abandonment without reviving the old capture, competing schema/backfill selection, restart with the original fence, and catalog-free reads/verification preserving exact source coverage and schema meaning. These tests replace the old adjacent-counter assertion with the actual equality-and-fencing invariants; they must not merely weaken it to an arbitrary increasing boundary.

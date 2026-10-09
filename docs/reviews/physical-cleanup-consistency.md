# Physical cleanup: consistency and reader-lifetime review

## Decision and findings

**APPROVE the proposed local design boundary, subject to the implementation and qualification below. This is not approval of a deletion implementation.** Add one bounded, owner-scoped removal service over explicitly selected exact objects, using the existing inventory, owned maintenance exclusion, guarded lifecycle commit and owner unavailability checks. Pair it with lifetime protection for Engine readers. Preserve independent catalog-free readers, with an explicit availability boundary rather than an invented permanent lease.

This static architecture review is pinned to `77eb5611b71de7f6dc3da62b0ed5c8512a23572f` on 2026-10-09. It applies the semi-formal architecture-review and Rust skills. The reviewer traced the plan, R1/R2/R6/R7 requirements, status, existing retirement reviews and the relevant production code. No Rust edits, Cargo commands, tests, cleanup, commits or provider operations were performed. References below identify this baseline; a later implementation needs a separate review.

### F1 — BLOCKER for deletion: current readers do not participate in maintenance exclusion

Engine group/evidence reads call catalog-free functions directly (`src/engine/read.rs:11`, `:23`). Combined reads and comparison likewise open immutable files without owned admission (`src/engine/backfill.rs:133`; `src/engine.rs:513`). The native reader retains a memory reservation, not a retention guard (`src/storage.rs:82`). HTTP retains an Engine in its outgoing stream, keeping workspace ownership alive, but that does not acquire the in-process maintenance lock (`src/http.rs:412`). An exclusive `maintain_work` therefore cannot yet prove the absence of readers.

**RESHAPE:** acquire shared maintenance ownership before resolving an Engine read, check the requested exact available result, then carry ownership through opening and the returned stream. Do not release it merely because the opening future returned. Include group, evidence, combined and comparison paths. Keep cancellation of opening inside an owned task; comparison can use the existing owned worker service. Return a structured availability refusal if retirement wins before reader acquisition. If reader acquisition wins, retirement/removal waits until that reader finishes or relinquishes its read.

Do not nest an admitted public lookup inside another maintenance read guard: a queued writer can make the second acquisition wait while the first prevents that writer from proceeding. Use already-admitted internal helpers, as worker code already does (`src/engine/worker.rs:127`, `:152`; `docs/STATUS.md:409`).

### F2 — BLOCKER for deletion: unavailability is only a prepared seam, not a durable removal decision

`UnavailableObject` already stores an exact object, intent ID and revision; acquisition checks it before returning even an existing owner grant (`src/engine/retention/owners.rs:42`, `:401`, `:661`). Managed input admission also checks it after matching retained creation evidence (`:614`). No removal intent or physical-outcome service exists at the reviewed commit. Inventory accepts a marker after checking its key, owner and revision, without checking the named intent (`src/engine/retention/inventory.rs:200`).

**KEEP and complete this seam:** append the removal intent and every reserved-object marker in the same owner lifecycle commit. Bind owner/table identity, stable intent ID, exact ordered candidate identities, reason, captured plan boundary, fresh decision boundary and the selected subset. Validate reciprocal membership between intent and markers, including exact object identity and native revisions. Reject orphan, mismatched or unknown records. Reuse `Lifecycle::commit` and exact-response reconciliation, rather than creating a second journal (`src/catalog.rs:253`; `src/engine/retention/retirement.rs:394`).

### F3 — BLOCKER for deletion: filesystem removal needs its own exact-effect and recovery rules

`ObjectRef` supplies owner, path, digest and bytes, not a storage version token (`src/model.rs:62`). Existing ownership checks establish lexical managed paths and original production reservations (`src/engine/retention/owners.rs:715`); exact-byte checking hashes a path (`src/storage.rs:288`). Neither operation alone establishes safe unlink behavior across path aliases, replacement, partial failure or a crash after unlink.

**RESHAPE:** restrict the first adapter to regular files in the owned `objects/<owner>/<attempt>/` namespace with retained typed creation evidence. Reject symlink components, directories, path traversal, conflicting aliases and foreign/catalog paths. Establish the exact local file identity at the destructive boundary; size alone is insufficient. Keep the qualified local namespace exclusively controlled through checking and unlinking, and refuse a changed path/file identity. If arbitrary concurrent filesystem replacement is part of the supported threat model, use a maintained directory-relative/no-follow primitive; a check followed by a separate path unlink is not an atomic compare-and-delete against an uncooperative process.

Persist a per-object `deleted`, `absent` or `failed` result only after the actual effect and required directory synchronization. Absence following a pending intent is successful reconciliation of absence, not proof that this invocation freed the original byte count. A failed or uncertain object stays unavailable. Do not roll its marker back. Retry only unresolved effects for that exact intent; an identical saved terminal outcome returns without redoing deletion. Keep logical deleted bytes, already-absent bytes and physically measured reclaimed storage distinct.

### F4 — CONCERN: saved identity, active reader lifetime and release availability must be explicit

No accepted requirement says that possessing a serialized `ReleasePin` creates an indefinite lease. R2-T05 promises an old pin through the release's availability window; R6 explicitly separates anonymous availability from authenticated leases with a concrete service and expiry policy (`docs/requirements/02-incremental-publication.md:27`, `:39`; `docs/requirements/06-maintenance-retention.md:35`, `:57`).

The existing statement that availability retirement “does not revoke already held immutable pins” describes logical retirement leaving physical files unchanged (`docs/STATUS.md:123`). The prior implementation review explicitly characterizes this as lack of access-control revocation and excludes physical removal and public/reader windows (`docs/reviews/availability-retirement-review.md:100`, `:206`). It does not establish permanent availability.

**KEEP the distinction and clarify it before enabling cleanup:** a pin continues to identify the same version after retirement; explicit availability retirement ends that standalone availability promise; an already admitted Engine reader retains its lifetime protection; independently retained results, phases, owners and other obligations can continue to protect the same bytes. Physical removal is a later, separately explicit operation after every applicable protection ends. A saved pin does not silently re-create protection when used after removal.

Catalog-free `read_group`, `read_evidence`, `verify_table` and `verify_release_at` require caller-retained bytes or an applicable published availability promise (`src/engine/read.rs:32`; `src/engine.rs:2113`, `:2153`). CLI `Read`, `ReadEvidence` and `Verify` intentionally bypass Engine opening (`src/main.rs:202`, `:218`). State this boundary directly in their API and CLI documentation. A live independent reader cannot silently extend an ended anonymous availability promise. Conversely, a declared public window or explicit external retention obligation must remain an inventory blocker; local result retirement cannot end such a distinct promise by inference. No public window, expiry or lease duration should be invented in this increment.

## Artifact and lineage

The beneficiary is the operator reclaiming disk from explicitly retired local work without losing selected results, qualified recovery or active managed reads. This is operational infrastructure supporting the product's correction and recovery behavior, not an automatic garbage collector.

| Prior artifact | Relationship | Evidence |
| --- | --- | --- |
| Accepted plan | Requires one logical protection inventory, durable reservation, new-reference exclusion and exact outcomes; ordinary reads stream without dependency validation | `docs/PLAN.md:41`, `:96` |
| R1 durable storage | Defines pre-effect intent and post-effect reconciliation; older supported restore points protect objects | `docs/requirements/01-durable-storage.md:25` |
| R2 consumer publication | Independent immutable readers rely on declared availability; explicit leases are separate | `docs/requirements/02-incremental-publication.md:17`, `:27`, `:39` |
| R6 retention | Separates availability, recovery, content and deletion; requires fresh plan intersection and exact-object reservation | `docs/requirements/06-maintenance-retention.md:9`, `:23`, `:29`, `:31` |
| R7 Q3/Q6 | Requires reader/protection races and recovery fencing at supported storage boundaries | `docs/requirements/07-operations-qualification.md:101`, `:125` |
| Prior next-step review and current status | Terminal-operation/attempt and owner-grant retirement remove specific root contributions; physical cleanup remained open | `docs/reviews/retention-next-step-review.md:50`, `:57`, `:76`; `docs/STATUS.md:17`, `:71` |

| Component or seam | Owner and current support | Remaining work |
| --- | --- | --- |
| Exact root inventory | Retention walker visits typed active roots and validated retired generations; conflicts and unknown formats refuse | Run fresh under exclusive maintenance, reuse admitted internal traversal, recognize and validate removal records (`inventory.rs:94`, `:147`, `:200`) |
| Local worker exclusion | Engine owns `host.lock`; shared work and exclusive maintenance survive caller cancellation | Extend to returned readers and comparison; preserve host quarantine on uncertain work (`worker.rs:55`, `:110`, `:135`) |
| Owner acquisition | Owner commits precede foreign consumer acknowledgment; creation evidence and unavailability are checked | Complete durable intent membership; qualify same-owner paths as well as foreign grant acquisition (`owners.rs:543`, `:614`, `:643`) |
| Local object effect | Immutable writes use create-new and synchronization; exact hashes exist | Add bounded exact-file deletion/reconciliation with directory durability (`storage.rs:288`, `:303`) |
| Independent readers | Resolve immutable manifests and requested data without catalog/dependency scans | State caller/public availability responsibility; do not silently add a producer callback (`read.rs:32`; `engine.rs:2214`) |
| Catalog recovery | Owned local catalog and process lock only | Keep catalog-file cleanup, restore execution, remote storage and stale remote deleter qualification outside this supported adapter; accepted R6/Q6 work stays open |

Abbreviated code paths in this table are under `src/engine/retention/` for `inventory.rs` and `owners.rs`, under `src/engine/` for `worker.rs` and `read.rs`, and under `src/` for other files.

## Smallest coherent implementation

1. Accept a bounded, sorted, deduplicated exact candidate list for one owner, a stable removal ID and an explicit reason. Preview records the observed inventory boundaries and protecting records, and estimates logical reclaimable bytes. Retain the preview's candidate scope; a later execution cannot add objects. Do not infer candidates from age, a directory walk, expired metadata or an object listing.
2. Execute through owned exclusive maintenance, then the owner's dataset lock. Restrict execution to the qualified owned embedded catalog, as logical retirement already does (`src/engine/retention/retirement.rs:514`). Recheck ownership/creation evidence and all current inventory roots. Execute the intersection of the original eligible plan and current eligibility. New protections can only remove candidates from the effect set; report the blockers.
3. Commit one exact intent and same-commit unavailable markers for the chosen subset before any file effect. Do not reinterpret logical retirement as that intent. Reconcile a lost commit response by rereading the exact committed intent; if its presence remains unknown, perform no file effects.
4. Run bounded file effects while the owned worker retains exclusive maintenance and host custody. Record each object outcome durably. Outcomes cannot make the object available again. Exact-path tombstones also prevent a later managed-input acquisition from treating retained creation evidence as authorization to reuse a removed object.
5. Resume from the retained intent and outcomes. The intent's fixed identities govern reconciliation; a retry must not reconstruct a larger candidate set from today's inventory. An unfinished removal remains inspectable and unavailable across Engine reopen. Fresh roots conflicting with a pending intent indicate corruption or unsupported bypass, not permission to delete newly protected content.
6. Keep native automatic cleanup disabled (`src/storage.rs:67`). Do not remove lifecycle catalog history or metadata used to prove creation, retirement, ordering or pending effects. Unknown/unsupported roots refuse. Compaction, restore points, provider fencing, independent policy windows and Q7/Comments performance qualification remain accepted unfinished work.

This design reuses the one existing inventory and native lifecycle journal. It needs no scheduler, separate SQL database, global reachability index, permanent per-reader catalog record or normal-read validation scan. The tradeoff is that local exclusive cleanup waits for all shared managed readers/work; bounded batches and explicit invocation keep that cost visible. No throughput or memory improvement is claimed without measurement.

## Invariants and falsification cases

| Invariant | Status at baseline / required result | Failure case that must refuse or remain safe |
| --- | --- | --- |
| A current or independently retained root protects all exact dependencies | Relied upon: typed inventory and generation retirement already implement it (`inventory.rs:147`, `:221`) | Retire the original parent but leave one reused phase, descendant, grant or current selection; shared bytes survive |
| Fresh protection beats a stale plan | New destructive service must preserve R6:29 | Acquire a grant between preview and execute; candidate disappears from deletion scope without expanding the rest |
| Unavailability precedes every effect | Newly completed seam required by R6:31 | Kill before intent commit: no unlink; lose commit response: reconcile before touching files |
| No acquisition binds removing bytes | Relies on owner markers and exclusive admitted work (`owners.rs:401`, `:661`) | Pause after intent, try fresh foreign and same-owner managed-input acquisition after reopen; both refuse |
| Saved removal identity cannot change | Existing retirement provides the pattern (`retirement.rs:401`, `:496`) | Reuse removal ID with changed owner, candidate digest, path, reason or selected subset; refuse |
| Reader lifetime protects deferred file opens | Newly introduced for Engine streams | Hold a multi-file stream after one batch; retire/remove concurrently; later batches retain original values until read completion/drop |
| Ordinary reads remain metadata resolution plus requested streaming | Preserved requirement (`AGENTS.md:29`; `engine.rs:2214`) | Instrument routine reads: no retention namespace enumeration, ancestor traversal or whole-payload hash appears |
| Absence is not a fabricated deletion count | Newly introduced result accounting | Kill after unlink before outcome commit; retry records reconciled absence and does not claim a new byte effect |
| Unknown effects remain unavailable | Required by R1:25 and R6:31 | Directory sync or outcome commit fails; pending/failed state persists and exact retry is possible |
| Restored catalog cannot promise deleted bytes | Unqualified outside this local adapter (`PLAN.md:104`; Q6) | Do not advertise point-in-time/restore support from these local tests; keep the actual old-deleter/provider test required |

## Reader and cancellation qualification

Cover every supported managed read boundary, not just `read_group`:

- Group, evidence, combined and comparison reads retain shared ownership through their last required I/O. `compare` consumes full results at `src/engine.rs:599`; its canceled caller must not release ownership while its owned work continues.
- Pause before immutable manifest opening, after native file planning, between data files and while the HTTP response is backpressured. Delete/retire waits; the reader returns the original values. If cleanup obtains exclusive ownership first, a new managed read refuses before opening retired bytes.
- Cancel before shared admission, during asynchronous opening, after stream return and during active polling. Confirm no leaked capacity after normal termination and no early admission release while detached I/O still uses the files.
- Qualify actual maintained reader cancellation, including combined DataFusion plans. The baseline provider defers native stream opening until execution (`src/query.rs:107`). A wrapper around the outward stream is sufficient only if native work cannot outlive the retained guard. Do not equate dropping an outward stream or its Budget permit with proof that all internal tasks stopped; retain ownership inside any detached work or drain it before releasing ownership.
- A queued exclusive maintainer must not deadlock nested read/lookup calls. HTTP currently performs lookup before opening its stream (`src/http.rs:412`); the protected stream-opening helper must recheck exact availability under its own acquired guard, or combine lookup/open under one guard.
- There is no baseline Engine verification method. Free verification is catalog-independent (`src/engine.rs:2113`, `:2153`). If local managed verification is offered, provide a thin owned-admitted wrapper around the same verification implementation; do not make independent verification depend on the producing catalog. Low-level `storage::read_batches` and external consumers of its `Table` likewise require an enclosing caller lifetime promise.

For file effects, additionally qualify same-size wrong bytes, outside-owner paths, traversal, symlinked leaf/parent, directory candidates, missing files, conflicting aliases, changed file identity, injected I/O failure, lost native replies, duplicate/concurrent retries, caller cancellation, SIGKILL at pre/post-effect boundaries and cold reopen. The positive test must really remove one no-longer-protected regular file while an independently selected replacement and shared retained dependencies remain readable. Checking that metadata says “deleted” is insufficient.

## User value, counterfactuals and verdict

The concrete benefit is reclaimed local storage with a retained explanation of which exact bytes were removed, why no promise required them, and what remains unresolved. The six-month failure criterion is either an unreachable retired output that still cannot be removed through the supported service, or a promised reader/result becoming unreadable after a successful removal. A third failure is moving inventory/hash work onto every ordinary read to avoid designing lifetime ownership.

Keeping deletion disabled is safe but leaves the operator's storage problem unsolved. Treating exported pins as permanent leases would avoid one race while contradicting the explicit R6 distinction and making retirement ineffective. Treating all readers as unprotected would simplify the code while violating managed-reader safety. A new lease service is unnecessary for this bounded local service; external authenticated leases require their own concrete product need and policy. Automatic sweeping cannot substitute for the existing exact inventory and reservations.

**Verdict: APPROVE the proposed boundary; implementation approval remains pending.** Intent and user value match the accepted plan. The design pays down permanent-root/storage debt by reusing the existing inventory and guarded commit path. The material commitments are honored only with the reader, exact-file, durable-intent and recovery qualifications above. Static confidence is high for current code-path support and the absence of an indefinite-pin requirement; actual filesystem durability and native-task cancellation remain execution questions. The complete R1–R7 scope, including compaction, restore, remote publication/fencing and the full Comments acceptance target, is unchanged.

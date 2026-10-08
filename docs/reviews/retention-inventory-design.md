# R6 typed retention inventory design review

**Verdict: RECONSIDER the current protection records as a deletion inventory; KEEP the existing catalog, immutable pins, and explicit abandonment.** Complete phase retention with one typed inventory, generation-specific retirement, and exact removal intents. Preserve every issued historical and readback release until a release-availability policy exists. This permits useful reclamation of abandoned, unselected work without changing catalog-free readers.

Reviewed commit: `e742395adb25b419aaeaf7cabb9132fcfbaf1485`. This is an independent static architecture review using the semi-formal architecture-review and Rust skills. No Rust changes, builds, tests, payload scans, downloads, or deletion were performed. Recommendations are implementation requirements, not qualified behavior. The workspace filemap index has no `docspec-rs` slice; repository instructions and source supplied navigation instead.

## Findings first

### RI1 — BLOCKER: lifecycle roots require explicit typed dispatch

**Category:** intent-vs-shape. **Location:** `src/model.rs:415`; `src/engine/backfill.rs:24`; `src/engine/backfill/preparation.rs:39`.

`PhaseResult` is only one completion representation. In particular, `phase/backfill/...` contains `FileCompletion`, not `PhaseResult`. Prepared groups, validation acknowledgments, group assembly, backfill captures and readbacks use additional types. `Protection.paths` is a pre-write reservation of an attempt directory; it does not enumerate exact objects or establish exclusive ownership (`src/model.rs:443`; `src/engine.rs:1160`). A prefix-only phase sweep or deletion by `ObjectRef.owner` loses required references.

**RESHAPE:** use exhaustive dispatch over the concrete record families listed below. Decode `phase/backfill/` before the other `phase/` families. Refuse unknown record families, malformed typed records and conflicting object identities before effects. Keep original lifecycle records; retirement changes a recovery promise, not historical facts. The inventory is an engine function over existing types, not another persistent graph or SQL database.

### RI2 — BLOCKER: retained phase inputs must survive another phase's retirement

**Category:** unstated-assumption. **Location:** `src/engine.rs:649`, `:692`, `:873`, `:1238`.

Admission records contain the copied input objects. Processing records contain output objects and optional coverage/change data; validation clones processing. Neither processing nor validation directly binds the retained admission record. The producer's original `operation/<id>` and `request/<id>` currently allow the material admission key to be recovered. A retained processing or validation result still requires those exact selected inputs under R1/R6; deleting its retired admission copy would remove recovery evidence even if its result table remains readable (`docs/requirements/01-durable-storage.md`, Shared phase completion and reuse; R6:27).

Retirement also makes replacement generations possible. Reusing the same material key for a newly copied input must not silently reinterpret an older processing completion's original input. The same issue applies to prepared/validation records keyed by material hashes, and file-completion records that carry source/output objects but no parent operation (`src/engine/backfill.rs:34`, `:674`; `src/engine/backfill/preparation.rs:31`).

**RESHAPE:** bind retirement to the exact lifecycle table, record key, original revision and record identity. Add only the missing explicit dependency bindings needed to retain exact admission/preparation evidence. Resolve existing records using their retained operation/request and current immutable completion where unambiguous; refuse retirement that would make an unresolved legacy binding unsafe. Preserve an old completion generation when producing a new one. A tombstone must disable that generation's reuse without making its objects unreachable from another retained generation. Do not use `PhaseResult.operation` as the retirement identity, and do not overwrite provenance merely to make a material-key retry succeed.

### RI3 — BLOCKER: all catalog roots participate in shared-object decisions

**Category:** named-seam. **Location:** `src/engine/inputs.rs:17`, `:79`; `src/storage.rs:495`.

Source generation seals live in the family's lifecycle table, and exact related input references live in consuming datasets. Native candidate pins inherit prior objects with their original owner. Therefore, scanning only the retiring dataset or treating the owning dataset as the sole protector is insufficient. A second dataset can still need the same source and evidence bytes after the producer phase's recovery promise ends.

**RESHAPE:** enumerate the whole owned local catalog through maintained catalog APIs, load existing lifecycle tables without creating them, and record each exact table metadata location/revision. Include family-only tables. Use returned `TableIdent`s; do not reverse a hashed namespace into a dataset ID. Capture this catalog boundary under the destructive maintenance exclusion described in RI5. Incomplete enumeration or an unsupported catalog authority model refuses destructive execution.

### RI4 — BLOCKER for release expiry: detached historical readers have no guard

**Category:** commitment-violation. **Location:** `src/engine/read.rs:38`.

The public reader receives a `ReleasePin` and `Budget`, resolves the release and returns a lazy stream. It does not register with the producing catalog. Engine methods delegate to that reader (`src/engine/read.rs:11`), so retaining an `Engine` elsewhere does not describe every reader. Reader resource permits in `storage::read_batches` protect memory, not file availability (`src/storage.rs:78`).

**KEEP:** every `result/`, `readback/` and `backfill-readback/` release and its required closure as a root for phase cleanup. This includes prepared readbacks of operations later abandoned. Preserve immutable native schema/default metadata and later unread stream files. The concrete public benefit is safe abandoned-work cleanup with unchanged ordinary reads. Release retirement requires an explicit availability window or qualified reader service; it is not implied by phase retirement. This checkpoint must not claim complete R6 release expiry or restore support (`docs/STATUS.md:238`; R6:35).

### RI5 — BLOCKER: a fresh inventory needs worker custody and persistent unavailability

**Category:** ownership. **Location:** `src/engine.rs:149`, `:1160`; `src/budget.rs:114`.

Dataset decision locks do not cover complete computations. Abandonment refuses later completion/selection, but running native/blocking work may continue (`src/engine/retention.rs:98`). A caller being canceled or an operation being abandoned does not close its output directory. `Budget::blocking` correctly retains resource permits in the worker, but does not retain lifecycle custody.

**RESHAPE:** acquire one local shared maintenance guard for normal root-creating/effect-producing operations before dataset locks. Clone custody into work that can outlive its caller; keep the workspace ownership alive as well. Destructive execution takes the exclusive side, waits for workers, reads fresh roots, and commits retirement plus exact removal intent before any effect. Do not recursively reacquire a fair read lock from nested helpers while an outer read guard is held; pass custody through the existing call chain. Keep pending removal durable so a restarted engine cannot admit a new reference to an unavailable object. Reconcile pending effects before conflicting work.

### RI6 — CONCERN: append-only metadata can prevent completion of cleanup

**Category:** debt-accretion. **Location:** `src/catalog.rs:155`, `:277`, `:319`.

The current lifecycle reader replays cumulative appended rows. Commits count superseded history and clone the current value map. Retirement/outcome records grow that history; deleting payloads does not reduce it. `Lifecycle::records` also builds a parsed copy of every row (`src/catalog.rs:146`). A plan that fits may still leave no budget to acknowledge the effects it caused.

**RESHAPE:** preflight intent and bounded outcome/reconciliation headroom before deleting. Iterate borrowed `SnapshotRows` records one dataset at a time. Bound candidate identities, traversal state, blocker reporting and serialized plans together; do not retain every dataset's parsed map. Batch per-object outcomes. Preserve required original commit revisions when lifecycle compaction later removes obsolete history; `history::read_link` requires result/succession revision equality (`src/engine/history.rs:277`). Report metadata exhaustion as refusal, never as proof that remaining roots do not exist.

## Artifact summary and decision lineage

The artifact under review is the next local phase-retention implementation. The user outcome is an exact explanation of disposable work followed by recoverable reclamation that preserves useful completed work and promised readers. It is an operational product capability with necessary proof records. Logical abandonment already exists at this commit; physical retirement, removal and restore remain unimplemented (`docs/STATUS.md:3`, `:238`).

| Prior artifact | Relation | Evidence |
| --- | --- | --- |
| `AGENTS.md`, Architecture and lifecycle decisions | Governs Iceberg-only lifecycle, retained work and disabled cleanup | Repository instructions |
| `docs/PLAN.md:23`, `:96`, `:104` | One selected result, one protection inventory, guarded removal and fresh-catalog restore | Normative plan |
| R1, Shared phase completion and reuse | A phase's input/output/evidence promise is independent of its parent | `docs/requirements/01-durable-storage.md`, Shared phase completion and reuse |
| R6 retention rules | Exact roots, explicit promise retirement, fresh intersection, pre-effect intent | `docs/requirements/06-maintenance-retention.md:23` |
| R7 Q3/Q6/Q7 | Shared-root races, old deleters after restore, metadata scaling | `docs/requirements/07-operations-qualification.md:101`, `:125`, `:133` |
| Prior retention review | Approves only logical abandonment; identifies deletion prerequisites | `docs/reviews/retention-architecture-review.md:3`, `:19`, `:95` |
| `86fb677`, `e742395` | Implement abandonment and distinguish source coverage from admission fences | `git log`; `src/engine/retention.rs:104`; `src/engine.rs:64` |

| Component / seam | Owner | Existing dependency | Consumer / status |
| --- | --- | --- | --- |
| Lifecycle root → physical objects | Engine retention | `SnapshotRows`, model/backfill types, `ObjectRef` | Preview and removal; required by R6:25 |
| Release → historical meaning | Model/read/schema/backfill/history | Exact manifests, native schemas, succession records | Detached reads, verification, subsequent composition |
| Cross-dataset input → producer objects | Inputs/refresh | `VersionRef`, `InputCoverage`, `GenerationSeal` | Related computations; source family table is a separate root location |
| Native snapshot → file closure | Storage | `TablePin.objects`, maintained native metadata | Shared values, manifest lists, manifests and statistics |
| Closed attempt → disposable objects | Engine/storage | Durable `Protection` plus worker completion | Missing typed closure/removal state; prefix alone is insufficient |
| Removal intent → local unlink | Engine/storage | Guarded lifecycle commit plus local exclusion | New thin adapter, qualified only for managed local paths |
| Restore point → recoverable closure | R6 | Catalog state plus exact object inventory | Missing; no current `TablePin` is a restore point |

## Concrete root inventory

The table distinguishes an availability root from historical metadata. Retain the latter without automatically promising every payload once it has an independent retirement mechanism. For this increment, all issued releases remain roots and optional change files remain retained because current checks require them.

| Lifecycle key / value | Direct dependencies and inventory rule | Source |
| --- | --- | --- |
| `authority` → `Authority` | Selected `DatasetPin`, current `ReleasePin`, selection revision. Check selected/release consistency; deduplicate copies of existing result objects. | `src/model.rs:432`; `src/engine/history.rs:153` |
| `result/<version>`, `readback/<operation>`, `backfill-readback/<operation>/<digest>` → `ReleasePin` | Manifest plus the exact dataset closure below. Keep historical and unselected readbacks. | `src/engine/history.rs:205`; `src/engine.rs:986`; `src/engine/backfill.rs:477` |
| `phase/backfill/<eligibility>/<digest>` → `FileCompletion` | Exact source file, Avro descriptor and output files. Descriptor is necessary for native file reconstruction. Independent completion, even when the parent failed. | `src/engine/backfill.rs:34`, `:674`, `:682` |
| Other supported `phase/...` → `PhaseResult` | Outputs, optional table, changes and coverage. Also original request/input dependencies and required completion evidence; admission/processing/evidence/validation have different roles. Retirement is per exact record generation. | `src/model.rs:417`; `src/engine.rs:609`, `:692`, `:873`; `src/engine/refresh.rs:160` |
| `backfill-prepared/<eligibility>/<digest>` → `TablePin` | Prepared native group and all pinned objects. Preserve its source interpretation/material dependency evidence. | `src/engine/backfill/preparation.rs:31`, `:135` |
| `backfill-validation/<eligibility>/<digest>` → `ObjectRef` | Exact prepared metadata validated by that acknowledgment. Bind to the matching prepared generation and its required source, not an arbitrary current table at the same material key. | `src/engine/backfill/preparation.rs:157` |
| `groups/<operation>` → `BTreeMap<String,ObjectRef>` | Exact group metadata completing assembly. Resolve to the already recorded prepared pins; a metadata-only reference is insufficient proof that dependent files may be removed. | `src/engine.rs:942` |
| `operation/<id>` → `Operation`; `request/<id>` → `OperationRequest` | Active operation's base manifest/version, source request and captured refresh inputs. Request input paths are external input declarations, not deletion candidates. Retain operation/request facts after abandonment for phase provenance and cutover checks. | `src/engine.rs:59`, `:798`; `src/model.rs:605`, `:664` |
| `backfill/<operation>` → `Capture` | Base manifest, old target metadata, target computed binding and related-result coverage/evidence. Active pending capture protects its required objects. | `src/engine/backfill.rs:25`, `:386` |
| `protection/<id>` → `Protection` | Entire owned attempt prefix until explicitly closed with worker safety established. After closing, enumerate only this known scope and classify exact objects using the common inventory. | `src/engine.rs:1160`; `src/engine/backfill/preparation.rs:190` |
| `generation/<id>` → `GenerationSeal`; `family-members` → set of IDs | Exact member version references across producer datasets; keep the corresponding release promises. Family membership itself has no file dependency. | `src/engine/inputs.rs:29`, `:62` |
| `succession/<id>` → `Succession`; `input-progress/<id>` → map of `SuccessionProof` | Preserve exact accepted ordering identities and original revision evidence. These records are not an excuse to scan every historical payload; current all-release retention independently protects those payloads. | `src/engine/history.rs:182`, `:210`, `:277` |
| `abandoned/<id>` → `Abandonment` | Preserve original request digest/admission revision and reason; ends operation advancement only. No implicit retirement of other records. | `src/model.rs:53`; `src/engine/retention.rs:124` |
| `runtime/<id>`, `failure/<operation>/<key>`, `invalidation/<eligibility>` | Retained provenance/diagnostics, with explicit format validation. Do not treat strings that happen to resemble paths as file roots. | `src/engine.rs:520`, `:801`; `src/engine/backfill/preparation.rs:90` |
| New retirement, removal and restore-point records | Explicitly typed records, dispatched by the same inventory. Pending removals are unavailable-for-reuse identities, not reusable roots; supported restore points protect their whole saved closure. | Required by R6:29–33; absent at reviewed HEAD |

### Dataset closure and physical identities

For a `ReleasePin`, start with `manifest`, then enumerate the typed `DatasetPin`:

- `groups`: each `TablePin.metadata` and every `TablePin.objects` entry, validating that metadata is present in the list.
- `inputs`: retained copied source objects. Never derive deletion authority from `OperationRequest`'s original external path.
- `computed`: `source_metadata`, `base_manifest`, native field mappings and original request. Preserve owning parent manifests/native source metadata needed by the existing binding-history checks.
- `related`: evidence `TablePin`, coverage input `VersionRef`s, their source release closures and required generation seals. Deduplicate the same consumed version wherever encountered.
- `parent_manifest`: required historical interpretation. Traverse the required schema/binding relationship using typed manifests, rather than assuming that a shallow reference alone preserves its dependent metadata.
- `changes`: base manifest, before/after metadata and added/removed `TablePin`s. Keep optional change payloads until checks have an explicit supported expired-history behavior.

These edges are explicit in `src/model.rs:84`, `:375`, `:594`, `:699`. Existing `release_objects` is shallow and omits traversal through owning bindings (`src/engine.rs:1650`); `verification::DependencyGraph` intentionally follows semantic inputs only (`src/engine/verification.rs:278`). Neither is the deletion proof by itself. Reuse their typed decoding and reference helpers without invoking payload validation or relational reconstruction. `backfill::verify_binding_history` demonstrates why owning parent manifests and source schemas matter (`src/engine/backfill/verification.rs:287`); schema availability opens the parent manifest too (`src/engine/schema.rs:580`).

Use one physical-path map within the qualified local namespace. Equal paths must carry equal owner/digest/length identities; conflicting declarations refuse. Count an identical object once regardless of how many tables, phases or datasets reference it. Preserve the original `ObjectRef.owner`; it describes protection lineage, not exclusive possession (`src/storage.rs:495`). Bind catalog/workspace identity separately so an identically named path in another store is not treated as the same object.

`storage::pin` already obtains exact dependencies from maintained native metadata. `snapshot_files` includes the selected metadata, statistics/partition statistics, all referenced snapshots' manifest lists, manifests and their live files; it deduplicates shared manifests (`src/storage.rs:369`). Reuse the retained `TablePin.objects` for reachability. It deliberately does not make previous metadata-log files or SQL catalog bookkeeping into a complete restore point (`docs/STATUS.md:296`). Unknown native structures or incomplete historical pins must refuse cleanup rather than silently downgrade the supported closure.

### Minimal explicit phase dependency shape

For new processing completions, prefer an optional typed `PhaseDependencies` containing named admitted input `ObjectRef`s and exact consumed base/schema references. Validation inherits this metadata. Keep existing `coverage` and `changes` as their typed dependency records. Missing dependency metadata on an older completion means unknown for reclamation; it can remain reusable while destructive cleanup is disabled. Release assembly must use the processing completion's captured admitted inputs when processing is reused, rather than substituting another copy from a newer admission record (`src/engine.rs:881`). An immutable admission-descriptor file adds an object and read without a demonstrated benefit when these input references are already small.

`Operation.base_manifest` alone is not a complete source of this field: `captured_manifest` fills it only for Schema and incremental Refresh (`src/engine.rs:1228`). Build reads its parent source schema/default history when evolved (`src/engine/write.rs:101`), while Update consumes its base source and already records `ChangePin.base_manifest` (`src/engine/write.rs:181`, `:312`). Record the actual consumed schema/base identity at processing time. Preserve Build's existing cross-base material reuse rule: a dependency records original provenance, and need not make the entire base release a material invalidation key when the existing schema identity is sufficient (`src/engine.rs:1258`).

Evidence dependencies follow the real code branches:

- Full Join reads the exact processing source table (`src/engine/refresh.rs:131`). It needs an additional exact processing-table dependency because the evidence phase replaces `table`/`outputs` with its own result. A `TablePin` reuses existing closure support but duplicates file metadata. A compact completion reference is sufficient only when it binds the original key/revision/expected native metadata and that generation stays addressable after retirement or recomputation. A bare metadata path must not become an unexpanded leaf.
- Full Aggregate constructs contributors from exact coverage inputs; it does not read `processing.table` (`src/engine/refresh.rs:142`). Do not duplicate that table solely for uniformity.
- Incremental Join and Aggregate read prior base evidence plus `changes.removed` and `changes.added` (`src/engine/incremental.rs:394`). Incremental Aggregate additionally reads coverage inputs (`src/engine/aggregation.rs:307`). The evidence phase already inherits processing's `ChangePin` (`src/engine/refresh.rs:160`), so traversing that pin and the base/coverage closure supplies those edges.

This fills concrete missing dependencies without a general descriptor service or duplicated table inventories for every phase. Independent backfill completion/prepared/validation formats still need their own typed inventory; changing `PhaseResult` alone does not complete retention.

### Catalog registration and attempts

Output tables are registered under random `data_<uuid>` names in the dataset namespace (`src/engine/write.rs:66`). The namespace is not a reversible encoding of the dataset identity (`src/catalog.rs:89`). Enumerate existing tables, inspect their exact metadata locations and correlate locations with the known protected attempt; do not infer an exclusive owner from table names.

A native table head is not an independent selected DocSpec result (`docs/PLAN.md:30`). Keeping every abandoned table head as a perpetual logical root would prevent useful cleanup. For a closed disposable candidate, include metadata-only catalog unregistering in the exact durable removal intent, after capturing its necessary metadata. Reconcile an absent registration and refuse a changed/rebound registration. Unregistering cannot authorize deleting inherited/shared files: those still pass the global root test.

The installed maintained SQL catalog's `drop_table` removes registration only; `purge_table` then calls physical bulk cleanup (`iceberg-catalog-sql-0.10.1/src/catalog.rs:750`, `:778`, inspected in the local Cargo registry). Use the former only within the qualified local boundary. Keep native automatic deletion disabled (`src/storage.rs:67`); do not call `purge_table` or `drop_table_data` for exact shared-object cleanup.

An attempt can contain provisional metadata, unused manifests or unacknowledged outputs absent from every completed pin. Inventory only its durably reserved, explicitly closed directory after writers exit. Check regular-file/path ownership and capture each identity; do not sweep the warehouse or delete original input files. A missing digest for an unacknowledged file is an unknown identity, not permission to label it equal to a known object. If exact content identity is required for such a candidate, admit/hash that candidate once under the closed-attempt boundary or refuse it; never scan unrelated retained payloads to establish reachability.

## Smallest concrete implementation path

Use shared engine services for preview, execute, inspect and resume, with thin CLI/HTTP entry points. The implementation dependency order is:

1. Add exhaustive typed root/dependency enumeration beside the existing retention module. Add narrow access to private backfill records through their owning module. Introduce generation-bound retirement and the missing phase dependency fields; preserve old record evidence. Root accounting includes every existing completion family before effects are enabled.
2. Add local worker-owned maintenance custody and durable closed-attempt state. Scope destructive support to `Engine::open` with its owned local catalog, unless an injected catalog has independently qualified equivalent exclusion and exact enumeration. A local filesystem lock does not fence arbitrary remote catalog writers (`src/engine.rs:121`).
3. Build a bounded `RetentionPlan` identifying policy/reason, exact candidate logical records/revisions, candidate objects, blockers, catalog boundary and unique logical-byte estimates. Preview has no external effect. Explicit retirement is the default; do not invent an automatic expiration duration (R6:57).
4. Execute the intersection of the submitted plan and fresh eligibility. Under exclusive maintenance custody, retire eligible record generations and reserve exact removal atomically in the owning lifecycle. Keep all historical/readback release closures. Record exact targets, expected identities, catalog unregister effects and the applicable local authority before deleting. A stale plan cannot acquire new candidates.
5. Apply only those effects and persist bounded per-object `deleted`, `already_absent` or `failed` outcomes, plus unknown/unreconciled status when appropriate. Keep incomplete objects unavailable to new bindings. Resume by exact intent identity, not by rescanning directories to invent replacement candidates. Startup must find and reconcile pending intents before allowing conflicting work.

The state transitions are: promised recovery → explicitly retired recovery; protected object → removal reserved/unavailable; removal reserved → per-object terminal outcome. Retiring a promise may reclaim no objects. A surviving result or phase keeps its own promise and shared bytes. Failed external deletion does not restore the retired phase's reuse eligibility. A replacement computation uses a new immutable attempt and completion generation.

This is the full minimum for a useful local phase-retention service, not a substitute for the accepted overall plan. Compaction, separate public-release/change-history policies and advertised restore points remain named R6 work. When restore points are implemented, they must use this inventory and retain catalog state plus object closure; restore into a fresh catalog only after the old destructive authority is fenced at storage (`docs/PLAN.md:104`; R7 Q6). Keeping the current service free of advertised historical restore points avoids inventing an unsupported disaster-recovery promise; it does not complete restore.

## Proof required before each deletion

The removal service must establish all of the following:

1. **Exact scope:** the immutable object identity is in the submitted plan and the durable current removal intent. Parent directories and matching filenames are not substitutes.
2. **Closed production:** all writers that could still affect the attempt are stopped or drained; caller cancellation and operation abandonment alone are insufficient.
3. **Complete fresh roots:** every owned lifecycle namespace was visited at a recorded exact boundary; supported retained roots and dependencies are decoded; the candidate has no surviving protection. Any unknown or incomplete root blocks the effect.
4. **Reference exclusion:** removal reservation/unavailability is durable, new selections and reuse cannot bind that identity, and old workers cannot acknowledge it after the barrier.
5. **Owned effect boundary:** the local target is a regular managed file in the exact owned attempt, with no symlink/foreign-path escape or conflicting identity. The managed immutable-path guarantee and worker exclusion must hold through unlink. A digest stored in metadata is not an operating-system version token; length alone does not detect replacement with same-size bytes. Capture/check the supported filesystem identity and qualify the adapter's assumptions; refuse changed identities. Remote storage needs actual provider version/precondition support.
6. **Recovery capacity:** durable intent and per-object outcomes fit the supported metadata budget, and supported restore points do not promise those objects. No old catalog image may reactivate this deletion or falsely promise its already-deleted objects.

Fsync the necessary directory changes under the supported local durability model and reconcile a crash/lost response as an exact absent/deleted/failed outcome. This is a required implementation/test behavior, not something established by the current `write_new` creation durability (`src/storage.rs:303`).

## Bounded cost and resource analysis

Let H be lifecycle history read, R be current typed references visited, C be planned candidate identities, D be catalog tables, and M be uniquely decoded referenced metadata. The initial explicit preview/execution can cost O(H + R + M), plus normal map lookup costs, while holding O(C) candidate state and a bounded metadata work queue/visited set. This is an algorithmic expectation, not a measured result. Candidate deduplication and blockers must be charged; exceeding the aggregate limit refuses safely. Deep dependency chains can exceed that limit even with few candidates.

Process existing dataset views sequentially. Keep one borrowed `SnapshotRows` at a time, record the compact table boundary, and release decoded records after marking candidates. `SnapshotRows::prefix/get_record` already permit selective access (`src/catalog.rs:39`). If all retained dataset maps are kept alive, each holds a metadata reservation and memory grows with the catalog. Do not call `Lifecycle::records` for the inventory.

For small retirements, keep only the bounded candidate map and mark which surviving roots protect those candidates. Deduplicate metadata traversal and store compact identity/blocker summaries, rather than constructing a permanent global reverse index. Avoid repeated full parses of the same release embedded in authority/result/readback; memoize only within the admitted maintenance request. Do not load every payload or recompute source/evidence relationships.

The maintained SQL catalog returns namespace/table lists as `Vec`s and first materializes query results (`iceberg-catalog-sql-0.10.1/src/catalog.rs:392`, `:684`). Post-return application checks bound admitted work, not the library's peak allocation. Record this limitation; qualify actual catalog sizes before claiming a strict whole-process ceiling. No new index is justified until a measured complete-path bottleneck requires it.

Measure metadata calls/bytes, roots visited, unique objects considered, inventory duration, preview serialization, retirement/outcome commits, peak maintenance memory/scratch, logical/allocated bytes actually reclaimed and retained bytes afterward. Hold C fixed while D/history/file count/chain depth grow. Report protected bytes separately from nominally expired bytes; phase retirement may save little when historical releases dominate storage. Lifecycle compaction must preserve required revision evidence and be measured separately.

## Invariants and commitments

| Invariant | Status in recommended design | Evidence / failure if violated |
| --- | --- | --- |
| Parent failure does not abandon completed phases | PRESERVED | R6:27; `Engine::abandon` currently retains them. Coupling the lifetimes forces repeat work or loses evidence. |
| One root inventory determines shared-object liveness | NEW ENFORCEMENT REQUIRED | R6:25; otherwise cross-dataset or backfill ownership is missed. |
| A preview is an upper bound; new protections win | NEW ENFORCEMENT REQUIRED | R6:29; otherwise stale execution can remove newly promised content. |
| Intent/unavailability precede deletion | NEW ENFORCEMENT REQUIRED | R6:31; otherwise crash/restart can reuse missing files. |
| Reads resolve immutable selection and stream requested data | PRESERVED | Plan:44–48; retaining all issued release closures avoids adding maintenance scans to reads. |
| Source/schema/required ordering evidence retains its meaning | RELIED UPON | `src/engine/backfill/verification.rs:287`; `src/engine/history.rs:277`; compaction or retirement must not rewrite original identity/revisions. |
| Restore promise and object retention share one support boundary | NOT YET IMPLEMENTED | Plan:104–106; a saved database image alone cannot promise deleted bytes. |
| No second mutable latest/SQL lifecycle authority | PRESERVED | Plan:11–17, 89–94; use ordinary guarded lifecycle rows. |

## Qualification, user value and counterfactuals

Run focused tests that independently establish:

- One exact file protected by a historical result, readback, phase, backfill file completion and related dataset. Retire each recovery promise and verify remaining readers/reuse; deletion occurs only after the final applicable protection ends.
- Retired admission still required by live processing/validation; replacement admission under the same material identity cannot alter old input provenance. Include legacy ambiguous metadata refusing safely.
- Every typed record family participates, especially `phase/backfill/`, prepared/validation/group records and family-only generation tables. Unknown families and malformed/conflicting references refuse.
- Preview then a new seal/input capture/phase/release; execution shrinks or refuses. Selected and readback releases, schema additions/defaults/supplied-state and unread later stream files remain readable without the catalog.
- Canceled blocking/native worker, abandonment and maintenance request racing. Worker custody lasts through the final possible I/O/acknowledgment; nested helpers do not deadlock behind a waiting exclusive guard.
- Crashes/lost responses before intent, after intent, after partial deletes, after unregister and before outcome commit. Restart reports exact pending outcomes and does not resurrect phase reuse or repeat confirmed effects.
- Foreign paths, symlink components, same-path conflicting content declarations, changed local identities, missing objects, incomplete catalog enumeration and metadata exhaustion. No unproven deletion occurs.
- Fixed candidate scope while history, catalog size and schema/evidence depth grow; record both retained and temporary maintenance cost. This addresses R7 Q7 without claiming payload throughput improvements.

The beneficiary is the operator who needs to reclaim abandoned work without invalidating a result or paying to recompute useful phases. The proposed shape adds only explicit promise/effect state and missing dependency edges; it pays down the current duplication among shallow object enumeration, phase-specific reuse formats and directory protection. A useful result must show what remains protected and why, even when reclaimed bytes are zero.

The counter-decision—delete owner directories after abandonment—is smaller but violates shared-object and worker safety. Keeping every native table head as a permanent root preserves bytes but fails the cleanup goal. A global reference-count database adds a second transactional authority and is not justified by the current implementation. Existing pins and lifecycle rows substantially subsume the required machinery, but no existing traversal is a complete deletion proof.

The kill criterion is any surviving consumer losing bytes, any retired generation being reused or rebound silently, or maintenance requiring full retained-payload scans just to discover liveness. Removing the inventory while enabling deletion first breaks the shared-file/late-worker cases. A future reviewer should reject a design that labels phase retirement complete while permanently protecting every abandoned attempt, or claims full R6 from phase cleanup alone.

**Final verdict: RECONSIDER the current protections as deletion authority; implement the typed, generation-bound, catalog-wide design above.** Intent versus current shape diverges only because physical retention is still unfinished and correctly reported as such. The user-value claim is supported. The proposed design honors the accepted commitments, reuses existing infrastructure and avoids a second lifecycle service. Confidence is high in the inventory and correctness gaps, and medium in performance/resource cost until the bounded race and scaling qualification runs.

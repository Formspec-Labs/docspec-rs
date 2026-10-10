# Compaction recovery retirement: resources and architecture

**Verdict: RECONSIDER the recovery-marker-only shape.** Add one exact compaction-recovery retirement scope, narrow selected representation roots and owner bindings together, and demonstrate removal of an obsolete intermediate representation through the existing removal service. This delivers actual local storage reclamation without changing logical results or introducing another catalog, cache, or scheduler.

Static review dated 2026-10-10, pinned to `a3d695fa9b4644d046f202a90de9c7ad9d9678a3`. All code line references below describe that baseline, not the implementation being edited concurrently. Initial live-file observations were checked against `git show a3d695f:<path>` before reaching this verdict. No builds, tests, benchmarks, corpus reads, payload hashes, or deletion were performed. The implementation owner supplies execution evidence. The unrelated untracked change-chain review was not read or changed.

## Findings

### F1 — BLOCKER: ending recovery alone cannot release the prior representation

Category: user-value; intent-versus-shape.

The selected `representation/` record follows the same inventory branch as prepared, verified, and completed compactions. That branch roots both `result.source` and `result.replacement` (`src/engine/retention/inventory.rs:1193–1228`). The attempt reservation also protects its entire directory (`src/engine/compaction.rs:232–247`; `src/engine/retention/inventory.rs:1494–1510`). Existing retirement scopes do not accept the compaction record families (`src/model.rs:548–555`; `src/engine/retention/retirement.rs:19–81`).

**RESHAPE:** `CompactionRecovery { operation }` should capture the exact request, optional prepared/verified/result generations, and all exactly owned compaction attempt reservations. Retire that set through the shared retirement service. Keep the raw rows as identity, verification, and creation evidence. For the selected representation, inventory must retain only its replacement pin, including its original logical metadata identity. Recovery rows continue retaining both sides until explicitly retired. The existing `table` walker already distinguishes the logical metadata object from the physical pin inventory (`src/engine/retention/inventory.rs:886–905`).

For canonical state S followed by C1 and C2, retiring C1 recovery alone is insufficient: C2 recovery still promises access to its captured C1 source. Reclaim C1 only after both recovery promises end and no independent consumer retains it. Do not remove the current `representation/` authority or fall back to S as part of this decision; that would change the selected physical layout while claiming only to end recovery.

### F2 — BLOCKER: current representation bindings retain obsolete foreign grants

Category: named-seam; resource retention.

Compaction acquires owner grants from the captured source objects and manifest (`src/engine/compaction.rs:161–164`) and attaches those same protections to the selected representation (`:350–355`). Grant retirement refuses any token appearing in an active binding, even if the claim no longer needs every object in that token (`src/engine/retention/grants.rs:125–163`). Every active owner grant independently roots its objects (`src/engine/retention/inventory.rs:289–301`). Narrowing inventory expansion without changing the binding therefore leaves a real reclamation blocker.

**RESHAPE:** acquire selection protections from the replacement's exact objects and optional logical metadata using existing `protect_objects`, outside the consumer dataset lock, before the guarded selection. Keep capture protections on request/prepared/verified/result recovery claims. Do not filter a broad grant token by owner alone: one owner token can contain both retained and obsolete objects. `protect_objects` already checks conflicting identities, deduplicates paths, groups foreign objects by owner, and acquires exact grants (`src/engine/retention/owners.rs:559–602`). Existing explicit grant retirement can end unused grants later.

Also include the source pin's optional logical metadata when acquiring capture protections. After an initial foreign-source compaction, a subsequent locally owned physical pin can still refer to foreign logical metadata: compaction assigns that separate identity after constructing the physical pin (`src/engine/compaction.rs:460–468`). The inventory roots it separately, and binding validation requires coverage of every foreign direct reference (`src/engine/retention/owners.rs:275–289`). This is an exact reference-completeness requirement, not a reason to add recursive scans at every selection.

### F3 — CONCERN: shared operation strings do not establish attempt ownership

Category: commitment-violation; scope.

Compaction names attempt purpose `<operation>/compaction` (`src/engine/compaction.rs:239–245`). Baseline ordinary operation recovery collects every protection whose purpose starts with `<operation>/` (`src/engine/retention/recovery.rs:216–234`), and its validator accepts any valid single purpose suffix (`:170–184`). A run and compaction can therefore collide on the string while remaining distinct record families.

**RESHAPE:** compaction collection must match the exact compaction purpose, consumer, reservation ID/key, canonical owned directory, and revision after the compaction request. Ordinary recovery must exclude compaction attempts. The scope must never capture `representation/`, ordinary run/request/result rows, or another operation's reservation. Recollect the complete exact record set under exclusive maintenance; a new attempt or completion makes a preview stale rather than expanding it. Explicit retirement of an unfinished compaction means abandoning its recovery, including its saved expensive phases; state this consequence in the API and fence all later `compact` retries for that identity before the existing completed-result early return (`src/engine/compaction.rs:99–131`).

### F4 — OBSERVATION: the existing removal path can supply the physical effect

Category: sibling subsumption.

`plan_removal` qualifies exact owned objects through retained creation evidence, then inspects roots and native registrations (`src/engine/retention/removal.rs:180–246`). Execution takes the intersection of previously eligible and freshly eligible objects, rejects new native registrations, and records object unavailability with durable intent (`:351–450`). It detaches exact native registrations before unlinking files (`:715–745`, `:796–823`). Compaction preparation already supplies replacement creation evidence (`src/engine/retention/owners.rs:997–1001`).

**KEEP:** reuse this path unchanged unless a concrete fixture exposes a defect. A successful retirement receipt or zero logical blockers is not a deletion result. The local removal adapter checks file identity and synchronizes the parent; its stated support excludes arbitrary simultaneous external filesystem mutation (`src/storage/removal.rs:18–34`). Native registration enumeration loads every table and follows the metadata closure (`src/engine/retention/removal.rs:883–907`; `src/storage.rs:519–593`). A registration left pointing at deleted manifests can therefore break later maintenance. Validate native detachment as part of real reclamation, not only file absence.

## Artifact, lineage, and relationships

The artifact is the next local R6 maintenance slice. Its beneficiary is the operator whose repeated compactions otherwise add permanently retained output. It is a product maintenance capability backed by proof infrastructure. It refines the implemented compaction slice, whose status explicitly says old storage and obsolete recovery remain retained (`docs/STATUS.md:840–850`).

| Prior artifact | Relation | Consequence |
| --- | --- | --- |
| `docs/PLAN.md`, Architecture and Lifecycle | Governs the shared lifecycle authority and explicit retirement | Reuse the dataset commit, one protection inventory, and exact removal service. |
| `docs/requirements/06-maintenance-retention.md:9–31` | Normative separation of physical layout, availability, recovery, and deletion | Ending recovery must preserve independently promised results and evidence. |
| `docs/requirements/06-maintenance-retention.md:53–57` | Cost and scheduling requirements | Measure actual reclaimed objects; keep automatic maintenance deferred. |
| `docs/requirements/07-operations-qualification.md:101–107` | Q3 cleanup acceptance | Shared references and new protections win; final eligible objects must really be reclaimable. |
| `docs/requirements/07-operations-qualification.md:133–137` | Q7 scaling acceptance | Hold logical data fixed while increasing fragments and retained recovery metadata. |
| `docs/reviews/compaction-prerequisites.md`, Select and reclaim through existing authority | Prior design lineage | Compaction and removal were designed to share the common inventory. |
| `docs/reviews/retention-next-step-review.md`, Minimum implementation shape | Sibling recovery design | Reuse exact-generation retirement and preserve raw creation evidence. This earlier review is intent evidence, not proof of current behavior. |

| Component/seam | Owner | Depends on | Consumers and declaration |
| --- | --- | --- | --- |
| Compaction capture and selected layout | Engine compaction | Exact `TablePin`, verified result, guarded lifecycle commit | Reads and update admission; `compaction.rs:53–83`, `read.rs:21–29`. |
| Exact retirement generations | Engine retention | Catalog revision, markers, durable receipt | Inventory and recovery fencing; `retention/retirement.rs:501–635`. |
| Foreign object protection | Owner and consumer lifecycle tables | `OwnerProtection`, `ReferenceBinding`, exact object sets | Retention and grant retirement; `retention/owners.rs:18–43`, `retention/grants.rs:45–168`. |
| Native registration and file removal | Existing local removal adapter | Fresh shared inventory and exact durable intent | Operator removal/resume; `retention/removal.rs:351–450`, `:604–661`, `:715–745`. |
| Reader/worker exclusion | Existing execution host | Shared admissions and exclusive maintenance | Engine readers and retention; `worker.rs:111–177`. |

No new subsystem owner is needed. Add thin library/CLI/HTTP entry points for the same scoped plan/execute services; do not introduce a separate representation-retirement authority.

## Invariants and commitments

| Invariant | Status in recommended design | Evidence and failure mode |
| --- | --- | --- |
| Layout changes do not advance logical state, schema meaning, or occurrence generations | PRESERVED | R6:15–17; `compaction.rs:460–479`, `storage/identity.rs:69–95`. Removing logical identity breaks reuse and historical interpretation. |
| Completed work remains recoverable until its promise explicitly ends | RELIED UPON | R6:27; capture and completion records stay available until exact retirement. Parent completion or age cannot authorize removal. |
| New protection wins over a stale removal preview | PRESERVED | R6:29–31; existing `removal.rs:369–450`. Bypassing it can delete a newly retained dependency. |
| Reader streams remain protected until EOF/drop | PRESERVED | `worker.rs:159–177`; `read.rs:7–29`. Retirement/removal use existing exclusive maintenance. Free serialized pins are not leases (`read.rs:112–116`, `:161–162`). |
| Identity evidence survives ending availability | PRESERVED | Existing retirement appends markers/receipts; creation proof reads retained compaction preparation. Deleting those rows would obstruct exact safe removal and verification. |
| Recovery identities cannot silently restart after retirement | NEWLY INTRODUCED for compaction | Required before the result/capture replay fast path (`compaction.rs:99–131`). Otherwise an old operation can return missing files or reacquire abandoned work. |

## Resources and user value

The improvement is eliminating unnecessary retention of obsolete rewritten files. It does not reduce compaction's own rewrite or equivalence cost. The selected-file stream is sequential, shares untouched `DataFile`s, and verifies aligned source/output batches (`src/engine/compaction.rs:434–479`, `:500–508`, `:540–609`). At least old and new files coexist during a rewrite; the implementation preflights `2 × input_bytes + scratch_bytes` and checks free space during streaming (`:392–440`). Those checks are conservative admission, not measured high-water storage.

Let W be newly written bytes per full rewrite and k the number of otherwise unreferenced completed compactions. Baseline recovery roots and attempt reservations can retain roughly the sum of those k outputs in addition to the canonical data; this is a structural growth argument, not a measured amplification ratio. Ending all obsolete recovery promises makes intermediate outputs candidates. Independent results, retained inputs, source ownership, releases, restore points, and shared unchanged files legitimately reduce reclaimable bytes. Canonical S may remain required even when C2 is the selected reader layout: result/authority traversal still retains canonical pins and evidence (`src/engine/retention/inventory.rs:1011–1049`, `:1353–1364`).

Planning should use lifecycle metadata and retained object identities, without opening payloads or re-running equivalence. Follow the current bounded per-record decoding and state charges; collection is linear in relevant protection records and retirement records. Physical removal separately pays for the shared inventory and native-table metadata enumeration, and for exact deletion identity checks. This slice should not claim lower RSS, read latency, or complete-history constant-time behavior. It adds no global index or cache; a later measured metadata bottleneck would require its own controlled comparison.

The concrete success measure is positive exact-file reclamation from an obsolete intermediate layout while promised reads, evidence, logical IDs, and recovery of unrelated work remain correct. A six-month falsifier is repeated compaction plus explicit recovery retirement still leaving every intermediate output protected solely by `representation/`, attempt reservations, or broad inherited owner grants.

## Bounded validation matrix

Use generated tiny native fixtures and the maintained integration/removal helpers. No full Comments corpus rerun is needed to qualify this local lifetime change.

| Case | Required observation |
| --- | --- |
| S → C1 → C2, same logical group | Retire C1 and C2 recovery; remove C1-exclusive Parquet and native metadata through `plan_removal`/`remove_objects`; files are absent, C1 registration is detached, reported removed bytes equal exact candidates. C2 remains selected and readable after cold reopen. |
| Historical and logical promises | Retain canonical S result, logical metadata, occurrence origins, field IDs, and succession. Check exact values/types/nulls/duplicates and identities, not row counts alone. Verify S through an independent canonical pin as well as the engine's selected C2 read. |
| Partial rewrite sharing | A C1 file still shared by C2 remains blocked; only exclusive files disappear. Verify unrelated current files and canonical metadata remain present. |
| Active reader and unfinished consumer | Open a reader on C1 before C2 selection and retirement; exclusive maintenance waits for EOF/drop. Separately retain a phase/input binding to C1; it continues blocking removal after compaction recovery retirement. |
| Interrupted compaction | Stop after rewrite and verification; explicit recovery retirement abandons only that compaction. Retry of the retired ID refuses without payload opens/rewrite; selected layout stays unchanged. |
| Foreign grants | Selected binding covers replacement objects and logical metadata, excluding obsolete rewritten foreign bytes. Old broad grant retirement is blocked while a recovery claim still needs it, succeeds when unused, and retains any separately claimed owner objects. Include a second foreign-source compaction. |
| Same operation string | Ordinary operation and compaction with the same ID retain independent attempts/results. Each retirement scope captures only its owned records; corruption of purpose, consumer, directory, ID, or revision refuses. |
| Stale or malformed plans | New attempt/completion after preview rejects the changed set. Missing/swapped completion, expanded scopes, orphan markers, changed revisions, and conflicting retries refuse before effects. |
| Lost responses and removal boundaries | Reuse existing intent/detachment/unlink fault hooks and cold reopen: exact retirement retry reconciles, deletion outcomes remain truthful, new native aliases block stale removal, and no completed effect repeats. |
| Bounded metadata | Small metadata allowance yields structured refusal while increasing retained attempts/history; no payload revalidation during retirement and no new global scan/cache on ordinary reads. |

Existing removal tests already demonstrate native aliases, shared data, active streams, and real file absence (`src/engine/retention/removal/native_tests.rs:39–190`); extend their concrete fixture relationships instead of duplicating the removal machinery. Record fixture logical file sizes and confirmed removed/absent bytes separately from allocated blocks or volume free-space changes.

## Counterfactuals and verdict

Keeping every recovery promise forever is safe but fails the resource outcome. Automatically retiring recovery at representation selection reduces retained space but silently revokes recovery and unfinished-work promises; reject that choice. A standalone representation registry duplicates the lifecycle authority. Generic expiry policies, broad orphan sweeping, and automatic scheduling are deferred by R6 and add no necessary value to this explicit slice.

Kill criterion: an object becomes removable while a selected replacement, active reader, retained result/phase, or valid owner claim still needs it. Another failure criterion is a retired compaction ID that replays a success naming deleted files. Removing the new scope entirely first breaks the ability to end compaction recovery; removing its evidence rows instead breaks exact ownership and saved-record validation.

**Final verdict: RECONSIDER → reshape as specified in F1–F3, retain F4.** Intent and the recommended shape match. User value is supported by the traced retention blockers; throughput and storage gains remain unmeasured. The design pays down lifetime ambiguity, partially reuses the existing operation-retirement design, and fully reuses the removal path. Confidence is high for these static local dependencies and medium for physical reclamation until the fixture matrix and repository gate pass. This review does not establish full R6, remote/provider cleanup, or performance acceptance.

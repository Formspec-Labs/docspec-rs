# R6 — Control physical cost while honoring history promises

Status: greenfield implementation plan. [Shared decisions](../PLAN.md) govern retained roots, schema interpretation, and commits. No existing maintenance API or storage layout must be preserved.

## User outcome and requirements

“Frequent corrections do not make reads progressively slower, and cleanup preserves every version, release, and unfinished operation we still rely on.”

Compaction changes physical representation without changing logical meaning. Retention changes which content is promised available. Neither action silently changes the other. Historical provenance records, retained content, optional acceleration, and recovery support have distinct lifetimes.

## Physical maintenance

Use the selected maintained table engine's compaction and metadata facilities where they preserve DocSpec's requirements. Read the effective pinned snapshot, including delete semantics and schema mappings. Rewrite bounded partitions/file groups and verify exact values, native types, keys, multiplicity, relevant order, membership, and required evidence. Counts alone do not establish equivalence.

An equivalent representation preserves logical state and value/occurrence identities, field IDs, schema meaning, and generation/reuse history. Existing occurrences absent from current rows may still be pinned by historical results. Do not reconstruct necessary history from current membership alone.

Publish a verified replacement representation through the catalog without advancing the logical dataset version. Concurrent writers and readers may still use old files; those references remain protected. Retain in-flight compaction roots before object production, reconcile interruption, and reclaim only when the common root inventory permits it. A failed verification keeps the previous representation.

Measure expected read savings against rewrite, verification, temporary storage, and metadata cost. No universal file-size or delete-count threshold is prescribed. Disclosure-based layout from R2 must not fragment reads beyond its justified benefit. Representation choice is independent of schema evolution; compaction cannot erase field meaning or fabricate processing.

## Retention and deletion

Declare separate policies for public-release availability, catalog recovery/restore points, optional R4 change history, and application-retained source/results. Exact input bindings, selected coverage/order evidence, active readers, legal application obligations, and pending operations can protect content beyond a nominal window. History is not automatically permanent, and expiration is not automatic deletion authority.

One inventory resolves immutable roots to exact physical objects and versions. Include selected and explicitly retained versions/results, source and value bindings, schema mappings, enrichment/group inputs, required succession evidence, supported restore points, public releases, reader pins, independently completed phase results, and unfinished upload/commit/publication/compaction/removal work. Deduplicate shared references; use incremental reachability or indexes only when maintained and independently checked.

Completed transfer, restoration, matching/processing, and validation outputs are recoverable roots, not scratch owned solely by the parent job. Preserve their exact selected inputs and required completion/verification evidence while the phase recovery promise stands. A failed parent, closed reader, stale running marker, or report-rendering exception must not delete those bytes. Retire phases only through an explicit abandonment or declared recovery-retention policy and fresh dependency checks; show expiration and blockers in status. A phase can expire while a separately retained result still protects shared objects. Never remove required evidence merely to make a retry appear reusable.

A retention plan records policy, catalog/inventory boundary, candidate logical records and physical identities, reasons, blockers, and estimated reclaimable bytes. Execute only the intersection of that plan and fresh eligibility. New protections win; a stale plan never expands scope. A nominally expired release can still have bytes required by a retained result.

Before external deletion, atomically reserve the removal, mark targets unavailable for new retained references, and durably record exact intent. New commits cannot bind objects being removed. Delete by immutable identity/version and retain per-object deleted/absent/failed outcomes. Reconcile unknown responses. Crash recovery must expose pending removal, never restored metadata falsely promising deleted bytes.

Catalog recovery and object retention must share a support boundary. Every advertised restore point or point-in-time recovery interval protects its required object versions. Retire that promise durably before reclaiming dependencies. Do not restore an old database image and assume current object contents still match it. Durable intent commits provide the pre/post-effect barriers; a full database backup before each delete is unnecessary.

Anonymous readers rely on published availability windows; they do not implicitly create leases. Explicit authenticated pins/leases require a concrete service and expiry policy. Cache eviction may remove verified local copies only when retained authoritative bytes remain recoverable; cache policy is not retention policy.

Automatic orphan sweeping is deferred. If later justified, inventory only an owned namespace, account for every active reservation and recoverable root, apply a bounded quarantine, and send exact candidates through the same removal service. Age or an object-store listing alone does not prove an orphan.

## Implementation tasks

| Task | Owner and dependencies | Deliverable and acceptance |
| --- | --- | --- |
| **R6-T01** | Physical equivalence and retention semantics; phase/root slice before first Comments path, coordinate R1–R5 | Define representation equivalence, required history/evidence, phase-completion roots and recovery promises, independent policy windows, and restore boundaries. Parent failure is not phase abandonment. Expiration, availability, reclaimability, and deletion remain distinct. |
| **R6-T02** | Native table maintenance; T01, selected table engine | Implement bounded typed compaction using maintained facilities and exact equivalence checks. Nested/null/decimal/time values, field mappings, effective deletes, and empty states remain exact; missing, resurrected, duplicate, or altered rows refuse. Include compaction during [Q2 concurrent field-group updates](07-operations-qualification.md#q2-concurrent-field-group-updates). |
| **R6-T03** | Catalog representation commits; T02, R1 | Publish equivalent representations and recover interrupted rewrites. Old readers/writers remain protected; logical changes report zero; physical replacement does not create a new occurrence generation. |
| **R6-T04** | Root inventory and reachability; T01 and available shared root formats; phase protection precedes first-path recovery | Resolve retained, independently completed phase, recovery, public, enrichment, ordering, reader, and in-flight references. A failed job/report or closed reader cannot delete qualified phase outputs. Removing one owner preserves shared bytes; optional changes expire independently of required inputs/evidence. Qualify native cleanup against [Q3 retained data](07-operations-qualification.md#q3-native-cleanup-and-retained-data). |
| **R6-T05** | Retention planning/removal; T04, R1 durable intents | Implement bounded preview, explicit phase abandonment/expiry, fresh eligibility, atomic removal reservations, exact-version deletion, and reconciliation. New protections shrink scope; concurrent commits cannot reference removing objects. Test recovery after late/reporting failure and phase expiry with shared dependencies, then every deletion boundary. Include [Q3 cleanup races](07-operations-qualification.md#q3-native-cleanup-and-retained-data) and [Q6 old deleters after restore](07-operations-qualification.md#q6-delayed-authority-and-restore). |
| **R6-T06** | API/CLI and status; protection/status slice follows T04/T05, compaction follows T03 | Expose compact, preview, execute exact plan, inspect blockers, and resume. Show phase recovery/abandonment, nominal expiry, protected/reclaimable/actually reclaimed bytes, and recovery-window consequences. No hidden bucket lifecycle bypasses retained obligations. |
| **R6-T07** | Qualification; early layout probes with R7, full validation after T06 | Run repeated updates, reader/writer/cleanup races, expired-but-required history, point-in-time restore, and corruption scenarios. Qualify provider behavior and measured cost before automatic maintenance. Include [Q7 metadata and retirement scaling](07-operations-qualification.md#q7-metadata-and-evidence-scaling). |

## Measurements and decisions

Hold update size fixed while history, file count, and deletes grow. Measure read amplification, file opens, bytes scanned, query latency, compaction/verification duration, catalog work, memory, scratch, and physical high-water storage. Include retained intermediate phases across interrupted/resumed jobs and the cost of their eventual retirement. Count shared bytes once; distinguish authoritative retained work from removable cache copies. Protected history can prevent net reclamation even after successful compaction.

Enable recurring compaction only when amortized benefit meets the declared workload horizon. Otherwise retain explicit operation and explain the bottleneck. Measure actual reader cost, not merely fewer files.

Product owners set release/recovery/change/application windows and any explicit reader-pin promise. There is no default automatic deletion. Orphan sweeping, broad indexes, and automatic scheduling require measured need and their own failure qualification.

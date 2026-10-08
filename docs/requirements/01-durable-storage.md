# R1 — Commit durable dataset updates efficiently

Status: greenfield implementation plan. [Shared decisions](../PLAN.md) govern the interfaces. Existing local storage and APIs impose no compatibility requirement.

## User outcome and requirements

“Once an update or expensive phase is durably complete, replacing a worker does not lose it or force me to repeat qualified work.”

An operation commits exact data references, schema and definition identities, result coverage, and any selected dataset version. Distinguish computation completed, candidate retained, catalog committed, response received, and publicly released. Lost responses must reconcile committed work; uncommitted computation may rerun. External side effects require their own idempotency.

The [research requirements](RESEARCH.md) make phase completion a first-delivery capability. Transfer, restoration, processing, validation, publication, and readback have separate outcomes. A complete transfer can survive an unfinished validation phase; it cannot claim validated or public status.

## Design

Use a maintained durable transactional catalog/database and immutable object storage. Select products and deployment from atomicity, recovery, concurrency, read patterns, operating cost, and the R7 failure-domain requirements. No particular database, table engine, or cloud provider is mandatory. Do not implement a custom replication log or consensus service.

Workers establish durable in-flight reservations before creating retained candidate objects, then write bounded immutable data and verify exact references under that protection. Private disposable scratch may precede the reservation. One atomic catalog commit validates availability, records the completed result, checks expected output and definition/policy guards, and selects the new version when requested. Commit references only objects whose storage acknowledgement satisfies the declared durability profile. Table-engine metadata is an immutable input to that selected version; a separate mutable engine head must not compete with the serving authority.

Concurrent workers may capture, compute, and upload independently. Conflicting selection commits use conditional catalog transactions. Old-definition workers cannot commit after cutover. Use scoped credentials and maintained fencing where required for destructive maintenance or worker ownership; a clock timeout alone is not a fence. Begin with the least operational complexity meeting the workload, without imposing one global writer on unrelated datasets.

A durable catalog commit, with the required objects durable, establishes acknowledgement. Consistent backups, replication, and restore checkpoints support the declared disaster-recovery promise independently of per-operation commits. A full SQLite backup per operation is not the default architecture. A local embedded implementation is acceptable only if it meets its declared durability profile and does not weaken the service profile.

Optional bounded group commit may amortize catalog or transport cost. Preserve each operation's identity, outcome, covered commit, and response. Declare maximum grouping delay/count/bytes and pending-work limits. A reached limit forces progress or backpressure; it never permits early durable acknowledgement. A later uncovered operation remains pending even when an earlier group committed.

External publication and deletion use durable pre-effect intents and post-effect reconciliation. A release pointer cannot advance until its candidate and retention roots are committed. Objects cannot be deleted until removal intent and unavailability are committed. These barriers use catalog durability, not mandatory full backups. Supported older restore points protect their referenced objects; retire those promises explicitly before deletion, and qualify restore against pending effects.

## Shared phase completion and reuse

Represent each expensive completed phase as an ordinary retained catalog result, with no second job ledger or general workflow framework. Select phase boundaries by measured recomputation/transfer cost and useful recovery points, not every internal function call. A phase completion records:

- Stable phase identity and kind, parent operation, exact selected subject/source and receipt/evidence pins, selection scope, ordering/multiplicity rules, and predecessor result references.
- Material processor/configuration, schema, policy, decoder, and validator identities for that phase. Retain full runtime/package provenance separately; a broad repository hash alone is not the invalidation rule.
- Immutable output identities, sizes/digests and interpretation metadata, explicit outcome, and qualified validation scope/results. Counts and readable footers can support diagnostics but cannot establish complete identity or admission.
- Durable commit identity, retained protection, and resume preconditions. The phase record distinguishes complete transfer, successful validation, and any outstanding checks rather than combining them into one success flag.

Commit completion and its retention roots only after outputs satisfy the declared phase checks. Preserve earlier phase results if a later phase fails, the worker exits, or a report cannot be serialized. Persist structured validation outcomes before rendering optional reports. Expired, corrupt, mismatched, or incompletely retained results cannot authorize reuse. R6 governs explicit retirement, so a failed parent job does not turn its useful completed phases into disposable scratch.

Select exact inputs once per operation and share a bounded read/admission context across its consumers. It owns pinned references, qualified verification results, maintained decoding/member-resolution helpers, resource observations, and reader lifetime. Reuse native relations, streams, and compact metadata; do not retain an unbounded decoded corpus or add a global cache. R4/R5 use this same interface rather than reselecting and reopening every input independently.

Before staging payloads, look up qualified results using known immutable input identity and the phase's material dependencies. A trusted unchanged selection may bypass full payload restaging or transfer; unknown identity still requires the necessary admission. Verification reuse requires exact content/evidence pins, scope, relevant schema/validator/policy versions, and the same qualified trust boundary. Changed requirements invalidate affected checks, not unrelated phases. Independent public readback and other checks at distinct trust boundaries still execute.

Expose one supported inspection/resume path over these results. It reports the identities actually consumed, reusable phases, required reruns and reasons, next action, and resource preconditions. A lost response reconciles its exact phase commit. An old running marker is an observation requiring reconciliation, not proof of completion or permission to delete its outputs.

## Implementation tasks

| Task | Owner and dependencies | Deliverable and acceptance |
| --- | --- | --- |
| **R1-T01** | Storage/catalog and phase semantics; R7 requirements and early probes | Compare maintained components against guarded commits, immutable references, recovery, concurrency, and cost. Define the shared phase-completion fields, selected-input context, reuse/trust rules, durability domains, and acknowledgement. Use the research cases before choosing infrastructure; preserve greenfield freedom. Run the bounded [Q1 memory](07-operations-qualification.md#q1-process-memory), [Q2 concurrency](07-operations-qualification.md#q2-concurrent-field-group-updates), and [Q3 cleanup](07-operations-qualification.md#q3-native-cleanup-and-retained-data) probes before selection. |
| **R1-T02** | Catalog transactions and runtime composition; T01 | Implement ordinary phase/result commits, exact idempotency, early completed-work lookup, shared input context, in-flight protection, definition fencing, and injected clients. A qualified unchanged operation avoids full staging; mismatched evidence refuses reuse. Parallel independent datasets progress; output races and conflicting retry identities remain guarded. |
| **R1-T03** | Table/object storage and maintained admission; T01 | Implement bounded snapshot I/O and exact inventory with shared decoding/member resolution and read lifetime. Consumers reuse the same pinned selection and equivalent verification within an operation. Append/correction/removal/empty states match an independent oracle; count actual reads and reject changed or unqualified inputs. |
| **R1-T04** | Durability and phase recovery; T02–T03 | Implement phase acknowledgement, lost-response reconciliation, supported resume, and qualified backup/restore. Kill after restoration, processing, transfer, validation, commit, and optional grouped flush. Complete transfer resumes at validation without retransferring; report formatting failure preserves verified data/results; uncovered or invalidated work still runs. |
| **R1-T05** | Commit/retention integration; T02–T04 and R2/R6 intent formats | Protect independently completed phases after parent failure and implement durable pre/post-effect records for promotion/deletion. Crash between effects and reconciliation restores safe intent and availability. Recovery/phase roots block premature deletion; a pending group cannot postpone a required barrier. |
| **R1-T06** | Qualification and operations; T01–T05, R7 | Run real-provider and independent-process qualification, backup/restore drills, and measured cost reports. Document exact supported failure domains, concurrency, versions, resource limits, and recovery procedures; test doubles alone do not establish provider guarantees. Include [Q6 delayed authority and restore](07-operations-qualification.md#q6-delayed-authority-and-restore). |

## Failure and operating behavior

Unknown object-write responses reconcile exact namespace/version/digest/size; do not overwrite a conflicting object. Unknown commit responses query the stable operation identity before retrying computation. Candidate protection lasts through commit or explicit abandonment. Failed processing remains visible without becoming a successful dataset result.

Run resource preflight before expensive phases using retained object sizes, explicit estimates, configured memory/scratch limits, and shared execution capacity. Unknown requirements remain unknown; observed engine limits do not describe whole-process memory. Insufficient capacity waits, applies backpressure, or refuses without discarding completed work. R7 records observations during the actual operation, without an extra data pass.

Recovery verifies a mutually consistent catalog point and referenced objects, including durable intents. Do not expose a catalog backup whose required object versions have expired. Source acquisition and non-idempotent external actions remain distinct from recoverable dataset commits.

## Measurements and decisions

Hold changed rows fixed while increasing dataset population and catalog history. Measure selection/decoding/admission passes, phase lookup and reuse overhead, cold restoration, verification, commit/acknowledgement delay, transferred bytes/requests, concurrency, whole-process and engine memory, scratch, backup growth, and restore time. Compare uninterrupted, unchanged-repeat, and interrupted/resumed paths. Every repeated expensive phase needs a recorded dependency, availability, or trust-boundary reason. Compare grouping only when needed; maintained database group commit may already suffice.

R7 sets acknowledgement/freshness, failure-domain, and recovery targets before mechanism selection. Full backup cost is a recovery measurement, not automatically an update cost. Include all required durability costs in the final update-to-independent-reader result.

Open choices are the maintained catalog/table/provider combination, deployment topology, numeric grouping/backpressure limits if used, and disaster-recovery targets. Automatic failover is deferred absent a demonstrated requirement; manual recovery must still enforce exclusive authority where required.

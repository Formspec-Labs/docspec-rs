# R6 local worker and maintenance exclusion review

**Recommendation: use Tokio's owned read/write guard and an owned service task.** Admit ordinary engine work once under a shared maintenance guard; move the guard, cloned engine and owned request into a private Tokio task. Caller cancellation then drops only its join handle, while the task continues awaiting the current operation and retains both maintenance exclusion and workspace ownership. Destructive maintenance owns the exclusive guard through fresh inventory, durable intent, effects and reconciliation. This adds no worker registry, scheduler or second database.

Reviewed `40293be790137f01d307d413d5f66c4a77489baf` plus the current read-only retention-inspection patch. Source locations refer to the working tree inspected during this review. Static inspection only: no code changes, Cargo, corpus reads or deletion. The recommendation covers ordinary caller cancellation in the qualified local runtime. Forced task abort, panic and runtime shutdown are separate failure boundaries; the guard alone does not prove those safe.

## Concrete findings

### W1 — A borrowed outer guard would release too early

`Engine` owns `Arc<Inner>`, but its async services currently execute in the caller's future. `Inner` owns `WorkspaceLock` (`src/engine.rs:46`). Dropping that future can drop the last engine while an already started `Budget::blocking` closure continues. `Budget::blocking` moves resource permits into the worker, but neither the engine nor a maintenance guard (`src/budget.rs:114`). Existing tests prove resource admission survives abort of the awaiting task, not lifecycle custody (`src/budget.rs:340`). The same issue affects copy admission, native metadata registration and metadata decoding (`src/engine.rs:660`; `src/storage/tables.rs:91`; `src/engine/inputs.rs:298`).

**Action:** do not merely add `let _guard = read().await` to the caller's existing service future. Put the admitted operation in a private owned task that awaits its existing work to a terminal result. The task owns a cloned engine and an `OwnedRwLockReadGuard`; a canceled HTTP or embedded caller only stops waiting. Existing explicit abandonment remains the way to revoke completion/selection authority. There is no implicit abandonment from losing a response.

### W2 — SQLite makes cancellation unsafe even when native file I/O is synchronous

The local catalog uses `LocalFsStorageFactory` (`src/storage.rs:52`). In pinned Iceberg `0.10.1`, that adapter's `write`, `delete`, and `FileWrite::write/close` call synchronous `std::fs` operations; `close` calls `sync_all`. These calls do not launch detached file-writing tasks. This is a verified property of the current adapter, not a guarantee for injected/R2 storage (`iceberg-0.10.1/src/io/storage/local_fs.rs:135`, `:179`, `:282`).

The SQL catalog does await SQLx operations, however (`iceberg-catalog-sql-0.10.1/src/catalog.rs:350–385`). SQLx SQLite queues owned commands to a separate connection thread. Its worker can execute a statement after the receiver stops waiting, and explicitly handles a committed transaction whose acknowledgment was lost (`sqlx-sqlite-0.8.6/src/connection/worker.rs:144`, `:254`, `:355`). Canceling an awaited catalog call is therefore not a proof that its effect stopped.

**Action:** retain service-task custody across the complete awaited native transaction and result reconciliation, not only Parquet writing. `Lifecycle::commit` writes lifecycle files and then awaits guarded native append (`src/catalog.rs:270–299`); releasing the guard between those steps would reopen the race. The existing error/lost-response reconciliation remains necessary; task ownership does not replace it.

### W3 — Reacquiring a shared guard inside helpers can deadlock

The actual nesting includes `run_internal` → source work → `release`, and Refresh admission → `capture_inputs`. Backfill planning also calls `release` (`src/engine.rs:873`; `src/engine/write.rs:103`, `:185`; `src/engine/refresh.rs:53`; `src/engine/backfill.rs:188`). Tokio's read/write lock is fair and blocks later readers behind a queued writer. Its own documentation warns about an outer reader waiting for an inner reader after a writer queues (`tokio-1.53.2/src/sync/rwlock.rs:537`).

**Action:** admit once. Split public `release` and `capture_inputs` entry points from private already-admitted bodies; internal calls use those bodies. Apply the same rule to inspection reused by exclusive maintenance. Preserve the existing sorted dataset-lock order beneath the maintenance guard (`src/engine/inputs.rs:139`). Do not use task-local reentrancy flags or acquire maintenance while holding a dataset lock.

### W4 — Some nominal reads still create catalog state

`plan_request`, `inspect`, `latest`, `release`, and `prove_succession` call `Lifecycle::open`, which can create a namespace and lifecycle table (`src/engine.rs:201`, `:307`, `:318`, `:337`; `src/engine/history.rs:16`; `src/catalog.rs:89`). Exempting these methods from admission would leave catalog mutation possible during destructive inventory. `Engine::open` itself holds the host lock in a cancellable future while opening/initializing the SQL catalog (`src/engine.rs:122`).

**Action:** include these services in shared admission while they use `open`; a later noncreating load helper can remove unnecessary writes if warranted. Retain owned host custody through initial catalog setup too. That initialization task needs no maintenance guard before the engine exists, but must own `WorkspaceLock` until initialization finishes or its failure is resolved.

### W5 — Cancellation resistance is not abort or restore fencing

Tokio documents that dropping a `JoinHandle` detaches the task, while runtime shutdown/abort can cancel async tasks. Started blocking jobs cannot be aborted and can outlive `shutdown_timeout` (`tokio-1.53.2/src/runtime/task/join.rs:18`; `src/task/blocking.rs:106`). An async service task forcibly aborted during a queued SQLite request may drop its engine/guard before that worker finishes. An owned Tokio guard is in-process exclusion, not a storage authority token.

**Action:** keep service join/abort handles private and do not place them in an abort-on-drop container. Graceful runtime shutdown must stop admission and drain admitted work before releasing the host/catalog; it must not abort those workers. If forced abort or panic makes effect completion uncertain, refuse destructive maintenance/reopen in that host until the uncertainty is resolved, or terminate the process so old actors cannot continue. Do not claim same-process runtime replacement or restore fencing from this guard. Supported restore and provider deletion still require R6's durable intent and actual old-actor fencing.

## Smallest concrete shape

Use one `Arc<tokio::sync::RwLock<()>>` inside shared engine state. Keep existing dataset mutexes for ordinary decision serialization. No reference counter is needed because the lock already tracks admitted holders.

At an ordinary public engine entry point:

1. Validate/own the bounded request and clone the engine.
2. Acquire an owned shared guard before dataset locks or externally visible work. If the caller cancels while waiting, nothing has started.
3. Without another await between acquisition and spawn, move engine, request and guard into one private Tokio task. The task invokes the existing internal implementation and awaits its terminal result, including reconciliation.
4. Await its join handle in the caller. Dropping the caller detaches the task; it does not abort it. Explicit abandonment still checks the existing dataset lifecycle and can run concurrently under another shared guard.

The guard is acquired outside the private task so the implementation does not create an unbounded population of detached tasks merely waiting for maintenance to end. Owned request memory must still respect existing request/metadata admission. Detached work continues the originally requested operation; this changes cancellation behavior deliberately and must be stated in the API documentation. It does not make new work or a new retry. If a lost client retries the same operation ID, retain current reconciliation/reuse behavior.

Normal operations hold shared guards together, preserving unrelated dataset concurrency. Maintenance takes `write_owned` before any dataset locks and keeps it through the exact removal service. It waits for admitted work instead of guessing from timestamps or forcibly canceling it. A currently long operation can delay maintenance; report that wait without promising preemption. A queued maintenance writer can also delay a newly requested abandonment until existing admitted work drains; this is a fairness/latency tradeoff, not a correctness deadlock.

The exclusive maintenance task must also be privately owned. Once its durable intent exists, disconnecting the caller must not abandon external deletion midway without reconciliation. Pending durable removal state remains necessary for process death and restart. The in-memory guard is not the persistent prohibition against reuse of removing objects.

### Entry-point coverage

| Service family | Admission / lifetime |
| --- | --- |
| Build, Update, Schema, Refresh and test `*_until` routes | One shared admitted task at common `run_internal` boundary; test wrappers use the same path. Keep internal calls already admitted. |
| Backfill and `backfill_until` | One shared admitted task around `backfill_internal`, including success-after-error reconciliation. |
| `invalidate`, `abandon`, `seal_generation` | Shared admitted tasks around lifecycle mutation; existing dataset/input locks remain beneath the gate. |
| `capture_inputs` | Shared public wrapper; Refresh calls its private admitted body. Coverage returned to an external caller is still not a new retained result or lease. |
| Plans, `inspect`, `latest`, `release`, `prove_succession` | Shared admission while they may call creating `Lifecycle::open`. Internal `release` callers bypass the public admission wrapper. |
| `inspect_retention` | Shared entry protects its metadata reads from destructive maintenance; other ordinary writers may still advance between reported table boundaries. Exclusive execution calls its internal collector with its already-held guard, then checks fresh eligibility. |
| `read_group`, `read_evidence`, `read_combined`, standalone verification | Keep catalog-free release semantics. Preserving all issued result/readback releases keeps their dependencies outside phase-cleanup candidates. The maintenance gate does not represent detached-reader leases. |
| Engine initialization | Owned setup task keeps `WorkspaceLock` across awaited local catalog initialization. |
| Future retirement/removal/restore | Exclusive owned task plus persistent intent/availability rules; remote/injected catalog support requires a separately qualified storage authority boundary. |

These are engine-mediated guarantees. Public low-level `Lifecycle`, catalog and storage APIs can be used independently (`src/lib.rs:6–13`); arbitrary direct catalog/filesystem mutation outside the owned engine is not fenced by its private lock. Keep destructive support scoped to the existing `owned_catalog` qualification (`src/engine/retention/inventory.rs:19`).

## Maintained facilities considered

| Facility | Decision and concrete reason |
| --- | --- |
| Existing `WorkspaceLock` / `fs2` | Keep for cross-process engine ownership. It cannot distinguish normal work from maintenance within the one host (`src/budget.rs:157`). |
| Tokio owned `RwLock` guards | Prefer: existing dependency, concurrent ordinary operations, FIFO writer admission, transferable guard lifetime. |
| Tokio `Mutex` around whole operations | Reject: serializes unrelated preparation and turns maintenance safety into a throughput restriction. |
| Tokio `Semaphore` plus acquire-all drain | Reject here: needs a chosen reader capacity and duplicates read/write semantics. Draining `Budget.blocking_jobs` alone misses queued SQLx/native work and can deadlock maintenance that itself needs `Budget::blocking`. |
| Private Tokio task + `JoinHandle` | Prefer for caller cancellation: keeps current native awaits alive and engine/guard ownership explicit. One task per service call; no durable task registry. |
| `JoinSet`, custom task tracker or general cancellation framework | Do not add now. No need to schedule jobs or enumerate task records; abort-on-drop behavior would oppose effect draining. A future shutdown need should use a maintained drain mechanism after its exact requirement is established. |
| Thread-local/task-local current worker identity | Reject: implicit propagation is fragile across spawned tasks and is unnecessary with admitted internal calls. |
| Scoped token cloned into every `Budget::blocking` closure | Useful if forced async-worker abort becomes supported. It protects application blocking jobs only; it does not reach SQLx's internal connection worker. Do not claim it alone solves native/catalog effects. |

## Verified guarantees and open proof obligations

**Verified from current code and pinned local sources:** caller futures own current engine operations; app blocking closures retain resource permits independently; SQLx queues effects on connection threads; owned Tokio join handles detach on caller drop; owned read guards can move into tasks; the local Iceberg writer does synchronous file writes and synchronous close/sync; current native append and lifecycle commits await the catalog.

**Bounded additional observation:** Iceberg snapshot duplicate-file validation spawns manifest-read tasks and folds them with `FuturesUnordered` (`iceberg-0.10.1/src/transaction/snapshot.rs:184`). On early failure, dropped join handles can leave reads running. These are reads, not catalog/root mutations; do not claim the proposed barrier drains every library thread. The qualified local adapter contains no detached payload-write path found in the inspected writer/I/O code. An exhaustive DataFusion/library shutdown audit is outside this bounded review.

**Assumptions requiring tests before deletion:** private service tasks are never externally aborted; all engine paths capable of creating a root or object enter the common admission boundary; internal nested calls never reacquire it; no supported callback starts unowned mutating tasks; terminal success/error paths do not leave a queued mutating catalog effect whose completion is still unknown. The latter needs delayed native/SQLite fault qualification, not a timing sleep. Changes to adapters or library execution behavior reopen this proof.

**Not established:** crash durability of new removal outcomes, restore fencing, safe arbitrary user-written filesystem mutation, R2/injected catalogs, forced async abort while retaining a live process, or release-expiry safety. Existing pins and disabled cleanup remain necessary while these are unfinished.

## Required focused qualification

- Stall `Budget::blocking` during admission and native metadata creation. Drop/abort only the public caller, drop its engine clones, then request exclusive maintenance and attempt a second `Engine::open`. Both remain excluded until the private operation finishes; completed work stays inspectable.
- Pause a native catalog mutation before acknowledgment, disconnect the public caller, and deliver success/error/lost response. Maintenance cannot inventory until the operation reaches its durable reconciliation boundary; retry observes the original operation.
- Queue exclusive maintenance while an admitted Build calls internal `release`, and while Refresh calls internal `capture_inputs`. Both finish without waiting for a second shared guard; maintenance proceeds afterward.
- Run unrelated datasets concurrently and show both hold shared admission. Verify queued maintenance eventually wins over later work without serializing the ordinary path.
- Exercise creating plan/read routes and canceled engine initialization. No catalog effect escapes the host/exclusion lifetime.
- Disconnect maintenance after intent and after partial effects. Its owned task finishes or leaves durable pending state; a new engine reconciles exact identities before conflicting work.
- Demonstrate graceful shutdown drain. Separately force-abort/panic a private worker in a fault test and verify the host refuses destructive continuation rather than treating a released in-memory guard as proof of safety.

Measure the ordinary-path cost of one task/owned request/guard and maintenance drain latency. There are no payload scans, per-file task records or repeated computations in this proposed boundary. Do not claim speed or memory improvement before measurement; its concrete simplification is reusing Tokio ownership to keep existing awaits alive instead of building a second worker-tracking service.

**Verdict: APPROVE the owned-task/shared-exclusive design for implementation, subject to the caller-cancellation and nested-call tests above.** Keep destructive cleanup disabled until the exact inventory, retirement generations, durable unavailability and recovery barriers are implemented together. The design supports full R6; this worker boundary alone does not complete it.

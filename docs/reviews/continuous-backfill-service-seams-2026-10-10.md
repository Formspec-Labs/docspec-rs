# Continuous scalar backfill: service integration seams

Reviewed commit: `76359912d5a7ea65ffd19a7280a65a05b2a3f5fa`, 2026-10-10.
Static source review; no builds, tests, benchmarks, or production edits. The
Rust and semi-formal architecture-review skills informed this report.

**Verdict: APPROVE the focused controller shape with the changes below.** Existing
scalar preparation, immutable file completions, retained readbacks, and native
selection cover the expensive work. A small parent intent, owned exact children,
and one dataset admission fence cover the missing coordination. This is a design
assessment, not evidence that continuous cutover is implemented or qualified.

## Findings that constrain implementation

### F1 — BLOCKER: selection must include the parent result and fence close

`select_with_progress` writes authority, result, succession, optional changes and
input progress in one commit (`src/engine/history.rs:162–246`). Writing a parent
completion or closing a fence afterward creates a lost-response gap: the child
may be selected while the parent appears unfinished or still blocks admission.

**RESHAPE:** add one private selection variant that accepts the already validated
continuous selection records and appends them to this same commit. Keep the
ordinary selection path as a wrapper with no additional records. Validate the
parent/child identities, exact fence token, epoch, eligibility, source boundary,
target lineage, pending admissions, corrections and live dependencies against the
locked selecting view before calling it. The helper must not accept arbitrary
public lifecycle mutations.

Resolve an existing parent completion before checking an expired fence, current
definition or current head. The existing backfill already resolves its own result
before promotion and reconciles a completed result after most errors
(`src/engine/backfill.rs:471–502,519–531,873–879`). Preserve that ordering for the
parent. A caller who loses the final response receives its original result after
restart or later selections; explicit result-availability retirement still wins.

### F2 — BLOCKER: private children need enforced ownership, not a naming convention

Every public `backfill` call runs the full prepare-and-select service. The supplied
operation ID is only checked against its request digest, retirement and
abandonment (`src/engine/backfill.rs:440–529`). A caller that learns a reserved
child's exact request could otherwise call that API and select it outside the
parent's lineage/fence rules.

**RESHAPE:** persist a strict child-owner record before, or atomically with, child
admission. It binds dataset, parent intent digest, round, child ID and exact child
request digest. Public backfill entry points, including the failure-injection
entry, reject owned children. A private mode accepts only an exact validated
parent/child association. Apply the check again at final selection; a generated
ID prefix alone is insufficient. Refuse collisions with already admitted ordinary
operations before reserving a child, and reject reuse of a parent ID by ordinary
source/backfill operations.

### F3 — BLOCKER: new metadata prefixes must be taught to retention and restore

The protection inventory visits every lifecycle record and rejects unrecognized
prefixes (`src/engine/retention/inventory.rs:279–362,1518–1527`). Restore uses that
same inventory (`src/engine/retention/inventory.rs:138–172`;
`src/engine/restore.rs:179–181`). Simply adding parent, round or fence records
would disable inspection, physical removal, and restore-point creation.

**RESHAPE:** give each new record a strict typed decoder and identity/revision
checks. Keep control records metadata-only: child records already retain captured
input through `operation`, `request-dependencies` and `backfill`; prepared groups,
validation and readbacks retain the outputs and original executions
(`inventory.rs:1324–1413`). The existing admission helper binds its retained roots
to acquired owner protections in one commit (`src/engine/admission.rs:48–71`).
Reuse it for each child. Do not copy the full object closure into the parent or
each fence generation, and do not add an independent parent payload promise.

A parent completion can name the exact selected child as metadata evidence. It
must resolve that child's ordinary result and availability status, not bypass
retirement through a copied release or independently protected alias. Existing
succession/input-progress records demonstrate metadata-only references that do
not promise perpetual payload availability (`inventory.rs:1438–1497`).

### F4 — BLOCKER: parent termination must cover child capabilities and cleanup

`abandon` only accepts `operation/{id}` and writes abandonment for that exact
operation (`src/engine/retention.rs:138–206`). Completion checks consult only the
child operation's abandonment and engine epoch (`src/engine.rs:1731–1746`).
Operation recovery retirement requires that operation to have a selected result
or a qualified abandonment (`src/engine/retention/recovery.rs:70–125`). A parent
outside `OperationRequest` therefore needs explicit termination support. Leaving
superseded children perpetually active also makes their captured bases and
attempt directories impossible to retire through the existing recovery API.

**RESHAPE:** define parent abandonment and serialize it with child admission and
final selection under the dataset lock. Every child completion/selection checks
the parent's live capability. On parent abandonment or success, terminalize owned
unselected children with validated ordinary abandonment records, or provide an
equally strict terminal receipt recognized by recovery retirement. Ordinary
abandonment is the smaller reuse option: existing phase/readback retention remains
independent. Do not abandon unrelated source admissions to force convergence.

When superseding a round, a controller-owned child's future selection can end
without deleting its shared file completions. An expired fence alone must not
discard completed work or permanently terminate a reusable child. Close only the
exact owned fence generation; an old attempt must never clear a successor's fence.

### F5 — CONCERN: nest private helpers, not public owned services

`admit_work` holds shared maintenance admission for its private worker. Acquiring
another shared admission while a writer waits can deadlock with that writer;
the stream path explicitly shares admission to avoid it
(`src/engine/worker.rs:111–134,159–178`).

**KEEP/RESHAPE:** the parent takes one owned admission, calls private child
preparation/composition helpers directly, and retains the existing budget. Do not
call public `backfill`, `plan_backfill` or other admitted services from inside it.
Keep large native futures boxed as the existing backfill does
(`src/engine/backfill.rs:480–482`).

## Concrete API and data shape

The following names are illustrative; the ownership and identity boundaries are
the proposed design. Public input needs a stable parent ID, initial exact source
version, writer definition, material group definition, finite catch-up rounds,
and a finite fence budget. A single `continuous_backfill(spec)` admits or resumes
that immutable intent. `plan_continuous_backfill` returns bounded metadata: latest
child, captured source, pending source IDs, current fence state and completion.
The same service backs thin CLI/HTTP commands beside the existing backfill commands
(`src/main.rs:223–225,667–674`; `src/http.rs:45–46,674–684`).

| Record | Minimal contents | Payload protection |
| --- | --- | --- |
| `continuous/{parent}` | Immutable request/digest; initial accepted `VersionRef`; expected target identity; initial source-field ID bindings; eligibility and epoch | None beyond metadata evidence; actual children own captured source |
| `continuous-child/{child}` | Dataset, parent/digest, stable round, exact `BackfillSpec` digest and capture identity | Existing child admission roots |
| `continuous-state/{parent}` | Current round/child and bounded progress; no repeated file inventory | None |
| `source-fence` | Owner parent, unique generation, host instance, monotonic deadline, open/closed state | None |
| `continuous-result/{parent}` | Intent digest, exact selected child `VersionRef`, consumed fence identity if any | Resolve existing selected child; no duplicate closure |
| `continuous-abandoned/{parent}` | Exact intent revision/digest and explicit reason | None; child work remains independently retained |

An exact winning child can remain the selected `ReleasePin.id` and
`DatasetPin.request = Backfill(child)`. This preserves existing history,
admission-proof and retention consumers without adding a new `OperationRequest`
variant across the engine. The parent completion identifies that child. Retrying
the same parent request returns the same child result. Changed user intent with
the same parent ID refuses. Reusing the same captured source plus admission
boundary need not allocate another child merely because a request was retried.

Avoid unbounded arrays of all rounds in a frequently rewritten parent record.
Store one compact immutable child association per genuine capture and a small
mutable cursor. Catalog commits already rewrite current record state, so control
metadata growth still needs measurement; this is not an O(1) catalog claim.

## Minimal code seams and flow

1. **Separate preparation from selection.** Current `prepare_backfill` combines
   admission and expensive group creation (`backfill.rs:505–700`), composed
   readback (`:701–862`), and final selection (`:863–892`). Extract private admitted
   preparation returning exact operation/capture/base plus prepared group and
   protections. Retain the same `prepare_group`, file completion keys, validation,
   origins and readback machinery. A real prepare-only path is required; calling
   public backfill with an injected failure is not a control API.
2. **Capture and catch up.** The parent admits an exact child against the current
   accepted source and computes outside dataset locks. Source movement creates
   another exact child within the finite round allowance. Existing file keys
   recover unchanged completed files; never mutate a prior child's `base`,
   `Capture`, fingerprint or readback. Complete endpoint preparation safely covers
   insertions, deletions and replacements; exact-file reuse is conservative and
   does not by itself close semantic-reuse requirements.
3. **Validate target lineage.** Preserve the initial target expectation through
   every accepted selection since the initial version. Source updates can refresh
   a group's binding while preserving its material definition. A separate backfill
   replacing that group, including A→B→A or a same-definition explicit replacement,
   must refuse. An initially absent group must remain absent until this cutover.
   Follow retained succession and narrow result/request headers, not historical
   payloads. Stable source field IDs retain authored meaning across renames; do
   not repeatedly parse a name against whichever field now owns that name.
4. **Acquire the bounded fence only when needed.** Give `Host` a fresh instance ID
   and `Instant` origin (`worker.rs:13–24`). A persisted fence deadline is an
   elapsed offset valid only for that exact host instance. A reopen expires old
   tokens, while existing workspace custody prevents live old workers from
   overlapping (`worker.rs:37–64`). Never compare an old instance's offset to a new
   origin. Fence reacquisition creates a new generation; all owner updates require
   exact token equality.
5. **Fence only new source acceptance.** In `engine.rs:997–1035`, reconcile existing
   result/request first. For a new Build/Update/Refresh, check the fence before
   `protect_admission` and again after its unlocked work before registration
   (`:1036–1093,1098–1150`). Existing admitted operations retain their normal retry
   and completion paths. A fence refusal happens before input admission; it does
   not silently queue or accept the request. Schema/evidence composition is not
   source admission, but its changed definitions/targets still fence cutover.
6. **Drain without inventing input ownership.** Use
   `pending_source_operations` against the captured locked view. It classifies
   all source admissions and accepts only matching selected succession or exact
   abandonment, including requests below selected boundary
   (`src/engine/history/admissions.rs:48–189`). Release the dataset lock while
   waiting so existing workers can finish. Use bounded notification/poll waits;
   avoid repeated hot O(history) scans. A stale accepted request that cannot finish
   causes bounded refusal with its ID. Do not rewrite or abandon that request.
7. **Compose and select the exact final child.** Once drained, capture the final
   source and prepare the child using retained work. Compose compatible sibling
   selections through the existing loop. For related data, hold dependency locks
   and call `guard_inputs(..., RefreshPolicy::Latest)`; the current generic
   `select` uses `Captured` (`history.rs:143–159`). Pass the resulting progress to
   the shared atomic selection. Recheck all cutover guards against the selecting
   view and write child result, succession, parent result and closed fence together.
8. **Exit and resume.** Success resolves before expired-fence checks. Failure ends
   only the owned fence and preserves prepared/readback records. After process
   loss, host mismatch permits new admissions immediately; retry uses retained
   child work and acquires a fresh fence only when required. A deadline check must
   occur after dependency/history work and immediately before selection. The
   deadline cannot bound a stalled native commit while a dataset mutex is held;
   measure that separate foreground-delay component and state the supported limit.

## Lineage, invariants and user value

| Prior artifact | Relationship |
| --- | --- |
| `docs/requirements/03-schema-backfill.md:39–47,53–59` | Candidate retention, complete catch-up, strict latest cutover, bounded admission fence, focused API |
| `docs/requirements/03-schema-backfill.md:15–19,33` | Stable field meaning, material reuse dependencies, original execution and evidence |
| `docs/reviews/continuous-backfill-design-consistency-2026-10-10.md` | Refines fixed-capture service with parent intent, complete admission accounting and target lineage |
| `docs/reviews/continuous-backfill-design-resources-2026-10-10.md` | Requires bounded rounds, metadata-only control, measured foreground delay and storage growth |
| `src/engine/history/admissions.rs:48–189` | Supplies the now-implemented complete source-admission proof |

The selected native combination remains the single result authority. A fence is
only temporary admission policy; it cannot become a second result head. Child
phases remain independently retained. Parent control records add no second copy
of the source/group object graph. New source input receives acceptance or an
explicit retryable refusal; accepted input never disappears. Each final result
binds one exact source and material group definition.

The user value is one resumable field-addition/replacement request while ingestion
continues. Manual exact-backfill retries already preserve safety but make callers
implement capture, catch-up, lineage checks and pause limits themselves. Removing
the parent first loses that single-intent recovery. Removing the fence preserves
safety but permits starvation. Existing services subsume data preparation and
retention, not this missing control behavior. A generic workflow engine or
separate queue adds unnecessary ownership and recovery seams.

## Qualification that would falsify the design

- Concurrent attempted ingestion with drainable accepted work produces the exact
  combined values, membership, evidence and definition for the final captured
  source; unchanged file generations remain unchanged.
- An earlier unresolved request below a later selected boundary prevents cutover,
  is named in bounded refusal, retains its input/work, and does not leave admission
  blocked after fence expiry or explicit close.
- A public call with a known child ID cannot select it. Parent abandonment racing
  child creation/completion stops further child capability without deleting
  completed file work. Superseded children can later retire their recovery.
- Lose responses or reopen after child admission, file acknowledgment, processing,
  readback, fence acquisition and final selection. A committed parent returns its
  original selected child even when its old fence expired or a successor exists.
- An old fence owner cannot close a new owner's fence or select under it. New
  source acceptance racing expiry and final commit has one unambiguous catalog
  ordering. Slow metadata/commit paths report actual foreground delay.
- Initial target absence, concurrent sibling group, same-target replacement,
  same-definition replacement, A→B→A, field rename/name reuse, eligibility change,
  dependency correction and upstream head movement each behave as declared.
- Retention inspection and restore-point creation succeed with active, failed,
  abandoned and completed parent records. Restored old fence tokens do not block
  new source requests. Explicit child availability retirement is not bypassed by
  parent completion. Existing foreign-owner protection remains exact.

These are required externally observable cases, not a suggestion to repeatedly
scan every payload on ordinary reads. Runtime/resource qualification must hold
update size fixed while source-file and history dimensions grow, accounting for
all round outputs, metadata and old/new coexistence. Confidence is high in the
identified source seams and medium in the proposed full control behavior until
these tests and measured fence limits exist.

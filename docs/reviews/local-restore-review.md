# Local restore and shared retention review

Current correction-delta verdict: **APPROVE for the scoped local implementation**,
with the qualification and resource warnings in the correction review below.
This is static approval; the parent task owns test results and the repository gate.

Verdict: **REQUEST CHANGES** on the initial uncommitted candidate reviewed on
2026-10-09 against `d57042e00a43b821b9730ad688d94ad6d2e011f4`.

This is an independent static review using the semi-formal code-review and Rust
skills. No Cargo command, test, formatter, or implementation change was run by
this reviewer. The parent task owns execution. The scope is the local Unix,
same-workspace restore implementation, its retention integration and transports;
this does not qualify remote storage or complete R6/Q6/Q7. Line references below
identify the initial candidate, before corrections made in response to this review.

## Findings

### F1 — BLOCKER: the saved catalog can contain native registrations absent from its retained closure

Category: correctness. Location: `src/engine/retention/inventory.rs:194-221`.

`collect_inventory` captures the native snapshot files of each `lifecycle` table,
then expands active application roots. It skips explicitly retired generations
at `inventory.rs:322-328`. However, `create_restore_point_admitted` copies the
entire SQLite catalog at `src/engine/restore.rs:79`. Retiring an old result,
phases and recovery does not detach its native data table; detachment happens
only during physical removal (`src/engine/retention/removal.rs:716-728`). Therefore
the saved catalog can still contain a native table whose metadata and manifests
are absent from `RestorePoint.objects`.

Concrete sequence: create independent old/new results; retire all old result,
phase and operation roots using the pattern at
`tests/physical_removal_process.rs:41-77`; create a restore point; normally remove
the eligible old table metadata/manifests; restore the point. Recovery copies the
saved database and verifies only its listed lifecycle boundaries
(`src/engine/restore.rs:265-279`), so it can select a catalog with a dangling native
registration. Subsequent removal previews enumerate every registration and
unconditionally load its table and snapshot files
(`src/engine/retention/removal.rs:890-907`), failing before they reach an unrelated
candidate. This also violates the requirement that restored metadata must not
promise deleted objects (`docs/requirements/06-maintenance-retention.md:31-33`).

Retain every native registration's complete supported object closure, or prune
unpromised registrations from the saved/fresh catalog through a validated exact
procedure. Test the sequence above through the normal removal API, including an
unrelated removal preview after restore. The existing manual-unlink fixture only
removes a file already present in the point's closure and does not cover this gap.

### F2 — BLOCKER: failed capture retries leave permanent protected attempts

Category: correctness/resource use. Location: `src/engine/restore.rs:56-79`.

Capture reconciles only a completed `restore-point/<id>` record
(`src/engine/restore.rs:43-52`). Otherwise it allocates a fresh random attempt and
commits an active `Protection` before dependency collection. Any later error
leaves this reservation active. A simple repeatable example is an unresolved
removal: the later traversal returns `restore_removal_pending`
(`src/engine/retention/inventory.rs:228-241`) after every new reservation. I/O,
capacity, process loss, or final-commit failure can also leave catalog images and
object links after partial capture (`src/engine/restore.rs:79-158`). A retry does
not discover or reuse them; it starts another UUID.

These objects remain protected by the prefix record
(`src/engine/retention/inventory.rs:1288-1305`). Restore-point retirement requires
the completed point record (`src/engine/retention/retirement.rs:405-417`), while
ordinary operation retirement requires an `operation/<id>` admission that restore
capture never creates (`src/engine/retention/recovery.rs:29-34,180-181`). Partial
artifacts also lack the `restore-materialized` creation evidence written only at
final publication (`src/engine/restore.rs:145-158`), so normal physical removal
cannot qualify them (`src/engine/retention/removal.rs:234-252`). Repeated failures
thus grow retained metadata, and failures after materialization grow retained
disk, without a supported completion or cleanup path.

Give capture a durable attempt identity keyed by dataset/point and a recoverable
state, reuse verified completed work, and provide exact retirement/cleanup for
abandoned partial work. Run preflight refusals before durable mutation where
possible. Add failure coverage after reservation, catalog image creation, object
retention, manifest publication, and uncertain final commit. Assert stable
attempt identity and bounded retained growth across retries.

### F3 — WARNING: successive active points recursively duplicate earlier backup inventories

Category: performance/resource use. Locations:
`src/engine/retention/inventory.rs:1353-1361` and `src/engine/restore.rs:91-113`.

Every active older point contributes each backup path to the next capture's
object collection. Capture creates another uniquely named backup for every one
of those paths. Consequently, the new point's object count includes the sum of
all previous active points' backup counts, even when logical data is unchanged.
Earlier backups are distinct paths, so the inventory's path deduplication
(`src/engine/retention/inventory.rs:731-748`) cannot collapse them. This gives
exponential link/manifest/reference growth as active points accumulate; hard
links save payload blocks but do not save these entries. On copy fallback it
also repeats physical payload copies. Metadata caps eventually refuse admission,
and F2 makes those refusals leave additional reservations.

Reuse already retained immutable backup identities through a shared, explicitly
owned backup store or another nonrecursive representation. Preserve all promised
older points. Qualify increasing active point count under Q7, reporting metadata,
links, copied bytes and retained storage; no timing claim is made by this static
review. An intentional small support limit must be explicit and checked before
mutation, rather than emerging from exponential metadata growth.

### F4 — WARNING: destructive-actor and partial-restore qualification remains incomplete

Category: test coverage. Location: `tests/restore.rs:292-315,397-472`.

The independent-process test pauses a writer before `Engine::run`; it proves a
live process holding the host lock prevents restore. It does not pause a deleter
after durable reservation or an uncertain unlink, then deliver delayed effects.
There are no restore-specific process-loss hooks covering missing-original
restoration, fresh-catalog mutation, or selector publication. The shared host
custody trace is consistent with local exclusion, but the stronger Q6 destructive
boundary remains untested (`docs/requirements/07-operations-qualification.md:129-131`).

Add a delayed/removal worker test and capture/restore interruption tests. Keep the
current support claim local; provider write fencing remains separate work.

## Patch summary

The patch adds explicit restore artifacts and references, immutable local backup
files, offline fresh-catalog recovery and a bounded catalog selector. Capture
uses exclusive maintenance; restore acquires host ownership before reading the
point. The shared retention traversal protects active restore backups and stops
doing so after exact-generation retirement. Creation records remain as evidence
for eventual explicit file removal.

The refactor reuses one root traversal for inspection and capture, the existing
worker lifetime/maintenance guard, native SQLite `VACUUM INTO`, and existing
retirement/removal logic. This reduces duplicated lifecycle rules. The object
closure and failed-attempt defects above prevent accepting the current behavior;
no measured speed or memory improvement is claimed.

## Function trace

| Function | Evidence | Input → output | Verified behavior |
| --- | --- | --- | --- |
| `create_restore_point` | `src/engine/restore.rs:25` | dataset/point → reference | Reserves request metadata and uses exclusive maintenance. |
| `create_restore_point_admitted` | `src/engine/restore.rs:35` | admitted identities → reference | Reconciles completed point; reserves attempt, collects closure, backs up DB/files, publishes point and creation evidence. Failure gaps are F1/F2. |
| `restore_dependencies` | `src/engine/retention/inventory.rs:136` | owner → boundaries/objects | Bounded shared traversal; hashes only required paths without retained identities. |
| `collect_inventory` | `src/engine/retention/inventory.rs:171` | collector/mode → boundaries | Validates active lifecycle roots, removal state and owner claims; native coverage gap is F1. |
| `Inventory::record` / `expand` | `src/engine/retention/inventory.rs:932,1321` | typed row/manifest → references | Active point protects manifest, saved catalog and backup paths; creation evidence is not an availability promise. |
| `plan_restore_point_retirement` | `src/engine/retention/retirement.rs:373` | exact names/reason → plan | Requires nonempty unique names, existing nonretired generations and native boundary. |
| `retire_restore_points` / `retire_retained` | `src/engine/retention/retirement.rs:439,462` | exact plan → retirement | Scope check, exclusive maintenance, fresh generation validation, durable markers, uncertain-commit reconciliation. |
| `visit_creation` | `src/engine/retention/owners.rs:795` | owner rows → observed identities | Includes restore materialization manifest/catalog/backups for exact object qualification. |
| `restore_local` | `src/engine/restore.rs:174` | root/reference/limits/import flag → fresh catalog result | Unix-only host acquisition, caller-independent task custody. |
| `restore_local_owned` | `src/engine/restore.rs:202` | owned request → selected catalog | Validates point/current authority, verifies all known bytes before restoring paths, creates fresh DB, checks boundaries, registers point, atomically selects. |
| `restore_reference` | `src/engine/restore.rs:355` | view/key → optional reference | Rejects retired points and wrong lifecycle owner. |
| `check_current_authority` | `src/engine/restore.rs:398` | reference/import policy → acceptance | Matching registration required normally; explicit import tolerates unavailable authority, never an observed retired/conflicting record. |
| `vacuum_catalog` | `src/engine/restore.rs:441` | source/new path → durable DB | Uses read-only source, available-space check, `VACUUM INTO`, fsync and SQLite integrity check. |
| `retain_file` | `src/engine/restore.rs:536` | immutable object/new path → link/copy flag | Size check, hard link, bounded verified-copy fallback, file/directory sync; no payload buffering. |
| `read_point` / `validate_point` | `src/restore.rs:129,148` | reference → validated point | Bounds manifest, checks its digest, identity, canonical workspace paths, distinct objects, accounting and ordered boundaries. |
| `atomic_json` / `select_catalog` | `src/restore.rs:264,109` | value/path → durable publication | New point uses exclusive hard-link publication; selector uses rename plus parent sync. |
| `catalog_path` / `local_catalog` | `src/restore.rs:93`, `src/storage.rs:54` | workspace → selected DB/catalog | Validates bounded selector and owned generation path before startup. |
| CLI / HTTP handlers | `src/main.rs:282,439,597`, `src/http.rs:213,229,429` | typed metadata → same engine methods | CLI restore runs offline before opening an Engine; HTTP exposes creation/retirement, which use live-engine guards. |

## Data flow and invariants

1. A restore point binds its owner, point ID, canonical workspace, exact catalog
   object and each original/backup object pair. Manifest integrity is checked on
   read (`src/restore.rs:129-237`). This state escapes through a public reference
   and retained lifecycle row. Original paths remain selected by native metadata;
   backup paths carry the continued restore promise.
2. Capture's temporary protection becomes retired in the same commit that
   publishes the active point and materialization evidence
   (`src/engine/restore.rs:145-158`). The missing intermediate state is F2.
3. Creation/retirement uses the engine's exclusive maintenance write guard
   (`src/engine/worker.rs:136-156`). Streams retain the read guard and Engine
   (`worker.rs:175-197`); the Engine retains `Host`; `Host` retains the OS lock
   (`worker.rs:13-23`, `src/budget.rs:168-181`). Restore takes that same host lock
   (`src/engine/restore.rs:187`). Abnormal private-worker termination quarantines
   custody until process exit (`worker.rs:56-64`).
4. Recovery verifies all enumerated backup files and surviving original files
   before recreating missing originals (`src/engine/restore.rs:225-259`). New
   files use exclusive publication; errors before selector publication leave the
   old selected catalog in place (`restore.rs:263-334`). The completeness of the
   enumeration, rather than the individual identity checks, is F1.
5. Retiring a point changes admission/protection, not immediate file presence.
   Fresh restore checks current retirement before effects
   (`src/engine/restore.rs:221,355-438`). Backup removal remains a separate exact
   plan. Previously captured points can legitimately preserve older point bytes.

## Test behavior and concrete edge cases

These are assertions and static path assessments, not executed results.

| Test | Evidence | Assessment |
| --- | --- | --- |
| `restores_exact_catalog_and_missing_originals_from_retained_copies` | `tests/restore.rs:84` | Exercises same-ID completed retry, retention blockers, missing original restoration, exact duplicate/null/Unicode values, rollback of later result and preservation of future physical bytes. Expected behavior follows known closure path; does not cover F1. |
| `corruption_and_foreign_workspace_refuse_before_catalog_selection` | `tests/restore.rs:175` | Replaces a backup entry and expects digest refusal, then foreign-workspace refusal; checks selector absent. Matches pre-effect checks. |
| `live_engine_and_held_stream_keep_the_storage_fence` | `tests/restore.rs:224` | Checks busy refusal for Engine and retained stream, then success after stream drop. Matches shared Host ownership. |
| `catalog_loss_requires_explicit_artifact_import` | `tests/restore.rs:259` | Renames original DB, checks ordinary refusal and explicit-import recovery. Does not test a lost restored-generation DB. |
| `explicit_retirement_ends_restore_admission_and_preserves_creation_evidence` | `tests/restore.rs:319` | Checks blockers end, bytes/evidence remain, same plan reconciles, create and ordinary/explicit restore refuse retirement. Does not physically remove retired backup objects. |
| `independent_delayed_writer_must_finish_before_restore_can_select` | `tests/restore.rs:397` | Cross-process lock exclusion with pre-run writer pause. Expected host-lock behavior; destructive boundary gap is F4. |
| `http_exports_compaction_and_restore_points_use_the_shared_services` | `tests/exports.rs:466` | HTTP creation returns the same reference as engine retry. Restore-retirement HTTP success/refusal paths are not covered. |
| `cli_compaction_and_offline_restore_share_the_application_paths` | `tests/exports.rs:569` | CLI creates point, restores offline, and reads original selection. Restore-retirement CLI paths are not covered. |

Additional concrete uncovered cases are partial capture retry (F2), multiple
active points (F3), pending-removal capture refusal, completed-removal capture,
post-restore normal removal, interrupted selector publication, and repeated
offline restore after lost response. Offline restore generates another fresh DB
on every invocation (`src/engine/restore.rs:261-265`), so restore retry reuse and
retained catalog growth also need explicit qualification.

## Review reasoning

- H1: reusing the cleanup traversal yields a complete catalog recovery closure.
  Refuted: active application roots and native catalog registrations have different
  lifetimes; F1 traces the missing edge.
- H2: stable point identity makes failure retries bounded. Refined: completed
  retries reconcile, but pre-completion attempts do not; F2.
- H3: shared host ownership excludes old supported local actors. Supported by the
  worker/stream/restore call trace and writer fixture; destructive effect-boundary
  qualification remains open under F4.
- H4: hard links keep repeated checkpoint growth small. Refuted for metadata and
  reference count: path-level backup recursion creates F3. Payload block savings
  alone do not establish bounded storage or affordable scaling.

## Conclusion

**VERDICT: REQUEST CHANGES.** The patch supplies useful local recovery and exact
retirement primitives, but the initial candidate can select an incomplete native
catalog and cannot safely reconcile failed capture attempts. Coverage of changed
paths is **INSUFFICIENT** for those failure cases. Confidence is **HIGH** in the
static traces; execution and real-provider behavior were outside this review.

## Correction delta — 2026-10-09

**VERDICT: APPROVE for the scoped local implementation.** The revised candidate
addresses the concrete mechanisms behind F1, F2 and F3. No additional blocking
finding was identified in this independent static pass. Existing F4 remains a
qualification warning; this verdict does not establish full Q6/Q7 acceptance.
The parent was about to execute the correction tests when this review began.
This reviewer did not run builds, formatting, tests, or implementation edits.

### Resolved findings and evidence

| Finding | Correction and checked trace | Assessment |
| --- | --- | --- |
| F1: native registrations missing from the retained closure | `collect_inventory` now enumerates every table in each owned namespace, captures each native head boundary and visits its snapshot files (`src/engine/retention/inventory.rs:219-248`). The existing recovery loop checks every supplied boundary before selecting the fresh catalog (`src/engine/restore.rs:389-400` in the formatted candidate). | Closed statically. The restore image and native registration closure now use the same catalog view under exclusive maintenance. |
| F2: failed retries create permanently growing attempts | Dataset/point deterministically select one attempt; the existing exact reservation is reused (`src/engine/restore.rs:85-117`). A published descriptor reconciles from retained bytes (`restore.rs:123-165`). An unpublished attempt validates its complete flat file set before exact cleanup (`restore.rs:668-725`), then rebuilds in the same directory. | Original repeated-UUID growth closed. Incomplete attempts that are never resumed retain their own prefix; there is still no separate physical-abandon command for it. This is an explicit conservative limit, not cleanup completion. |
| F3: each checkpoint clones previous checkpoint trees | Capture indexes existing backup identities by digest and length, preserving the chosen `ObjectRef`, and only creates a file when no retained backup has that identity (`src/engine/restore.rs:209-249`). `materialized` records only uniquely owned files created in the current directory (`restore.rs:513-527`). | Recursive backup-file duplication closed. Reference/manifest growth remains proportional to the collected native and retained history, and still needs Q7 measurement. |

### Shared dependency and retirement trace

The revised `RestoreReuse` row holds the exact previously retained backup
identities required while capture is unfinished (`src/restore.rs:66-73`). Capture
writes it before the database image and refreshes the captured owner boundary to
include that commit (`src/engine/restore.rs:174-196,558-592`). The shared inventory
dispatches that row as direct object roots
(`src/engine/retention/inventory.rs:969-978`). This protects a foreign owner's
backup even after that owner's older restore-point promise retires; ownership is
preserved rather than rewritten to the newer checkpoint owner.

A completed-descriptor retry first checks that each external backup remains
listed in its saved reuse row, then verifies the catalog and all backup byte
identities before publishing (`src/engine/restore.rs:140-163`). The publication
commit atomically writes the active point and own-file creation evidence, clears
the temporary reuse list, and retires the attempt's prefix protection
(`restore.rs:595-623`). The active point then protects those same backups through
the common manifest expansion (`inventory.rs:1397-1405`).

`restore-reuse/` participates in exact-generation `PhaseRecovery` retirement
(`src/engine/retention/retirement.rs:19-34`). A retired nonempty shared-preparation
row refuses a retry before cleanup or publication (`src/engine/restore.rs:530-555`).
Retiring that temporary row does not remove the attempt's own files and does not
retire a separately advertised restore point. Published points retain an empty
reuse row, so the temporary preparation cannot silently keep foreign backups
alive after their actual point promises end.

The relaxed descriptor validation permits repeated identical backup paths while
still requiring one original mapping per path and refusing conflicting identity
claims (`src/restore.rs:195-241`). Backup paths remain within their declared
owner's narrow restore namespace (`restore.rs:265-297`). Recovery verification
uses Unix device/inode identity to hash shared immutable bytes once, compares
every reused identity claim, and checks original paths separately when present
(`src/engine/restore.rs:627-665`). This removes a concrete repeated digest pass;
it is not a measured throughput or memory claim.

### Additional function traces

| Function | Evidence | Inputs → outputs | Failure/property checked |
| --- | --- | --- | --- |
| `restore_reuse` | `src/engine/restore.rs:530` | lifecycle/view/point → saved shared roots | Exact owner/ID validation; retired nonempty preparation refuses before effects. |
| `refresh_capture_owner` | `src/engine/restore.rs:558` | new owner view + collected closure → updated boundary/files | Adds native files from the reuse commit before taking the catalog image; limits metadata growth. |
| `publish_point` | `src/engine/restore.rs:595` | completed descriptor + reservation → lifecycle commit | Active point, own creation evidence and cleared temporary protections change atomically. |
| `verify_point_files` | `src/engine/restore.rs:627` | point + original-check policy → verification count | Rejects missing/changed backups, symlinks and conflicting inode identities; avoids repeat hashing of hard links. |
| `clear_incomplete_attempt` | `src/engine/restore.rs:668` | owned flat attempt + metadata cap → exact removals | Refuses a completed marker, unknown filename, directory, symlink or over-limit path inventory before first unlink. |

### Added test assertions reviewed

- `failed_capture_retries_reuse_one_reservation_and_bounded_partial_files`
  (`tests/restore.rs:51`) injects failures at reservation, reuse, catalog and
  partial-object boundaries, checks stable records and file count over repeated
  retries, and checks that eventual completion uses the same directory.
- `completed_capture_reconciles_manifest_and_commit_response_without_recopying`
  (`tests/restore.rs:130`) covers descriptor publication and post-commit response
  failure. It checks preserved filenames, lengths and modification times on
  replay. These are logical injected errors, not process-kill durability proof.
- `repeated_points_reuse_source_backups_and_do_not_clone_backup_trees`
  (`tests/restore.rs:183`) checks that later foreign-owned points use the exact
  earlier backup identity, own creation evidence remains local, a fixture growth
  bound holds, and retirement of the first point preserves shared protection.
- `interrupted_shared_capture_protects_reuse_after_older_point_retires`
  (`tests/restore.rs:258`) checks foreign backup protection between descriptor
  publication and point registration, then verifies temporary reuse clears.
- `explicitly_retired_shared_preparation_refuses_same_point_retry`
  (`tests/restore.rs:312`) checks exact-generation retirement reconciliation,
  repeated refusal without further catalog changes, and absence of an active
  point after that refusal.
- `restore_reconstructs_an_unpromised_native_registration`
  (`tests/restore_catalog.rs:19`) captures an otherwise unreferenced native head,
  detaches/unlinks it, restores and loads the exact metadata, and performs another
  complete capture. It directly exercises F1's missing native-head edge. It uses
  catalog detachment and unlink as fixture actions; full normal-removal and
  delayed-deleter integration remains F4 work.
- `verification_hashes_shared_inode_once_and_refuses_conflicting_claims` and
  `incomplete_cleanup_refuses_symlinks_and_completed_artifacts_without_deleting`
  (`src/engine/restore.rs:914,950`) check the inode reuse count and fail-closed
  cleanup ordering. These assertions match their helper traces.

These tests have plausible assertion paths under the correction; this reviewer
makes no claim that they passed. Coverage is adequate to exercise the reviewed
correction mechanisms, subject to execution by the parent. Full local crash,
destructive-worker and scaling qualification remains incomplete.

### Remaining warnings

- F4 remains open: the independent actor fixture pauses an ordinary writer;
  capture errors are injected returns. Recovery process loss around restored
  originals/fresh-catalog mutation/selector publication and delayed destructive
  effects are not established by this delta. Remote qualification remains out of
  scope.
- Completed-descriptor retry checks each mapping with `saved.objects.contains`
  (`src/engine/restore.rs:149-152` in the reviewed candidate), producing a product
  of mapping count and shared-backup count in the worst case. A single path map
  with exact identity comparison would remove the repeated linear lookup. This
  is a bounded metadata-path resource warning, not a safety blocker or measured
  performance failure.
- A permanently abandoned incomplete attempt keeps its own directory protected.
  Same-ID retries no longer accumulate new prefixes, but this is not a general
  abandoned-backup reclamation capability. The narrower shared-preparation
  retirement is explicitly distinct.
- Repeated offline restore still creates fresh catalog generations; restore
  invocation reconciliation and catalog-generation disposal remain separate
  qualification/retention work (`src/engine/restore.rs:380-386`).
- At review time, `README.md:240-241,743-744` still described compaction and restore
  as disabled, conflicting with the new commands at `README.md:35-50`. Those
  obsolete statements should be corrected before presenting support status.

Confidence: **HIGH** in the local static correction traces; no execution or
provider guarantees are inferred from that confidence.

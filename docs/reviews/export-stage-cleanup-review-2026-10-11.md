# Remote export stage cleanup review

VERDICT: APPROVE (static only). Reviewed final `/tmp/docspec-export-stage-cleanup-v6.patch`, SHA256 `f8312184cc3e21b625364d726748e4523647216218acb0463365ec0d23de253a`. Exact digest independently confirmed. Previous v4 hold is resolved by the v5 grant guard, preserved in v6. All changed production/test hunks read, with current owner acquisition/admission/local removal callers. No builds/provider/shared edits.

## Verified boundaries

CompletedStage proves exact original attempt/protection/native revisions, staged immutable identity and remote materialized+validation atomic revision. Stage root ending leaves original records unchanged and remote prefix protected. Local default RemovalScope is serde-defaulted/skipped, preserving existing JSON/material identities. Scope plan accepts one exact original stage only; configured current host/root and remoteURI scope are effect-time checks, while saved-intent validity remains portable. Existing owned regular-file identity/hash/symlink refusal/unlink/parent synchronization and removal reservations/outcomes serve execution. Maintenance excludes this host's ordinary readers/workers. Finished transfer/retry/fresh-host flat read use remote identity rather than deleted host copy. Partials/losing stages remain outside this exact creation proof and are not scanned/unlinked.

Generic remote object/catalog removal stays refused. ExportHostStage bypasses generic retained S3 owner-path/single-prefix check only after its stronger original two-prefix proof. Public qualified_object/acquire_owner still reject host stage as a managed object (`owners.rs:615–661,663–700,729–763`). Local composition also rejects two-prefix reservation. Remote local input admission treats such a path as an external locator and copies it into independently protected retained input (`admission.rs:259–302`, `locations.rs:117–165`); it grants no durable reference to original host bytes.

## Required stronger-root check

The new scope bypasses the full retention graph and constructs blocker=None. Existing valid `owner-protection/` metadata must still win, even if current public acquisition cannot newly qualify this stage. Before publishing intent and before each unresolved effect/resume, scan only owner rows for exact path, validate grants and exact native-generation retirement markers, and refuse active matching grant (conflicting identity is corruption). This adds no remote namespace enumeration or broad deletion authority. Add a concrete active-grant refusal case and retired-grant success if practical. The original reservation ending must not silently end a foreign consumer promise.

Fixtures exercise real maintained Local Parquet materialization and conditional S3 transfer plus scoped cleanup/retry/cold configured flat read, rather than remote native source materialization. They cover incomplete transfer refusal, intent/unlink/lost-outcome recovery, exact changed identity/child/bytes and symlink refusal. Runtime remains parent-owned. No full remote lifecycle, orphan cleanup, resource gate or total disk-reclaim claim follows.

## Final v6 closure

`removal/export_stage.rs:55–70` scans only the existing owner-protection rows, admits each decoded grant, validates exact native identity, and refuses an active matching stage-path grant. Conflicting path identity refuses. Exact retirement marker and receipt validation uses the existing `record_retired` path. Calls at plan (`:93`), new intent (`:133`) and unfinished saved effect (`removal.rs:637`) close both preview-to-intent and resumed-effect gaps; immutable saved validity remains portable and finished outcomes do not invent new effects.

The conservative replay fixture (`stage_removal_tests.rs:192–198`, test helper `export_stage.rs:152–184`) retains a valid typed grant after plan capture, proves execution and fresh plan refuse before any unlink, then ends only that exact native generation with the existing marker/receipt form and permits cleanup. Its explicit comment distinguishes legacy retained metadata from a newly supported public grant API. No new grant authority or remote catalog enumeration is added.

The sole v6 addition (`stage_removal_tests.rs:201–226`) opens actual configured Engine.read_export with a held GET, queues exact stage removal on the same composed Engine, checks the file remains while GET is held, then confirms removal also waits while the returned admitted stream exists. Dropping that stream permits cleanup. This matches `worker.rs:145–164,168–211`: maintenance takes the write guard, stream opening and lifetime retain the shared read guard without reacquisition. The fixture has bounded deadlines and does not claim remote source materialization or cross-host exclusion.

No actionable static finding remains in this frozen scope. Parent must run focused stage removal tests, local removal/retention regression tests and its full gate. Automatic/partial-stage cleanup, generic hosted remote deletion and total resource qualification remain outside this increment.

## Held-reader fixture correction and runtime

Independent static review approved `/tmp/docspec-stage-held-reader-fix.patch`
(SHA256 `4a3153b449591109ae2d2653d4a732fa4665623d205801534593ad2ef4bc1558`).
The original fixture returned the full body for a requested Parquet footer range.
The correction uses the existing exact URI/range responder with the same held-GET
signal; both maintenance assertions remain unchanged. No production reader changed.
`reuse-stage-focused-2026-10-11.log` retains the failed fixture and passing other
stage cases. `held-stage-reader-fixed-2026-10-11.log` passes the corrected case.

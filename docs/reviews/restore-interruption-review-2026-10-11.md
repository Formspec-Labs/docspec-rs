# Actual local restore interruption review

Artifact `/tmp/docspec-restore-interruption.patch`, SHA256 `1ef060f21ee52265d42a33affb50e159f41fbbe576fb26ebe72a2c2139639856`.

VERDICT: APPROVE statically. Coverage of changed paths: adequate for this bounded local process-interruption fixture. Runtime remains parent-owned.

Production delta consists solely of three test-util work_started calls. The first follows the existing admitted blocking exact-object verification/restoration loop. The second follows fresh native catalog boundary validation, restored point identity/recovery registration and file+parent sync. The third follows select_catalog's bounded atomic catalog-current.json publication. Normal builds gain no behavior, API, journal or cleanup authority.

restore_local acquires the existing workspace Host before spawning its owned runtime worker; the worker retains Host and Custody through the entire operation. The real child calls that service, rather than simulating an error return. Marker creation occurs within the existing pause implementation and has a 60-second worker bound; the parent has a 30-second reach deadline, checks child liveness, and observes workspace_busy while it is held. Actual Child::kill on Unix plus ExitStatusExt.signal()==Some(9) confirms SIGKILL, and wait releases the host fence. RAII kill+wait handles assertions/timeouts before explicit termination; each boundary has an isolated temporary workspace and marker. The standalone restore target and exact unqualified child test name match Cargo registration, and tests/restore.rs is Unix-only.

The preselection pointer absence assertion is valid for this new workspace: ordinary initial Engine opening uses its default catalog without writing a restore selector. Both preselection hooks precede the only select_catalog call. At the postselection hook, the selector has already been published. Comparing exact pointer bytes immediately before and after SIGKILL proves death itself did not modify selection; it does not prove selector idempotency on retry.

The fixture captures the selected source+real scalar group, later advances scalar selection, deletes one exact backed-up source Parquet object and the actual scalar Origins declaration, and verifies backup identities before deletion. All three hooks require those original object identities restored before death. Retry asserts restored_bytes==0 and sufficient reused_bytes, then fresh Engine opening observes the captured old selection, literal duplicate/null/Unicode source rows, literal scalar lengths [null,2,4,4], exact immutable Origins bytes, exact restore point identity and independent deep verification. This distinguishes real scalar origin evidence from absent acquisition receipts. No synthetic lifecycle records or byte identities are introduced.

Important limit: explicit retry, including after postselection lost response, intentionally makes another fresh catalog generation and performs the existing restore verification path. The test asserts neither the same generation nor unchanged pointer after retry, and makes no no-repeat/idempotent-selector claim. It does not qualify power loss, remote restore, whole-population cost, receipt restoration or orphan candidate cleanup. Restored object reuse is proved by byte accounting and exact object verification; payload re-verification remains existing restore behavior.

No actionable static finding. Relevant production trace: src/engine/restore.rs restore_local/restore_local_owned; src/restore.rs select_catalog; src/engine.rs work_started; Cargo.toml restore test target.

## Fixture type correction

Reviewed supplemental `/tmp/docspec-restore-fixture-fix.patch`, SHA256 `7a7749f7d09d39731e032e4e3764a0754c24d4ba8393d703e40ffb59e0fb23f0`. The original direct Int64Array downcast incorrectly assumed the physical return type of length(text). Maintained DataFusion character_length returns Int32 for Utf8. The supplement uses maintained Arrow cast to Int64 before the same nullable value iteration and exact literal oracle, matching tests/backfill.rs:91; it preserves values and nulls and introduces no production change. Initial review missed this physical-type detail; original artifact alone should not be considered fixture-ready.

Cumulative verdict: APPROVE statically for 1ef060f2 + 7a7749f7. Current shared Engine actually derives Debug at src/engine.rs:71, so the reported unwrap_err Debug concern is not present in the inspected tree. The supplemental artifact only contains the necessary type correction. Runtime remains parent-owned.

## Final supplement closure

Final supplemental artifact supersedes 7a7749f7: `/tmp/docspec-restore-fixture-fix.patch`, SHA256 `6450918c94b85f61af5653f6e34422274eea7631aeb2455f60e8171beda5cdde`. Its sole additional behavior opens a cold Engine immediately after SIGKILL and before explicit restore retry, then checks metadata selection equals the later original-catalog selection at both preselection boundaries and equals the restored captured selection at the postselection boundary. It drops the host before retry. This strengthens the actual authority oracle without asserting unchanged generation or pointer across retry or rereading unavailable payloads.

Actual failed evidence retained at `docs/reviews/restore-interruption-focused-2026-10-11.log`: the original fixture compiled and reached restore hooks, then failed at tests/restore.rs:1100 on the incorrect physical Int64Array downcast. No Engine Debug compile failure occurred. The maintained cast repair directly addresses that failure; parent rerun is ongoing.

Final cumulative static verdict: APPROVE original 1ef060f2 + final supplemental 6450918c. No actionable new finding.

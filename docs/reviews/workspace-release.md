# Workspace ownership release review

**No open findings.** The explicit unlock fixes the reproduced descriptor-lifetime
failure without adding retries or delaying workspace admission. Review scope is
`WorkspaceLock::drop` and its regression test; the reviewer performed static
analysis and inspected owner execution evidence, without running tests.

## Patch summary

Previously `WorkspaceLock` relied on its `File` closing to release ownership.
A duplicate can keep the same Unix lock alive. `Drop` now calls the maintained
`fs2::FileExt::unlock` before normal field destruction and logs an unlock error
(`src/budget.rs:144`). It does not remove the lock file or change acquisition.

Apple documents that descriptors duplicated through `dup` or `fork` reference
one lock and that an explicit unlock removes it. This supports the mechanism
exercised by the test. [Apple flock(2) manual](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/flock.2.html)

The original intermittent `workspace_busy` results occurred when scalar fixtures
dropped and immediately reopened an engine while other tests launched CLI workers.
An unmodified repeat passed. A transient inherited descriptor is a plausible
explanation, not a proven attribution of those particular failures. The deterministic
duplicate-descriptor failure below is independently reproduced.

## Function trace

| Function or owner | Location | Inputs → outputs and verified behavior |
| --- | --- | --- |
| `WorkspaceLock::acquire` | `src/budget.rs:126` | Workspace → private open lock file with exclusive nonblocking ownership; an existing live owner still refuses. |
| `WorkspaceLock::drop` | `src/budget.rs:145` | Sole ownership wrapper → explicit unlock, then ordinary file close; failure emits a warning. No retry loop or timer is introduced. |
| `Engine::open`, `with_catalog`, `compose` | `src/engine.rs:78`, `:89`, `:98` | Both engine creation paths acquire the lock and move it into `Inner::_workspace`. |
| `Engine` / `Inner` | `src/engine.rs:34`, `:39` | Engine clones share one `Arc<Inner>` and one non-Clone `WorkspaceLock`; an ordinary Engine clone drop cannot release another live clone's ownership. |
| `releasing_workspace_ownership_unlocks_inherited_descriptors` | `src/budget.rs:160` | Acquires owner, duplicates its private file, drops owner, reacquires while duplicate survives, then drops duplicate and checks replacement owner's exclusivity. |
| Process harness | `tests/support/mod.rs:12`, `:69` | Starts CLI workers through `std::process::Command`; this motivates the inherited-descriptor scenario but does not itself prove it caused the intermittent failure. |

The pinned `fs2` implementation was read locally:
`fs2-0.4.3/src/unix.rs:38` invokes `flock(LOCK_EX | LOCK_NB)` and `:42` invokes
`flock(LOCK_UN)`. The local standard-library Unix file implementation opens with
`O_CLOEXEC` (`std/src/sys/fs/unix.rs:1177`); close-on-exec does not describe the
preceding descriptor lifetime modeled by the test.

## Data flow and invariants

- The private `File` belongs to one `WorkspaceLock`. The production type exposes
  neither cloning nor its descriptor, so explicit unlock happens when that owner
  ends, not when an arbitrary duplicate closes.
- A temporary duplicate references the old open file description. Explicit owner
  unlock makes a separately opened replacement owner possible even while that
  duplicate exists.
- Closing the old duplicate afterward must not release the replacement owner's
  lock. The regression explicitly checks this state transition.
- Process death still relies on operating-system cleanup because destructors do
  not run on SIGKILL. This change qualifies orderly owner release, not arbitrary
  restore-time fencing or hostile processes that ignore advisory locks.

## Test behavior and evidence

The reviewer inspected `docs/reviews/workspace-release-failed.log`: before the fix,
the deterministic test fails with `workspace_busy` when reacquiring after dropping
the owner while its duplicate remains alive. The owner-run
`docs/reviews/workspace-release-check.log` shows the same test passing after the
fix. This file explicitly identifies itself as the unit-suite excerpt from the
successful full gate retained with the following rebuild checkpoint.

The assertions also preserve live-owner exclusion, verify that later duplicate
close cannot unlock the replacement owner, and verify acquisition after that owner
drops. These checks exercise the resource lifetime rather than merely mirroring
the implementation's unlock call. The test is Unix-specific; no Windows execution
claim is made. Unlock-error logging is statically traced and is not fault-injected.

## Conclusion

**VERDICT: APPROVE.** The patch releases intended ownership at the owner's lifetime
boundary while retaining live-owner exclusion. Changed-path coverage is
**ADEQUATE** and static confidence is **HIGH**. The observed red/green mechanism
is established; the origin of the intermittent parallel-suite failures remains an
inference. No latency, process-memory or full durability claim follows from this fix.

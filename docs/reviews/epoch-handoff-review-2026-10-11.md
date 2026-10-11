# Independent epoch-handoff fixture review

**APPROVE for bounded opt-in execution**, artifact
`007571d230f53d184e884e2228859861e9994c2ad88de127066bb1ff692a0b34`.
Read-only review; no build, test or provider call performed.

Traced `epoch_handoff.rs:118–232` through current `ExportSpec`, completion helpers,
`RaceCatalog::update_table`, unchanged native forwarding, child file rendezvous,
worker cleanup and actual configured `connect`/`Engine::with_remote` paths.
The dataset is explicitly the retained synthetic publication qualification;
canonical fresh UUID-derived channel/operations and epoch0 are checked before
new effects. There is no new source admission, object deletion or credentials
change. Existing public/source records remain pinned.

A's independent child holds the original maintained native commit at the same
observed metadata that the controller reads. The controller advances the real
lifecycle authority, releases that exact commit and requires the provider's
`CatalogCommitConflicts` kind. A retained stale-export retry preserves actual
completion revisions/objects, raw records and lifecycle head.

B prepares its own supported export after the fence. The wrapper forwards the
native commit unchanged, receives the actual successful provider Table and holds
that response before Engine acknowledgment. The controller resolves the real
selected result, then kills that OS child. A fresh-host retry preserves exact
completed output, validation, lifecycle metadata/revision and raw retained
records; literal duplicate/null/empty rows and primitive field IDs/types/
nullability are independently checked through the maintained export reader.
The child cannot produce a successful lost-response result itself; the parent
checks the native selected marker and catalog result before killing it.

Atomic create-only control files, finite waits and worker Drop kill/reap bound
normal failure cleanup. Parent uses exact child test name and requires actual
child PID receipts and held metadata, so a zero-test child cannot pass. A finite
outer root watchdog remains necessary and failure artifacts must be retained.

**Precise limits:** successful execution would qualify epoch fencing plus unknown
selection-response recovery. It does not prove global quiescence, revocation,
remote deletion/restore or power loss. It measures no physical requests/bytes.
The original public export and source are checked by exact pin equality; only the
new winner has a literal payload read in this frozen version. Thus do not claim
an additional old-source/old-export payload qualification from this fixture.
One cheap optional strengthening is `read_values(&cold, &public, true)` because
the entry guard requires that retained public export to bind the known updated
four-row source. This is an assertion precision point, not an effects blocker.

## Execution result

The frozen actual-provider test passed on 2026-10-11. Two independent child
processes exercised the native commit boundaries. The provider rejected A's
original held commit after the epoch advanced; its retry returned `stale_export`
and preserved completed work. B's provider commit succeeded, its response was
withheld, and the child was killed. A fresh host reconciled B without advancing
the lifecycle or changing retained completions, then read the exact winner values.
The fixture took 114.00 seconds; this is correctness evidence, not throughput.

See [parent output](epoch-handoff-provider-2026-10-11.stdout.log),
[supervision](epoch-handoff-provider-2026-10-11.supervision.json), and
[child records](epoch-handoff-provider-2026-10-11/processes/). Existing source and
public pins remained unchanged. The dataset epoch is now 1; do not replay the
epoch-0 fixture. No object deletion, source rebuild or credential rotation ran.

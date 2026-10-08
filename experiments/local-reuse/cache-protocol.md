# Exact-snapshot cache intervention

Frozen after the baseline history probe and before collecting intervention results.
The original history protocol and its failed result remain unchanged.

Observed baseline: In `history-20261007T213458`, the unchanged operation at 50
retained operations took 5.503 seconds, exceeding the preregistered 3-second budget.
Its native payload pins were unchanged. A latest lookup took a median 0.595 seconds.
The code opens the same immutable lifecycle snapshot for several key lookups.

Decision: Add a bounded, disposable cache only if avoiding repeated lifecycle
scans improves the complete unchanged-run path without changing authority checks.

Hypothesis: Reading a snapshot's bounded lifecycle rows once and caching them by
its exact metadata location removes repeated scans within the operation. Native
catalog load and guarded commit must still determine whether the selection is
current. Cached values cannot establish authority across a changed snapshot.

Arms: Retained append-only baseline above versus the same engine with one shared
snapshot cache per execution host. The cache has one entry, reserves its byte
budget and is discarded on restart. No durable application index or SQL state.

Cases and settings: Repeat the history protocol's exact 1/10/50 operation workload,
input, debug profile, command timeouts, limits and restarted CLI probes. Run the
native stale-view, concurrent-retry and process-kill tests before measurement.
Source fixtures and protocol remain unchanged. The baseline is historical; OS
caches and workstation background load are uncontrolled in both arms.

Decision rule: Any incorrect result, stale selection or changed payload pin is
NOT SOLVED. Accept the local cache if the unchanged run at 50 operations is below
3 seconds and at least 40% faster than the baseline, latest remains below 1 second,
and peak RSS remains below 256 MiB. Other history and resource qualification
requirements remain open even if this bounded intervention passes.

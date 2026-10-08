# Bounded metadata history probe

Frozen before collection on 2026-10-07. This extends the local reuse probe with
an early Q7 check; it does not qualify the final metadata layout.

Decision: Can the simplest append-only lifecycle table remain the local foundation
while broader lifecycle work proceeds, or does history already require attention?

Hypothesis: Keeping the data, query, update size and native snapshot fixed while
retaining more operations should leave payload preparation unchanged. Catalog
selection and commit may grow with history; measure that cost before adding caches
or indexes.

Arms/cases: One retained Bill Actions input, checked against its original SHA-256
manifest. Create unchanged operations in one workspace. At 1, 10 and 50 retained
operations measure a restarted CLI's latest lookup, plan and inspect; capture
every operation's total run time. Every operation must preserve the first exact
source and native data pins. No source mutation or payload rewrite is allowed.

Held constant: Debug binary and Cargo.lock, default limits, same filesystem, warm
or unspecified OS cache, no cache reset. Three latest lookups per history point.
Record command outputs, errors, elapsed time, RSS, files, metadata bytes and retained
storage. Bound each command to 120 seconds and total work to five minutes. Retain
the measured failure if a bound is reached.

Decision rule: Wrong selection, rewritten payload, or changed phase generation is
NOT SOLVED. Provisional local interaction budgets are median latest <= 1 second,
plan <= 2 seconds, inspect <= 3 seconds and unchanged run <= 3 seconds at 50
operations. Exceeding any budget blocks accepting this layout for longer histories;
report the observed bottleneck before choosing an optimization. These are local
prototype budgets, not established product service levels. Passing supports only
this history range. Q7 also requires independent fragment/evidence-depth scaling,
request counters and maintenance operations, which this probe does not cover.

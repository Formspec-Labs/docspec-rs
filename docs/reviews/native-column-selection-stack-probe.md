# Native selection stack failure probe

Decision: identify and remove the stack regression before committing native
column selection. Keep the normal thread stack and production resource limits.

Observed: the second repository gate aborted in
`incremental::incremental_disk_refusal_retains_admission_for_a_smaller_resource_profile`.
The preceding HEAD gate passed; the current run includes native selection.

Hypotheses: larger nested reader futures may consume stack before the existing
SQL planner runs; alternatively the failure may arise in a different operation
or depend on concurrent tests. A debugger trace of the exact failing test will
locate the boundary. A passing isolated test would leave concurrency or marginal
stack depth unresolved rather than prove a fix.

Arms: current working tree; then one evidence-directed reduction of retained
future size or unnecessary SQL parsing. Capture the trace before choosing the
intervention. Do not change stack limits or data fixtures to pass the test.

Held constant: debug profile, lockfile, all features, exact failing fixture,
normal stack, existing resource profile. Bound diagnosis to one isolated trace
and one initial intervention; record any revised hypothesis before further work.

Decision rule: the trace must support the changed boundary, the exact failing
case must pass, and the repository gate must pass before claiming resolution.
No timing, resident-memory or corpus-throughput improvement will be inferred.

The isolated [debugger trace](native-column-selection-stack-backtrace.log)
reproduces the failure while parsing the incremental replacement join. The
native reader is not actively decoding at that frame; its concrete futures are
embedded in enclosing lifecycle futures. The first intervention boxes the shared
native `plan_files` future once in `schema::plan_selected_files`, reducing this
embedded state for full and selected readers. Whether that reduction is enough
remains a hypothesis until the exact failing case and gate complete.

The [single-case rerun](native-column-selection-stack-boxed.log) passes with the
same normal stack and unchanged fixture after that intervention. This supports
the narrow correction; it does not measure stack savings or establish that
every planning path has sufficient headroom. The subsequent
[complete repository gate](native-column-selection-full-check-3.log) passes
formatting, strict Clippy and all-feature tests, including the same failing case.
Adopt the shared planner boxing as a bounded fix supported by these executions;
stack savings and performance remain unmeasured.

# Native scan and join correction

Frozen before collecting plan or intervention evidence on 2026-10-10.

**Decision:** repair the existing native scan integration if maintained DataFusion
planning can choose the appropriate build input using retained row-count hints.

**Observed failure:** the `ce3ad6b` full acceptance attempt failed after 48.76
seconds in the Comments-to-Dockets stage with a partitioned hash-build reservation
error. Its input pins and failed workspace remain in `acceptance-20261010.json`.
The failed phase may be the cardinality probe or the main join.

**Hypotheses:** missing native statistics cause the main query to retain the
10-million-row Comments side. The alternate explanation is a problematic
cardinality-probe plan. Creating both production physical plans without executing
them distinguishes their build inputs; it does not alone prove which query threw
the recorded runtime error.

**Arms:** baseline `775fd75` Rust behavior with a test-only plan diagnostic;
then the same SQL and inputs through a maintained `DataSourceExec` with native
snapshot/file row-count hints. Keep decoded byte statistics unknown and hints
inexact so planning estimates cannot replace payload reads with exact aggregates.
Column pushdown and cardinality-query changes are separate later interventions.

**Cases:** the retained full Comments/Dockets pins and both their actual queries;
small asymmetric fixtures in both directions; empty inputs, nulls and duplicates;
existing related-result and incremental equivalence tests. Plan-only collection
opens selected metadata but never executes data scans. Do not build an alternative
plain-Parquet provider for this comparison.

**Held constant:** native decoder, occurrence semantics, SQL, DataFusion 53.1.0,
Arrow/Parquet 58.4.0, resource settings, and the accepted complete-path workload.
Use the failed run's 12 threads, 24 GiB memory and 32 GiB scratch profile for its
plan diagnostic. Record build time separately; run no benchmark during builds or
tests. One baseline/intervention plan capture is enough because the immediate
decision concerns deterministic planning, not throughput variability.

**Decision rule:** adopt the adapter only after row hints reach physical planning,
the asymmetric plans use an appropriate smaller build, semantics and admission
tests pass, and independent review finds no material defect. Missing/malformed
summary values must remain unknown; an inexact hint must not satisfy an exact
verification by itself. Retain all failures. If hints do not change the relevant
plan, investigate that plan before another corpus execution or memory increase.

**Performance evidence:** follow the unchanged full protocol after a separately
captured corrected build. The historical failed run remains a failure. A resumed
diagnostic can establish the failed phase now completes, but cannot pass the
three-fresh-run acceptance gate. Measure elapsed time, process memory, scratch,
peak additional disk and retained storage without deleting old attempts.

## Revision before corrected collection

The initial focused runtime check reproduced a pinned DataFusion correctness
issue: a seven-row native source with a scalar-function filter, `OFFSET 2` and
outer `COUNT(*)` returns zero instead of four when a row hint is available.
Unknown source counts return four. `FilterExec` estimates two rows; the limit
promotes that estimate to exact zero and `aggregate_statistics` folds the result.
See `docs/reviews/native-statistics-initial-check.log` (from the repository root).

Use row counts bound to the actual selected TablePin or captured file set, not
unqualified snapshot summaries. Disable only the affected physical aggregate
rewrite through the maintained session builder and retain the other default
optimizers. This also guards the existing flat-Parquet path. The intervention is
now this required correctness guard plus native scan statistics. Inexactness
alone is not a guarantee that downstream rules cannot fold a result. The
regression must return the actual count; additionally test the exact-size source
through a maintained flat-Parquet provider. This is a correctness prerequisite,
not a changed performance target.

## Planning and correctness outcome

The baseline main plan builds the Comments side. The final diagnostic admits the
recorded release manifests through the production bounded reader and chooses
Dockets (279,719 rows) as the build side, with Comments (10 million rows) probing.
Both plans keep the cardinality check's Dockets build. The earlier baseline and
first intervention diagnostic checked identifiers but not the enclosing manifest
digest; retain that limitation with those logs. The final captured plan is bound
to the recorded manifest identity and opens no payload streams.

The first focused execution reproduced the predicted false count. After the
single-rule guard, the native and maintained flat-Parquet regressions return
four. Asymmetric fixtures choose the smaller build in both SQL directions. The
repository gate passed; subsequent review corrections received strict lint,
library checks, a final verified plan capture and independent static approval.
The file correction uses the existing map instead of repeating an O(F) traversal
for each of F file providers. No lookup-only test or alternate index was added.

**Decision: adopt the narrow adapter plus required correctness guard.** This is
bounded planning and correctness evidence, not a throughput or memory result.
The complete performance question still requires the unchanged full protocol.
Evidence is retained under `docs/reviews/native-*` and linked from `docs/STATUS.md`.

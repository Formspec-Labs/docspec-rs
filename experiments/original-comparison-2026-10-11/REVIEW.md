# Independent benchmark review

**Verdict: APPROVE**, 2026-10-11. Reviewer: independent subagent
`/root/related_compaction_selection`. Static harness review preceded collection;
this final evidence review followed the current-release series. The reviewer ran
no builds, workloads, provider calls or payload scans.

The reviewer reconciled every primary observation with its retained worker result,
confirmed both exact schema/value comparisons and unchanged-selection assertions,
and recalculated the summary. The protocol, preparation, worker, runner, limits
and monitor hashes match the recorded execution sources. Package/source/binary
pins agree with the report.

| Question | Finding |
|---|---|
| Does the report use current optimized Rust? | Yes. The older optimized/current debug diagnostics are excluded from primary figures. |
| Are the timing boundaries accurate? | Yes. Admission/read, retry, reopened read and the complete worker are distinguished. Interpreter/CLI asymmetry and differing native thread configurations are disclosed. |
| Were unfavorable observations preserved? | Yes. The slower first Rust Bill Actions admission/read remains in the range and report. |
| Are correctness and resource claims bounded? | Yes. Every selected value/type/nullability is checked. Memory includes the oracle and is observed rather than a continuous aggregate peak. Logical storage and APFS allocation are not presented as unique physical bytes. |
| Do the results support the reported ratios? | Yes. Comments admission/read, whole-worker, observed-memory and retained-storage calculations agree with raw observations. |
| Does scope remain accurate? | Yes. Native admission/read/retry only; no language-level, export, update/history, remote or full-10M comparison claim. |

The pre-collection review identified one inherited monitor cleanup problem:
Darwin can return EPERM for an already empty process group. The private monitor
now accepts that result only after confirming the group contains no process;
live or uncertain cases still fail. The reviewer checked that correction before
collection. No remaining material finding was identified.

Confidence applies to these retained measurements and the reviewed harness, not
to general performance outside the measured cases and configurations.

Final repository checks: Python syntax compilation, completed release build,
primary runtime assertions, evidence-link resolution and source-hash checks passed.
The frozen monitor retains one trailing blank line from its captured source; it
is preserved to keep the execution hash exact. No production test suite was
rerun for this experiment-only change.

# Comments resumed-result evidence review

**Disposition: accept the reported bounded resumed result.** No material mismatch
was found between the retained evidence, frozen protocol, driver, updated report,
and status summary. This is not the full lifecycle performance acceptance result.

Reviewed on 2026-10-08: `experiments/comments-input/resume-20261008T113117/`,
`resume-20261008.log`, `resume-protocol.md`, `resume.py`, `run.py`, the updated
`REPORT.md`, and the Comments checkpoint in `docs/STATUS.md`. The reviewer parsed
only small JSON/log artifacts; no Cargo command, large payload read, or payload
revalidation was performed.

## Evidence reconciled

- The recorded source is `7052a2b6182eddcab3a912bb1b01166750fa44a4`. Captured driver,
  monitor, protocol, dependency-lock, and original-metrics hashes match their
  retained files. The command log's final result equals `results.json`. All
  monitored commands report exit 0 with no monitor stop reason.
- Preflight state had no selected result and only the completed admission phase.
  The resumed release names that same admitted object and qualified digest.
  Selection returns the exact resumed release. Stderr records admission reuse
  and new processing/validation completion.
- Independent readback reports exact live-file membership and footer/schema/size
  checks across four selected files totaling 10 million rows. It decoded 24 sample
  rows across three files, reporting 129,699 returned sample bytes. These checks
  do not establish every unsampled value or complete reconstruction.
- The unchanged operation preserves exact input and group pins, and its logs
  report qualified phase reuse. It adds 99,726 logical metadata bytes. The exact
  retry returns the same release and has identical before/after inventories.
  These observations establish physical reuse, not absence of cached reads.

## Time and storage arithmetic

| Quantity | Independently reconciled value |
| --- | ---: |
| Resumed processing, selection, and readback | 41.346891 seconds |
| Original failed attempt plus those steps | 49.294980 seconds |
| All monitored commands, including inspection/reuse arms, plus original failure | 51.326110 seconds |
| Workspace before resume, including failed work | 2,802,772,903 logical bytes |
| Final workspace | 5,452,875,422 logical bytes |
| Additional retained workspace | 2,650,102,519 logical bytes |
| Final/sampled-peak workspace allocation | 5,501,767,680 filesystem-reported bytes |
| Final workspace / frozen encoded input | 2.041900 times |

The workspace sum includes admitted input (2,670,490,619 bytes), selected native
output (2,649,864,239), prior failed native output (132,213,327), and metadata
(307,237). The inventories retain earlier immutable paths, sizes, and metadata;
the existing catalog is the expected mutable exception. No failed-output cleanup
is credited. Adding the external prepared input and codec outputs gives the
report's 8,221,928,385-byte subtotal. The report explicitly excludes diagnostic
logs, build artifacts, and the separately shared original corpus from that subtotal.

The 49.295-second figure excludes preparation, diagnostics, build, preflight
inspection, and reuse arms; the report states these exclusions and reports the
broader monitored-command total. It is neither fresh-workspace timing nor an
uncompressed-versus-compressed complete-path speed comparison.

## Resource and acceptance limits

Raw samples reproduce the reported process-tree peaks; raw timing logs reproduce
their recorded maximum RSS values. The resumed command reached 3,306,389,504
sampled process-tree bytes and 3,305,832,448 timing-tool bytes. Final volume free
space was 17,574,912,000 bytes, above the 2 GiB stop. Sampled workspace use remained
below the 16,601,406,449-byte allowance; no scratch was observed.

Workspace inventories are sampled lower bounds and can miss temporary peaks.
Allocated blocks do not identify unique APFS extents. Volume free-space changes
include unrelated activity and are not a DocSpec write counter. The separate
5.2 GiB debug-cache cleanup is not dataset storage savings. None of these
observations establishes a continuous production quota or future retention and
maintenance capacity.

The result appropriately keeps `full_performance_acceptance_passed=false` and
`payload_read_avoidance_measured=false`. The cache may be warm. Joins/summaries,
complete evidence, exports, maintenance, remote publication, scaling across
history, and the required fresh full-flow repetitions remain unqualified. The
original 64 MiB attempt remains a retained failure.

Evidence SHA-256:

- `resume-20261008T113117/results.json`: `8e51a86dc5de3c4ecc77215967c78cc2ec96a09a1c801764e7b560cb37abe3de`
- `resume-20261008.log`: `7a833aa239237c85d4af37f5fe4456d028a85760bcc51b166def4ac1f5ef2bbb`

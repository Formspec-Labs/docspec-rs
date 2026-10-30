---
id: dec-lphk
status: closed
deps: []
links: []
created: 2026-10-28T03:30:01Z
type: bug
priority: 0
assignee: mikewolfd
---
# Fit scalar-function batches before output byte admission

## Acceptance Criteria

Contract first: multi-row function output fits through bounded batches, exact values/nulls and single-row refusal; preserve output ownership and limits; official audit measured; independent review.


## Notes

**2026-10-30T20:16:24Z**

Validated on main (ce75e1b): FitProjections, FitFunctions, FittedTable, FittedProjection, FittingInput, FittedPredicate, windows::rewrite, and ScalarOp::fit_output slice scalar inputs before output admission across projections, filters, sorts, aggregates, joins, and window frames; verified in tests/sql_functions.rs.

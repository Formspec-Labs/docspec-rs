---
id: dr-rtdp
status: open
deps: []
links: [dr-agq0]
created: 2026-11-13T19:44:54Z
type: bug
priority: 1
tags: [owner-worker, greenfield, exact-values]
---
# Preserve exact decimal parameter semantics in native SQL

Owner role: DocSpec SQL parameter owner.

User outcome: Money and other declared decimals retain exact values and types through queries.

Split SDK4 review P3-3 from dr-agq0. Fixed-point parameter lexemes must not become Float64 through incidental DOUBLE casts. Use maintained native typing and retain explicit unsupported/overflow behavior; this is independent of page-reading performance.

Sources: [Original review task](/Users/mikewolfd/Work/spicy-stack/docspec-rs/.tickets/dr-agq0.md), [greenfield work](/Users/mikewolfd/Work/data-design-kit/docs/work-backlog.md).

Greenfield scope, priorities and evidence: [work backlog](/Users/mikewolfd/Work/data-design-kit/docs/work-backlog.md#greenfield-delivery).

## Acceptance Criteria

- Reproduce the current decimal arithmetic/type behavior with independently expected values, including 0.10 and exact trailing-zero scale cases.
- Verify Decimal128 results for supported decimal inputs, plus explicit tests for integer, floating, exponent, negative, null and overflow cases as applicable.
- Exercise the actual owner query consumer and exact result transport; parameter binding remains non-interpolating and bounded.
- Run meaningful native/consumer regressions with required dependencies and no skipped coverage.

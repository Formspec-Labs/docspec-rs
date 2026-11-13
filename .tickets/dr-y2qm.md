---
id: dr-y2qm
status: open
deps: []
links: []
created: 2026-11-13T15:23:34Z
type: task
priority: 1
assignee: mikewolfd
parent: dr-dlb1
tags: [engine, managed-job]
---
# Return large job witnesses through bounded native object references

Owner role: DocSpec retained-job owner with DDK receipt adapter owner.

User outcome: Large valid jobs complete without oversized inline result payloads.

Retain bulk completion witnesses under the existing native owner and return exact object references through bounded metadata. Preserve native reference schemes/digest algorithms, protection, recovery and resource accounting. The existing 32 MiB transport ceiling is not a reason to raise every caller limit.

Sources: [Greenfield work](/Users/mikewolfd/Work/data-design-kit/docs/work-backlog.md).

Greenfield scope, priorities and evidence: [work backlog](/Users/mikewolfd/Work/data-design-kit/docs/work-backlog.md#greenfield-delivery).

## Acceptance Criteria

- Exercise a real retained witness larger than the inline transport cap; the job completes and the caller binds the exact native object, digest algorithm, size and result identity.
- Verify missing, wrong-digest, wrong-candidate and retired/protected-object cases plus interrupted consumer recovery.
- Measure peak memory, serialized response size and bytes copied through the DDK consumer; avoid duplicate base64/JSON materialization.

## Reconciliation

November 13, 2026: Original finding retained below.

### Original finding

# Pass large whole-job completion witnesses by object reference

Whole-job completion witnesses are base64-nested inside JSON envelopes; large jobs approach the 32 MiB stdout cap (RG-7 / F8). Return an ObjectRef the caller reads instead.

## Acceptance Criteria

A job whose witness exceeds the stdout cap completes and the caller verifies the witness by sha256 through the reference.

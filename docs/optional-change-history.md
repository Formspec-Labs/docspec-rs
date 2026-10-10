# Optional applied-change history

Complete results keep their values, schemas, original inputs and required
evidence. Applied-row history has a separate lifetime so it can be retired
without making those results unreadable or unusable as inputs.

New local writes record their production rules in their recoverable processing
results. After validation, an immutable `AppliedChangeSet` describes the source
and receipt changes. The lifecycle commit selects a `ChangeCertificate` together
with the exact resulting version. Their native commit revision must match.
Sampling or possession of an exported JSON object does not grant this local
authority. Older processing results without the production marker remain usable
but cannot acquire a certificate from a later sampled check.

The complete `DatasetPin` and receipt state contain no applied-row payloads.
Recovery phases retain their own changes and dependencies until explicitly
retired. A certificate protects its descriptor, interpretation metadata and
applied rows; it does not recursively protect every complete endpoint or older
change set. Independent consumer protections remain authoritative.

## Inspection and retirement

`Engine::change_certificate` accepts an exact `VersionRef` and returns bounded
metadata. Missing or retired history returns `None`; malformed present evidence
refuses. Inspection reads no row payloads and creates no lasting protection for
the caller. Full endpoint comparison continues to be the public comparison path;
certificate discovery does not enable change-chain acceleration.

The CLI provides `inspect-changes`, `plan-change-retirement` and
`retire-changes`. HTTP provides the same services at `/changes/inspect`,
`/retention/changes/plan` and `/retention/changes/retire`. Retirement planning
captures exact certificate generations; execution rechecks them through the
existing guarded retirement service and reconciles identical retries.

Retiring a certificate removes only that promise. It does not delete bytes.
Prepared changes, processing phases, operation attempt reservations and consumer
protections may still require the same objects. End each unwanted recovery
promise through the existing phase or operation retirement service, then use
retention inspection and the supported removal planner to identify eligible
objects. The descriptor has owner creation evidence, just like other removable
outputs. A saved retirement plan is not permission to bypass a current blocker.

## Verification and existing results

Ordinary reads continue to resolve an immutable selection and stream values.
Routine verification checks required state metadata and bounded samples; it does
not open optional applied-row files. Explicit deep verification reconstructs
source updates and receipt replacements from their retained original inputs and
predecessors. It preserves duplicate/null semantics, schema identity and the
unchanged source occurrences. Optional history is unnecessary for those checks.

`verify_applied_changes` is a separate explicit transition audit. It resolves
both exact endpoint manifests and checks applied-row membership and reconstruction.
The caller retains its objects during the audit. This is diagnostic work, not a
new certificate authority or a prerequisite for normal reading.

Supported older state formats remain readable. Retention recognizes only the
former complete-state `changes` members, validates their old shape and drops
them from the complete-state roots. Unknown dependency fields still refuse;
required `PhaseResult.changes` is unchanged. Older optional members are never
promoted to local certificates. See [status](STATUS.md) for qualification evidence
and the remaining lifecycle work.

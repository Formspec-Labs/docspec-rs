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
the caller. Public comparison can use qualified local history as described below;
inspection alone does not reserve it for a later operation.

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

## Forward comparison

`compare` and `changes` use qualified local source history when it reaches the
exact requested before version from the after version. The selected certificate
and result must share a native commit revision; predecessor revisions must
decrease. Each source transition binds the adjacent physical metadata identities.
Receipt-only transitions preserve logical source identity, including verified
compaction. Native field identities, types and interpretation rules must match.
Metadata effects still describe the requested endpoints.

The engine checks this chain from one captured catalog view before opening delta
readers. It composes retained removed/added rows through the same DataFusion
operators used by full comparison. Exact intermediate occurrences cancel before
value-only representatives are chosen, preserving duplicates, nulls and actual
endpoint addresses. Intermediate complete-result retirement does not end the
independent history promise.

Planning admits at most 64 links and the configured cumulative metadata bytes.
The link cap is a safety bound, not a measured performance crossover. Native
readers open sequentially through a maintained finite streaming table; existing
memory admissions and maintenance custody last through the returned stream.
Cold catalog lookup can still scan lifecycle history. No new durable delta files
are written, and this path makes no whole-process memory or peak-disk guarantee.

Missing/retired optional history, reverse or unrelated versions, unsupported
field groups, changed schema rules and planning limits select `full` with an
explicit reason. Corrupt or inconsistent present history refuses. `force_full`
always selects the endpoint comparison; it does not read optional certificates.
Local incremental joins and summaries share this resolver, then read raw old/new
key columns without value cancellation. Their conservative fallback reads changed
endpoint files. The admitted refresh protects transient history consumption;
completed processing retains its own outputs and inputs, so recovery does not
require keeping that optional history. Portable trust remains separate work.

High-churn histories can write more temporary keys than the fallback because
staging grows with total delta rows before downstream key deduplication. Link and
metadata limits are not a bound on key bytes. Complete-path timing and peak/retained
disk qualification remain required; see [current status](STATUS.md#shared-history-for-dependent-refresh).

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

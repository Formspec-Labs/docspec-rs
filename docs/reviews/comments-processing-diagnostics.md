# Comments processing diagnostics review

**Approve the corrected diagnostic probes for collection.** No remaining material
finding was identified in this bounded review. This approval does not establish a
production configuration, lossless codec result, or complete-path performance pass.

Reviewed on 2026-10-08 against `AGENTS.md`, the applicable Microsoft Rust
guidelines, the semi-formal code-review workflow, and the frozen
`experiments/comments-input/processing-protocol.md`.

## Findings resolved before collection

| Original finding | Verified correction |
| --- | --- |
| Inspecting string lengths only at new allocation maxima could miss an individually oversized row in a later, smaller allocation. | `examples/comments_scan.rs:61–77` inspects every oversized batch and retains row/value maxima independently of allocation maxima. |
| Keeping all diagnostic observations until the final stdout result lost useful evidence on failure or an external stop. | `examples/comments_scan.rs:79–95` immediately emits new allocation maxima, increasing oversized-row maxima, and periodic progress on stderr. The monitor retains that stream. |
| The driver reported the mutable prepared input's expected digest while checking only its size; the diagnostic bypasses admission. | `processing.py:35–44` now resolves the completed admission's immutable object from retained CLI inspection and requires its qualified digest and size to match the frozen preparation. The inspected record identifies the failed pilot's admitted object and retained protection. The driver records this identity basis and hashes the inspection evidence. |

The protocol also now distinguishes the failed-run inventory's size/mtime evidence
from a content digest. The codec source receives one SHA-256 before the arms.

## Traces and executed check

The scan matches `process_build` → `write_frame` → `storage::write_batches` for
Parquet registration, `SELECT *`, stream execution, and the reported Arrow memory
metric used at the observed refusal. It retains the same execution partitions and
DataFusion memory pool while varying batch rows and forced string views. These
read-only diagnostics deliberately omit the native writer and lifecycle path.

The codec probe holds reader batch and writer row-group sizes constant and changes
only the requested compression setting. Its elapsed re-encoding time includes
source reading/decoding, writing, close, and file sync. Driver readback runs as a
separate monitored command. `processing.py:14–27` compares schema metadata, footer
row counts, and every ordered decoded batch; `zip_longest` refuses unmatched tails.
The caller also reconciles compared, encoded, and retained source row counts.

A tiny temporary PyArrow fixture exercised the actual `compare` function:

```json
{"changed_metadata":"rejected","changed_value":"rejected","missing_row":"rejected","reordered_rows":"rejected","same_values_different_row_groups":"accepted"}
```

The fixture included strings, nulls, and empty strings. It verified the independent
comparison, not the Rust writer or the retained Comments values. The reviewer ran
no Cargo command or large scan. Final Rust build/gate evidence remains the main
task's responsibility.

The driver reuses the previously reviewed resource monitor, retains failed arms
and partial outputs, and keeps `full_performance_acceptance_passed=false`.
Diagnostic completion does not imply that any scan configuration stayed below
the batch allowance. Sampled resource peaks remain lower bounds; Arrow's reported
array memory can count shared buffers more than once. Codec comparisons on one
retained physical file support a next candidate, not a corpus-wide compression or
end-to-end speed claim.

Reviewed SHA-256 identities:

- `examples/comments_scan.rs`: `b974e43b10c2b01f2359bfe561c850997ee11155c59facbdf601a3b7bd199c34`
- `examples/comments_codec.rs`: `6a23c2f1c29c3942a9cb3a1298680cb1695bbd2921207195548f31978114ad3c`
- `experiments/comments-input/processing.py`: `dc459942d1665d86af00e42a06daf6032db42eb217bad439b498cd8008bea5ff`
- `experiments/comments-input/processing-protocol.md`: `6308a3d277c106be6a2372a3fdbe6197d019c44180243a382632abaa0dd71be4`

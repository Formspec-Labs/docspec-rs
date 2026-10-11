# Scalar readback decoder repair review

**APPROVE, static scope.** No actionable finding in `/tmp/docspec-scalar-readback-codec.patch`, SHA256 `ecb36b1c257852d93a1420889aaa0d3c500c33fabd8d174a48521aaa53c00d0e`. References below use the applied, formatted worktree reviewed on 2026-10-11. No builds, formatter, provider calls or shared edits were performed by this reviewer. Runtime qualification remains parent-owned.

## Repair and function trace

| Function | Evidence | Verified behavior |
| --- | --- | --- |
| `ReadbackCompletion::deserialize` | `src/engine/backfill.rs:105–135` | Retains the existing flattened serialized shape. Types the optional private validator member, then consumes the JSON Value through maintained `ReleasePin` decoding. No additional payload or catalog read and no whole-pin clone in this decoder. |
| `retained_backfill_readback` | `src/engine/backfill.rs:1023–1107` | Calls the repaired decoder within existing metadata admission; preserves exact source/target/request/format/history guards, selects retained native revision, and compares the private built-in validator identity. |
| `Inventory::record` | `src/engine/retention/inventory.rs:1470–1476` | Only `backfill-readback/` uses strict `ReadbackCompletion`. Other result/readback rows continue to use strict public `ReleasePin`. Both delegate to the same release dependency expansion. |
| `Strict<T>::deserialize` / `decode` | `src/engine/retention/inventory.rs:1815–1835` | Existing typed roundtrip and recursive unknown-field refusal remain unchanged. The new private field survives only the correct typed roundtrip. |
| `normalize_legacy_state_fields` | `src/engine/retention/inventory.rs:1838–1864` | Extends the existing exact complete-state normalization to the private readback type at `dataset`; it does not add a global field exception or loosen nested dependency checking. |
| Other retained-readback consumers | `src/engine/retention/owners.rs:1056–1066`; `availability.rs:190–230` | Still read the unchanged public pin or bounded identity for their own purpose; neither requires validator interpretation. Strict inventory remains the dependency-field gate. |

## Data flow and resolved failures

The new row member is written with the existing completion/protection commit (`backfill.rs:961`); public manifest bytes, pin identity and result rows remain unchanged. Missing/older private identity still requires current qualification. A known current readback keeps its independent promise without reviving retired upstream validation.

The integer-map failure is resolved by a concrete maintained decoder difference: locked `serde_json` is 1.0.151 (`Cargo.lock:4714`), whose Value `MapKeyDeserializer` parses JSON numeric map keys for `i32` (`serde_json-1.0.151/src/value/de.rs:1213–1258`). The repaired decoder uses that implementation instead of flattened Serde Content. The full pin is moved into it; only the small optional validator Value is cloned.

The strict-retention failure is resolved by selecting `Strict<ReadbackCompletion>` only for its native row family. Its serialization includes the recognized private member, so `known_fields` accepts it there. `Strict<ReleasePin>` still rejects the same member elsewhere; top-level or nested unknown dependencies still disappear from the typed representation and are refused. Legacy dataset normalization still validates a non-null former `changes` member with the existing strict `ChangePin` decoder before discarding it.

Existing metadata admission and row limits remain in place. This repair substitutes a bounded JSON representation for the former buffered representation; it is not a measured heap/RSS result. It adds no retained telemetry, additional lifecycle lookup, new ownership authority or payload validation.

## Tests and limitations

`private_scalar_validator_is_typed_only_on_scalar_readback` (`inventory.rs:2455`) exercises current/absent identity, wrong scalar type, unknown top-level/nested fields, confinement to the scalar row type, and existing legacy null normalization. It does not contain native integer-key maps; those are exercised by the actual Engine validator fixtures, including retained readback and `current_scalar_readback_retains_validation_after_phase_retirement` (`backfill/validator_tests.rs:462`). That test retains real computed field maps, cold-opens, verifies values, and requires every retired upstream native revision to remain unchanged.

Earlier approval missed both runtime decoder incompatibilities; their failed logs and withdrawn verdict remain in `/tmp/docspec-scalar-validator-review.md`. This review closes the specific repair statically after inspecting actual callers and the maintained numeric-key decoder. It does not replace the required focused rerun and repository gate or establish remote scalar support.

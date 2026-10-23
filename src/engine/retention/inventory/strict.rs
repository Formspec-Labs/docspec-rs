//! Fail-closed JSON deserialization and object identity validation for retention inventory.

use crate::{Error, Result, model::ObjectRef, storage};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;

pub(super) struct Strict<T>(pub(super) T);

impl<'de, T: DeserializeOwned + Serialize> Deserialize<'de> for Strict<T> {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        let parsed = T::deserialize(&value).map_err(serde::de::Error::custom)?;
        let known = serde_json::to_value(&parsed).map_err(serde::de::Error::custom)?;
        if !known_fields(&value, &known) {
            return Err(serde::de::Error::custom(
                "unsupported retention metadata fields",
            ));
        }
        Ok(Self(parsed))
    }
}

pub(super) fn decode<T: DeserializeOwned + Serialize>(json: &str) -> Result<T> {
    Ok(serde_json::from_str::<Strict<T>>(json)?.0)
}

// Serde's flatten and untagged enums buffer unknown fields, bypassing ordinary
// ignored-field callbacks. Require the typed round trip to retain every key.
// Values are interpreted by the maintained deserializers; this checks only
// whether an unknown dependency-bearing field would otherwise disappear.
pub(super) fn known_fields(original: &serde_json::Value, known: &serde_json::Value) -> bool {
    match original {
        serde_json::Value::Object(fields) => fields.iter().all(|(key, value)| {
            known
                .get(key)
                .is_some_and(|field| known_fields(value, field))
        }),
        serde_json::Value::Array(values) => known.as_array().is_some_and(|fields| {
            if values.len() != fields.len() {
                return false;
            }
            // Iceberg keeps these collections in maps and can reorder them on
            // serialization. Match their stable IDs in O(n log n), not position
            // or pairwise search. Ordered schema fields and other arrays stay positional.
            for id in ["schema-id", "snapshot-id", "spec-id", "order-id"] {
                let indexed: Option<BTreeMap<_, _>> = fields
                    .iter()
                    .map(|field| Some((field.get(id)?.as_i64()?, field)))
                    .collect();
                if let Some(indexed) = indexed
                    && indexed.len() == fields.len()
                    && values
                        .iter()
                        .all(|value| value.get(id).and_then(serde_json::Value::as_i64).is_some())
                {
                    return values.iter().all(|value| {
                        indexed
                            .get(&value[id].as_i64().expect("ID checked above"))
                            .is_some_and(|field| known_fields(value, field))
                    });
                }
            }
            values
                .iter()
                .zip(fields)
                .all(|(value, field)| known_fields(value, field))
        }),
        _ => true,
    }
}

pub(super) fn reference_bytes(object: &ObjectRef) -> usize {
    object
        .path
        .len()
        .saturating_add(object.digest.len())
        .saturating_add(object.owner.as_str().len())
        .saturating_add(256)
}

pub(in crate::engine) fn validate_object(object: &ObjectRef) -> Result<()> {
    if !storage::valid_object_locator(&object.path)
        || object.digest.len() != 64
        || !object.digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(invalid_inventory(
            "object needs an absolute path and a BLAKE3 identity",
        ));
    }
    Ok(())
}

pub(super) fn invalid_inventory(message: impl Into<String>) -> Error {
    Error::refusal("retention_inventory", message)
}

pub(super) fn unknown_dependencies() -> Error {
    Error::refusal(
        "retention_unknown_dependencies",
        "retained work lacks exact dependency metadata; cleanup cannot infer it",
    )
}

pub(super) fn capacity() -> Error {
    Error::refusal(
        "metadata_capacity",
        "retention inspection exceeds its metadata allowance",
    )
}

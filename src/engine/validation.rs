//! Built-in routine-check identity, independent of retained value computation.
// Rust guideline compliant 2026-02-21

use super::*;

// Bump when routine native inspection or assembled-result acceptance changes.
// Computation changes still bump PROCESSOR; package-only changes bump neither.
pub(super) const VALIDATOR: &str = "docspec.dataset-validation.v1";

pub(super) fn phase_key(material: &str) -> Result<String> {
    Ok(format!(
        "phase/checked/{}/validation",
        storage::json_digest(&(VALIDATOR, material))?.to_hex()
    ))
}

/// Keeps current qualification with the readback after upstream phases retire.
#[derive(Clone, Debug, Serialize)]
pub(in crate::engine) struct ValidatedReadback {
    #[serde(flatten)]
    pub(in crate::engine) release: ReleasePin,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    validator: Option<String>,
}

// Preserve JSON's integer native-field-map keys through the maintained Value
// decoder; flattened Serde Content cannot decode those maps. Strict retention
// decoding admits this private stamp only on the readback record family.
impl<'de> Deserialize<'de> for ValidatedReadback {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        let validator = serde_json::from_value(
            value
                .get("validator")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        )
        .map_err(serde::de::Error::custom)?;
        let release = serde_json::from_value(value).map_err(serde::de::Error::custom)?;
        Ok(Self { release, validator })
    }
}

impl ValidatedReadback {
    pub(super) fn stamp(release: &ReleasePin) -> impl Serialize + '_ {
        #[derive(Serialize)]
        struct Stamp<'a> {
            #[serde(flatten)]
            release: &'a ReleasePin,
            validator: &'static str,
        }
        Stamp {
            release,
            validator: VALIDATOR,
        }
    }

    pub(super) fn current(&self) -> bool {
        self.validator.as_deref() == Some(VALIDATOR)
    }

    pub(super) fn require_current(&self) -> Result<()> {
        if self.current() {
            Ok(())
        } else {
            Err(Error::refusal(
                "validator_changed",
                "retained readback needs the current routine check; resume the operation",
            ))
        }
    }
}

impl Engine {
    pub(super) async fn trust_current_readback(
        &self,
        release: &ReleasePin,
        checks: &mut verification::Checks,
    ) -> Result<()> {
        // Read only the proof header, not another copy of the full inventory.
        #[derive(Deserialize)]
        struct Stamp {
            manifest: ObjectRef,
            #[serde(default)]
            validator: Option<String>,
        }
        let view = self.read_view(&release.dataset.dataset).await?;
        let stamp = self
            .inner
            .lifecycle
            .get::<Stamp>(&view, &format!("readback/{}", release.id))
            .await?;
        if stamp.is_some_and(|stamp| {
            stamp.manifest == release.manifest && stamp.validator.as_deref() == Some(VALIDATOR)
        }) {
            checks.trust(release, &self.inner.lifecycle.budget).await?;
        }
        Ok(())
    }

    pub(super) async fn revalidate_readback(
        &self,
        spec: &OperationRequest,
        operation: &Operation,
        retained: Retained<ReleasePin>,
    ) -> Result<Retained<ReleasePin>> {
        let lifecycle = &self.inner.lifecycle;
        crate::progress::phase(spec.dataset(), spec.operation(), "validation", async {
            #[cfg(feature = "test-util")]
            work_started("validation");
            #[cfg(test)]
            self.inner
                .routine_checks
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut checks = verification::Checks::routine(self.inner.objects.clone());
            if retained.dataset.related.is_some() {
                // The shared graph walk reads each exact manifest once and runs
                // the current checker over the captured semantic input graph.
                verification::verify_all(&[&retained], &lifecycle.budget, &mut checks).await?;
            } else {
                let memory = Arc::new(
                    lifecycle
                        .budget
                        .reserve(lifecycle.budget.limits.max_metadata_bytes)?,
                );
                let decoded: DatasetPin = inputs::read_manifest(
                    &self.inner.objects,
                    &retained.manifest,
                    &lifecycle.budget,
                    &mut 0,
                    &memory,
                )
                .await?;
                if decoded != retained.dataset {
                    return Err(Error::refusal(
                        "release_mismatch",
                        "readback manifest differs from its retained result",
                    ));
                }
                drop(decoded);
                drop(memory);
                verify_local_release(&retained, &lifecycle.budget, &mut checks).await?;
            }
            Ok::<_, Error>(())
        })
        .await?;
        crate::progress::phase(spec.dataset(), spec.operation(), "readback", async {
            #[cfg(feature = "test-util")]
            work_started("readback");
            let lock = self.lock(spec.dataset()).await;
            let _guard = lock.lock().await;
            let view = lifecycle.open(spec.dataset()).await?;
            self.check_completion(&view, operation, spec.operation())
                .await?;
            let key = format!("readback/{}", spec.operation());
            let current = lifecycle
                .get::<ValidatedReadback>(&view, &key)
                .await?
                .ok_or_else(|| Error::refusal("corrupt_phase", "retained readback disappeared"))?;
            if current.release != retained.value {
                return Err(Error::refusal(
                    "phase_generation_conflict",
                    "readback generation changed during routine qualification",
                ));
            }
            if !current.current() {
                let records = completion_records(
                    spec.dataset(),
                    &view,
                    &key,
                    &ValidatedReadback::stamp(&current.release),
                    retained.protections.as_ref(),
                )?;
                lifecycle.commit(&view, records).await?;
            }
            Ok(retained)
        })
        .await
    }
}

#[cfg(all(test, feature = "test-util"))]
mod tests;

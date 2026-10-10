//! Local forward certificates feed the shared comparison without endpoint payload scans.

use super::*;

#[cfg(test)]
mod tests;

// Bound planning latency and retained tiny/empty transitions as well as byte size.
// This conservative safety limit is not a measured crossover point; longer
// histories explicitly use complete endpoint comparison.
pub(super) const MAX_LINKS: usize = 64;

pub(super) enum Forward {
    Eligible(Option<Box<DataFrame>>),
    Fallback(ComparisonFallback),
}

fn invalid() -> Error {
    Error::refusal(
        "invalid_changes",
        "qualified changes have inconsistent source continuity",
    )
}

fn charge(total: &mut usize, additional: usize, budget: &Budget) -> bool {
    *total = total.saturating_add(additional);
    *total <= budget.limits.max_metadata_bytes
}

impl Engine {
    pub(super) async fn forward_changes(
        &self,
        view: &View,
        endpoints: (&ReleasePin, &ReleasePin),
        canonical: (&TablePin, &TablePin),
        native: &iceberg::table::Table,
        output: (&Fields, bool),
        max_links: usize,
    ) -> Result<Forward> {
        let (before, after) = endpoints;
        if before.dataset.dataset != after.dataset.dataset {
            return Ok(Forward::Fallback(ComparisonFallback::NotForward));
        }
        let budget = &self.inner.lifecycle.budget;
        let rows = self.inner.lifecycle.rows(view).await?;
        let before_key = format!("result/{}", before.id);
        let after_key = format!("result/{}", after.id);
        let Some(before_revision) = rows.revision(&before_key) else {
            return Ok(Forward::Fallback(ComparisonFallback::HistoryNotQualified));
        };
        let Some(after_revision) = rows.revision(&after_key) else {
            return Ok(Forward::Fallback(ComparisonFallback::HistoryNotQualified));
        };
        if before_revision >= after_revision {
            return Ok(Forward::Fallback(ComparisonFallback::NotForward));
        }
        let mut bytes = 0;
        if !charge(&mut bytes, rows.record_bytes(&after_key), budget) {
            return Ok(Forward::Fallback(ComparisonFallback::HistoryLimit));
        }
        let mut cursor = after.version_ref();
        let target = before.version_ref();
        let mut selected = changes::change_selection(&rows, &cursor, budget)?;
        if selected.metadata != canonical.1.metadata
            || &selected.logical_metadata != canonical.1.logical_metadata()
        {
            return Err(invalid());
        }
        let mut pins = Vec::new();
        let mut memory = Vec::new();
        let mut links = 0;
        while cursor != target {
            if links == max_links
                || !charge(
                    &mut bytes,
                    rows.record_bytes(&format!("change-certificate/{}", cursor.version)),
                    budget,
                )
            {
                return Ok(Forward::Fallback(ComparisonFallback::HistoryLimit));
            }
            let Some((certificate, certificate_memory)) =
                changes::local_certificate(view, &rows, &cursor, &selected, budget)?
            else {
                return Ok(Forward::Fallback(ComparisonFallback::HistoryNotQualified));
            };
            let predecessor = certificate.before.clone();
            let predecessor_key = format!("result/{}", predecessor.version);
            if !charge(&mut bytes, rows.record_bytes(&predecessor_key), budget)
                || !charge(
                    &mut bytes,
                    usize::try_from(certificate.descriptor.bytes).unwrap_or(usize::MAX),
                    budget,
                )
            {
                return Ok(Forward::Fallback(ComparisonFallback::HistoryLimit));
            }
            // Source identities and causal revisions come from retained catalog
            // receipts. Intermediate complete-state availability is not required.
            let previous = changes::change_selection(&rows, &predecessor, budget)?;
            if previous.revision >= selected.revision {
                return Err(invalid());
            }
            let descriptor_memory = Arc::new(
                budget.reserve(
                    (certificate.descriptor.bytes as usize)
                        .saturating_mul(8)
                        .saturating_add(4096),
                )?,
            );
            let applied: AppliedChangeSet =
                inputs::read_manifest(&certificate.descriptor, budget, &mut 0, &descriptor_memory)
                    .await?;
            changes::check_applied(&applied, &(&certificate).into())?;
            if let Some(source) = applied.source {
                if source.after_metadata != selected.metadata
                    || source.before_metadata != previous.metadata
                {
                    return Err(invalid());
                }
                for pin in [&source.removed, &source.added] {
                    if !charge(
                        &mut bytes,
                        usize::try_from(pin.metadata.bytes).unwrap_or(usize::MAX),
                        budget,
                    ) {
                        return Ok(Forward::Fallback(ComparisonFallback::HistoryLimit));
                    }
                }
                pins.push(source);
            } else if selected.logical_metadata != previous.logical_metadata {
                // A receipt-only hop may share a verified physical replacement,
                // but cannot silently bridge a different logical source.
                return Err(invalid());
            }
            memory.extend([certificate_memory, descriptor_memory]);
            links += 1;
            cursor = predecessor;
            selected = previous;
            if cursor != target && selected.revision <= before_revision {
                return Ok(Forward::Fallback(ComparisonFallback::NotForward));
            }
        }
        if selected.metadata != canonical.0.metadata
            || &selected.logical_metadata != canonical.0.logical_metadata()
        {
            return Err(invalid());
        }
        // Eligibility is complete before opening delta table metadata or payloads.
        // Keep decoded descriptors and parsed native metadata admitted through
        // the last stream owner, rather than only through this planning call.
        let context = self.context();
        let expected_schema = storage::identity::public_schema(native)?;
        let public_rules = |schema: &iceberg::spec::Schema| {
            schema
                .as_struct()
                .fields()
                .iter()
                .filter(|field| !storage::identity::hidden(&field.name))
                .cloned()
                .collect::<Vec<_>>()
        };
        let expected_rules = public_rules(native.metadata().current_schema());
        let mut frames = Vec::new();
        for source in pins {
            for (pin, weight) in [(source.removed, -1_i64), (source.added, 1_i64)] {
                let table_memory = Arc::new(
                    budget.reserve(
                        (pin.metadata.bytes as usize)
                            .saturating_mul(8)
                            .saturating_add(4096),
                    )?,
                );
                let table = storage::open_pin(&pin).await?;
                storage::identity::has_origins(table.metadata().current_schema())?;
                if storage::identity::public_schema(&table)? != expected_schema
                    || public_rules(table.metadata().current_schema()) != expected_rules
                {
                    return Err(invalid());
                }
                if pin.rows != 0 {
                    let provider = if output.1 {
                        crate::query::occurrences(table, budget.clone(), Some(pin.rows))?
                    } else {
                        crate::query::table(table, budget.clone(), Some(pin.rows))?
                    };
                    frames.push(signed_frame(
                        context.read_table(provider)?,
                        output.0,
                        output.1,
                        weight,
                    )?);
                    memory.push(table_memory);
                }
            }
        }
        Ok(Forward::Eligible(if frames.is_empty() {
            None
        } else {
            Some(Box::new(
                context.read_table(crate::query::sequence(frames, memory)?)?,
            ))
        }))
    }
}

//! Export pin lookup and stream reading entrypoints.

use super::{ExportOperation, check_pin, check_result, describe_schema, invalid_export, remote};
use crate::{Error, Result, engine::Engine, export::ExportPin, model::Id};
use datafusion::physical_plan::SendableRecordBatchStream;

impl Engine {
    /// Resolves a channel once to its catalog-selected immutable export.
    ///
    /// # Errors
    /// Refuses an unknown channel or inconsistent retained selection metadata.
    pub async fn latest_export(&self, dataset: &Id, channel: &Id) -> Result<ExportPin> {
        self.lookup_export(dataset, &format!("export-channel/{channel}"))
            .await
    }

    /// Resolves an export's immutable result without advancing its channel.
    ///
    /// # Errors
    /// Refuses an unknown export or inconsistent retained selection metadata.
    pub async fn export_pin(&self, dataset: &Id, operation: &Id) -> Result<ExportPin> {
        self.lookup_export(dataset, &format!("export-result/{operation}"))
            .await
    }

    /// Streams an exact retained primitive export with configured object credentials.
    ///
    /// The pin fixes values and disclosure independently of a later channel selection.
    /// Ordinary reads check availability and schema, without a full payload digest pass.
    ///
    /// # Errors
    /// Refuses inconsistent pins, unavailable objects, unqualified scope, or exhausted capacity.
    pub async fn read_export(&self, pin: &ExportPin) -> Result<SendableRecordBatchStream> {
        let memory = self.request_memory(pin)?;
        let pin = pin.clone();
        self.admit_stream(memory, move |engine| async move {
            let retained = engine
                .load_export_pin(&pin.dataset, &format!("export-result/{}", pin.id))
                .await?;
            if retained != pin {
                return Err(invalid_export());
            }
            engine.open_export_stream(&retained).await
        })
        .await
    }

    /// Streams a catalog-retained export by its immutable operation identity.
    ///
    /// # Errors
    /// Refuses unknown or inconsistent exports, unavailable objects, or exhausted capacity.
    pub async fn read_export_id(
        &self,
        dataset: &Id,
        operation: &Id,
    ) -> Result<SendableRecordBatchStream> {
        let memory = self.request_memory(&(dataset, operation))?;
        let dataset = dataset.clone();
        let operation = operation.clone();
        self.admit_stream(memory, move |engine| async move {
            let pin = engine
                .load_export_pin(&dataset, &format!("export-result/{operation}"))
                .await?;
            engine.open_export_stream(&pin).await
        })
        .await
    }

    async fn open_export_stream(&self, pin: &ExportPin) -> Result<SendableRecordBatchStream> {
        let stream = self
            .inner
            .objects
            .read_flat_parquet(&pin.object, &self.inner.lifecycle.budget)
            .await?;
        if describe_schema(&stream.schema())? != pin.fields {
            return Err(invalid_export());
        }
        Ok(stream)
    }

    async fn lookup_export(&self, dataset: &Id, key: &str) -> Result<ExportPin> {
        let memory = self.request_memory(&(dataset, key))?;
        let dataset = dataset.clone();
        let key = key.to_owned();
        self.admit_work(memory, move |engine| async move {
            engine.load_export_pin(&dataset, &key).await
        })
        .await
    }

    pub(super) async fn load_export_pin(&self, dataset: &Id, key: &str) -> Result<ExportPin> {
        let lock = self.lock(dataset).await;
        let _guard = lock.lock().await;
        let view = self.read_view(dataset).await?;
        let pin: ExportPin = self
            .inner
            .lifecycle
            .get(&view, key)
            .await?
            .ok_or_else(|| Error::refusal("no_export", "no completed export is selected"))?;
        if pin.dataset != *dataset {
            return Err(invalid_export());
        }
        if key != format!("export-result/{}", pin.id) {
            let result: ExportPin = self
                .inner
                .lifecycle
                .get(&view, &format!("export-result/{}", pin.id))
                .await?
                .ok_or_else(invalid_export)?;
            if result != pin {
                return Err(invalid_export());
            }
        }
        check_pin(&pin)?;
        let operation: ExportOperation = self
            .inner
            .lifecycle
            .get(&view, &format!("export-operation/{}", pin.id))
            .await?
            .ok_or_else(invalid_export)?;
        check_result(&pin, &operation)?;
        self.check_export_backend(&operation.source, &operation.request.policy)?;
        self.inner
            .objects
            .validate_retained_path(&pin.object.path)?;
        for child in pin.child_tables.values() {
            self.inner
                .objects
                .validate_retained_path(&child.object.path)?;
        }
        if !self.inner.retained.is_local() {
            remote::check_pin_binding(
                &pin,
                &operation,
                self.inner.lifecycle.rows(&view).await?.as_ref(),
                Some(&self.inner.lifecycle.budget),
            )?;
        }
        let expected_key = if key.starts_with("export-result/") {
            format!("export-result/{}", pin.id)
        } else {
            format!("export-channel/{}", operation.request.channel)
        };
        if key != expected_key {
            return Err(invalid_export());
        }
        Ok(pin)
    }
}

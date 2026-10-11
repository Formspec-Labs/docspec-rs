//! Startup-bound catalog generations; native commits keep their physical identifiers.
// Rust guideline compliant 2026-02-21

use crate::{Error, Result};
use iceberg::{Catalog, Namespace, NamespaceIdent, TableIdent};
use std::collections::HashMap;

#[derive(Debug, Default)]
pub(super) struct Scope {
    root: Option<NamespaceIdent>,
}

impl Scope {
    pub(super) fn from_property(value: Option<String>) -> Result<Self> {
        let Some(value) = value else {
            return Ok(Self::default());
        };
        let generation = uuid::Uuid::parse_str(&value).map_err(|_| invalid_config())?;
        if generation.to_string() != value {
            return Err(invalid_config());
        }
        Ok(Self {
            root: Some(NamespaceIdent::new(format!(
                "docspec_generation_{}",
                generation.simple()
            ))),
        })
    }

    // Logical DocSpec namespaces are flat. Already-physical identifiers are
    // accepted only for this exact root, as required by native Transaction calls.
    pub(super) fn physical(&self, namespace: &NamespaceIdent) -> iceberg::Result<NamespaceIdent> {
        let Some(root) = &self.root else {
            return Ok(namespace.clone());
        };
        match namespace.as_ref().as_slice() {
            [logical] if !logical.is_empty() && !logical.starts_with("docspec_generation_") => {
                NamespaceIdent::from_strs([root[0].as_str(), logical.as_str()])
            }
            [generation, logical]
                if generation == &root[0]
                    && !logical.is_empty()
                    && !logical.starts_with("docspec_generation_") =>
            {
                Ok(namespace.clone())
            }
            _ => Err(invalid_scope()),
        }
    }

    fn logical(&self, namespace: &NamespaceIdent) -> iceberg::Result<NamespaceIdent> {
        let Some(root) = &self.root else {
            return Ok(namespace.clone());
        };
        match namespace.as_ref().as_slice() {
            [generation, logical]
                if generation == &root[0]
                    && !logical.is_empty()
                    && !logical.starts_with("docspec_generation_") =>
            {
                Ok(NamespaceIdent::new(logical.clone()))
            }
            _ => Err(invalid_scope()),
        }
    }

    pub(super) fn physical_table(&self, table: &TableIdent) -> iceberg::Result<TableIdent> {
        Ok(TableIdent::new(
            self.physical(table.namespace())?,
            table.name().to_owned(),
        ))
    }

    pub(super) fn logical_table(&self, table: &TableIdent) -> iceberg::Result<TableIdent> {
        Ok(TableIdent::new(
            self.logical(table.namespace())?,
            table.name().to_owned(),
        ))
    }

    pub(super) fn check_commit(&self, table: &TableIdent) -> iceberg::Result<()> {
        if self.physical_table(table)? != *table {
            return Err(invalid_scope());
        }
        Ok(())
    }

    pub(super) fn namespace(&self, namespace: Namespace) -> iceberg::Result<Namespace> {
        Ok(Namespace::with_properties(
            self.logical(namespace.name())?,
            namespace.properties().clone(),
        ))
    }

    pub(super) async fn list_namespaces(
        &self,
        catalog: &dyn Catalog,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        let parent = match parent {
            Some(parent) => Some(self.physical(parent)?),
            None => self.root.clone(),
        };
        // The maintained REST catalog owns pagination. Do not enumerate other
        // generations and then filter them, or silently drop unknown children.
        let namespaces = match catalog.list_namespaces(parent.as_ref()).await {
            Ok(namespaces) => namespaces,
            Err(error)
                if self.root.is_some()
                    && parent == self.root
                    && error.kind() == iceberg::ErrorKind::NamespaceNotFound =>
            {
                Vec::new()
            }
            Err(error) => return Err(error),
        };
        namespaces
            .into_iter()
            .map(|namespace| self.logical(&namespace))
            .collect()
    }

    pub(super) async fn create_namespace(
        &self,
        catalog: &dyn Catalog,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        let physical = self.physical(namespace)?;
        if let Some(root) = &self.root
            && !catalog.namespace_exists(root).await?
        {
            match catalog.create_namespace(root, HashMap::new()).await {
                Ok(_) => {}
                Err(error) if error.kind() == iceberg::ErrorKind::NamespaceAlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        self.namespace(catalog.create_namespace(&physical, properties).await?)
    }
}

fn invalid_config() -> Error {
    Error::refusal(
        "remote_config",
        "namespace generation must be a canonical UUID",
    )
}

fn invalid_scope() -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::DataInvalid,
        "catalog identifier is outside the configured namespace generation",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        budget::{Budget, Limits},
        catalog::{Lifecycle, lifecycle_ident},
        model::Id,
        storage,
    };
    use std::sync::Arc;

    fn scoped(inner: Arc<dyn Catalog>, generation: &str) -> Arc<dyn Catalog> {
        Arc::new(super::super::RemoteCatalog {
            inner,
            scope: Scope::from_property(Some(generation.into())).unwrap(),
        })
    }

    #[test]
    fn foreign_physical_identifiers_and_logical_commits_refuse() {
        let scope =
            Scope::from_property(Some("00000000-0000-4000-8000-000000000001".into())).unwrap();
        let logical = TableIdent::from_strs(["d_owner", "lifecycle"]).unwrap();
        let physical = scope.physical_table(&logical).unwrap();
        assert_eq!(scope.logical_table(&physical).unwrap(), logical);
        assert_eq!(scope.physical_table(&physical).unwrap(), physical);
        assert!(scope.check_commit(&physical).is_ok());
        assert!(scope.check_commit(&logical).is_err());
        assert!(
            scope
                .physical(
                    &NamespaceIdent::from_strs(["docspec_generation_other", "d_owner"]).unwrap()
                )
                .is_err()
        );
        assert!(scope.physical(&scope.root.clone().unwrap()).is_err());
        assert!(Scope::from_property(Some("not-a-uuid".into())).is_err());
    }

    #[tokio::test]
    async fn restored_logical_identity_keeps_native_commits_in_each_generation() {
        let root = tempfile::tempdir().unwrap();
        let inner = storage::local_catalog(root.path()).await.unwrap();
        let first = scoped(inner.clone(), "00000000-0000-4000-8000-000000000001");
        let second = scoped(inner, "00000000-0000-4000-8000-000000000002");
        let budget = Budget::new(Limits {
            compute_threads: 1,
            memory_bytes: 128 << 20,
            max_batch_bytes: 4 << 20,
            scratch_bytes: 16 << 20,
            max_metadata_bytes: 1 << 20,
            ..Limits::default()
        })
        .unwrap();
        let old = Lifecycle::new(first.clone(), budget.clone());
        let replacement = Lifecycle::new(second.clone(), budget);
        let dataset = Id::new("namespace-restore").unwrap();
        let logical = lifecycle_ident(&dataset);
        let old_view = old.open(&dataset).await.unwrap();
        let old_view = old
            .commit(&old_view, vec![("value".into(), serde_json::json!(1))])
            .await
            .unwrap();
        let original_metadata = old_view
            .table
            .metadata_location_result()
            .unwrap()
            .to_owned();
        // Real provider requires detachment before reusing a native table UUID.
        first.drop_table(&logical).await.unwrap();
        second
            .create_namespace(logical.namespace(), HashMap::new())
            .await
            .unwrap();
        second
            .register_table(&logical, original_metadata.clone())
            .await
            .unwrap();
        let new_view = replacement.open(&dataset).await.unwrap();
        assert_eq!(old_view.identifier(), &logical);
        assert_eq!(new_view.identifier(), &logical);
        assert_ne!(old_view.table.identifier(), new_view.table.identifier());
        assert_eq!(
            old_view.table.metadata().uuid(),
            new_view.table.metadata().uuid()
        );
        assert_eq!(
            new_view.table.metadata_location_result().unwrap(),
            original_metadata
        );
        assert_eq!(
            first.list_namespaces(None).await.unwrap(),
            vec![logical.namespace().clone()]
        );
        assert_eq!(
            second.list_tables(logical.namespace()).await.unwrap(),
            vec![logical.clone()]
        );
        assert!(
            second
                .load_table(old_view.table.identifier())
                .await
                .is_err()
        );
        // The detached old native identifier cannot commit after restoration.
        // No request is retargeted to the replacement generation.
        let advanced_new = replacement
            .commit(&new_view, vec![("value".into(), serde_json::json!(3))])
            .await
            .unwrap();
        assert_eq!(
            replacement
                .get::<i64>(&advanced_new, "value")
                .await
                .unwrap(),
            Some(3)
        );
        assert_eq!(old.get::<i64>(&old_view, "value").await.unwrap(), Some(1));
        assert!(
            old.commit(&old_view, vec![("value".into(), serde_json::json!(4))])
                .await
                .is_err()
        );
        assert!(first.load_table(&logical).await.is_err());
        assert_ne!(
            original_metadata,
            advanced_new.table.metadata_location_result().unwrap()
        );
    }
}

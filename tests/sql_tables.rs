//! Local native-catalog fixtures specify retries without application bookkeeping.
// Rust guideline compliant 2026-02-21
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use docspec::{
    budget::{Budget, Limits},
    job::tables::{AppendWork, append_with_retry, purge_if_current},
};
use iceberg::{
    Catalog, NamespaceIdent, TableCreation, TableIdent,
    spec::{DataFile, FormatVersion, NestedField, PrimitiveType, Schema, Type},
    table::Table,
};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

async fn setup(root: &std::path::Path) -> (Arc<dyn Catalog>, Table, Arc<Budget>) {
    let catalog = docspec::storage::local_catalog(root).await.unwrap();
    let namespace = NamespaceIdent::new("archive_fixture".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let table = create(
        catalog.as_ref(),
        &TableIdent::new(namespace, "events".into()),
        &root.join("events"),
    )
    .await;
    let budget = Budget::new(Limits {
        compute_threads: 2,
        memory_bytes: 32 << 20,
        batch_rows: 64,
        max_batch_bytes: 2 << 20,
        max_metadata_bytes: 2 << 20,
        scratch_bytes: 8 << 20,
    })
    .unwrap();
    (catalog, table, Arc::new(budget))
}
async fn create(catalog: &dyn Catalog, ident: &TableIdent, path: &std::path::Path) -> Table {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .with_identifier_field_ids([1])
        .build()
        .unwrap();
    catalog
        .create_table(
            ident.namespace(),
            TableCreation::builder()
                .name(ident.name().to_owned())
                .schema(schema)
                .location(path.to_string_lossy().into_owned())
                .format_version(FormatVersion::V2)
                .properties([
                    ("gc.enabled".into(), "true".into()),
                    ("commit.retry.num-retries".into(), "0".into()),
                ])
                .build(),
        )
        .await
        .unwrap()
}
async fn files(table: &Table, budget: &Budget, ids: Vec<i64>) -> docspec::Result<Vec<DataFile>> {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))])?;
    docspec::storage::write_batches_with_layout(
        table,
        futures::stream::iter([Ok(batch)]),
        budget,
        docspec::storage::WriteLayout {
            file_bytes: 1 << 20,
            row_group_rows: 64,
            row_group_bytes: 1 << 20,
        },
    )
    .await
}
pub(super) async fn count(table: &Table, predicate: &str) -> i64 {
    if table.metadata().current_snapshot().is_none() {
        return 0;
    }
    let context = datafusion::prelude::SessionContext::new();
    context
        .register_table(
            "current",
            Arc::new(
                iceberg_datafusion::table::IcebergStaticTableProvider::try_new_from_table(
                    table.clone(),
                )
                .await
                .unwrap(),
            ),
        )
        .unwrap();
    let batches = context
        .sql(&format!(
            "SELECT count(*) AS n FROM current WHERE {predicate}"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn append_recomputes_work_after_conflict_and_reuses_unchanged_files() {
    let root = tempfile::tempdir().unwrap();
    let (catalog, table, budget) = setup(root.path()).await;
    assert_eq!(table.metadata().format_version(), FormatVersion::V2);
    assert_eq!(table.metadata().properties()["gc.enabled"], "true");
    let writes = Arc::new(AtomicUsize::new(0));
    let writer_count = writes.clone();
    let writer_catalog = catalog.clone();
    let writer_budget = budget.clone();
    let result = append_with_retry(
        catalog.as_ref(),
        table.identifier(),
        |_| async {
            Ok(AppendWork::Rows {
                identity: "one".into(),
                rows: vec![1],
            })
        },
        move |table, ids| {
            let calls = writer_count.clone();
            let catalog = writer_catalog.clone();
            let budget = writer_budget.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let ours = files(&table, &budget, ids).await?;
                let competing = files(&table, &budget, vec![99]).await?;
                docspec::storage::append(&table, catalog.as_ref(), competing).await?;
                Ok(ours)
            }
        },
    )
    .await
    .unwrap();
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert_eq!(result.retries, 1);
    assert_eq!(count(&result.table, "true").await, 2);
    assert_eq!(count(&result.table, "id=1").await, 1);
    let unchanged = result.table.metadata_location().map(str::to_owned);
    let again = append_with_retry(
        catalog.as_ref(),
        table.identifier(),
        |table| async move {
            assert_eq!(count(&table, "id=1").await, 1);
            Ok(AppendWork::<Vec<i64>>::Empty)
        },
        |_, _| async {
            panic!("empty work must not invoke a writer");
            #[allow(unreachable_code)]
            Ok(Vec::<DataFile>::new())
        },
    )
    .await
    .unwrap();
    assert_eq!(again.table.metadata_location(), unchanged.as_deref());
    assert!(again.files.is_empty());
}

#[tokio::test]
async fn changed_work_rewrites_once_instead_of_appending_stale_rows() {
    let root = tempfile::tempdir().unwrap();
    let (catalog, table, budget) = setup(root.path()).await;
    let writes = Arc::new(AtomicUsize::new(0));
    let calls = writes.clone();
    let cat = catalog.clone();
    let pool = budget.clone();
    let result = append_with_retry(
        catalog.as_ref(),
        table.identifier(),
        |table| async move {
            Ok(if count(&table, "id=1").await == 0 {
                AppendWork::Rows {
                    identity: "both".into(),
                    rows: vec![1, 2],
                }
            } else {
                AppendWork::Rows {
                    identity: "remaining".into(),
                    rows: vec![2],
                }
            })
        },
        move |table, ids| {
            let calls = calls.clone();
            let cat = cat.clone();
            let pool = pool.clone();
            async move {
                let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                let ours = files(&table, &pool, ids).await?;
                if first {
                    let other = files(&table, &pool, vec![1]).await?;
                    docspec::storage::append(&table, cat.as_ref(), other).await?;
                }
                Ok(ours)
            }
        },
    )
    .await
    .unwrap();
    assert_eq!(writes.load(Ordering::SeqCst), 2);
    assert_eq!(result.retries, 1);
    assert_eq!(count(&result.table, "true").await, 2);
    assert_eq!(count(&result.table, "id=1").await, 1);
    assert_eq!(count(&result.table, "id=2").await, 1);
}

#[tokio::test]
async fn fourth_conflict_refuses_and_non_conflict_errors_do_not_retry() {
    let root = tempfile::tempdir().unwrap();
    let (catalog, table, budget) = setup(root.path()).await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let calls = attempts.clone();
    let cat = catalog.clone();
    let pool = budget.clone();
    let writer_pool = budget.clone();
    let error = append_with_retry(
        catalog.as_ref(),
        table.identifier(),
        move |table| {
            let calls = calls.clone();
            let cat = cat.clone();
            let pool = pool.clone();
            async move {
                let attempt = calls.fetch_add(1, Ordering::SeqCst);
                let competing = files(&table, &pool, vec![99 + attempt as i64]).await?;
                docspec::storage::append(&table, cat.as_ref(), competing).await?;
                Ok(AppendWork::Rows {
                    identity: "unchanged".into(),
                    rows: vec![1],
                })
            }
        },
        move |table, ids| {
            let pool = writer_pool.clone();
            async move { files(&table, &pool, ids).await }
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), "catalog_conflict");
    assert_eq!(attempts.load(Ordering::SeqCst), 4);
    let before = attempts.load(Ordering::SeqCst);
    let calls = attempts.clone();
    let error = append_with_retry(
        catalog.as_ref(),
        table.identifier(),
        move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async {
                Ok(AppendWork::Rows {
                    identity: "bad".into(),
                    rows: (),
                })
            }
        },
        |_, _| async {
            Err(docspec::Error::refusal(
                "invalid_spec",
                "authored writer refusal",
            ))
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), "invalid_spec");
    assert_eq!(attempts.load(Ordering::SeqCst), before + 1);
}

#[tokio::test]
async fn purge_refuses_changed_metadata_or_replaced_uuid_before_deleting() {
    let root = tempfile::tempdir().unwrap();
    let (catalog, table, budget) = setup(root.path()).await;
    let new = docspec::storage::append(
        &table,
        catalog.as_ref(),
        files(&table, &budget, vec![1]).await.unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        purge_if_current(catalog.as_ref(), &table)
            .await
            .unwrap_err()
            .code(),
        "stale_authority"
    );
    assert_eq!(
        count(
            &catalog.load_table(table.identifier()).await.unwrap(),
            "true"
        )
        .await,
        1
    );
    catalog.drop_table(table.identifier()).await.unwrap();
    let replacement = create(
        catalog.as_ref(),
        table.identifier(),
        &root.path().join("replacement"),
    )
    .await;
    assert_ne!(replacement.metadata().uuid(), new.metadata().uuid());
    assert_eq!(
        purge_if_current(catalog.as_ref(), &new)
            .await
            .unwrap_err()
            .code(),
        "stale_authority"
    );
    assert!(catalog.table_exists(table.identifier()).await.unwrap());
    purge_if_current(catalog.as_ref(), &replacement)
        .await
        .unwrap();
    assert!(!catalog.table_exists(table.identifier()).await.unwrap());
}

#[tokio::test]
async fn replacement_uuid_never_reuses_the_previous_tables_files() {
    let root = tempfile::tempdir().unwrap();
    let (catalog, table, budget) = setup(root.path()).await;
    let writes = Arc::new(AtomicUsize::new(0));
    let calls = writes.clone();
    let cat = catalog.clone();
    let pool = budget.clone();
    let replacement = root.path().join("replacement");
    let result = append_with_retry(
        catalog.as_ref(),
        table.identifier(),
        |_| async {
            Ok(AppendWork::Rows {
                identity: "same-work".into(),
                rows: vec![1],
            })
        },
        move |table, ids| {
            let calls = calls.clone();
            let cat = cat.clone();
            let pool = pool.clone();
            let replacement = replacement.clone();
            async move {
                let ours = files(&table, &pool, ids).await?;
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    cat.drop_table(table.identifier()).await?;
                    create(cat.as_ref(), table.identifier(), &replacement).await;
                }
                Ok(ours)
            }
        },
    )
    .await
    .unwrap();
    assert_eq!(writes.load(Ordering::SeqCst), 2);
    assert_ne!(result.table.metadata().uuid(), table.metadata().uuid());
    assert_eq!(count(&result.table, "true").await, 1);
    let prefix = root.path().join("replacement");
    assert!(result.files.iter().all(|file| {
        file.file_path()
            .starts_with(prefix.to_string_lossy().as_ref())
    }));
}

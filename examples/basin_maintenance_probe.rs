//! Scratch-only Basin maintenance experiment; no application tables or policy.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_names_and_noncanonical_probe_names_refuse() {
        for name in ["captures", "docspec_m1_captures", "../raw", ""] {
            assert!(validate_namespace(name).is_err());
        }
        assert!(validate_namespace("docspec_m1_71969fc54a0f4f4eaa49d2dd9f4c2272").is_ok());
    }

    #[test]
    fn no_format_verdict_before_ten_complete_maintenance_observations() {
        assert_eq!(format_verdict(9, true, 1000, true), "incomplete");
        assert_eq!(format_verdict(10, true, 59999, true), "iceberg");
        assert_eq!(format_verdict(10, true, 60000, true), "dated_parquet");
        assert_eq!(format_verdict(10, false, 1000, true), "dated_parquet");
        assert_eq!(format_verdict(10, true, 1000, false), "incomplete");
    }
}

#[cfg(test)]
mod native_tests {
    use super::*;
    use iceberg::CatalogBuilder;

    #[tokio::test]
    async fn native_writer_commit_race_reuses_files_and_pushes_partition_filter() -> Result<()> {
        let root = tempfile::tempdir()?;
        let catalog = iceberg_catalog_sql::SqlCatalogBuilder::default()
            .uri(format!(
                "sqlite://{}?mode=rwc",
                root.path().join("catalog.db").display()
            ))
            .warehouse_location(root.path().join("warehouse").to_string_lossy())
            .sql_bind_style(iceberg_catalog_sql::SqlBindStyle::QMark)
            .with_storage_factory(Arc::new(iceberg::io::LocalFsStorageFactory))
            .load("m1", HashMap::new())
            .await?;
        let catalog: Arc<dyn Catalog> = Arc::new(catalog);
        let ns = NamespaceIdent::new("docspec_m1_71969fc54a0f4f4eaa49d2dd9f4c2272".into());
        catalog.create_namespace(&ns, HashMap::new()).await?;
        let ident = TableIdent::new(ns, "uncapped".into());
        let table = create_table(
            catalog.as_ref(),
            &ident,
            &root.path().join("table").to_string_lossy(),
        )
        .await?;
        let table = append(catalog.as_ref(), &table, files(&table, 0, 4, false).await?).await?;
        let args = Args::parse_from([
            "probe",
            "--namespace",
            "docspec_m1_71969fc54a0f4f4eaa49d2dd9f4c2272",
            "--output",
            root.path().join("out.json").to_str().unwrap(),
            "--scratch",
            root.path().join("scratch").to_str().unwrap(),
            "--day",
            "1",
            "--rows-per-day",
            "4",
        ]);
        let result = race(catalog.clone(), table, &args).await?;
        assert_eq!(result["server_snapshot_conflict"], true);
        assert_eq!(result["retries"], 1);
        let table = catalog.load_table(&ident).await?;
        let ctx = context(&args)?;
        ctx.register_table(
            "current",
            Arc::new(IcebergStaticTableProvider::try_new_from_table(table).await?),
        )?;
        assert_eq!(
            scalar(
                &ctx,
                "SELECT count(*) FROM current WHERE length(payload) > 0"
            )
            .await?,
            12
        );
        assert_eq!(
            scalar(&ctx, "SELECT count(*) FROM current WHERE day=2").await?,
            4
        );
        let plan = ctx
            .sql("SELECT item FROM current WHERE day=2")
            .await?
            .create_physical_plan()
            .await?;
        let plan = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        assert!(
            plan.contains("predicate:[") && !plan.contains("predicate:[]"),
            "{plan}"
        );
        Ok(())
    }
}

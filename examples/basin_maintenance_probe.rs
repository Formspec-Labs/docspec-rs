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
        let before_repeat = catalog.load_table(&ident).await?;
        assert!(race(catalog.clone(),before_repeat.clone(),&args).await.is_err());
        assert_eq!(catalog.load_table(&ident).await?.metadata_location(),before_repeat.metadata_location());
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

#[cfg(test)]
mod qualification_tests {
    use super::*;
    fn growth() -> Vec<Value> {
        (0..10).map(|day| json!({"namespace":"docspec_m1_71969fc54a0f4f4eaa49d2dd9f4c2272",
            "table":"uncapped","table_uuid":"fixture-table","day":day,"rows_per_day":4,
            "writer_profile":"engine-v1-uncapped","rows_equal":true,"data_files":day+1,"manifests":day+1,
            "maintenance_run":format!("basin-{day}"),"maintenance_evidence":{"source":"basin","completed":true,
            "run_id":format!("basin-{day}"),"table_uuid":"fixture-table"}})).collect()
    }
    #[test]
    fn projected_workload_must_cover_the_measured_growth_targets() -> Result<()> {
        let ns = "docspec_m1_71969fc54a0f4f4eaa49d2dd9f4c2272";
        let mut reports = growth();
        let projection = projection_for(ns,&reports)?;
        assert_eq!(projection["target_files"],3660);
        assert_eq!(projection["target_manifests"],3660);
        let mut projected = json!({"namespace":ns,"table":"projected","projected":true,
            "projection":projection,"data_files":1,"manifests":1,"rows_equal":true,
            "runner_os":"linux","runner_threads":4,"runner_memory_bytes":16_u64 << 30,"scan_ms":1000});
        reports.push(projected.clone());
        assert_eq!(decision_report(ns,&reports)?["capacity_decision"],"incomplete");
        projected["data_files"] = json!(3660);
        projected["manifests"] = json!(3660);
        reports.pop(); reports.push(projected);
        assert_eq!(decision_report(ns,&reports)?["capacity_decision"],"iceberg");
        reports.last_mut().unwrap()["scan_ms"] = json!(60000);
        assert_eq!(decision_report(ns,&reports)?["capacity_decision"],"dated_parquet");
        reports[0]["namespace"] = json!("different");
        assert!(projection_for(ns,&reports).is_err());
        Ok(())
    }
    #[test]
    fn compaction_codec_comes_from_a_file_the_writer_did_not_create() {
        let current = std::collections::BTreeSet::from(["old.parquet".into(),"compacted.parquet".into()]);
        let writers = std::collections::BTreeSet::from(["old.parquet".into()]);
        assert_eq!(compacted_file(&current,&writers).as_deref(),Some("compacted.parquet"));
        assert!(compacted_file(&writers,&writers).is_none());
    }
}

#[cfg(test)]
mod layout_tests {
    use super::*;
    use parquet::arrow::async_reader::AsyncFileReader;

    #[tokio::test]
    async fn both_arms_use_the_engine_writer_and_their_pinned_native_layout() -> Result<()> {
        let root = tempfile::tempdir()?;
        let catalog = docspec::storage::local_catalog(root.path()).await?;
        let ns = NamespaceIdent::new("m1_layout".into());
        catalog.create_namespace(&ns,HashMap::new()).await?;
        for capped in [true,false] {
            let name = if capped {"capped"} else {"uncapped"};
            let ident = TableIdent::new(ns.clone(),name.into());
            let table = create_table(catalog.as_ref(),&ident,&root.path().join(name).to_string_lossy()).await?;
            assert!(files(&table,0,5000,!capped).await.is_err());
            let written = files(&table,0,5000,capped).await?;
            assert!(!written.is_empty());
            let mut groups = Vec::new();
            for file in &written {
                let mut reader = iceberg::arrow::ArrowFileReader::new(
                    iceberg::io::FileMetadata {size:file.file_size_in_bytes()},
                    table.file_io().new_input(file.file_path())?.reader().await?);
                let footer = reader.get_metadata(None).await?;
                groups.extend(footer.row_groups().iter().map(|g|g.num_rows()));
            }
            assert_eq!(groups.iter().sum::<i64>(),5000);
            if capped {assert!(groups.len()>1 && groups.iter().all(|n|*n<=2048));}
            else {assert_eq!(groups,vec![5000]);}
        }
        Ok(())
    }

    #[test]
    fn failure_reports_keep_stage_and_refuse_to_print_dynamic_secrets() {
        let error = anyhow::anyhow!(String::from("Bearer never-report-this-value"));
        let report = failure_report(&error,"Observe");
        assert_eq!(report["stage"],"Observe");
        assert_eq!(report["status"],"failed");
        assert!(!report.to_string().contains("never-report-this-value"));
    }
}

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
            "table":"growth","table_uuid":"fixture-table","day":day,"rows_per_day":4,
            "writer_profile":"engine-v1-uncapped","rows_equal":true,"data_files":day+1,"manifests":day+1,"data_bytes":(day+1)*315000,"binary_blake3":"fixture",
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
            "projection":projection,"rows_per_day":4,"rows":14640,"data_bytes":1152900000,"writer_profile":"engine-v1-uncapped","binary_blake3":"fixture","data_files":1,"manifests":1,"rows_equal":true,
            "runner_os":"linux","runner_threads":4,"runner_memory_bytes":16_u64 << 30,"scan_ms":1000});
        reports.push(projected.clone());
        assert_eq!(decision_report(ns,&reports)?["capacity_decision"],"incomplete");
        projected["data_files"] = json!(3660);
        projected["manifests"] = json!(3660);
        reports.pop(); reports.push(projected);
        assert_eq!(decision_report(ns,&reports)?["capacity_decision"],"iceberg");
        reports.last_mut().unwrap()["rows_per_day"] = json!(1);
        assert_eq!(decision_report(ns,&reports)?["capacity_decision"],"incomplete");
        reports.last_mut().unwrap()["rows_per_day"] = json!(4);
        reports.last_mut().unwrap()["rows"] = json!(4);
        assert_eq!(decision_report(ns,&reports)?["capacity_decision"],"incomplete");
        reports.last_mut().unwrap()["rows"] = json!(14640);
        reports.last_mut().unwrap()["data_bytes"] = json!(1);
        assert_eq!(decision_report(ns,&reports)?["capacity_decision"],"incomplete");
        reports.last_mut().unwrap()["data_bytes"] = json!(1152900000);
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

#[cfg(test)]
mod metadata_tests {
    use super::*;
    #[tokio::test]
    async fn explicit_small_row_groups_cannot_undercharge_writer_metadata() -> Result<()> {
        let root = tempfile::tempdir()?;
        let catalog = docspec::storage::local_catalog(root.path()).await?;
        let ns = NamespaceIdent::new("m1_metadata".into());
        catalog.create_namespace(&ns,HashMap::new()).await?;
        let table = create_table(catalog.as_ref(),&TableIdent::new(ns,"uncapped".into()),
            &root.path().join("table").to_string_lossy(),200).await?;
        let budget = docspec::budget::Budget::new(docspec::budget::Limits {
            compute_threads:1,memory_bytes:1 << 30,scratch_bytes:1 << 20,
            batch_rows:65536,max_batch_bytes:64 << 20,max_metadata_bytes:32 << 10,
        })?;
        let input = futures::stream::iter([Ok(batch(&table,0,0,200)?)]);
        let error = docspec::storage::write_batches_with_layout(&table,input,&budget,
            docspec::storage::WriteLayout {file_bytes:128 << 20,row_group_rows:1,row_group_bytes:32 << 20}).await.unwrap_err();
        assert_eq!(error.code(),"metadata_capacity");
        Ok(())
    }
}

#[cfg(test)]
mod cap_qualification_tests {
    use super::*;
    fn trials() -> Vec<Value> {
        let ns="docspec_m1_71969fc54a0f4f4eaa49d2dd9f4c2272";
        let mut values=Vec::new();
        for day in 0..3 {for arm in 0..2 {
            let table=if arm==0 {"capped"} else {"uncapped"};
            let profile=if arm==0 {"engine-v1-capped"} else {"engine-v1-uncapped"};
            let cost=if arm==0 {1} else {10};
            values.push(json!({"namespace":ns,"table":table,"table_uuid":table,"writer_profile":profile,
                "binary_blake3":"fixture","cap_trial":true,"batches":[{"day":day,"commits":1,"work_list_rows":4,
                    "commit_ms":cost,"started_at_ns":100+day*2+arm,"committed_snapshot":1000+day}]}));
            values.push(json!({"namespace":ns,"table":table,"table_uuid":table,"writer_profile":profile,
                "binary_blake3":"fixture","cap_trial":true,"day":day,"rows_per_day":4,"rows":(day+1)*4,
                "rows_equal":true,"scan_ms":cost,"scan_started_at_ns":200+day*2+arm,"snapshot":1000+day}));
        }}
        values
    }
    #[test]
    fn cap_costs_require_equal_workload_snapshot_and_interleaved_scans() -> Result<()> {
        let ns="docspec_m1_71969fc54a0f4f4eaa49d2dd9f4c2272";
        let reports=trials();
        assert_eq!(cap_decision(ns,&reports)?["decision"],"keep_caps");
        for field in ["rows_per_day","rows","snapshot","table_uuid","binary_blake3"] {
            let mut bad=reports.clone();bad[1][field]=json!(99999);
            assert_eq!(cap_decision(ns,&bad)?["decision"],"incomplete","{field}");
        }
        let mut bad=reports;
        for (index,row) in bad.iter_mut().enumerate().filter(|(_,r)|r["scan_ms"].is_number()) {
            row["scan_started_at_ns"]=json!(if row["table"]=="capped" {index} else {100+index});
        }
        assert_eq!(cap_decision(ns,&bad)?["decision"],"incomplete");
        Ok(())
    }
}

#[cfg(test)]
mod observation_evidence_tests {
    use super::*;
    #[test]
    fn compactor_attribution_requires_same_generation_and_complete_successful_writes() -> Result<()> {
        let identity = json!({"namespace":"ns","table":"growth","table_uuid":"uuid","writer_profile":"engine-v1-uncapped","binary_blake3":"pin"});
        let mut previous = identity.clone();
        previous["rows_per_day"] = json!(4);
        previous["rows_equal"] = json!(true);
        previous["day"] = json!(0);
        previous["data_paths"] = json!(["old"]);
        let mut record = identity.clone();
        record["batches"] = json!([{"day":1,"commits":1,"work_list_rows":4,"committed_snapshot":7,"files":[{"path":"new","bytes":8,"rows":4}]}]);
        let known = writer_evidence(Some(&previous), &[record.clone()], &identity, 1, 4)?;
        assert!(known.contains("old") && known.contains("new"));
        for field in ["namespace","table","table_uuid","writer_profile","binary_blake3","rows_per_day","rows_equal"] {
            let mut bad = previous.clone();
            bad[field] = json!("wrong");
            assert!(writer_evidence(Some(&bad), &[record.clone()], &identity, 1, 4).is_err(), "{field}");
        }
        for field in ["namespace","table","table_uuid","writer_profile","binary_blake3"] {
            let mut bad = record.clone();
            bad[field] = json!("wrong");
            assert!(writer_evidence(Some(&previous), &[bad], &identity, 1, 4).is_err(), "{field}");
        }
        for files in [json!([]),json!([{"path":"new","bytes":8,"rows":3}]),json!([{"path":"new","bytes":0,"rows":4}])] {
            let mut bad = record.clone(); bad["batches"][0]["files"] = files;
            assert!(writer_evidence(Some(&previous), &[bad], &identity, 1, 4).is_err());
        }
        let mut noop = record;
        noop["batches"][0]["commits"] = json!(0);
        noop["batches"][0]["files"] = json!([]);
        noop["batches"][0]["work_list_rows"] = json!(0);
        assert!(writer_evidence(Some(&previous), &[noop], &identity, 1, 4).is_err());
        assert!(writer_evidence(Some(&previous), &[], &identity, -1, 4).is_err());
        Ok(())
    }
}

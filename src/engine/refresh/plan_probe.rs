//! Inspects retained native join plans without executing payload scans.
// Rust guideline compliant 2026-02-21

use super::{invalid, join_queries, read_release};
use crate::{
    Error, Result,
    budget::{Budget, Limits},
    model::{RefreshSpec, RefreshTransform},
    query, storage,
};
use datafusion::physical_plan::{ExecutionPlan, displayable, joins::HashJoinExec};
use serde::de::DeserializeOwned;
use std::{fs::File, io::Read, path::Path, sync::Arc};

#[tokio::test]
#[ignore = "requires DOCSPEC_JOIN_PLAN_RUN pointing to a retained Comments run"]
async fn retained_comments_join_physical_plans() -> Result<()> {
    let directory = std::env::var_os("DOCSPEC_JOIN_PLAN_RUN").ok_or_else(|| {
        Error::refusal(
            "plan_probe_input",
            "set DOCSPEC_JOIN_PLAN_RUN to the retained run directory",
        )
    })?;
    let directory = Path::new(&directory);
    // The tiny execution configuration is read before its metadata limit is known.
    let mut config_bytes = 64 << 10;
    let limits: Limits = read_json(&directory.join("limits.json"), &mut config_bytes)?;
    let budget = Budget::new(limits)?;
    let memory = Arc::new(budget.reserve(budget.limits.max_metadata_bytes)?);
    let mut remaining = budget.limits.max_metadata_bytes;
    let request: RefreshSpec = read_json(
        &directory.join("comments-dockets.request.json"),
        &mut remaining,
    )?;
    let RefreshTransform::Join(join) = &request.transform else {
        return Err(invalid());
    };
    let context = budget.context();
    let mut bytes = budget.limits.max_metadata_bytes - remaining;
    println!("limits={:?}; join={:?}", budget.limits, join.join);
    for (alias, version) in [("join_left", &join.left), ("join_right", &join.right)] {
        let release = read_release(version, &budget, &mut bytes, &memory).await?;
        let dataset = release.dataset;
        let pin = dataset.groups.get("source").ok_or_else(invalid)?;
        println!(
            "input={alias}; dataset={}; version={}; rows={}; snapshot={:?}; schema_id={}; metadata={}",
            dataset.dataset,
            dataset.version,
            pin.rows,
            pin.snapshot,
            pin.schema_id,
            pin.metadata.path
        );
        let native = storage::open_pin(pin, &budget).await?;
        context.register_table(
            alias,
            query::occurrences(native, budget.clone(), Some(pin.rows))?,
        )?;
    }
    let (probes, main) = join_queries(&context, join, true).await?;
    for (side, sql) in probes {
        println!("query=cardinality_{side:?}; sql={sql}");
        let plan = context.sql(&sql).await?.create_physical_plan().await?;
        println!("{}", displayable(plan.as_ref()).indent(true));
        print_nodes(plan.as_ref(), "root")?;
    }
    println!("query=main; sql={main}");
    let plan = context.sql(&main).await?.create_physical_plan().await?;
    println!("{}", displayable(plan.as_ref()).indent(true));
    print_nodes(plan.as_ref(), "root")?;
    // Do not collect or execute these plans: the failed workload retains large payloads.
    Ok(())
}

fn read_json<T: DeserializeOwned>(path: &Path, remaining: &mut usize) -> Result<T> {
    let file = File::open(path)?;
    let bytes = usize::try_from(file.metadata()?.len()).map_err(|_| invalid())?;
    if bytes > *remaining {
        return Err(Error::refusal(
            "plan_probe_metadata",
            "retained metadata exceeds the declared limit",
        ));
    }
    let mut contents = Vec::with_capacity(bytes);
    file.take(bytes as u64 + 1).read_to_end(&mut contents)?;
    if contents.len() != bytes {
        return Err(invalid());
    }
    *remaining -= bytes;
    Ok(serde_json::from_slice(&contents)?)
}

fn print_nodes(plan: &dyn ExecutionPlan, path: &str) -> Result<()> {
    println!(
        "node={path}; name={}; statistics={:?}; schema={:?}",
        plan.name(),
        plan.partition_statistics(None)?,
        plan.schema()
    );
    if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        println!(
            "node={path}; join_type={:?}; partition_mode={:?}; null_equality={:?}; build_child={path}/0; probe_child={path}/1",
            join.join_type(),
            join.partition_mode(),
            join.null_equality()
        );
    }
    for (index, child) in plan.children().iter().enumerate() {
        print_nodes(child.as_ref(), &format!("{path}/{index}"))?;
    }
    Ok(())
}

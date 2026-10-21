//! Loopback HTTP access to the same application operations as the CLI.
//!
//! This is a local host interface. Public authentication and hosted deployment are
//! deliberately absent until the remote provider and disclosure gates are qualified.
// Rust guideline compliant 2026-02-21

use crate::{
    Engine, Error,
    evidence::EvidenceSpec,
    export::{ExportPin, ExportSpec},
    model::{
        Abandonment, BackfillSpec, CertifiedChanges, CompactionResult, CompactionSpec, Comparison,
        ComparisonIdentity, ComparisonOptions, ContinuousBackfillPlan, ContinuousBackfillSpec,
        CorrectionDecision, DependencyCorrection, GenerationSeal, Id, InputCoverage, ObjectRef,
        RefreshSpec, ReleasePin, RemovalPlan, RemovalStatus, RetentionInspection, RetentionPlan,
        Retirement, RunSpec, SchemaSpec, SuccessionProof, UpdateSpec, VersionRef,
    },
    restore::RestorePointRef,
};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::StreamExt;

mod deadline;
mod ipc;

/// Loopback protocol version with request deadlines and bound-listener readiness.
pub const PROTOCOL_VERSION: u32 = 1;

/// Builds a bounded local API over an existing execution host.
pub fn router(engine: Engine) -> Router {
    let router = Router::new()
        .route("/plan", post(plan))
        .route("/run", post(run))
        .route("/compare", post(compare))
        .route("/changes", post(changes))
        .route("/changes/inspect", post(inspect_changes))
        .route("/updates/plan", post(plan_update))
        .route("/updates/run", post(update))
        .route("/receipts/plan", post(plan_evidence))
        .route("/receipts/run", post(update_evidence))
        .route("/schemas/plan", post(plan_schema))
        .route("/schemas/run", post(evolve_schema))
        .route("/backfills/plan", post(plan_backfill))
        .route("/backfills/run", post(backfill))
        .route("/continuous-backfill", post(continuous_backfill))
        .route("/plan-continuous-backfill", post(plan_continuous_backfill))
        .route("/refreshes/plan", post(plan_refresh))
        .route("/refreshes/run", post(refresh))
        .route("/exports/run", post(export))
        .route("/compactions/run", post(compact))
        .route(
            "/datasets/{dataset}/lifecycle/compact",
            post(compact_lifecycle),
        )
        .route("/restore-points", post(create_restore_point))
        .route("/succession", post(succession))
        .route("/generations/seal", post(seal_generation))
        .route("/inputs/capture", post(capture_inputs))
        .route("/dependencies/corrections", post(correct_dependencies))
        .route("/retention/inspect", post(inspect_retention))
        .route("/retention/removal/plan", post(plan_removal))
        .route(
            "/retention/removal/export-stage/plan",
            post(plan_export_stage_removal),
        )
        .route(
            "/retention/removal/export-file/plan",
            post(plan_export_file_removal),
        )
        .route("/retention/exports/plan", post(plan_export_retirement))
        .route("/retention/exports/retire", post(retire_export))
        .route("/retention/removal/execute", post(remove_objects))
        .route(
            "/datasets/{dataset}/removals/{removal}",
            get(inspect_removal),
        )
        .route(
            "/datasets/{dataset}/removals/{removal}/resume",
            post(resume_removal),
        )
        .route("/retention/phases/plan", post(plan_phase_retirement))
        .route("/retention/phases/retire", post(retire_phases))
        .route("/retention/changes/plan", post(plan_change_retirement))
        .route("/retention/changes/retire", post(retire_changes))
        .route(
            "/retention/restore-points/plan",
            post(plan_restore_point_retirement),
        )
        .route(
            "/retention/restore-points/retire",
            post(retire_restore_points),
        )
        .route(
            "/retention/generations/plan",
            post(plan_generation_retirement),
        )
        .route("/retention/generations/retire", post(retire_generation))
        .route("/retention/grants/plan", post(plan_grant_retirement))
        .route("/retention/grants/retire", post(retire_grants))
        .route(
            "/retention/availability/plan",
            post(plan_availability_retirement),
        )
        .route("/retention/availability/retire", post(retire_availability))
        .route(
            "/retention/operations/plan",
            post(plan_operation_retirement),
        )
        .route("/retention/operations/retire", post(retire_operation))
        .route(
            "/retention/compactions/plan",
            post(plan_compaction_retirement),
        )
        .route("/retention/compactions/retire", post(retire_compaction))
        .route("/datasets/{dataset}", get(inspect))
        .route(
            "/datasets/{dataset}/authority/fence",
            post(fence_operations),
        )
        .route(
            "/datasets/{dataset}/operations/{operation}/abandon",
            post(abandon),
        )
        .route("/datasets/{dataset}/latest", get(latest))
        .route("/datasets/{dataset}/versions/{version}", get(release))
        .route(
            "/datasets/{dataset}/exports/channels/{channel}",
            get(latest_export),
        )
        .route("/datasets/{dataset}/exports/{operation}", get(export_pin))
        .route(
            "/datasets/{dataset}/exports/{operation}/arrow",
            get(read_export),
        )
        .route(
            "/datasets/{dataset}/versions/{version}/arrow",
            get(read_combined),
        )
        .route(
            "/datasets/{dataset}/versions/{version}/arrow/{group}",
            get(read_group),
        )
        .route(
            "/datasets/{dataset}/versions/{version}/evidence",
            get(read_evidence),
        )
        .route(
            "/datasets/{dataset}/versions/{version}/receipts",
            get(read_receipts),
        )
        .route(
            "/datasets/{dataset}/versions/{version}/occurrences",
            get(read_occurrences),
        );
    #[cfg(feature = "remote")]
    let router = router
        .route(
            "/retention/removal/remote/prepare",
            post(prepare_remote_removal),
        )
        .route(
            "/retention/removal/remote/{dataset}/{removal}/resume",
            post(resume_remote_removal),
        );
    #[cfg(feature = "remote")]
    let router = router.route("/restore-remote", post(restore_remote));
    router
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(axum::middleware::from_fn(deadline::apply))
        .with_state(engine)
}

struct HttpError(Error);

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ComparisonRequest {
    before: ReleasePin,
    after: ReleasePin,
    #[serde(default = "source_group")]
    group: String,
    // Omission preserves value-summary compatibility; streaming defaults to occurrences.
    options: Option<ComparisonOptions>,
}

fn source_group() -> String {
    "source".into()
}

async fn compare(
    State(engine): State<Engine>,
    Json(request): Json<ComparisonRequest>,
) -> Result<Json<Comparison>, HttpError> {
    let options = request.options.unwrap_or(ComparisonOptions {
        identity: ComparisonIdentity::Values,
        force_full: false,
    });
    Ok(Json(
        engine
            .compare_with_options(&request.before, &request.after, &request.group, &options)
            .await?,
    ))
}

async fn changes(
    State(engine): State<Engine>,
    Json(request): Json<ComparisonRequest>,
) -> Result<Response, HttpError> {
    let batches = engine
        .changes(
            &request.before,
            &request.after,
            &request.group,
            &request.options.unwrap_or_default(),
        )
        .await?;
    let stream = ipc::encode(batches, engine.read_budget())?.map(move |chunk| {
        // Retain workspace ownership until the response completes or is canceled.
        let _owner = &engine;
        chunk
    });
    Ok((
        [("content-type", "application/vnd.apache.arrow.stream")],
        Body::from_stream(stream),
    )
        .into_response())
}

async fn inspect_changes(
    State(engine): State<Engine>,
    Json(version): Json<VersionRef>,
) -> Result<Json<Option<CertifiedChanges>>, HttpError> {
    Ok(Json(engine.change_certificate(&version).await?))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RemovalRequest {
    dataset: Id,
    removal: Id,
    objects: Vec<ObjectRef>,
    reason: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportStageRemovalRequest {
    dataset: Id,
    removal: Id,
    operation: Id,
    reason: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportFileRemovalRequest {
    dataset: Id,
    removal: Id,
    operation: Id,
    file: Id,
    reason: String,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportRetirementRequest {
    dataset: Id,
    retirement: Id,
    operation: Id,
    reason: String,
}
async fn plan_export_file_removal(
    State(engine): State<Engine>,
    Json(request): Json<ExportFileRemovalRequest>,
) -> Result<Json<RemovalPlan>, HttpError> {
    Ok(Json(
        engine
            .plan_export_file_removal(
                &request.dataset,
                &request.removal,
                &request.operation,
                &request.file,
                &request.reason,
            )
            .await?,
    ))
}
async fn plan_export_retirement(
    State(engine): State<Engine>,
    Json(request): Json<ExportRetirementRequest>,
) -> Result<Json<RetentionPlan>, HttpError> {
    Ok(Json(
        engine
            .plan_export_retirement(
                &request.dataset,
                &request.retirement,
                &request.operation,
                &request.reason,
            )
            .await?,
    ))
}
async fn retire_export(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_export(&plan).await?))
}

async fn plan_export_stage_removal(
    State(engine): State<Engine>,
    Json(request): Json<ExportStageRemovalRequest>,
) -> Result<Json<RemovalPlan>, HttpError> {
    Ok(Json(
        engine
            .plan_export_stage_removal(
                &request.dataset,
                &request.removal,
                &request.operation,
                &request.reason,
            )
            .await?,
    ))
}

async fn plan_removal(
    State(engine): State<Engine>,
    Json(request): Json<RemovalRequest>,
) -> Result<Json<RemovalPlan>, HttpError> {
    Ok(Json(
        engine
            .plan_removal(
                &request.dataset,
                &request.removal,
                &request.objects,
                &request.reason,
            )
            .await?,
    ))
}

async fn remove_objects(
    State(engine): State<Engine>,
    Json(plan): Json<RemovalPlan>,
) -> Result<Json<RemovalStatus>, HttpError> {
    Ok(Json(engine.remove_objects(&plan).await?))
}

#[cfg(feature = "remote")]
async fn prepare_remote_removal(
    State(engine): State<Engine>,
    Json(plan): Json<RemovalPlan>,
) -> Result<Json<RemovalStatus>, HttpError> {
    Ok(Json(engine.prepare_remote_removal(&plan).await?))
}
#[cfg(feature = "remote")]
async fn resume_remote_removal(
    State(engine): State<Engine>,
    Path((dataset, removal)): Path<(String, String)>,
    Json(authority): Json<crate::storage::remote::objects::RemoteRemovalConfig>,
) -> Result<Json<RemovalStatus>, HttpError> {
    Ok(Json(
        engine
            .resume_removal_remote(&Id::new(dataset)?, &Id::new(removal)?, authority.resolve()?)
            .await?,
    ))
}

async fn inspect_removal(
    State(engine): State<Engine>,
    Path((dataset, removal)): Path<(String, String)>,
) -> Result<Json<RemovalStatus>, HttpError> {
    Ok(Json(
        engine
            .inspect_removal(&Id::new(dataset)?, &Id::new(removal)?)
            .await?,
    ))
}

async fn resume_removal(
    State(engine): State<Engine>,
    Path((dataset, removal)): Path<(String, String)>,
) -> Result<Json<RemovalStatus>, HttpError> {
    Ok(Json(
        engine
            .resume_removal(&Id::new(dataset)?, &Id::new(removal)?)
            .await?,
    ))
}

async fn inspect_retention(
    State(engine): State<Engine>,
    Json(objects): Json<Vec<ObjectRef>>,
) -> Result<Json<RetentionInspection>, HttpError> {
    Ok(Json(engine.inspect_retention(&objects).await?))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyRetirementRequest {
    dataset: Id,
    retirement: Id,
    keys: Vec<String>,
    reason: String,
}

async fn plan_phase_retirement(
    State(engine): State<Engine>,
    Json(request): Json<KeyRetirementRequest>,
) -> Result<Json<RetentionPlan>, HttpError> {
    Ok(Json(
        engine
            .plan_phase_retirement(
                &request.dataset,
                &request.retirement,
                &request.keys,
                &request.reason,
            )
            .await?,
    ))
}

async fn retire_phases(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_phases(&plan).await?))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangeRetirementRequest {
    dataset: Id,
    retirement: Id,
    versions: Vec<Id>,
    reason: String,
}

async fn plan_change_retirement(
    State(engine): State<Engine>,
    Json(request): Json<ChangeRetirementRequest>,
) -> Result<Json<RetentionPlan>, HttpError> {
    Ok(Json(
        engine
            .plan_change_retirement(
                &request.dataset,
                &request.retirement,
                &request.versions,
                &request.reason,
            )
            .await?,
    ))
}

async fn retire_changes(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_changes(&plan).await?))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RestorePointRetirementRequest {
    dataset: Id,
    retirement: Id,
    points: Vec<Id>,
    reason: String,
}

async fn plan_restore_point_retirement(
    State(engine): State<Engine>,
    Json(request): Json<RestorePointRetirementRequest>,
) -> Result<Json<RetentionPlan>, HttpError> {
    Ok(Json(
        engine
            .plan_restore_point_retirement(
                &request.dataset,
                &request.retirement,
                &request.points,
                &request.reason,
            )
            .await?,
    ))
}

async fn retire_restore_points(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_restore_points(&plan).await?))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct GenerationRetirementRequest {
    family: Id,
    retirement: Id,
    generation: Id,
    reason: String,
}

async fn plan_generation_retirement(
    State(engine): State<Engine>,
    Json(request): Json<GenerationRetirementRequest>,
) -> Result<Json<RetentionPlan>, HttpError> {
    Ok(Json(
        engine
            .plan_generation_retirement(
                &request.family,
                &request.retirement,
                &request.generation,
                &request.reason,
            )
            .await?,
    ))
}

async fn retire_generation(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_generation(&plan).await?))
}

async fn plan_grant_retirement(
    State(engine): State<Engine>,
    Json(request): Json<KeyRetirementRequest>,
) -> Result<Json<RetentionPlan>, HttpError> {
    Ok(Json(
        engine
            .plan_grant_retirement(
                &request.dataset,
                &request.retirement,
                &request.keys,
                &request.reason,
            )
            .await?,
    ))
}

async fn retire_grants(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_grants(&plan).await?))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OperationRetirementRequest {
    dataset: Id,
    retirement: Id,
    operation: Id,
    reason: String,
}

async fn plan_availability_retirement(
    State(engine): State<Engine>,
    Json(request): Json<OperationRetirementRequest>,
) -> Result<Json<RetentionPlan>, HttpError> {
    Ok(Json(
        engine
            .plan_availability_retirement(
                &request.dataset,
                &request.retirement,
                &request.operation,
                &request.reason,
            )
            .await?,
    ))
}

async fn retire_availability(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_availability(&plan).await?))
}

async fn plan_operation_retirement(
    State(engine): State<Engine>,
    Json(request): Json<OperationRetirementRequest>,
) -> Result<Json<RetentionPlan>, HttpError> {
    Ok(Json(
        engine
            .plan_operation_retirement(
                &request.dataset,
                &request.retirement,
                &request.operation,
                &request.reason,
            )
            .await?,
    ))
}

async fn retire_operation(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_operation(&plan).await?))
}

async fn plan_compaction_retirement(
    State(engine): State<Engine>,
    Json(request): Json<OperationRetirementRequest>,
) -> Result<Json<RetentionPlan>, HttpError> {
    Ok(Json(
        engine
            .plan_compaction_retirement(
                &request.dataset,
                &request.retirement,
                &request.operation,
                &request.reason,
            )
            .await?,
    ))
}

async fn retire_compaction(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_compaction(&plan).await?))
}

impl From<Error> for HttpError {
    fn from(error: Error) -> Self {
        Self(error)
    }
}
impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status = match self.0.code() {
            "invalid_request_timeout" => StatusCode::BAD_REQUEST,
            "request_timeout" => StatusCode::GATEWAY_TIMEOUT,
            "availability_retired" => StatusCode::GONE,
            "no_release" | "no_export" | "no_operation" | "no_compaction" | "no_phase"
            | "no_removal" | "no_changes" | "missing_group" | "missing_evidence" => {
                StatusCode::NOT_FOUND
            }
            "operation_conflict"
            | "source_fenced"
            | "source_fence_lost"
            | "continuous_changed"
            | "continuous_child_owned"
            | "backfill_target_conflict"
            | "backfill_source_identity_changed"
            | "stale_continuous_definition"
            | "correction_conflict"
            | "correction_required"
            | "correction_mismatch"
            | "correction_unknown"
            | "selected_result"
            | "operation_active"
            | "operation_completed"
            | "operation_abandoned"
            | "abandonment_conflict"
            | "generation_conflict"
            | "generation_retired"
            | "phase_generation_conflict"
            | "phase_retired"
            | "changes_retired"
            | "grant_in_use"
            | "grant_retired"
            | "recovery_retired"
            | "compaction_retired"
            | "retirement_conflict"
            | "removal_conflict"
            | "stale_removal"
            | "stale_retirement"
            | "stale_definition"
            | "stale_eligibility"
            | "stale_authority"
            | "stale_input"
            | "pending_source"
            | "export_conflict"
            | "export_regression"
            | "stale_export"
            | "stale_representation"
            | "coverage_regression" => StatusCode::CONFLICT,
            "stale_base" | "stale_source_boundary" | "stale_group" | "unproven_succession" => {
                StatusCode::CONFLICT
            }
            "capacity" | "metadata_capacity" | "disk_capacity" => StatusCode::SERVICE_UNAVAILABLE,
            "worker" => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::UNPROCESSABLE_ENTITY,
        };
        (
            status,
            Json(serde_json::json!({"code": self.0.code(), "message": self.0.to_string()})),
        )
            .into_response()
    }
}

async fn seal_generation(
    State(engine): State<Engine>,
    Json(seal): Json<GenerationSeal>,
) -> Result<Json<GenerationSeal>, HttpError> {
    Ok(Json(engine.seal_generation(&seal).await?))
}

async fn capture_inputs(
    State(engine): State<Engine>,
    Json(inputs): Json<std::collections::BTreeMap<Id, VersionRef>>,
) -> Result<Json<InputCoverage>, HttpError> {
    Ok(Json(engine.capture_inputs(&inputs).await?))
}
async fn correct_dependencies(
    State(engine): State<Engine>,
    Json(correction): Json<DependencyCorrection>,
) -> Result<Json<CorrectionDecision>, HttpError> {
    Ok(Json(engine.correct_dependencies(&correction).await?))
}
async fn plan(
    State(engine): State<Engine>,
    Json(spec): Json<RunSpec>,
) -> Result<Json<crate::engine::RunPlan>, HttpError> {
    Ok(Json(engine.plan(&spec).await?))
}
async fn run(
    State(engine): State<Engine>,
    Json(spec): Json<RunSpec>,
) -> Result<Json<ReleasePin>, HttpError> {
    Ok(Json(engine.run(&spec).await?))
}

async fn export(
    State(engine): State<Engine>,
    Json(spec): Json<ExportSpec>,
) -> Result<Json<ExportPin>, HttpError> {
    Ok(Json(engine.export(&spec).await?))
}

async fn latest_export(
    State(engine): State<Engine>,
    Path((dataset, channel)): Path<(String, String)>,
) -> Result<Json<ExportPin>, HttpError> {
    Ok(Json(
        engine
            .latest_export(&Id::new(dataset)?, &Id::new(channel)?)
            .await?,
    ))
}

async fn export_pin(
    State(engine): State<Engine>,
    Path((dataset, operation)): Path<(String, String)>,
) -> Result<Json<ExportPin>, HttpError> {
    Ok(Json(
        engine
            .export_pin(&Id::new(dataset)?, &Id::new(operation)?)
            .await?,
    ))
}

async fn compact(
    State(engine): State<Engine>,
    Json(spec): Json<CompactionSpec>,
) -> Result<Json<CompactionResult>, HttpError> {
    Ok(Json(engine.compact(&spec).await?))
}

async fn compact_lifecycle(
    State(engine): State<Engine>,
    Path(dataset): Path<String>,
) -> Result<Json<crate::LifecycleCompactionResult>, HttpError> {
    Ok(Json(engine.compact_lifecycle(&Id::new(dataset)?).await?))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RestorePointRequest {
    dataset: Id,
    point: Id,
}

async fn create_restore_point(
    State(engine): State<Engine>,
    Json(request): Json<RestorePointRequest>,
) -> Result<Json<RestorePointRef>, HttpError> {
    Ok(Json(
        engine
            .create_restore_point(&request.dataset, &request.point)
            .await?,
    ))
}
async fn plan_update(
    State(engine): State<Engine>,
    Json(spec): Json<UpdateSpec>,
) -> Result<Json<crate::engine::RunPlan>, HttpError> {
    Ok(Json(engine.plan_update(&spec).await?))
}
async fn update(
    State(engine): State<Engine>,
    Json(spec): Json<UpdateSpec>,
) -> Result<Json<ReleasePin>, HttpError> {
    Ok(Json(engine.update(&spec).await?))
}
async fn plan_evidence(
    State(engine): State<Engine>,
    Json(spec): Json<EvidenceSpec>,
) -> Result<Json<crate::engine::RunPlan>, HttpError> {
    Ok(Json(engine.plan_evidence(&spec).await?))
}
async fn update_evidence(
    State(engine): State<Engine>,
    Json(spec): Json<EvidenceSpec>,
) -> Result<Json<ReleasePin>, HttpError> {
    Ok(Json(engine.update_evidence(&spec).await?))
}
async fn plan_schema(
    State(engine): State<Engine>,
    Json(spec): Json<SchemaSpec>,
) -> Result<Json<crate::engine::RunPlan>, HttpError> {
    Ok(Json(engine.plan_schema(&spec).await?))
}
async fn evolve_schema(
    State(engine): State<Engine>,
    Json(spec): Json<SchemaSpec>,
) -> Result<Json<ReleasePin>, HttpError> {
    Ok(Json(engine.evolve_schema(&spec).await?))
}
async fn inspect(
    State(engine): State<Engine>,
    Path(dataset): Path<String>,
) -> Result<Json<serde_json::Value>, HttpError> {
    Ok(Json(serde_json::json!(
        engine.inspect(&Id::new(dataset)?).await?
    )))
}

#[derive(serde::Deserialize)]
struct AbandonRequest {
    reason: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FenceRequest {
    expected_epoch: u64,
}

async fn fence_operations(
    State(engine): State<Engine>,
    Path(dataset): Path<String>,
    Json(request): Json<FenceRequest>,
) -> Result<Json<u64>, HttpError> {
    Ok(Json(
        engine
            .fence_operations(&Id::new(dataset)?, request.expected_epoch)
            .await?,
    ))
}

async fn abandon(
    State(engine): State<Engine>,
    Path((dataset, operation)): Path<(String, String)>,
    Json(request): Json<AbandonRequest>,
) -> Result<Json<Abandonment>, HttpError> {
    Ok(Json(
        engine
            .abandon(&Id::new(dataset)?, &Id::new(operation)?, &request.reason)
            .await?,
    ))
}
async fn latest(
    State(engine): State<Engine>,
    Path(dataset): Path<String>,
) -> Result<Json<ReleasePin>, HttpError> {
    Ok(Json(engine.latest(&Id::new(dataset)?).await?))
}
async fn release(
    State(engine): State<Engine>,
    Path((dataset, version)): Path<(String, String)>,
) -> Result<Json<ReleasePin>, HttpError> {
    Ok(Json(
        engine
            .release(&Id::new(dataset)?, &Id::new(version)?)
            .await?,
    ))
}

async fn plan_backfill(
    State(engine): State<Engine>,
    Json(spec): Json<BackfillSpec>,
) -> Result<Json<crate::engine::BackfillPlan>, HttpError> {
    Ok(Json(engine.plan_backfill(&spec).await?))
}
async fn backfill(
    State(engine): State<Engine>,
    Json(spec): Json<BackfillSpec>,
) -> Result<Json<ReleasePin>, HttpError> {
    Ok(Json(engine.backfill(&spec).await?))
}

async fn plan_continuous_backfill(
    State(engine): State<Engine>,
    Json(spec): Json<ContinuousBackfillSpec>,
) -> Result<Json<ContinuousBackfillPlan>, HttpError> {
    Ok(Json(engine.plan_continuous_backfill(&spec).await?))
}

async fn continuous_backfill(
    State(engine): State<Engine>,
    Json(spec): Json<ContinuousBackfillSpec>,
) -> Result<Json<ReleasePin>, HttpError> {
    Ok(Json(engine.continuous_backfill(&spec).await?))
}

async fn plan_refresh(
    State(engine): State<Engine>,
    Json(spec): Json<RefreshSpec>,
) -> Result<Json<crate::engine::RunPlan>, HttpError> {
    Ok(Json(engine.plan_refresh(&spec).await?))
}

async fn refresh(
    State(engine): State<Engine>,
    Json(spec): Json<RefreshSpec>,
) -> Result<Json<ReleasePin>, HttpError> {
    Ok(Json(engine.refresh(&spec).await?))
}

#[derive(serde::Deserialize)]
struct SuccessionRequest {
    prior: VersionRef,
    target: VersionRef,
}

async fn succession(
    State(engine): State<Engine>,
    Json(request): Json<SuccessionRequest>,
) -> Result<Json<SuccessionProof>, HttpError> {
    Ok(Json(
        engine
            .prove_succession(&request.prior, &request.target)
            .await?,
    ))
}

async fn read_combined(
    State(engine): State<Engine>,
    Path((dataset, version)): Path<(String, String)>,
) -> Result<Response, HttpError> {
    read(engine, dataset, version, ReadKind::Combined).await
}

async fn read_export(
    State(engine): State<Engine>,
    Path((dataset, operation)): Path<(String, String)>,
) -> Result<Response, HttpError> {
    let dataset = Id::new(dataset)?;
    let operation = Id::new(operation)?;
    let batches = engine.read_export_id(&dataset, &operation).await?;
    arrow_response(
        engine,
        batches,
        [
            (
                "content-type",
                "application/vnd.apache.arrow.stream".to_owned(),
            ),
            ("docspec-dataset", dataset.to_string()),
            ("docspec-export", operation.to_string()),
        ],
    )
}

async fn read_group(
    State(engine): State<Engine>,
    Path((dataset, version, group)): Path<(String, String, String)>,
) -> Result<Response, HttpError> {
    read(engine, dataset, version, ReadKind::Group(group)).await
}

async fn read_evidence(
    State(engine): State<Engine>,
    Path((dataset, version)): Path<(String, String)>,
) -> Result<Response, HttpError> {
    read(engine, dataset, version, ReadKind::Evidence).await
}

enum ReadKind {
    Combined,
    Group(String),
    Evidence,
    Receipts,
    Occurrences,
}

async fn read_receipts(
    State(engine): State<Engine>,
    Path((dataset, version)): Path<(String, String)>,
) -> Result<Response, HttpError> {
    read(engine, dataset, version, ReadKind::Receipts).await
}
async fn read_occurrences(
    State(engine): State<Engine>,
    Path((dataset, version)): Path<(String, String)>,
) -> Result<Response, HttpError> {
    read(engine, dataset, version, ReadKind::Occurrences).await
}

async fn read(
    engine: Engine,
    dataset: String,
    version: String,
    kind: ReadKind,
) -> Result<Response, HttpError> {
    let release = engine
        .release(&Id::new(dataset)?, &Id::new(version)?)
        .await?;
    let batches = match kind {
        ReadKind::Group(group) => engine.read_group(&release, &group).await?,
        ReadKind::Combined => engine.read_combined(&release).await?,
        ReadKind::Evidence => engine.read_evidence(&release).await?,
        ReadKind::Receipts => engine.read_receipts(&release).await?,
        ReadKind::Occurrences => engine.read_source_occurrences(&release).await?,
    };
    arrow_response(
        engine,
        batches,
        [
            (
                "content-type",
                "application/vnd.apache.arrow.stream".to_owned(),
            ),
            ("docspec-dataset", release.dataset.dataset.to_string()),
            ("docspec-version", release.id.to_string()),
            ("docspec-manifest-blake3", release.manifest.digest),
        ],
    )
}

fn arrow_response<const N: usize>(
    engine: Engine,
    batches: datafusion::physical_plan::SendableRecordBatchStream,
    headers: [(&'static str, String); N],
) -> Result<Response, HttpError> {
    let stream = ipc::encode(batches, engine.read_budget())?.map(move |chunk| {
        // In-flight responses retain host custody and reader admission.
        let _owner = &engine;
        chunk
    });
    Ok((headers, Body::from_stream(stream)).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn correction_refusals_preserve_codes_and_return_conflict() {
        for code in [
            "correction_conflict",
            "correction_required",
            "correction_mismatch",
            "correction_unknown",
        ] {
            let response =
                HttpError(Error::refusal(code, "correction needs review")).into_response();
            assert_eq!(response.status(), StatusCode::CONFLICT);
            let body = axum::body::to_bytes(response.into_body(), 1 << 20)
                .await
                .unwrap();
            let refusal: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(refusal["code"], code);
        }
    }

    #[tokio::test]
    async fn worker_termination_returns_internal_server_error() {
        let handle = tokio::spawn(async {
            panic!("simulated worker panic");
        });
        let join_error = handle.await.unwrap_err();
        let error = Error::from(join_error);
        assert_eq!(error.code(), "worker");
        let response = HttpError::from(error).into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["code"], "worker");
    }
}

#[cfg(feature = "remote")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteRestoreRequest {
    point: RestorePointRef,
    target_owned_prefix: String,
    #[serde(default)]
    target_properties: std::collections::HashMap<String, String>,
    #[serde(default)]
    target_secret_env: std::collections::HashMap<String, String>,
    #[serde(default)]
    allow_unregistered_import: bool,
}

#[cfg(feature = "remote")]
async fn restore_remote(
    State(engine): State<Engine>,
    Json(request): Json<RemoteRestoreRequest>,
) -> Result<Json<crate::restore::remote::RestoredRemoteCatalog>, HttpError> {
    let target = crate::storage::remote::remote_storage(
        &request.target_owned_prefix,
        request.target_properties,
        request.target_secret_env,
    )
    .await?;
    Ok(Json(
        engine
            .restore_remote(&request.point, target, request.allow_unregistered_import)
            .await?,
    ))
}

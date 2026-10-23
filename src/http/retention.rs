// Rust guideline compliant 2026-02-21

use super::*;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RemovalRequest {
    dataset: Id,
    removal: Id,
    objects: Vec<ObjectRef>,
    reason: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ExportStageRemovalRequest {
    dataset: Id,
    removal: Id,
    operation: Id,
    reason: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ExportFileRemovalRequest {
    dataset: Id,
    removal: Id,
    operation: Id,
    file: Id,
    reason: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct KeyRetirementRequest {
    dataset: Id,
    retirement: Id,
    keys: Vec<String>,
    reason: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ChangeRetirementRequest {
    dataset: Id,
    retirement: Id,
    versions: Vec<Id>,
    reason: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RestorePointRetirementRequest {
    dataset: Id,
    retirement: Id,
    points: Vec<Id>,
    reason: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GenerationRetirementRequest {
    family: Id,
    retirement: Id,
    generation: Id,
    reason: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OperationRetirementRequest {
    dataset: Id,
    retirement: Id,
    operation: Id,
    reason: String,
}

pub(super) async fn plan_export_file_removal(
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

pub(super) async fn plan_export_retirement(
    State(engine): State<Engine>,
    Json(request): Json<OperationRetirementRequest>,
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

pub(super) async fn retire_export(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_export(&plan).await?))
}

pub(super) async fn plan_export_stage_removal(
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

pub(super) async fn plan_removal(
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

pub(super) async fn remove_objects(
    State(engine): State<Engine>,
    Json(plan): Json<RemovalPlan>,
) -> Result<Json<RemovalStatus>, HttpError> {
    Ok(Json(engine.remove_objects(&plan).await?))
}

#[cfg(feature = "remote")]
pub(super) async fn prepare_remote_removal(
    State(engine): State<Engine>,
    Json(plan): Json<RemovalPlan>,
) -> Result<Json<RemovalStatus>, HttpError> {
    Ok(Json(engine.prepare_remote_removal(&plan).await?))
}

#[cfg(feature = "remote")]
pub(super) async fn resume_remote_removal(
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

pub(super) async fn inspect_removal(
    State(engine): State<Engine>,
    Path((dataset, removal)): Path<(String, String)>,
) -> Result<Json<RemovalStatus>, HttpError> {
    Ok(Json(
        engine
            .inspect_removal(&Id::new(dataset)?, &Id::new(removal)?)
            .await?,
    ))
}

pub(super) async fn resume_removal(
    State(engine): State<Engine>,
    Path((dataset, removal)): Path<(String, String)>,
) -> Result<Json<RemovalStatus>, HttpError> {
    Ok(Json(
        engine
            .resume_removal(&Id::new(dataset)?, &Id::new(removal)?)
            .await?,
    ))
}

pub(super) async fn inspect_retention(
    State(engine): State<Engine>,
    Json(objects): Json<Vec<ObjectRef>>,
) -> Result<Json<RetentionInspection>, HttpError> {
    Ok(Json(engine.inspect_retention(&objects).await?))
}

pub(super) async fn plan_phase_retirement(
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

pub(super) async fn retire_phases(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_phases(&plan).await?))
}

pub(super) async fn plan_change_retirement(
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

pub(super) async fn retire_changes(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_changes(&plan).await?))
}

pub(super) async fn plan_restore_point_retirement(
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

pub(super) async fn retire_restore_points(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_restore_points(&plan).await?))
}

pub(super) async fn plan_generation_retirement(
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

pub(super) async fn retire_generation(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_generation(&plan).await?))
}

pub(super) async fn plan_grant_retirement(
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

pub(super) async fn retire_grants(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_grants(&plan).await?))
}

pub(super) async fn plan_availability_retirement(
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

pub(super) async fn retire_availability(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_availability(&plan).await?))
}

pub(super) async fn plan_operation_retirement(
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

pub(super) async fn retire_operation(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_operation(&plan).await?))
}

pub(super) async fn plan_compaction_retirement(
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

pub(super) async fn retire_compaction(
    State(engine): State<Engine>,
    Json(plan): Json<RetentionPlan>,
) -> Result<Json<Retirement>, HttpError> {
    Ok(Json(engine.retire_compaction(&plan).await?))
}

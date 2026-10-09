//! Loopback HTTP access to the same application operations as the CLI.
//!
//! This is a local host interface. Public authentication and hosted deployment are
//! deliberately absent until the remote provider and disclosure gates are qualified.

use crate::{
    Engine, Error,
    model::{
        Abandonment, BackfillSpec, GenerationSeal, Id, InputCoverage, ObjectRef, RefreshSpec,
        ReleasePin, RetentionInspection, RetentionPlan, Retirement, RunSpec, SchemaSpec,
        SuccessionProof, UpdateSpec, VersionRef,
    },
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

mod ipc;

/// Builds a bounded local API over an existing execution host.
pub fn router(engine: Engine) -> Router {
    Router::new()
        .route("/plan", post(plan))
        .route("/run", post(run))
        .route("/updates/plan", post(plan_update))
        .route("/updates/run", post(update))
        .route("/schemas/plan", post(plan_schema))
        .route("/schemas/run", post(evolve_schema))
        .route("/backfills/plan", post(plan_backfill))
        .route("/backfills/run", post(backfill))
        .route("/refreshes/plan", post(plan_refresh))
        .route("/refreshes/run", post(refresh))
        .route("/succession", post(succession))
        .route("/generations/seal", post(seal_generation))
        .route("/inputs/capture", post(capture_inputs))
        .route("/retention/inspect", post(inspect_retention))
        .route("/retention/phases/plan", post(plan_phase_retirement))
        .route("/retention/phases/retire", post(retire_phases))
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
        .route("/datasets/{dataset}", get(inspect))
        .route(
            "/datasets/{dataset}/operations/{operation}/abandon",
            post(abandon),
        )
        .route("/datasets/{dataset}/latest", get(latest))
        .route("/datasets/{dataset}/versions/{version}", get(release))
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
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(engine)
}

struct HttpError(Error);

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

impl From<Error> for HttpError {
    fn from(error: Error) -> Self {
        Self(error)
    }
}
impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status = match self.0.code() {
            "availability_retired" => StatusCode::GONE,
            "no_release" | "no_operation" | "no_phase" | "missing_group" | "missing_evidence" => {
                StatusCode::NOT_FOUND
            }
            "operation_conflict"
            | "selected_result"
            | "operation_active"
            | "operation_completed"
            | "operation_abandoned"
            | "abandonment_conflict"
            | "generation_conflict"
            | "phase_generation_conflict"
            | "phase_retired"
            | "grant_in_use"
            | "grant_retired"
            | "recovery_retired"
            | "retirement_conflict"
            | "stale_retirement"
            | "stale_definition"
            | "stale_eligibility"
            | "stale_input"
            | "pending_source"
            | "coverage_regression" => StatusCode::CONFLICT,
            "stale_base" | "stale_source_boundary" | "stale_group" | "unproven_succession" => {
                StatusCode::CONFLICT
            }
            "capacity" | "metadata_capacity" | "disk_capacity" => StatusCode::SERVICE_UNAVAILABLE,
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
    };
    let stream = ipc::encode(batches, engine.read_budget())?.map(move |chunk| {
        // The in-flight response retains local workspace ownership as well as
        // reader admission, even if the router itself is dropped.
        let _owner = &engine;
        chunk
    });
    Ok((
        [
            (
                "content-type",
                "application/vnd.apache.arrow.stream".to_owned(),
            ),
            ("docspec-dataset", release.dataset.dataset.to_string()),
            ("docspec-version", release.id.to_string()),
            ("docspec-manifest-blake3", release.manifest.digest),
        ],
        Body::from_stream(stream),
    )
        .into_response())
}

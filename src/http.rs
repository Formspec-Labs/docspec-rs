//! Loopback HTTP access to the same application operations as the CLI.
//!
//! This is a local host interface. Public authentication and hosted deployment are
//! deliberately absent until the remote provider and disclosure gates are qualified.

use crate::{
    Engine, Error,
    model::{
        BackfillSpec, GenerationSeal, Id, InputCoverage, ReleasePin, RunSpec, SuccessionProof,
        UpdateSpec, VersionRef,
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
        .route("/backfills/plan", post(plan_backfill))
        .route("/backfills/run", post(backfill))
        .route("/succession", post(succession))
        .route("/generations/seal", post(seal_generation))
        .route("/inputs/capture", post(capture_inputs))
        .route("/datasets/{dataset}", get(inspect))
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
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(engine)
}

struct HttpError(Error);
impl From<Error> for HttpError {
    fn from(error: Error) -> Self {
        Self(error)
    }
}
impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status = match self.0.code() {
            "no_release" | "missing_group" => StatusCode::NOT_FOUND,
            "operation_conflict"
            | "generation_conflict"
            | "stale_definition"
            | "stale_eligibility"
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
async fn inspect(
    State(engine): State<Engine>,
    Path(dataset): Path<String>,
) -> Result<Json<serde_json::Value>, HttpError> {
    Ok(Json(serde_json::json!(
        engine.inspect(&Id::new(dataset)?).await?
    )))
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
    read(engine, dataset, version, None).await
}

async fn read_group(
    State(engine): State<Engine>,
    Path((dataset, version, group)): Path<(String, String, String)>,
) -> Result<Response, HttpError> {
    read(engine, dataset, version, Some(group)).await
}

async fn read(
    engine: Engine,
    dataset: String,
    version: String,
    group: Option<String>,
) -> Result<Response, HttpError> {
    let release = engine
        .release(&Id::new(dataset)?, &Id::new(version)?)
        .await?;
    let batches = if let Some(group) = group {
        engine.read_group(&release, &group).await?
    } else {
        engine.read_combined(&release).await?
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

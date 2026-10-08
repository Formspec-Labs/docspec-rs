//! Loopback HTTP access to the same application operations as the CLI.
//!
//! This is a local host interface. Public authentication and hosted deployment are
//! deliberately absent until the remote provider and disclosure gates are qualified.

use crate::{
    Engine, Error,
    model::{Id, ReleasePin, RunSpec},
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};

/// Builds a bounded local API over an existing execution host.
pub fn router(engine: Engine) -> Router {
    Router::new()
        .route("/plan", post(plan))
        .route("/run", post(run))
        .route("/datasets/{dataset}", get(inspect))
        .route("/datasets/{dataset}/latest", get(latest))
        .route("/datasets/{dataset}/versions/{version}", get(release))
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
            "no_release" => StatusCode::NOT_FOUND,
            "operation_conflict"
            | "stale_definition"
            | "stale_eligibility"
            | "coverage_regression" => StatusCode::CONFLICT,
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

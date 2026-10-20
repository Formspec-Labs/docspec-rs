//! One optional caller deadline covers handler work and outgoing response chunks.
// Rust guideline compliant 2026-02-21

use super::HttpError;
use crate::Error;
use axum::{
    body::Body,
    extract::Request,
    http::HeaderMap,
    middleware::Next,
    response::{IntoResponse, Response},
};
use futures::StreamExt;
use std::time::Duration;
use tokio::time::{Instant, sleep_until};

const HEADER: &str = "docspec-timeout-ms";

fn deadline(headers: &HeaderMap) -> Result<Option<Instant>, HttpError> {
    let mut values = headers.get_all(HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    let invalid = || {
        HttpError(Error::refusal(
            "invalid_request_timeout",
            "docspec-timeout-ms requires one positive representable integer millisecond budget",
        ))
    };
    if values.next().is_some() {
        return Err(invalid());
    }
    let text = value.to_str().map_err(|_| invalid())?;
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    let millis: u64 = text.parse().map_err(|_| invalid())?;
    if millis == 0 {
        return Err(invalid());
    }
    Instant::now()
        .checked_add(Duration::from_millis(millis))
        .map(Some)
        .ok_or_else(invalid)
}

fn expired() -> Error {
    Error::refusal("request_timeout", "HTTP request deadline elapsed")
}

pub(super) async fn apply(request: Request, next: Next) -> Response {
    let until = match deadline(request.headers()) {
        Ok(Some(until)) => until,
        Ok(None) => return next.run(request).await,
        Err(error) => return error.into_response(),
    };
    let response = tokio::select! {
        biased;
        () = sleep_until(until) => return HttpError(expired()).into_response(),
        response = next.run(request) => response,
    };
    let (parts, body) = response.into_parts();
    let stream = futures::stream::try_unfold(body.into_data_stream(), move |mut body| async move {
        // No detached response producer: cancellation drops the original body.
        // Engine-owned durable work already admitted by the handler keeps its
        // existing lifetime; an expired HTTP caller cannot roll it back.
        let chunk = tokio::select! {
            biased;
            () = sleep_until(until) => return Err(expired()),
            chunk = body.next() => chunk,
        };
        chunk
            .transpose()
            .map(|chunk| chunk.map(|chunk| (chunk, body)))
            .map_err(|error| Error::refusal("http_body", error.to_string()))
    });
    Response::from_parts(parts, Body::from_stream(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        body::{Bytes, to_bytes},
        http::{Request, StatusCode},
        routing::get,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tower::ServiceExt;

    fn request(path: &str, millis: &str) -> Request<Body> {
        Request::builder()
            .uri(path)
            .header(HEADER, millis)
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn handler_deadline_refuses_before_headers_without_timing_out_other_requests() {
        let router = Router::new()
            .route(
                "/slow",
                get(|| async {
                    sleep_until(Instant::now() + Duration::from_millis(100)).await;
                    "late"
                }),
            )
            .route("/ready", get(|| async { "ready" }))
            .layer(axum::middleware::from_fn(apply));
        let response = router
            .clone()
            .oneshot(request("/slow", "10"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        let response = router.oneshot(request("/ready", "1000")).await.unwrap();
        assert_eq!(to_bytes(response.into_body(), 4096).await.unwrap(), "ready");
    }

    #[tokio::test]
    async fn stalled_and_periodic_bodies_abort_without_success_end() {
        for delay in [Duration::from_millis(100), Duration::from_millis(5)] {
            let emitted = Arc::new(AtomicUsize::new(0));
            let count = emitted.clone();
            let router = Router::new()
                .route(
                    "/body",
                    get(move || {
                        let count = count.clone();
                        async move {
                            let chunks = futures::stream::unfold(0, move |index| {
                                let count = count.clone();
                                async move {
                                    if index == 10 {
                                        return None;
                                    }
                                    tokio::time::sleep(delay).await;
                                    count.fetch_add(1, Ordering::SeqCst);
                                    Some((
                                        Ok::<_, std::io::Error>(Bytes::from_static(b"chunk")),
                                        index + 1,
                                    ))
                                }
                            });
                            Body::from_stream(chunks)
                        }
                    }),
                )
                .layer(axum::middleware::from_fn(apply));
            let response = router.oneshot(request("/body", "30")).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert!(to_bytes(response.into_body(), 4096).await.is_err());
            assert!(emitted.load(Ordering::SeqCst) < 10);
            if delay.as_millis() == 5 {
                assert!(emitted.load(Ordering::SeqCst) > 0);
            }
        }
    }

    #[tokio::test]
    async fn original_body_failures_do_not_become_success() {
        let router = Router::new()
            .route(
                "/body",
                get(|| async {
                    Body::from_stream(futures::stream::once(async {
                        Err::<Bytes, _>(std::io::Error::other("source failed"))
                    }))
                }),
            )
            .layer(axum::middleware::from_fn(apply));
        let response = router.oneshot(request("/body", "1000")).await.unwrap();
        assert!(to_bytes(response.into_body(), 4096).await.is_err());
    }

    #[tokio::test]
    async fn partial_arrow_deadline_does_not_emit_the_success_terminator() {
        use arrow::{
            array::Int64Array,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::TryStreamExt;
        let router = Router::new()
            .route(
                "/arrow",
                get(|| async {
                    let schema =
                        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int64Array::from(vec![Some(7), None, Some(7)]))],
                    )
                    .unwrap();
                    let batches = futures::stream::iter(vec![Ok(batch.clone())]).chain(
                        futures::stream::once(async move {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            Ok(batch)
                        }),
                    );
                    let budget = crate::budget::Budget::new(crate::budget::Limits {
                        memory_bytes: 64 << 10,
                        max_batch_bytes: 4096,
                        max_metadata_bytes: 4096,
                        ..crate::budget::Limits::default()
                    })
                    .unwrap();
                    Body::from_stream(
                        super::super::ipc::encode(
                            Box::pin(RecordBatchStreamAdapter::new(schema, batches)),
                            budget,
                        )
                        .unwrap(),
                    )
                }),
            )
            .layer(axum::middleware::from_fn(apply));
        let response = router.oneshot(request("/arrow", "30")).await.unwrap();
        let mut stream = response.into_body().into_data_stream();
        let mut bytes = Vec::new();
        let mut frames = 0;
        loop {
            match stream.try_next().await {
                Ok(Some(chunk)) => {
                    bytes.extend_from_slice(&chunk);
                    frames += 1;
                }
                Ok(None) => panic!("deadline must abort the partial Arrow body"),
                Err(_) => break,
            }
        }
        assert_eq!(
            frames, 2,
            "schema and first batch precede the stalled batch"
        );
        assert!(!bytes.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
    }
}

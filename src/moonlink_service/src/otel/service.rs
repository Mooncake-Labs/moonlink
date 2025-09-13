use crate::otel::otel_state::OtelState;
use crate::Result;
use axum::error_handling::HandleErrorLayer;
use axum::http::Method;
use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::Response,
    routing::post,
    Router,
};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use prost::Message;
use std::sync::Arc;
use tokio::sync::oneshot;
use tower::timeout::TimeoutLayer;
use tower::{BoxError, ServiceBuilder};
use tower_http::cors::{Any, CorsLayer};
use tracing::error;

/// Default timeout for otel API calls.
const DEFAULT_REST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

pub fn create_otel_router(state: OtelState) -> Router {
    let timeout_layer = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(|err: BoxError| async move {
            if err.is::<tower::timeout::error::Elapsed>() {
                return Response::builder()
                    .status(StatusCode::REQUEST_TIMEOUT)
                    .body::<String>("request timed out".into())
                    .unwrap();
            }
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("internal middleware error".into())
                .unwrap()
        }))
        .layer(TimeoutLayer::new(DEFAULT_REST_TIMEOUT));

    Router::new()
        .route("/v1/metrics", post(handle_metrics))
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::POST])
                .allow_headers(Any),
        )
        .layer(timeout_layer)
}

pub async fn start_otel_service(
    otel_port: u16,
    rest_port: u16,
    moonlink_backend: Arc<moonlink_backend::MoonlinkBackend>,
    shutdown_signal: oneshot::Receiver<()>,
) -> Result<()> {
    let otel_state = OtelState::new(rest_port, moonlink_backend).await?;
    let app = create_otel_router(otel_state);
    let otel_addr = format!("0.0.0.0:{otel_port}");

    let listener = tokio::net::TcpListener::bind(&otel_addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            shutdown_signal.await.ok();
        })
        .await?;

    Ok(())
}

async fn handle_metrics(
    State(state): State<OtelState>,
    _headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, [(header::HeaderName, &'static str); 1], Vec<u8>) {
    match ExportMetricsServiceRequest::decode(body) {
        Ok(req) => {
            match state.metrics_handler.handle_request(req).await {
                Ok(resp) => {
                    let bytes = resp.encode_to_vec();
                    (
                        StatusCode::OK,
                        [(header::CONTENT_TYPE, "application/x-protobuf")],
                        bytes,
                    )
                }
                // TODO(hjiang): Better error propagation.
                Err(err) => {
                    // Different from general user-facing requests, failed otel request won't be processed usually, so to detect errors we log on server side.
                    error!("Failed to process otel ingestion request: {:?}", err);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        [(header::CONTENT_TYPE, "text/plain")],
                        format!("protobuf decode failed: {err}").into_bytes(),
                    )
                }
            }
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            [(header::CONTENT_TYPE, "text/plain")],
            format!("protobuf decode failed: {e}").into_bytes(),
        ),
    }
}

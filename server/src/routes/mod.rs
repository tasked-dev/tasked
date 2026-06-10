//! HTTP route handlers and router construction.

pub(crate) mod artifacts;
pub(crate) mod flows;
pub(crate) mod queues;
pub(crate) mod schedules;
pub(crate) mod sse;
pub(crate) mod tasks;

use axum::{
    Json, Router,
    body::Body,
    extract::{MatchedPath, Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use std::sync::Arc;
use tasked::{
    engine::Engine,
    types::{Flow, FlowId, Queue, QueueId, Schedule, ScheduleId, Task, TaskId},
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use crate::error::{
    ApiError, flow_not_found_msg, queue_not_found_msg, schedule_not_found_msg, task_not_found_msg,
};

pub(crate) type AppState = Arc<Engine>;

// -- Lookup helpers --
//
// Fetch an entity and convert "not present" into the canonical 404 ApiError.
// The not-found message is built from the raw path segment, exactly as the
// handlers did before these helpers existed.

pub(crate) async fn find_queue(engine: &Engine, qid: &str) -> Result<Queue, ApiError> {
    let queue_id = QueueId::from(qid);
    engine
        .get_queue(&queue_id)
        .await?
        .ok_or_else(|| ApiError::not_found("queue_not_found", queue_not_found_msg(qid)))
}

pub(crate) async fn find_flow(engine: &Engine, fid: &str) -> Result<Flow, ApiError> {
    let flow_id = FlowId::from(fid);
    engine
        .get_flow(&flow_id)
        .await?
        .ok_or_else(|| ApiError::not_found("flow_not_found", flow_not_found_msg(fid)))
}

pub(crate) async fn find_flow_with_tasks(
    engine: &Engine,
    fid: &str,
) -> Result<(Flow, Vec<Task>), ApiError> {
    let flow_id = FlowId::from(fid);
    engine
        .get_flow_with_tasks(&flow_id)
        .await?
        .ok_or_else(|| ApiError::not_found("flow_not_found", flow_not_found_msg(fid)))
}

pub(crate) async fn find_task(engine: &Engine, fid: &str, tid: &str) -> Result<Task, ApiError> {
    let flow_id = FlowId::from(fid);
    let task_id = TaskId::from(tid);
    engine
        .get_task(&task_id, &flow_id)
        .await?
        .ok_or_else(|| ApiError::not_found("task_not_found", task_not_found_msg(tid, fid)))
}

pub(crate) async fn find_schedule(engine: &Engine, sid: &str) -> Result<Schedule, ApiError> {
    let schedule_id = ScheduleId::from(sid);
    engine
        .get_schedule(&schedule_id)
        .await?
        .ok_or_else(|| ApiError::not_found("schedule_not_found", schedule_not_found_msg(sid)))
}

// -- Health & metrics --

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

/// Prometheus metrics endpoint using the shared handle.
async fn metrics_handler_with_state(
    State(handle): State<metrics_exporter_prometheus::PrometheusHandle>,
) -> impl IntoResponse {
    handle.render()
}

/// Middleware that records request duration and response status as Prometheus metrics.
async fn metrics_middleware(req: Request<Body>, next: Next) -> Response {
    let method = req.method().clone();
    let path = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| "unknown".to_owned());

    let start = std::time::Instant::now();
    let response = next.run(req).await;
    let elapsed = start.elapsed().as_secs_f64();

    let status_class = match response.status().as_u16() {
        200..=299 => "2xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    };

    // Skip SSE endpoints (long-lived connections would distort latency histograms)
    if !path.contains("/events") {
        metrics::histogram!(
            "tasked_http_request_duration_seconds",
            "method" => method.to_string(),
            "route" => path.clone(),
        )
        .record(elapsed);
    }

    metrics::counter!(
        "tasked_http_responses_total",
        "method" => method.to_string(),
        "route" => path,
        "status" => status_class.to_owned(),
    )
    .increment(1);

    response
}

// -- Router --

fn build_cors_layer(origins: &[String]) -> CorsLayer {
    use axum::http::{HeaderValue, Method};

    if origins.iter().any(|o| o == "*") {
        return CorsLayer::permissive();
    }

    if origins.is_empty() {
        // No origins specified: only same-origin requests allowed
        return CorsLayer::new();
    }

    let allowed: Vec<HeaderValue> = origins
        .iter()
        .filter_map(|o| match o.parse() {
            Ok(v) => Some(v),
            Err(_) => {
                tracing::warn!(origin = %o, "ignoring invalid CORS origin");
                None
            }
        })
        .collect();

    CorsLayer::new()
        .allow_origin(allowed)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
        ])
        .allow_headers([
            axum::http::header::AUTHORIZATION,
            axum::http::header::CONTENT_TYPE,
        ])
}

pub(crate) fn build_router(engine: Arc<Engine>, cors_origins: &[String]) -> Router {
    Router::new()
        // Health check
        .route("/healthz", get(health))
        // Queue routes
        .route(
            "/api/v1/queues",
            post(queues::create_queue).get(queues::list_queues),
        )
        .route(
            "/api/v1/queues/{qid}",
            get(queues::get_queue).delete(queues::delete_queue),
        )
        // Flow routes
        .route(
            "/api/v1/queues/{qid}/flows",
            post(flows::submit_flow).get(flows::list_flows),
        )
        .route(
            "/api/v1/flows/{fid}",
            get(flows::get_flow).delete(flows::cancel_flow),
        )
        // Flow export
        .route(
            "/api/v1/flows/{fid}/export",
            get(flows::export_flow_handler),
        )
        // Flow SSE events
        .route("/api/v1/flows/{fid}/events", get(sse::flow_events))
        // Task ack
        .route("/api/v1/flows/{fid}/tasks/{tid}/ack", post(tasks::ack_task))
        // Artifact routes
        .route(
            "/api/v1/flows/{fid}/artifacts",
            get(artifacts::list_artifacts),
        )
        .route(
            "/api/v1/flows/{fid}/artifacts/{*name}",
            get(artifacts::download_artifact).put(artifacts::upload_artifact),
        )
        // Schedule routes
        .route(
            "/api/v1/queues/{qid}/schedules",
            post(schedules::create_schedule_handler).get(schedules::list_schedules_handler),
        )
        .route(
            "/api/v1/schedules/{sid}",
            get(schedules::get_schedule_handler)
                .put(schedules::update_schedule_handler)
                .delete(schedules::delete_schedule_handler),
        )
        // Middleware (order matters: outermost layer runs first)
        .layer(axum::middleware::from_fn(metrics_middleware))
        .layer(TraceLayer::new_for_http())
        .layer(build_cors_layer(cors_origins))
        .layer(axum::extract::DefaultBodyLimit::max(10 * 1024 * 1024)) // 10 MB
        .with_state(engine)
}

pub(crate) fn build_router_with_metrics(
    engine: Arc<Engine>,
    metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
    cors_origins: &[String],
) -> Router {
    let metrics_router = metrics_router(metrics_handle);
    build_router(engine, cors_origins).merge(metrics_router)
}

/// A standalone router serving only `/metrics`, used both for merging into
/// the main router and for the dedicated `--metrics-port` listener.
pub(crate) fn metrics_router(
    metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler_with_state))
        .with_state(metrics_handle)
}

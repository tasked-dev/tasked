#![deny(unsafe_code)]

mod mcp;

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{MatchedPath, Path, Query, State},
    http::{Request, StatusCode},
    middleware::Next,
    response::{
        IntoResponse, Response,
        sse::{Event, Sse},
    },
    routing::{get, post},
};
use clap::{Parser, Subcommand};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tasked::{
    engine::{Engine, EngineConfig, EngineError},
    executor::{
        CallbackExecutor, NoopExecutor,
        api::{self, InlineApiExecutor},
        approval::ApprovalExecutor,
        delay::DelayExecutor,
        http::HttpExecutor,
        remote::RemoteExecutor,
        shell::ShellExecutor,
    },
    store::{memory::MemoryStorage, sharded::ShardedStorage, sqlite::SqliteStorage},
    types::*,
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::info;

// -- CLI --

#[derive(Parser)]
#[command(
    name = "tasked",
    about = "HTTP server and CLI for the Tasked DAG execution engine",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the HTTP server
    Serve {
        /// Data directory for per-queue databases (default: ./tasked-data)
        #[arg(long, default_value = "tasked-data")]
        data_dir: String,

        /// Storage engine: "sqlite" (default, per-queue SQLite databases) or
        /// "journal" (in-memory state with append-only SQLite journal)
        #[arg(long, default_value = "sqlite")]
        engine: String,

        /// Port to listen on
        #[arg(long, default_value_t = 8080)]
        port: u16,

        /// Host to bind to
        #[arg(long, default_value = "0.0.0.0")]
        host: String,

        /// Authentication mode: none, api-key
        #[arg(long, default_value = "none")]
        auth_mode: String,

        /// API key for api-key auth mode
        #[arg(long, env = "TASKED_API_KEY")]
        api_key: Option<String>,

        /// URL to push Prometheus metrics to (enables metrics push mode)
        #[arg(long)]
        metrics_push_url: Option<String>,

        /// Directory containing integration definition JSON files
        #[arg(long, env = "TASKED_INTEGRATIONS_DIR")]
        integrations_dir: Option<String>,

        /// SQLite path for persisting OAuth2 tokens (default: in-memory only)
        #[arg(long, env = "TASKED_TOKEN_CACHE")]
        token_cache: Option<String>,

        /// Port for a dedicated metrics listener on 127.0.0.1.
        /// When set, /metrics is served only on this port and removed from the main router.
        /// Recommended when auth is disabled to prevent unauthenticated metrics scraping.
        #[arg(long, env = "TASKED_METRICS_PORT")]
        metrics_port: Option<u16>,

        /// Allowed CORS origins (repeatable). If omitted, no cross-origin requests are allowed.
        /// Use "*" to allow all origins (not recommended in production).
        #[arg(long, env = "TASKED_CORS_ORIGIN")]
        cors_origin: Vec<String>,
    },
    /// Execute a flow definition from a JSON file and exit
    Run {
        /// Path to a flow definition JSON file
        file: String,

        /// Queue to submit the flow to (created if it doesn't exist)
        #[arg(long, default_value = "default")]
        queue: String,

        /// SQLite database path (use :memory: for in-memory)
        #[arg(long, default_value = ":memory:")]
        db: String,

        /// Auto-approve all approval tasks without prompting
        #[arg(long)]
        auto_approve: bool,

        /// Write task outputs to a JSON file on completion
        #[arg(long, short)]
        output: Option<String>,

        /// Directory containing integration definition JSON files
        #[arg(long, env = "TASKED_INTEGRATIONS_DIR")]
        integrations_dir: Option<String>,

        /// SQLite path for persisting OAuth2 tokens (default: in-memory only)
        #[arg(long, env = "TASKED_TOKEN_CACHE")]
        token_cache: Option<String>,
    },
    /// Start an MCP (Model Context Protocol) server on stdio
    Mcp {
        /// Data directory for per-queue databases (default: ./tasked-data)
        #[arg(long, default_value = "tasked-data")]
        data_dir: String,

        /// Storage engine: "sqlite" (default) or "journal" (in-memory with SQLite journal)
        #[arg(long, default_value = "sqlite")]
        engine: String,
    },
    /// Export a flow's complete state for archival or replay
    Export {
        /// Flow ID to export
        flow_id: String,

        /// Server base URL
        #[arg(long, default_value = "http://localhost:8080")]
        server: String,

        /// Include artifact data in the export
        #[arg(long)]
        with_artifacts: bool,

        /// Output file (default: stdout)
        #[arg(long, short)]
        output: Option<String>,

        /// Export format: "json" (default) or "tar" (tar.gz archive with artifacts)
        #[arg(long, default_value = "json")]
        format: String,

        /// API key for authenticated servers
        #[arg(long, env = "TASKED_API_KEY")]
        api_key: Option<String>,
    },
}

// -- App state --

type AppState = Arc<Engine>;

// -- API request/response types --

#[derive(Deserialize)]
struct CreateQueueRequest {
    id: String,
    #[serde(default)]
    config: QueueConfig,
}

#[derive(Serialize)]
struct QueueResponse {
    id: String,
    config: QueueConfig,
    created_at: String,
    updated_at: String,
}

impl From<Queue> for QueueResponse {
    fn from(q: Queue) -> Self {
        Self {
            id: q.id.to_string(),
            config: q.config,
            created_at: q.created_at.to_rfc3339(),
            updated_at: q.updated_at.to_rfc3339(),
        }
    }
}

#[derive(Serialize)]
struct FlowResponse {
    id: String,
    queue_id: String,
    state: String,
    task_count: usize,
    tasks_succeeded: usize,
    tasks_failed: usize,
    fail_fast: bool,
    created_at: String,
    updated_at: String,
}

impl From<Flow> for FlowResponse {
    fn from(f: Flow) -> Self {
        Self {
            id: f.id.to_string(),
            queue_id: f.queue_id.to_string(),
            state: f.state.to_string(),
            task_count: f.task_count,
            tasks_succeeded: f.tasks_succeeded,
            tasks_failed: f.tasks_failed,
            fail_fast: f.fail_fast,
            created_at: f.created_at.to_rfc3339(),
            updated_at: f.updated_at.to_rfc3339(),
        }
    }
}

#[derive(Serialize)]
struct TaskResponse {
    id: String,
    flow_id: String,
    queue_id: String,
    state: String,
    executor_type: String,
    input: Option<serde_json::Value>,
    output: Option<serde_json::Value>,
    error: Option<String>,
    retries_remaining: u32,
    timeout_secs: u64,
    started_at: Option<String>,
    completed_at: Option<String>,
    created_at: String,
}

impl From<Task> for TaskResponse {
    fn from(t: Task) -> Self {
        Self {
            id: t.id.to_string(),
            flow_id: t.flow_id.to_string(),
            queue_id: t.queue_id.to_string(),
            state: t.state.to_string(),
            executor_type: t.executor_type,
            input: t.input,
            output: t.output,
            error: t.error,
            retries_remaining: t.retries_remaining,
            timeout_secs: t.timeout_secs,
            started_at: t.started_at.map(|dt| dt.to_rfc3339()),
            completed_at: t.completed_at.map(|dt| dt.to_rfc3339()),
            created_at: t.created_at.to_rfc3339(),
        }
    }
}

#[derive(Serialize)]
struct FlowDetailResponse {
    id: String,
    queue_id: String,
    state: String,
    task_count: usize,
    tasks_succeeded: usize,
    tasks_failed: usize,
    tasks: Vec<TaskResponse>,
    created_at: String,
    updated_at: String,
}

#[derive(Deserialize)]
struct AckRequest {
    status: String,
    #[serde(default)]
    output: Option<serde_json::Value>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    retryable: Option<bool>,
    #[serde(default)]
    approved_by: Option<String>,
}

#[derive(Deserialize)]
struct ScheduleRequest {
    cron: String,
    flow: FlowDef,
    #[serde(default)]
    name: Option<String>,
    #[serde(default = "default_enabled")]
    enabled: bool,
}

fn default_enabled() -> bool {
    true
}

#[derive(Serialize)]
struct ScheduleResponse {
    id: String,
    queue_id: String,
    name: Option<String>,
    cron: String,
    enabled: bool,
    last_triggered_at: Option<String>,
    next_run_at: Option<String>,
    created_at: String,
    updated_at: String,
}

impl From<Schedule> for ScheduleResponse {
    fn from(s: Schedule) -> Self {
        Self {
            id: s.id.to_string(),
            queue_id: s.queue_id.to_string(),
            name: s.name,
            cron: s.cron,
            enabled: s.enabled,
            last_triggered_at: s.last_triggered_at.map(|dt| dt.to_rfc3339()),
            next_run_at: s.next_run_at.map(|dt| dt.to_rfc3339()),
            created_at: s.created_at.to_rfc3339(),
            updated_at: s.updated_at.to_rfc3339(),
        }
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    message: String,
}

// -- Error handling --

enum ApiError {
    NotFound { error: String, message: String },
    BadRequest { error: String, message: String },
    Conflict { error: String, message: String },
    Internal { error: String, message: String },
    ServiceUnavailable { error: String, message: String },
    TooManyRequests { error: String, message: String },
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, body) = match self {
            ApiError::NotFound { error, message } => {
                (StatusCode::NOT_FOUND, ErrorResponse { error, message })
            }
            ApiError::BadRequest { error, message } => {
                (StatusCode::BAD_REQUEST, ErrorResponse { error, message })
            }
            ApiError::Conflict { error, message } => {
                (StatusCode::CONFLICT, ErrorResponse { error, message })
            }
            ApiError::Internal { error, message } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorResponse { error, message },
            ),
            ApiError::ServiceUnavailable { error, message } => (
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorResponse { error, message },
            ),
            ApiError::TooManyRequests { error, message } => {
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    [("retry-after", "1")],
                    Json(ErrorResponse { error, message }),
                )
                    .into_response();
            }
        };
        (status, Json(body)).into_response()
    }
}

impl From<EngineError> for ApiError {
    fn from(err: EngineError) -> Self {
        match &err {
            EngineError::QueueNotFound(id) => ApiError::NotFound {
                error: "queue_not_found".to_string(),
                message: format!("Queue '{id}' not found"),
            },
            EngineError::NoExecutor(name) => ApiError::BadRequest {
                error: "no_executor".to_string(),
                message: format!("No executor registered for type '{name}'"),
            },
            EngineError::Graph(e) => ApiError::BadRequest {
                error: "invalid_graph".to_string(),
                message: e.to_string(),
            },
            EngineError::InvalidCronExpression(msg) => ApiError::BadRequest {
                error: "invalid_cron_expression".to_string(),
                message: msg.clone(),
            },
            EngineError::Spawn(msg) => ApiError::BadRequest {
                error: "spawn_error".to_string(),
                message: msg.clone(),
            },
            EngineError::TriggerDepthExceeded(max) => ApiError::BadRequest {
                error: "trigger_depth_exceeded".to_string(),
                message: format!("Trigger depth limit ({max}) exceeded"),
            },
            EngineError::TaskLimitExceeded(max) => ApiError::BadRequest {
                error: "task_limit_exceeded".to_string(),
                message: format!("Flow task limit ({max}) exceeded"),
            },
            EngineError::FlowLimitExceeded(queue, max) => ApiError::TooManyRequests {
                error: "flow_limit_exceeded".to_string(),
                message: format!("Queue '{queue}' has reached its pending flow limit ({max})"),
            },
            EngineError::InvalidQueueConfig(msg) => ApiError::BadRequest {
                error: "invalid_queue_config".to_string(),
                message: msg.clone(),
            },
            EngineError::Storage(e) => map_storage_error(e),
            EngineError::Export(msg) => ApiError::Internal {
                error: "export_failed".to_string(),
                message: msg.clone(),
            },
        }
    }
}

fn map_storage_error(err: &tasked::store::StorageError) -> ApiError {
    use tasked::store::StorageError;
    match err {
        StorageError::QueueAlreadyExists(id) => ApiError::Conflict {
            error: "queue_already_exists".to_string(),
            message: format!("Queue '{id}' already exists"),
        },
        StorageError::QueueNotFound(id) => ApiError::NotFound {
            error: "queue_not_found".to_string(),
            message: format!("Queue '{id}' not found"),
        },
        StorageError::FlowNotFound(id) => ApiError::NotFound {
            error: "flow_not_found".to_string(),
            message: format!("Flow '{id}' not found"),
        },
        StorageError::TaskNotFound(tid, fid) => ApiError::NotFound {
            error: "task_not_found".to_string(),
            message: format!("Task '{tid}' not found in flow '{fid}'"),
        },
        StorageError::InvalidStateTransition(from, to) => ApiError::Conflict {
            error: "invalid_state_transition".to_string(),
            message: format!("Invalid state transition: {from} -> {to}"),
        },
        StorageError::ScheduleNotFound(id) => ApiError::NotFound {
            error: "schedule_not_found".to_string(),
            message: format!("Schedule '{id}' not found"),
        },
        StorageError::Internal(msg) => ApiError::Internal {
            error: "internal_error".to_string(),
            message: msg.clone(),
        },
    }
}

// -- Route handlers --

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

async fn create_queue(
    State(engine): State<AppState>,
    Json(req): Json<CreateQueueRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let queue_id = QueueId::from(req.id);
    let queue = engine.create_queue(&queue_id, req.config).await?;
    Ok((StatusCode::CREATED, Json(QueueResponse::from(queue))))
}

async fn list_queues(State(engine): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let queues = engine.list_queues().await?;
    let response: Vec<QueueResponse> = queues.into_iter().map(QueueResponse::from).collect();
    Ok(Json(response))
}

async fn get_queue(
    State(engine): State<AppState>,
    Path(qid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let not_found_msg = format!("Queue '{qid}' not found");
    let queue_id = QueueId::from(qid);
    let queue = engine
        .get_queue(&queue_id)
        .await?
        .ok_or_else(|| ApiError::NotFound {
            error: "queue_not_found".to_string(),
            message: not_found_msg,
        })?;
    Ok(Json(QueueResponse::from(queue)))
}

async fn delete_queue(
    State(engine): State<AppState>,
    Path(qid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let queue_id = QueueId::from(qid);
    engine.delete_queue(&queue_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn submit_flow(
    State(engine): State<AppState>,
    Path(qid): Path<String>,
    Json(flow_def): Json<FlowDef>,
) -> Result<impl IntoResponse, ApiError> {
    let queue_id = QueueId::from(qid);
    let flow = engine.submit_flow(&queue_id, flow_def).await?;
    Ok((StatusCode::CREATED, Json(FlowResponse::from(flow))))
}

async fn list_flows(
    State(engine): State<AppState>,
    Path(qid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let not_found_msg = format!("Queue '{qid}' not found");
    let queue_id = QueueId::from(qid);

    // Verify queue exists
    engine
        .get_queue(&queue_id)
        .await?
        .ok_or_else(|| ApiError::NotFound {
            error: "queue_not_found".to_string(),
            message: not_found_msg,
        })?;

    let flows = engine.list_flows(&queue_id, None).await?;
    let response: Vec<FlowResponse> = flows.into_iter().map(FlowResponse::from).collect();
    Ok(Json(response))
}

async fn get_flow(
    State(engine): State<AppState>,
    Path(fid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let not_found_msg = format!("Flow '{fid}' not found");
    let flow_id = FlowId::from(fid);
    let (flow, tasks) =
        engine
            .get_flow_with_tasks(&flow_id)
            .await?
            .ok_or_else(|| ApiError::NotFound {
                error: "flow_not_found".to_string(),
                message: not_found_msg,
            })?;

    let response = FlowDetailResponse {
        id: flow.id.to_string(),
        queue_id: flow.queue_id.to_string(),
        state: flow.state.to_string(),
        task_count: flow.task_count,
        tasks_succeeded: flow.tasks_succeeded,
        tasks_failed: flow.tasks_failed,
        tasks: tasks.into_iter().map(TaskResponse::from).collect(),
        created_at: flow.created_at.to_rfc3339(),
        updated_at: flow.updated_at.to_rfc3339(),
    };

    Ok(Json(response))
}

async fn cancel_flow(
    State(engine): State<AppState>,
    Path(fid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let not_found_msg = format!("Flow '{fid}' not found");
    let flow_id = FlowId::from(fid);

    // Verify flow exists
    engine
        .get_flow(&flow_id)
        .await?
        .ok_or_else(|| ApiError::NotFound {
            error: "flow_not_found".to_string(),
            message: not_found_msg,
        })?;

    engine.cancel_flow(&flow_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn ack_task(
    State(engine): State<AppState>,
    Path((fid, tid)): Path<(String, String)>,
    Json(req): Json<AckRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let not_found_msg = format!("Task '{tid}' not found in flow '{fid}'");
    let flow_id = FlowId::from(fid);
    let task_id = TaskId::from(tid);

    // Get the task to pass to handle_task_result
    let task = engine
        .get_task(&task_id, &flow_id)
        .await?
        .ok_or_else(|| ApiError::NotFound {
            error: "task_not_found".to_string(),
            message: not_found_msg,
        })?;

    // Merge approval metadata into output if provided
    let output = match (&req.approved_by, req.output) {
        (Some(who), Some(serde_json::Value::Object(mut map))) => {
            map.insert(
                "approved_by".to_string(),
                serde_json::Value::String(who.clone()),
            );
            map.insert(
                "acked_at".to_string(),
                serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
            );
            Some(serde_json::Value::Object(map))
        }
        (Some(who), other) => {
            let mut map = serde_json::Map::new();
            if let Some(val) = other {
                map.insert("original_output".to_string(), val);
            }
            map.insert(
                "approved_by".to_string(),
                serde_json::Value::String(who.clone()),
            );
            map.insert(
                "acked_at".to_string(),
                serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
            );
            Some(serde_json::Value::Object(map))
        }
        (None, output) => output,
    };

    let result = match req.status.as_str() {
        "success" => ExecuteResult::Success { output },
        "failed" => ExecuteResult::Failed {
            error: req.error.unwrap_or_else(|| "unknown error".to_string()),
            retryable: req.retryable.unwrap_or(false),
        },
        other => {
            return Err(ApiError::BadRequest {
                error: "invalid_status".to_string(),
                message: format!("Invalid ack status: '{other}'. Must be 'success' or 'failed'"),
            });
        }
    };

    engine.handle_task_result(&task, result).await?;
    Ok(StatusCode::NO_CONTENT)
}

// -- Schedule handlers --

async fn create_schedule_handler(
    State(engine): State<AppState>,
    Path(qid): Path<String>,
    Json(req): Json<ScheduleRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let queue_id = QueueId::from(qid);
    let schedule = engine
        .create_schedule(
            &queue_id,
            ScheduleDef {
                cron: req.cron,
                flow: req.flow,
                name: req.name,
                enabled: req.enabled,
            },
        )
        .await?;
    Ok((StatusCode::CREATED, Json(ScheduleResponse::from(schedule))))
}

async fn list_schedules_handler(
    State(engine): State<AppState>,
    Path(qid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let queue_id = QueueId::from(qid);
    let schedules = engine.list_schedules(&queue_id).await?;
    let response: Vec<ScheduleResponse> =
        schedules.into_iter().map(ScheduleResponse::from).collect();
    Ok(Json(response))
}

async fn get_schedule_handler(
    State(engine): State<AppState>,
    Path(sid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let not_found_msg = format!("Schedule '{sid}' not found");
    let schedule_id = ScheduleId::from(sid);
    let schedule = engine
        .get_schedule(&schedule_id)
        .await?
        .ok_or_else(|| ApiError::NotFound {
            error: "schedule_not_found".into(),
            message: not_found_msg,
        })?;
    Ok(Json(ScheduleResponse::from(schedule)))
}

async fn update_schedule_handler(
    State(engine): State<AppState>,
    Path(sid): Path<String>,
    Json(req): Json<ScheduleRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let schedule_id = ScheduleId::from(sid);
    let schedule = engine
        .update_schedule(
            &schedule_id,
            ScheduleDef {
                cron: req.cron,
                flow: req.flow,
                name: req.name,
                enabled: req.enabled,
            },
        )
        .await?;
    Ok(Json(ScheduleResponse::from(schedule)))
}

async fn delete_schedule_handler(
    State(engine): State<AppState>,
    Path(sid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let schedule_id = ScheduleId::from(sid);
    engine.delete_schedule(&schedule_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

// -- SSE handler --

/// Maximum number of concurrent SSE connections.
static SSE_SEMAPHORE: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(100));

async fn flow_events(
    State(engine): State<AppState>,
    Path(fid): Path<String>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError> {
    let permit = SSE_SEMAPHORE
        .try_acquire()
        .map_err(|_| ApiError::ServiceUnavailable {
            error: "too_many_connections".to_string(),
            message: "Too many concurrent SSE connections".to_string(),
        })?;

    let not_found_msg = format!("Flow '{fid}' not found");
    let flow_id = FlowId::from(fid);

    // Verify flow exists
    engine
        .get_flow(&flow_id)
        .await?
        .ok_or_else(|| ApiError::NotFound {
            error: "flow_not_found".to_string(),
            message: not_found_msg,
        })?;

    let stream = async_stream::stream! {
        // Hold the permit for the lifetime of the SSE stream so it is released on disconnect.
        let _permit = permit;
        let mut prev_states: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        let mut prev_outputs: std::collections::HashMap<String, Option<serde_json::Value>> = std::collections::HashMap::new();

        loop {
            let tasks = match engine.get_flow_tasks(&flow_id).await {
                Ok(tasks) => tasks,
                Err(e) => {
                    let error_data = serde_json::json!({
                        "error": "engine_error",
                        "message": e.to_string(),
                    });
                    if let Ok(data) = serde_json::to_string(&error_data) {
                        yield Ok(Event::default().event("error").data(data));
                    }
                    break;
                }
            };

            for task in &tasks {
                let state_str = task.state.to_string();
                let prev_state = prev_states.get(task.id.as_str());

                // Emit on state change
                if prev_state != Some(&state_str) {
                    prev_states.insert(task.id.to_string(), state_str.clone());

                    let event_data = serde_json::json!({
                        "task_id": task.id.as_str(),
                        "state": state_str,
                        "output": task.output,
                        "error": task.error,
                        "started_at": task.started_at.map(|d| d.to_rfc3339()),
                        "completed_at": task.completed_at.map(|d| d.to_rfc3339()),
                    });

                    if let Ok(data) = serde_json::to_string(&event_data) {
                        yield Ok(Event::default().event("task_state").data(data));
                    }
                }

                // Emit on output change (for streaming output updates while Running)
                if task.state == tasked::types::TaskState::Running {
                    let prev_output = prev_outputs.get(task.id.as_str());
                    if prev_output != Some(&task.output) {
                        prev_outputs.insert(task.id.to_string(), task.output.clone());
                        if let Some(ref output) = task.output
                            && let Ok(data) = serde_json::to_string(&serde_json::json!({
                                "task_id": task.id.as_str(),
                                "output": output,
                            }))
                        {
                            yield Ok(Event::default().event("task_output").data(data));
                        }
                    }
                }
            }

            // Check flow terminal state
            match engine.get_flow(&flow_id).await {
                Ok(Some(flow)) if flow.state.is_terminal() => {
                    let event_data = serde_json::json!({
                        "flow_id": flow.id.as_str(),
                        "state": flow.state.to_string(),
                        "task_count": flow.task_count,
                        "tasks_succeeded": flow.tasks_succeeded,
                        "tasks_failed": flow.tasks_failed,
                    });
                    if let Ok(data) = serde_json::to_string(&event_data) {
                        yield Ok(Event::default().event("flow_complete").data(data));
                    }
                    break;
                }
                Err(e) => {
                    let error_data = serde_json::json!({
                        "error": "engine_error",
                        "message": e.to_string(),
                    });
                    if let Ok(data) = serde_json::to_string(&error_data) {
                        yield Ok(Event::default().event("error").data(data));
                    }
                    break;
                }
                _ => {}
            }

            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    };

    Ok(Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("ping"),
    ))
}

/// Prometheus metrics endpoint using the shared handle.
async fn metrics_handler_with_state(
    State(handle): State<metrics_exporter_prometheus::PrometheusHandle>,
) -> impl IntoResponse {
    handle.render()
}

// -- Artifact handlers --

async fn upload_artifact(
    State(engine): State<AppState>,
    Path((fid, name)): Path<(String, String)>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let not_found_msg = format!("Flow '{fid}' not found");
    let flow_id = FlowId::from(fid);
    engine
        .get_flow(&flow_id)
        .await?
        .ok_or_else(|| ApiError::NotFound {
            error: "flow_not_found".to_string(),
            message: not_found_msg,
        })?;

    let artifacts = engine.artifact_store().ok_or_else(|| ApiError::Internal {
        error: "artifacts_not_configured".to_string(),
        message: "Artifact storage not configured".to_string(),
    })?;

    artifacts
        .upload(&flow_id, &name, &body)
        .await
        .map_err(|e| ApiError::Internal {
            error: "artifact_upload_failed".to_string(),
            message: e.to_string(),
        })?;

    Ok(StatusCode::CREATED)
}

async fn download_artifact(
    State(engine): State<AppState>,
    Path((fid, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let flow_id = FlowId::from(fid);
    let artifacts = engine.artifact_store().ok_or_else(|| ApiError::Internal {
        error: "artifacts_not_configured".to_string(),
        message: "Artifact storage not configured".to_string(),
    })?;

    let data = artifacts
        .download(&flow_id, &name)
        .await
        .map_err(|e| match &e {
            tasked::artifacts::ArtifactError::Io(io_err)
                if io_err.kind() == std::io::ErrorKind::NotFound =>
            {
                ApiError::NotFound {
                    error: "artifact_not_found".to_string(),
                    message: e.to_string(),
                }
            }
            tasked::artifacts::ArtifactError::InvalidName(_) => ApiError::NotFound {
                error: "artifact_not_found".to_string(),
                message: e.to_string(),
            },
            _ => ApiError::Internal {
                error: "artifact_download_failed".to_string(),
                message: e.to_string(),
            },
        })?;

    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        data,
    ))
}

async fn list_artifacts(
    State(engine): State<AppState>,
    Path(fid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let flow_id = FlowId::from(fid);
    let artifacts = engine.artifact_store().ok_or_else(|| ApiError::Internal {
        error: "artifacts_not_configured".to_string(),
        message: "Artifact storage not configured".to_string(),
    })?;

    let names = artifacts
        .list(&flow_id)
        .await
        .map_err(|e| ApiError::Internal {
            error: "artifact_list_failed".to_string(),
            message: e.to_string(),
        })?;

    Ok(Json(names))
}

// -- Export handler --

#[derive(Deserialize)]
struct ExportParams {
    #[serde(default)]
    with_artifacts: bool,
    /// Export format: "json" (default) or "tar" (tar.gz archive with artifacts).
    #[serde(default)]
    format: Option<String>,
}

fn map_export_error(e: EngineError, not_found_msg: String) -> ApiError {
    match e {
        EngineError::Storage(tasked::store::StorageError::FlowNotFound(_)) => ApiError::NotFound {
            error: "flow_not_found".to_string(),
            message: not_found_msg,
        },
        _ => ApiError::Internal {
            error: "export_failed".to_string(),
            message: e.to_string(),
        },
    }
}

async fn export_flow_handler(
    State(engine): State<AppState>,
    Path(fid): Path<String>,
    Query(params): Query<ExportParams>,
) -> Result<axum::response::Response, ApiError> {
    let not_found_msg = format!("Flow '{fid}' not found");
    let flow_id = FlowId::from(fid);

    let format = params.format.as_deref().unwrap_or("json");
    match format {
        "tar" => {
            let tar_bytes = engine
                .export_flow_tar(&flow_id)
                .await
                .map_err(|e| map_export_error(e, not_found_msg))?;
            Ok(axum::response::Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/gzip")
                .header(
                    "content-disposition",
                    format!("attachment; filename=\"{}.tar.gz\"", flow_id.as_str()),
                )
                .body(Body::from(tar_bytes))
                .unwrap())
        }
        "json" => {
            let export = engine
                .export_flow(&flow_id, params.with_artifacts)
                .await
                .map_err(|e| map_export_error(e, not_found_msg))?;
            Ok(Json(export).into_response())
        }
        other => Err(ApiError::BadRequest {
            error: "invalid_format".to_string(),
            message: format!("Unknown export format '{other}'. Supported formats: json, tar"),
        }),
    }
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

fn build_router(engine: Arc<Engine>, cors_origins: &[String]) -> Router {
    Router::new()
        // Health check
        .route("/healthz", get(health))
        // Queue routes
        .route("/api/v1/queues", post(create_queue).get(list_queues))
        .route("/api/v1/queues/{qid}", get(get_queue).delete(delete_queue))
        // Flow routes
        .route(
            "/api/v1/queues/{qid}/flows",
            post(submit_flow).get(list_flows),
        )
        .route("/api/v1/flows/{fid}", get(get_flow).delete(cancel_flow))
        // Flow export
        .route("/api/v1/flows/{fid}/export", get(export_flow_handler))
        // Flow SSE events
        .route("/api/v1/flows/{fid}/events", get(flow_events))
        // Task ack
        .route("/api/v1/flows/{fid}/tasks/{tid}/ack", post(ack_task))
        // Artifact routes
        .route("/api/v1/flows/{fid}/artifacts", get(list_artifacts))
        .route(
            "/api/v1/flows/{fid}/artifacts/{*name}",
            get(download_artifact).put(upload_artifact),
        )
        // Schedule routes
        .route(
            "/api/v1/queues/{qid}/schedules",
            post(create_schedule_handler).get(list_schedules_handler),
        )
        .route(
            "/api/v1/schedules/{sid}",
            get(get_schedule_handler)
                .put(update_schedule_handler)
                .delete(delete_schedule_handler),
        )
        // Middleware (order matters: outermost layer runs first)
        .layer(axum::middleware::from_fn(metrics_middleware))
        .layer(TraceLayer::new_for_http())
        .layer(build_cors_layer(cors_origins))
        .layer(axum::extract::DefaultBodyLimit::max(10 * 1024 * 1024)) // 10 MB
        .with_state(engine)
}

fn build_router_with_metrics(
    engine: Arc<Engine>,
    metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
    cors_origins: &[String],
) -> Router {
    let metrics_router = Router::new()
        .route("/metrics", get(metrics_handler_with_state))
        .with_state(metrics_handle);

    build_router(engine, cors_origins).merge(metrics_router)
}

fn register_executors(
    engine: &mut Engine,
    integrations_dir: Option<&str>,
    token_cache_path: Option<&str>,
) {
    // Orchestration executors — always run locally (no user code).
    engine.register_executor("http", Arc::new(HttpExecutor::new()));
    engine.register_executor("noop", Arc::new(NoopExecutor));
    engine.register_executor("callback", Arc::new(CallbackExecutor::always_succeed()));
    engine.register_executor("delay", Arc::new(DelayExecutor));
    engine.register_executor("approval", Arc::new(ApprovalExecutor));
    engine.register_executor("remote", Arc::new(RemoteExecutor::new()));
    engine.register_executor("api", Arc::new(InlineApiExecutor::new()));

    // Local mode: shell runs locally, container uses Docker.
    engine.register_executor("shell", Arc::new(ShellExecutor));

    {
        use tasked::executor::agent::AgentExecutor;
        use tasked::executor::container::{ContainerExecutor, docker::DockerBackend};

        if let Ok(backend) = DockerBackend::new() {
            tracing::info!("using Docker container backend");
            engine.register_executor("container", Arc::new(ContainerExecutor::new(backend)));

            if let Ok(agent_backend) = DockerBackend::new() {
                engine.register_executor(
                    "agent",
                    Arc::new(AgentExecutor::new(ContainerExecutor::new(agent_backend))),
                );
            }
        }
    }

    // Trigger executor: submits child flows and optionally waits for completion.
    {
        use tasked::executor::trigger::TriggerExecutor;
        engine.register_executor("trigger", Arc::new(TriggerExecutor));
    }

    // Load integration definitions from directory (each registers as a named executor).
    if let Some(dir) = integrations_dir {
        use tasked::executor::api::oauth2::TokenCache;

        let token_cache = token_cache_path
            .map(|p| Arc::new(TokenCache::with_persistence(std::path::Path::new(p))));

        let path = std::path::Path::new(dir);
        let count = api::register_integrations(engine, path, token_cache);
        if count > 0 {
            info!(count, dir, "loaded integration executors");
        }
    }

    // Spawn executor: delegates to any registered executor, parses output as tasks.
    // Registered last so it can see all other executors (including integrations).
    {
        use tasked::executor::spawn::SpawnExecutor;
        let executors = engine.executors().clone();
        engine.register_executor("spawn", Arc::new(SpawnExecutor::new(executors)));
    }
}

/// Constant-time byte comparison to prevent timing attacks on API key validation.
/// Uses `subtle::ConstantTimeEq` which does not short-circuit on length mismatch.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    a.ct_eq(b).into()
}

fn add_auth_layer(app: Router, auth_mode: &str, api_key: Option<&str>) -> Router {
    match auth_mode {
        "api-key" => {
            let key = api_key
                .expect("--api-key required when --auth-mode=api-key")
                .to_string();
            app.layer(axum::middleware::from_fn(
                move |req: Request<Body>, next: Next| {
                    let key = key.clone();
                    async move {
                        let auth_header = req.headers().get("authorization");
                        let expected = format!("Bearer {key}");
                        match auth_header.and_then(|v| v.to_str().ok()) {
                            Some(val) if constant_time_eq(val.as_bytes(), expected.as_bytes()) => {
                                next.run(req).await
                            }
                            _ => {
                                let body = Json(serde_json::json!({
                                    "error": "unauthorized",
                                    "message": "Invalid or missing API key"
                                }));
                                (StatusCode::UNAUTHORIZED, body).into_response()
                            }
                        }
                    }
                },
            ))
        }
        "none" => app,
        other => {
            eprintln!("fatal: unrecognized auth mode '{other}'. Valid modes: none, api-key");
            std::process::exit(1);
        }
    }
}

// -- Main --

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Serve {
            data_dir,
            engine,
            port,
            host,
            auth_mode,
            api_key,
            metrics_push_url,
            metrics_port,
            integrations_dir,
            token_cache,
            cors_origin,
        } => {
            // Verbose logging for server mode
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                        "tasked_server=info,tasked=info,tower_http=info"
                            .parse()
                            .unwrap()
                    }),
                )
                .init();
            run_serve(
                data_dir,
                engine,
                port,
                host,
                auth_mode,
                api_key,
                metrics_push_url,
                metrics_port,
                integrations_dir,
                token_cache,
                cors_origin,
            )
            .await;
        }
        Commands::Run {
            file,
            queue,
            db,
            auto_approve,
            output,
            integrations_dir,
            token_cache,
        } => {
            // Quiet logging for run mode — clean output only
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "warn".parse().unwrap()),
                )
                .init();
            let code = run_flow(
                file,
                queue,
                db,
                auto_approve,
                output,
                integrations_dir,
                token_cache,
            )
            .await;
            std::process::exit(code);
        }
        Commands::Mcp { data_dir, engine } => {
            // Quiet logging for MCP mode — output goes to stderr
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "warn".parse().unwrap()),
                )
                .with_writer(std::io::stderr)
                .init();
            mcp::run_mcp_server(data_dir, engine).await;
        }
        Commands::Export {
            flow_id,
            server,
            with_artifacts,
            output,
            format,
            api_key,
        } => {
            let url = format!(
                "{}/api/v1/flows/{}/export?with_artifacts={}&format={}",
                server.trim_end_matches('/'),
                flow_id,
                with_artifacts,
                format,
            );
            let client = reqwest::Client::new();
            let mut req = client.get(&url);
            if let Some(key) = &api_key {
                req = req.header("authorization", format!("Bearer {key}"));
            }
            let resp = req.send().await.unwrap_or_else(|e| {
                eprintln!("Error connecting to server: {e}");
                std::process::exit(1);
            });

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                eprintln!("Error (HTTP {status}): {body}");
                std::process::exit(1);
            }

            let is_tar = format == "tar";
            let bytes = resp.bytes().await.unwrap_or_else(|e| {
                eprintln!("Error reading response: {e}");
                std::process::exit(1);
            });

            match output {
                Some(path) if path != "-" => {
                    if let Err(e) = std::fs::write(&path, &bytes) {
                        eprintln!("Error writing to {path}: {e}");
                        std::process::exit(1);
                    }
                    eprintln!("Export written to {path}");
                }
                _ => {
                    if is_tar {
                        use std::io::Write;
                        std::io::stdout().write_all(&bytes).unwrap_or_else(|e| {
                            eprintln!("Error writing to stdout: {e}");
                            std::process::exit(1);
                        });
                    } else {
                        let text = String::from_utf8_lossy(&bytes);
                        println!("{text}");
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_serve(
    data_dir: String,
    engine_mode: String,
    port: u16,
    host: String,
    auth_mode: String,
    api_key: Option<String>,
    metrics_push_url: Option<String>,
    metrics_port: Option<u16>,
    integrations_dir: Option<String>,
    token_cache: Option<String>,
    cors_origins: Vec<String>,
) {
    // Install Prometheus metrics recorder with histogram buckets for request latency
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full(
                "tasked_http_request_duration_seconds".to_owned(),
            ),
            &[
                0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
            ],
        )
        .expect("failed to set histogram buckets")
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full(
                "tasked_task_execution_duration_seconds".to_owned(),
            ),
            &[
                0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0, 60.0, 300.0,
            ],
        )
        .expect("failed to set histogram buckets")
        .build_recorder();
    let metrics_handle = recorder.handle();
    metrics::set_global_recorder(recorder).expect("failed to install metrics recorder");

    // Create storage backend based on --engine flag
    let storage: Arc<dyn tasked::store::Storage> = match engine_mode.as_str() {
        "sqlite" => {
            let storage = ShardedStorage::open(&data_dir).expect("failed to open data directory");
            Arc::new(storage)
        }
        #[cfg(feature = "journaled")]
        "journal" => {
            let data_path = std::path::PathBuf::from(&data_dir);
            std::fs::create_dir_all(&data_path).expect("failed to create data directory");
            let config = tasked::store::journaled::config::JournalConfig {
                journal_path: Some(data_path.join("journal.db")),
                snapshot_path: Some(data_path.join("snapshot.db")),
                ..Default::default()
            };
            let storage = tasked::store::journaled::JournaledStorage::open(config)
                .expect("failed to open journaled storage");
            info!(engine = "journal", data_dir = %data_dir, "using journaled in-memory engine");
            Arc::new(storage)
        }
        other => {
            eprintln!("unknown engine mode: {other} (valid: sqlite, journal)");
            std::process::exit(1);
        }
    };

    // Create engine
    let mut engine = Engine::new(storage, EngineConfig::default());
    register_executors(
        &mut engine,
        integrations_dir.as_deref(),
        token_cache.as_deref(),
    );

    // Configure artifact storage
    let artifacts_dir = std::path::PathBuf::from(&data_dir).join("artifacts");
    engine.set_artifact_store(Arc::new(tasked::artifacts::LocalArtifactStore::new(
        &artifacts_dir,
    )));

    let engine = Arc::new(engine);

    // Clone for the engine loop — spawned after listener binds (see below).
    let engine_handle = engine.clone();

    // Warn if running without authentication on a non-localhost address
    if auth_mode == "none" && host != "127.0.0.1" && host != "localhost" && host != "::1" {
        tracing::warn!(
            host = %host,
            "server starting with NO authentication on a non-localhost address — \
             anyone who can reach this address can submit flows and execute commands. \
             Use --auth-mode=api-key for production deployments."
        );
    }

    // Build router: if --metrics-port is set, serve metrics on a separate listener;
    // otherwise keep metrics on the main port (with a warning when auth is disabled).
    let app = if metrics_port.is_some() {
        build_router(engine, &cors_origins)
    } else {
        if auth_mode == "none" {
            tracing::warn!(
                "metrics endpoint (/metrics) is unauthenticated because --auth-mode=none. \
                 Use --metrics-port to serve metrics on a separate listener."
            );
        }
        build_router_with_metrics(engine, metrics_handle.clone(), &cors_origins)
    };

    // Apply auth layer
    let app = add_auth_layer(app, &auth_mode, api_key.as_deref());

    // Spawn dedicated metrics listener if configured
    if let Some(m_port) = metrics_port {
        let handle = metrics_handle.clone();
        tokio::spawn(async move {
            let metrics_router = Router::new()
                .route("/metrics", get(metrics_handler_with_state))
                .with_state(handle);

            let metrics_addr = format!("127.0.0.1:{m_port}");
            info!(addr = %metrics_addr, "starting dedicated metrics listener");

            let listener = tokio::net::TcpListener::bind(&metrics_addr)
                .await
                .expect("failed to bind metrics listener");

            axum::serve(listener, metrics_router)
                .await
                .expect("metrics server error");
        });
    }

    // Spawn metrics push task if configured
    if let Some(push_url) = metrics_push_url {
        let handle = metrics_handle;
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                let metrics_text = handle.render();
                if let Err(e) = client
                    .post(&push_url)
                    .header("content-type", "text/plain")
                    .body(metrics_text)
                    .send()
                    .await
                {
                    tracing::warn!(error = %e, "metrics push failed");
                }
            }
        });
    }

    // Start server
    let addr = format!("{host}:{port}");
    info!(addr = %addr, data_dir = %data_dir, "starting tasked-server");

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("failed to bind");

    // Spawn engine processing loop AFTER listener is bound.
    // Recovery dispatch happens inside run() — by deferring it until after
    // the listener is ready, healthcheck and API endpoints are available
    // immediately even when recovering a large backlog.
    tokio::spawn(async move {
        engine_handle.run().await;
    });

    axum::serve(listener, app).await.expect("server error");
}

async fn run_flow(
    file: String,
    queue: String,
    db: String,
    auto_approve: bool,
    output_file: Option<String>,
    integrations_dir: Option<String>,
    token_cache: Option<String>,
) -> i32 {
    // Read flow definition from file
    let content = match std::fs::read_to_string(&file) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error reading flow file '{file}': {e}");
            return 1;
        }
    };

    let flow_def: FlowDef = match serde_json::from_str(&content) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error parsing flow JSON: {e}");
            return 1;
        }
    };

    // Create storage
    let storage: Arc<dyn tasked::store::Storage> = if db == ":memory:" {
        Arc::new(MemoryStorage::new())
    } else {
        match SqliteStorage::open(&db) {
            Ok(s) => Arc::new(s),
            Err(e) => {
                eprintln!("Error opening database '{db}': {e}");
                return 1;
            }
        }
    };

    // Create engine
    let mut engine = Engine::new(
        storage,
        EngineConfig {
            poll_interval: std::time::Duration::from_millis(100),
            ..EngineConfig::default()
        },
    );
    register_executors(
        &mut engine,
        integrations_dir.as_deref(),
        token_cache.as_deref(),
    );

    // Configure artifact storage in temp directory
    let artifacts_dir = std::env::temp_dir().join("tasked-artifacts");
    engine.set_artifact_store(Arc::new(tasked::artifacts::LocalArtifactStore::new(
        &artifacts_dir,
    )));

    let engine = Arc::new(engine);

    // Create queue if it doesn't exist
    let queue_id = QueueId::from(queue.clone());
    if engine.get_queue(&queue_id).await.unwrap_or(None).is_none()
        && let Err(e) = engine.create_queue(&queue_id, QueueConfig::default()).await
    {
        eprintln!("Error creating queue '{queue}': {e}");
        return 1;
    }

    // Submit the flow
    let flow = match engine.submit_flow(&queue_id, flow_def).await {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error submitting flow: {e}");
            return 1;
        }
    };

    const DIM: &str = "\x1b[2m";
    const RESET: &str = "\x1b[0m";

    let flow_short = flow.id.as_str().get(..8).unwrap_or(flow.id.as_str());
    println!(
        "\x1b[33m▸\x1b[0m {DIM}Flow {flow_short} submitted ({} tasks){RESET}",
        flow.task_count
    );

    // Spawn the engine loop for concurrent execution
    let engine_loop = engine.clone();
    let engine_handle = tokio::spawn(async move {
        engine_loop.run().await;
    });

    let start = std::time::Instant::now();

    // Compute max task name length for column alignment
    let initial_tasks = engine.get_flow_tasks(&flow.id).await.unwrap_or_default();
    let max_len = initial_tasks
        .iter()
        .map(|t| t.id.as_str().len())
        .max()
        .unwrap_or(4);

    // Track displayed state per task
    // "none" = not yet printed, "running" = showing running line, "done" = final state printed
    let mut displayed: std::collections::HashMap<String, &str> = std::collections::HashMap::new();
    // Count of "running" lines currently visible (for cursor-up erasing)
    let mut running_lines: usize = 0;
    // Track which approval tasks have already been prompted/auto-approved
    let mut approvals_handled: std::collections::HashSet<String> = std::collections::HashSet::new();

    let flow_id = flow.id.clone();
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let tasks = engine.get_flow_tasks(&flow_id).await.unwrap_or_default();

        // Find newly completed tasks (were running or unseen, now terminal)
        let mut newly_done: Vec<&Task> = Vec::new();
        let mut still_running: Vec<&Task> = Vec::new();
        let mut newly_running: Vec<&Task> = Vec::new();

        for task in &tasks {
            let prev = *displayed.get(task.id.as_str()).unwrap_or(&"none");
            match task.state {
                TaskState::Succeeded | TaskState::Failed | TaskState::Cancelled
                    if prev != "done" =>
                {
                    newly_done.push(task);
                }
                TaskState::Running if prev == "none" => {
                    newly_running.push(task);
                }
                TaskState::Running if prev == "running" => {
                    still_running.push(task);
                }
                _ => {}
            }
        }

        // Handle approval tasks (Running with awaiting_approval output)
        for task in &tasks {
            if task.state == TaskState::Running
                && !approvals_handled.contains(task.id.as_str())
                && let Some(ref output) = task.output
                && output.get("awaiting_approval").and_then(|v| v.as_bool()) == Some(true)
            {
                approvals_handled.insert(task.id.to_string());
                let message = output
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Approval required");

                // Erase running lines before prompting
                for _ in 0..running_lines {
                    print!("\x1b[A\x1b[2K");
                }
                running_lines = 0;

                let padded = format!("{:<width$}", task.id.as_str(), width = max_len);
                let approved = if auto_approve {
                    println!("\x1b[35m?\x1b[0m [{padded}]  {message} {DIM}(auto-approved){RESET}");
                    true
                } else {
                    print!("\x1b[35m?\x1b[0m [{padded}]  {message} \x1b[1m[y/N]\x1b[0m ");
                    use std::io::Write;
                    if let Err(e) = std::io::stdout().flush() {
                        eprintln!("Error flushing stdout: {e}");
                        false
                    } else {
                        let mut input = String::new();
                        if let Err(e) = std::io::stdin().read_line(&mut input) {
                            eprintln!("Error reading stdin: {e}");
                            false
                        } else {
                            let answer = input.trim().to_lowercase();
                            answer == "y" || answer == "yes"
                        }
                    }
                };

                if approved {
                    let result = ExecuteResult::Success {
                        output: Some(serde_json::json!({"approved": true, "approved_by": "cli"})),
                    };
                    if let Err(e) = engine.handle_task_result(task, result).await {
                        eprintln!("Error approving task: {e}");
                    }
                } else {
                    let result = ExecuteResult::Failed {
                        error: "rejected by user".to_string(),
                        retryable: false,
                    };
                    if let Err(e) = engine.handle_task_result(task, result).await {
                        eprintln!("Error rejecting task: {e}");
                    }
                }
            }
        }

        if newly_done.is_empty() && newly_running.is_empty() {
            // Check flow completion
            let flow = match engine.get_flow(&flow_id).await {
                Ok(Some(f)) => f,
                Ok(None) => {
                    eprintln!("Flow disappeared unexpectedly");
                    return 1;
                }
                Err(e) => {
                    eprintln!("Error fetching flow: {e}");
                    return 1;
                }
            };
            if flow.state.is_terminal() {
                // Erase any remaining running lines
                for _ in 0..running_lines {
                    print!("\x1b[A\x1b[2K");
                }
                let elapsed = format!("{:.1}s", start.elapsed().as_secs_f64());
                println!();
                match flow.state {
                    FlowState::Succeeded => println!(
                        "\x1b[32m✓\x1b[0m \x1b[32m\x1b[1mFlow complete\x1b[0m  {DIM}{}/{} tasks succeeded ({elapsed}){RESET}",
                        flow.tasks_succeeded, flow.task_count
                    ),
                    FlowState::Failed => println!(
                        "\x1b[31m✗\x1b[0m \x1b[31m\x1b[1mFlow failed\x1b[0m  {DIM}{} succeeded, {} failed ({elapsed}){RESET}",
                        flow.tasks_succeeded, flow.tasks_failed
                    ),
                    FlowState::Cancelled => println!("{DIM}– Flow cancelled ({elapsed}){RESET}"),
                    _ => {}
                }
                // Write output file if requested
                if let Some(ref path) = output_file {
                    let tasks = engine.get_flow_tasks(&flow_id).await.unwrap_or_default();
                    let outputs: serde_json::Map<String, serde_json::Value> = tasks
                        .iter()
                        .map(|t| {
                            (
                                t.id.to_string(),
                                serde_json::json!({
                                    "state": t.state.to_string(),
                                    "output": t.output,
                                    "error": t.error,
                                }),
                            )
                        })
                        .collect();
                    let json = match serde_json::to_string_pretty(&outputs) {
                        Ok(j) => j,
                        Err(e) => {
                            eprintln!("Error serializing outputs: {e}");
                            return 1;
                        }
                    };
                    if path == "-" {
                        println!("{json}");
                    } else if let Err(e) = std::fs::write(path, &json) {
                        eprintln!("{DIM}Warning: failed to write output file: {e}{RESET}");
                    } else {
                        println!("{DIM}Output written to {path}{RESET}");
                    }
                }

                engine_handle.abort();
                return if flow.state == FlowState::Succeeded {
                    0
                } else {
                    1
                };
            }
            continue;
        }

        // Erase current running lines (they'll be reprinted or replaced)
        for _ in 0..running_lines {
            print!("\x1b[A\x1b[2K");
        }

        // Print newly completed tasks as permanent lines
        for task in &newly_done {
            let padded = format!("{:<width$}", task.id.as_str(), width = max_len);
            match task.state {
                TaskState::Succeeded => {
                    let dur = task
                        .completed_at
                        .and_then(|c| task.started_at.map(|s| c - s))
                        .map(|d| format!("{:.1}s", d.num_milliseconds() as f64 / 1000.0))
                        .unwrap_or_default();
                    println!(
                        "\x1b[32m✓\x1b[0m [{padded}]  \x1b[32msucceeded\x1b[0m  {DIM}{dur}{RESET}"
                    );
                    // Show last 3 lines of stdout if available
                    #[allow(clippy::collapsible_if)]
                    if let Some(ref output) = task.output {
                        if let Some(stdout) = output.get("stdout").and_then(|v| v.as_str()) {
                            let stdout = stdout.trim();
                            if !stdout.is_empty() {
                                let lines: Vec<&str> = stdout.lines().collect();
                                let start = lines.len().saturating_sub(3);
                                for line in &lines[start..] {
                                    println!("  {DIM}  {line}{RESET}");
                                }
                            }
                        } else if let Some(response) =
                            output.get("response").and_then(|v| v.as_str())
                        {
                            // Agent executor output
                            let response = response.trim();
                            if !response.is_empty() {
                                let lines: Vec<&str> = response.lines().collect();
                                let start = lines.len().saturating_sub(3);
                                for line in &lines[start..] {
                                    println!("  {DIM}  {line}{RESET}");
                                }
                            }
                        }
                    }
                }
                TaskState::Failed => {
                    let err = task.error.as_deref().unwrap_or("unknown error");
                    println!(
                        "\x1b[31m✗\x1b[0m [{padded}]  \x1b[31mfailed\x1b[0m     {DIM}{err}{RESET}"
                    );
                    // Show stderr if available
                    if let Some(ref output) = task.output
                        && let Some(stderr) = output.get("stderr").and_then(|v| v.as_str())
                    {
                        let stderr = stderr.trim();
                        if !stderr.is_empty() {
                            let lines: Vec<&str> = stderr.lines().collect();
                            let start = lines.len().saturating_sub(3);
                            for line in &lines[start..] {
                                println!("  {DIM}  {line}{RESET}");
                            }
                        }
                    }
                }
                TaskState::Cancelled => {
                    println!("{DIM}– [{padded}]  cancelled{RESET}");
                }
                _ => {}
            }
            displayed.insert(task.id.to_string(), "done");
        }

        // Print all currently running tasks (ephemeral, will be erased next cycle)
        let all_running: Vec<&Task> = tasks
            .iter()
            .filter(|t| {
                t.state == TaskState::Running
                    && *displayed.get(t.id.as_str()).unwrap_or(&"none") != "done"
            })
            .collect();

        let mut total_running_lines = 0;
        for task in &all_running {
            let padded = format!("{:<width$}", task.id.as_str(), width = max_len);
            println!("\x1b[33m▸\x1b[0m [{padded}]  \x1b[33mrunning...\x1b[0m");
            total_running_lines += 1;
            // Show live output preview (last 3 lines)
            if let Some(ref output) = task.output
                && let Some(stdout) = output.get("stdout").and_then(|v| v.as_str())
            {
                let lines: Vec<&str> = stdout.trim().lines().collect();
                let start = lines.len().saturating_sub(3);
                for line in &lines[start..] {
                    println!("  {DIM}  {line}{RESET}");
                    total_running_lines += 1;
                }
            }
            displayed.insert(task.id.to_string(), "running");
        }
        running_lines = total_running_lines;

        for task in &newly_running {
            if !all_running
                .iter()
                .any(|t| t.id.as_str() == task.id.as_str())
            {
                displayed.insert(task.id.to_string(), "running");
            }
        }
    }
}

// -- Tests --

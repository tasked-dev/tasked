//! API error type, engine/storage error mapping, and the error message
//! strings shared between the HTTP API and the MCP server.

use axum::{Json, http::StatusCode, response::IntoResponse};
use tasked::engine::EngineError;

use crate::dto::ErrorResponse;

// -- Shared message helpers --
//
// These produce the exact message strings used by both the HTTP API
// (`ApiError`) and the MCP server (`engine_error_message`), so the two
// surfaces cannot drift apart for the cases where they intentionally agree.

pub(crate) fn queue_not_found_msg(id: impl std::fmt::Display) -> String {
    format!("Queue '{id}' not found")
}

pub(crate) fn flow_not_found_msg(id: impl std::fmt::Display) -> String {
    format!("Flow '{id}' not found")
}

pub(crate) fn task_not_found_msg(
    tid: impl std::fmt::Display,
    fid: impl std::fmt::Display,
) -> String {
    format!("Task '{tid}' not found in flow '{fid}'")
}

pub(crate) fn schedule_not_found_msg(id: impl std::fmt::Display) -> String {
    format!("Schedule '{id}' not found")
}

fn trigger_depth_exceeded_msg(max: impl std::fmt::Display) -> String {
    format!("Trigger depth limit ({max}) exceeded")
}

fn task_limit_exceeded_msg(max: impl std::fmt::Display) -> String {
    format!("Flow task limit ({max}) exceeded")
}

fn flow_limit_exceeded_msg(queue: impl std::fmt::Display, max: impl std::fmt::Display) -> String {
    format!("Queue '{queue}' has reached its pending flow limit ({max})")
}

// -- Error handling --

pub(crate) enum ApiError {
    NotFound { error: String, message: String },
    BadRequest { error: String, message: String },
    Forbidden { error: String, message: String },
    Conflict { error: String, message: String },
    Internal { error: String, message: String },
    ServiceUnavailable { error: String, message: String },
    TooManyRequests { error: String, message: String },
}

impl ApiError {
    pub(crate) fn not_found(error: impl Into<String>, message: impl Into<String>) -> Self {
        Self::NotFound {
            error: error.into(),
            message: message.into(),
        }
    }

    pub(crate) fn bad_request(error: impl Into<String>, message: impl Into<String>) -> Self {
        Self::BadRequest {
            error: error.into(),
            message: message.into(),
        }
    }

    pub(crate) fn forbidden(error: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Forbidden {
            error: error.into(),
            message: message.into(),
        }
    }

    pub(crate) fn conflict(error: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Conflict {
            error: error.into(),
            message: message.into(),
        }
    }

    pub(crate) fn internal(error: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Internal {
            error: error.into(),
            message: message.into(),
        }
    }

    /// Internal error with the detail redacted from the client. The caller is
    /// responsible for logging the detail server-side before constructing this.
    pub(crate) fn internal_redacted(error: impl Into<String>) -> Self {
        Self::internal(error, "internal error")
    }

    pub(crate) fn service_unavailable(
        error: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::ServiceUnavailable {
            error: error.into(),
            message: message.into(),
        }
    }

    pub(crate) fn too_many_requests(error: impl Into<String>, message: impl Into<String>) -> Self {
        Self::TooManyRequests {
            error: error.into(),
            message: message.into(),
        }
    }
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
            ApiError::Forbidden { error, message } => {
                (StatusCode::FORBIDDEN, ErrorResponse { error, message })
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
            EngineError::QueueNotFound(id) => {
                ApiError::not_found("queue_not_found", queue_not_found_msg(id))
            }
            EngineError::NoExecutor(name) => ApiError::bad_request(
                "no_executor",
                format!("No executor registered for type '{name}'"),
            ),
            EngineError::Graph(e) => ApiError::bad_request("invalid_graph", e.to_string()),
            EngineError::InvalidCronExpression(msg) => {
                ApiError::bad_request("invalid_cron_expression", msg.clone())
            }
            EngineError::Spawn(msg) => ApiError::bad_request("spawn_error", msg.clone()),
            EngineError::TriggerDepthExceeded(max) => {
                ApiError::bad_request("trigger_depth_exceeded", trigger_depth_exceeded_msg(max))
            }
            EngineError::TaskLimitExceeded(max) => {
                ApiError::bad_request("task_limit_exceeded", task_limit_exceeded_msg(max))
            }
            EngineError::FlowLimitExceeded(queue, max) => ApiError::too_many_requests(
                "flow_limit_exceeded",
                flow_limit_exceeded_msg(queue, max),
            ),
            EngineError::InvalidQueueConfig(msg) => {
                ApiError::bad_request("invalid_queue_config", msg.clone())
            }
            EngineError::Storage(e) => map_storage_error(e),
            EngineError::Export(msg) => {
                tracing::error!(error = %msg, "flow export failed");
                ApiError::internal_redacted("export_failed")
            }
        }
    }
}

pub(crate) fn map_storage_error(err: &tasked::store::StorageError) -> ApiError {
    use tasked::store::StorageError;
    match err {
        StorageError::QueueAlreadyExists(id) => ApiError::conflict(
            "queue_already_exists",
            format!("Queue '{id}' already exists"),
        ),
        StorageError::QueueNotFound(id) => {
            ApiError::not_found("queue_not_found", queue_not_found_msg(id))
        }
        StorageError::FlowNotFound(id) => {
            ApiError::not_found("flow_not_found", flow_not_found_msg(id))
        }
        StorageError::TaskNotFound(tid, fid) => {
            ApiError::not_found("task_not_found", task_not_found_msg(tid, fid))
        }
        StorageError::InvalidStateTransition(from, to) => ApiError::conflict(
            "invalid_state_transition",
            format!("Invalid state transition: {from} -> {to}"),
        ),
        StorageError::ScheduleNotFound(id) => {
            ApiError::not_found("schedule_not_found", schedule_not_found_msg(id))
        }
        StorageError::Internal(msg) => {
            // Log the detail server-side; never echo internal errors to clients.
            tracing::error!(error = %msg, "internal storage error");
            ApiError::internal_redacted("internal_error")
        }
    }
}

/// Human-readable engine error messages for MCP tool responses.
///
/// Where the wording matches the HTTP API exactly, the shared helpers above
/// are used. Several variants intentionally use different wording from the
/// HTTP API (inherited behavior, preserved as-is):
/// - `NoExecutor` lists the executors available in MCP mode;
/// - `Graph`/`InvalidCronExpression`/`Spawn`/`InvalidQueueConfig` carry an
///   explanatory prefix;
/// - `Storage`/`Export` include the underlying detail rather than redacting
///   it, since MCP runs over local stdio for a trusted operator.
pub(crate) fn engine_error_message(err: EngineError) -> String {
    match err {
        EngineError::QueueNotFound(id) => queue_not_found_msg(id),
        EngineError::NoExecutor(name) => {
            format!(
                "No executor registered for type '{name}'. Available: shell, http, noop, delay, approval, spawn, trigger, callback, remote"
            )
        }
        EngineError::Graph(e) => format!("Invalid DAG: {e}"),
        EngineError::InvalidCronExpression(msg) => format!("Invalid cron expression: {msg}"),
        EngineError::Spawn(msg) => format!("Spawn error: {msg}"),
        EngineError::TriggerDepthExceeded(max) => trigger_depth_exceeded_msg(max),
        EngineError::TaskLimitExceeded(max) => task_limit_exceeded_msg(max),
        EngineError::FlowLimitExceeded(queue, max) => flow_limit_exceeded_msg(queue, max),
        EngineError::InvalidQueueConfig(msg) => format!("Invalid queue config: {msg}"),
        EngineError::Storage(e) => format!("Storage error: {e}"),
        EngineError::Export(msg) => format!("Export error: {msg}"),
    }
}

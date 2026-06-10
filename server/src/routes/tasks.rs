//! Task route handlers: external acknowledgement of callback/remote/approval tasks.

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use tasked::types::ExecuteResult;

use crate::auth::constant_time_eq;
use crate::dto::AckRequest;
use crate::error::ApiError;
use crate::routes::{AppState, find_task};

pub(crate) async fn ack_task(
    State(engine): State<AppState>,
    Path((fid, tid)): Path<(String, String)>,
    Json(req): Json<AckRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Get the task to pass to handle_task_result
    let task = find_task(&engine, &fid, &tid).await?;

    // If the task is awaiting approval and its output carries a verification
    // code, the ack must supply the matching code. Tasks without a code
    // (older approval outputs, callback/remote tasks) are accepted as before.
    if let Some(expected) = task
        .output
        .as_ref()
        .filter(|o| o.get("awaiting_approval").and_then(|v| v.as_bool()) == Some(true))
        .and_then(|o| o.get("code"))
        .and_then(|v| v.as_str())
    {
        let supplied = req.code.as_deref().unwrap_or("");
        if !constant_time_eq(supplied.as_bytes(), expected.as_bytes()) {
            return Err(ApiError::forbidden(
                "invalid_approval_code",
                "This task requires an approval code: pass the 'code' value from \
                 the task output in the ack request body",
            ));
        }
    }

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
            return Err(ApiError::bad_request(
                "invalid_status",
                format!("Invalid ack status: '{other}'. Must be 'success' or 'failed'"),
            ));
        }
    };

    engine.handle_task_result(&task, result).await?;
    Ok(StatusCode::NO_CONTENT)
}

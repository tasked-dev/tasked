//! Flow route handlers: submit, list, get, cancel, and export.

use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use tasked::{
    engine::EngineError,
    types::{FlowDef, FlowId, QueueId},
};

use crate::dto::{ExportParams, FlowDetailResponse, FlowResponse, TaskResponse};
use crate::error::{ApiError, flow_not_found_msg};
use crate::routes::{AppState, find_flow, find_flow_with_tasks, find_queue};

pub(crate) async fn submit_flow(
    State(engine): State<AppState>,
    Path(qid): Path<String>,
    Json(flow_def): Json<FlowDef>,
) -> Result<impl IntoResponse, ApiError> {
    let queue_id = QueueId::from(qid);
    let flow = engine.submit_flow(&queue_id, flow_def).await?;
    Ok((StatusCode::CREATED, Json(FlowResponse::from(flow))))
}

pub(crate) async fn list_flows(
    State(engine): State<AppState>,
    Path(qid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify queue exists
    find_queue(&engine, &qid).await?;

    let queue_id = QueueId::from(qid);
    let flows = engine.list_flows(&queue_id, None).await?;
    let response: Vec<FlowResponse> = flows.into_iter().map(FlowResponse::from).collect();
    Ok(Json(response))
}

pub(crate) async fn get_flow(
    State(engine): State<AppState>,
    Path(fid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let (flow, tasks) = find_flow_with_tasks(&engine, &fid).await?;

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

pub(crate) async fn cancel_flow(
    State(engine): State<AppState>,
    Path(fid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    // Verify flow exists
    find_flow(&engine, &fid).await?;

    let flow_id = FlowId::from(fid);
    engine.cancel_flow(&flow_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

// -- Export handler --

fn map_export_error(e: EngineError, not_found_msg: String) -> ApiError {
    match e {
        EngineError::Storage(tasked::store::StorageError::FlowNotFound(_)) => {
            ApiError::not_found("flow_not_found", not_found_msg)
        }
        _ => {
            tracing::error!(error = %e, "flow export failed");
            ApiError::internal_redacted("export_failed")
        }
    }
}

/// Sanitize a string for safe use as a filename inside an HTTP header.
/// Only `[A-Za-z0-9._-]` are kept; every other character becomes `_`.
/// This prevents header injection (e.g. CR/LF) via percent-decoded path
/// segments embedded in `content-disposition`.
fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

pub(crate) async fn export_flow_handler(
    State(engine): State<AppState>,
    Path(fid): Path<String>,
    Query(params): Query<ExportParams>,
) -> Result<axum::response::Response, ApiError> {
    let not_found_msg = flow_not_found_msg(&fid);
    let flow_id = FlowId::from(fid);

    let format = params.format.as_deref().unwrap_or("json");
    match format {
        "tar" => {
            let tar_bytes = engine
                .export_flow_tar(&flow_id)
                .await
                .map_err(|e| map_export_error(e, not_found_msg))?;
            let filename = sanitize_filename(flow_id.as_str());
            axum::response::Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/gzip")
                .header(
                    "content-disposition",
                    format!("attachment; filename=\"{filename}.tar.gz\""),
                )
                .body(Body::from(tar_bytes))
                .map_err(|e| {
                    tracing::error!(error = %e, "failed to build tar export response");
                    ApiError::internal_redacted("internal_error")
                })
        }
        "json" => {
            let export = engine
                .export_flow(&flow_id, params.with_artifacts)
                .await
                .map_err(|e| map_export_error(e, not_found_msg))?;
            Ok(Json(export).into_response())
        }
        other => Err(ApiError::bad_request(
            "invalid_format",
            format!("Unknown export format '{other}'. Supported formats: json, tar"),
        )),
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_filename_keeps_safe_chars() {
        assert_eq!(sanitize_filename("flow-123_v2.tar"), "flow-123_v2.tar");
        assert_eq!(
            sanitize_filename("AZaz09._-"),
            "AZaz09._-",
            "all allowed classes pass through"
        );
    }

    #[test]
    fn sanitize_filename_replaces_header_injection_chars() {
        // %0A / %0D decoded into the path must not survive into headers.
        assert_eq!(sanitize_filename("abc\r\ndef"), "abc__def");
        assert_eq!(sanitize_filename("a\"b"), "a_b");
        assert_eq!(sanitize_filename("a/b\\c d"), "a_b_c_d");
        assert_eq!(sanitize_filename("été"), "_t_");
    }

    #[test]
    fn sanitize_filename_empty() {
        assert_eq!(sanitize_filename(""), "");
    }
}

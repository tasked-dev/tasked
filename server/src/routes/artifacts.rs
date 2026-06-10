//! Artifact route handlers.

use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use std::sync::Arc;
use tasked::types::FlowId;

use crate::error::ApiError;
use crate::routes::{AppState, find_flow};

/// Get the configured artifact store or the canonical "not configured" error.
fn artifact_store(
    engine: &AppState,
) -> Result<Arc<dyn tasked::artifacts::ArtifactStore>, ApiError> {
    engine.artifact_store().ok_or_else(|| {
        ApiError::internal(
            "artifacts_not_configured",
            "Artifact storage not configured",
        )
    })
}

pub(crate) async fn upload_artifact(
    State(engine): State<AppState>,
    Path((fid, name)): Path<(String, String)>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    find_flow(&engine, &fid).await?;
    let flow_id = FlowId::from(fid);

    let artifacts = artifact_store(&engine)?;

    artifacts
        .upload(&flow_id, &name, &body)
        .await
        .map_err(|e| match &e {
            tasked::artifacts::ArtifactError::InvalidName(_) => {
                ApiError::bad_request("invalid_artifact_name", e.to_string())
            }
            _ => {
                tracing::error!(error = %e, "artifact upload failed");
                ApiError::internal_redacted("artifact_upload_failed")
            }
        })?;

    Ok(StatusCode::CREATED)
}

pub(crate) async fn download_artifact(
    State(engine): State<AppState>,
    Path((fid, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let flow_id = FlowId::from(fid);
    let artifacts = artifact_store(&engine)?;

    let data = artifacts
        .download(&flow_id, &name)
        .await
        .map_err(|e| match &e {
            tasked::artifacts::ArtifactError::Io(io_err)
                if io_err.kind() == std::io::ErrorKind::NotFound =>
            {
                ApiError::not_found("artifact_not_found", e.to_string())
            }
            tasked::artifacts::ArtifactError::InvalidName(_) => {
                ApiError::bad_request("invalid_artifact_name", e.to_string())
            }
            _ => {
                tracing::error!(error = %e, "artifact download failed");
                ApiError::internal_redacted("artifact_download_failed")
            }
        })?;

    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        data,
    ))
}

pub(crate) async fn list_artifacts(
    State(engine): State<AppState>,
    Path(fid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let flow_id = FlowId::from(fid);
    let artifacts = artifact_store(&engine)?;

    let names = artifacts.list(&flow_id).await.map_err(|e| {
        tracing::error!(error = %e, "artifact list failed");
        ApiError::internal_redacted("artifact_list_failed")
    })?;

    Ok(Json(names))
}

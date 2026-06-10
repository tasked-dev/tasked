//! Queue route handlers.

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use tasked::types::QueueId;

use crate::dto::{CreateQueueRequest, QueueResponse};
use crate::error::ApiError;
use crate::routes::{AppState, find_queue};

pub(crate) async fn create_queue(
    State(engine): State<AppState>,
    Json(req): Json<CreateQueueRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let queue_id = QueueId::from(req.id);
    let queue = engine.create_queue(&queue_id, req.config).await?;
    Ok((StatusCode::CREATED, Json(QueueResponse::from(queue))))
}

pub(crate) async fn list_queues(
    State(engine): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let queues = engine.list_queues().await?;
    let response: Vec<QueueResponse> = queues.into_iter().map(QueueResponse::from).collect();
    Ok(Json(response))
}

pub(crate) async fn get_queue(
    State(engine): State<AppState>,
    Path(qid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let queue = find_queue(&engine, &qid).await?;
    Ok(Json(QueueResponse::from(queue)))
}

pub(crate) async fn delete_queue(
    State(engine): State<AppState>,
    Path(qid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let queue_id = QueueId::from(qid);
    engine.delete_queue(&queue_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

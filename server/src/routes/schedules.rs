//! Schedule route handlers.

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use tasked::types::{QueueId, ScheduleDef, ScheduleId};

use crate::dto::{ScheduleRequest, ScheduleResponse};
use crate::error::ApiError;
use crate::routes::{AppState, find_schedule};

pub(crate) async fn create_schedule_handler(
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

pub(crate) async fn list_schedules_handler(
    State(engine): State<AppState>,
    Path(qid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let queue_id = QueueId::from(qid);
    let schedules = engine.list_schedules(&queue_id).await?;
    let response: Vec<ScheduleResponse> =
        schedules.into_iter().map(ScheduleResponse::from).collect();
    Ok(Json(response))
}

pub(crate) async fn get_schedule_handler(
    State(engine): State<AppState>,
    Path(sid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let schedule = find_schedule(&engine, &sid).await?;
    Ok(Json(ScheduleResponse::from(schedule)))
}

pub(crate) async fn update_schedule_handler(
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

pub(crate) async fn delete_schedule_handler(
    State(engine): State<AppState>,
    Path(sid): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let schedule_id = ScheduleId::from(sid);
    engine.delete_schedule(&schedule_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

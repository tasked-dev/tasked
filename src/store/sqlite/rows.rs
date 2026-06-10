use super::super::StorageError;
use crate::types::*;
use chrono::{DateTime, Utc};
use rusqlite::types::Type;

/// Build a conversion error for a corrupted column value.
///
/// Row mappers must surface corruption as an error instead of fabricating
/// fallback data (e.g. treating an unknown task state as Pending would
/// silently re-execute terminal tasks).
fn corrupt(idx: usize, msg: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(idx, Type::Text, msg.into())
}

fn parse_dt(idx: usize, field: &str, s: &str) -> Result<DateTime<Utc>, rusqlite::Error> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| corrupt(idx, format!("invalid {field} timestamp {s:?}: {e}")))
}

fn parse_opt_dt(
    idx: usize,
    field: &str,
    s: Option<String>,
) -> Result<Option<DateTime<Utc>>, rusqlite::Error> {
    s.map(|s| parse_dt(idx, field, &s)).transpose()
}

fn parse_json<T: serde::de::DeserializeOwned>(
    idx: usize,
    field: &str,
    s: &str,
) -> Result<T, rusqlite::Error> {
    serde_json::from_str(s).map_err(|e| corrupt(idx, format!("invalid {field} JSON: {e}")))
}

fn task_state_from_str(idx: usize, s: &str) -> Result<TaskState, rusqlite::Error> {
    match s {
        "pending" => Ok(TaskState::Pending),
        "ready" => Ok(TaskState::Ready),
        "running" => Ok(TaskState::Running),
        "succeeded" => Ok(TaskState::Succeeded),
        "failed" => Ok(TaskState::Failed),
        "delayed" => Ok(TaskState::Delayed),
        "cancelled" => Ok(TaskState::Cancelled),
        _ => Err(corrupt(idx, format!("unknown task state: {s}"))),
    }
}

pub(crate) fn parse_task_state(s: &str) -> Result<TaskState, StorageError> {
    task_state_from_str(0, s).map_err(|_| StorageError::Internal(format!("unknown task state: {s}")))
}

pub(crate) fn row_to_queue(row: &rusqlite::Row<'_>) -> Result<Queue, rusqlite::Error> {
    let id: String = row.get(0)?;
    let config_str: String = row.get(1)?;
    let created_at_str: String = row.get(2)?;
    let updated_at_str: String = row.get(3)?;

    let config: QueueConfig = parse_json(1, "queue config", &config_str)?;
    let created_at = parse_dt(2, "queue created_at", &created_at_str)?;
    let updated_at = parse_dt(3, "queue updated_at", &updated_at_str)?;

    Ok(Queue {
        id: QueueId::from(id),
        config,
        created_at,
        updated_at,
    })
}

pub(crate) fn row_to_flow(row: &rusqlite::Row<'_>) -> Result<Flow, rusqlite::Error> {
    let id: String = row.get(0)?;
    let queue_id: String = row.get(1)?;
    let state_str: String = row.get(2)?;
    let task_count: i64 = row.get(3)?;
    let tasks_succeeded: i64 = row.get(4)?;
    let tasks_failed: i64 = row.get(5)?;
    let webhooks_str: Option<String> = row.get(6)?;
    let trigger_depth: i64 = row.get(7)?;
    let flow_def_str: Option<String> = row.get(8)?;
    let fail_fast: i64 = row.get(9)?;
    let parent_flow_id_str: Option<String> = row.get(10)?;
    let created_at_str: String = row.get(11)?;
    let updated_at_str: String = row.get(12)?;

    let state = match state_str.as_str() {
        "running" => FlowState::Running,
        "succeeded" => FlowState::Succeeded,
        "failed" => FlowState::Failed,
        "cancelled" => FlowState::Cancelled,
        _ => return Err(corrupt(2, format!("unknown flow state: {state_str}"))),
    };
    // webhooks/flow_def are optional, forward-compatible structures: tolerate
    // deserialization failures (with a warning) rather than failing the row.
    let webhooks: Option<FlowWebhooks> = webhooks_str.and_then(|s| {
        serde_json::from_str(&s)
            .map_err(|e| tracing::warn!(flow_id = %id, "failed to deserialize webhooks: {e}"))
            .ok()
    });
    let flow_def: Option<FlowDef> = flow_def_str.and_then(|s| {
        serde_json::from_str(&s)
            .map_err(|e| tracing::warn!(flow_id = %id, "failed to deserialize flow_def: {e}"))
            .ok()
    });
    let created_at = parse_dt(11, "flow created_at", &created_at_str)?;
    let updated_at = parse_dt(12, "flow updated_at", &updated_at_str)?;

    Ok(Flow {
        id: FlowId::from(id),
        queue_id: QueueId::from(queue_id),
        state,
        task_count: task_count as usize,
        tasks_succeeded: tasks_succeeded as usize,
        tasks_failed: tasks_failed as usize,
        webhooks,
        trigger_depth: trigger_depth as u32,
        flow_def,
        fail_fast: fail_fast != 0,
        parent_flow_id: parent_flow_id_str.map(FlowId::from),
        created_at,
        updated_at,
    })
}

pub(crate) fn row_to_task(row: &rusqlite::Row<'_>) -> Result<Task, rusqlite::Error> {
    let id: String = row.get(0)?;
    let flow_id: String = row.get(1)?;
    let queue_id: String = row.get(2)?;
    let state_str: String = row.get(3)?;
    let executor_type: String = row.get(4)?;
    let executor_config_str: String = row.get(5)?;
    let input_str: Option<String> = row.get(6)?;
    let output_str: Option<String> = row.get(7)?;
    let error: Option<String> = row.get(8)?;
    let retries_remaining: i64 = row.get(9)?;
    let backoff_str: String = row.get(10)?;
    let timeout_secs: i64 = row.get(11)?;
    let condition: Option<String> = row.get(12)?;
    let retry_at_str: Option<String> = row.get(13)?;
    let started_at_str: Option<String> = row.get(14)?;
    let completed_at_str: Option<String> = row.get(15)?;
    let created_at_str: String = row.get(16)?;

    let state = task_state_from_str(3, &state_str)?;

    let executor_config: serde_json::Value =
        parse_json(5, "task executor_config", &executor_config_str)?;
    let input: Option<serde_json::Value> = input_str
        .as_deref()
        .map(|s| parse_json(6, "task input", s))
        .transpose()?;
    let output: Option<serde_json::Value> = output_str
        .as_deref()
        .map(|s| parse_json(7, "task output", s))
        .transpose()?;
    let backoff: BackoffStrategy = parse_json(10, "task backoff", &backoff_str)?;

    let retry_at = parse_opt_dt(13, "task retry_at", retry_at_str)?;
    let started_at = parse_opt_dt(14, "task started_at", started_at_str)?;
    let completed_at = parse_opt_dt(15, "task completed_at", completed_at_str)?;
    let created_at = parse_dt(16, "task created_at", &created_at_str)?;

    Ok(Task {
        id: TaskId::from(id),
        flow_id: FlowId::from(flow_id),
        queue_id: QueueId::from(queue_id),
        state,
        executor_type,
        executor_config,
        input,
        output,
        error,
        retries_remaining: retries_remaining as u32,
        backoff,
        timeout_secs: timeout_secs as u64,
        condition,
        retry_at,
        started_at,
        completed_at,
        created_at,
    })
}

pub(crate) fn row_to_schedule(row: &rusqlite::Row<'_>) -> Result<Schedule, rusqlite::Error> {
    let id: String = row.get(0)?;
    let queue_id: String = row.get(1)?;
    let name: Option<String> = row.get(2)?;
    let cron: String = row.get(3)?;
    let flow_def_str: String = row.get(4)?;
    let enabled: bool = row.get(5)?;
    let last_triggered_at_str: Option<String> = row.get(6)?;
    let next_run_at_str: Option<String> = row.get(7)?;
    let created_at_str: String = row.get(8)?;
    let updated_at_str: String = row.get(9)?;

    let flow_def: FlowDef = parse_json(4, "schedule flow_def", &flow_def_str)?;

    let last_triggered_at = parse_opt_dt(6, "schedule last_triggered_at", last_triggered_at_str)?;
    let next_run_at = parse_opt_dt(7, "schedule next_run_at", next_run_at_str)?;
    let created_at = parse_dt(8, "schedule created_at", &created_at_str)?;
    let updated_at = parse_dt(9, "schedule updated_at", &updated_at_str)?;

    Ok(Schedule {
        id: ScheduleId::from(id),
        queue_id: QueueId::from(queue_id),
        name,
        cron,
        flow_def,
        enabled,
        last_triggered_at,
        next_run_at,
        created_at,
        updated_at,
    })
}

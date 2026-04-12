use super::super::StorageError;
use crate::types::*;
use chrono::{DateTime, Utc};

pub(crate) fn parse_task_state(s: &str) -> Result<TaskState, StorageError> {
    match s {
        "pending" => Ok(TaskState::Pending),
        "ready" => Ok(TaskState::Ready),
        "running" => Ok(TaskState::Running),
        "succeeded" => Ok(TaskState::Succeeded),
        "failed" => Ok(TaskState::Failed),
        "delayed" => Ok(TaskState::Delayed),
        "cancelled" => Ok(TaskState::Cancelled),
        _ => Err(StorageError::Internal(format!("unknown task state: {s}"))),
    }
}

pub(crate) fn row_to_queue(row: &rusqlite::Row<'_>) -> Result<Queue, rusqlite::Error> {
    let id: String = row.get(0)?;
    let config_str: String = row.get(1)?;
    let created_at_str: String = row.get(2)?;
    let updated_at_str: String = row.get(3)?;

    let config: QueueConfig = serde_json::from_str(&config_str).unwrap_or_default();
    let created_at = DateTime::parse_from_rfc3339(&created_at_str)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());

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
        _ => FlowState::Running,
    };
    let webhooks: Option<FlowWebhooks> = webhooks_str.and_then(|s| serde_json::from_str(&s).ok());
    let flow_def: Option<FlowDef> = flow_def_str.and_then(|s| {
        serde_json::from_str(&s)
            .map_err(|e| tracing::warn!(flow_id = %id, "failed to deserialize flow_def: {e}"))
            .ok()
    });
    let created_at = DateTime::parse_from_rfc3339(&created_at_str)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());

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

    let state = match state_str.as_str() {
        "pending" => TaskState::Pending,
        "ready" => TaskState::Ready,
        "running" => TaskState::Running,
        "succeeded" => TaskState::Succeeded,
        "failed" => TaskState::Failed,
        "delayed" => TaskState::Delayed,
        "cancelled" => TaskState::Cancelled,
        _ => TaskState::Pending,
    };

    let executor_config: serde_json::Value =
        serde_json::from_str(&executor_config_str).unwrap_or(serde_json::Value::Null);
    let input: Option<serde_json::Value> = input_str.and_then(|s| serde_json::from_str(&s).ok());
    let output: Option<serde_json::Value> = output_str.and_then(|s| serde_json::from_str(&s).ok());
    let backoff: BackoffStrategy = serde_json::from_str(&backoff_str).unwrap_or_default();

    let parse_opt_dt = |s: Option<String>| -> Option<DateTime<Utc>> {
        s.and_then(|s| {
            DateTime::parse_from_rfc3339(&s)
                .map(|dt| dt.with_timezone(&Utc))
                .ok()
        })
    };

    let retry_at = parse_opt_dt(retry_at_str);
    let started_at = parse_opt_dt(started_at_str);
    let completed_at = parse_opt_dt(completed_at_str);
    let created_at = DateTime::parse_from_rfc3339(&created_at_str)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());

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

    let flow_def: FlowDef = serde_json::from_str(&flow_def_str).unwrap_or_else(|_| FlowDef {
        tasks: vec![],
        ..FlowDef::default()
    });

    let parse_opt_dt = |s: Option<String>| -> Option<DateTime<Utc>> {
        s.and_then(|s| {
            DateTime::parse_from_rfc3339(&s)
                .map(|dt| dt.with_timezone(&Utc))
                .ok()
        })
    };

    let last_triggered_at = parse_opt_dt(last_triggered_at_str);
    let next_run_at = parse_opt_dt(next_run_at_str);
    let created_at = DateTime::parse_from_rfc3339(&created_at_str)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());

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


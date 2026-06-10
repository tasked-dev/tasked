//! Request/response DTO types for the HTTP API, plus shared serde defaults.

use serde::{Deserialize, Serialize};
use tasked::types::*;

/// Shared serde default: schedules are enabled unless stated otherwise.
/// Used by both the HTTP API ([`ScheduleRequest`]) and the MCP server.
pub(crate) fn default_enabled() -> bool {
    true
}

#[derive(Deserialize)]
pub(crate) struct CreateQueueRequest {
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) config: QueueConfig,
}

#[derive(Serialize)]
pub(crate) struct QueueResponse {
    pub(crate) id: String,
    pub(crate) config: QueueConfig,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
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
pub(crate) struct FlowResponse {
    pub(crate) id: String,
    pub(crate) queue_id: String,
    pub(crate) state: String,
    pub(crate) task_count: usize,
    pub(crate) tasks_succeeded: usize,
    pub(crate) tasks_failed: usize,
    pub(crate) fail_fast: bool,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
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
pub(crate) struct TaskResponse {
    pub(crate) id: String,
    pub(crate) flow_id: String,
    pub(crate) queue_id: String,
    pub(crate) state: String,
    pub(crate) executor_type: String,
    pub(crate) input: Option<serde_json::Value>,
    pub(crate) output: Option<serde_json::Value>,
    pub(crate) error: Option<String>,
    pub(crate) retries_remaining: u32,
    pub(crate) timeout_secs: u64,
    pub(crate) started_at: Option<String>,
    pub(crate) completed_at: Option<String>,
    pub(crate) created_at: String,
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
pub(crate) struct FlowDetailResponse {
    pub(crate) id: String,
    pub(crate) queue_id: String,
    pub(crate) state: String,
    pub(crate) task_count: usize,
    pub(crate) tasks_succeeded: usize,
    pub(crate) tasks_failed: usize,
    pub(crate) tasks: Vec<TaskResponse>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Deserialize)]
pub(crate) struct AckRequest {
    pub(crate) status: String,
    #[serde(default)]
    pub(crate) output: Option<serde_json::Value>,
    #[serde(default)]
    pub(crate) error: Option<String>,
    #[serde(default)]
    pub(crate) retryable: Option<bool>,
    #[serde(default)]
    pub(crate) approved_by: Option<String>,
    /// Approval verification code. Required when acking an approval task
    /// whose output contains a `code` field.
    #[serde(default)]
    pub(crate) code: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct ScheduleRequest {
    pub(crate) cron: String,
    pub(crate) flow: FlowDef,
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default = "default_enabled")]
    pub(crate) enabled: bool,
}

#[derive(Serialize)]
pub(crate) struct ScheduleResponse {
    pub(crate) id: String,
    pub(crate) queue_id: String,
    pub(crate) name: Option<String>,
    pub(crate) cron: String,
    pub(crate) enabled: bool,
    pub(crate) last_triggered_at: Option<String>,
    pub(crate) next_run_at: Option<String>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
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
pub(crate) struct ErrorResponse {
    pub(crate) error: String,
    pub(crate) message: String,
}

#[derive(Deserialize)]
pub(crate) struct ExportParams {
    #[serde(default)]
    pub(crate) with_artifacts: bool,
    /// Export format: "json" (default) or "tar" (tar.gz archive with artifacts).
    #[serde(default)]
    pub(crate) format: Option<String>,
}

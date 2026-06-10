//! Task ack operations.

use crate::error::{TaskedError, parse_opt_timestamp, parse_timestamp};
use crate::{TaskedClient, encode_path};
use serde::{Deserialize, Serialize};
use tasked::types::*;

/// Response body for task endpoints.
#[derive(Deserialize)]
pub(crate) struct TaskResponse {
    pub id: String,
    pub flow_id: String,
    pub queue_id: String,
    pub state: String,
    pub executor_type: String,
    pub input: Option<serde_json::Value>,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    pub retries_remaining: u32,
    pub timeout_secs: u64,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub created_at: String,
}

impl TaskResponse {
    pub(crate) fn into_task(self) -> Result<Task, TaskedError> {
        Ok(Task {
            id: TaskId::from(self.id),
            flow_id: FlowId::from(self.flow_id),
            queue_id: QueueId::from(self.queue_id),
            state: parse_task_state(&self.state)?,
            executor_type: self.executor_type,
            executor_config: serde_json::Value::Null,
            input: self.input,
            output: self.output,
            error: self.error,
            retries_remaining: self.retries_remaining,
            backoff: BackoffStrategy::default(),
            timeout_secs: self.timeout_secs,
            condition: None,
            retry_at: None,
            started_at: parse_opt_timestamp(self.started_at, "started_at")?,
            completed_at: parse_opt_timestamp(self.completed_at, "completed_at")?,
            created_at: parse_timestamp(&self.created_at, "created_at")?,
        })
    }
}

fn parse_task_state(s: &str) -> Result<TaskState, TaskedError> {
    match s {
        "pending" => Ok(TaskState::Pending),
        "ready" => Ok(TaskState::Ready),
        "running" => Ok(TaskState::Running),
        "succeeded" => Ok(TaskState::Succeeded),
        "failed" => Ok(TaskState::Failed),
        "delayed" => Ok(TaskState::Delayed),
        "cancelled" => Ok(TaskState::Cancelled),
        other => Err(TaskedError::InvalidResponse(format!(
            "unknown task state: {other:?}"
        ))),
    }
}

/// Request body for acknowledging a task.
#[derive(Serialize)]
struct AckRequest {
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    output: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retryable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    approved_by: Option<String>,
}

/// Ack payload for marking a task as succeeded or failed.
pub enum TaskAck {
    /// Mark the task as succeeded with optional output.
    Success {
        output: Option<serde_json::Value>,
        approved_by: Option<String>,
    },
    /// Mark the task as failed.
    Failed { error: String, retryable: bool },
}

impl TaskedClient {
    /// Acknowledge a task result (success or failure).
    pub async fn ack_task(
        &self,
        flow_id: &str,
        task_id: &str,
        ack: TaskAck,
    ) -> Result<(), TaskedError> {
        let url = format!(
            "{}/api/v1/flows/{}/tasks/{}/ack",
            self.base_url,
            encode_path(flow_id),
            encode_path(task_id)
        );

        let req = match ack {
            TaskAck::Success {
                output,
                approved_by,
            } => AckRequest {
                status: "success".to_string(),
                output,
                error: None,
                retryable: None,
                approved_by,
            },
            TaskAck::Failed { error, retryable } => AckRequest {
                status: "failed".to_string(),
                output: None,
                error: Some(error),
                retryable: Some(retryable),
                approved_by: None,
            },
        };

        self.request_empty(self.client.post(&url).json(&req)).await
    }
}

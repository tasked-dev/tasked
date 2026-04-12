//! Task ack operations.

use crate::TaskedClient;
use crate::error::TaskedError;
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
    pub(crate) fn into_task(self) -> Task {
        Task {
            id: TaskId::from(self.id),
            flow_id: FlowId::from(self.flow_id),
            queue_id: QueueId::from(self.queue_id),
            state: parse_task_state(&self.state),
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
            started_at: self.started_at.and_then(|s| s.parse().ok()),
            completed_at: self.completed_at.and_then(|s| s.parse().ok()),
            created_at: self
                .created_at
                .parse()
                .unwrap_or_else(|_| chrono::Utc::now()),
        }
    }
}

fn parse_task_state(s: &str) -> TaskState {
    match s {
        "pending" => TaskState::Pending,
        "ready" => TaskState::Ready,
        "running" => TaskState::Running,
        "succeeded" => TaskState::Succeeded,
        "failed" => TaskState::Failed,
        "delayed" => TaskState::Delayed,
        "cancelled" => TaskState::Cancelled,
        _ => TaskState::Pending,
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
            "{}/api/v1/flows/{flow_id}/tasks/{task_id}/ack",
            self.base_url
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

        let resp = self.client.post(&url).json(&req).send().await?;

        if !resp.status().is_success() {
            return Err(self.parse_error(resp).await);
        }

        Ok(())
    }
}

//! Flow submit, get, list, and cancel operations.

use crate::error::{TaskedError, parse_timestamp};
use crate::tasks::TaskResponse;
use crate::{TaskedClient, encode_path};
use serde::Deserialize;
use tasked::types::{Flow, FlowDef, FlowId, FlowState, QueueId, Task};

/// Response body for flow list endpoints.
#[derive(Deserialize)]
pub(crate) struct FlowResponse {
    pub id: String,
    pub queue_id: String,
    pub state: String,
    pub task_count: usize,
    pub tasks_succeeded: usize,
    pub tasks_failed: usize,
    #[serde(default)]
    pub fail_fast: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl FlowResponse {
    pub(crate) fn into_flow(self) -> Result<Flow, TaskedError> {
        Ok(Flow {
            id: FlowId::from(self.id),
            queue_id: QueueId::from(self.queue_id),
            state: parse_flow_state(&self.state)?,
            task_count: self.task_count,
            tasks_succeeded: self.tasks_succeeded,
            tasks_failed: self.tasks_failed,
            webhooks: None,
            trigger_depth: 0,
            flow_def: None,
            fail_fast: self.fail_fast,
            parent_flow_id: None,
            created_at: parse_timestamp(&self.created_at, "created_at")?,
            updated_at: parse_timestamp(&self.updated_at, "updated_at")?,
        })
    }
}

/// Detailed flow response including tasks.
#[derive(Deserialize)]
struct FlowDetailResponse {
    id: String,
    queue_id: String,
    state: String,
    task_count: usize,
    tasks_succeeded: usize,
    tasks_failed: usize,
    #[serde(default)]
    fail_fast: bool,
    tasks: Vec<TaskResponse>,
    created_at: String,
    updated_at: String,
}

/// A flow with its tasks.
#[derive(Debug)]
pub struct FlowDetail {
    /// The flow itself.
    pub flow: Flow,
    /// All tasks in this flow.
    pub tasks: Vec<Task>,
}

fn parse_flow_state(s: &str) -> Result<FlowState, TaskedError> {
    match s {
        "running" => Ok(FlowState::Running),
        "succeeded" => Ok(FlowState::Succeeded),
        "failed" => Ok(FlowState::Failed),
        "cancelled" => Ok(FlowState::Cancelled),
        other => Err(TaskedError::InvalidResponse(format!(
            "unknown flow state: {other:?}"
        ))),
    }
}

impl TaskedClient {
    /// Submit a new flow to a queue.
    pub async fn submit_flow(
        &self,
        queue_id: &str,
        flow_def: FlowDef,
    ) -> Result<Flow, TaskedError> {
        let url = format!(
            "{}/api/v1/queues/{}/flows",
            self.base_url,
            encode_path(queue_id)
        );
        let body: FlowResponse = self
            .request_json(self.client.post(&url).json(&flow_def))
            .await?;
        body.into_flow()
    }

    /// List all flows in a queue.
    pub async fn list_flows(&self, queue_id: &str) -> Result<Vec<Flow>, TaskedError> {
        let url = format!(
            "{}/api/v1/queues/{}/flows",
            self.base_url,
            encode_path(queue_id)
        );
        let body: Vec<FlowResponse> = self.request_json(self.client.get(&url)).await?;
        body.into_iter().map(|f| f.into_flow()).collect()
    }

    /// Get a flow by ID, including all its tasks.
    pub async fn get_flow(&self, flow_id: &str) -> Result<FlowDetail, TaskedError> {
        let url = format!("{}/api/v1/flows/{}", self.base_url, encode_path(flow_id));
        let body: FlowDetailResponse = self.request_json(self.client.get(&url)).await?;
        let flow = Flow {
            id: FlowId::from(body.id),
            queue_id: QueueId::from(body.queue_id),
            state: parse_flow_state(&body.state)?,
            task_count: body.task_count,
            tasks_succeeded: body.tasks_succeeded,
            tasks_failed: body.tasks_failed,
            webhooks: None,
            trigger_depth: 0,
            flow_def: None,
            fail_fast: body.fail_fast,
            parent_flow_id: None,
            created_at: parse_timestamp(&body.created_at, "created_at")?,
            updated_at: parse_timestamp(&body.updated_at, "updated_at")?,
        };
        let tasks = body
            .tasks
            .into_iter()
            .map(|t| t.into_task())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FlowDetail { flow, tasks })
    }

    /// Cancel a flow.
    pub async fn cancel_flow(&self, flow_id: &str) -> Result<(), TaskedError> {
        let url = format!("{}/api/v1/flows/{}", self.base_url, encode_path(flow_id));
        self.request_empty(self.client.delete(&url)).await
    }
}

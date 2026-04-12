//! Flow submit, get, list, and cancel operations.

use crate::TaskedClient;
use crate::error::TaskedError;
use crate::tasks::TaskResponse;
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
    pub(crate) fn into_flow(self) -> Flow {
        Flow {
            id: FlowId::from(self.id),
            queue_id: QueueId::from(self.queue_id),
            state: parse_flow_state(&self.state),
            task_count: self.task_count,
            tasks_succeeded: self.tasks_succeeded,
            tasks_failed: self.tasks_failed,
            webhooks: None,
            trigger_depth: 0,
            flow_def: None,
            fail_fast: self.fail_fast,
            parent_flow_id: None,
            created_at: self
                .created_at
                .parse()
                .unwrap_or_else(|_| chrono::Utc::now()),
            updated_at: self
                .updated_at
                .parse()
                .unwrap_or_else(|_| chrono::Utc::now()),
        }
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

fn parse_flow_state(s: &str) -> FlowState {
    match s {
        "running" => FlowState::Running,
        "succeeded" => FlowState::Succeeded,
        "failed" => FlowState::Failed,
        "cancelled" => FlowState::Cancelled,
        _ => FlowState::Running,
    }
}

impl TaskedClient {
    /// Submit a new flow to a queue.
    pub async fn submit_flow(
        &self,
        queue_id: &str,
        flow_def: FlowDef,
    ) -> Result<Flow, TaskedError> {
        let url = format!("{}/api/v1/queues/{queue_id}/flows", self.base_url);
        let resp = self.client.post(&url).json(&flow_def).send().await?;

        if !resp.status().is_success() {
            return Err(self.parse_error(resp).await);
        }

        let body: FlowResponse = resp.json().await?;
        Ok(body.into_flow())
    }

    /// List all flows in a queue.
    pub async fn list_flows(&self, queue_id: &str) -> Result<Vec<Flow>, TaskedError> {
        let url = format!("{}/api/v1/queues/{queue_id}/flows", self.base_url);
        let resp = self.client.get(&url).send().await?;

        if !resp.status().is_success() {
            return Err(self.parse_error(resp).await);
        }

        let body: Vec<FlowResponse> = resp.json().await?;
        Ok(body.into_iter().map(|f| f.into_flow()).collect())
    }

    /// Get a flow by ID, including all its tasks.
    pub async fn get_flow(&self, flow_id: &str) -> Result<FlowDetail, TaskedError> {
        let url = format!("{}/api/v1/flows/{flow_id}", self.base_url);
        let resp = self.client.get(&url).send().await?;

        if !resp.status().is_success() {
            return Err(self.parse_error(resp).await);
        }

        let body: FlowDetailResponse = resp.json().await?;
        let flow = Flow {
            id: FlowId::from(body.id),
            queue_id: QueueId::from(body.queue_id),
            state: parse_flow_state(&body.state),
            task_count: body.task_count,
            tasks_succeeded: body.tasks_succeeded,
            tasks_failed: body.tasks_failed,
            webhooks: None,
            trigger_depth: 0,
            flow_def: None,
            fail_fast: body.fail_fast,
            parent_flow_id: None,
            created_at: body
                .created_at
                .parse()
                .unwrap_or_else(|_| chrono::Utc::now()),
            updated_at: body
                .updated_at
                .parse()
                .unwrap_or_else(|_| chrono::Utc::now()),
        };
        let tasks = body.tasks.into_iter().map(|t| t.into_task()).collect();
        Ok(FlowDetail { flow, tasks })
    }

    /// Cancel a flow.
    pub async fn cancel_flow(&self, flow_id: &str) -> Result<(), TaskedError> {
        let url = format!("{}/api/v1/flows/{flow_id}", self.base_url);
        let resp = self.client.delete(&url).send().await?;

        if !resp.status().is_success() {
            return Err(self.parse_error(resp).await);
        }

        Ok(())
    }
}

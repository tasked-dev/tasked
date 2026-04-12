//! Schedule CRUD operations.

use crate::TaskedClient;
use crate::error::TaskedError;
use serde::{Deserialize, Serialize};
use tasked::types::{FlowDef, QueueId, Schedule, ScheduleDef, ScheduleId};

/// Request body for creating/updating a schedule.
#[derive(Serialize)]
struct ScheduleRequest {
    cron: String,
    flow: FlowDef,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    enabled: bool,
}

/// Response body for schedule endpoints.
#[derive(Deserialize)]
struct ScheduleResponse {
    id: String,
    queue_id: String,
    name: Option<String>,
    cron: String,
    enabled: bool,
    last_triggered_at: Option<String>,
    next_run_at: Option<String>,
    created_at: String,
    updated_at: String,
}

impl ScheduleResponse {
    fn into_schedule(self, flow_def: FlowDef) -> Schedule {
        Schedule {
            id: ScheduleId::from(self.id),
            queue_id: QueueId::from(self.queue_id),
            name: self.name,
            cron: self.cron.clone(),
            flow_def,
            enabled: self.enabled,
            last_triggered_at: self.last_triggered_at.and_then(|s| s.parse().ok()),
            next_run_at: self.next_run_at.and_then(|s| s.parse().ok()),
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

    /// Convert to a Schedule with a placeholder flow_def (for list/get where
    /// the server response doesn't include the full flow definition).
    fn into_schedule_no_flow(self) -> Schedule {
        let placeholder = FlowDef {
            tasks: vec![],
            ..FlowDef::default()
        };
        self.into_schedule(placeholder)
    }
}

impl TaskedClient {
    /// Create a new schedule in a queue.
    pub async fn create_schedule(
        &self,
        queue_id: &str,
        schedule_def: ScheduleDef,
    ) -> Result<Schedule, TaskedError> {
        let url = format!("{}/api/v1/queues/{queue_id}/schedules", self.base_url);
        let req = ScheduleRequest {
            cron: schedule_def.cron,
            flow: schedule_def.flow.clone(),
            name: schedule_def.name,
            enabled: schedule_def.enabled,
        };
        let resp = self.client.post(&url).json(&req).send().await?;

        if !resp.status().is_success() {
            return Err(self.parse_error(resp).await);
        }

        let body: ScheduleResponse = resp.json().await?;
        Ok(body.into_schedule(schedule_def.flow))
    }

    /// List all schedules in a queue.
    pub async fn list_schedules(&self, queue_id: &str) -> Result<Vec<Schedule>, TaskedError> {
        let url = format!("{}/api/v1/queues/{queue_id}/schedules", self.base_url);
        let resp = self.client.get(&url).send().await?;

        if !resp.status().is_success() {
            return Err(self.parse_error(resp).await);
        }

        let body: Vec<ScheduleResponse> = resp.json().await?;
        Ok(body
            .into_iter()
            .map(|s| s.into_schedule_no_flow())
            .collect())
    }

    /// Get a schedule by ID.
    pub async fn get_schedule(&self, schedule_id: &str) -> Result<Schedule, TaskedError> {
        let url = format!("{}/api/v1/schedules/{schedule_id}", self.base_url);
        let resp = self.client.get(&url).send().await?;

        if !resp.status().is_success() {
            return Err(self.parse_error(resp).await);
        }

        let body: ScheduleResponse = resp.json().await?;
        Ok(body.into_schedule_no_flow())
    }

    /// Update a schedule.
    pub async fn update_schedule(
        &self,
        schedule_id: &str,
        schedule_def: ScheduleDef,
    ) -> Result<Schedule, TaskedError> {
        let url = format!("{}/api/v1/schedules/{schedule_id}", self.base_url);
        let req = ScheduleRequest {
            cron: schedule_def.cron,
            flow: schedule_def.flow.clone(),
            name: schedule_def.name,
            enabled: schedule_def.enabled,
        };
        let resp = self.client.put(&url).json(&req).send().await?;

        if !resp.status().is_success() {
            return Err(self.parse_error(resp).await);
        }

        let body: ScheduleResponse = resp.json().await?;
        Ok(body.into_schedule(schedule_def.flow))
    }

    /// Delete a schedule.
    pub async fn delete_schedule(&self, schedule_id: &str) -> Result<(), TaskedError> {
        let url = format!("{}/api/v1/schedules/{schedule_id}", self.base_url);
        let resp = self.client.delete(&url).send().await?;

        if !resp.status().is_success() {
            return Err(self.parse_error(resp).await);
        }

        Ok(())
    }
}

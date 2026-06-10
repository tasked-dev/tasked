//! Schedule CRUD operations.

use crate::error::{TaskedError, parse_opt_timestamp, parse_timestamp};
use crate::{TaskedClient, encode_path};
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
    /// The flow definition, if the server includes it in the response.
    /// Current server versions omit it from all schedule responses.
    #[serde(default)]
    flow_def: Option<FlowDef>,
    last_triggered_at: Option<String>,
    next_run_at: Option<String>,
    created_at: String,
    updated_at: String,
}

impl ScheduleResponse {
    /// Convert to a [`Schedule`], preferring the `flow_def` from the response
    /// body and falling back to `fallback` (e.g. the flow definition that was
    /// submitted in a create/update request) when the server omits it.
    fn into_schedule(mut self, fallback: Option<FlowDef>) -> Result<Schedule, TaskedError> {
        let flow_def = self.flow_def.take().or(fallback).unwrap_or_default();
        Ok(Schedule {
            id: ScheduleId::from(self.id),
            queue_id: QueueId::from(self.queue_id),
            name: self.name,
            cron: self.cron,
            flow_def,
            enabled: self.enabled,
            last_triggered_at: parse_opt_timestamp(self.last_triggered_at, "last_triggered_at")?,
            next_run_at: parse_opt_timestamp(self.next_run_at, "next_run_at")?,
            created_at: parse_timestamp(&self.created_at, "created_at")?,
            updated_at: parse_timestamp(&self.updated_at, "updated_at")?,
        })
    }
}

impl TaskedClient {
    /// Create a new schedule in a queue.
    ///
    /// The returned [`Schedule::flow_def`] is the definition you submitted
    /// (the server's response does not echo it back).
    pub async fn create_schedule(
        &self,
        queue_id: &str,
        schedule_def: ScheduleDef,
    ) -> Result<Schedule, TaskedError> {
        let url = format!(
            "{}/api/v1/queues/{}/schedules",
            self.base_url,
            encode_path(queue_id)
        );
        let req = ScheduleRequest {
            cron: schedule_def.cron,
            flow: schedule_def.flow.clone(),
            name: schedule_def.name,
            enabled: schedule_def.enabled,
        };
        let body: ScheduleResponse = self.request_json(self.client.post(&url).json(&req)).await?;
        body.into_schedule(Some(schedule_def.flow))
    }

    /// List all schedules in a queue.
    ///
    /// Note: the server's schedule list response does not include the flow
    /// definition. If a response omits the `flow_def` field, the returned
    /// [`Schedule::flow_def`] is an empty [`FlowDef::default()`]; it does not
    /// reflect the schedule's actual flow definition.
    pub async fn list_schedules(&self, queue_id: &str) -> Result<Vec<Schedule>, TaskedError> {
        let url = format!(
            "{}/api/v1/queues/{}/schedules",
            self.base_url,
            encode_path(queue_id)
        );
        let body: Vec<ScheduleResponse> = self.request_json(self.client.get(&url)).await?;
        body.into_iter().map(|s| s.into_schedule(None)).collect()
    }

    /// Get a schedule by ID.
    ///
    /// Note: the server's schedule response does not include the flow
    /// definition. If the response omits the `flow_def` field, the returned
    /// [`Schedule::flow_def`] is an empty [`FlowDef::default()`]; it does not
    /// reflect the schedule's actual flow definition.
    pub async fn get_schedule(&self, schedule_id: &str) -> Result<Schedule, TaskedError> {
        let url = format!(
            "{}/api/v1/schedules/{}",
            self.base_url,
            encode_path(schedule_id)
        );
        let body: ScheduleResponse = self.request_json(self.client.get(&url)).await?;
        body.into_schedule(None)
    }

    /// Update a schedule.
    ///
    /// The returned [`Schedule::flow_def`] is the definition you submitted
    /// (the server's response does not echo it back).
    pub async fn update_schedule(
        &self,
        schedule_id: &str,
        schedule_def: ScheduleDef,
    ) -> Result<Schedule, TaskedError> {
        let url = format!(
            "{}/api/v1/schedules/{}",
            self.base_url,
            encode_path(schedule_id)
        );
        let req = ScheduleRequest {
            cron: schedule_def.cron,
            flow: schedule_def.flow.clone(),
            name: schedule_def.name,
            enabled: schedule_def.enabled,
        };
        let body: ScheduleResponse = self.request_json(self.client.put(&url).json(&req)).await?;
        body.into_schedule(Some(schedule_def.flow))
    }

    /// Delete a schedule.
    pub async fn delete_schedule(&self, schedule_id: &str) -> Result<(), TaskedError> {
        let url = format!(
            "{}/api/v1/schedules/{}",
            self.base_url,
            encode_path(schedule_id)
        );
        self.request_empty(self.client.delete(&url)).await
    }
}

//! Server-Sent Events (SSE) subscription for flow events.

use crate::TaskedClient;
use crate::error::TaskedError;
use futures::stream::{Stream, StreamExt};
use serde::Deserialize;

/// An SSE event from the flow events stream.
#[derive(Debug, Clone)]
pub enum FlowEvent {
    /// A task changed state.
    TaskState {
        task_id: String,
        state: String,
        output: Option<serde_json::Value>,
        error: Option<String>,
        started_at: Option<String>,
        completed_at: Option<String>,
    },
    /// A task's output was updated (while running).
    TaskOutput {
        task_id: String,
        output: serde_json::Value,
    },
    /// The flow reached a terminal state.
    FlowComplete {
        flow_id: String,
        state: String,
        task_count: usize,
        tasks_succeeded: usize,
        tasks_failed: usize,
    },
    /// A keep-alive ping.
    Ping,
    /// An unknown or unparseable event.
    Unknown { event: String, data: String },
}

#[derive(Deserialize)]
struct TaskStateData {
    task_id: String,
    state: String,
    output: Option<serde_json::Value>,
    error: Option<String>,
    started_at: Option<String>,
    completed_at: Option<String>,
}

#[derive(Deserialize)]
struct TaskOutputData {
    task_id: String,
    output: serde_json::Value,
}

#[derive(Deserialize)]
struct FlowCompleteData {
    flow_id: String,
    state: String,
    task_count: usize,
    tasks_succeeded: usize,
    tasks_failed: usize,
}

impl TaskedClient {
    /// Subscribe to SSE events for a flow.
    ///
    /// Returns a stream of [`FlowEvent`]s. The stream ends when the flow
    /// reaches a terminal state or the connection is closed.
    pub async fn flow_events(
        &self,
        flow_id: &str,
    ) -> Result<impl Stream<Item = Result<FlowEvent, TaskedError>>, TaskedError> {
        let url = format!("{}/api/v1/flows/{flow_id}/events", self.base_url);
        let resp = self.client.get(&url).send().await?;

        if !resp.status().is_success() {
            return Err(self.parse_error(resp).await);
        }

        let stream = resp.bytes_stream().map(move |chunk| {
            let chunk = chunk.map_err(TaskedError::Http)?;
            let text = String::from_utf8_lossy(&chunk);

            let mut event_type = String::new();
            let mut data = String::new();

            for line in text.lines() {
                if let Some(et) = line.strip_prefix("event:") {
                    event_type = et.trim().to_string();
                } else if let Some(d) = line.strip_prefix("data:") {
                    data = d.trim().to_string();
                } else if line == ":ping" || line.starts_with(": ") {
                    return Ok(FlowEvent::Ping);
                }
            }

            if event_type.is_empty() && data.is_empty() {
                return Ok(FlowEvent::Ping);
            }

            match event_type.as_str() {
                "task_state" => {
                    if let Ok(d) = serde_json::from_str::<TaskStateData>(&data) {
                        Ok(FlowEvent::TaskState {
                            task_id: d.task_id,
                            state: d.state,
                            output: d.output,
                            error: d.error,
                            started_at: d.started_at,
                            completed_at: d.completed_at,
                        })
                    } else {
                        Ok(FlowEvent::Unknown {
                            event: event_type,
                            data,
                        })
                    }
                }
                "task_output" => {
                    if let Ok(d) = serde_json::from_str::<TaskOutputData>(&data) {
                        Ok(FlowEvent::TaskOutput {
                            task_id: d.task_id,
                            output: d.output,
                        })
                    } else {
                        Ok(FlowEvent::Unknown {
                            event: event_type,
                            data,
                        })
                    }
                }
                "flow_complete" => {
                    if let Ok(d) = serde_json::from_str::<FlowCompleteData>(&data) {
                        Ok(FlowEvent::FlowComplete {
                            flow_id: d.flow_id,
                            state: d.state,
                            task_count: d.task_count,
                            tasks_succeeded: d.tasks_succeeded,
                            tasks_failed: d.tasks_failed,
                        })
                    } else {
                        Ok(FlowEvent::Unknown {
                            event: event_type,
                            data,
                        })
                    }
                }
                _ => Ok(FlowEvent::Unknown {
                    event: event_type,
                    data,
                }),
            }
        });

        Ok(stream)
    }
}

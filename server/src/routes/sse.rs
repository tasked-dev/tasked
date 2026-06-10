//! Server-sent events stream for flow progress.

use axum::{
    extract::{Path, State},
    response::sse::{Event, Sse},
};
use futures::stream::Stream;
use tasked::types::FlowId;

use crate::error::ApiError;
use crate::routes::{AppState, find_flow};

/// Maximum number of concurrent SSE connections.
static SSE_SEMAPHORE: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(100));

pub(crate) async fn flow_events(
    State(engine): State<AppState>,
    Path(fid): Path<String>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError> {
    let permit = SSE_SEMAPHORE.try_acquire().map_err(|_| {
        ApiError::service_unavailable(
            "too_many_connections",
            "Too many concurrent SSE connections",
        )
    })?;

    // Verify flow exists
    find_flow(&engine, &fid).await?;
    let flow_id = FlowId::from(fid);

    let stream = async_stream::stream! {
        // Hold the permit for the lifetime of the SSE stream so it is released on disconnect.
        let _permit = permit;
        let mut prev_states: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        let mut prev_outputs: std::collections::HashMap<String, Option<serde_json::Value>> = std::collections::HashMap::new();

        loop {
            let tasks = match engine.get_flow_tasks(&flow_id).await {
                Ok(tasks) => tasks,
                Err(e) => {
                    tracing::error!(error = %e, flow_id = %flow_id, "engine error in SSE stream");
                    let error_data = serde_json::json!({
                        "error": "engine_error",
                        "message": "internal error",
                    });
                    if let Ok(data) = serde_json::to_string(&error_data) {
                        yield Ok(Event::default().event("error").data(data));
                    }
                    break;
                }
            };

            for task in &tasks {
                let state_str = task.state.to_string();
                let prev_state = prev_states.get(task.id.as_str());

                // Emit on state change
                if prev_state != Some(&state_str) {
                    prev_states.insert(task.id.to_string(), state_str.clone());

                    let event_data = serde_json::json!({
                        "task_id": task.id.as_str(),
                        "state": state_str,
                        "output": task.output,
                        "error": task.error,
                        "started_at": task.started_at.map(|d| d.to_rfc3339()),
                        "completed_at": task.completed_at.map(|d| d.to_rfc3339()),
                    });

                    if let Ok(data) = serde_json::to_string(&event_data) {
                        yield Ok(Event::default().event("task_state").data(data));
                    }
                }

                // Emit on output change (for streaming output updates while Running)
                if task.state == tasked::types::TaskState::Running {
                    let prev_output = prev_outputs.get(task.id.as_str());
                    if prev_output != Some(&task.output) {
                        prev_outputs.insert(task.id.to_string(), task.output.clone());
                        if let Some(ref output) = task.output
                            && let Ok(data) = serde_json::to_string(&serde_json::json!({
                                "task_id": task.id.as_str(),
                                "output": output,
                            }))
                        {
                            yield Ok(Event::default().event("task_output").data(data));
                        }
                    }
                }
            }

            // Check flow terminal state
            match engine.get_flow(&flow_id).await {
                Ok(Some(flow)) if flow.state.is_terminal() => {
                    let event_data = serde_json::json!({
                        "flow_id": flow.id.as_str(),
                        "state": flow.state.to_string(),
                        "task_count": flow.task_count,
                        "tasks_succeeded": flow.tasks_succeeded,
                        "tasks_failed": flow.tasks_failed,
                    });
                    if let Ok(data) = serde_json::to_string(&event_data) {
                        yield Ok(Event::default().event("flow_complete").data(data));
                    }
                    break;
                }
                Err(e) => {
                    tracing::error!(error = %e, flow_id = %flow_id, "engine error in SSE stream");
                    let error_data = serde_json::json!({
                        "error": "engine_error",
                        "message": "internal error",
                    });
                    if let Ok(data) = serde_json::to_string(&error_data) {
                        yield Ok(Event::default().event("error").data(data));
                    }
                    break;
                }
                _ => {}
            }

            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    };

    Ok(Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("ping"),
    ))
}

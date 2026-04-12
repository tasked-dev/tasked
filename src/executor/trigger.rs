use crate::types::{ExecuteResult, FlowDef, FlowState, QueueId, Task};
use async_trait::async_trait;
use serde_json::json;
use std::time::Duration;
use tracing::{debug, info};

use super::{ExecutionContext, Executor};

/// Trigger executor — submits a new flow to a queue and optionally waits for completion.
///
/// Config modes:
/// - Static: `{ "queue": "deploy", "flow": { "tasks": [...] } }`
/// - Dynamic: `{ "queue": "deploy", "flow": "${tasks.gen.output.flow_def}" }`
///   (variable substitution already applied by the time executor runs)
///
/// Options:
/// - `wait`: bool (default: true) — block until child flow completes
pub struct TriggerExecutor;

#[async_trait]
impl Executor for TriggerExecutor {
    async fn execute(&self, task: &Task, ctx: &ExecutionContext) -> ExecuteResult {
        // Validate config before checking for submitter — gives clearer errors.

        // Get queue
        let queue_id = match task.executor_config.get("queue").and_then(|v| v.as_str()) {
            Some(q) => QueueId::from(q),
            None => {
                return ExecuteResult::Failed {
                    error: "missing 'queue' in trigger config".to_string(),
                    retryable: false,
                };
            }
        };

        // Get flow definition
        let flow_def = match task.executor_config.get("flow") {
            Some(flow_val) => match serde_json::from_value::<FlowDef>(flow_val.clone()) {
                Ok(fd) => fd,
                Err(e) => {
                    return ExecuteResult::Failed {
                        error: format!("invalid flow definition: {e}"),
                        retryable: false,
                    };
                }
            },
            None => {
                return ExecuteResult::Failed {
                    error: "missing 'flow' in trigger config".to_string(),
                    retryable: false,
                };
            }
        };

        let wait = task
            .executor_config
            .get("wait")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let submitter = match &ctx.flow_submitter {
            Some(s) => s,
            None => {
                return ExecuteResult::Failed {
                    error: "trigger executor requires flow submission capability".to_string(),
                    retryable: false,
                };
            }
        };

        debug!(
            task_id = %task.id,
            queue = %queue_id,
            wait = wait,
            "triggering child flow"
        );

        // Submit the child flow (depth incremented by FlowSubmitter)
        let child = match submitter
            .submit(
                &queue_id,
                flow_def,
                ctx.trigger_depth,
                Some(task.flow_id.clone()),
            )
            .await
        {
            Ok(f) => f,
            Err(e) => {
                return ExecuteResult::Failed {
                    error: format!("failed to submit child flow: {e}"),
                    retryable: true,
                };
            }
        };

        info!(
            task_id = %task.id,
            child_flow_id = %child.id,
            queue = %queue_id,
            "child flow submitted"
        );

        if !wait {
            // Fire-and-forget
            return ExecuteResult::Success {
                output: Some(json!({
                    "flow_id": child.id.as_str(),
                    "queue_id": queue_id.as_str(),
                    "async": true,
                })),
            };
        }

        // Release the concurrency permit before entering the wait loop.
        // The trigger task is just polling, not doing compute work. Holding
        // the permit would deadlock if the child flow targets the same queue.
        ctx.release_concurrency_permit();

        // Wait for child flow to complete, respecting task timeout.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(task.timeout_secs);

        loop {
            if tokio::time::Instant::now() >= deadline {
                return ExecuteResult::Failed {
                    error: format!(
                        "trigger timed out waiting for child flow after {}s",
                        task.timeout_secs
                    ),
                    retryable: true,
                };
            }
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Check if parent task was cancelled (by cancel_flow or fail_fast).
            // The engine handles cancelling the child flow — we just stop polling.
            if ctx.is_cancelled().await {
                info!(
                    task_id = %task.id,
                    child_flow_id = %child.id,
                    "trigger task cancelled, stopping poll loop"
                );
                return ExecuteResult::Failed {
                    error: "trigger task cancelled".to_string(),
                    retryable: false,
                };
            }

            let flow = match submitter.query_flow(&child.id).await {
                Ok(Some(f)) => f,
                Ok(None) => {
                    return ExecuteResult::Failed {
                        error: "child flow disappeared".to_string(),
                        retryable: false,
                    };
                }
                Err(e) => {
                    return ExecuteResult::Failed {
                        error: format!("failed to query child flow: {e}"),
                        retryable: true,
                    };
                }
            };

            // Update parent task with child progress
            ctx.flush_output(json!({
                "flow_id": child.id.as_str(),
                "state": flow.state.to_string(),
                "tasks_succeeded": flow.tasks_succeeded,
                "tasks_failed": flow.tasks_failed,
                "task_count": flow.task_count,
            }))
            .await;

            if flow.state.is_terminal() {
                let output = json!({
                    "flow_id": child.id.as_str(),
                    "queue_id": queue_id.as_str(),
                    "state": flow.state.to_string(),
                    "task_count": flow.task_count,
                    "tasks_succeeded": flow.tasks_succeeded,
                    "tasks_failed": flow.tasks_failed,
                });

                return match flow.state {
                    FlowState::Succeeded => ExecuteResult::Success {
                        output: Some(output),
                    },
                    _ => ExecuteResult::Failed {
                        error: format!(
                            "child flow {}: {} succeeded, {} failed",
                            flow.state, flow.tasks_succeeded, flow.tasks_failed
                        ),
                        retryable: false,
                    },
                };
            }
        }
    }
}

//! Delay executor — sleeps for a configured duration, then succeeds.
//!
//! Task config:
//! ```json
//! { "seconds": 5 }
//! ```
//!
//! Useful for testing, rate-limiting, and flow timing.

use crate::types::{ExecuteResult, Task};
use async_trait::async_trait;
use serde_json::json;
use tracing::debug;

use super::{ExecutionContext, Executor};

/// Delay executor — sleeps for a configured number of seconds, then succeeds.
pub struct DelayExecutor;

#[async_trait]
impl Executor for DelayExecutor {
    async fn execute(&self, task: &Task, _ctx: &ExecutionContext) -> ExecuteResult {
        let seconds = match task.executor_config.get("seconds") {
            Some(v) => match v.as_f64() {
                Some(s) if s >= 0.0 => s,
                Some(s) => {
                    return ExecuteResult::Failed {
                        error: format!("'seconds' must be non-negative, got {s}"),
                        retryable: false,
                    };
                }
                None => {
                    return ExecuteResult::Failed {
                        error: format!("'seconds' must be a number, got {v}"),
                        retryable: false,
                    };
                }
            },
            None => {
                return ExecuteResult::Failed {
                    error: "missing 'seconds' in executor config".to_string(),
                    retryable: false,
                };
            }
        };

        debug!(task_id = %task.id, seconds, "delaying");

        let timeout = std::time::Duration::from_secs(task.timeout_secs);
        let delay = std::time::Duration::from_secs_f64(seconds);

        if delay > timeout {
            return ExecuteResult::Failed {
                error: format!(
                    "delay {}s exceeds task timeout {}s",
                    seconds, task.timeout_secs
                ),
                retryable: false,
            };
        }

        tokio::time::sleep(delay).await;

        ExecuteResult::Success {
            output: Some(json!({ "delayed_seconds": seconds })),
        }
    }
}

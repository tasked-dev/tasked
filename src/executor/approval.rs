//! Approval executor -- pauses the task until a human approves or rejects it.
//!
//! Task config:
//! ```json
//! { "message": "Deploy to production?" }
//! ```
//!
//! The task enters Running state with output:
//! ```json
//! { "awaiting_approval": true, "message": "...", "code": "abc12345" }
//! ```
//!
//! Complete via: `POST /api/v1/flows/{fid}/tasks/{tid}/ack`
//! with: `{ "status": "success" }` or `{ "status": "failed", "error": "rejected" }`

use crate::types::{ExecuteResult, Task};
use async_trait::async_trait;
use rand::random;
use serde_json::json;

use super::{ExecutionContext, Executor};

/// Approval executor -- pauses the task until a human approves or rejects it.
pub struct ApprovalExecutor;

#[async_trait]
impl Executor for ApprovalExecutor {
    async fn execute(&self, task: &Task, _ctx: &ExecutionContext) -> ExecuteResult {
        let message = task
            .executor_config
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("Approval required")
            .to_string();

        // 128 bits: the code may be used as an approval credential, so it
        // must not be brute-forceable.
        let code = format!("{:016x}{:016x}", random::<u64>(), random::<u64>());

        ExecuteResult::AwaitingApproval {
            output: json!({
                "awaiting_approval": true,
                "message": message,
                "code": code,
            }),
        }
    }
}

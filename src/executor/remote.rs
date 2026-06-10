//! Remote executor — delegates task execution to an external HTTP service.
//!
//! Provides a standardized bidirectional protocol: task context goes in,
//! structured result comes out. This enables user-supplied executors in any
//! language without compiling them into the tasked binary.
//!
//! # Security — use HTTPS for non-loopback endpoints
//!
//! When the configured URL uses plain HTTP and the host is not a loopback
//! address (127.0.0.0/8, ::1, or `localhost`), a warning is logged at
//! runtime. Bearer tokens, API keys, and other credentials in request
//! headers are transmitted in cleartext over HTTP, so **HTTPS should be
//! used for any endpoint reachable over a network**.
//!
//! # Task config
//!
//! ```json
//! {
//!   "executor": "remote",
//!   "config": {
//!     "url": "http://localhost:9090/execute",
//!     "timeout_secs": 30,
//!     "headers": { "Authorization": "Bearer xxx" }
//!   }
//! }
//! ```
//!
//! # Protocol
//!
//! **Request** (POST to configured URL):
//! ```json
//! {
//!   "task_id": "my-task",
//!   "flow_id": "abc-123",
//!   "executor_type": "remote",
//!   "config": { ... },
//!   "input": { ... }
//! }
//! ```
//!
//! **Response** (expected from the service):
//! ```json
//! {
//!   "status": "success",
//!   "output": { "result": "data" }
//! }
//! ```
//! or:
//! ```json
//! {
//!   "status": "failed",
//!   "error": "something went wrong",
//!   "retryable": true
//! }
//! ```

use crate::types::{ExecuteResult, Task};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, warn};

use super::{ExecutionContext, Executor};

/// Remote executor — delegates to an external HTTP service.
pub struct RemoteExecutor {
    client: Client,
}

impl RemoteExecutor {
    pub fn new() -> Self {
        Self {
            client: crate::url_policy::ssrf_safe_client(),
        }
    }
}

impl Default for RemoteExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Request payload sent to the remote service.
#[derive(Debug, Serialize)]
struct RemoteRequest<'a> {
    task_id: &'a str,
    flow_id: &'a str,
    executor_type: &'a str,
    config: &'a Value,
    input: &'a Option<Value>,
}

/// Response expected from the remote service.
///
/// The `status` field is required and must be either `"success"` or `"failed"`.
/// Unknown fields are silently ignored so the protocol can be extended.
#[derive(Debug, Deserialize)]
struct RemoteResponse {
    status: String,
    #[serde(default)]
    output: Option<Value>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    retryable: bool,
}

#[async_trait]
impl Executor for RemoteExecutor {
    async fn execute(&self, task: &Task, _ctx: &ExecutionContext) -> ExecuteResult {
        let config = &task.executor_config;

        let url = match config.get("url").and_then(|v| v.as_str()) {
            Some(url) => url.to_string(),
            None => {
                return ExecuteResult::Failed {
                    error: "missing 'url' in remote executor config".to_string(),
                    retryable: false,
                };
            }
        };

        if let Err(reason) = crate::url_policy::validate_url(&url) {
            return ExecuteResult::Failed {
                error: format!("SSRF blocked: {reason}"),
                retryable: false,
            };
        }

        // Warn when credentials may be sent over an unencrypted connection.
        // We don't hard-fail to avoid breaking existing deployments, but
        // operators should migrate to HTTPS for any non-loopback endpoint.
        if crate::url_policy::is_insecure_non_loopback(&url) {
            warn!(
                task_id = %task.id,
                url = %url,
                "remote executor URL uses plain HTTP on a non-loopback host — \
                 credentials may be transmitted in cleartext; use HTTPS in production"
            );
        }

        let timeout_secs = config
            .get("timeout_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(task.timeout_secs);

        let headers: HashMap<String, String> = match config.get("headers") {
            Some(v) => match serde_json::from_value(v.clone()) {
                Ok(h) => h,
                Err(e) => {
                    warn!(
                        task_id = %task.id,
                        error = %e,
                        "invalid 'headers' in remote executor config, ignoring headers"
                    );
                    HashMap::new()
                }
            },
            None => HashMap::new(),
        };

        debug!(
            task_id = %task.id,
            url = %url,
            "delegating to remote executor"
        );

        // Strip sensitive fields (url, headers) from the config before sending
        // to the remote service. These contain credentials and the endpoint URL
        // which the remote side should not receive.
        let mut sanitized_config = task.executor_config.clone();
        if let Some(obj) = sanitized_config.as_object_mut() {
            obj.remove("url");
            obj.remove("headers");
        }

        let payload = RemoteRequest {
            task_id: task.id.as_str(),
            flow_id: task.flow_id.as_str(),
            executor_type: &task.executor_type,
            config: &sanitized_config,
            input: &task.input,
        };

        let mut request = self
            .client
            .post(&url)
            .timeout(Duration::from_secs(timeout_secs))
            .json(&payload);

        for (key, value) in &headers {
            request = request.header(key.as_str(), value.as_str());
        }

        match request.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let body = match super::read_response_body(resp).await {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(
                            task_id = %task.id,
                            error = %e,
                            "failed to read response body from remote executor"
                        );
                        return ExecuteResult::Failed {
                            error: format!("remote executor response error: {e}"),
                            retryable: false,
                        };
                    }
                };

                if !(200..300).contains(&status) {
                    return ExecuteResult::Failed {
                        error: format!(
                            "remote executor returned HTTP {status}: {}",
                            super::truncate_body_for_error(&body)
                        ),
                        retryable: status >= 500,
                    };
                }

                // Parse the response
                match serde_json::from_str::<RemoteResponse>(&body) {
                    Ok(remote_resp) => match remote_resp.status.as_str() {
                        "success" => ExecuteResult::Success {
                            output: remote_resp.output,
                        },
                        "failed" => ExecuteResult::Failed {
                            error: remote_resp
                                .error
                                .unwrap_or_else(|| "remote executor reported failure".to_string()),
                            retryable: remote_resp.retryable,
                        },
                        other => ExecuteResult::Failed {
                            error: format!(
                                "remote executor returned unknown status: '{other}'. \
                                 Expected 'success' or 'failed'."
                            ),
                            retryable: false,
                        },
                    },
                    Err(e) => ExecuteResult::Failed {
                        error: format!(
                            "failed to parse remote executor response: {e}. Body: {}",
                            super::truncate_body_for_error(&body)
                        ),
                        retryable: false,
                    },
                }
            }
            Err(e) => {
                warn!(task_id = %task.id, error = %e, "remote executor request failed");
                let retryable = e.is_timeout() || e.is_connect();
                ExecuteResult::Failed {
                    error: format!("remote executor request failed: {e}"),
                    retryable,
                }
            }
        }
    }
}

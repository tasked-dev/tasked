use crate::types::{ExecuteResult, Task};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, warn};

use super::{ExecutionContext, Executor};

/// HTTP executor — sends an HTTP request to a configured URL.
///
/// Task config should contain:
/// ```json
/// {
///     "url": "https://example.com/webhook",
///     "method": "POST",
///     "headers": {"Authorization": "Bearer xxx"},
///     "body": {"key": "value"},
///     "timeout_secs": 30,
///     "mode": "inline"
/// }
/// ```
///
/// Modes:
/// - `inline` (default): waits for HTTP response. 2xx = success, anything else = failure.
/// - `callback`: fires request and returns Pending, expecting the target to call back via /ack.
pub struct HttpExecutor {
    client: Client,
}

impl HttpExecutor {
    pub fn new() -> Self {
        Self {
            client: crate::url_policy::ssrf_safe_client(),
        }
    }
}

impl Default for HttpExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Executor for HttpExecutor {
    async fn execute(&self, task: &Task, ctx: &ExecutionContext) -> ExecuteResult {
        let config = &task.executor_config;

        let url = match config.get("url").and_then(|v| v.as_str()) {
            Some(url) => url.to_string(),
            None => {
                return ExecuteResult::Failed {
                    error: "missing 'url' in executor config".to_string(),
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

        let method = config
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or("POST")
            .to_uppercase();

        let timeout_secs = config
            .get("timeout_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(task.timeout_secs);

        let mode = config
            .get("mode")
            .and_then(|v| v.as_str())
            .unwrap_or("inline");

        let headers: HashMap<String, String> = config
            .get("headers")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        debug!(
            task_id = %task.id,
            url = %url,
            method = %method,
            mode = %mode,
            "executing HTTP request"
        );

        let mut request = match method.as_str() {
            "GET" => self.client.get(&url),
            "POST" => self.client.post(&url),
            "PUT" => self.client.put(&url),
            "PATCH" => self.client.patch(&url),
            "DELETE" => self.client.delete(&url),
            other => {
                return ExecuteResult::Failed {
                    error: format!("unsupported HTTP method: {other}"),
                    retryable: false,
                };
            }
        };

        request = request.timeout(Duration::from_secs(timeout_secs));

        for (key, value) in &headers {
            request = request.header(key.as_str(), value.as_str());
        }

        // Add body for methods that support it
        if let Some(body) = config.get("body")
            && matches!(method.as_str(), "POST" | "PUT" | "PATCH")
        {
            request = request.json(body);
        }

        // Add task input as body if no explicit body and method supports it
        if config.get("body").is_none()
            && let Some(input) = &task.input
            && matches!(method.as_str(), "POST" | "PUT" | "PATCH")
        {
            request = request.json(input);
        }

        // Callback mode: not yet implemented
        if mode == "callback" {
            return ExecuteResult::Failed {
                error: "callback mode is not yet implemented".into(),
                retryable: false,
            };
        }

        // Inline mode: wait for response, aborting if the flow is cancelled
        // (dropping the future aborts the in-flight request).
        let send_result = tokio::select! {
            r = request.send() => r,
            _ = ctx.cancelled() => {
                return ExecuteResult::Failed {
                    error: "task cancelled".to_string(),
                    retryable: false,
                };
            }
        };
        match send_result {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let body = match super::read_response_body(resp).await {
                    Ok(b) => b,
                    Err(e) => {
                        return ExecuteResult::Failed {
                            error: e,
                            retryable: false,
                        };
                    }
                };

                if (200..300).contains(&status) {
                    ExecuteResult::Success {
                        output: Some(json!({
                            "status": status,
                            "body": body,
                        })),
                    }
                } else {
                    ExecuteResult::Failed {
                        error: format!("HTTP {status}: {}", super::truncate_body_for_error(&body)),
                        retryable: status >= 500,
                    }
                }
            }
            Err(e) => {
                warn!(task_id = %task.id, error = %e, "HTTP request failed");
                let retryable = e.is_timeout() || e.is_connect();
                ExecuteResult::Failed {
                    error: format!("HTTP request failed: {e}"),
                    retryable,
                }
            }
        }
    }
}

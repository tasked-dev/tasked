//! Webhook delivery for flow lifecycle events.

use crate::types::Flow;
#[cfg(feature = "http")]
use crate::types::FlowState;
use tracing::debug;
#[cfg(feature = "http")]
use tracing::warn;

/// Fire a webhook HTTP POST for a flow event. Best-effort, non-blocking.
pub fn fire(url: &str, flow: &Flow) {
    fire_inner(url, flow);
}

#[cfg(feature = "http")]
fn shared_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(crate::url_policy::ssrf_safe_client)
}

#[cfg(feature = "http")]
fn fire_inner(url: &str, flow: &Flow) {
    if let Err(reason) = crate::url_policy::validate_url(url) {
        warn!(url = %url, reason = %reason, "webhook blocked by SSRF policy");
        return;
    }

    let payload = serde_json::json!({
        "event": if flow.state == FlowState::Succeeded { "flow_completed" } else { "flow_failed" },
        "flow_id": flow.id.as_str(),
        "queue_id": flow.queue_id.as_str(),
        "state": flow.state.to_string(),
        "task_count": flow.task_count,
        "tasks_succeeded": flow.tasks_succeeded,
        "tasks_failed": flow.tasks_failed,
    });

    let url = url.to_string();
    tokio::spawn(async move {
        let client = shared_client();
        match client
            .post(&url)
            .json(&payload)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            Ok(resp) => {
                debug!(url = %url, status = resp.status().as_u16(), "webhook delivered");
            }
            Err(e) => {
                warn!(url = %url, error = %e, "webhook delivery failed");
            }
        }
    });
}

#[cfg(not(feature = "http"))]
fn fire_inner(url: &str, _flow: &Flow) {
    debug!(url = %url, "webhook skipped (http feature disabled)");
}


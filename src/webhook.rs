//! Webhook delivery for flow lifecycle events.

use crate::types::Flow;
#[cfg(feature = "http")]
use crate::types::FlowState;
use tracing::debug;
#[cfg(feature = "http")]
use tracing::warn;

/// Fire a webhook HTTP POST for a flow event. Best-effort, non-blocking.
///
/// When the flow's webhook config carries a `secret`, the request includes an
/// `X-Tasked-Signature: sha256=<hex>` header with the HMAC-SHA256 of the body
/// so receivers can authenticate the sender.
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
    let payload = serde_json::json!({
        "event": if flow.state == FlowState::Succeeded { "flow_completed" } else { "flow_failed" },
        "flow_id": flow.id.as_str(),
        "queue_id": flow.queue_id.as_str(),
        "state": flow.state.to_string(),
        "task_count": flow.task_count,
        "tasks_succeeded": flow.tasks_succeeded,
        "tasks_failed": flow.tasks_failed,
    });
    let secret = flow.webhooks.as_ref().and_then(|w| w.secret.clone());

    let url = url.to_string();
    tokio::spawn(async move {
        // Async SSRF validation — the resolver-level defense in
        // ssrf_safe_client still applies at connection time.
        if let Err(reason) = crate::url_policy::validate_url_async(&url).await {
            warn!(url = %url, reason = %reason, "webhook blocked by SSRF policy");
            return;
        }

        let body = match serde_json::to_vec(&payload) {
            Ok(b) => b,
            Err(e) => {
                warn!(url = %url, error = %e, "webhook payload serialization failed");
                return;
            }
        };

        let client = shared_client();
        let mut req = client
            .post(&url)
            .header("content-type", "application/json")
            .timeout(std::time::Duration::from_secs(10));
        if let Some(secret) = secret {
            let sig = hmac_sha256(secret.as_bytes(), &body);
            let hex: String = sig.iter().map(|b| format!("{b:02x}")).collect();
            req = req.header("x-tasked-signature", format!("sha256={hex}"));
        }

        match req.body(body).send().await {
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

/// HMAC-SHA256 (RFC 2104), implemented directly on `sha2` to avoid a
/// version-coupled `hmac` crate dependency. Verified against RFC 4231 test
/// vectors below.
#[cfg(feature = "http")]
fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    const BLOCK: usize = 64;

    let mut key_block = [0u8; BLOCK];
    if key.len() > BLOCK {
        key_block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }

    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= key_block[i];
        opad[i] ^= key_block[i];
    }

    let inner = Sha256::new()
        .chain_update(ipad)
        .chain_update(message)
        .finalize();
    let outer = Sha256::new()
        .chain_update(opad)
        .chain_update(inner)
        .finalize();
    outer.into()
}

#[cfg(all(test, feature = "http"))]
mod tests {
    use super::hmac_sha256;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// RFC 4231 test case 2.
    #[test]
    fn hmac_sha256_rfc4231_case2() {
        let out = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(
            hex(&out),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    /// RFC 4231 test case 1 (20-byte 0x0b key).
    #[test]
    fn hmac_sha256_rfc4231_case1() {
        let out = hmac_sha256(&[0x0b; 20], b"Hi There");
        assert_eq!(
            hex(&out),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    /// RFC 4231 test case 6 (131-byte key, exercising the key-hashing path).
    #[test]
    fn hmac_sha256_rfc4231_case6() {
        let out = hmac_sha256(
            &[0xaa; 131],
            b"Test Using Larger Than Block-Size Key - Hash Key First",
        );
        assert_eq!(
            hex(&out),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }
}

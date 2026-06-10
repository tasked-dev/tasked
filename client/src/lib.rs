//! Rust HTTP client for the Tasked server API.
//!
//! Provides a typed client for interacting with a running `tasked-server`
//! instance. Uses the [`tasked`] crate's types for shared type definitions so
//! that request/response types match the server exactly.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use tasked_client::TaskedClient;
//! use tasked::types::*;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let client = TaskedClient::builder("http://localhost:8080")
//!     .bearer_token("my-secret-token")
//!     .build()?;
//!
//! // Create a queue
//! let queue = client.create_queue("my-queue", QueueConfig::default()).await?;
//!
//! // Submit a flow
//! let flow = client.submit_flow("my-queue", FlowDef {
//!     tasks: vec![TaskDef {
//!         id: TaskId::from("hello"),
//!         executor: "shell".into(),
//!         config: serde_json::json!({ "command": "echo hello" }),
//!         ..Default::default()
//!     }],
//!     ..Default::default()
//! }).await?;
//!
//! // Get flow details
//! let detail = client.get_flow(flow.id.as_str()).await?;
//! # Ok(())
//! # }
//! ```

pub mod error;
pub mod export;
pub mod flows;
pub mod queues;
pub mod schedules;
pub mod sse;
pub mod tasks;

pub use error::TaskedError;
pub use flows::FlowDetail;
pub use sse::FlowEvent;
pub use tasks::TaskAck;

use reqwest::header::{AUTHORIZATION, HeaderValue};
use serde::de::DeserializeOwned;
use std::time::Duration;

/// Default total request timeout for regular (non-streaming) API calls.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
/// Default connect timeout for all requests.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Client for the Tasked server HTTP API.
#[derive(Debug, Clone)]
pub struct TaskedClient {
    pub(crate) client: reqwest::Client,
    /// Dedicated client for the SSE event stream: it must not have a total
    /// request timeout because the stream is long-lived.
    pub(crate) sse_client: reqwest::Client,
    pub(crate) base_url: String,
    pub(crate) auth: Option<HeaderValue>,
}

/// Builder for [`TaskedClient`].
pub struct TaskedClientBuilder {
    base_url: String,
    bearer_token: Option<String>,
    client: Option<reqwest::Client>,
}

impl TaskedClientBuilder {
    /// Set an optional Bearer token for authentication.
    ///
    /// The token is applied per-request, so it works with both the default
    /// client and a custom client supplied via [`Self::client`].
    pub fn bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }

    /// Supply a custom [`reqwest::Client`] for regular API requests
    /// (e.g. to configure proxies, TLS, or different timeouts).
    ///
    /// The SSE event stream ([`TaskedClient::flow_events`]) always uses a
    /// dedicated internal client without a total request timeout, since the
    /// stream is long-lived; the custom client is used for everything else.
    pub fn client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Build the client.
    pub fn build(self) -> Result<TaskedClient, TaskedError> {
        let auth = match &self.bearer_token {
            Some(token) => Some(
                HeaderValue::from_str(&format!("Bearer {token}"))
                    .map_err(|e| TaskedError::Deserialize(format!("invalid auth header: {e}")))?,
            ),
            None => None,
        };

        let client = match self.client {
            Some(c) => c,
            None => reqwest::Client::builder()
                .timeout(DEFAULT_TIMEOUT)
                .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
                .build()?,
        };

        // SSE streams are long-lived: connect timeout only, no total timeout.
        let sse_client = reqwest::Client::builder()
            .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
            .build()?;

        Ok(TaskedClient {
            client,
            sse_client,
            base_url: self.base_url.trim_end_matches('/').to_string(),
            auth,
        })
    }
}

impl TaskedClient {
    /// Create a new client builder with the given base URL.
    pub fn builder(base_url: impl Into<String>) -> TaskedClientBuilder {
        TaskedClientBuilder {
            base_url: base_url.into(),
            bearer_token: None,
            client: None,
        }
    }

    /// Create a client with no authentication. Shorthand for
    /// `TaskedClient::builder(base_url).build()`.
    pub fn new(base_url: impl Into<String>) -> Result<Self, TaskedError> {
        Self::builder(base_url).build()
    }

    /// Return the base URL this client is configured for.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Attach the configured auth header (if any) to a request.
    pub(crate) fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            Some(value) => req.header(AUTHORIZATION, value.clone()),
            None => req,
        }
    }

    /// Send a request, check the response status, and deserialize the JSON body.
    pub(crate) async fn request_json<T: DeserializeOwned>(
        &self,
        req: reqwest::RequestBuilder,
    ) -> Result<T, TaskedError> {
        let resp = self.apply_auth(req).send().await?;
        if !resp.status().is_success() {
            return Err(error::parse_error(resp).await);
        }
        Ok(resp.json().await?)
    }

    /// Send a request and check the response status, discarding the body.
    pub(crate) async fn request_empty(
        &self,
        req: reqwest::RequestBuilder,
    ) -> Result<(), TaskedError> {
        let resp = self.apply_auth(req).send().await?;
        if !resp.status().is_success() {
            return Err(error::parse_error(resp).await);
        }
        Ok(())
    }
}

/// Percent-encode a string for use as a single URL path segment.
///
/// Everything outside the RFC 3986 unreserved set (`A-Z a-z 0-9 - _ . ~`)
/// is encoded, so IDs containing `/`, `?`, `#`, spaces, etc. cannot change
/// the request target.
pub(crate) fn encode_path(segment: &str) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(segment.len());
    for &b in segment.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => {
                // Writing to a String cannot fail.
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::encode_path;

    #[test]
    fn encode_path_passes_unreserved() {
        assert_eq!(encode_path("abc-DEF_123.~"), "abc-DEF_123.~");
    }

    #[test]
    fn encode_path_escapes_delimiters() {
        assert_eq!(encode_path("a/b?c#d"), "a%2Fb%3Fc%23d");
        assert_eq!(encode_path("a b%"), "a%20b%25");
    }

    #[test]
    fn encode_path_escapes_multibyte_utf8() {
        assert_eq!(encode_path("héllo"), "h%C3%A9llo");
    }
}

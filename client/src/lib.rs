//! Rust HTTP client for the Tasked server API.
//!
//! Provides a typed client for interacting with a running `tasked-server`
//! instance. Uses [`tasked_types`] for shared type definitions so that
//! request/response types match the server exactly.
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
//!     webhooks: None,
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

use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};

/// Client for the Tasked server HTTP API.
#[derive(Debug, Clone)]
pub struct TaskedClient {
    pub(crate) client: reqwest::Client,
    pub(crate) base_url: String,
}

/// Builder for [`TaskedClient`].
pub struct TaskedClientBuilder {
    base_url: String,
    bearer_token: Option<String>,
}

impl TaskedClientBuilder {
    /// Set an optional Bearer token for authentication.
    pub fn bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }

    /// Build the client.
    pub fn build(self) -> Result<TaskedClient, TaskedError> {
        let mut headers = HeaderMap::new();
        if let Some(token) = &self.bearer_token {
            let value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|e| TaskedError::Deserialize(format!("invalid auth header: {e}")))?;
            headers.insert(AUTHORIZATION, value);
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        Ok(TaskedClient {
            client,
            base_url: self.base_url.trim_end_matches('/').to_string(),
        })
    }
}

impl TaskedClient {
    /// Create a new client builder with the given base URL.
    pub fn builder(base_url: impl Into<String>) -> TaskedClientBuilder {
        TaskedClientBuilder {
            base_url: base_url.into(),
            bearer_token: None,
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
}

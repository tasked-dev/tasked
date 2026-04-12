//! Error types for the Tasked client.

use serde::Deserialize;

/// Error returned by the Tasked client.
#[derive(Debug, thiserror::Error)]
pub enum TaskedError {
    /// HTTP transport error.
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    /// Server returned an error response.
    #[error("api error ({status}): {error} - {message}")]
    Api {
        status: u16,
        error: String,
        message: String,
    },

    /// Failed to deserialize response body.
    #[error("deserialization error: {0}")]
    Deserialize(String),
}

/// Error response body from the server.
#[derive(Debug, Deserialize)]
pub(crate) struct ErrorResponse {
    pub error: String,
    pub message: String,
}

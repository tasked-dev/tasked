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

    /// The server returned a response the client could not interpret
    /// (unknown enum value, malformed timestamp, invalid UTF-8, ...).
    #[error("invalid response from server: {0}")]
    InvalidResponse(String),
}

/// Error response body from the server.
#[derive(Debug, Deserialize)]
pub(crate) struct ErrorResponse {
    pub error: String,
    pub message: String,
}

/// Parse an error response from the server into a [`TaskedError::Api`].
pub(crate) async fn parse_error(resp: reqwest::Response) -> TaskedError {
    let status = resp.status().as_u16();
    match resp.json::<ErrorResponse>().await {
        Ok(body) => TaskedError::Api {
            status,
            error: body.error,
            message: body.message,
        },
        Err(_) => TaskedError::Api {
            status,
            error: "unknown".to_string(),
            message: format!("Server returned status {status}"),
        },
    }
}

/// Parse an RFC 3339 timestamp, mapping failures to [`TaskedError::InvalidResponse`].
pub(crate) fn parse_timestamp(
    value: &str,
    field: &str,
) -> Result<chrono::DateTime<chrono::Utc>, TaskedError> {
    value.parse().map_err(|e| {
        TaskedError::InvalidResponse(format!("invalid timestamp in `{field}`: {value:?} ({e})"))
    })
}

/// Parse an optional RFC 3339 timestamp, mapping failures to
/// [`TaskedError::InvalidResponse`].
pub(crate) fn parse_opt_timestamp(
    value: Option<String>,
    field: &str,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, TaskedError> {
    value.map(|s| parse_timestamp(&s, field)).transpose()
}

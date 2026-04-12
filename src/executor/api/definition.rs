//! Integration definition types.
//!
//! An integration definition is a JSON file that describes an API service:
//! its base URL, authentication strategy, and named operations (HTTP endpoints).
//!
//! # Example
//!
//! ```json
//! {
//!   "name": "github",
//!   "version": 1,
//!   "base_url": "https://api.github.com",
//!   "default_headers": { "Accept": "application/vnd.github.v3+json" },
//!   "auth": { "type": "bearer", "token_template": "${credential}" },
//!   "operations": {
//!     "list_issues": {
//!       "method": "GET",
//!       "path": "/repos/${params.owner}/${params.repo}/issues",
//!       "query": { "state": "${params.state}" }
//!     }
//!   }
//! }
//! ```

use serde::Deserialize;
use std::collections::HashMap;

/// A complete integration definition.
#[derive(Debug, Clone, Deserialize)]
pub struct IntegrationDef {
    pub name: String,
    #[serde(default = "default_version")]
    pub version: u32,
    pub base_url: String,
    #[serde(default)]
    pub default_headers: HashMap<String, String>,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    pub operations: HashMap<String, OperationDef>,
}

fn default_version() -> u32 {
    1
}

/// Authentication strategy for the integration.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfig {
    /// Set a custom header (e.g., `Authorization: token xxx`).
    Header {
        header: String,
        value_template: String,
    },
    /// Append a query parameter (e.g., `?api_key=xxx`).
    Query {
        param: String,
        value_template: String,
    },
    /// HTTP Basic Auth.
    Basic {
        username_template: String,
        password_template: String,
    },
    /// Bearer token (`Authorization: Bearer xxx`).
    Bearer { token_template: String },
    /// OAuth2 with automatic token refresh.
    ///
    /// The credential should be a JSON object with `client_id`, `client_secret`,
    /// and `refresh_token` fields (or use templates to reference them).
    #[serde(rename = "oauth2")]
    OAuth2 {
        token_url: String,
        client_id_template: String,
        client_secret_template: String,
        refresh_token_template: String,
        #[serde(default)]
        scopes: Option<String>,
    },
}

/// An API operation (a single endpoint).
#[derive(Debug, Clone, Deserialize)]
pub struct OperationDef {
    pub method: String,
    pub path: String,
    #[serde(default)]
    pub query: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub body: Option<serde_json::Value>,
    #[serde(default)]
    pub pagination: Option<PaginationConfig>,
    #[serde(default)]
    pub response: Option<ResponseConfig>,
}

/// Pagination strategy for an operation.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PaginationConfig {
    /// Follow RFC 8288 `Link` headers.
    LinkHeader {
        #[serde(default = "default_max_pages")]
        max_pages: u32,
    },
    /// Cursor-based: read cursor from response, send as query param.
    Cursor {
        param: String,
        response_path: String,
        #[serde(default = "default_max_pages")]
        max_pages: u32,
    },
    /// Offset-based: increment offset by limit each page.
    Offset {
        param: String,
        limit_param: String,
        limit: u32,
        #[serde(default = "default_max_pages")]
        max_pages: u32,
    },
}

fn default_max_pages() -> u32 {
    10
}

/// Response extraction configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ResponseConfig {
    /// Map of output field name → JSON path in the response body.
    /// Simple dot-separated paths: `"title"` or `"head.sha"`.
    #[serde(default)]
    pub extract: HashMap<String, String>,
}


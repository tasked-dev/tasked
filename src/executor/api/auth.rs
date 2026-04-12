//! Auth application strategies for integration executors.

use super::definition::AuthConfig;
use super::interpolate::{InterpolationContext, interpolate};
use serde_json::Value;

/// Resolved authentication, ready to apply to requests.
///
/// Pre-resolves templates so the result can be reused across paginated requests.
#[derive(Debug, Clone)]
pub enum ResolvedAuth {
    Header { header: String, value: String },
    Query { param: String, value: String },
    Basic { username: String, password: String },
    Bearer { token: String },
}

/// Resolve authentication config templates into a reusable `ResolvedAuth`.
pub fn resolve_auth(auth: &AuthConfig, ctx: &InterpolationContext) -> ResolvedAuth {
    match auth {
        AuthConfig::Header {
            header,
            value_template,
        } => ResolvedAuth::Header {
            header: header.clone(),
            value: resolve_template(value_template, ctx),
        },
        AuthConfig::Query {
            param,
            value_template,
        } => ResolvedAuth::Query {
            param: param.clone(),
            value: resolve_template(value_template, ctx),
        },
        AuthConfig::Basic {
            username_template,
            password_template,
        } => ResolvedAuth::Basic {
            username: resolve_template(username_template, ctx),
            password: resolve_template(password_template, ctx),
        },
        AuthConfig::Bearer { token_template } => ResolvedAuth::Bearer {
            token: resolve_template(token_template, ctx),
        },
        AuthConfig::OAuth2 { .. } => {
            // OAuth2 is resolved separately in IntegrationExecutor::execute()
            // via the token refresh flow. This branch should not be reached.
            unreachable!("OAuth2 auth should be resolved before calling resolve_auth")
        }
    }
}

impl ResolvedAuth {
    /// Apply this auth to a request builder.
    pub fn apply(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self {
            Self::Header { header, value } => request.header(header.as_str(), value.as_str()),
            Self::Query { param, value } => request.query(&[(param.as_str(), value.as_str())]),
            Self::Basic { username, password } => request.basic_auth(username, Some(password)),
            Self::Bearer { token } => request.bearer_auth(token),
        }
    }
}

/// Resolve a template string to its final string value.
fn resolve_template(template: &str, ctx: &InterpolationContext) -> String {
    let resolved = interpolate(&Value::String(template.to_string()), ctx);
    match resolved {
        Value::String(s) => s,
        other => other.to_string(),
    }
}


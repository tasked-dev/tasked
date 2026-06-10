//! Integration executor — interprets JSON integration definitions at runtime.
//!
//! Each integration definition file registers as a named executor. Loading
//! `github.json` registers a `github` executor, making it a first-class citizen
//! alongside `shell`, `http`, `container`, etc.
//!
//! # Task config
//!
//! ```json
//! {
//!   "executor": "github",
//!   "config": {
//!     "operation": "list_issues",
//!     "credential": "${secrets.GITHUB_TOKEN}",
//!     "owner": "myorg",
//!     "repo": "myrepo",
//!     "state": "open"
//!   }
//! }
//! ```
//!
//! Reserved config keys: `operation`, `credential`, `definition`.
//! All other keys are treated as params available via `${params.*}` in the definition.

pub mod auth;
pub mod definition;
pub mod interpolate;
pub mod oauth2;
pub mod pagination;
pub mod response;

use crate::types::{ExecuteResult, Task};
use async_trait::async_trait;
use definition::{AuthConfig, IntegrationDef};
use interpolate::InterpolationContext;
use oauth2::TokenCache;
use pagination::RequestTemplate;
use reqwest::Client;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

use super::{ExecutionContext, Executor};

/// Reserved keys in the task config that are not treated as params.
const RESERVED_KEYS: &[&str] = &["operation", "credential", "definition"];

/// An executor backed by an integration definition.
///
/// Created once per integration definition and registered under the integration's name.
pub struct IntegrationExecutor {
    client: Client,
    definition: IntegrationDef,
    token_cache: Option<Arc<TokenCache>>,
}

impl IntegrationExecutor {
    pub fn new(definition: IntegrationDef) -> Self {
        Self {
            client: crate::url_policy::ssrf_safe_client(),
            definition,
            token_cache: None,
        }
    }

    pub fn with_client(definition: IntegrationDef, client: Client) -> Self {
        Self {
            client,
            definition,
            token_cache: None,
        }
    }

    pub fn with_token_cache(
        definition: IntegrationDef,
        client: Client,
        token_cache: Arc<TokenCache>,
    ) -> Self {
        Self {
            client,
            definition,
            token_cache: Some(token_cache),
        }
    }

    /// Obtain an OAuth2 access token: cached if still valid, otherwise refresh.
    ///
    /// With a token cache configured, a per-cache-key async lock serializes
    /// concurrent refreshes of the same credential (preventing a thundering
    /// herd against the token endpoint) and the cache is re-checked after the
    /// lock is acquired. Rotated refresh tokens returned by the endpoint are
    /// stored and preferred over the stale template value on later refreshes.
    /// `token_url` is SSRF-validated inside [`oauth2::refresh_token`].
    async fn obtain_oauth2_token(
        &self,
        token_url: &str,
        client_id: &str,
        client_secret: &str,
        template_refresh_token: &str,
        scopes: Option<&str>,
        cache_key: &str,
    ) -> Result<String, String> {
        let Some(ref tc) = self.token_cache else {
            // No cache — always refresh (rotated refresh tokens cannot be kept).
            let (token, _, _) = oauth2::refresh_token(
                &self.client,
                token_url,
                client_id,
                client_secret,
                template_refresh_token,
                scopes,
            )
            .await?;
            return Ok(token);
        };

        if let Some(cached) = tc.get_token(cache_key) {
            debug!(integration = %self.definition.name, "using cached OAuth2 token");
            return Ok(cached);
        }

        let lock = tc.refresh_lock(cache_key);
        let _guard = lock.lock().await;

        // Another task may have refreshed while we waited for the lock.
        if let Some(cached) = tc.get_token(cache_key) {
            debug!(
                integration = %self.definition.name,
                "using OAuth2 token refreshed by a concurrent task"
            );
            return Ok(cached);
        }

        // Prefer a previously rotated refresh token over the template value.
        let refresh = tc
            .get_refresh_token(cache_key)
            .unwrap_or_else(|| template_refresh_token.to_string());

        let (token, expires_in, rotated_refresh) = oauth2::refresh_token(
            &self.client,
            token_url,
            client_id,
            client_secret,
            &refresh,
            scopes,
        )
        .await?;
        tc.set_token_with_refresh(cache_key, &token, expires_in, rotated_refresh.as_deref());
        Ok(token)
    }
}

#[async_trait]
impl Executor for IntegrationExecutor {
    async fn execute(&self, task: &Task, _ctx: &ExecutionContext) -> ExecuteResult {
        let config = &task.executor_config;

        // Get operation name
        let operation_name = match config.get("operation").and_then(|v| v.as_str()) {
            Some(name) => name,
            None => {
                return ExecuteResult::Failed {
                    error: format!(
                        "missing 'operation' in config. Available operations: {}",
                        self.definition
                            .operations
                            .keys()
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    retryable: false,
                };
            }
        };

        // Look up operation
        let operation = match self.definition.operations.get(operation_name) {
            Some(op) => op,
            None => {
                return ExecuteResult::Failed {
                    error: format!(
                        "operation '{}' not found in {} integration. Available: {}",
                        operation_name,
                        self.definition.name,
                        self.definition
                            .operations
                            .keys()
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    retryable: false,
                };
            }
        };

        // Build interpolation context: all non-reserved config keys become params
        let params = extract_params(config);
        let credential = config.get("credential").cloned();
        let ctx = InterpolationContext { params, credential };

        // Resolve path, percent-encoding interpolated values so a parameter
        // like `../`, `?x=y` or `#frag` cannot rewrite the request target.
        let path = interpolate::interpolate_path(&operation.path, &ctx);
        let url = format!("{}{}", self.definition.base_url, path);

        if let Err(reason) = crate::url_policy::validate_url(&url) {
            return ExecuteResult::Failed {
                error: format!("SSRF blocked: {reason}"),
                retryable: false,
            };
        }

        // Resolve method
        let method = operation.method.to_uppercase();

        debug!(
            task_id = %task.id,
            integration = %self.definition.name,
            operation = %operation_name,
            url = %url,
            method = %method,
            "executing integration request"
        );

        // Validate method
        if !matches!(
            method.as_str(),
            "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD"
        ) {
            return ExecuteResult::Failed {
                error: format!("unsupported HTTP method: {method}"),
                retryable: false,
            };
        }

        let timeout = Duration::from_secs(task.timeout_secs);

        // Collect headers (default + operation-specific)
        let mut headers: Vec<(String, String)> = Vec::new();
        for (key, value_template) in &self.definition.default_headers {
            headers.push((key.clone(), resolve_string(value_template, &ctx)));
        }
        for (key, value_template) in &operation.headers {
            headers.push((key.clone(), resolve_string(value_template, &ctx)));
        }

        // Resolve query parameters
        let mut base_query: Vec<(String, String)> = Vec::new();
        if !operation.query.is_empty() {
            let resolved_query = interpolate::interpolate(
                &Value::Object(
                    operation
                        .query
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                ),
                &ctx,
            );
            if let Value::Object(map) = resolved_query {
                base_query = map
                    .into_iter()
                    .filter(|(_, v)| !v.is_null())
                    .map(|(k, v)| (k, value_as_query_string(&v)))
                    .collect();
            }
        }

        // Resolve auth (stored in template for reuse across pages)
        let resolved_auth = match self.definition.auth {
            Some(AuthConfig::OAuth2 {
                ref token_url,
                ref client_id_template,
                ref client_secret_template,
                ref refresh_token_template,
                ref scopes,
            }) => {
                // Note: token_url is SSRF-validated inside oauth2::refresh_token,
                // immediately before any request is made.

                // Resolve templates
                let client_id = resolve_string(client_id_template, &ctx);
                let client_secret = resolve_string(client_secret_template, &ctx);
                let refresh_token_val = resolve_string(refresh_token_template, &ctx);

                // Build cache key from credential
                let credential_json = config
                    .get("credential")
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                let cache_key = oauth2::cache_key(&self.definition.name, &credential_json);

                match self
                    .obtain_oauth2_token(
                        token_url,
                        &client_id,
                        &client_secret,
                        &refresh_token_val,
                        scopes.as_deref(),
                        &cache_key,
                    )
                    .await
                {
                    Ok(token) => Some(auth::ResolvedAuth::Bearer { token }),
                    Err(e) => {
                        return ExecuteResult::Failed {
                            error: format!("OAuth2 token refresh failed: {e}"),
                            retryable: true,
                        };
                    }
                }
            }
            Some(ref auth_config) => match auth::resolve_auth(auth_config, &ctx) {
                Ok(resolved) => Some(resolved),
                Err(e) => {
                    return ExecuteResult::Failed {
                        error: format!("failed to resolve auth: {e}"),
                        retryable: false,
                    };
                }
            },
            None => None,
        };

        // Build request template for pagination support
        let template = RequestTemplate {
            client: self.client.clone(),
            url: url.clone(),
            method: method.clone(),
            headers,
            base_query,
            timeout,
            auth: resolved_auth,
        };

        // Execute (with pagination if configured)
        let result = if let Some(ref pagination) = operation.pagination {
            pagination::execute_paginated(&template, pagination).await
        } else {
            // Build a single request from the template
            let mut request = template.build_single_request();

            // Apply body for single (non-paginated) requests
            if let Some(ref body_template) = operation.body
                && matches!(method.as_str(), "POST" | "PUT" | "PATCH")
            {
                let resolved_body = interpolate::interpolate(body_template, &ctx);
                request = request.json(&resolved_body);
            }

            // Apply task input as body if no explicit body defined
            if operation.body.is_none()
                && let Some(ref input) = task.input
                && matches!(method.as_str(), "POST" | "PUT" | "PATCH")
            {
                request = request.json(input);
            }

            execute_single(request).await
        };

        // Apply response extraction if configured
        if let ExecuteResult::Success {
            output: Some(ref output),
        } = result
            && let Some(ref response_config) = operation.response
            && !response_config.extract.is_empty()
            && let Some(body) = output.get("body")
        {
            let extracted = response::extract_fields(body, response_config);
            return ExecuteResult::Success {
                output: Some(json!({
                    "status": output.get("status").cloned().unwrap_or(json!(200)),
                    "body": extracted,
                })),
            };
        }

        result
    }
}

/// Execute a single HTTP request and return the result.
async fn execute_single(request: reqwest::RequestBuilder) -> ExecuteResult {
    match request.send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let headers: HashMap<String, String> = resp
                .headers()
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                .collect();
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
                // Try to parse as JSON, fall back to string
                let body_value: Value = serde_json::from_str(&body).unwrap_or(Value::String(body));

                ExecuteResult::Success {
                    output: Some(json!({
                        "status": status,
                        "headers": headers,
                        "body": body_value,
                    })),
                }
            } else {
                ExecuteResult::Failed {
                    error: format!(
                        "HTTP {status}: {}",
                        crate::executor::truncate_body_for_error(&body)
                    ),
                    retryable: status >= 500,
                }
            }
        }
        Err(e) => {
            let retryable = e.is_timeout() || e.is_connect();
            ExecuteResult::Failed {
                error: format!("HTTP request failed: {e}"),
                retryable,
            }
        }
    }
}

/// Extract params from task config: all keys except reserved ones.
fn extract_params(config: &Value) -> HashMap<String, Value> {
    let mut params = HashMap::new();
    if let Value::Object(map) = config {
        for (key, value) in map {
            if !RESERVED_KEYS.contains(&key.as_str()) {
                params.insert(key.clone(), value.clone());
            }
        }
    }
    params
}

/// Resolve a template string to its final string value.
fn resolve_string(template: &str, ctx: &InterpolationContext) -> String {
    match interpolate::interpolate(&Value::String(template.to_string()), ctx) {
        Value::String(s) => s,
        other => other.to_string(),
    }
}

/// Convert a JSON value to a query string parameter value.
fn value_as_query_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

// ── Registry & loader ────────────────────────────────────────────────

/// Load all integration definitions from a directory.
///
/// Returns a Vec of parsed definitions. Files that fail to parse are logged
/// and skipped.
pub fn load_integrations(dir: &Path) -> Vec<IntegrationDef> {
    let mut defs = Vec::new();

    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => {
            warn!(dir = %dir.display(), error = %e, "failed to read integrations directory");
            return defs;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "json") {
            match std::fs::read_to_string(&path) {
                Ok(contents) => match serde_json::from_str::<IntegrationDef>(&contents) {
                    Ok(def) => {
                        debug!(
                            name = %def.name,
                            operations = def.operations.len(),
                            path = %path.display(),
                            "loaded integration definition"
                        );
                        defs.push(def);
                    }
                    Err(e) => {
                        warn!(path = %path.display(), error = %e, "failed to parse integration definition");
                    }
                },
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "failed to read integration file");
                }
            }
        }
    }

    defs
}

/// Register all integration definitions from a directory as named executors.
///
/// Returns the number of integrations registered.
pub fn register_integrations(
    engine: &mut crate::engine::Engine,
    dir: &Path,
    token_cache: Option<Arc<TokenCache>>,
) -> usize {
    let defs = load_integrations(dir);
    let count = defs.len();

    let client = crate::url_policy::ssrf_safe_client();
    for def in defs {
        let name = def.name.clone();
        let executor = match token_cache {
            Some(ref tc) => IntegrationExecutor::with_token_cache(def, client.clone(), tc.clone()),
            None => IntegrationExecutor::with_client(def, client.clone()),
        };
        engine.register_executor(name, Arc::new(executor));
    }

    count
}

/// Fallback executor for inline integration definitions.
///
/// Used as `executor: "api"` with an inline `definition` in the task config:
/// ```json
/// {
///   "executor": "api",
///   "config": {
///     "definition": { "name": "inline", "base_url": "...", "operations": { ... } },
///     "operation": "default",
///     "param1": "value1"
///   }
/// }
/// ```
pub struct InlineApiExecutor {
    client: Client,
}

impl InlineApiExecutor {
    pub fn new() -> Self {
        Self {
            client: crate::url_policy::ssrf_safe_client(),
        }
    }
}

impl Default for InlineApiExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Executor for InlineApiExecutor {
    async fn execute(&self, task: &Task, ctx: &ExecutionContext) -> ExecuteResult {
        let config = &task.executor_config;

        // Parse inline definition
        let def_value = match config.get("definition") {
            Some(v) => v,
            None => {
                return ExecuteResult::Failed {
                    error: "missing 'definition' in api executor config".to_string(),
                    retryable: false,
                };
            }
        };

        let definition: IntegrationDef = match serde_json::from_value(def_value.clone()) {
            Ok(def) => def,
            Err(e) => {
                return ExecuteResult::Failed {
                    error: format!("invalid integration definition: {e}"),
                    retryable: false,
                };
            }
        };

        // Delegate to a temporary IntegrationExecutor
        let executor = IntegrationExecutor::with_client(definition, self.client.clone());
        executor.execute(task, ctx).await
    }
}

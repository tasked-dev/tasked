//! Pagination strategies for integration executors.
//!
//! Since `reqwest::RequestBuilder` is consumed on `.send()`, we use a
//! `RequestTemplate` to rebuild requests for each page.

use super::auth::ResolvedAuth;
use super::definition::PaginationConfig;
use crate::executor::read_response_body;
use crate::types::ExecuteResult;
use reqwest::Client;
use serde_json::{Value, json};
use std::time::Duration;
use tracing::debug;

/// Reusable request template for building paginated requests.
///
/// Captures all the resolved request parameters so we can rebuild
/// the request for each page with modified query params or URL.
pub struct RequestTemplate {
    pub client: Client,
    pub url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub base_query: Vec<(String, String)>,
    pub timeout: Duration,
    /// Pre-resolved auth to apply to each request.
    pub auth: Option<ResolvedAuth>,
}

impl RequestTemplate {
    /// Build a request from this template with no extra params.
    /// Used for non-paginated single requests.
    pub fn build_single_request(&self) -> reqwest::RequestBuilder {
        self.build_request(None, &[])
    }

    /// Build a request from this template, optionally overriding the URL
    /// and adding extra query params.
    fn build_request(
        &self,
        url_override: Option<&str>,
        extra_query: &[(String, String)],
    ) -> reqwest::RequestBuilder {
        let url = url_override.unwrap_or(&self.url);
        let mut request = match self.method.as_str() {
            "GET" => self.client.get(url),
            "POST" => self.client.post(url),
            "PUT" => self.client.put(url),
            "PATCH" => self.client.patch(url),
            "DELETE" => self.client.delete(url),
            "HEAD" => self.client.head(url),
            _ => self.client.get(url),
        };

        request = request.timeout(self.timeout);

        for (key, value) in &self.headers {
            request = request.header(key.as_str(), value.as_str());
        }

        if let Some(ref auth) = self.auth {
            request = auth.apply(request);
        }

        // Combine base query with extra params
        let all_query: Vec<(&str, &str)> = self
            .base_query
            .iter()
            .chain(extra_query.iter())
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        if !all_query.is_empty() {
            request = request.query(&all_query);
        }

        request
    }
}

/// Hard upper bound for `max_pages` to prevent runaway pagination.
const MAX_PAGES_LIMIT: u32 = 100;

/// Execute a paginated request, collecting results from all pages.
pub async fn execute_paginated(
    template: &RequestTemplate,
    pagination: &PaginationConfig,
) -> ExecuteResult {
    match pagination {
        PaginationConfig::LinkHeader { max_pages } => {
            paginate_link_header(template, (*max_pages).min(MAX_PAGES_LIMIT)).await
        }
        PaginationConfig::Cursor {
            param,
            response_path,
            max_pages,
        } => {
            paginate_cursor(
                template,
                param,
                response_path,
                (*max_pages).min(MAX_PAGES_LIMIT),
            )
            .await
        }
        PaginationConfig::Offset {
            param,
            limit_param,
            limit,
            max_pages,
        } => {
            paginate_offset(
                template,
                param,
                limit_param,
                *limit,
                (*max_pages).min(MAX_PAGES_LIMIT),
            )
            .await
        }
    }
}

/// Link header pagination: follow `rel="next"` URLs.
async fn paginate_link_header(template: &RequestTemplate, max_pages: u32) -> ExecuteResult {
    let mut all_items: Vec<Value> = Vec::new();
    let mut next_url: Option<String> = None;
    let mut pages = 0u32;
    let mut last_status = 200u16;

    loop {
        if pages >= max_pages {
            break;
        }

        let request = template.build_request(next_url.as_deref(), &[]);
        let resp = match request.send().await {
            Ok(r) => r,
            Err(e) => {
                if pages > 0 {
                    // We got some pages, return what we have
                    break;
                }
                let retryable = e.is_timeout() || e.is_connect();
                return ExecuteResult::Failed {
                    error: format!("HTTP request failed: {e}"),
                    retryable,
                };
            }
        };

        last_status = resp.status().as_u16();

        // Parse Link header for next URL before consuming the body
        next_url = resp
            .headers()
            .get("link")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_link_next);

        // Validate next URL against SSRF policy
        if let Some(ref url) = next_url
            && crate::url_policy::validate_url(url).is_err()
        {
            next_url = None;
        }

        let body = match read_response_body(resp).await {
            Ok(b) => b,
            Err(e) => {
                if pages > 0 {
                    break;
                }
                return ExecuteResult::Failed {
                    error: e,
                    retryable: false,
                };
            }
        };

        if !(200..300).contains(&last_status) {
            if pages > 0 {
                break; // Return what we have
            }
            return ExecuteResult::Failed {
                error: format!("HTTP {last_status}: {body}"),
                retryable: last_status >= 500,
            };
        }

        let body_value: Value = serde_json::from_str(&body).unwrap_or(Value::String(body));

        collect_items(&body_value, &mut all_items);
        pages += 1;

        debug!(pages, items = all_items.len(), "paginated: fetched page");

        if next_url.is_none() {
            break;
        }
    }

    build_paginated_result(last_status, pages, all_items)
}

/// Cursor pagination: extract cursor from response, send as query param.
async fn paginate_cursor(
    template: &RequestTemplate,
    param: &str,
    response_path: &str,
    max_pages: u32,
) -> ExecuteResult {
    let mut all_items: Vec<Value> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0u32;
    let mut last_status = 200u16;

    loop {
        if pages >= max_pages {
            break;
        }

        let extra_query: Vec<(String, String)> = cursor
            .as_ref()
            .map(|c| vec![(param.to_string(), c.clone())])
            .unwrap_or_default();

        let request = template.build_request(None, &extra_query);
        let resp = match request.send().await {
            Ok(r) => r,
            Err(e) => {
                if pages > 0 {
                    break;
                }
                let retryable = e.is_timeout() || e.is_connect();
                return ExecuteResult::Failed {
                    error: format!("HTTP request failed: {e}"),
                    retryable,
                };
            }
        };

        last_status = resp.status().as_u16();
        let body = match read_response_body(resp).await {
            Ok(b) => b,
            Err(e) => {
                if pages > 0 {
                    break;
                }
                return ExecuteResult::Failed {
                    error: e,
                    retryable: false,
                };
            }
        };

        if !(200..300).contains(&last_status) {
            if pages > 0 {
                break;
            }
            return ExecuteResult::Failed {
                error: format!("HTTP {last_status}: {body}"),
                retryable: last_status >= 500,
            };
        }

        let body_value: Value = serde_json::from_str(&body).unwrap_or(Value::String(body));

        // Extract cursor for next page
        cursor = navigate_json_path(&body_value, response_path).and_then(|v| match v {
            Value::String(s) if !s.is_empty() => Some(s),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        });

        collect_items(&body_value, &mut all_items);
        pages += 1;

        debug!(pages, items = all_items.len(), cursor = ?cursor, "paginated: fetched page");

        if cursor.is_none() {
            break;
        }
    }

    build_paginated_result(last_status, pages, all_items)
}

/// Offset pagination: increment offset by limit each page.
async fn paginate_offset(
    template: &RequestTemplate,
    param: &str,
    limit_param: &str,
    limit: u32,
    max_pages: u32,
) -> ExecuteResult {
    let mut all_items: Vec<Value> = Vec::new();
    let mut offset = 0u32;
    let mut pages = 0u32;
    let mut last_status = 200u16;

    loop {
        if pages >= max_pages {
            break;
        }

        let extra_query = vec![
            (param.to_string(), offset.to_string()),
            (limit_param.to_string(), limit.to_string()),
        ];

        let request = template.build_request(None, &extra_query);
        let resp = match request.send().await {
            Ok(r) => r,
            Err(e) => {
                if pages > 0 {
                    break;
                }
                let retryable = e.is_timeout() || e.is_connect();
                return ExecuteResult::Failed {
                    error: format!("HTTP request failed: {e}"),
                    retryable,
                };
            }
        };

        last_status = resp.status().as_u16();
        let body = match read_response_body(resp).await {
            Ok(b) => b,
            Err(e) => {
                if pages > 0 {
                    break;
                }
                return ExecuteResult::Failed {
                    error: e,
                    retryable: false,
                };
            }
        };

        if !(200..300).contains(&last_status) {
            if pages > 0 {
                break;
            }
            return ExecuteResult::Failed {
                error: format!("HTTP {last_status}: {body}"),
                retryable: last_status >= 500,
            };
        }

        let body_value: Value = serde_json::from_str(&body).unwrap_or(Value::String(body));

        let page_item_count = count_items(&body_value);
        collect_items(&body_value, &mut all_items);
        pages += 1;
        offset += limit;

        debug!(
            pages,
            items = all_items.len(),
            offset,
            "paginated: fetched page"
        );

        // If we got fewer items than the limit, we've reached the last page
        if page_item_count < limit as usize {
            break;
        }
    }

    build_paginated_result(last_status, pages, all_items)
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Parse a Link header and extract the URL for `rel="next"`.
///
/// Handles the standard format: `<url>; rel="next", <url>; rel="prev"`
fn parse_link_next(header: &str) -> Option<String> {
    for part in header.split(',') {
        let part = part.trim();
        if let Some(url_end) = part.find('>')
            && part.starts_with('<')
        {
            let url = &part[1..url_end];
            let rest = &part[url_end + 1..];
            if rest.contains("rel=\"next\"") || rest.contains("rel='next'") {
                return Some(url.to_string());
            }
        }
    }
    None
}

/// Navigate a JSON value by a dot-separated path.
fn navigate_json_path(value: &Value, path: &str) -> Option<Value> {
    let segments: Vec<&str> = path.split('.').collect();
    let mut current = value;
    for segment in &segments {
        current = match current {
            Value::Object(map) => map.get(*segment)?,
            Value::Array(arr) => {
                let idx: usize = segment.parse().ok()?;
                arr.get(idx)?
            }
            _ => return None,
        };
    }
    Some(current.clone())
}

/// Collect items from a response body into the accumulator.
///
/// If the body is an array, extend with its items.
/// If the body is an object with exactly one array field, extend with that array's items.
/// Otherwise, push the entire body as a single item.
fn collect_items(body: &Value, items: &mut Vec<Value>) {
    match body {
        Value::Array(arr) => items.extend(arr.iter().cloned()),
        Value::Object(map) => {
            // Look for a single array field (common: {"items": [...], "total": 100})
            let arrays: Vec<(&String, &Vec<Value>)> = map
                .iter()
                .filter_map(|(k, v)| v.as_array().map(|a| (k, a)))
                .collect();
            if arrays.len() == 1 {
                items.extend(arrays[0].1.iter().cloned());
            } else {
                items.push(body.clone());
            }
        }
        _ => items.push(body.clone()),
    }
}

/// Count items in a response body (for offset pagination end detection).
fn count_items(body: &Value) -> usize {
    match body {
        Value::Array(arr) => arr.len(),
        Value::Object(map) => {
            let arrays: Vec<&Vec<Value>> = map.values().filter_map(|v| v.as_array()).collect();
            if arrays.len() == 1 {
                arrays[0].len()
            } else {
                1
            }
        }
        _ => 1,
    }
}

/// Build the final paginated result.
fn build_paginated_result(status: u16, pages: u32, items: Vec<Value>) -> ExecuteResult {
    ExecuteResult::Success {
        output: Some(json!({
            "status": status,
            "pages": pages,
            "body": Value::Array(items),
        })),
    }
}

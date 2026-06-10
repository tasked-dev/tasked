//! Second-pass interpolation for integration definition templates.
//!
//! The engine's first pass resolves `${tasks.*}` and `${secrets.*}` in the task config.
//! This module performs a second pass within the integration executor, resolving
//! `${params.*}` and `${credential}` / `${credential.*}` in the definition templates.
//!
//! `${params.owner}` resolves from the task's config (all keys except reserved ones).
//! `${credential}` resolves to the raw credential string.
//! `${credential.field}` resolves by parsing the credential as JSON and navigating the path.

use serde_json::Value;
use std::collections::HashMap;

/// Context for resolving integration definition templates.
pub struct InterpolationContext {
    /// Flat parameter map from the task config.
    pub params: HashMap<String, Value>,
    /// Resolved credential value (may be a plain string or JSON).
    pub credential: Option<Value>,
}

/// Interpolate all `${params.*}` and `${credential}` references in a JSON value tree.
pub fn interpolate(value: &Value, ctx: &InterpolationContext) -> Value {
    match value {
        Value::String(s) => interpolate_string(s, ctx),
        Value::Array(arr) => Value::Array(arr.iter().map(|v| interpolate(v, ctx)).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), interpolate(v, ctx)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Interpolate a single string. If the entire string is a single `${...}` reference,
/// return the resolved value directly (preserving JSON type). If mixed with other text,
/// substitute as strings.
fn interpolate_string(s: &str, ctx: &InterpolationContext) -> Value {
    if !s.contains("${") {
        return Value::String(s.to_string());
    }

    // Check if the entire string is exactly one variable reference
    let trimmed = s.trim();
    if trimmed.starts_with("${") && trimmed.ends_with('}') && count_refs(trimmed) == 1 {
        if let Some(resolved) = resolve_ref(&trimmed[2..trimmed.len() - 1], ctx) {
            return resolved;
        }
        return Value::String(s.to_string());
    }

    // Mixed content: replace each ${...} with its string representation
    let result = replace_refs(s, ctx);
    Value::String(result)
}

/// Count `${...}` references in a string.
fn count_refs(s: &str) -> usize {
    let mut count = 0;
    let mut depth = 0;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'$' && bytes[i + 1] == b'{' {
            if depth == 0 {
                count += 1;
            }
            depth += 1;
            i += 2;
        } else if bytes[i] == b'}' && depth > 0 {
            depth -= 1;
            i += 1;
        } else {
            i += 1;
        }
    }
    count
}

/// Replace all `${...}` references with their string representations.
fn replace_refs(s: &str, ctx: &InterpolationContext) -> String {
    replace_refs_with(s, ctx, value_to_string)
}

/// Interpolate a URL path template, percent-encoding each resolved value so
/// interpolated params cannot rewrite the request target (e.g. `../x`,
/// `?admin=1` or `#frag`). Literal text in the template (including the `/`
/// separators) is left untouched, and already-encoded `%XX` escapes inside
/// values are preserved, so nothing is double-encoded.
pub fn interpolate_path(s: &str, ctx: &InterpolationContext) -> String {
    replace_refs_with(s, ctx, |v| encode_path_segment(&value_to_string(v)))
}

/// Replace all `${...}` references, transforming each resolved value with
/// `render` before substitution. Unresolved references are kept verbatim.
fn replace_refs_with(
    s: &str,
    ctx: &InterpolationContext,
    render: impl Fn(&Value) -> String,
) -> String {
    let mut result = String::with_capacity(s.len());
    let mut rest = s;

    while let Some(start) = rest.find("${") {
        result.push_str(&rest[..start]);
        let after_open = &rest[start + 2..];
        if let Some(end) = after_open.find('}') {
            let ref_path = &after_open[..end];
            if let Some(resolved) = resolve_ref(ref_path, ctx) {
                result.push_str(&render(&resolved));
            } else {
                // Unresolved: keep original
                result.push_str(&rest[start..start + 2 + end + 1]);
            }
            rest = &after_open[end + 1..];
        } else {
            result.push_str(&rest[start..]);
            rest = "";
        }
    }
    result.push_str(rest);
    result
}

/// Percent-encode characters that are not valid in a URL path segment
/// (RFC 3986 `pchar`: unreserved, sub-delims, `:` and `@`). In particular
/// `/`, `?` and `#` are encoded so a value cannot alter the URL structure.
/// An existing `%XX` escape is passed through unchanged to avoid
/// double-encoding values that are already percent-encoded.
fn encode_path_segment(s: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        let is_pchar = b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'-' | b'.'
                    | b'_'
                    | b'~'
                    | b'!'
                    | b'$'
                    | b'&'
                    | b'\''
                    | b'('
                    | b')'
                    | b'*'
                    | b'+'
                    | b','
                    | b';'
                    | b'='
                    | b':'
                    | b'@'
            );
        let is_existing_escape = b == b'%'
            && i + 2 < bytes.len()
            && bytes[i + 1].is_ascii_hexdigit()
            && bytes[i + 2].is_ascii_hexdigit();
        if is_pchar || is_existing_escape {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        }
        i += 1;
    }
    out
}

/// Resolve a reference path.
///
/// - `params.<key>` — look up in the params map
/// - `params.<key>.<path>` — look up in params map, then navigate JSON path
/// - `credential` — the raw credential value
/// - `credential.<path>` — navigate into the credential (if it's JSON)
fn resolve_ref(path: &str, ctx: &InterpolationContext) -> Option<Value> {
    let parts: Vec<&str> = path.split('.').collect();

    if parts.is_empty() {
        return None;
    }

    if parts[0] == "params" && parts.len() >= 2 {
        let value = ctx.params.get(parts[1])?;
        return navigate_path(value, &parts[2..]);
    }

    if parts[0] == "credential" {
        let cred = ctx.credential.as_ref()?;
        if parts.len() == 1 {
            return Some(cred.clone());
        }
        // Navigate into credential (must be an object)
        return navigate_path(cred, &parts[1..]);
    }

    None
}

/// Navigate a JSON value by dot-separated path segments.
/// Delegates to the shared implementation in [`super::response`].
fn navigate_path(value: &Value, segments: &[&str]) -> Option<Value> {
    super::response::navigate_segments(value, segments.iter().copied())
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx_with(params: &[(&str, Value)]) -> InterpolationContext {
        InterpolationContext {
            params: params
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            credential: None,
        }
    }

    #[test]
    fn interpolate_path_encodes_traversal_and_query() {
        let ctx = ctx_with(&[("owner", json!("../admin")), ("repo", json!("x?y=z#f"))]);
        let path = interpolate_path("/repos/${params.owner}/${params.repo}/issues", &ctx);
        // '/' '?' '#' are encoded; '=' is a sub-delim and stays as-is.
        assert_eq!(path, "/repos/..%2Fadmin/x%3Fy=z%23f/issues");
    }

    #[test]
    fn interpolate_path_keeps_safe_chars_and_literals() {
        let ctx = ctx_with(&[("owner", json!("my-org_1.2~ok")), ("n", json!(42))]);
        let path = interpolate_path("/repos/${params.owner}/items/${params.n}", &ctx);
        assert_eq!(path, "/repos/my-org_1.2~ok/items/42");
    }

    #[test]
    fn interpolate_path_does_not_double_encode() {
        let ctx = ctx_with(&[("name", json!("a%20b"))]);
        let path = interpolate_path("/files/${params.name}", &ctx);
        assert_eq!(path, "/files/a%20b");
        // A bare '%' that is not a valid escape IS encoded.
        let ctx = ctx_with(&[("name", json!("50%"))]);
        assert_eq!(
            interpolate_path("/files/${params.name}", &ctx),
            "/files/50%25"
        );
    }

    #[test]
    fn interpolate_path_keeps_unresolved_refs() {
        let ctx = ctx_with(&[]);
        let path = interpolate_path("/x/${params.missing}", &ctx);
        assert_eq!(path, "/x/${params.missing}");
    }

    #[test]
    fn interpolate_resolves_params_and_credential_paths() {
        let mut ctx = ctx_with(&[("obj", json!({"inner": [1, 2, 3]}))]);
        ctx.credential = Some(json!({"token": "secret"}));
        assert_eq!(interpolate(&json!("${params.obj.inner.1}"), &ctx), json!(2));
        assert_eq!(
            interpolate(&json!("${credential.token}"), &ctx),
            json!("secret")
        );
    }
}

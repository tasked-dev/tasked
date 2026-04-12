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
    let mut result = String::with_capacity(s.len());
    let mut rest = s;

    while let Some(start) = rest.find("${") {
        result.push_str(&rest[..start]);
        let after_open = &rest[start + 2..];
        if let Some(end) = after_open.find('}') {
            let ref_path = &after_open[..end];
            if let Some(resolved) = resolve_ref(ref_path, ctx) {
                result.push_str(&value_to_string(&resolved));
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
fn navigate_path(value: &Value, segments: &[&str]) -> Option<Value> {
    let mut current = value;
    for &segment in segments {
        current = match current {
            Value::Object(map) => map.get(segment)?,
            Value::Array(arr) => {
                let idx: usize = segment.parse().ok()?;
                arr.get(idx)?
            }
            _ => return None,
        };
    }
    Some(current.clone())
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

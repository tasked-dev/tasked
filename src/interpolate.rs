//! Variable interpolation for executor configs.
//!
//! Replaces `${tasks.<task_id>.output}` and `${tasks.<task_id>.output.<json_path>}`
//! patterns in string values within a `serde_json::Value` tree.
//!
//! # Examples
//!
//! Given task "fetch" produced output `{"body": {"count": 42}}`:
//!
//! - `${tasks.fetch.output}` → the entire output JSON (inlined as string)
//! - `${tasks.fetch.output.body}` → `{"count": 42}` (as string)
//! - `${tasks.fetch.output.body.count}` → `42`

use serde_json::Value;
use std::collections::HashMap;
use std::path::{Component, Path};

use crate::types::TaskId;

/// A map from dependency task ID to its output value.
pub type TaskOutputs = HashMap<TaskId, Option<Value>>;

/// A map from secret name to its resolved value.
pub type Secrets = HashMap<String, String>;

/// Resolve secrets from a queue's secret configuration.
///
/// For each named secret, reads the value from either an environment variable
/// or a file path, returning a map of secret name to resolved string value.
pub fn resolve_secrets(secrets_config: &HashMap<String, crate::types::SecretRef>) -> Secrets {
    let mut resolved = HashMap::new();

    for (name, secret_ref) in secrets_config {
        if let Some(ref env_var) = secret_ref.env {
            if let Ok(val) = std::env::var(env_var) {
                resolved.insert(name.clone(), val);
            }
        } else if let Some(ref file_path) = secret_ref.file {
            let path = Path::new(file_path);
            if path.components().any(|c| matches!(c, Component::ParentDir)) {
                tracing::warn!(
                    file_path,
                    "secret file path rejected: contains parent directory component"
                );
            } else if !path.symlink_metadata().is_ok_and(|m| m.is_file()) {
                tracing::warn!(
                    file_path,
                    "secret file path rejected: not a regular file or is a symlink"
                );
            } else if let Ok(val) = std::fs::read_to_string(path) {
                resolved.insert(name.clone(), val.trim().to_string());
            } else {
                tracing::warn!(file_path, "failed to read secret file");
            }
        }
    }
    resolved
}

/// Interpolate variable references in a JSON value tree.
///
/// Walks the tree and replaces `${tasks.<id>.output...}` patterns in string
/// values. Non-string values are returned unchanged.
pub fn interpolate(value: &Value, outputs: &TaskOutputs, secrets: &Secrets) -> Value {
    match value {
        Value::String(s) => interpolate_string(s, outputs, secrets),
        Value::Array(arr) => Value::Array(
            arr.iter()
                .map(|v| interpolate(v, outputs, secrets))
                .collect(),
        ),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), interpolate(v, outputs, secrets)))
                .collect(),
        ),
        // Numbers, bools, nulls pass through unchanged
        other => other.clone(),
    }
}

/// Interpolate a single string. If the entire string is a single `${...}` reference,
/// return the resolved value directly (preserving its JSON type). If the string
/// contains references mixed with other text, substitute as strings.
fn interpolate_string(s: &str, outputs: &TaskOutputs, secrets: &Secrets) -> Value {
    // Fast path: no interpolation needed
    if !s.contains("${") {
        return Value::String(s.to_string());
    }

    // Check if the entire string is exactly one variable reference.
    // Exact match only: `" ${ref} "` is mixed content (the surrounding
    // whitespace is meaningful), not a typed whole-string reference.
    if let Some(ref_path) = whole_string_ref(s) {
        if let Some(resolved) = resolve_ref(ref_path, outputs, secrets) {
            return resolved;
        }
        // If resolution fails, return original string
        return Value::String(s.to_string());
    }

    // Mixed content: replace each ${...} with its string representation
    let result = replace_refs(s, outputs, secrets);
    Value::String(result)
}

/// If the string is exactly one `${...}` reference, return the inner path.
/// Uses the same non-nesting rule as [`replace_refs`] (a reference ends at
/// the first `}`), so the two parsers can never disagree.
fn whole_string_ref(s: &str) -> Option<&str> {
    let inner = s.strip_prefix("${")?.strip_suffix('}')?;
    if inner.contains("${") || inner.contains('}') {
        return None;
    }
    Some(inner)
}

/// Replace all `${...}` references with their string representations.
pub fn replace_refs(s: &str, outputs: &TaskOutputs, secrets: &Secrets) -> String {
    let mut result = String::with_capacity(s.len());
    let mut rest = s;

    while let Some(start) = rest.find("${") {
        result.push_str(&rest[..start]);
        let after_open = &rest[start + 2..];
        if let Some(end) = after_open.find('}') {
            let ref_path = &after_open[..end];
            if let Some(resolved) = resolve_ref(ref_path, outputs, secrets) {
                result.push_str(&value_to_string(&resolved));
            } else {
                // Unresolved: keep original
                result.push_str(&rest[start..start + 2 + end + 1]);
            }
            rest = &after_open[end + 1..];
        } else {
            // No closing brace: keep rest as-is
            result.push_str(&rest[start..]);
            rest = "";
        }
    }
    result.push_str(rest);
    result
}

/// Resolve a reference path like `tasks.fetch.output.body.count` or `secrets.MY_KEY`.
fn resolve_ref(path: &str, outputs: &TaskOutputs, secrets: &Secrets) -> Option<Value> {
    let parts: Vec<&str> = path.split('.').collect();

    // Handle secrets.<name> references
    if parts.len() == 2 && parts[0] == "secrets" {
        return secrets.get(parts[1]).map(|v| Value::String(v.clone()));
    }

    // Must start with "tasks"
    if parts.len() < 3 || parts[0] != "tasks" {
        return None;
    }

    let task_id = TaskId::from(parts[1]);

    // parts[2] must be "output"
    if parts[2] != "output" {
        return None;
    }

    let output = outputs.get(&task_id)?.as_ref()?;

    // Navigate remaining path segments
    let mut current = output;
    for &segment in &parts[3..] {
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

/// Convert a JSON value to a string for embedding in mixed-content strings.
fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => "null".to_string(),
        // For numbers, bools, objects, arrays: use compact JSON
        other => other.to_string(),
    }
}

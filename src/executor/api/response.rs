//! Response extraction for integration executors.
//!
//! Given a `ResponseConfig` with an `extract` map, pulls specific fields from
//! an API response body using dot-separated JSON paths.
//!
//! # Example
//!
//! Given response body:
//! ```json
//! {"user": {"name": "Alice", "id": 42}, "status": "active"}
//! ```
//!
//! And extract config `{"name": "user.name", "user_id": "user.id"}`, produces:
//! ```json
//! {"name": "Alice", "user_id": 42}
//! ```

use super::definition::ResponseConfig;
use serde_json::Value;

/// Extract specific fields from a response body according to the extraction config.
///
/// Returns a new JSON object with the extracted fields. If a path doesn't resolve,
/// the field is omitted from the output (not set to null).
pub fn extract_fields(body: &Value, config: &ResponseConfig) -> Value {
    if config.extract.is_empty() {
        return body.clone();
    }

    let mut result = serde_json::Map::new();
    for (output_key, json_path) in &config.extract {
        if let Some(value) = navigate_path(body, json_path) {
            result.insert(output_key.clone(), value);
        }
    }
    Value::Object(result)
}

/// Navigate a JSON value by a dot-separated path string.
///
/// Supports object field access and array index access:
/// - `"user.name"` → `body["user"]["name"]`
/// - `"items.0.id"` → `body["items"][0]["id"]`
fn navigate_path(value: &Value, path: &str) -> Option<Value> {
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


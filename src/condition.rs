//! Condition evaluation using a sandboxed Rhai scripting engine.
//!
//! Task outputs and secrets are injected as Rhai scope variables rather than
//! string-interpolated into the expression. This prevents injection attacks
//! where attacker-controlled task output could break out of string context
//! and execute arbitrary Rhai code (e.g., via `eval()`).
//!
//! # Condition syntax
//!
//! Conditions reference task outputs and secrets as native variables:
//!
//! ```text
//! tasks.fetch.output.status == "ok"
//! tasks.fetch.output.body.count > 0
//! tasks.fetch.output.items[0] == "first"
//! tasks["my-task"].output.exit_code == 0   // hyphenated task IDs
//! secrets.API_KEY != ""
//! ```
//!
//! Standard boolean operators (`&&`, `||`, `!`), comparisons (`==`, `!=`,
//! `<`, `>`, `<=`, `>=`), arithmetic, and string methods (`.contains()`,
//! `.starts_with()`, `.ends_with()`, `.len()`) are all available.

use crate::interpolate::{Secrets, TaskOutputs};
use rhai::packages::{
    ArithmeticPackage, BasicArrayPackage, BasicMapPackage, BasicStringPackage, LanguageCorePackage,
    LogicPackage, MoreStringPackage, Package,
};
use rhai::{Dynamic, Engine as RhaiEngine, Map, Scope};
use std::time::Duration;

/// Wall-clock timeout for condition evaluation.
const EVAL_TIMEOUT: Duration = Duration::from_millis(100);

/// Build a sandboxed Rhai engine with only the packages we need.
///
/// Uses `Engine::new_raw()` which starts with zero capabilities — no
/// `FileModuleResolver`, no standard library, no I/O. We then register
/// only arithmetic, logic, and basic string operations.
fn make_engine() -> RhaiEngine {
    let mut engine = RhaiEngine::new_raw();

    // Register only the packages we actually need for condition expressions.
    // This structurally eliminates filesystem access (no FileModuleResolver,
    // no StandardPackage with its I/O capabilities).
    engine.register_global_module(LanguageCorePackage::new().as_shared_module());
    engine.register_global_module(ArithmeticPackage::new().as_shared_module());
    engine.register_global_module(LogicPackage::new().as_shared_module());
    engine.register_global_module(BasicStringPackage::new().as_shared_module());
    engine.register_global_module(MoreStringPackage::new().as_shared_module());
    engine.register_global_module(BasicArrayPackage::new().as_shared_module());
    engine.register_global_module(BasicMapPackage::new().as_shared_module());

    // Disable dangerous built-ins that bypass eval_expression restrictions
    engine.disable_symbol("eval"); // full script execution escape
    engine.disable_symbol("print"); // side-effect: writes to stdout
    engine.disable_symbol("debug"); // side-effect: writes to stderr

    // Resource limits to prevent DoS
    engine.set_max_operations(10_000);
    engine.set_max_call_levels(16);
    engine.set_max_string_size(10_000);
    engine.set_max_array_size(1_000);
    engine.set_max_map_size(1_000);

    engine
}

/// Convert a serde_json::Value into a Rhai Dynamic value.
fn json_to_dynamic(value: &serde_json::Value) -> Dynamic {
    match value {
        serde_json::Value::Null => Dynamic::UNIT,
        serde_json::Value::Bool(b) => Dynamic::from(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Dynamic::from(i)
            } else if let Some(f) = n.as_f64() {
                Dynamic::from(f)
            } else {
                Dynamic::UNIT
            }
        }
        serde_json::Value::String(s) => Dynamic::from(s.clone()),
        serde_json::Value::Array(arr) => {
            let rhai_arr: Vec<Dynamic> = arr.iter().map(json_to_dynamic).collect();
            Dynamic::from(rhai_arr)
        }
        serde_json::Value::Object(map) => {
            let mut rhai_map = Map::new();
            for (k, v) in map {
                rhai_map.insert(k.as_str().into(), json_to_dynamic(v));
            }
            Dynamic::from(rhai_map)
        }
    }
}

/// Build a Rhai Scope with task outputs and secrets as variables.
///
/// Creates:
/// - `tasks`: a map of `{ task_id: { output: <dynamic> } }`
/// - `secrets`: a map of `{ secret_name: "value" }`
fn build_scope(outputs: &TaskOutputs, secrets: &Secrets) -> Scope<'static> {
    let mut scope = Scope::new();

    // Build tasks map: { "task_id": { "output": <value> } }
    let mut tasks_map = Map::new();
    for (task_id, output) in outputs {
        let mut task_entry = Map::new();
        match output {
            Some(val) => task_entry.insert("output".into(), json_to_dynamic(val)),
            None => task_entry.insert("output".into(), Dynamic::UNIT),
        };
        tasks_map.insert(task_id.as_str().into(), Dynamic::from(task_entry));
    }
    scope.push("tasks", tasks_map);

    // Build secrets map: { "name": "value" }
    let mut secrets_map = Map::new();
    for (name, val) in secrets {
        secrets_map.insert(name.as_str().into(), Dynamic::from(val.clone()));
    }
    scope.push("secrets", secrets_map);

    scope
}

/// Synchronous inner evaluation — runs on a blocking thread.
fn evaluate_sync(expr: &str, outputs: &TaskOutputs, secrets: &Secrets) -> Result<bool, String> {
    let engine = make_engine();
    let mut scope = build_scope(outputs, secrets);
    engine
        .eval_expression_with_scope::<bool>(&mut scope, expr)
        .map_err(|e| format!("{e}"))
}

/// Evaluate a condition expression with task outputs and secrets injected
/// as Rhai scope variables.
///
/// Task outputs are available as `tasks.<id>.output.<path>` and secrets
/// as `secrets.<name>`. Values are passed as native Rhai types, never
/// string-interpolated, preventing injection attacks.
///
/// The evaluation runs on a blocking thread with a wall-clock timeout
/// (`EVAL_TIMEOUT`) to prevent runaway expressions from stalling the
/// async executor.
///
/// Returns true/false or an error for invalid/timed-out expressions.
pub async fn evaluate(
    expr: &str,
    outputs: &TaskOutputs,
    secrets: &Secrets,
) -> Result<bool, String> {
    let expr = expr.to_owned();
    let outputs = outputs.clone();
    let secrets = secrets.clone();

    let result = tokio::time::timeout(
        EVAL_TIMEOUT,
        tokio::task::spawn_blocking(move || evaluate_sync(&expr, &outputs, &secrets)),
    )
    .await;

    match result {
        Ok(Ok(inner)) => inner,
        Ok(Err(join_err)) => Err(format!("condition eval panicked: {join_err}")),
        Err(_elapsed) => Err("condition evaluation timed out".to_string()),
    }
}


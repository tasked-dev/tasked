use crate::types::{ExecuteResult, Task, TaskDef};
use async_trait::async_trait;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

use super::{ExecutionContext, Executor};

/// Spawn executor — delegates to an inner executor and parses its output as generated tasks.
///
/// Config format:
/// ```json
/// {
///     "executor": "shell",
///     "config": { "command": "./discover.sh" }
/// }
/// ```
///
/// The inner executor can be any registered executor (shell, http, container, agent, etc.).
/// The spawn executor runs it, extracts text output (stdout for shell/container, body for
/// http), and parses it as a JSON array of [`TaskDef`] objects.
///
/// **Shorthand**: if no `executor` field is present, defaults to `"shell"` and the
/// spawn config itself is passed as the inner config:
/// ```json
/// { "command": "./discover.sh" }
/// ```
///
/// # Security
///
/// Generated tasks run with the same privileges as the server process.
/// There is no sandboxing — a generated task with `"executor": "shell"`
/// can run arbitrary commands. Do not use the spawn executor with
/// untrusted input unless tasks are isolated via the container executor.
/// The `max_spawn_depth` engine config limits recursive spawning.
pub struct SpawnExecutor {
    executors: HashMap<String, Arc<dyn Executor>>,
}

impl SpawnExecutor {
    /// Create a spawn executor with access to the engine's executor registry.
    pub fn new(executors: HashMap<String, Arc<dyn Executor>>) -> Self {
        Self { executors }
    }
}

#[async_trait]
impl Executor for SpawnExecutor {
    async fn execute(&self, task: &Task, ctx: &ExecutionContext) -> ExecuteResult {
        // Determine inner executor name and config
        let (inner_name, inner_config) = match task.executor_config.get("executor") {
            Some(name) => {
                let name = match name.as_str() {
                    Some(n) => n.to_string(),
                    None => {
                        return ExecuteResult::Failed {
                            error: "'executor' field must be a string".to_string(),
                            retryable: false,
                        };
                    }
                };
                let config = task
                    .executor_config
                    .get("config")
                    .cloned()
                    .unwrap_or(json!({}));
                (name, config)
            }
            None => {
                // Shorthand: default to shell, use spawn config as inner config
                if task.executor_config.get("command").is_some() {
                    ("shell".to_string(), task.executor_config.clone())
                } else {
                    return ExecuteResult::Failed {
                        error: "spawn config must have 'executor' field or 'command' shorthand"
                            .to_string(),
                        retryable: false,
                    };
                }
            }
        };

        // Prevent spawning yourself (infinite recursion)
        if inner_name == "spawn" {
            return ExecuteResult::Failed {
                error: "spawn executor cannot delegate to itself".to_string(),
                retryable: false,
            };
        }

        // Look up the inner executor
        let inner_executor = match self.executors.get(&inner_name) {
            Some(e) => e.clone(),
            None => {
                return ExecuteResult::Failed {
                    error: format!("unknown inner executor '{inner_name}'"),
                    retryable: false,
                };
            }
        };

        debug!(
            task_id = %task.id,
            inner_executor = %inner_name,
            "executing spawn with inner executor"
        );

        // Build a synthetic task with the inner config
        let inner_task = Task {
            executor_type: inner_name.clone(),
            executor_config: inner_config,
            ..task.clone()
        };

        // Run the inner executor
        let result = inner_executor.execute(&inner_task, ctx).await;

        // Extract text output and parse as tasks
        match result {
            ExecuteResult::Success {
                output: Some(ref output),
            } => {
                // Try stdout first (shell, container), then body (http)
                let text = output
                    .get("stdout")
                    .and_then(|v| v.as_str())
                    .or_else(|| output.get("body").and_then(|v| v.as_str()))
                    .unwrap_or("");

                Self::parse_spawn_output(text, output, &task.id)
            }
            ExecuteResult::Success { output: None } => ExecuteResult::Failed {
                error: format!("inner executor '{inner_name}' produced no output"),
                retryable: false,
            },
            // Pass through failures
            other => other,
        }
    }
}

impl SpawnExecutor {
    /// Parse text as a JSON array of TaskDef.
    fn parse_spawn_output(
        text: &str,
        raw_output: &serde_json::Value,
        task_id: &crate::types::TaskId,
    ) -> ExecuteResult {
        match serde_json::from_str::<Vec<TaskDef>>(text.trim()) {
            Ok(tasks) => {
                debug!(
                    task_id = %task_id,
                    count = tasks.len(),
                    "spawn generated tasks"
                );
                ExecuteResult::Spawn {
                    output: Some(json!({
                        "generated_count": tasks.len(),
                        "inner_output": raw_output,
                    })),
                    tasks,
                }
            }
            Err(e) => ExecuteResult::Failed {
                error: format!("failed to parse spawn output as tasks: {e}"),
                retryable: false,
            },
        }
    }
}


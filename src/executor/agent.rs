use crate::types::{ExecuteResult, Task};
use async_trait::async_trait;
use serde_json::json;
use tracing::debug;

use super::container::ContainerExecutor;
use super::{ExecutionContext, Executor};

/// Known AI agent providers and their Docker images.
const PROVIDER_IMAGES: &[(&str, &str)] = &[
    ("claude", "ghcr.io/tasked/agent-claude:latest"),
    ("openai", "ghcr.io/tasked/agent-openai:latest"),
    ("gemini", "ghcr.io/tasked/agent-gemini:latest"),
];

/// Agent executor — runs AI model prompts in containers.
///
/// Task config:
/// ```json
/// {
///     "provider": "claude",
///     "prompt": "Review this code and suggest improvements",
///     "model": "sonnet",
///     "max_tokens": 4096
/// }
/// ```
///
/// Requires secrets: the provider's API key (e.g., `${secrets.ANTHROPIC_API_KEY}`)
/// must be available in the task's env via secret interpolation.
///
/// Returns: `{ "provider", "model", "response", "usage": { "input_tokens", "output_tokens" } }`
pub struct AgentExecutor {
    container: ContainerExecutor,
}

impl AgentExecutor {
    pub fn new(container: ContainerExecutor) -> Self {
        Self { container }
    }
}

#[async_trait]
impl Executor for AgentExecutor {
    async fn execute(&self, task: &Task, ctx: &ExecutionContext) -> ExecuteResult {
        let provider = match task
            .executor_config
            .get("provider")
            .and_then(|v| v.as_str())
        {
            Some(p) => p.to_string(),
            None => {
                return ExecuteResult::Failed {
                    error: "missing 'provider' in agent config".to_string(),
                    retryable: false,
                };
            }
        };

        let prompt = match task.executor_config.get("prompt").and_then(|v| v.as_str()) {
            Some(p) => p.to_string(),
            None => {
                return ExecuteResult::Failed {
                    error: "missing 'prompt' in agent config".to_string(),
                    retryable: false,
                };
            }
        };

        let image = PROVIDER_IMAGES
            .iter()
            .find(|(name, _)| *name == provider)
            .map(|(_, img)| img.to_string());

        // Allow custom images via "image" field
        let image = task
            .executor_config
            .get("image")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or(image);

        let image = match image {
            Some(img) => img,
            None => {
                return ExecuteResult::Failed {
                    error: format!(
                        "unknown agent provider '{provider}'. Use one of: claude, openai, gemini, or specify a custom 'image'"
                    ),
                    retryable: false,
                };
            }
        };

        let model = task
            .executor_config
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();

        let max_tokens = task
            .executor_config
            .get("max_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(4096);

        debug!(
            task_id = %task.id,
            provider = %provider,
            model = %model,
            "running agent"
        );

        // Build env from task's executor_config env field (secrets already interpolated)
        let mut env = Vec::new();
        if let Some(env_obj) = task.executor_config.get("env").and_then(|v| v.as_object()) {
            for (k, v) in env_obj {
                if let Some(val) = v.as_str() {
                    env.push((k.clone(), val.to_string()));
                }
            }
        }
        env.push(("AGENT_PROMPT".to_string(), prompt.clone()));
        env.push(("AGENT_MODEL".to_string(), model.clone()));
        env.push(("AGENT_MAX_TOKENS".to_string(), max_tokens.to_string()));
        env.push(("AGENT_PROVIDER".to_string(), provider.clone()));

        // Build a synthetic task that uses the container executor
        let mut container_task = task.clone();
        container_task.executor_config = json!({
            "image": image,
            "env": env.iter().map(|(k, v)| (k.clone(), json!(v))).collect::<serde_json::Map<String, serde_json::Value>>(),
            "timeout_secs": task.timeout_secs,
        });

        // Delegate to container executor
        let result = self.container.execute(&container_task, ctx).await;

        // Parse structured output from container stdout
        match &result {
            ExecuteResult::Success {
                output: Some(output),
            } => {
                let stdout = output.get("stdout").and_then(|v| v.as_str()).unwrap_or("");
                // Try to parse JSON from stdout (agent images output structured JSON)
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(stdout.trim()) {
                    ExecuteResult::Success {
                        output: Some(json!({
                            "provider": provider,
                            "model": model,
                            "response": parsed.get("response").unwrap_or(&parsed),
                            "usage": parsed.get("usage"),
                            "raw": output,
                        })),
                    }
                } else {
                    // Fallback: treat stdout as the response text
                    ExecuteResult::Success {
                        output: Some(json!({
                            "provider": provider,
                            "model": model,
                            "response": stdout.trim(),
                            "raw": output,
                        })),
                    }
                }
            }
            _ => result,
        }
    }
}

use crate::types::{ExecuteResult, Task};
use async_trait::async_trait;
use serde_json::json;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{debug, warn};

use super::{ExecutionContext, Executor};

/// Maximum combined stdout+stderr size before truncation (10 MB).
const MAX_OUTPUT_BYTES: usize = 10 * 1024 * 1024;

/// Shell executor — runs a command via the system shell.
///
/// Task config should contain:
/// ```json
/// { "command": "echo hello" }
/// ```
///
/// An optional `env` object can pass environment variables to the child process:
/// ```json
/// { "command": "echo $MY_VAR", "env": { "MY_VAR": "safe_value" } }
/// ```
///
/// **Warning:** Never interpolate untrusted values (e.g. previous task outputs) directly
/// into `command` strings — this enables shell injection. Use `env` to pass dynamic
/// values safely and reference them as `$VAR` in the command instead.
///
/// Returns output as `{"stdout": "...", "stderr": "...", "exit_code": 0}` on success.
/// Streams partial output to storage periodically while the command is running.
pub struct ShellExecutor;

#[async_trait]
impl Executor for ShellExecutor {
    async fn execute(&self, task: &Task, ctx: &ExecutionContext) -> ExecuteResult {
        let command = match task.executor_config.get("command").and_then(|v| v.as_str()) {
            Some(cmd) => cmd.to_string(),
            None => {
                return ExecuteResult::Failed {
                    error: "missing 'command' in executor config".to_string(),
                    retryable: false,
                };
            }
        };

        debug!(task_id = %task.id, command = %command, "executing shell command");

        let timeout = Duration::from_secs(task.timeout_secs);

        let result = tokio::time::timeout(timeout, async {
            let mut cmd = tokio::process::Command::new("sh");
            cmd.arg("-c").arg(&command);
            cmd.kill_on_drop(true);

            // Clear inherited environment to prevent leaking server secrets
            // (AWS keys, tokens, etc.) into child processes.
            cmd.env_clear();

            // Restore PATH so the shell can find binaries
            if let Some(path) = std::env::var_os("PATH") {
                cmd.env("PATH", path);
            }

            // Inject user-supplied environment variables from executor config
            if let Some(env_obj) = task.executor_config.get("env").and_then(|v| v.as_object()) {
                for (key, value) in env_obj {
                    if let Some(val) = value.as_str() {
                        cmd.env(key, val);
                    }
                }
            }

            // Inject TASKED_* artifact environment variables
            if let Some(ref dir) = ctx.artifacts_dir {
                cmd.env("TASKED_ARTIFACTS", dir.display().to_string());
            }
            if let Some(ref url) = ctx.artifact_url {
                cmd.env("TASKED_ARTIFACT_URL", url);
            }

            let mut child = match cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn() {
                Ok(c) => c,
                Err(e) => return Err(e),
            };

            let stdout_pipe = child.stdout.take().expect("stdout pipe configured");
            let stderr_pipe = child.stderr.take().expect("stderr pipe configured");

            let mut stdout_reader = BufReader::new(stdout_pipe).lines();
            let mut stderr_reader = BufReader::new(stderr_pipe).lines();

            let mut stdout = String::new();
            let mut stderr = String::new();
            let mut last_flush = Instant::now();
            let flush_interval = Duration::from_millis(500);

            let mut stdout_done = false;
            let mut stderr_done = false;

            while !stdout_done || !stderr_done {
                tokio::select! {
                    line = stdout_reader.next_line(), if !stdout_done => {
                        match line {
                            Ok(Some(line)) => {
                                stdout.push_str(&line);
                                stdout.push('\n');
                                if stdout.len() + stderr.len() > MAX_OUTPUT_BYTES {
                                    warn!(task_id = %task.id, "output exceeded 10 MB, truncating");
                                    stderr.push_str("\n[tasked: output truncated at 10MB]");
                                    stdout_done = true;
                                    stderr_done = true;
                                }
                            }
                            _ => stdout_done = true,
                        }
                    }
                    line = stderr_reader.next_line(), if !stderr_done => {
                        match line {
                            Ok(Some(line)) => {
                                stderr.push_str(&line);
                                stderr.push('\n');
                                if stdout.len() + stderr.len() > MAX_OUTPUT_BYTES {
                                    warn!(task_id = %task.id, "output exceeded 10 MB, truncating");
                                    stderr.push_str("\n[tasked: output truncated at 10MB]");
                                    stdout_done = true;
                                    stderr_done = true;
                                }
                            }
                            _ => stderr_done = true,
                        }
                    }
                }

                // Periodic flush to storage
                if last_flush.elapsed() >= flush_interval {
                    ctx.flush_output(json!({
                        "stdout": &stdout,
                        "stderr": &stderr,
                        "exit_code": null,
                    }))
                    .await;
                    last_flush = Instant::now();
                }
            }

            let status = child.wait().await?;
            Ok((stdout, stderr, status))
        })
        .await;

        match result {
            Ok(Ok((stdout, stderr, status))) => {
                let exit_code = status.code().unwrap_or(-1);
                let output_json = json!({
                    "stdout": stdout,
                    "stderr": stderr,
                    "exit_code": exit_code,
                });

                if status.success() {
                    ExecuteResult::Success {
                        output: Some(output_json),
                    }
                } else {
                    ExecuteResult::Failed {
                        error: format!("command exited with code {exit_code}: {}", stderr.trim()),
                        retryable: true,
                    }
                }
            }
            Ok(Err(e)) => {
                warn!(task_id = %task.id, error = %e, "shell command failed to execute");
                ExecuteResult::Failed {
                    error: format!("failed to execute command: {e}"),
                    retryable: true,
                }
            }
            Err(_) => {
                warn!(task_id = %task.id, timeout_secs = task.timeout_secs, "shell command timed out");
                ExecuteResult::Failed {
                    error: format!("command timed out after {}s", task.timeout_secs),
                    retryable: true,
                }
            }
        }
    }
}


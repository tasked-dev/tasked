use crate::types::{ExecuteResult, Task};
use async_trait::async_trait;
use serde_json::json;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tracing::{debug, warn};

use super::{ExecutionContext, Executor};

/// Maximum combined stdout+stderr size before truncation (10 MB).
const MAX_OUTPUT_BYTES: usize = 10 * 1024 * 1024;

/// Marker appended to stderr when output is truncated.
const TRUNCATION_MARKER: &str = "\n[tasked: output truncated at 10MB]";

/// Send SIGKILL to the entire process group of `pid` (unix only).
///
/// The shell is spawned in its own process group so that grandchildren
/// (e.g. backgrounded processes holding the pipes open) are killed too,
/// instead of surviving a kill of just the direct `sh` child.
#[cfg(unix)]
#[allow(unsafe_code)] // raw libc::kill is the only dependency-free way to signal a process group
fn kill_process_group(pid: Option<u32>) {
    if let Some(pid) = pid {
        // SAFETY: libc::kill has no memory-safety preconditions; a negative
        // pid targets the process group we created with process_group(0).
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
    }
}

#[cfg(not(unix))]
fn kill_process_group(_pid: Option<u32>) {}

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

        // Deliberately do NOT log the resolved command: it may contain
        // interpolated ${secrets.*} values that must not end up in logs.
        debug!(task_id = %task.id, executor = "shell", "executing shell command");

        let timeout = Duration::from_secs(task.timeout_secs);

        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c").arg(&command);
        cmd.kill_on_drop(true);

        // Run the shell in its own process group (unix) so we can kill the
        // whole process tree on timeout or output truncation, not just `sh`.
        #[cfg(unix)]
        cmd.process_group(0);

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
            Err(e) => {
                warn!(task_id = %task.id, error = %e, "shell command failed to execute");
                return ExecuteResult::Failed {
                    error: format!("failed to execute command: {e}"),
                    retryable: true,
                };
            }
        };

        // Captured before the capture future borrows `child`, so the timeout
        // and cancellation paths can still kill the process group after the
        // future is dropped.
        let child_pid = child.id();

        let capture = tokio::time::timeout(timeout, async {
            let mut stdout_pipe = child.stdout.take().expect("stdout pipe configured");
            let mut stderr_pipe = child.stderr.take().expect("stderr pipe configured");

            // Read raw byte chunks rather than lines: a single giant line
            // without newlines must not buffer unboundedly before the cap
            // check.
            let mut stdout_buf: Vec<u8> = Vec::new();
            let mut stderr_buf: Vec<u8> = Vec::new();
            let mut out_chunk = [0u8; 8192];
            let mut err_chunk = [0u8; 8192];
            let mut stdout_done = false;
            let mut stderr_done = false;
            let mut truncated = false;

            let mut last_flush = Instant::now();
            let flush_interval = Duration::from_millis(500);

            while !stdout_done || !stderr_done {
                tokio::select! {
                    n = stdout_pipe.read(&mut out_chunk), if !stdout_done => match n {
                        Ok(0) | Err(_) => stdout_done = true,
                        Ok(n) => stdout_buf.extend_from_slice(&out_chunk[..n]),
                    },
                    n = stderr_pipe.read(&mut err_chunk), if !stderr_done => match n {
                        Ok(0) | Err(_) => stderr_done = true,
                        Ok(n) => stderr_buf.extend_from_slice(&err_chunk[..n]),
                    },
                }

                if stdout_buf.len() + stderr_buf.len() > MAX_OUTPUT_BYTES {
                    // Kill the child (and its whole process group) instead of
                    // merely closing the pipes: a child blocked writing to a
                    // full pipe would otherwise hang until the outer timeout,
                    // discarding all captured output.
                    warn!(task_id = %task.id, "output exceeded 10 MB, truncating and killing child");
                    truncated = true;
                    kill_process_group(child_pid);
                    let _ = child.kill().await;
                    break;
                }

                // Periodic flush to storage
                if last_flush.elapsed() >= flush_interval {
                    ctx.flush_output(json!({
                        "stdout": String::from_utf8_lossy(&stdout_buf),
                        "stderr": String::from_utf8_lossy(&stderr_buf),
                        "exit_code": null,
                    }))
                    .await;
                    last_flush = Instant::now();
                }
            }

            let status = child.wait().await?;
            let stdout = String::from_utf8_lossy(&stdout_buf).into_owned();
            let mut stderr = String::from_utf8_lossy(&stderr_buf).into_owned();
            if truncated {
                stderr.push_str(TRUNCATION_MARKER);
            }
            Ok::<_, std::io::Error>((stdout, stderr, status))
        });

        // Race the capture against engine-level cancellation so a cancelled
        // flow doesn't leave the command running until its timeout. The
        // capture future borrows `child`, so it must be dropped (scope end)
        // before the cancellation path can kill the child directly.
        let result = {
            tokio::pin!(capture);
            tokio::select! {
                r = &mut capture => Some(r),
                _ = ctx.cancelled() => None,
            }
        };

        let Some(result) = result else {
            kill_process_group(child_pid);
            let _ = child.kill().await;
            debug!(task_id = %task.id, "shell command aborted — task cancelled");
            return ExecuteResult::Failed {
                error: "task cancelled".to_string(),
                retryable: false,
            };
        };

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
                // The capture future was dropped by the timeout; make sure the
                // entire process group is gone, not just the direct child.
                kill_process_group(child_pid);
                let _ = child.kill().await;
                warn!(task_id = %task.id, timeout_secs = task.timeout_secs, "shell command timed out");
                ExecuteResult::Failed {
                    error: format!("command timed out after {}s", task.timeout_secs),
                    retryable: true,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStorage;
    use crate::types::{BackoffStrategy, FlowId, TaskId, TaskState};
    use std::sync::Arc;

    fn make_task(config: serde_json::Value, timeout_secs: u64) -> Task {
        Task {
            id: TaskId::from("test-shell"),
            flow_id: FlowId::new(),
            queue_id: crate::types::QueueId::from("test"),
            state: TaskState::Running,
            executor_type: "shell".to_string(),
            executor_config: config,
            input: None,
            output: None,
            error: None,
            retries_remaining: 0,
            backoff: BackoffStrategy::default(),
            timeout_secs,
            condition: None,
            retry_at: None,
            started_at: None,
            completed_at: None,
            created_at: chrono::Utc::now(),
        }
    }

    fn make_ctx() -> ExecutionContext {
        ExecutionContext::new(
            Arc::new(MemoryStorage::new()),
            TaskId::from("test-shell"),
            FlowId::new(),
        )
    }

    #[tokio::test]
    async fn runs_simple_command() {
        let task = make_task(json!({"command": "echo hello"}), 10);
        let result = ShellExecutor.execute(&task, &make_ctx()).await;
        match result {
            ExecuteResult::Success { output: Some(out) } => {
                assert_eq!(out["exit_code"], 0);
                assert_eq!(out["stdout"], "hello\n");
            }
            other => panic!("expected success, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn giant_single_line_is_truncated_and_child_killed() {
        // Emit far more than the cap on a single line with no newline;
        // the executor must kill the child and return promptly with the
        // truncation marker rather than hanging until the timeout.
        let task = make_task(
            json!({"command": "yes | tr -d '\\n' | head -c 50000000; sleep 60"}),
            30,
        );
        let start = std::time::Instant::now();
        let result = ShellExecutor.execute(&task, &make_ctx()).await;
        assert!(
            start.elapsed() < Duration::from_secs(25),
            "should not wait for the sleep/timeout"
        );
        match result {
            ExecuteResult::Failed { error, .. } => {
                assert!(
                    error.contains("output truncated at 10MB"),
                    "error should carry the truncation marker: {error}"
                );
            }
            other => panic!("expected failed (killed child), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn timeout_kills_process_group() {
        let task = make_task(json!({"command": "sleep 30"}), 1);
        let result = ShellExecutor.execute(&task, &make_ctx()).await;
        match result {
            ExecuteResult::Failed { error, .. } => {
                assert!(error.contains("timed out"), "got: {error}");
            }
            other => panic!("expected timeout failure, got {other:?}"),
        }
    }
}

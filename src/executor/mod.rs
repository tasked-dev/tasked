#[cfg(feature = "docker")]
pub mod agent;
#[cfg(feature = "http")]
pub mod api;
pub mod approval;
#[cfg(feature = "docker")]
pub mod container;
pub mod delay;
#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "http")]
pub mod remote;
#[cfg(feature = "shell")]
pub mod shell;
pub mod spawn;
pub mod trigger;

use crate::store::Storage;
use crate::types::{ExecuteResult, Flow, FlowDef, FlowId, QueueId, Task, TaskId, TaskState};
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;

/// Maximum response body size (16 MiB). Any HTTP response exceeding this
/// limit will be rejected to prevent unbounded memory consumption.
#[cfg(feature = "http")]
pub const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Read a response body with a size cap of [`MAX_RESPONSE_BYTES`].
///
/// Returns the body as a `String`, or an error message if the body exceeds the
/// limit or cannot be read.
#[cfg(feature = "http")]
pub async fn read_response_body(mut resp: reqwest::Response) -> Result<String, String> {
    // Fast path: if the server declares a Content-Length beyond the cap,
    // fail before reading anything.
    if let Some(len) = resp.content_length()
        && len > MAX_RESPONSE_BYTES as u64
    {
        return Err(format!(
            "response body too large: {len} bytes (limit: {MAX_RESPONSE_BYTES} bytes)"
        ));
    }

    // Stream the body chunk by chunk, aborting as soon as the running total
    // exceeds the cap, instead of buffering the whole body first.
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let chunk = resp
            .chunk()
            .await
            .map_err(|e| format!("failed to read response body: {e}"))?;
        let Some(chunk) = chunk else { break };
        if buf.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return Err(format!(
                "response body too large: {} bytes (limit: {} bytes)",
                buf.len() + chunk.len(),
                MAX_RESPONSE_BYTES
            ));
        }
        buf.extend_from_slice(&chunk);
    }
    String::from_utf8(buf).map_err(|e| format!("response body is not valid UTF-8: {e}"))
}

/// Maximum number of response-body bytes embedded into error strings (4 KB).
#[cfg(feature = "http")]
const MAX_ERROR_BODY_BYTES: usize = 4096;

/// Truncate a response body for safe embedding in an error message.
///
/// Bodies longer than 4 KB are cut at a char boundary and suffixed with an
/// ellipsis marker noting how many bytes were dropped.
#[cfg(feature = "http")]
pub(crate) fn truncate_body_for_error(body: &str) -> std::borrow::Cow<'_, str> {
    if body.len() <= MAX_ERROR_BODY_BYTES {
        return std::borrow::Cow::Borrowed(body);
    }
    let mut end = MAX_ERROR_BODY_BYTES;
    while end > 0 && !body.is_char_boundary(end) {
        end -= 1;
    }
    std::borrow::Cow::Owned(format!(
        "{}… [truncated {} bytes]",
        &body[..end],
        body.len() - end
    ))
}

/// Trait for submitting and querying flows. Implemented by Engine (via wrapper).
/// Passed to executors via ExecutionContext to avoid circular dependencies.
#[async_trait]
pub trait FlowSubmitter: Send + Sync {
    async fn submit(
        &self,
        queue_id: &QueueId,
        flow_def: FlowDef,
        parent_depth: u32,
        parent_flow_id: Option<FlowId>,
    ) -> Result<Flow, String>;
    async fn query_flow(&self, flow_id: &FlowId) -> Result<Option<Flow>, String>;
    /// Cancel a flow (used by the trigger executor to stop an orphaned child).
    async fn cancel_flow(&self, flow_id: &FlowId) -> Result<(), String>;
}

/// Context passed to executors during task execution.
/// Provides access to storage for streaming partial output.
pub struct ExecutionContext {
    store: Arc<dyn Storage>,
    task_id: TaskId,
    flow_id: FlowId,
    /// Local directory path for artifacts (if available).
    pub artifacts_dir: Option<std::path::PathBuf>,
    /// HTTP URL for artifact API (if available in server mode).
    pub artifact_url: Option<String>,
    /// Optional flow submitter for trigger executor sub-flow composition.
    pub flow_submitter: Option<Arc<dyn FlowSubmitter>>,
    /// Trigger depth of the parent flow (0 for top-level).
    pub trigger_depth: u32,
    /// Concurrency permit from the engine's per-queue semaphore.
    /// Executors that block for extended periods (e.g., trigger with wait: true)
    /// should release this early to avoid deadlocking the queue.
    /// Uses Mutex for interior mutability so executors can release via `&self`.
    concurrency_permit: std::sync::Mutex<Option<OwnedSemaphorePermit>>,
    /// Flow-level cancellation signal from the engine. `true` once the flow
    /// (or this task, via fail_fast) has been cancelled.
    cancel: Option<tokio::sync::watch::Receiver<bool>>,
}

impl ExecutionContext {
    /// Create a new execution context for a task.
    pub fn new(store: Arc<dyn Storage>, task_id: TaskId, flow_id: FlowId) -> Self {
        Self {
            store,
            task_id,
            flow_id,
            artifacts_dir: None,
            artifact_url: None,
            trigger_depth: 0,
            flow_submitter: None,
            concurrency_permit: std::sync::Mutex::new(None),
            cancel: None,
        }
    }

    /// Attach the engine's flow-level cancellation signal.
    pub fn with_cancellation(mut self, rx: tokio::sync::watch::Receiver<bool>) -> Self {
        self.cancel = Some(rx);
        self
    }

    /// Configure artifact storage paths for this execution.
    pub fn with_artifacts(mut self, dir: Option<std::path::PathBuf>, url: Option<String>) -> Self {
        self.artifacts_dir = dir;
        self.artifact_url = url;
        self
    }

    /// Set the flow submitter for sub-flow composition (used by trigger executor).
    pub fn with_flow_submitter(mut self, submitter: Arc<dyn FlowSubmitter>) -> Self {
        self.flow_submitter = Some(submitter);
        self
    }

    /// Set the trigger nesting depth of the parent flow.
    pub fn with_trigger_depth(mut self, depth: u32) -> Self {
        self.trigger_depth = depth;
        self
    }

    /// Attach a concurrency permit from the engine's per-queue semaphore.
    pub fn with_concurrency_permit(self, permit: OwnedSemaphorePermit) -> Self {
        *self
            .concurrency_permit
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(permit);
        self
    }

    /// Release the concurrency permit early. Call this before entering a
    /// long-running wait loop (e.g., trigger executor polling) to avoid
    /// deadlocking the queue's concurrency semaphore.
    pub fn release_concurrency_permit(&self) {
        self.concurrency_permit
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
    }

    /// Write partial output to storage while task is running.
    /// Best-effort — errors are silently ignored.
    pub async fn flush_output(&self, output: serde_json::Value) {
        let _ = self
            .store
            .set_task_output(&self.task_id, &self.flow_id, output)
            .await;
    }

    /// Check whether this task has been cancelled in the store.
    /// Used by long-running executors (e.g., trigger with wait) to detect
    /// cancellation and exit early. Best-effort — returns false on errors.
    pub async fn is_cancelled(&self) -> bool {
        if self.cancel_requested() {
            return true;
        }
        self.store
            .get_task(&self.task_id, &self.flow_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|t| t.state == TaskState::Cancelled)
    }

    /// Non-blocking check of the engine's cancellation signal.
    /// Unlike [`Self::is_cancelled`], this performs no storage round-trip.
    pub fn cancel_requested(&self) -> bool {
        self.cancel.as_ref().is_some_and(|rx| *rx.borrow())
    }

    /// Clone the raw cancellation receiver, for handing into components that
    /// outlive a borrow of the context (e.g. container backend specs).
    pub fn cancel_receiver(&self) -> Option<tokio::sync::watch::Receiver<bool>> {
        self.cancel.clone()
    }

    /// Resolves when the engine cancels this task's flow (or never, if no
    /// cancellation signal was attached). Executors should race long-running
    /// work against this and clean up their external resources (child
    /// processes, containers, in-flight requests) when it fires.
    pub async fn cancelled(&self) {
        match self.cancel.clone() {
            Some(mut rx) => {
                if *rx.borrow() {
                    return;
                }
                // Wait until the engine flips the signal (or it is dropped at
                // flow finalization — also treated as "stop waiting" only when
                // the last seen value was true).
                while rx.changed().await.is_ok() {
                    if *rx.borrow() {
                        return;
                    }
                }
                // Sender dropped without signaling: the flow finalized
                // normally. Never resolve — completion wins the race.
                std::future::pending::<()>().await;
            }
            None => std::future::pending::<()>().await,
        }
    }
}

/// Executor trait — implement this to define how a task type is executed.
///
/// Register executors with [`Engine::register_executor`](crate::engine::Engine::register_executor)
/// or [`EngineBuilder::executor`](crate::engine::EngineBuilder::executor). The engine
/// dispatches tasks to the executor whose name matches [`TaskDef::executor`](crate::types::TaskDef::executor).
///
/// Built-in executors: `ShellExecutor`, `HttpExecutor`, `ContainerExecutor`,
/// `DelayExecutor`, `ApprovalExecutor`, `TriggerExecutor`, `SpawnExecutor`.
/// For in-process logic, use [`CallbackExecutor`].
#[async_trait]
pub trait Executor: Send + Sync {
    /// Execute a task and return its result.
    ///
    /// The task's `executor_config` contains executor-specific configuration
    /// (e.g., shell command, HTTP URL). Use `ctx` to stream partial output
    /// or access artifacts.
    async fn execute(&self, task: &Task, ctx: &ExecutionContext) -> ExecuteResult;
}

type CallbackFn =
    dyn Fn(&Task) -> Pin<Box<dyn Future<Output = ExecuteResult> + Send>> + Send + Sync;

/// Callback executor — executes an in-process function.
/// Used for library mode and testing.
pub struct CallbackExecutor {
    callback: Arc<CallbackFn>,
}

impl CallbackExecutor {
    /// Create a callback executor from an async function.
    ///
    /// The function receives a [`Task`] reference and returns an [`ExecuteResult`].
    /// Extract data from the task *before* the async block to avoid lifetime issues.
    pub fn new<F, Fut>(f: F) -> Self
    where
        F: Fn(&Task) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ExecuteResult> + Send + 'static,
    {
        Self {
            callback: Arc::new(move |task| Box::pin(f(task))),
        }
    }

    /// Create a callback executor that always succeeds with no output.
    pub fn always_succeed() -> Self {
        Self::new(|_| async { ExecuteResult::Success { output: None } })
    }

    /// Create a callback executor that always fails.
    pub fn always_fail(error: &'static str) -> Self {
        Self::new(move |_| async move {
            ExecuteResult::Failed {
                error: error.to_string(),
                retryable: true,
            }
        })
    }

    /// Create a callback executor that always fails non-retryably.
    pub fn always_fail_permanent(error: &'static str) -> Self {
        Self::new(move |_| async move {
            ExecuteResult::Failed {
                error: error.to_string(),
                retryable: false,
            }
        })
    }
}

#[async_trait]
impl Executor for CallbackExecutor {
    async fn execute(&self, task: &Task, _ctx: &ExecutionContext) -> ExecuteResult {
        (self.callback)(task).await
    }
}

/// Noop executor — succeeds immediately. Useful for testing and scheduling.
pub struct NoopExecutor;

#[async_trait]
impl Executor for NoopExecutor {
    async fn execute(&self, _task: &Task, _ctx: &ExecutionContext) -> ExecuteResult {
        ExecuteResult::Success { output: None }
    }
}

#[cfg(all(test, feature = "http"))]
mod tests {
    use super::*;

    #[test]
    fn truncate_body_short_is_unchanged() {
        let body = "hello";
        assert_eq!(truncate_body_for_error(body), "hello");
    }

    #[test]
    fn truncate_body_long_is_capped_with_marker() {
        let body = "x".repeat(MAX_ERROR_BODY_BYTES + 100);
        let out = truncate_body_for_error(&body);
        assert!(out.starts_with(&"x".repeat(MAX_ERROR_BODY_BYTES)));
        assert!(out.contains("[truncated 100 bytes]"));
    }

    #[test]
    fn truncate_body_respects_char_boundaries() {
        // Multi-byte char straddling the cap must not cause a panic.
        let mut body = "x".repeat(MAX_ERROR_BODY_BYTES - 1);
        body.push('é'); // 2 bytes, crosses the boundary
        body.push_str(&"y".repeat(100));
        let out = truncate_body_for_error(&body);
        assert!(out.contains("[truncated"));
    }
}

//! Shared type definitions for the Tasked DAG execution engine.
//!
//! This crate contains the core types used across the Tasked ecosystem:
//! queue, flow, task, and schedule identifiers, state machines, configuration
//! structs, and execution result types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use uuid::Uuid;

/// Unique identifier for a queue.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueueId(String);

impl QueueId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for QueueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for QueueId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for QueueId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Unique identifier for a flow.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FlowId(String);

impl Default for FlowId {
    fn default() -> Self {
        Self::new()
    }
}

impl FlowId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for FlowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for FlowId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for FlowId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Unique identifier for a task within a flow.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(String);

impl TaskId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for TaskId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for TaskId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Task state machine states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    /// Waiting for dependencies to complete.
    Pending,
    /// Dependencies met, ready to dispatch.
    Ready,
    /// Currently being executed.
    Running,
    /// Completed successfully.
    Succeeded,
    /// Failed after all retries exhausted.
    Failed,
    /// Waiting for retry.
    Delayed,
    /// Cancelled (dependency failed or flow cancelled).
    Cancelled,
}

impl TaskState {
    /// Returns whether this is a terminal state.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    /// Validate state transitions.
    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::Ready)
                | (Self::Pending, Self::Cancelled)
                | (Self::Ready, Self::Running)
                | (Self::Ready, Self::Succeeded)
                | (Self::Ready, Self::Failed)
                | (Self::Ready, Self::Cancelled)
                | (Self::Running, Self::Succeeded)
                | (Self::Running, Self::Failed)
                | (Self::Running, Self::Delayed)
                | (Self::Running, Self::Cancelled)
                | (Self::Delayed, Self::Ready)
                | (Self::Delayed, Self::Cancelled)
        )
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Ready => write!(f, "ready"),
            Self::Running => write!(f, "running"),
            Self::Succeeded => write!(f, "succeeded"),
            Self::Failed => write!(f, "failed"),
            Self::Delayed => write!(f, "delayed"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// Flow-level state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowState {
    /// Flow is actively executing tasks.
    Running,
    /// All tasks completed successfully.
    Succeeded,
    /// One or more tasks failed terminally.
    Failed,
    /// Flow was explicitly cancelled.
    Cancelled,
}

impl FlowState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

impl fmt::Display for FlowState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Running => write!(f, "running"),
            Self::Succeeded => write!(f, "succeeded"),
            Self::Failed => write!(f, "failed"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// Backoff strategy for task retries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackoffStrategy {
    /// Fixed delay between retries.
    Fixed { delay_ms: u64 },
    /// Exponential backoff: delay_ms * 2^attempt.
    Exponential { initial_delay_ms: u64 },
    /// Exponential backoff with random jitter.
    ExponentialJitter { initial_delay_ms: u64 },
}

impl Default for BackoffStrategy {
    fn default() -> Self {
        Self::Exponential {
            initial_delay_ms: 1000,
        }
    }
}

impl BackoffStrategy {
    /// Calculate delay for a given attempt number (0-indexed).
    pub fn delay_ms(&self, attempt: u32) -> u64 {
        match self {
            Self::Fixed { delay_ms } => *delay_ms,
            Self::Exponential { initial_delay_ms } => {
                initial_delay_ms.saturating_mul(2u64.saturating_pow(attempt))
            }
            Self::ExponentialJitter { initial_delay_ms } => {
                let base = initial_delay_ms.saturating_mul(2u64.saturating_pow(attempt));
                // Simple jitter: 50-150% of base delay

                (base as f64 * (0.5 + rand::random::<f64>())) as u64
            }
        }
    }
}

/// Reference to a secret value, resolved at dispatch time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SecretRef {
    /// Read secret from this environment variable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    /// Read secret from this file path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
}

/// Rate limit configuration for a queue (token bucket).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Maximum burst size (token bucket capacity).
    pub max_burst: u64,
    /// Tokens refilled per second.
    pub per_second: f64,
}

/// Configuration for a queue.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueConfig {
    /// Maximum concurrent running tasks.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Default max retries for tasks in this queue.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Default timeout in seconds for tasks in this queue.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    /// Default backoff strategy.
    #[serde(default)]
    pub backoff: BackoffStrategy,
    /// Optional rate limit (token bucket). If None, no rate limiting.
    #[serde(default)]
    pub rate_limit: Option<RateLimitConfig>,
    /// Retention period for completed flows (seconds). Flows in terminal state
    /// older than this are automatically deleted. None = keep forever (default).
    #[serde(default)]
    pub retention_secs: Option<u64>,
    /// Maximum number of non-terminal flows allowed in this queue.
    /// When the limit is reached, new submissions return 429 Too Many Requests.
    /// None = no limit (default, backwards compatible).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_pending_flows: Option<usize>,
    /// Named secrets available to tasks via `${secrets.<name>}` interpolation.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub secrets: HashMap<String, SecretRef>,
}

fn default_concurrency() -> usize {
    10
}
fn default_max_retries() -> u32 {
    3
}
fn default_timeout_secs() -> u64 {
    300
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            concurrency: default_concurrency(),
            max_retries: default_max_retries(),
            timeout_secs: default_timeout_secs(),
            backoff: BackoffStrategy::default(),
            rate_limit: None,
            retention_secs: Some(2_592_000),
            max_pending_flows: None,
            secrets: HashMap::new(),
        }
    }
}

/// A queue definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Queue {
    pub id: QueueId,
    pub config: QueueConfig,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A task definition as submitted by the user.
///
/// Use struct update syntax with `Default` to set only the fields you need:
/// ```rust
/// use crate::types::*;
///
/// let task = TaskDef {
///     id: TaskId::from("my-task"),
///     executor: "shell".into(),
///     config: serde_json::json!({ "command": "echo hello" }),
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TaskDef {
    pub id: TaskId,
    pub executor: String,
    #[serde(default)]
    pub config: serde_json::Value,
    #[serde(default)]
    pub input: Option<serde_json::Value>,
    #[serde(default)]
    pub depends_on: Vec<TaskId>,
    pub timeout_secs: Option<u64>,
    pub retries: Option<u32>,
    pub backoff: Option<BackoffStrategy>,
    pub condition: Option<String>,
    /// IDs of generated tasks that are exported as dependency targets.
    /// Non-empty only for spawn executor tasks.
    /// Downstream tasks reference these as "{this_task_id}/{output_name}".
    #[serde(default)]
    pub spawn_output: Vec<String>,
}

/// Webhook configuration for flow lifecycle events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FlowWebhooks {
    /// URL to POST when flow succeeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_complete: Option<String>,
    /// URL to POST when flow fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_failure: Option<String>,
}

/// A flow definition as submitted by the user.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FlowDef {
    pub tasks: Vec<TaskDef>,
    #[serde(default)]
    pub webhooks: Option<FlowWebhooks>,
    /// When true, cancel all non-terminal tasks on first task failure
    /// instead of only downstream dependents.
    #[serde(default)]
    pub fail_fast: bool,
}

/// Internal task representation with runtime state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub flow_id: FlowId,
    pub queue_id: QueueId,
    pub state: TaskState,
    pub executor_type: String,
    pub executor_config: serde_json::Value,
    pub input: Option<serde_json::Value>,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    pub retries_remaining: u32,
    pub backoff: BackoffStrategy,
    pub timeout_secs: u64,
    pub condition: Option<String>,
    pub retry_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Internal flow representation with runtime state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Flow {
    pub id: FlowId,
    pub queue_id: QueueId,
    pub state: FlowState,
    pub task_count: usize,
    pub tasks_succeeded: usize,
    pub tasks_failed: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhooks: Option<FlowWebhooks>,
    /// Trigger nesting depth. 0 for top-level flows, incremented for each
    /// trigger submission. Used to prevent unbounded trigger chains.
    #[serde(default)]
    pub trigger_depth: u32,
    /// The original FlowDef as submitted by the user, stored verbatim for
    /// replay and audit purposes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_def: Option<FlowDef>,
    /// When true, cancel all non-terminal tasks on first task failure.
    #[serde(default)]
    pub fail_fast: bool,
    /// ID of the parent flow that spawned this one via a trigger executor.
    /// `None` for top-level flows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_flow_id: Option<FlowId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Result of executing a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecuteResult {
    Success {
        output: Option<serde_json::Value>,
    },
    Failed {
        error: String,
        retryable: bool,
    },
    /// Task is waiting for external approval. Output contains approval info.
    /// The task stays in Running state until acked via the API.
    AwaitingApproval {
        output: serde_json::Value,
    },
    /// Task succeeded and generated new tasks to inject into the flow.
    Spawn {
        output: Option<serde_json::Value>,
        tasks: Vec<TaskDef>,
    },
}

/// Unique identifier for a schedule.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScheduleId(String);

impl Default for ScheduleId {
    fn default() -> Self {
        Self::new()
    }
}

impl ScheduleId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ScheduleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for ScheduleId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for ScheduleId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// A schedule definition as submitted by the user.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleDef {
    /// Cron expression (standard 5-field or 7-field with seconds)
    pub cron: String,
    /// Flow definition to submit on each trigger
    pub flow: FlowDef,
    /// Optional human-readable name
    #[serde(default)]
    pub name: Option<String>,
    /// Whether the schedule is active (default: true)
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

/// Internal schedule representation with runtime state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schedule {
    pub id: ScheduleId,
    pub queue_id: QueueId,
    pub name: Option<String>,
    pub cron: String,
    pub flow_def: FlowDef,
    pub enabled: bool,
    pub last_triggered_at: Option<DateTime<Utc>>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// -- Export types --

/// Flow-level metadata in an export document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowExportMeta {
    pub id: FlowId,
    pub queue_id: QueueId,
    pub state: FlowState,
    pub task_count: usize,
    pub tasks_succeeded: usize,
    pub tasks_failed: usize,
    #[serde(default)]
    pub trigger_depth: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhooks: Option<FlowWebhooks>,
    /// The original FlowDef as submitted, if persisted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_def: Option<FlowDef>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Exported task state snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskExport {
    pub id: TaskId,
    pub executor_type: String,
    pub executor_config: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub state: TaskState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<TaskId>,
    pub retries_remaining: u32,
    pub timeout_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Artifact metadata (and optionally inline data) in an export.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactExport {
    pub name: String,
    pub size_bytes: u64,
    /// Base64-encoded content for small artifacts (< 1 MB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_base64: Option<String>,
}

/// Complete flow export document for archival, compliance, or replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowExport {
    /// Schema version (starts at 1).
    pub version: u32,
    /// Flow metadata.
    pub flow: FlowExportMeta,
    /// All tasks with full state and dependencies.
    pub tasks: Vec<TaskExport>,
    /// Artifact metadata and optionally inline data.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactExport>,
    /// SHA-256 hex digest of the canonical JSON (computed with this field as null).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    /// When this export was generated.
    pub exported_at: DateTime<Utc>,
}

#[cfg(feature = "journaled")]
pub mod journaled;
pub mod memory;
#[cfg(feature = "sqlite")]
pub mod sharded;
#[cfg(feature = "sqlite")]
pub mod sqlite;

use crate::types::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Storage backend trait. Implementations must be Send + Sync for use with tokio.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Store a new queue.
    async fn create_queue(&self, queue: &Queue) -> Result<(), StorageError>;

    /// Get a queue by ID.
    async fn get_queue(&self, id: &QueueId) -> Result<Option<Queue>, StorageError>;

    /// List all queues.
    async fn list_queues(&self) -> Result<Vec<Queue>, StorageError>;

    /// Delete a queue by ID.
    async fn delete_queue(&self, id: &QueueId) -> Result<(), StorageError>;

    /// Store a new flow and its tasks atomically.
    async fn create_flow(
        &self,
        flow: &Flow,
        tasks: &[Task],
        deps: &HashMap<TaskId, Vec<TaskId>>,
    ) -> Result<(), StorageError>;

    /// Get a flow by ID.
    async fn get_flow(&self, id: &FlowId) -> Result<Option<Flow>, StorageError>;

    /// List flows for a queue, optionally filtered by state.
    async fn list_flows(
        &self,
        queue_id: &QueueId,
        state: Option<FlowState>,
    ) -> Result<Vec<Flow>, StorageError>;

    /// Update flow state.
    async fn update_flow_state(&self, id: &FlowId, state: FlowState) -> Result<(), StorageError>;

    /// Increment flow success/failure counters.
    async fn increment_flow_counter(
        &self,
        id: &FlowId,
        succeeded: bool,
    ) -> Result<Flow, StorageError>;

    /// Get a task by (task_id, flow_id).
    async fn get_task(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<Option<Task>, StorageError>;

    /// Get all tasks for a flow.
    async fn get_flow_tasks(&self, flow_id: &FlowId) -> Result<Vec<Task>, StorageError>;

    /// Get a flow and all its tasks in a single operation.
    /// Default implementation calls get_flow + get_flow_tasks separately.
    /// Backends can override for single-lock optimization.
    async fn get_flow_with_tasks(
        &self,
        flow_id: &FlowId,
    ) -> Result<Option<(Flow, Vec<Task>)>, StorageError> {
        let flow = self.get_flow(flow_id).await?;
        match flow {
            Some(flow) => {
                let tasks = self.get_flow_tasks(&flow.id).await?;
                Ok(Some((flow, tasks)))
            }
            None => Ok(None),
        }
    }

    /// Fetch tasks in `Ready` state for a given queue, up to `limit`.
    async fn fetch_ready_tasks(
        &self,
        queue_id: &QueueId,
        limit: usize,
    ) -> Result<Vec<Task>, StorageError>;

    /// Fetch tasks in `Delayed` state whose retry_at has passed.
    async fn fetch_delayed_tasks_due(&self) -> Result<Vec<Task>, StorageError>;

    /// Fetch tasks in `Running` state that have exceeded their timeout.
    async fn fetch_timed_out_tasks(&self) -> Result<Vec<Task>, StorageError>;

    /// Transition a task to a new state.
    async fn update_task_state(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        new_state: TaskState,
    ) -> Result<(), StorageError>;

    /// Mark a task as running (set state + started_at).
    async fn mark_task_running(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<(), StorageError>;

    /// Mark multiple tasks as running in a single batch.
    /// Returns task IDs that were successfully transitioned to Running.
    /// Tasks that are no longer in Ready state are silently skipped.
    async fn mark_tasks_running_batch(
        &self,
        tasks: &[(&TaskId, &FlowId)],
    ) -> Result<Vec<(TaskId, FlowId)>, StorageError> {
        let mut succeeded = Vec::with_capacity(tasks.len());
        for &(task_id, flow_id) in tasks {
            match self.mark_task_running(task_id, flow_id).await {
                Ok(()) => succeeded.push((task_id.clone(), flow_id.clone())),
                Err(StorageError::InvalidStateTransition(_, _)) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(succeeded)
    }

    /// Write output to a task without changing its state.
    /// Used by the approval executor to store approval info while the task remains Running.
    async fn set_task_output(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: serde_json::Value,
    ) -> Result<(), StorageError>;

    /// Mark a task as succeeded with output.
    async fn mark_task_succeeded(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: Option<serde_json::Value>,
    ) -> Result<(), StorageError>;

    /// Mark a task as failed with error.
    async fn mark_task_failed(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        error: &str,
    ) -> Result<(), StorageError>;

    /// Mark a task as delayed (for retry).
    async fn mark_task_delayed(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        retry_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StorageError>;

    /// Get all dependencies for every task in a flow, in a single query.
    async fn get_flow_dependencies(
        &self,
        flow_id: &FlowId,
    ) -> Result<HashMap<TaskId, Vec<TaskId>>, StorageError>;

    /// Get the dependencies of a task.
    async fn get_task_dependencies(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<Vec<TaskId>, StorageError>;

    /// Get all tasks that depend on the given task.
    async fn get_task_dependents(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<Vec<TaskId>, StorageError>;

    /// Resolve dependencies: find pending tasks whose dependencies are all succeeded,
    /// and transition them to ready.
    async fn resolve_ready_tasks(&self, flow_id: &FlowId) -> Result<Vec<TaskId>, StorageError>;

    /// Atomically: mark a task as succeeded, increment the flow counter, and resolve
    /// newly-ready dependents. Returns the updated flow and IDs of promoted tasks.
    ///
    /// Default implementation calls the three methods individually. Backends can
    /// override this to batch the operations in a single lock/transaction.
    async fn complete_task_success(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: Option<serde_json::Value>,
    ) -> Result<(Flow, Vec<TaskId>), StorageError> {
        self.mark_task_succeeded(task_id, flow_id, output).await?;
        let flow = self.increment_flow_counter(flow_id, true).await?;
        let newly_ready = if flow.tasks_succeeded < flow.task_count {
            self.resolve_ready_tasks(flow_id).await?
        } else {
            vec![]
        };
        Ok((flow, newly_ready))
    }

    /// Mark task succeeded, increment flow counter, and mark pre-resolved tasks as ready.
    ///
    /// Like [`Self::complete_task_success`], but the caller provides the list of newly-ready
    /// task IDs (resolved via an in-memory dependency graph) instead of querying the DB.
    async fn complete_task_with_ready(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: Option<serde_json::Value>,
        newly_ready: &[TaskId],
    ) -> Result<Flow, StorageError> {
        self.mark_task_succeeded(task_id, flow_id, output).await?;
        let flow = self.increment_flow_counter(flow_id, true).await?;
        for tid in newly_ready {
            self.update_task_state(tid, flow_id, TaskState::Ready)
                .await?;
        }
        Ok(flow)
    }

    /// Batch complete multiple tasks in a single transaction.
    /// Each entry: (task_id, flow_id, output, newly_ready_task_ids).
    /// Returns a Vec of `Option<Flow>` for each entry; None means the task was
    /// already cancelled/succeeded and was silently skipped.
    async fn complete_tasks_with_ready_batch(
        &self,
        completions: &[(TaskId, FlowId, Option<serde_json::Value>, Vec<TaskId>)],
    ) -> Result<Vec<Option<Flow>>, StorageError> {
        // Default: call complete_task_with_ready individually
        let mut results = Vec::with_capacity(completions.len());
        for (task_id, flow_id, output, newly_ready) in completions {
            match self
                .complete_task_with_ready(task_id, flow_id, output.clone(), newly_ready)
                .await
            {
                Ok(flow) => results.push(Some(flow)),
                Err(StorageError::InvalidStateTransition(_, _)) => results.push(None),
                Err(e) => return Err(e),
            }
        }
        Ok(results)
    }

    /// Inject new tasks into an existing flow atomically.
    /// Inserts tasks and deps, and increments flow.task_count.
    async fn inject_tasks(
        &self,
        flow_id: &FlowId,
        tasks: &[Task],
        deps: &HashMap<TaskId, Vec<TaskId>>,
    ) -> Result<Flow, StorageError>;

    /// Get IDs of child flows spawned by trigger tasks in a parent flow.
    async fn get_child_flow_ids(
        &self,
        parent_flow_id: &FlowId,
    ) -> Result<Vec<FlowId>, StorageError>;

    /// Delete terminal flows (and their tasks/deps) updated before `cutoff`.
    /// Returns the number of flows deleted.
    async fn delete_terminal_flows_before(
        &self,
        queue_id: &QueueId,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<usize, StorageError>;

    // -- Schedules --

    async fn create_schedule(&self, schedule: &Schedule) -> Result<(), StorageError>;
    async fn get_schedule(&self, id: &ScheduleId) -> Result<Option<Schedule>, StorageError>;
    async fn list_schedules(&self, queue_id: &QueueId) -> Result<Vec<Schedule>, StorageError>;
    async fn update_schedule(&self, schedule: &Schedule) -> Result<(), StorageError>;
    async fn delete_schedule(&self, id: &ScheduleId) -> Result<(), StorageError>;
    async fn fetch_due_schedules(&self) -> Result<Vec<Schedule>, StorageError>;
    async fn mark_schedule_triggered(
        &self,
        id: &ScheduleId,
        triggered_at: chrono::DateTime<chrono::Utc>,
        next_run_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), StorageError>;

    /// Run a WAL checkpoint to consolidate the write-ahead log.
    /// Default implementation is a no-op (for non-SQLite backends).
    async fn checkpoint(&self) -> Result<(), StorageError> {
        Ok(())
    }

    /// Check storage backend health.
    /// Default is a no-op. The journaled backend overrides this to detect
    /// when the journal writer thread has died.
    async fn health_check(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

/// Errors returned by [`Storage`] implementations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("queue '{0}' already exists")]
    QueueAlreadyExists(String),
    #[error("queue '{0}' not found")]
    QueueNotFound(String),
    #[error("flow '{0}' not found")]
    FlowNotFound(String),
    #[error("task '{0}' not found in flow '{1}'")]
    TaskNotFound(String, String),
    #[error("invalid state transition: {0} → {1}")]
    InvalidStateTransition(String, String),
    #[error("schedule '{0}' not found")]
    ScheduleNotFound(String),
    #[error("storage error: {0}")]
    Internal(String),
}


pub mod config;
pub(crate) mod events;
pub(crate) mod recovery;
pub(crate) mod snapshot;
pub(crate) mod state;
pub(crate) mod writer;

use super::{Storage, StorageError};
use crate::types::*;
use async_trait::async_trait;
use chrono::Utc;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use config::JournalConfig;
use events::{JournalEntry, JournalEvent};
use state::MemState;
use writer::{JournalWriter, WriterConfig};

/// Journaled storage engine.
///
/// All state lives in a `parking_lot::RwLock<MemState>` for lock-free reads.
/// When `journal_path` is configured, state-change events are sent to a
/// background writer thread that batch-writes them to a SQLite WAL journal.
pub struct JournaledStorage {
    state: Arc<RwLock<MemState>>,
    journal_tx: Option<tokio::sync::mpsc::Sender<JournalEntry>>,
    writer_handle: Option<tokio::task::JoinHandle<()>>,
    next_seq: AtomicU64,
    #[allow(dead_code)]
    flush_watermark: Arc<AtomicU64>,
    journal_dead: Arc<AtomicBool>,
    #[allow(dead_code)]
    config: JournalConfig,
}

impl JournaledStorage {
    /// Create a new in-memory journaled storage (no persistence).
    pub fn new() -> Self {
        Self::with_config(JournalConfig::default())
    }

    /// Create with explicit configuration (in-memory only if journal_path is None).
    pub fn with_config(config: JournalConfig) -> Self {
        Self {
            state: Arc::new(RwLock::new(MemState::new())),
            journal_tx: None,
            writer_handle: None,
            next_seq: AtomicU64::new(1),
            flush_watermark: Arc::new(AtomicU64::new(0)),
            journal_dead: Arc::new(AtomicBool::new(false)),
            config,
        }
    }

    /// Open journaled storage with optional durability.
    ///
    /// If `config.journal_path` is `Some`, runs recovery (loading any existing
    /// snapshot and replaying the journal), then spawns a background writer
    /// thread that batch-flushes events to a SQLite WAL journal. If `None`,
    /// operates in memory-only mode (identical to `new()`).
    pub fn open(config: JournalConfig) -> Result<Self, StorageError> {
        let flush_watermark = Arc::new(AtomicU64::new(0));
        let journal_dead = Arc::new(AtomicBool::new(false));

        // Determine snapshot path: explicit or derived from journal path
        let snapshot_path = config.snapshot_path.clone().or_else(|| {
            config
                .journal_path
                .as_ref()
                .map(|p| p.with_file_name("snapshot.db"))
        });

        // Run recovery if journal or snapshot exists
        let (recovered_state, next_seq) = if let Some(ref journal_path) = config.journal_path {
            let snap_ref = snapshot_path.as_deref();
            let journal_exists = journal_path.exists();
            let snapshot_exists = snap_ref.is_some_and(|p| p.exists());

            if journal_exists || snapshot_exists {
                recovery::recover(snap_ref, journal_path)?
            } else {
                (MemState::new(), 1)
            }
        } else {
            (MemState::new(), 1)
        };

        let state = Arc::new(RwLock::new(recovered_state));

        let (journal_tx, writer_handle) = if let Some(ref path) = config.journal_path {
            let (tx, rx) = tokio::sync::mpsc::channel(config.channel_capacity);
            let writer_config = WriterConfig {
                flush_interval: config.flush_interval,
                max_batch_size: config.max_batch_size,
                snapshot_interval: config.snapshot_interval,
                snapshot_time_interval: config.snapshot_time_interval,
            };
            let writer = JournalWriter::new(
                rx,
                path,
                writer_config,
                Arc::clone(&flush_watermark),
                Arc::clone(&journal_dead),
                Arc::clone(&state),
                snapshot_path,
            )
            .map_err(|e| StorageError::Internal(format!("failed to open journal: {e}")))?;

            let handle = tokio::spawn(writer.run());
            (Some(tx), Some(handle))
        } else {
            (None, None)
        };

        Ok(Self {
            state,
            journal_tx,
            writer_handle,
            next_seq: AtomicU64::new(next_seq),
            flush_watermark,
            journal_dead,
            config,
        })
    }

    /// Check if the journal writer is still alive.
    pub fn check_journal_health(&self) -> Result<(), StorageError> {
        if self.journal_dead.load(Ordering::Acquire) {
            return Err(StorageError::Internal(
                "journal writer thread has died".into(),
            ));
        }
        Ok(())
    }

    /// Send an event to the journal writer. No-op if in memory-only mode.
    async fn emit(&self, event: JournalEvent) {
        if let Some(tx) = &self.journal_tx {
            let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
            let entry = JournalEntry {
                seq,
                event,
                created_at: chrono::Utc::now(),
            };
            if tx.send(entry).await.is_err() {
                self.journal_dead.store(true, Ordering::Release);
            }
        }
    }

    /// Send an event and wait until it is durably flushed to SQLite.
    /// Used for operations where the caller needs a durability guarantee
    /// before returning (e.g., flow submission — HTTP 200 means persisted).
    async fn emit_durable(&self, event: JournalEvent) {
        if let Some(tx) = &self.journal_tx {
            let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
            let entry = JournalEntry {
                seq,
                event,
                created_at: chrono::Utc::now(),
            };
            if tx.send(entry).await.is_err() {
                self.journal_dead.store(true, Ordering::Release);
                return;
            }
            // Poll until the writer has flushed past our sequence number.
            // The writer updates flush_watermark after each batch commit + fsync.
            while self.flush_watermark.load(Ordering::Acquire) < seq {
                tokio::time::sleep(std::time::Duration::from_micros(100)).await;
            }
        }
    }

    /// Graceful shutdown: drop the channel sender so the writer drains remaining
    /// entries and exits, then wait for the writer task to finish.
    pub async fn shutdown(&mut self) {
        // Drop sender to signal writer to finish draining
        self.journal_tx.take();
        // Wait for writer to flush remaining entries and exit
        if let Some(handle) = self.writer_handle.take() {
            let _ = handle.await;
        }
    }
}

impl Default for JournaledStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for JournaledStorage {
    // ---- Queue CRUD ----

    async fn create_queue(&self, queue: &Queue) -> Result<(), StorageError> {
        {
            let mut state = self.state.write();
            if state.queues.contains_key(&queue.id) {
                return Err(StorageError::QueueAlreadyExists(queue.id.to_string()));
            }
            state.queues.insert(queue.id.clone(), queue.clone());
        }
        self.emit(JournalEvent::QueueCreated {
            queue: queue.clone(),
        })
        .await;
        Ok(())
    }

    async fn get_queue(&self, id: &QueueId) -> Result<Option<Queue>, StorageError> {
        let state = self.state.read();
        Ok(state.queues.get(id).cloned())
    }

    async fn list_queues(&self) -> Result<Vec<Queue>, StorageError> {
        let state = self.state.read();
        Ok(state.queues.values().cloned().collect())
    }

    async fn delete_queue(&self, id: &QueueId) -> Result<(), StorageError> {
        {
            let mut state = self.state.write();
            state.queues.remove(id);
        }
        self.emit(JournalEvent::QueueDeleted {
            queue_id: id.clone(),
        })
        .await;
        Ok(())
    }

    // ---- Flow CRUD ----

    async fn create_flow(
        &self,
        flow: &Flow,
        tasks: &[Task],
        deps: &HashMap<TaskId, Vec<TaskId>>,
    ) -> Result<(), StorageError> {
        {
            let mut state = self.state.write();
            state.flows.insert(flow.id.clone(), flow.clone());

            for task in tasks {
                let key = (task.id.clone(), task.flow_id.clone());
                state.tasks.insert(key, task.clone());
                state.index_add(task);
            }

            // Store dependency relationships
            for (task_id, dep_ids) in deps {
                state
                    .deps
                    .insert((task_id.clone(), flow.id.clone()), dep_ids.clone());

                // Build reverse index
                for dep_id in dep_ids {
                    state
                        .dependents
                        .entry((dep_id.clone(), flow.id.clone()))
                        .or_default()
                        .push(task_id.clone());
                }
            }
        }
        // Durable emit: wait for journal flush before returning.
        // This guarantees that an HTTP 200 means the flow is persisted.
        self.emit_durable(JournalEvent::FlowCreated {
            flow: flow.clone(),
            tasks: tasks.to_vec(),
            deps: deps.clone(),
        })
        .await;
        Ok(())
    }

    async fn get_flow(&self, id: &FlowId) -> Result<Option<Flow>, StorageError> {
        let state = self.state.read();
        Ok(state.flows.get(id).cloned())
    }

    async fn list_flows(
        &self,
        queue_id: &QueueId,
        filter_state: Option<FlowState>,
    ) -> Result<Vec<Flow>, StorageError> {
        let state = self.state.read();
        Ok(state
            .flows
            .values()
            .filter(|f| f.queue_id == *queue_id && filter_state.is_none_or(|s| f.state == s))
            .cloned()
            .collect())
    }

    async fn update_flow_state(
        &self,
        id: &FlowId,
        new_state: FlowState,
    ) -> Result<(), StorageError> {
        let updated_at;
        {
            let mut state = self.state.write();
            let flow = state
                .flows
                .get_mut(id)
                .ok_or_else(|| StorageError::FlowNotFound(id.to_string()))?;
            flow.state = new_state;
            flow.updated_at = Utc::now();
            updated_at = flow.updated_at;
        }
        self.emit(JournalEvent::FlowStateChanged {
            flow_id: id.clone(),
            new_state,
            updated_at,
        })
        .await;
        Ok(())
    }

    async fn increment_flow_counter(
        &self,
        id: &FlowId,
        succeeded: bool,
    ) -> Result<Flow, StorageError> {
        // increment_flow_counter is always called right after a
        // complete_task_with_ready (which already emits TaskCompleted with
        // the succeeded flag), so we do NOT emit a separate event here.
        let mut state = self.state.write();
        let flow = state
            .flows
            .get_mut(id)
            .ok_or_else(|| StorageError::FlowNotFound(id.to_string()))?;
        if succeeded {
            flow.tasks_succeeded += 1;
        } else {
            flow.tasks_failed += 1;
        }
        flow.updated_at = Utc::now();
        Ok(flow.clone())
    }

    // ---- Task reads ----

    async fn get_task(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<Option<Task>, StorageError> {
        let state = self.state.read();
        Ok(state
            .tasks
            .get(&(task_id.clone(), flow_id.clone()))
            .cloned())
    }

    async fn get_flow_tasks(&self, flow_id: &FlowId) -> Result<Vec<Task>, StorageError> {
        let state = self.state.read();
        Ok(state
            .tasks
            .values()
            .filter(|t| t.flow_id == *flow_id)
            .cloned()
            .collect())
    }

    async fn get_flow_with_tasks(
        &self,
        flow_id: &FlowId,
    ) -> Result<Option<(Flow, Vec<Task>)>, StorageError> {
        let state = self.state.read();
        let flow = match state.flows.get(flow_id) {
            Some(f) => f.clone(),
            None => return Ok(None),
        };
        let tasks: Vec<Task> = state
            .tasks
            .values()
            .filter(|t| t.flow_id == *flow_id)
            .cloned()
            .collect();
        Ok(Some((flow, tasks)))
    }

    // ---- Indexed fetch operations ----

    async fn fetch_ready_tasks(
        &self,
        queue_id: &QueueId,
        limit: usize,
    ) -> Result<Vec<Task>, StorageError> {
        let state = self.state.read();
        let Some(set) = state.ready_index.get(queue_id) else {
            return Ok(vec![]);
        };
        let mut result = Vec::with_capacity(limit.min(set.len()));
        for (_, tid_str, fid_str) in set.iter() {
            if result.len() >= limit {
                break;
            }
            let key = (
                TaskId::from(tid_str.as_str()),
                FlowId::from(fid_str.as_str()),
            );
            if let Some(task) = state.tasks.get(&key) {
                result.push(task.clone());
            }
        }
        Ok(result)
    }

    async fn fetch_delayed_tasks_due(&self) -> Result<Vec<Task>, StorageError> {
        let now = Utc::now();
        let state = self.state.read();
        let mut result = Vec::new();
        for (retry_at, tid_str, fid_str) in state.delayed_index.iter() {
            if *retry_at > now {
                break;
            }
            let key = (
                TaskId::from(tid_str.as_str()),
                FlowId::from(fid_str.as_str()),
            );
            if let Some(task) = state.tasks.get(&key) {
                result.push(task.clone());
            }
        }
        Ok(result)
    }

    async fn fetch_timed_out_tasks(&self) -> Result<Vec<Task>, StorageError> {
        let now = Utc::now();
        let state = self.state.read();
        let mut result = Vec::new();
        for key in &state.running_index {
            if let Some(task) = state.tasks.get(key)
                && task
                    .started_at
                    .is_some_and(|s| (now - s).num_seconds() as u64 > task.timeout_secs)
            {
                result.push(task.clone());
            }
        }
        Ok(result)
    }

    // ---- Task state mutations ----

    async fn update_task_state(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        new_state: TaskState,
    ) -> Result<(), StorageError> {
        {
            let mut state = self.state.write();
            let key = (task_id.clone(), flow_id.clone());
            state.transition_task_state(&key, new_state)?;
        }
        self.emit(JournalEvent::TaskStateChanged {
            task_id: task_id.clone(),
            flow_id: flow_id.clone(),
            new_state,
            retry_at: None,
            started_at: None,
            retries_remaining: None,
        })
        .await;
        Ok(())
    }

    async fn mark_task_running(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<(), StorageError> {
        let started_at;
        {
            let mut state = self.state.write();
            let key = (task_id.clone(), flow_id.clone());

            let task = state.tasks.get(&key).ok_or_else(|| {
                StorageError::TaskNotFound(task_id.to_string(), flow_id.to_string())
            })?;

            if !task.state.can_transition_to(TaskState::Running) {
                return Err(StorageError::InvalidStateTransition(
                    task.state.to_string(),
                    TaskState::Running.to_string(),
                ));
            }

            let old_snapshot = task.clone();
            state.index_remove(&old_snapshot);

            let task = state.tasks.get_mut(&key).expect("checked above");
            task.state = TaskState::Running;
            task.started_at = Some(Utc::now());
            started_at = task.started_at;

            let new_snapshot = task.clone();
            state.index_add(&new_snapshot);
        }
        self.emit(JournalEvent::TaskStateChanged {
            task_id: task_id.clone(),
            flow_id: flow_id.clone(),
            new_state: TaskState::Running,
            retry_at: None,
            started_at,
            retries_remaining: None,
        })
        .await;
        Ok(())
    }

    async fn mark_tasks_running_batch(
        &self,
        tasks: &[(&TaskId, &FlowId)],
    ) -> Result<Vec<(TaskId, FlowId)>, StorageError> {
        let succeeded;
        let now = Utc::now();
        {
            let mut state = self.state.write();
            let mut batch_succeeded = Vec::with_capacity(tasks.len());

            for &(task_id, flow_id) in tasks {
                let key = (task_id.clone(), flow_id.clone());
                let Some(task) = state.tasks.get(&key) else {
                    continue;
                };
                if !task.state.can_transition_to(TaskState::Running) {
                    continue;
                }

                let old_snapshot = task.clone();
                state.index_remove(&old_snapshot);

                let task = state.tasks.get_mut(&key).expect("checked above");
                task.state = TaskState::Running;
                task.started_at = Some(now);

                let new_snapshot = task.clone();
                state.index_add(&new_snapshot);

                batch_succeeded.push((task_id.clone(), flow_id.clone()));
            }
            succeeded = batch_succeeded;
        }
        // Emit individual events for each successfully transitioned task
        for (task_id, flow_id) in &succeeded {
            self.emit(JournalEvent::TaskStateChanged {
                task_id: task_id.clone(),
                flow_id: flow_id.clone(),
                new_state: TaskState::Running,
                retry_at: None,
                started_at: Some(now),
                retries_remaining: None,
            })
            .await;
        }
        Ok(succeeded)
    }

    async fn set_task_output(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: serde_json::Value,
    ) -> Result<(), StorageError> {
        {
            let mut state = self.state.write();
            let key = (task_id.clone(), flow_id.clone());
            let task = state.tasks.get_mut(&key).ok_or_else(|| {
                StorageError::TaskNotFound(task_id.to_string(), flow_id.to_string())
            })?;
            task.output = Some(output.clone());
        }
        self.emit(JournalEvent::TaskOutputSet {
            task_id: task_id.clone(),
            flow_id: flow_id.clone(),
            output,
        })
        .await;
        Ok(())
    }

    async fn mark_task_succeeded(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: Option<serde_json::Value>,
    ) -> Result<(), StorageError> {
        let completed_at;
        {
            let mut state = self.state.write();
            let key = (task_id.clone(), flow_id.clone());

            let task = state.tasks.get(&key).ok_or_else(|| {
                StorageError::TaskNotFound(task_id.to_string(), flow_id.to_string())
            })?;

            if !task.state.can_transition_to(TaskState::Succeeded) {
                return Err(StorageError::InvalidStateTransition(
                    task.state.to_string(),
                    TaskState::Succeeded.to_string(),
                ));
            }

            let old_snapshot = task.clone();
            state.index_remove(&old_snapshot);

            let task = state.tasks.get_mut(&key).expect("checked above");
            task.state = TaskState::Succeeded;
            task.output = output.clone();
            task.completed_at = Some(Utc::now());
            completed_at = task.completed_at.unwrap();
        }
        self.emit(JournalEvent::TaskCompleted {
            task_id: task_id.clone(),
            flow_id: flow_id.clone(),
            new_state: TaskState::Succeeded,
            output,
            error: None,
            completed_at,
            succeeded: true,
            newly_ready: vec![],
        })
        .await;
        Ok(())
    }

    async fn mark_task_failed(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        error: &str,
    ) -> Result<(), StorageError> {
        let completed_at;
        {
            let mut state = self.state.write();
            let key = (task_id.clone(), flow_id.clone());

            let task = state.tasks.get(&key).ok_or_else(|| {
                StorageError::TaskNotFound(task_id.to_string(), flow_id.to_string())
            })?;

            if !task.state.can_transition_to(TaskState::Failed) {
                return Err(StorageError::InvalidStateTransition(
                    task.state.to_string(),
                    TaskState::Failed.to_string(),
                ));
            }

            let old_snapshot = task.clone();
            state.index_remove(&old_snapshot);

            let task = state.tasks.get_mut(&key).expect("checked above");
            task.state = TaskState::Failed;
            task.error = Some(error.to_string());
            task.completed_at = Some(Utc::now());
            completed_at = task.completed_at.unwrap();
        }
        self.emit(JournalEvent::TaskCompleted {
            task_id: task_id.clone(),
            flow_id: flow_id.clone(),
            new_state: TaskState::Failed,
            output: None,
            error: Some(error.to_string()),
            completed_at,
            succeeded: false,
            newly_ready: vec![],
        })
        .await;
        Ok(())
    }

    async fn mark_task_delayed(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        retry_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StorageError> {
        let retries_remaining;
        {
            let mut state = self.state.write();
            let key = (task_id.clone(), flow_id.clone());

            let task = state.tasks.get(&key).ok_or_else(|| {
                StorageError::TaskNotFound(task_id.to_string(), flow_id.to_string())
            })?;

            if !task.state.can_transition_to(TaskState::Delayed) {
                return Err(StorageError::InvalidStateTransition(
                    task.state.to_string(),
                    TaskState::Delayed.to_string(),
                ));
            }

            let old_snapshot = task.clone();
            state.index_remove(&old_snapshot);

            let task = state.tasks.get_mut(&key).expect("checked above");
            task.state = TaskState::Delayed;
            task.retry_at = Some(retry_at);
            task.retries_remaining = task.retries_remaining.saturating_sub(1);
            task.started_at = None;
            retries_remaining = task.retries_remaining;

            let new_snapshot = task.clone();
            state.index_add(&new_snapshot);
        }
        self.emit(JournalEvent::TaskStateChanged {
            task_id: task_id.clone(),
            flow_id: flow_id.clone(),
            new_state: TaskState::Delayed,
            retry_at: Some(retry_at),
            started_at: None,
            retries_remaining: Some(retries_remaining),
        })
        .await;
        Ok(())
    }

    // ---- Dependencies ----

    async fn get_flow_dependencies(
        &self,
        flow_id: &FlowId,
    ) -> Result<HashMap<TaskId, Vec<TaskId>>, StorageError> {
        let state = self.state.read();
        let mut result: HashMap<TaskId, Vec<TaskId>> = HashMap::new();
        for ((task_id, fid), dep_ids) in &state.deps {
            if fid == flow_id {
                result.insert(task_id.clone(), dep_ids.clone());
            }
        }
        Ok(result)
    }

    async fn get_task_dependencies(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<Vec<TaskId>, StorageError> {
        let state = self.state.read();
        Ok(state
            .deps
            .get(&(task_id.clone(), flow_id.clone()))
            .cloned()
            .unwrap_or_default())
    }

    async fn get_task_dependents(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<Vec<TaskId>, StorageError> {
        let state = self.state.read();
        Ok(state
            .dependents
            .get(&(task_id.clone(), flow_id.clone()))
            .cloned()
            .unwrap_or_default())
    }

    async fn resolve_ready_tasks(&self, flow_id: &FlowId) -> Result<Vec<TaskId>, StorageError> {
        let newly_ready;
        {
            let mut state = self.state.write();
            let mut ready = Vec::new();

            // Find all pending tasks in this flow
            let pending_tasks: Vec<TaskId> = state
                .tasks
                .values()
                .filter(|t| t.flow_id == *flow_id && t.state == TaskState::Pending)
                .map(|t| t.id.clone())
                .collect();

            for task_id in pending_tasks {
                let dep_ids = state
                    .deps
                    .get(&(task_id.clone(), flow_id.clone()))
                    .cloned()
                    .unwrap_or_default();

                // Check if all dependencies have succeeded
                let all_deps_succeeded = dep_ids.iter().all(|dep_id| {
                    state
                        .tasks
                        .get(&(dep_id.clone(), flow_id.clone()))
                        .is_some_and(|t| t.state == TaskState::Succeeded)
                });

                if all_deps_succeeded {
                    let key = (task_id.clone(), flow_id.clone());
                    if let Some(task) = state.tasks.get_mut(&key) {
                        task.state = TaskState::Ready;
                        let snapshot = task.clone();
                        state.index_add(&snapshot);
                        ready.push(task_id);
                    }
                }
            }
            newly_ready = ready;
        }
        // Emit individual TaskStateChanged for each newly-ready task
        for task_id in &newly_ready {
            self.emit(JournalEvent::TaskStateChanged {
                task_id: task_id.clone(),
                flow_id: flow_id.clone(),
                new_state: TaskState::Ready,
                retry_at: None,
                started_at: None,
                retries_remaining: None,
            })
            .await;
        }
        Ok(newly_ready)
    }

    // ---- Composite operations (single write lock) ----

    async fn complete_task_with_ready(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: Option<serde_json::Value>,
        newly_ready: &[TaskId],
    ) -> Result<Flow, StorageError> {
        let (flow, completed_at);
        {
            let mut state = self.state.write();
            let now = Utc::now();

            // 1. Mark task succeeded
            let key = (task_id.clone(), flow_id.clone());
            {
                let task = state.tasks.get(&key).ok_or_else(|| {
                    StorageError::TaskNotFound(task_id.to_string(), flow_id.to_string())
                })?;

                if !task.state.can_transition_to(TaskState::Succeeded) {
                    return Err(StorageError::InvalidStateTransition(
                        task.state.to_string(),
                        TaskState::Succeeded.to_string(),
                    ));
                }

                let old_snapshot = task.clone();
                state.index_remove(&old_snapshot);
            }

            let task = state.tasks.get_mut(&key).expect("checked above");
            task.state = TaskState::Succeeded;
            task.output = output.clone();
            task.completed_at = Some(now);
            completed_at = now;
            // Succeeded is not indexed, no index_add

            // 2. Increment flow counter
            let f = state
                .flows
                .get_mut(flow_id)
                .ok_or_else(|| StorageError::FlowNotFound(flow_id.to_string()))?;
            f.tasks_succeeded += 1;
            f.updated_at = now;

            // 3. Promote newly-ready tasks
            for tid in newly_ready {
                let dep_key = (tid.clone(), flow_id.clone());
                if let Some(dep_task) = state.tasks.get(&dep_key)
                    && dep_task.state == TaskState::Pending
                {
                    let old_snapshot = dep_task.clone();
                    state.index_remove(&old_snapshot);

                    let dep_task = state.tasks.get_mut(&dep_key).expect("checked above");
                    dep_task.state = TaskState::Ready;

                    let new_snapshot = dep_task.clone();
                    state.index_add(&new_snapshot);
                }
            }

            flow = state.flows.get(flow_id).expect("checked above").clone();
        }
        self.emit(JournalEvent::TaskCompleted {
            task_id: task_id.clone(),
            flow_id: flow_id.clone(),
            new_state: TaskState::Succeeded,
            output,
            error: None,
            completed_at,
            succeeded: true,
            newly_ready: newly_ready.to_vec(),
        })
        .await;
        Ok(flow)
    }

    async fn complete_tasks_with_ready_batch(
        &self,
        completions: &[(TaskId, FlowId, Option<serde_json::Value>, Vec<TaskId>)],
    ) -> Result<Vec<Option<Flow>>, StorageError> {
        let results;
        let events;
        {
            let mut state = self.state.write();
            let now = Utc::now();
            let mut batch_results = Vec::with_capacity(completions.len());
            let mut batch_events = Vec::new();

            for (task_id, flow_id, output, newly_ready) in completions {
                let key = (task_id.clone(), flow_id.clone());

                // 1. Mark task succeeded
                let valid = match state.tasks.get(&key) {
                    Some(task) => {
                        if !task.state.can_transition_to(TaskState::Succeeded) {
                            batch_results.push(None);
                            continue;
                        }
                        let old_snapshot = task.clone();
                        state.index_remove(&old_snapshot);
                        true
                    }
                    None => {
                        batch_results.push(None);
                        continue;
                    }
                };

                if valid {
                    let task = state.tasks.get_mut(&key).expect("checked above");
                    task.state = TaskState::Succeeded;
                    task.output = output.clone();
                    task.completed_at = Some(now);
                }

                // 2. Increment flow counter
                let flow = match state.flows.get_mut(flow_id) {
                    Some(f) => {
                        f.tasks_succeeded += 1;
                        f.updated_at = now;
                        f.clone()
                    }
                    None => {
                        return Err(StorageError::FlowNotFound(flow_id.to_string()));
                    }
                };

                // 3. Promote newly-ready tasks
                for tid in newly_ready {
                    let dep_key = (tid.clone(), flow_id.clone());
                    if let Some(dep_task) = state.tasks.get(&dep_key)
                        && dep_task.state == TaskState::Pending
                    {
                        let old_snapshot = dep_task.clone();
                        state.index_remove(&old_snapshot);

                        let dep_task = state.tasks.get_mut(&dep_key).expect("checked above");
                        dep_task.state = TaskState::Ready;

                        let new_snapshot = dep_task.clone();
                        state.index_add(&new_snapshot);
                    }
                }

                batch_events.push(JournalEvent::TaskCompleted {
                    task_id: task_id.clone(),
                    flow_id: flow_id.clone(),
                    new_state: TaskState::Succeeded,
                    output: output.clone(),
                    error: None,
                    completed_at: now,
                    succeeded: true,
                    newly_ready: newly_ready.clone(),
                });

                batch_results.push(Some(flow));
            }
            results = batch_results;
            events = batch_events;
        }
        for event in events {
            self.emit(event).await;
        }
        Ok(results)
    }

    // ---- Inject tasks ----

    async fn inject_tasks(
        &self,
        flow_id: &FlowId,
        tasks: &[Task],
        deps: &HashMap<TaskId, Vec<TaskId>>,
    ) -> Result<Flow, StorageError> {
        let (flow, new_task_count);
        {
            let mut state = self.state.write();

            // Insert tasks + indexes
            for task in tasks {
                let key = (task.id.clone(), task.flow_id.clone());
                state.tasks.insert(key, task.clone());
                state.index_add(task);
            }

            // Insert deps and reverse index
            for (task_id, dep_ids) in deps {
                state
                    .deps
                    .insert((task_id.clone(), flow_id.clone()), dep_ids.clone());

                for dep_id in dep_ids {
                    state
                        .dependents
                        .entry((dep_id.clone(), flow_id.clone()))
                        .or_default()
                        .push(task_id.clone());
                }
            }

            // Update flow task_count
            new_task_count = tasks.len();
            let f = state
                .flows
                .get_mut(flow_id)
                .ok_or_else(|| StorageError::FlowNotFound(flow_id.to_string()))?;
            f.task_count += new_task_count;
            f.updated_at = Utc::now();

            flow = f.clone();
        }
        self.emit(JournalEvent::TasksInjected {
            flow_id: flow_id.clone(),
            tasks: tasks.to_vec(),
            deps: deps.clone(),
            new_task_count,
        })
        .await;
        Ok(flow)
    }

    // ---- Child flows ----

    async fn get_child_flow_ids(
        &self,
        parent_flow_id: &FlowId,
    ) -> Result<Vec<FlowId>, StorageError> {
        let state = self.state.read();
        Ok(state
            .flows
            .values()
            .filter(|f| f.parent_flow_id.as_ref() == Some(parent_flow_id))
            .map(|f| f.id.clone())
            .collect())
    }

    // ---- Cleanup ----

    async fn delete_terminal_flows_before(
        &self,
        queue_id: &QueueId,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<usize, StorageError> {
        let (count, flow_ids);
        {
            let mut state = self.state.write();

            // Find flow IDs to delete
            let to_delete: Vec<FlowId> = state
                .flows
                .values()
                .filter(|f| {
                    f.queue_id == *queue_id && f.state.is_terminal() && f.updated_at < cutoff
                })
                .map(|f| f.id.clone())
                .collect();

            count = to_delete.len();

            for flow_id in &to_delete {
                state.flows.remove(flow_id);

                // Collect task keys for this flow so we can remove from indexes
                let task_keys: Vec<(TaskId, FlowId)> = state
                    .tasks
                    .keys()
                    .filter(|(_, fid)| fid == flow_id)
                    .cloned()
                    .collect();

                for key in &task_keys {
                    if let Some(task) = state.tasks.remove(key) {
                        state.index_remove_all(&task);
                    }
                }

                // Remove deps/dependents belonging to this flow
                state.deps.retain(|(_tid, fid), _| fid != flow_id);
                state.dependents.retain(|(_tid, fid), _| fid != flow_id);
            }
            flow_ids = to_delete;
        }
        if !flow_ids.is_empty() {
            self.emit(JournalEvent::FlowsDeleted {
                queue_id: queue_id.clone(),
                flow_ids,
            })
            .await;
        }
        Ok(count)
    }

    // ---- Schedules ----

    async fn create_schedule(&self, schedule: &Schedule) -> Result<(), StorageError> {
        {
            let mut state = self.state.write();
            state
                .schedules
                .insert(schedule.id.clone(), schedule.clone());
        }
        self.emit(JournalEvent::ScheduleCreated {
            schedule: schedule.clone(),
        })
        .await;
        Ok(())
    }

    async fn get_schedule(&self, id: &ScheduleId) -> Result<Option<Schedule>, StorageError> {
        let state = self.state.read();
        Ok(state.schedules.get(id).cloned())
    }

    async fn list_schedules(&self, queue_id: &QueueId) -> Result<Vec<Schedule>, StorageError> {
        let state = self.state.read();
        let mut schedules: Vec<Schedule> = state
            .schedules
            .values()
            .filter(|s| s.queue_id == *queue_id)
            .cloned()
            .collect();
        schedules.sort_by_key(|s| s.created_at);
        Ok(schedules)
    }

    async fn update_schedule(&self, schedule: &Schedule) -> Result<(), StorageError> {
        {
            let mut state = self.state.write();
            if !state.schedules.contains_key(&schedule.id) {
                return Err(StorageError::ScheduleNotFound(schedule.id.to_string()));
            }
            state
                .schedules
                .insert(schedule.id.clone(), schedule.clone());
        }
        self.emit(JournalEvent::ScheduleUpdated {
            schedule: schedule.clone(),
        })
        .await;
        Ok(())
    }

    async fn delete_schedule(&self, id: &ScheduleId) -> Result<(), StorageError> {
        {
            let mut state = self.state.write();
            state.schedules.remove(id);
        }
        self.emit(JournalEvent::ScheduleDeleted {
            schedule_id: id.clone(),
        })
        .await;
        Ok(())
    }

    async fn fetch_due_schedules(&self) -> Result<Vec<Schedule>, StorageError> {
        let now = Utc::now();
        let state = self.state.read();
        Ok(state
            .schedules
            .values()
            .filter(|s| s.enabled && s.next_run_at.is_some_and(|nr| nr <= now))
            .cloned()
            .collect())
    }

    async fn mark_schedule_triggered(
        &self,
        id: &ScheduleId,
        triggered_at: chrono::DateTime<chrono::Utc>,
        next_run_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), StorageError> {
        {
            let mut state = self.state.write();
            let schedule = state
                .schedules
                .get_mut(id)
                .ok_or_else(|| StorageError::ScheduleNotFound(id.to_string()))?;
            schedule.last_triggered_at = Some(triggered_at);
            schedule.next_run_at = next_run_at;
            schedule.updated_at = Utc::now();
        }
        self.emit(JournalEvent::ScheduleTriggered {
            schedule_id: id.clone(),
            triggered_at,
            next_run_at,
        })
        .await;
        Ok(())
    }

    async fn health_check(&self) -> Result<(), StorageError> {
        self.check_journal_health()
    }
}

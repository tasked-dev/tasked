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
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use config::JournalConfig;
use events::{JournalEntry, JournalEvent};
use state::MemState;
use writer::{JournalWriter, WriterConfig};

/// How long shutdown/Drop waits for the writer thread to drain and exit.
const SHUTDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Journaled storage engine.
///
/// All state lives in a `parking_lot::RwLock<MemState>` for lock-free reads.
/// When `journal_path` is configured, state-change events are sent to a
/// dedicated background writer thread that batch-writes them to a SQLite WAL
/// journal.
///
/// Ordering invariant: the journal sequence number is allocated and the event
/// is enqueued to the writer channel *while the state write lock is held*
/// (see [`Self::emit_locked`]), so journal replay order always equals the
/// order in which mutations were applied to memory.
pub struct JournaledStorage {
    state: Arc<RwLock<MemState>>,
    /// Sender to the writer thread. `None` in memory-only mode or after
    /// shutdown. RwLock so `shutdown(&self)` can take it with interior
    /// mutability while the hot path only needs a read lock.
    journal_tx: RwLock<Option<tokio::sync::mpsc::UnboundedSender<JournalEntry>>>,
    writer_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    next_seq: AtomicU64,
    /// Watermark of the last fsync'd journal sequence, published by the
    /// writer after each committed batch. Used by emit_durable waits.
    watermark_rx: Option<tokio::sync::watch::Receiver<u64>>,
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
            journal_tx: RwLock::new(None),
            writer_handle: Mutex::new(None),
            next_seq: AtomicU64::new(1),
            watermark_rx: None,
            journal_dead: Arc::new(AtomicBool::new(false)),
            config,
        }
    }

    /// Open journaled storage with optional durability.
    ///
    /// If `config.journal_path` is `Some`, runs recovery (loading any existing
    /// snapshot and replaying the journal), then spawns a dedicated OS thread
    /// that batch-flushes events to a SQLite WAL journal. If `None`, operates
    /// in memory-only mode (identical to `new()`).
    pub fn open(config: JournalConfig) -> Result<Self, StorageError> {
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

        let (journal_tx, writer_handle, watermark_rx) = if let Some(ref path) = config.journal_path
        {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let (watermark_tx, watermark_rx) = tokio::sync::watch::channel(0u64);
            let writer_config = WriterConfig {
                max_batch_size: config.max_batch_size,
                snapshot_interval: config.snapshot_interval,
                snapshot_time_interval: config.snapshot_time_interval,
            };
            let writer = JournalWriter::new(
                rx,
                path,
                writer_config,
                watermark_tx,
                Arc::clone(&journal_dead),
                Arc::clone(&state),
                snapshot_path,
            )
            .map_err(|e| StorageError::Internal(format!("failed to open journal: {e}")))?;

            // The writer performs synchronous SQLite commits (fsyncs), so it
            // runs on its own OS thread; the channel is the async boundary.
            let handle = std::thread::Builder::new()
                .name("tasked-journal-writer".into())
                .spawn(move || writer.run())
                .map_err(|e| {
                    StorageError::Internal(format!("failed to spawn journal writer: {e}"))
                })?;
            (Some(tx), Some(handle), Some(watermark_rx))
        } else {
            (None, None, None)
        };

        Ok(Self {
            state,
            journal_tx: RwLock::new(journal_tx),
            writer_handle: Mutex::new(writer_handle),
            next_seq: AtomicU64::new(next_seq),
            watermark_rx,
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

    /// Fail fast before mutating: once the journal writer has died, accepting
    /// writes would silently lose them (memory would change but nothing would
    /// be journaled).
    fn ensure_journal_alive(&self) -> Result<(), StorageError> {
        if self.journal_dead.load(Ordering::Acquire) {
            return Err(StorageError::Internal(
                "journal writer thread has died; rejecting write".into(),
            ));
        }
        Ok(())
    }

    /// Allocate a sequence number and enqueue the event to the writer.
    ///
    /// MUST be called while the state write lock is held (the `_state`
    /// parameter exists to enforce this at call sites): this guarantees that
    /// the journal order equals the order mutations were applied, and that
    /// the flush watermark never claims durability for an event whose
    /// mutation is not yet visible.
    ///
    /// Returns the allocated sequence number, or `None` in memory-only mode.
    fn emit_locked(
        &self,
        _state: &MemState,
        event: JournalEvent,
    ) -> Result<Option<u64>, StorageError> {
        let tx_guard = self.journal_tx.read();
        let Some(tx) = tx_guard.as_ref() else {
            return Ok(None);
        };
        if self.journal_dead.load(Ordering::Acquire) {
            return Err(StorageError::Internal(
                "journal writer thread has died; rejecting write".into(),
            ));
        }
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let entry = JournalEntry {
            seq,
            event,
            created_at: chrono::Utc::now(),
        };
        if tx.send(entry).is_err() {
            self.journal_dead.store(true, Ordering::Release);
            return Err(StorageError::Internal(
                "journal writer thread has died; write not journaled".into(),
            ));
        }
        Ok(Some(seq))
    }

    /// Wait until the writer has fsync'd past `seq`.
    ///
    /// Used for operations where the caller needs a durability guarantee
    /// before returning (e.g. flow submission — HTTP 200 means persisted).
    /// Returns an error instead of hanging if the writer dies.
    async fn wait_durable(&self, seq: Option<u64>) -> Result<(), StorageError> {
        let Some(seq) = seq else { return Ok(()) };
        let Some(rx) = &self.watermark_rx else {
            return Ok(());
        };
        let mut rx = rx.clone();
        loop {
            if *rx.borrow() >= seq {
                return Ok(());
            }
            if self.journal_dead.load(Ordering::Acquire) {
                return Err(StorageError::Internal(
                    "journal writer died before the write was made durable".into(),
                ));
            }
            // The writer publishes the watermark after each fsync'd batch and
            // drops the sender when it exits, so this cannot hang forever.
            if rx.changed().await.is_err() {
                if *rx.borrow() >= seq {
                    return Ok(());
                }
                return Err(StorageError::Internal(
                    "journal writer died before the write was made durable".into(),
                ));
            }
        }
    }

    /// Graceful shutdown: drop the channel sender so the writer drains
    /// remaining entries and exits, then wait (bounded) for the writer
    /// thread to finish.
    pub async fn shutdown(&self) {
        // Drop sender to signal the writer to finish draining.
        drop(self.journal_tx.write().take());
        let handle = self.writer_handle.lock().take();
        if let Some(handle) = handle {
            let deadline = std::time::Instant::now() + SHUTDOWN_JOIN_TIMEOUT;
            while !handle.is_finished() && std::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            if handle.is_finished() {
                let _ = handle.join();
            } else {
                tracing::warn!("journal writer did not exit within shutdown timeout; detaching");
            }
        }
    }

    /// Synchronous best-effort shutdown used by Drop.
    fn shutdown_blocking(&self) {
        drop(self.journal_tx.write().take());
        let handle = self.writer_handle.lock().take();
        if let Some(handle) = handle {
            let deadline = std::time::Instant::now() + SHUTDOWN_JOIN_TIMEOUT;
            while !handle.is_finished() && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            if handle.is_finished() {
                let _ = handle.join();
            } else {
                tracing::warn!("journal writer did not exit within drop timeout; detaching");
            }
        }
    }
}

impl Drop for JournaledStorage {
    fn drop(&mut self) {
        self.shutdown_blocking();
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
        self.ensure_journal_alive()?;
        let seq;
        {
            let mut state = self.state.write();
            if state.queues.contains_key(&queue.id) {
                return Err(StorageError::QueueAlreadyExists(queue.id.to_string()));
            }
            state.queues.insert(queue.id.clone(), queue.clone());
            seq = self.emit_locked(
                &state,
                JournalEvent::QueueCreated {
                    queue: queue.clone(),
                },
            )?;
        }
        // Queue creation is rare and structural: make it durable like
        // create_flow so an acknowledged queue survives a crash.
        self.wait_durable(seq).await
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
        self.ensure_journal_alive()?;
        {
            let mut state = self.state.write();
            // Cascade: delete the queue's flows, tasks, deps, and schedules,
            // maintaining secondary indexes. Replay of QueueDeleted performs
            // the same cascade so recovery matches.
            state.remove_queue_cascade(id);
            self.emit_locked(
                &state,
                JournalEvent::QueueDeleted {
                    queue_id: id.clone(),
                },
            )?;
        }
        Ok(())
    }

    // ---- Flow CRUD ----

    async fn create_flow(
        &self,
        flow: &Flow,
        tasks: &[Task],
        deps: &HashMap<TaskId, Vec<TaskId>>,
    ) -> Result<(), StorageError> {
        self.ensure_journal_alive()?;
        let seq;
        {
            let mut state = self.state.write();
            if state.flows.contains_key(&flow.id) {
                return Err(StorageError::Internal(format!(
                    "flow '{}' already exists",
                    flow.id
                )));
            }
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

            seq = self.emit_locked(
                &state,
                JournalEvent::FlowCreated {
                    flow: flow.clone(),
                    tasks: tasks.to_vec(),
                    deps: deps.clone(),
                },
            )?;
        }
        // Durable emit: wait for journal flush before returning.
        // This guarantees that an HTTP 200 means the flow is persisted.
        self.wait_durable(seq).await
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
        self.ensure_journal_alive()?;
        {
            let mut state = self.state.write();
            let flow = state
                .flows
                .get_mut(id)
                .ok_or_else(|| StorageError::FlowNotFound(id.to_string()))?;
            flow.state = new_state;
            flow.updated_at = Utc::now();
            let updated_at = flow.updated_at;
            self.emit_locked(
                &state,
                JournalEvent::FlowStateChanged {
                    flow_id: id.clone(),
                    new_state,
                    updated_at,
                },
            )?;
        }
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
        // Recovery additionally recomputes counters from task states.
        self.ensure_journal_alive()?;
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
        self.ensure_journal_alive()?;
        {
            let mut state = self.state.write();
            let key = (task_id.clone(), flow_id.clone());
            state.transition_task_state(&key, new_state)?;
            self.emit_locked(
                &state,
                JournalEvent::TaskStateChanged {
                    task_id: task_id.clone(),
                    flow_id: flow_id.clone(),
                    new_state,
                    retry_at: None,
                    started_at: None,
                    retries_remaining: None,
                },
            )?;
        }
        Ok(())
    }

    async fn mark_task_running(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<(), StorageError> {
        self.ensure_journal_alive()?;
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
            let started_at = task.started_at;

            let new_snapshot = task.clone();
            state.index_add(&new_snapshot);

            self.emit_locked(
                &state,
                JournalEvent::TaskStateChanged {
                    task_id: task_id.clone(),
                    flow_id: flow_id.clone(),
                    new_state: TaskState::Running,
                    retry_at: None,
                    started_at,
                    retries_remaining: None,
                },
            )?;
        }
        Ok(())
    }

    async fn mark_tasks_running_batch(
        &self,
        tasks: &[(&TaskId, &FlowId)],
    ) -> Result<Vec<(TaskId, FlowId)>, StorageError> {
        self.ensure_journal_alive()?;
        let now = Utc::now();
        let succeeded;
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

                self.emit_locked(
                    &state,
                    JournalEvent::TaskStateChanged {
                        task_id: task_id.clone(),
                        flow_id: flow_id.clone(),
                        new_state: TaskState::Running,
                        retry_at: None,
                        started_at: Some(now),
                        retries_remaining: None,
                    },
                )?;

                batch_succeeded.push((task_id.clone(), flow_id.clone()));
            }
            succeeded = batch_succeeded;
        }
        Ok(succeeded)
    }

    async fn set_task_output(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: serde_json::Value,
    ) -> Result<(), StorageError> {
        self.ensure_journal_alive()?;
        {
            let mut state = self.state.write();
            let key = (task_id.clone(), flow_id.clone());
            let task = state.tasks.get_mut(&key).ok_or_else(|| {
                StorageError::TaskNotFound(task_id.to_string(), flow_id.to_string())
            })?;
            task.output = Some(output.clone());
            self.emit_locked(
                &state,
                JournalEvent::TaskOutputSet {
                    task_id: task_id.clone(),
                    flow_id: flow_id.clone(),
                    output,
                },
            )?;
        }
        Ok(())
    }

    async fn mark_task_succeeded(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: Option<serde_json::Value>,
    ) -> Result<(), StorageError> {
        self.ensure_journal_alive()?;
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
            let completed_at = task.completed_at.unwrap();

            self.emit_locked(
                &state,
                JournalEvent::TaskCompleted {
                    task_id: task_id.clone(),
                    flow_id: flow_id.clone(),
                    new_state: TaskState::Succeeded,
                    output,
                    error: None,
                    completed_at,
                    succeeded: true,
                    newly_ready: vec![],
                },
            )?;
        }
        Ok(())
    }

    async fn mark_task_failed(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        error: &str,
    ) -> Result<(), StorageError> {
        self.ensure_journal_alive()?;
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
            let completed_at = task.completed_at.unwrap();

            self.emit_locked(
                &state,
                JournalEvent::TaskCompleted {
                    task_id: task_id.clone(),
                    flow_id: flow_id.clone(),
                    new_state: TaskState::Failed,
                    output: None,
                    error: Some(error.to_string()),
                    completed_at,
                    succeeded: false,
                    newly_ready: vec![],
                },
            )?;
        }
        Ok(())
    }

    async fn mark_task_delayed(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        retry_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StorageError> {
        self.ensure_journal_alive()?;
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
            let retries_remaining = task.retries_remaining;

            let new_snapshot = task.clone();
            state.index_add(&new_snapshot);

            self.emit_locked(
                &state,
                JournalEvent::TaskStateChanged {
                    task_id: task_id.clone(),
                    flow_id: flow_id.clone(),
                    new_state: TaskState::Delayed,
                    retry_at: Some(retry_at),
                    started_at: None,
                    retries_remaining: Some(retries_remaining),
                },
            )?;
        }
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
        self.ensure_journal_alive()?;
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
                        self.emit_locked(
                            &state,
                            JournalEvent::TaskStateChanged {
                                task_id: task_id.clone(),
                                flow_id: flow_id.clone(),
                                new_state: TaskState::Ready,
                                retry_at: None,
                                started_at: None,
                                retries_remaining: None,
                            },
                        )?;
                        ready.push(task_id);
                    }
                }
            }
            newly_ready = ready;
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
        self.ensure_journal_alive()?;
        let flow;
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
            let completed_at = now;
            // Succeeded is not indexed, no index_add

            // 2. Increment flow counter
            let f = state
                .flows
                .get_mut(flow_id)
                .ok_or_else(|| StorageError::FlowNotFound(flow_id.to_string()))?;
            f.tasks_succeeded += 1;
            f.updated_at = now;

            // 3. Promote newly-ready tasks (only those still Pending)
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

            self.emit_locked(
                &state,
                JournalEvent::TaskCompleted {
                    task_id: task_id.clone(),
                    flow_id: flow_id.clone(),
                    new_state: TaskState::Succeeded,
                    output,
                    error: None,
                    completed_at,
                    succeeded: true,
                    newly_ready: newly_ready.to_vec(),
                },
            )?;
        }
        Ok(flow)
    }

    async fn complete_tasks_with_ready_batch(
        &self,
        completions: &[(TaskId, FlowId, Option<serde_json::Value>, Vec<TaskId>)],
    ) -> Result<Vec<Option<Flow>>, StorageError> {
        self.ensure_journal_alive()?;
        let results;
        {
            let mut state = self.state.write();
            let now = Utc::now();
            let mut batch_results = Vec::with_capacity(completions.len());

            for (task_id, flow_id, output, newly_ready) in completions {
                let key = (task_id.clone(), flow_id.clone());

                // 1. Mark task succeeded
                match state.tasks.get(&key) {
                    Some(task) => {
                        if !task.state.can_transition_to(TaskState::Succeeded) {
                            batch_results.push(None);
                            continue;
                        }
                        let old_snapshot = task.clone();
                        state.index_remove(&old_snapshot);
                    }
                    None => {
                        batch_results.push(None);
                        continue;
                    }
                }

                let task = state.tasks.get_mut(&key).expect("checked above");
                task.state = TaskState::Succeeded;
                task.output = output.clone();
                task.completed_at = Some(now);

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

                // 3. Promote newly-ready tasks (only those still Pending)
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

                self.emit_locked(
                    &state,
                    JournalEvent::TaskCompleted {
                        task_id: task_id.clone(),
                        flow_id: flow_id.clone(),
                        new_state: TaskState::Succeeded,
                        output: output.clone(),
                        error: None,
                        completed_at: now,
                        succeeded: true,
                        newly_ready: newly_ready.clone(),
                    },
                )?;

                batch_results.push(Some(flow));
            }
            results = batch_results;
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
        self.ensure_journal_alive()?;
        let flow;
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
            let new_task_count = tasks.len();
            let f = state
                .flows
                .get_mut(flow_id)
                .ok_or_else(|| StorageError::FlowNotFound(flow_id.to_string()))?;
            f.task_count += new_task_count;
            f.updated_at = Utc::now();

            flow = f.clone();

            self.emit_locked(
                &state,
                JournalEvent::TasksInjected {
                    flow_id: flow_id.clone(),
                    tasks: tasks.to_vec(),
                    deps: deps.clone(),
                    new_task_count,
                },
            )?;
        }
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
        self.ensure_journal_alive()?;
        let count;
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

            if !to_delete.is_empty() {
                self.emit_locked(
                    &state,
                    JournalEvent::FlowsDeleted {
                        queue_id: queue_id.clone(),
                        flow_ids: to_delete,
                    },
                )?;
            }
        }
        Ok(count)
    }

    // ---- Schedules ----

    async fn create_schedule(&self, schedule: &Schedule) -> Result<(), StorageError> {
        self.ensure_journal_alive()?;
        {
            let mut state = self.state.write();
            state
                .schedules
                .insert(schedule.id.clone(), schedule.clone());
            self.emit_locked(
                &state,
                JournalEvent::ScheduleCreated {
                    schedule: schedule.clone(),
                },
            )?;
        }
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
        self.ensure_journal_alive()?;
        {
            let mut state = self.state.write();
            if !state.schedules.contains_key(&schedule.id) {
                return Err(StorageError::ScheduleNotFound(schedule.id.to_string()));
            }
            state
                .schedules
                .insert(schedule.id.clone(), schedule.clone());
            self.emit_locked(
                &state,
                JournalEvent::ScheduleUpdated {
                    schedule: schedule.clone(),
                },
            )?;
        }
        Ok(())
    }

    async fn delete_schedule(&self, id: &ScheduleId) -> Result<(), StorageError> {
        self.ensure_journal_alive()?;
        {
            let mut state = self.state.write();
            state.schedules.remove(id);
            self.emit_locked(
                &state,
                JournalEvent::ScheduleDeleted {
                    schedule_id: id.clone(),
                },
            )?;
        }
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
        self.ensure_journal_alive()?;
        {
            let mut state = self.state.write();
            let schedule = state
                .schedules
                .get_mut(id)
                .ok_or_else(|| StorageError::ScheduleNotFound(id.to_string()))?;
            schedule.last_triggered_at = Some(triggered_at);
            schedule.next_run_at = next_run_at;
            schedule.updated_at = Utc::now();
            self.emit_locked(
                &state,
                JournalEvent::ScheduleTriggered {
                    schedule_id: id.clone(),
                    triggered_at,
                    next_run_at,
                },
            )?;
        }
        Ok(())
    }

    async fn health_check(&self) -> Result<(), StorageError> {
        self.check_journal_health()
    }
}

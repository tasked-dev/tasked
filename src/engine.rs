use crate::artifacts::ArtifactStore;
use crate::executor::{ExecutionContext, Executor, FlowSubmitter};
use crate::graph::{GraphError, TaskGraph};
use crate::interpolate::{self, TaskOutputs};
use crate::rate_limit::RateLimiter;
use crate::store::{Storage, StorageError};
use crate::types::*;
use chrono::{Duration, Utc};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{Notify, Semaphore};
use tracing::{debug, error, info, instrument, warn};

/// Engine errors.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("graph error: {0}")]
    Graph(#[from] GraphError),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("no executor registered for type '{0}'")]
    NoExecutor(String),
    #[error("queue '{0}' not found")]
    QueueNotFound(String),
    #[error("invalid cron expression: {0}")]
    InvalidCronExpression(String),
    #[error("spawn error: {0}")]
    Spawn(String),
    #[error("trigger depth limit ({0}) exceeded")]
    TriggerDepthExceeded(u32),
    #[error("flow task limit ({0}) exceeded")]
    TaskLimitExceeded(usize),
    #[error("export error: {0}")]
    Export(String),
    #[error("queue '{0}' has reached its pending flow limit ({1})")]
    FlowLimitExceeded(String, usize),
}

/// Result of evaluating a task condition expression.
#[cfg_attr(not(feature = "scripting"), allow(dead_code))]
enum ConditionResult {
    /// Condition was true -- proceed with dispatch.
    Proceed,
    /// Condition was false or invalid -- task has been marked accordingly, skip dispatch.
    Handled,
    /// An engine error occurred while evaluating the condition.
    Err(EngineError),
}

/// Configuration for the engine.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// How often to poll for work when idle (fallback).
    pub poll_interval: std::time::Duration,
    /// How often to check for timed-out tasks.
    pub recovery_interval: std::time::Duration,
    /// How often to run dead flow cleanup.
    pub cleanup_interval: std::time::Duration,
    /// How often to evaluate cron schedules.
    pub schedule_interval: std::time::Duration,
    /// Max tasks to fetch per batch.
    pub batch_size: usize,
    /// Maximum nesting depth for spawn-generated tasks.
    pub max_spawn_depth: usize,
    /// Maximum nesting depth for trigger-submitted flows (default: 8).
    pub max_trigger_depth: u32,
    /// Maximum number of tasks allowed in a single flow (default: 10,000).
    pub max_tasks_per_flow: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            poll_interval: std::time::Duration::from_secs(1),
            recovery_interval: std::time::Duration::from_secs(10),
            cleanup_interval: std::time::Duration::from_secs(300),
            schedule_interval: std::time::Duration::from_secs(60),
            batch_size: 500,
            max_spawn_depth: 8,
            max_trigger_depth: 8,
            max_tasks_per_flow: 10_000,
        }
    }
}

/// The core task execution engine.
///
/// Manages queues, accepts flow submissions, resolves task dependencies,
/// In-memory dependency resolution for a single flow.
///
/// Tracks reverse dependencies (task → who depends on it) and unsatisfied counts
/// (task → how many deps haven't succeeded yet). When a task succeeds, decrement
/// its dependents' counts — those reaching 0 are newly ready.
///
/// This replaces the SQL NOT EXISTS subquery in `resolve_ready_tasks`, cutting
/// ~100us per task completion on the hot path.
struct FlowDepGraph {
    /// task_id → tasks that depend on it (reverse index).
    dependents: HashMap<TaskId, Vec<TaskId>>,
    /// task_id → count of unsatisfied (not-yet-succeeded) dependencies.
    unsatisfied: HashMap<TaskId, usize>,
}

impl FlowDepGraph {
    /// Build from the deps HashMap created at flow submission time.
    /// `deps` maps task_id → [task_ids it depends on].
    fn build(deps: &HashMap<TaskId, Vec<TaskId>>, all_task_ids: &[TaskId]) -> Self {
        let mut dependents: HashMap<TaskId, Vec<TaskId>> = HashMap::new();
        let mut unsatisfied: HashMap<TaskId, usize> = HashMap::new();

        // Initialize all tasks with 0 unsatisfied (overridden below for those with deps)
        for tid in all_task_ids {
            unsatisfied.entry(tid.clone()).or_insert(0);
        }

        for (task_id, dep_ids) in deps {
            unsatisfied.insert(task_id.clone(), dep_ids.len());
            for dep_id in dep_ids {
                dependents
                    .entry(dep_id.clone())
                    .or_default()
                    .push(task_id.clone());
            }
        }

        Self {
            dependents,
            unsatisfied,
        }
    }

    /// Build from deps + current task states (for restart recovery).
    /// Only counts deps that haven't yet succeeded as unsatisfied.
    fn build_with_state(
        deps: &HashMap<TaskId, Vec<TaskId>>,
        all_task_ids: &[TaskId],
        succeeded: &HashSet<TaskId>,
    ) -> Self {
        let mut dependents: HashMap<TaskId, Vec<TaskId>> = HashMap::new();
        let mut unsatisfied: HashMap<TaskId, usize> = HashMap::new();

        for tid in all_task_ids {
            unsatisfied.entry(tid.clone()).or_insert(0);
        }

        for (task_id, dep_ids) in deps {
            let count = dep_ids.iter().filter(|d| !succeeded.contains(d)).count();
            unsatisfied.insert(task_id.clone(), count);
            for dep_id in dep_ids {
                dependents
                    .entry(dep_id.clone())
                    .or_default()
                    .push(task_id.clone());
            }
        }

        Self {
            dependents,
            unsatisfied,
        }
    }

    /// A task succeeded. Decrement its dependents' unsatisfied counts.
    /// Returns task IDs whose count reached 0 (newly ready).
    fn on_task_succeeded(&mut self, task_id: &TaskId) -> Vec<TaskId> {
        let mut newly_ready = Vec::new();
        if let Some(deps) = self.dependents.get(task_id) {
            for dep_id in deps {
                if let Some(count) = self.unsatisfied.get_mut(dep_id) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        newly_ready.push(dep_id.clone());
                    }
                }
            }
        }
        newly_ready
    }

    /// Inject new tasks from a spawn executor. Accounts for already-succeeded deps.
    fn inject(
        &mut self,
        deps: &HashMap<TaskId, Vec<TaskId>>,
        new_task_ids: &[TaskId],
        succeeded: &HashSet<TaskId>,
    ) {
        for tid in new_task_ids {
            self.unsatisfied.entry(tid.clone()).or_insert(0);
        }
        for (task_id, dep_ids) in deps {
            let count = dep_ids.iter().filter(|d| !succeeded.contains(d)).count();
            self.unsatisfied.insert(task_id.clone(), count);
            for dep_id in dep_ids {
                self.dependents
                    .entry(dep_id.clone())
                    .or_default()
                    .push(task_id.clone());
            }
        }
    }
}

/// Tracks which queues have running flows, with a per-queue generation counter
/// to prevent stale deactivation from racing with concurrent `submit_flow`.
///
/// The generation counter implements optimistic concurrency control:
/// - `submit_flow` bumps the generation when activating a queue.
/// - `deactivate_if_idle` reads the generation *before* querying the DB,
///   then only removes the entry if the generation has not advanced.
struct ActiveQueues {
    queues: HashMap<QueueId, u64>,
}

impl ActiveQueues {
    fn new() -> Self {
        Self {
            queues: HashMap::new(),
        }
    }

    fn len(&self) -> usize {
        self.queues.len()
    }

    /// Activate a queue (or bump its generation if already active).
    fn activate(&mut self, queue_id: QueueId) {
        let g = self.queues.entry(queue_id).or_insert(0);
        *g += 1;
    }

    /// Insert a queue at generation 0 (startup scan — no concurrent race possible).
    fn insert_startup(&mut self, queue_id: QueueId) {
        self.queues.entry(queue_id).or_insert(0);
    }

    /// Read the current generation for a queue. Returns `None` if not active.
    fn generation(&self, queue_id: &QueueId) -> Option<u64> {
        self.queues.get(queue_id).copied()
    }

    /// Remove a queue only if its generation matches the expected value.
    fn deactivate_if_unchanged(&mut self, queue_id: &QueueId, expected_gen: u64) {
        if let Some(&current_gen) = self.queues.get(queue_id)
            && current_gen == expected_gen
        {
            self.queues.remove(queue_id);
        }
    }

    /// Unconditional remove (for `delete_queue`).
    fn remove(&mut self, queue_id: &QueueId) {
        self.queues.remove(queue_id);
    }

    fn keys(&self) -> impl Iterator<Item = &QueueId> {
        self.queues.keys()
    }

    /// Returns true if the queue is currently active.
    fn contains(&self, queue_id: &QueueId) -> bool {
        self.queues.contains_key(queue_id)
    }
}

/// dispatches tasks to registered [`Executor`]s, and handles retries, timeouts,
/// and cancellation.
///
/// Create one via [`Engine::builder`] or [`Engine::new`], then either:
/// - Call [`Engine::run`] in a spawned task for continuous background processing
/// - Call [`Engine::process_cycle_sync`] in a loop for inline/embedded use
pub struct Engine {
    store: Arc<dyn Storage>,
    executors: HashMap<String, Arc<dyn Executor>>,
    config: EngineConfig,
    notify: Arc<Notify>,
    /// Per-queue concurrency semaphores (lazy-initialized).
    semaphores: std::sync::Mutex<HashMap<QueueId, Arc<Semaphore>>>,
    /// Per-queue rate limiters (lazy-initialized, only for queues with rate_limit config).
    rate_limiters: std::sync::Mutex<HashMap<QueueId, Arc<RateLimiter>>>,
    /// Queues with active (running) flows, guarded by a generation counter
    /// to prevent stale deactivation from racing with concurrent `submit_flow`.
    /// Only these queues are visited during each process_cycle, so idle queues cost nothing.
    active_queues: std::sync::Mutex<ActiveQueues>,
    /// Optional artifact storage backend.
    artifacts: Option<Arc<dyn ArtifactStore>>,
    /// Cache of flow_id → trigger_depth. Trigger depth never changes during a flow's
    /// lifecycle, so this is safe to cache permanently (entries evicted on flow deletion).
    trigger_depth_cache: std::sync::Mutex<HashMap<FlowId, u32>>,
    /// Cache of queue_id → QueueConfig. Invalidated on queue update/delete.
    queue_config_cache: std::sync::Mutex<HashMap<QueueId, QueueConfig>>,
    /// Count of tasks currently in Delayed state. When 0, promote_delayed_tasks is skipped.
    delayed_task_count: std::sync::atomic::AtomicUsize,
    /// In-memory dependency graphs per flow for O(degree) dep resolution.
    dep_graphs: std::sync::Mutex<HashMap<FlowId, FlowDepGraph>>,
    /// Buffer for completed task results pending batch write to storage.
    completion_buffer: std::sync::Mutex<Vec<CompletionEvent>>,
    /// Per-queue wake signals. Workers block on their queue's Notify.
    queue_notifiers: std::sync::Mutex<HashMap<QueueId, Arc<Notify>>>,
    /// Handles for active queue workers (for cleanup on queue deletion).
    queue_workers: std::sync::Mutex<HashMap<QueueId, tokio::task::JoinHandle<()>>>,
    /// Lightweight counters for status reporting (not Prometheus — internal only).
    stats: EngineStats,
}

/// Atomic counters for engine status reporting.
struct EngineStats {
    flows_submitted: std::sync::atomic::AtomicU64,
    flows_completed: std::sync::atomic::AtomicU64,
    tasks_dispatched: std::sync::atomic::AtomicU64,
    tasks_completed: std::sync::atomic::AtomicU64,
}

impl EngineStats {
    fn new() -> Self {
        Self {
            flows_submitted: std::sync::atomic::AtomicU64::new(0),
            flows_completed: std::sync::atomic::AtomicU64::new(0),
            tasks_dispatched: std::sync::atomic::AtomicU64::new(0),
            tasks_completed: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

/// A completed task result waiting to be written to storage in a batch.
struct CompletionEvent {
    task: Task,
    output: Option<serde_json::Value>,
    newly_ready: Option<Vec<TaskId>>,
}

/// Builder for constructing an [`Engine`] with a fluent API.
///
/// # Example
/// ```rust,no_run
/// use tasked::prelude::*;
/// use std::sync::Arc;
///
/// let engine = Engine::builder(Arc::new(MemoryStorage::new()))
///     .config(EngineConfig::default())
///     .executor("noop", Arc::new(NoopExecutor))
///     .executor("callback", Arc::new(CallbackExecutor::always_succeed()))
///     .build();
/// ```
pub struct EngineBuilder {
    store: Arc<dyn Storage>,
    config: EngineConfig,
    executors: HashMap<String, Arc<dyn Executor>>,
    artifacts: Option<Arc<dyn ArtifactStore>>,
}

impl EngineBuilder {
    fn new(store: Arc<dyn Storage>) -> Self {
        Self {
            store,
            config: EngineConfig::default(),
            executors: HashMap::new(),
            artifacts: None,
        }
    }

    /// Set the engine configuration.
    pub fn config(mut self, config: EngineConfig) -> Self {
        self.config = config;
        self
    }

    /// Register an executor for a given type name.
    pub fn executor(mut self, name: impl Into<String>, executor: Arc<dyn Executor>) -> Self {
        self.executors.insert(name.into(), executor);
        self
    }

    /// Set the artifact storage backend.
    pub fn artifact_store(mut self, store: Arc<dyn ArtifactStore>) -> Self {
        self.artifacts = Some(store);
        self
    }

    /// Build the engine.
    pub fn build(self) -> Engine {
        Engine {
            store: self.store,
            executors: self.executors,
            config: self.config,
            notify: Arc::new(Notify::new()),
            semaphores: std::sync::Mutex::new(HashMap::new()),
            rate_limiters: std::sync::Mutex::new(HashMap::new()),
            active_queues: std::sync::Mutex::new(ActiveQueues::new()),
            artifacts: self.artifacts,
            trigger_depth_cache: std::sync::Mutex::new(HashMap::new()),
            queue_config_cache: std::sync::Mutex::new(HashMap::new()),
            delayed_task_count: std::sync::atomic::AtomicUsize::new(0),
            dep_graphs: std::sync::Mutex::new(HashMap::new()),
            completion_buffer: std::sync::Mutex::new(Vec::new()),
            queue_notifiers: std::sync::Mutex::new(HashMap::new()),
            queue_workers: std::sync::Mutex::new(HashMap::new()),
            stats: EngineStats::new(),
        }
    }
}

impl Engine {
    /// Create a builder for constructing an engine.
    pub fn builder(store: Arc<dyn Storage>) -> EngineBuilder {
        EngineBuilder::new(store)
    }

    /// Create a new engine with the given storage and config.
    pub fn new(store: Arc<dyn Storage>, config: EngineConfig) -> Self {
        Self {
            store,
            executors: HashMap::new(),
            config,
            notify: Arc::new(Notify::new()),
            semaphores: std::sync::Mutex::new(HashMap::new()),
            rate_limiters: std::sync::Mutex::new(HashMap::new()),
            active_queues: std::sync::Mutex::new(ActiveQueues::new()),
            artifacts: None,
            trigger_depth_cache: std::sync::Mutex::new(HashMap::new()),
            queue_config_cache: std::sync::Mutex::new(HashMap::new()),
            delayed_task_count: std::sync::atomic::AtomicUsize::new(0),
            dep_graphs: std::sync::Mutex::new(HashMap::new()),
            completion_buffer: std::sync::Mutex::new(Vec::new()),
            queue_notifiers: std::sync::Mutex::new(HashMap::new()),
            queue_workers: std::sync::Mutex::new(HashMap::new()),
            stats: EngineStats::new(),
        }
    }

    /// Set the artifact storage backend.
    pub fn set_artifact_store(&mut self, store: Arc<dyn ArtifactStore>) {
        self.artifacts = Some(store);
    }

    /// Get the artifact storage backend (if configured).
    pub fn artifact_store(&self) -> Option<Arc<dyn ArtifactStore>> {
        self.artifacts.clone()
    }

    /// Register an executor for a given type name.
    pub fn register_executor(&mut self, name: impl Into<String>, executor: Arc<dyn Executor>) {
        self.executors.insert(name.into(), executor);
    }

    /// Get a reference to the registered executors map.
    pub fn executors(&self) -> &HashMap<String, Arc<dyn Executor>> {
        &self.executors
    }

    /// Get a handle to notify the engine of new work.
    pub fn notifier(&self) -> Arc<Notify> {
        self.notify.clone()
    }

    /// Get or create the per-queue Notify for a given queue.
    fn get_or_create_queue_notify(&self, queue_id: &QueueId) -> Arc<Notify> {
        let mut notifiers = self
            .queue_notifiers
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        notifiers
            .entry(queue_id.clone())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    /// Wake the worker for a specific queue.
    fn notify_queue(&self, queue_id: &QueueId) {
        if let Some(n) = self
            .queue_notifiers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(queue_id)
        {
            n.notify_one();
        }
    }

    /// Spawn a queue worker if one is not already running.
    fn spawn_queue_worker(self: &Arc<Self>, queue_id: QueueId) {
        let mut workers = self.queue_workers.lock().unwrap_or_else(|e| e.into_inner());

        // Check if there's already a live worker for this queue
        if let Some(handle) = workers.get(&queue_id)
            && !handle.is_finished()
        {
            return;
        }

        let notify = self.get_or_create_queue_notify(&queue_id);
        let engine = self.clone();
        let qid = queue_id.clone();
        let handle = tokio::spawn(async move {
            queue_worker_loop(engine, qid, notify).await;
        });
        workers.insert(queue_id, handle);
    }

    /// Ensure every active queue has a running worker.
    fn ensure_workers_for_active_queues(self: &Arc<Self>) {
        let active: Vec<QueueId> = self
            .active_queues
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect();
        for qid in active {
            self.spawn_queue_worker(qid);
        }
    }

    /// Get queue config from cache, falling back to storage.
    async fn get_cached_queue_config(&self, queue_id: &QueueId) -> Option<QueueConfig> {
        // Fast path: check cache
        {
            let cache = self
                .queue_config_cache
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(config) = cache.get(queue_id) {
                return Some(config.clone());
            }
        }
        // Slow path: fetch from storage + populate cache
        if let Ok(Some(q)) = self.store.get_queue(queue_id).await {
            self.queue_config_cache
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(q.id.clone(), q.config.clone());
            Some(q.config)
        } else {
            None
        }
    }

    /// Promote delayed tasks and wake the specific queue workers that have newly-ready work.
    async fn promote_delayed_tasks_and_wake(&self) -> Result<(), EngineError> {
        if self
            .delayed_task_count
            .load(std::sync::atomic::Ordering::Relaxed)
            == 0
        {
            return Ok(());
        }
        let delayed = self.store.fetch_delayed_tasks_due().await?;
        let mut woken_queues = HashSet::new();
        for task in &delayed {
            debug!(task_id = %task.id, flow_id = %task.flow_id, "promoting delayed task to ready");
            self.store
                .update_task_state(&task.id, &task.flow_id, TaskState::Ready)
                .await?;
            woken_queues.insert(task.queue_id.clone());
        }
        self.delayed_task_count
            .fetch_sub(delayed.len(), std::sync::atomic::Ordering::Relaxed);
        // Wake the specific queue workers that have newly-ready tasks
        for qid in &woken_queues {
            self.notify_queue(qid);
        }
        Ok(())
    }

    /// Ensure a semaphore exists for the given queue. Returns it.
    fn ensure_semaphore(&self, queue_id: &QueueId, concurrency: usize) -> Arc<Semaphore> {
        let mut sems = self.semaphores.lock().unwrap_or_else(|e| e.into_inner());
        sems.entry(queue_id.clone())
            .or_insert_with(|| Arc::new(Semaphore::new(concurrency)))
            .clone()
    }

    /// Ensure a rate limiter exists for the given queue (if configured). Returns it.
    fn ensure_rate_limiter(
        &self,
        queue_id: &QueueId,
        config: &Option<RateLimitConfig>,
    ) -> Option<Arc<RateLimiter>> {
        let rl_config = config.as_ref()?;
        let mut limiters = self.rate_limiters.lock().unwrap_or_else(|e| e.into_inner());
        Some(
            limiters
                .entry(queue_id.clone())
                .or_insert_with(|| {
                    Arc::new(RateLimiter::new(rl_config.max_burst, rl_config.per_second))
                })
                .clone(),
        )
    }

    // -- Queue operations --

    /// Create a new queue with the given ID and configuration.
    #[instrument(skip(self, config), fields(queue_id = %id))]
    pub async fn create_queue(
        &self,
        id: &QueueId,
        config: QueueConfig,
    ) -> Result<Queue, EngineError> {
        let now = Utc::now();
        let queue = Queue {
            id: id.clone(),
            config,
            created_at: now,
            updated_at: now,
        };
        self.store.create_queue(&queue).await?;
        // Cache the config for fast access during process_cycle.
        self.queue_config_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id.clone(), queue.config.clone());
        Ok(queue)
    }

    /// Get a queue by ID, or `None` if it doesn't exist.
    pub async fn get_queue(&self, id: &QueueId) -> Result<Option<Queue>, EngineError> {
        Ok(self.store.get_queue(id).await?)
    }

    /// List all queues.
    pub async fn list_queues(&self) -> Result<Vec<Queue>, EngineError> {
        Ok(self.store.list_queues().await?)
    }

    /// Delete a queue by ID.
    pub async fn delete_queue(&self, id: &QueueId) -> Result<(), EngineError> {
        self.store.delete_queue(id).await?;
        self.queue_config_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id);
        self.active_queues
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id);
        // Abort the queue worker (if running) and clean up its notifier.
        if let Some(handle) = self
            .queue_workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id)
        {
            handle.abort();
        }
        self.queue_notifiers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id);
        Ok(())
    }

    // -- Flow operations --

    /// Submit a new flow to a queue (top-level, trigger_depth = 0).
    #[instrument(skip(self, flow_def), fields(queue_id = %queue_id))]
    pub async fn submit_flow(
        &self,
        queue_id: &QueueId,
        flow_def: FlowDef,
    ) -> Result<Flow, EngineError> {
        self.submit_flow_with_depth(queue_id, flow_def, 0, None)
            .await
    }

    /// Submit a flow with an explicit trigger depth and optional parent flow ID.
    /// Used by the trigger executor to propagate depth through sub-flow chains.
    async fn submit_flow_with_depth(
        &self,
        queue_id: &QueueId,
        flow_def: FlowDef,
        trigger_depth: u32,
        parent_flow_id: Option<FlowId>,
    ) -> Result<Flow, EngineError> {
        // Enforce trigger depth limit
        if trigger_depth > self.config.max_trigger_depth {
            return Err(EngineError::TriggerDepthExceeded(
                self.config.max_trigger_depth,
            ));
        }
        // Verify queue exists
        let queue = self
            .store
            .get_queue(queue_id)
            .await?
            .ok_or_else(|| EngineError::QueueNotFound(queue_id.to_string()))?;

        // Enforce max_pending_flows backpressure limit
        if let Some(max) = queue.config.max_pending_flows {
            let active_flows = self
                .store
                .list_flows(queue_id, Some(FlowState::Running))
                .await?;
            if active_flows.len() >= max {
                return Err(EngineError::FlowLimitExceeded(queue_id.to_string(), max));
            }
        }

        // Validate executors exist
        for task_def in &flow_def.tasks {
            if !self.executors.contains_key(&task_def.executor) {
                return Err(EngineError::NoExecutor(task_def.executor.clone()));
            }
        }

        // Build and validate DAG
        // If any task declares spawn_output, use the spawn-aware builder
        // that allows deferred "/" dependencies.
        let has_spawn = flow_def.tasks.iter().any(|t| !t.spawn_output.is_empty());
        let graph = if has_spawn {
            TaskGraph::build_with_spawn_deps(&flow_def.tasks)?
        } else {
            let task_ids: Vec<TaskId> = flow_def.tasks.iter().map(|t| t.id.clone()).collect();
            let deps: HashMap<TaskId, Vec<TaskId>> = flow_def
                .tasks
                .iter()
                .filter(|t| !t.depends_on.is_empty())
                .map(|t| (t.id.clone(), t.depends_on.clone()))
                .collect();
            TaskGraph::build(&task_ids, &deps)?
        };

        // Store all dependencies including deferred spawn refs (containing "/").
        // Deferred deps reference tasks that don't exist yet; those tasks stay pending
        // until the generated task is injected and succeeds.
        let deps: HashMap<TaskId, Vec<TaskId>> = flow_def
            .tasks
            .iter()
            .filter(|t| !t.depends_on.is_empty())
            .map(|t| (t.id.clone(), t.depends_on.clone()))
            .collect();

        // Enforce per-flow task count limit
        if graph.task_count() > self.config.max_tasks_per_flow {
            return Err(EngineError::TaskLimitExceeded(
                self.config.max_tasks_per_flow,
            ));
        }

        // Create flow — store the original FlowDef verbatim for replay/audit.
        let now = Utc::now();
        let flow_id = FlowId::new();

        // Create tasks -- roots start as Ready, others as Pending.
        // A task with deferred spawn deps (containing "/") is never a root
        // even if the graph considers it one (because deferred edges aren't in the graph).
        let roots = graph.roots();
        let tasks: Vec<Task> = flow_def
            .tasks
            .iter()
            .map(|def| {
                let has_deferred_dep = def.depends_on.iter().any(|d| d.as_str().contains('/'));
                let is_root = roots.contains(&def.id) && !has_deferred_dep;
                Task {
                    id: def.id.clone(),
                    flow_id: flow_id.clone(),
                    queue_id: queue_id.clone(),
                    state: if is_root {
                        TaskState::Ready
                    } else {
                        TaskState::Pending
                    },
                    executor_type: def.executor.clone(),
                    executor_config: def.config.clone(),
                    input: def.input.clone(),
                    output: None,
                    error: None,
                    retries_remaining: def.retries.unwrap_or(queue.config.max_retries),
                    backoff: def
                        .backoff
                        .clone()
                        .unwrap_or_else(|| queue.config.backoff.clone()),
                    timeout_secs: def.timeout_secs.unwrap_or(queue.config.timeout_secs),
                    condition: def.condition.clone(),
                    retry_at: None,
                    started_at: None,
                    completed_at: None,
                    created_at: now,
                }
            })
            .collect();

        let fail_fast = flow_def.fail_fast;
        let flow = Flow {
            id: flow_id.clone(),
            queue_id: queue_id.clone(),
            state: FlowState::Running,
            task_count: graph.task_count(),
            tasks_succeeded: 0,
            tasks_failed: 0,
            webhooks: flow_def.webhooks.clone(),
            trigger_depth,
            flow_def: Some(flow_def),
            fail_fast,
            parent_flow_id,
            created_at: now,
            updated_at: now,
        };

        // Store atomically
        self.store.create_flow(&flow, &tasks, &deps).await?;

        // Lazy-init semaphore and rate limiter for this queue
        self.ensure_semaphore(queue_id, queue.config.concurrency);
        self.ensure_rate_limiter(queue_id, &queue.config.rate_limit);

        // Mark queue as active (bumps generation to guard against stale deactivation)
        self.active_queues
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .activate(queue_id.clone());

        // Pre-cache trigger_depth for this flow
        self.trigger_depth_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(flow.id.clone(), flow.trigger_depth);

        // Build in-memory dependency graph for O(degree) dep resolution
        let all_task_ids: Vec<TaskId> = tasks.iter().map(|t| t.id.clone()).collect();
        let dep_graph = FlowDepGraph::build(&deps, &all_task_ids);
        self.dep_graphs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(flow.id.clone(), dep_graph);

        // Wake the per-queue worker (if running) and the main loop (to spawn worker if needed)
        self.notify_queue(queue_id);
        self.notify.notify_one();

        // Metrics
        metrics::counter!("tasked_flows_submitted_total", "queue_id" => queue_id.as_str().to_owned())
            .increment(1);
        self.stats
            .flows_submitted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        debug!(flow_id = %flow.id, queue_id = %queue_id, tasks = graph.task_count(), "flow submitted");
        Ok(flow)
    }

    /// Get flow status with all task states.
    pub async fn get_flow(&self, flow_id: &FlowId) -> Result<Option<Flow>, EngineError> {
        Ok(self.store.get_flow(flow_id).await?)
    }

    /// List flows for a queue, optionally filtered by state.
    pub async fn list_flows(
        &self,
        queue_id: &QueueId,
        state: Option<FlowState>,
    ) -> Result<Vec<Flow>, EngineError> {
        Ok(self.store.list_flows(queue_id, state).await?)
    }

    /// Get all tasks for a flow.
    pub async fn get_flow_tasks(&self, flow_id: &FlowId) -> Result<Vec<Task>, EngineError> {
        Ok(self.store.get_flow_tasks(flow_id).await?)
    }

    /// Get a flow and all its tasks in a single operation.
    pub async fn get_flow_with_tasks(
        &self,
        flow_id: &FlowId,
    ) -> Result<Option<(Flow, Vec<Task>)>, EngineError> {
        Ok(self.store.get_flow_with_tasks(flow_id).await?)
    }

    /// Get a single task by ID within a flow.
    pub async fn get_task(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<Option<Task>, EngineError> {
        Ok(self.store.get_task(task_id, flow_id).await?)
    }

    /// Cancel a flow and all its non-terminal tasks.
    #[instrument(skip(self), fields(flow_id = %flow_id))]
    pub async fn cancel_flow(&self, flow_id: &FlowId) -> Result<(), EngineError> {
        let tasks = self.store.get_flow_tasks(flow_id).await?;
        for task in &tasks {
            if !task.state.is_terminal() && task.state.can_transition_to(TaskState::Cancelled) {
                self.store
                    .update_task_state(&task.id, flow_id, TaskState::Cancelled)
                    .await?;
            }
        }
        self.store
            .update_flow_state(flow_id, FlowState::Cancelled)
            .await?;

        // Evict dep graph — flow is terminal
        self.evict_dep_graph(flow_id);

        // Propagate cancellation to child flows spawned by trigger tasks
        self.cancel_child_flows(flow_id).await;

        // Metrics + deactivate queue if no more running flows
        if let Ok(Some(flow)) = self.store.get_flow(flow_id).await {
            metrics::counter!(
                "tasked_flows_completed_total",
                "queue_id" => flow.queue_id.as_str().to_owned(),
                "status" => "cancelled"
            )
            .increment(1);

            self.deactivate_if_idle(&flow.queue_id).await?;
        }

        debug!(flow_id = %flow_id, "flow cancelled");
        Ok(())
    }

    // -- Export --

    /// Export a flow's complete state for archival, compliance, or replay.
    pub async fn export_flow(
        &self,
        flow_id: &FlowId,
        include_artifacts: bool,
    ) -> Result<FlowExport, EngineError> {
        use base64::Engine as _;
        use sha2::{Digest, Sha256};

        let flow = self.store.get_flow(flow_id).await?.ok_or_else(|| {
            EngineError::Storage(crate::store::StorageError::FlowNotFound(
                flow_id.as_str().to_owned(),
            ))
        })?;

        let tasks = self.store.get_flow_tasks(flow_id).await?;
        let deps = self.store.get_flow_dependencies(flow_id).await?;

        let task_exports: Vec<TaskExport> = tasks
            .iter()
            .map(|t| TaskExport {
                id: t.id.clone(),
                executor_type: t.executor_type.clone(),
                executor_config: t.executor_config.clone(),
                input: t.input.clone(),
                output: t.output.clone(),
                error: t.error.clone(),
                state: t.state,
                depends_on: deps.get(&t.id).cloned().unwrap_or_default(),
                retries_remaining: t.retries_remaining,
                timeout_secs: t.timeout_secs,
                condition: t.condition.clone(),
                started_at: t.started_at,
                completed_at: t.completed_at,
                created_at: t.created_at,
            })
            .collect();

        let mut artifact_exports = Vec::new();
        if include_artifacts && let Some(ref store) = self.artifacts {
            let names = store
                .list(flow_id)
                .await
                .map_err(|e| EngineError::Export(e.to_string()))?;
            for name in names {
                let data = store
                    .download(flow_id, &name)
                    .await
                    .map_err(|e| EngineError::Export(e.to_string()))?;
                let size_bytes = data.len() as u64;
                let data_base64 = if size_bytes <= 1_048_576 {
                    Some(base64::engine::general_purpose::STANDARD.encode(&data))
                } else {
                    None
                };
                artifact_exports.push(ArtifactExport {
                    name,
                    size_bytes,
                    data_base64,
                });
            }
        }

        let flow_meta = FlowExportMeta {
            id: flow.id.clone(),
            queue_id: flow.queue_id.clone(),
            state: flow.state,
            task_count: flow.task_count,
            tasks_succeeded: flow.tasks_succeeded,
            tasks_failed: flow.tasks_failed,
            trigger_depth: flow.trigger_depth,
            webhooks: flow.webhooks.clone(),
            flow_def: flow.flow_def.clone(),
            created_at: flow.created_at,
            updated_at: flow.updated_at,
        };

        let mut export = FlowExport {
            version: 1,
            flow: flow_meta,
            tasks: task_exports,
            artifacts: artifact_exports,
            checksum: None,
            exported_at: chrono::Utc::now(),
        };

        // Compute checksum over canonical JSON (with checksum = null)
        let json_bytes =
            serde_json::to_vec(&export).map_err(|e| EngineError::Export(e.to_string()))?;
        let hash = Sha256::digest(&json_bytes);
        export.checksum = Some(hash.iter().map(|b| format!("{b:02x}")).collect());

        Ok(export)
    }

    /// Export a flow as a tar.gz archive containing `export.json` and `artifacts/<name>` files.
    ///
    /// Archive format choice: tar.gz (tar + flate2).
    /// - Cross-platform: supported natively on Linux/macOS, widely available on Windows (7-zip, WSL).
    /// - Streaming: tar is a sequential format, ideal for writing without buffering the whole archive.
    /// - Rust ecosystem: `tar` and `flate2` are mature, well-maintained crates with millions of downloads.
    /// - Considered alternatives:
    ///   - zip: better Windows native support but no streaming writes for large archives.
    ///   - tar.zst: better compression but `zstd` crate has a C dependency; not worth the complexity.
    pub async fn export_flow_tar(&self, flow_id: &FlowId) -> Result<Vec<u8>, EngineError> {
        use flate2::Compression;
        use flate2::write::GzEncoder;

        // Build the JSON export with artifacts always included (archive mode implies artifacts)
        let export = self.export_flow(flow_id, true).await?;
        let json_bytes =
            serde_json::to_vec_pretty(&export).map_err(|e| EngineError::Export(e.to_string()))?;

        let buf = Vec::new();
        let enc = GzEncoder::new(buf, Compression::default());
        let mut tar = tar::Builder::new(enc);

        // Add export.json
        let mut header = tar::Header::new_gnu();
        header.set_size(json_bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append_data(&mut header, "export.json", json_bytes.as_slice())
            .map_err(|e| EngineError::Export(format!("tar write error: {e}")))?;

        // Add artifact files under artifacts/
        if let Some(ref store) = self.artifacts {
            let names = store
                .list(flow_id)
                .await
                .map_err(|e| EngineError::Export(e.to_string()))?;
            for name in names {
                let data = store
                    .download(flow_id, &name)
                    .await
                    .map_err(|e| EngineError::Export(e.to_string()))?;
                let mut header = tar::Header::new_gnu();
                header.set_size(data.len() as u64);
                header.set_mode(0o644);
                header.set_cksum();
                let path = format!("artifacts/{name}");
                tar.append_data(&mut header, &path, data.as_slice())
                    .map_err(|e| EngineError::Export(format!("tar write error: {e}")))?;
            }
        }

        let enc = tar
            .into_inner()
            .map_err(|e| EngineError::Export(format!("tar finish error: {e}")))?;
        let compressed = enc
            .finish()
            .map_err(|e| EngineError::Export(format!("gzip finish error: {e}")))?;

        Ok(compressed)
    }

    // -- Engine loop --

    /// Run the engine processing loop. Call this in a spawned tokio task.
    /// The engine must be wrapped in Arc for concurrent task dispatch.
    ///
    /// Uses per-queue workers: each active queue gets its own tokio task that
    /// blocks on a per-queue `Notify`. A global sweeper handles cross-queue
    /// periodic operations (recovery, cleanup, schedules, delayed task promotion).
    pub async fn run(self: &Arc<Self>) {
        info!("engine started (per-queue worker mode)");

        // Activate queues that have running flows on disk and spawn workers.
        // Dep graphs are built lazily on first task completion (via ensure_dep_graph),
        // so startup is O(queues) not O(flows).
        if let Ok(queues) = self.store.list_queues().await {
            for q in &queues {
                if let Ok(flows) = self.store.list_flows(&q.id, Some(FlowState::Running)).await
                    && !flows.is_empty()
                {
                    info!(
                        queue_id = %q.id,
                        running_flows = flows.len(),
                        "recovering queue with running flows"
                    );
                    self.active_queues
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .insert_startup(q.id.clone());
                    self.spawn_queue_worker(q.id.clone());
                }
            }
        }

        // Spawn the global sweeper for periodic cross-queue operations
        let sweeper_engine = self.clone();
        let _sweeper_handle = tokio::spawn(async move {
            global_sweeper_loop(sweeper_engine).await;
        });

        // Main loop: listen for new queue activations and spawn workers as needed
        loop {
            tokio::select! {
                _ = self.notify.notified() => {
                    debug!("engine woken by notification");
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {}
            }

            // Check storage backend health (detects dead journal writer, etc.)
            if let Err(e) = self.store.health_check().await {
                error!(error = %e, "storage health check failed — stopping dispatch");
                break;
            }

            // Ensure every active queue has a running worker
            self.ensure_workers_for_active_queues();
        }
    }

    /// Run a single processing cycle. Useful for testing.
    /// When called on `&Arc<Self>`, tasks are dispatched concurrently across queues.
    pub async fn process_cycle(self: &Arc<Self>) -> Result<(), EngineError> {
        metrics::counter!("tasked_engine_cycles_total").increment(1);

        // 1. Promote delayed tasks
        let t0 = std::time::Instant::now();
        self.promote_delayed_tasks().await?;
        crate::perf::counters::promote_delayed.record(t0);

        // 2. Collect active queue IDs (only queues with running flows)
        let active: Vec<QueueId> = {
            self.active_queues
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .keys()
                .cloned()
                .collect()
        };

        if active.is_empty() {
            return Ok(());
        }

        // 3. Fetch configs for active queues (cache-first, fallback to storage)
        let t0 = std::time::Instant::now();
        let mut queue_configs = Vec::with_capacity(active.len());
        for qid in &active {
            let cached = self
                .queue_config_cache
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(qid)
                .cloned();
            if let Some(config) = cached {
                queue_configs.push((qid.clone(), config));
            } else if let Ok(Some(q)) = self.store.get_queue(qid).await {
                self.queue_config_cache
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(q.id.clone(), q.config.clone());
                queue_configs.push((q.id, q.config));
            }
        }
        crate::perf::counters::get_queue_config.record(t0);

        // 4. Dispatch per-queue work in parallel
        let mut handles = Vec::with_capacity(queue_configs.len());
        for (qid, config) in queue_configs {
            let engine = self.clone();
            handles.push(tokio::spawn(async move {
                if let Err(e) = engine.dispatch_ready_tasks(&qid, &config).await {
                    error!(queue_id = %qid, error = %e, "dispatch failed");
                }
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }

        Ok(())
    }

    /// Run a single processing cycle with synchronous (inline) task execution.
    /// Useful for tests that don't need concurrency.
    ///
    /// Unlike [`Engine::process_cycle`], which only iterates `active_queues` (the
    /// set of queues with currently running flows), this method calls
    /// [`Storage::list_queues`] to scan **all** queues.  This is intentional:
    /// `process_cycle_sync` is the primary entry-point for tests, where flows
    /// may have just been submitted but the active-queues set has not yet been
    /// updated.  Scanning all queues guarantees no work is missed in a
    /// single-threaded test harness at the cost of an extra storage read -- a
    /// negligible overhead given test-sized queue counts.
    pub async fn process_cycle_sync(&self) -> Result<(), EngineError> {
        metrics::counter!("tasked_engine_cycles_total").increment(1);
        self.promote_delayed_tasks().await?;
        let queues = self.store.list_queues().await?;
        for queue in &queues {
            self.dispatch_ready_tasks_sync(&queue.id, &queue.config)
                .await?;
        }
        Ok(())
    }

    /// Promote delayed tasks whose retry_at has passed back to Ready.
    async fn promote_delayed_tasks(&self) -> Result<(), EngineError> {
        // Skip the DB query entirely when no tasks are delayed.
        if self
            .delayed_task_count
            .load(std::sync::atomic::Ordering::Relaxed)
            == 0
        {
            return Ok(());
        }
        let delayed = self.store.fetch_delayed_tasks_due().await?;
        for task in &delayed {
            debug!(task_id = %task.id, flow_id = %task.flow_id, "promoting delayed task to ready");
            self.store
                .update_task_state(&task.id, &task.flow_id, TaskState::Ready)
                .await?;
        }
        self.delayed_task_count
            .fetch_sub(delayed.len(), std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Deactivate a queue if it has no more running flows.
    ///
    /// Uses a generation counter to prevent a concurrent `submit_flow` from
    /// having its `active_queues` insertion undone by a stale deactivation.
    /// The generation is read *before* the async DB query; if `submit_flow`
    /// bumps the generation in between, the removal is skipped.
    async fn deactivate_if_idle(&self, queue_id: &QueueId) -> Result<(), EngineError> {
        let gen_before = self
            .active_queues
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .generation(queue_id);

        let remaining = self
            .store
            .list_flows(queue_id, Some(FlowState::Running))
            .await?;

        if remaining.is_empty()
            && let Some(expected_gen) = gen_before
        {
            self.active_queues
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .deactivate_if_unchanged(queue_id, expected_gen);
        }
        Ok(())
    }

    /// Get trigger_depth for a flow, using the engine-level cache.
    /// Trigger depth is immutable for a flow's lifetime, so caching is safe.
    async fn cached_trigger_depth(&self, flow_id: &FlowId) -> Result<u32, EngineError> {
        // Fast path: check cache
        {
            let cache = self
                .trigger_depth_cache
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(&depth) = cache.get(flow_id) {
                return Ok(depth);
            }
        }
        // Slow path: fetch from storage + populate cache
        let depth = self
            .store
            .get_flow(flow_id)
            .await?
            .map(|f| f.trigger_depth)
            .unwrap_or(0);
        self.trigger_depth_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(flow_id.clone(), depth);
        Ok(depth)
    }

    /// Resolve dependencies in-memory via FlowDepGraph. Returns newly-ready task IDs.
    /// Falls back to an empty vec if no graph is cached (e.g., flow predates the cache).
    fn resolve_deps_in_memory(&self, flow_id: &FlowId, task_id: &TaskId) -> Option<Vec<TaskId>> {
        let mut graphs = self.dep_graphs.lock().unwrap_or_else(|e| e.into_inner());
        graphs
            .get_mut(flow_id)
            .map(|graph| graph.on_task_succeeded(task_id))
    }

    /// Lazily build and cache a dep graph for a flow that doesn't have one yet.
    /// Called on first completion for flows recovered from disk without pre-built graphs.
    async fn ensure_dep_graph(&self, flow_id: &FlowId) {
        {
            let graphs = self.dep_graphs.lock().unwrap_or_else(|e| e.into_inner());
            if graphs.contains_key(flow_id) {
                return;
            }
        }
        // Build outside the lock
        if let Ok(deps) = self.store.get_flow_dependencies(flow_id).await
            && let Ok(tasks) = self.store.get_flow_tasks(flow_id).await
        {
            let task_ids: Vec<TaskId> = tasks.iter().map(|t| t.id.clone()).collect();
            let succeeded: HashSet<TaskId> = tasks
                .iter()
                .filter(|t| t.state == TaskState::Succeeded)
                .map(|t| t.id.clone())
                .collect();
            let graph = FlowDepGraph::build_with_state(&deps, &task_ids, &succeeded);
            self.dep_graphs
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(flow_id.clone(), graph);
        }
    }

    /// Evict the dep graph for a flow (on cancel or terminal state).
    fn evict_dep_graph(&self, flow_id: &FlowId) {
        self.dep_graphs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(flow_id);
    }

    /// Drain the completion buffer and write pending success completions to storage.
    async fn process_completions_batch(&self) -> Result<(), EngineError> {
        // Swap out the buffer (brief Mutex hold)
        let events: Vec<CompletionEvent> = {
            let mut buf = self
                .completion_buffer
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *buf)
        };

        if events.is_empty() {
            return Ok(());
        }

        let t0 = std::time::Instant::now();
        let batch_count = events.len();

        // Build batch from events. Use pre-resolved newly_ready if available
        // (from in-memory dep graph), otherwise pass empty and let storage
        // resolve deps via SQL. This avoids ensure_dep_graph overhead (2 DB
        // queries per uncached flow) which dominated batch time under backlog.
        let batch: Vec<(TaskId, FlowId, Option<serde_json::Value>, Vec<TaskId>)> = events
            .iter()
            .map(|e| {
                (
                    e.task.id.clone(),
                    e.task.flow_id.clone(),
                    e.output.clone(),
                    e.newly_ready.clone().unwrap_or_default(),
                )
            })
            .collect();

        // Single batched transaction
        let t_batch_build = t0.elapsed();
        let t_storage = std::time::Instant::now();
        let results = self.store.complete_tasks_with_ready_batch(&batch).await?;
        let t_storage_elapsed = t_storage.elapsed();

        debug!(
            batch_size = batch_count,
            build_ms = t_batch_build.as_millis() as u64,
            storage_ms = t_storage_elapsed.as_millis() as u64,
            "completion batch timing"
        );

        // Process results (metrics, flow completion, webhooks)
        for (event, result) in events.iter().zip(results.iter()) {
            let Some(flow) = result else {
                // Task was cancelled/already succeeded — skip
                continue;
            };

            metrics::counter!(
                "tasked_tasks_completed_total",
                "queue_id" => event.task.queue_id.as_str().to_owned(),
                "status" => "succeeded"
            )
            .increment(1);

            // Check if flow is complete
            if flow.tasks_succeeded == flow.task_count {
                self.store
                    .update_flow_state(&event.task.flow_id, FlowState::Succeeded)
                    .await?;

                metrics::counter!(
                    "tasked_flows_completed_total",
                    "queue_id" => event.task.queue_id.as_str().to_owned(),
                    "status" => "succeeded"
                )
                .increment(1);

                debug!(flow_id = %event.task.flow_id, "flow succeeded");
                self.stats
                    .flows_completed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // Cleanup artifacts
                if let Some(ref artifacts) = self.artifacts
                    && let Err(e) = artifacts.cleanup(&event.task.flow_id).await
                {
                    warn!(flow_id = %event.task.flow_id, error = %e, "artifact cleanup failed");
                }

                // Fire on_complete webhook if configured
                if let Some(ref webhooks) = flow.webhooks
                    && let Some(ref url) = webhooks.on_complete
                {
                    crate::webhook::fire(url, flow);
                }

                // Deactivate queue if no more running flows remain
                self.deactivate_if_idle(&event.task.queue_id).await?;
                self.evict_dep_graph(&event.task.flow_id);
            }
        }

        crate::perf::counters::completion_total.record(t0);
        crate::perf::counters::mark_succeeded.record(t0);

        Ok(())
    }

    /// Resolve variable references in a task's executor_config and input.
    ///
    /// Loads the outputs of all dependency tasks and replaces `${tasks.<id>.output...}`
    /// and `${secrets.<name>}` patterns in the task's executor_config and input fields.
    ///
    /// When `queue_config` is provided, it is used directly for secret resolution
    /// instead of re-fetching the queue from storage.
    async fn resolve_variables(
        &self,
        task: &mut Task,
        queue_config: Option<&QueueConfig>,
    ) -> Result<(), EngineError> {
        let deps = self
            .store
            .get_task_dependencies(&task.id, &task.flow_id)
            .await?;

        // Build the outputs map from dependency tasks
        let mut outputs: TaskOutputs = HashMap::new();
        for dep_id in &deps {
            let dep_task = self.store.get_task(dep_id, &task.flow_id).await?;
            if let Some(dep) = dep_task {
                outputs.insert(dep.id, dep.output);
            }
        }

        // Resolve secrets from the queue config (use provided config or fetch)
        let secrets = if let Some(config) = queue_config {
            interpolate::resolve_secrets(&config.secrets)
        } else {
            let queue = self.store.get_queue(&task.queue_id).await?;
            queue
                .map(|q| interpolate::resolve_secrets(&q.config.secrets))
                .unwrap_or_default()
        };

        // Interpolate executor_config
        task.executor_config = interpolate::interpolate(&task.executor_config, &outputs, &secrets);

        // Interpolate input if present
        if let Some(ref input) = task.input {
            task.input = Some(interpolate::interpolate(input, &outputs, &secrets));
        }

        Ok(())
    }

    /// Check if a JSON value contains any `${` interpolation patterns.
    fn needs_interpolation(value: &serde_json::Value) -> bool {
        match value {
            serde_json::Value::String(s) => s.contains("${"),
            serde_json::Value::Array(arr) => arr.iter().any(Self::needs_interpolation),
            serde_json::Value::Object(map) => map.values().any(Self::needs_interpolation),
            _ => false,
        }
    }

    /// Evaluate a task's condition expression. Handles skip (false) and error cases
    /// by updating the task/flow state. Returns whether to proceed with dispatch.
    async fn evaluate_condition(&self, task: &Task, condition: &str) -> ConditionResult {
        #[cfg(not(feature = "scripting"))]
        {
            let _ = condition;
            warn!(
                task_id = %task.id,
                "condition ignored (scripting feature disabled)"
            );
            ConditionResult::Proceed
        }

        // Gather task outputs and secrets, then evaluate the condition
        // with values bound as Rhai scope variables (no string interpolation).
        #[cfg(feature = "scripting")]
        {
            let deps = match self
                .store
                .get_task_dependencies(&task.id, &task.flow_id)
                .await
            {
                Ok(deps) => deps,
                Err(e) => return ConditionResult::Err(e.into()),
            };
            let mut outputs = interpolate::TaskOutputs::new();
            for dep_id in &deps {
                match self.store.get_task(dep_id, &task.flow_id).await {
                    Ok(Some(dep)) => {
                        outputs.insert(dep.id, dep.output);
                    }
                    Ok(None) => {}
                    Err(e) => return ConditionResult::Err(e.into()),
                }
            }
            let secrets = match self.store.get_queue(&task.queue_id).await {
                Ok(Some(q)) => interpolate::resolve_secrets(&q.config.secrets),
                Ok(None) => Default::default(),
                Err(e) => return ConditionResult::Err(e.into()),
            };

            match crate::condition::evaluate(condition, &outputs, &secrets).await {
                Ok(true) => ConditionResult::Proceed,
                Ok(false) => {
                    // Skip task: mark as succeeded with skip metadata
                    debug!(task_id = %task.id, condition = %condition, "task skipped (condition false)");
                    if let Err(e) = self
                        .store
                        .mark_task_succeeded(
                            &task.id,
                            &task.flow_id,
                            Some(serde_json::json!({"skipped": true, "condition": condition})),
                        )
                        .await
                    {
                        return ConditionResult::Err(e.into());
                    }

                    metrics::counter!(
                        "tasked_tasks_completed_total",
                        "queue_id" => task.queue_id.as_str().to_owned(),
                        "status" => "skipped"
                    )
                    .increment(1);

                    // Resolve deps in-memory for the skipped task
                    let newly_ready = self.resolve_deps_in_memory(&task.flow_id, &task.id);

                    match self
                        .finish_skipped_or_failed_condition(task, true, newly_ready)
                        .await
                    {
                        Ok(()) => ConditionResult::Handled,
                        Err(e) => ConditionResult::Err(e),
                    }
                }
                Err(e) => {
                    // Invalid condition: fail the task
                    warn!(task_id = %task.id, error = %e, "condition evaluation failed");
                    if let Err(err) = self
                        .store
                        .mark_task_failed(
                            &task.id,
                            &task.flow_id,
                            &format!("condition evaluation failed: {e}"),
                        )
                        .await
                    {
                        return ConditionResult::Err(err.into());
                    }

                    if let Err(err) = self.cascade_cancel(&task.id, &task.flow_id).await {
                        return ConditionResult::Err(err);
                    }

                    match self
                        .finish_skipped_or_failed_condition(task, false, None)
                        .await
                    {
                        Ok(()) => ConditionResult::Handled,
                        Err(e) => ConditionResult::Err(e),
                    }
                }
            }
        } // #[cfg(feature = "scripting")]
    }

    /// After skipping (condition false) or failing (condition error) a task,
    /// update flow counters and check for flow completion.
    #[cfg_attr(not(feature = "scripting"), allow(dead_code))]
    async fn finish_skipped_or_failed_condition(
        &self,
        task: &Task,
        succeeded: bool,
        pre_resolved_ready: Option<Vec<TaskId>>,
    ) -> Result<(), EngineError> {
        let flow = self
            .store
            .increment_flow_counter(&task.flow_id, succeeded)
            .await?;

        if succeeded && flow.tasks_succeeded == flow.task_count {
            self.store
                .update_flow_state(&task.flow_id, FlowState::Succeeded)
                .await?;
            // Cleanup artifacts
            if let Some(ref artifacts) = self.artifacts
                && let Err(e) = artifacts.cleanup(&task.flow_id).await
            {
                warn!(flow_id = %task.flow_id, error = %e, "artifact cleanup failed");
            }
            if let Some(ref webhooks) = flow.webhooks
                && let Some(ref url) = webhooks.on_complete
            {
                crate::webhook::fire(url, &flow);
            }
            self.deactivate_if_idle(&task.queue_id).await?;
            self.evict_dep_graph(&task.flow_id);
        } else if succeeded {
            // Promote newly-ready tasks using pre-resolved list or fall back to SQL
            if let Some(ready) = pre_resolved_ready {
                for tid in &ready {
                    self.store
                        .update_task_state(tid, &task.flow_id, TaskState::Ready)
                        .await?;
                }
            } else {
                self.store.resolve_ready_tasks(&task.flow_id).await?;
            }
        } else {
            // Failed condition: check if all tasks are terminal
            let tasks = self.store.get_flow_tasks(&task.flow_id).await?;
            let all_terminal = tasks.iter().all(|t| t.state.is_terminal());
            if all_terminal {
                self.store
                    .update_flow_state(&flow.id, FlowState::Failed)
                    .await?;

                metrics::counter!(
                    "tasked_flows_completed_total",
                    "queue_id" => task.queue_id.as_str().to_owned(),
                    "status" => "failed"
                )
                .increment(1);

                // Cleanup artifacts
                if let Some(ref artifacts) = self.artifacts
                    && let Err(e) = artifacts.cleanup(&task.flow_id).await
                {
                    warn!(flow_id = %task.flow_id, error = %e, "artifact cleanup failed");
                }

                if let Some(ref webhooks) = flow.webhooks
                    && let Some(ref url) = webhooks.on_failure
                {
                    crate::webhook::fire(url, &flow);
                }

                self.deactivate_if_idle(&task.queue_id).await?;
                self.evict_dep_graph(&task.flow_id);
            }
        }
        Ok(())
    }

    /// Dispatch ready tasks concurrently, respecting concurrency and rate limits.
    /// Tasks are spawned as tokio tasks and run in parallel.
    #[instrument(skip(self, queue_config), fields(queue_id = %queue_id))]
    async fn dispatch_ready_tasks(
        self: &Arc<Self>,
        queue_id: &QueueId,
        queue_config: &QueueConfig,
    ) -> Result<(), EngineError> {
        // Get or create the semaphore for this queue
        let semaphore = self.ensure_semaphore(queue_id, queue_config.concurrency);

        // Only fetch as many tasks as we can actually dispatch
        let available = semaphore.available_permits();
        if available == 0 {
            return Ok(());
        }
        let fetch_limit = available.min(self.config.batch_size);

        let t0 = std::time::Instant::now();
        let ready = self.store.fetch_ready_tasks(queue_id, fetch_limit).await?;
        crate::perf::counters::fetch_ready.record(t0);

        // Update the ready gauge
        metrics::gauge!(
            "tasked_tasks_ready",
            "queue_id" => queue_id.as_str().to_owned()
        )
        .set(ready.len() as f64);

        // Get rate limiter if configured
        let rate_limiter = self.ensure_rate_limiter(queue_id, &queue_config.rate_limit);

        let dispatch_start = std::time::Instant::now();

        // --- Phase 1: Collect eligible tasks ---
        // Walk the ready list doing condition eval, var resolution, rate limiting,
        // permit acquisition, and executor lookup. Build a list of eligible tasks.
        struct EligibleTask {
            task: Task,
            executor: Arc<dyn Executor>,
            permit: tokio::sync::OwnedSemaphorePermit,
            trigger_depth: u32,
        }
        let mut eligible: Vec<EligibleTask> = Vec::with_capacity(ready.len());
        let mut batch_ids: Vec<(TaskId, FlowId)> = Vec::with_capacity(ready.len());

        for mut task in ready {
            // Phase 1b: Skip resolve_variables when no interpolation patterns present.
            let needs_vars = Self::needs_interpolation(&task.executor_config)
                || task.input.as_ref().is_some_and(Self::needs_interpolation);
            if needs_vars {
                let t0 = std::time::Instant::now();
                // Phase 1d: Pass queue_config to avoid re-fetching from storage.
                self.resolve_variables(&mut task, Some(queue_config))
                    .await?;
                crate::perf::counters::resolve_vars.record(t0);
            }

            // Evaluate condition before dispatch
            if let Some(ref condition) = task.condition {
                match self.evaluate_condition(&task, condition).await {
                    ConditionResult::Proceed => { /* continue to dispatch */ }
                    ConditionResult::Handled => continue,
                    ConditionResult::Err(e) => return Err(e),
                }
            }

            // Check rate limit first (if configured)
            if let Some(ref rl) = rate_limiter
                && !rl.try_acquire()
            {
                debug!(
                    task_id = %task.id,
                    queue_id = %queue_id,
                    "rate limited, skipping remaining tasks"
                );
                break;
            }

            // Try to acquire a concurrency permit (non-blocking)
            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    debug!(
                        queue_id = %queue_id,
                        "concurrency limit reached, skipping remaining tasks"
                    );
                    break;
                }
            };

            let executor = self
                .executors
                .get(&task.executor_type)
                .ok_or_else(|| EngineError::NoExecutor(task.executor_type.clone()))?
                .clone();

            // Resolve trigger depth from engine-level cache (persists across cycles).
            let t0 = std::time::Instant::now();
            let trigger_depth = self.cached_trigger_depth(&task.flow_id).await?;
            crate::perf::counters::get_flow_depth.record(t0);

            batch_ids.push((task.id.clone(), task.flow_id.clone()));
            eligible.push(EligibleTask {
                task,
                executor,
                permit,
                trigger_depth,
            });
        }

        // --- Phase 2: Batch mark running ---
        let batch_refs: Vec<(&TaskId, &FlowId)> =
            batch_ids.iter().map(|(tid, fid)| (tid, fid)).collect();
        let t0 = std::time::Instant::now();
        let marked = self.store.mark_tasks_running_batch(&batch_refs).await?;
        crate::perf::counters::mark_running.record(t0);

        let marked_set: HashSet<(TaskId, FlowId)> = marked.into_iter().collect();

        // --- Phase 3: Spawn executors ---
        for entry in eligible {
            let key = (entry.task.id.clone(), entry.task.flow_id.clone());
            if !marked_set.contains(&key) {
                // Task was no longer in Ready state (e.g. cancelled by fail_fast).
                // Permit drops automatically, returning the semaphore slot.
                debug!(
                    task_id = %entry.task.id,
                    flow_id = %entry.task.flow_id,
                    "task no longer ready, skipping"
                );
                continue;
            }

            debug!(task_id = %entry.task.id, flow_id = %entry.task.flow_id, executor = %entry.task.executor_type, "dispatching task");

            // Metrics
            metrics::counter!(
                "tasked_tasks_dispatched_total",
                "queue_id" => queue_id.as_str().to_owned(),
                "executor" => entry.task.executor_type.clone()
            )
            .increment(1);
            self.stats
                .tasks_dispatched
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            // Spawn task execution concurrently
            let engine = self.clone();
            let store = self.store.clone();
            let submitter: Arc<dyn FlowSubmitter> = Arc::new(EngineFlowSubmitter(self.clone()));
            let artifacts_dir = self
                .artifacts
                .as_ref()
                .and_then(|a| a.local_dir(&entry.task.flow_id));
            let queue_id_label = queue_id.as_str().to_owned();
            let executor_label = entry.task.executor_type.clone();
            let task = entry.task;
            let executor = entry.executor;
            let permit = entry.permit;
            let trigger_depth = entry.trigger_depth;
            tokio::spawn(async move {
                let ctx = ExecutionContext::new(store, task.id.clone(), task.flow_id.clone())
                    .with_artifacts(artifacts_dir, None)
                    .with_flow_submitter(submitter)
                    .with_trigger_depth(trigger_depth)
                    .with_concurrency_permit(permit);
                let dispatch_time = std::time::Instant::now();
                let result = executor.execute(&task, &ctx).await;
                metrics::histogram!(
                    "tasked_task_execution_duration_seconds",
                    "executor" => executor_label,
                    "queue_id" => queue_id_label,
                )
                .record(dispatch_time.elapsed().as_secs_f64());
                // Permit is released when ctx is dropped (or earlier by executors
                // that call release_concurrency_permit, e.g., trigger with wait).
                let task_queue_id = task.queue_id.clone();
                match result {
                    ExecuteResult::Success { output } => {
                        // Batch success completions: resolve deps in-memory, buffer for batch write.
                        // Dep graph is ensured in process_completions_batch (not here) to avoid
                        // concurrent Mutex contention from spawned tasks.
                        let newly_ready = engine.resolve_deps_in_memory(&task.flow_id, &task.id);
                        engine
                            .stats
                            .tasks_completed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        engine
                            .completion_buffer
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .push(CompletionEvent {
                                task,
                                output,
                                newly_ready,
                            });
                    }
                    other => {
                        // Non-success results handled directly (rare path)
                        if let Err(e) = engine.handle_task_result(&task, other).await {
                            error!(task_id = %task.id, flow_id = %task.flow_id, error = %e, "failed to handle task result");
                        }
                    }
                }
                // Wake the specific queue's worker to process newly ready dependents
                engine.notify_queue(&task_queue_id);
            });
        }
        crate::perf::counters::dispatch_total.record(dispatch_start);

        Ok(())
    }

    /// Dispatch ready tasks synchronously (inline), still respecting rate limits.
    /// Used by tests and CLI run mode.
    #[instrument(skip(self, queue_config), fields(queue_id = %queue_id))]
    async fn dispatch_ready_tasks_sync(
        &self,
        queue_id: &QueueId,
        queue_config: &QueueConfig,
    ) -> Result<(), EngineError> {
        let ready = self
            .store
            .fetch_ready_tasks(queue_id, self.config.batch_size)
            .await?;

        let rate_limiter = self.ensure_rate_limiter(queue_id, &queue_config.rate_limit);

        // --- Phase 1: Collect eligible tasks ---
        struct SyncEligible {
            task: Task,
            executor: Arc<dyn Executor>,
        }
        let mut eligible: Vec<SyncEligible> = Vec::with_capacity(ready.len());
        let mut batch_ids: Vec<(TaskId, FlowId)> = Vec::with_capacity(ready.len());

        for mut task in ready {
            // Re-check task state: it may have been cancelled (e.g. by fail_fast)
            // between the batch fetch and this iteration.
            if let Some(current) = self.store.get_task(&task.id, &task.flow_id).await?
                && current.state != TaskState::Ready
            {
                continue;
            }

            // Check rate limit
            if let Some(ref rl) = rate_limiter
                && !rl.try_acquire()
            {
                break;
            }

            // Resolve variable references in executor_config and input
            self.resolve_variables(&mut task, None).await?;

            // Evaluate condition before dispatch
            if let Some(ref condition) = task.condition {
                match self.evaluate_condition(&task, condition).await {
                    ConditionResult::Proceed => { /* continue to dispatch */ }
                    ConditionResult::Handled => continue,
                    ConditionResult::Err(e) => return Err(e),
                }
            }

            let executor = self
                .executors
                .get(&task.executor_type)
                .ok_or_else(|| EngineError::NoExecutor(task.executor_type.clone()))?
                .clone();

            batch_ids.push((task.id.clone(), task.flow_id.clone()));
            eligible.push(SyncEligible { task, executor });
        }

        // --- Phase 2: Batch mark running ---
        let batch_refs: Vec<(&TaskId, &FlowId)> =
            batch_ids.iter().map(|(tid, fid)| (tid, fid)).collect();
        let marked = self.store.mark_tasks_running_batch(&batch_refs).await?;
        let marked_set: HashSet<(TaskId, FlowId)> = marked.into_iter().collect();

        // --- Phase 3: Execute tasks inline ---
        for entry in eligible {
            let key = (entry.task.id.clone(), entry.task.flow_id.clone());
            if !marked_set.contains(&key) {
                continue;
            }

            let artifacts_dir = self
                .artifacts
                .as_ref()
                .and_then(|a| a.local_dir(&entry.task.flow_id));
            let ctx = ExecutionContext::new(
                self.store.clone(),
                entry.task.id.clone(),
                entry.task.flow_id.clone(),
            )
            .with_artifacts(artifacts_dir, None);
            let result = entry.executor.execute(&entry.task, &ctx).await;
            self.handle_task_result(&entry.task, result).await?;
        }

        Ok(())
    }

    /// Handle the result of a task execution.
    #[instrument(skip_all, fields(task_id = %task.id, flow_id = %task.flow_id))]
    pub async fn handle_task_result(
        &self,
        task: &Task,
        result: ExecuteResult,
    ) -> Result<(), EngineError> {
        let completion_start = std::time::Instant::now();
        self.stats
            .tasks_completed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // No explicit cancel check — the conditional UPDATEs in complete_task_with_ready,
        // mark_task_failed, and mark_task_delayed all use WHERE state IN (...) and return
        // InvalidStateTransition if the task was cancelled between dispatch and completion.

        match result {
            ExecuteResult::Success { output } => {
                debug!(task_id = %task.id, flow_id = %task.flow_id, "task succeeded");

                // Resolve deps in-memory (avoids NOT EXISTS SQL query).
                // Graph is built lazily on first completion if not cached (e.g., recovered flows).
                self.ensure_dep_graph(&task.flow_id).await;
                let t0 = std::time::Instant::now();
                let (flow, newly_ready) = if let Some(ready) =
                    self.resolve_deps_in_memory(&task.flow_id, &task.id)
                {
                    match self
                        .store
                        .complete_task_with_ready(&task.id, &task.flow_id, output, &ready)
                        .await
                    {
                        Ok(flow) => (flow, ready),
                        Err(StorageError::InvalidStateTransition(ref from, _))
                            if from == "cancelled" || from == "succeeded" =>
                        {
                            debug!(task_id = %task.id, flow_id = %task.flow_id, "skipping result for already-{from} task");
                            return Ok(());
                        }
                        Err(e) => return Err(e.into()),
                    }
                } else {
                    match self
                        .store
                        .complete_task_success(&task.id, &task.flow_id, output)
                        .await
                    {
                        Ok(result) => result,
                        Err(StorageError::InvalidStateTransition(ref from, _))
                            if from == "cancelled" || from == "succeeded" =>
                        {
                            debug!(task_id = %task.id, flow_id = %task.flow_id, "skipping result for already-{from} task");
                            return Ok(());
                        }
                        Err(e) => return Err(e.into()),
                    }
                };
                crate::perf::counters::mark_succeeded.record(t0);

                // Metrics
                metrics::counter!(
                    "tasked_tasks_completed_total",
                    "queue_id" => task.queue_id.as_str().to_owned(),
                    "status" => "succeeded"
                )
                .increment(1);

                for tid in &newly_ready {
                    debug!(task_id = %tid, flow_id = %task.flow_id, "task promoted to ready");
                }

                // Check if flow is complete
                if flow.tasks_succeeded == flow.task_count {
                    self.store
                        .update_flow_state(&task.flow_id, FlowState::Succeeded)
                        .await?;

                    metrics::counter!(
                        "tasked_flows_completed_total",
                        "queue_id" => task.queue_id.as_str().to_owned(),
                        "status" => "succeeded"
                    )
                    .increment(1);

                    debug!(flow_id = %task.flow_id, "flow succeeded");
                    self.stats
                        .flows_completed
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    // Cleanup artifacts
                    if let Some(ref artifacts) = self.artifacts
                        && let Err(e) = artifacts.cleanup(&task.flow_id).await
                    {
                        warn!(flow_id = %task.flow_id, error = %e, "artifact cleanup failed");
                    }

                    // Fire on_complete webhook if configured
                    if let Some(ref webhooks) = flow.webhooks
                        && let Some(ref url) = webhooks.on_complete
                    {
                        crate::webhook::fire(url, &flow);
                    }

                    // Deactivate queue if no more running flows remain
                    self.deactivate_if_idle(&task.queue_id).await?;
                    self.evict_dep_graph(&task.flow_id);
                }
                crate::perf::counters::completion_total.record(completion_start);
            }
            ExecuteResult::Failed { error, retryable } => {
                warn!(task_id = %task.id, flow_id = %task.flow_id, error = %error, "task failed");

                if retryable && task.retries_remaining > 0 {
                    // Schedule retry -- compute which attempt this is so backoff
                    // escalates. retries_remaining starts at max_retries and is
                    // decremented each retry, so attempt = max_retries - retries_remaining.
                    let max_retries = self
                        .store
                        .get_queue(&task.queue_id)
                        .await?
                        .map(|q| q.config.max_retries)
                        .unwrap_or(task.retries_remaining);
                    let attempt = max_retries.saturating_sub(task.retries_remaining);
                    let delay_ms = task.backoff.delay_ms(attempt);
                    let retry_at = Utc::now() + Duration::milliseconds(delay_ms as i64);

                    match self
                        .store
                        .mark_task_delayed(&task.id, &task.flow_id, retry_at)
                        .await
                    {
                        Ok(()) => {}
                        Err(StorageError::InvalidStateTransition(ref from, _))
                            if from == "cancelled" =>
                        {
                            debug!(task_id = %task.id, flow_id = %task.flow_id, "skipping retry for already-cancelled task");
                            return Ok(());
                        }
                        Err(e) => return Err(e.into()),
                    }
                    self.delayed_task_count
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    // Metrics
                    metrics::counter!(
                        "tasked_tasks_retried_total",
                        "queue_id" => task.queue_id.as_str().to_owned()
                    )
                    .increment(1);

                    debug!(
                        task_id = %task.id,
                        flow_id = %task.flow_id,
                        retries_remaining = task.retries_remaining - 1,
                        retry_at = %retry_at,
                        "task scheduled for retry"
                    );
                } else {
                    // Terminal failure
                    match self
                        .store
                        .mark_task_failed(&task.id, &task.flow_id, &error)
                        .await
                    {
                        Ok(()) => {}
                        Err(StorageError::InvalidStateTransition(ref from, _))
                            if from == "cancelled" =>
                        {
                            debug!(task_id = %task.id, flow_id = %task.flow_id, "skipping failure for already-cancelled task");
                            return Ok(());
                        }
                        Err(e) => return Err(e.into()),
                    }

                    // Metrics
                    metrics::counter!(
                        "tasked_tasks_completed_total",
                        "queue_id" => task.queue_id.as_str().to_owned(),
                        "status" => "failed"
                    )
                    .increment(1);

                    // Cascade: cancel all dependents
                    self.cascade_cancel(&task.id, &task.flow_id).await?;

                    // If fail_fast, cancel ALL remaining non-terminal tasks
                    if let Some(ff_flow) = self.store.get_flow(&task.flow_id).await?
                        && ff_flow.fail_fast
                    {
                        let all_tasks = self.store.get_flow_tasks(&task.flow_id).await?;
                        for t in &all_tasks {
                            if !t.state.is_terminal()
                                && t.state.can_transition_to(TaskState::Cancelled)
                            {
                                self.store
                                    .update_task_state(&t.id, &task.flow_id, TaskState::Cancelled)
                                    .await?;

                                metrics::counter!(
                                    "tasked_tasks_completed_total",
                                    "queue_id" => t.queue_id.as_str().to_owned(),
                                    "status" => "cancelled"
                                )
                                .increment(1);

                                debug!(
                                    task_id = %t.id,
                                    flow_id = %task.flow_id,
                                    "task cancelled (fail_fast)"
                                );
                            }
                        }

                        // Propagate cancellation to child flows
                        self.cancel_child_flows(&task.flow_id).await;
                    }

                    // Update flow counter and state
                    let flow = self
                        .store
                        .increment_flow_counter(&task.flow_id, false)
                        .await?;

                    // Check if all non-cancelled tasks are terminal
                    let tasks = self.store.get_flow_tasks(&task.flow_id).await?;
                    let all_terminal = tasks.iter().all(|t| t.state.is_terminal());
                    if all_terminal {
                        self.store
                            .update_flow_state(&flow.id, FlowState::Failed)
                            .await?;

                        metrics::counter!(
                            "tasked_flows_completed_total",
                            "queue_id" => task.queue_id.as_str().to_owned(),
                            "status" => "failed"
                        )
                        .increment(1);

                        debug!(flow_id = %task.flow_id, "flow failed");
                        self.stats
                            .flows_completed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        // Cleanup artifacts
                        if let Some(ref artifacts) = self.artifacts
                            && let Err(e) = artifacts.cleanup(&task.flow_id).await
                        {
                            warn!(flow_id = %task.flow_id, error = %e, "artifact cleanup failed");
                        }

                        // Fire on_failure webhook if configured
                        if let Some(ref webhooks) = flow.webhooks
                            && let Some(ref url) = webhooks.on_failure
                        {
                            crate::webhook::fire(url, &flow);
                        }

                        // Deactivate queue if no more running flows remain
                        self.deactivate_if_idle(&task.queue_id).await?;
                        self.evict_dep_graph(&task.flow_id);
                    }
                }
            }
            ExecuteResult::AwaitingApproval { output } => {
                debug!(task_id = %task.id, flow_id = %task.flow_id, "task awaiting approval");
                // Write the approval info to the task's output so it's visible via API,
                // but leave the task in Running state. It will be completed via /ack.
                self.store
                    .set_task_output(&task.id, &task.flow_id, output)
                    .await?;
            }
            ExecuteResult::Spawn {
                output,
                tasks: task_defs,
            } => {
                debug!(task_id = %task.id, flow_id = %task.flow_id,
                      count = task_defs.len(), "spawn tasks generated");

                match self.inject_spawn_tasks(task, &task_defs).await {
                    Ok(_injected) => {
                        // Resolve deps for generator in-memory (graph was already updated
                        // by inject_spawn_tasks), then write to storage.
                        let newly_ready = self.resolve_deps_in_memory(&task.flow_id, &task.id);
                        let flow = if let Some(ref ready) = newly_ready {
                            self.store
                                .complete_task_with_ready(&task.id, &task.flow_id, output, ready)
                                .await?
                        } else {
                            // Fallback: no dep graph, use SQL
                            self.store
                                .mark_task_succeeded(&task.id, &task.flow_id, output)
                                .await?;
                            let flow = self
                                .store
                                .increment_flow_counter(&task.flow_id, true)
                                .await?;
                            self.store.resolve_ready_tasks(&task.flow_id).await?;
                            flow
                        };

                        metrics::counter!(
                            "tasked_tasks_completed_total",
                            "queue_id" => task.queue_id.as_str().to_owned(),
                            "status" => "succeeded"
                        )
                        .increment(1);

                        let newly_ready = newly_ready.unwrap_or_default();
                        for tid in &newly_ready {
                            debug!(task_id = %tid, flow_id = %task.flow_id, "task promoted to ready");
                        }

                        // Check if flow actually completed (unlikely right after injection)
                        if flow.tasks_succeeded == flow.task_count {
                            self.store
                                .update_flow_state(&task.flow_id, FlowState::Succeeded)
                                .await?;

                            metrics::counter!(
                                "tasked_flows_completed_total",
                                "queue_id" => task.queue_id.as_str().to_owned(),
                                "status" => "succeeded"
                            )
                            .increment(1);

                            // Cleanup artifacts
                            if let Some(ref artifacts) = self.artifacts
                                && let Err(e) = artifacts.cleanup(&task.flow_id).await
                            {
                                warn!(flow_id = %task.flow_id, error = %e, "artifact cleanup failed");
                            }

                            if let Some(ref webhooks) = flow.webhooks
                                && let Some(ref url) = webhooks.on_complete
                            {
                                crate::webhook::fire(url, &flow);
                            }

                            // Deactivate queue if no more running flows remain
                            self.deactivate_if_idle(&task.queue_id).await?;
                            self.evict_dep_graph(&task.flow_id);
                        }
                    }
                    Err(e) => {
                        // Injection failed -- fail the generator task
                        warn!(task_id = %task.id, error = %e, "spawn injection failed");
                        self.store
                            .mark_task_failed(
                                &task.id,
                                &task.flow_id,
                                &format!("spawn injection failed: {e}"),
                            )
                            .await?;

                        metrics::counter!(
                            "tasked_tasks_completed_total",
                            "queue_id" => task.queue_id.as_str().to_owned(),
                            "status" => "failed"
                        )
                        .increment(1);

                        self.cascade_cancel(&task.id, &task.flow_id).await?;

                        // Also cancel tasks with deferred deps on this generator's
                        // namespace -- those deps can never be fulfilled now.
                        let spawn_prefix = format!("{}/", task.id.as_str());
                        let all_flow_tasks = self.store.get_flow_tasks(&task.flow_id).await?;
                        for ft in &all_flow_tasks {
                            if ft.state.is_terminal() {
                                continue;
                            }
                            let deps = self
                                .store
                                .get_task_dependencies(&ft.id, &task.flow_id)
                                .await?;
                            let has_deferred =
                                deps.iter().any(|d| d.as_str().starts_with(&spawn_prefix));
                            if has_deferred && ft.state.can_transition_to(TaskState::Cancelled) {
                                self.store
                                    .update_task_state(&ft.id, &task.flow_id, TaskState::Cancelled)
                                    .await?;

                                metrics::counter!(
                                    "tasked_tasks_completed_total",
                                    "queue_id" => task.queue_id.as_str().to_owned(),
                                    "status" => "cancelled"
                                )
                                .increment(1);

                                // Cascade further from this cancelled task
                                self.cascade_cancel(&ft.id, &task.flow_id).await?;
                            }
                        }

                        let flow = self
                            .store
                            .increment_flow_counter(&task.flow_id, false)
                            .await?;

                        let tasks = self.store.get_flow_tasks(&task.flow_id).await?;
                        let all_terminal = tasks.iter().all(|t| t.state.is_terminal());
                        if all_terminal {
                            self.store
                                .update_flow_state(&flow.id, FlowState::Failed)
                                .await?;

                            metrics::counter!(
                                "tasked_flows_completed_total",
                                "queue_id" => task.queue_id.as_str().to_owned(),
                                "status" => "failed"
                            )
                            .increment(1);

                            // Cleanup artifacts
                            if let Some(ref artifacts) = self.artifacts
                                && let Err(e) = artifacts.cleanup(&task.flow_id).await
                            {
                                warn!(flow_id = %task.flow_id, error = %e, "artifact cleanup failed");
                            }

                            if let Some(ref webhooks) = flow.webhooks
                                && let Some(ref url) = webhooks.on_failure
                            {
                                crate::webhook::fire(url, &flow);
                            }

                            // Deactivate queue if no more running flows remain
                            self.deactivate_if_idle(&task.queue_id).await?;
                            self.evict_dep_graph(&task.flow_id);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Inject spawn-generated tasks into the running flow.
    async fn inject_spawn_tasks(
        &self,
        generator: &Task,
        task_defs: &[TaskDef],
    ) -> Result<usize, EngineError> {
        if task_defs.is_empty() {
            return Ok(0);
        }

        let prefix = format!("{}/", generator.id.as_str());
        let depth = generator.id.as_str().matches('/').count() + 1;
        // Depth is the nesting level of the *generator*, not the generated tasks.
        // A depth of N means N levels of spawn-within-spawn. The generated tasks
        // themselves sit at depth N and can run any executor, but if they are
        // spawn executors, they'll be at depth N+1 when they try to inject.
        // With max_spawn_depth=8, you get 8 levels of nested generators.
        if depth > self.config.max_spawn_depth {
            return Err(EngineError::Spawn(format!(
                "spawn depth limit ({}) exceeded",
                self.config.max_spawn_depth
            )));
        }

        // Check for duplicate IDs
        let mut seen = HashSet::new();
        for def in task_defs {
            if !seen.insert(&def.id) {
                return Err(EngineError::Spawn(format!(
                    "duplicate task id '{}'",
                    def.id
                )));
            }
        }

        // Validate executor types
        for def in task_defs {
            if !self.executors.contains_key(&def.executor) {
                return Err(EngineError::NoExecutor(def.executor.clone()));
            }
        }

        // Namespace IDs and rewrite depends_on
        let generated_ids: HashSet<String> =
            task_defs.iter().map(|d| d.id.as_str().to_owned()).collect();

        let namespaced_defs: Vec<TaskDef> = task_defs
            .iter()
            .map(|def| {
                let new_id = TaskId::from(format!("{}{}", prefix, def.id.as_str()));
                let new_deps: Vec<TaskId> = def
                    .depends_on
                    .iter()
                    .map(|dep| {
                        if generated_ids.contains(dep.as_str()) {
                            TaskId::from(format!("{}{}", prefix, dep.as_str()))
                        } else {
                            dep.clone() // Allow external references (to existing flow tasks)
                        }
                    })
                    .collect();
                TaskDef {
                    id: new_id,
                    depends_on: new_deps,
                    ..def.clone()
                }
            })
            .collect();

        // Validate subgraph: check that all internal depends_on references are valid
        let namespaced_ids: HashSet<String> = namespaced_defs
            .iter()
            .map(|d| d.id.as_str().to_owned())
            .collect();
        for def in &namespaced_defs {
            for dep in &def.depends_on {
                if dep.as_str().starts_with(&prefix) && !namespaced_ids.contains(dep.as_str()) {
                    return Err(EngineError::Spawn(format!(
                        "generated task '{}' depends on unknown generated task '{}'",
                        def.id, dep
                    )));
                }
            }
        }

        // Check for ID collisions with existing flow tasks.
        // NOTE: This loads all tasks in the flow. At 1000+ generated tasks, the
        // inject_tasks transaction dominates (~667% overhead in benchmarks), not this
        // scan. Optimize with point lookups if spawn sizes regularly exceed 1000.
        let existing = self.store.get_flow_tasks(&generator.flow_id).await?;

        // Enforce per-flow task count limit
        let total = existing.len() + namespaced_defs.len();
        if total > self.config.max_tasks_per_flow {
            return Err(EngineError::TaskLimitExceeded(
                self.config.max_tasks_per_flow,
            ));
        }

        let existing_ids: HashSet<String> =
            existing.iter().map(|t| t.id.as_str().to_owned()).collect();
        for def in &namespaced_defs {
            if existing_ids.contains(def.id.as_str()) {
                return Err(EngineError::Spawn(format!(
                    "task id '{}' already exists in flow",
                    def.id
                )));
            }
        }

        // Validate external deps exist in existing flow
        for def in &namespaced_defs {
            for dep in &def.depends_on {
                if !dep.as_str().starts_with(&prefix)
                    && !namespaced_ids.contains(dep.as_str())
                    && !existing_ids.contains(dep.as_str())
                {
                    return Err(EngineError::Spawn(format!(
                        "generated task '{}' depends on unknown task '{}'",
                        def.id, dep
                    )));
                }
            }
        }

        // Validate spawn_output contract: check that deferred deps targeting this
        // generator's namespace actually exist in the generated set
        for existing_task in &existing {
            let deps = self
                .store
                .get_task_dependencies(&existing_task.id, &generator.flow_id)
                .await?;
            for dep_id in &deps {
                if dep_id.as_str().starts_with(&prefix) && !namespaced_ids.contains(dep_id.as_str())
                {
                    return Err(EngineError::Spawn(format!(
                        "downstream task '{}' depends on '{}' which was not generated",
                        existing_task.id, dep_id
                    )));
                }
            }
        }

        // Build dependency map: wire root tasks to depend on generator
        let mut deps_map: HashMap<TaskId, Vec<TaskId>> = HashMap::new();
        for def in &namespaced_defs {
            if !def.depends_on.is_empty() {
                deps_map.insert(def.id.clone(), def.depends_on.clone());
            }
        }

        // Find roots (no deps among generated set) and add generator as dependency
        for def in &namespaced_defs {
            let has_internal_dep = def
                .depends_on
                .iter()
                .any(|d| d.as_str().starts_with(&prefix));
            if !has_internal_dep {
                deps_map
                    .entry(def.id.clone())
                    .or_default()
                    .push(generator.id.clone());
            }
        }

        // Get queue config for defaults
        let queue = self
            .store
            .get_queue(&generator.queue_id)
            .await?
            .ok_or_else(|| EngineError::QueueNotFound(generator.queue_id.as_str().to_owned()))?;

        // Build Task objects
        let now = chrono::Utc::now();
        let tasks: Vec<Task> = namespaced_defs
            .iter()
            .map(|def| Task {
                id: def.id.clone(),
                flow_id: generator.flow_id.clone(),
                queue_id: generator.queue_id.clone(),
                state: TaskState::Pending,
                executor_type: def.executor.clone(),
                executor_config: def.config.clone(),
                input: def.input.clone(),
                output: None,
                error: None,
                retries_remaining: def.retries.unwrap_or(queue.config.max_retries),
                backoff: def
                    .backoff
                    .clone()
                    .unwrap_or_else(|| queue.config.backoff.clone()),
                timeout_secs: def.timeout_secs.unwrap_or(queue.config.timeout_secs),
                condition: def.condition.clone(),
                retry_at: None,
                started_at: None,
                completed_at: None,
                created_at: now,
            })
            .collect();

        // Inject atomically
        self.store
            .inject_tasks(&generator.flow_id, &tasks, &deps_map)
            .await?;

        // Update the in-memory dep graph with the new tasks and dependencies.
        // Collect already-succeeded task IDs so inject can set correct unsatisfied counts.
        {
            let succeeded: HashSet<TaskId> = existing
                .iter()
                .filter(|t| t.state == TaskState::Succeeded)
                .map(|t| t.id.clone())
                .collect();
            let new_task_ids: Vec<TaskId> = tasks.iter().map(|t| t.id.clone()).collect();
            self.dep_graphs
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry(generator.flow_id.clone())
                .and_modify(|graph| graph.inject(&deps_map, &new_task_ids, &succeeded));
        }

        let count = tasks.len();
        metrics::counter!(
            "tasked_spawn_tasks_injected_total",
            "queue_id" => generator.queue_id.as_str().to_owned()
        )
        .increment(count as u64);

        debug!(
            task_id = %generator.id,
            flow_id = %generator.flow_id,
            injected = count,
            "spawn tasks injected"
        );

        Ok(count)
    }

    // -- Schedule operations --

    /// Create a cron-based schedule that submits a flow on each trigger.
    pub async fn create_schedule(
        &self,
        queue_id: &QueueId,
        def: ScheduleDef,
    ) -> Result<Schedule, EngineError> {
        // Verify queue exists
        self.store
            .get_queue(queue_id)
            .await?
            .ok_or_else(|| EngineError::QueueNotFound(queue_id.as_str().to_owned()))?;
        // Validate cron
        let normalized = crate::schedule::normalize_cron(&def.cron);
        cron::Schedule::from_str(&normalized)
            .map_err(|e| EngineError::InvalidCronExpression(e.to_string()))?;
        // Validate DAG
        let task_ids: Vec<TaskId> = def.flow.tasks.iter().map(|t| t.id.clone()).collect();
        let deps: HashMap<TaskId, Vec<TaskId>> = def
            .flow
            .tasks
            .iter()
            .filter(|t| !t.depends_on.is_empty())
            .map(|t| (t.id.clone(), t.depends_on.clone()))
            .collect();
        TaskGraph::build(&task_ids, &deps)?;
        // Validate executors
        for task_def in &def.flow.tasks {
            if !self.executors.contains_key(&task_def.executor) {
                return Err(EngineError::NoExecutor(task_def.executor.clone()));
            }
        }
        let now = Utc::now();
        let next_run = if def.enabled {
            crate::schedule::compute_next_run(&normalized, now)
        } else {
            None
        };
        let schedule = Schedule {
            id: ScheduleId::new(),
            queue_id: queue_id.clone(),
            name: def.name,
            cron: def.cron,
            flow_def: def.flow,
            enabled: def.enabled,
            last_triggered_at: None,
            next_run_at: next_run,
            created_at: now,
            updated_at: now,
        };
        self.store.create_schedule(&schedule).await?;
        Ok(schedule)
    }

    /// Get a schedule by ID, or `None` if it doesn't exist.
    pub async fn get_schedule(&self, id: &ScheduleId) -> Result<Option<Schedule>, EngineError> {
        Ok(self.store.get_schedule(id).await?)
    }

    /// List all schedules for a queue.
    pub async fn list_schedules(&self, queue_id: &QueueId) -> Result<Vec<Schedule>, EngineError> {
        Ok(self.store.list_schedules(queue_id).await?)
    }

    /// Update a schedule's definition (cron expression, flow template, etc.).
    pub async fn update_schedule(
        &self,
        id: &ScheduleId,
        def: ScheduleDef,
    ) -> Result<Schedule, EngineError> {
        let existing = self.store.get_schedule(id).await?.ok_or_else(|| {
            EngineError::Storage(StorageError::ScheduleNotFound(id.as_str().to_owned()))
        })?;
        let normalized = crate::schedule::normalize_cron(&def.cron);
        cron::Schedule::from_str(&normalized)
            .map_err(|e| EngineError::InvalidCronExpression(e.to_string()))?;
        let task_ids: Vec<TaskId> = def.flow.tasks.iter().map(|t| t.id.clone()).collect();
        let deps: HashMap<TaskId, Vec<TaskId>> = def
            .flow
            .tasks
            .iter()
            .filter(|t| !t.depends_on.is_empty())
            .map(|t| (t.id.clone(), t.depends_on.clone()))
            .collect();
        TaskGraph::build(&task_ids, &deps)?;
        for task_def in &def.flow.tasks {
            if !self.executors.contains_key(&task_def.executor) {
                return Err(EngineError::NoExecutor(task_def.executor.clone()));
            }
        }
        let now = Utc::now();
        let next_run = if def.enabled {
            crate::schedule::compute_next_run(&normalized, now)
        } else {
            None
        };
        let schedule = Schedule {
            id: existing.id,
            queue_id: existing.queue_id,
            name: def.name,
            cron: def.cron,
            flow_def: def.flow,
            enabled: def.enabled,
            last_triggered_at: existing.last_triggered_at,
            next_run_at: next_run,
            created_at: existing.created_at,
            updated_at: now,
        };
        self.store.update_schedule(&schedule).await?;
        Ok(schedule)
    }

    /// Delete a schedule by ID.
    pub async fn delete_schedule(&self, id: &ScheduleId) -> Result<(), EngineError> {
        Ok(self.store.delete_schedule(id).await?)
    }

    /// Process due schedules: submit flows for any schedules whose next_run_at has passed.
    async fn process_schedules(self: &Arc<Self>) -> Result<(), EngineError> {
        let due = self.store.fetch_due_schedules().await?;
        for schedule in due {
            info!(schedule_id = %schedule.id, queue_id = %schedule.queue_id, "triggering scheduled flow");
            match self
                .submit_flow(&schedule.queue_id, schedule.flow_def.clone())
                .await
            {
                Ok(flow) => {
                    info!(schedule_id = %schedule.id, flow_id = %flow.id, "scheduled flow submitted");
                    metrics::counter!(
                        "tasked_scheduled_flows_triggered_total",
                        "queue_id" => schedule.queue_id.as_str().to_owned()
                    )
                    .increment(1);
                }
                Err(e) => {
                    error!(schedule_id = %schedule.id, error = %e, "failed to submit scheduled flow");
                }
            }
            let now = Utc::now();
            let normalized = crate::schedule::normalize_cron(&schedule.cron);
            let next_run = crate::schedule::compute_next_run(&normalized, now);
            self.store
                .mark_schedule_triggered(&schedule.id, now, next_run)
                .await?;
        }
        Ok(())
    }

    /// Delete terminal flows older than the queue's retention period.
    async fn cleanup_dead_flows(&self) -> Result<(), EngineError> {
        let queues = self.store.list_queues().await?;
        for queue in &queues {
            if let Some(retention_secs) = queue.config.retention_secs {
                let cutoff = Utc::now() - Duration::seconds(retention_secs as i64);
                let deleted = self
                    .store
                    .delete_terminal_flows_before(&queue.id, cutoff)
                    .await?;
                if deleted > 0 {
                    info!(queue_id = %queue.id, deleted = deleted, "cleaned up dead flows");
                }
            }
        }
        self.store.checkpoint().await?;
        Ok(())
    }

    /// Cancel all transitive dependents of a failed task.
    async fn cascade_cancel(&self, task_id: &TaskId, flow_id: &FlowId) -> Result<(), EngineError> {
        let dependents = self.store.get_task_dependents(task_id, flow_id).await?;
        for dep_id in dependents {
            let task = self.store.get_task(&dep_id, flow_id).await?;
            if let Some(task) = task
                && !task.state.is_terminal()
                && task.state.can_transition_to(TaskState::Cancelled)
            {
                self.store
                    .update_task_state(&dep_id, flow_id, TaskState::Cancelled)
                    .await?;

                metrics::counter!(
                    "tasked_tasks_completed_total",
                    "queue_id" => task.queue_id.as_str().to_owned(),
                    "status" => "cancelled"
                )
                .increment(1);

                debug!(task_id = %dep_id, flow_id = %flow_id, "task cancelled (dependency failed)");

                // Recurse into further dependents
                Box::pin(self.cascade_cancel(&dep_id, flow_id)).await?;
            }
        }
        Ok(())
    }

    /// Cancel all running child flows that were spawned by trigger tasks in this flow.
    /// Errors are logged and swallowed — a failure to cancel one child should not
    /// prevent cancellation of siblings.
    async fn cancel_child_flows(&self, flow_id: &FlowId) {
        let child_ids = match self.store.get_child_flow_ids(flow_id).await {
            Ok(ids) => ids,
            Err(e) => {
                warn!(flow_id = %flow_id, error = %e, "failed to query child flows for cancellation");
                return;
            }
        };
        for child_id in child_ids {
            if let Ok(Some(child)) = self.store.get_flow(&child_id).await
                && !child.state.is_terminal()
                && let Err(e) = Box::pin(self.cancel_flow(&child_id)).await
            {
                warn!(
                    parent_flow_id = %flow_id,
                    child_flow_id = %child_id,
                    error = %e,
                    "failed to cancel child flow"
                );
            }
        }
    }

    /// Recover tasks that have been running longer than their timeout.
    async fn recover_timed_out_tasks(&self) -> Result<(), EngineError> {
        let timed_out = self.store.fetch_timed_out_tasks().await?;
        for task in &timed_out {
            warn!(
                task_id = %task.id,
                flow_id = %task.flow_id,
                timeout_secs = task.timeout_secs,
                "task timed out"
            );

            self.handle_task_result(
                task,
                ExecuteResult::Failed {
                    error: format!("task timed out after {}s", task.timeout_secs),
                    retryable: true,
                },
            )
            .await?;
        }
        Ok(())
    }
}

/// Wrapper that implements FlowSubmitter by delegating to an Arc<Engine>.
/// Kept private — only used to bridge Engine into ExecutionContext.
struct EngineFlowSubmitter(Arc<Engine>);

#[async_trait::async_trait]
impl FlowSubmitter for EngineFlowSubmitter {
    async fn submit(
        &self,
        queue_id: &QueueId,
        flow_def: FlowDef,
        parent_depth: u32,
        parent_flow_id: Option<FlowId>,
    ) -> Result<Flow, String> {
        self.0
            .submit_flow_with_depth(queue_id, flow_def, parent_depth + 1, parent_flow_id)
            .await
            .map_err(|e| e.to_string())
    }

    async fn query_flow(&self, flow_id: &FlowId) -> Result<Option<Flow>, String> {
        self.0.get_flow(flow_id).await.map_err(|e| e.to_string())
    }
}

/// Per-queue worker loop. Each active queue gets one of these running as a
/// tokio task. The worker blocks on a per-queue `Notify`, waking only when
/// there is work for this specific queue — idle queues consume zero CPU.
async fn queue_worker_loop(engine: Arc<Engine>, queue_id: QueueId, notify: Arc<Notify>) {
    debug!(queue_id = %queue_id, "queue worker started");

    // Pre-notify so the first iteration runs immediately without blocking.
    notify.notify_one();

    loop {
        // Wait for work signal or 1-second fallback poll
        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
        }

        // Check if queue is still active
        let is_active = engine
            .active_queues
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&queue_id);
        if !is_active {
            debug!(queue_id = %queue_id, "queue worker exiting — no active flows");
            break;
        }

        // Get queue config (cache-first, fallback to storage)
        let config = match engine.get_cached_queue_config(&queue_id).await {
            Some(c) => c,
            None => continue,
        };

        // Process any buffered completions first — this flushes results from
        // previously dispatched tasks, which may promote new tasks to Ready.
        if let Err(e) = engine.process_completions_batch().await {
            error!(queue_id = %queue_id, error = %e, "completion batch failed");
        }

        // Dispatch ready tasks for THIS queue only
        if let Err(e) = engine.dispatch_ready_tasks(&queue_id, &config).await {
            error!(queue_id = %queue_id, error = %e, "dispatch failed");
        }
    }

    // Clean up worker handle and notifier
    engine
        .queue_workers
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&queue_id);
    engine
        .queue_notifiers
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&queue_id);
    debug!(queue_id = %queue_id, "queue worker stopped");
}

/// Global sweeper loop. Handles periodic cross-queue operations that used to
/// live in the old `run()` select! arms: recovery, cleanup, schedules, delayed
/// task promotion, and status reporting.
async fn global_sweeper_loop(engine: Arc<Engine>) {
    let mut recovery_interval = tokio::time::interval(engine.config.recovery_interval);
    let mut cleanup_interval = tokio::time::interval(engine.config.cleanup_interval);
    let mut schedule_interval = tokio::time::interval(engine.config.schedule_interval);
    let mut status_interval = tokio::time::interval(std::time::Duration::from_secs(5));
    let mut last_status = std::time::Instant::now();
    let mut last_flows_completed: u64 = 0;
    let mut last_tasks_completed: u64 = 0;

    loop {
        tokio::select! {
            _ = recovery_interval.tick() => {
                if let Err(e) = engine.recover_timed_out_tasks().await {
                    error!(error = %e, "recovery failed");
                }
            }
            _ = cleanup_interval.tick() => {
                if let Err(e) = engine.cleanup_dead_flows().await {
                    error!(error = %e, "dead flow cleanup failed");
                }
            }
            _ = schedule_interval.tick() => {
                if let Err(e) = engine.process_schedules().await {
                    error!(error = %e, "schedule processing failed");
                }
            }
            _ = status_interval.tick() => {
                let active_count = engine.active_queues
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .len();
                let worker_count = engine.queue_workers
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .len();
                let fs = engine.stats.flows_submitted.load(std::sync::atomic::Ordering::Relaxed);
                let fc = engine.stats.flows_completed.load(std::sync::atomic::Ordering::Relaxed);
                let td = engine.stats.tasks_dispatched.load(std::sync::atomic::Ordering::Relaxed);
                let tc = engine.stats.tasks_completed.load(std::sync::atomic::Ordering::Relaxed);

                if active_count > 0 || fc > 0 {
                    let dt = last_status.elapsed().as_secs_f64();
                    let f_rate = (fc - last_flows_completed) as f64 / dt;
                    let t_rate = (tc - last_tasks_completed) as f64 / dt;

                    let snap = crate::perf::snapshot();
                    info!(
                        "flows: {} active, {} done ({:.0}/s) | tasks: {} inflight, {} done ({:.0}/s) | queues: {} | workers: {}",
                        fs.saturating_sub(fc), fc, f_rate,
                        td.saturating_sub(tc), tc, t_rate,
                        active_count, worker_count,
                    );
                    info!("{snap}");
                    crate::perf::reset();

                    last_status = std::time::Instant::now();
                    last_flows_completed = fc;
                    last_tasks_completed = tc;
                }
            }
        }

        // Promote delayed tasks and wake specific queue workers
        if let Err(e) = engine.promote_delayed_tasks_and_wake().await {
            error!(error = %e, "delayed task promotion failed");
        }
    }
}

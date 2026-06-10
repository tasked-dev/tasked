use super::sqlite::SqliteStorage;
use super::{Storage, StorageError};
use crate::types::*;
use async_trait::async_trait;
use rusqlite::OptionalExtension;
use rusqlite::params;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// Sharded storage backend that routes to per-queue SQLite databases.
///
/// Queue metadata lives in a single `catalog.db`, while each queue's flows,
/// tasks, dependencies, and schedules are stored in a dedicated
/// `queues/<queue_id>.db` file. This keeps per-queue write contention low
/// and allows individual queues to be backed up or migrated independently.
pub struct ShardedStorage {
    /// Catalog database holding queue metadata and routing tables.
    catalog: SqliteStorage,
    /// Per-queue shard databases, opened lazily and cached.
    shards: RwLock<HashMap<QueueId, Arc<SqliteStorage>>>,
    /// In-memory flow_id → queue_id cache (avoids catalog DB Mutex for lookups).
    flow_cache: RwLock<HashMap<FlowId, QueueId>>,
    /// In-memory schedule_id → queue_id cache.
    schedule_cache: RwLock<HashMap<ScheduleId, QueueId>>,
    /// Root data directory (contains `catalog.db` and `queues/`).
    data_dir: PathBuf,
}

impl ShardedStorage {
    /// Open (or create) a sharded storage rooted at `data_dir`.
    ///
    /// Creates `data_dir/catalog.db` for queue metadata and
    /// `data_dir/queues/` for per-queue database files.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(data_dir.join("queues"))
            .map_err(|e| StorageError::Internal(format!("failed to create data dir: {e}")))?;

        let catalog = SqliteStorage::open_catalog(data_dir.join("catalog.db"))?;

        let storage = Self {
            catalog,
            shards: RwLock::new(HashMap::new()),
            flow_cache: RwLock::new(HashMap::new()),
            schedule_cache: RwLock::new(HashMap::new()),
            data_dir,
        };

        // Pre-open shards for any existing queue DB files on disk.
        storage.load_existing_shards()?;
        // Populate in-memory caches from catalog.
        storage.load_caches()?;

        Ok(storage)
    }

    /// Validate that a queue ID is safe to embed in a shard filename.
    ///
    /// Queue IDs are used to derive `queues/<queue_id>.db` paths, so anything
    /// containing path separators, `..`, or characters outside `[A-Za-z0-9._-]`
    /// is rejected to prevent path traversal outside the data directory.
    fn validate_queue_id_for_path(queue_id: &QueueId) -> Result<(), StorageError> {
        let s = queue_id.as_str();
        let valid = !s.is_empty()
            && !s.contains("..")
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
        if !valid {
            return Err(StorageError::Internal(format!(
                "invalid queue id {s:?} for sharded storage: queue ids must match \
                 [A-Za-z0-9._-]+ and must not contain '..'"
            )));
        }
        Ok(())
    }

    /// Return the filesystem path for a queue's shard database.
    /// Fails if the queue ID is not a safe filename component.
    fn queue_db_path(&self, queue_id: &QueueId) -> Result<PathBuf, StorageError> {
        Self::validate_queue_id_for_path(queue_id)?;
        Ok(self
            .data_dir
            .join("queues")
            .join(format!("{}.db", queue_id.as_str())))
    }

    /// Get a shard for `queue_id`, opening it if necessary.
    ///
    /// Opening (file creation + schema init) is blocking I/O, so it runs on
    /// the tokio blocking pool.
    async fn get_or_open_shard(
        &self,
        queue_id: &QueueId,
    ) -> Result<Arc<SqliteStorage>, StorageError> {
        // Fast path: read lock
        {
            let shards = self.shards.read().unwrap_or_else(|e| e.into_inner());
            if let Some(shard) = shards.get(queue_id) {
                return Ok(shard.clone());
            }
        }
        // Slow path: open on a blocking thread, then insert (first opener wins).
        let path = self.queue_db_path(queue_id)?;
        let opened = tokio::task::spawn_blocking(move || SqliteStorage::open_queue(&path))
            .await
            .map_err(|e| StorageError::Internal(format!("open shard task panicked: {e}")))??;
        let mut shards = self.shards.write().unwrap_or_else(|e| e.into_inner());
        Ok(shards
            .entry(queue_id.clone())
            .or_insert_with(|| Arc::new(opened))
            .clone())
    }

    /// Resolve a flow ID to the queue that owns it. Checks in-memory cache first,
    /// falls back to catalog DB only on cache miss.
    async fn resolve_flow_queue(&self, flow_id: &FlowId) -> Result<QueueId, StorageError> {
        // Fast path: in-memory cache (RwLock read — no DB contention)
        {
            let cache = self.flow_cache.read().unwrap_or_else(|e| e.into_inner());
            if let Some(qid) = cache.get(flow_id) {
                return Ok(qid.clone());
            }
        }
        // Slow path: catalog DB lookup + cache populate
        let fid = flow_id.as_str().to_owned();
        let qid = self
            .catalog
            .with_conn(move |conn| {
                Ok(conn
                    .query_row(
                        "SELECT queue_id FROM flow_map WHERE flow_id = ?1",
                        params![fid],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?)
            })
            .await?
            .map(QueueId::from)
            .ok_or_else(|| StorageError::FlowNotFound(flow_id.as_str().to_owned()))?;
        // Populate cache for future lookups
        self.flow_cache
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(flow_id.clone(), qid.clone());
        Ok(qid)
    }

    /// Resolve a schedule ID to the queue that owns it. Cache-first.
    async fn resolve_schedule_queue(
        &self,
        schedule_id: &ScheduleId,
    ) -> Result<QueueId, StorageError> {
        {
            let cache = self
                .schedule_cache
                .read()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(qid) = cache.get(schedule_id) {
                return Ok(qid.clone());
            }
        }
        let sid = schedule_id.as_str().to_owned();
        let qid = self
            .catalog
            .with_conn(move |conn| {
                Ok(conn
                    .query_row(
                        "SELECT queue_id FROM schedule_map WHERE schedule_id = ?1",
                        params![sid],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?)
            })
            .await?
            .map(QueueId::from)
            .ok_or_else(|| StorageError::ScheduleNotFound(schedule_id.as_str().to_owned()))?;
        self.schedule_cache
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(schedule_id.clone(), qid.clone());
        Ok(qid)
    }

    /// Load in-memory caches from catalog DB (called once at startup).
    fn load_caches(&self) -> Result<(), StorageError> {
        // Load flow_map
        {
            let rows = self.catalog.with_conn_sync(|conn| {
                let mut stmt = conn.prepare("SELECT flow_id, queue_id FROM flow_map")?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })?;
            let mut cache = self.flow_cache.write().unwrap_or_else(|e| e.into_inner());
            for (fid, qid) in rows {
                cache.insert(FlowId::from(fid), QueueId::from(qid));
            }
        }
        // Load schedule_map
        {
            let rows = self.catalog.with_conn_sync(|conn| {
                let mut stmt = conn.prepare("SELECT schedule_id, queue_id FROM schedule_map")?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })?;
            let mut cache = self
                .schedule_cache
                .write()
                .unwrap_or_else(|e| e.into_inner());
            for (sid, qid) in rows {
                cache.insert(ScheduleId::from(sid), QueueId::from(qid));
            }
        }
        Ok(())
    }

    /// Scan the `queues/` directory and open shard databases for every `.db` file found.
    fn load_existing_shards(&self) -> Result<(), StorageError> {
        let queues_dir = self.data_dir.join("queues");
        if let Ok(entries) = std::fs::read_dir(queues_dir) {
            let mut shards = self.shards.write().unwrap_or_else(|e| e.into_inner());
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "db")
                    && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                {
                    let qid = QueueId::from(stem);
                    if let std::collections::hash_map::Entry::Vacant(e) = shards.entry(qid) {
                        match SqliteStorage::open_queue(&path) {
                            Ok(s) => {
                                e.insert(Arc::new(s));
                            }
                            Err(err) => {
                                tracing::warn!(
                                    queue_id = stem,
                                    error = %err,
                                    "failed to open shard"
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Return the shard for a specific queue, if it is already open.
    pub fn get_shard(&self, queue_id: &QueueId) -> Option<Arc<SqliteStorage>> {
        self.shards
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(queue_id)
            .cloned()
    }

    /// Return a snapshot of all currently-open shards.
    fn all_shards(&self) -> Vec<Arc<SqliteStorage>> {
        let shards = self.shards.read().unwrap_or_else(|e| e.into_inner());
        shards.values().cloned().collect()
    }
}

#[async_trait]
impl Storage for ShardedStorage {
    // ---- Queue CRUD (catalog) ----

    async fn create_queue(&self, queue: &Queue) -> Result<(), StorageError> {
        // Reject queue IDs that are unsafe as shard filenames before any write.
        Self::validate_queue_id_for_path(&queue.id)?;
        // Insert queue metadata into catalog.
        self.catalog.create_queue(queue).await?;
        // Create the shard DB file (opens + initialises schema).
        self.get_or_open_shard(&queue.id).await?;
        Ok(())
    }

    async fn get_queue(&self, id: &QueueId) -> Result<Option<Queue>, StorageError> {
        self.catalog.get_queue(id).await
    }

    async fn list_queues(&self) -> Result<Vec<Queue>, StorageError> {
        self.catalog.list_queues().await
    }

    async fn delete_queue(&self, id: &QueueId) -> Result<(), StorageError> {
        // Clean up catalog routing tables for this queue.
        let qid = id.as_str().to_owned();
        self.catalog
            .with_conn(move |conn| {
                conn.execute("DELETE FROM flow_map WHERE queue_id = ?1", params![qid])?;
                conn.execute("DELETE FROM schedule_map WHERE queue_id = ?1", params![qid])?;
                Ok(())
            })
            .await?;

        // Evict from in-memory caches.
        {
            let mut fc = self.flow_cache.write().unwrap_or_else(|e| e.into_inner());
            fc.retain(|_, qid| qid != id);
        }
        {
            let mut sc = self
                .schedule_cache
                .write()
                .unwrap_or_else(|e| e.into_inner());
            sc.retain(|_, qid| qid != id);
        }

        // Remove queue row from catalog.
        self.catalog.delete_queue(id).await?;

        // Evict shard from the in-memory map and drop the connection.
        {
            let mut shards = self.shards.write().unwrap_or_else(|e| e.into_inner());
            shards.remove(id);
        }

        // Delete the shard DB file (and WAL/SHM companions).
        let base = self.queue_db_path(id)?;
        for suffix in &["", "-wal", "-shm"] {
            let p = base.with_extension(format!("db{suffix}"));
            let _ = std::fs::remove_file(p);
        }

        Ok(())
    }

    // ---- Flow methods ----

    async fn create_flow(
        &self,
        flow: &Flow,
        tasks: &[Task],
        deps: &HashMap<TaskId, Vec<TaskId>>,
    ) -> Result<(), StorageError> {
        let shard = self.get_or_open_shard(&flow.queue_id).await?;

        // Register the flow → queue mapping in the catalog FIRST, then write
        // the flow to the shard. A crash between the two steps leaves a stale
        // catalog entry (resolvable: lookups return FlowNotFound and the entry
        // is cleaned up lazily), whereas the reverse order would strand a
        // committed flow that no lookup could ever resolve.
        let fid = flow.id.as_str().to_owned();
        let qid = flow.queue_id.as_str().to_owned();
        let parent = flow
            .parent_flow_id
            .as_ref()
            .map(|id| id.as_str().to_owned());
        self.catalog
            .with_conn(move |conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO flow_map (flow_id, queue_id, parent_flow_id) VALUES (?1, ?2, ?3)",
                    params![fid, qid, parent],
                )?;
                Ok(())
            })
            .await?;

        if let Err(e) = shard.create_flow(flow, tasks, deps).await {
            // Best-effort rollback of the catalog mapping so the failed flow
            // does not linger in the routing table.
            let fid = flow.id.as_str().to_owned();
            let _ = self
                .catalog
                .with_conn(move |conn| {
                    conn.execute("DELETE FROM flow_map WHERE flow_id = ?1", params![fid])?;
                    Ok(())
                })
                .await;
            return Err(e);
        }

        // Populate in-memory cache.
        self.flow_cache
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(flow.id.clone(), flow.queue_id.clone());

        Ok(())
    }

    async fn get_flow(&self, id: &FlowId) -> Result<Option<Flow>, StorageError> {
        let queue_id = match self.resolve_flow_queue(id).await {
            Ok(qid) => qid,
            Err(StorageError::FlowNotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.get_flow(id).await
    }

    async fn list_flows(
        &self,
        queue_id: &QueueId,
        state: Option<FlowState>,
    ) -> Result<Vec<Flow>, StorageError> {
        let shard = self.get_or_open_shard(queue_id).await?;
        shard.list_flows(queue_id, state).await
    }

    async fn update_flow_state(&self, id: &FlowId, state: FlowState) -> Result<(), StorageError> {
        let queue_id = self.resolve_flow_queue(id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.update_flow_state(id, state).await
    }

    async fn increment_flow_counter(
        &self,
        id: &FlowId,
        succeeded: bool,
    ) -> Result<Flow, StorageError> {
        let queue_id = self.resolve_flow_queue(id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.increment_flow_counter(id, succeeded).await
    }

    // ---- Task methods ----

    async fn get_task(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<Option<Task>, StorageError> {
        let queue_id = match self.resolve_flow_queue(flow_id).await {
            Ok(qid) => qid,
            Err(StorageError::FlowNotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.get_task(task_id, flow_id).await
    }

    async fn get_flow_tasks(&self, flow_id: &FlowId) -> Result<Vec<Task>, StorageError> {
        let queue_id = self.resolve_flow_queue(flow_id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.get_flow_tasks(flow_id).await
    }

    async fn get_flow_with_tasks(
        &self,
        flow_id: &FlowId,
    ) -> Result<Option<(Flow, Vec<Task>)>, StorageError> {
        let queue_id = match self.resolve_flow_queue(flow_id).await {
            Ok(qid) => qid,
            Err(StorageError::FlowNotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.get_flow_with_tasks(flow_id).await
    }

    async fn fetch_ready_tasks(
        &self,
        queue_id: &QueueId,
        limit: usize,
    ) -> Result<Vec<Task>, StorageError> {
        let shard = self.get_or_open_shard(queue_id).await?;
        shard.fetch_ready_tasks(queue_id, limit).await
    }

    async fn fetch_delayed_tasks_due(&self) -> Result<Vec<Task>, StorageError> {
        let mut all = Vec::new();
        for shard in self.all_shards() {
            let mut tasks = shard.fetch_delayed_tasks_due().await?;
            all.append(&mut tasks);
        }
        Ok(all)
    }

    async fn fetch_timed_out_tasks(&self) -> Result<Vec<Task>, StorageError> {
        let mut all = Vec::new();
        for shard in self.all_shards() {
            let mut tasks = shard.fetch_timed_out_tasks().await?;
            all.append(&mut tasks);
        }
        Ok(all)
    }

    async fn update_task_state(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        new_state: TaskState,
    ) -> Result<(), StorageError> {
        let queue_id = self.resolve_flow_queue(flow_id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.update_task_state(task_id, flow_id, new_state).await
    }

    async fn mark_task_running(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<(), StorageError> {
        let queue_id = self.resolve_flow_queue(flow_id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.mark_task_running(task_id, flow_id).await
    }

    async fn mark_tasks_running_batch(
        &self,
        tasks: &[(&TaskId, &FlowId)],
    ) -> Result<Vec<(TaskId, FlowId)>, StorageError> {
        if tasks.is_empty() {
            return Ok(vec![]);
        }
        // Entries may span queues: group by resolved queue and execute per shard.
        let mut groups: HashMap<QueueId, Vec<(&TaskId, &FlowId)>> = HashMap::new();
        for &(task_id, flow_id) in tasks {
            let queue_id = self.resolve_flow_queue(flow_id).await?;
            groups.entry(queue_id).or_default().push((task_id, flow_id));
        }
        let mut succeeded = Vec::with_capacity(tasks.len());
        for (queue_id, group) in groups {
            let shard = self.get_or_open_shard(&queue_id).await?;
            succeeded.extend(shard.mark_tasks_running_batch(&group).await?);
        }
        Ok(succeeded)
    }

    async fn set_task_output(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: serde_json::Value,
    ) -> Result<(), StorageError> {
        let queue_id = self.resolve_flow_queue(flow_id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.set_task_output(task_id, flow_id, output).await
    }

    async fn mark_task_succeeded(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: Option<serde_json::Value>,
    ) -> Result<(), StorageError> {
        let queue_id = self.resolve_flow_queue(flow_id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.mark_task_succeeded(task_id, flow_id, output).await
    }

    async fn mark_task_failed(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        error: &str,
    ) -> Result<(), StorageError> {
        let queue_id = self.resolve_flow_queue(flow_id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.mark_task_failed(task_id, flow_id, error).await
    }

    async fn mark_task_delayed(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        retry_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StorageError> {
        let queue_id = self.resolve_flow_queue(flow_id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.mark_task_delayed(task_id, flow_id, retry_at).await
    }

    async fn get_flow_dependencies(
        &self,
        flow_id: &FlowId,
    ) -> Result<HashMap<TaskId, Vec<TaskId>>, StorageError> {
        let queue_id = self.resolve_flow_queue(flow_id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.get_flow_dependencies(flow_id).await
    }

    async fn get_task_dependencies(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<Vec<TaskId>, StorageError> {
        let queue_id = self.resolve_flow_queue(flow_id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.get_task_dependencies(task_id, flow_id).await
    }

    async fn get_task_dependents(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<Vec<TaskId>, StorageError> {
        let queue_id = self.resolve_flow_queue(flow_id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.get_task_dependents(task_id, flow_id).await
    }

    async fn resolve_ready_tasks(&self, flow_id: &FlowId) -> Result<Vec<TaskId>, StorageError> {
        let queue_id = self.resolve_flow_queue(flow_id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.resolve_ready_tasks(flow_id).await
    }

    async fn complete_task_success(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: Option<serde_json::Value>,
    ) -> Result<(Flow, Vec<TaskId>), StorageError> {
        let queue_id = self.resolve_flow_queue(flow_id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.complete_task_success(task_id, flow_id, output).await
    }

    async fn complete_task_with_ready(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: Option<serde_json::Value>,
        newly_ready: &[TaskId],
    ) -> Result<Flow, StorageError> {
        let queue_id = self.resolve_flow_queue(flow_id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard
            .complete_task_with_ready(task_id, flow_id, output, newly_ready)
            .await
    }

    async fn complete_tasks_with_ready_batch(
        &self,
        completions: &[(TaskId, FlowId, Option<serde_json::Value>, Vec<TaskId>)],
    ) -> Result<Vec<Option<Flow>>, StorageError> {
        if completions.is_empty() {
            return Ok(vec![]);
        }
        // Entries may span queues: group by resolved queue, execute per shard,
        // and stitch the per-shard results back into input order.
        let mut groups: HashMap<QueueId, Vec<usize>> = HashMap::new();
        for (i, (_, flow_id, _, _)) in completions.iter().enumerate() {
            let queue_id = self.resolve_flow_queue(flow_id).await?;
            groups.entry(queue_id).or_default().push(i);
        }
        let mut results: Vec<Option<Flow>> = vec![None; completions.len()];
        for (queue_id, indices) in groups {
            let shard = self.get_or_open_shard(&queue_id).await?;
            let subset: Vec<_> = indices.iter().map(|&i| completions[i].clone()).collect();
            let sub_results = shard.complete_tasks_with_ready_batch(&subset).await?;
            for (slot, result) in indices.into_iter().zip(sub_results) {
                results[slot] = result;
            }
        }
        Ok(results)
    }

    async fn inject_tasks(
        &self,
        flow_id: &FlowId,
        tasks: &[Task],
        deps: &HashMap<TaskId, Vec<TaskId>>,
    ) -> Result<Flow, StorageError> {
        let queue_id = self.resolve_flow_queue(flow_id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.inject_tasks(flow_id, tasks, deps).await
    }

    async fn get_child_flow_ids(
        &self,
        parent_flow_id: &FlowId,
    ) -> Result<Vec<FlowId>, StorageError> {
        let pid = parent_flow_id.as_str().to_owned();
        let rows = self
            .catalog
            .with_conn(move |conn| {
                let mut stmt =
                    conn.prepare_cached("SELECT flow_id FROM flow_map WHERE parent_flow_id = ?1")?;
                let rows = stmt
                    .query_map(params![pid], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await?;
        Ok(rows.into_iter().map(FlowId::from).collect())
    }

    async fn delete_terminal_flows_before(
        &self,
        queue_id: &QueueId,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<usize, StorageError> {
        let shard = self.get_or_open_shard(queue_id).await?;
        let deleted = shard.delete_terminal_flows_before(queue_id, cutoff).await?;

        // Lazily clean up stale flow_map entries: remove any flow_map rows
        // for this queue whose flow no longer exists in the shard.
        // We tolerate stale entries elsewhere (resolve_flow_queue returns
        // FlowNotFound if the shard doesn't have the flow), but this is a
        // good opportunity to keep the catalog tidy.
        if deleted > 0 {
            // Collect remaining flow IDs from the shard.
            let remaining = shard.list_flows(queue_id, None).await?;
            let remaining_ids: Vec<String> = remaining.iter().map(|f| f.id.to_string()).collect();

            // Evict deleted flows from in-memory cache.
            {
                let remaining_set: std::collections::HashSet<FlowId> =
                    remaining.into_iter().map(|f| f.id).collect();
                let mut fc = self.flow_cache.write().unwrap_or_else(|e| e.into_inner());
                fc.retain(|fid, qid| qid != queue_id || remaining_set.contains(fid));
            }

            let qid = queue_id.as_str().to_owned();
            self.catalog
                .with_conn(move |conn| {
                    if remaining_ids.is_empty() {
                        // All flows for this queue have been deleted.
                        conn.execute("DELETE FROM flow_map WHERE queue_id = ?1", params![qid])?;
                    } else {
                        // Build a parameterised IN clause.
                        let placeholders: Vec<String> = (0..remaining_ids.len())
                            .map(|i| format!("?{}", i + 2))
                            .collect();
                        let sql = format!(
                            "DELETE FROM flow_map WHERE queue_id = ?1 AND flow_id NOT IN ({})",
                            placeholders.join(", ")
                        );
                        let mut sql_params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(qid)];
                        for id in &remaining_ids {
                            sql_params.push(Box::new(id.clone()));
                        }
                        let param_refs: Vec<&dyn rusqlite::ToSql> =
                            sql_params.iter().map(|p| p.as_ref()).collect();
                        conn.execute(&sql, param_refs.as_slice())?;
                    }
                    Ok(())
                })
                .await?;
        }

        Ok(deleted)
    }

    // ---- Schedule methods ----

    async fn create_schedule(&self, schedule: &Schedule) -> Result<(), StorageError> {
        let shard = self.get_or_open_shard(&schedule.queue_id).await?;
        shard.create_schedule(schedule).await?;

        // Register schedule → queue mapping in the catalog.
        let sid = schedule.id.as_str().to_owned();
        let qid = schedule.queue_id.as_str().to_owned();
        self.catalog
            .with_conn(move |conn| {
                conn.execute(
                    "INSERT OR IGNORE INTO schedule_map (schedule_id, queue_id) VALUES (?1, ?2)",
                    params![sid, qid],
                )?;
                Ok(())
            })
            .await?;

        // Populate cache.
        self.schedule_cache
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(schedule.id.clone(), schedule.queue_id.clone());

        Ok(())
    }

    async fn get_schedule(&self, id: &ScheduleId) -> Result<Option<Schedule>, StorageError> {
        let queue_id = match self.resolve_schedule_queue(id).await {
            Ok(qid) => qid,
            Err(StorageError::ScheduleNotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.get_schedule(id).await
    }

    async fn list_schedules(&self, queue_id: &QueueId) -> Result<Vec<Schedule>, StorageError> {
        let shard = self.get_or_open_shard(queue_id).await?;
        shard.list_schedules(queue_id).await
    }

    async fn update_schedule(&self, schedule: &Schedule) -> Result<(), StorageError> {
        let shard = self.get_or_open_shard(&schedule.queue_id).await?;
        shard.update_schedule(schedule).await
    }

    async fn delete_schedule(&self, id: &ScheduleId) -> Result<(), StorageError> {
        let queue_id = self.resolve_schedule_queue(id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard.delete_schedule(id).await?;

        // Remove from catalog routing table and cache.
        let sid = id.as_str().to_owned();
        self.catalog
            .with_conn(move |conn| {
                conn.execute(
                    "DELETE FROM schedule_map WHERE schedule_id = ?1",
                    params![sid],
                )?;
                Ok(())
            })
            .await?;
        self.schedule_cache
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id);

        Ok(())
    }

    async fn fetch_due_schedules(&self) -> Result<Vec<Schedule>, StorageError> {
        let mut all = Vec::new();
        for shard in self.all_shards() {
            let mut schedules = shard.fetch_due_schedules().await?;
            all.append(&mut schedules);
        }
        Ok(all)
    }

    async fn mark_schedule_triggered(
        &self,
        id: &ScheduleId,
        triggered_at: chrono::DateTime<chrono::Utc>,
        next_run_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), StorageError> {
        let queue_id = self.resolve_schedule_queue(id).await?;
        let shard = self.get_or_open_shard(&queue_id).await?;
        shard
            .mark_schedule_triggered(id, triggered_at, next_run_at)
            .await
    }

    async fn checkpoint(&self) -> Result<(), StorageError> {
        for shard in self.all_shards() {
            shard.checkpoint().await?;
        }
        Ok(())
    }
}

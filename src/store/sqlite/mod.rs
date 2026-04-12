pub(crate) mod rows;
mod schema;

use super::{Storage, StorageError};
use crate::types::*;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rows::{parse_task_state, row_to_flow, row_to_queue, row_to_schedule, row_to_task};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Mutex;

/// SQLite storage backend with WAL mode for durable task persistence.
pub struct SqliteStorage {
    conn: Mutex<Connection>,
}

impl SqliteStorage {
    /// Open (or create) a SQLite database at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let conn = Connection::open(path).map_err(|e| StorageError::Internal(e.to_string()))?;
        schema::init_schema(&conn)?;
        schema::migrate_flows_add_flow_def(&conn)?;
        let storage = Self {
            conn: Mutex::new(conn),
        };
        Ok(storage)
    }

    /// Create an in-memory SQLite database (useful for testing).
    pub fn in_memory() -> Result<Self, StorageError> {
        let conn =
            Connection::open_in_memory().map_err(|e| StorageError::Internal(e.to_string()))?;
        schema::init_schema(&conn)?;
        let storage = Self {
            conn: Mutex::new(conn),
        };
        Ok(storage)
    }

    /// Open a catalog-mode database.
    pub fn open_catalog(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let conn = Connection::open(path).map_err(|e| StorageError::Internal(e.to_string()))?;
        schema::init_catalog_schema(&conn)?;
        schema::migrate_flow_map_add_parent(&conn)?;
        let storage = Self {
            conn: Mutex::new(conn),
        };
        Ok(storage)
    }

    /// Open a per-queue database.
    pub fn open_queue(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let conn = Connection::open(path).map_err(|e| StorageError::Internal(e.to_string()))?;
        schema::init_queue_schema(&conn)?;
        schema::migrate_flows_add_flow_def(&conn)?;
        schema::migrate_add_flow_id_indexes(&conn)?;
        let storage = Self {
            conn: Mutex::new(conn),
        };
        Ok(storage)
    }

    /// Execute a raw SQL statement on the underlying connection (for catalog operations).
    pub(crate) fn execute(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> Result<usize, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(sql, params)
            .map_err(|e| StorageError::Internal(e.to_string()))
    }

    /// Query a single value from the catalog (for routing lookups).
    pub(crate) fn query_row_optional<T: 'static>(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
        f: impl FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<Option<T>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.query_row(sql, params, f)
            .optional()
            .map_err(|e| StorageError::Internal(e.to_string()))
    }

    /// Query multiple rows, returning a Vec of mapped results.
    pub(crate) fn query_all<T: 'static>(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
        f: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<Vec<T>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn
            .prepare_cached(sql)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let rows = stmt
            .query_map(params, f)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Internal(e.to_string()))
    }
}

#[async_trait]
impl Storage for SqliteStorage {
    async fn create_queue(&self, queue: &Queue) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Check for existing queue
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM queues WHERE id = ?1)",
                params![queue.id.as_str()],
                |row| row.get(0),
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if exists {
            return Err(StorageError::QueueAlreadyExists(queue.id.to_string()));
        }

        let config_json = serde_json::to_string(&queue.config)
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        conn.execute(
            "INSERT INTO queues (id, config, created_at, updated_at) VALUES (?1, ?2, ?3, ?4)",
            params![
                queue.id.as_str(),
                config_json,
                queue.created_at.to_rfc3339(),
                queue.updated_at.to_rfc3339(),
            ],
        )
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(())
    }

    async fn get_queue(&self, id: &QueueId) -> Result<Option<Queue>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn
            .prepare_cached("SELECT id, config, created_at, updated_at FROM queues WHERE id = ?1")
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let result = stmt
            .query_row(params![id.as_str()], row_to_queue)
            .optional()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(result)
    }

    async fn list_queues(&self) -> Result<Vec<Queue>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn
            .prepare_cached("SELECT id, config, created_at, updated_at FROM queues")
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let queues = stmt
            .query_map([], row_to_queue)
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(queues)
    }

    async fn delete_queue(&self, id: &QueueId) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute("DELETE FROM queues WHERE id = ?1", params![id.as_str()])
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn create_flow(
        &self,
        flow: &Flow,
        tasks: &[Task],
        deps: &HashMap<TaskId, Vec<TaskId>>,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Insert flow
        let webhooks_json = flow
            .webhooks
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let flow_def_json = flow
            .flow_def
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        tx.execute(
            "INSERT INTO flows (id, queue_id, state, task_count, tasks_succeeded, tasks_failed, webhooks, trigger_depth, flow_def, fail_fast, parent_flow_id, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                flow.id.as_str(),
                flow.queue_id.as_str(),
                flow.state.to_string(),
                flow.task_count as i64,
                flow.tasks_succeeded as i64,
                flow.tasks_failed as i64,
                webhooks_json,
                flow.trigger_depth as i64,
                flow_def_json,
                flow.fail_fast as i64,
                flow.parent_flow_id.as_ref().map(|id| id.as_str().to_owned()),
                flow.created_at.to_rfc3339(),
                flow.updated_at.to_rfc3339(),
            ],
        )
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Insert tasks
        let mut task_stmt = tx
            .prepare_cached(
                "INSERT INTO tasks (id, flow_id, queue_id, state, executor_type, executor_config, input, output, error, retries_remaining, backoff, timeout_secs, condition, retry_at, started_at, completed_at, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        for task in tasks {
            let executor_config_json = serde_json::to_string(&task.executor_config)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let input_json = task
                .input
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let output_json = task
                .output
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let backoff_json = serde_json::to_string(&task.backoff)
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            task_stmt
                .execute(params![
                    task.id.as_str(),
                    task.flow_id.as_str(),
                    task.queue_id.as_str(),
                    task.state.to_string(),
                    task.executor_type,
                    executor_config_json,
                    input_json,
                    output_json,
                    task.error,
                    task.retries_remaining as i64,
                    backoff_json,
                    task.timeout_secs as i64,
                    task.condition,
                    task.retry_at.map(|dt| dt.to_rfc3339()),
                    task.started_at.map(|dt| dt.to_rfc3339()),
                    task.completed_at.map(|dt| dt.to_rfc3339()),
                    task.created_at.to_rfc3339(),
                ])
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }
        drop(task_stmt);

        // Insert deps
        let mut dep_stmt = tx
            .prepare_cached(
                "INSERT INTO task_deps (flow_id, task_id, depends_on_task_id) VALUES (?1, ?2, ?3)",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        for (task_id, dep_ids) in deps {
            for dep_id in dep_ids {
                dep_stmt
                    .execute(params![flow.id.as_str(), task_id.as_str(), dep_id.as_str()])
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }
        drop(dep_stmt);

        tx.commit()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(())
    }

    async fn get_flow(&self, id: &FlowId) -> Result<Option<Flow>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, queue_id, state, task_count, tasks_succeeded, tasks_failed, webhooks, trigger_depth, flow_def, fail_fast, parent_flow_id, created_at, updated_at
                 FROM flows WHERE id = ?1",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let result = stmt
            .query_row(params![id.as_str()], row_to_flow)
            .optional()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(result)
    }

    async fn list_flows(
        &self,
        queue_id: &QueueId,
        state: Option<FlowState>,
    ) -> Result<Vec<Flow>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

        let (sql, param_values): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = match state {
            Some(s) => (
                "SELECT id, queue_id, state, task_count, tasks_succeeded, tasks_failed, webhooks, trigger_depth, flow_def, fail_fast, parent_flow_id, created_at, updated_at FROM flows WHERE queue_id = ?1 AND state = ?2".to_string(),
                vec![Box::new(queue_id.as_str().to_owned()), Box::new(s.to_string())],
            ),
            None => (
                "SELECT id, queue_id, state, task_count, tasks_succeeded, tasks_failed, webhooks, trigger_depth, flow_def, fail_fast, parent_flow_id, created_at, updated_at FROM flows WHERE queue_id = ?1".to_string(),
                vec![Box::new(queue_id.as_str().to_owned())],
            ),
        };

        let mut stmt = conn
            .prepare_cached(&sql)
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            param_values.iter().map(|p| p.as_ref()).collect();

        let flows = stmt
            .query_map(param_refs.as_slice(), row_to_flow)
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(flows)
    }

    async fn update_flow_state(&self, id: &FlowId, state: FlowState) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = Utc::now();
        let updated = conn
            .execute(
                "UPDATE flows SET state = ?1, updated_at = ?2 WHERE id = ?3",
                params![state.to_string(), now.to_rfc3339(), id.as_str()],
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if updated == 0 {
            return Err(StorageError::FlowNotFound(id.to_string()));
        }
        Ok(())
    }

    async fn increment_flow_counter(
        &self,
        id: &FlowId,
        succeeded: bool,
    ) -> Result<Flow, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = Utc::now();

        let column = if succeeded {
            "tasks_succeeded"
        } else {
            "tasks_failed"
        };

        // UPDATE + RETURNING to get updated counters without a separate SELECT.
        let sql = format!(
            "UPDATE flows SET {column} = {column} + 1, updated_at = ?1 WHERE id = ?2 \
             RETURNING id, queue_id, state, task_count, tasks_succeeded, tasks_failed, \
             webhooks, trigger_depth, flow_def, fail_fast, parent_flow_id, created_at, updated_at"
        );

        let flow = conn
            .query_row(&sql, params![now.to_rfc3339(), id.as_str()], row_to_flow)
            .optional()
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .ok_or_else(|| StorageError::FlowNotFound(id.to_string()))?;

        Ok(flow)
    }

    async fn get_task(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<Option<Task>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, flow_id, queue_id, state, executor_type, executor_config, input, output, error, retries_remaining, backoff, timeout_secs, condition, retry_at, started_at, completed_at, created_at
                 FROM tasks WHERE id = ?1 AND flow_id = ?2",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let result = stmt
            .query_row(params![task_id.as_str(), flow_id.as_str()], row_to_task)
            .optional()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(result)
    }

    async fn get_flow_tasks(&self, flow_id: &FlowId) -> Result<Vec<Task>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, flow_id, queue_id, state, executor_type, executor_config, input, output, error, retries_remaining, backoff, timeout_secs, condition, retry_at, started_at, completed_at, created_at
                 FROM tasks WHERE flow_id = ?1",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let tasks = stmt
            .query_map(params![flow_id.as_str()], row_to_task)
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(tasks)
    }

    async fn get_flow_with_tasks(
        &self,
        flow_id: &FlowId,
    ) -> Result<Option<(Flow, Vec<Task>)>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

        let mut flow_stmt = conn
            .prepare_cached(
                "SELECT id, queue_id, state, task_count, tasks_succeeded, tasks_failed, webhooks, trigger_depth, flow_def, fail_fast, parent_flow_id, created_at, updated_at
                 FROM flows WHERE id = ?1",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let flow = flow_stmt
            .query_row(params![flow_id.as_str()], row_to_flow)
            .optional()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let flow = match flow {
            Some(f) => f,
            None => return Ok(None),
        };

        let mut task_stmt = conn
            .prepare_cached(
                "SELECT id, flow_id, queue_id, state, executor_type, executor_config, input, output, error, retries_remaining, backoff, timeout_secs, condition, retry_at, started_at, completed_at, created_at
                 FROM tasks WHERE flow_id = ?1",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let tasks = task_stmt
            .query_map(params![flow_id.as_str()], row_to_task)
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(Some((flow, tasks)))
    }

    async fn fetch_ready_tasks(
        &self,
        queue_id: &QueueId,
        limit: usize,
    ) -> Result<Vec<Task>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, flow_id, queue_id, state, executor_type, executor_config, input, output, error, retries_remaining, backoff, timeout_secs, condition, retry_at, started_at, completed_at, created_at
                 FROM tasks WHERE queue_id = ?1 AND state = 'ready'
                 ORDER BY created_at ASC LIMIT ?2",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let tasks = stmt
            .query_map(params![queue_id.as_str(), limit as i64], row_to_task)
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(tasks)
    }

    async fn fetch_delayed_tasks_due(&self) -> Result<Vec<Task>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = Utc::now().to_rfc3339();
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, flow_id, queue_id, state, executor_type, executor_config, input, output, error, retries_remaining, backoff, timeout_secs, condition, retry_at, started_at, completed_at, created_at
                 FROM tasks WHERE state = 'delayed' AND retry_at IS NOT NULL AND retry_at <= ?1",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let tasks = stmt
            .query_map(params![now], row_to_task)
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(tasks)
    }

    async fn fetch_timed_out_tasks(&self) -> Result<Vec<Task>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = Utc::now();
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, flow_id, queue_id, state, executor_type, executor_config, input, output, error, retries_remaining, backoff, timeout_secs, condition, retry_at, started_at, completed_at, created_at
                 FROM tasks WHERE state = 'running' AND started_at IS NOT NULL",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let all_running: Vec<Task> = stmt
            .query_map([], row_to_task)
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Filter in Rust for precise datetime arithmetic
        let timed_out = all_running
            .into_iter()
            .filter(|t| {
                t.started_at
                    .is_some_and(|s| (now - s).num_seconds() as u64 > t.timeout_secs)
            })
            .collect();

        Ok(timed_out)
    }

    async fn update_task_state(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        new_state: TaskState,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Fetch current state for validation
        let current_state: String = conn
            .query_row(
                "SELECT state FROM tasks WHERE id = ?1 AND flow_id = ?2",
                params![task_id.as_str(), flow_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .ok_or_else(|| StorageError::TaskNotFound(task_id.to_string(), flow_id.to_string()))?;

        let current = parse_task_state(&current_state)?;
        if !current.can_transition_to(new_state) {
            return Err(StorageError::InvalidStateTransition(
                current.to_string(),
                new_state.to_string(),
            ));
        }

        conn.execute(
            "UPDATE tasks SET state = ?1 WHERE id = ?2 AND flow_id = ?3",
            params![new_state.to_string(), task_id.as_str(), flow_id.as_str()],
        )
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(())
    }

    async fn mark_task_running(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = Utc::now();

        // Conditional UPDATE: only transitions from 'ready' (the sole valid source state).
        let rows = conn
            .execute(
                "UPDATE tasks SET state = 'running', started_at = ?1 WHERE id = ?2 AND flow_id = ?3 AND state = 'ready'",
                params![now.to_rfc3339(), task_id.as_str(), flow_id.as_str()],
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if rows == 0 {
            // Distinguish "not found" from "wrong state" for accurate error reporting.
            let current_state: Option<String> = conn
                .query_row(
                    "SELECT state FROM tasks WHERE id = ?1 AND flow_id = ?2",
                    params![task_id.as_str(), flow_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            return match current_state {
                None => Err(StorageError::TaskNotFound(
                    task_id.to_string(),
                    flow_id.to_string(),
                )),
                Some(state) => Err(StorageError::InvalidStateTransition(
                    state,
                    "running".to_string(),
                )),
            };
        }

        Ok(())
    }

    async fn mark_tasks_running_batch(
        &self,
        tasks: &[(&TaskId, &FlowId)],
    ) -> Result<Vec<(TaskId, FlowId)>, StorageError> {
        if tasks.is_empty() {
            return Ok(vec![]);
        }
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let now = Utc::now().to_rfc3339();
        let mut stmt = tx
            .prepare_cached(
                "UPDATE tasks SET state = 'running', started_at = ?1 WHERE id = ?2 AND flow_id = ?3 AND state = 'ready'",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut succeeded = Vec::with_capacity(tasks.len());
        for &(task_id, flow_id) in tasks {
            let rows = stmt
                .execute(params![&now, task_id.as_str(), flow_id.as_str()])
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            if rows > 0 {
                succeeded.push((task_id.clone(), flow_id.clone()));
            }
        }
        drop(stmt);
        tx.commit()
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(succeeded)
    }

    async fn set_task_output(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: serde_json::Value,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let output_json =
            serde_json::to_string(&output).map_err(|e| StorageError::Internal(e.to_string()))?;

        let rows = conn
            .execute(
                "UPDATE tasks SET output = ?1 WHERE id = ?2 AND flow_id = ?3",
                params![output_json, task_id.as_str(), flow_id.as_str()],
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if rows == 0 {
            return Err(StorageError::TaskNotFound(
                task_id.to_string(),
                flow_id.to_string(),
            ));
        }
        Ok(())
    }

    async fn mark_task_succeeded(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: Option<serde_json::Value>,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = Utc::now();

        let output_json = output
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Conditional UPDATE: valid source states are 'running' and 'ready' (condition skip).
        let rows = conn
            .execute(
                "UPDATE tasks SET state = 'succeeded', output = ?1, completed_at = ?2 WHERE id = ?3 AND flow_id = ?4 AND state IN ('running', 'ready')",
                params![output_json, now.to_rfc3339(), task_id.as_str(), flow_id.as_str()],
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if rows == 0 {
            let current_state: Option<String> = conn
                .query_row(
                    "SELECT state FROM tasks WHERE id = ?1 AND flow_id = ?2",
                    params![task_id.as_str(), flow_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            return match current_state {
                None => Err(StorageError::TaskNotFound(
                    task_id.to_string(),
                    flow_id.to_string(),
                )),
                Some(state) => Err(StorageError::InvalidStateTransition(
                    state,
                    "succeeded".to_string(),
                )),
            };
        }

        Ok(())
    }

    async fn mark_task_failed(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        error: &str,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = Utc::now();

        // Conditional UPDATE: valid source states are 'running' and 'ready' (condition error).
        let rows = conn
            .execute(
                "UPDATE tasks SET state = 'failed', error = ?1, completed_at = ?2 WHERE id = ?3 AND flow_id = ?4 AND state IN ('running', 'ready')",
                params![error, now.to_rfc3339(), task_id.as_str(), flow_id.as_str()],
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if rows == 0 {
            let current_state: Option<String> = conn
                .query_row(
                    "SELECT state FROM tasks WHERE id = ?1 AND flow_id = ?2",
                    params![task_id.as_str(), flow_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            return match current_state {
                None => Err(StorageError::TaskNotFound(
                    task_id.to_string(),
                    flow_id.to_string(),
                )),
                Some(state) => Err(StorageError::InvalidStateTransition(
                    state,
                    "failed".to_string(),
                )),
            };
        }

        Ok(())
    }

    async fn mark_task_delayed(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        retry_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Conditional UPDATE: only valid from 'running'.
        let rows = conn
            .execute(
                "UPDATE tasks SET state = 'delayed', retry_at = ?1, retries_remaining = MAX(retries_remaining - 1, 0), started_at = NULL WHERE id = ?2 AND flow_id = ?3 AND state = 'running'",
                params![retry_at.to_rfc3339(), task_id.as_str(), flow_id.as_str()],
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if rows == 0 {
            let current_state: Option<String> = conn
                .query_row(
                    "SELECT state FROM tasks WHERE id = ?1 AND flow_id = ?2",
                    params![task_id.as_str(), flow_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            return match current_state {
                None => Err(StorageError::TaskNotFound(
                    task_id.to_string(),
                    flow_id.to_string(),
                )),
                Some(state) => Err(StorageError::InvalidStateTransition(
                    state,
                    "delayed".to_string(),
                )),
            };
        }

        Ok(())
    }

    async fn get_flow_dependencies(
        &self,
        flow_id: &FlowId,
    ) -> Result<HashMap<TaskId, Vec<TaskId>>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn
            .prepare_cached(
                "SELECT task_id, depends_on_task_id FROM task_deps WHERE flow_id = ?1 ORDER BY task_id",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let rows = stmt
            .query_map(params![flow_id.as_str()], |row| {
                let task_id: String = row.get(0)?;
                let dep_id: String = row.get(1)?;
                Ok((TaskId::from(task_id), TaskId::from(dep_id)))
            })
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut deps: HashMap<TaskId, Vec<TaskId>> = HashMap::new();
        for (task_id, dep_id) in rows {
            deps.entry(task_id).or_default().push(dep_id);
        }
        Ok(deps)
    }

    async fn get_task_dependencies(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<Vec<TaskId>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn
            .prepare_cached(
                "SELECT depends_on_task_id FROM task_deps WHERE flow_id = ?1 AND task_id = ?2",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let deps = stmt
            .query_map(params![flow_id.as_str(), task_id.as_str()], |row| {
                let id: String = row.get(0)?;
                Ok(TaskId::from(id))
            })
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(deps)
    }

    async fn get_task_dependents(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
    ) -> Result<Vec<TaskId>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn
            .prepare_cached(
                "SELECT task_id FROM task_deps WHERE flow_id = ?1 AND depends_on_task_id = ?2",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let dependents = stmt
            .query_map(params![flow_id.as_str(), task_id.as_str()], |row| {
                let id: String = row.get(0)?;
                Ok(TaskId::from(id))
            })
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(dependents)
    }

    async fn resolve_ready_tasks(&self, flow_id: &FlowId) -> Result<Vec<TaskId>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

        // Find pending tasks whose dependencies have all succeeded.
        // A pending task is ready when it either has no deps, or all deps are succeeded.
        // Uses LEFT JOIN so that a dependency referencing a not-yet-existing task
        // (e.g. a deferred spawn dependency) is treated as unsatisfied.
        let mut stmt = conn
            .prepare_cached(
                "SELECT t.id FROM tasks t
                 WHERE t.flow_id = ?1 AND t.state = 'pending'
                 AND NOT EXISTS (
                     SELECT 1 FROM task_deps d
                     LEFT JOIN tasks dep ON dep.id = d.depends_on_task_id AND dep.flow_id = d.flow_id
                     WHERE d.flow_id = ?1 AND d.task_id = t.id
                     AND (dep.id IS NULL OR dep.state != 'succeeded')
                 )",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let ready_ids: Vec<TaskId> = stmt
            .query_map(params![flow_id.as_str()], |row| {
                let id: String = row.get(0)?;
                Ok(TaskId::from(id))
            })
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Transition them to ready
        let mut update_stmt = conn
            .prepare_cached("UPDATE tasks SET state = 'ready' WHERE id = ?1 AND flow_id = ?2")
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        for tid in &ready_ids {
            update_stmt
                .execute(params![tid.as_str(), flow_id.as_str()])
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        Ok(ready_ids)
    }

    async fn complete_task_success(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: Option<serde_json::Value>,
    ) -> Result<(Flow, Vec<TaskId>), StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let now = Utc::now();

        // -- 1. Mark task succeeded (conditional UPDATE) --
        let output_json = output
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let rows = tx
            .execute(
                "UPDATE tasks SET state = 'succeeded', output = ?1, completed_at = ?2 WHERE id = ?3 AND flow_id = ?4 AND state IN ('running', 'ready')",
                params![output_json, now.to_rfc3339(), task_id.as_str(), flow_id.as_str()],
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if rows == 0 {
            // tx rolls back on drop — no state modified
            let current_state: Option<String> = tx
                .query_row(
                    "SELECT state FROM tasks WHERE id = ?1 AND flow_id = ?2",
                    params![task_id.as_str(), flow_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            return match current_state {
                None => Err(StorageError::TaskNotFound(
                    task_id.to_string(),
                    flow_id.to_string(),
                )),
                Some(state) => Err(StorageError::InvalidStateTransition(
                    state,
                    "succeeded".to_string(),
                )),
            };
        }

        // -- 2. Increment flow counter (UPDATE...RETURNING) --
        let flow = tx
            .query_row(
                "UPDATE flows SET tasks_succeeded = tasks_succeeded + 1, updated_at = ?1 WHERE id = ?2 \
                 RETURNING id, queue_id, state, task_count, tasks_succeeded, tasks_failed, \
                 webhooks, trigger_depth, flow_def, fail_fast, parent_flow_id, created_at, updated_at",
                params![now.to_rfc3339(), flow_id.as_str()],
                row_to_flow,
            )
            .optional()
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .ok_or_else(|| StorageError::FlowNotFound(flow_id.to_string()))?;

        // -- 3. Resolve ready dependents (skip if flow is complete) --
        let newly_ready = if flow.tasks_succeeded < flow.task_count {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT t.id FROM tasks t
                     WHERE t.flow_id = ?1 AND t.state = 'pending'
                     AND NOT EXISTS (
                         SELECT 1 FROM task_deps d
                         LEFT JOIN tasks dep ON dep.id = d.depends_on_task_id AND dep.flow_id = d.flow_id
                         WHERE d.flow_id = ?1 AND d.task_id = t.id
                         AND (dep.id IS NULL OR dep.state != 'succeeded')
                     )",
                )
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let ready_ids: Vec<TaskId> = stmt
                .query_map(params![flow_id.as_str()], |row| {
                    let id: String = row.get(0)?;
                    Ok(TaskId::from(id))
                })
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            drop(stmt);
            let mut update_stmt = tx
                .prepare_cached("UPDATE tasks SET state = 'ready' WHERE id = ?1 AND flow_id = ?2")
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            for tid in &ready_ids {
                update_stmt
                    .execute(params![tid.as_str(), flow_id.as_str()])
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }

            ready_ids
        } else {
            vec![]
        };

        tx.commit()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok((flow, newly_ready))
    }

    async fn complete_task_with_ready(
        &self,
        task_id: &TaskId,
        flow_id: &FlowId,
        output: Option<serde_json::Value>,
        newly_ready: &[TaskId],
    ) -> Result<Flow, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let now = Utc::now();

        // 1. Mark task succeeded (conditional UPDATE)
        let output_json = output
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let rows = tx
            .execute(
                "UPDATE tasks SET state = 'succeeded', output = ?1, completed_at = ?2 WHERE id = ?3 AND flow_id = ?4 AND state IN ('running', 'ready')",
                params![output_json, now.to_rfc3339(), task_id.as_str(), flow_id.as_str()],
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if rows == 0 {
            // tx rolls back on drop — no state modified
            let current_state: Option<String> = tx
                .query_row(
                    "SELECT state FROM tasks WHERE id = ?1 AND flow_id = ?2",
                    params![task_id.as_str(), flow_id.as_str()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            return match current_state {
                None => Err(StorageError::TaskNotFound(
                    task_id.to_string(),
                    flow_id.to_string(),
                )),
                Some(state) => Err(StorageError::InvalidStateTransition(
                    state,
                    "succeeded".to_string(),
                )),
            };
        }

        // 2. Increment flow counter (UPDATE...RETURNING)
        let flow = tx
            .query_row(
                "UPDATE flows SET tasks_succeeded = tasks_succeeded + 1, updated_at = ?1 WHERE id = ?2 \
                 RETURNING id, queue_id, state, task_count, tasks_succeeded, tasks_failed, \
                 webhooks, trigger_depth, flow_def, fail_fast, parent_flow_id, created_at, updated_at",
                params![now.to_rfc3339(), flow_id.as_str()],
                row_to_flow,
            )
            .optional()
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .ok_or_else(|| StorageError::FlowNotFound(flow_id.to_string()))?;

        // 3. Promote pre-resolved ready tasks (NO SELECT query — caller resolved in-memory)
        if !newly_ready.is_empty() {
            let mut stmt = tx
                .prepare_cached("UPDATE tasks SET state = 'ready' WHERE id = ?1 AND flow_id = ?2 AND state = 'pending'")
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            for tid in newly_ready {
                stmt.execute(params![tid.as_str(), flow_id.as_str()])
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }

        tx.commit()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(flow)
    }

    async fn complete_tasks_with_ready_batch(
        &self,
        completions: &[(TaskId, FlowId, Option<serde_json::Value>, Vec<TaskId>)],
    ) -> Result<Vec<Option<Flow>>, StorageError> {
        if completions.is_empty() {
            return Ok(vec![]);
        }
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let now = Utc::now();
        let now_str = now.to_rfc3339();

        // Phase 1: Bulk-insert completion data into a temp table
        tx.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS _batch_complete(\
                 idx INTEGER, task_id TEXT, flow_id TEXT, output TEXT\
             )",
        )
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        tx.execute("DELETE FROM _batch_complete", [])
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        {
            let mut insert_stmt = tx
                .prepare_cached(
                    "INSERT INTO _batch_complete(idx, task_id, flow_id, output) VALUES(?1, ?2, ?3, ?4)",
                )
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            for (i, (task_id, flow_id, output, _)) in completions.iter().enumerate() {
                let output_json = output
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                insert_stmt
                    .execute(params![
                        i as i64,
                        task_id.as_str(),
                        flow_id.as_str(),
                        output_json
                    ])
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }

        // Phase 2: Bulk mark tasks succeeded (single UPDATE from temp table)
        tx.execute(
            "UPDATE tasks SET state = 'succeeded', \
                 output = (SELECT output FROM _batch_complete bc WHERE bc.task_id = tasks.id AND bc.flow_id = tasks.flow_id), \
                 completed_at = ?1 \
             WHERE (id, flow_id) IN (SELECT task_id, flow_id FROM _batch_complete) \
             AND state IN ('running', 'ready')",
            params![&now_str],
        )
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Phase 3: Batch increment flow counters (one UPDATE per unique flow_id)
        let mut flow_counts: HashMap<String, usize> = HashMap::new();
        for (_, flow_id, _, _) in completions {
            *flow_counts.entry(flow_id.as_str().to_owned()).or_default() += 1;
        }
        for (fid, count) in &flow_counts {
            tx.execute(
                "UPDATE flows SET tasks_succeeded = tasks_succeeded + ?1, updated_at = ?2 WHERE id = ?3",
                params![*count as i64, &now_str, fid],
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Phase 4: Promote newly-ready tasks
        // For tasks with pre-resolved deps (in-memory graph), promote individually.
        // For tasks without, use SQL dep resolution per flow.
        let mut resolved_flows: HashSet<String> = HashSet::new();
        for (_, flow_id, _, newly_ready) in completions {
            if !newly_ready.is_empty() {
                let mut stmt = tx
                    .prepare_cached("UPDATE tasks SET state = 'ready' WHERE id = ?1 AND flow_id = ?2 AND state = 'pending'")
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                for tid in newly_ready {
                    stmt.execute(params![tid.as_str(), flow_id.as_str()])
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                }
            } else if resolved_flows.insert(flow_id.as_str().to_owned()) {
                // SQL dep resolution — once per flow, not per task
                tx.execute(
                    "UPDATE tasks SET state = 'ready' WHERE flow_id = ?1 AND state = 'pending' \
                     AND NOT EXISTS ( \
                         SELECT 1 FROM task_deps d \
                         LEFT JOIN tasks dep ON dep.id = d.depends_on_task_id AND dep.flow_id = d.flow_id \
                         WHERE d.flow_id = ?1 AND d.task_id = tasks.id \
                         AND (dep.id IS NULL OR dep.state != 'succeeded') \
                     )",
                    params![flow_id.as_str()],
                )
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }

        tx.commit()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Phase 5: Fetch flows that were modified
        let mut flow_cache: HashMap<String, Flow> = HashMap::new();
        for fid in flow_counts.keys() {
            if let Some(flow) = conn
                .query_row(
                    "SELECT id, queue_id, state, task_count, tasks_succeeded, tasks_failed, \
                     webhooks, trigger_depth, flow_def, fail_fast, parent_flow_id, created_at, updated_at \
                     FROM flows WHERE id = ?1",
                    params![fid],
                    row_to_flow,
                )
                .optional()
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                flow_cache.insert(fid.clone(), flow);
            }
        }

        // Build results — all entries are considered succeeded (bulk UPDATE doesn't track per-row)
        let results: Vec<Option<Flow>> = completions
            .iter()
            .map(|(_, flow_id, _, _)| flow_cache.get(flow_id.as_str()).cloned())
            .collect();

        Ok(results)
    }

    async fn inject_tasks(
        &self,
        flow_id: &FlowId,
        tasks: &[Task],
        deps: &HashMap<TaskId, Vec<TaskId>>,
    ) -> Result<Flow, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Insert tasks
        let mut task_stmt = tx
            .prepare_cached(
                "INSERT INTO tasks (id, flow_id, queue_id, state, executor_type, executor_config, input, output, error, retries_remaining, backoff, timeout_secs, condition, retry_at, started_at, completed_at, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        for task in tasks {
            let executor_config_json = serde_json::to_string(&task.executor_config)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let input_json = task
                .input
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let output_json = task
                .output
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let backoff_json = serde_json::to_string(&task.backoff)
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            task_stmt
                .execute(params![
                    task.id.as_str(),
                    task.flow_id.as_str(),
                    task.queue_id.as_str(),
                    task.state.to_string(),
                    task.executor_type,
                    executor_config_json,
                    input_json,
                    output_json,
                    task.error,
                    task.retries_remaining as i64,
                    backoff_json,
                    task.timeout_secs as i64,
                    task.condition,
                    task.retry_at.map(|dt| dt.to_rfc3339()),
                    task.started_at.map(|dt| dt.to_rfc3339()),
                    task.completed_at.map(|dt| dt.to_rfc3339()),
                    task.created_at.to_rfc3339(),
                ])
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }
        drop(task_stmt);

        // Insert deps
        let mut dep_stmt = tx
            .prepare_cached(
                "INSERT INTO task_deps (flow_id, task_id, depends_on_task_id) VALUES (?1, ?2, ?3)",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        for (task_id, dep_ids) in deps {
            for dep_id in dep_ids {
                dep_stmt
                    .execute(params![flow_id.as_str(), task_id.as_str(), dep_id.as_str()])
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }
        drop(dep_stmt);

        // Increment task_count
        let now = Utc::now();
        let n = tasks.len() as i64;
        tx.execute(
            "UPDATE flows SET task_count = task_count + ?1, updated_at = ?2 WHERE id = ?3",
            params![n, now.to_rfc3339(), flow_id.as_str()],
        )
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        tx.commit()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Fetch and return updated flow
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, queue_id, state, task_count, tasks_succeeded, tasks_failed, webhooks, trigger_depth, flow_def, fail_fast, parent_flow_id, created_at, updated_at FROM flows WHERE id = ?1",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        stmt.query_row(params![flow_id.as_str()], row_to_flow)
            .map_err(|e| StorageError::Internal(e.to_string()))
    }

    async fn get_child_flow_ids(
        &self,
        parent_flow_id: &FlowId,
    ) -> Result<Vec<FlowId>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn
            .prepare_cached("SELECT id FROM flows WHERE parent_flow_id = ?1")
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let ids = stmt
            .query_map(params![parent_flow_id.as_str()], |row| {
                row.get::<_, String>(0)
            })
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(ids.into_iter().map(FlowId::from).collect())
    }

    async fn delete_terminal_flows_before(
        &self,
        queue_id: &QueueId,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<usize, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let cutoff_str = cutoff.to_rfc3339();

        // Select a batch of flows to delete (capped to prevent long Mutex holds)
        let flow_ids: Vec<String> = {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT id FROM flows
                     WHERE queue_id = ?1 AND state IN ('succeeded', 'failed', 'cancelled')
                     AND updated_at < ?2
                     LIMIT 1000",
                )
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            stmt.query_map(params![queue_id.as_str(), cutoff_str], |row| row.get(0))
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| StorageError::Internal(e.to_string()))?
        };

        if flow_ids.is_empty() {
            return Ok(0);
        }

        // Build IN clause for the batch
        let placeholders: String = flow_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");

        // Delete task_deps for matching flows
        let sql = format!("DELETE FROM task_deps WHERE flow_id IN ({placeholders})");
        conn.execute(&sql, rusqlite::params_from_iter(flow_ids.iter()))
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Delete tasks for matching flows
        let sql = format!("DELETE FROM tasks WHERE flow_id IN ({placeholders})");
        conn.execute(&sql, rusqlite::params_from_iter(flow_ids.iter()))
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Delete the flows themselves
        let sql = format!("DELETE FROM flows WHERE id IN ({placeholders})");
        let deleted = conn
            .execute(&sql, rusqlite::params_from_iter(flow_ids.iter()))
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(deleted)
    }

    // -- Schedules --

    async fn create_schedule(&self, schedule: &Schedule) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let flow_def_json = serde_json::to_string(&schedule.flow_def)
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        conn.execute(
            "INSERT INTO schedules (id, queue_id, name, cron, flow_def, enabled, last_triggered_at, next_run_at, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                schedule.id.as_str(),
                schedule.queue_id.as_str(),
                schedule.name,
                schedule.cron,
                flow_def_json,
                schedule.enabled,
                schedule.last_triggered_at.map(|dt| dt.to_rfc3339()),
                schedule.next_run_at.map(|dt| dt.to_rfc3339()),
                schedule.created_at.to_rfc3339(),
                schedule.updated_at.to_rfc3339(),
            ],
        )
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(())
    }

    async fn get_schedule(&self, id: &ScheduleId) -> Result<Option<Schedule>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, queue_id, name, cron, flow_def, enabled, last_triggered_at, next_run_at, created_at, updated_at
                 FROM schedules WHERE id = ?1",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let result = stmt
            .query_row(params![id.as_str()], row_to_schedule)
            .optional()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(result)
    }

    async fn list_schedules(&self, queue_id: &QueueId) -> Result<Vec<Schedule>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, queue_id, name, cron, flow_def, enabled, last_triggered_at, next_run_at, created_at, updated_at
                 FROM schedules WHERE queue_id = ?1 ORDER BY created_at ASC",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let schedules = stmt
            .query_map(params![queue_id.as_str()], row_to_schedule)
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(schedules)
    }

    async fn update_schedule(&self, schedule: &Schedule) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let flow_def_json = serde_json::to_string(&schedule.flow_def)
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let updated = conn
            .execute(
                "UPDATE schedules SET name = ?1, cron = ?2, flow_def = ?3, enabled = ?4, last_triggered_at = ?5, next_run_at = ?6, updated_at = ?7
                 WHERE id = ?8",
                params![
                    schedule.name,
                    schedule.cron,
                    flow_def_json,
                    schedule.enabled,
                    schedule.last_triggered_at.map(|dt| dt.to_rfc3339()),
                    schedule.next_run_at.map(|dt| dt.to_rfc3339()),
                    schedule.updated_at.to_rfc3339(),
                    schedule.id.as_str(),
                ],
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if updated == 0 {
            return Err(StorageError::ScheduleNotFound(schedule.id.to_string()));
        }

        Ok(())
    }

    async fn delete_schedule(&self, id: &ScheduleId) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute("DELETE FROM schedules WHERE id = ?1", params![id.as_str()])
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }

    async fn fetch_due_schedules(&self) -> Result<Vec<Schedule>, StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = Utc::now().to_rfc3339();
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, queue_id, name, cron, flow_def, enabled, last_triggered_at, next_run_at, created_at, updated_at
                 FROM schedules WHERE enabled = 1 AND next_run_at IS NOT NULL AND next_run_at <= ?1",
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let schedules = stmt
            .query_map(params![now], row_to_schedule)
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(schedules)
    }

    async fn mark_schedule_triggered(
        &self,
        id: &ScheduleId,
        triggered_at: DateTime<Utc>,
        next_run_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = Utc::now();
        let updated = conn
            .execute(
                "UPDATE schedules SET last_triggered_at = ?1, next_run_at = ?2, updated_at = ?3 WHERE id = ?4",
                params![
                    triggered_at.to_rfc3339(),
                    next_run_at.map(|dt| dt.to_rfc3339()),
                    now.to_rfc3339(),
                    id.as_str(),
                ],
            )
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if updated == 0 {
            return Err(StorageError::ScheduleNotFound(id.to_string()));
        }

        Ok(())
    }

    async fn checkpoint(&self) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(())
    }
}

use std::path::Path;

use chrono::Utc;
use rusqlite::Connection;

use super::state::MemState;
use crate::store::StorageError;

/// Write a full snapshot of in-memory state to a SQLite database.
///
/// Uses the same schema as `init_queue_schema()` so the snapshot is
/// directly queryable for debugging. An additional `_meta` table stores
/// the journal sequence number and statistics.
///
/// The write is atomic: we write to a `.tmp` file first, then rename.
pub(crate) fn write_snapshot(
    state: &MemState,
    snapshot_path: &Path,
    journal_seq: u64,
) -> Result<(), StorageError> {
    let tmp_path = snapshot_path.with_extension("db.tmp");

    // Remove stale temp file if it exists
    let _ = std::fs::remove_file(&tmp_path);

    let conn = Connection::open(&tmp_path)
        .map_err(|e| StorageError::Internal(format!("snapshot open: {e}")))?;

    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = FULL;
         PRAGMA busy_timeout = 5000;
         PRAGMA temp_store = MEMORY;",
    )
    .map_err(|e| StorageError::Internal(format!("snapshot pragmas: {e}")))?;

    create_snapshot_schema(&conn)?;

    let tx = conn
        .unchecked_transaction()
        .map_err(|e| StorageError::Internal(format!("snapshot begin: {e}")))?;

    write_queues(&tx, state)?;
    write_flows(&tx, state)?;
    write_tasks(&tx, state)?;
    write_task_deps(&tx, state)?;
    write_schedules(&tx, state)?;
    write_meta(&tx, state, journal_seq)?;

    tx.commit()
        .map_err(|e| StorageError::Internal(format!("snapshot commit: {e}")))?;

    // Close the connection before rename so WAL files are cleaned up
    drop(conn);

    std::fs::rename(&tmp_path, snapshot_path)
        .map_err(|e| StorageError::Internal(format!("snapshot rename: {e}")))?;

    metrics::counter!("tasked_journal_snapshots_total").increment(1);

    Ok(())
}

fn create_snapshot_schema(conn: &Connection) -> Result<(), StorageError> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS queues (
            id TEXT PRIMARY KEY,
            config TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS flows (
            id TEXT PRIMARY KEY,
            queue_id TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'running',
            task_count INTEGER NOT NULL,
            tasks_succeeded INTEGER NOT NULL DEFAULT 0,
            tasks_failed INTEGER NOT NULL DEFAULT 0,
            webhooks TEXT,
            trigger_depth INTEGER NOT NULL DEFAULT 0,
            flow_def TEXT,
            fail_fast INTEGER NOT NULL DEFAULT 0,
            parent_flow_id TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT NOT NULL,
            flow_id TEXT NOT NULL,
            queue_id TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending',
            executor_type TEXT NOT NULL,
            executor_config TEXT NOT NULL,
            input TEXT,
            output TEXT,
            error TEXT,
            retries_remaining INTEGER NOT NULL DEFAULT 3,
            backoff TEXT NOT NULL,
            timeout_secs INTEGER NOT NULL DEFAULT 300,
            condition TEXT,
            retry_at TEXT,
            started_at TEXT,
            completed_at TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY (id, flow_id)
        );

        CREATE TABLE IF NOT EXISTS task_deps (
            flow_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            depends_on_task_id TEXT NOT NULL,
            PRIMARY KEY (flow_id, task_id, depends_on_task_id)
        );

        CREATE TABLE IF NOT EXISTS schedules (
            id TEXT PRIMARY KEY,
            queue_id TEXT NOT NULL,
            name TEXT,
            cron TEXT NOT NULL,
            flow_def TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            last_triggered_at TEXT,
            next_run_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS _meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        ",
    )
    .map_err(|e| StorageError::Internal(format!("snapshot schema: {e}")))?;
    Ok(())
}

fn write_queues(conn: &Connection, state: &MemState) -> Result<(), StorageError> {
    let mut stmt = conn
        .prepare_cached(
            "INSERT INTO queues (id, config, created_at, updated_at) VALUES (?1, ?2, ?3, ?4)",
        )
        .map_err(|e| StorageError::Internal(format!("snapshot prepare queues: {e}")))?;

    for queue in state.queues.values() {
        let config_json = serde_json::to_string(&queue.config).unwrap_or_default();
        stmt.execute(rusqlite::params![
            queue.id.as_str(),
            config_json,
            queue.created_at.to_rfc3339(),
            queue.updated_at.to_rfc3339(),
        ])
        .map_err(|e| StorageError::Internal(format!("snapshot insert queue: {e}")))?;
    }
    Ok(())
}

fn write_flows(conn: &Connection, state: &MemState) -> Result<(), StorageError> {
    let mut stmt = conn
        .prepare_cached(
            "INSERT INTO flows (id, queue_id, state, task_count, tasks_succeeded, tasks_failed,
              webhooks, trigger_depth, flow_def, fail_fast, parent_flow_id, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        )
        .map_err(|e| StorageError::Internal(format!("snapshot prepare flows: {e}")))?;

    for flow in state.flows.values() {
        let state_str = flow.state.to_string();
        let webhooks_json = flow
            .webhooks
            .as_ref()
            .and_then(|w| serde_json::to_string(w).ok());
        let flow_def_json = flow
            .flow_def
            .as_ref()
            .and_then(|fd| serde_json::to_string(fd).ok());
        let parent_flow_id = flow.parent_flow_id.as_ref().map(|p| p.as_str().to_owned());

        stmt.execute(rusqlite::params![
            flow.id.as_str(),
            flow.queue_id.as_str(),
            state_str,
            flow.task_count as i64,
            flow.tasks_succeeded as i64,
            flow.tasks_failed as i64,
            webhooks_json,
            flow.trigger_depth as i64,
            flow_def_json,
            flow.fail_fast as i64,
            parent_flow_id,
            flow.created_at.to_rfc3339(),
            flow.updated_at.to_rfc3339(),
        ])
        .map_err(|e| StorageError::Internal(format!("snapshot insert flow: {e}")))?;
    }
    Ok(())
}

fn write_tasks(conn: &Connection, state: &MemState) -> Result<(), StorageError> {
    let mut stmt = conn
        .prepare_cached(
            "INSERT INTO tasks (id, flow_id, queue_id, state, executor_type, executor_config,
              input, output, error, retries_remaining, backoff, timeout_secs, condition,
              retry_at, started_at, completed_at, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
        )
        .map_err(|e| StorageError::Internal(format!("snapshot prepare tasks: {e}")))?;

    for task in state.tasks.values() {
        let state_str = task.state.to_string();
        let executor_config_json = serde_json::to_string(&task.executor_config).unwrap_or_default();
        let input_json = task
            .input
            .as_ref()
            .and_then(|v| serde_json::to_string(v).ok());
        let output_json = task
            .output
            .as_ref()
            .and_then(|v| serde_json::to_string(v).ok());
        let backoff_json = serde_json::to_string(&task.backoff).unwrap_or_default();
        let retry_at_str = task.retry_at.map(|dt| dt.to_rfc3339());
        let started_at_str = task.started_at.map(|dt| dt.to_rfc3339());
        let completed_at_str = task.completed_at.map(|dt| dt.to_rfc3339());

        stmt.execute(rusqlite::params![
            task.id.as_str(),
            task.flow_id.as_str(),
            task.queue_id.as_str(),
            state_str,
            task.executor_type,
            executor_config_json,
            input_json,
            output_json,
            task.error,
            task.retries_remaining as i64,
            backoff_json,
            task.timeout_secs as i64,
            task.condition,
            retry_at_str,
            started_at_str,
            completed_at_str,
            task.created_at.to_rfc3339(),
        ])
        .map_err(|e| StorageError::Internal(format!("snapshot insert task: {e}")))?;
    }
    Ok(())
}

fn write_task_deps(conn: &Connection, state: &MemState) -> Result<(), StorageError> {
    let mut stmt = conn
        .prepare_cached(
            "INSERT INTO task_deps (flow_id, task_id, depends_on_task_id) VALUES (?1, ?2, ?3)",
        )
        .map_err(|e| StorageError::Internal(format!("snapshot prepare task_deps: {e}")))?;

    for ((task_id, flow_id), dep_ids) in &state.deps {
        for dep_id in dep_ids {
            stmt.execute(rusqlite::params![
                flow_id.as_str(),
                task_id.as_str(),
                dep_id.as_str(),
            ])
            .map_err(|e| StorageError::Internal(format!("snapshot insert task_dep: {e}")))?;
        }
    }
    Ok(())
}

fn write_schedules(conn: &Connection, state: &MemState) -> Result<(), StorageError> {
    let mut stmt = conn
        .prepare_cached(
            "INSERT INTO schedules (id, queue_id, name, cron, flow_def, enabled,
              last_triggered_at, next_run_at, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )
        .map_err(|e| StorageError::Internal(format!("snapshot prepare schedules: {e}")))?;

    for schedule in state.schedules.values() {
        let flow_def_json = serde_json::to_string(&schedule.flow_def).unwrap_or_default();
        let last_triggered = schedule.last_triggered_at.map(|dt| dt.to_rfc3339());
        let next_run = schedule.next_run_at.map(|dt| dt.to_rfc3339());

        stmt.execute(rusqlite::params![
            schedule.id.as_str(),
            schedule.queue_id.as_str(),
            schedule.name,
            schedule.cron,
            flow_def_json,
            schedule.enabled as i64,
            last_triggered,
            next_run,
            schedule.created_at.to_rfc3339(),
            schedule.updated_at.to_rfc3339(),
        ])
        .map_err(|e| StorageError::Internal(format!("snapshot insert schedule: {e}")))?;
    }
    Ok(())
}

fn write_meta(conn: &Connection, state: &MemState, journal_seq: u64) -> Result<(), StorageError> {
    let mut stmt = conn
        .prepare_cached("INSERT INTO _meta (key, value) VALUES (?1, ?2)")
        .map_err(|e| StorageError::Internal(format!("snapshot prepare meta: {e}")))?;

    stmt.execute(rusqlite::params!["journal_seq", journal_seq.to_string()])
        .map_err(|e| StorageError::Internal(format!("snapshot insert meta: {e}")))?;
    stmt.execute(rusqlite::params!["created_at", Utc::now().to_rfc3339()])
        .map_err(|e| StorageError::Internal(format!("snapshot insert meta: {e}")))?;
    stmt.execute(rusqlite::params![
        "flow_count",
        state.flows.len().to_string()
    ])
    .map_err(|e| StorageError::Internal(format!("snapshot insert meta: {e}")))?;
    stmt.execute(rusqlite::params![
        "task_count",
        state.tasks.len().to_string()
    ])
    .map_err(|e| StorageError::Internal(format!("snapshot insert meta: {e}")))?;

    Ok(())
}

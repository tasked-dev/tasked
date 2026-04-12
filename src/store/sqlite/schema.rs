use super::super::StorageError;
use rusqlite::Connection;

/// Initialize the full schema (queues + flows + tasks + deps + schedules).
pub(crate) fn init_schema(conn: &Connection) -> Result<(), StorageError> {
    conn.execute_batch(
        "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA busy_timeout = 5000;
            PRAGMA cache_size = -65536;
            PRAGMA foreign_keys = ON;
            PRAGMA temp_store = MEMORY;

            CREATE TABLE IF NOT EXISTS queues (
                id TEXT PRIMARY KEY,
                config TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS flows (
                id TEXT PRIMARY KEY,
                queue_id TEXT NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
                state TEXT NOT NULL DEFAULT 'running'
                    CHECK (state IN ('running', 'succeeded', 'failed', 'cancelled')),
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
                flow_id TEXT NOT NULL REFERENCES flows(id) ON DELETE CASCADE,
                queue_id TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'ready', 'running', 'succeeded', 'failed', 'delayed', 'cancelled')),
                executor_type TEXT NOT NULL,
                executor_config TEXT NOT NULL,
                input TEXT,
                output TEXT,
                error TEXT,
                retries_remaining INTEGER NOT NULL DEFAULT 3,
                backoff TEXT NOT NULL DEFAULT '{\"exponential\":{\"initial_delay_ms\":1000}}',
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

            CREATE INDEX IF NOT EXISTS idx_tasks_ready ON tasks(queue_id, created_at) WHERE state = 'ready';
            CREATE INDEX IF NOT EXISTS idx_tasks_delayed ON tasks(retry_at) WHERE state = 'delayed';
            CREATE INDEX IF NOT EXISTS idx_tasks_running ON tasks(started_at) WHERE state = 'running';
            CREATE INDEX IF NOT EXISTS idx_flows_state ON flows(queue_id, state);
            CREATE INDEX IF NOT EXISTS idx_flows_parent ON flows(parent_flow_id) WHERE parent_flow_id IS NOT NULL;

            CREATE TABLE IF NOT EXISTS schedules (
                id TEXT PRIMARY KEY,
                queue_id TEXT NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
                name TEXT,
                cron TEXT NOT NULL,
                flow_def TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                last_triggered_at TEXT,
                next_run_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_schedules_next_run ON schedules(next_run_at) WHERE enabled = 1;
            ",
    )
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    // Migration: add fail_fast column (ignore if already exists)
    let _ =
        conn.execute_batch("ALTER TABLE flows ADD COLUMN fail_fast INTEGER NOT NULL DEFAULT 0;");

    // Migration: add parent_flow_id column (ignore if already exists)
    let _ = conn.execute_batch("ALTER TABLE flows ADD COLUMN parent_flow_id TEXT;");

    Ok(())
}

/// Initialize schema for a catalog database (queue metadata + routing tables).
pub(crate) fn init_catalog_schema(conn: &Connection) -> Result<(), StorageError> {
    conn.execute_batch(
        "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA busy_timeout = 5000;
            PRAGMA foreign_keys = ON;
            PRAGMA temp_store = MEMORY;

            CREATE TABLE IF NOT EXISTS queues (
                id TEXT PRIMARY KEY,
                config TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS flow_map (
                flow_id TEXT PRIMARY KEY,
                queue_id TEXT NOT NULL,
                parent_flow_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_flow_map_queue ON flow_map(queue_id);
            CREATE INDEX IF NOT EXISTS idx_flow_map_parent ON flow_map(parent_flow_id) WHERE parent_flow_id IS NOT NULL;

            CREATE TABLE IF NOT EXISTS schedule_map (
                schedule_id TEXT PRIMARY KEY,
                queue_id TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_schedule_map_queue ON schedule_map(queue_id);
            ",
    )
    .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(())
}

/// Initialize schema for a per-queue database (flows, tasks, deps, schedules).
pub(crate) fn init_queue_schema(conn: &Connection) -> Result<(), StorageError> {
    conn.execute_batch(
        "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA busy_timeout = 5000;
            PRAGMA foreign_keys = ON;
            PRAGMA temp_store = MEMORY;
            PRAGMA cache_size = -65536;

            CREATE TABLE IF NOT EXISTS flows (
                id TEXT PRIMARY KEY,
                queue_id TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'running'
                    CHECK (state IN ('running', 'succeeded', 'failed', 'cancelled')),
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
                flow_id TEXT NOT NULL REFERENCES flows(id) ON DELETE CASCADE,
                queue_id TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'ready', 'running', 'succeeded', 'failed', 'delayed', 'cancelled')),
                executor_type TEXT NOT NULL,
                executor_config TEXT NOT NULL,
                input TEXT,
                output TEXT,
                error TEXT,
                retries_remaining INTEGER NOT NULL DEFAULT 3,
                backoff TEXT NOT NULL DEFAULT '{\"exponential\":{\"initial_delay_ms\":1000}}',
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

            CREATE INDEX IF NOT EXISTS idx_tasks_ready ON tasks(queue_id, created_at) WHERE state = 'ready';
            CREATE INDEX IF NOT EXISTS idx_tasks_delayed ON tasks(retry_at) WHERE state = 'delayed';
            CREATE INDEX IF NOT EXISTS idx_tasks_running ON tasks(started_at) WHERE state = 'running';
            CREATE INDEX IF NOT EXISTS idx_flows_state ON flows(queue_id, state);
            CREATE INDEX IF NOT EXISTS idx_flows_parent ON flows(parent_flow_id) WHERE parent_flow_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_tasks_flow_id ON tasks(flow_id);
            CREATE INDEX IF NOT EXISTS idx_task_deps_flow_id ON task_deps(flow_id);

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

            CREATE INDEX IF NOT EXISTS idx_schedules_next_run ON schedules(next_run_at) WHERE enabled = 1;
            ",
    )
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    // Migration: add fail_fast column (ignore if already exists)
    let _ =
        conn.execute_batch("ALTER TABLE flows ADD COLUMN fail_fast INTEGER NOT NULL DEFAULT 0;");

    // Migration: add parent_flow_id column (ignore if already exists)
    let _ = conn.execute_batch("ALTER TABLE flows ADD COLUMN parent_flow_id TEXT;");

    Ok(())
}

/// Migrate existing databases that lack the `flow_def` column on the `flows` table.
/// Idempotent: silently ignores "duplicate column name" if the column already exists,
/// which also avoids a TOCTOU race when multiple processes open the same DB file.
pub(crate) fn migrate_flows_add_flow_def(conn: &Connection) -> Result<(), StorageError> {
    match conn.execute_batch("ALTER TABLE flows ADD COLUMN flow_def TEXT") {
        Ok(()) => Ok(()),
        Err(e) if e.to_string().contains("duplicate column name") => Ok(()),
        Err(e) => Err(StorageError::Internal(e.to_string())),
    }
}

/// Add indexes on `tasks(flow_id)` and `task_deps(flow_id)` to speed up
/// flow-scoped queries. Idempotent via `CREATE INDEX IF NOT EXISTS`.
pub(crate) fn migrate_add_flow_id_indexes(conn: &Connection) -> Result<(), StorageError> {
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_tasks_flow_id ON tasks(flow_id);
         CREATE INDEX IF NOT EXISTS idx_task_deps_flow_id ON task_deps(flow_id);",
    )
    .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(())
}

/// Migrate the catalog's `flow_map` table to include `parent_flow_id`.
/// Idempotent: silently ignores "duplicate column name".
pub(crate) fn migrate_flow_map_add_parent(conn: &Connection) -> Result<(), StorageError> {
    match conn.execute_batch(
        "ALTER TABLE flow_map ADD COLUMN parent_flow_id TEXT;
         CREATE INDEX IF NOT EXISTS idx_flow_map_parent ON flow_map(parent_flow_id) WHERE parent_flow_id IS NOT NULL;",
    ) {
        Ok(()) => Ok(()),
        Err(e) if e.to_string().contains("duplicate column name") => Ok(()),
        Err(e) => Err(StorageError::Internal(e.to_string())),
    }
}

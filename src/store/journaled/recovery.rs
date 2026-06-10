use std::collections::HashMap;
use std::path::Path;

use chrono::Utc;
use rusqlite::Connection;

use super::events::JournalEvent;
use super::state::MemState;
use crate::store::StorageError;
use crate::types::*;

/// Recover state from a snapshot and/or journal.
///
/// Returns `(recovered_state, next_sequence_number)`.
///
/// Recovery proceeds in phases:
/// 1. Load snapshot (if present) to bootstrap state
/// 2. Replay journal entries after the snapshot sequence
/// 3. Recompute flow counters for consistency
/// 4. Recover in-flight tasks (Running -> Delayed or Failed)
/// 5. Detect terminal flows whose state needs updating
pub(crate) fn recover(
    snapshot_path: Option<&Path>,
    journal_path: &Path,
) -> Result<(MemState, u64), StorageError> {
    let recovery_start = std::time::Instant::now();

    // 1. Load snapshot
    let t0 = std::time::Instant::now();
    let (mut state, snapshot_seq) = match snapshot_path {
        Some(path) if path.exists() => load_snapshot(path)?,
        _ => (MemState::new(), 0),
    };
    let snapshot_elapsed = t0.elapsed();
    tracing::info!(
        elapsed_ms = snapshot_elapsed.as_millis(),
        snapshot_seq = snapshot_seq,
        flows = state.flows.len(),
        tasks = state.tasks.len(),
        "recovery step 1: load snapshot"
    );

    // 2. Replay journal
    let t0 = std::time::Instant::now();
    let max_seq = if journal_path.exists() {
        replay_journal(journal_path, snapshot_seq, &mut state)?
    } else {
        snapshot_seq
    };
    let replay_elapsed = t0.elapsed();
    let entries_replayed = max_seq.saturating_sub(snapshot_seq);
    tracing::info!(
        elapsed_ms = replay_elapsed.as_millis(),
        entries = entries_replayed,
        flows = state.flows.len(),
        tasks = state.tasks.len(),
        "recovery step 2: replay journal"
    );

    // 3. Recompute flow counters
    let t0 = std::time::Instant::now();
    recompute_flow_counters(&mut state);
    tracing::info!(
        elapsed_ms = t0.elapsed().as_millis(),
        "recovery step 3: recompute flow counters"
    );

    // 4. Recover in-flight tasks
    let t0 = std::time::Instant::now();
    recover_in_flight_tasks(&mut state);
    tracing::info!(
        elapsed_ms = t0.elapsed().as_millis(),
        "recovery step 4: recover in-flight tasks"
    );

    // 5. Detect terminal flows
    let t0 = std::time::Instant::now();
    detect_terminal_flows(&mut state);
    tracing::info!(
        elapsed_ms = t0.elapsed().as_millis(),
        "recovery step 5: detect terminal flows"
    );

    tracing::info!(
        total_ms = recovery_start.elapsed().as_millis(),
        "recovery complete"
    );

    let next_seq = if max_seq > 0 { max_seq + 1 } else { 1 };

    tracing::info!(
        snapshot_seq = snapshot_seq,
        replayed_to = max_seq,
        next_seq = next_seq,
        flows = state.flows.len(),
        tasks = state.tasks.len(),
        queues = state.queues.len(),
        "journal recovery complete"
    );

    Ok((state, next_seq))
}

/// Load full state from a snapshot database.
fn load_snapshot(path: &Path) -> Result<(MemState, u64), StorageError> {
    let conn = Connection::open(path)
        .map_err(|e| StorageError::Internal(format!("snapshot open: {e}")))?;

    conn.execute_batch(
        "PRAGMA busy_timeout = 5000;
         PRAGMA temp_store = MEMORY;",
    )
    .map_err(|e| StorageError::Internal(format!("snapshot pragmas: {e}")))?;

    let mut state = MemState::new();

    // Read journal_seq from _meta
    let journal_seq: u64 = conn
        .query_row(
            "SELECT value FROM _meta WHERE key = 'journal_seq'",
            [],
            |row| {
                let v: String = row.get(0)?;
                Ok(v.parse::<u64>().unwrap_or(0))
            },
        )
        .map_err(|e| StorageError::Internal(format!("snapshot read meta: {e}")))?;

    // Load queues
    load_queues(&conn, &mut state)?;

    // Load flows
    load_flows(&conn, &mut state)?;

    // Load tasks (and build secondary indexes)
    load_tasks(&conn, &mut state)?;

    // Load task_deps (and build forward + reverse indexes)
    load_task_deps(&conn, &mut state)?;

    // Load schedules
    load_schedules(&conn, &mut state)?;

    tracing::info!(
        journal_seq = journal_seq,
        queues = state.queues.len(),
        flows = state.flows.len(),
        tasks = state.tasks.len(),
        schedules = state.schedules.len(),
        "snapshot loaded"
    );

    Ok((state, journal_seq))
}

fn load_queues(conn: &Connection, state: &mut MemState) -> Result<(), StorageError> {
    let mut stmt = conn
        .prepare("SELECT id, config, created_at, updated_at FROM queues")
        .map_err(|e| StorageError::Internal(format!("snapshot query queues: {e}")))?;

    let queues = stmt
        .query_map([], crate::store::sqlite::rows::row_to_queue)
        .map_err(|e| StorageError::Internal(format!("snapshot read queues: {e}")))?;

    for queue in queues {
        let queue =
            queue.map_err(|e| StorageError::Internal(format!("snapshot parse queue: {e}")))?;
        state.queues.insert(queue.id.clone(), queue);
    }
    Ok(())
}

fn load_flows(conn: &Connection, state: &mut MemState) -> Result<(), StorageError> {
    let mut stmt = conn
        .prepare(
            "SELECT id, queue_id, state, task_count, tasks_succeeded, tasks_failed,
                    webhooks, trigger_depth, flow_def, fail_fast, parent_flow_id,
                    created_at, updated_at
             FROM flows",
        )
        .map_err(|e| StorageError::Internal(format!("snapshot query flows: {e}")))?;

    let flows = stmt
        .query_map([], crate::store::sqlite::rows::row_to_flow)
        .map_err(|e| StorageError::Internal(format!("snapshot read flows: {e}")))?;

    for flow in flows {
        let flow = flow.map_err(|e| StorageError::Internal(format!("snapshot parse flow: {e}")))?;
        state.flows.insert(flow.id.clone(), flow);
    }
    Ok(())
}

fn load_tasks(conn: &Connection, state: &mut MemState) -> Result<(), StorageError> {
    let mut stmt = conn
        .prepare(
            "SELECT id, flow_id, queue_id, state, executor_type, executor_config,
                    input, output, error, retries_remaining, backoff, timeout_secs,
                    condition, retry_at, started_at, completed_at, created_at
             FROM tasks",
        )
        .map_err(|e| StorageError::Internal(format!("snapshot query tasks: {e}")))?;

    let tasks = stmt
        .query_map([], crate::store::sqlite::rows::row_to_task)
        .map_err(|e| StorageError::Internal(format!("snapshot read tasks: {e}")))?;

    for task in tasks {
        let task = task.map_err(|e| StorageError::Internal(format!("snapshot parse task: {e}")))?;
        let key = (task.id.clone(), task.flow_id.clone());
        state.index_add(&task);
        state.tasks.insert(key, task);
    }
    Ok(())
}

fn load_task_deps(conn: &Connection, state: &mut MemState) -> Result<(), StorageError> {
    let mut stmt = conn
        .prepare("SELECT flow_id, task_id, depends_on_task_id FROM task_deps")
        .map_err(|e| StorageError::Internal(format!("snapshot query task_deps: {e}")))?;

    let deps = stmt
        .query_map([], |row| {
            let flow_id: String = row.get(0)?;
            let task_id: String = row.get(1)?;
            let dep_id: String = row.get(2)?;
            Ok((flow_id, task_id, dep_id))
        })
        .map_err(|e| StorageError::Internal(format!("snapshot read task_deps: {e}")))?;

    // Accumulate forward deps
    let mut forward: HashMap<(TaskId, FlowId), Vec<TaskId>> = HashMap::new();

    for dep in deps {
        let (flow_id_str, task_id_str, dep_id_str) =
            dep.map_err(|e| StorageError::Internal(format!("snapshot parse dep: {e}")))?;

        let flow_id = FlowId::from(flow_id_str);
        let task_id = TaskId::from(task_id_str);
        let dep_id = TaskId::from(dep_id_str);

        // Forward: (task, flow) -> [deps]
        forward
            .entry((task_id.clone(), flow_id.clone()))
            .or_default()
            .push(dep_id.clone());

        // Reverse: (dep, flow) -> [dependents]
        state
            .dependents
            .entry((dep_id, flow_id))
            .or_default()
            .push(task_id);
    }

    state.deps = forward;
    Ok(())
}

fn load_schedules(conn: &Connection, state: &mut MemState) -> Result<(), StorageError> {
    let mut stmt = conn
        .prepare(
            "SELECT id, queue_id, name, cron, flow_def, enabled,
                    last_triggered_at, next_run_at, created_at, updated_at
             FROM schedules",
        )
        .map_err(|e| StorageError::Internal(format!("snapshot query schedules: {e}")))?;

    let schedules = stmt
        .query_map([], crate::store::sqlite::rows::row_to_schedule)
        .map_err(|e| StorageError::Internal(format!("snapshot read schedules: {e}")))?;

    for schedule in schedules {
        let schedule = schedule
            .map_err(|e| StorageError::Internal(format!("snapshot parse schedule: {e}")))?;
        state.schedules.insert(schedule.id.clone(), schedule);
    }
    Ok(())
}

/// Replay journal entries after `after_seq` into the given state.
/// Returns the highest sequence number replayed, or `after_seq` if nothing was replayed.
fn replay_journal(
    journal_path: &Path,
    after_seq: u64,
    state: &mut MemState,
) -> Result<u64, StorageError> {
    let conn = Connection::open(journal_path)
        .map_err(|e| StorageError::Internal(format!("journal open: {e}")))?;

    conn.execute_batch(
        "PRAGMA busy_timeout = 5000;
         PRAGMA temp_store = MEMORY;",
    )
    .map_err(|e| StorageError::Internal(format!("journal pragmas: {e}")))?;

    // Check if the journal table exists
    let table_exists: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='journal'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| StorageError::Internal(format!("journal table check: {e}")))?
        > 0;

    if !table_exists {
        return Ok(after_seq);
    }

    let mut stmt = conn
        .prepare("SELECT seq, payload, crc32 FROM journal WHERE seq > ?1 ORDER BY seq ASC")
        .map_err(|e| StorageError::Internal(format!("journal prepare: {e}")))?;

    let mut max_seq = after_seq;
    let mut replayed = 0u64;

    let rows = stmt
        .query_map(rusqlite::params![after_seq as i64], |row| {
            let seq: i64 = row.get(0)?;
            let payload: Vec<u8> = row.get(1)?;
            let crc: i64 = row.get(2)?;
            Ok((seq, payload, crc))
        })
        .map_err(|e| StorageError::Internal(format!("journal query: {e}")))?;

    for row in rows {
        let (seq, payload, stored_crc) =
            row.map_err(|e| StorageError::Internal(format!("journal row: {e}")))?;

        // Verify CRC32
        let computed_crc = crc32fast::hash(&payload) as i64;
        if computed_crc != stored_crc {
            tracing::warn!(
                seq = seq,
                expected_crc = stored_crc,
                computed_crc = computed_crc,
                "CRC32 mismatch at seq {seq}, stopping replay (prefix property)"
            );
            break;
        }

        // Deserialize event
        let event: JournalEvent = serde_json::from_slice(&payload).map_err(|e| {
            StorageError::Internal(format!(
                "journal deserialize seq {seq} (payload len {}): {e}",
                payload.len()
            ))
        })?;

        apply_event(state, event);

        max_seq = seq as u64;
        replayed += 1;
    }

    tracing::info!(
        after_seq = after_seq,
        replayed = replayed,
        max_seq = max_seq,
        "journal replay complete"
    );

    Ok(max_seq)
}

/// Apply a journal event to in-memory state.
/// This is the core replay function and must match the mutations
/// performed by the `Storage` trait implementation.
fn apply_event(state: &mut MemState, event: JournalEvent) {
    match event {
        JournalEvent::QueueCreated { queue } => {
            state.queues.insert(queue.id.clone(), queue);
        }
        JournalEvent::QueueDeleted { queue_id } => {
            // Cascade exactly like the live delete_queue path.
            state.remove_queue_cascade(&queue_id);
        }
        JournalEvent::FlowCreated { flow, tasks, deps } => {
            let flow_id = flow.id.clone();
            state.flows.insert(flow.id.clone(), flow);

            for task in tasks {
                let key = (task.id.clone(), task.flow_id.clone());
                state.index_add(&task);
                state.tasks.insert(key, task);
            }

            for (task_id, dep_ids) in deps {
                for dep_id in &dep_ids {
                    state
                        .dependents
                        .entry((dep_id.clone(), flow_id.clone()))
                        .or_default()
                        .push(task_id.clone());
                }
                state.deps.insert((task_id, flow_id.clone()), dep_ids);
            }
        }
        JournalEvent::FlowStateChanged {
            flow_id,
            new_state,
            updated_at,
        } => {
            if let Some(flow) = state.flows.get_mut(&flow_id) {
                flow.state = new_state;
                flow.updated_at = updated_at;
            }
        }
        JournalEvent::TaskCompleted {
            task_id,
            flow_id,
            new_state,
            output,
            error,
            completed_at,
            succeeded,
            newly_ready,
        } => {
            let key = (task_id, flow_id.clone());
            if let Some(task) = state.tasks.get(&key) {
                let snapshot = task.clone();
                state.index_remove(&snapshot);
            }
            if let Some(task) = state.tasks.get_mut(&key) {
                task.state = new_state;
                task.output = output;
                task.error = error;
                task.completed_at = Some(completed_at);
                let snapshot = task.clone();
                state.index_add(&snapshot);
            }
            // Update flow counter
            if let Some(flow) = state.flows.get_mut(&flow_id) {
                if succeeded {
                    flow.tasks_succeeded += 1;
                } else {
                    flow.tasks_failed += 1;
                }
                flow.updated_at = completed_at;
            }
            // Promote newly ready tasks
            for tid in newly_ready {
                let rkey = (tid, flow_id.clone());
                if let Some(task) = state.tasks.get(&rkey) {
                    let snapshot = task.clone();
                    state.index_remove(&snapshot);
                }
                if let Some(task) = state.tasks.get_mut(&rkey) {
                    task.state = TaskState::Ready;
                    let snapshot = task.clone();
                    state.index_add(&snapshot);
                }
            }
        }
        JournalEvent::TaskStateChanged {
            task_id,
            flow_id,
            new_state,
            retry_at,
            started_at,
            retries_remaining,
        } => {
            let key = (task_id, flow_id);
            if let Some(task) = state.tasks.get(&key) {
                let snapshot = task.clone();
                state.index_remove(&snapshot);
            }
            if let Some(task) = state.tasks.get_mut(&key) {
                task.state = new_state;
                if let Some(ra) = retry_at {
                    task.retry_at = Some(ra);
                }
                if let Some(sa) = started_at {
                    task.started_at = Some(sa);
                }
                if let Some(rr) = retries_remaining {
                    task.retries_remaining = rr;
                }
                let snapshot = task.clone();
                state.index_add(&snapshot);
            }
        }
        JournalEvent::TaskOutputSet {
            task_id,
            flow_id,
            output,
        } => {
            let key = (task_id, flow_id);
            if let Some(task) = state.tasks.get_mut(&key) {
                task.output = Some(output);
            }
        }
        JournalEvent::TasksInjected {
            flow_id,
            tasks,
            deps,
            new_task_count,
        } => {
            // Insert tasks + indexes
            for task in tasks {
                let key = (task.id.clone(), task.flow_id.clone());
                state.index_add(&task);
                state.tasks.insert(key, task);
            }
            // Insert deps
            for (task_id, dep_ids) in deps {
                for dep_id in &dep_ids {
                    state
                        .dependents
                        .entry((dep_id.clone(), flow_id.clone()))
                        .or_default()
                        .push(task_id.clone());
                }
                state.deps.insert((task_id, flow_id.clone()), dep_ids);
            }
            // Update flow task_count
            if let Some(flow) = state.flows.get_mut(&flow_id) {
                flow.task_count += new_task_count;
                flow.updated_at = Utc::now();
            }
        }
        JournalEvent::FlowsDeleted {
            queue_id: _,
            flow_ids,
        } => {
            for flow_id in &flow_ids {
                state.flows.remove(flow_id);

                // Remove tasks and their indexes
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

                // Remove deps/dependents for this flow
                state.deps.retain(|(_, fid), _| fid != flow_id);
                state.dependents.retain(|(_, fid), _| fid != flow_id);
            }
        }
        JournalEvent::ScheduleCreated { schedule } => {
            state.schedules.insert(schedule.id.clone(), schedule);
        }
        JournalEvent::ScheduleUpdated { schedule } => {
            state.schedules.insert(schedule.id.clone(), schedule);
        }
        JournalEvent::ScheduleDeleted { schedule_id } => {
            state.schedules.remove(&schedule_id);
        }
        JournalEvent::ScheduleTriggered {
            schedule_id,
            triggered_at,
            next_run_at,
        } => {
            if let Some(schedule) = state.schedules.get_mut(&schedule_id) {
                schedule.last_triggered_at = Some(triggered_at);
                schedule.next_run_at = next_run_at;
                schedule.updated_at = Utc::now();
            }
        }
    }
}

/// Recompute flow counters from actual task states.
/// Logs a warning if the cached counters were inconsistent.
fn recompute_flow_counters(state: &mut MemState) {
    // Count per-flow
    let mut succeeded_counts: HashMap<FlowId, usize> = HashMap::new();
    let mut failed_counts: HashMap<FlowId, usize> = HashMap::new();

    for task in state.tasks.values() {
        match task.state {
            TaskState::Succeeded => {
                *succeeded_counts.entry(task.flow_id.clone()).or_default() += 1;
            }
            TaskState::Failed => {
                *failed_counts.entry(task.flow_id.clone()).or_default() += 1;
            }
            _ => {}
        }
    }

    for flow in state.flows.values_mut() {
        let actual_succeeded = succeeded_counts.get(&flow.id).copied().unwrap_or(0);
        let actual_failed = failed_counts.get(&flow.id).copied().unwrap_or(0);

        if flow.tasks_succeeded != actual_succeeded {
            tracing::warn!(
                flow_id = %flow.id,
                cached = flow.tasks_succeeded,
                actual = actual_succeeded,
                "flow tasks_succeeded counter mismatch, correcting"
            );
            flow.tasks_succeeded = actual_succeeded;
        }
        if flow.tasks_failed != actual_failed {
            tracing::warn!(
                flow_id = %flow.id,
                cached = flow.tasks_failed,
                actual = actual_failed,
                "flow tasks_failed counter mismatch, correcting"
            );
            flow.tasks_failed = actual_failed;
        }
    }
}

/// Recover tasks that were running when the process crashed.
/// - Running with retries_remaining > 0 -> Delayed (retry_at = now)
/// - Running with retries_remaining == 0 -> Failed
fn recover_in_flight_tasks(state: &mut MemState) {
    let now = Utc::now();

    // Collect running task keys
    let running_keys: Vec<(TaskId, FlowId)> = state.running_index.iter().cloned().collect();

    for key in running_keys {
        if let Some(task) = state.tasks.get(&key) {
            let snapshot = task.clone();
            state.index_remove(&snapshot);
        }

        if let Some(task) = state.tasks.get_mut(&key) {
            if task.retries_remaining > 0 {
                tracing::info!(
                    task_id = %task.id,
                    flow_id = %task.flow_id,
                    retries_remaining = task.retries_remaining,
                    "recovering in-flight task as delayed"
                );
                task.state = TaskState::Delayed;
                task.retry_at = Some(now);
                task.started_at = None;
            } else {
                tracing::info!(
                    task_id = %task.id,
                    flow_id = %task.flow_id,
                    "recovering in-flight task as failed (no retries remaining)"
                );
                task.state = TaskState::Failed;
                task.error = Some("recovered after crash with no retries remaining".to_string());
                task.completed_at = Some(now);
            }
            let snapshot = task.clone();
            state.index_add(&snapshot);
        }
    }
}

/// Detect flows that should be terminal but are still marked Running.
/// A flow is terminal if all its tasks are in terminal states.
fn detect_terminal_flows(state: &mut MemState) {
    // Build per-flow task state summary in a single pass over all tasks: O(tasks).
    // Previous implementation was O(flows × tasks) — 43K × 426K = catastrophic.
    struct FlowSummary {
        all_terminal: bool,
        any_failed: bool,
    }

    let mut summaries: HashMap<FlowId, FlowSummary> = HashMap::new();
    for task in state.tasks.values() {
        let entry = summaries
            .entry(task.flow_id.clone())
            .or_insert(FlowSummary {
                all_terminal: true,
                any_failed: false,
            });
        if !task.state.is_terminal() {
            entry.all_terminal = false;
        }
        if task.state == TaskState::Failed || task.state == TaskState::Cancelled {
            entry.any_failed = true;
        }
    }

    let running_flow_ids: Vec<FlowId> = state
        .flows
        .values()
        .filter(|f| f.state == FlowState::Running)
        .map(|f| f.id.clone())
        .collect();

    for flow_id in running_flow_ids {
        let summary = match summaries.get(&flow_id) {
            Some(s) => s,
            None => continue,
        };

        if !summary.all_terminal {
            continue;
        }

        let new_state = if summary.any_failed {
            FlowState::Failed
        } else {
            FlowState::Succeeded
        };

        tracing::info!(
            flow_id = %flow_id,
            new_state = %new_state,
            "detected terminal flow during recovery"
        );

        if let Some(flow) = state.flows.get_mut(&flow_id) {
            flow.state = new_state;
            flow.updated_at = Utc::now();
        }
    }
}

/// Read the journal_seq from a snapshot's _meta table.
/// Returns 0 if the snapshot doesn't exist or has no journal_seq.
#[allow(dead_code)]
pub(crate) fn read_snapshot_seq(snapshot_path: &Path) -> u64 {
    if !snapshot_path.exists() {
        return 0;
    }
    let conn = match Connection::open(snapshot_path) {
        Ok(c) => c,
        Err(_) => return 0,
    };
    conn.query_row(
        "SELECT value FROM _meta WHERE key = 'journal_seq'",
        [],
        |row| {
            let v: String = row.get(0)?;
            Ok(v.parse::<u64>().unwrap_or(0))
        },
    )
    .unwrap_or(0)
}

//! Durability tests for the journaled storage backend (issue #6):
//! write -> reopen -> recover round-trips, CRC-corruption truncation,
//! cancelled tasks staying cancelled across replay, and emit_durable
//! failing fast (instead of hanging) when the writer dies.

#![cfg(feature = "journaled")]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use chrono::Utc;
use tasked::store::journaled::JournaledStorage;
use tasked::store::journaled::config::JournalConfig;
use tasked::store::{Storage, StorageError};
use tasked::types::*;

fn config_for(dir: &Path) -> JournalConfig {
    JournalConfig {
        journal_path: Some(dir.join("journal.db")),
        ..JournalConfig::default()
    }
}

fn mk_queue(id: &str) -> Queue {
    let now = Utc::now();
    Queue {
        id: QueueId::from(id),
        config: QueueConfig::default(),
        created_at: now,
        updated_at: now,
    }
}

fn mk_flow(queue_id: &QueueId, id: &str, task_count: usize) -> Flow {
    let now = Utc::now();
    Flow {
        id: FlowId::from(id),
        queue_id: queue_id.clone(),
        state: FlowState::Running,
        task_count,
        tasks_succeeded: 0,
        tasks_failed: 0,
        webhooks: None,
        trigger_depth: 0,
        flow_def: None,
        fail_fast: false,
        parent_flow_id: None,
        created_at: now,
        updated_at: now,
    }
}

fn mk_task(queue_id: &QueueId, flow_id: &FlowId, id: &str, state: TaskState) -> Task {
    Task {
        id: TaskId::from(id),
        flow_id: flow_id.clone(),
        queue_id: queue_id.clone(),
        state,
        executor_type: "shell".into(),
        executor_config: serde_json::json!({ "command": "true" }),
        input: None,
        output: None,
        error: None,
        retries_remaining: 3,
        backoff: BackoffStrategy::default(),
        timeout_secs: 300,
        condition: None,
        retry_at: None,
        started_at: None,
        completed_at: None,
        created_at: Utc::now(),
    }
}

/// Write some state, shut down, reopen, and verify recovery reproduces it.
#[tokio::test]
async fn roundtrip_write_reopen_recover() {
    let dir = tempfile::tempdir().unwrap();
    let qid = QueueId::from("q");
    let f1 = FlowId::from("f1");
    let f2 = FlowId::from("f2");

    {
        let store = JournaledStorage::open(config_for(dir.path())).unwrap();
        store.create_queue(&mk_queue("q")).await.unwrap();

        // Flow with a completed task and a promoted dependent.
        let x = mk_task(&qid, &f1, "x", TaskState::Ready);
        let y = mk_task(&qid, &f1, "y", TaskState::Pending);
        let mut deps = HashMap::new();
        deps.insert(TaskId::from("y"), vec![TaskId::from("x")]);
        store
            .create_flow(&mk_flow(&qid, "f1", 2), &[x, y], &deps)
            .await
            .unwrap();
        store
            .mark_task_running(&TaskId::from("x"), &f1)
            .await
            .unwrap();
        store
            .complete_task_with_ready(
                &TaskId::from("x"),
                &f1,
                Some(serde_json::json!({ "out": 42 })),
                &[TaskId::from("y")],
            )
            .await
            .unwrap();

        // Flow with an in-flight (running) task at "crash" time.
        let z = mk_task(&qid, &f2, "z", TaskState::Ready);
        store
            .create_flow(&mk_flow(&qid, "f2", 1), &[z], &HashMap::new())
            .await
            .unwrap();
        store
            .mark_task_running(&TaskId::from("z"), &f2)
            .await
            .unwrap();

        // Graceful shutdown flushes everything; Drop would do the same.
        store.shutdown().await;
    }

    let store = JournaledStorage::open(config_for(dir.path())).unwrap();
    assert!(store.get_queue(&qid).await.unwrap().is_some());

    let flow1 = store.get_flow(&f1).await.unwrap().expect("f1 recovered");
    assert_eq!(flow1.tasks_succeeded, 1);
    let x = store
        .get_task(&TaskId::from("x"), &f1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(x.state, TaskState::Succeeded);
    assert_eq!(x.output, Some(serde_json::json!({ "out": 42 })));
    let y = store
        .get_task(&TaskId::from("y"), &f1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(y.state, TaskState::Ready);
    // The recovered ready task must be visible through the ready index.
    assert!(
        store
            .fetch_ready_tasks(&qid, 10)
            .await
            .unwrap()
            .iter()
            .any(|t| t.id == TaskId::from("y"))
    );

    // Dependencies survived recovery.
    assert_eq!(
        store
            .get_task_dependencies(&TaskId::from("y"), &f1)
            .await
            .unwrap(),
        vec![TaskId::from("x")]
    );

    // The in-flight task is recovered as Delayed (it had retries left).
    let z = store
        .get_task(&TaskId::from("z"), &f2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(z.state, TaskState::Delayed);
    assert!(z.retry_at.is_some());
}

/// Corrupt a journal row mid-file: recovery must stop at the corruption,
/// truncate the journal there, and allow NEW writes (no seq PRIMARY KEY
/// collision) plus a clean second recovery. Regression for issue #6 item 4.
#[tokio::test]
async fn crc_corruption_truncates_and_allows_new_writes() {
    let dir = tempfile::tempdir().unwrap();
    let journal_path = dir.path().join("journal.db");
    let qid = QueueId::from("q");

    {
        let store = JournaledStorage::open(config_for(dir.path())).unwrap();
        store.create_queue(&mk_queue("q")).await.unwrap(); // seq 1
        store
            .create_flow(&mk_flow(&qid, "f1", 0), &[], &HashMap::new())
            .await
            .unwrap(); // seq 2
        store
            .create_flow(&mk_flow(&qid, "f2", 0), &[], &HashMap::new())
            .await
            .unwrap(); // seq 3
        store.shutdown().await;
    }

    // Corrupt the payload of the last journal row (CRC no longer matches).
    {
        let conn = rusqlite::Connection::open(&journal_path).unwrap();
        let max_seq: i64 = conn
            .query_row("SELECT MAX(seq) FROM journal", [], |r| r.get(0))
            .unwrap();
        assert_eq!(max_seq, 3, "expected three journal entries");
        conn.execute(
            "UPDATE journal SET payload = zeroblob(4) WHERE seq = ?1",
            rusqlite::params![max_seq],
        )
        .unwrap();
    }

    // First recovery: replay stops at seq 3 and deletes rows >= 3.
    {
        let store = JournaledStorage::open(config_for(dir.path())).unwrap();
        assert!(store.get_queue(&qid).await.unwrap().is_some());
        assert!(
            store
                .get_flow(&FlowId::from("f1"))
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store.get_flow(&FlowId::from("f2")).await.unwrap().is_none(),
            "the corrupted event must not be applied"
        );

        // NEW writes reuse seq 3; without truncation this collides on the
        // journal PRIMARY KEY and kills the writer.
        store
            .create_flow(&mk_flow(&qid, "f3", 0), &[], &HashMap::new())
            .await
            .expect("write after CRC truncation must succeed");
        store.health_check().await.expect("writer must stay alive");
        store.shutdown().await;
    }

    // Second recovery sees the post-corruption write.
    {
        let store = JournaledStorage::open(config_for(dir.path())).unwrap();
        assert!(
            store
                .get_flow(&FlowId::from("f1"))
                .await
                .unwrap()
                .is_some()
        );
        assert!(store.get_flow(&FlowId::from("f2")).await.unwrap().is_none());
        assert!(
            store
                .get_flow(&FlowId::from("f3"))
                .await
                .unwrap()
                .is_some(),
            "write made after truncation must survive a second recovery"
        );
    }
}

/// A task cancelled after appearing in a TaskCompleted.newly_ready list must
/// not be resurrected to Ready by journal replay. Regression for issue #6
/// item 5.
#[tokio::test]
async fn replay_does_not_resurrect_cancelled_tasks() {
    let dir = tempfile::tempdir().unwrap();
    let qid = QueueId::from("q");
    let f1 = FlowId::from("f1");

    {
        let store = JournaledStorage::open(config_for(dir.path())).unwrap();
        store.create_queue(&mk_queue("q")).await.unwrap();

        let x = mk_task(&qid, &f1, "x", TaskState::Ready);
        let y = mk_task(&qid, &f1, "y", TaskState::Pending);
        let mut deps = HashMap::new();
        deps.insert(TaskId::from("y"), vec![TaskId::from("x")]);
        store
            .create_flow(&mk_flow(&qid, "f1", 2), &[x, y], &deps)
            .await
            .unwrap();

        store
            .mark_task_running(&TaskId::from("x"), &f1)
            .await
            .unwrap();
        // Cancel y BEFORE x completes; the engine may still pass y in
        // newly_ready (it resolves dependents from the in-memory graph).
        store
            .update_task_state(&TaskId::from("y"), &f1, TaskState::Cancelled)
            .await
            .unwrap();
        store
            .complete_task_with_ready(&TaskId::from("x"), &f1, None, &[TaskId::from("y")])
            .await
            .unwrap();

        // Live path must not have promoted it either.
        assert_eq!(
            store
                .get_task(&TaskId::from("y"), &f1)
                .await
                .unwrap()
                .unwrap()
                .state,
            TaskState::Cancelled
        );
        store.shutdown().await;
    }

    let store = JournaledStorage::open(config_for(dir.path())).unwrap();
    let y = store
        .get_task(&TaskId::from("y"), &f1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        y.state,
        TaskState::Cancelled,
        "replay of TaskCompleted.newly_ready must not resurrect a cancelled task"
    );
    assert!(
        !store
            .fetch_ready_tasks(&qid, 10)
            .await
            .unwrap()
            .iter()
            .any(|t| t.id == TaskId::from("y")),
        "cancelled task must not reappear in the ready index"
    );
}

/// Kill the writer by poisoning the journal with a row at the next sequence
/// number (the INSERT collides on the PRIMARY KEY, exhausting the writer's
/// retries). emit_durable must then return an error instead of hanging, and
/// subsequent mutations must fail fast.
#[tokio::test]
async fn emit_durable_errors_when_writer_dies() {
    let dir = tempfile::tempdir().unwrap();
    let journal_path = dir.path().join("journal.db");
    let qid = QueueId::from("q");

    let store = Arc::new(JournaledStorage::open(config_for(dir.path())).unwrap());
    store.create_queue(&mk_queue("q")).await.unwrap(); // seq 1, flushed

    // Poison: pre-insert a row at the writer's next sequence number.
    {
        let conn = rusqlite::Connection::open(&journal_path).unwrap();
        conn.execute(
            "INSERT INTO journal (seq, event_type, payload, crc32, created_at) \
             VALUES (2, 99, zeroblob(1), 0, '2026-01-01T00:00:00Z')",
            [],
        )
        .unwrap();
    }

    // The next durable write hits the PK collision; the writer retries,
    // gives up, and dies. The watch channel closes, so this returns an
    // error rather than busy-waiting forever.
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        store.create_flow(&mk_flow(&qid, "f1", 0), &[], &HashMap::new()),
    )
    .await
    .expect("emit_durable must not hang when the writer dies");
    let err = result.expect_err("durable write must fail when the writer dies");
    assert!(matches!(err, StorageError::Internal(_)), "{err:?}");

    // health_check reports the dead writer...
    assert!(store.health_check().await.is_err());
    // ...and every further mutation fails fast instead of silently
    // mutating memory and losing the write.
    let err = store
        .create_flow(&mk_flow(&qid, "f2", 0), &[], &HashMap::new())
        .await
        .expect_err("mutations must fail fast once the journal is dead");
    assert!(matches!(err, StorageError::Internal(_)), "{err:?}");
    let err = store
        .create_queue(&mk_queue("q2"))
        .await
        .expect_err("mutations must fail fast once the journal is dead");
    assert!(matches!(err, StorageError::Internal(_)), "{err:?}");
}

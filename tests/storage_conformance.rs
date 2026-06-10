//! Storage backend conformance suite.
//!
//! Runs the same scenario against every Storage backend to ensure they all
//! implement the trait contract identically (issue #7): queue CRUD, flow and
//! task lifecycle, dependency resolution, batch operations (including the
//! skipped-cancelled-task contract of complete_tasks_with_ready_batch),
//! fetch queries, task injection, queue-deletion cascade, and schedules.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{Duration, Utc};
use tasked::store::memory::MemoryStorage;
use tasked::store::{Storage, StorageError};
use tasked::types::*;

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

fn mk_schedule(queue_id: &QueueId, id: &str) -> Schedule {
    let now = Utc::now();
    Schedule {
        id: ScheduleId::from(id),
        queue_id: queue_id.clone(),
        name: Some("nightly".into()),
        cron: "0 0 * * *".into(),
        flow_def: FlowDef::default(),
        enabled: true,
        last_triggered_at: None,
        next_run_at: Some(now - Duration::seconds(10)),
        created_at: now,
        updated_at: now,
    }
}

async fn run_suite(store: Arc<dyn Storage>) {
    let qid = QueueId::from("conformance-q");

    // ---- Queue CRUD ----
    store.create_queue(&mk_queue("conformance-q")).await.unwrap();
    let err = store
        .create_queue(&mk_queue("conformance-q"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::QueueAlreadyExists(_)),
        "duplicate create_queue: {err}"
    );
    assert_eq!(store.get_queue(&qid).await.unwrap().unwrap().id, qid);
    assert!(
        store
            .list_queues()
            .await
            .unwrap()
            .iter()
            .any(|q| q.id == qid)
    );

    // ---- create_flow + get_flow_with_tasks + dependencies ----
    let f1 = FlowId::from("flow-1");
    let t_a = mk_task(&qid, &f1, "a", TaskState::Ready);
    let t_b = mk_task(&qid, &f1, "b", TaskState::Pending);
    let t_c = mk_task(&qid, &f1, "c", TaskState::Pending);
    let mut deps: HashMap<TaskId, Vec<TaskId>> = HashMap::new();
    deps.insert(TaskId::from("b"), vec![TaskId::from("a")]);
    deps.insert(TaskId::from("c"), vec![TaskId::from("a"), TaskId::from("b")]);
    store
        .create_flow(&mk_flow(&qid, "flow-1", 3), &[t_a, t_b, t_c], &deps)
        .await
        .unwrap();

    assert_eq!(store.get_flow(&f1).await.unwrap().unwrap().task_count, 3);
    assert!(
        store
            .get_flow(&FlowId::from("missing"))
            .await
            .unwrap()
            .is_none()
    );

    let (gf, gtasks) = store.get_flow_with_tasks(&f1).await.unwrap().unwrap();
    assert_eq!(gf.id, f1);
    assert_eq!(gtasks.len(), 3);

    assert_eq!(store.get_flow_dependencies(&f1).await.unwrap().len(), 2);
    let mut c_deps = store
        .get_task_dependencies(&TaskId::from("c"), &f1)
        .await
        .unwrap();
    c_deps.sort_by(|x, y| x.as_str().cmp(y.as_str()));
    assert_eq!(c_deps, vec![TaskId::from("a"), TaskId::from("b")]);
    let a_dependents: HashSet<TaskId> = store
        .get_task_dependents(&TaskId::from("a"), &f1)
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert!(a_dependents.contains(&TaskId::from("b")));
    assert!(a_dependents.contains(&TaskId::from("c")));

    assert_eq!(store.list_flows(&qid, None).await.unwrap().len(), 1);
    assert_eq!(
        store
            .list_flows(&qid, Some(FlowState::Succeeded))
            .await
            .unwrap()
            .len(),
        0
    );

    // ---- fetch_ready_tasks ----
    let ready = store.fetch_ready_tasks(&qid, 10).await.unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].id, TaskId::from("a"));

    // ---- mark running + invalid transition errors ----
    store
        .mark_task_running(&TaskId::from("a"), &f1)
        .await
        .unwrap();
    let err = store
        .mark_task_running(&TaskId::from("a"), &f1)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::InvalidStateTransition(_, _)),
        "double mark_task_running: {err}"
    );
    let err = store
        .mark_task_running(&TaskId::from("nope"), &f1)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::TaskNotFound(_, _)),
        "mark_task_running on missing task: {err}"
    );
    let err = store
        .update_task_state(&TaskId::from("b"), &f1, TaskState::Running)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::InvalidStateTransition(_, _)),
        "pending->running: {err}"
    );

    // set_task_output does not change state
    store
        .set_task_output(&TaskId::from("a"), &f1, serde_json::json!({ "wip": true }))
        .await
        .unwrap();
    assert_eq!(
        store
            .get_task(&TaskId::from("a"), &f1)
            .await
            .unwrap()
            .unwrap()
            .state,
        TaskState::Running
    );

    // ---- complete_task_with_ready ----
    let flow_after = store
        .complete_task_with_ready(
            &TaskId::from("a"),
            &f1,
            Some(serde_json::json!({ "ok": 1 })),
            &[TaskId::from("b")],
        )
        .await
        .unwrap();
    assert_eq!(flow_after.tasks_succeeded, 1);
    let a = store
        .get_task(&TaskId::from("a"), &f1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.state, TaskState::Succeeded);
    assert_eq!(a.output, Some(serde_json::json!({ "ok": 1 })));
    assert_eq!(
        store
            .get_task(&TaskId::from("b"), &f1)
            .await
            .unwrap()
            .unwrap()
            .state,
        TaskState::Ready
    );
    let err = store
        .complete_task_with_ready(&TaskId::from("a"), &f1, None, &[])
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::InvalidStateTransition(_, _)),
        "double complete: {err}"
    );

    // ---- complete_task_success resolves dependents ----
    store
        .mark_task_running(&TaskId::from("b"), &f1)
        .await
        .unwrap();
    let (flow_after, newly_ready) = store
        .complete_task_success(&TaskId::from("b"), &f1, None)
        .await
        .unwrap();
    assert_eq!(flow_after.tasks_succeeded, 2);
    assert_eq!(newly_ready, vec![TaskId::from("c")]);
    assert_eq!(
        store
            .get_task(&TaskId::from("c"), &f1)
            .await
            .unwrap()
            .unwrap()
            .state,
        TaskState::Ready
    );

    // ---- delayed + fetch_delayed_tasks_due ----
    store
        .mark_task_running(&TaskId::from("c"), &f1)
        .await
        .unwrap();
    let retry_at = Utc::now() - Duration::seconds(1);
    store
        .mark_task_delayed(&TaskId::from("c"), &f1, retry_at)
        .await
        .unwrap();
    let c = store
        .get_task(&TaskId::from("c"), &f1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(c.state, TaskState::Delayed);
    assert_eq!(c.retries_remaining, 2);
    assert!(
        store
            .fetch_delayed_tasks_due()
            .await
            .unwrap()
            .iter()
            .any(|t| t.id == TaskId::from("c") && t.flow_id == f1)
    );
    let err = store
        .mark_task_delayed(&TaskId::from("c"), &f1, retry_at)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::InvalidStateTransition(_, _)),
        "delayed->delayed: {err}"
    );

    // delayed -> ready -> running -> failed
    store
        .update_task_state(&TaskId::from("c"), &f1, TaskState::Ready)
        .await
        .unwrap();
    store
        .mark_task_running(&TaskId::from("c"), &f1)
        .await
        .unwrap();
    store
        .mark_task_failed(&TaskId::from("c"), &f1, "boom")
        .await
        .unwrap();
    let c = store
        .get_task(&TaskId::from("c"), &f1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(c.state, TaskState::Failed);
    assert_eq!(c.error.as_deref(), Some("boom"));
    let flow_after = store.increment_flow_counter(&f1, false).await.unwrap();
    assert_eq!(flow_after.tasks_failed, 1);

    // ---- update_flow_state ----
    store
        .update_flow_state(&f1, FlowState::Failed)
        .await
        .unwrap();
    assert_eq!(
        store.get_flow(&f1).await.unwrap().unwrap().state,
        FlowState::Failed
    );
    let err = store
        .update_flow_state(&FlowId::from("missing"), FlowState::Failed)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::FlowNotFound(_)),
        "update_flow_state missing: {err}"
    );
    // Reset to Running so the retention sweep below does not collect flow-1.
    store
        .update_flow_state(&f1, FlowState::Running)
        .await
        .unwrap();

    // ---- fetch_timed_out_tasks ----
    let f2 = FlowId::from("flow-2");
    let mut t_t = mk_task(&qid, &f2, "t", TaskState::Ready);
    t_t.timeout_secs = 0;
    store
        .create_flow(&mk_flow(&qid, "flow-2", 1), &[t_t], &HashMap::new())
        .await
        .unwrap();
    store
        .mark_task_running(&TaskId::from("t"), &f2)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert!(
        store
            .fetch_timed_out_tasks()
            .await
            .unwrap()
            .iter()
            .any(|t| t.id == TaskId::from("t") && t.flow_id == f2),
        "running task past its timeout must be reported"
    );
    store
        .mark_task_failed(&TaskId::from("t"), &f2, "timed out")
        .await
        .unwrap();

    // ---- mark_tasks_running_batch ----
    let f3 = FlowId::from("flow-3");
    let r1 = mk_task(&qid, &f3, "r1", TaskState::Ready);
    let r2 = mk_task(&qid, &f3, "r2", TaskState::Ready);
    let p = mk_task(&qid, &f3, "p", TaskState::Pending);
    let mut deps3: HashMap<TaskId, Vec<TaskId>> = HashMap::new();
    deps3.insert(TaskId::from("p"), vec![TaskId::from("r1")]);
    store
        .create_flow(&mk_flow(&qid, "flow-3", 3), &[r1, r2, p], &deps3)
        .await
        .unwrap();

    let (id_r1, id_r2, id_p) = (TaskId::from("r1"), TaskId::from("r2"), TaskId::from("p"));
    let batch: Vec<(&TaskId, &FlowId)> = vec![(&id_r1, &f3), (&id_r2, &f3), (&id_p, &f3)];
    let started: HashSet<TaskId> = store
        .mark_tasks_running_batch(&batch)
        .await
        .unwrap()
        .into_iter()
        .map(|(t, _)| t)
        .collect();
    assert_eq!(started.len(), 2, "pending task must be skipped");
    assert!(started.contains(&id_r1) && started.contains(&id_r2));

    // ---- complete_tasks_with_ready_batch with a skipped cancelled task ----
    // Cancel p before the batch lands (regression for issue #7 item 1).
    store
        .update_task_state(&id_p, &f3, TaskState::Cancelled)
        .await
        .unwrap();

    let completions = vec![
        // p appears in newly_ready but is cancelled: it must NOT be resurrected.
        (
            id_r1.clone(),
            f3.clone(),
            Some(serde_json::json!({ "r": 1 })),
            vec![id_p.clone()],
        ),
        // p itself is cancelled: skipped, returns None, no counter increment.
        (id_p.clone(), f3.clone(), None, vec![]),
        (id_r2.clone(), f3.clone(), None, vec![]),
    ];
    let results = store
        .complete_tasks_with_ready_batch(&completions)
        .await
        .unwrap();
    assert_eq!(results.len(), 3);
    assert!(results[0].is_some(), "r1 completed");
    assert!(
        results[1].is_none(),
        "skipped cancelled task must return None"
    );
    assert!(results[2].is_some(), "r2 completed");
    let f3_after = store.get_flow(&f3).await.unwrap().unwrap();
    assert_eq!(
        f3_after.tasks_succeeded, 2,
        "tasks_succeeded must not count skipped entries"
    );
    assert_eq!(
        store.get_task(&id_p, &f3).await.unwrap().unwrap().state,
        TaskState::Cancelled,
        "cancelled task must not be resurrected by newly_ready"
    );

    // ---- inject_tasks ----
    let inj = mk_task(&qid, &f3, "i1", TaskState::Pending);
    let mut inj_deps: HashMap<TaskId, Vec<TaskId>> = HashMap::new();
    inj_deps.insert(TaskId::from("i1"), vec![id_r1.clone()]);
    let f3_after = store.inject_tasks(&f3, &[inj], &inj_deps).await.unwrap();
    assert_eq!(f3_after.task_count, 4);
    assert_eq!(store.get_flow_tasks(&f3).await.unwrap().len(), 4);
    let err = store
        .inject_tasks(&FlowId::from("missing"), &[], &HashMap::new())
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::FlowNotFound(_)),
        "inject into missing flow: {err}"
    );

    // ---- resolve_ready_tasks ----
    let promoted = store.resolve_ready_tasks(&f3).await.unwrap();
    assert_eq!(promoted, vec![TaskId::from("i1")]);
    assert_eq!(
        store
            .get_task(&TaskId::from("i1"), &f3)
            .await
            .unwrap()
            .unwrap()
            .state,
        TaskState::Ready
    );

    // ---- child flows ----
    let f4 = FlowId::from("flow-4");
    let mut child = mk_flow(&qid, "flow-4", 0);
    child.parent_flow_id = Some(f3.clone());
    store
        .create_flow(&child, &[], &HashMap::new())
        .await
        .unwrap();
    assert_eq!(store.get_child_flow_ids(&f3).await.unwrap(), vec![f4.clone()]);

    // ---- delete_terminal_flows_before ----
    store
        .update_flow_state(&f4, FlowState::Succeeded)
        .await
        .unwrap();
    let deleted = store
        .delete_terminal_flows_before(&qid, Utc::now() + Duration::hours(1))
        .await
        .unwrap();
    assert_eq!(deleted, 1, "only the terminal flow is collected");
    assert!(store.get_flow(&f4).await.unwrap().is_none());
    assert!(store.get_flow(&f3).await.unwrap().is_some());

    // ---- schedules ----
    let sid = ScheduleId::from("sched-1");
    let sched = mk_schedule(&qid, "sched-1");
    store.create_schedule(&sched).await.unwrap();
    assert!(store.get_schedule(&sid).await.unwrap().is_some());
    assert_eq!(store.list_schedules(&qid).await.unwrap().len(), 1);
    assert!(
        store
            .fetch_due_schedules()
            .await
            .unwrap()
            .iter()
            .any(|s| s.id == sid),
        "schedule with past next_run_at is due"
    );
    store
        .mark_schedule_triggered(&sid, Utc::now(), Some(Utc::now() + Duration::hours(1)))
        .await
        .unwrap();
    let s = store.get_schedule(&sid).await.unwrap().unwrap();
    assert!(s.last_triggered_at.is_some());
    assert!(s.next_run_at.is_some());
    assert!(
        !store
            .fetch_due_schedules()
            .await
            .unwrap()
            .iter()
            .any(|s| s.id == sid),
        "schedule with future next_run_at is not due"
    );
    let mut renamed = s.clone();
    renamed.name = Some("renamed".into());
    store.update_schedule(&renamed).await.unwrap();
    assert_eq!(
        store
            .get_schedule(&sid)
            .await
            .unwrap()
            .unwrap()
            .name
            .as_deref(),
        Some("renamed")
    );
    let err = store
        .mark_schedule_triggered(&ScheduleId::from("missing"), Utc::now(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::ScheduleNotFound(_)),
        "trigger missing schedule: {err}"
    );
    store.delete_schedule(&sid).await.unwrap();
    assert!(store.get_schedule(&sid).await.unwrap().is_none());

    // Recreate a schedule so delete_queue below has something to cascade.
    store.create_schedule(&sched).await.unwrap();

    // ---- delete_queue cascades flows, tasks, deps, and schedules ----
    store.delete_queue(&qid).await.unwrap();
    assert!(store.get_queue(&qid).await.unwrap().is_none());
    assert!(
        store.get_flow(&f1).await.unwrap().is_none(),
        "delete_queue must cascade flows"
    );
    assert!(store.get_flow(&f2).await.unwrap().is_none());
    assert!(store.get_flow(&f3).await.unwrap().is_none());
    assert!(
        store.get_task(&id_r1, &f3).await.unwrap().is_none(),
        "delete_queue must cascade tasks"
    );
    assert!(
        store.get_schedule(&sid).await.unwrap().is_none(),
        "delete_queue must cascade schedules"
    );
    assert!(
        store.fetch_ready_tasks(&qid, 10).await.unwrap().is_empty(),
        "no ready tasks may survive queue deletion"
    );
}

#[tokio::test]
async fn conformance_memory() {
    run_suite(Arc::new(MemoryStorage::new())).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn conformance_sqlite_file() {
    use tasked::store::sqlite::SqliteStorage;
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteStorage::open(dir.path().join("storage.db")).unwrap();
    run_suite(Arc::new(store)).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn conformance_sqlite_in_memory() {
    use tasked::store::sqlite::SqliteStorage;
    run_suite(Arc::new(SqliteStorage::in_memory().unwrap())).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn conformance_sharded() {
    use tasked::store::sharded::ShardedStorage;
    let dir = tempfile::tempdir().unwrap();
    run_suite(Arc::new(ShardedStorage::open(dir.path()).unwrap())).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sharded_rejects_path_traversal_queue_ids() {
    use tasked::store::sharded::ShardedStorage;
    let dir = tempfile::tempdir().unwrap();
    let store = ShardedStorage::open(dir.path()).unwrap();
    for bad in ["../escape", "a/b", "a\\b", "..", "", "x/../../y"] {
        let err = store.create_queue(&mk_queue(bad)).await.unwrap_err();
        assert!(
            matches!(err, StorageError::Internal(_)),
            "queue id {bad:?} must be rejected, got: {err:?}"
        );
    }
    // A well-formed ID still works.
    store.create_queue(&mk_queue("ok-queue_1.x")).await.unwrap();
}

#[cfg(feature = "journaled")]
#[tokio::test]
async fn conformance_journaled_memory_only() {
    use tasked::store::journaled::JournaledStorage;
    run_suite(Arc::new(JournaledStorage::new())).await;
}

#[cfg(feature = "journaled")]
#[tokio::test]
async fn conformance_journaled_durable() {
    use tasked::store::journaled::JournaledStorage;
    use tasked::store::journaled::config::JournalConfig;
    let dir = tempfile::tempdir().unwrap();
    let config = JournalConfig {
        journal_path: Some(dir.path().join("journal.db")),
        ..JournalConfig::default()
    };
    run_suite(Arc::new(JournaledStorage::open(config).unwrap())).await;
}

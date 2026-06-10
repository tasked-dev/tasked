//! Regression tests for flow finalization (issue #3).
//!
//! A flow with a failed branch only becomes fully terminal when its *last*
//! still-running branch finishes — which may be a success. The engine must
//! re-check terminality on every completion, not just at failure time.

use std::sync::Arc;
use tasked::engine::Engine;
use tasked::executor::CallbackExecutor;
use tasked::prelude::*;
use tasked::store::memory::MemoryStorage;

fn two_root_flow() -> FlowDef {
    FlowDef {
        tasks: vec![
            TaskDef {
                id: TaskId::from("a"),
                executor: "test".into(),
                retries: Some(0),
                ..Default::default()
            },
            TaskDef {
                id: TaskId::from("b"),
                executor: "test".into(),
                retries: Some(0),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

async fn engine_with_flow() -> (Arc<Engine>, Flow) {
    let engine = Arc::new(
        Engine::builder(Arc::new(MemoryStorage::new()))
            .executor("test", Arc::new(CallbackExecutor::always_succeed()))
            .build(),
    );
    let queue_id = QueueId::from("q");
    engine
        .create_queue(&queue_id, QueueConfig::default())
        .await
        .unwrap();
    let flow = engine
        .submit_flow(&queue_id, two_root_flow())
        .await
        .unwrap();
    (engine, flow)
}

/// Regression: task `a` fails while `b` is still running; when `b` later
/// succeeds the flow must transition to Failed instead of staying Running
/// forever.
#[tokio::test]
async fn failed_branch_then_late_success_finalizes_flow_as_failed() {
    let (engine, flow) = engine_with_flow().await;

    let task_a = engine
        .get_task(&TaskId::from("a"), &flow.id)
        .await
        .unwrap()
        .unwrap();
    let task_b = engine
        .get_task(&TaskId::from("b"), &flow.id)
        .await
        .unwrap()
        .unwrap();

    // a fails terminally while b is still not terminal
    engine
        .handle_task_result(
            &task_a,
            ExecuteResult::Failed {
                error: "boom".into(),
                retryable: false,
            },
        )
        .await
        .unwrap();

    let mid = engine.get_flow(&flow.id).await.unwrap().unwrap();
    assert_eq!(
        mid.state,
        FlowState::Running,
        "flow must stay Running while b is unfinished"
    );

    // b succeeds afterwards — this is the last branch to finish
    engine
        .handle_task_result(&task_b, ExecuteResult::Success { output: None })
        .await
        .unwrap();

    let done = engine.get_flow(&flow.id).await.unwrap().unwrap();
    assert_eq!(
        done.state,
        FlowState::Failed,
        "flow with a failed branch must finalize as Failed once all tasks are terminal"
    );
}

#[tokio::test]
async fn all_tasks_succeed_finalizes_flow_as_succeeded() {
    let (engine, flow) = engine_with_flow().await;

    for id in ["a", "b"] {
        let task = engine
            .get_task(&TaskId::from(id), &flow.id)
            .await
            .unwrap()
            .unwrap();
        engine
            .handle_task_result(&task, ExecuteResult::Success { output: None })
            .await
            .unwrap();
    }

    let done = engine.get_flow(&flow.id).await.unwrap().unwrap();
    assert_eq!(done.state, FlowState::Succeeded);
}

/// Regression (issue #4): driving the engine with the public process_cycle
/// must persist completions and finish flows. Completions used to be flushed
/// only by run()'s queue workers, so process_cycle executed tasks whose
/// successes were never written — dependents never became ready.
#[tokio::test]
async fn process_cycle_persists_completions_and_finishes_flows() {
    let engine = Arc::new(
        Engine::builder(Arc::new(MemoryStorage::new()))
            .executor("test", Arc::new(CallbackExecutor::always_succeed()))
            .build(),
    );
    let queue_id = QueueId::from("q");
    engine
        .create_queue(&queue_id, QueueConfig::default())
        .await
        .unwrap();
    // a -> b dependency chain: b only runs if a's success is persisted.
    let flow = engine
        .submit_flow(
            &queue_id,
            FlowDef {
                tasks: vec![
                    TaskDef {
                        id: TaskId::from("a"),
                        executor: "test".into(),
                        ..Default::default()
                    },
                    TaskDef {
                        id: TaskId::from("b"),
                        executor: "test".into(),
                        depends_on: vec![TaskId::from("a")],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    for _ in 0..50 {
        engine.process_cycle().await.unwrap();
        if engine
            .get_flow(&flow.id)
            .await
            .unwrap()
            .unwrap()
            .state
            .is_terminal()
        {
            break;
        }
        // Dispatched executors run as spawned tasks; give them a beat to finish.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let done = engine.get_flow(&flow.id).await.unwrap().unwrap();
    assert_eq!(done.state, FlowState::Succeeded);
}

/// End-to-end via process_cycle_sync: one branch fails, the other succeeds —
/// regardless of dispatch order, the flow must reach Failed.
#[tokio::test]
async fn mixed_results_via_process_cycle_reach_failed() {
    let engine = Arc::new(
        Engine::builder(Arc::new(MemoryStorage::new()))
            .executor("ok", Arc::new(CallbackExecutor::always_succeed()))
            .executor(
                "fail",
                Arc::new(CallbackExecutor::always_fail_permanent("boom")),
            )
            .build(),
    );
    let queue_id = QueueId::from("q");
    engine
        .create_queue(&queue_id, QueueConfig::default())
        .await
        .unwrap();
    let flow = engine
        .submit_flow(
            &queue_id,
            FlowDef {
                tasks: vec![
                    TaskDef {
                        id: TaskId::from("good"),
                        executor: "ok".into(),
                        retries: Some(0),
                        ..Default::default()
                    },
                    TaskDef {
                        id: TaskId::from("bad"),
                        executor: "fail".into(),
                        retries: Some(0),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    for _ in 0..10 {
        engine.process_cycle_sync().await.unwrap();
        if engine
            .get_flow(&flow.id)
            .await
            .unwrap()
            .unwrap()
            .state
            .is_terminal()
        {
            break;
        }
    }

    let done = engine.get_flow(&flow.id).await.unwrap().unwrap();
    assert_eq!(done.state, FlowState::Failed);
}

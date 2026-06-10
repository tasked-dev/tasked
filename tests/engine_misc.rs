//! Tests for assorted engine correctness fixes (issue #10).

use std::sync::Arc;
use tasked::engine::{Engine, EngineError};
use tasked::executor::CallbackExecutor;
use tasked::prelude::*;
use tasked::store::StorageError;
use tasked::store::memory::MemoryStorage;

fn one_task_flow(executor: &str) -> FlowDef {
    FlowDef {
        tasks: vec![TaskDef {
            id: TaskId::from("t"),
            executor: executor.into(),
            retries: Some(0),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Cancelling an already-finished flow must not rewrite its terminal state.
#[tokio::test]
async fn cancel_flow_does_not_overwrite_terminal_state() {
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
        .submit_flow(&queue_id, one_task_flow("test"))
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
    assert_eq!(
        engine.get_flow(&flow.id).await.unwrap().unwrap().state,
        FlowState::Succeeded
    );

    engine.cancel_flow(&flow.id).await.unwrap();
    assert_eq!(
        engine.get_flow(&flow.id).await.unwrap().unwrap().state,
        FlowState::Succeeded,
        "cancel must not rewrite a terminal flow state"
    );
}

#[tokio::test]
async fn cancel_flow_errors_on_unknown_flow() {
    let engine = Engine::builder(Arc::new(MemoryStorage::new())).build();
    let result = engine.cancel_flow(&FlowId::from("nope")).await;
    assert!(matches!(
        result,
        Err(EngineError::Storage(StorageError::FlowNotFound(_)))
    ));
}

/// A ready task whose executor is no longer registered (e.g. after a restart
/// with different features) must fail the task — not wedge the whole queue.
#[tokio::test]
async fn unregistered_executor_fails_task_instead_of_wedging_queue() {
    let store = Arc::new(MemoryStorage::new());

    // First engine registers the executor and submits the flow.
    let submitter = Arc::new(
        Engine::builder(store.clone())
            .executor("gone", Arc::new(CallbackExecutor::always_succeed()))
            .executor("ok", Arc::new(CallbackExecutor::always_succeed()))
            .build(),
    );
    let queue_id = QueueId::from("q");
    submitter
        .create_queue(&queue_id, QueueConfig::default())
        .await
        .unwrap();
    let stuck = submitter
        .submit_flow(&queue_id, one_task_flow("gone"))
        .await
        .unwrap();
    let healthy = submitter
        .submit_flow(&queue_id, one_task_flow("ok"))
        .await
        .unwrap();

    // Second engine (same storage) lacks the "gone" executor.
    let restarted = Arc::new(
        Engine::builder(store)
            .executor("ok", Arc::new(CallbackExecutor::always_succeed()))
            .build(),
    );
    for _ in 0..10 {
        restarted.process_cycle_sync().await.unwrap();
    }

    let stuck_flow = restarted.get_flow(&stuck.id).await.unwrap().unwrap();
    assert_eq!(
        stuck_flow.state,
        FlowState::Failed,
        "task with unregistered executor must fail its flow"
    );
    let healthy_flow = restarted.get_flow(&healthy.id).await.unwrap().unwrap();
    assert_eq!(
        healthy_flow.state,
        FlowState::Succeeded,
        "other tasks in the queue must still dispatch"
    );
}

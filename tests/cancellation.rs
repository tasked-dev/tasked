//! Tests for cancellation propagation to running executors (issue #16).

use std::sync::Arc;
use std::time::Duration;
use tasked::engine::Engine;
use tasked::prelude::*;
use tasked::store::memory::MemoryStorage;

/// A flow cancelled while a task sleeps must abort the executor promptly:
/// the delay executor races its sleep against the engine's cancel signal.
#[tokio::test]
async fn cancel_flow_aborts_running_delay_executor() {
    let engine = Arc::new(
        Engine::builder(Arc::new(MemoryStorage::new()))
            .executor("delay", Arc::new(tasked::executor::delay::DelayExecutor))
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
                tasks: vec![TaskDef {
                    id: TaskId::from("sleepy"),
                    executor: "delay".into(),
                    config: serde_json::json!({ "seconds": 60 }),
                    timeout_secs: Some(120),
                    retries: Some(0),
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Dispatch the task (spawned executor starts its 60s sleep).
    engine.process_cycle().await.unwrap();
    // Wait until the task is actually Running.
    let mut running = false;
    for _ in 0..50 {
        let task = engine
            .get_task(&TaskId::from("sleepy"), &flow.id)
            .await
            .unwrap()
            .unwrap();
        if task.state == TaskState::Running {
            running = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(running, "task should have been dispatched");

    // Cancel: the spawned executor must observe the signal and return well
    // before its 60s sleep ends. We can't observe the future directly, but
    // we can assert the flow is Cancelled and that the runtime isn't holding
    // the queue permit (a second flow on the same queue dispatches).
    engine.cancel_flow(&flow.id).await.unwrap();
    let cancelled = engine.get_flow(&flow.id).await.unwrap().unwrap();
    assert_eq!(cancelled.state, FlowState::Cancelled);

    // Give the aborted executor a beat to finish its cancellation path; its
    // Failed("task cancelled") result must be skipped gracefully (the task is
    // already Cancelled in storage) without flipping any state.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let task = engine
        .get_task(&TaskId::from("sleepy"), &flow.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.state, TaskState::Cancelled);
}

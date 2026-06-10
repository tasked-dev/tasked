//! Regression tests for panics reachable from untrusted config (issue #8).

use std::sync::Arc;
use tasked::engine::{Engine, EngineError};
use tasked::executor::delay::DelayExecutor;
use tasked::executor::{ExecutionContext, Executor};
use tasked::prelude::*;
use tasked::store::memory::MemoryStorage;

fn engine() -> Engine {
    Engine::builder(Arc::new(MemoryStorage::new())).build()
}

fn queue_config_with_rate(max_burst: u64, per_second: f64) -> QueueConfig {
    QueueConfig {
        rate_limit: Some(RateLimitConfig {
            max_burst,
            per_second,
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn create_queue_rejects_invalid_rate_limits() {
    let engine = engine();
    for (burst, per_second) in [
        (0, 1.0),
        (1, 0.0),
        (1, -5.0),
        (1, f64::NAN),
        (1, f64::INFINITY),
        (1, 1e12), // > 1 token/ns would make nanos_per_token zero (div-by-zero)
    ] {
        let result = engine
            .create_queue(
                &QueueId::from("q"),
                queue_config_with_rate(burst, per_second),
            )
            .await;
        assert!(
            matches!(result, Err(EngineError::InvalidQueueConfig(_))),
            "expected rejection for max_burst={burst}, per_second={per_second}, got {result:?}"
        );
    }
}

#[tokio::test]
async fn create_queue_rejects_zero_concurrency() {
    let result = engine()
        .create_queue(
            &QueueId::from("q"),
            QueueConfig {
                concurrency: 0,
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(result, Err(EngineError::InvalidQueueConfig(_))));
}

#[tokio::test]
async fn create_queue_accepts_valid_rate_limit() {
    let result = engine()
        .create_queue(&QueueId::from("q"), queue_config_with_rate(10, 5.0))
        .await;
    assert!(result.is_ok());
}

/// Regression: Duration::from_secs_f64 panics on huge values; the panic
/// happened inside the spawned executor task, stranding the task in Running.
#[tokio::test]
async fn delay_executor_rejects_out_of_range_seconds_without_panicking() {
    let store = Arc::new(MemoryStorage::new());
    let task = Task {
        id: TaskId::from("d"),
        flow_id: FlowId::new(),
        queue_id: QueueId::from("q"),
        state: TaskState::Running,
        executor_type: "delay".into(),
        executor_config: serde_json::json!({ "seconds": 1e300 }),
        input: None,
        output: None,
        error: None,
        retries_remaining: 0,
        backoff: BackoffStrategy::default(),
        timeout_secs: 300,
        condition: None,
        retry_at: None,
        started_at: None,
        completed_at: None,
        created_at: chrono::Utc::now(),
    };
    let ctx = ExecutionContext::new(store, task.id.clone(), task.flow_id.clone());
    let result = DelayExecutor.execute(&task, &ctx).await;
    match result {
        ExecuteResult::Failed { retryable, .. } => assert!(!retryable),
        other => panic!("expected Failed, got {other:?}"),
    }
}

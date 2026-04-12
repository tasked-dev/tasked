//! Minimal example: embed the tasked engine in a Rust application.
//!
//! Run with: cargo run -p tasked --example embed

use std::sync::Arc;
use tasked::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create an in-memory storage backend (no disk, no SQLite)
    let store = Arc::new(MemoryStorage::new());

    // 2. Build the engine with a custom callback executor
    let engine = Engine::builder(store)
        .executor(
            "greet",
            Arc::new(CallbackExecutor::new(|task| {
                let name = task
                    .executor_config
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("world")
                    .to_string();
                async move {
                    println!("Hello, {name}!");
                    ExecuteResult::Success {
                        output: Some(serde_json::json!({ "greeting": format!("Hello, {name}!") })),
                    }
                }
            })),
        )
        .executor("noop", Arc::new(NoopExecutor))
        .build();
    let engine = Arc::new(engine);

    // 3. Create a queue
    engine
        .create_queue(&QueueId::from("default"), QueueConfig::default())
        .await?;

    // 4. Submit a flow with two tasks (second depends on first)
    let flow = engine
        .submit_flow(
            &QueueId::from("default"),
            FlowDef {
                tasks: vec![
                    TaskDef {
                        id: TaskId::from("say-hello"),
                        executor: "greet".into(),
                        config: serde_json::json!({ "name": "Tasked" }),
                        depends_on: vec![],
                        ..default_task_def()
                    },
                    TaskDef {
                        id: TaskId::from("done"),
                        executor: "noop".into(),
                        config: serde_json::json!({}),
                        depends_on: vec![TaskId::from("say-hello")],
                        ..default_task_def()
                    },
                ],
                ..FlowDef::default()
            },
        )
        .await?;

    println!("Submitted flow: {}", flow.id);

    // 5. Run processing cycles until the flow completes
    loop {
        engine.process_cycle_sync().await?;
        let f = engine.get_flow(&flow.id).await?.unwrap();
        if f.state.is_terminal() {
            println!("Flow finished: {}", f.state);
            break;
        }
    }

    // 6. Print task results
    let tasks = engine.get_flow_tasks(&flow.id).await?;
    for task in tasks {
        println!("  {} [{}]: {:?}", task.id, task.state, task.output);
    }

    Ok(())
}

fn default_task_def() -> TaskDef {
    TaskDef {
        id: TaskId::from(""),
        executor: String::new(),
        config: serde_json::json!({}),
        input: None,
        depends_on: vec![],
        timeout_secs: None,
        retries: None,
        backoff: None,
        condition: None,
        spawn_output: vec![],
    }
}


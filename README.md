[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

# tasked

Embeddable DAG task execution engine with durable SQLite storage.

`tasked` is the core library crate for the Tasked project. It provides the engine, storage backends, executor traits, and DAG scheduling logic that power both the standalone server and library-embedded use cases.

> **Pre-release:** This crate is under active development. APIs may change without notice until 1.0.0.

## Installation

Add to your `Cargo.toml`:

```toml
# Full feature set (SQLite + shell + HTTP executors + scripting)
tasked = "0.0.1"

# Minimal — in-memory storage, callback executors only
tasked = { version = "0.0.1", default-features = false }

# Pick features as needed
tasked = { version = "0.0.1", default-features = false, features = ["sqlite", "shell"] }
```

### Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `sqlite` | yes | Durable SQLite WAL storage backend |
| `http` | yes | HTTP executor for making API calls |
| `shell` | yes | Shell command executor |
| `scripting` | yes | Rhai scripting for task conditions |
| `docker` | no | Docker/OCI container executor |

## Quick start

```rust,no_run
use tasked::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::builder(Arc::new(MemoryStorage::new()))
        .executor("my-task", Arc::new(CallbackExecutor::new(|task| {
            let input = task.executor_config["value"].as_i64().unwrap_or(0);
            async move {
                ExecuteResult::Success {
                    output: Some(serde_json::json!({ "result": input * 2 })),
                }
            }
        })))
        .build();
    let engine = Arc::new(engine);

    engine.create_queue(&QueueId::from("default"), QueueConfig::default()).await?;
    let flow = engine.submit_flow(&QueueId::from("default"), FlowDef {
        tasks: vec![TaskDef {
            id: TaskId::from("double"),
            executor: "my-task".into(),
            config: serde_json::json!({ "value": 21 }),
            ..Default::default()
        }],
        ..Default::default()
    }).await?;

    loop {
        engine.process_cycle_sync().await?;
        if engine.get_flow(&flow.id).await?.unwrap().state.is_terminal() {
            break;
        }
    }
    Ok(())
}
```

See `examples/embed.rs` for a full working example.

## Documentation

- [tasked.dev](https://tasked.dev) — project homepage
- [docs.rs/tasked](https://docs.rs/tasked) — API reference

## License

MIT

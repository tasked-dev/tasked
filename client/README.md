[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](../LICENSE)

# tasked-client

Rust HTTP client for the Tasked server API.

`tasked-client` provides a typed async client for interacting with a running `tasked-server` instance. It uses `tasked-types` for shared type definitions so request and response types match the server exactly.

## Installation

```toml
[dependencies]
tasked-client = "0.1"
```

## Quick start

```rust,no_run
use tasked_client::TaskedClient;
use tasked_types::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = TaskedClient::builder("http://localhost:8080")
        .bearer_token("my-secret-token")
        .build()?;

    // Create a queue
    let queue = client.create_queue("builds", QueueConfig::default()).await?;

    // Submit a flow
    let flow = client.submit_flow("builds", FlowDef {
        tasks: vec![TaskDef {
            id: TaskId::from("build"),
            executor: "shell".into(),
            config: serde_json::json!({ "command": "cargo build --release" }),
            ..Default::default()
        }],
        ..Default::default()
    }).await?;

    // Poll for completion
    let detail = client.get_flow(&flow.id).await?;
    println!("Flow state: {:?}", detail.state);
    Ok(())
}
```

## Documentation

Full documentation, architecture details, and API reference are in the [main Tasked README](https://github.com/tasked-dev/tasked).

## License

MIT

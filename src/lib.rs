#![deny(unsafe_code)]

//! # Tasked
//!
//! An embeddable DAG task execution engine for Rust applications.
//!
//! Tasked lets you define workflows as directed acyclic graphs (DAGs) of tasks,
//! then execute them with configurable concurrency, retries, timeouts, and
//! dependency resolution. It can run as a standalone server or be embedded
//! directly into your application.
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use tasked::prelude::*;
//! use std::sync::Arc;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create an engine with in-memory storage
//! let engine = Engine::builder(Arc::new(MemoryStorage::new()))
//!     .executor("greet", Arc::new(CallbackExecutor::new(|task| {
//!         let name = task.executor_config["name"].as_str().unwrap_or("world").to_string();
//!         async move {
//!             ExecuteResult::Success {
//!                 output: Some(serde_json::json!({ "message": format!("Hello, {name}!") })),
//!             }
//!         }
//!     })))
//!     .build();
//! let engine = Arc::new(engine);
//!
//! // Create a queue and submit a flow
//! engine.create_queue(&QueueId::from("default"), QueueConfig::default()).await?;
//! let flow = engine.submit_flow(&QueueId::from("default"), FlowDef {
//!     tasks: vec![TaskDef {
//!         id: TaskId::from("hello"),
//!         executor: "greet".into(),
//!         config: serde_json::json!({ "name": "Tasked" }),
//!         ..Default::default()
//!     }],
//!     ..Default::default()
//! }).await?;
//!
//! // Process until complete
//! loop {
//!     engine.process_cycle_sync().await?;
//!     if engine.get_flow(&flow.id).await?.unwrap().state.is_terminal() {
//!         break;
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Embedded mode
//!
//! Tasked can run entirely inside your application with no external dependencies —
//! no SQLite, no HTTP server, no disk I/O. Use `MemoryStorage` for ephemeral
//! in-process workflows, and `CallbackExecutor` to run Rust functions as tasks:
//!
//! ```toml
//! # Cargo.toml — minimal dependency, no SQLite/HTTP/shell
//! tasked = { version = "0.0.3", default-features = false }
//! ```
//!
//! ### Rust functions as executors
//!
//! [`CallbackExecutor`](executor::CallbackExecutor) wraps an async closure so you
//! can run arbitrary Rust code as a task. Extract any data from the [`Task`](types::Task)
//! reference *before* the async block to avoid lifetime issues:
//!
//! ```rust,no_run
//! # use tasked::prelude::*;
//! # use std::sync::Arc;
//! let exec = CallbackExecutor::new(|task| {
//!     let url = task.executor_config["url"].as_str().unwrap_or("").to_string();
//!     async move {
//!         // Any async Rust code — HTTP calls, DB queries, file I/O, etc.
//!         ExecuteResult::Success { output: Some(serde_json::json!({ "fetched": url })) }
//!     }
//! });
//! ```
//!
//! For more control, implement the [`Executor`](executor::Executor) trait directly:
//!
//! ```rust,no_run
//! # use tasked::prelude::*;
//! # use async_trait::async_trait;
//! struct MyExecutor;
//!
//! #[async_trait]
//! impl Executor for MyExecutor {
//!     async fn execute(&self, task: &Task, ctx: &ExecutionContext) -> ExecuteResult {
//!         // Full access to task config, execution context, and artifact store
//!         ExecuteResult::Success { output: None }
//!     }
//! }
//! ```
//!
//! ### Processing modes
//!
//! - [`engine.run()`](engine::Engine::run) — spawns a background tokio loop that
//!   continuously dispatches tasks with full concurrency. Use this when embedding
//!   the engine alongside other async work.
//! - [`engine.process_cycle_sync()`](engine::Engine::process_cycle_sync) — runs
//!   one dispatch cycle inline, executing tasks sequentially. Use this for
//!   deterministic control in tests or batch scripts.
//!
//! ### Custom storage
//!
//! The [`Storage`](store::Storage) trait is open for custom backends. Implement it
//! to persist tasks in Postgres, DynamoDB, or any other store.
//!
//! See `examples/embed.rs` for a complete working example.
//!
//! ## Feature flags
//!
//! All features are enabled by default. Disable defaults for a minimal core
//! (`MemoryStorage` + `CallbackExecutor` only):
//!
//! ```toml
//! tasked = { version = "0.0.3", default-features = false }
//! ```
//!
//! | Feature      | Default | Description |
//! |-------------|---------|-------------|
//! | `sqlite`    | yes     | `SqliteStorage` and `ShardedStorage` backends |
//! | `http`      | yes     | `HttpExecutor` and webhook delivery |
//! | `shell`     | yes     | `ShellExecutor` for running shell commands |
//! | `scripting` | yes     | Condition expressions via Rhai |
//! | `docker`    | no      | `ContainerExecutor` and `AgentExecutor` |
//!
//! ## Architecture
//!
//! - [`Engine`](engine::Engine) — the core orchestrator that manages queues, flows, and task dispatch
//! - [`Storage`](store::Storage) — trait for pluggable persistence backends
//! - [`Executor`](executor::Executor) — trait for implementing task execution strategies
//! - [`ArtifactStore`](artifacts::ArtifactStore) — trait for sharing files between tasks
//!
//! See the [`prelude`] module for convenient imports.

pub mod artifacts;
#[cfg(feature = "scripting")]
pub mod condition;
pub mod engine;
pub mod executor;
pub mod graph;
pub mod interpolate;
pub mod perf;
pub mod prelude;
pub mod rate_limit;
pub mod schedule;
pub mod store;
pub mod types;
#[cfg(feature = "http")]
pub mod url_policy;
pub mod webhook;

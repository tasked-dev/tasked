//! Convenience re-exports for common types.
//!
//! ```rust
//! use tasked::prelude::*;
//! ```

// Engine
pub use crate::engine::{Engine, EngineBuilder, EngineConfig, EngineError};

// Executor traits and helpers
pub use crate::executor::{CallbackExecutor, ExecutionContext, Executor, NoopExecutor};

// Storage
pub use crate::store::memory::MemoryStorage;
pub use crate::store::{Storage, StorageError};

// Core types
pub use crate::types::{
    BackoffStrategy, ExecuteResult, Flow, FlowDef, FlowId, FlowState, FlowWebhooks, Queue,
    QueueConfig, QueueId, RateLimitConfig, Schedule, ScheduleDef, ScheduleId, Task, TaskDef,
    TaskId, TaskState,
};

// Artifacts
pub use crate::artifacts::{ArtifactError, ArtifactStore, LocalArtifactStore};

// Optional re-exports
#[cfg(feature = "docker")]
pub use crate::executor::agent::AgentExecutor;
#[cfg(feature = "docker")]
pub use crate::executor::container::ContainerExecutor;
#[cfg(feature = "http")]
pub use crate::executor::http::HttpExecutor;
#[cfg(feature = "shell")]
pub use crate::executor::shell::ShellExecutor;
#[cfg(feature = "sqlite")]
pub use crate::store::sharded::ShardedStorage;
#[cfg(feature = "sqlite")]
pub use crate::store::sqlite::SqliteStorage;

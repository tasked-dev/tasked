//! Shared startup wiring: storage backend selection and executor
//! registration, used by both the HTTP server and the MCP server.

use std::sync::Arc;
use tasked::{
    engine::Engine,
    executor::{
        CallbackExecutor, NoopExecutor,
        api::{self, InlineApiExecutor},
        approval::ApprovalExecutor,
        delay::DelayExecutor,
        http::HttpExecutor,
        remote::RemoteExecutor,
        shell::ShellExecutor,
        spawn::SpawnExecutor,
        trigger::TriggerExecutor,
    },
    store::sharded::ShardedStorage,
};
use tracing::info;

/// Open the storage backend selected by the `--engine` flag ("sqlite" or,
/// with the `journaled` feature, "journal"). Exits the process on an
/// unknown engine mode.
pub(crate) fn open_storage(data_dir: &str, engine_mode: &str) -> Arc<dyn tasked::store::Storage> {
    match engine_mode {
        "sqlite" => {
            let storage = ShardedStorage::open(data_dir).expect("failed to open data directory");
            Arc::new(storage)
        }
        #[cfg(feature = "journaled")]
        "journal" => {
            let data_path = std::path::PathBuf::from(data_dir);
            std::fs::create_dir_all(&data_path).expect("failed to create data directory");
            let config = tasked::store::journaled::config::JournalConfig {
                journal_path: Some(data_path.join("journal.db")),
                snapshot_path: Some(data_path.join("snapshot.db")),
                ..Default::default()
            };
            let storage = tasked::store::journaled::JournaledStorage::open(config)
                .expect("failed to open journaled storage");
            info!(engine = "journal", data_dir = %data_dir, "using journaled in-memory engine");
            Arc::new(storage)
        }
        other => {
            eprintln!("unknown engine mode: {other} (valid: sqlite, journal)");
            std::process::exit(1);
        }
    }
}

/// Which set of executors to register.
///
/// The two sets differ: the MCP server registers a smaller set (no `api`,
/// no Docker `container`/`agent`, no integrations). The difference is
/// inherited from the original per-binary registration code and is
/// intentional — do not "harmonize" the sets without considering what an
/// MCP client would gain access to.
pub(crate) enum ExecutorProfile<'a> {
    /// Full HTTP-server set, including `api`, Docker-backed
    /// `container`/`agent` (when Docker is available), and integration
    /// executors loaded from a directory.
    Server {
        integrations_dir: Option<&'a str>,
        token_cache_path: Option<&'a str>,
    },
    /// MCP stdio set: executors that work without special configuration.
    Mcp,
}

pub(crate) fn register_executors(engine: &mut Engine, profile: ExecutorProfile) {
    match profile {
        ExecutorProfile::Server {
            integrations_dir,
            token_cache_path,
        } => register_server_executors(engine, integrations_dir, token_cache_path),
        ExecutorProfile::Mcp => register_mcp_executors(engine),
    }
}

fn register_server_executors(
    engine: &mut Engine,
    integrations_dir: Option<&str>,
    token_cache_path: Option<&str>,
) {
    // Orchestration executors — always run locally (no user code).
    engine.register_executor("http", Arc::new(HttpExecutor::new()));
    engine.register_executor("noop", Arc::new(NoopExecutor));
    engine.register_executor("callback", Arc::new(CallbackExecutor::always_succeed()));
    engine.register_executor("delay", Arc::new(DelayExecutor));
    engine.register_executor("approval", Arc::new(ApprovalExecutor));
    engine.register_executor("remote", Arc::new(RemoteExecutor::new()));
    engine.register_executor("api", Arc::new(InlineApiExecutor::new()));

    // Local mode: shell runs locally, container uses Docker.
    engine.register_executor("shell", Arc::new(ShellExecutor));

    {
        use tasked::executor::agent::AgentExecutor;
        use tasked::executor::container::{ContainerExecutor, docker::DockerBackend};

        if let Ok(backend) = DockerBackend::new() {
            tracing::info!("using Docker container backend");
            engine.register_executor("container", Arc::new(ContainerExecutor::new(backend)));

            if let Ok(agent_backend) = DockerBackend::new() {
                engine.register_executor(
                    "agent",
                    Arc::new(AgentExecutor::new(ContainerExecutor::new(agent_backend))),
                );
            }
        }
    }

    // Trigger executor: submits child flows and optionally waits for completion.
    engine.register_executor("trigger", Arc::new(TriggerExecutor));

    // Load integration definitions from directory (each registers as a named executor).
    if let Some(dir) = integrations_dir {
        use tasked::executor::api::oauth2::TokenCache;

        let token_cache = token_cache_path
            .map(|p| Arc::new(TokenCache::with_persistence(std::path::Path::new(p))));

        let path = std::path::Path::new(dir);
        let count = api::register_integrations(engine, path, token_cache);
        if count > 0 {
            info!(count, dir, "loaded integration executors");
        }
    }

    // Spawn executor: delegates to any registered executor, parses output as tasks.
    // Registered last so it can see all other executors (including integrations).
    register_spawn_executor(engine);
}

fn register_mcp_executors(engine: &mut Engine) {
    engine.register_executor("shell", Arc::new(ShellExecutor));
    engine.register_executor("http", Arc::new(HttpExecutor::new()));
    engine.register_executor("noop", Arc::new(NoopExecutor));
    engine.register_executor("delay", Arc::new(DelayExecutor));
    engine.register_executor("approval", Arc::new(ApprovalExecutor));
    engine.register_executor("callback", Arc::new(CallbackExecutor::always_succeed()));
    engine.register_executor("remote", Arc::new(RemoteExecutor::new()));
    engine.register_executor("trigger", Arc::new(TriggerExecutor));
    // Spawn must be registered last — it captures a snapshot of the executor registry.
    register_spawn_executor(engine);
}

fn register_spawn_executor(engine: &mut Engine) {
    let executors = engine.executors().clone();
    engine.register_executor("spawn", Arc::new(SpawnExecutor::new(executors)));
}

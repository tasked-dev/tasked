//! Container executor — runs tasks in Docker containers.
//!
//! Task config:
//! ```json
//! {
//!     "image": "python:3.12-slim",
//!     "command": ["python", "-c", "print('hello')"],
//!     "env": { "KEY": "value" },
//!     "working_dir": "/app",
//!     "timeout_secs": 300
//! }
//! ```
//!
//! Returns: `{ "exit_code": 0, "stdout": "...", "stderr": "..." }`

use crate::types::{ExecuteResult, Task};
use async_trait::async_trait;
use serde_json::json;
use tracing::{debug, warn};

use super::{ExecutionContext, Executor};

/// Maximum combined stdout+stderr size before truncation (10 MB).
const MAX_OUTPUT_BYTES: usize = 10 * 1024 * 1024;

/// Truncate `s` to at most `max` bytes, backing up to a char boundary.
fn truncate_at_char_boundary(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Container executor — delegates to a ContainerBackend (Docker, Fly, etc.)
pub struct ContainerExecutor {
    backend: Box<dyn ContainerBackend>,
    /// If non-empty, only images whose name starts with one of these prefixes
    /// may be pulled. If empty, all images are allowed (backwards compatible).
    /// Prefix matches must end at a `/`, `:`, `@` or end-of-string boundary,
    /// so allowing `myorg` does not permit `myorgevil/x`.
    ///
    /// **Warning:** Without an image allowlist, tasks can pull and run arbitrary
    /// images from public registries, which may contain malicious code.
    allowed_image_prefixes: Vec<String>,
    /// If non-empty, only volume host paths under these prefixes are permitted.
    /// If empty, all volume mounts are rejected. Host paths are canonicalized
    /// (symlinks resolved) before matching.
    allowed_volume_prefixes: Vec<String>,
    /// Networks tasks may request via the `network` config key.
    /// Defaults to `["none"]` — tasks cannot opt into network access unless
    /// the operator explicitly allows it (e.g. `["none", "bridge"]`).
    allowed_networks: Vec<String>,
    /// Maximum memory a task may request via `memory_mb` (default 4096 MB).
    max_memory_mb: u64,
    /// Maximum CPUs a task may request via `cpus` (default 4.0).
    max_cpus: f64,
    /// Maximum PIDs a task may request via `pids_limit` (default 1024).
    max_pids: i64,
}

impl ContainerExecutor {
    pub fn new(backend: impl ContainerBackend + 'static) -> Self {
        Self {
            backend: Box::new(backend),
            allowed_image_prefixes: Vec::new(),
            allowed_volume_prefixes: Vec::new(),
            allowed_networks: vec!["none".to_string()],
            max_memory_mb: DEFAULT_MAX_MEMORY_MB,
            max_cpus: DEFAULT_MAX_CPUS,
            max_pids: DEFAULT_MAX_PIDS,
        }
    }

    /// Set the allowed image prefixes. When non-empty, only images whose name
    /// starts with one of these prefixes will be accepted. A prefix match must
    /// end at a `/`, `:`, `@` or end-of-string boundary.
    ///
    /// **Warning:** Leaving this empty allows *any* image to be pulled and run.
    /// In multi-tenant or untrusted environments, always configure an allowlist.
    pub fn with_allowed_image_prefixes(mut self, prefixes: Vec<String>) -> Self {
        self.allowed_image_prefixes = prefixes;
        self
    }

    /// Set the allowed volume mount prefixes. Only host paths under these
    /// prefixes will be permitted as bind mounts. If empty, all mounts are
    /// rejected. Host paths are canonicalized before matching, so symlinks
    /// cannot be used to escape the allowlist.
    pub fn with_allowed_volume_prefixes(mut self, prefixes: Vec<String>) -> Self {
        self.allowed_volume_prefixes = prefixes;
        self
    }

    /// Set the Docker network modes tasks may request via the `network`
    /// config key. Defaults to `["none"]`. Add `"bridge"` to allow egress,
    /// e.g. for `AgentExecutor` tasks that must reach AI provider APIs.
    /// Requests for networks outside this list fail non-retryably.
    pub fn with_allowed_networks(mut self, networks: Vec<String>) -> Self {
        self.allowed_networks = networks;
        self
    }

    /// Set the maximum memory (in MB) a task may request via `memory_mb`.
    pub fn with_max_memory_mb(mut self, max_memory_mb: u64) -> Self {
        self.max_memory_mb = max_memory_mb;
        self
    }

    /// Set the maximum CPU count a task may request via `cpus`.
    pub fn with_max_cpus(mut self, max_cpus: f64) -> Self {
        self.max_cpus = max_cpus;
        self
    }

    /// Set the maximum PID limit a task may request via `pids_limit`.
    pub fn with_max_pids(mut self, max_pids: i64) -> Self {
        self.max_pids = max_pids;
        self
    }
}

/// Default resource limits for containers.
const DEFAULT_MEMORY_BYTES: i64 = 536_870_912; // 512 MB
const DEFAULT_NANO_CPUS: i64 = 1_000_000_000; // 1 CPU
const DEFAULT_PIDS_LIMIT: i64 = 256;

/// Default caps on what tasks may request via executor_config overrides.
const DEFAULT_MAX_MEMORY_MB: u64 = 4096;
const DEFAULT_MAX_CPUS: f64 = 4.0;
const DEFAULT_MAX_PIDS: i64 = 1024;

/// Specification for running a container.
#[derive(Debug, Clone)]
pub struct ContainerSpec {
    pub image: String,
    pub command: Option<Vec<String>>,
    pub env: Vec<(String, String)>,
    pub working_dir: Option<String>,
    pub timeout: std::time::Duration,
    /// Volume bind mounts: `["host_path:container_path", ...]`
    pub volumes: Vec<String>,
    /// Memory limit in bytes.
    pub memory: i64,
    /// Total memory + swap limit in bytes (same as memory to disable swap).
    pub memory_swap: i64,
    /// CPU quota in units of 10^-9 CPUs.
    pub nano_cpus: i64,
    /// Maximum number of PIDs in the container.
    pub pids_limit: i64,
    /// Docker network mode (default: "none" for isolation).
    pub network_mode: String,
}

/// Result from running a container.
#[derive(Debug)]
pub struct ContainerResult {
    pub exit_code: i64,
    pub stdout: String,
    pub stderr: String,
}

/// Backend trait for container execution.
#[async_trait]
pub trait ContainerBackend: Send + Sync {
    async fn run(&self, spec: ContainerSpec) -> Result<ContainerResult, String>;
}

/// Validate a volume bind mount string (format: `host_path:container_path[:options]`).
///
/// Uses an allowlist approach: only mounts under the specified prefixes are permitted.
/// If no prefixes are configured, all volumes are rejected.
/// Host paths must be absolute, free of path traversal, and are canonicalized
/// (symlinks resolved) before prefix matching so a symlink under an allowed
/// prefix cannot escape to an arbitrary host path.
fn validate_volume(volume: &str, allowed_prefixes: &[String]) -> Result<(), String> {
    let host_path = volume.split(':').next().unwrap_or(volume);

    if host_path.is_empty() {
        return Err("empty host path".to_string());
    }

    // Host paths must be absolute.
    if !host_path.starts_with('/') {
        return Err(format!("volume host path must be absolute: '{host_path}'"));
    }

    if host_path == "/" {
        return Err("mounting root filesystem is not allowed".to_string());
    }

    // Lexical first pass: reject path traversal outright.
    if std::path::Path::new(host_path)
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(format!("path traversal in volume: '{host_path}'"));
    }

    // Check against allowlist — if empty, reject everything.
    if allowed_prefixes.is_empty() {
        return Err(format!(
            "no volume prefixes configured; mount of '{host_path}' is not allowed"
        ));
    }

    // Canonicalize the host path (resolving symlinks) so a symlink under an
    // allowed prefix cannot point outside it. A path that cannot be
    // canonicalized (e.g. does not exist) is rejected.
    let canonical = std::fs::canonicalize(host_path).map_err(|e| {
        format!("volume host path '{host_path}' cannot be canonicalized (must exist): {e}")
    })?;

    if canonical == std::path::Path::new("/") {
        return Err("mounting root filesystem is not allowed".to_string());
    }

    // Canonicalize allowlist entries too; entries that don't exist can never
    // match and are skipped.
    let allowed = allowed_prefixes.iter().any(|prefix| {
        std::fs::canonicalize(prefix)
            .is_ok_and(|canonical_prefix| canonical.starts_with(&canonical_prefix))
    });

    if !allowed {
        return Err(format!(
            "volume '{host_path}' is not under any allowed prefix"
        ));
    }

    Ok(())
}

/// Check whether `image` matches an allowlist `prefix` with a proper boundary:
/// the match must end at `/`, `:`, `@`, or end-of-string, so allowing `myorg`
/// does not also permit `myorgevil/x`.
fn image_matches_prefix(image: &str, prefix: &str) -> bool {
    match image.strip_prefix(prefix) {
        None => false,
        Some(rest) => {
            rest.is_empty()
                || rest.starts_with(['/', ':', '@'])
                || prefix.ends_with(['/', ':', '@'])
        }
    }
}

#[async_trait]
impl Executor for ContainerExecutor {
    async fn execute(&self, task: &Task, ctx: &ExecutionContext) -> ExecuteResult {
        let image = match task.executor_config.get("image").and_then(|v| v.as_str()) {
            Some(img) => img.to_string(),
            None => {
                return ExecuteResult::Failed {
                    error: "missing 'image' in executor config".to_string(),
                    retryable: false,
                };
            }
        };

        // Image allowlist check (#134)
        if !self.allowed_image_prefixes.is_empty()
            && !self
                .allowed_image_prefixes
                .iter()
                .any(|prefix| image_matches_prefix(&image, prefix))
        {
            return ExecuteResult::Failed {
                error: format!("image '{image}' is not in the allowed image prefixes"),
                retryable: false,
            };
        }

        let command = task.executor_config.get("command").and_then(|v| {
            v.as_array().map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect::<Vec<_>>()
            })
        });

        let mut env = task
            .executor_config
            .get("env")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|val| (k.clone(), val.to_string())))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        // Add artifact env vars
        if ctx.artifacts_dir.is_some() {
            env.push(("TASKED_ARTIFACTS".to_string(), "/artifacts".to_string()));
        }
        if let Some(ref url) = ctx.artifact_url {
            env.push(("TASKED_ARTIFACT_URL".to_string(), url.clone()));
        }

        let working_dir = task
            .executor_config
            .get("working_dir")
            .and_then(|v| v.as_str())
            .map(String::from);

        let timeout_secs = task
            .executor_config
            .get("timeout_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(task.timeout_secs);

        let volumes = task
            .executor_config
            .get("volumes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        for vol in &volumes {
            if let Err(reason) = validate_volume(vol, &self.allowed_volume_prefixes) {
                return ExecuteResult::Failed {
                    error: format!("volume rejected: {reason}"),
                    retryable: false,
                };
            }
        }

        // Network mode: default to "none" for isolation. A task may only
        // request a network that the executor-level policy allows; otherwise
        // `"network": "host"` would defeat isolation and the SSRF policy.
        let network_mode = match task.executor_config.get("network").and_then(|v| v.as_str()) {
            None => "none".to_string(),
            Some(requested) => {
                if !self.allowed_networks.iter().any(|n| n == requested) {
                    return ExecuteResult::Failed {
                        error: format!(
                            "network '{requested}' is not allowed by the container executor \
                             policy (allowed: {})",
                            self.allowed_networks.join(", ")
                        ),
                        retryable: false,
                    };
                }
                requested.to_string()
            }
        };

        // Resource limits — task overrides are bounded by executor-level caps.
        let memory = match task.executor_config.get("memory_mb").and_then(|v| v.as_u64()) {
            None => DEFAULT_MEMORY_BYTES,
            Some(mb) => {
                if mb == 0 || mb > self.max_memory_mb {
                    return ExecuteResult::Failed {
                        error: format!(
                            "memory_mb {mb} is outside the allowed range 1..={} MB",
                            self.max_memory_mb
                        ),
                        retryable: false,
                    };
                }
                (mb as i64) * 1_024 * 1_024
            }
        };

        let memory_swap = memory; // always equal to memory (disable swap)

        let nano_cpus = match task.executor_config.get("cpus").and_then(|v| v.as_f64()) {
            None => DEFAULT_NANO_CPUS,
            Some(cpus) => {
                if !(cpus > 0.0 && cpus <= self.max_cpus) {
                    return ExecuteResult::Failed {
                        error: format!(
                            "cpus {cpus} is outside the allowed range (0, {}]",
                            self.max_cpus
                        ),
                        retryable: false,
                    };
                }
                (cpus * 1_000_000_000.0) as i64
            }
        };

        let pids_limit = match task.executor_config.get("pids_limit").and_then(|v| v.as_i64()) {
            None => DEFAULT_PIDS_LIMIT,
            Some(pids) => {
                // Negative/zero would mean "unlimited" to Docker — reject.
                if pids < 1 || pids > self.max_pids {
                    return ExecuteResult::Failed {
                        error: format!(
                            "pids_limit {pids} is outside the allowed range 1..={}",
                            self.max_pids
                        ),
                        retryable: false,
                    };
                }
                pids
            }
        };

        let spec = ContainerSpec {
            image,
            command,
            env,
            working_dir,
            timeout: std::time::Duration::from_secs(timeout_secs),
            volumes,
            memory,
            memory_swap,
            nano_cpus,
            pids_limit,
            network_mode,
        };

        debug!(task_id = %task.id, image = %spec.image, "running container");

        match self.backend.run(spec).await {
            Ok(result) => {
                // Truncate output if it exceeds the 10 MB cap (#138).
                let mut stdout = result.stdout;
                let mut stderr = result.stderr;
                if stdout.len() + stderr.len() > MAX_OUTPUT_BYTES {
                    warn!(task_id = %task.id, "container output exceeded 10 MB, truncating");
                    let half = MAX_OUTPUT_BYTES / 2;
                    stdout.truncate(half);
                    stderr.truncate(half);
                    stderr.push_str("\n[tasked: output truncated at 10MB]");
                }

                let output = json!({
                    "exit_code": result.exit_code,
                    "stdout": stdout,
                    "stderr": stderr,
                });

                if result.exit_code == 0 {
                    ExecuteResult::Success {
                        output: Some(output),
                    }
                } else {
                    ExecuteResult::Failed {
                        error: format!(
                            "container exited with code {}: {}",
                            result.exit_code,
                            stderr.trim()
                        ),
                        retryable: true,
                    }
                }
            }
            Err(e) => {
                warn!(task_id = %task.id, error = %e, "container execution failed");
                ExecuteResult::Failed {
                    error: e,
                    retryable: true,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStorage;
    use crate::types::{BackoffStrategy, FlowId, QueueId, TaskId, TaskState};
    use std::sync::Arc;

    struct OkBackend;

    #[async_trait]
    impl ContainerBackend for OkBackend {
        async fn run(&self, _spec: ContainerSpec) -> Result<ContainerResult, String> {
            Ok(ContainerResult {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            })
        }
    }

    fn make_task(config: serde_json::Value) -> Task {
        Task {
            id: TaskId::from("test-container"),
            flow_id: FlowId::new(),
            queue_id: QueueId::from("test"),
            state: TaskState::Running,
            executor_type: "container".to_string(),
            executor_config: config,
            input: None,
            output: None,
            error: None,
            retries_remaining: 0,
            backoff: BackoffStrategy::default(),
            timeout_secs: 5,
            condition: None,
            retry_at: None,
            started_at: None,
            completed_at: None,
            created_at: chrono::Utc::now(),
        }
    }

    fn make_ctx() -> ExecutionContext {
        ExecutionContext::new(
            Arc::new(MemoryStorage::new()),
            TaskId::from("test-container"),
            FlowId::new(),
        )
    }

    fn assert_policy_failure(result: ExecuteResult, needle: &str) {
        match result {
            ExecuteResult::Failed { error, retryable } => {
                assert!(!retryable, "policy failures must be non-retryable");
                assert!(error.contains(needle), "error '{error}' should mention '{needle}'");
            }
            other => panic!("expected non-retryable failure, got {other:?}"),
        }
    }

    #[test]
    fn image_prefix_requires_boundary() {
        assert!(image_matches_prefix("myorg/app:1", "myorg"));
        assert!(image_matches_prefix("myorg:latest", "myorg"));
        assert!(image_matches_prefix("myorg@sha256:abc", "myorg"));
        assert!(image_matches_prefix("myorg", "myorg"));
        assert!(image_matches_prefix("myorg/app", "myorg/"));
        assert!(!image_matches_prefix("myorgevil/x", "myorg"));
        assert!(!image_matches_prefix("evilmyorg/x", "myorg"));
    }

    #[tokio::test]
    async fn host_network_rejected_by_default() {
        let exec = ContainerExecutor::new(OkBackend);
        let task = make_task(serde_json::json!({"image": "alpine", "network": "host"}));
        assert_policy_failure(exec.execute(&task, &make_ctx()).await, "network 'host'");
    }

    #[tokio::test]
    async fn allowed_network_accepted() {
        let exec = ContainerExecutor::new(OkBackend)
            .with_allowed_networks(vec!["none".into(), "bridge".into()]);
        let task = make_task(serde_json::json!({"image": "alpine", "network": "bridge"}));
        match exec.execute(&task, &make_ctx()).await {
            ExecuteResult::Success { .. } => {}
            other => panic!("expected success, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resource_overrides_above_caps_rejected() {
        let exec = ContainerExecutor::new(OkBackend);
        let ctx = make_ctx();

        let task = make_task(serde_json::json!({"image": "alpine", "memory_mb": 8192}));
        assert_policy_failure(exec.execute(&task, &ctx).await, "memory_mb");

        let task = make_task(serde_json::json!({"image": "alpine", "cpus": 64.0}));
        assert_policy_failure(exec.execute(&task, &ctx).await, "cpus");

        let task = make_task(serde_json::json!({"image": "alpine", "pids_limit": 100000}));
        assert_policy_failure(exec.execute(&task, &ctx).await, "pids_limit");

        // Negative pids_limit means "unlimited" to Docker — must be rejected.
        let task = make_task(serde_json::json!({"image": "alpine", "pids_limit": -1}));
        assert_policy_failure(exec.execute(&task, &ctx).await, "pids_limit");
    }

    #[tokio::test]
    async fn resource_overrides_within_caps_accepted() {
        let exec = ContainerExecutor::new(OkBackend);
        let task = make_task(serde_json::json!({
            "image": "alpine", "memory_mb": 1024, "cpus": 2.0, "pids_limit": 512
        }));
        match exec.execute(&task, &make_ctx()).await {
            ExecuteResult::Success { .. } => {}
            other => panic!("expected success, got {other:?}"),
        }
    }

    #[test]
    fn volume_requires_existing_path() {
        let allowed = vec!["/tmp".to_string()];
        let err = validate_volume("/tmp/definitely-not-existing-xyz123:/data", &allowed)
            .expect_err("nonexistent path must be rejected");
        assert!(err.contains("canonicalized"), "got: {err}");
    }

    #[test]
    fn volume_lexical_traversal_rejected() {
        let allowed = vec!["/tmp".to_string()];
        assert!(validate_volume("/tmp/../etc:/data", &allowed).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn volume_symlink_escape_rejected() {
        let base = std::env::temp_dir().join(format!("tasked-voltest-{}", uuid::Uuid::new_v4()));
        let inside = base.join("inside");
        let outside = base.join("outside");
        std::fs::create_dir_all(&inside).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let link = inside.join("escape");
        std::os::unix::fs::symlink(&outside, &link).unwrap();

        let allowed = vec![inside.display().to_string()];

        // A real path under the prefix is fine.
        let sub = inside.join("data");
        std::fs::create_dir_all(&sub).unwrap();
        assert!(validate_volume(&format!("{}:/data", sub.display()), &allowed).is_ok());

        // A symlink under the prefix pointing outside must be rejected.
        let err = validate_volume(&format!("{}:/data", link.display()), &allowed)
            .expect_err("symlink escape must be rejected");
        assert!(err.contains("not under any allowed prefix"), "got: {err}");

        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn truncate_helper_respects_char_boundaries() {
        assert_eq!(truncate_at_char_boundary("hello", 10), "hello");
        assert_eq!(truncate_at_char_boundary("hello", 3), "hel");
        // 'é' is 2 bytes; cutting at byte 1 must back up to 0.
        assert_eq!(truncate_at_char_boundary("é", 1), "");
    }
}

#[cfg(feature = "docker")]
pub mod docker {
    use super::*;
    use bollard::Docker;
    use bollard::container::LogOutput;
    use bollard::models::ContainerCreateBody;
    use bollard::query_parameters::{
        CreateContainerOptions, CreateImageOptions, LogsOptions, RemoveContainerOptions,
    };
    use futures::StreamExt;
    use tracing::info;

    pub struct DockerBackend {
        client: Docker,
    }

    impl DockerBackend {
        pub fn new() -> Result<Self, String> {
            let client = Docker::connect_with_local_defaults()
                .map_err(|e| format!("failed to connect to Docker: {e}"))?;
            Ok(Self { client })
        }
    }

    /// Best-effort force-remove of a container. Used on every exit path
    /// (success, timeout, inspect failure, start failure) to avoid leaks.
    async fn cleanup_container(client: &Docker, container_id: &str) {
        if let Err(e) = client
            .remove_container(
                container_id,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await
        {
            warn!(container_id = %container_id, error = %e, "failed to remove container");
        }
    }

    #[async_trait]
    impl ContainerBackend for DockerBackend {
        async fn run(&self, spec: ContainerSpec) -> Result<ContainerResult, String> {
            let t_start = std::time::Instant::now();

            // Only pull if image doesn't exist locally.
            let needs_pull = self.client.inspect_image(&spec.image).await.is_err();
            if needs_pull {
                let pull_opts = CreateImageOptions {
                    from_image: Some(spec.image.clone()),
                    ..Default::default()
                };
                let mut pull_stream = self.client.create_image(Some(pull_opts), None, None);
                while let Some(result) = pull_stream.next().await {
                    if let Err(e) = result {
                        return Err(format!("failed to pull image '{}': {}", spec.image, e));
                    }
                }
            }
            let t_pull = t_start.elapsed();

            // Build container config
            let env: Vec<String> = spec.env.iter().map(|(k, v)| format!("{k}={v}")).collect();

            let mut config = ContainerCreateBody {
                image: Some(spec.image.clone()),
                env: Some(env),
                ..Default::default()
            };

            if let Some(cmd) = &spec.command {
                config.cmd = Some(cmd.clone());
            }

            if let Some(ref wd) = spec.working_dir {
                config.working_dir = Some(wd.clone());
            }

            // Always set HostConfig with resource limits, network mode, and binds.
            let binds = if spec.volumes.is_empty() {
                None
            } else {
                Some(spec.volumes.clone())
            };
            config.host_config = Some(bollard::models::HostConfig {
                binds,
                memory: Some(spec.memory),
                memory_swap: Some(spec.memory_swap),
                nano_cpus: Some(spec.nano_cpus),
                pids_limit: Some(spec.pids_limit),
                network_mode: Some(spec.network_mode.clone()),
                ..Default::default()
            });

            // Create container
            let t_config = t_start.elapsed();
            let container_name = format!("tasked-{}", uuid::Uuid::new_v4());
            let create_opts = CreateContainerOptions {
                name: Some(container_name),
                ..Default::default()
            };
            let container = self
                .client
                .create_container(Some(create_opts), config)
                .await
                .map_err(|e| format!("failed to create container: {e}"))?;

            let t_create = t_start.elapsed();

            // Start container — clean up the created container on failure.
            if let Err(e) = self.client.start_container(&container.id, None).await {
                cleanup_container(&self.client, &container.id).await;
                return Err(format!("failed to start container: {e}"));
            }

            let t_started = t_start.elapsed();

            // Wait for container to exit by polling inspect.
            // Uses polling instead of the wait API for broad Docker backend compatibility.
            let deadline = tokio::time::Instant::now() + spec.timeout;
            let exit_code = loop {
                if tokio::time::Instant::now() > deadline {
                    let _ = self.client.kill_container(&container.id, None).await;
                    cleanup_container(&self.client, &container.id).await;
                    return Err(format!(
                        "container timed out after {}s",
                        spec.timeout.as_secs()
                    ));
                }

                match self.client.inspect_container(&container.id, None).await {
                    Ok(info) => {
                        let running = info.state.as_ref().and_then(|s| s.running).unwrap_or(false);
                        if !running {
                            break info.state.and_then(|s| s.exit_code).unwrap_or(-1);
                        }
                    }
                    Err(e) => {
                        cleanup_container(&self.client, &container.id).await;
                        return Err(format!("container inspect failed: {e}"));
                    }
                }

                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            };

            let t_exited = t_start.elapsed();

            // Capture logs
            let logs_opts = LogsOptions {
                stdout: true,
                stderr: true,
                ..Default::default()
            };
            let mut logs_stream = self.client.logs(&container.id, Some(logs_opts));
            let mut stdout = String::new();
            let mut stderr = String::new();
            let mut truncated = false;
            while let Some(item) = logs_stream.next().await {
                let log = match item {
                    Ok(log) => log,
                    Err(e) => {
                        warn!(
                            container_id = %container.id,
                            error = %e,
                            "container log stream ended with an error; captured logs may be incomplete"
                        );
                        break;
                    }
                };
                if truncated {
                    continue; // drain remaining stream without accumulating
                }
                // Enforce the cap BEFORE pushing: truncate the chunk to fit
                // rather than letting one oversized chunk blow past the limit.
                let remaining = MAX_OUTPUT_BYTES.saturating_sub(stdout.len() + stderr.len());
                let (target, message) = match &log {
                    LogOutput::StdOut { message } => (&mut stdout, message),
                    LogOutput::StdErr { message } => (&mut stderr, message),
                    _ => continue,
                };
                let text = String::from_utf8_lossy(message);
                if text.len() > remaining {
                    target.push_str(truncate_at_char_boundary(&text, remaining));
                    truncated = true;
                    warn!(
                        container_id = %container.id,
                        "container logs exceeded 10 MB, truncating"
                    );
                } else {
                    target.push_str(&text);
                }
            }

            let t_logs = t_start.elapsed();

            // Remove container
            let _ = self
                .client
                .remove_container(
                    &container.id,
                    Some(RemoveContainerOptions {
                        force: true,
                        ..Default::default()
                    }),
                )
                .await;

            let t_removed = t_start.elapsed();

            info!(
                image = %spec.image,
                pull_ms = t_pull.as_millis(),
                config_ms = (t_config - t_pull).as_millis(),
                create_ms = (t_create - t_config).as_millis(),
                start_ms = (t_started - t_create).as_millis(),
                run_ms = (t_exited - t_started).as_millis(),
                logs_ms = (t_logs - t_exited).as_millis(),
                remove_ms = (t_removed - t_logs).as_millis(),
                total_ms = t_removed.as_millis(),
                "container lifecycle timing"
            );

            Ok(ContainerResult {
                exit_code,
                stdout,
                stderr,
            })
        }
    }
}

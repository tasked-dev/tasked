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

/// Container executor — delegates to a ContainerBackend (Docker, Fly, etc.)
pub struct ContainerExecutor {
    backend: Box<dyn ContainerBackend>,
    /// If non-empty, only images whose name starts with one of these prefixes
    /// may be pulled. If empty, all images are allowed (backwards compatible).
    ///
    /// **Warning:** Without an image allowlist, tasks can pull and run arbitrary
    /// images from public registries, which may contain malicious code.
    allowed_image_prefixes: Vec<String>,
    /// If non-empty, only volume host paths under these prefixes are permitted.
    /// If empty, all volume mounts are rejected.
    allowed_volume_prefixes: Vec<String>,
}

impl ContainerExecutor {
    pub fn new(backend: impl ContainerBackend + 'static) -> Self {
        Self {
            backend: Box::new(backend),
            allowed_image_prefixes: Vec::new(),
            allowed_volume_prefixes: Vec::new(),
        }
    }

    /// Set the allowed image prefixes. When non-empty, only images whose name
    /// starts with one of these prefixes will be accepted.
    ///
    /// **Warning:** Leaving this empty allows *any* image to be pulled and run.
    /// In multi-tenant or untrusted environments, always configure an allowlist.
    pub fn with_allowed_image_prefixes(mut self, prefixes: Vec<String>) -> Self {
        self.allowed_image_prefixes = prefixes;
        self
    }

    /// Set the allowed volume mount prefixes. Only host paths under these
    /// prefixes will be permitted as bind mounts. If empty, all mounts are
    /// rejected.
    pub fn with_allowed_volume_prefixes(mut self, prefixes: Vec<String>) -> Self {
        self.allowed_volume_prefixes = prefixes;
        self
    }
}

/// Default resource limits for containers.
const DEFAULT_MEMORY_BYTES: i64 = 536_870_912; // 512 MB
const DEFAULT_NANO_CPUS: i64 = 1_000_000_000; // 1 CPU
const DEFAULT_PIDS_LIMIT: i64 = 256;

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
/// Host paths must be absolute and free of path traversal.
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

    // Reject path traversal
    if std::path::Path::new(host_path)
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(format!("path traversal in volume: '{host_path}'"));
    }

    // Canonicalize by resolving . components (we already rejected ..)
    // and stripping trailing slashes for consistent prefix matching.
    let normalized = host_path.trim_end_matches('/');

    // Check against allowlist — if empty, reject everything.
    if allowed_prefixes.is_empty() {
        return Err(format!(
            "no volume prefixes configured; mount of '{host_path}' is not allowed"
        ));
    }

    let allowed = allowed_prefixes.iter().any(|prefix| {
        let prefix = prefix.trim_end_matches('/');
        normalized == prefix || normalized.starts_with(&format!("{prefix}/"))
    });

    if !allowed {
        return Err(format!(
            "volume '{host_path}' is not under any allowed prefix"
        ));
    }

    Ok(())
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
                .any(|prefix| image.starts_with(prefix.as_str()))
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

        // Network mode: default to "none" for isolation, allow override via config (#136).
        let network_mode = task
            .executor_config
            .get("network")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| "none".to_string());

        // Resource limits — override defaults from executor_config.
        let memory = task
            .executor_config
            .get("memory_mb")
            .and_then(|v| v.as_u64())
            .map(|mb| (mb as i64) * 1_024 * 1_024)
            .unwrap_or(DEFAULT_MEMORY_BYTES);

        let memory_swap = memory; // always equal to memory (disable swap)

        let nano_cpus = task
            .executor_config
            .get("cpus")
            .and_then(|v| v.as_f64())
            .map(|c| (c * 1_000_000_000.0) as i64)
            .unwrap_or(DEFAULT_NANO_CPUS);

        let pids_limit = task
            .executor_config
            .get("pids_limit")
            .and_then(|v| v.as_i64())
            .unwrap_or(DEFAULT_PIDS_LIMIT);

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

            // Start container
            self.client
                .start_container(&container.id, None)
                .await
                .map_err(|e| format!("failed to start container: {e}"))?;

            let t_started = t_start.elapsed();

            // Wait for container to exit by polling inspect.
            // Uses polling instead of the wait API for broad Docker backend compatibility.
            let deadline = tokio::time::Instant::now() + spec.timeout;
            let exit_code = loop {
                if tokio::time::Instant::now() > deadline {
                    let _ = self.client.kill_container(&container.id, None).await;
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
            while let Some(Ok(log)) = logs_stream.next().await {
                if truncated {
                    continue; // drain remaining stream without accumulating
                }
                match log {
                    LogOutput::StdOut { message } => {
                        stdout.push_str(&String::from_utf8_lossy(&message));
                    }
                    LogOutput::StdErr { message } => {
                        stderr.push_str(&String::from_utf8_lossy(&message));
                    }
                    _ => {}
                }
                if stdout.len() + stderr.len() > MAX_OUTPUT_BYTES {
                    truncated = true;
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


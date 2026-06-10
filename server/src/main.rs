#![deny(unsafe_code)]

mod auth;
mod bootstrap;
mod dto;
mod error;
mod mcp;
mod routes;
mod run_cmd;

use clap::{Parser, Subcommand};
use std::sync::Arc;
use tasked::engine::{Engine, EngineConfig};
use tracing::info;

use crate::auth::{add_auth_layer, is_loopback_host};
use crate::bootstrap::{ExecutorProfile, open_storage, register_executors};
use crate::routes::{build_router, build_router_with_metrics, metrics_router};

// -- CLI --

#[derive(Parser)]
#[command(
    name = "tasked",
    about = "HTTP server and CLI for the Tasked DAG execution engine",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the HTTP server
    Serve {
        /// Data directory for per-queue databases (default: ./tasked-data)
        #[arg(long, default_value = "tasked-data")]
        data_dir: String,

        /// Storage engine: "sqlite" (default, per-queue SQLite databases) or
        /// "journal" (in-memory state with append-only SQLite journal)
        #[arg(long, default_value = "sqlite")]
        engine: String,

        /// Port to listen on
        #[arg(long, default_value_t = 8080)]
        port: u16,

        /// Host to bind to (defaults to loopback only; binding a non-loopback
        /// address with --auth-mode=none requires --allow-unauthenticated)
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        /// Authentication mode: none, api-key
        #[arg(long, default_value = "none")]
        auth_mode: String,

        /// Allow starting with --auth-mode=none on a non-loopback host.
        /// DANGEROUS: anyone who can reach the address can submit flows and
        /// execute shell commands. Prefer --auth-mode=api-key.
        #[arg(long)]
        allow_unauthenticated: bool,

        /// API key for api-key auth mode
        #[arg(long, env = "TASKED_API_KEY")]
        api_key: Option<String>,

        /// URL to push Prometheus metrics to (enables metrics push mode)
        #[arg(long)]
        metrics_push_url: Option<String>,

        /// Directory containing integration definition JSON files
        #[arg(long, env = "TASKED_INTEGRATIONS_DIR")]
        integrations_dir: Option<String>,

        /// SQLite path for persisting OAuth2 tokens (default: in-memory only)
        #[arg(long, env = "TASKED_TOKEN_CACHE")]
        token_cache: Option<String>,

        /// Port for a dedicated metrics listener on 127.0.0.1.
        /// When set, /metrics is served only on this port and removed from the main router.
        /// Recommended when auth is disabled to prevent unauthenticated metrics scraping.
        #[arg(long, env = "TASKED_METRICS_PORT")]
        metrics_port: Option<u16>,

        /// Allowed CORS origins (repeatable). If omitted, no cross-origin requests are allowed.
        /// Use "*" to allow all origins (not recommended in production).
        #[arg(long, env = "TASKED_CORS_ORIGIN")]
        cors_origin: Vec<String>,
    },
    /// Execute a flow definition from a JSON file and exit
    Run {
        /// Path to a flow definition JSON file
        file: String,

        /// Queue to submit the flow to (created if it doesn't exist)
        #[arg(long, default_value = "default")]
        queue: String,

        /// SQLite database path (use :memory: for in-memory)
        #[arg(long, default_value = ":memory:")]
        db: String,

        /// Auto-approve all approval tasks without prompting
        #[arg(long)]
        auto_approve: bool,

        /// Write task outputs to a JSON file on completion
        #[arg(long, short)]
        output: Option<String>,

        /// Directory containing integration definition JSON files
        #[arg(long, env = "TASKED_INTEGRATIONS_DIR")]
        integrations_dir: Option<String>,

        /// SQLite path for persisting OAuth2 tokens (default: in-memory only)
        #[arg(long, env = "TASKED_TOKEN_CACHE")]
        token_cache: Option<String>,
    },
    /// Start an MCP (Model Context Protocol) server on stdio
    Mcp {
        /// Data directory for per-queue databases (default: ./tasked-data)
        #[arg(long, default_value = "tasked-data")]
        data_dir: String,

        /// Storage engine: "sqlite" (default) or "journal" (in-memory with SQLite journal)
        #[arg(long, default_value = "sqlite")]
        engine: String,
    },
    /// Export a flow's complete state for archival or replay
    Export {
        /// Flow ID to export
        flow_id: String,

        /// Server base URL
        #[arg(long, default_value = "http://localhost:8080")]
        server: String,

        /// Include artifact data in the export
        #[arg(long)]
        with_artifacts: bool,

        /// Output file (default: stdout)
        #[arg(long, short)]
        output: Option<String>,

        /// Export format: "json" (default) or "tar" (tar.gz archive with artifacts)
        #[arg(long, default_value = "json")]
        format: String,

        /// API key for authenticated servers
        #[arg(long, env = "TASKED_API_KEY")]
        api_key: Option<String>,
    },
}

// -- Main --

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Serve {
            data_dir,
            engine,
            port,
            host,
            auth_mode,
            allow_unauthenticated,
            api_key,
            metrics_push_url,
            metrics_port,
            integrations_dir,
            token_cache,
            cors_origin,
        } => {
            // Verbose logging for server mode
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                        "tasked_server=info,tasked=info,tower_http=info"
                            .parse()
                            .unwrap()
                    }),
                )
                .init();
            run_serve(
                data_dir,
                engine,
                port,
                host,
                auth_mode,
                allow_unauthenticated,
                api_key,
                metrics_push_url,
                metrics_port,
                integrations_dir,
                token_cache,
                cors_origin,
            )
            .await;
        }
        Commands::Run {
            file,
            queue,
            db,
            auto_approve,
            output,
            integrations_dir,
            token_cache,
        } => {
            // Quiet logging for run mode — clean output only
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "warn".parse().unwrap()),
                )
                .init();
            let code = run_cmd::run_flow(
                file,
                queue,
                db,
                auto_approve,
                output,
                integrations_dir,
                token_cache,
            )
            .await;
            std::process::exit(code);
        }
        Commands::Mcp { data_dir, engine } => {
            // Quiet logging for MCP mode — output goes to stderr
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "warn".parse().unwrap()),
                )
                .with_writer(std::io::stderr)
                .init();
            mcp::run_mcp_server(data_dir, engine).await;
        }
        Commands::Export {
            flow_id,
            server,
            with_artifacts,
            output,
            format,
            api_key,
        } => {
            let url = format!(
                "{}/api/v1/flows/{}/export?with_artifacts={}&format={}",
                server.trim_end_matches('/'),
                flow_id,
                with_artifacts,
                format,
            );
            let client = reqwest::Client::new();
            let mut req = client.get(&url);
            if let Some(key) = &api_key {
                req = req.header("authorization", format!("Bearer {key}"));
            }
            let resp = req.send().await.unwrap_or_else(|e| {
                eprintln!("Error connecting to server: {e}");
                std::process::exit(1);
            });

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                eprintln!("Error (HTTP {status}): {body}");
                std::process::exit(1);
            }

            let is_tar = format == "tar";
            let bytes = resp.bytes().await.unwrap_or_else(|e| {
                eprintln!("Error reading response: {e}");
                std::process::exit(1);
            });

            match output {
                Some(path) if path != "-" => {
                    if let Err(e) = std::fs::write(&path, &bytes) {
                        eprintln!("Error writing to {path}: {e}");
                        std::process::exit(1);
                    }
                    eprintln!("Export written to {path}");
                }
                _ => {
                    if is_tar {
                        use std::io::Write;
                        std::io::stdout().write_all(&bytes).unwrap_or_else(|e| {
                            eprintln!("Error writing to stdout: {e}");
                            std::process::exit(1);
                        });
                    } else {
                        let text = String::from_utf8_lossy(&bytes);
                        println!("{text}");
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_serve(
    data_dir: String,
    engine_mode: String,
    port: u16,
    host: String,
    auth_mode: String,
    allow_unauthenticated: bool,
    api_key: Option<String>,
    metrics_push_url: Option<String>,
    metrics_port: Option<u16>,
    integrations_dir: Option<String>,
    token_cache: Option<String>,
    cors_origins: Vec<String>,
) {
    // Install Prometheus metrics recorder with histogram buckets for request latency
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full(
                "tasked_http_request_duration_seconds".to_owned(),
            ),
            &[
                0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
            ],
        )
        .expect("failed to set histogram buckets")
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full(
                "tasked_task_execution_duration_seconds".to_owned(),
            ),
            &[
                0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0, 60.0, 300.0,
            ],
        )
        .expect("failed to set histogram buckets")
        .build_recorder();
    let metrics_handle = recorder.handle();
    metrics::set_global_recorder(recorder).expect("failed to install metrics recorder");

    // Create storage backend based on --engine flag
    let storage = open_storage(&data_dir, &engine_mode);

    // Create engine
    let mut engine = Engine::new(storage, EngineConfig::default());
    register_executors(
        &mut engine,
        ExecutorProfile::Server {
            integrations_dir: integrations_dir.as_deref(),
            token_cache_path: token_cache.as_deref(),
        },
    );

    // Configure artifact storage
    let artifacts_dir = std::path::PathBuf::from(&data_dir).join("artifacts");
    engine.set_artifact_store(Arc::new(tasked::artifacts::LocalArtifactStore::new(
        &artifacts_dir,
    )));

    let engine = Arc::new(engine);

    // Clone for the engine loop — spawned after listener binds (see below).
    let engine_handle = engine.clone();

    // Refuse to serve unauthenticated on a non-loopback address unless the
    // operator explicitly opts in. The shell executor is registered, so an
    // unauthenticated reachable server is remote command execution.
    if auth_mode == "none" && !is_loopback_host(&host) {
        if !allow_unauthenticated {
            eprintln!(
                "fatal: refusing to start with --auth-mode=none on non-loopback host '{host}'. \
                 Anyone who can reach this address could submit flows and execute shell commands. \
                 Use --auth-mode=api-key, bind to 127.0.0.1, or pass --allow-unauthenticated to override."
            );
            std::process::exit(1);
        }
        tracing::warn!(
            host = %host,
            "server starting with NO authentication on a non-localhost address \
             (--allow-unauthenticated) — anyone who can reach this address can submit \
             flows and execute commands. Use --auth-mode=api-key for production deployments."
        );
    }

    // Build router: if --metrics-port is set, serve metrics on a separate listener;
    // otherwise keep metrics on the main port (with a warning when auth is disabled).
    let app = if metrics_port.is_some() {
        build_router(engine, &cors_origins)
    } else {
        tracing::warn!(
            "metrics endpoint (/metrics) on the main listener is exempt from authentication \
             so scrapers can reach it. Use --metrics-port to serve metrics on a separate \
             loopback-only listener instead."
        );
        build_router_with_metrics(engine, metrics_handle.clone(), &cors_origins)
    };

    // Apply auth layer
    let app = add_auth_layer(app, &auth_mode, api_key.as_deref());

    // Spawn dedicated metrics listener if configured
    if let Some(m_port) = metrics_port {
        let handle = metrics_handle.clone();
        tokio::spawn(async move {
            let metrics_router = metrics_router(handle);

            let metrics_addr = format!("127.0.0.1:{m_port}");
            info!(addr = %metrics_addr, "starting dedicated metrics listener");

            let listener = tokio::net::TcpListener::bind(&metrics_addr)
                .await
                .expect("failed to bind metrics listener");

            axum::serve(listener, metrics_router)
                .await
                .expect("metrics server error");
        });
    }

    // Spawn metrics push task if configured
    if let Some(push_url) = metrics_push_url {
        let handle = metrics_handle;
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                let metrics_text = handle.render();
                if let Err(e) = client
                    .post(&push_url)
                    .header("content-type", "text/plain")
                    .body(metrics_text)
                    .send()
                    .await
                {
                    tracing::warn!(error = %e, "metrics push failed");
                }
            }
        });
    }

    // Start server
    let addr = format!("{host}:{port}");
    info!(addr = %addr, data_dir = %data_dir, "starting tasked-server");

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("failed to bind");

    // Spawn engine processing loop AFTER listener is bound.
    // Recovery dispatch happens inside run() — by deferring it until after
    // the listener is ready, healthcheck and API endpoints are available
    // immediately even when recovering a large backlog.
    tokio::spawn(async move {
        engine_handle.run().await;
    });

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");

    info!("server shut down gracefully");
}

/// Resolves when the process receives SIGTERM or ctrl-c (SIGINT),
/// triggering graceful shutdown of in-flight HTTP requests.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %e, "failed to install ctrl-c handler");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("received ctrl-c, shutting down"),
        _ = terminate => info!("received SIGTERM, shutting down"),
    }
}

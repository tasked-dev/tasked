//! The interactive `run` subcommand: execute a flow definition from a JSON
//! file with live terminal progress output, then exit.

use std::sync::Arc;
use tasked::{
    engine::{Engine, EngineConfig},
    store::{memory::MemoryStorage, sqlite::SqliteStorage},
    types::*,
};

use crate::bootstrap::{ExecutorProfile, register_executors};

pub(crate) async fn run_flow(
    file: String,
    queue: String,
    db: String,
    auto_approve: bool,
    output_file: Option<String>,
    integrations_dir: Option<String>,
    token_cache: Option<String>,
) -> i32 {
    // Read flow definition from file
    let content = match std::fs::read_to_string(&file) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error reading flow file '{file}': {e}");
            return 1;
        }
    };

    let flow_def: FlowDef = match serde_json::from_str(&content) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error parsing flow JSON: {e}");
            return 1;
        }
    };

    // Create storage
    let storage: Arc<dyn tasked::store::Storage> = if db == ":memory:" {
        Arc::new(MemoryStorage::new())
    } else {
        match SqliteStorage::open(&db) {
            Ok(s) => Arc::new(s),
            Err(e) => {
                eprintln!("Error opening database '{db}': {e}");
                return 1;
            }
        }
    };

    // Create engine
    let mut engine = Engine::new(
        storage,
        EngineConfig {
            poll_interval: std::time::Duration::from_millis(100),
            ..EngineConfig::default()
        },
    );
    register_executors(
        &mut engine,
        ExecutorProfile::Server {
            integrations_dir: integrations_dir.as_deref(),
            token_cache_path: token_cache.as_deref(),
        },
    );

    // Configure artifact storage in temp directory
    let artifacts_dir = std::env::temp_dir().join("tasked-artifacts");
    engine.set_artifact_store(Arc::new(tasked::artifacts::LocalArtifactStore::new(
        &artifacts_dir,
    )));

    let engine = Arc::new(engine);

    // Create queue if it doesn't exist
    let queue_id = QueueId::from(queue.clone());
    if engine.get_queue(&queue_id).await.unwrap_or(None).is_none()
        && let Err(e) = engine.create_queue(&queue_id, QueueConfig::default()).await
    {
        eprintln!("Error creating queue '{queue}': {e}");
        return 1;
    }

    // Submit the flow
    let flow = match engine.submit_flow(&queue_id, flow_def).await {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error submitting flow: {e}");
            return 1;
        }
    };

    const DIM: &str = "\x1b[2m";
    const RESET: &str = "\x1b[0m";

    let flow_short = flow.id.as_str().get(..8).unwrap_or(flow.id.as_str());
    println!(
        "\x1b[33m▸\x1b[0m {DIM}Flow {flow_short} submitted ({} tasks){RESET}",
        flow.task_count
    );

    // Spawn the engine loop for concurrent execution
    let engine_loop = engine.clone();
    let engine_handle = tokio::spawn(async move {
        engine_loop.run().await;
    });

    let start = std::time::Instant::now();

    // Compute max task name length for column alignment
    let initial_tasks = engine.get_flow_tasks(&flow.id).await.unwrap_or_default();
    let max_len = initial_tasks
        .iter()
        .map(|t| t.id.as_str().len())
        .max()
        .unwrap_or(4);

    // Track displayed state per task
    // "none" = not yet printed, "running" = showing running line, "done" = final state printed
    let mut displayed: std::collections::HashMap<String, &str> = std::collections::HashMap::new();
    // Count of "running" lines currently visible (for cursor-up erasing)
    let mut running_lines: usize = 0;
    // Track which approval tasks have already been prompted/auto-approved
    let mut approvals_handled: std::collections::HashSet<String> = std::collections::HashSet::new();

    let flow_id = flow.id.clone();
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let tasks = engine.get_flow_tasks(&flow_id).await.unwrap_or_default();

        // Find newly completed tasks (were running or unseen, now terminal)
        let mut newly_done: Vec<&Task> = Vec::new();
        let mut still_running: Vec<&Task> = Vec::new();
        let mut newly_running: Vec<&Task> = Vec::new();

        for task in &tasks {
            let prev = *displayed.get(task.id.as_str()).unwrap_or(&"none");
            match task.state {
                TaskState::Succeeded | TaskState::Failed | TaskState::Cancelled
                    if prev != "done" =>
                {
                    newly_done.push(task);
                }
                TaskState::Running if prev == "none" => {
                    newly_running.push(task);
                }
                TaskState::Running if prev == "running" => {
                    still_running.push(task);
                }
                _ => {}
            }
        }

        // Handle approval tasks (Running with awaiting_approval output)
        for task in &tasks {
            if task.state == TaskState::Running
                && !approvals_handled.contains(task.id.as_str())
                && let Some(ref output) = task.output
                && output.get("awaiting_approval").and_then(|v| v.as_bool()) == Some(true)
            {
                approvals_handled.insert(task.id.to_string());
                let message = output
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Approval required");

                // Erase running lines before prompting
                for _ in 0..running_lines {
                    print!("\x1b[A\x1b[2K");
                }
                running_lines = 0;

                let padded = format!("{:<width$}", task.id.as_str(), width = max_len);
                let approved = if auto_approve {
                    println!("\x1b[35m?\x1b[0m [{padded}]  {message} {DIM}(auto-approved){RESET}");
                    true
                } else {
                    print!("\x1b[35m?\x1b[0m [{padded}]  {message} \x1b[1m[y/N]\x1b[0m ");
                    use std::io::Write;
                    if let Err(e) = std::io::stdout().flush() {
                        eprintln!("Error flushing stdout: {e}");
                        false
                    } else {
                        let mut input = String::new();
                        if let Err(e) = std::io::stdin().read_line(&mut input) {
                            eprintln!("Error reading stdin: {e}");
                            false
                        } else {
                            let answer = input.trim().to_lowercase();
                            answer == "y" || answer == "yes"
                        }
                    }
                };

                if approved {
                    let result = ExecuteResult::Success {
                        output: Some(serde_json::json!({"approved": true, "approved_by": "cli"})),
                    };
                    if let Err(e) = engine.handle_task_result(task, result).await {
                        eprintln!("Error approving task: {e}");
                    }
                } else {
                    let result = ExecuteResult::Failed {
                        error: "rejected by user".to_string(),
                        retryable: false,
                    };
                    if let Err(e) = engine.handle_task_result(task, result).await {
                        eprintln!("Error rejecting task: {e}");
                    }
                }
            }
        }

        if newly_done.is_empty() && newly_running.is_empty() {
            // Check flow completion
            let flow = match engine.get_flow(&flow_id).await {
                Ok(Some(f)) => f,
                Ok(None) => {
                    eprintln!("Flow disappeared unexpectedly");
                    return 1;
                }
                Err(e) => {
                    eprintln!("Error fetching flow: {e}");
                    return 1;
                }
            };
            if flow.state.is_terminal() {
                // Erase any remaining running lines
                for _ in 0..running_lines {
                    print!("\x1b[A\x1b[2K");
                }
                let elapsed = format!("{:.1}s", start.elapsed().as_secs_f64());
                println!();
                match flow.state {
                    FlowState::Succeeded => println!(
                        "\x1b[32m✓\x1b[0m \x1b[32m\x1b[1mFlow complete\x1b[0m  {DIM}{}/{} tasks succeeded ({elapsed}){RESET}",
                        flow.tasks_succeeded, flow.task_count
                    ),
                    FlowState::Failed => println!(
                        "\x1b[31m✗\x1b[0m \x1b[31m\x1b[1mFlow failed\x1b[0m  {DIM}{} succeeded, {} failed ({elapsed}){RESET}",
                        flow.tasks_succeeded, flow.tasks_failed
                    ),
                    FlowState::Cancelled => println!("{DIM}– Flow cancelled ({elapsed}){RESET}"),
                    _ => {}
                }
                // Write output file if requested
                if let Some(ref path) = output_file {
                    let tasks = engine.get_flow_tasks(&flow_id).await.unwrap_or_default();
                    let outputs: serde_json::Map<String, serde_json::Value> = tasks
                        .iter()
                        .map(|t| {
                            (
                                t.id.to_string(),
                                serde_json::json!({
                                    "state": t.state.to_string(),
                                    "output": t.output,
                                    "error": t.error,
                                }),
                            )
                        })
                        .collect();
                    let json = match serde_json::to_string_pretty(&outputs) {
                        Ok(j) => j,
                        Err(e) => {
                            eprintln!("Error serializing outputs: {e}");
                            return 1;
                        }
                    };
                    if path == "-" {
                        println!("{json}");
                    } else if let Err(e) = std::fs::write(path, &json) {
                        eprintln!("{DIM}Warning: failed to write output file: {e}{RESET}");
                    } else {
                        println!("{DIM}Output written to {path}{RESET}");
                    }
                }

                engine_handle.abort();
                return if flow.state == FlowState::Succeeded {
                    0
                } else {
                    1
                };
            }
            continue;
        }

        // Erase current running lines (they'll be reprinted or replaced)
        for _ in 0..running_lines {
            print!("\x1b[A\x1b[2K");
        }

        // Print newly completed tasks as permanent lines
        for task in &newly_done {
            let padded = format!("{:<width$}", task.id.as_str(), width = max_len);
            match task.state {
                TaskState::Succeeded => {
                    let dur = task
                        .completed_at
                        .and_then(|c| task.started_at.map(|s| c - s))
                        .map(|d| format!("{:.1}s", d.num_milliseconds() as f64 / 1000.0))
                        .unwrap_or_default();
                    println!(
                        "\x1b[32m✓\x1b[0m [{padded}]  \x1b[32msucceeded\x1b[0m  {DIM}{dur}{RESET}"
                    );
                    // Show last 3 lines of stdout if available
                    #[allow(clippy::collapsible_if)]
                    if let Some(ref output) = task.output {
                        if let Some(stdout) = output.get("stdout").and_then(|v| v.as_str()) {
                            let stdout = stdout.trim();
                            if !stdout.is_empty() {
                                let lines: Vec<&str> = stdout.lines().collect();
                                let start = lines.len().saturating_sub(3);
                                for line in &lines[start..] {
                                    println!("  {DIM}  {line}{RESET}");
                                }
                            }
                        } else if let Some(response) =
                            output.get("response").and_then(|v| v.as_str())
                        {
                            // Agent executor output
                            let response = response.trim();
                            if !response.is_empty() {
                                let lines: Vec<&str> = response.lines().collect();
                                let start = lines.len().saturating_sub(3);
                                for line in &lines[start..] {
                                    println!("  {DIM}  {line}{RESET}");
                                }
                            }
                        }
                    }
                }
                TaskState::Failed => {
                    let err = task.error.as_deref().unwrap_or("unknown error");
                    println!(
                        "\x1b[31m✗\x1b[0m [{padded}]  \x1b[31mfailed\x1b[0m     {DIM}{err}{RESET}"
                    );
                    // Show stderr if available
                    if let Some(ref output) = task.output
                        && let Some(stderr) = output.get("stderr").and_then(|v| v.as_str())
                    {
                        let stderr = stderr.trim();
                        if !stderr.is_empty() {
                            let lines: Vec<&str> = stderr.lines().collect();
                            let start = lines.len().saturating_sub(3);
                            for line in &lines[start..] {
                                println!("  {DIM}  {line}{RESET}");
                            }
                        }
                    }
                }
                TaskState::Cancelled => {
                    println!("{DIM}– [{padded}]  cancelled{RESET}");
                }
                _ => {}
            }
            displayed.insert(task.id.to_string(), "done");
        }

        // Print all currently running tasks (ephemeral, will be erased next cycle)
        let all_running: Vec<&Task> = tasks
            .iter()
            .filter(|t| {
                t.state == TaskState::Running
                    && *displayed.get(t.id.as_str()).unwrap_or(&"none") != "done"
            })
            .collect();

        let mut total_running_lines = 0;
        for task in &all_running {
            let padded = format!("{:<width$}", task.id.as_str(), width = max_len);
            println!("\x1b[33m▸\x1b[0m [{padded}]  \x1b[33mrunning...\x1b[0m");
            total_running_lines += 1;
            // Show live output preview (last 3 lines)
            if let Some(ref output) = task.output
                && let Some(stdout) = output.get("stdout").and_then(|v| v.as_str())
            {
                let lines: Vec<&str> = stdout.trim().lines().collect();
                let start = lines.len().saturating_sub(3);
                for line in &lines[start..] {
                    println!("  {DIM}  {line}{RESET}");
                    total_running_lines += 1;
                }
            }
            displayed.insert(task.id.to_string(), "running");
        }
        running_lines = total_running_lines;

        for task in &newly_running {
            if !all_running
                .iter()
                .any(|t| t.id.as_str() == task.id.as_str())
            {
                displayed.insert(task.id.to_string(), "running");
            }
        }
    }
}

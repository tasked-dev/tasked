//! MCP (Model Context Protocol) server implementation.
//!
//! Implements the MCP protocol over stdio using JSON-RPC 2.0 with
//! newline-delimited framing, as specified by the MCP stdio transport
//! (protocol version 2024-11-05): each message is a single line of JSON
//! terminated by `\n`. This allows AI agents like Claude Code to use
//! Tasked as a tool for managing DAG workflows.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tasked::{
    engine::{Engine, EngineConfig},
    types::*,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{debug, error, info, warn};

use crate::bootstrap::{ExecutorProfile, open_storage, register_executors};
use crate::dto::default_enabled;
use crate::error::engine_error_message;

// -- JSON-RPC types --

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Option<Value>,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

impl JsonRpcResponse {
    fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: Some(result),
            error: None,
        }
    }

    fn error(id: Value, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data: None,
            }),
        }
    }
}

// -- JSON-RPC error codes --

const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;

// -- Tool error types --

/// Distinguishes parameter validation errors from tool runtime errors.
/// Parameter errors become JSON-RPC INVALID_PARAMS (-32602) responses;
/// runtime errors become MCP tool-level errors (isError: true).
#[derive(Debug)]
enum ToolError {
    /// Missing or invalid parameter (maps to INVALID_PARAMS).
    InvalidParam(String),
    /// Runtime failure during tool execution (maps to isError: true).
    Runtime(String),
}

impl ToolError {
    fn param(msg: impl Into<String>) -> Self {
        Self::InvalidParam(msg.into())
    }

    fn runtime(msg: impl Into<String>) -> Self {
        Self::Runtime(msg.into())
    }
}

// -- Tool definitions --

fn tool_definitions() -> Value {
    serde_json::json!({
        "tools": [
            {
                "name": "tasked_submit_flow",
                "description": "Submit a DAG workflow for execution. Tasks run concurrently where dependencies allow. Auto-creates the queue if it doesn't exist.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "queue": {
                            "type": "string",
                            "description": "Queue name to submit the flow to (created automatically if it doesn't exist)"
                        },
                        "tasks": {
                            "type": "array",
                            "description": "Array of task definitions forming the DAG",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "id": {
                                        "type": "string",
                                        "description": "Unique task identifier within the flow"
                                    },
                                    "executor": {
                                        "type": "string",
                                        "description": "Executor type",
                                        "enum": ["shell", "http", "noop", "delay", "approval", "spawn", "trigger", "callback", "remote"]
                                    },
                                    "config": {
                                        "type": "object",
                                        "description": "Executor-specific configuration. For 'shell': {\"command\": \"...\", \"args\": [...], \"cwd\": \"...\"}. For 'http': {\"url\": \"...\", \"method\": \"GET\", \"headers\": {}, \"body\": \"...\"}. For 'delay': {\"seconds\": N}. For 'spawn': {\"executor\": \"shell\", \"config\": {...}}. For 'trigger': {\"queue\": \"...\", \"flow\": {...}, \"wait\": true}. For 'remote': {\"url\": \"...\"}."
                                    },
                                    "input": {
                                        "description": "Optional input data for the task"
                                    },
                                    "depends_on": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "description": "Task IDs this task depends on (must complete first)"
                                    },
                                    "timeout_secs": {
                                        "type": "integer",
                                        "description": "Timeout in seconds (default: queue default)"
                                    },
                                    "retries": {
                                        "type": "integer",
                                        "description": "Max retry count (default: queue default)"
                                    }
                                },
                                "required": ["id", "executor"]
                            }
                        },
                        "queue_config": {
                            "type": "object",
                            "description": "Optional queue configuration (only used when creating a new queue)",
                            "properties": {
                                "concurrency": {
                                    "type": "integer",
                                    "description": "Max concurrent tasks (default: 10)"
                                },
                                "max_retries": {
                                    "type": "integer",
                                    "description": "Default max retries (default: 3)"
                                },
                                "timeout_secs": {
                                    "type": "integer",
                                    "description": "Default task timeout in seconds (default: 300)"
                                }
                            }
                        },
                        "fail_fast": {
                            "type": "boolean",
                            "description": "Cancel all tasks on first failure (default: false)",
                            "default": false
                        }
                    },
                    "required": ["queue", "tasks"]
                }
            },
            {
                "name": "tasked_flow_status",
                "description": "Check the status of a submitted flow, including per-task state and outputs.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "flow_id": {
                            "type": "string",
                            "description": "The flow ID returned by tasked_submit_flow"
                        }
                    },
                    "required": ["flow_id"]
                }
            },
            {
                "name": "tasked_task_output",
                "description": "Get the output or error of a specific task within a flow.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "flow_id": {
                            "type": "string",
                            "description": "The flow ID"
                        },
                        "task_id": {
                            "type": "string",
                            "description": "The task ID within the flow"
                        }
                    },
                    "required": ["flow_id", "task_id"]
                }
            },
            {
                "name": "tasked_cancel_flow",
                "description": "Cancel a running flow and all its non-terminal tasks.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "flow_id": {
                            "type": "string",
                            "description": "The flow ID to cancel"
                        }
                    },
                    "required": ["flow_id"]
                }
            },
            {
                "name": "tasked_list_flows",
                "description": "List active and recent flows, optionally filtered by queue or state.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "queue": {
                            "type": "string",
                            "description": "Filter by queue name (required)"
                        },
                        "state": {
                            "type": "string",
                            "description": "Filter by state: 'running', 'succeeded', 'failed', 'cancelled'",
                            "enum": ["running", "succeeded", "failed", "cancelled"]
                        }
                    },
                    "required": ["queue"]
                }
            },
            {
                "name": "tasked_create_schedule",
                "description": "Create a cron schedule that automatically submits a flow on a recurring basis. Auto-creates the queue if it doesn't exist.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "queue": {
                            "type": "string",
                            "description": "Queue name to submit scheduled flows to (created automatically if it doesn't exist)"
                        },
                        "cron": {
                            "type": "string",
                            "description": "Cron expression (standard 5-field, e.g. '*/5 * * * *' for every 5 minutes)"
                        },
                        "flow": {
                            "type": "object",
                            "description": "Flow definition with tasks array",
                            "properties": {
                                "tasks": {
                                    "type": "array",
                                    "description": "Array of task definitions forming the DAG",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "string"},
                                            "executor": {"type": "string", "enum": ["shell", "http", "noop", "delay", "approval", "spawn", "trigger", "callback", "remote"]},
                                            "config": {"type": "object"},
                                            "input": {},
                                            "depends_on": {"type": "array", "items": {"type": "string"}}
                                        },
                                        "required": ["id", "executor"]
                                    }
                                }
                            },
                            "required": ["tasks"]
                        },
                        "name": {
                            "type": "string",
                            "description": "Optional human-readable name for the schedule"
                        },
                        "enabled": {
                            "type": "boolean",
                            "description": "Whether the schedule is active (default: true)"
                        }
                    },
                    "required": ["queue", "cron", "flow"]
                }
            },
            {
                "name": "tasked_list_schedules",
                "description": "List all cron schedules for a queue.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "queue": {
                            "type": "string",
                            "description": "Queue name to list schedules for"
                        }
                    },
                    "required": ["queue"]
                }
            },
            {
                "name": "tasked_delete_schedule",
                "description": "Delete a cron schedule by ID.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "schedule_id": {
                            "type": "string",
                            "description": "The schedule ID to delete"
                        }
                    },
                    "required": ["schedule_id"]
                }
            }
        ]
    })
}

// -- MCP server --

/// Run the MCP server on stdio.
pub async fn run_mcp_server(data_dir: String, engine_mode: String) {
    // Create storage backend based on --engine flag
    let storage = open_storage(&data_dir, &engine_mode);

    // Create engine with all executors that work without special config
    let mut engine = Engine::new(storage, EngineConfig::default());
    register_executors(&mut engine, ExecutorProfile::Mcp);

    let engine = Arc::new(engine);

    // Spawn engine processing loop
    let engine_loop = engine.clone();
    tokio::spawn(async move {
        engine_loop.run().await;
    });

    info!("MCP server starting on stdio");

    // Run the stdio message loop
    if let Err(e) = stdio_loop(engine).await {
        error!(error = %e, "MCP server error");
    }
}

/// Maximum accepted length of a single newline-delimited message.
const MAX_LINE_BYTES: usize = 10 * 1024 * 1024; // 10 MB

/// Result of reading one newline-delimited message from the stream.
enum ReadLine {
    /// End of stream with no pending data.
    Eof,
    /// One line, without the trailing newline.
    Line(String),
    /// The line exceeded `MAX_LINE_BYTES` and was discarded.
    TooLong,
}

/// Read and process MCP messages from stdin, write responses to stdout.
async fn stdio_loop(engine: Arc<Engine>) -> Result<(), Box<dyn std::error::Error>> {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin);

    loop {
        // Read one newline-delimited JSON-RPC message.
        let line = match read_line_capped(&mut reader, MAX_LINE_BYTES).await? {
            ReadLine::Eof => {
                debug!("stdin closed, shutting down MCP server");
                return Ok(());
            }
            ReadLine::TooLong => {
                warn!(
                    max_bytes = MAX_LINE_BYTES,
                    "MCP message exceeds size limit, discarding"
                );
                let response = JsonRpcResponse::error(
                    Value::Null,
                    PARSE_ERROR,
                    format!("Parse error: message exceeds maximum size of {MAX_LINE_BYTES} bytes"),
                );
                write_message(&mut stdout, &response).await?;
                continue;
            }
            ReadLine::Line(line) => line,
        };

        let body_str = line.trim();

        // Skip blank lines between messages.
        if body_str.is_empty() {
            continue;
        }

        debug!(body = %body_str, "received MCP message");

        // Parse JSON-RPC; malformed JSON gets a -32700 parse error response.
        let request: JsonRpcRequest = match serde_json::from_str(body_str) {
            Ok(req) => req,
            Err(e) => {
                let response =
                    JsonRpcResponse::error(Value::Null, PARSE_ERROR, format!("Parse error: {e}"));
                write_message(&mut stdout, &response).await?;
                continue;
            }
        };

        // Notifications have no id and don't get responses
        if request.id.is_none() {
            debug!(method = %request.method, "received notification, no response needed");
            continue;
        }

        let id = request.id.unwrap();

        // Validate jsonrpc version
        if request.jsonrpc != "2.0" {
            let response = JsonRpcResponse::error(
                id,
                INVALID_REQUEST,
                "Invalid JSON-RPC version, expected \"2.0\"",
            );
            write_message(&mut stdout, &response).await?;
            continue;
        }

        // Dispatch method
        let response = match request.method.as_str() {
            "initialize" => handle_initialize(id, request.params),
            "tools/list" => handle_tools_list(id),
            "tools/call" => handle_tools_call(id, request.params, &engine).await,
            "ping" => JsonRpcResponse::success(id, serde_json::json!({})),
            _ => JsonRpcResponse::error(
                id,
                METHOD_NOT_FOUND,
                format!("Method not found: {}", request.method),
            ),
        };

        write_message(&mut stdout, &response).await?;
    }
}

/// Read one line (terminated by `\n` or EOF) from the stream, enforcing a
/// maximum length. Oversized lines are consumed and discarded without
/// buffering more than `max` bytes, so a single huge message cannot exhaust
/// memory. A final unterminated line before EOF is returned as a line.
async fn read_line_capped<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    max: usize,
) -> std::io::Result<ReadLine> {
    let mut buf: Vec<u8> = Vec::new();
    let mut overflow = false;

    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            // EOF
            if buf.is_empty() && !overflow {
                return Ok(ReadLine::Eof);
            }
            break;
        }

        match available.iter().position(|&b| b == b'\n') {
            Some(pos) => {
                if !overflow {
                    buf.extend_from_slice(&available[..pos]);
                }
                reader.consume(pos + 1);
                break;
            }
            None => {
                let len = available.len();
                if !overflow {
                    buf.extend_from_slice(available);
                }
                reader.consume(len);
                if buf.len() > max {
                    overflow = true;
                    buf.clear();
                }
            }
        }
    }

    if overflow || buf.len() > max {
        return Ok(ReadLine::TooLong);
    }
    Ok(ReadLine::Line(String::from_utf8_lossy(&buf).into_owned()))
}

/// Write a JSON-RPC response as a single newline-delimited JSON message.
async fn write_message<W: tokio::io::AsyncWrite + Unpin>(
    out: &mut W,
    response: &JsonRpcResponse,
) -> Result<(), Box<dyn std::error::Error>> {
    let json = serde_json::to_string(response)?;
    out.write_all(json.as_bytes()).await?;
    out.write_all(b"\n").await?;
    out.flush().await?;

    debug!(response = %json, "sent MCP response");
    Ok(())
}

// -- Method handlers --

fn handle_initialize(id: Value, _params: Option<Value>) -> JsonRpcResponse {
    JsonRpcResponse::success(
        id,
        serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "tools": {}
            },
            "serverInfo": {
                "name": "tasked",
                "version": env!("CARGO_PKG_VERSION")
            }
        }),
    )
}

fn handle_tools_list(id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(id, tool_definitions())
}

async fn handle_tools_call(
    id: Value,
    params: Option<Value>,
    engine: &Arc<Engine>,
) -> JsonRpcResponse {
    let params = match params {
        Some(p) => p,
        None => {
            return JsonRpcResponse::error(id, INVALID_PARAMS, "Missing params for tools/call");
        }
    };

    let tool_name = match params.get("name").and_then(|v| v.as_str()) {
        Some(name) => name.to_string(),
        None => {
            return JsonRpcResponse::error(id, INVALID_PARAMS, "Missing 'name' in tools/call");
        }
    };

    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or(Value::Object(serde_json::Map::new()));

    let result = match tool_name.as_str() {
        "tasked_submit_flow" => tool_submit_flow(engine, arguments).await,
        "tasked_flow_status" => tool_flow_status(engine, arguments).await,
        "tasked_task_output" => tool_task_output(engine, arguments).await,
        "tasked_cancel_flow" => tool_cancel_flow(engine, arguments).await,
        "tasked_list_flows" => tool_list_flows(engine, arguments).await,
        "tasked_create_schedule" => tool_create_schedule(engine, arguments).await,
        "tasked_list_schedules" => tool_list_schedules(engine, arguments).await,
        "tasked_delete_schedule" => tool_delete_schedule(engine, arguments).await,
        _ => Err(ToolError::InvalidParam(format!(
            "Unknown tool: {tool_name}"
        ))),
    };

    match result {
        Ok(text) => JsonRpcResponse::success(
            id,
            serde_json::json!({
                "content": [
                    {
                        "type": "text",
                        "text": text
                    }
                ]
            }),
        ),
        Err(ToolError::InvalidParam(msg)) => JsonRpcResponse::error(id, INVALID_PARAMS, msg),
        Err(ToolError::Runtime(err)) => JsonRpcResponse::success(
            id,
            serde_json::json!({
                "content": [
                    {
                        "type": "text",
                        "text": err
                    }
                ],
                "isError": true
            }),
        ),
    }
}

// -- Tool implementations --

/// Input for tasked_submit_flow
#[derive(Debug, Deserialize)]
struct SubmitFlowInput {
    queue: String,
    tasks: Vec<TaskDef>,
    #[serde(default)]
    queue_config: Option<QueueConfig>,
    #[serde(default)]
    fail_fast: bool,
}

async fn tool_submit_flow(engine: &Arc<Engine>, args: Value) -> Result<String, ToolError> {
    let input: SubmitFlowInput = serde_json::from_value(args)
        .map_err(|e| ToolError::param(format!("Invalid arguments: {e}")))?;

    let queue_id = QueueId::from(input.queue);

    // Auto-create queue if it doesn't exist
    if engine
        .get_queue(&queue_id)
        .await
        .map_err(|e| ToolError::runtime(format!("Engine error: {e}")))?
        .is_none()
    {
        let config = input.queue_config.unwrap_or_default();
        engine
            .create_queue(&queue_id, config)
            .await
            .map_err(|e| ToolError::runtime(format!("Failed to create queue: {e}")))?;
    }

    let flow_def = FlowDef {
        tasks: input.tasks,
        webhooks: None,
        fail_fast: input.fail_fast,
    };

    let flow = engine
        .submit_flow(&queue_id, flow_def)
        .await
        .map_err(|e| ToolError::runtime(engine_error_message(e)))?;

    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "flow_id": flow.id.as_str(),
        "queue": flow.queue_id.as_str(),
        "state": flow.state.to_string(),
        "task_count": flow.task_count
    }))
    .unwrap())
}

async fn tool_flow_status(engine: &Arc<Engine>, args: Value) -> Result<String, ToolError> {
    let flow_id_str = args
        .get("flow_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::param("Missing 'flow_id' parameter"))?;

    let flow_id = FlowId::from(flow_id_str);

    let flow = engine
        .get_flow(&flow_id)
        .await
        .map_err(|e| ToolError::runtime(format!("Engine error: {e}")))?
        .ok_or_else(|| ToolError::runtime(format!("Flow '{flow_id_str}' not found")))?;

    let tasks = engine
        .get_flow_tasks(&flow_id)
        .await
        .map_err(|e| ToolError::runtime(format!("Engine error: {e}")))?;

    let task_statuses: Vec<Value> = tasks
        .iter()
        .map(|t| {
            let mut obj = serde_json::json!({
                "id": t.id.as_str(),
                "state": t.state.to_string(),
                "executor": t.executor_type,
            });
            if let Some(ref output) = t.output {
                obj["output"] = output.clone();
            }
            if let Some(ref error) = t.error {
                obj["error"] = Value::String(error.clone());
            }
            obj
        })
        .collect();

    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "flow_id": flow.id.as_str(),
        "queue": flow.queue_id.as_str(),
        "state": flow.state.to_string(),
        "task_count": flow.task_count,
        "tasks_succeeded": flow.tasks_succeeded,
        "tasks_failed": flow.tasks_failed,
        "tasks": task_statuses
    }))
    .unwrap())
}

async fn tool_task_output(engine: &Arc<Engine>, args: Value) -> Result<String, ToolError> {
    let flow_id_str = args
        .get("flow_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::param("Missing 'flow_id' parameter"))?;

    let task_id_str = args
        .get("task_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::param("Missing 'task_id' parameter"))?;

    let flow_id = FlowId::from(flow_id_str);
    let task_id = TaskId::from(task_id_str);

    let task = engine
        .get_task(&task_id, &flow_id)
        .await
        .map_err(|e| ToolError::runtime(format!("Engine error: {e}")))?
        .ok_or_else(|| {
            ToolError::runtime(format!(
                "Task '{task_id_str}' not found in flow '{flow_id_str}'"
            ))
        })?;

    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "task_id": task.id.as_str(),
        "flow_id": task.flow_id.as_str(),
        "state": task.state.to_string(),
        "output": task.output,
        "error": task.error
    }))
    .unwrap())
}

async fn tool_cancel_flow(engine: &Arc<Engine>, args: Value) -> Result<String, ToolError> {
    let flow_id_str = args
        .get("flow_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::param("Missing 'flow_id' parameter"))?;

    let flow_id = FlowId::from(flow_id_str);

    // Verify flow exists
    engine
        .get_flow(&flow_id)
        .await
        .map_err(|e| ToolError::runtime(format!("Engine error: {e}")))?
        .ok_or_else(|| ToolError::runtime(format!("Flow '{flow_id_str}' not found")))?;

    engine
        .cancel_flow(&flow_id)
        .await
        .map_err(|e| ToolError::runtime(format!("Engine error: {e}")))?;

    Ok(format!("Flow '{flow_id_str}' cancelled successfully"))
}

async fn tool_list_flows(engine: &Arc<Engine>, args: Value) -> Result<String, ToolError> {
    let queue_str = args
        .get("queue")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::param("Missing 'queue' parameter"))?;

    let state_filter = args
        .get("state")
        .and_then(|v| v.as_str())
        .map(parse_flow_state)
        .transpose()
        .map_err(ToolError::InvalidParam)?;

    let queue_id = QueueId::from(queue_str);

    let flows = engine
        .list_flows(&queue_id, state_filter)
        .await
        .map_err(|e| ToolError::runtime(format!("Engine error: {e}")))?;

    let flow_summaries: Vec<Value> = flows
        .iter()
        .map(|f| {
            serde_json::json!({
                "flow_id": f.id.as_str(),
                "state": f.state.to_string(),
                "task_count": f.task_count,
                "tasks_succeeded": f.tasks_succeeded,
                "tasks_failed": f.tasks_failed,
                "created_at": f.created_at.to_rfc3339()
            })
        })
        .collect();

    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "queue": queue_str,
        "count": flows.len(),
        "flows": flow_summaries
    }))
    .unwrap())
}

// -- Schedule tool implementations --

#[derive(Debug, Deserialize)]
struct CreateScheduleInput {
    queue: String,
    cron: String,
    flow: FlowDef,
    #[serde(default)]
    name: Option<String>,
    #[serde(default = "default_enabled")]
    enabled: bool,
}

async fn tool_create_schedule(engine: &Arc<Engine>, args: Value) -> Result<String, ToolError> {
    let input: CreateScheduleInput = serde_json::from_value(args)
        .map_err(|e| ToolError::param(format!("Invalid arguments: {e}")))?;

    let queue_id = QueueId::from(input.queue);

    // Auto-create queue if it doesn't exist
    if engine
        .get_queue(&queue_id)
        .await
        .map_err(|e| ToolError::runtime(format!("Engine error: {e}")))?
        .is_none()
    {
        engine
            .create_queue(&queue_id, QueueConfig::default())
            .await
            .map_err(|e| ToolError::runtime(format!("Failed to create queue: {e}")))?;
    }

    let schedule_def = ScheduleDef {
        cron: input.cron,
        flow: input.flow,
        name: input.name,
        enabled: input.enabled,
    };

    let schedule = engine
        .create_schedule(&queue_id, schedule_def)
        .await
        .map_err(|e| ToolError::runtime(engine_error_message(e)))?;

    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "schedule_id": schedule.id.as_str(),
        "queue": schedule.queue_id.as_str(),
        "cron": schedule.cron,
        "enabled": schedule.enabled,
        "next_run_at": schedule.next_run_at.map(|dt| dt.to_rfc3339()),
    }))
    .unwrap())
}

async fn tool_list_schedules(engine: &Arc<Engine>, args: Value) -> Result<String, ToolError> {
    let queue_str = args
        .get("queue")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::param("Missing 'queue' parameter"))?;

    let queue_id = QueueId::from(queue_str);

    let schedules = engine
        .list_schedules(&queue_id)
        .await
        .map_err(|e| ToolError::runtime(format!("Engine error: {e}")))?;

    let schedule_summaries: Vec<Value> = schedules
        .iter()
        .map(|s| {
            serde_json::json!({
                "schedule_id": s.id.as_str(),
                "name": s.name,
                "cron": s.cron,
                "enabled": s.enabled,
                "next_run_at": s.next_run_at.map(|dt| dt.to_rfc3339()),
                "last_triggered_at": s.last_triggered_at.map(|dt| dt.to_rfc3339()),
            })
        })
        .collect();

    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "queue": queue_str,
        "count": schedules.len(),
        "schedules": schedule_summaries
    }))
    .unwrap())
}

async fn tool_delete_schedule(engine: &Arc<Engine>, args: Value) -> Result<String, ToolError> {
    let schedule_id_str = args
        .get("schedule_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::param("Missing 'schedule_id' parameter"))?;

    let schedule_id = ScheduleId::from(schedule_id_str);

    // Verify schedule exists
    engine
        .get_schedule(&schedule_id)
        .await
        .map_err(|e| ToolError::runtime(format!("Engine error: {e}")))?
        .ok_or_else(|| ToolError::runtime(format!("Schedule '{schedule_id_str}' not found")))?;

    engine
        .delete_schedule(&schedule_id)
        .await
        .map_err(|e| ToolError::runtime(format!("Engine error: {e}")))?;

    Ok(format!("Schedule '{schedule_id_str}' deleted successfully"))
}

// -- Helpers --

fn parse_flow_state(s: &str) -> Result<FlowState, String> {
    match s {
        "running" => Ok(FlowState::Running),
        "succeeded" => Ok(FlowState::Succeeded),
        "failed" => Ok(FlowState::Failed),
        "cancelled" => Ok(FlowState::Cancelled),
        other => Err(format!(
            "Invalid flow state: '{other}'. Must be one of: running, succeeded, failed, cancelled"
        )),
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    /// write_message emits exactly one line of JSON terminated by `\n`,
    /// which read_line_capped reads back and serde parses (round-trip).
    #[tokio::test]
    async fn newline_framing_round_trip() {
        let response = JsonRpcResponse::success(
            Value::from(7),
            serde_json::json!({"hello": "world", "n": 42}),
        );

        let mut out: Vec<u8> = Vec::new();
        write_message(&mut out, &response).await.unwrap();

        // Exactly one trailing newline, no embedded newlines.
        assert_eq!(out.last(), Some(&b'\n'));
        assert_eq!(out.iter().filter(|&&b| b == b'\n').count(), 1);

        let mut reader = out.as_slice();
        let line = match read_line_capped(&mut reader, MAX_LINE_BYTES).await.unwrap() {
            ReadLine::Line(l) => l,
            _ => panic!("expected a line"),
        };
        let parsed: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed["jsonrpc"], "2.0");
        assert_eq!(parsed["id"], 7);
        assert_eq!(parsed["result"]["hello"], "world");
        assert_eq!(parsed["result"]["n"], 42);

        // Stream is exhausted afterwards.
        assert!(matches!(
            read_line_capped(&mut reader, MAX_LINE_BYTES).await.unwrap(),
            ReadLine::Eof
        ));
    }

    #[tokio::test]
    async fn read_line_capped_multiple_and_empty_lines() {
        let data = b"{\"a\":1}\n\n{\"b\":2}\n";
        let mut reader = data.as_slice();

        match read_line_capped(&mut reader, 1024).await.unwrap() {
            ReadLine::Line(l) => assert_eq!(l, "{\"a\":1}"),
            _ => panic!("expected first line"),
        }
        // Empty line is returned as an (empty) line; the stdio loop skips it.
        match read_line_capped(&mut reader, 1024).await.unwrap() {
            ReadLine::Line(l) => assert!(l.is_empty()),
            _ => panic!("expected empty line"),
        }
        match read_line_capped(&mut reader, 1024).await.unwrap() {
            ReadLine::Line(l) => assert_eq!(l, "{\"b\":2}"),
            _ => panic!("expected second line"),
        }
        assert!(matches!(
            read_line_capped(&mut reader, 1024).await.unwrap(),
            ReadLine::Eof
        ));
    }

    #[tokio::test]
    async fn read_line_capped_unterminated_final_line() {
        let data = b"{\"a\":1}";
        let mut reader = data.as_slice();
        match read_line_capped(&mut reader, 1024).await.unwrap() {
            ReadLine::Line(l) => assert_eq!(l, "{\"a\":1}"),
            _ => panic!("expected line at EOF without newline"),
        }
        assert!(matches!(
            read_line_capped(&mut reader, 1024).await.unwrap(),
            ReadLine::Eof
        ));
    }

    #[tokio::test]
    async fn read_line_capped_rejects_oversized_line_and_recovers() {
        let mut data = vec![b'x'; 100];
        data.push(b'\n');
        data.extend_from_slice(b"{\"ok\":true}\n");
        let mut reader = data.as_slice();

        // First line exceeds the 10-byte cap and is discarded...
        assert!(matches!(
            read_line_capped(&mut reader, 10).await.unwrap(),
            ReadLine::TooLong
        ));
        // ...but the next message is still readable.
        match read_line_capped(&mut reader, 1024).await.unwrap() {
            ReadLine::Line(l) => assert_eq!(l, "{\"ok\":true}"),
            _ => panic!("expected line after oversized one"),
        }
    }
}

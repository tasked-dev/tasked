[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](../LICENSE)

# tasked-server

HTTP and MCP server binary for the Tasked DAG execution engine.

`tasked-server` wraps the `tasked` core library in an axum HTTP server with a REST API, Prometheus metrics, SSE streaming, and an MCP tool server for AI agent integration. It is distributed as a single static binary.

## Installation

### From source

```sh
git clone https://github.com/tasked-dev/tasked.git
cd tasked
cargo build --release
# Binary at target/release/tasked-server
```

### From crates.io

```sh
cargo install tasked-server
```

## Quick start

### Start the HTTP server

```sh
tasked-server serve --db tasked.db --port 8080
```

### Run a flow from a file and exit

```sh
tasked-server run flow.json --queue default
```

### Start the MCP tool server (stdio)

```sh
tasked-server mcp --db tasked.db
```

### Submit a flow via the API

```sh
curl -X POST http://localhost:8080/api/v1/queues -H 'Content-Type: application/json' \
  -d '{"id": "default"}'

curl -X POST http://localhost:8080/api/v1/queues/default/flows -H 'Content-Type: application/json' \
  -d '{
    "tasks": [
      {"id": "hello", "executor": "shell", "config": {"command": "echo hello world"}}
    ]
  }'
```

## Documentation

Full documentation, architecture details, and API reference are in the [main Tasked README](https://github.com/tasked-dev/tasked).

## License

MIT

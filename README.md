# Kervan-Proxy

A high-performance, zero-dependency, protocol-agnostic L7 application-level proxy and circuit-buffering engine in Go. 

Kervan-Proxy is inspired by the ancient Silk Road **Caravanserais** (*Kervansaray*). It acts as a digital sanctuary: when upstream backend targets undergo deployments or outages, it avoids severing the client connection. Instead, it holds the connection, buffers incoming stream payloads into a highly-optimized in-memory ring-buffer (`Sanctuary`), and flushes them in a strict FIFO sequence once target health is restored.

---

## Key Features

- ⚡ **Zero-Allocation Data Path**: Zero heap allocations on the steady-state streaming data path (`OPEN` state).
- 🔒 **Concurrency Safe**: Atomic CAS (Compare-And-Swap) transitions for safe state routing.
- ⚙️ **Rules Engine**: Programmatic and reactive `WHEN-THEN` interceptor chains.
- 📊 **Telemetry**: Bounded async `EventBus` with optional, transactional PostgreSQL event recorder.
- 🌐 **Protocol Bridging**: Serves as a WebSocket-to-TCP bridge out of the box.

---

## Architecture & Core Primitives

Kervan-Proxy divides stream orchestration into four core primitives:

| Primitive | Description |
|-----------|-------------|
| **Pipeline** | The logical streaming container binding one distinct `Source` (Client) and one `Target` (Upstream). |
| **Valve** | The internal execution gatekeeper controlling stream routing FSM states: `OPEN`, `HELD`, and `DRAINING`. |
| **Sanctuary** | A thread-safe, bounded, in-memory FIFO ring-buffer recycling fixed 4KB blocks from a `sync.Pool`. |
| **Interceptor** | Event-driven predicate rules (`WHEN-THEN` specification) driving automated state transitions. |

### Operational State Transitions

```mermaid
stateDiagram-v2
    [*] --> OPEN : Startup
    OPEN --> HELD : Target Failure / WHEN Condition Met
    HELD --> DRAINING : Target Recovered / Handshake Successful
    DRAINING --> OPEN : Buffer Drained to 0
    DRAINING --> HELD : Cascading Failure (Target drops mid-drain)
```

---

## Quick Start

### 1. Build and Run the Proxy Server

Compile and launch the proxy binary with default options (listens on port `:8080`, forwards to `:9000`):

```sh
# Build binary
make build

# Run with defaults
./build/kervan-proxy

# Run with custom configuration flags
./build/kervan-proxy \
    --listen :9090 \
    --upstream 127.0.0.1:3000 \
    --capacity 5000 \
    --backpressure drop_oldest \
    --max-connections 10000
```

### 2. Docker Compose (Recommended)

Run the proxy with all defaults (listens on `:8080`, forwards to `host.docker.internal:9000`):

```sh
docker compose up
```

With a TCP echo server for local development:

```sh
docker compose --profile dev up
```

With PostgreSQL monitoring enabled:

```sh
docker compose --profile monitoring up
```

### 3. Building the Docker Image

Build the standard image:

```sh
docker build -t kervan-proxy .
```

Build with PostgreSQL monitoring support:

```sh
docker build --build-arg BUILD_TAGS=monitoring -t kervan-proxy:monitoring .
```

### 4. Testing WebSocket Bridging

Since Kervan-Proxy upgrades connections on `/ws` to WebSocket and bridges them to a raw TCP upstream:

```sh
# Terminal 1: Start a TCP backend listener
nc -l 9000

# Terminal 2: Start the proxy
./build/kervan-proxy --listen :8080 --upstream 127.0.0.1:9000

# Terminal 3: Connect using a WebSocket client (e.g. websocat or wscat)
websocat ws://localhost:8080/ws
# Type any text and watch it arrive on the TCP backend (Terminal 1)
```

With the `dev` Docker Compose profile, the echo server is already running:

```sh
websocat ws://localhost:8080/ws
# Type any text and it echoes back
```

### 5. Health Check Telemetry

Query the server stats:

```sh
curl http://localhost:8080/healthz
# Output: {"connections":0,"status":"ok","uptime":"1m23s"}
```

---

## Configuration Reference

All configurations can be loaded via environment variables or CLI flags:

| Flag | Environment Variable | Default | Description |
|------|----------------------|---------|-------------|
| `--listen` | `LISTEN` | `:8080` | Host and port to listen on |
| `--upstream` | `UPSTREAM` | `127.0.0.1:9000` | Upstream backend address |
| `--capacity` | `CAPACITY` | `10000` | Sanctuary buffer capacity per pipeline |
| `--backpressure` | `BACKPRESSURE` | `drop_oldest` | Backpressure mode (`drop_oldest` or `reject_new`) |
| `--max-connections` | `MAX_CONNECTIONS` | `10000` | Max concurrent proxy connections |
| `--upstream-timeout` | `UPSTREAM_TIMEOUT` | `10s` | Upstream target connection dial timeout |
| `--read-timeout` | `READ_TIMEOUT` | `30s` | HTTP connection read timeout |
| `--write-timeout` | `WRITE_TIMEOUT` | `30s` | HTTP connection write timeout |
| `--database-url` | `DATABASE_URL` | *See Description* | Postgres connection URL. Defaults to: `postgres://localhost:5432/kervan?sslmode=disable` |

---

## Using the SDK

Kervan-Proxy is also designed to be imported directly into Go applications.

### Basic Stream Relay (OPEN mode)

```go
import "github.com/ysBayram/kervan-proxy/internal/pipeline"

// Bind a Reader and Writer
p := pipeline.NewPipeline(srcReader, dstWriter)
p.Start()
defer p.Stop()
```

### Manual Valve State Transitions

```go
import "github.com/ysBayram/kervan-proxy/internal/valve"

// Freeze target writes and buffer data in Sanctuary
p.Valve().TransitionTo(valve.HELD)

// Draining phase: flushes buffered data to target in FIFO sequence
p.Valve().TransitionTo(valve.DRAINING)
```

### Registering Interceptor Rules

```go
import "github.com/ysBayram/kervan-proxy/internal/interceptor"

p.RegisterRule(interceptor.Rule{
    Name:      "apply-backpressure",
    OnEvent:   interceptor.EventBackpressure,
    Predicate: interceptor.SanctuaryUsageAbove(p.Sanctuary(), 0.9),
    Action:    interceptor.ApplyBackpressureAction(p.Sanctuary(), "drop_oldest"),
})
```

---

## Telemetry & Event Recording (Optional)

To enable Postgres-backed logging, compile Kervan-Proxy with the `monitoring` build tag:

```sh
# Build with monitoring
make build-monitoring

# Set up PostgreSQL database URL and run proxy
export DATABASE_URL="postgres://postgres:password@localhost:5432/kervan?sslmode=disable"
./build/kervan-proxy
```
*Note: The recorder automatically performs table schema migrations on startup.*

---

## Docker Development Guide

### Profiles Overview

The `docker-compose.yml` defines three profiles for different workflows:

| Profile | Services | Use Case |
|---------|----------|----------|
| `default` | `kervan-proxy` | Standalone proxy for production or integration testing |
| `dev` | `kervan-proxy` + `echo-server` | Local development with a TCP echo server |
| `monitoring` | `kervan-proxy-monitoring` + `postgres` | Proxy with PostgreSQL event recording |

### Dev Profile — WebSocket ↔ TCP Echo

Start the proxy alongside a TCP echo server:

```sh
docker compose --profile dev up
```

The echo server listens on `:9000` and replies with whatever it receives. The proxy forwards WebSocket traffic from `ws://localhost:8080/ws` to the echo server:

```sh
websocat ws://localhost:8080/ws
> hello
< hello
```

### Monitoring Profile — PostgreSQL Event Recording

Start the proxy with PostgreSQL-backed telemetry:

```sh
docker compose --profile monitoring up
```

This starts a PostgreSQL 16 instance (data persisted in `pgdata` named volume) and the proxy binary compiled with the `monitoring` build tag. All pipeline events (valve transitions, backpressure actions, disconnections) are recorded in the `pipeline_events`, `backpressure_events`, and `disruption_sessions` tables. The recorder automatically runs schema migrations on startup.

### Docker Compose Configuration

The `kervan-proxy` service accepts the same environment variables listed in the [Configuration Reference](#configuration-reference). Override any value with an `environment` block:

```yaml
services:
  kervan-proxy:
    environment:
      - LISTEN=:9090
      - CAPACITY=5000
      - BACKPRESSURE=reject_new
```

---

## Project Structure

```
├── cmd/kervan-proxy/        # Main CLI entry point
├── internal/
│   ├── interceptor/         # Rules engine (WHEN-THEN evaluations)
│   ├── monitoring/          # EventBus and pgx database logger (isolated via build tag)
│   ├── pipeline/            # Bi-directional dual-goroutine pipeline manager
│   ├── sanctuary/           # Thread-safe in-memory ring-buffer
│   ├── server/              # HTTP/WebSocket gateway server
│   └── valve/               # Concurrency-safe state controller
├── sql/migrations/          # Database migrations for telemetry
├── docs/                    # Architectural documents and design specifications
├── Makefile                 # Automation targets (build, test, lint, bench)
├── Dockerfile               # Multi-stage Docker image build
├── docker-compose.yml       # Docker Compose with 3 profiles
└── .github/workflows/       # CI pipelines
```

---

## Contributing

Contributions are welcome! Please review [CONTRIBUTING.md](CONTRIBUTING.md) to get started with local development setups, coding conventions, and pull request workflows.

## License

This project is licensed under the MIT License - see [LICENSE](LICENSE) for details.

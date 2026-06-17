# Kervan-Proxy

A zero-dependency, protocol-agnostic L7 proxy and circuit-buffering engine in Go. Kervan-Proxy shields upstream backends by buffering payloads during outages and replaying them in FIFO order once the target recovers.

## Architecture

Kervan-Proxy is built on four core primitives:

| Primitive | Description |
|-----------|-------------|
| **Pipeline** | Logical stream binding one Source and one Target |
| **Valve** | FSM with states `OPEN ↔ HELD ↔ DRAINING` |
| **Sanctuary** | Per-pipeline bounded FIFO ring-buffer (zero-alloc via `sync.Pool`) |
| **Interceptor** | WHEN-THEN predicate rules driving state transitions |

### States

```
OPEN ──→ HELD ──→ DRAINING ──→ OPEN
  │        │         │
  │        └── ← ────┘ (cascading failure)
  └── (target error → preserve frame in Sanctuary)
```

## Quick Start

### Run as a proxy server

```sh
# Build
make build

# Run with defaults (listens :8080, forwards to 127.0.0.1:9000)
./build/kervan-proxy

# With custom flags
./build/kervan-proxy \
    --listen :9090 \
    --upstream 10.0.0.1:3000 \
    --capacity 5000 \
    --backpressure drop_oldest \
    --max-connections 10000
```

### WebSocket echo test

```sh
# Terminal 1: start echo server
nc -l -k -p 9000

# Terminal 2: start proxy
./build/kervan-proxy --listen :8080 --upstream :9000

# Terminal 3: connect via WebSocket
curl -i -N -H "Connection: Upgrade" -H "Upgrade: websocket" \
    -H "Sec-WebSocket-Version: 13" \
    -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
    http://localhost:8080/ws
```

### Health check

```sh
curl http://localhost:8080/healthz
# {"connections":0,"status":"ok","uptime":"1m23s"}
```

## Configuration

All options available as CLI flags or environment variables:

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--listen` | `LISTEN` | `:8080` | Listen address (host:port) |
| `--upstream` | `UPSTREAM` | `127.0.0.1:9000` | Upstream backend address |
| `--capacity` | `CAPACITY` | `10000` | Sanctuary buffer capacity per pipeline |
| `--backpressure` | `BACKPRESSURE` | `drop_oldest` | Backpressure mode (`drop_oldest` or `reject_new`) |
| `--max-connections` | `MAX_CONNECTIONS` | `10000` | Maximum concurrent connections |
| `--upstream-timeout` | `UPSTREAM_TIMEOUT` | `10s` | Upstream dial timeout |
| `--read-timeout` | `READ_TIMEOUT` | `30s` | HTTP read timeout |
| `--write-timeout` | `WRITE_TIMEOUT` | `30s` | HTTP write timeout |

## Using the SDK

### Basic relay (OPEN mode)

```go
import "github.com/ysBayram/kervan-proxy/internal/pipeline"

srcR, srcW := io.Pipe()
var buf bytes.Buffer

p := pipeline.NewPipeline(srcR, &buf)
p.Start()

srcW.Write([]byte("data"))
srcW.Close()
p.Stop()
// buf contains "data"
```

### Circuit breaking (HELD mode)

```go
p.Valve().TransitionTo(valve.HELD)
srcW.Write([]byte("buffered")) // goes to Sanctuary, not target
p.Valve().TransitionTo(valve.DRAINING)
// data drains to target in FIFO order
```

### Interceptors

```go
import "github.com/ysBayram/kervan-proxy/internal/interceptor"

p.RegisterRule(interceptor.Rule{
    Name: "backpressure-threshold",
    Predicate: interceptor.SanctuaryUsageAbove(sanctuary, 0.9),
    Action:    interceptor.ApplyBackpressureAction(sanctuary, "drop_oldest"),
})
```

### WebSocket proxy (bi-directional)

```go
import "github.com/ysBayram/kervan-proxy/internal/server"

cfg := server.ConfigFromFlags()
srv := server.NewProxyServer(cfg)
srv.Start()
defer srv.Stop()
// Listen on :8080/ws, proxies to --upstream
```

## Project Structure

```
├── cmd/kervan-proxy/        # Main binary entry point (proxy server)
├── internal/
│   ├── interceptor/         # Rule engine (WHEN-THEN predicates)
│   ├── monitoring/          # Async EventBus + pgx recorder (build-tag isolated)
│   ├── pipeline/            # Stream orchestrator (dual-goroutine)
│   ├── sanctuary/           # Bounded FIFO ring-buffer
│   ├── server/              # TCP/WebSocket proxy server
│   └── valve/               # Atomic CAS state machine
├── sql/migrations/          # PostgreSQL migration scripts
├── docs/                    # Technical design, implementation plan, results
├── Makefile                 # Build, test, lint, bench targets
└── .github/workflows/       # GitHub Actions CI
```

## Development

### Prerequisites

- Go 1.26+
- `golangci-lint` (optional, for `make lint`)

### Commands

| Command | Description |
|---------|-------------|
| `make build` | Build the proxy binary to `build/kervan-proxy` |
| `make test` | Run all tests with race detector |
| `make lint` | Run golangci-lint (falls back to `go vet`) |
| `make bench` | Run all benchmarks |
| `make fmt` | Format all Go source files |
| `make clean` | Remove build artifacts |

### Monitoring (optional)

Enable PostgreSQL-backed monitoring with the `monitoring` build tag:

```sh
make build-monitoring
# or directly:
go build -tags monitoring ./cmd/kervan-proxy
```

## Scenarios

| # | Scenario | Description |
|---|----------|-------------|
| S1 | Basic Stream Relay | Pipeline starts OPEN, traffic flows as passthrough |
| S2 | Circuit Breaking | Target fails → Valve HELD → payloads buffer → target recovers → Drain → OPEN |
| S3 | Backpressure | Sanctuary near capacity → DropOldest or RejectNew |
| S4 | Cascading Failure | Target dies during DRAINING → revert to HELD, buffer preserved |
| S5 | Zombie Source | Source disconnects while HELD → pipeline drains and cleans up |

## License

MIT

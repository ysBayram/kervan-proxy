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

## States

```
OPEN ──→ HELD ──→ DRAINING ──→ OPEN
  │        │         │
  │        └── ← ────┘ (cascading failure)
  └── (target error → preserve frame in Sanctuary)
```

## Quick Start

```go
package main

import (
    "io"
    "os"
    "github.com/ysBayram/kervan-proxy/internal/pipeline"
)

func main() {
    p := pipeline.NewPipeline(os.Stdin, os.Stdout)
    if err := p.Start(); err != nil {
        panic(err)
    }
    defer p.Stop()
    io.Copy(os.Stdout, os.Stdin) // handled by pipeline
}
```

## Usage

### Basic relay (OPEN mode)

```go
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
p.RegisterRule(interceptor.Rule{
    Name: "backpressure-threshold",
    Predicate: interceptor.SanctuaryUsageAbove(sanctuary, 0.9),
    Action:    interceptor.ApplyBackpressureAction(sanctuary, "drop_oldest"),
})
```

## Project Structure

```
├── cmd/kervan-proxy/        # Main binary entry point
├── internal/
│   ├── interceptor/         # Rule engine (WHEN-THEN predicates)
│   ├── monitoring/           # Async EventBus + pgx recorder (build-tag isolated)
│   ├── pipeline/            # Stream orchestrator (dual-goroutine)
│   ├── sanctuary/           # Bounded FIFO ring-buffer
│   └── valve/               # Atomic CAS state machine
├── sql/migrations/          # PostgreSQL migration scripts
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
| `make build` | Build the proxy binary |
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

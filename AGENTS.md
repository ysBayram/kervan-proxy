# kervan-proxy — Agent Guide

## State

All 9 of 9 phases are fully implemented and verified. All core primitives and server components exist with comprehensive tests and zero lint issues.

| Package | Phase | Status | Key Responsibility |
|---------|-------|--------|---------------------|
| `internal/valve` | P2 | ✅ Tests pass, `-race` clean | Valve FSM (OPEN, HELD, DRAINING states) |
| `internal/sanctuary` | P3 | ✅ Tests pass, `-race` clean | Zero-allocation byte buffer pool and ring buffer |
| `internal/pipeline` | P4, P6 | ✅ Tests pass, `-race` clean | Bidirectional pipeline logic, edge case handling, backpressure |
| `internal/interceptor` | P5 | ✅ Tests pass, `-race` clean | Rule-engine evaluating predicate-action (WHEN-THEN) maps |
| `internal/monitoring` | P7 | ✅ Tests pass, `-race` clean | EventBus telemetry and optional `pgx` event recorder |
| `internal/server` | P9 | ✅ Tests pass, `-race` clean | HTTP/WebSocket proxy server, config loader, connection management |

## Design

`docs/technical-design-document.md` details the architectural layout, and the implementation strictly adheres to:
- **Pipeline** — logical stream container binding one Source and one Target
- **Valve** — atomic CAS transitions driving stream routing states: `OPEN → HELD → DRAINING → OPEN`
- **Sanctuary** — per-pipeline bounded FIFO ring-buffer recycling fixed 4096-byte arrays from a `sync.Pool`
- **Interceptors** — event-driven predicate evaluations determining transition rules
- **Server** — upgrade HTTP requests to WebSocket connection tunnels and relay them via pipelines

## Commands

```sh
make fmt              # Format Go source files
make vet              # Run go vet
make test             # Run tests with race detector (60s timeout)
make test-short       # Run short tests with race detector (30s timeout)
make lint             # Run golangci-lint (falls back to go vet)
make build            # Build server binary into build/kervan-proxy
make build-monitoring # Build binary with pgx monitoring enabled (requires postgres)
make run              # Run proxy server locally
make bench            # Run benchmarks (120s timeout)
make bench-profile    # Run benchmarks and output CPU/Memory profiles
make clean            # Remove build directory and clean Go build cache
make ci-check         # Complete local validation suite (fmt -> vet -> test -> lint)
```

## Conventions

- **Dependencies**: 
  - Standard library only for core packages.
  - `github.com/gorilla/websocket` is restricted to the networking entry points (`internal/server`, `cmd/kervan-proxy`).
  - `github.com/jackc/pgx/v5` is restricted to monitoring telemetry database writes behind the `//go:build monitoring` build tag.
- **Concurrency & Memory**: 
  - Use `sync.Pool` for zero-allocation memory recycling.
  - Strict double-goroutine execution model per active pipeline (Ingestion and Execution paths).
  - All Valve transitions must employ atomic CAS loops.
- **Git workflow**: 
  - `main` is the stable release branch; `develop` holds the latest integrated changes.
  - Changes should be pushed on clean, single-purpose branches. Commits must follow conventional commits style.
  - **No pushing directly to remote main/master branches** (enforced by project guidelines).

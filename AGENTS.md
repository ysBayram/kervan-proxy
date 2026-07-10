# kervan-proxy — Agent Guide

## State

All 11 phases implemented and verified (50 test functions across 6 packages, all `-race` clean, 0 lint issues).

- `internal/valve` — FSM with atomic CAS: OPEN→HELD→DRAINING→OPEN
- `internal/sanctuary` — bounded FIFO ring-buffer with `sync.Pool`; `PopTo(io.Writer)` for zero-alloc drain
- `internal/pipeline` — dual-goroutine loop (ingestion+execution); `Stop(drainTimeout)` with configurable drain phase
- `internal/interceptor` — WHEN-THEN rule engine evaluated inline in execution loop
- `internal/monitoring` — EventBus + optional pgx recorder behind `//go:build monitoring`
- `internal/server` — HTTP/WS gateway; `ResilientUpstream` with reconnect timeout; graceful shutdown

## Architecture

Per WebSocket connection, `runPipelines` wires **two pipelines** sharing the same `ResilientUpstream`:

```
WS client ↔ cancelReader ↔ [ingress: ws→upstream] ↔ ResilientUpstream ↔ TCP backend
WS client ↔ cancelReader ↔ [egress: upstream→ws]  ↔ ResilientUpstream ↔ TCP backend
```

Both `cancelReader` instances share the `runPipelines` context. When either read fails, `cancel()` unblocks `<-ctx.Done()` to trigger shutdown.

**Shutdown deadlock gotcha**: After `ingress.Stop()`, you MUST call `ru.Close()` on the `ResilientUpstream` before `egress.Stop(0)` — otherwise egress ingestion loop stays blocked in `conn.Read()`.

Valve FSM: `OPEN` (direct write)→`HELD` (buffer in sanctuary)→`DRAINING` (flush)→`OPEN`. DRAINING→HELD on cascading write failure.

## Commands

```sh
make fmt              # go fmt ./...
make vet              # go vet ./...
make test             # go test -race -count=1 -timeout 60s ./...
make test-short       # same but -short -timeout 30s
make lint             # golangci-lint (v2); falls back to go vet
make build            # builds into build/kervan-proxy
make build-monitoring # go build -tags monitoring
make run              # go run ./cmd/kervan-proxy
make bench            # go test -bench=. -benchmem -timeout 120s ./...
make ci-check         # fmt → vet → test → lint (canonical pre-push gate)
```

## Repo-specific Conventions

- **Dependencies**: stdlib only for core packages. gorilla/websocket restricted to `internal/server` + `cmd/`. pgx isolated behind `//go:build monitoring`.
- **API changes since P10**: `Pipeline.Stop()` → `Stop(drainTimeout time.Duration)`. `NewResilientUpstream(ctx, addr, dialTimeout, reconnectTimeout)` — 4 params now. `newWSAdapter(conn)` returns `(io.Reader, io.Writer, func())`.
- **cancelReader** wraps source readers with context check + auto-cancel on read error. Stores `ctx` field checked at Read entry.
- **Active conn tracking**: `activeWSConns sync.Map` + `activeConns sync.WaitGroup`. `Stop()` calls `SetReadDeadline(time.Now())` on all active WS conns to interrupt pending reads.
- **Ping/pong**: 30s ping ticker, 60s pong read deadline set before each `ReadMessage`. Shared `writeMu` mutex guards all writes.
- **Valve transitions**: Must use `atomic.CompareAndSwapInt32`. Single `v.State()` read for both `CanTransitionTo` check and CAS operand (prevents TOCTOU).
- **Config**: flag `--listen` / env `LISTEN`, `--upstream` / `UPSTREAM`, `--capacity` / `CAPACITY` etc. `envStr`/`envInt`/`envDur` helpers. Default reconnect timeout 30s, shutdown drain timeout 30s.
- **Backpressure modes**: `drop_oldest` (default), `reject_new`, `drop_connection` (cancels pipeline).
- **Build tag**: `monitoring` enables pgx recorder + auto-migration on startup.

## Git

- Branch from `develop`, not `main`. Feature branches: `feature/<phase>-<desc>`.
- Conventional commits: `feat(<scope>):`, `fix(<scope>):`, `docs(<scope>):`, `chore(<scope>):`, `perf(<scope>):`.
- No direct pushes to `main`/`master`. PRs require passing `make ci-check`.

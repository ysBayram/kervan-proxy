# kervan-proxy — Agent Guide

## State

Implemented across 7 of 8 phases. All core primitives exist with tests:

| Package | Phase | Status |
|---------|-------|--------|
| `internal/valve` | P2 | ✅ Tests pass, `-race` clean |
| `internal/sanctuary` | P3 | ✅ Tests pass, `-race` clean |
| `internal/pipeline` | P4, P6 | ✅ Tests pass, `-race` clean |
| `internal/interceptor` | P5 | ✅ Tests pass, `-race` clean |
| `internal/monitoring` | P7 | ✅ Tests pass, `-race` clean (build-tag isolated pgx) |

## Design

`docs/technical-design-document.md` is aspirational — the code is the source of truth. Core primitives:
- **Pipeline** — logical stream container binding one Source and one Target
- **Valve** — FSM with states `OPEN → HELD → DRAINING → OPEN` (atomic CAS transitions)
- **Sanctuary** — per-pipeline bounded FIFO ring-buffer (zero-alloc via `sync.Pool`)
- **Interceptors** — `WHEN-THEN` predicate rules driving state transitions

## Commands

```sh
make test      # go test -race -count=1 -timeout 60s ./...
make lint      # golangci-lint run ./... (falls back to go vet)
make build     # go build -o build/kervan-proxy ./cmd/kervan-proxy
make bench     # go test -bench=. -benchmem ./...
make fmt       # go fmt ./...
make vet       # go vet ./...
make clean     # rm -rf build/ && go clean -cache
```

## Implementation plan

`docs/implementation-plan.md` contains the phased build plan. Monitoring (Phase 7) uses pgx + PostgreSQL — the only planned external dependency, isolated in `internal/monitoring/` behind a build tag.

## Conventions

- Zero external dependencies for core; pgx for monitoring behind `//go:build monitoring`
- `sync.Pool` for zero-allocation byte buffers; atomic CAS for valve FSM
- Double-goroutine per pipeline (ingestion / execution)
- Workflow: `make fmt` → `make vet` → `make test` → `make build`
- Branching: `main` (stable), `develop`, `feature/*`, `hotfix/*`; conventional commits
- Every feature/fix/phase implemented on separate branch with micro-commits per subtask

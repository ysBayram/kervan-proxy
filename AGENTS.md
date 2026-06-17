# kervan-proxy — Agent Guide

## State

Greenfield Go project. No `.go` files, `go.mod`, or `go.sum` exist yet. All guidance is aspirational.

`CONTRIBUTING.md` exists but is aspirational — it references a `Makefile`, `golangci-lint`, Docker, and commands (`make test`, `make lint`, `make build`, `make bench`) that do not exist yet. Treat its commands as intended but unverified.

## Design

`docs/technical-design-document.md` is the single source of truth for intended architecture. Core primitives:
- **Pipeline** — logical stream container binding one Source and one Target
- **Valve** — FSM with states `OPEN → HELD → DRAINING → OPEN` (atomic CAS transitions)
- **Sanctuary** — per-pipeline bounded FIFO ring-buffer (zero-alloc via `sync.Pool`)
- **Interceptors** — `WHEN-THEN` predicate rules driving state transitions

## Getting started

```sh
go mod init github.com/ysBayram/kervan-proxy
```

No test / lint / build commands are wired yet.

## Implementation plan

`docs/implementation-plan.md` contains the phased build plan (8 phases, ~16 days). Follow it for implementation order. Monitoring (Phase 7) uses pgx + PostgreSQL — the only planned external dependency, isolated in `internal/monitoring/` behind a build tag.

## Conventions

- Zero external dependencies (stdlib only)
- `sync.Pool` for zero-allocation byte buffers; atomic CAS for valve FSM
- Double-goroutine per pipeline (ingestion / execution)
- Intended workflow (not yet enforced): `go fmt ./...` → `go vet ./...` → `go test -race ./...` → `go build ./...`
- Branching intent: `main` (stable), `develop`, `feature/*`, `hotfix/*`; conventional commits
- Every feature/fix/phase will be implemented seperated branch with micro commit based on each sub tasks 

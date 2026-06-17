# Contributing to Kervan-Proxy

First off, thank you for taking the time to contribute! Contributions are what make the open-source community such an amazing place to learn, inspire, and create.

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) (TBD) before contributing.

---

## How Can I Contribute?

### Reporting Bugs
If you find a bug, please open an issue on GitHub. Before submitting, please:
- Search existing issues to ensure it hasn't already been reported.
- Use a clear and descriptive title.
- Describe the steps to reproduce, the expected behavior, and the actual behavior.
- Include environment details (OS, Go version, etc.) and any relevant log output or stack traces.

### Suggesting Enhancements
If you have an idea for a feature or improvement:
- Open a feature request issue.
- Explain the use case and why this change would benefit Kervan-Proxy.
- Describe the proposed design or implementation if you have one.

### Submitting Pull Requests
1. Fork the repository and create your branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. Write clean, idiomatic Go code.
3. Ensure your changes follow the existing project design and guidelines in the [docs](docs/) directory.
4. Add unit tests for any new functionality.
5. Verify that all tests pass:
   ```bash
   make test
   ```
6. Format your code:
   ```bash
   go fmt ./...
   ```
7. Run the linter:
   ```bash
   make lint
   ```
8. Commit your changes. We recommend using [Conventional Commits](https://www.conventionalcommits.org/):
   ```bash
   git commit -m "feat(sanctuary): add support for custom backpressure action"
   ```
9. Push to your fork and submit a Pull Request targeting the `main` branch.

---

## Development Setup

### Prerequisites
- **Go**: Version 1.26 or higher.
- **Docker**: For running external coordination stores like etcd or Redis during local testing.
- **Make**: For running Makefile tasks.

### Local Development Lifecycle
- **Building the proxy:**
  ```bash
  make build
  ```
- **Running tests:**
  ```bash
  make test
  ```
- **Running benchmarks:**
  ```bash
  make bench
  ```

---

## Coding Guidelines
- **Go Idioms**: Write clean, modern, and idiomatic Go code. Refer to [Effective Go](https://go.dev/doc/effective_go) and the [Go Wiki Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments).
- **No Hot-path Allocations**: In performance-critical areas (like NetPoller, SessionManager, Ring Buffer), minimize allocations. Use `sync.Pool` where appropriate and verify with memory benchmarks.
- **Concurrency & Safety**: Always prioritize race-free code. Use Go's race detector during testing:
  ```bash
  go test -race ./...
  ```
- **Documentation**: Document public APIs, structs, and interfaces. Document complex logic inline where necessary.

### Branching Strategy
We follow a GitFlow-inspired strategy:
- **`main`**: Highly stable, production-ready code. Commits trigger ArgoCD deployments.
- **`develop`**: Primary integration branch.
- **`feature/<ticket-id>-<short-desc>`**: Used for developing new functionalities (e.g., `feature/123-kafka-consumer`).
- **`hotfix/<ticket-id>-<desc>`**: Urgent fixes branching directly from `main`.
- **Merge Policy**: All PRs must have 2 approvals, pass `golangci-lint`, and maintain 80%+ test coverage.

.PHONY: build build-monitoring test test-short lint vet fmt bench bench-profile clean run ci-check

APP_NAME := kervan-proxy
BUILD_DIR := ./build
LINT_BIN := $(shell command -v golangci-lint 2>/dev/null || echo "")

build:
	@echo "Building $(APP_NAME)..."
	go build -o $(BUILD_DIR)/$(APP_NAME) ./cmd/$(APP_NAME)
	@echo "Build complete: $(BUILD_DIR)/$(APP_NAME)"

build-monitoring:
	@echo "Building $(APP_NAME) with monitoring..."
	go build -tags monitoring -o $(BUILD_DIR)/$(APP_NAME) ./cmd/$(APP_NAME)
	@echo "Build complete: $(BUILD_DIR)/$(APP_NAME)"

test:
	go test -race -count=1 -timeout 60s ./...

test-short:
	go test -race -count=1 -short -timeout 30s ./...

lint:
	@if [ -n "$(LINT_BIN)" ]; then \
		$(LINT_BIN) run ./... --timeout 5m; \
	elif [ -x ~/go/bin/golangci-lint-v2 ]; then \
		~/go/bin/golangci-lint-v2 run ./... --timeout 5m; \
	else \
		echo "golangci-lint not found; falling back to go vet"; \
		go vet ./...; \
	fi

vet:
	go vet ./...

fmt:
	go fmt ./...

fmt-check:
	@if go fmt ./... 2>&1 | grep -q .; then \
		echo "Unformatted files found; run make fmt"; \
		exit 1; \
	fi

bench:
	go test -bench=. -benchmem -timeout 120s ./...

bench-profile:
	go test -bench=. -benchmem -cpuprofile cpu.prof -memprofile mem.prof -timeout 120s ./...

clean:
	rm -rf $(BUILD_DIR)/
	go clean -cache

run:
	go run ./cmd/$(APP_NAME)

ci-check: fmt-check vet test lint

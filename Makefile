.PHONY: build test lint clean install check bench-memory libcbm service-graph

BINARY=codebase-memory-mcp
MODULE=github.com/DeusData/codebase-memory-mcp

build:
	go build -o bin/$(BINARY) ./cmd/codebase-memory-mcp/

test:
	go test ./... -v

check: lint test  ## Run lint + tests

lint:  ## Run golangci-lint
	golangci-lint run --timeout=5m ./...

clean:
	rm -rf bin/
	$(MAKE) -f Makefile.cbm clean-c
	rm -rf service-graph/bin

install:
	go install ./cmd/codebase-memory-mcp/

bench-memory:  ## Run memory stability benchmark
	go test -run TestMemoryStability -v -count=1 -timeout=5m ./internal/pipeline/

# ── CGo / service-graph targets ──────────────────────────────

libcbm:  ## Build static C library for CGo embedding
	$(MAKE) -f Makefile.cbm libcbm

service-graph: libcbm  ## Build service-graph Go binary (depends on libcbm)
	cd service-graph && CGO_ENABLED=1 GOARCH=$(shell uname -m | sed 's/aarch64/arm64/' | sed 's/x86_64/amd64/') go build -o bin/codebase-memory-mcp .

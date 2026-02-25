.PHONY: build test lint clean install check

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

install:
	go install ./cmd/codebase-memory-mcp/

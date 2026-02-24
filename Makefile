.PHONY: build test lint clean install

BINARY=codebase-memory-mcp
MODULE=github.com/DeusData/codebase-memory-mcp

build:
	go build -o bin/$(BINARY) ./cmd/codebase-memory-mcp/

test:
	go test ./... -v

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/

install:
	go install ./cmd/codebase-memory-mcp/

package main

import (
	"context"
	"log"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
	"github.com/DeusData/codebase-memory-mcp/internal/tools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	s, err := store.Open("codebase-memory")
	if err != nil {
		log.Fatalf("store open err=%v", err)
	}
	defer s.Close()

	srv := tools.NewServer(s)

	if err := srv.MCPServer().Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		log.Fatalf("server err=%v", err)
	}
}

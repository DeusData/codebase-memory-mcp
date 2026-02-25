package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/DeusData/codebase-memory-mcp/internal/store"
	"github.com/DeusData/codebase-memory-mcp/internal/tools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		fmt.Println("codebase-memory-mcp", version)
		os.Exit(0)
	}
	s, err := store.Open("codebase-memory")
	if err != nil {
		log.Fatalf("store open err=%v", err)
	}

	srv := tools.NewServer(s)

	runErr := srv.MCPServer().Run(context.Background(), &mcp.StdioTransport{})
	s.Close()
	if runErr != nil {
		log.Fatalf("server err=%v", runErr)
	}
}

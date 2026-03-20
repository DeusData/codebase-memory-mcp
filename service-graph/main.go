package main

import (
	"fmt"
	"os"

	"github.com/Shidfar/codebase-memory-mcp/service-graph/mcp"
)

func main() {
	srv, err := mcp.NewServer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal: %v\n", err)
		os.Exit(1)
	}
	defer srv.Close()

	if err := srv.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal: %v\n", err)
		os.Exit(1)
	}
}

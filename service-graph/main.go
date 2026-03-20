package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Shidfar/codebase-memory-mcp/service-graph/mcp"
)

func main() {
	ui := flag.Bool("ui", false, "Start HTTP UI server instead of stdio MCP")
	port := flag.Int("port", 9749, "HTTP server port (used with --ui)")
	flag.Parse()

	srv, err := mcp.NewServer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal: %v\n", err)
		os.Exit(1)
	}
	defer srv.Close()

	if *ui {
		httpSrv := mcp.NewHTTPServer(srv, *port)
		if err := httpSrv.ListenAndServe(); err != nil {
			fmt.Fprintf(os.Stderr, "HTTP server error: %v\n", err)
			os.Exit(1)
		}
	} else {
		if err := srv.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "Fatal: %v\n", err)
			os.Exit(1)
		}
	}
}

package mcp_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/mcp"
)

// echoServer is a tiny Go program used as a fake codebase-memory-mcp binary.
// It reads a JSON-RPC request from stdin and echoes a fixed response to stdout.
const echoServerSrc = `
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" { continue }
		var req map[string]interface{}
		if err := json.Unmarshal([]byte(line), &req); err != nil { continue }

		id := req["id"]
		method, _ := req["method"].(string)

		switch method {
		case "initialize":
			resp := map[string]interface{}{
				"jsonrpc": "2.0", "id": id,
				"result": map[string]interface{}{
					"protocolVersion": "2024-11-05",
					"capabilities":    map[string]interface{}{"tools": map[string]interface{}{}},
					"serverInfo":      map[string]interface{}{"name": "codebase-memory-mcp", "version": "0.5.5"},
				},
			}
			b, _ := json.Marshal(resp)
			fmt.Println(string(b))
		case "tools/call":
			params, _ := req["params"].(map[string]interface{})
			toolName, _ := params["name"].(string)
			resp := map[string]interface{}{
				"jsonrpc": "2.0", "id": id,
				"result": map[string]interface{}{
					"content": []interface{}{
						map[string]interface{}{"type": "text", "text": "ok:" + toolName},
					},
					"isError": false,
				},
			}
			b, _ := json.Marshal(resp)
			fmt.Println(string(b))
		default:
			resp := map[string]interface{}{
				"jsonrpc": "2.0", "id": id,
				"error": map[string]interface{}{"code": -32601, "message": "method not found"},
			}
			b, _ := json.Marshal(resp)
			fmt.Println(string(b))
		}
	}
}
`

// buildEchoServer compiles the echo server and returns its path.
func buildEchoServer(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	// Write source
	srcPath := dir + "/main.go"
	if err := os.WriteFile(srcPath, []byte(echoServerSrc), 0600); err != nil {
		t.Fatalf("write echo server src: %v", err)
	}

	// Init module
	cmd := exec.Command("go", "mod", "init", "echoserver")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go mod init: %v\n%s", err, out)
	}

	// Build
	binPath := dir + "/echoserver"
	cmd = exec.Command("go", "build", "-o", binPath, ".")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build echo server: %v\n%s", err, out)
	}

	return binPath
}

func TestClient_Initialize(t *testing.T) {
	bin := buildEchoServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := mcp.NewClient(ctx, bin)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	info := c.ServerInfo()
	if info.Name != "codebase-memory-mcp" {
		t.Errorf("ServerInfo.Name: want codebase-memory-mcp, got %q", info.Name)
	}
	if info.Version != "0.5.5" {
		t.Errorf("ServerInfo.Version: want 0.5.5, got %q", info.Version)
	}
}

func TestClient_CallTool_Success(t *testing.T) {
	bin := buildEchoServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := mcp.NewClient(ctx, bin)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	result, err := c.CallTool(ctx, "list_projects", nil)
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if len(result.Content) == 0 {
		t.Fatal("CallTool: expected content, got empty")
	}
	text := result.Content[0].Text
	if !strings.HasPrefix(text, "ok:") {
		t.Errorf("CallTool: unexpected response %q", text)
	}
}

func TestClient_CallTool_IndexRepository(t *testing.T) {
	bin := buildEchoServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := mcp.NewClient(ctx, bin)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	params := map[string]interface{}{
		"repo_path": "/tmp/test-repo",
		"mode":      "full",
	}
	result, err := c.CallTool(ctx, "index_repository", params)
	if err != nil {
		t.Fatalf("CallTool index_repository: %v", err)
	}
	if result.IsError {
		t.Errorf("CallTool: unexpected error result")
	}
}

func TestClient_CallTool_Timeout(t *testing.T) {
	bin := buildEchoServer(t)
	// Very short timeout — should cause context deadline exceeded
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Give enough time to start but the tool call will use the expired ctx
	startCtx, startCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer startCancel()

	c, err := mcp.NewClient(startCtx, bin)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	// Cancel before calling
	cancel()
	_, err = c.CallTool(ctx, "list_projects", nil)
	if err == nil {
		t.Error("CallTool: expected error from cancelled context, got nil")
	}
}

func TestClient_SerializeParams(t *testing.T) {
	// Ensure params are correctly serialized to JSON
	params := map[string]interface{}{
		"repo_path": "/app/fleet-cache/membership-backend",
		"mode":      "moderate",
	}
	b, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	var roundtrip map[string]interface{}
	if err := json.Unmarshal(b, &roundtrip); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if roundtrip["mode"] != "moderate" {
		t.Errorf("mode: want moderate, got %v", roundtrip["mode"])
	}
}

func TestClient_Close_Idempotent(t *testing.T) {
	bin := buildEchoServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := mcp.NewClient(ctx, bin)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.Close()
	c.Close() // should not panic
}

func TestClient_RemainsUsableAfterInitContextCancel(t *testing.T) {
	bin := buildEchoServer(t)
	startCtx, cancel := context.WithCancel(context.Background())

	c, err := mcp.NewClient(startCtx, bin)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	cancel()
	time.Sleep(100 * time.Millisecond)

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()

	result, err := c.CallTool(callCtx, "list_projects", nil)
	if err != nil {
		t.Fatalf("CallTool after init context cancel: %v", err)
	}
	if len(result.Content) == 0 {
		t.Fatal("CallTool after init context cancel: expected content, got empty")
	}
}

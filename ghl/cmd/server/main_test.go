package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/bridge"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/mcp"
)

type fakeBridgeClient struct {
	info       mcp.ServerInfo
	callMethod string
	callParams interface{}
	callResult json.RawMessage
	callErr    error
	toolName   string
	toolArgs   map[string]interface{}
	toolResult *mcp.ToolResult
	toolErr    error
}

func (f *fakeBridgeClient) ServerInfo() mcp.ServerInfo {
	return f.info
}

func (f *fakeBridgeClient) Call(_ context.Context, method string, params interface{}) (json.RawMessage, error) {
	f.callMethod = method
	f.callParams = params
	return f.callResult, f.callErr
}

func (f *fakeBridgeClient) CallTool(_ context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error) {
	f.toolName = name
	f.toolArgs = params
	return f.toolResult, f.toolErr
}

func TestMCPBridgeBackendInitializeNegotiatesProtocol(t *testing.T) {
	backend := &mcpBridgeBackend{
		client: &fakeBridgeClient{
			info: mcp.ServerInfo{Name: "codebase-memory-mcp", Version: "0.10.0"},
		},
	}

	raw, err := backend.Call("initialize", json.RawMessage(`{"protocolVersion":"2025-03-26"}`))
	if err != nil {
		t.Fatalf("initialize: %v", err)
	}

	var result struct {
		ProtocolVersion string                 `json:"protocolVersion"`
		Capabilities    map[string]interface{} `json:"capabilities"`
		ServerInfo      mcp.ServerInfo         `json:"serverInfo"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("parse initialize result: %v", err)
	}

	if result.ProtocolVersion != "2025-03-26" {
		t.Errorf("protocolVersion: want 2025-03-26, got %q", result.ProtocolVersion)
	}
	if result.ServerInfo.Version != "0.10.0" {
		t.Errorf("server version: want 0.10.0, got %q", result.ServerInfo.Version)
	}
	if _, ok := result.Capabilities["tools"]; !ok {
		t.Errorf("capabilities.tools: expected tools capability")
	}
}

func TestMCPBridgeBackendForwardsToolsList(t *testing.T) {
	client := &fakeBridgeClient{
		callResult: json.RawMessage(`{"tools":[{"name":"list_projects"}]}`),
	}
	backend := &mcpBridgeBackend{client: client}

	raw, err := backend.Call("tools/list", nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}

	if client.callMethod != "tools/list" {
		t.Errorf("call method: want tools/list, got %q", client.callMethod)
	}
	if string(raw) != `{"tools":[{"name":"list_projects"}]}` {
		t.Errorf("raw result: got %s", raw)
	}
}

func TestMCPBridgeBackendForwardsToolsCall(t *testing.T) {
	client := &fakeBridgeClient{
		toolResult: &mcp.ToolResult{
			Content: []mcp.Content{{Type: "text", Text: "ok"}},
		},
	}
	backend := &mcpBridgeBackend{client: client}

	raw, err := backend.Call("tools/call", json.RawMessage(`{"name":"list_projects","arguments":{"project":"demo"}}`))
	if err != nil {
		t.Fatalf("tools/call: %v", err)
	}

	if client.toolName != "list_projects" {
		t.Errorf("tool name: want list_projects, got %q", client.toolName)
	}
	if got := client.toolArgs["project"]; got != "demo" {
		t.Errorf("tool args.project: want demo, got %v", got)
	}
	if string(raw) != `{"content":[{"type":"text","text":"ok"}],"isError":false}` {
		t.Errorf("raw result: got %s", raw)
	}
}

func TestMCPBridgeBackendRejectsUnknownMethod(t *testing.T) {
	backend := &mcpBridgeBackend{client: &fakeBridgeClient{}}

	_, err := backend.Call("resources/list", nil)
	if err == nil {
		t.Fatal("expected error for unknown method")
	}
	if err != bridge.ErrMethodNotFound {
		t.Fatalf("want ErrMethodNotFound, got %v", err)
	}
}

type fakeIndexToolClient struct {
	inFlight  *atomic.Int64
	maxFlight *atomic.Int64
	delay     time.Duration
	toolErr   error
	result    *mcp.ToolResult
}

func (f *fakeIndexToolClient) CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error) {
	if name != "index_repository" {
		return nil, errors.New("unexpected tool")
	}
	current := f.inFlight.Add(1)
	for {
		old := f.maxFlight.Load()
		if current <= old || f.maxFlight.CompareAndSwap(old, current) {
			break
		}
	}
	defer f.inFlight.Add(-1)

	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.toolErr != nil {
		return nil, f.toolErr
	}
	if f.result != nil {
		return f.result, nil
	}
	return &mcp.ToolResult{}, nil
}

func (f *fakeIndexToolClient) Close() {}

func TestMCPIndexClientPoolRunsConcurrentIndexing(t *testing.T) {
	var inFlight atomic.Int64
	var maxFlight atomic.Int64

	prevFactory := newIndexToolClient
	newIndexToolClient = func(ctx context.Context, binPath string) (indexToolClient, error) {
		return &fakeIndexToolClient{
			inFlight:  &inFlight,
			maxFlight: &maxFlight,
			delay:     20 * time.Millisecond,
		}, nil
	}
	defer func() { newIndexToolClient = prevFactory }()

	pool, err := newMCPIndexClientPool(context.Background(), "/tmp/cbm", 3)
	if err != nil {
		t.Fatalf("newMCPIndexClientPool: %v", err)
	}
	defer pool.Close()

	errCh := make(chan error, 6)
	for i := 0; i < 6; i++ {
		go func() {
			errCh <- pool.IndexRepository(context.Background(), "/tmp/repo", "moderate")
		}()
	}
	for i := 0; i < 6; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("IndexRepository: %v", err)
		}
	}

	if got := maxFlight.Load(); got < 2 {
		t.Fatalf("max concurrent workers: want >= 2, got %d", got)
	}
	if got := maxFlight.Load(); got > 3 {
		t.Fatalf("max concurrent workers: want <= 3, got %d", got)
	}
}

func TestMCPIndexClientPoolPropagatesToolErrors(t *testing.T) {
	prevFactory := newIndexToolClient
	newIndexToolClient = func(ctx context.Context, binPath string) (indexToolClient, error) {
		return &fakeIndexToolClient{
			inFlight:  &atomic.Int64{},
			maxFlight: &atomic.Int64{},
			result: &mcp.ToolResult{
				IsError: true,
				Content: []mcp.Content{{Type: "text", Text: "bad repo"}},
			},
		}, nil
	}
	defer func() { newIndexToolClient = prevFactory }()

	pool, err := newMCPIndexClientPool(context.Background(), "/tmp/cbm", 1)
	if err != nil {
		t.Fatalf("newMCPIndexClientPool: %v", err)
	}
	defer pool.Close()

	err = pool.IndexRepository(context.Background(), "/tmp/repo", "full")
	if err == nil {
		t.Fatal("expected tool error")
	}
	if got := err.Error(); got != "index_repository: bad repo" {
		t.Fatalf("unexpected error: %s", got)
	}
}

func TestIsGitHubHTTPSAuthError(t *testing.T) {
	if !isGitHubHTTPSAuthError("fatal: could not read Username for 'https://github.com': No such device or address") {
		t.Fatal("expected GitHub HTTPS auth error to be detected")
	}
	if isGitHubHTTPSAuthError("fatal: some other git failure") {
		t.Fatal("unexpected auth error match")
	}
}

func TestHasWorkingTreeFilesRejectsGitOnlyClone(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatalf("mkdir .git: %v", err)
	}

	ok, err := hasWorkingTreeFiles(root)
	if err != nil {
		t.Fatalf("hasWorkingTreeFiles: %v", err)
	}
	if ok {
		t.Fatal("expected git-only directory to be rejected")
	}
}

func TestHasWorkingTreeFilesAcceptsCheckedOutFile(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatalf("mkdir .git: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "package.json"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}

	ok, err := hasWorkingTreeFiles(root)
	if err != nil {
		t.Fatalf("hasWorkingTreeFiles: %v", err)
	}
	if !ok {
		t.Fatal("expected checked out file to be accepted")
	}
}

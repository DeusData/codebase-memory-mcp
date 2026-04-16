package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/bridge"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/discovery"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/mcp"
)

type fakeRequestAuthenticator struct {
	token string
	calls int
}

func (f *fakeRequestAuthenticator) Authenticate(_ context.Context, bearerToken string) error {
	f.calls++
	if bearerToken != f.token {
		return errors.New("unauthorized")
	}
	return nil
}

type fakeBridgeClient struct {
	info       mcp.ServerInfo
	callCtx    context.Context
	callMethod string
	callParams interface{}
	callResult json.RawMessage
	callErr    error
	toolCtx    context.Context
	toolName   string
	toolArgs   map[string]interface{}
	toolResult *mcp.ToolResult
	toolErr    error
}

func (f *fakeBridgeClient) ServerInfo() mcp.ServerInfo {
	return f.info
}

func (f *fakeBridgeClient) Call(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	f.callCtx = ctx
	f.callMethod = method
	f.callParams = params
	return f.callResult, f.callErr
}

func (f *fakeBridgeClient) CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error) {
	f.toolCtx = ctx
	f.toolName = name
	f.toolArgs = params
	return f.toolResult, f.toolErr
}

type fakeDiscoverer struct {
	definition discovery.ToolDefinition
	request    discovery.Request
	response   discovery.Response
	err        error
}

func (f *fakeDiscoverer) Definition() discovery.ToolDefinition {
	return f.definition
}

func (f *fakeDiscoverer) DiscoverProjects(_ context.Context, req discovery.Request) (discovery.Response, error) {
	f.request = req
	return f.response, f.err
}

func TestMCPBridgeBackendInitializeNegotiatesProtocol(t *testing.T) {
	backend := &mcpBridgeBackend{
		client: &fakeBridgeClient{
			info: mcp.ServerInfo{Name: "codebase-memory-mcp", Version: "0.10.0"},
		},
	}

	raw, err := backend.Call(context.Background(), "initialize", json.RawMessage(`{"protocolVersion":"2025-03-26"}`))
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

	raw, err := backend.Call(context.Background(), "tools/list", nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}

	if client.callMethod != "tools/list" {
		t.Errorf("call method: want tools/list, got %q", client.callMethod)
	}
	if client.callCtx == nil {
		t.Error("call ctx: expected non-nil context")
	}
	if string(raw) != `{"tools":[{"name":"list_projects"}]}` {
		t.Errorf("raw result: got %s", raw)
	}
}

func TestMCPBridgeBackendToolsListIncludesDiscoverProjects(t *testing.T) {
	client := &fakeBridgeClient{
		callResult: json.RawMessage(`{"tools":[{"name":"list_projects"}]}`),
	}
	backend := &mcpBridgeBackend{
		client: client,
		discovery: &fakeDiscoverer{
			definition: discovery.ToolDefinition{
				Name:        "discover_projects",
				Description: "Discover likely repos",
				InputSchema: map[string]interface{}{"type": "object"},
			},
		},
	}

	raw, err := backend.Call(context.Background(), "tools/list", nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}

	var result struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("parse tools/list result: %v", err)
	}

	if len(result.Tools) != 2 {
		t.Fatalf("tools count: want 2, got %d", len(result.Tools))
	}
	if result.Tools[0].Name != "list_projects" {
		t.Fatalf("first tool: want list_projects, got %q", result.Tools[0].Name)
	}
	if result.Tools[1].Name != "discover_projects" {
		t.Fatalf("second tool: want discover_projects, got %q", result.Tools[1].Name)
	}
}

func TestMCPBridgeBackendForwardsToolsCall(t *testing.T) {
	client := &fakeBridgeClient{
		toolResult: &mcp.ToolResult{
			Content: []mcp.Content{{Type: "text", Text: "ok"}},
		},
	}
	backend := &mcpBridgeBackend{client: client}

	raw, err := backend.Call(context.Background(), "tools/call", json.RawMessage(`{"name":"list_projects","arguments":{"project":"demo"}}`))
	if err != nil {
		t.Fatalf("tools/call: %v", err)
	}

	if client.toolName != "list_projects" {
		t.Errorf("tool name: want list_projects, got %q", client.toolName)
	}
	if client.toolCtx == nil {
		t.Error("tool ctx: expected non-nil context")
	}
	if got := client.toolArgs["project"]; got != "demo" {
		t.Errorf("tool args.project: want demo, got %v", got)
	}
	if string(raw) != `{"content":[{"type":"text","text":"ok"}],"isError":false}` {
		t.Errorf("raw result: got %s", raw)
	}
}

func TestMCPBridgeBackendHandlesDiscoverProjects(t *testing.T) {
	backend := &mcpBridgeBackend{
		client: &fakeBridgeClient{},
		discovery: &fakeDiscoverer{
			response: discovery.Response{
				Query: "membership checkout lock",
				PrimaryRepos: []discovery.Candidate{
					{Project: "app-fleet-cache-membership-backend", RepoSlug: "membership-backend"},
				},
			},
		},
	}

	raw, err := backend.Call(context.Background(), "tools/call", json.RawMessage(`{"name":"discover_projects","arguments":{"query":"membership checkout lock","limit":3}}`))
	if err != nil {
		t.Fatalf("tools/call discover_projects: %v", err)
	}

	var result struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("parse discover_projects result: %v", err)
	}
	if result.IsError {
		t.Fatal("discover_projects result unexpectedly marked as error")
	}
	if len(result.Content) != 1 {
		t.Fatalf("content count: want 1, got %d", len(result.Content))
	}

	var payload discovery.Response
	if err := json.Unmarshal([]byte(result.Content[0].Text), &payload); err != nil {
		t.Fatalf("parse discover_projects payload: %v", err)
	}
	if payload.Query != "membership checkout lock" {
		t.Fatalf("query: want %q, got %q", "membership checkout lock", payload.Query)
	}
	if len(payload.PrimaryRepos) != 1 || payload.PrimaryRepos[0].RepoSlug != "membership-backend" {
		t.Fatalf("unexpected primary repos: %+v", payload.PrimaryRepos)
	}
}

func TestMCPBridgeBackendRejectsUnknownMethod(t *testing.T) {
	backend := &mcpBridgeBackend{client: &fakeBridgeClient{}}

	_, err := backend.Call(context.Background(), "resources/list", nil)
	if err == nil {
		t.Fatal("expected error for unknown method")
	}
	if err != bridge.ErrMethodNotFound {
		t.Fatalf("want ErrMethodNotFound, got %v", err)
	}
}

func TestMakeAuthMiddlewareUsesAuthenticatorWhenConfigured(t *testing.T) {
	auth := &fakeRequestAuthenticator{token: "ghp-valid"}
	handler := makeAuthMiddleware("legacy-token", auth)(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	req.Header.Set("Authorization", "Bearer ghp-valid")
	rr := httptest.NewRecorder()
	handler(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: want %d, got %d", http.StatusAccepted, rr.Code)
	}
	if auth.calls != 1 {
		t.Fatalf("auth calls: want 1, got %d", auth.calls)
	}
}

func TestMakeAuthMiddlewareRejectsLegacyBearerWhenAuthenticatorConfigured(t *testing.T) {
	auth := &fakeRequestAuthenticator{token: "ghp-valid"}
	handler := makeAuthMiddleware("legacy-token", auth)(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	req.Header.Set("Authorization", "Bearer legacy-token")
	rr := httptest.NewRecorder()
	handler(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status: want %d, got %d", http.StatusUnauthorized, rr.Code)
	}
	if auth.calls != 1 {
		t.Fatalf("auth calls: want 1, got %d", auth.calls)
	}
}

func TestMakeAuthMiddlewareFallsBackToStaticBearerToken(t *testing.T) {
	handler := makeAuthMiddleware("legacy-token", nil)(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	req.Header.Set("Authorization", "Bearer legacy-token")
	rr := httptest.NewRecorder()
	handler(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: want %d, got %d", http.StatusAccepted, rr.Code)
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

type blockingToolClient struct {
	started chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func newBlockingToolClient() *blockingToolClient {
	return &blockingToolClient{
		started: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (f *blockingToolClient) CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error) {
	close(f.started)
	select {
	case <-f.closed:
		return nil, context.DeadlineExceeded
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *blockingToolClient) Close() {
	f.once.Do(func() {
		close(f.closed)
	})
}

type fastToolClient struct {
	result *mcp.ToolResult
}

func (f *fastToolClient) CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error) {
	if f.result != nil {
		return f.result, nil
	}
	return &mcp.ToolResult{}, nil
}

func (f *fastToolClient) Close() {}

type blockingBridgeClient struct {
	info    mcp.ServerInfo
	started chan struct{}
	once    sync.Once
}

func newBlockingBridgeClient() *blockingBridgeClient {
	return &blockingBridgeClient{
		info:    mcp.ServerInfo{Name: "codebase-memory-mcp", Version: "test"},
		started: make(chan struct{}),
	}
}

func (f *blockingBridgeClient) ServerInfo() mcp.ServerInfo {
	return f.info
}

func (f *blockingBridgeClient) Call(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	f.once.Do(func() { close(f.started) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *blockingBridgeClient) CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error) {
	f.once.Do(func() { close(f.started) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *blockingBridgeClient) Close() {}

type fastBridgeClient struct {
	info   mcp.ServerInfo
	result json.RawMessage
}

func (f *fastBridgeClient) ServerInfo() mcp.ServerInfo {
	return f.info
}

func (f *fastBridgeClient) Call(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	if f.result != nil {
		return f.result, nil
	}
	return json.RawMessage(`{}`), nil
}

func (f *fastBridgeClient) CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error) {
	return &mcp.ToolResult{}, nil
}

func (f *fastBridgeClient) Close() {}

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

func TestMCPToolClientPoolReplacesTimedOutClient(t *testing.T) {
	blocking := newBlockingToolClient()
	replacement := &fastToolClient{
		result: &mcp.ToolResult{Content: []mcp.Content{{Type: "text", Text: "ok"}}},
	}

	var factoryCalls atomic.Int64
	prevFactory := newIndexToolClient
	newIndexToolClient = func(ctx context.Context, binPath string) (indexToolClient, error) {
		switch factoryCalls.Add(1) {
		case 1:
			return blocking, nil
		case 2:
			return replacement, nil
		default:
			return &fastToolClient{
				result: &mcp.ToolResult{Content: []mcp.Content{{Type: "text", Text: "ok"}}},
			}, nil
		}
	}
	defer func() { newIndexToolClient = prevFactory }()

	pool, err := newMCPToolClientPool(context.Background(), "/tmp/cbm", 1)
	if err != nil {
		t.Fatalf("newMCPToolClientPool: %v", err)
	}
	defer pool.Close()

	select {
	case <-blocking.started:
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = pool.CallTool(ctx, "search_graph", map[string]interface{}{"project": "demo"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("timed out call returned too slowly: %s", elapsed)
	}

	result, err := pool.CallTool(context.Background(), "search_graph", map[string]interface{}{"project": "demo"})
	if err != nil {
		t.Fatalf("replacement client call failed: %v", err)
	}
	if len(result.Content) != 1 || result.Content[0].Text != "ok" {
		t.Fatalf("unexpected replacement result: %+v", result)
	}
	if got := factoryCalls.Load(); got < 2 {
		t.Fatalf("expected replacement factory call, got %d", got)
	}
}

func TestMCPBridgeClientPoolReturnsBusyWhenAcquireTimesOut(t *testing.T) {
	blocking := newBlockingBridgeClient()

	prevFactory := newBridgePoolClient
	newBridgePoolClient = func(ctx context.Context, binPath string) (bridgePoolClient, error) {
		return blocking, nil
	}
	defer func() { newBridgePoolClient = prevFactory }()

	pool, err := newMCPBridgeClientPool(context.Background(), "/tmp/cbm", 1, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("newMCPBridgeClientPool: %v", err)
	}
	defer pool.Close()

	firstCtx, firstCancel := context.WithCancel(context.Background())
	defer firstCancel()

	errCh := make(chan error, 1)
	go func() {
		_, callErr := pool.Call(firstCtx, "tools/list", nil)
		errCh <- callErr
	}()

	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("first bridge call did not start")
	}

	start := time.Now()
	_, err = pool.Call(context.Background(), "tools/list", nil)
	if !errors.Is(err, bridge.ErrBackendBusy) {
		t.Fatalf("expected ErrBackendBusy, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("busy call returned too slowly: %s", elapsed)
	}

	firstCancel()
	if callErr := <-errCh; !errors.Is(callErr, context.Canceled) {
		t.Fatalf("expected first call to be canceled, got %v", callErr)
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

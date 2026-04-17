package bridge_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/bridge"
)

// ── Fake MCP backend ──────────────────────────────────────────

type fakeBackend struct {
	response json.RawMessage
	err      error
	method   string
	params   json.RawMessage
	calls    int
	ctx      context.Context
}

func (f *fakeBackend) Call(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
	f.ctx = ctx
	f.method = method
	f.params = append(json.RawMessage(nil), params...)
	f.calls++
	return f.response, f.err
}

// ── Helpers ────────────────────────────────────────────────────

func mcpRequest(t *testing.T, id interface{}, method string, params interface{}) []byte {
	t.Helper()
	p, _ := json.Marshal(params)
	req := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  json.RawMessage(p),
	}
	b, _ := json.Marshal(req)
	return b
}

type fakeAuthenticator struct {
	token string
	calls int
}

func (f *fakeAuthenticator) Authenticate(_ context.Context, bearerToken string) error {
	f.calls++
	if bearerToken != f.token {
		return bridge.ErrBackendUnavailable
	}
	return nil
}

// ── Tests ──────────────────────────────────────────────────────

func TestBridge_ForwardsToolCall(t *testing.T) {
	expected := json.RawMessage(`{"content":[{"type":"text","text":"ok"}],"isError":false}`)
	backend := &fakeBackend{response: expected}
	h := bridge.NewHandler(backend, bridge.Config{})

	body := mcpRequest(t, 1, "tools/call", map[string]interface{}{
		"name":      "list_projects",
		"arguments": map[string]interface{}{},
	})

	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status: want 200, got %d\nbody: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse response: %v\nbody: %s", err, rr.Body.String())
	}
	if resp["jsonrpc"] != "2.0" {
		t.Errorf("jsonrpc: want 2.0, got %v", resp["jsonrpc"])
	}
	if resp["result"] == nil {
		t.Error("result: want non-nil")
	}
	if backend.method != "tools/call" {
		t.Errorf("method: want tools/call, got %q", backend.method)
	}
	if backend.ctx == nil {
		t.Error("backend ctx: expected request context to be forwarded")
	}
}

func TestBridge_ReturnsErrorOnBackendFailure(t *testing.T) {
	backend := &fakeBackend{err: bridge.ErrBackendUnavailable}
	h := bridge.NewHandler(backend, bridge.Config{})

	body := mcpRequest(t, 2, "tools/call", map[string]interface{}{"name": "list_projects"})
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	// HTTP level: still 200 (MCP errors are in the JSON body)
	if rr.Code != http.StatusOK {
		t.Errorf("status: want 200, got %d", rr.Code)
	}

	var resp map[string]interface{}
	json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp["error"] == nil {
		t.Error("expected JSON-RPC error field for backend failure")
	}
}

func TestBridge_ReturnsServiceUnavailableWhenBackendBusy(t *testing.T) {
	backend := &fakeBackend{err: bridge.ErrBackendBusy}
	h := bridge.NewHandler(backend, bridge.Config{})

	body := mcpRequest(t, 2, "tools/call", map[string]interface{}{"name": "list_projects"})
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: want 503, got %d", rr.Code)
	}
	if got := rr.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After: want 1, got %q", got)
	}
}

func TestBridge_RequiresAuthToken(t *testing.T) {
	backend := &fakeBackend{response: json.RawMessage(`{}`)}
	h := bridge.NewHandler(backend, bridge.Config{
		BearerToken: "secret-token",
	})

	body := mcpRequest(t, 3, "tools/call", nil)

	// Request without token
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status: want 401 without token, got %d", rr.Code)
	}

	// Request with correct token
	req2 := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Authorization", "Bearer secret-token")
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusOK {
		t.Errorf("status: want 200 with correct token, got %d", rr2.Code)
	}
}

func TestBridge_UsesAuthenticatorWhenConfigured(t *testing.T) {
	backend := &fakeBackend{response: json.RawMessage(`{}`)}
	auth := &fakeAuthenticator{token: "ghp-valid"}
	h := bridge.NewHandler(backend, bridge.Config{
		BearerToken:   "legacy-token",
		Authenticator: auth,
	})

	body := mcpRequest(t, 4, "tools/call", nil)

	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer ghp-valid")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status: want 200 with valid authenticator token, got %d", rr.Code)
	}
	if auth.calls != 1 {
		t.Fatalf("auth calls: want 1, got %d", auth.calls)
	}
}

func TestBridge_RejectsInvalidAuthenticatorToken(t *testing.T) {
	backend := &fakeBackend{response: json.RawMessage(`{}`)}
	auth := &fakeAuthenticator{token: "ghp-valid"}
	h := bridge.NewHandler(backend, bridge.Config{
		Authenticator: auth,
	})

	body := mcpRequest(t, 5, "tools/call", nil)

	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer ghp-invalid")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status: want 401 with invalid authenticator token, got %d", rr.Code)
	}
	if auth.calls != 1 {
		t.Fatalf("auth calls: want 1, got %d", auth.calls)
	}
}

func TestBridge_InvalidJSON_BadRequest(t *testing.T) {
	backend := &fakeBackend{response: json.RawMessage(`{}`)}
	h := bridge.NewHandler(backend, bridge.Config{})

	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte("not json {")))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status: want 400 for invalid JSON, got %d", rr.Code)
	}
}

func TestBridge_MethodNotAllowed(t *testing.T) {
	backend := &fakeBackend{response: json.RawMessage(`{}`)}
	h := bridge.NewHandler(backend, bridge.Config{})

	req := httptest.NewRequest(http.MethodGet, "/mcp", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("status: want 405 for GET, got %d", rr.Code)
	}
	if got := rr.Header().Get("Allow"); got != http.MethodPost {
		t.Errorf("Allow: want POST, got %q", got)
	}
}

func TestBridge_HealthEndpoint(t *testing.T) {
	backend := &fakeBackend{response: json.RawMessage(`{}`)}
	h := bridge.NewHandler(backend, bridge.Config{})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status: want 200 for /health, got %d", rr.Code)
	}
}

func TestBridge_PreservesRequestID(t *testing.T) {
	backend := &fakeBackend{response: json.RawMessage(`{"content":[],"isError":false}`)}
	h := bridge.NewHandler(backend, bridge.Config{})

	body := mcpRequest(t, "req-42", "tools/call", map[string]interface{}{"name": "list_projects"})
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	var resp map[string]interface{}
	json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp["id"] != "req-42" {
		t.Errorf("id: want req-42, got %v", resp["id"])
	}
}

func TestBridge_NotificationAcceptedWithoutResponse(t *testing.T) {
	backend := &fakeBackend{response: json.RawMessage(`{}`)}
	h := bridge.NewHandler(backend, bridge.Config{})

	body := []byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Errorf("status: want 202 for notification, got %d", rr.Code)
	}
	if rr.Body.Len() != 0 {
		t.Errorf("body: want empty notification response, got %q", rr.Body.String())
	}
	if backend.calls != 0 {
		t.Errorf("backend calls: want 0, got %d", backend.calls)
	}
}

func TestBridge_ReturnsMethodNotFound(t *testing.T) {
	backend := &fakeBackend{err: bridge.ErrMethodNotFound}
	h := bridge.NewHandler(backend, bridge.Config{})

	body := mcpRequest(t, 9, "unknown/method", nil)
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}

	errObj, _ := resp["error"].(map[string]interface{})
	if code := int(errObj["code"].(float64)); code != -32601 {
		t.Errorf("error code: want -32601, got %d", code)
	}
}

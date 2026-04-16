// Package bridge exposes the codebase-memory-mcp stdio binary as an HTTP endpoint.
package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

// ErrBackendUnavailable is returned when the underlying MCP binary is not ready.
var ErrBackendUnavailable = errors.New("bridge: backend unavailable")

// ErrBackendBusy is returned when the backend has no capacity for another request.
var ErrBackendBusy = errors.New("bridge: backend busy")

// ErrMethodNotFound is returned when the bridge backend does not implement an MCP method.
var ErrMethodNotFound = errors.New("bridge: method not found")

// Backend is the interface to the underlying MCP binary.
type Backend interface {
	// Call forwards a JSON-RPC method + params and returns the raw result or error.
	Call(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error)
}

// Config configures the HTTP bridge.
type Config struct {
	// BearerToken, if non-empty, requires all /mcp requests to carry
	// "Authorization: Bearer <token>".
	BearerToken string
	// Authenticator, if non-nil, validates bearer tokens dynamically.
	// When set, it takes precedence over BearerToken.
	Authenticator Authenticator
}

// Authenticator validates bearer tokens for HTTP requests.
type Authenticator interface {
	Authenticate(ctx context.Context, bearerToken string) error
}

// Handler is an http.Handler that bridges HTTP JSON-RPC requests to the MCP backend.
type Handler struct {
	backend Backend
	cfg     Config
}

// NewHandler creates a new bridge Handler.
func NewHandler(backend Backend, cfg Config) *Handler {
	return &Handler{backend: backend, cfg: cfg}
}

// jsonrpcRequest is the inbound envelope.
type jsonrpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// ServeHTTP routes requests:
//
//	GET  /health  — liveness check, no auth required
//	POST /mcp     — Streamable HTTP JSON-RPC, auth required if BearerToken is set
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
		return
	}

	if r.Method == http.MethodGet {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Auth check
	if h.cfg.Authenticator != nil {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if err := h.cfg.Authenticator.Authenticate(r.Context(), strings.TrimPrefix(auth, "Bearer ")); err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	} else if h.cfg.BearerToken != "" {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") || strings.TrimPrefix(auth, "Bearer ") != h.cfg.BearerToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20)) // 4 MB cap
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var req jsonrpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if req.JSONRPC != "" && req.JSONRPC != "2.0" {
		w.Header().Set("Content-Type", "application/json")
		writeError(w, req.ID, -32600, "invalid request: jsonrpc must be 2.0")
		return
	}

	// MCP notifications do not expect a JSON-RPC response body.
	if req.ID == nil && strings.HasPrefix(req.Method, "notifications/") {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	result, backendErr := h.backend.Call(r.Context(), req.Method, req.Params)
	if backendErr != nil {
		switch {
		case errors.Is(backendErr, context.Canceled):
			return
		case errors.Is(backendErr, context.DeadlineExceeded):
			http.Error(w, "backend timed out", http.StatusGatewayTimeout)
			return
		case errors.Is(backendErr, ErrBackendBusy):
			w.Header().Set("Retry-After", "1")
			http.Error(w, "backend overloaded, retry later", http.StatusServiceUnavailable)
			return
		case errors.Is(backendErr, ErrMethodNotFound):
			w.Header().Set("Content-Type", "application/json")
			writeError(w, req.ID, -32601, backendErr.Error())
		default:
			w.Header().Set("Content-Type", "application/json")
			writeError(w, req.ID, -32603, "backend error: "+backendErr.Error())
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")

	resp := struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      interface{}     `json:"id"`
		Result  json.RawMessage `json:"result"`
	}{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result:  result,
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func writeError(w http.ResponseWriter, id interface{}, code int, message string) {
	resp := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]interface{}{
			"code":    code,
			"message": message,
		},
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

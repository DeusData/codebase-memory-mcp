// Package bridge exposes the codebase-memory-mcp stdio binary as an HTTP endpoint.
// It serialises concurrent HTTP requests into sequential JSON-RPC calls on the binary.
package bridge

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

// ErrBackendUnavailable is returned when the underlying MCP binary is not ready.
var ErrBackendUnavailable = errors.New("bridge: backend unavailable")

// Backend is the interface to the underlying MCP binary.
type Backend interface {
	// Call forwards a JSON-RPC method + params and returns the raw result or error.
	Call(method string, params json.RawMessage) (json.RawMessage, error)
}

// Config configures the HTTP bridge.
type Config struct {
	// BearerToken, if non-empty, requires all /mcp requests to carry
	// "Authorization: Bearer <token>".
	BearerToken string
}

// Handler is an http.Handler that bridges HTTP POST requests to the MCP backend.
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
//	POST /mcp     — JSON-RPC forwarding, auth required if BearerToken is set
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Auth check
	if h.cfg.BearerToken != "" {
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

	w.Header().Set("Content-Type", "application/json")

	result, backendErr := h.backend.Call(req.Method, req.Params)
	if backendErr != nil {
		writeError(w, req.ID, -32603, "backend error: "+backendErr.Error())
		return
	}

	resp := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      req.ID,
		"result":  result,
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

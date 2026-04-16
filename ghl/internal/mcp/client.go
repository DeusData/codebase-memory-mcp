// Package mcp provides a JSON-RPC 2.0 MCP client that speaks to the
// codebase-memory-mcp binary over stdin/stdout.
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"sync/atomic"
)

// ServerInfo holds identifying information returned during initialization.
type ServerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Content is a single item returned in a tool result.
type Content struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// ToolResult is the parsed result of a tools/call response.
type ToolResult struct {
	Content []Content `json:"content"`
	IsError bool      `json:"isError"`
}

// Client manages a single subprocess running codebase-memory-mcp and serializes
// MCP JSON-RPC requests over stdin/stdout.
type Client struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	reader *bufio.Scanner
	mu     sync.Mutex
	nextID atomic.Int64
	info   ServerInfo
	closed bool
}

// jsonrpcRequest is the envelope for outbound MCP calls.
type jsonrpcRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      int64       `json:"id"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

// jsonrpcResponse is the envelope for inbound MCP responses.
type jsonrpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonrpcError   `json:"error,omitempty"`
}

type jsonrpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// initResult is the subset of the initialize response we care about.
type initResult struct {
	ServerInfo struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"serverInfo"`
}

// toolCallResult is the subset of tools/call response we care about.
type toolCallResult struct {
	Content []Content `json:"content"`
	IsError bool      `json:"isError"`
}

// NewClient launches the binary at binPath, performs MCP initialization, and
// returns a ready-to-use Client. It blocks until initialization succeeds or ctx
// is cancelled.
func NewClient(ctx context.Context, binPath string) (*Client, error) {
	cmd := exec.CommandContext(ctx, binPath)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("mcp: stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("mcp: stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("mcp: start binary %q: %w", binPath, err)
	}

	c := &Client{
		cmd:    cmd,
		stdin:  stdin,
		reader: bufio.NewScanner(stdout),
	}
	// Increase scanner buffer for large responses (e.g. index_repository results)
	c.reader.Buffer(make([]byte, 4*1024*1024), 4*1024*1024)

	if err := c.initialize(ctx); err != nil {
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("mcp: initialize: %w", err)
	}

	return c, nil
}

// ServerInfo returns the server name and version reported during initialization.
func (c *Client) ServerInfo() ServerInfo {
	return c.info
}

// Call sends an arbitrary MCP request and returns the raw result payload.
// It is safe to call from multiple goroutines — requests are serialized.
func (c *Client) Call(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.roundtrip(ctx, method, params)
}

// CallTool sends a tools/call request and returns the parsed result.
// It is safe to call from multiple goroutines — requests are serialized.
func (c *Client) CallTool(ctx context.Context, name string, params map[string]interface{}) (*ToolResult, error) {
	toolParams := map[string]interface{}{
		"name": name,
	}
	if params != nil {
		toolParams["arguments"] = params
	}

	raw, err := c.Call(ctx, "tools/call", toolParams)
	if err != nil {
		return nil, err
	}

	var result toolCallResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("mcp: parse tools/call result: %w", err)
	}
	return &ToolResult{Content: result.Content, IsError: result.IsError}, nil
}

// Close terminates the subprocess. Safe to call multiple times.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	_ = c.stdin.Close()
	if c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
	}
	_ = c.cmd.Wait()
}

// ── Internal ───────────────────────────────────────────────────

func (c *Client) initialize(ctx context.Context) error {
	initParams := map[string]interface{}{
		"protocolVersion": "2025-11-25",
		"capabilities":    map[string]interface{}{},
		"clientInfo":      map[string]interface{}{"name": "ghl-fleet", "version": "1.0.0"},
	}
	raw, err := c.roundtrip(ctx, "initialize", initParams)
	if err != nil {
		return err
	}

	var result initResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return fmt.Errorf("parse initialize result: %w", err)
	}
	c.info = ServerInfo{
		Name:    result.ServerInfo.Name,
		Version: result.ServerInfo.Version,
	}

	// Send initialized notification (no response expected)
	_ = c.send(jsonrpcRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
	})

	return nil
}

// roundtrip sends a request and reads the matching response.
// Requests are serialized via the mutex so only one is in-flight at a time.
func (c *Client) roundtrip(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	id := c.nextID.Add(1)
	req := jsonrpcRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  params,
	}

	if err := c.send(req); err != nil {
		return nil, fmt.Errorf("mcp: send %q: %w", method, err)
	}

	// Read lines until we get a response with our ID
	for {
		// Check context before blocking read
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if !c.reader.Scan() {
			if err := c.reader.Err(); err != nil {
				return nil, fmt.Errorf("mcp: read: %w", err)
			}
			return nil, fmt.Errorf("mcp: subprocess closed stdout unexpectedly")
		}

		line := c.reader.Text()
		if line == "" {
			continue
		}

		var resp jsonrpcResponse
		if err := json.Unmarshal([]byte(line), &resp); err != nil {
			// Not valid JSON-RPC — might be a progress notification, skip
			continue
		}

		// Skip notifications (no ID)
		if resp.ID == 0 && resp.JSONRPC == "2.0" {
			continue
		}

		if resp.ID != id {
			// Response for a different request (shouldn't happen with serialization)
			continue
		}

		if resp.Error != nil {
			return nil, fmt.Errorf("mcp: %q error %d: %s", method, resp.Error.Code, resp.Error.Message)
		}

		return resp.Result, nil
	}
}

func (c *Client) send(req jsonrpcRequest) error {
	b, err := json.Marshal(req)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = c.stdin.Write(b)
	return err
}

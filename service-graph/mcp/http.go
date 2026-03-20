package mcp

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/Shidfar/codebase-memory-mcp/service-graph/cbm"
)

// HTTPServer serves the graph UI and proxies JSON-RPC to the MCP server.
type HTTPServer struct {
	srv      *Server
	port     int
	logRing  *logRing
	indexMu  sync.Mutex
	indexJobs [maxIndexJobs]indexJob
}

const maxIndexJobs = 4

type indexJob struct {
	RootPath string `json:"path"`
	Status   string `json:"status"` // "", "indexing", "done", "error"
	Error    string `json:"error"`
}

// logRing is a thread-safe ring buffer for log lines.
type logRing struct {
	mu    sync.Mutex
	lines [500]string
	head  int
	count int
}

func (r *logRing) append(line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines[r.head] = line
	r.head = (r.head + 1) % len(r.lines)
	if r.count < len(r.lines) {
		r.count++
	}
}

func (r *logRing) last(n int) ([]string, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n > r.count {
		n = r.count
	}
	out := make([]string, n)
	start := (r.head - n + len(r.lines)) % len(r.lines)
	for i := 0; i < n; i++ {
		out[i] = r.lines[(start+i)%len(r.lines)]
	}
	return out, r.count
}

// NewHTTPServer creates a new HTTP server wrapping the MCP server.
func NewHTTPServer(srv *Server, port int) *HTTPServer {
	return &HTTPServer{srv: srv, port: port, logRing: &logRing{}}
}

// cacheDir returns ~/.cache/codebase-memory-mcp
func cacheDir() string {
	home, _ := os.UserHomeDir()
	if home == "" {
		home = "/tmp"
	}
	return filepath.Join(home, ".cache", "codebase-memory-mcp")
}

func dbPath(project string) string {
	return filepath.Join(cacheDir(), project+".db")
}

func cors(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func jsonReply(w http.ResponseWriter, status int, v interface{}) {
	cors(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func jsonError(w http.ResponseWriter, status int, msg string) {
	jsonReply(w, status, map[string]string{"error": msg})
}

// ListenAndServe starts the HTTP server.
func (h *HTTPServer) ListenAndServe() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/rpc", h.handleRPC)
	mux.HandleFunc("/api/layout", h.handleLayout)
	mux.HandleFunc("/api/index", h.handleIndex)
	mux.HandleFunc("/api/index-status", h.handleIndexStatus)
	mux.HandleFunc("/api/project", h.handleProject)
	mux.HandleFunc("/api/project-health", h.handleProjectHealth)
	mux.HandleFunc("/api/browse", h.handleBrowse)
	mux.HandleFunc("/api/adr", h.handleADR)
	mux.HandleFunc("/api/processes", h.handleProcesses)
	mux.HandleFunc("/api/logs", h.handleLogs)
	mux.HandleFunc("/api/process-kill", h.handleProcessKill)

	addr := fmt.Sprintf("127.0.0.1:%d", h.port)
	log.Printf("UI server listening on http://%s", addr)
	return http.ListenAndServe(addr, mux)
}

// ── POST /rpc — JSON-RPC dispatch ────────────────────────────

func (h *HTTPServer) handleRPC(w http.ResponseWriter, r *http.Request) {
	cors(w)
	if r.Method == "OPTIONS" {
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(204)
		return
	}
	if r.Method != "POST" {
		http.Error(w, "method not allowed", 405)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil || len(body) == 0 {
		jsonError(w, 400, "invalid request")
		return
	}

	resp := h.srv.handleLine(string(body))

	w.Header().Set("Content-Type", "application/json")
	if resp == "" {
		w.WriteHeader(204)
		return
	}
	w.WriteHeader(200)
	fmt.Fprint(w, resp)
}

// ── GET /api/layout ──────────────────────────────────────────

func (h *HTTPServer) handleLayout(w http.ResponseWriter, r *http.Request) {
	cors(w)
	project := r.URL.Query().Get("project")
	if project == "" {
		jsonError(w, 400, "missing project parameter")
		return
	}
	maxNodes := 50000
	if v, err := strconv.Atoi(r.URL.Query().Get("max_nodes")); err == nil && v > 0 {
		maxNodes = v
	}

	dp := dbPath(project)
	if !cbm.FileExists(dp) {
		jsonError(w, 404, "project not found")
		return
	}

	store, err := cbm.OpenStore(dp)
	if err != nil {
		jsonError(w, 500, "cannot open store")
		return
	}
	defer store.Close()

	jsonStr, err := store.ComputeLayout(project, maxNodes)
	if err != nil {
		jsonError(w, 500, err.Error())
		return
	}

	cors(w)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, jsonStr)
}

// ── POST /api/index ──────────────────────────────────────────

func (h *HTTPServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	cors(w)
	if r.Method == "OPTIONS" {
		w.WriteHeader(204)
		return
	}
	if r.Method == "GET" {
		h.handleIndexStatus(w, r)
		return
	}
	if r.Method != "POST" {
		http.Error(w, "method not allowed", 405)
		return
	}

	var body struct {
		RootPath string `json:"root_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.RootPath == "" {
		jsonError(w, 400, "missing root_path")
		return
	}

	if !cbm.IsDir(body.RootPath) {
		jsonError(w, 400, "directory not found")
		return
	}

	h.indexMu.Lock()
	slot := -1
	for i := 0; i < maxIndexJobs; i++ {
		if h.indexJobs[i].Status == "" || h.indexJobs[i].Status == "done" || h.indexJobs[i].Status == "error" {
			slot = i
			break
		}
	}
	if slot < 0 {
		h.indexMu.Unlock()
		jsonError(w, 429, "all index slots busy")
		return
	}
	h.indexJobs[slot] = indexJob{RootPath: body.RootPath, Status: "indexing"}
	h.indexMu.Unlock()

	go h.runIndex(slot)

	jsonReply(w, 202, map[string]interface{}{
		"status": "indexing", "slot": slot, "path": body.RootPath,
	})
}

func (h *HTTPServer) runIndex(slot int) {
	job := &h.indexJobs[slot]

	argsJSON := fmt.Sprintf(`{"repo_path":"%s"}`, job.RootPath)
	h.logRing.append(fmt.Sprintf("Indexing %s ...", job.RootPath))

	// Call the C index_repository tool via the bridge
	resp, err := h.srv.bridge.HandleTool("index_repository", argsJSON)

	h.indexMu.Lock()
	defer h.indexMu.Unlock()
	if err != nil {
		job.Status = "error"
		job.Error = fmt.Sprintf("indexing failed: %v", err)
		h.logRing.append(fmt.Sprintf("Index error for %s: %v", job.RootPath, err))
	} else {
		job.Status = "done"
		// Log a snippet of the result
		if len(resp) > 200 {
			resp = resp[:200] + "..."
		}
		h.logRing.append(fmt.Sprintf("Index done for %s: %s", job.RootPath, resp))
	}
}

// ── GET /api/index-status ────────────────────────────────────

func (h *HTTPServer) handleIndexStatus(w http.ResponseWriter, r *http.Request) {
	cors(w)
	h.indexMu.Lock()
	var out []map[string]interface{}
	for i := 0; i < maxIndexJobs; i++ {
		j := &h.indexJobs[i]
		if j.Status == "" {
			continue
		}
		out = append(out, map[string]interface{}{
			"slot": i, "status": j.Status, "path": j.RootPath, "error": j.Error,
		})
	}
	h.indexMu.Unlock()
	if out == nil {
		out = []map[string]interface{}{}
	}
	jsonReply(w, 200, out)
}

// ── DELETE /api/project & GET /api/project-health ────────────

func (h *HTTPServer) handleProject(w http.ResponseWriter, r *http.Request) {
	cors(w)
	if r.Method == "OPTIONS" {
		w.WriteHeader(204)
		return
	}
	if r.Method != "DELETE" {
		http.Error(w, "method not allowed", 405)
		return
	}
	name := r.URL.Query().Get("name")
	if name == "" {
		jsonError(w, 400, "missing name")
		return
	}
	dp := dbPath(name)
	if !cbm.FileExists(dp) {
		jsonError(w, 404, "project not found")
		return
	}
	if err := os.Remove(dp); err != nil {
		jsonError(w, 500, "failed to delete")
		return
	}
	os.Remove(dp + "-wal")
	os.Remove(dp + "-shm")
	jsonReply(w, 200, map[string]bool{"deleted": true})
}

func (h *HTTPServer) handleProjectHealth(w http.ResponseWriter, r *http.Request) {
	cors(w)
	name := r.URL.Query().Get("name")
	if name == "" {
		jsonError(w, 400, "missing name")
		return
	}
	dp := dbPath(name)
	if !cbm.FileExists(dp) {
		jsonReply(w, 200, map[string]string{"status": "missing"})
		return
	}
	store, err := cbm.OpenStore(dp)
	if err != nil {
		jsonReply(w, 200, map[string]interface{}{"status": "corrupt", "reason": "cannot open"})
		return
	}
	nodes := store.CountNodes(name)
	edges := store.CountEdges(name)
	store.Close()
	size := cbm.FileSize(dp)
	jsonReply(w, 200, map[string]interface{}{
		"status": "healthy", "nodes": nodes, "edges": edges, "size_bytes": size,
	})
}

// ── GET /api/browse ──────────────────────────────────────────

func (h *HTTPServer) handleBrowse(w http.ResponseWriter, r *http.Request) {
	cors(w)
	path := r.URL.Query().Get("path")
	if path == "" {
		home, _ := os.UserHomeDir()
		if home == "" {
			home = "/"
		}
		path = home
	}

	if !cbm.IsDir(path) {
		jsonError(w, 400, "not a directory")
		return
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		jsonError(w, 403, "cannot open directory")
		return
	}

	var dirs []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}
	sort.Strings(dirs)
	if len(dirs) > 200 {
		dirs = dirs[:200]
	}

	parent := filepath.Dir(path)
	if parent == path {
		parent = "/"
	}

	jsonReply(w, 200, map[string]interface{}{
		"path": path, "dirs": dirs, "parent": parent,
	})
}

// ── GET/POST /api/adr ────────────────────────────────────────

func (h *HTTPServer) handleADR(w http.ResponseWriter, r *http.Request) {
	cors(w)
	if r.Method == "OPTIONS" {
		w.WriteHeader(204)
		return
	}

	if r.Method == "GET" {
		project := r.URL.Query().Get("project")
		if project == "" {
			jsonError(w, 400, "missing project")
			return
		}
		dp := dbPath(project)
		store, err := cbm.OpenStore(dp)
		if err != nil {
			jsonReply(w, 200, map[string]bool{"has_adr": false})
			return
		}
		defer store.Close()

		content, updatedAt, hasAdr := store.AdrGet(project)
		if !hasAdr {
			jsonReply(w, 200, map[string]bool{"has_adr": false})
			return
		}
		jsonReply(w, 200, map[string]interface{}{
			"has_adr": true, "content": content, "updated_at": updatedAt,
		})
		return
	}

	if r.Method == "POST" {
		var body struct {
			Project string `json:"project"`
			Content string `json:"content"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Project == "" {
			jsonError(w, 400, "missing project or content")
			return
		}
		dp := dbPath(body.Project)
		store, err := cbm.OpenStore(dp)
		if err != nil {
			jsonError(w, 500, "cannot open store")
			return
		}
		defer store.Close()

		if err := store.AdrStore(body.Project, body.Content); err != nil {
			jsonError(w, 500, "save failed")
			return
		}
		jsonReply(w, 200, map[string]bool{"saved": true})
		return
	}

	http.Error(w, "method not allowed", 405)
}

// ── GET /api/processes ───────────────────────────────────────

func (h *HTTPServer) handleProcesses(w http.ResponseWriter, r *http.Request) {
	cors(w)
	pid := os.Getpid()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	rssMB := float64(m.Sys) / (1024 * 1024)

	jsonReply(w, 200, map[string]interface{}{
		"self_pid":       pid,
		"self_rss_mb":    rssMB,
		"self_user_cpu_s": 0,
		"self_sys_cpu_s":  0,
		"processes":      []interface{}{},
	})
}

// ── GET /api/logs ────────────────────────────────────────────

func (h *HTTPServer) handleLogs(w http.ResponseWriter, r *http.Request) {
	cors(w)
	n := 100
	if v, err := strconv.Atoi(r.URL.Query().Get("lines")); err == nil && v > 0 && v <= 500 {
		n = v
	}
	lines, total := h.logRing.last(n)
	jsonReply(w, 200, map[string]interface{}{"lines": lines, "total": total})
}

// ── POST /api/process-kill ───────────────────────────────────

func (h *HTTPServer) handleProcessKill(w http.ResponseWriter, r *http.Request) {
	cors(w)
	if r.Method == "OPTIONS" {
		w.WriteHeader(204)
		return
	}
	var body struct {
		PID int `json:"pid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.PID == 0 {
		jsonError(w, 400, "missing pid")
		return
	}
	if body.PID == os.Getpid() {
		jsonError(w, 400, "cannot kill self")
		return
	}

	proc, err := os.FindProcess(body.PID)
	if err != nil {
		jsonError(w, 500, "process not found")
		return
	}
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		jsonError(w, 500, "kill failed")
		return
	}
	jsonReply(w, 200, map[string]int{"killed": body.PID})
}

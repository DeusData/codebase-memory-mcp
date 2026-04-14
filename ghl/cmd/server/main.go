// ghl-fleet — GHL additions to codebase-memory-mcp.
//
// Runs three services in one process:
//   - HTTP bridge: exposes the codebase-memory-mcp binary as an HTTP MCP endpoint
//   - Fleet indexer: clones + indexes all 200 GHL repos on a schedule
//   - Webhook handler: triggers re-index on GitHub push events
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/robfig/cron/v3"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/bridge"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/indexer"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/mcp"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/webhook"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg := loadConfig()

	// ── Load fleet manifest ──────────────────────────────────

	m, err := manifest.Load(cfg.ReposManifest)
	if err != nil {
		slog.Error("failed to load repos manifest", "path", cfg.ReposManifest, "err", err)
		os.Exit(1)
	}
	slog.Info("fleet manifest loaded", "repos", len(m.Repos))

	// ── Start MCP binary client ──────────────────────────────

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mcpClient, err := mcp.NewClient(ctx, cfg.BinaryPath)
	if err != nil {
		slog.Error("failed to start codebase-memory-mcp binary", "binary", cfg.BinaryPath, "err", err)
		os.Exit(1)
	}
	defer mcpClient.Close()
	slog.Info("codebase-memory-mcp started", "name", mcpClient.ServerInfo().Name, "version", mcpClient.ServerInfo().Version)

	// ── Build indexer ────────────────────────────────────────

	cloner := &gitCloner{logger: logger}
	mcpIndexClient := &mcpIndexClient{client: mcpClient, logger: logger}

	idx := indexer.New(indexer.Config{
		Client:      mcpIndexClient,
		Cloner:      cloner,
		CacheDir:    cfg.CacheDir,
		Concurrency: cfg.Concurrency,
		OnRepoStart: func(slug string) { slog.Info("indexing repo", "repo", slug) },
		OnRepoDone:  func(slug string) { slog.Info("repo indexed", "repo", slug) },
	})

	// ── Fleet scheduler ──────────────────────────────────────

	c := cron.New()
	c.AddFunc(cfg.IncrementalCron, func() {
		slog.Info("fleet index (incremental) starting")
		result := idx.IndexAll(context.Background(), m.Repos, false)
		slog.Info("fleet index (incremental) complete",
			"total", result.Total, "ok", result.Succeeded, "failed", result.Failed)
	})
	c.AddFunc(cfg.FullCron, func() {
		slog.Info("fleet index (full) starting")
		result := idx.IndexAll(context.Background(), m.Repos, true)
		slog.Info("fleet index (full) complete",
			"total", result.Total, "ok", result.Succeeded, "failed", result.Failed)
	})
	c.Start()
	defer c.Stop()

	// ── HTTP router ──────────────────────────────────────────

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(5 * time.Minute))

	// Bridge: forward MCP calls to the binary
	bridgeHandler := bridge.NewHandler(
		&mcpBridgeBackend{client: mcpClient},
		bridge.Config{BearerToken: cfg.BearerToken},
	)
	r.Mount("/mcp", bridgeHandler)
	r.Get("/health", bridgeHandler.ServeHTTP)

	// Webhook: trigger re-index on GitHub push
	wh := webhook.NewHandler(webhook.Config{
		Secret: []byte(cfg.WebhookSecret),
		OnPush: func(repoSlug string) {
			repo, ok := m.FindByName(repoSlug)
			if !ok {
				slog.Warn("webhook: repo not in manifest", "repo", repoSlug)
				return
			}
			slog.Info("webhook: re-indexing repo", "repo", repoSlug)
			if err := idx.IndexRepo(context.Background(), repo, false); err != nil {
				slog.Error("webhook: index failed", "repo", repoSlug, "err", err)
			}
		},
	})
	r.Post("/webhooks/github", wh.ServeHTTP)

	// Manual trigger: index a single repo by slug
	r.Post("/index/{repoSlug}", func(w http.ResponseWriter, req *http.Request) {
		slug := chi.URLParam(req, "repoSlug")
		repo, ok := m.FindByName(slug)
		if !ok {
			http.Error(w, "repo not found in manifest", http.StatusNotFound)
			return
		}
		go func() {
			if err := idx.IndexRepo(context.Background(), repo, true); err != nil {
				slog.Error("manual index failed", "repo", slug, "err", err)
			}
		}()
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"accepted":true,"repo":%q}`, slug)
	})

	// Fleet status endpoint
	r.Get("/status", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"repos":   len(m.Repos),
			"version": mcpClient.ServerInfo().Version,
			"binary":  cfg.BinaryPath,
			"cache":   cfg.CacheDir,
		})
	})

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 10 * time.Minute,
		IdleTimeout:  120 * time.Second,
	}

	// ── Startup indexing pass ────────────────────────────────

	go func() {
		slog.Info("startup: running initial fleet index")
		result := idx.IndexAll(context.Background(), m.Repos, false)
		slog.Info("startup: initial fleet index complete",
			"total", result.Total, "ok", result.Succeeded, "failed", result.Failed)
	}()

	// ── Serve ────────────────────────────────────────────────

	go func() {
		slog.Info("server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown error", "err", err)
	}
}

// ── Config ─────────────────────────────────────────────────────

type config struct {
	Port            string
	BinaryPath      string
	CacheDir        string
	ReposManifest   string
	BearerToken     string
	WebhookSecret   string
	Concurrency     int
	IncrementalCron string
	FullCron        string
}

func loadConfig() config {
	getEnv := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	getConcurrency := func() int {
		v := getEnv("FLEET_CONCURRENCY", "5")
		n := 5
		fmt.Sscanf(v, "%d", &n)
		return n
	}
	return config{
		Port:            getEnv("PORT", "8080"),
		BinaryPath:      getEnv("CBM_BINARY", defaultBinaryPath()),
		CacheDir:        getEnv("FLEET_CACHE_DIR", "/app/fleet-cache"),
		ReposManifest:   getEnv("REPOS_MANIFEST", "/app/REPOS.yaml"),
		BearerToken:     getEnv("BEARER_TOKEN", ""),
		WebhookSecret:   getEnv("GITHUB_WEBHOOK_SECRET", ""),
		Concurrency:     getConcurrency(),
		IncrementalCron: getEnv("CRON_INCREMENTAL", "0 */6 * * *"),
		FullCron:        getEnv("CRON_FULL", "0 2 * * 0"),
	}
}

func defaultBinaryPath() string {
	name := "codebase-memory-mcp"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	exe, _ := os.Executable()
	dir := filepath.Dir(exe)
	candidate := filepath.Join(dir, name)
	if _, err := os.Stat(candidate); err == nil {
		return candidate
	}
	// Fallback: find in PATH
	if path, err := exec.LookPath(name); err == nil {
		return path
	}
	return name
}

// ── Adapters ───────────────────────────────────────────────────

// gitCloner implements indexer.Cloner using git CLI.
type gitCloner struct {
	logger *slog.Logger
}

func (g *gitCloner) EnsureClone(ctx context.Context, githubURL, localPath string) error {
	if _, err := os.Stat(filepath.Join(localPath, ".git")); err == nil {
		// Already cloned — fetch latest
		g.logger.Debug("updating clone", "path", localPath)
		cmd := exec.CommandContext(ctx, "git", "fetch", "--depth=1", "origin", "HEAD")
		cmd.Dir = localPath
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("git fetch: %w\n%s", err, out)
		}
		cmd = exec.CommandContext(ctx, "git", "reset", "--hard", "FETCH_HEAD")
		cmd.Dir = localPath
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("git reset: %w\n%s", err, out)
		}
		return nil
	}
	// Fresh clone
	if err := os.MkdirAll(localPath, 0750); err != nil {
		return fmt.Errorf("mkdir %q: %w", localPath, err)
	}
	// Remove empty dir to allow clone into it
	os.Remove(localPath)
	g.logger.Info("cloning repo", "url", githubURL, "path", localPath)
	cloneCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cloneCtx, "git", "clone", "--depth=1", githubURL, localPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git clone %q: %w\n%s", githubURL, err, out)
	}
	return nil
}

// mcpIndexClient implements indexer.Client by calling the MCP binary.
type mcpIndexClient struct {
	client *mcp.Client
	logger *slog.Logger
}

func (m *mcpIndexClient) IndexRepository(ctx context.Context, repoPath, mode string) error {
	result, err := m.client.CallTool(ctx, "index_repository", map[string]interface{}{
		"repo_path": repoPath,
		"mode":      mode,
	})
	if err != nil {
		return fmt.Errorf("index_repository: %w", err)
	}
	if result.IsError {
		msg := "index_repository returned error"
		if len(result.Content) > 0 {
			msg = result.Content[0].Text
		}
		return fmt.Errorf("index_repository: %s", msg)
	}
	return nil
}

// mcpBridgeBackend implements bridge.Backend by forwarding to the MCP client.
type mcpBridgeBackend struct {
	client *mcp.Client
}

func (b *mcpBridgeBackend) Call(method string, params json.RawMessage) (json.RawMessage, error) {
	if b.client == nil {
		return nil, bridge.ErrBackendUnavailable
	}
	var paramMap map[string]interface{}
	if len(params) > 0 {
		if err := json.Unmarshal(params, &paramMap); err != nil {
			return nil, fmt.Errorf("parse params: %w", err)
		}
	}

	// Extract tool name and arguments from tools/call params
	name, _ := paramMap["name"].(string)
	args, _ := paramMap["arguments"].(map[string]interface{})

	result, err := b.client.CallTool(context.Background(), name, args)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

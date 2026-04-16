// ghl-fleet — GHL additions to codebase-memory-mcp.
//
// Runs three services in one process:
//   - HTTP bridge: exposes the codebase-memory-mcp binary as an HTTP MCP endpoint
//   - Fleet indexer: clones + indexes all 200 GHL repos on a schedule
//   - Webhook handler: triggers re-index on GitHub push events
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/robfig/cron/v3"

	ghlauth "github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/auth"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/bridge"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/discovery"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/indexer"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/mcp"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/webhook"
)

var supportedProtocolVersions = []string{
	"2025-11-25",
	"2025-06-18",
	"2025-03-26",
	"2024-11-05",
}

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

	// ── Start MCP binary clients ─────────────────────────────

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	bridgePool, err := newMCPBridgeClientPool(ctx, cfg.BinaryPath, cfg.BridgeClients, cfg.BridgeAcquireTimeout)
	if err != nil {
		slog.Error("failed to start bridge client pool", "binary", cfg.BinaryPath, "clients", cfg.BridgeClients, "err", err)
		os.Exit(1)
	}
	defer bridgePool.Close()
	slog.Info(
		"bridge client pool started",
		"name", bridgePool.ServerInfo().Name,
		"version", bridgePool.ServerInfo().Version,
		"clients", cfg.BridgeClients,
		"acquire_timeout_ms", cfg.BridgeAcquireTimeout.Milliseconds(),
	)

	indexPool, err := newMCPIndexClientPool(ctx, cfg.BinaryPath, cfg.IndexerClients)
	if err != nil {
		slog.Error("failed to start indexer client pool", "clients", cfg.IndexerClients, "err", err)
		os.Exit(1)
	}
	defer indexPool.Close()
	slog.Info("indexer client pool started", "clients", cfg.IndexerClients)

	discoveryPool, err := newMCPDiscoveryClientPool(ctx, cfg.BinaryPath, cfg.DiscoveryClients)
	if err != nil {
		slog.Error("failed to start discovery client pool", "clients", cfg.DiscoveryClients, "err", err)
		os.Exit(1)
	}
	defer discoveryPool.Close()
	slog.Info("discovery client pool started", "clients", cfg.DiscoveryClients)

	var requestAuthenticator bridge.Authenticator
	if cfg.GitHubAuthEnabled {
		requestAuthenticator = ghlauth.NewGitHubAuthenticator(ghlauth.GitHubConfig{
			BaseURL:     cfg.GitHubAPIBaseURL,
			AllowedOrgs: cfg.GitHubAllowedOrgs,
			CacheTTL:    cfg.GitHubAuthCacheTTL,
		})
		slog.Info("github bearer auth enabled", "allowed_orgs", cfg.GitHubAllowedOrgs)
	}

	// ── Build indexer ────────────────────────────────────────

	var discoverySvc *discovery.Discoverer
	cloner := &gitCloner{
		logger:      logger,
		githubToken: cfg.GitHubToken,
	}

	idx := indexer.New(indexer.Config{
		Client:      indexPool,
		Cloner:      cloner,
		CacheDir:    cfg.CacheDir,
		Concurrency: cfg.Concurrency,
		OnRepoStart: func(slug string) { slog.Info("indexing repo", "repo", slug) },
		OnRepoDone: func(slug string, err error) {
			if err != nil {
				slog.Error("repo indexing failed", "repo", slug, "err", err)
				return
			}
			if discoverySvc != nil {
				discoverySvc.Invalidate()
			}
			slog.Info("repo indexed", "repo", slug)
		},
	})

	maxGraphCandidates := 3
	if cfg.DiscoveryMaxCandidates > 0 && cfg.DiscoveryMaxCandidates < maxGraphCandidates {
		maxGraphCandidates = cfg.DiscoveryMaxCandidates
	}
	discoverySvc = discovery.NewService(discoveryPool, *m, discovery.Options{
		MaxBM25Candidates:  cfg.DiscoveryMaxCandidates,
		MaxGraphCandidates: maxGraphCandidates,
		RequestTimeout:     cfg.DiscoveryTimeout,
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
		&mcpBridgeBackend{client: bridgePool, discovery: discoverySvc},
		bridge.Config{BearerToken: cfg.BearerToken, Authenticator: requestAuthenticator},
	)
	r.Mount("/mcp", bridgeHandler)
	r.Get("/health", bridgeHandler.ServeHTTP)

	requireAuth := makeAuthMiddleware(cfg.BearerToken, requestAuthenticator)

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
	r.Post("/index/{repoSlug}", requireAuth(func(w http.ResponseWriter, req *http.Request) {
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
	}))

	// Fleet status endpoint
	r.Get("/status", requireAuth(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"repos":                    len(m.Repos),
			"version":                  bridgePool.ServerInfo().Version,
			"binary":                   cfg.BinaryPath,
			"cache":                    cfg.CacheDir,
			"manifest":                 cfg.ReposManifest,
			"concurrency":              cfg.Concurrency,
			"bridge_clients":           cfg.BridgeClients,
			"bridge_acquire_timeout":   cfg.BridgeAcquireTimeout.Milliseconds(),
			"indexer_clients":          cfg.IndexerClients,
			"discovery_clients":        cfg.DiscoveryClients,
			"discovery_max_candidates": cfg.DiscoveryMaxCandidates,
			"discovery_timeout_ms":     cfg.DiscoveryTimeout.Milliseconds(),
			"github_auth_enabled":      cfg.GitHubAuthEnabled,
		})
	}))

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

func makeAuthMiddleware(staticToken string, auth bridge.Authenticator) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, req *http.Request) {
			authHeader := req.Header.Get("Authorization")
			if auth != nil {
				if !strings.HasPrefix(authHeader, "Bearer ") {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				if err := auth.Authenticate(req.Context(), strings.TrimPrefix(authHeader, "Bearer ")); err != nil {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
			} else if staticToken != "" {
				if !strings.HasPrefix(authHeader, "Bearer ") || strings.TrimPrefix(authHeader, "Bearer ") != staticToken {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
			}
			next(w, req)
		}
	}
}

// ── Config ─────────────────────────────────────────────────────

type config struct {
	Port                   string
	BinaryPath             string
	CacheDir               string
	ReposManifest          string
	BearerToken            string
	GitHubToken            string
	GitHubAuthEnabled      bool
	GitHubAllowedOrgs      []string
	GitHubAPIBaseURL       string
	GitHubAuthCacheTTL     time.Duration
	WebhookSecret          string
	Concurrency            int
	BridgeClients          int
	BridgeAcquireTimeout   time.Duration
	IndexerClients         int
	DiscoveryClients       int
	DiscoveryMaxCandidates int
	DiscoveryTimeout       time.Duration
	IncrementalCron        string
	FullCron               string
}

func loadConfig() config {
	getEnv := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	getBool := func(key string, def bool) bool {
		v := strings.TrimSpace(getEnv(key, ""))
		if v == "" {
			return def
		}
		switch strings.ToLower(v) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		default:
			return def
		}
	}
	getStringList := func(key string) []string {
		raw := strings.TrimSpace(getEnv(key, ""))
		if raw == "" {
			return nil
		}
		parts := strings.Split(raw, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
		return out
	}
	getConcurrency := func() int {
		v := getEnv("FLEET_CONCURRENCY", "5")
		n := 5
		fmt.Sscanf(v, "%d", &n)
		return n
	}
	getBridgeClients := func() int {
		v := getEnv("BRIDGE_CLIENTS", "")
		if v == "" {
			n := runtime.GOMAXPROCS(0)
			if n < 2 {
				return 2
			}
			if n > 4 {
				return 4
			}
			return n
		}
		n := 1
		fmt.Sscanf(v, "%d", &n)
		if n <= 0 {
			return 1
		}
		return n
	}
	getBridgeAcquireTimeout := func() time.Duration {
		v := getEnv("BRIDGE_ACQUIRE_TIMEOUT_MS", "1500")
		n := 1500
		fmt.Sscanf(v, "%d", &n)
		if n <= 0 {
			return 1500 * time.Millisecond
		}
		return time.Duration(n) * time.Millisecond
	}
	getIndexerClients := func(concurrency int) int {
		v := getEnv("INDEXER_CLIENTS", "")
		if v == "" {
			return concurrency
		}
		n := concurrency
		fmt.Sscanf(v, "%d", &n)
		if n <= 0 {
			return concurrency
		}
		return n
	}
	getDiscoveryClients := func(concurrency int) int {
		v := getEnv("DISCOVERY_CLIENTS", "")
		if v == "" {
			if concurrency < 2 {
				return 2
			}
			return concurrency
		}
		n := concurrency
		fmt.Sscanf(v, "%d", &n)
		if n <= 0 {
			if concurrency < 2 {
				return 2
			}
			return concurrency
		}
		return n
	}
	getDiscoveryMaxCandidates := func() int {
		v := getEnv("DISCOVERY_MAX_CANDIDATES", "5")
		n := 5
		fmt.Sscanf(v, "%d", &n)
		if n <= 0 {
			return 5
		}
		return n
	}
	getDiscoveryTimeout := func() time.Duration {
		v := getEnv("DISCOVERY_TIMEOUT_MS", "5000")
		n := 5000
		fmt.Sscanf(v, "%d", &n)
		if n <= 0 {
			return 5 * time.Second
		}
		return time.Duration(n) * time.Millisecond
	}
	getGitHubAuthCacheTTL := func() time.Duration {
		v := getEnv("GITHUB_AUTH_CACHE_TTL_MS", "300000")
		n := 300000
		fmt.Sscanf(v, "%d", &n)
		if n <= 0 {
			return 5 * time.Minute
		}
		return time.Duration(n) * time.Millisecond
	}
	concurrency := getConcurrency()
	return config{
		Port:                   getEnv("PORT", "8080"),
		BinaryPath:             getEnv("CBM_BINARY", defaultBinaryPath()),
		CacheDir:               getEnv("FLEET_CACHE_DIR", "/app/fleet-cache"),
		ReposManifest:          getEnv("REPOS_MANIFEST", defaultManifestPath()),
		BearerToken:            getEnv("BEARER_TOKEN", ""),
		GitHubToken:            getEnv("GITHUB_TOKEN", ""),
		GitHubAuthEnabled:      getBool("GITHUB_AUTH_ENABLED", false),
		GitHubAllowedOrgs:      getStringList("GITHUB_ALLOWED_ORGS"),
		GitHubAPIBaseURL:       getEnv("GITHUB_API_BASE_URL", "https://api.github.com"),
		GitHubAuthCacheTTL:     getGitHubAuthCacheTTL(),
		WebhookSecret:          getEnv("GITHUB_WEBHOOK_SECRET", ""),
		Concurrency:            concurrency,
		BridgeClients:          getBridgeClients(),
		BridgeAcquireTimeout:   getBridgeAcquireTimeout(),
		IndexerClients:         getIndexerClients(concurrency),
		DiscoveryClients:       getDiscoveryClients(concurrency),
		DiscoveryMaxCandidates: getDiscoveryMaxCandidates(),
		DiscoveryTimeout:       getDiscoveryTimeout(),
		IncrementalCron:        getEnv("CRON_INCREMENTAL", "0 */6 * * *"),
		FullCron:               getEnv("CRON_FULL", "0 2 * * 0"),
	}
}

func defaultManifestPath() string {
	candidates := []string{
		"/app/REPOS.local.yaml",
		"/app/REPOS.yaml",
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return "/app/REPOS.yaml"
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
	logger      *slog.Logger
	githubToken string
}

func (g *gitCloner) EnsureClone(ctx context.Context, githubURL, localPath string) error {
	if _, err := os.Stat(filepath.Join(localPath, ".git")); err == nil {
		// Already cloned — fetch latest
		g.logger.Debug("updating clone", "path", localPath)
		cmd := g.gitCommand(ctx, localPath, githubURL, "fetch", "--depth=1", "origin", "HEAD")
		if out, err := cmd.CombinedOutput(); err != nil {
			if isGitHubHTTPSAuthError(string(out)) {
				g.logger.Warn("git fetch auth failed, using existing clone", "path", localPath)
				if err := g.restoreWorkingTree(ctx, githubURL, localPath, "HEAD"); err != nil {
					return err
				}
				return g.validateClone(localPath)
			}
			return fmt.Errorf("git fetch: %w\n%s", err, out)
		}
		if err := g.restoreWorkingTree(ctx, githubURL, localPath, "FETCH_HEAD"); err != nil {
			return err
		}
		return g.validateClone(localPath)
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
	cmd := g.gitCommand(cloneCtx, "", githubURL, "clone", "--depth=1", githubURL, localPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git clone %q: %w\n%s", githubURL, err, out)
	}
	return g.validateClone(localPath)
}

func isGitHubHTTPSAuthError(output string) bool {
	return strings.Contains(output, "could not read Username for 'https://github.com'")
}

func (g *gitCloner) gitCommand(ctx context.Context, dir, githubURL string, args ...string) *exec.Cmd {
	gitArgs := make([]string, 0, len(args)+4)
	if g.githubToken != "" && strings.HasPrefix(githubURL, "https://github.com/") {
		auth := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + g.githubToken))
		gitArgs = append(gitArgs,
			"-c", "credential.helper=",
			"-c", "http.https://github.com/.extraheader=AUTHORIZATION: basic "+auth,
		)
	}
	gitArgs = append(gitArgs, args...)
	cmd := exec.CommandContext(ctx, "git", gitArgs...)
	if dir != "" {
		cmd.Dir = dir
	}
	return cmd
}

func (g *gitCloner) restoreWorkingTree(ctx context.Context, githubURL, localPath, ref string) error {
	cmd := g.gitCommand(ctx, localPath, githubURL, "reset", "--hard", ref)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git reset --hard %s: %w\n%s", ref, err, out)
	}
	cmd = g.gitCommand(ctx, localPath, githubURL, "clean", "-fd")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git clean -fd: %w\n%s", err, out)
	}
	return nil
}

func (g *gitCloner) validateClone(localPath string) error {
	ok, err := hasWorkingTreeFiles(localPath)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("clone at %q has no checked out files", localPath)
	}
	return nil
}

func hasWorkingTreeFiles(root string) (bool, error) {
	var found bool
	stop := errors.New("found working tree file")
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		if info.IsDir() {
			if info.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		found = true
		return stop
	})
	if err != nil && !errors.Is(err, stop) {
		return false, err
	}
	return found, nil
}

type bridgePoolClient interface {
	ServerInfo() mcp.ServerInfo
	Call(ctx context.Context, method string, params interface{}) (json.RawMessage, error)
	CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error)
	Close()
}

var newBridgePoolClient = func(ctx context.Context, binPath string) (bridgePoolClient, error) {
	return mcp.NewClient(ctx, binPath)
}

type mcpBridgeClientPool struct {
	binPath        string
	acquireTimeout time.Duration
	mu             sync.Mutex
	clients        chan bridgePoolClient
	all            []bridgePoolClient
	info           mcp.ServerInfo
}

func newMCPBridgeClientPool(ctx context.Context, binPath string, size int, acquireTimeout time.Duration) (*mcpBridgeClientPool, error) {
	if size <= 0 {
		size = 1
	}
	pool := &mcpBridgeClientPool{
		binPath:        binPath,
		acquireTimeout: acquireTimeout,
		clients:        make(chan bridgePoolClient, size),
		all:            make([]bridgePoolClient, 0, size),
	}
	for i := 0; i < size; i++ {
		client, err := newBridgePoolClient(ctx, binPath)
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("start bridge client %d/%d: %w", i+1, size, err)
		}
		if i == 0 {
			pool.info = client.ServerInfo()
		}
		pool.all = append(pool.all, client)
		pool.clients <- client
	}
	return pool, nil
}

func (p *mcpBridgeClientPool) ServerInfo() mcp.ServerInfo {
	return p.info
}

func (p *mcpBridgeClientPool) Close() {
	for _, client := range p.all {
		client.Close()
	}
}

func (p *mcpBridgeClientPool) borrow(ctx context.Context) (bridgePoolClient, error) {
	if p.acquireTimeout <= 0 {
		select {
		case client := <-p.clients:
			return client, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	acquireCtx, cancel := context.WithTimeoutCause(ctx, p.acquireTimeout, bridge.ErrBackendBusy)
	defer cancel()

	select {
	case client := <-p.clients:
		return client, nil
	case <-acquireCtx.Done():
		if errors.Is(context.Cause(acquireCtx), bridge.ErrBackendBusy) {
			return nil, bridge.ErrBackendBusy
		}
		return nil, ctx.Err()
	}
}

func (p *mcpBridgeClientPool) release(client bridgePoolClient) {
	if client == nil {
		return
	}
	p.clients <- client
}

func (p *mcpBridgeClientPool) Call(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	client, err := p.borrow(ctx)
	if err != nil {
		return nil, err
	}

	type callResult struct {
		result json.RawMessage
		err    error
	}

	resultCh := make(chan callResult, 1)
	go func() {
		result, callErr := client.Call(ctx, method, params)
		resultCh <- callResult{result: result, err: callErr}
	}()

	select {
	case out := <-resultCh:
		p.release(client)
		return out.result, out.err
	case <-ctx.Done():
		client.Close()
		go p.replaceClientAsync(client)
		return nil, ctx.Err()
	}
}

func (p *mcpBridgeClientPool) CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error) {
	client, err := p.borrow(ctx)
	if err != nil {
		return nil, err
	}

	type toolCallResult struct {
		result *mcp.ToolResult
		err    error
	}

	resultCh := make(chan toolCallResult, 1)
	go func() {
		result, callErr := client.CallTool(ctx, name, params)
		resultCh <- toolCallResult{result: result, err: callErr}
	}()

	select {
	case out := <-resultCh:
		p.release(client)
		return out.result, out.err
	case <-ctx.Done():
		client.Close()
		go p.replaceClientAsync(client)
		return nil, ctx.Err()
	}
}

func (p *mcpBridgeClientPool) replaceClientAsync(dead bridgePoolClient) {
	replacementCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	replacement, err := newBridgePoolClient(replacementCtx, p.binPath)
	if err != nil {
		slog.Error("failed to replace timed out bridge client", "err", err)
		return
	}

	p.mu.Lock()
	for i, client := range p.all {
		if client == dead {
			p.all[i] = replacement
			break
		}
	}
	p.mu.Unlock()

	p.release(replacement)
}

type indexToolClient interface {
	CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error)
	Close()
}

var newIndexToolClient = func(ctx context.Context, binPath string) (indexToolClient, error) {
	return mcp.NewClient(ctx, binPath)
}

type mcpToolClientPool struct {
	binPath string
	mu      sync.Mutex
	clients chan indexToolClient
	all     []indexToolClient
}

func newMCPToolClientPool(ctx context.Context, binPath string, size int) (*mcpToolClientPool, error) {
	if size <= 0 {
		size = 1
	}
	pool := &mcpToolClientPool{
		binPath: binPath,
		clients: make(chan indexToolClient, size),
		all:     make([]indexToolClient, 0, size),
	}
	for i := 0; i < size; i++ {
		client, err := newIndexToolClient(ctx, binPath)
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("start indexer client %d/%d: %w", i+1, size, err)
		}
		pool.all = append(pool.all, client)
		pool.clients <- client
	}
	return pool, nil
}

func (p *mcpToolClientPool) Close() {
	for _, client := range p.all {
		client.Close()
	}
}

func (p *mcpToolClientPool) borrow(ctx context.Context) (indexToolClient, error) {
	select {
	case client := <-p.clients:
		return client, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *mcpToolClientPool) release(client indexToolClient) {
	if client == nil {
		return
	}
	p.clients <- client
}

func (p *mcpToolClientPool) CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error) {
	client, err := p.borrow(ctx)
	if err != nil {
		return nil, err
	}

	type toolCallResult struct {
		result *mcp.ToolResult
		err    error
	}

	resultCh := make(chan toolCallResult, 1)
	go func() {
		result, err := client.CallTool(ctx, name, params)
		resultCh <- toolCallResult{result: result, err: err}
	}()

	select {
	case out := <-resultCh:
		p.release(client)
		return out.result, out.err
	case <-ctx.Done():
		client.Close()
		go p.replaceClientAsync(client)
		return nil, ctx.Err()
	}
}

func (p *mcpToolClientPool) replaceClientAsync(dead indexToolClient) {
	replacementCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	replacement, err := newIndexToolClient(replacementCtx, p.binPath)
	if err != nil {
		slog.Error("failed to replace timed out MCP client", "err", err)
		return
	}

	p.mu.Lock()
	for i, client := range p.all {
		if client == dead {
			p.all[i] = replacement
			break
		}
	}
	p.mu.Unlock()

	p.release(replacement)
}

type mcpIndexClientPool struct {
	*mcpToolClientPool
}

func newMCPIndexClientPool(ctx context.Context, binPath string, size int) (*mcpIndexClientPool, error) {
	pool, err := newMCPToolClientPool(ctx, binPath, size)
	if err != nil {
		return nil, err
	}
	return &mcpIndexClientPool{mcpToolClientPool: pool}, nil
}

func (p *mcpIndexClientPool) IndexRepository(ctx context.Context, repoPath, mode string) error {
	result, err := p.CallTool(ctx, "index_repository", map[string]interface{}{
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

type mcpDiscoveryClientPool struct {
	*mcpToolClientPool
}

func newMCPDiscoveryClientPool(ctx context.Context, binPath string, size int) (*mcpDiscoveryClientPool, error) {
	pool, err := newMCPToolClientPool(ctx, binPath, size)
	if err != nil {
		return nil, err
	}
	return &mcpDiscoveryClientPool{mcpToolClientPool: pool}, nil
}

type bridgeClient interface {
	ServerInfo() mcp.ServerInfo
	Call(ctx context.Context, method string, params interface{}) (json.RawMessage, error)
	CallTool(ctx context.Context, name string, params map[string]interface{}) (*mcp.ToolResult, error)
}

// mcpBridgeBackend implements bridge.Backend by forwarding to the MCP client.
type mcpBridgeBackend struct {
	client    bridgeClient
	discovery discovery.Service
}

func (b *mcpBridgeBackend) Call(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
	if b.client == nil {
		return nil, bridge.ErrBackendUnavailable
	}

	switch method {
	case "initialize":
		return b.initialize(params)
	case "ping":
		return json.RawMessage(`{}`), nil
	case "tools/list":
		raw, err := b.client.Call(ctx, "tools/list", nil)
		if err != nil {
			return nil, err
		}
		return b.appendDiscoveryTool(raw)
	case "tools/call":
		var paramMap map[string]interface{}
		if len(params) > 0 {
			if err := json.Unmarshal(params, &paramMap); err != nil {
				return nil, fmt.Errorf("parse params: %w", err)
			}
		}

		name, _ := paramMap["name"].(string)
		if name == "" {
			return nil, errors.New("missing tool name")
		}
		args, _ := paramMap["arguments"].(map[string]interface{})
		if name == discovery.NewDefinition().Name {
			return b.callDiscoveryTool(ctx, args)
		}

		result, err := b.client.CallTool(ctx, name, args)
		if err != nil {
			return nil, err
		}

		return json.Marshal(result)
	default:
		return nil, bridge.ErrMethodNotFound
	}
}

func (b *mcpBridgeBackend) appendDiscoveryTool(raw json.RawMessage) (json.RawMessage, error) {
	if b.discovery == nil {
		return raw, nil
	}

	var payload struct {
		Tools []map[string]interface{} `json:"tools"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("parse tools/list response: %w", err)
	}

	def := b.discovery.Definition()
	tool := map[string]interface{}{
		"name":        def.Name,
		"description": def.Description,
		"inputSchema": def.InputSchema,
	}
	payload.Tools = append(payload.Tools, tool)
	return json.Marshal(payload)
}

func (b *mcpBridgeBackend) callDiscoveryTool(ctx context.Context, args map[string]interface{}) (json.RawMessage, error) {
	if b.discovery == nil {
		return nil, errors.New("discover_projects unavailable")
	}

	var req discovery.Request
	if args != nil {
		rawArgs, err := json.Marshal(args)
		if err != nil {
			return nil, fmt.Errorf("marshal discover_projects args: %w", err)
		}
		if err := json.Unmarshal(rawArgs, &req); err != nil {
			return nil, fmt.Errorf("parse discover_projects args: %w", err)
		}
	}
	req.Query = strings.TrimSpace(req.Query)
	if req.Query == "" {
		return nil, errors.New("discover_projects: query is required")
	}
	if req.Limit <= 0 {
		req.Limit = 5
	}
	if _, ok := args["include_graph_confidence"]; !ok {
		req.IncludeGraphConfidence = true
	}

	resp, err := b.discovery.DiscoverProjects(ctx, req)
	if err != nil {
		return nil, err
	}
	text, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("marshal discover_projects response: %w", err)
	}

	return json.Marshal(mcp.ToolResult{
		Content: []mcp.Content{{Type: "text", Text: string(text)}},
		IsError: false,
	})
}

func (b *mcpBridgeBackend) initialize(params json.RawMessage) (json.RawMessage, error) {
	type initializeParams struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	type initializeResult struct {
		ProtocolVersion string                 `json:"protocolVersion"`
		Capabilities    map[string]interface{} `json:"capabilities"`
		ServerInfo      mcp.ServerInfo         `json:"serverInfo"`
	}

	version := supportedProtocolVersions[0]
	if len(params) > 0 {
		var p initializeParams
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, fmt.Errorf("parse initialize params: %w", err)
		}
		for _, supported := range supportedProtocolVersions {
			if p.ProtocolVersion == supported {
				version = supported
				break
			}
		}
	}

	return json.Marshal(initializeResult{
		ProtocolVersion: version,
		Capabilities: map[string]interface{}{
			"tools": map[string]interface{}{},
		},
		ServerInfo: b.client.ServerInfo(),
	})
}

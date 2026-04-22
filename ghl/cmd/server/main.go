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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/robfig/cron/v3"

	ghlauth "github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/auth"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/bridge"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/cachepersist"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/discovery"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/indexer"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/mcp"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/searchtools"
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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := os.MkdirAll(cfg.CloneCacheDir, 0o750); err != nil {
		slog.Error("failed to create clone cache dir", "path", cfg.CloneCacheDir, "err", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(cfg.CBMCacheDir, 0o750); err != nil {
		slog.Error("failed to create cbm cache dir", "path", cfg.CBMCacheDir, "err", err)
		os.Exit(1)
	}

	var artifactSync *cachepersist.Syncer
	if cfg.ArtifactsEnabled {
		var err error
		switch strings.ToLower(strings.TrimSpace(cfg.ArtifactsBackend)) {
		case "gcs":
			artifactSync, err = cachepersist.NewGCS(ctx, cfg.CBMCacheDir, cfg.ArtifactsBucket, cfg.ArtifactsPrefix)
		default:
			artifactSync, err = cachepersist.New(cfg.CBMCacheDir, cfg.ArtifactDir)
		}
		if err != nil {
			slog.Error("failed to initialize artifact sync", "runtime_dir", cfg.CBMCacheDir, "artifact_dir", cfg.ArtifactDir, "err", err)
			os.Exit(1)
		}
		defer func() {
			if err := artifactSync.Close(); err != nil {
				slog.Warn("failed to close artifact sync", "err", err)
			}
		}()
		if cfg.ArtifactsSkipHydrate {
			slog.Info("skipping persisted index hydrate", "artifact_dir", cfg.ArtifactDir, "cache_dir", cfg.CBMCacheDir)
		} else {
			hydrateStart := time.Now()
			hydrated, err := artifactSync.Hydrate()
			if err != nil {
				slog.Warn("failed to hydrate persisted indexes (continuing with empty cache)", "err", err, "duration", time.Since(hydrateStart))
			} else {
				slog.Info("hydration complete", "files", hydrated, "duration", time.Since(hydrateStart), "artifact_dir", cfg.ArtifactDir, "cache_dir", cfg.CBMCacheDir)
			}
		}
	}

	// ── Load fleet manifest (YAML first for fast startup) ────

	m, err := manifest.Load(cfg.ReposManifest)
	if err != nil {
		slog.Error("failed to load repos manifest", "path", cfg.ReposManifest, "err", err)
		os.Exit(1)
	}
	slog.Info("fleet manifest loaded", "repos", len(m.Repos))

	cloner := &gitCloner{
		logger:      logger,
		githubToken: cfg.GitHubToken,
	}

	// activityChecker filters stale repos during fleet runs.
	var actChecker indexer.ActivityChecker
	if cfg.GitHubToken != "" {
		actChecker = &githubActivityChecker{
			token:   cfg.GitHubToken,
			baseURL: cfg.GitHubAPIBaseURL,
			org:     firstOrg(cfg.GitHubAllowedOrgs),
			days:    7,
		}
	}

	newFleetIndexer := func(client indexer.Client, discoverySvc *discovery.Discoverer) *indexer.Indexer {
		return indexer.New(indexer.Config{
			Client:      client,
			Cloner:      cloner,
			CacheDir:    cfg.CloneCacheDir,
			Concurrency: cfg.Concurrency,
			ProjectNameFunc: func(repoSlug string) string {
				return projectNameFromPath(filepath.Join(cfg.CloneCacheDir, repoSlug))
			},
			ActivityChecker: actChecker,
			OnRepoStart:     func(slug string) { slog.Info("indexing repo", "repo", slug) },
			OnRepoDone: func(slug string, err error) {
				if err != nil {
					slog.Error("repo indexing failed", "repo", slug, "err", err)
					return
				}
				if artifactSync != nil {
					clonePath := filepath.Join(cfg.CloneCacheDir, slug)
					projectName := projectNameFromPath(clonePath)
					persisted, persistErr := artifactSync.PersistProject(projectName)
					if persistErr != nil {
						slog.Error("failed to persist project index", "repo", slug, "project", projectName, "err", persistErr)
					} else {
						slog.Info("persisted project index", "repo", slug, "project", projectName, "files", persisted)
					}
				}
				if discoverySvc != nil {
					discoverySvc.Invalidate()
				}
				slog.Info("repo indexed", "repo", slug)
			},
			OnAllComplete: func(result indexer.IndexResult) {
				slog.Info("fleet indexing complete", "total", result.Total, "ok", result.Succeeded, "failed", result.Failed, "skipped", result.Skipped)
			},
		})
	}

	if cfg.RunMode == "index-all" {
		indexPool, err := newMCPIndexClientPool(ctx, cfg.BinaryPath, cfg.IndexerClients, cfg.IndexerClientMaxUses)
		if err != nil {
			slog.Error("failed to start indexer client pool", "clients", cfg.IndexerClients, "err", err)
			os.Exit(1)
		}
		defer indexPool.Close()
		slog.Info("indexer client pool started", "clients", cfg.IndexerClients, "max_uses", cfg.IndexerClientMaxUses)

		idx := newFleetIndexer(indexPool, nil)
		slog.Info("running one-shot fleet indexing job", "force", cfg.RunForce)
		result := idx.IndexAll(context.Background(), m.Repos, cfg.RunForce)
		slog.Info("one-shot fleet indexing complete", "total", result.Total, "ok", result.Succeeded, "failed", result.Failed, "skipped", result.Skipped)
		if result.Failed > 0 {
			os.Exit(1)
		}
		return
	}

	// ── Start MCP binary clients ─────────────────────────────

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

	indexPool, err := newMCPIndexClientPool(ctx, cfg.BinaryPath, cfg.IndexerClients, cfg.IndexerClientMaxUses)
	if err != nil {
		slog.Error("failed to start indexer client pool", "clients", cfg.IndexerClients, "err", err)
		os.Exit(1)
	}
	defer indexPool.Close()
	slog.Info("indexer client pool started", "clients", cfg.IndexerClients, "max_uses", cfg.IndexerClientMaxUses)

	discoveryPool, err := newMCPDiscoveryClientPool(ctx, cfg.BinaryPath, cfg.DiscoveryClients)
	if err != nil {
		slog.Error("failed to start discovery client pool", "clients", cfg.DiscoveryClients, "err", err)
		os.Exit(1)
	}
	defer discoveryPool.Close()
	slog.Info("discovery client pool started", "clients", cfg.DiscoveryClients)

	var requestAuthenticator bridge.Authenticator
	{ // Auth is always on — no env flag
		requestAuthenticator = ghlauth.NewGitHubAuthenticator(ghlauth.GitHubConfig{
			BaseURL:     cfg.GitHubAPIBaseURL,
			AllowedOrgs: cfg.GitHubAllowedOrgs,
			CacheTTL:    cfg.GitHubAuthCacheTTL,
		})
		slog.Info("github bearer auth enabled", "allowed_orgs", cfg.GitHubAllowedOrgs)
	}

	// ── Build indexer ────────────────────────────────────────

	var discoverySvc *discovery.Discoverer
	maxGraphCandidates := 3
	if cfg.DiscoveryMaxCandidates > 0 && cfg.DiscoveryMaxCandidates < maxGraphCandidates {
		maxGraphCandidates = cfg.DiscoveryMaxCandidates
	}
	discoverySvc = discovery.NewService(discoveryPool, *m, discovery.Options{
		MaxBM25Candidates:  cfg.DiscoveryMaxCandidates,
		MaxGraphCandidates: maxGraphCandidates,
		RequestTimeout:     cfg.DiscoveryTimeout,
	})
	idx := newFleetIndexer(indexPool, discoverySvc)

	var fleetIndexing atomic.Bool
	startFleetIndex := func(reason string, force bool) bool {
		if !fleetIndexing.CompareAndSwap(false, true) {
			slog.Warn("fleet index already running", "reason", reason, "force", force)
			return false
		}
		go func() {
			defer fleetIndexing.Store(false)
			slog.Info("fleet index starting", "reason", reason, "force", force)
			result := idx.IndexAll(context.Background(), m.Repos, force)
			slog.Info("fleet index complete", "reason", reason, "force", force, "total", result.Total, "ok", result.Succeeded, "failed", result.Failed, "skipped", result.Skipped)
		}()
		return true
	}

	// ── Fleet scheduler ──────────────────────────────────────

	c := cron.New()
	{ // Scheduled indexing — always on
		c.AddFunc(cfg.IncrementalCron, func() {
			startFleetIndex("cron-incremental", false)
		})
		c.AddFunc(cfg.FullCron, func() {
			startFleetIndex("cron-full", true)
		})
		c.Start()
		defer c.Stop()
		slog.Info("scheduled indexing enabled", "incremental_cron", cfg.IncrementalCron, "full_cron", cfg.FullCron)
	}

	// ── HTTP router ──────────────────────────────────────────

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(5 * time.Minute))

	// Search result cache — per-instance, 60 s TTL, 1000 entry max.
	searchCache := bridge.NewSearchCache(1000, 60*time.Second)
	slog.Info("search result cache enabled", "max_size", 1000, "ttl_s", 60)

	// Bridge: forward MCP calls to the binary
	bridgeHandler := bridge.NewHandler(
		&mcpBridgeBackend{client: bridgePool, discovery: discoverySvc, cache: searchCache, cacheDir: cfg.CBMCacheDir},
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
				return
			}
			// Persist .db to GCS (same as fleet OnRepoDone)
			if artifactSync != nil {
				clonePath := filepath.Join(cfg.CloneCacheDir, repoSlug)
				projectName := projectNameFromPath(clonePath)
				if _, persistErr := artifactSync.PersistProject(projectName); persistErr != nil {
					slog.Error("webhook: persist failed", "repo", repoSlug, "err", persistErr)
				} else {
					slog.Info("webhook: persisted", "repo", repoSlug)
				}
			}
			if discoverySvc != nil {
				discoverySvc.Invalidate()
			}
		},
	})
	r.Post("/webhooks/github", wh.ServeHTTP)

	// Manual trigger: index a single repo by slug.
	// Runs the same persist as the fleet OnRepoDone callback.
	r.Post("/index/{repoSlug}", requireAuth(func(w http.ResponseWriter, req *http.Request) {
		slug := chi.URLParam(req, "repoSlug")
		repo, ok := m.FindByName(slug)
		if !ok {
			http.Error(w, "repo not found in manifest", http.StatusNotFound)
			return
		}
		go func() {
			slog.Info("manual index: starting", "repo", slug)
			indexCtx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer cancel()
			if err := idx.IndexRepo(indexCtx, repo, true); err != nil {
				slog.Error("manual index failed", "repo", slug, "err", err)
				return
			}
			// Persist .db to GCS (same as fleet OnRepoDone)
			if artifactSync != nil {
				clonePath := filepath.Join(cfg.CloneCacheDir, slug)
				projectName := projectNameFromPath(clonePath)
				persisted, persistErr := artifactSync.PersistProject(projectName)
				if persistErr != nil {
					slog.Error("manual index: persist failed", "repo", slug, "project", projectName, "err", persistErr)
				} else {
					slog.Info("manual index: persisted", "repo", slug, "project", projectName, "files", persisted)
				}
			}
			if discoverySvc != nil {
				discoverySvc.Invalidate()
			}
			slog.Info("manual index complete", "repo", slug)
		}()
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"accepted":true,"repo":%q}`, slug)
	}))

	r.Post("/index-all", requireAuth(func(w http.ResponseWriter, req *http.Request) {
		force := req.URL.Query().Get("force") == "1" || strings.EqualFold(req.URL.Query().Get("force"), "true")
		if !startFleetIndex("manual", force) {
			http.Error(w, "fleet index already running", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"accepted":true,"force":%t}`, force)
	}))

	// Fleet status endpoint
	r.Get("/status", requireAuth(func(w http.ResponseWriter, req *http.Request) {
		artifactCount := 0
		artifactLocation := cfg.ArtifactDir
		if artifactSync != nil {
			count, err := artifactSync.CountArtifacts()
			if err != nil {
				slog.Warn("failed to count persisted indexes", "err", err)
			} else {
				artifactCount = count
			}
			artifactLocation = artifactSync.ArtifactDir
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"repos":                    len(m.Repos),
			"version":                  bridgePool.ServerInfo().Version,
			"binary":                   cfg.BinaryPath,
			"clone_cache":              cfg.CloneCacheDir,
			"cbm_cache":                cfg.CBMCacheDir,
			"artifact_dir":             artifactLocation,
			"artifact_files":           artifactCount,
			"artifacts_enabled":        cfg.ArtifactsEnabled,
			"manifest":                 cfg.ReposManifest,
			"concurrency":              cfg.Concurrency,
			"bridge_clients":           cfg.BridgeClients,
			"bridge_acquire_timeout":   cfg.BridgeAcquireTimeout.Milliseconds(),
			"indexer_clients":          cfg.IndexerClients,
			"discovery_clients":        cfg.DiscoveryClients,
			"discovery_max_candidates": cfg.DiscoveryMaxCandidates,
			"discovery_timeout_ms":     cfg.DiscoveryTimeout.Milliseconds(),
			"startup_index_enabled":    false,
			"scheduled_index_enabled":  true,
			"fleet_index_running":      fleetIndexing.Load(),
			"github_auth_enabled":      true,
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

	// Startup indexing disabled — hydration from GCS is sufficient.
	// Scheduled cron handles ongoing indexing.

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
	Port                     string
	BinaryPath               string
	CloneCacheDir            string
	CBMCacheDir              string
	ArtifactDir              string
	ArtifactsEnabled         bool
	ArtifactsBackend         string
	ArtifactsBucket          string
	ArtifactsPrefix          string
	ArtifactsSkipHydrate     bool
	ReposManifest            string
	BearerToken              string
	GitHubToken              string
	GitHubAuthEnabled        bool
	GitHubAllowedOrgs        []string
	GitHubAPIBaseURL         string
	GitHubAuthCacheTTL       time.Duration
	WebhookSecret            string
	Concurrency              int
	BridgeClients            int
	BridgeAcquireTimeout     time.Duration
	IndexerClients           int
	IndexerClientMaxUses     int
	DiscoveryClients         int
	DiscoveryMaxCandidates   int
	DiscoveryTimeout         time.Duration
	IncrementalCron          string
	FullCron                 string
	StartupIndexEnabled      bool
	ScheduledIndexingEnabled bool
	RunMode                  string
	RunForce                 bool
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
	getIndexerClientMaxUses := func() int {
		v := getEnv("INDEXER_CLIENT_MAX_USES", "1")
		n := 1
		fmt.Sscanf(v, "%d", &n)
		if n <= 0 {
			return 1
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
		Port:                     getEnv("PORT", "8080"),
		BinaryPath:               getEnv("CBM_BINARY", defaultBinaryPath()),
		CloneCacheDir:            getEnv("FLEET_CACHE_DIR", "/data/fleet-cache/repos"),
		CBMCacheDir:              getEnv("CBM_CACHE_DIR", "/tmp/codebase-memory-mcp"),
		ArtifactDir:              getEnv("CBM_ARTIFACT_DIR", "/data/fleet-cache/indexes"),
		ArtifactsEnabled:         getBool("ARTIFACTS_ENABLED", true),
		ArtifactsBackend:         getEnv("ARTIFACTS_BACKEND", "filesystem"),
		ArtifactsBucket:          getEnv("ARTIFACTS_BUCKET", ""),
		ArtifactsPrefix:          getEnv("ARTIFACTS_PREFIX", ""),
		ArtifactsSkipHydrate:     getBool("ARTIFACTS_SKIP_HYDRATE", false),
		ReposManifest:            getEnv("REPOS_MANIFEST", defaultManifestPath()),
		BearerToken:              getEnv("BEARER_TOKEN", ""),
		GitHubToken:              getEnv("GITHUB_TOKEN", ""),
		GitHubAuthEnabled:        getBool("GITHUB_AUTH_ENABLED", false),
		GitHubAllowedOrgs:        getStringList("GITHUB_ALLOWED_ORGS"),
		GitHubAPIBaseURL:         getEnv("GITHUB_API_BASE_URL", "https://api.github.com"),
		GitHubAuthCacheTTL:       getGitHubAuthCacheTTL(),
		WebhookSecret:            getEnv("GITHUB_WEBHOOK_SECRET", ""),
		Concurrency:              concurrency,
		BridgeClients:            getBridgeClients(),
		BridgeAcquireTimeout:     getBridgeAcquireTimeout(),
		IndexerClients:           getIndexerClients(concurrency),
		IndexerClientMaxUses:     getIndexerClientMaxUses(),
		DiscoveryClients:         getDiscoveryClients(concurrency),
		DiscoveryMaxCandidates:   getDiscoveryMaxCandidates(),
		DiscoveryTimeout:         getDiscoveryTimeout(),
		IncrementalCron:          getEnv("CRON_INCREMENTAL", "0 */6 * * *"),
		FullCron:                 getEnv("CRON_FULL", "0 2 * * 0"),
		StartupIndexEnabled:      getBool("STARTUP_INDEX_ENABLED", false),
		ScheduledIndexingEnabled: getBool("SCHEDULED_INDEXING_ENABLED", false),
		RunMode:                  strings.TrimSpace(getEnv("RUN_MODE", "serve")),
		RunForce:                 getBool("RUN_FORCE", false),
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

// projectNamePrefix is hardcoded — always use this prefix for consistent naming.
var projectNamePrefix = "data-fleet-cache-repos"

// projectNameFromPath returns the canonical project name for a clone path.
// When PROJECT_NAME_PREFIX is set, it uses prefix + slug (e.g.
// "data-fleet-cache-repos-membership-backend"). Otherwise it falls back to
// replacing path separators with dashes.
func projectNameFromPath(absPath string) string {
	path := filepath.ToSlash(strings.TrimSpace(absPath))
	if path == "" {
		return "root"
	}

	// Preferred: prefix + last path segment (the repo slug).
	if projectNamePrefix != "" {
		slug := filepath.Base(path)
		if slug == "" || slug == "." || slug == "/" {
			return "root"
		}
		return projectNamePrefix + "-" + slug
	}

	// Fallback (local dev, no prefix set): replace separators with dashes.
	var b strings.Builder
	b.Grow(len(path))
	prevDash := false
	for _, r := range path {
		if r == '/' || r == ':' {
			if prevDash {
				continue
			}
			b.WriteByte('-')
			prevDash = true
			continue
		}
		b.WriteRune(r)
		prevDash = r == '-'
	}

	project := strings.Trim(b.String(), "-")
	if project == "" {
		return "root"
	}
	return project
}

// ── Activity checking ──────────────────────────────────────────

// githubActivityChecker implements indexer.ActivityChecker using the GitHub API
// to skip repos with no commits in the last N days.
type githubActivityChecker struct {
	token   string
	baseURL string
	org     string
	days    int
}

func (c *githubActivityChecker) IsActive(ctx context.Context, repoName string) bool {
	if c.token == "" || c.org == "" {
		return true // can't check, assume active
	}
	url := fmt.Sprintf("%s/repos/%s/%s/commits?per_page=1", c.baseURL, c.org, repoName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		slog.Warn("activity check: request build failed", "repo", repoName, "err", err)
		return true
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github+json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		slog.Warn("activity check: request failed", "repo", repoName, "err", err)
		return true // network error — assume active to avoid skipping
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// 404 = repo deleted/renamed, 403 = rate limited; assume active to be safe
		return true
	}

	var commits []struct {
		Commit struct {
			Committer struct {
				Date time.Time `json:"date"`
			} `json:"committer"`
		} `json:"commit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&commits); err != nil || len(commits) == 0 {
		return true
	}

	cutoff := time.Now().AddDate(0, 0, -c.days)
	active := commits[0].Commit.Committer.Date.After(cutoff)
	if !active {
		slog.Info("activity check: stale repo, skipping", "repo", repoName,
			"last_commit", commits[0].Commit.Committer.Date.Format(time.RFC3339),
			"cutoff_days", c.days)
	}
	return active
}

func firstOrg(orgs []string) string {
	if len(orgs) > 0 {
		return orgs[0]
	}
	return ""
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
			outStr := string(out)
			if isGitHubHTTPSAuthError(outStr) {
				g.logger.Warn("git fetch auth failed, using existing clone", "path", localPath)
				if err := g.restoreWorkingTree(ctx, githubURL, localPath, "HEAD"); err != nil {
					return err
				}
				return g.validateClone(localPath)
			}
			// Corrupt clone (e.g. "index file smaller than expected") — nuke and re-clone
			if strings.Contains(outStr, "index file smaller") ||
				strings.Contains(outStr, "bad signature") ||
				strings.Contains(outStr, "index file corrupt") {
				g.logger.Warn("corrupt git clone detected, removing for fresh clone", "path", localPath, "err", outStr)
				os.RemoveAll(localPath)
				// Fall through to fresh clone below
			} else {
				return fmt.Errorf("git fetch: %w\n%s", err, out)
			}
		} else {
			if err := g.restoreWorkingTree(ctx, githubURL, localPath, "FETCH_HEAD"); err != nil {
				return err
			}
			return g.validateClone(localPath)
		}
	}
	// Fresh clone
	if err := os.MkdirAll(localPath, 0750); err != nil {
		return fmt.Errorf("mkdir %q: %w", localPath, err)
	}
	// Remove empty dir to allow clone into it
	os.Remove(localPath)
	g.logger.Info("cloning repo", "url", githubURL, "path", localPath)
	// No timeout — large monorepos can take 20+ minutes to clone and index.
	// The fleet indexer uses context.Background() which has no deadline.
	cmd := g.gitCommand(ctx, "", githubURL, "clone", "--depth=1", githubURL, localPath)
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
	// Remove stale index.lock left by crashed git processes — prevents permanent failure
	// Remove stale lock files left by crashed git processes
	for _, lockFile := range []string{"index.lock", "HEAD.lock", "config.lock"} {
		lockPath := filepath.Join(localPath, ".git", lockFile)
		if _, err := os.Stat(lockPath); err == nil {
			os.Remove(lockPath)
			g.logger.Info("removed stale git lock", "file", lockFile, "path", lockPath)
		}
	}
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

// isDeadClientError returns true if the error indicates the C binary subprocess is dead
// (broken pipe, closed stdout). These clients must be retired, not reused.
func isDeadClientError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "subprocess closed stdout") ||
		strings.Contains(msg, "mcp: read:") ||
		strings.Contains(msg, "mcp: send")
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
		if isDeadClientError(out.err) {
			client.Close()
			go p.replaceClientAsync(client)
		} else {
			p.release(client)
		}
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
		if isDeadClientError(out.err) {
			client.Close()
			go p.replaceClientAsync(client)
		} else {
			p.release(client)
		}
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
	maxUses int
	mu      sync.Mutex
	clients chan indexToolClient
	all     []indexToolClient
	uses    map[indexToolClient]int
}

func newMCPToolClientPool(ctx context.Context, binPath string, size int, maxUses int) (*mcpToolClientPool, error) {
	if size <= 0 {
		size = 1
	}
	pool := &mcpToolClientPool{
		binPath: binPath,
		maxUses: maxUses,
		clients: make(chan indexToolClient, size),
		all:     make([]indexToolClient, 0, size),
		uses:    make(map[indexToolClient]int, size),
	}
	for i := 0; i < size; i++ {
		client, err := newIndexToolClient(ctx, binPath)
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("start indexer client %d/%d: %w", i+1, size, err)
		}
		pool.all = append(pool.all, client)
		pool.uses[client] = 0
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

func (p *mcpToolClientPool) retire(client indexToolClient) {
	if client == nil {
		return
	}
	client.Close()
	go p.replaceClientAsync(client)
}

func (p *mcpToolClientPool) shouldRecycle(client indexToolClient) bool {
	if p.maxUses <= 0 || client == nil {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	next := p.uses[client] + 1
	p.uses[client] = next
	return next >= p.maxUses
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
		if out.err != nil {
			p.retire(client)
			return nil, out.err
		}
		if p.shouldRecycle(client) {
			p.retire(client)
		} else {
			p.release(client)
		}
		return out.result, out.err
	case <-ctx.Done():
		p.retire(client)
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
	delete(p.uses, dead)
	for i, client := range p.all {
		if client == dead {
			p.all[i] = replacement
			break
		}
	}
	p.uses[replacement] = 0
	p.mu.Unlock()

	p.release(replacement)
}

type mcpIndexClientPool struct {
	*mcpToolClientPool
}

func newMCPIndexClientPool(ctx context.Context, binPath string, size int, maxUses int) (*mcpIndexClientPool, error) {
	pool, err := newMCPToolClientPool(ctx, binPath, size, maxUses)
	if err != nil {
		return nil, err
	}
	return &mcpIndexClientPool{mcpToolClientPool: pool}, nil
}

func (p *mcpIndexClientPool) IndexRepository(ctx context.Context, repoPath, mode, projectName string) error {
	args := map[string]interface{}{
		"repo_path": repoPath,
		"mode":      mode,
	}
	if projectName != "" {
		args["project"] = projectName
	}
	result, err := p.CallTool(ctx, "index_repository", args)
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
	pool, err := newMCPToolClientPool(ctx, binPath, size, 0)
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
	cache     *bridge.SearchCache
	cacheDir  string // CBM cache dir where per-project .db files live
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
		// Hard timeout: tools/list should be fast
		listCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		raw, err := b.client.Call(listCtx, "tools/list", nil)
		if err != nil {
			return nil, err
		}
		raw, err = b.appendDiscoveryTool(raw)
		if err != nil {
			return nil, err
		}
		return b.appendCustomerSurfaceTool(raw)
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

		// Cache check: return instantly for repeated identical queries.
		cacheable := b.cache != nil && (name == "search_code" || name == "search_graph" || name == "get_code_snippet")
		var cacheKey string
		if cacheable {
			cacheKey = b.cache.Key(name, args)
			if cached, ok := b.cache.Get(cacheKey); ok {
				slog.Debug("search cache hit", "tool", name)
				return cached, nil
			}
		}

		// ── Go-native search_code ──
		// Bypass C binary entirely for search_code. The C binary runs `grep -rn`
		// on GCS Fuse-mounted repos which is catastrophically slow (minutes for
		// 63K-file repos). Our Go path:
		//   1. Queries SQLite for the pre-indexed file list (no filesystem walk)
		//   2. Reads files in parallel (64 goroutines — saturates GCS Fuse)
		//   3. Runs Go regexp (same semantics as grep -E)
		//   4. Classifies matches against indexed nodes (same output as C binary)
		// Full grep accuracy, never hangs, hard 30s deadline.
		if name == "search_code" {
			goSearchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			raw, goErr := b.runGoSearchCode(goSearchCtx, args)
			if goErr == nil {
				if cacheable {
					b.cache.Set(cacheKey, raw)
				}
				return raw, nil
			}
			// Log and fall through to C binary as safety net.
			slog.Warn("go-native search_code failed, falling back to C binary", "err", goErr)
		}

		// ── Go-native customer_surface ──
		// Composite enricher: fuses product-area + Vue metadata + FE fetch-call
		// extraction into one record per file. No C-binary fallback — this tool
		// is Go-only by design (the C binary has no concept of product areas
		// or Vue SFC structure).
		if name == "customer_surface" {
			goCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			return b.runGoCustomerSurface(goCtx, args)
		}

		// Hard timeout on every C binary tool call. Without this, hung C
		// binaries block the bridge client forever because bufio.Scanner.Scan()
		// doesn't respect context cancellation.
		toolCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		result, err := b.client.CallTool(toolCtx, name, args)
		if err != nil {
			return nil, err
		}

		raw, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}

		// Cache successful (non-error) results only.
		if cacheable && !result.IsError {
			b.cache.Set(cacheKey, raw)
		}

		return raw, nil
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

// runGoSearchCode executes search_code entirely in Go — bypasses the C binary.
// See searchtools package for architecture details.
func (b *mcpBridgeBackend) runGoSearchCode(ctx context.Context, args map[string]interface{}) (json.RawMessage, error) {
	if b.cacheDir == "" {
		return nil, errors.New("cache dir not configured")
	}

	// Unmarshal args.
	raw, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("marshal args: %w", err)
	}
	var sargs searchtools.SearchCodeArgs
	if err := json.Unmarshal(raw, &sargs); err != nil {
		return nil, fmt.Errorf("parse args: %w", err)
	}

	result, err := searchtools.HandleSearchCode(ctx, b.cacheDir, sargs)
	if err != nil {
		return nil, err
	}

	// Format as MCP ToolResult (same shape as C binary output).
	body, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("marshal search result: %w", err)
	}
	return json.Marshal(mcp.ToolResult{
		Content: []mcp.Content{{Type: "text", Text: string(body)}},
		IsError: false,
	})
}

// runGoCustomerSurface executes the customer-surface composite enricher.
// Pure compute path — sources are passed inline by the caller, so no SQLite,
// no GCS Fuse, no filesystem walk. Hot path latency is dominated by regex
// work across the batched files (single-digit ms per file on typical SFCs).
func (b *mcpBridgeBackend) runGoCustomerSurface(ctx context.Context, args map[string]interface{}) (json.RawMessage, error) {
	raw, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("marshal args: %w", err)
	}
	var sargs searchtools.CustomerSurfaceArgs
	if err := json.Unmarshal(raw, &sargs); err != nil {
		return nil, fmt.Errorf("parse args: %w", err)
	}

	result, err := searchtools.HandleCustomerSurface(ctx, sargs)
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("marshal customer-surface result: %w", err)
	}
	return json.Marshal(mcp.ToolResult{
		Content: []mcp.Content{{Type: "text", Text: string(body)}},
		IsError: false,
	})
}

// appendCustomerSurfaceTool advertises the customer_surface wrapper-owned
// tool on `tools/list`. The C binary knows nothing about it, so we have to
// inject the definition into the list response before returning to the client.
func (b *mcpBridgeBackend) appendCustomerSurfaceTool(raw json.RawMessage) (json.RawMessage, error) {
	var payload struct {
		Tools []map[string]interface{} `json:"tools"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("parse tools/list response: %w", err)
	}
	payload.Tools = append(payload.Tools, customerSurfaceToolDefinition())
	return json.Marshal(payload)
}

// customerSurfaceToolDefinition is the MCP JSON-Schema for the customer_surface
// tool. Kept here (not in the searchtools package) to avoid circular imports
// and to mirror the discovery tool registration pattern.
func customerSurfaceToolDefinition() map[string]interface{} {
	return map[string]interface{}{
		"name": "customer_surface",
		"description": "Fuse product area + Vue component metadata + frontend fetch calls into a " +
			"per-file customer-surface record. Used by PR-impact analysis to map changed files " +
			"to product areas, pages, and UI components. Pass the repo slug and a list of " +
			"(path, source) pairs; returns one surface record per file in input order.",
		"inputSchema": map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"repo": map[string]interface{}{
					"type":        "string",
					"description": "Short repo slug used for product-map lookup (e.g. 'ghl-crm-frontend', 'platform-backend').",
				},
				"files": map[string]interface{}{
					"type":        "array",
					"description": "Files to analyze. Each entry carries a repo-root-relative path and the full source.",
					"items": map[string]interface{}{
						"type": "object",
						"properties": map[string]interface{}{
							"path":   map[string]interface{}{"type": "string"},
							"source": map[string]interface{}{"type": "string"},
						},
						"required": []string{"path", "source"},
					},
				},
				"product_map_path": map[string]interface{}{
					"type":        "string",
					"description": "Optional filesystem override of the embedded product map (local dev / tests only).",
				},
			},
			"required": []string{"repo", "files"},
		},
	}
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

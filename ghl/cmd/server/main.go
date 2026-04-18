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
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/orgdb"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/orgdiscovery"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/orgtools"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/pipeline"
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
			hydrated, err := artifactSync.Hydrate()
			if err != nil {
				slog.Error("failed to hydrate persisted indexes", "artifact_dir", cfg.ArtifactDir, "cache_dir", cfg.CBMCacheDir, "err", err)
				os.Exit(1)
			}
			slog.Info("hydrated persisted indexes", "count", hydrated, "artifact_dir", cfg.ArtifactDir, "cache_dir", cfg.CBMCacheDir)
		}
	}

	// ── Org graph (optional) ─────────────────────────────────

	var orgDB *orgdb.DB
	if cfg.OrgGraphEnabled {
		orgDBPath := cfg.OrgDBPath
		if orgDBPath == "" {
			orgDBPath = filepath.Join(cfg.CBMCacheDir, "org", "org.db")
		}
		if err := os.MkdirAll(filepath.Dir(orgDBPath), 0o750); err != nil {
			slog.Error("failed to create org db dir", "path", orgDBPath, "err", err)
			os.Exit(1)
		}
		var dbErr error
		orgDB, dbErr = orgdb.Open(orgDBPath)
		if dbErr != nil {
			slog.Error("failed to open org db", "path", orgDBPath, "err", dbErr)
			os.Exit(1)
		}
		defer orgDB.Close()
		slog.Info("org graph enabled", "path", orgDBPath)

		// Hydrate org.db from artifacts if available
		if artifactSync != nil && !cfg.ArtifactsSkipHydrate {
			hydrated, err := artifactSync.HydrateOrgGraph()
			if err != nil {
				slog.Warn("failed to hydrate org graph", "err", err)
			} else if hydrated > 0 {
				slog.Info("hydrated org graph", "count", hydrated)
			}
		}
	}

	// ── Discover repos (API-first, YAML fallback) ───────────

	var m *manifest.Manifest
	if cfg.GitHubToken != "" && cfg.GitHubAllowedOrgs != nil && len(cfg.GitHubAllowedOrgs) > 0 {
		orgName := cfg.GitHubAllowedOrgs[0]
		scanner := orgdiscovery.NewScanner(orgName, cfg.GitHubToken)
		slog.Info("scanning GitHub org for repos", "org", orgName)

		repos, scanErr := scanner.ScanOrg(ctx)
		if scanErr != nil {
			slog.Warn("github org scan failed, falling back to manifest", "org", orgName, "err", scanErr)
		} else {
			slog.Info("discovered repos via GitHub API", "count", len(repos))

			// Enrich with ownership (CODEOWNERS + Teams API)
			if ownerErr := scanner.EnrichOwnership(ctx, repos); ownerErr != nil {
				slog.Warn("ownership enrichment failed (continuing)", "err", ownerErr)
			} else {
				slog.Info("enriched repo ownership", "repos", len(repos))
			}

			// Enrich with framework detection
			if fwErr := scanner.EnrichFrameworks(ctx, repos); fwErr != nil {
				slog.Warn("framework detection failed (continuing)", "err", fwErr)
			} else {
				slog.Info("enriched repo frameworks", "repos", len(repos))
			}

			m = &manifest.Manifest{Repos: repos}
		}
	}

	// Fallback to REPOS.yaml if API scan didn't work
	if m == nil {
		var err error
		m, err = manifest.Load(cfg.ReposManifest)
		if err != nil {
			slog.Error("failed to load repos manifest", "path", cfg.ReposManifest, "err", err)
			os.Exit(1)
		}
		slog.Info("fleet manifest loaded from YAML", "repos", len(m.Repos))
	}

	cloner := &gitCloner{
		logger:      logger,
		githubToken: cfg.GitHubToken,
	}

	newFleetIndexer := func(client indexer.Client, discoverySvc *discovery.Discoverer) *indexer.Indexer {
		return indexer.New(indexer.Config{
			Client:      client,
			Cloner:      cloner,
			CacheDir:    cfg.CloneCacheDir,
			Concurrency: cfg.Concurrency,
			OnRepoStart: func(slug string) { slog.Info("indexing repo", "repo", slug) },
			OnRepoDone: func(slug string, err error) {
				if err != nil {
					slog.Error("repo indexing failed", "repo", slug, "err", err)
					return
				}
				if artifactSync != nil {
					projectName := projectNameFromPath(filepath.Join(cfg.CloneCacheDir, slug))
					persisted, persistErr := artifactSync.PersistProject(projectName)
					if persistErr != nil {
						slog.Error("failed to persist project index", "repo", slug, "project", projectName, "err", persistErr)
					} else {
						slog.Info("persisted project index", "repo", slug, "project", projectName, "files", persisted)
					}
				}
				// ── Org graph enrichment ──
				if orgDB != nil {
					repo, ok := m.FindByName(slug)
					if ok {
						if enrichErr := pipeline.PopulateRepoData(orgDB, repo, cfg.CloneCacheDir); enrichErr != nil {
							slog.Warn("org enrichment failed", "repo", slug, "err", enrichErr)
						} else {
							slog.Info("org enrichment complete", "repo", slug)
						}
					}
				}
				if discoverySvc != nil {
					discoverySvc.Invalidate()
				}
				slog.Info("repo indexed", "repo", slug)
			},
			OnAllComplete: func(result indexer.IndexResult) {
				slog.Info("fleet indexing complete", "total", result.Total, "ok", result.Succeeded, "failed", result.Failed)
				// ── Cross-reference org contracts ──
				if orgDB != nil {
					matched, err := orgDB.CrossReferenceContracts()
					if err != nil {
						slog.Warn("cross-reference contracts failed", "err", err)
					} else {
						slog.Info("cross-referenced API contracts", "matched", matched)
					}
					// Persist org.db to artifacts
					if artifactSync != nil {
						persisted, err := artifactSync.PersistOrgGraph()
						if err != nil {
							slog.Warn("failed to persist org graph", "err", err)
						} else {
							slog.Info("persisted org graph", "files", persisted)
						}
					}
				}
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
		slog.Info("one-shot fleet indexing complete", "total", result.Total, "ok", result.Succeeded, "failed", result.Failed)
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
			slog.Info("fleet index complete", "reason", reason, "force", force, "total", result.Total, "ok", result.Succeeded, "failed", result.Failed)
		}()
		return true
	}

	// ── Fleet scheduler ──────────────────────────────────────

	c := cron.New()
	if cfg.ScheduledIndexingEnabled {
		c.AddFunc(cfg.IncrementalCron, func() {
			startFleetIndex("cron-incremental", false)
		})
		c.AddFunc(cfg.FullCron, func() {
			startFleetIndex("cron-full", true)
		})
		c.Start()
		defer c.Stop()
		slog.Info("scheduled indexing enabled", "incremental_cron", cfg.IncrementalCron, "full_cron", cfg.FullCron)
	} else {
		slog.Info("scheduled indexing disabled")
	}

	// ── HTTP router ──────────────────────────────────────────

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(5 * time.Minute))

	// Wire org graph into discovery scoring
	if orgDB != nil {
		discoverySvc.SetOrgDB(orgDB)
		slog.Info("org graph wired into discovery scoring")
	}

	// Build org tool service
	var orgToolSvc *orgtools.OrgService
	if orgDB != nil {
		orgToolSvc = orgtools.New(orgDB)
		slog.Info("org tools enabled", "tools", len(orgToolSvc.Definitions()))
	}

	// Bridge: forward MCP calls to the binary
	bridgeHandler := bridge.NewHandler(
		&mcpBridgeBackend{client: bridgePool, discovery: discoverySvc, orgTools: orgToolSvc},
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
			"startup_index_enabled":    cfg.StartupIndexEnabled,
			"scheduled_index_enabled":  cfg.ScheduledIndexingEnabled,
			"fleet_index_running":      fleetIndexing.Load(),
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

	if cfg.StartupIndexEnabled {
		startFleetIndex("startup", false)
	} else {
		slog.Info("startup indexing disabled")
	}

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
	OrgGraphEnabled          bool
	OrgDBPath                string
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
		OrgGraphEnabled:          getBool("ORG_GRAPH_ENABLED", false),
		OrgDBPath:                getEnv("ORG_DB_PATH", ""),
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

func projectNameFromPath(absPath string) string {
	path := filepath.ToSlash(strings.TrimSpace(absPath))
	if path == "" {
		return "root"
	}

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

// orgToolService is the subset of orgtools.OrgService used by the bridge backend.
type orgToolService interface {
	Definitions() []discovery.ToolDefinition
	IsOrgTool(name string) bool
	CallTool(ctx context.Context, name string, args map[string]interface{}) (interface{}, error)
}

// mcpBridgeBackend implements bridge.Backend by forwarding to the MCP client.
type mcpBridgeBackend struct {
	client    bridgeClient
	discovery discovery.Service
	orgTools  orgToolService
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
		raw, err = b.appendDiscoveryTool(raw)
		if err != nil {
			return nil, err
		}
		return b.appendOrgTools(raw)
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
		if b.orgTools != nil && b.orgTools.IsOrgTool(name) {
			return b.callOrgTool(ctx, name, args)
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

func (b *mcpBridgeBackend) appendOrgTools(raw json.RawMessage) (json.RawMessage, error) {
	if b.orgTools == nil {
		return raw, nil
	}
	var payload struct {
		Tools []map[string]interface{} `json:"tools"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("parse tools/list response: %w", err)
	}
	for _, def := range b.orgTools.Definitions() {
		tool := map[string]interface{}{
			"name":        def.Name,
			"description": def.Description,
			"inputSchema": def.InputSchema,
		}
		payload.Tools = append(payload.Tools, tool)
	}
	return json.Marshal(payload)
}

func (b *mcpBridgeBackend) callOrgTool(ctx context.Context, name string, args map[string]interface{}) (json.RawMessage, error) {
	if b.orgTools == nil {
		return nil, errors.New("org tools unavailable")
	}
	result, err := b.orgTools.CallTool(ctx, name, args)
	if err != nil {
		return nil, err
	}
	text, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("marshal org tool response: %w", err)
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

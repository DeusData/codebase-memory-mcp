// Package indexer orchestrates fleet-wide repository cloning and indexing.
package indexer

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
)

// Client is the interface for calling the codebase-memory-mcp binary.
type Client interface {
	IndexRepository(ctx context.Context, repoPath, mode string) error
}

// Cloner is the interface for ensuring a local clone of a repository exists.
type Cloner interface {
	EnsureClone(ctx context.Context, githubURL, localPath string) error
}

// IndexResult summarises the outcome of an IndexAll call.
type IndexResult struct {
	Total     int
	Succeeded int
	Failed    int
	Errors    []RepoError
}

// RepoError records an indexing failure for a single repo.
type RepoError struct {
	RepoSlug string
	Err      error
}

// Config configures the Indexer.
type Config struct {
	Client      Client
	Cloner      Cloner
	CacheDir    string // local directory where repos are cloned
	Concurrency int    // max parallel indexing goroutines (default: 5)

	// Optional callbacks for observability / testing.
	OnRepoStart func(repoSlug string)
	OnRepoDone  func(repoSlug string)
	OnClone     func(githubURL, localPath string)
}

// Indexer manages cloning and indexing a fleet of repositories.
type Indexer struct {
	cfg Config
}

// New creates a new Indexer with the given config.
// Concurrency defaults to 5 if <= 0.
func New(cfg Config) *Indexer {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 5
	}
	return &Indexer{cfg: cfg}
}

// IndexAll clones and indexes every repo in the list.
// It respects the configured concurrency limit and continues on per-repo errors.
// If force is true, re-indexes repos even if already up-to-date.
// It returns immediately if ctx is cancelled, but in-flight goroutines may still complete.
func (i *Indexer) IndexAll(ctx context.Context, repos []manifest.Repo, force bool) IndexResult {
	result := IndexResult{Total: len(repos)}
	if len(repos) == 0 {
		return result
	}

	type repoErr struct {
		slug string
		err  error
	}

	sem := make(chan struct{}, i.cfg.Concurrency)
	errs := make(chan repoErr, len(repos))
	var wg sync.WaitGroup

	for _, repo := range repos {
		// Check context before dispatching
		select {
		case <-ctx.Done():
			// Record remaining as failed
			result.Failed++
			result.Errors = append(result.Errors, RepoError{RepoSlug: repo.Name, Err: ctx.Err()})
			continue
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(r manifest.Repo) {
			defer wg.Done()
			defer func() { <-sem }()

			if i.cfg.OnRepoStart != nil {
				i.cfg.OnRepoStart(r.Name)
			}
			err := i.IndexRepo(ctx, r, force)
			if i.cfg.OnRepoDone != nil {
				i.cfg.OnRepoDone(r.Name)
			}
			errs <- repoErr{slug: r.Name, err: err}
		}(repo)
	}

	wg.Wait()
	close(errs)

	for re := range errs {
		if re.err != nil {
			result.Failed++
			result.Errors = append(result.Errors, RepoError{RepoSlug: re.slug, Err: re.err})
		} else {
			result.Succeeded++
		}
	}

	return result
}

// IndexRepo clones (or updates) a single repo and triggers indexing.
func (i *Indexer) IndexRepo(ctx context.Context, repo manifest.Repo, force bool) error {
	localPath := filepath.Join(i.cfg.CacheDir, repo.Name)

	if i.cfg.OnClone != nil {
		i.cfg.OnClone(repo.GitHubURL, localPath)
	}

	// Step 1: Ensure local clone exists
	if err := i.cfg.Cloner.EnsureClone(ctx, repo.GitHubURL, localPath); err != nil {
		return fmt.Errorf("indexer: clone %q: %w", repo.Name, err)
	}

	// Step 2: Index via MCP binary
	mode := "moderate" // fast enough for incremental; use "full" for weekly force run
	if force {
		mode = "full"
	}
	if err := i.cfg.Client.IndexRepository(ctx, localPath, mode); err != nil {
		return fmt.Errorf("indexer: index %q: %w", repo.Name, err)
	}

	return nil
}

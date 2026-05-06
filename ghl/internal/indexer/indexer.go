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
	// IndexRepository indexes a repository. If projectName is non-empty, the
	// C binary uses it as the internal project name instead of deriving one
	// from repoPath.
	IndexRepository(ctx context.Context, repoPath, mode, projectName string) error
}

// ActivityChecker determines whether a repo has recent activity.
// Used during fleet indexing to skip stale repos.
type ActivityChecker interface {
	IsActive(ctx context.Context, repoName string) bool
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
	Skipped   int // repos skipped due to inactivity
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

	// ProjectNameFunc computes the project name to pass to the C binary.
	// When set, the returned name is used as the internal project identifier
	// instead of the path-derived default. If nil or returns "", the C binary
	// derives the name from the filesystem path.
	ProjectNameFunc func(repoSlug string) string

	// ActivityChecker, if set, is consulted during IndexAll. Repos for which
	// IsActive returns false are skipped (unless force=true).
	ActivityChecker ActivityChecker

	// Optional callbacks for observability / testing.
	OnRepoStart func(repoSlug string)
	OnRepoDone  func(repoSlug string, err error)
	OnClone       func(githubURL, localPath string)
	OnAllComplete func(result IndexResult)
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
		// Activity check: skip stale repos unless forced
		if !force && i.cfg.ActivityChecker != nil {
			if !i.cfg.ActivityChecker.IsActive(ctx, repo.Name) {
				result.Skipped++
				continue
			}
		}

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
				i.cfg.OnRepoDone(r.Name, err)
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

	if i.cfg.OnAllComplete != nil {
		i.cfg.OnAllComplete(result)
	}

	return result
}

// IndexRepo clones (or updates) a single repo and triggers indexing.
// The project name is computed from Config.ProjectNameFunc if set.
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
	// "fast" skips similarity/semantic edge passes — 2-3x cheaper per repo.
	// "full" only on forced weekly runs where graph completeness matters.
	mode := "fast"
	if force {
		mode = "full"
	}
	projectName := ""
	if i.cfg.ProjectNameFunc != nil {
		projectName = i.cfg.ProjectNameFunc(repo.Name)
	}
	if err := i.cfg.Client.IndexRepository(ctx, localPath, mode, projectName); err != nil {
		return fmt.Errorf("indexer: index %q: %w", repo.Name, err)
	}

	return nil
}

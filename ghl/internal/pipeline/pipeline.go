// Package pipeline wires the enricher and orgdb into the indexer pipeline.
// It keeps main.go clean and makes the enrichment flow testable.
package pipeline

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/enricher"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/orgdb"
)

// PopulateRepoData runs enrichment on a single repo and writes results to org.db.
// It clears stale data first, then inserts fresh repo metadata, dependencies,
// and API contracts (both provider and consumer sides).
func PopulateRepoData(db *orgdb.DB, repo manifest.Repo, cloneDir string) error {
	repoPath := filepath.Join(cloneDir, repo.Name)

	// 1. Clear old enrichment data for this repo
	if err := db.ClearRepoData(repo.Name); err != nil {
		return fmt.Errorf("pipeline: clear repo data %q: %w", repo.Name, err)
	}

	// 2. Upsert repo record
	if err := db.UpsertRepo(orgdb.RepoRecord{
		Name:      repo.Name,
		GitHubURL: repo.GitHubURL,
		Team:      repo.Team,
		Type:      repo.Type,
	}); err != nil {
		return fmt.Errorf("pipeline: upsert repo %q: %w", repo.Name, err)
	}

	// 3. Upsert team ownership
	if err := db.UpsertTeamOwnership(repo.Name, repo.Team, ""); err != nil {
		return fmt.Errorf("pipeline: upsert team ownership %q: %w", repo.Name, err)
	}

	// 4. Parse package.json dependencies (skip if missing)
	pkgPath := filepath.Join(repoPath, "package.json")
	if deps, err := orgdb.ParsePackageJSON(pkgPath); err == nil {
		for _, dep := range deps {
			if err := db.UpsertPackageDep(repo.Name, dep); err != nil {
				return fmt.Errorf("pipeline: upsert dep %q: %w", dep.Name, err)
			}
		}
	}

	// 4b. If this repo IS a GHL-internal package, set it as the provider
	if scope, name, err := orgdb.ParsePackageName(pkgPath); err == nil && scope != "" {
		if err := db.SetPackageProvider(scope, name, repo.Name); err != nil {
			return fmt.Errorf("pipeline: set package provider %s/%s: %w", scope, name, err)
		}
	}

	// 5. Run NestJS enricher
	result, err := enricher.EnrichRepo(repoPath)
	if err != nil {
		return fmt.Errorf("pipeline: enrich %q: %w", repo.Name, err)
	}

	// 6. Store controller routes as provider-side API contracts
	for _, ctrl := range result.Controllers {
		for _, route := range ctrl.Routes {
			path := buildPath(ctrl.ControllerPath, route.Path)
			if err := db.InsertAPIContract(orgdb.APIContract{
				ProviderRepo:   repo.Name,
				Method:         strings.ToUpper(route.Method),
				Path:           path,
				ProviderSymbol: ctrl.ClassName + "." + route.Path,
				Confidence:     0.2, // provider-only, no consumer match yet
			}); err != nil {
				return fmt.Errorf("pipeline: insert provider contract %s %s: %w", route.Method, path, err)
			}
		}
	}

	// 7. Store InternalRequest calls as consumer-side contracts
	for _, call := range result.InternalCalls {
		path := buildPath(call.ServiceName, call.Route)
		if err := db.InsertAPIContract(orgdb.APIContract{
			ConsumerRepo:   repo.Name,
			Method:         strings.ToUpper(call.Method),
			Path:           path,
			ConsumerSymbol: call.ServiceName + "." + call.Route,
			Confidence:     0.5, // consumer-only
		}); err != nil {
			return fmt.Errorf("pipeline: insert consumer contract %s %s: %w", call.Method, path, err)
		}
	}

	// 8. Store event patterns as event contracts
	for _, ep := range result.EventPatterns {
		contract := orgdb.EventContract{
			Topic:     ep.Topic,
			EventType: "pubsub",
		}
		if ep.Role == "producer" {
			contract.ProducerRepo = repo.Name
			contract.ProducerSymbol = ep.Symbol
		} else {
			contract.ConsumerRepo = repo.Name
			contract.ConsumerSymbol = ep.Symbol
		}
		if err := db.InsertEventContract(contract); err != nil {
			return fmt.Errorf("pipeline: insert event contract %q: %w", ep.Topic, err)
		}
	}

	return nil
}

// buildPath joins a base and suffix with a leading slash, avoiding double slashes.
func buildPath(base, suffix string) string {
	base = strings.TrimPrefix(base, "/")
	suffix = strings.TrimPrefix(suffix, "/")
	if suffix == "" {
		return "/" + base
	}
	return "/" + base + "/" + suffix
}

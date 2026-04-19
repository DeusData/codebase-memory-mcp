package enricher

import (
	"os"
	"path/filepath"
	"strings"
)

// RepoEnrichResult aggregates all NestJS metadata extracted from a repository.
type RepoEnrichResult struct {
	Controllers   []NestJSMetadata
	Injectables   []NestJSMetadata
	InternalCalls []InternalRequestCall
	EventPatterns []EventPatternCall
	RepoPath      string
}

var skipDirs = map[string]bool{
	"node_modules": true, ".git": true, "dist": true,
	"build": true, "coverage": true, ".next": true, ".nuxt": true,
}

// EnrichRepo walks the repo directory tree and extracts NestJS metadata from .ts files.
func EnrichRepo(repoPath string) (RepoEnrichResult, error) {
	result := RepoEnrichResult{RepoPath: repoPath}

	err := filepath.WalkDir(repoPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // silently skip unreadable files/dirs
		}
		if d.IsDir() {
			if skipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".ts") ||
			strings.HasSuffix(d.Name(), ".d.ts") ||
			strings.HasSuffix(d.Name(), ".spec.ts") {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return nil // silently skip unreadable files
		}
		source := string(data)

		hasNest := strings.Contains(source, "@Controller") ||
			strings.Contains(source, "@Injectable") ||
			strings.Contains(source, "InternalRequest.")
		hasEvents := strings.Contains(source, "@EventPattern") ||
			strings.Contains(source, "@MessagePattern") ||
			strings.Contains(source, "pubSub.publish") ||
			strings.Contains(source, ".emit(")
		if !hasNest && !hasEvents {
			return nil
		}

		relPath, _ := filepath.Rel(repoPath, path)

		meta, err := ExtractNestJSMetadata(source, relPath)
		if err != nil {
			return nil
		}

		if meta.ControllerPath != "" {
			result.Controllers = append(result.Controllers, meta)
		} else if meta.IsInjectable {
			result.Injectables = append(result.Injectables, meta)
		}

		calls, err := ExtractInternalRequests(source)
		if err != nil {
			return nil
		}
		result.InternalCalls = append(result.InternalCalls, calls...)

		// Extract event patterns
		eventPatterns := ExtractEventPatterns(source, relPath)
		result.EventPatterns = append(result.EventPatterns, eventPatterns...)

		return nil
	})

	return result, err
}

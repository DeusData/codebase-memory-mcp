// Package searchtools — customer_surface.go
//
// Go-native MCP tool that composes enricher.BuildCustomerSurface across a
// batch of files. Used by the PR-impact-analyzer workflow to fuse (product area
// + Vue metadata + FE fetch calls) into a single per-file "customer surface"
// record that the downstream reviewer skill renders as a human-readable panel.
//
// Design choices (to keep this narrow and reviewable):
//   - Pure compute: caller passes sources inline. No SQLite open, no root_path
//     resolution, no filesystem walk. PR tooling already has file contents from
//     `git show` / `gh pr diff`.
//   - Product map loaded once from embedded YAML (see enricher.LoadDefaultProductMap).
//     An opt-in ProductMapPath override exists for local dev / test scenarios.
//   - Graceful degradation: unknown repo → each surface labelled "Unknown —
//     no product mapping"; empty files slice → Count=0; per-file enricher
//     errors are unreachable today (enricher returns nil error by design).

package searchtools

import (
	"context"
	"errors"
	"fmt"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/enricher"
)

// CustomerSurfaceArgs is the JSON input to the customer-surface tool.
type CustomerSurfaceArgs struct {
	// Repo is the short repo slug used for product-map lookup, e.g. "ghl-crm-frontend".
	Repo string `json:"repo"`
	// Files is the list of (path, source) pairs to analyze. Paths are repo-root
	// relative; sources are the full file contents (may be empty for deleted files).
	Files []CustomerSurfaceFile `json:"files"`
	// ProductMapPath is an optional filesystem override of the embedded product map.
	// When empty (the default), the binary-embedded YAML is used.
	ProductMapPath string `json:"product_map_path,omitempty"`
}

// CustomerSurfaceFile is one file in a customer-surface batch request.
type CustomerSurfaceFile struct {
	Path   string `json:"path"`
	Source string `json:"source"`
}

// CustomerSurfaceResult is the JSON output: one surface record per input file,
// in input order. The count is provided explicitly so downstream tooling can
// assert on it without re-computing len.
type CustomerSurfaceResult struct {
	Repo     string                      `json:"repo"`
	Count    int                         `json:"count"`
	Surfaces []enricher.CustomerSurface  `json:"surfaces"`
}

// HandleCustomerSurface fuses product-area, Vue metadata, and FE fetch-call
// extraction across a batch of files. Pure compute — the only I/O is the
// optional product-map override load path.
func HandleCustomerSurface(ctx context.Context, args CustomerSurfaceArgs) (*CustomerSurfaceResult, error) {
	if args.Repo == "" {
		return nil, errors.New("customer-surface: repo is required")
	}

	pm, err := loadProductMap(args.ProductMapPath)
	if err != nil {
		return nil, fmt.Errorf("customer-surface: load product map: %w", err)
	}

	out := &CustomerSurfaceResult{
		Repo:     args.Repo,
		Surfaces: make([]enricher.CustomerSurface, 0, len(args.Files)),
	}

	for _, f := range args.Files {
		// Honor caller cancellation mid-batch — a PR with hundreds of files
		// should not keep burning CPU after a client drop.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cs, err := enricher.BuildCustomerSurface(enricher.BuildCustomerSurfaceArgs{
			Repo:       args.Repo,
			FilePath:   f.Path,
			Source:     f.Source,
			ProductMap: pm,
		})
		if err != nil {
			// Enricher currently never returns an error; if it ever does in the
			// future (e.g. a hard parse failure), skip the file rather than
			// failing the whole batch — individual file failures should not
			// poison a PR-wide analysis.
			continue
		}
		out.Surfaces = append(out.Surfaces, cs)
	}
	out.Count = len(out.Surfaces)
	return out, nil
}

// loadProductMap returns the embedded default unless an override path is set.
// Keeps HandleCustomerSurface itself uncluttered by the load-path branching.
func loadProductMap(overridePath string) (*enricher.ProductMap, error) {
	if overridePath != "" {
		return enricher.LoadProductMap(overridePath)
	}
	return enricher.LoadDefaultProductMap()
}

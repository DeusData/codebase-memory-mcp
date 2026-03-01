package watcher

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/DeusData/codebase-memory-mcp/internal/discover"
	"github.com/DeusData/codebase-memory-mcp/internal/store"
)

const (
	baseInterval = 1 * time.Second
	maxInterval  = 60 * time.Second
)

type fileSnapshot struct {
	modTime time.Time
	size    int64
}

type projectState struct {
	snapshot map[string]fileSnapshot
	interval time.Duration
	nextPoll time.Time
}

// IndexFunc is the callback signature for triggering a re-index.
type IndexFunc func(ctx context.Context, projectName, rootPath string) error

// Watcher polls indexed projects for file changes and triggers re-indexing.
type Watcher struct {
	router   *store.StoreRouter
	indexFn  IndexFunc
	projects map[string]*projectState
	ctx      context.Context
}

// New creates a Watcher. indexFn is called when file changes are detected.
func New(r *store.StoreRouter, indexFn IndexFunc) *Watcher {
	return &Watcher{
		router:   r,
		indexFn:  indexFn,
		projects: make(map[string]*projectState),
	}
}

// Run blocks until ctx is cancelled. Ticks at baseInterval, polling each
// project only when its adaptive interval has elapsed.
func (w *Watcher) Run(ctx context.Context) {
	w.ctx = ctx
	ticker := time.NewTicker(baseInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.pollAll()
		}
	}
}

// pollAll lists all indexed projects and polls each that is due.
func (w *Watcher) pollAll() {
	projectInfos, err := w.router.ListProjects()
	if err != nil {
		slog.Warn("watcher.list_projects", "err", err)
		return
	}

	now := time.Now()
	for _, info := range projectInfos {
		// Get the store for this project (never cache directly)
		st, stErr := w.router.ForProject(info.Name)
		if stErr != nil {
			continue
		}

		// Get the project metadata from the store
		proj, projErr := st.GetProject(info.Name)
		if projErr != nil || proj == nil {
			continue
		}

		state, exists := w.projects[info.Name]
		if !exists {
			state = &projectState{}
			w.projects[info.Name] = state
		}

		if exists && now.Before(state.nextPoll) {
			continue // not due yet
		}

		w.pollProject(proj, state)
	}
}

// pollProject captures a snapshot of the file tree and compares with previous.
// First poll: captures baseline without triggering indexing.
// Subsequent polls: triggers indexFn if any file changed.
func (w *Watcher) pollProject(proj *store.Project, state *projectState) {
	// Verify root path still exists
	if _, err := os.Stat(proj.RootPath); err != nil {
		slog.Warn("watcher.root_gone", "project", proj.Name, "path", proj.RootPath)
		state.nextPoll = time.Now().Add(maxInterval)
		return
	}

	snap, err := captureSnapshot(proj.RootPath)
	if err != nil {
		slog.Warn("watcher.snapshot", "project", proj.Name, "err", err)
		state.nextPoll = time.Now().Add(state.interval)
		return
	}

	interval := pollInterval(len(snap))

	if state.snapshot == nil {
		// First poll — capture baseline, no index trigger
		slog.Debug("watcher.baseline", "project", proj.Name, "files", len(snap))
		state.snapshot = snap
		state.interval = interval
		state.nextPoll = time.Now().Add(interval)
		return
	}

	if snapshotsEqual(state.snapshot, snap) {
		state.interval = interval
		state.nextPoll = time.Now().Add(interval)
		return
	}

	slog.Info("watcher.changed", "project", proj.Name, "files", len(snap))
	if err := w.indexFn(w.ctx, proj.Name, proj.RootPath); err != nil {
		slog.Warn("watcher.index", "project", proj.Name, "err", err)
		// Keep old snapshot so we retry next cycle
		state.nextPoll = time.Now().Add(interval)
		return
	}

	// Successful index — update snapshot and recalculate interval
	state.snapshot = snap
	state.interval = pollInterval(len(snap))
	state.nextPoll = time.Now().Add(state.interval)
}

// captureSnapshot walks the file tree using discover.Discover and captures
// mtime+size for each file.
func captureSnapshot(rootPath string) (map[string]fileSnapshot, error) {
	files, err := discover.Discover(context.Background(), rootPath, nil)
	if err != nil {
		return nil, err
	}

	snap := make(map[string]fileSnapshot, len(files))
	for _, f := range files {
		info, statErr := os.Stat(f.Path)
		if statErr != nil {
			continue
		}
		snap[f.RelPath] = fileSnapshot{
			modTime: info.ModTime(),
			size:    info.Size(),
		}
	}
	return snap, nil
}

// snapshotsEqual returns true if two snapshots have identical files with same mtime+size.
func snapshotsEqual(a, b map[string]fileSnapshot) bool {
	if len(a) != len(b) {
		return false
	}
	for path, aSnap := range a {
		bSnap, ok := b[path]
		if !ok {
			return false
		}
		if !aSnap.modTime.Equal(bSnap.modTime) || aSnap.size != bSnap.size {
			return false
		}
	}
	return true
}

// pollInterval computes the adaptive interval from file count.
// 1s base + 1s per 500 files, capped at 60s.
func pollInterval(fileCount int) time.Duration {
	ms := 1000 + (fileCount/500)*1000
	if ms > 60000 {
		ms = 60000
	}
	return time.Duration(ms) * time.Millisecond
}

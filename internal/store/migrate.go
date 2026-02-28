package store

import (
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

// migrate performs a one-time migration from the legacy single-DB layout
// (codebase-memory.db) to per-project .db files.
// Safe to call multiple times — it's a no-op if the legacy DB doesn't exist
// or has already been migrated.
func (r *StoreRouter) migrate() error {
	legacyPath := filepath.Join(r.dir, "codebase-memory.db")
	if _, err := os.Stat(legacyPath); os.IsNotExist(err) {
		return nil // nothing to migrate
	}

	slog.Info("migrate.start", "legacy_db", legacyPath)

	// Backup the legacy DB before any modification
	backupPath := legacyPath + ".backup"
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		if err := copyFile(legacyPath, backupPath); err != nil {
			return fmt.Errorf("backup: %w", err)
		}
		slog.Info("migrate.backup", "path", backupPath)
	}

	// Open legacy DB
	legacyDB, err := sql.Open("sqlite3", legacyPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return fmt.Errorf("open legacy: %w", err)
	}
	defer legacyDB.Close()

	// Get list of projects
	rows, err := legacyDB.Query("SELECT name FROM projects")
	if err != nil {
		return fmt.Errorf("list projects: %w", err)
	}
	defer rows.Close()

	var projectNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("scan project: %w", err)
		}
		projectNames = append(projectNames, name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration: %w", err)
	}

	if len(projectNames) == 0 {
		slog.Info("migrate.skip", "reason", "no_projects")
		return r.renameLegacy(legacyPath)
	}

	// Migrate each project
	migrated := 0
	for _, name := range projectNames {
		targetPath := filepath.Join(r.dir, name+".db")
		if _, err := os.Stat(targetPath); err == nil {
			slog.Info("migrate.project.exists", "project", name)
			migrated++
			continue
		}

		if err := r.migrateProject(legacyDB, name, targetPath); err != nil {
			slog.Warn("migrate.project.err", "project", name, "err", err)
			continue // skip failed projects, user can re-index
		}
		migrated++
	}

	slog.Info("migrate.done", "total", len(projectNames), "migrated", migrated)
	return r.renameLegacy(legacyPath)
}

// migrateProject copies one project's data from the legacy DB into a new per-project DB.
func (r *StoreRouter) migrateProject(legacyDB *sql.DB, projectName, targetPath string) error {
	// Create the target DB with schema
	targetStore, err := OpenPath(targetPath)
	if err != nil {
		return fmt.Errorf("open target: %w", err)
	}
	targetStore.Close()

	// Use ATTACH DATABASE for efficient bulk copy
	_, err = legacyDB.Exec("ATTACH DATABASE ? AS target", targetPath)
	if err != nil {
		return fmt.Errorf("attach: %w", err)
	}
	defer func() {
		_, _ = legacyDB.Exec("DETACH DATABASE target")
	}()

	// Copy projects
	_, err = legacyDB.Exec(`INSERT OR REPLACE INTO target.projects (name, indexed_at, root_path)
		SELECT name, indexed_at, root_path FROM projects WHERE name=?`, projectName)
	if err != nil {
		return fmt.Errorf("copy projects: %w", err)
	}

	// Copy nodes (preserving IDs)
	_, err = legacyDB.Exec(`INSERT OR REPLACE INTO target.nodes (id, project, label, name, qualified_name, file_path, start_line, end_line, properties)
		SELECT id, project, label, name, qualified_name, file_path, start_line, end_line, properties FROM nodes WHERE project=?`, projectName)
	if err != nil {
		return fmt.Errorf("copy nodes: %w", err)
	}

	// Copy edges (preserving IDs)
	_, err = legacyDB.Exec(`INSERT OR REPLACE INTO target.edges (id, project, source_id, target_id, type, properties)
		SELECT id, project, source_id, target_id, type, properties FROM edges WHERE project=?`, projectName)
	if err != nil {
		return fmt.Errorf("copy edges: %w", err)
	}

	// Copy file_hashes
	_, err = legacyDB.Exec(`INSERT OR REPLACE INTO target.file_hashes (project, rel_path, sha256)
		SELECT project, rel_path, sha256 FROM file_hashes WHERE project=?`, projectName)
	if err != nil {
		return fmt.Errorf("copy file_hashes: %w", err)
	}

	// Verify row counts
	var srcNodes, tgtNodes int
	_ = legacyDB.QueryRow("SELECT COUNT(*) FROM nodes WHERE project=?", projectName).Scan(&srcNodes)
	_ = legacyDB.QueryRow("SELECT COUNT(*) FROM target.nodes WHERE project=?", projectName).Scan(&tgtNodes)
	if srcNodes != tgtNodes {
		return fmt.Errorf("node count mismatch: src=%d tgt=%d", srcNodes, tgtNodes)
	}

	var srcEdges, tgtEdges int
	_ = legacyDB.QueryRow("SELECT COUNT(*) FROM edges WHERE project=?", projectName).Scan(&srcEdges)
	_ = legacyDB.QueryRow("SELECT COUNT(*) FROM target.edges WHERE project=?", projectName).Scan(&tgtEdges)
	if srcEdges != tgtEdges {
		return fmt.Errorf("edge count mismatch: src=%d tgt=%d", srcEdges, tgtEdges)
	}

	slog.Info("migrate.project.ok", "project", projectName, "nodes", srcNodes, "edges", srcEdges)
	return nil
}

// renameLegacy renames the legacy DB to mark migration as complete.
func (r *StoreRouter) renameLegacy(legacyPath string) error {
	migratedPath := legacyPath + ".migrated"
	if err := os.Rename(legacyPath, migratedPath); err != nil {
		return fmt.Errorf("rename legacy: %w", err)
	}
	// Also clean up WAL/SHM files
	for _, suffix := range []string{"-wal", "-shm"} {
		_ = os.Remove(legacyPath + suffix)
	}
	slog.Info("migrate.renamed", "from", legacyPath, "to", migratedPath)
	return nil
}

// copyFile copies a file from src to dst.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Sync()
}

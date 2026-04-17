package orgdb

import (
	"path/filepath"
	"testing"
)

func TestOpen_CreatesSchema(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "org.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tables := []string{
		"repos", "packages", "repo_dependencies",
		"api_contracts", "event_contracts",
		"shared_databases", "service_mesh",
		"team_ownership", "deployments", "version_conflicts",
	}
	for _, table := range tables {
		var count int
		err := db.db.QueryRow(
			"SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", table,
		).Scan(&count)
		if err != nil {
			t.Fatalf("query sqlite_master for %s: %v", table, err)
		}
		if count != 1 {
			t.Errorf("table %s: want 1, got %d", table, count)
		}
	}
}

func TestOpen_Idempotent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "org.db")

	db1, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open (first): %v", err)
	}
	db1.Close()

	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open (second): %v", err)
	}
	defer db2.Close()
}

func TestUpsertRepo(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "org.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	err = db.UpsertRepo(RepoRecord{
		Name:      "ghl-revex-backend",
		GitHubURL: "https://github.com/GoHighLevel/ghl-revex-backend.git",
		Team:      "revex",
		Type:      "backend",
		Languages: `["typescript"]`,
	})
	if err != nil {
		t.Fatalf("UpsertRepo: %v", err)
	}

	// Verify inserted
	var name, team string
	err = db.db.QueryRow("SELECT name, team FROM repos WHERE name = ?", "ghl-revex-backend").Scan(&name, &team)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if team != "revex" {
		t.Errorf("team: got %q, want %q", team, "revex")
	}

	// Upsert again with different team — should update
	err = db.UpsertRepo(RepoRecord{
		Name:      "ghl-revex-backend",
		GitHubURL: "https://github.com/GoHighLevel/ghl-revex-backend.git",
		Team:      "communities",
		Type:      "backend",
	})
	if err != nil {
		t.Fatalf("UpsertRepo (update): %v", err)
	}
	err = db.db.QueryRow("SELECT team FROM repos WHERE name = ?", "ghl-revex-backend").Scan(&team)
	if err != nil {
		t.Fatalf("query after update: %v", err)
	}
	if team != "communities" {
		t.Errorf("team after update: got %q, want %q", team, "communities")
	}
}

func TestUpsertTeamOwnership(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "org.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	err = db.UpsertTeamOwnership("ghl-revex-backend", "revex", "communities")
	if err != nil {
		t.Fatalf("UpsertTeamOwnership: %v", err)
	}

	var team, subTeam string
	err = db.db.QueryRow("SELECT team, sub_team FROM team_ownership WHERE repo_name = ?", "ghl-revex-backend").Scan(&team, &subTeam)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if team != "revex" {
		t.Errorf("team: got %q, want %q", team, "revex")
	}
	if subTeam != "communities" {
		t.Errorf("sub_team: got %q, want %q", subTeam, "communities")
	}
}

package orgdb

import (
	"path/filepath"
	"testing"
)

func TestCountRepoDependencies_ReturnsCorrectCount(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "org.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	seedRepo(t, db, "repo-a")

	// Before any deps
	if got := db.CountRepoDependencies("repo-a"); got != 0 {
		t.Errorf("before deps: got %d, want 0", got)
	}

	// Add two deps
	db.UpsertPackageDep("repo-a", Dep{Scope: "@platform-core", Name: "base-service", DepType: "dependencies", VersionSpec: "^3.0.0"})
	db.UpsertPackageDep("repo-a", Dep{Scope: "@platform-core", Name: "pubsub", DepType: "dependencies", VersionSpec: "^1.0.0"})

	if got := db.CountRepoDependencies("repo-a"); got != 2 {
		t.Errorf("after two deps: got %d, want 2", got)
	}

	// Unknown repo returns 0
	if got := db.CountRepoDependencies("nonexistent"); got != 0 {
		t.Errorf("nonexistent repo: got %d, want 0", got)
	}
}

func TestCountRepoContracts_ReturnsCorrectCount(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "org.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Before any contracts
	if got := db.CountRepoContracts("repo-a"); got != 0 {
		t.Errorf("before contracts: got %d, want 0", got)
	}

	// Add contracts
	db.InsertAPIContract(APIContract{
		ProviderRepo: "repo-a", ConsumerRepo: "repo-b",
		Method: "GET", Path: "/api/v1/foo",
		Confidence: 0.9,
	})
	db.InsertAPIContract(APIContract{
		ProviderRepo: "repo-c", ConsumerRepo: "repo-a",
		Method: "POST", Path: "/api/v1/bar",
		Confidence: 0.8,
	})

	// repo-a is provider in one, consumer in another = 2
	if got := db.CountRepoContracts("repo-a"); got != 2 {
		t.Errorf("repo-a contracts: got %d, want 2", got)
	}

	// repo-b only consumer in one = 1
	if got := db.CountRepoContracts("repo-b"); got != 1 {
		t.Errorf("repo-b contracts: got %d, want 1", got)
	}

	// Unknown repo returns 0
	if got := db.CountRepoContracts("nonexistent"); got != 0 {
		t.Errorf("nonexistent repo: got %d, want 0", got)
	}
}

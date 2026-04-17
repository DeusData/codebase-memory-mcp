package orgdb

import (
	"path/filepath"
	"testing"
)

// helper: open a temp DB and upsert a repo, returning the DB.
func openTestDB(t *testing.T) *DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "org.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func seedRepo(t *testing.T, db *DB, name string) {
	t.Helper()
	err := db.UpsertRepo(RepoRecord{
		Name:      name,
		GitHubURL: "https://github.com/GoHighLevel/" + name + ".git",
		Team:      "test",
		Type:      "backend",
		Languages: `["typescript"]`,
	})
	if err != nil {
		t.Fatalf("UpsertRepo(%s): %v", name, err)
	}
}

// ---------- ClearRepoData ----------

func TestClearRepoData_RemovesDepsContractsEventsDeployments(t *testing.T) {
	db := openTestDB(t)
	seedRepo(t, db, "repo-a")

	// Insert a package dep
	if err := db.UpsertPackageDep("repo-a", Dep{
		Scope: "@platform-core", Name: "base-service",
		DepType: "dependencies", VersionSpec: "^3.0.0",
	}); err != nil {
		t.Fatalf("UpsertPackageDep: %v", err)
	}

	// Insert an API contract
	if err := db.InsertAPIContract(APIContract{
		ProviderRepo: "repo-a", ConsumerRepo: "repo-b",
		Method: "GET", Path: "/api/v1/foo",
		ProviderSymbol: "FooController.get", ConsumerSymbol: "fooClient.fetch",
		Confidence: 0.9,
	}); err != nil {
		t.Fatalf("InsertAPIContract: %v", err)
	}

	// Insert an event contract
	if err := db.InsertEventContract(EventContract{
		Topic: "user.created", EventType: "pubsub",
		ProducerRepo: "repo-a", ConsumerRepo: "repo-b",
		ProducerSymbol: "UserService.emit", ConsumerSymbol: "UserWorker.handle",
	}); err != nil {
		t.Fatalf("InsertEventContract: %v", err)
	}

	// Insert team ownership
	if err := db.UpsertTeamOwnership("repo-a", "revex", "sub"); err != nil {
		t.Fatalf("UpsertTeamOwnership: %v", err)
	}

	// Insert a deployment
	if _, err := db.db.Exec(
		`INSERT INTO deployments (repo_name, app_name, deploy_type, env) VALUES (?, ?, ?, ?)`,
		"repo-a", "repo-a-app", "helm", "production",
	); err != nil {
		t.Fatalf("insert deployment: %v", err)
	}

	// Now clear
	if err := db.ClearRepoData("repo-a"); err != nil {
		t.Fatalf("ClearRepoData: %v", err)
	}

	// Verify deps cleared
	var count int
	db.db.QueryRow(`SELECT count(*) FROM repo_dependencies`).Scan(&count)
	if count != 0 {
		t.Errorf("repo_dependencies: want 0, got %d", count)
	}

	// Verify API contracts cleared
	db.db.QueryRow(`SELECT count(*) FROM api_contracts WHERE provider_repo = ? OR consumer_repo = ?`, "repo-a", "repo-a").Scan(&count)
	if count != 0 {
		t.Errorf("api_contracts: want 0, got %d", count)
	}

	// Verify event contracts cleared
	db.db.QueryRow(`SELECT count(*) FROM event_contracts WHERE producer_repo = ? OR consumer_repo = ?`, "repo-a", "repo-a").Scan(&count)
	if count != 0 {
		t.Errorf("event_contracts: want 0, got %d", count)
	}

	// Verify team ownership cleared
	db.db.QueryRow(`SELECT count(*) FROM team_ownership WHERE repo_name = ?`, "repo-a").Scan(&count)
	if count != 0 {
		t.Errorf("team_ownership: want 0, got %d", count)
	}

	// Verify deployments cleared
	db.db.QueryRow(`SELECT count(*) FROM deployments WHERE repo_name = ?`, "repo-a").Scan(&count)
	if count != 0 {
		t.Errorf("deployments: want 0, got %d", count)
	}

	// Verify repos table NOT cleared
	db.db.QueryRow(`SELECT count(*) FROM repos WHERE name = ?`, "repo-a").Scan(&count)
	if count != 1 {
		t.Errorf("repos: want 1 (not deleted), got %d", count)
	}
}

func TestClearRepoData_DoesNotAffectOtherRepos(t *testing.T) {
	db := openTestDB(t)
	seedRepo(t, db, "repo-a")
	seedRepo(t, db, "repo-b")

	// Add deps to both repos
	if err := db.UpsertPackageDep("repo-a", Dep{
		Scope: "@platform-core", Name: "base-service",
		DepType: "dependencies", VersionSpec: "^3.0.0",
	}); err != nil {
		t.Fatalf("UpsertPackageDep repo-a: %v", err)
	}
	if err := db.UpsertPackageDep("repo-b", Dep{
		Scope: "@platform-core", Name: "base-service",
		DepType: "dependencies", VersionSpec: "^4.0.0",
	}); err != nil {
		t.Fatalf("UpsertPackageDep repo-b: %v", err)
	}

	// Add team ownership to both
	db.UpsertTeamOwnership("repo-a", "teamA", "")
	db.UpsertTeamOwnership("repo-b", "teamB", "")

	// Clear only repo-a
	if err := db.ClearRepoData("repo-a"); err != nil {
		t.Fatalf("ClearRepoData: %v", err)
	}

	// repo-b deps should remain
	var count int
	db.db.QueryRow(`SELECT count(*) FROM repo_dependencies rd
		JOIN repos r ON r.id = rd.repo_id WHERE r.name = ?`, "repo-b").Scan(&count)
	if count != 1 {
		t.Errorf("repo-b deps: want 1, got %d", count)
	}

	// repo-b team ownership should remain
	db.db.QueryRow(`SELECT count(*) FROM team_ownership WHERE repo_name = ?`, "repo-b").Scan(&count)
	if count != 1 {
		t.Errorf("repo-b team_ownership: want 1, got %d", count)
	}
}

// ---------- UpsertPackageDep ----------

func TestUpsertPackageDep_CreatesPackageAndDep(t *testing.T) {
	db := openTestDB(t)
	seedRepo(t, db, "repo-a")

	err := db.UpsertPackageDep("repo-a", Dep{
		Scope: "@platform-core", Name: "base-service",
		DepType: "dependencies", VersionSpec: "^3.2.0",
	})
	if err != nil {
		t.Fatalf("UpsertPackageDep: %v", err)
	}

	// Verify package was created
	var pkgScope, pkgName string
	err = db.db.QueryRow(`SELECT scope, name FROM packages WHERE scope = ? AND name = ?`,
		"@platform-core", "base-service").Scan(&pkgScope, &pkgName)
	if err != nil {
		t.Fatalf("query package: %v", err)
	}
	if pkgScope != "@platform-core" || pkgName != "base-service" {
		t.Errorf("package: got %s/%s", pkgScope, pkgName)
	}

	// Verify dependency link
	var depType, versionSpec string
	err = db.db.QueryRow(`
		SELECT rd.dep_type, rd.version_spec
		FROM repo_dependencies rd
		JOIN repos r ON r.id = rd.repo_id
		JOIN packages p ON p.id = rd.package_id
		WHERE r.name = ? AND p.scope = ? AND p.name = ?`,
		"repo-a", "@platform-core", "base-service").Scan(&depType, &versionSpec)
	if err != nil {
		t.Fatalf("query dep: %v", err)
	}
	if depType != "dependencies" {
		t.Errorf("dep_type: got %q, want %q", depType, "dependencies")
	}
	if versionSpec != "^3.2.0" {
		t.Errorf("version_spec: got %q, want %q", versionSpec, "^3.2.0")
	}
}

func TestUpsertPackageDep_UpdatesVersionOnConflict(t *testing.T) {
	db := openTestDB(t)
	seedRepo(t, db, "repo-a")

	dep := Dep{
		Scope: "@platform-core", Name: "base-service",
		DepType: "dependencies", VersionSpec: "^3.0.0",
	}
	if err := db.UpsertPackageDep("repo-a", dep); err != nil {
		t.Fatalf("UpsertPackageDep (first): %v", err)
	}

	dep.VersionSpec = "^4.0.0"
	dep.DepType = "peerDependencies"
	if err := db.UpsertPackageDep("repo-a", dep); err != nil {
		t.Fatalf("UpsertPackageDep (update): %v", err)
	}

	var versionSpec, depType string
	err := db.db.QueryRow(`
		SELECT rd.dep_type, rd.version_spec
		FROM repo_dependencies rd
		JOIN repos r ON r.id = rd.repo_id
		JOIN packages p ON p.id = rd.package_id
		WHERE r.name = ? AND p.scope = ? AND p.name = ?`,
		"repo-a", "@platform-core", "base-service").Scan(&depType, &versionSpec)
	if err != nil {
		t.Fatalf("query dep: %v", err)
	}
	if versionSpec != "^4.0.0" {
		t.Errorf("version_spec: got %q, want %q", versionSpec, "^4.0.0")
	}
	if depType != "peerDependencies" {
		t.Errorf("dep_type: got %q, want %q", depType, "peerDependencies")
	}
}

// ---------- InsertAPIContract ----------

func TestInsertAPIContract_StoresContract(t *testing.T) {
	db := openTestDB(t)

	err := db.InsertAPIContract(APIContract{
		ProviderRepo:   "repo-a",
		ConsumerRepo:   "repo-b",
		Method:         "POST",
		Path:           "/api/v1/users",
		ProviderSymbol: "UserController.create",
		ConsumerSymbol: "userClient.createUser",
		Confidence:     0.85,
	})
	if err != nil {
		t.Fatalf("InsertAPIContract: %v", err)
	}

	var method, path, providerRepo, consumerRepo string
	var confidence float64
	err = db.db.QueryRow(`
		SELECT provider_repo, consumer_repo, method, path, confidence
		FROM api_contracts WHERE provider_repo = ? AND path = ?`,
		"repo-a", "/api/v1/users").Scan(&providerRepo, &consumerRepo, &method, &path, &confidence)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if method != "POST" {
		t.Errorf("method: got %q, want %q", method, "POST")
	}
	if consumerRepo != "repo-b" {
		t.Errorf("consumer_repo: got %q, want %q", consumerRepo, "repo-b")
	}
	if confidence != 0.85 {
		t.Errorf("confidence: got %f, want %f", confidence, 0.85)
	}
}

// ---------- InsertEventContract ----------

func TestInsertEventContract_StoresContract(t *testing.T) {
	db := openTestDB(t)

	err := db.InsertEventContract(EventContract{
		Topic:          "user.created",
		EventType:      "pubsub",
		ProducerRepo:   "repo-a",
		ConsumerRepo:   "repo-b",
		ProducerSymbol: "UserService.emit",
		ConsumerSymbol: "UserWorker.handle",
	})
	if err != nil {
		t.Fatalf("InsertEventContract: %v", err)
	}

	var topic, eventType, producerRepo, consumerRepo string
	err = db.db.QueryRow(`
		SELECT topic, event_type, producer_repo, consumer_repo
		FROM event_contracts WHERE topic = ?`, "user.created").Scan(&topic, &eventType, &producerRepo, &consumerRepo)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if eventType != "pubsub" {
		t.Errorf("event_type: got %q, want %q", eventType, "pubsub")
	}
	if producerRepo != "repo-a" {
		t.Errorf("producer_repo: got %q, want %q", producerRepo, "repo-a")
	}
	if consumerRepo != "repo-b" {
		t.Errorf("consumer_repo: got %q, want %q", consumerRepo, "repo-b")
	}
}

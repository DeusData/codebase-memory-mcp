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

// ---------- InferPackageProviders ----------

func TestInferPackageProviders_MatchesByRepoName(t *testing.T) {
	db := openTestDB(t)

	// Create repos
	seedRepo(t, db, "platform-core-base-service")
	seedRepo(t, db, "platform-core-logger")
	seedRepo(t, db, "some-unrelated-repo")

	// Create packages WITHOUT provider_repo
	db.UpsertPackageDep("some-unrelated-repo", Dep{
		Scope: "@platform-core", Name: "base-service",
		DepType: "dependencies", VersionSpec: "^3.0.0",
	})
	db.UpsertPackageDep("some-unrelated-repo", Dep{
		Scope: "@platform-core", Name: "logger",
		DepType: "dependencies", VersionSpec: "^1.0.0",
	})

	// Infer providers
	count, err := db.InferPackageProviders()
	if err != nil {
		t.Fatalf("InferPackageProviders: %v", err)
	}
	if count < 2 {
		t.Errorf("expected at least 2 providers inferred, got %d", count)
	}

	// Verify base-service got the right provider
	var providerRepo string
	err = db.db.QueryRow(`SELECT provider_repo FROM packages WHERE scope = ? AND name = ?`,
		"@platform-core", "base-service").Scan(&providerRepo)
	if err != nil {
		t.Fatalf("query base-service provider: %v", err)
	}
	if providerRepo != "platform-core-base-service" {
		t.Errorf("base-service provider: got %q, want %q", providerRepo, "platform-core-base-service")
	}

	// Verify logger got the right provider
	err = db.db.QueryRow(`SELECT provider_repo FROM packages WHERE scope = ? AND name = ?`,
		"@platform-core", "logger").Scan(&providerRepo)
	if err != nil {
		t.Fatalf("query logger provider: %v", err)
	}
	if providerRepo != "platform-core-logger" {
		t.Errorf("logger provider: got %q, want %q", providerRepo, "platform-core-logger")
	}
}

func TestInferPackageProviders_DoesNotOverwriteExisting(t *testing.T) {
	db := openTestDB(t)

	seedRepo(t, db, "wrong-repo")
	seedRepo(t, db, "correct-repo")

	// Create package with existing provider_repo
	db.SetPackageProvider("@platform-core", "base-service", "correct-repo")

	// Create a repo that could also match
	seedRepo(t, db, "base-service")

	count, err := db.InferPackageProviders()
	if err != nil {
		t.Fatalf("InferPackageProviders: %v", err)
	}
	_ = count

	// Should NOT have overwritten the existing provider
	var providerRepo string
	db.db.QueryRow(`SELECT provider_repo FROM packages WHERE scope = ? AND name = ?`,
		"@platform-core", "base-service").Scan(&providerRepo)
	if providerRepo != "correct-repo" {
		t.Errorf("provider should remain %q, got %q", "correct-repo", providerRepo)
	}
}

// ---------- extractServiceIdentifier ----------

func TestExtractServiceIdentifier(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		// Provider paths (from @Controller)
		{"/contacts/list", "contacts"},
		{"/api/v1/contacts/list", "contacts"},
		{"/api/v2/users/create", "users"},
		{"/api/contacts/list", "contacts"},
		// Consumer paths (from InternalRequest)
		{"/CONTACTS_API/list", "contacts"},
		{"/PAYMENTS_SERVICE/charge", "payments"},
		{"/USERS_WORKER/process", "users"},
		// Edge cases
		{"/api/v1", "api"},       // only has api/version, fallback
		{"/health", "health"},    // single segment
		{"", ""},                 // empty
		{"/", ""},                // just slash
	}

	for _, tt := range tests {
		got := extractServiceIdentifier(tt.path)
		if got != tt.want {
			t.Errorf("extractServiceIdentifier(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}

// ---------- CrossReferenceContracts false positives ----------

func TestCrossReferenceContracts_NoFalsePositive(t *testing.T) {
	db := openTestDB(t)

	// Provider: contacts-service exposes GET /contacts/list (simple path)
	db.InsertAPIContract(APIContract{
		ProviderRepo:   "contacts-service",
		Method:         "GET",
		Path:           "/contacts/list",
		ProviderSymbol: "ContactsController.list",
		Confidence:     0.3,
	})

	// Provider: users-service exposes GET /users/list
	db.InsertAPIContract(APIContract{
		ProviderRepo:   "users-service",
		Method:         "GET",
		Path:           "/users/list",
		ProviderSymbol: "UsersController.list",
		Confidence:     0.3,
	})

	// Consumer: workflow calls CONTACTS_API/list — should only match contacts, not users
	db.InsertAPIContract(APIContract{
		ConsumerRepo:   "workflow-service",
		Method:         "GET",
		Path:           "/CONTACTS_API/list",
		ConsumerSymbol: "WorkflowService.fetch",
		Confidence:     0.5,
	})

	matched, err := db.CrossReferenceContracts()
	if err != nil {
		t.Fatalf("CrossReferenceContracts: %v", err)
	}

	if matched != 1 {
		t.Errorf("expected exactly 1 match, got %d", matched)
	}

	// Verify the matched consumer got contacts-service, not users-service
	var providerRepo string
	err = db.db.QueryRow(`
		SELECT provider_repo FROM api_contracts
		WHERE consumer_repo = 'workflow-service' AND provider_repo != ''
	`).Scan(&providerRepo)
	if err != nil {
		t.Fatalf("query matched contract: %v", err)
	}
	if providerRepo != "contacts-service" {
		t.Errorf("expected provider contacts-service, got %q", providerRepo)
	}
}

func TestCrossReferenceContracts_APIVersionedPaths(t *testing.T) {
	db := openTestDB(t)

	// Provider: contacts-service exposes GET /api/v1/contacts/list (versioned API path)
	db.InsertAPIContract(APIContract{
		ProviderRepo:   "contacts-service",
		Method:         "GET",
		Path:           "/api/v1/contacts/list",
		ProviderSymbol: "ContactsController.list",
		Confidence:     0.3,
	})

	// Consumer: workflow calls CONTACTS_API/list
	db.InsertAPIContract(APIContract{
		ConsumerRepo:   "workflow-service",
		Method:         "GET",
		Path:           "/CONTACTS_API/list",
		ConsumerSymbol: "WorkflowService.fetch",
		Confidence:     0.5,
	})

	matched, err := db.CrossReferenceContracts()
	if err != nil {
		t.Fatalf("CrossReferenceContracts: %v", err)
	}

	if matched != 1 {
		t.Errorf("expected 1 match (api/v1/contacts/list ↔ CONTACTS_API/list), got %d", matched)
	}
}

// ---------- SetPackageProvider ----------

func TestSetPackageProvider_SetsAndUpdates(t *testing.T) {
	db := openTestDB(t)

	// First set
	if err := db.SetPackageProvider("@platform-core", "base-service", "platform-core-repo"); err != nil {
		t.Fatalf("SetPackageProvider: %v", err)
	}

	var providerRepo string
	err := db.db.QueryRow(`SELECT provider_repo FROM packages WHERE scope = ? AND name = ?`,
		"@platform-core", "base-service").Scan(&providerRepo)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if providerRepo != "platform-core-repo" {
		t.Errorf("provider_repo: got %q, want %q", providerRepo, "platform-core-repo")
	}

	// Update
	if err := db.SetPackageProvider("@platform-core", "base-service", "new-repo"); err != nil {
		t.Fatalf("SetPackageProvider update: %v", err)
	}
	err = db.db.QueryRow(`SELECT provider_repo FROM packages WHERE scope = ? AND name = ?`,
		"@platform-core", "base-service").Scan(&providerRepo)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if providerRepo != "new-repo" {
		t.Errorf("provider_repo after update: got %q, want %q", providerRepo, "new-repo")
	}
}

// ---------- CrossReferenceEventContracts ----------

func TestCrossReferenceEventContracts_MatchesByTopic(t *testing.T) {
	db := openTestDB(t)

	// Producer-only
	db.InsertEventContract(EventContract{
		Topic: "user.created", EventType: "pubsub",
		ProducerRepo: "auth-service", ProducerSymbol: "AuthService.emit",
	})

	// Consumer-only
	db.InsertEventContract(EventContract{
		Topic: "user.created", EventType: "pubsub",
		ConsumerRepo: "notification-service", ConsumerSymbol: "NotifyWorker.handle",
	})

	// Unrelated consumer (different topic, should NOT match)
	db.InsertEventContract(EventContract{
		Topic: "order.placed", EventType: "pubsub",
		ConsumerRepo: "billing-service", ConsumerSymbol: "BillingWorker.handle",
	})

	matched, err := db.CrossReferenceEventContracts()
	if err != nil {
		t.Fatalf("CrossReferenceEventContracts: %v", err)
	}

	if matched != 1 {
		t.Errorf("expected 1 match, got %d", matched)
	}

	// Verify the consumer got the producer info
	var producerRepo string
	err = db.db.QueryRow(`
		SELECT producer_repo FROM event_contracts
		WHERE consumer_repo = 'notification-service' AND topic = 'user.created'
	`).Scan(&producerRepo)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if producerRepo != "auth-service" {
		t.Errorf("producer_repo: got %q, want %q", producerRepo, "auth-service")
	}

	// Verify unmatched consumer still has empty producer
	var unmatchedProducer *string
	db.db.QueryRow(`
		SELECT producer_repo FROM event_contracts
		WHERE consumer_repo = 'billing-service'
	`).Scan(&unmatchedProducer)
	if unmatchedProducer != nil && *unmatchedProducer != "" {
		t.Errorf("unmatched consumer should have no producer, got %q", *unmatchedProducer)
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

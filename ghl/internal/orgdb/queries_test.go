package orgdb

import (
	"testing"
)

// ---------- helpers ----------

// seedRepoWithTeam creates a repo with a specific team and type.
func seedRepoWithTeam(t *testing.T, db *DB, name, team, typ string) {
	t.Helper()
	err := db.UpsertRepo(RepoRecord{
		Name:      name,
		GitHubURL: "https://github.com/GoHighLevel/" + name + ".git",
		Team:      team,
		Type:      typ,
		Languages: `["typescript"]`,
		NodeCount: 10,
		EdgeCount: 5,
	})
	if err != nil {
		t.Fatalf("UpsertRepo(%s): %v", name, err)
	}
}

// seedPackageWithProvider ensures a package row exists with a provider_repo set.
func seedPackageWithProvider(t *testing.T, db *DB, scope, name, providerRepo string) {
	t.Helper()
	_, err := db.db.Exec(
		`INSERT INTO packages (scope, name, provider_repo) VALUES (?, ?, ?)
		 ON CONFLICT(scope, name) DO UPDATE SET provider_repo = excluded.provider_repo`,
		scope, name, providerRepo,
	)
	if err != nil {
		t.Fatalf("seed package %s/%s: %v", scope, name, err)
	}
}

// ---------- QueryDependents ----------

func TestQueryDependents_FindsAllDependentRepos(t *testing.T) {
	db := openTestDB(t)

	// 3 repos depending on @platform-core/base-service
	seedRepo(t, db, "repo-a")
	seedRepo(t, db, "repo-b")
	seedRepo(t, db, "repo-c")
	seedRepo(t, db, "repo-d") // does NOT depend on the package

	for _, name := range []string{"repo-a", "repo-b", "repo-c"} {
		if err := db.UpsertPackageDep(name, Dep{
			Scope: "@platform-core", Name: "base-service",
			DepType: "dependencies", VersionSpec: "^3.0.0",
		}); err != nil {
			t.Fatalf("UpsertPackageDep(%s): %v", name, err)
		}
	}
	// repo-d depends on a different package
	if err := db.UpsertPackageDep("repo-d", Dep{
		Scope: "@platform-ui", Name: "components",
		DepType: "dependencies", VersionSpec: "^1.0.0",
	}); err != nil {
		t.Fatalf("UpsertPackageDep(repo-d): %v", err)
	}

	results, err := db.QueryDependents("@platform-core", "base-service")
	if err != nil {
		t.Fatalf("QueryDependents: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("want 3 results, got %d", len(results))
	}

	// Results should be ordered by repo name
	expected := []string{"repo-a", "repo-b", "repo-c"}
	for i, r := range results {
		if r.RepoName != expected[i] {
			t.Errorf("result[%d].RepoName: got %q, want %q", i, r.RepoName, expected[i])
		}
		if r.Scope != "@platform-core" {
			t.Errorf("result[%d].Scope: got %q", i, r.Scope)
		}
		if r.PackageName != "base-service" {
			t.Errorf("result[%d].PackageName: got %q", i, r.PackageName)
		}
	}
}

func TestQueryDependents_EmptyResult(t *testing.T) {
	db := openTestDB(t)

	results, err := db.QueryDependents("@nonexistent", "package")
	if err != nil {
		t.Fatalf("QueryDependents: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("want 0 results, got %d", len(results))
	}
}

// ---------- QueryBlastRadius ----------

func TestQueryBlastRadius_CombinesAllImpactTypes(t *testing.T) {
	db := openTestDB(t)

	// Setup: provider-repo provides a package, an API, and produces events
	seedRepoWithTeam(t, db, "provider-repo", "platform", "backend")
	seedRepoWithTeam(t, db, "pkg-consumer", "revex", "backend")
	seedRepoWithTeam(t, db, "api-consumer", "payments", "backend")
	seedRepoWithTeam(t, db, "event-consumer", "notifications", "backend")

	// Package dependency: pkg-consumer uses a package from provider-repo
	seedPackageWithProvider(t, db, "@platform-core", "base-service", "provider-repo")
	if err := db.UpsertPackageDep("pkg-consumer", Dep{
		Scope: "@platform-core", Name: "base-service",
		DepType: "dependencies", VersionSpec: "^3.0.0",
	}); err != nil {
		t.Fatalf("UpsertPackageDep: %v", err)
	}

	// API contract: provider-repo → api-consumer
	if err := db.InsertAPIContract(APIContract{
		ProviderRepo: "provider-repo", ConsumerRepo: "api-consumer",
		Method: "GET", Path: "/api/v1/users",
		Confidence: 0.9,
	}); err != nil {
		t.Fatalf("InsertAPIContract: %v", err)
	}

	// Event contract: provider-repo produces → event-consumer consumes
	if err := db.InsertEventContract(EventContract{
		Topic: "user.created", EventType: "pubsub",
		ProducerRepo: "provider-repo", ConsumerRepo: "event-consumer",
	}); err != nil {
		t.Fatalf("InsertEventContract: %v", err)
	}

	result, err := db.QueryBlastRadius("provider-repo")
	if err != nil {
		t.Fatalf("QueryBlastRadius: %v", err)
	}

	if result.TotalRepos != 3 {
		t.Errorf("TotalRepos: want 3, got %d", result.TotalRepos)
	}

	// Check we have all three impact types
	reasons := map[string]bool{}
	for _, ar := range result.AffectedRepos {
		reasons[ar.Reason] = true
	}
	for _, expected := range []string{"depends_on_package", "api_consumer", "event_consumer"} {
		if !reasons[expected] {
			t.Errorf("missing reason: %s", expected)
		}
	}
}

func TestQueryBlastRadius_EmptyForIsolatedRepo(t *testing.T) {
	db := openTestDB(t)
	seedRepoWithTeam(t, db, "isolated-repo", "team", "backend")

	result, err := db.QueryBlastRadius("isolated-repo")
	if err != nil {
		t.Fatalf("QueryBlastRadius: %v", err)
	}
	if result.TotalRepos != 0 {
		t.Errorf("TotalRepos: want 0, got %d", result.TotalRepos)
	}
}

// ---------- TraceFlow ----------

func TestTraceFlow_DownstreamChain(t *testing.T) {
	db := openTestDB(t)

	// A → B via API, B → C via API
	seedRepo(t, db, "svc-a")
	seedRepo(t, db, "svc-b")
	seedRepo(t, db, "svc-c")

	if err := db.InsertAPIContract(APIContract{
		ProviderRepo: "svc-a", ConsumerRepo: "svc-b",
		Method: "GET", Path: "/api/v1/a-to-b", Confidence: 0.9,
	}); err != nil {
		t.Fatalf("InsertAPIContract A→B: %v", err)
	}
	if err := db.InsertAPIContract(APIContract{
		ProviderRepo: "svc-b", ConsumerRepo: "svc-c",
		Method: "POST", Path: "/api/v1/b-to-c", Confidence: 0.8,
	}); err != nil {
		t.Fatalf("InsertAPIContract B→C: %v", err)
	}

	steps, err := db.TraceFlow("svc-a", "downstream", 3)
	if err != nil {
		t.Fatalf("TraceFlow: %v", err)
	}

	if len(steps) < 2 {
		t.Fatalf("want at least 2 steps, got %d", len(steps))
	}

	// Verify A→B exists
	found := false
	for _, s := range steps {
		if s.FromRepo == "svc-a" && s.ToRepo == "svc-b" {
			found = true
			break
		}
	}
	if !found {
		t.Error("missing step svc-a → svc-b")
	}

	// Verify B→C exists
	found = false
	for _, s := range steps {
		if s.FromRepo == "svc-b" && s.ToRepo == "svc-c" {
			found = true
			break
		}
	}
	if !found {
		t.Error("missing step svc-b → svc-c")
	}
}

func TestTraceFlow_MaxHopsLimitsDepth(t *testing.T) {
	db := openTestDB(t)

	// A → B → C → D chain
	seedRepo(t, db, "svc-a")
	seedRepo(t, db, "svc-b")
	seedRepo(t, db, "svc-c")
	seedRepo(t, db, "svc-d")

	db.InsertAPIContract(APIContract{
		ProviderRepo: "svc-a", ConsumerRepo: "svc-b",
		Method: "GET", Path: "/a-to-b", Confidence: 0.9,
	})
	db.InsertAPIContract(APIContract{
		ProviderRepo: "svc-b", ConsumerRepo: "svc-c",
		Method: "GET", Path: "/b-to-c", Confidence: 0.9,
	})
	db.InsertAPIContract(APIContract{
		ProviderRepo: "svc-c", ConsumerRepo: "svc-d",
		Method: "GET", Path: "/c-to-d", Confidence: 0.9,
	})

	// maxHops=1: should only get A→B
	steps, err := db.TraceFlow("svc-a", "downstream", 1)
	if err != nil {
		t.Fatalf("TraceFlow maxHops=1: %v", err)
	}

	for _, s := range steps {
		if s.FromRepo != "svc-a" {
			t.Errorf("maxHops=1: unexpected step from %q (should only be from svc-a)", s.FromRepo)
		}
	}
}

func TestTraceFlow_Upstream(t *testing.T) {
	db := openTestDB(t)

	seedRepo(t, db, "svc-a")
	seedRepo(t, db, "svc-b")

	db.InsertAPIContract(APIContract{
		ProviderRepo: "svc-a", ConsumerRepo: "svc-b",
		Method: "GET", Path: "/api/v1/data", Confidence: 0.9,
	})

	// Upstream from svc-b: who calls svc-b? → svc-a
	steps, err := db.TraceFlow("svc-b", "upstream", 3)
	if err != nil {
		t.Fatalf("TraceFlow upstream: %v", err)
	}

	if len(steps) == 0 {
		t.Fatal("want at least 1 upstream step, got 0")
	}

	found := false
	for _, s := range steps {
		if s.FromRepo == "svc-a" && s.ToRepo == "svc-b" {
			found = true
			break
		}
	}
	if !found {
		t.Error("missing upstream step svc-a → svc-b")
	}
}

func TestTraceFlow_EventPropagation(t *testing.T) {
	db := openTestDB(t)

	// A → B via API, B → C via event, C → D via event
	seedRepo(t, db, "svc-a")
	seedRepo(t, db, "svc-b")
	seedRepo(t, db, "svc-c")
	seedRepo(t, db, "svc-d")

	db.InsertAPIContract(APIContract{
		ProviderRepo: "svc-a", ConsumerRepo: "svc-b",
		Method: "POST", Path: "/api/trigger", Confidence: 0.9,
	})
	db.InsertEventContract(EventContract{
		Topic: "order.created", EventType: "pubsub",
		ProducerRepo: "svc-b", ConsumerRepo: "svc-c",
	})
	db.InsertEventContract(EventContract{
		Topic: "order.processed", EventType: "pubsub",
		ProducerRepo: "svc-c", ConsumerRepo: "svc-d",
	})

	steps, err := db.TraceFlow("svc-a", "downstream", 4)
	if err != nil {
		t.Fatalf("TraceFlow: %v", err)
	}

	// Should reach svc-d through the event chain
	reachedD := false
	for _, s := range steps {
		if s.ToRepo == "svc-d" {
			reachedD = true
			break
		}
	}
	if !reachedD {
		t.Errorf("expected to reach svc-d through event propagation, got steps: %v", steps)
	}

	// Verify at least 3 steps: A→B, B→C, C→D
	if len(steps) < 3 {
		t.Errorf("expected at least 3 steps, got %d", len(steps))
	}
}

func TestTraceFlow_UpstreamEventPropagation(t *testing.T) {
	db := openTestDB(t)

	seedRepo(t, db, "svc-a")
	seedRepo(t, db, "svc-b")
	seedRepo(t, db, "svc-c")

	// A produces event → B consumes, B produces event → C consumes
	db.InsertEventContract(EventContract{
		Topic: "user.created", EventType: "pubsub",
		ProducerRepo: "svc-a", ConsumerRepo: "svc-b",
	})
	db.InsertEventContract(EventContract{
		Topic: "user.enriched", EventType: "pubsub",
		ProducerRepo: "svc-b", ConsumerRepo: "svc-c",
	})

	// Upstream from svc-c should reach svc-a
	steps, err := db.TraceFlow("svc-c", "upstream", 4)
	if err != nil {
		t.Fatalf("TraceFlow upstream: %v", err)
	}

	reachedA := false
	for _, s := range steps {
		if s.FromRepo == "svc-a" {
			reachedA = true
			break
		}
	}
	if !reachedA {
		t.Errorf("expected to reach svc-a through upstream event propagation, got steps: %v", steps)
	}
}

// ---------- TeamTopology ----------

func TestTeamTopology_ReposAndDepTeams(t *testing.T) {
	db := openTestDB(t)

	// revex team has 3 repos
	seedRepoWithTeam(t, db, "revex-backend", "revex", "backend")
	seedRepoWithTeam(t, db, "revex-frontend", "revex", "frontend")
	seedRepoWithTeam(t, db, "revex-worker", "revex", "worker")

	// platform team has a repo that provides a package
	seedRepoWithTeam(t, db, "platform-core", "platform", "library")
	seedPackageWithProvider(t, db, "@platform-core", "base-service", "platform-core")

	// revex-backend depends on platform-core's package
	if err := db.UpsertPackageDep("revex-backend", Dep{
		Scope: "@platform-core", Name: "base-service",
		DepType: "dependencies", VersionSpec: "^3.0.0",
	}); err != nil {
		t.Fatalf("UpsertPackageDep: %v", err)
	}

	info, err := db.TeamTopology("revex")
	if err != nil {
		t.Fatalf("TeamTopology: %v", err)
	}

	if info.Team != "revex" {
		t.Errorf("Team: got %q, want %q", info.Team, "revex")
	}

	if len(info.Repos) != 3 {
		t.Errorf("Repos: want 3, got %d", len(info.Repos))
	}

	if len(info.DepTeams) != 1 || info.DepTeams[0] != "platform" {
		t.Errorf("DepTeams: want [platform], got %v", info.DepTeams)
	}
}

func TestTeamTopology_NoRepos(t *testing.T) {
	db := openTestDB(t)

	info, err := db.TeamTopology("nonexistent")
	if err != nil {
		t.Fatalf("TeamTopology: %v", err)
	}
	if len(info.Repos) != 0 {
		t.Errorf("Repos: want 0, got %d", len(info.Repos))
	}
	if len(info.DepTeams) != 0 {
		t.Errorf("DepTeams: want 0, got %d", len(info.DepTeams))
	}
}

// ---------- SearchRepos ----------

func TestSearchRepos_ByNameSubstring(t *testing.T) {
	db := openTestDB(t)
	seedRepoWithTeam(t, db, "ghl-revex-backend", "revex", "backend")
	seedRepoWithTeam(t, db, "ghl-revex-frontend", "revex", "frontend")
	seedRepoWithTeam(t, db, "ghl-payments-backend", "payments", "backend")

	results, err := db.SearchRepos("revex", "", "", 10)
	if err != nil {
		t.Fatalf("SearchRepos: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("want 2 results, got %d", len(results))
	}
}

func TestSearchRepos_ByTeamFilter(t *testing.T) {
	db := openTestDB(t)
	seedRepoWithTeam(t, db, "ghl-revex-backend", "revex", "backend")
	seedRepoWithTeam(t, db, "ghl-payments-backend", "payments", "backend")

	results, err := db.SearchRepos("backend", "", "payments", 10)
	if err != nil {
		t.Fatalf("SearchRepos: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("want 1 result, got %d", len(results))
	}
	if results[0].Name != "ghl-payments-backend" {
		t.Errorf("Name: got %q, want %q", results[0].Name, "ghl-payments-backend")
	}
}

func TestSearchRepos_EmptyResult(t *testing.T) {
	db := openTestDB(t)
	seedRepoWithTeam(t, db, "ghl-revex-backend", "revex", "backend")

	results, err := db.SearchRepos("nonexistent", "", "", 10)
	if err != nil {
		t.Fatalf("SearchRepos: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("want 0 results, got %d", len(results))
	}
}

func TestSearchRepos_ByScopeFilter(t *testing.T) {
	db := openTestDB(t)
	seedRepoWithTeam(t, db, "ghl-revex-backend", "revex", "backend")
	seedRepoWithTeam(t, db, "ghl-revex-frontend", "revex", "frontend")

	results, err := db.SearchRepos("revex", "backend", "", 10)
	if err != nil {
		t.Fatalf("SearchRepos: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("want 1 result, got %d", len(results))
	}
	if results[0].Type != "backend" {
		t.Errorf("Type: got %q, want %q", results[0].Type, "backend")
	}
}

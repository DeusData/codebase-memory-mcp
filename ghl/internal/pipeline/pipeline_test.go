package pipeline

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/manifest"
	"github.com/GoHighLevel/codebase-memory-mcp/ghl/internal/orgdb"
)

// helper: create a temp org.db and return it with cleanup.
func openTestDB(t *testing.T) *orgdb.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "org.db")
	db, err := orgdb.Open(dbPath)
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// helper: scaffold a fake repo directory under cloneDir with the given files.
func scaffoldRepo(t *testing.T, cloneDir, repoName string, files map[string]string) {
	t.Helper()
	for relPath, content := range files {
		full := filepath.Join(cloneDir, repoName, relPath)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", relPath, err)
		}
	}
}

func TestPopulateRepoData_BasicRepo(t *testing.T) {
	db := openTestDB(t)
	cloneDir := t.TempDir()

	// Scaffold a repo with package.json + NestJS controller
	scaffoldRepo(t, cloneDir, "contacts-service", map[string]string{
		"package.json": `{
			"dependencies": {
				"@platform-core/base-service": "^3.2.0",
				"express": "^4.18.0"
			},
			"devDependencies": {
				"@gohighlevel/test-utils": "^1.0.0"
			}
		}`,
		"src/contacts.controller.ts": `
import { Controller, Get, Post } from '@nestjs/common';

@Controller('contacts')
export class ContactsController {
	@Get('list')
	getList() {}

	@Post('create')
	createContact() {}
}
`,
	})

	repo := manifest.Repo{
		Name:      "contacts-service",
		GitHubURL: "https://github.com/GoHighLevel/contacts-service",
		Team:      "contacts",
		Type:      "backend",
	}

	err := PopulateRepoData(db, repo, cloneDir)
	if err != nil {
		t.Fatalf("PopulateRepoData: %v", err)
	}

	// Verify dependencies were stored (only internal ones)
	depCount := db.CountRepoDependencies("contacts-service")
	if depCount != 2 {
		t.Errorf("expected 2 internal deps, got %d", depCount)
	}

	// Verify API contracts were created for the controller routes
	contractCount := db.CountRepoContracts("contacts-service")
	if contractCount < 2 {
		t.Errorf("expected at least 2 API contracts (2 routes), got %d", contractCount)
	}
}

func TestPopulateRepoData_WithInternalRequests(t *testing.T) {
	db := openTestDB(t)
	cloneDir := t.TempDir()

	// Scaffold a consumer repo that calls InternalRequest
	scaffoldRepo(t, cloneDir, "workflow-service", map[string]string{
		"package.json": `{"dependencies": {}}`,
		"src/workflow.service.ts": `
import { Injectable } from '@nestjs/common';

@Injectable()
export class WorkflowService {
	async triggerContact() {
		await InternalRequest.get({
			serviceName: SERVICE_NAME.CONTACTS_API,
			route: 'list',
		});
		await InternalRequest.post({
			serviceName: SERVICE_NAME.CONTACTS_API,
			route: 'create',
		});
	}
}
`,
	})

	repo := manifest.Repo{
		Name:      "workflow-service",
		GitHubURL: "https://github.com/GoHighLevel/workflow-service",
		Team:      "workflows",
		Type:      "backend",
	}

	err := PopulateRepoData(db, repo, cloneDir)
	if err != nil {
		t.Fatalf("PopulateRepoData: %v", err)
	}

	// Consumer-side contracts should exist
	contractCount := db.CountRepoContracts("workflow-service")
	if contractCount < 2 {
		t.Errorf("expected at least 2 consumer contracts, got %d", contractCount)
	}
}

func TestPopulateRepoData_NoPackageJSON(t *testing.T) {
	db := openTestDB(t)
	cloneDir := t.TempDir()

	// Scaffold repo with no package.json
	scaffoldRepo(t, cloneDir, "simple-service", map[string]string{
		"src/app.controller.ts": `
import { Controller, Get } from '@nestjs/common';

@Controller('health')
export class AppController {
	@Get('check')
	healthCheck() {}
}
`,
	})

	repo := manifest.Repo{
		Name:      "simple-service",
		GitHubURL: "https://github.com/GoHighLevel/simple-service",
		Team:      "platform",
		Type:      "backend",
	}

	// Should not error even without package.json
	err := PopulateRepoData(db, repo, cloneDir)
	if err != nil {
		t.Fatalf("PopulateRepoData without package.json: %v", err)
	}

	contractCount := db.CountRepoContracts("simple-service")
	if contractCount < 1 {
		t.Errorf("expected at least 1 API contract, got %d", contractCount)
	}
}

func TestPopulateRepoData_ClearsOldData(t *testing.T) {
	db := openTestDB(t)
	cloneDir := t.TempDir()

	scaffoldRepo(t, cloneDir, "evolving-service", map[string]string{
		"package.json": `{"dependencies": {"@platform-core/base-service": "^1.0.0"}}`,
		"src/app.controller.ts": `
import { Controller, Get } from '@nestjs/common';

@Controller('api')
export class AppController {
	@Get('v1')
	v1() {}
}
`,
	})

	repo := manifest.Repo{
		Name:      "evolving-service",
		GitHubURL: "https://github.com/GoHighLevel/evolving-service",
		Team:      "core",
		Type:      "backend",
	}

	// First run
	if err := PopulateRepoData(db, repo, cloneDir); err != nil {
		t.Fatalf("first PopulateRepoData: %v", err)
	}

	// Update the repo to have different routes
	scaffoldRepo(t, cloneDir, "evolving-service", map[string]string{
		"package.json": `{"dependencies": {}}`,
		"src/app.controller.ts": `
import { Controller, Get } from '@nestjs/common';

@Controller('api')
export class AppController {
	@Get('v2')
	v2() {}

	@Get('v3')
	v3() {}
}
`,
	})

	// Second run should clear old data
	if err := PopulateRepoData(db, repo, cloneDir); err != nil {
		t.Fatalf("second PopulateRepoData: %v", err)
	}

	// Should have 0 deps now (no internal deps in updated package.json)
	depCount := db.CountRepoDependencies("evolving-service")
	if depCount != 0 {
		t.Errorf("expected 0 deps after update, got %d", depCount)
	}

	// Should have 2 contracts (v2, v3) not 3 (v1 was cleared)
	contractCount := db.CountRepoContracts("evolving-service")
	if contractCount != 2 {
		t.Errorf("expected 2 contracts after update, got %d", contractCount)
	}
}

func TestPopulateRepoData_EventContracts(t *testing.T) {
	db := openTestDB(t)
	cloneDir := t.TempDir()

	// Scaffold a producer repo
	scaffoldRepo(t, cloneDir, "order-service", map[string]string{
		"package.json": `{"dependencies": {}}`,
		"src/order.service.ts": `
import { Injectable } from '@nestjs/common';

@Injectable()
export class OrderService {
	async createOrder() {
		await this.pubSub.publish('order.created', { id: 1 });
	}
}
`,
	})

	// Scaffold a consumer repo
	scaffoldRepo(t, cloneDir, "notification-worker", map[string]string{
		"package.json": `{"dependencies": {}}`,
		"src/notification.worker.ts": `
import { EventPattern } from '@nestjs/microservices';

export class NotificationWorker {
	@EventPattern('order.created')
	handleOrderCreated(data: any) {}
}
`,
	})

	producer := manifest.Repo{
		Name: "order-service", GitHubURL: "https://github.com/GoHighLevel/order-service",
		Team: "orders", Type: "backend",
	}
	consumer := manifest.Repo{
		Name: "notification-worker", GitHubURL: "https://github.com/GoHighLevel/notification-worker",
		Team: "notifications", Type: "worker",
	}

	if err := PopulateRepoData(db, producer, cloneDir); err != nil {
		t.Fatalf("PopulateRepoData producer: %v", err)
	}
	if err := PopulateRepoData(db, consumer, cloneDir); err != nil {
		t.Fatalf("PopulateRepoData consumer: %v", err)
	}

	// Cross-reference should match the producer and consumer on 'order.created'
	matched, err := db.CrossReferenceEventContracts()
	if err != nil {
		t.Fatalf("CrossReferenceEventContracts: %v", err)
	}
	if matched < 1 {
		t.Errorf("expected at least 1 event cross-reference match, got %d", matched)
	}

	// After cross-reference, TraceFlow should find the connection
	steps, err := db.TraceFlow("order-service", "downstream", 2)
	if err != nil {
		t.Fatalf("TraceFlow: %v", err)
	}

	found := false
	for _, s := range steps {
		if s.FromRepo == "order-service" && s.ToRepo == "notification-worker" && s.EdgeType == "event_contract" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected event flow order-service → notification-worker, got steps: %v", steps)
	}
}

func TestCrossReferenceContracts(t *testing.T) {
	db := openTestDB(t)
	cloneDir := t.TempDir()

	// Provider repo: contacts-service with @Controller('contacts') + @Get('list')
	scaffoldRepo(t, cloneDir, "contacts-service", map[string]string{
		"package.json": `{"dependencies": {}}`,
		"src/contacts.controller.ts": `
import { Controller, Get, Post } from '@nestjs/common';

@Controller('contacts')
export class ContactsController {
	@Get('list')
	getList() {}

	@Post('create')
	createContact() {}
}
`,
	})

	// Consumer repo: workflow-service calls InternalRequest.get({serviceName: ..., route: 'list'})
	scaffoldRepo(t, cloneDir, "workflow-service", map[string]string{
		"package.json": `{"dependencies": {}}`,
		"src/workflow.service.ts": `
import { Injectable } from '@nestjs/common';

@Injectable()
export class WorkflowService {
	async triggerContact() {
		await InternalRequest.get({
			serviceName: SERVICE_NAME.CONTACTS_API,
			route: 'list',
		});
	}
}
`,
	})

	providerRepo := manifest.Repo{
		Name:      "contacts-service",
		GitHubURL: "https://github.com/GoHighLevel/contacts-service",
		Team:      "contacts",
		Type:      "backend",
	}
	consumerRepo := manifest.Repo{
		Name:      "workflow-service",
		GitHubURL: "https://github.com/GoHighLevel/workflow-service",
		Team:      "workflows",
		Type:      "backend",
	}

	if err := PopulateRepoData(db, providerRepo, cloneDir); err != nil {
		t.Fatalf("populate provider: %v", err)
	}
	if err := PopulateRepoData(db, consumerRepo, cloneDir); err != nil {
		t.Fatalf("populate consumer: %v", err)
	}

	// Before cross-reference: contracts are separate (provider-only and consumer-only)
	providerContracts := db.CountRepoContracts("contacts-service")
	consumerContracts := db.CountRepoContracts("workflow-service")
	t.Logf("before cross-ref: provider=%d, consumer=%d", providerContracts, consumerContracts)

	matched, err := db.CrossReferenceContracts()
	if err != nil {
		t.Fatalf("CrossReferenceContracts: %v", err)
	}

	t.Logf("cross-referenced %d contracts", matched)

	// After cross-reference: at least one match should have happened
	// The GET /contacts/list provider should match the GET contacts/list consumer
	if matched < 1 {
		t.Errorf("expected at least 1 cross-reference match, got %d", matched)
	}
}

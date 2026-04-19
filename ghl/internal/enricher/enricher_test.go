package enricher

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTestFile(t *testing.T, dir, relPath, content string) {
	t.Helper()
	full := filepath.Join(dir, relPath)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestEnrichRepo_CollectsNestJSMetadata(t *testing.T) {
	dir := t.TempDir()

	writeTestFile(t, dir, "src/billing/billing.controller.ts", `
import { Controller, Get, Post } from '@nestjs/common';

@Controller('billing')
export class BillingController {
  constructor(private readonly billingService: BillingService) {}

  @Get('invoices')
  async getInvoices() {}

  @Post('refund')
  async processRefund() {}
}
`)
	writeTestFile(t, dir, "src/billing/billing.service.ts", `
import { Injectable } from '@nestjs/common';

@Injectable()
export class BillingService {
  constructor(private readonly stripeClient: StripeClient) {}
}
`)
	writeTestFile(t, dir, "src/utils/helper.ts", `
export function add(a: number, b: number) { return a + b; }
`)
	writeTestFile(t, dir, "src/internal-caller.ts", `
async function call() {
  await InternalRequest.post({
    serviceName: SERVICE_NAME.CONTACTS_API,
    route: 'upsert',
  });
}
`)

	result, err := EnrichRepo(dir)
	if err != nil {
		t.Fatalf("EnrichRepo: %v", err)
	}

	if len(result.Controllers) != 1 {
		t.Fatalf("Controllers count: got %d, want 1", len(result.Controllers))
	}
	if result.Controllers[0].ClassName != "BillingController" {
		t.Errorf("Controller: got %q, want %q", result.Controllers[0].ClassName, "BillingController")
	}
	if len(result.Controllers[0].Routes) != 2 {
		t.Errorf("Routes: got %d, want 2", len(result.Controllers[0].Routes))
	}

	if len(result.Injectables) != 1 {
		t.Fatalf("Injectables count: got %d, want 1", len(result.Injectables))
	}
	if result.Injectables[0].ClassName != "BillingService" {
		t.Errorf("Injectable: got %q, want %q", result.Injectables[0].ClassName, "BillingService")
	}

	if len(result.InternalCalls) != 1 {
		t.Fatalf("InternalCalls count: got %d, want 1", len(result.InternalCalls))
	}
	if result.InternalCalls[0].ServiceName != "CONTACTS_API" {
		t.Errorf("InternalCall ServiceName: got %q, want %q", result.InternalCalls[0].ServiceName, "CONTACTS_API")
	}
}

func TestEnrichRepo_ExtractsEventPatterns(t *testing.T) {
	dir := t.TempDir()

	writeTestFile(t, dir, "src/order/order.worker.ts", `
import { EventPattern } from '@nestjs/microservices';

export class OrderWorker {
  @EventPattern('order.created')
  handleOrderCreated(data: any) {}

  async processOrder() {
    await this.pubSub.publish('order.processed', { id: 1 });
  }
}
`)

	result, err := EnrichRepo(dir)
	if err != nil {
		t.Fatalf("EnrichRepo: %v", err)
	}

	if len(result.EventPatterns) != 2 {
		t.Fatalf("EventPatterns count: got %d, want 2", len(result.EventPatterns))
	}

	// Verify consumer
	if result.EventPatterns[0].Topic != "order.created" || result.EventPatterns[0].Role != "consumer" {
		t.Errorf("EventPatterns[0] = {%q, %q}, want {order.created, consumer}",
			result.EventPatterns[0].Topic, result.EventPatterns[0].Role)
	}

	// Verify producer
	if result.EventPatterns[1].Topic != "order.processed" || result.EventPatterns[1].Role != "producer" {
		t.Errorf("EventPatterns[1] = {%q, %q}, want {order.processed, producer}",
			result.EventPatterns[1].Topic, result.EventPatterns[1].Role)
	}
}

func TestEnrichRepo_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	result, err := EnrichRepo(dir)
	if err != nil {
		t.Fatalf("EnrichRepo: %v", err)
	}
	if len(result.Controllers) != 0 {
		t.Errorf("expected 0 controllers")
	}
}

func TestEnrichRepo_SkipsNodeModules(t *testing.T) {
	dir := t.TempDir()
	writeTestFile(t, dir, "node_modules/@nestjs/core/controller.ts", `
@Controller('internal')
export class InternalController {}
`)
	result, err := EnrichRepo(dir)
	if err != nil {
		t.Fatalf("EnrichRepo: %v", err)
	}
	if len(result.Controllers) != 0 {
		t.Errorf("expected 0 controllers (node_modules should be skipped)")
	}
}

func TestEnrichRepo_SkipsDTS(t *testing.T) {
	dir := t.TempDir()
	writeTestFile(t, dir, "src/types/billing.d.ts", `
@Controller('types')
export class TypeController {}
`)
	result, err := EnrichRepo(dir)
	if err != nil {
		t.Fatalf("EnrichRepo: %v", err)
	}
	if len(result.Controllers) != 0 {
		t.Errorf("expected 0 controllers (.d.ts should be skipped)")
	}
}

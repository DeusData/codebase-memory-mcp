package enricher

import (
	"testing"
)

func TestExtractNestJSMetadata_ControllerWithGetAndPost(t *testing.T) {
	source := `
import { Controller, Get, Post, Body } from '@nestjs/common';

@Controller('contacts')
export class ContactsController {
  @Get('list')
  findAll() {
    return [];
  }

  @Post('create')
  create(@Body() dto: CreateContactDto) {
    return dto;
  }
}
`
	meta, err := ExtractNestJSMetadata(source, "src/contacts/contacts.controller.ts")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta.ClassName != "ContactsController" {
		t.Errorf("ClassName = %q, want %q", meta.ClassName, "ContactsController")
	}
	if meta.ControllerPath != "contacts" {
		t.Errorf("ControllerPath = %q, want %q", meta.ControllerPath, "contacts")
	}
	if meta.IsInjectable {
		t.Errorf("IsInjectable = true, want false")
	}
	if meta.FilePath != "src/contacts/contacts.controller.ts" {
		t.Errorf("FilePath = %q, want %q", meta.FilePath, "src/contacts/contacts.controller.ts")
	}
	if len(meta.Routes) != 2 {
		t.Fatalf("len(Routes) = %d, want 2", len(meta.Routes))
	}
	if meta.Routes[0].Method != "Get" || meta.Routes[0].Path != "list" {
		t.Errorf("Routes[0] = {%q, %q, ...}, want {Get, list, ...}", meta.Routes[0].Method, meta.Routes[0].Path)
	}
	if meta.Routes[1].Method != "Post" || meta.Routes[1].Path != "create" {
		t.Errorf("Routes[1] = {%q, %q, ...}, want {Post, create, ...}", meta.Routes[1].Method, meta.Routes[1].Path)
	}
}

func TestExtractNestJSMetadata_RouteWithUseGuards(t *testing.T) {
	source := `
import { Controller, Post, UseGuards } from '@nestjs/common';

@Controller('orders')
export class OrdersController {
  @UseGuards(AuthGuard)
  @Post('submit')
  submit() {}
}
`
	meta, err := ExtractNestJSMetadata(source, "orders.controller.ts")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(meta.Routes) != 1 {
		t.Fatalf("len(Routes) = %d, want 1", len(meta.Routes))
	}
	if meta.Routes[0].Method != "Post" || meta.Routes[0].Path != "submit" {
		t.Errorf("Routes[0] = {%q, %q, ...}, want {Post, submit, ...}", meta.Routes[0].Method, meta.Routes[0].Path)
	}
	if len(meta.Routes[0].Guards) != 1 || meta.Routes[0].Guards[0] != "AuthGuard" {
		t.Errorf("Routes[0].Guards = %v, want [AuthGuard]", meta.Routes[0].Guards)
	}
}

func TestExtractNestJSMetadata_ConstructorDIDependencies(t *testing.T) {
	source := `
import { Controller, Get } from '@nestjs/common';

@Controller('users')
export class UsersController {
  constructor(
    private readonly userService: UserService,
    private readonly logger: LoggerService,
  ) {}

  @Get('')
  findAll() {}
}
`
	meta, err := ExtractNestJSMetadata(source, "users.controller.ts")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(meta.Dependencies) != 2 {
		t.Fatalf("len(Dependencies) = %d, want 2", len(meta.Dependencies))
	}
	if meta.Dependencies[0].ParamName != "userService" || meta.Dependencies[0].TypeName != "UserService" {
		t.Errorf("Dependencies[0] = {%q, %q}, want {userService, UserService}", meta.Dependencies[0].ParamName, meta.Dependencies[0].TypeName)
	}
	if meta.Dependencies[1].ParamName != "logger" || meta.Dependencies[1].TypeName != "LoggerService" {
		t.Errorf("Dependencies[1] = {%q, %q}, want {logger, LoggerService}", meta.Dependencies[1].ParamName, meta.Dependencies[1].TypeName)
	}
}

func TestExtractNestJSMetadata_InjectableService(t *testing.T) {
	source := `
import { Injectable } from '@nestjs/common';

@Injectable()
export class ContactsService {
  constructor(
    private readonly repo: ContactsRepository,
  ) {}

  findAll() {
    return this.repo.findAll();
  }
}
`
	meta, err := ExtractNestJSMetadata(source, "contacts.service.ts")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta.ClassName != "ContactsService" {
		t.Errorf("ClassName = %q, want %q", meta.ClassName, "ContactsService")
	}
	if !meta.IsInjectable {
		t.Errorf("IsInjectable = false, want true")
	}
	if meta.ControllerPath != "" {
		t.Errorf("ControllerPath = %q, want empty", meta.ControllerPath)
	}
	if len(meta.Routes) != 0 {
		t.Errorf("len(Routes) = %d, want 0", len(meta.Routes))
	}
	if len(meta.Dependencies) != 1 {
		t.Fatalf("len(Dependencies) = %d, want 1", len(meta.Dependencies))
	}
	if meta.Dependencies[0].ParamName != "repo" || meta.Dependencies[0].TypeName != "ContactsRepository" {
		t.Errorf("Dependencies[0] = {%q, %q}, want {repo, ContactsRepository}", meta.Dependencies[0].ParamName, meta.Dependencies[0].TypeName)
	}
}

func TestExtractNestJSMetadata_NonNestJSFile(t *testing.T) {
	source := `
export function helper(x: number): number {
  return x * 2;
}
`
	meta, err := ExtractNestJSMetadata(source, "helper.ts")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta.ClassName != "" {
		t.Errorf("ClassName = %q, want empty", meta.ClassName)
	}
	if meta.ControllerPath != "" {
		t.Errorf("ControllerPath = %q, want empty", meta.ControllerPath)
	}
	if meta.IsInjectable {
		t.Errorf("IsInjectable = true, want false")
	}
	if len(meta.Routes) != 0 {
		t.Errorf("len(Routes) = %d, want 0", len(meta.Routes))
	}
	if len(meta.Dependencies) != 0 {
		t.Errorf("len(Dependencies) = %d, want 0", len(meta.Dependencies))
	}
}

// ---------- ExtractEventPatterns ----------

func TestExtractEventPatterns_ConsumerAndProducer(t *testing.T) {
	source := `
import { EventPattern, MessagePattern } from '@nestjs/microservices';

export class OrderWorker {
  @EventPattern('order.created')
  handleOrderCreated(data: any) {}

  @MessagePattern('order.query')
  handleQuery(data: any) {}

  async processOrder() {
    await this.pubSub.publish('order.processed', { id: 1 });
  }
}
`
	patterns := ExtractEventPatterns(source, "src/order.worker.ts")
	if len(patterns) != 3 {
		t.Fatalf("len(patterns) = %d, want 3", len(patterns))
	}

	// @EventPattern consumer
	if patterns[0].Topic != "order.created" || patterns[0].Role != "consumer" {
		t.Errorf("patterns[0] = {%q, %q}, want {order.created, consumer}", patterns[0].Topic, patterns[0].Role)
	}
	// @MessagePattern consumer
	if patterns[1].Topic != "order.query" || patterns[1].Role != "consumer" {
		t.Errorf("patterns[1] = {%q, %q}, want {order.query, consumer}", patterns[1].Topic, patterns[1].Role)
	}
	// pubSub.publish producer
	if patterns[2].Topic != "order.processed" || patterns[2].Role != "producer" {
		t.Errorf("patterns[2] = {%q, %q}, want {order.processed, producer}", patterns[2].Topic, patterns[2].Role)
	}

	// All should have the class name
	for i, p := range patterns {
		if p.Symbol != "OrderWorker" {
			t.Errorf("patterns[%d].Symbol = %q, want %q", i, p.Symbol, "OrderWorker")
		}
	}
}

func TestExtractEventPatterns_NoPatterns(t *testing.T) {
	source := `
export class Helper {
  doSomething() { return 42; }
}
`
	patterns := ExtractEventPatterns(source, "helper.ts")
	if len(patterns) != 0 {
		t.Errorf("len(patterns) = %d, want 0", len(patterns))
	}
}

func TestExtractInternalRequests_PostAndGet(t *testing.T) {
	source := `
const result = await InternalRequest.post({
  serviceName: SERVICE_NAME.CONTACTS_API,
  route: 'upsert',
});

const data = await InternalRequest.get({
  serviceName: SERVICE_NAME.PAYMENTS_API,
  route: 'status',
});
`
	calls, err := ExtractInternalRequests(source)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("len(calls) = %d, want 2", len(calls))
	}
	if calls[0].Method != "post" || calls[0].ServiceName != "CONTACTS_API" || calls[0].Route != "upsert" {
		t.Errorf("calls[0] = {%q, %q, %q}, want {post, CONTACTS_API, upsert}", calls[0].Method, calls[0].ServiceName, calls[0].Route)
	}
	if calls[1].Method != "get" || calls[1].ServiceName != "PAYMENTS_API" || calls[1].Route != "status" {
		t.Errorf("calls[1] = {%q, %q, %q}, want {get, PAYMENTS_API, status}", calls[1].Method, calls[1].ServiceName, calls[1].Route)
	}
}

func TestExtractInternalRequests_MultipleCallsSameMethod(t *testing.T) {
	source := `
await InternalRequest.post({
  serviceName: SERVICE_NAME.CONTACTS_API,
  route: 'create',
});
await InternalRequest.post({
  serviceName: SERVICE_NAME.CONTACTS_API,
  route: 'update',
});
await InternalRequest.delete({
  serviceName: SERVICE_NAME.CONTACTS_API,
  route: 'remove',
});
`
	calls, err := ExtractInternalRequests(source)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(calls) != 3 {
		t.Fatalf("len(calls) = %d, want 3", len(calls))
	}
	if calls[0].Method != "post" || calls[0].Route != "create" {
		t.Errorf("calls[0] = {%q, _, %q}, want {post, _, create}", calls[0].Method, calls[0].Route)
	}
	if calls[1].Method != "post" || calls[1].Route != "update" {
		t.Errorf("calls[1] = {%q, _, %q}, want {post, _, update}", calls[1].Method, calls[1].Route)
	}
	if calls[2].Method != "delete" || calls[2].Route != "remove" {
		t.Errorf("calls[2] = {%q, _, %q}, want {delete, _, remove}", calls[2].Method, calls[2].Route)
	}
}

func TestExtractInternalRequests_NoCallsReturnsEmpty(t *testing.T) {
	source := `
export function helper() {
  return 42;
}
`
	calls, err := ExtractInternalRequests(source)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(calls) != 0 {
		t.Errorf("len(calls) = %d, want 0", len(calls))
	}
}

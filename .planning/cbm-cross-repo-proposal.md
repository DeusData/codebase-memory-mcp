# Protocol-Aware Cross-Repo Intelligence

**Status:** Tier 1a+1b shipped as PR #293. Tiers 1c–1f, 2, 3, 4 sequenced as follow-up PRs.
**Audience:** cbm maintainer + reviewers
**Scope:** Extends `pass_cross_repo` from literal-string matching to protocol-aware matching across the four cross-language patterns that account for >95% of inter-service communication in modern codebases.
**Compatibility:** Strictly additive throughout. No breaking changes to existing tools, edges, or APIs.
**Companion artifacts:**
- **#292** — this proposal (issue thread on `DeusData/codebase-memory-mcp`)
- **#293** — Tier 1a + 1b PR (`pass_idl_scan`: gRPC IDL Route emission + consumer HANDLES + producer GRPC_CALLS via `type_assigns`). Validated against real-world .NET microservice fleet; covers ~30% of producer idioms (Python `*Stub`, manual C# `*Client`, Rust tonic, TS `@grpc/grpc-js`).
- **#294** — pre-existing `pass_parallel.c emit_grpc_edge` issue (phantom Routes from heuristic var-name matching, greedy `ServiceClient` suffix stripping). Discovered during validation; out of scope for #293.
- **PR #281** — rich `get_architecture` fields. Initially documented as a precondition for this work; Tier 1 implementation does not strictly depend on it.

---

## TL;DR

cbm already has the scaffolding for cross-repo intelligence (`pass_cross_repo.c`, `CROSS_HTTP_CALLS` / `CROSS_ASYNC_CALLS` / `CROSS_GRPC_CALLS` edge types, named-route matching). The original implementation only fired when a call site had a **literal URL or topic string** as its first argument. That covers idiomatic Python/Node code well, but misses the dominant pattern in modern strongly-typed stacks (Java/Spring, .NET, Kotlin, Go-with-codegen): **typed clients and message handlers where the routing identifier is a generic type parameter, an interface ancestor, an attribute, a constructor parameter type, a field type, a factory-function return type, or a config-resolved name** — never a literal string at the call site.

This proposal adds four protocol-aware extraction tiers, each language-generic. **Tier 1 is decomposed into six sub-tiers (1a–1f) by signal source**, each one a small isolated extension to a unified `pass_idl_scan`. Tiers 2–4 follow as separate PRs once Tier 1 stabilizes; they reuse Tier 1's `(var, type, scope) → service` abstraction as foundation.

Cross-language framework coverage matrix in §6. Acceptance gating: each sub-tier ships independently, success measured by precision/recall against multi-language test fixtures and validated against real-world fleet indexing.

**Key insight from validation (April 2026):** Tier 1 is fundamentally a **type-flow problem**, not a call-site-string problem. The original v1 spec assumed `var = SomeStub(...)` covers most cases. Real fleets show that modern OOP DI (constructor injection, field declarations, factory return types) is the majority pattern, and `type_assigns`-based detection alone misses 60–70% of producers.

---

## 1. Background

### 1.1 What works in cbm today (as of `main` plus #293)

After PR #281 lands (or its consumer-side equivalent — see note above), `get_architecture(aspects=["all"])` returns rich structural data (entry_points, routes, hotspots, layers, boundaries, languages). The per-repo extraction pipeline detects:

- Library identifiers in resolved qualified names (`service_patterns.c:631` — 252 patterns across HTTP, async, gRPC, config, route-registration kinds, covering Python/Node/Go/Java/Rust/PHP/Ruby/C# basics)
- Literal URL / topic strings at call sites (`pass_calls.c:emit_http_async_edge`)
- Route registration via `app.get("/x", ...)` and attribute-routed framework styles (`pass_route_nodes.c`)
- Cross-repo matching when both sides have a literal-route identifier (`pass_cross_repo.c:cbm_cross_repo_match`)
- **(After #293)** IDL Route emission from `.proto` files via `pass_idl_scan` with QN format `__route__grpc__<Service>/<rpc>`
- **(After #293)** Consumer-side `HANDLES` edges from server-stub-base subclasses (Python `*Servicer`, Java `*ImplBase`, C# `*Base`/`*ServiceBase`)
- **(After #293)** Producer-side `GRPC_CALLS` edges from `type_assigns`-captured stub variable assignments

The `cross-repo-intelligence` mode in `index_repository` matches `__route__<METHOD>__<path>` keys across project DBs and emits CROSS_HTTP_CALLS / CROSS_ASYNC_CALLS / CROSS_GRPC_CALLS edges. **Phase D** of `pass_cross_repo.c` (`match_typed_routes`) handles gRPC/GraphQL/tRPC by reading the source-DB Route's QN and looking it up in target DBs — protocol-agnostic matching, agnostic to which pass emitted the Route.

### 1.2 What didn't work before #293

`emit_http_async_edge` (pass_calls.c, line ~232):

```c
bool is_url = (url_or_topic && url_or_topic[0] != '\0' &&
               (url_or_topic[0] == '/' || strstr(url_or_topic, "://") != NULL));
bool is_topic = (url_or_topic && ... && svc == CBM_SVC_ASYNC && ...);
if (!is_url && !is_topic) {
    /* fall back to plain CALLS edge */
    return;
}
```

If the first string argument was not a literal URL or topic, the edge fell through to `CALLS` — generic, unrouted, untaggable for cross-repo matching. Idiomatic code in major modern frameworks rarely passes a literal URL or topic at the call site:

```csharp
// .NET / MassTransit — no topic string, message type is the identifier
await _publishEndpoint.Publish(new VoucherRedeemed(voucher.Id), ct);

// .NET / generated gRPC client — no URL string, service.method is the identifier
var resp = await _promoCodeClient.GetVoucherAsync(req, cancellationToken: ct);

// Java / Spring Cloud Stream — no topic string, message type is the identifier
streamBridge.send("output", new OrderShipped(order));

// Java / Feign — interface annotation IS the route, no literal at call site
return feignClient.getVoucher(id);

// Kotlin / Retrofit — same shape as Feign
return retrofitApi.getVoucher(id)

// Go with gRPC codegen — generated client method, no string at call site
resp, err := promoClient.GetVoucher(ctx, req)

// Python / FastAPI typed httpx client (oapi-codegen-derived) — same shape
resp = await client.get_voucher(id=id)
```

In each case, the producer-side identifier (message type FQN, gRPC service.method, Feign interface annotation) was statically present and resolvable — but not as a literal string argument. It lives in: a generic type parameter, the constructor type of an argument, a class-level attribute, a method-level attribute, a field/property declared type, a factory-function return type, or a DI registration generic argument.

The consumer side has the same identifier visible in a different syntactic position: an `IConsumer<T>` declaration, a `*Base` implementation, a `@StreamListener<T>` annotation, an attribute-routed controller method.

**The matching problem is solvable.** The producer/consumer identifier exists statically on both sides. cbm's original extractor just didn't extract it.

### 1.3 What works after #293 — and what doesn't

PR #293 (`pass_idl_scan`) addressed the consumer side completely and the producer side partially:

**Consumer side — fully working:**
- Python `*Servicer` subclasses → `HANDLES` to IDL Route ✓
- Java `*ImplBase` subclasses → `HANDLES` ✓
- C# `*ServiceBase` / `*Base` subclasses → `HANDLES` ✓
- Cross-language: longest-suffix-first ordering disambiguates `ImplBase` vs `Base`

**Producer side — partial:**
- Python `stub = pb2_grpc.FooStub(channel)` → `GRPC_CALLS` ✓
- C# manual `var c = new Foo.FooClient(channel)` → `GRPC_CALLS` ✓
- Rust `let c = FooClient::new(channel)` → `GRPC_CALLS` ✓
- TS `const c = new pb.FooClient(...)` → `GRPC_CALLS` ✓
- **Modern C# Grpc.Net.ClientFactory (DI ctor injection)** → ✗ no detection
- **Java `stub = FooGrpc.newBlockingStub(...)`** → ✗ no detection (return-type inference missing)
- **Go `c := pb.NewFooClient(conn)`** → ✗ no detection (same)
- **NestJS `@Client()` decorator pattern** → ✗ no detection
- **Spring `@GrpcClient` annotation** → ✗ no detection

The gap clusters around three signal sources that `type_assigns` doesn't capture:
1. Constructor parameter types (DI injection)
2. Function return types (factory functions)
3. Annotations / decorators (DI registration)

### 1.4 Why this matters now

Cross-repo intelligence is one of the most-asked-for capabilities in code-graph tools. Industry tooling that does parts of this:

| Tool | Approach | Limitation |
|---|---|---|
| Backstage (Spotify) | Service catalog from OpenAPI / AsyncAPI / .proto files | Manual catalog maintenance, declarative not derived |
| Sourcegraph | Cross-repo references via SCIP indexes | Per-symbol, doesn't protocol-match (only name-matches) |
| Apollo Studio | Federated GraphQL via `@key` directives | GraphQL only |
| AsyncAPI tooling | Typed async-message matching | AsyncAPI-spec only, requires explicit AsyncAPI files |
| GitHub CodeQL | Cross-repo dataflow for security | Security-focused, heavyweight |
| stack-graphs (GitHub) | Universal name resolution graph | Within-repo only |

cbm is uniquely positioned: single binary, AST + LSP-grade extraction, sub-second incremental indexing, no external service dependencies. The cross-repo capability matters because it's the missing 20% of value that turns "smart code search" into "service-architecture truth source."

---

## 2. Cross-language pattern audit

The producer→consumer routing problem decomposes into four tiers. Each tier is generic across major language ecosystems. Concrete framework instances per tier:

### Tier 1 — IDL-driven typed stubs (gRPC, GraphQL, OpenAPI, AsyncAPI)

The stable identifier lives in an IDL file shared between producer and consumer repos. Both sides reference generated types derived from the same IDL.

| Ecosystem | Producer pattern | Consumer pattern | Stable identifier |
|---|---|---|---|
| **gRPC** (Go, Java, Python, Rust, TS, C#, Kotlin, Swift) | `*Client` from .proto codegen | `*Base` / `*Servicer` impl from .proto codegen | `service.method` from .proto |
| **GraphQL Federation** (any GraphQL stack) | typed query/mutation client | resolver bound to type with `@key` directive | type + key from .graphql |
| **OpenAPI** (NSwag/openapi-generator/oapi-codegen) | generated typed client per language | controller/handler matching path+method | path + method from openapi.yaml |
| **AsyncAPI** | generated publisher | generated subscriber | channel + message from asyncapi.yaml |

**Detection**: parse the IDL file (or extract via existing tree-sitter grammars — `grammar_protobuf` is already vendored), extract canonical IDs as routes; on producer side find references to generated client types; on consumer side find generated base-class implementations.

**Genericity**: 100%. gRPC alone covers 8+ languages. .proto/.graphql/.openapi files are language-agnostic by design.

### Tier 2 — Typed message pub/sub (interface-ancestor + generic-type)

The stable identifier is a message type's fully-qualified name. Producer has `Publish<T>` / `Send<T>` / equivalent on a known interface; consumer implements `IConsumer<T>` / `@MessageHandler` / equivalent.

| Language | Frameworks | Producer shape | Consumer shape |
|---|---|---|---|
| C# | MassTransit, NServiceBus, Wolverine, Brighter, Rebus | `IPublishEndpoint.Publish<T>` / `ISendEndpoint.Send<T>` / `IBus.Publish<T>` | `IConsumer<T>` / `IHandleMessages<T>` |
| Java/Kotlin | Spring Cloud Stream, Axon, Eventuate | `streamBridge.send(...)` / `@CommandHandler` | `@StreamListener<T>` / `@EventHandler` |
| Node/TS | NestJS microservices, Moleculer, EventBus libs | `@MessagePattern<T>` emit | `@EventPattern<T>` handler |
| Python | Faust, Celery (typed), aio-pika typed wrappers | `@app.agent` send | typed handler funcs |
| Go | Watermill, NATS-typed, Wire | typed publish via marshalers | typed subscriber registration |
| Rust | Lapin + serde, async-nats with typed deserialization | typed publish | typed subscribe |

**Detection**: pattern-match the producer interface (e.g., `IPublishEndpoint`, `streamBridge`) with its `Publish<T>` / `Send<T>` method, extract `T` from the generic param or the constructor argument's type. On consumer side, find classes implementing `IConsumer<T>` / `IHandleMessages<T>` / classes with `@StreamListener<T>` on a method, extract `T`. Match by FQN.

**Genericity**: highly cross-language. ~6 framework families, identical abstract pattern.

### Tier 3 — Attribute / decorator-driven HTTP routes

Producer is a typed HTTP client whose interface methods carry route attributes; consumer is a controller/handler with matching route attributes. Both attribute values are literal strings — the easiest tier *if extracted from the attribute, not the call site*.

| Language | Producer | Consumer |
|---|---|---|
| C# | Refit, RestEase (`[Get("/x")]` on interface) | ASP.NET Core (`[HttpGet("/x")]` controller) |
| Java | Feign (`@RequestLine("GET /x")`), Retrofit (`@GET("/x")`) | Spring (`@GetMapping("/x")`), JAX-RS (`@GET @Path("/x")`) |
| Kotlin | Retrofit | Spring, Ktor route DSL |
| TypeScript | tsoa, NestJS HttpService with openapi-derived clients | NestJS (`@Get("/x")`), Hono, Express decorators |
| Python | httpx-codegen, aiohttp wrappers from openapi | FastAPI (`@app.get("/x")`), Litestar |
| Go | huma generated, oapi-codegen clients | huma, chi, gin, echo route registration |
| Rust | utoipa generated | actix-web, axum, rocket route attributes |

**Detection**: extract HTTP method + path from class-level / method-level attributes on both interfaces (producer) and concrete classes (consumer). Match.

**Genericity**: most universal — decorator-driven HTTP routing is the modern default in every serious web ecosystem.

### Tier 4 — Config-resolved service discovery

The producer's call site has only a relative path or named-client reference; the actual base URL lives in a config file (`appsettings.json`, `application.yaml`, env vars, Kubernetes Service DNS, service-registry config). Consumer side uses Tier 3 attribute-driven detection.

| Ecosystem | Producer | Config source |
|---|---|---|
| C# | `IHttpClientFactory.CreateClient("name")` | `appsettings*.json`, `services.AddHttpClient(...)` |
| Spring | `@FeignClient(name="x", url="${promo.url}")` | `application.yaml`, env |
| Spring Cloud / Eureka / Consul | service registry lookups | registry config |
| Kubernetes | Service DNS (`http://my-service:80/x`) | Service / Ingress YAML |
| Node | env-driven base URLs in axios/fetch wrappers | `.env`, Helm values |
| Go | viper-loaded named services | YAML / env |

**Detection**: scan config files for named-service → base-URL mappings; trace `CreateClient("name")` / `@FeignClient("name")` to resolved URL; combine with the variable URL path within the calling method to reconstruct the full route.

**Genericity**: universal microservice pattern.

### Tier 5 — Reflection / runtime-resolved DI (out of scope)

`_serviceProvider.GetService(Type.GetType(configString))?.Invoke(...)` is genuinely impossible to resolve statically. This tier is named for completeness but explicitly out of scope. Estimated <5% of cross-service calls in practice.

---

## 3. Proposed architecture

### 3.1 Plugin-based service-pattern registry

`internal/cbm/service_patterns.c` currently hardcodes 252 patterns in a C array. Adding a new framework requires a C patch + recompile. Proposal: externalize the pattern table to a YAML / JSON registry loaded at startup.

Format example (registry-format-1.yaml — actual schema TBD with maintainer):

```yaml
patterns:
  # Tier 2 — typed-message pub/sub
  - id: masstransit-publish
    languages: [csharp]
    kind: ASYNC_CALLS
    producer:
      match:
        type_implements: IPublishEndpoint
        method_pattern: "Publish<T>(...)"
      extract_id_from: generic_type_arg_or_first_arg_type
      id_kind: message_fqn
      broker: rabbitmq
    consumer:
      match:
        class_implements: "IConsumer<T>"
      extract_id_from: generic_type_arg
      id_kind: message_fqn

  - id: spring-cloud-stream-handler
    languages: [java, kotlin]
    kind: ASYNC_CALLS
    producer:
      match:
        method_calls: "streamBridge.send"
        first_arg_kind: string_literal
      extract_id_from: first_arg
      id_kind: channel_name
    consumer:
      match:
        method_annotation: "@StreamListener"
      extract_id_from: annotation_value
      id_kind: channel_name

  - id: refit-client
    languages: [csharp]
    kind: HTTP_CALLS
    producer:
      match:
        interface_method_attribute: "[Get|Post|Put|Delete|Patch]"
      extract_id_from: attribute_arg
      id_kind: http_route
    consumer:
      match:
        method_attribute: "[HttpGet|HttpPost|HttpPut|HttpDelete|HttpPatch]"
      extract_id_from: attribute_arg
      id_kind: http_route
```

Benefits:
- Adding Wolverine, Watermill, or any new framework is one YAML entry, not a code patch + release cycle
- Maintainer review surface drops dramatically (review YAML, not C)
- Community contributions become low-risk (a YAML PR can't crash the binary)
- Multi-language patterns compose naturally (one ID matches both Java and Kotlin via `languages: [java, kotlin]`)

Existing 252 patterns in `service_patterns.c` can be migrated to YAML in a separate cleanup PR (no behavior change, pure refactor) — out of scope for this proposal but a natural follow-on.

### 3.2 Pipeline integration

Two changes to the existing pipeline:

1. **New pass: `pass_idl_scan`** — runs once per repo before `pass_definitions`. Scans for IDL files (.proto, .graphql, openapi.yaml, asyncapi.yaml) and emits canonical Route nodes derived from them. Each Route gets a stable QN like `__route__grpc__<package.Service>/<method>` regardless of which language consumes it.

   **Status:** ✅ landed in #293. Runs after `pass_calls` (not before `pass_definitions`) because it needs the proto-derived Class+Function nodes and INHERITS edges already in the gbuf. Implementation choice was simpler than re-parsing IDL files; CBM's existing tree-sitter-protobuf grammar already extracts services as Class nodes and rpcs as Function nodes via `pass_definitions`, so `pass_idl_scan` walks the gbuf rather than the filesystem.

2. **Extend `pass_calls.c emit_classified_edge`** — when matching against the new YAML-driven patterns, support extracting identifiers from:
   - Generic type parameters (`Publish<T>`)
   - Constructor argument types (`Publish(new T(...))`)
   - Interface-method attributes (`[Get("/x")]`)
   - Class-level attributes (`@FeignClient(name="x")`)
   - Combined with config-resolved values (Tier 4)

   **Status:** Tier 2/3/4 work. `pass_idl_scan` (#293) does this for gRPC `GRPC_CALLS` edges via a separate detection path keyed by `type_assigns`.

3. **Auto-trigger cross-repo pass for workspace siblings** — when a repo is part of a workspace (e.g., `cross-repo-intelligence` mode is invoked once with `target_projects: ["*"]`), persist the workspace membership in the artifact, and on subsequent re-indexes auto-fire cross-repo matching against the same sibling set.

   **Status:** still open. Useful UX improvement but orthogonal to the type-detection work.

### 3.3 Cross-repo extension

The existing `cbm_cross_repo_match` already supports topic-based matching. Two extensions:

1. **Add `match_by_message_fqn`** — phase D (after HTTP / Async / Channel matching). For each ASYNC_CALLS edge with `message_fqn` property, find consumer-side `IConsumer<message_fqn>` registrations in target DBs and emit CROSS_ASYNC_CALLS edges.

2. **Add `match_by_grpc_method`** — phase E. For each gRPC client call with `service.method` identifier, find consumer-side `*Base` overrides of the same `service.method` and emit CROSS_GRPC_CALLS edges. Reuses the existing CROSS_GRPC_CALLS edge type and emission helper at `pass_cross_repo.c:657`.

**Status:** Both extensions ALREADY EXIST in `main` via `match_typed_routes` (lines 492-549 of `pass_cross_repo.c`). The function reads the source-DB Route's QN and looks it up in target DBs, agnostic to which pass emitted it. Phase D handles GRPC_CALLS / GRAPHQL_CALLS / TRPC_CALLS uniformly. Tier 1 (#293) plugs into this as-is.

---

## 4. Tier 1 detailed spec — gRPC

### 4.1 Original v1 producer-side spec

(Quoted from initial proposal.) Detect references to generated gRPC client types. The detection signal is the **type name pattern**, not call-site strings:

- C#: classes/interfaces ending in `Client` derived from `Grpc.Core.ClientBase<T>` (generated by `Grpc.Tools`)
- Go: structs with `*grpc.ClientConn` field + methods matching .proto service methods
- Python: classes from `*_pb2_grpc.py` ending in `Stub`
- Java/Kotlin: classes ending in `*Grpc.*Stub` (generated by `protoc-gen-grpc-java`)
- TypeScript: classes from `*_pb_grpc.d.ts` with the right shape
- Rust: tonic-generated `*Client` structs

For each method call on a generated client type:
1. Resolve the client type to its underlying `service.method` pair (recoverable from .proto)
2. Emit a `GRPC_CALLS` edge with new properties: `{rpc_kind: "grpc", service: "promocode.PromoCodeManagerGrpcService", method: "GetVoucher"}`

### 4.2 Consumer-side extraction (shipped in #293)

Detect classes implementing the generated gRPC server-base type via INHERITS edges:

- C#: `: PromoCodeManagerGrpcServiceBase`
- Go: structs with method receivers matching the unimplemented server interface (currently out of scope — see §4.6)
- Python: classes inheriting `*Servicer`
- Java: `extends *ImplBase`
- Rust: `impl *Server for ...` (handled by IMPLEMENTS, not INHERITS — out of scope for v1)

For each method override of a service method, emit a HANDLES edge from the implementing method to the corresponding Route node.

Server-side base class suffixes (longest-first to disambiguate):
```
ServicerBase, AsyncServicer, ServiceBase, ImplBase, Servicer, Base
```

### 4.3 IDL parsing (shipped in #293)

`pass_idl_scan` walks the **gbuf** (not the filesystem) for `Class` nodes whose `file_path` ends in `.proto`. CBM's tree-sitter-protobuf grammar (`grammar_protobuf.c`, language `CBM_LANG_PROTOBUF`) already extracts proto services as Class nodes (`service` kind) and rpcs as Function nodes (`rpc` kind) via the standard `pass_definitions` pipeline. `pass_idl_scan` walks `DEFINES_METHOD` edges from each proto-Class to its rpcs and emits canonical Route nodes.

Route QN format: `__route__grpc__<Service>/<rpc>`. Aligned with the existing `__route__<protocol>__<id>` convention used for HTTP and async Routes.

(Originally the spec called for re-parsing `.proto` files in a separate `pass_idl_scan` pre-pass; the gbuf-walking approach is simpler and avoids duplicate parsing.)

### 4.4 Cross-repo matching (no changes needed)

Phase D in `cbm_cross_repo_match` (`match_typed_routes` at `pass_cross_repo.c:492`) iterates `GRPC_CALLS` edges in the source store, reads the Route's QN, looks it up in target stores, follows the `HANDLES` edge to the impl method, and emits `CROSS_GRPC_CALLS` bidirectionally. **Already in `main`** before this proposal — Tier 1 just plugs in as a producer/consumer of QNs.

### 4.5 v1 estimated diff size (actual #293)

Original estimate: ~560 LOC.
Actual #293: ~1483 lines added across 14 files (pass + tests + 9 fixture files). The pass itself is ~580 LOC. The discrepancy is due to (1) unit tests being more thorough than estimated, (2) fixture coverage across 6 languages, (3) extensive docstring comments documenting cross-language conventions.

### 4.6 Test fixtures (multi-language) — shipped in #293

Reference fixtures under `testdata/cross-repo/grpc/`:
- `contracts/promo.proto` — single .proto with `package promo; service PromoCodeService { rpc GetVoucher(...); rpc RedeemVoucher(...); }`
- `server-python/promo_server.py` — Python `PromoCodeServiceServicer` subclass
- `server-csharp/PromoCodeService.cs` — .NET `PromoCodeServiceBase` subclass with `*Async` methods
- `server-java/PromoCodeServiceImpl.java` — Java `PromoCodeServiceImplBase` subclass
- `client-python/promo_client.py` — Python `stub = PromoCodeServiceStub(channel)` pattern
- `client-csharp/PromoCodeClient.cs` — C# `var c = new FooServiceClient(channel)` pattern
- `client-java/PromoCodeClient.java` — Java `stub = PromoCodeServiceGrpc.newBlockingStub(channel)` pattern (currently NOT detected by v1 — see §5.4)

Tests assert: after gbuf population mirroring fixture shapes, expected Route nodes exist + expected HANDLES + expected GRPC_CALLS edges per language.

### 4.7 Success criteria — original

| Metric | Target | Actual (v1, #293) |
|---|---|---|
| Precision on test fixtures | 100% | ✓ 100% |
| Recall on test fixtures | 100% | ✓ 100% (synthetic) |
| Index-time overhead | <5% per repo with .proto files | ✓ <2% in fleet test |
| Index-time overhead per repo without .proto | 0% (pass is no-op) | ✓ ~5ms (file_count loop only) |
| Memory overhead | proportional to .proto count | ✓ ~1KB per service |
| Backwards compatibility | All existing tests pass | ✓ 2609/2609 |

### 4.8 Real-world success criteria (introduced by validation, April 2026)

The original criteria covered synthetic test fixtures. Real-world validation against an 11-service .NET microservice fleet revealed criteria the original spec didn't define:

| Metric | Target | Actual (v1, #293) |
|---|---|---|
| `CROSS_GRPC_CALLS` edges emitted per fleet | matches manual service-graph audit | **0** (gap analysis below) |
| Producer detection rate on modern .NET fleet | >80% | **<20%** (only manual `var = new Client()` patterns hit) |
| False-positive Route count from v1 detection | 0 | 0 (✓ — denylist works) |
| False-positive Route count from pre-existing `pass_parallel` | 0 | **dozens** (✗ — see #294) |

The fleet test exposed three gaps that drove the Tier 1 sub-decomposition described in §5.

---

## 5. Tier 1 sub-decomposition (1a–1f)

The original proposal treated Tier 1 as a single ~560-LOC PR. Real-world validation showed the producer-side problem is a family of **type-flow signals**, each requiring its own small extension. Decomposing into sub-tiers makes each one independently shippable, testable, and reviewable.

### 5.1 Tier 1a — IDL Route emission ✅ SHIPPED in #293

Walk gbuf for `.proto`-derived `Class` nodes; emit `Route` nodes with QN `__route__grpc__<Service>/<rpc>` plus `HANDLES` edge from each rpc Function back to its Route.

**Coverage:** universal — fires whenever a `.proto` is indexed, language-agnostic.
**LOC:** ~80 in `pass_idl_scan.c`.

### 5.2 Tier 1b — `type_assigns`-based producer detection ✅ SHIPPED in #293

Walk per-file `CBMFileResult.type_assigns`; for each `var = SomeStub(...)` assignment whose RHS type matches a stub-suffix pattern and isn't denylisted, record `(enclosing_qn, var_name, service_name)`. For each `var.Method(...)` call, emit a `GRPC_CALLS` edge.

Includes file-scope fallback (var-scope match across functions to handle C# class fields), `*Async` method stripping, lowerCamelCase → PascalCase capitalization, and a non-gRPC type denylist (`System.Net.*`, `Microsoft.Extensions.Http`, `RestSharp`, `Refit`, etc.).

**Coverage:** patterns where the consumer code explicitly assigns a stub variable:
- Python `stub = pb2_grpc.FooStub(channel)` ✓
- C# manual `var c = new Foo.FooClient(channel)` ✓
- Rust `let c = FooClient::new(channel)` ✓
- TS `const c = new pb.FooClient(...)` ✓
- C# class-field via ctor body `_client = new FooClient(channel)` ✓ (file-scope fallback)

**LOC:** ~500 in `pass_idl_scan.c` + ~200 in tests.

### 5.3 Tier 1c — Constructor-parameter tracking 📋 PROPOSED

**Problem:** Modern .NET DI (Grpc.Net.ClientFactory) registers stubs in `Startup.cs` and consumers receive them as constructor parameters:

```csharp
public class FooController(FooServiceClient client) {
    public async Task X() { await client.GetVoucherAsync(req); }
}
```

There is no `var = ` assignment for `client`. `type_assigns` captures nothing. Tier 1b misses entirely.

Same shape applies to:
- Spring Java `@Autowired` constructor injection
- Kotlin Spring DI
- ASP.NET Core primary constructors (C# 12)
- Any DI framework where the type signal lives in the constructor param type, not in user-written assignment code

**Implementation:**
1. For each `Class` node `C` in gbuf, find its constructor methods. Heuristic per language:
   - C#: Method named like `<ClassName>` OR Method with label `Constructor` (CBM-specific)
   - Java: same
   - Kotlin: same plus `init {}` blocks
   - Python: Method named `__init__`
   - TypeScript: Method named `constructor`
2. Read `param_names[]` and `param_types[]` from the ctor's properties_json (CBM extracts these today — `pass_definitions.c:191-192`).
3. For each ctor param of type matching a stub suffix (and not denylisted), record `(class_qn, param_name, service_name)` with **class-wide scope** — meaning every method of class `C` can match this var name.
4. Extend `idl_stub_var_arr_find` to also try class-scope lookup when both function-scope and file-scope misses fail.

**Coverage gain:** modern .NET DI, Spring constructor injection, Kotlin DI. Conservatively 50%+ of OOP gRPC consumers that #293 currently misses.

**LOC estimate:** ~250 (pass extension) + ~150 (tests covering 3 languages) = ~400 LOC.

**Risk:** medium-low. CBM already extracts param types reliably across languages. Main risk is edge cases (e.g., C# primary constructors generate synthetic parameterless ctors — need to walk multiple ctors).

### 5.4 Tier 1d — Factory-function return-type inference 📋 PROPOSED

**Problem:** Java and Go gRPC consumers don't type variables explicitly — they rely on type inference from a factory function's return type:

```java
// Java
PromoCodeServiceGrpc.PromoCodeServiceBlockingStub stub
    = PromoCodeServiceGrpc.newBlockingStub(channel);
// In practice, often written as:
var stub = PromoCodeServiceGrpc.newBlockingStub(channel);
```

```go
// Go
client := pb.NewPromoCodeClient(conn)
```

CBM's `type_assigns` may capture the LHS type if the extractor handles `var` / `:=` inference, but the type name itself comes from the factory function's name pattern.

**Implementation:**
1. Detect factory-function call patterns in `type_assigns` RHS:
   - `<X>Grpc.newBlockingStub(...)` / `newFutureStub` / `newAsyncStub` / `newStub` (Java/Kotlin)
   - `<pkg>.New<X>Client(...)` (Go)
   - `<X>::new(channel)` (Rust — already covered by suffix on type)
2. For each match, derive the service name from the factory function's name pattern (strip `New`, `new`, `pb.`, etc.; strip `Stub` / `Client` suffix from the result).
3. Synthesize a `(var, type_name, service)` tuple as if it were a regular `type_assigns` entry.

**Coverage gain:** Go grpc-go (a major miss), Java `newBlockingStub` / `newFutureStub` / `newStub`, Kotlin equivalents.

**LOC estimate:** ~150 + ~100 tests = ~250.

**Risk:** medium. Factory function naming is conventional but not enforced. False positives possible from unrelated `New<X>Client` functions in non-gRPC codebases (mitigated by Phase D filter — non-matching cross-repo lookups produce no edges).

### 5.5 Tier 1e — DI-registration scanning 📋 PROPOSED

**Problem:** When the type signal lives in a registration call rather than at the consumer site:

```csharp
// Startup.cs
services.AddGrpcClient<Acme.Foo.V1.FooServiceClient>(o => o.Address = ...);

// Later, in consumer code:
public class FooController(FooServiceClient _client) { ... }
```

Even with Tier 1c, this works only if `FooServiceClient` ends in a recognized suffix. For custom-named clients (rare but real, especially with manual stub wrapping), the only reliable signal is the registration generic argument.

Same concept in Spring:
```java
@GrpcClient("promo")
private PromoCodeServiceBlockingStub stub;
```

**Implementation:**
1. Detect DI-registration calls:
   - C# `services.AddGrpcClient<T>(...)` — extract `T` from generic argument
   - Spring `@GrpcClient("name")` annotation — bind annotation arg + field type
   - NestJS `@Client({ ... })` decorator on class properties
2. Build a per-repo "known stub types" registry, keyed by FQN.
3. When Tier 1c detects a ctor param of a registered type (or any type ending in a stub suffix already covered), confidence is upgraded.
4. When the suffix-stripped service name matches a registered type's name pattern, emit even if no other signal matched.

**Coverage gain:** custom-named clients, Spring annotation-based clients, NestJS decorator-based clients. Edge cases that Tier 1c doesn't cover.

**LOC estimate:** ~200 + ~150 tests = ~350. Higher than 1c/1d because of cross-language registration syntax variance.

**Risk:** medium. Generic-argument extraction in tree-sitter requires careful AST navigation. Annotation-based detection (Spring/NestJS) needs decorator-arg traversal that CBM has elsewhere but is non-trivial to wire in.

### 5.6 Tier 1f — Field/property type tracking 📋 PROPOSED

**Problem:** Some consumers declare stub fields/properties without going through a constructor or DI registration, e.g.:

```csharp
public class Worker {
    private static readonly FooServiceClient Client = CreateClient();
    public async Task X() { await Client.GetVoucherAsync(req); }
}
```

Static field initialization with a non-ctor factory. Tier 1c (ctor params) doesn't apply. Tier 1b (`type_assigns`) might fire if CBM extracts the field's type from its declaration (which it does), but the var-scope is class-static, not function or class-instance.

**Implementation:**
1. Verify CBM extracts class fields with type info. Inspection: `internal/cbm/extract_defs.c` field handling for each language. Likely yes for Java/C#/Kotlin, less certain for Python (annotations).
2. For each class field of a stub-suffix type, record `(class_qn, field_name, service_name)` with class-wide scope (same shape as Tier 1c).
3. Extend lookup to include this scope as a fallback after function/file/class.

**Coverage gain:** static field patterns, less common but appears in singleton-style consumer code.

**LOC estimate:** ~100 + ~100 tests = ~200, **assuming CBM already extracts field types**. If extractor work is needed, +200-400 LOC.

**Risk:** low coverage gain, medium implementation risk depending on extractor state.

### 5.7 Tier 1g — Contract-package FQN extraction 📋 PROPOSED (deferred from the big Tier 1 PR)

**Problem:** the Route key shipped in #293 is `__route__grpc__<Service>/<method>` using the **bare** service name. Producer (.proto) has the proto `package`; consumer (typed-client class reference) has only the language-side namespace, which is *not always* the proto package — `option csharp_namespace`, Java's `option java_package`, etc. let the two diverge.

The adversarial review of the tier1 branch (April 2026) flagged this as a real risk: two `.proto` files in different packages but with the same `service` + `rpc` names will `cbm_gbuf_upsert_node()` to the same Route node, both within a project (multi-`.proto` repos) and across repos. The current code mitigates by emitting a `cbm_log_warn("idl_scan.route_collision", ...)` when a second file produces the same QN, and by carrying `service_qn` (proto Class qualified_name) in Route properties so a future FQN-aware matcher has the package available — but cross-repo lookup still joins on the bare key.

A symmetric FQN fix needs the consumer to know the proto package. None of Tier 1c/1d/1e/1f can derive it from typed-client source alone. Tier 1g closes that gap.

**Implementation:**

1. **Locate the contract package** for each consumer-side typed-client reference. Distribution patterns:
   - **NuGet:** `<Org>.<Domain>.<Service>.Contracts` package referenced from `*.csproj` (`<PackageReference Include="...Contracts" />`); the package's `lib/` directory ships generated `Service.pb.cs` / `ServiceGrpc.cs` plus the original `.proto` under `contentFiles/` or `tools/proto/` depending on packaging convention. Cache lives in `~/.nuget/packages/<id>/<version>/`.
   - **Maven:** `*-contracts` JAR under `META-INF/proto/` or as a sibling resource directory; cache in `~/.m2/repository/...`.
   - **Vendored:** `contracts/`, `protos/`, `proto/`, or `submodule/` directories inside the consumer repo with `.proto` files alongside generated bindings.
   - **Submodule / git-subtree:** `.gitmodules` or shared-checkout dirs that already extract as Class nodes.

   `pass_idl_scan` already runs after `pass_definitions` and walks the gbuf for proto Class nodes, so vendored / submodule cases are free — proto Class nodes carry `qualified_name` of the form `<package>.<Service>` already. The new work is only NuGet/Maven cache resolution.

2. **Build a `<typed-client-class-fqn> → <proto-package.Service>` map** at the start of `pass_idl_scan` for the consumer repo:
   - For each proto Class node in the gbuf (vendored case), record `<bare service> → <package.Service>`.
   - For each NuGet/Maven contract package referenced from the manifest (d94a501 already detects these), parse `.proto` files in the cached package directory, extract `package` declarations + service names, and add to the map.
   - Tie-break: when the bare name is ambiguous (two packages contributing the same service name to one consumer), record both and emit cross-product `CROSS_GRPC_CALLS` flagged with `ambiguous: true`. Better than silently picking one; surfaces the collision at query time.

3. **Re-key consumer-side Routes to FQN** when the map resolves the bare name unambiguously. Producer-side already has `service_qn` available from §Gap 7's mitigation — wire `idl_emit_route_for_rpc` to prefer it when the consumer's contract distribution is also FQN-aware.

4. **Backwards-compatible matching:** during the transition, `match_typed_routes` should accept either bare or FQN keys on either side. Index Route nodes by both `qualified_name` and a derived `service_qn` property. Avoids a flag-day migration.

**Coverage gain:** symmetric FQN keying for any fleet using contract-package distribution (the dominant .NET pattern via NuGet `*.Contracts`, the standard Java pattern via `*-contracts` JARs) or vendored `.proto`. Removes the cross-package collision risk in Gap 7 of `tier1-extractor-fixes.md`.

**LOC estimate:** ~250 + ~150 tests = ~400.
- NuGet/Maven cache scan: ~80 LOC (reuses d94a501's contract-package detection, extends with `.proto` filesystem walk).
- `.proto` minimal parse for `package` + `service`: ~60 LOC (or reuse `pass_idl_scan`'s gbuf walk if the contract `.proto`s are also extracted as gbuf nodes when the cache directory is in the indexer's scope — easier path; depends on whether `~/.nuget/packages/` paths are indexable).
- Map + re-keying: ~50 LOC.
- Backwards-compatible matcher: ~30 LOC in `pass_cross_repo.c`.
- Tests: ~150 LOC across .NET / Java / vendored fixtures.

**Risk:** low to medium. NuGet/Maven cache layout differs across Windows/Linux/macOS and across package versions (the `proto` files aren't always at the same relative path within the package). Mitigation: probe a small allowlist of conventional locations and skip when none match — the system stays as-is when the contract package doesn't ship `.proto` (some packages ship only generated bindings). Java's `protoc-gen-java` doesn't include the `.proto` source by default, so Java fleets that don't bundle `.proto` in their contract JAR are out of luck and stay on bare keys; in practice most Spring Cloud Contract / `protobuf-maven-plugin` setups *do* bundle.

**Dependencies:** d94a501's contract-package detection (already shipped). Independent of Tier 1c–1f — Tier 1g is purely a Route-key-derivation enhancement; the consumer-side detection passes feed the same routes regardless.

#### Status — deferred from the big Tier 1 PR

After validating Tier 1g.1 (producer-side FQN dual emission) on the test fixture and pushing it through five rounds of adversarial review, the conclusion was that **the half-shipped version is dead weight without consumer-side FQN derivation**:

- Cross-repo matching still joins on the bare-name key, so the FQN-keyed Routes are essentially unused nodes in the matching path.
- The collision-defense it provides only matters when two `.proto` files in the indexed set share `<service>/<method>` names in different proto packages — real-world fleet validation didn't trigger this once across hundreds of `.proto` files.
- Each adversarial-review round uncovered new edge cases (TOCTOU on canonicalize-then-open, order-dependent winner under parallel scheduling, incremental orphan after the winning `.proto` is deleted, monotonic property preservation under collision + parse failure, etc.) — all real, but only relevant when collisions actually occur.

The pragmatic call: **drop the entire Tier 1g family from the big Tier 1 PR** and ship it as a focused follow-on PR once a fleet actually reports a collision. The big PR's scope stays cohesive (extractor fixes from `tier1-extractor-fixes.md` Gaps 1-6 + producer-side detection + the Gap 7 collision-warning mitigation, which is the cheap ~15-LOC observability piece). The proposed Tier 1g work is sequenced as four pieces in the follow-on PR:

- **Tier 1g.1 — producer-side dual emission.** Parse `.proto` `package <id>;` and emit both an FQN-keyed Route (`__route__grpc__<package>.<service>/<method>`) and a bare-keyed alias, with `HANDLES` from the rpc Function and `proto_package` + `key_kind` properties on both. ~150 LOC base. Adversarial review surfaced and fixed: tri-state `IDL_PKG_OK/NONE/ERROR` so parse failures don't silently mask as "no package", BOM and multi-line block-comment handling, `snprintf`-truncation hard-fails, the bare alias's `file_path` set to `""` to survive incremental proto-file deletion, monotonic property preservation when a colliding second proto hits `IDL_PKG_ERROR`, and skipping the duplicate `HANDLES` edge on package mismatch so the bare Route doesn't acquire handlers from multiple unrelated impls. Hardened total ~390 LOC.

- **Tier 1g.1b — package extraction at AST time.** `pass_idl_scan` shouldn't reopen `.proto` files by path to read the package declaration; the proto Class / Function nodes were already extracted by `pass_definitions` reading the same file earlier in the same pipeline run, and a TOCTOU window between the two reads can let the package metadata come from a different snapshot than the AST nodes. Fix: extend `internal/cbm/extract_defs.c`'s protobuf branch to extract the `package` declaration during the AST walk and stash it as a property on the proto Class node. `pass_idl_scan` then reads package from the gbuf node, eliminating the second open and any TOCTOU window. ~50 LOC in `extract_defs.c` plus a small reader change in `pass_idl_scan.c`.

- **Tier 1g.1c — deterministic collision resolution.** The 1g.1 bare-key collision handling has two leftover issues: (1) *incremental orphan* — when the winning emitter's `.proto` is later deleted/renamed, its HANDLES edge is purged with the rpc Function, but the bare Route survives (`file_path=""`) with the old `proto_package` and zero HANDLES; subsequent reindexes of the colliding partner skip HANDLES too, leaving the bare Route handlerless until full reindex. (2) *Order dependence* — proto Class iteration in the parallel pipeline depends on worker-bucket scheduling, so which package "wins" the bare HANDLES is non-deterministic across reindexes. Fix needs (a) a new graph-buffer primitive to remove HANDLES edges from a bare Route on collision detection and (b) a deterministic tie-break (lexicographic-smallest `proto_package` wins, or equivalent). ~80 LOC in graph_buffer.c + ~50 LOC restructuring in pass_idl_scan.c + targeted fixtures.

- **Tier 1g.2 — consumer-side FQN derivation.** Scan referenced NuGet/Maven contract packages in the manifest cache (`~/.nuget/packages/`, `~/.m2/repository/`, etc.), extract `.proto` package declarations from packaged sources where present, build a `<typed-client class FQN> -> <proto-package.Service>` map, and re-key consumer-side Routes to FQN. After this lands, both ends agree on FQN keys; the bare-key alias from 1g.1 can be retired. ~250 LOC + ~150 tests. This is the piece that makes Tier 1g.1's FQN Routes non-dormant on the cross-repo matching path.

The four pieces together form a coherent "collision-safe FQN matching" PR. Until then, the big Tier 1 PR's Gap 7 mitigation (collision warning at `idl_scan.route_collision` + `service_qn` property in Route props, ~15 LOC, see `tier1-extractor-fixes.md`) gives operators the diagnostic signal when collisions occur, without the complexity surface of the FQN data model.


### 5.8 Sub-tier sequencing recommendation

Land in this order:
1. **#293 (1a + 1b)** — already up. Validates architecture, ships proto Routes + HANDLES + manual stub-var producer detection.
2. **Tier 1c (ctor params)** — biggest unlock for OOP DI-heavy fleets. Single high-leverage PR.
3. **Tier 1d (factory return types)** — closes the Java/Go gap.
4. **Tier 1e (DI registration)** — cleans up edge cases and custom-named clients.
5. **Tier 1f (field types)** — diminishing returns; ship if the extractor work is cheap.
6. **Tier 1g (collision-safe FQN matching)** — bundled focused follow-on PR sequencing 1g.1 + 1g.1b + 1g.1c + 1g.2 together once a fleet actually reports a `<service>/<method>` collision across proto packages. Until then the big Tier 1 PR ships only the Gap 7 collision-warning mitigation (~15 LOC, see `tier1-extractor-fixes.md`); the FQN data model is gated on consumer-side derivation (1g.2), so half-shipping just 1g.1 produces dormant nodes that don't change matching behaviour.
7. **Tier 2-4** — proceed once Tier 1 hits ~90% coverage and the `(var, type, scope)` abstraction is proven.

After 1c+1d, real-world coverage hits ~85% across the major OOP languages. After 1e, ~95%. 1f closes residual cases. The 1g family doesn't change coverage — it tightens the correctness contract for fleets where service+method names collide across proto packages, a scenario that's rare but observable.

---

## 6. Producer-side signal source matrix

The signal source is what tells the indexer that a given variable holds a gRPC client/stub. Same problem cross-language; different syntactic locations.

| Idiom | Signal source | Tier | Captured? |
|---|---|---|---|
| Python `stub = pb2_grpc.FooStub(ch)` | local var `type_assigns` | 1b | ✅ #293 |
| Python class field `self.stub = FooStub(ch)` | `type_assigns` (in `__init__`) | 1b | ✅ #293 (file-scope fallback) |
| C# `var c = new FooClient(ch)` | local var `type_assigns` | 1b | ✅ #293 |
| C# class field `_client = new FooClient(ch)` in ctor body | `type_assigns` | 1b | ✅ #293 (file-scope fallback) |
| C# Grpc.Net.ClientFactory ctor param `(FooClient client)` | ctor `param_types` | **1c** | ❌ proposed |
| C# class field declaration `private readonly FooClient _client;` | field decl type | **1f** | ❌ proposed |
| C# Spring-style `[FromServices] FooClient client` action param | action `param_types` | **1c** (extension) | ❌ proposed |
| Java `stub = FooGrpc.newBlockingStub(ch)` (typed `var`) | factory func name + return type | **1d** | ❌ proposed |
| Java Spring `@Autowired private FooStub stub;` | field decl type + annotation | **1f** + **1e** | ❌ proposed |
| Java Spring ctor injection `(FooStub stub)` | ctor `param_types` | **1c** | ❌ proposed |
| Kotlin `val stub = FooGrpc.newBlockingStub(ch)` | same as Java | **1d** | ❌ proposed |
| Kotlin Spring DI ctor `(stub: FooStub)` | ctor `param_types` | **1c** | ❌ proposed |
| Go `c := pb.NewFooClient(conn)` | factory func name + return type | **1d** | ❌ proposed |
| Go struct field `Client pb.FooClient` | struct field decl type | **1f** | ❌ proposed |
| Rust `let c = FooClient::new(ch)` | local var `type_assigns` | 1b | ✅ #293 |
| Rust `let c = FooClient::connect(...).await?` | factory call return type | **1d** | ❌ proposed |
| TS `const c = new pb.FooClient(...)` | local var `type_assigns` | 1b | ✅ #293 |
| TS NestJS `@Client({transport: ...}) private client: ClientGrpc;` | decorator + field type | **1e** + **1f** | ❌ proposed |
| TS NestJS `@MessagePattern('foo.bar')` | decorator-driven (Tier 2) | Tier 2 | future |

---

## 7. Architectural reusability for Tiers 2–4

The `(var_name, type_name, scope) → service_name` abstraction Tier 1 builds is reusable across all subsequent tiers. Each tier contributes its own detection rule that produces the same tuple shape, then the existing emission machinery (Route upsert, edge insertion, Phase D matching) kicks in.

### 7.1 Tier 2 — typed-message pub/sub

```csharp
// MassTransit
await _publishEndpoint.Publish<VoucherRedeemed>(new VoucherRedeemed(voucher.Id), ct);
```

Detection: `_publishEndpoint` is typed as `IPublishEndpoint` (via Tier 1c ctor injection). Method `Publish<T>` extracts `T` from the generic-arg position OR from the first argument's constructed type. Service "name" is the message type FQN `VoucherRedeemed`.

Reuses Tier 1's machinery:
- Tier 1c provides `_publishEndpoint` → `IPublishEndpoint` mapping
- New: extract generic-arg `T` from method invocation (small AST helper)
- New: emit `ASYNC_CALLS` edge to a Route with QN `__route__msgfqn__VoucherRedeemed`
- Consumer side: walk INHERITS for `IConsumer<T>` (interface inheritance, similar to Tier 1's `*Servicer` walk); extract `T` from the generic argument; emit HANDLES

The producer side's variable scoping is **identical** to Tier 1c. The only difference is how the service name is derived (generic arg, not type name).

### 7.2 Tier 3 — attribute-driven HTTP routes

Refit / Feign / Retrofit declare HTTP routes via attributes on interface methods:

```csharp
public interface IPromoApi {
    [Get("/api/voucher/{id}")]
    Task<Voucher> GetVoucher(string id);
}
```

Producer side: when a class field is typed as `IPromoApi` (Tier 1f / 1c) and a method is invoked, use the **interface method's attribute** to derive the route. This is consumer-side detection logic in producer-side code — same shape as Tier 1's `*Servicer` walk, applied to interfaces with route attributes.

Reuses Tier 1's machinery:
- Tier 1c/1f provides `_promoApi` → `IPromoApi` mapping
- New: walk `IPromoApi`'s method definitions in the gbuf (already extracted by `pass_definitions`)
- New: read `[Get("...")]` attributes from method properties_json
- Emit `HTTP_CALLS` edge to Route node `__route__GET__/api/voucher/{id}` with the route arg
- Consumer side already extracts `[HttpGet("...")]` controller routes (existing `pass_route_nodes` capability)

The variable scoping is **identical** to Tier 1c/1f. The detection rule changes (look at interface attributes instead of class hierarchy).

### 7.3 Tier 4 — config-resolved discovery

```csharp
var http = _httpClientFactory.CreateClient("promo-service");
var resp = await http.GetAsync("/api/voucher/" + id);
```

`_httpClientFactory` is typed as `IHttpClientFactory` (Tier 1c). The string `"promo-service"` is a named-client identifier. Resolution requires:
1. Detect `CreateClient("name")` calls — name extracted from first string arg
2. Read `services.AddHttpClient("promo-service", c => c.BaseAddress = ...)` registrations from `Startup.cs` (or `appsettings.json` `HttpClientFactoryOptions:promo-service:BaseAddress`)
3. Combine the named client's base URL with the call site's relative path to reconstruct the full target URL

Reuses Tier 1's machinery:
- Tier 1c provides `_httpClientFactory` → `IHttpClientFactory` mapping
- Tier 1e's DI-registration scanning provides the `name → URL` map from `services.AddHttpClient(...)` and config files
- New: track string-flow within a function scope (the `id` variable in the URL concatenation)

The variable scoping is **identical** to Tier 1. The new piece is config-file scanning + intra-method dataflow for URL composition.

### 7.4 Compounding investment summary

| Tier | New machinery | Reuses |
|---|---|---|
| 1a | gbuf walk for `.proto` Class nodes | — |
| 1b | `type_assigns` consumer + suffix matcher | 1a |
| 1c | ctor `param_types` consumer | 1a, 1b stub-var emission |
| 1d | factory func name → service inference | 1a, 1b stub-var emission |
| 1e | DI registration scan + custom registry | 1c, 1f |
| 1f | field `param_types` (or extractor extension) | 1a, 1b stub-var emission |
| 1g | NuGet/Maven contract-package `.proto` scan + FQN re-keying | d94a501 contract detection, 1a Route emission |
| 2 | generic-arg extraction + interface walk for `IConsumer<T>` | 1c (var → interface mapping) |
| 3 | interface-method-attribute reader | 1c/1f (var → interface), `pass_route_nodes` (consumer) |
| 4 | config file scan + named-client resolution | 1c (var → factory), 1e (registration) |

By the time Tier 4 lands, ~70% of its implementation is leveraging machinery built for Tier 1. **The cost-per-tier decreases monotonically.**

---

## 8. Roadmap — Tiers 2–4

Each tier is a separate PR after Tier 1 completes (1a + 1b shipped, 1c–1f sequenced). Sequence chosen by descending universality and ascending implementation complexity.

### 8.1 Tier 2 — typed message pub/sub (after Tier 1)

**Scope:** introduce the YAML-driven service-pattern registry; ship initial registry covering MassTransit (C#), Spring Cloud Stream (Java/Kotlin), and NestJS (TS) as proof of multi-language genericity. Add `pass_message_synthesis` (or extend `pass_idl_scan`) that emits ASYNC_CALLS edges keyed by `message_fqn` instead of requiring a topic literal. Extend `pass_cross_repo` Phase D to match by `message_fqn` (likely already covered by existing `match_typed_routes` with new property keys).

**Estimated LOC:** ~800 (registry loader, YAML schema, three framework definitions, new pass code, cross-repo extension, tests).

**Risk:** brittleness on framework-version drift (MassTransit v8 vs v7 have slightly different interface shapes). Mitigation: registry entries can be version-tagged; pattern matching tolerates shape variance.

**Dependencies on Tier 1:** Tier 1c (ctor param tracking) is a prerequisite — `_publishEndpoint` typing comes from DI injection.

### 8.2 Tier 3 — attribute-driven routes (after Tier 2)

**Scope:** extend `pass_route_nodes.c` to extract routes from interface-method attributes (Refit / Retrofit / Feign) on the producer side. Match against existing controller-side attribute extraction. Most attribute-driven controller patterns are already detected by cbm — this tier closes the producer-side gap.

**Estimated LOC:** ~400.

**Risk:** low. Attribute syntax is declarative and stable across framework versions.

**Dependencies on Tier 1:** Tier 1c/1f (var → interface mapping) is a prerequisite.

### 8.3 Tier 4 — config-resolved service discovery (after Tier 3)

**Scope:** extend `pass_envscan` to also parse `appsettings.json`, `application.yaml`, helm values, kustomize overlays. Build named-client → base-URL maps. Add light intra-method dataflow to resolve `path = $"/api/{x}"` patterns. Combine with named-client resolution to reconstruct full URLs.

**Estimated LOC:** ~1200. Largest tier — config parsing across multiple ecosystems is genuinely complex.

**Risk:** medium-high. Variable resolution can produce false positives; mitigation is confidence scoring on the emitted edges (high confidence when literal, lower when resolved through 2+ hops).

**Dependencies on Tier 1:** Tier 1e (DI registration scan) is a prerequisite for the C# `IHttpClientFactory` case.

### 8.4 Combined coverage estimate

After Tiers 1–3 land (Tier 4 is bonus), realistic recall on cross-service edges in modern strongly-typed codebases:

| Code style | Estimated recall |
|---|---|
| Go + gRPC + literal HTTP URLs | 95%+ (Tiers 1a+1b+1d) |
| Java/Spring + Feign + Cloud Stream | 90%+ (Tiers 1+2+3) |
| .NET / CQRS + MediatR + MassTransit + gRPC | 90%+ (Tiers 1c+2; HttpClient gap = Tier 4) |
| TypeScript / NestJS + microservices | 85%+ (Tiers 1+2+3 with decorator detection) |
| Python / FastAPI + Celery + httpx-codegen | 85%+ |
| Plain Python/Node with literal URLs (today's recall) | unchanged, still works |

---

## 9. Risks and mitigations

Updated with what's been learned from #293's validation.

| Risk | Likelihood | Mitigation |
|---|---|---|
| Tree-sitter pattern brittleness across language versions | Medium | YAML registry allows per-version patterns; tests cover N-1 and N versions of each framework |
| YAML registry becomes a maintenance burden | Medium | Limit official registry to top 10 frameworks per language; community contributions land via PR review with required test fixtures |
| False-positive cross-repo edges from name collisions | Low (validated) | Phase D filter — non-matching target lookups produce no edges. Tier 1b denylist mitigates the producer-side false positives. |
| Increased index time | Low | `pass_idl_scan` is conditional (no `.proto`/no `type_assigns` matches = no work). #293 measured <2% overhead in fleet test. |
| Variable URL resolution (Tier 4) produces wrong routes | Medium | Confidence scoring; only emit cross-repo edge if resolved confidence > 0.7; consumer-side validation catches bad matches |
| Reflection / runtime-resolved DI is impossible | High but acknowledged | Explicitly out of scope (Tier 5); document as known limitation |
| Maintainer-burden objection | Medium | Plugin registry shifts most additions to YAML; core C surface area kept minimal; Tier 1 sub-decomposition keeps individual PRs small |
| Patch size scares reviewers | High for big-bang, Low for tier-by-tier | #293 demonstrates the small-PR cadence; subsequent tiers reference #293's architecture |
| **(NEW) Producer detection misses modern OOP DI** | High (validated) | Tier 1c sub-tier addresses constructor injection — biggest leverage missing piece |
| **(NEW) Pre-existing `pass_parallel.c emit_grpc_edge` emits phantom Routes** | Validated | Filed as #294, out of scope for #293; needs separate cleanup PR |
| **(NEW) Multiple Route-QN namespaces coexist** | Low | `pass_idl_scan` uses `__route__grpc__`, `pass_parallel` uses `__grpc__`; they're parallel namespaces but Phase D matches each independently. Aligning them is a #294 follow-up. |

---

## 10. Open questions for the maintainer

1. **Pattern-registry format preference**: YAML, TOML, JSON, or compiled-in C tables with a build-time generator? YAML is most readable but adds a YAML parser to runtime; TOML or JSON minimize parser surface.
2. **Where should IDL files be discovered**: walk-the-repo by default, or require explicit `idl_paths` config? Walk-the-repo has zero-config UX cost but may pick up vendored proto files in `node_modules` or `vendor/`. **Note (post-#293):** the gbuf-walking approach in `pass_idl_scan` sidesteps this question — it works on whatever `.proto`s end up in the gbuf, which is governed by existing discovery rules.
3. **Cross-repo auto-trigger model**: store workspace membership in the per-repo artifact, or in a separate workspace-level artifact? Per-repo is simpler but duplicates state; workspace-level is cleaner but adds a new artifact kind.
4. **Confidence scoring**: should cross-repo edges carry a `confidence` property explicitly, or rely on the existing `properties` JSON blob? A first-class confidence field makes downstream consumers' job easier, especially for Tier 4.
5. **Existing pattern table migration**: should the 252 patterns in `service_patterns.c` migrate to the YAML registry as part of this work, or stay in C with the registry only handling new patterns? Recommendation: keep C patterns as-is for v1, migrate in a separate cleanup PR after the YAML schema is proven stable.
6. **Tier 4 dataflow scope**: how aggressive should intra-method variable resolution be? Single-assignment + string-concat is safe; following data through helper methods gets harder. Suggest single-method scope for v1.
7. **Test-fixture monorepo strategy**: ship the multi-language fixtures in the cbm repo, or reference an external `cbm-test-fixtures` repo to keep the main repo small? The fixtures total ~5MB across 3-4 languages — manageable in-tree. **Note (post-#293):** in-tree fixtures shipped at `testdata/cross-repo/grpc/` total ~10KB; well within budget.
8. **(NEW) Route QN namespace consolidation**: align `pass_parallel.c emit_grpc_edge`'s `__grpc__<svc>/<m>` to the canonical `__route__grpc__<svc>/<m>` used by `pass_idl_scan` and HTTP/async passes? Filed as #294.
9. **(NEW) Should Tier 1c land as part of #293 or as a separate PR?** Recommendation: separate PR (~400 LOC) to keep review scope tractable.

---

## 11. Why this is worth merging upstream

cbm's competitive position vs. Sourcegraph / Backstage / Apollo:

- **Sourcegraph** does cross-repo references but per-symbol, not protocol-aware. cbm + Tiers 1–3 would be the only AST-based tool emitting structured `CROSS_GRPC_CALLS` / `CROSS_ASYNC_CALLS` edges keyed by protocol identifiers.
- **Backstage** does service-graph from declarative IDL files but requires manual catalog upkeep. cbm + this proposal derives the service graph automatically from the same IDL files plus the consuming code.
- **Apollo Studio** does federated GraphQL via `@key` matching. cbm + this proposal generalizes the same idea to gRPC, OpenAPI, AsyncAPI, and typed-message ecosystems.

Position: **cbm becomes the only single-binary, AST+LSP-grade tool that derives a complete service interaction graph automatically from source.** That's a defensible product position.

The capability is asked for in every code-graph tool's roadmap (often as "service mesh visualization" or "API surface discovery"). cbm has the structural advantage to ship it first.

#293 demonstrates the technical feasibility on real-world code (validated against an 11-service .NET microservice fleet using NuGet-distributed contracts). The remaining sub-tiers are incremental extensions of the same architecture.

---

## 12. Appendix A — example YAML registry entries

Full registry entries for the ten frameworks Tier 2 should ship with:

```yaml
patterns:
  # ── C# / .NET ──────────────────────────────────────────────────
  - id: masstransit-publish
    languages: [csharp]
    kind: ASYNC_CALLS
    producer:
      match: { type_implements: "IPublishEndpoint", method: "Publish" }
      extract_id_from: generic_arg_or_first_arg_type
      id_kind: message_fqn
      broker: rabbitmq
    consumer:
      match: { class_implements: "IConsumer<T>" }
      extract_id_from: generic_type_arg
      id_kind: message_fqn

  - id: masstransit-send
    languages: [csharp]
    kind: ASYNC_CALLS
    producer:
      match: { type_implements: "ISendEndpoint", method: "Send" }
      extract_id_from: generic_arg_or_first_arg_type
      id_kind: message_fqn
      broker: rabbitmq
    consumer: { same_as: masstransit-publish.consumer }

  - id: nservicebus-publish
    languages: [csharp]
    kind: ASYNC_CALLS
    producer:
      match: { type_implements: "IMessageSession", method: "Publish" }
      extract_id_from: first_arg_type
      id_kind: message_fqn
    consumer:
      match: { class_implements: "IHandleMessages<T>" }
      extract_id_from: generic_type_arg
      id_kind: message_fqn

  # ── Java / Kotlin / Spring ─────────────────────────────────────
  - id: spring-cloud-stream-bridge
    languages: [java, kotlin]
    kind: ASYNC_CALLS
    producer:
      match: { type_implements: "StreamBridge", method: "send" }
      extract_id_from: first_arg
      id_kind: channel_name
    consumer:
      match: { method_annotation: "@StreamListener" }
      extract_id_from: annotation_value
      id_kind: channel_name

  - id: axon-command
    languages: [java, kotlin]
    kind: ASYNC_CALLS
    producer:
      match: { type_implements: "CommandGateway", method: "send" }
      extract_id_from: first_arg_type
      id_kind: message_fqn
    consumer:
      match: { method_annotation: "@CommandHandler" }
      extract_id_from: parameter_type
      id_kind: message_fqn

  # ── Node / TypeScript ──────────────────────────────────────────
  - id: nestjs-message-pattern
    languages: [typescript]
    kind: ASYNC_CALLS
    producer:
      match: { method_annotation: "@MessagePattern" }
      extract_id_from: annotation_value
      id_kind: message_pattern
    consumer:
      match: { method_annotation: "@MessagePattern" }
      extract_id_from: annotation_value
      id_kind: message_pattern

  - id: nestjs-event-pattern
    languages: [typescript]
    kind: ASYNC_CALLS
    producer:
      match: { method: "emit", type_implements: "ClientProxy" }
      extract_id_from: first_arg
      id_kind: event_pattern
    consumer:
      match: { method_annotation: "@EventPattern" }
      extract_id_from: annotation_value
      id_kind: event_pattern

  # ── Python ──────────────────────────────────────────────────────
  - id: faust-agent
    languages: [python]
    kind: ASYNC_CALLS
    producer:
      match: { method_call: "topic.send" }
      extract_id_from: receiver_var_topic_name
      id_kind: kafka_topic
    consumer:
      match: { decorator: "@app.agent" }
      extract_id_from: decorator_arg
      id_kind: kafka_topic

  # ── Go ──────────────────────────────────────────────────────────
  - id: watermill-publish
    languages: [go]
    kind: ASYNC_CALLS
    producer:
      match: { type_implements: "message.Publisher", method: "Publish" }
      extract_id_from: first_arg
      id_kind: topic_name
    consumer:
      match: { type_implements: "message.Subscriber", method: "Subscribe" }
      extract_id_from: first_arg
      id_kind: topic_name

  # ── Rust ───────────────────────────────────────────────────────
  - id: async-nats-publish
    languages: [rust]
    kind: ASYNC_CALLS
    producer:
      match: { method: "publish", type_implements: "Client" }
      extract_id_from: first_arg
      id_kind: nats_subject
    consumer:
      match: { method: "subscribe", type_implements: "Client" }
      extract_id_from: first_arg
      id_kind: nats_subject
```

Schema notes:
- `match` block defines the AST-pattern selector (interface implementation, attribute presence, method-call shape)
- `extract_id_from` names a strategy from a fixed enum (`generic_type_arg`, `first_arg`, `first_arg_type`, `annotation_value`, `attribute_arg`, `receiver_var_topic_name`, etc.)
- `id_kind` declares the namespace of the extracted identifier (so `kafka_topic` from one framework matches `kafka_topic` from another, but never matches `message_fqn`)
- `broker` is optional metadata that flows into the emitted edge

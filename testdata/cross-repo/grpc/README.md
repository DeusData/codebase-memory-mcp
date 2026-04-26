# Cross-repo gRPC fixtures

Reference fixtures for `pass_idl_scan` covering both producer (client) and consumer (server) sides of cross-repo gRPC matching.

## Layout

| Path | Role | What cbm should detect |
|---|---|---|
| `contracts/promo.proto` | Shared IDL | `PromoCodeService` + rpcs `GetVoucher`, `RedeemVoucher` extracted as Class+Function nodes; `pass_idl_scan` emits two `Route` nodes with QN `__route__grpc__PromoCodeService/<rpc>` plus `HANDLES` from each rpc back to its Route. |
| **Server side (consumer)** | | |
| `server-python/promo_server.py` | Python consumer | `PromoCodeServicer` inherits from generated `PromoCodeServiceServicer`. The `Servicer` suffix is stripped → service name `PromoCodeService`. Each method emits a `HANDLES` edge to the matching Route. |
| `server-csharp/PromoCodeService.cs` | .NET consumer | `PromoCodeServiceImpl` inherits from generated `PromoCodeServiceBase`. The `Base` suffix is stripped, `*Async` method suffixes are stripped before route lookup. |
| `server-java/PromoCodeServiceImpl.java` | Java consumer | `PromoCodeServiceImpl` extends `PromoCodeServiceImplBase`. The `ImplBase` suffix is stripped (matched before `Base` — longer-first ordering). |
| **Client side (producer)** | | |
| `client-python/promo_client.py` | Python producer | `stub = PromoCodeServiceStub(channel)` records `stub` as a typed-stub variable; `stub.GetVoucher(...)` calls emit `GRPC_CALLS` edges with `{service: "PromoCodeService", method: "GetVoucher"}` properties to the local Route node. |
| `client-csharp/PromoCodeClient.cs` | .NET producer | `_client = new PromoCodeServiceClient(channel)` records `_client` as a typed-stub variable; `_client.GetVoucherAsync(...)` calls have `Async` stripped before route lookup. |
| `client-java/PromoCodeClient.java` | Java producer | `stub = PromoCodeServiceGrpc.newBlockingStub(channel)` types `stub` as `PromoCodeServiceBlockingStub`; the `BlockingStub` suffix is stripped to derive `PromoCodeService`. Method calls use lowerCamelCase (`stub.getVoucher`) which is capitalized before route lookup. |

## How matching works end-to-end

After indexing the proto + a producer + a consumer:

1. **`pass_idl_scan`** emits Route nodes from `.proto`-derived service/rpc Class+Function pairs.
2. **Consumer side**: walks `INHERITS` edges; classes inheriting `*Servicer` / `*ImplBase` / `*ServiceBase` / `*Base` get `HANDLES` edges from their methods to the matching Routes.
3. **Producer side**: walks per-file `type_assigns` to find variables typed as `*Stub` / `*BlockingStub` / `*FutureStub` / `*Client` / `*AsyncClient` / `*AsyncStub`. For each `var.Method(...)` call on such a variable, emits a `GRPC_CALLS` edge from the caller to the Route.
4. **`pass_cross_repo`** Phase D matcher (already in `main`): for each `GRPC_CALLS` edge, looks up the Route's QN in target-project DBs; if found, follows the target's `HANDLES` edge to the impl method; emits `CROSS_GRPC_CALLS` bidirectionally.

## Why fixtures stay tiny

These are reference snippets, not buildable projects — no `Cargo.toml`, `pom.xml`, `requirements.txt`, `*.csproj`. Their job is to show the indexer realistic shapes (suffix patterns, inheritance, stub instantiation). Unit tests in `tests/test_pipeline.c` mirror these shapes with synthetic gbuf nodes + synthetic `CBMFileResult` extraction data so each rule is exercised in isolation.

## What v1 doesn't cover

- **Go grpc-go**: uses pointer types (`*PromoCodeClient`) and struct embedding (`UnimplementedPromoCodeServer`) instead of classical inheritance / class typing. Producer-side detection is feasible but needs additional plumbing in the call extractor; deferred to follow-up.
- **TypeScript `@grpc/grpc-js` dynamic clients**: stubs are generated at runtime from `.proto` rather than statically typed; out of scope for v1.
- **Generated stub source not indexed**: producer-side detection requires the producer repo to also index the `.proto` (vendored, submoduled, or inline). Repos that import only compiled stubs without source access fall through to ordinary `CALLS` edges.

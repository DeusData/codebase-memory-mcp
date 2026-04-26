# Cross-repo gRPC fixtures

Reference fixtures for `pass_idl_scan` (gRPC IDL Route + HANDLES emission).

## Layout

| Path | Role | What cbm should detect |
|---|---|---|
| `contracts/promo.proto` | Shared IDL | Service `PromoCodeService` + rpcs `GetVoucher`, `RedeemVoucher` extracted as Class+Function nodes; `pass_idl_scan` emits two `Route` nodes with QN `__route__grpc__PromoCodeService/<rpc>` plus `HANDLES` from each rpc back to its Route. |
| `server-python/promo_server.py` | Python consumer | `PromoCodeServicer` inherits from generated `PromoCodeServiceServicer` base. The "Servicer" suffix is stripped → service name `PromoCodeService`. Each method emits a `HANDLES` edge to the matching Route. |
| `server-csharp/PromoCodeService.cs` | .NET consumer | `PromoCodeServiceImpl` inherits from generated `PromoCodeServiceBase`. The "Base" suffix is stripped, `*Async` method suffixes are stripped before route lookup. |
| `server-java/PromoCodeServiceImpl.java` | Java consumer | `PromoCodeServiceImpl` extends `PromoCodeServiceImplBase`. The "ImplBase" suffix is stripped (matched before "Base" — longer-first ordering). |

## Producer side

Producer-side typed-client `GRPC_CALLS` emission is intentionally deferred to
follow-up Tier 1b. Once it lands, fixtures here will gain `client-csharp/`,
`client-go/`, `client-python/` directories whose calls into the generated stubs
get classified as `GRPC_CALLS` edges, completing the cross-repo round-trip via
the existing `pass_cross_repo.c` Phase D matcher (`match_typed_routes`).

## Why fixtures stay tiny

These are reference snippets, not buildable projects — no `Cargo.toml`,
`pom.xml`, `requirements.txt`, `*.csproj`. Their only job is to give the
indexer realistic class shapes (suffix names, inheritance) for the unit tests
in `tests/test_pipeline.c` to mirror.

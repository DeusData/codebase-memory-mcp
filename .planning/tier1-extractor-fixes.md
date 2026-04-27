# Tier 1 cross-repo gRPC: extractor + pipeline fixes

Companion to `cbm-cross-repo-proposal.md` and PR #293. Production-readiness gaps that prevented Tier 1's producer-side detection from firing on a real .NET microservice fleet, all addressed in this branch.

---

## Gap 1: `pass_idl_scan` not called in the parallel pipeline

**Where:** `src/pipeline/pipeline.c`

The pass was registered only in `seq_passes[]` (sequential path). The parallel path ran extract → registry → resolve → infra → free(cache) → k8s and never invoked `pass_idl_scan`. Repos cross the parallel-pipeline threshold at ~50 files, so production codebases silently skipped Tier 1.

**Fix:** invoke `cbm_pipeline_pass_idl_scan` in `run_parallel_pipeline` after `process_infra_bindings`, before the cache is freed, with `ctx->result_cache` set to the cache pointer.

**Diff:** `src/pipeline/pipeline.c` ~12 lines.

---

## Gap 2: C# 12 primary-constructor params not surfaced as Field defs

**Where:** `internal/cbm/extract_defs.c::extract_class_def`

Modern .NET 8+/9+ controllers/services use the C# 12 primary-constructor syntax. The params on the class declaration line bind to implicit captured fields accessible from instance members, but `extract_class_def` only walked body `field_declaration` / `property_declaration` nodes, missing the primary-ctor params entirely. Tier 1c (ctor params) and Tier 1f (class fields) couldn't fire because there was no Method "ctor" def with `param_names`/`param_types` and no Field def for the captured param.

**Fix:** after the existing class extraction, when `language == CBM_LANG_CSHARP`, locate the primary `parameter_list` (try `child_by_field_name("parameters")` first, fall back to direct child walk for grammars that don't surface the field name) and emit a `Field` def per param with `parent_class` and `return_type` set.

**Diff:** `internal/cbm/extract_defs.c` ~35 lines.

---

## Gap 3: protobuf rpc Functions not linked to their service Class

**Where:** `src/pipeline/pass_idl_scan.c::idl_proto_class_visitor`

The visitor used `cbm_gbuf_find_edges_by_source_type(class.id, "DEFINES_METHOD", ...)` to find rpc methods of each proto service. tree-sitter-protobuf emits rpc Functions as **flat siblings** of the service Class (not children), so `DEFINES_METHOD` returned empty for every proto Class and zero `__route__grpc__` Routes were created. `pass_route_nodes` did emit Routes but in the old `__grpc__<svc>/<method>` format, which doesn't match Tier 1's `__route__grpc__<svc>/<method>` consumer-side QN.

**Fix:** when `DEFINES_METHOD` is empty, fall back to scanning proto Functions in the same file whose `start_line`/`end_line` falls within the service Class's range. Optimized to O(N+F) via a single pre-pass that collects all proto Classes and Functions into flat arrays.

**Diff:** `src/pipeline/pass_idl_scan.c` ~85 lines (pre-collection helpers + refactored visitor).

---

## Gap 4: graph UI dropped `linked_projects` so cross-galaxy never rendered

**Where:** `graph-ui/src/components/GraphTab.tsx`

`/api/layout` returns `{nodes, edges, total_nodes, linked_projects}` where each linked-project entry carries the satellite's nodes + edges + `cross_edges` (primary→linked id pairs). `GraphScene` already knew how to render satellites, but `GraphTab` rebuilt `filteredData` as `{nodes, edges, total_nodes}` and silently dropped `linked_projects`, so the scene received `data.linked_projects === undefined` on every render.

**Fix:** pass `linked_projects` through the `useMemo` and apply the same enabled-labels / enabled-edge-types filter inside satellites. Filter init + `enableAll` union-in labels and edge types from satellites so they're visible by default. The binary embeds the built UI via `scripts/embed-frontend.sh`; rebuild with `scripts/build.sh --with-ui`.

**Diff:** `graph-ui/src/components/GraphTab.tsx` ~25 lines.

---

## Adversarial-review follow-ups

Three additional findings from `/codex:adversarial-review`. Two fixed in-line; the third (route-key uniqueness) is mitigated rather than fully resolved.

### Gap 5: incremental indexing skipped `pass_idl_scan`

**Where:** `src/pipeline/pipeline_incremental.c::run_extract_resolve`

Sequential incremental called `cbm_pipeline_pass_idl_scan` without attaching a cache, so the pass returned early at `if (!ctx->result_cache)`. Parallel incremental built a cache for extract+resolve but never called the pass. Producer-side edges only refreshed on full reindex.

**Fix:** mirror the full-pipeline pattern in both branches — allocate a `CBMFileResult **` cache, attach to `ctx->result_cache`, run the pass, free.

**Diff:** `src/pipeline/pipeline_incremental.c` ~25 lines.

### Gap 6: project-wide stub-var name-only fallback could misattribute calls

**Where:** `src/pipeline/pass_idl_scan.c::idl_stub_var_arr_find`

The lookup ran function-scope exact, then class-scope, then a name-only fallback. The fallback is safe for `file_vars` (one TU) but unsafe for `class_vars` (project-wide) — two unrelated classes with a `_client` field would silently bind to each other.

**Fix:** thread `allow_name_only_fallback` flag. The `class_vars` call site passes `false` (fail closed); `file_vars` lookups keep it `true`.

**Diff:** `src/pipeline/pass_idl_scan.c` ~20 lines.

### Gap 7 (mitigation): gRPC route-key collisions across proto packages

**Where:** `src/pipeline/pass_idl_scan.c::idl_emit_route_for_rpc`

Routes are keyed `__route__grpc__<service>/<method>` using the bare service name. Two `.proto` files in different proto packages with the same `service` + `rpc` names will upsert to the same Route node. A symmetric FQN fix needs both producer and consumer to derive the same fully-qualified key, which the consumer side can't do from typed-client class names alone.

**Mitigation:** log `idl_scan.route_collision` when an existing Route's `file_path` differs from the incoming emission, and write the proto Class's `qualified_name` as a `service_qn` Route property so a future FQN-aware matcher can recover provenance.

**Full fix path:** tracked as Tier 1g in `cbm-cross-repo-proposal.md` §5.7. The four-piece sequence (producer dual emission, AST-time package extraction, deterministic collision resolution, NuGet/Maven consumer-side scan) ships as a focused follow-on PR if/when a fleet hits an actual collision. Five rounds of `/codex:adversarial-review` on a 1g.1 prototype showed it's preventive defense for a scenario that didn't fire in real-world validation, and a half-shipped 1g.1 without consumer-side derivation produces dormant nodes.

**Diff:** `src/pipeline/pass_idl_scan.c` ~15 lines.

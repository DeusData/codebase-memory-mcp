# Feature Landscape

**Domain:** GDScript support proof for a code intelligence/indexing MCP
**Researched:** 2026-04-11

## Table Stakes

Features required to credibly prove GDScript support. Missing any of these means the milestone does not really demonstrate end-to-end usefulness on real Godot code.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Real repo `.gd` discovery and indexing | The proof has to show GDScript enters the normal parser-to-MCP pipeline, not a fixture-only lane | Medium | Must work on at least one real Godot 4.x repo and preferably support multiple repos per run |
| Non-zero GDScript class and method extraction | Basic code intelligence starts with finding definitions users can query | Medium | Evidence should include counts plus small samples, not just raw success logs |
| Queryable script-anchor model for top-level script members | GDScript files are script-centric; methods must resolve under a stable per-file anchor to be useful in search and tracing | High | This is the core contract behind reliable qualified names and same-file lookup |
| Same-script call resolution | A proof is weak if `foo()` inside a script cannot resolve to that script's own method definitions | High | Needed for practical call graphs and explicitly called out in design intent |
| Queryable inheritance validation (`extends`) | Inheritance is a core semantic relation in Godot scripts and part of the acceptance set | Medium | Repo-local/script-resolvable inheritance is in scope; built-in Godot engine bases can remain metadata only |
| Queryable `.gd` dependency/import validation (`preload` / `load`) | Real Godot projects often reference sibling scripts through resource paths; proving this is central to usefulness | High | Only resolvable `.gd` targets are required in v1; non-code assets should not become graph edges |
| Signal declaration and signal-call validation | Godot code intelligence without signals is not believable because signals are a primary communication pattern | High | v1 only needs conservative coverage for declarations plus resolvable `connect`, `emit`, and `emit_signal` forms |
| Sequential and parallel pipeline parity for core GDScript behavior | The brownfield codebase already has both paths; proof must show GDScript support survives the real production path | High | Parity is explicitly part of acceptance criteria |
| Reproducible proof workflow with isolated local state | Maintainers need a fast regression-check path, not one-off manual evidence | Medium | Scripted run, local-only artifacts, captured commands, per-repo summaries |
| Fixed query suite with machine-readable outputs | The proof must be reviewable and comparable across runs | Medium | Minimum suite: gd files, classes, methods, signal calls, inherits, imports, plus samples |

## Differentiators

Useful validations that strengthen confidence and make the proof more maintainable, but are not required for v1 acceptance.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Manifest-driven curated proof lane | Makes runs comparable over time by pinning exact repos, commits, and required categories | Medium | Great for “good-tier” regression tracking, but coarse coverage mode is enough for initial proof |
| Multi-repo aggregate coverage rules | Lets the proof pass even if no single demo repo covers all behaviors | Medium | Helpful because Godot demos may be narrow; not strictly required if one repo covers everything |
| Per-category gating assertions in summaries | Turns proof output from raw evidence into explicit pass/fail/incomplete signals | Medium | Valuable for maintainers and future automation |
| Repo metadata qualification (Godot version, pinned commit, label) | Prevents weak claims from unqualified or drifting demo repos | Low | Especially useful to distinguish confirmed Godot 4.x evidence from unknown-version runs |
| Additional edge-level samples beyond counts | Improves reviewability by showing actual signal-call, import, or inherit edges rather than only totals | Low | Nice-to-have for humans auditing correctness |
| Informational checks for non-required behaviors | Surfaces promising follow-up areas without making v1 fail | Low | Example: reporting string refs for non-code assets or incomplete signal patterns |
| Support for project subpaths within a larger upstream repo | Makes Godot demo monorepos easier to validate reproducibly | Medium | Useful for curated demo corpora, not core to proving GDScript support |

## Anti-Features

Things to deliberately not scope into this first milestone.

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Generic Godot app feature validation | The milestone is about code intelligence quality, not whether demo games function as apps | Validate indexing/query outcomes only |
| New graph schema or new signal-specific edge types | The design explicitly targets proof within the existing graph model | Encode signals using existing nodes/edges and deterministic naming |
| Non-GDScript language expansion | Dilutes the milestone and does not help prove the target capability | Keep focus on `.gd` parser-through-MCP behavior |
| Godot 3.x compatibility work | Out of scope in the design and adds version ambiguity | Require at least one confirmed Godot 4.x proof target |
| Asset-graph modeling for `.tscn`, `.tres`, textures, etc. | High scope growth with weak value for this milestone | Keep non-code asset refs as strings/metadata only |
| Broad engine semantic inference (scene tree, autoload heuristics, runtime behavior modeling) | Too speculative for a first proof and likely rewrite-prone | Prove definitions, calls, inheritance, signals, and `.gd` dependencies first |
| UI redesign or visualization work | Not needed to establish indexing correctness | Use existing MCP/query surfaces and concise markdown summaries |
| CI automation against external demo repos | Valuable later, but adds environment and availability complexity to v1 | Keep the first milestone local, scripted, and reproducible |

## Feature Dependencies

```text
Real `.gd` discovery/indexing
  → class/method extraction
  → script-anchor qualified naming
    → same-script call resolution
    → signal declaration/query stability
    → inheritance target resolution
    → `.gd` preload/load dependency resolution

Class/method extraction + semantic resolution
  → fixed proof query suite
  → per-repo summaries
  → aggregate acceptance decision

Isolated proof workspace
  → reproducible local validation
  → future regression re-checks
```

## MVP Recommendation

Prioritize:
1. **Real-project `.gd` discovery plus class/method indexing**
2. **Stable script-anchor naming with same-script call resolution**
3. **Validation of the three Godot-specific semantic categories: signals, `.gd` imports, and inheritance**
4. **A reproducible proof script with a fixed query suite and isolated local artifacts**

Defer: **Manifest-driven good-tier comparability** — high leverage for regression management, but not necessary to prove the base capability in the first milestone.

## Sources

- `.planning/PROJECT.md` — project goal, active requirements, and explicit out-of-scope boundaries. **Confidence: HIGH**
- `docs/superpowers/specs/2026-03-30-gdscript-support-design.md` — GDScript semantic scope, acceptance criteria, and deferred items. **Confidence: HIGH**
- `docs/superpowers/specs/2026-04-04-gdscript-real-project-proof-design.md` — proof workflow, required query suite, and aggregate acceptance semantics. **Confidence: HIGH**
- `docs/superpowers/proofs/gdscript-real-project-validation.md` — current runbook shape, manifest mode, artifact layout, and operator expectations. **Confidence: HIGH**

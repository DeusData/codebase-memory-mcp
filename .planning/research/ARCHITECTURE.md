# Architecture Patterns

**Domain:** Real-project GDScript support verification for codebase-memory-mcp
**Researched:** 2026-04-11

## Recommended Architecture

Treat this verification effort as a **proof pipeline layered on top of the existing product pipeline**, not as ad hoc test scripting. The architecture should have one narrow orchestration path:

**proof target selection → isolated proof runtime → local binary build → repository indexing → project ID resolution → fixed MCP query suite → evidence synthesis → aggregate acceptance decision**

The key architectural rule is: **verification code should prove the existing extraction/store/MCP stack, not reimplement it**. That means the proof automation should drive the same local binary and MCP CLI surface that users depend on, while keeping proof-only concerns (repo manifests, artifact directories, summaries, comparability checks) outside the core indexing/store code.

### Component Boundaries

| Component | Responsibility | Communicates With |
|-----------|---------------|-------------------|
| Proof target definition | Define which real Godot repos/subpaths are valid proof inputs, their labels, pinned commits, Godot version, and required coverage categories | Proof orchestrator, aggregate evaluator |
| Proof orchestrator (`scripts/gdscript-proof.sh`) | Own run lifecycle: parse args, create run root, build binary, iterate repos, invoke MCP CLI tools, capture outputs, set statuses | Proof target definition, isolated runtime state, local binary, summaries |
| Isolated proof runtime | Provide per-run and per-repo `HOME` / `XDG_CONFIG_HOME` / `XDG_CACHE_HOME` and local artifact storage so proof runs do not contaminate normal user state | Proof orchestrator, local binary |
| Product pipeline under test | Discover files, extract GDScript symbols/relations, run pipeline passes, persist graph data | Local binary, SQLite-backed store |
| Project resolution layer | Resolve the indexed repo to the concrete project ID from `list_projects` so later queries target the exact stored graph | Proof orchestrator, MCP CLI outputs |
| Query capture layer | Run the fixed `query_graph` suite and store one wrapper JSON file per query with literal query text plus raw result | Proof orchestrator, project resolution layer, store/query engine |
| Evidence synthesis layer | Convert raw query outputs and repo metadata into per-repo `summary.md` plus `aggregate-summary.md` | Query capture layer, proof target definition |
| Acceptance evaluator | Decide `pass` / `fail` / `incomplete` based on manifest or coarse coverage semantics; this is the only place that should make aggregate proof claims | Evidence synthesis layer |

### Data Flow

1. Operator supplies one or more real repo paths, optionally plus manifest-driven labels and Godot metadata.
2. Proof orchestrator creates a timestamped run root under `.artifacts/gdscript-proof/`.
3. Orchestrator builds the local `build/c/codebase-memory-mcp` binary from the worktree under test.
4. For each repo, orchestrator provisions isolated runtime state so indexing and queries use proof-local config/cache/store paths.
5. The local binary runs `index_repository`, which exercises the real discovery → extraction → pipeline pass → SQLite persistence path.
6. Orchestrator runs `list_projects` and resolves the stored project ID by matching the repo root path.
7. Orchestrator runs the fixed `query_graph` suite against that project ID, writing one wrapper JSON artifact per query.
8. Summary generation reads only `repo-meta.json` plus query wrapper files and produces a human-readable per-repo summary.
9. Aggregate evaluation combines repo-level outcomes into one acceptance result, preserving `incomplete` when the run is not comparable.

## Patterns to Follow

### Pattern 1: Black-box proof over the real product boundary
**What:** Drive verification through the built binary and MCP CLI tools instead of direct internal test hooks.
**When:** For end-to-end proof of discovery, extraction, indexing, storage, and query behavior.
**Example:** `gdscript-proof.sh` uses `index_repository`, `list_projects`, and `query_graph` via the CLI surface rather than calling internal functions directly.

### Pattern 2: Proof-only concerns live outside core indexing code
**What:** Keep manifest parsing, artifact slugs, summary rendering, and aggregate pass/fail logic in proof automation and docs.
**When:** Whenever the concern exists only to make validation reproducible or reviewable.
**Example:** `repo-meta.json`, query wrapper JSON, and `aggregate-summary.md` belong under `.artifacts/gdscript-proof/...`, not in `src/pipeline/` or `src/store/`.

### Pattern 3: Per-repo isolation inside a shared run
**What:** One run can cover multiple repos, but each repo gets its own local state subtree.
**When:** Always; this avoids cross-repo contamination and makes failures attributable.
**Example:** `state/<artifact-slug>/home|config|cache` plus `<artifact-slug>/queries/*.json`.

### Pattern 4: Fixed query contract before summary logic
**What:** Treat the query suite as the contract and make summaries consume those exact captured files.
**When:** For any proof claim about indexed GDScript behavior.
**Example:** `.gd` file/class/method counts, signal `CALLS`, `.gd` `IMPORTS`, `.gd` `INHERITS`, and supporting edge-detail queries.

### Pattern 5: Three-level outcome model
**What:** Distinguish `pass`, `fail`, and `incomplete` rather than collapsing all non-passes into failure.
**When:** Especially in manifest mode, where missing repos, pinned-commit mismatches, or query parse issues make runs non-comparable.
**Example:** A repo with successful indexing but missing query wrappers should be `incomplete`, not silently counted as a negative proof.

## Anti-Patterns to Avoid

### Anti-Pattern 1: Reaching around MCP/store with bespoke assertions
**What:** Validating extraction by reading internal temporary structures or adding proof-only internal APIs.
**Why bad:** It can “prove” parser behavior while missing persistence, project resolution, or MCP output regressions.
**Instead:** Keep proof on the binary/CLI path so repository discovery through query output is covered.

### Anti-Pattern 2: Shared operator state
**What:** Letting proof runs read/write the operator’s normal cache, config, or existing project database.
**Why bad:** Results become irreproducible and can hide bugs via stale data.
**Instead:** Force all runtime state under the run artifact root.

### Anti-Pattern 3: Summary-first architecture
**What:** Making Markdown summaries the source of truth.
**Why bad:** Human-readable summaries are lossy and hard to validate automatically.
**Instead:** Raw wrapper JSON is authoritative; summaries are derived views.

### Anti-Pattern 4: Monolithic “one repo must prove everything” coupling
**What:** Designing the proof flow so all required behaviors must appear in a single demo repo.
**Why bad:** It makes the proof fragile and blocks progress when coverage is naturally split across projects.
**Instead:** Support aggregate coverage across multiple pinned repos, with at least one confirmed Godot 4.x contributor.

## Suggested Build / Verification Order

1. **Lock the proof contract**
   - Freeze the required query suite, repo metadata fields, artifact layout, and pass/fail/incomplete semantics first.
   - This prevents later automation from baking in the wrong acceptance model.

2. **Stabilize proof runtime isolation**
   - Ensure per-run and per-repo state isolation is correct before investigating GDScript behavior.
   - Otherwise you cannot trust any later result.

3. **Verify repository indexing and project resolution**
   - Prove `index_repository` works on real repos and that `list_projects` resolves the exact stored project ID.
   - This is the handoff between extraction/persistence and query validation.

4. **Verify query capture at the MCP/store boundary**
   - Run the fixed `query_graph` suite and confirm wrapper JSON is complete and parseable.
   - Only after this step do you know the MCP output path is usable.

5. **Add evidence synthesis and aggregate evaluation**
   - Build per-repo and aggregate summaries on top of the raw artifacts.
   - Keep this downstream so it cannot mask indexing/query failures.

6. **Expand to curated multi-repo proof coverage**
   - Once the single-repo path is stable, enforce manifest/pinned-commit comparability and category coverage across the chosen Godot demos.

## Scalability Considerations

| Concern | At 1-2 repos | At 5-10 repos | At ongoing regression use |
|---------|--------------|---------------|----------------------------|
| Proof runtime isolation | Manual inspection is enough | Per-repo state separation becomes mandatory | Must remain deterministic and disposable |
| Query volume | Cheap | Still manageable with wrapper JSON | Keep fixed suite stable to preserve historical comparability |
| Reviewability | Human summary works well | Aggregate summary becomes necessary | Manifest-driven comparability is required |
| Failure diagnosis | Read one log | Need repo-local status + query-level failures | Preserve raw artifacts and explicit incomplete states |

## Sources

- `.planning/PROJECT.md` — HIGH confidence
- `.planning/codebase/ARCHITECTURE.md` — HIGH confidence
- `.planning/codebase/STRUCTURE.md` — HIGH confidence
- `docs/superpowers/specs/2026-04-04-gdscript-real-project-proof-design.md` — HIGH confidence
- `docs/superpowers/proofs/gdscript-real-project-validation.md` — HIGH confidence
- `scripts/gdscript-proof.sh` — HIGH confidence
- `codemap.md` — HIGH confidence

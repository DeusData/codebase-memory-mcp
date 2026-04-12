# Technology Stack

**Project:** GDScript support verification for codebase-memory-mcp using real Godot demo projects
**Researched:** 2026-04-11

## Recommended Stack

This is **verification infrastructure for an existing native indexer**, not a new product stack. Stay inside the repository's current architecture: **C11 + Tree-sitter + SQLite + MCP**, with shell/Python automation around it. Validate against **real Godot 4.x repos**, but do not introduce a second parser, a second graph store, or editor-driven test harnesses.

### Core Framework
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| C11 native engine (`codebase-memory-mcp`) | Existing repo baseline | System under test | The whole proof goal is parser-through-MCP validation of the existing binary, not reimplementing behavior in another runtime. This fits the repo's composition root, extraction pipeline, and MCP server in `src/main.c`, `internal/cbm/`, and `src/mcp/mcp.c`. **Confidence: HIGH** |
| Tree-sitter runtime | 0.24.4 (repo-pinned) | Parse GDScript and produce extraction input | The repository already vendors Tree-sitter 0.24.4 and routes extraction through `internal/cbm/`. Tree-sitter remains the right parser layer because the architecture already depends on language grammars, `CBMLangSpec`, and pass-based extraction. **Confidence: HIGH** |
| Vendored GDScript grammar shim | Existing `tree-sitter-gdscript` integration under `internal/cbm/grammar_gdscript.c` | Godot 4.x syntax coverage in the native parser pipeline | Godot docs explicitly position GDScript grammar as relevant to third-party tooling, and this repo already chose vendored grammar wiring. Keep validation focused on whether the grammar-backed extractor survives real repos. **Confidence: HIGH** |
| MCP JSON-RPC tool surface | Existing repo baseline | End-to-end acceptance surface | Acceptance is not "parser can parse"; it is "index_repository`, `list_projects`, and `query_graph` return useful real-project answers." Use the MCP tool surface as the contract under test. **Confidence: HIGH** |

### Database
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| SQLite | 3.49.1 (repo-pinned) | Durable graph store for proof runs | The architecture already persists all graph nodes/edges through SQLite. Proof runs should validate what the real store persists and what MCP can query back, not a mocked in-memory graph. **Confidence: HIGH** |
| Local JSON artifact wrappers | Existing script format | Persist raw evidence per query | The proof workflow already writes one JSON wrapper per query under `.artifacts/gdscript-proof/`. This is the right review artifact because it is machine-checkable, easy to diff locally, and decoupled from raw DB internals. **Confidence: HIGH** |

### Infrastructure
| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| GNU Make via `Makefile.cbm` | Existing repo baseline | Canonical native build/test entrypoint | The repo already standardizes builds here. Reuse it so proof runs exercise the exact binary layout the project ships. **Confidence: HIGH** |
| Bash proof orchestration (`scripts/gdscript-proof.sh`) | Existing repo baseline | Build, isolate state, index repos, capture evidence | This script already implements the correct shape for brownfield validation: timestamped artifact roots, isolated per-repo HOME/XDG state, MCP CLI invocation, and aggregate summaries. Extend this rather than replacing it. **Confidence: HIGH** |
| Python 3 helper scripts | Existing repo baseline | Deterministic JSON/assertion logic around proof runs | The repo already uses Python for focused proof/regression tasks. Python is appropriate here for manifest parsing, assertion evaluation, and fixture repo generation without contaminating the core product runtime. **Confidence: HIGH** |
| Git + pinned commits | Current practice | Reproducible corpus selection | Validation must be reproducible. The existing good-tier manifest pins commits and subpaths; keep that pattern so proof expectations stay stable even as upstream demos evolve. **Confidence: HIGH** |
| Official Godot demo corpus (`godotengine/godot-demo-projects`) | Current upstream repo | Primary real-world validation corpus | This is the canonical public Godot sample corpus, organized as real projects with `project.godot` roots and explicit branch/version guidance. Use it as the primary target before reaching for random GitHub repos. **Confidence: HIGH** |
| Manifest-driven golden set (`docs/superpowers/proofs/gdscript-good-tier-manifest.json`) | Existing repo baseline | Curated repo/subpath/assertion contract | For this repo, the right 2025-2026 validation pattern is not broad fuzzy crawling; it is a curated, pinned, multi-repo acceptance set that covers indexing, signals, imports, inheritance, and same-script calls. **Confidence: HIGH** |
| GitHub Actions | Existing repo baseline | Optional CI smoke for repo-owned checks | Use Actions for build/test/proof harness checks that do not depend on downloading large external corpora in CI. Keep full real-repo proof primarily local/manual unless a stable cache strategy is added later. **Confidence: MEDIUM** |

### Supporting Libraries
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| yyjson | 0.10.0 (repo-pinned) | MCP/config JSON parsing in the native binary | Keep for product-side JSON handling; do not add another C JSON library just for proof work. **Confidence: HIGH** |
| `git` CLI | Current system tool | Resolve repo metadata and pinned revisions | Use during proof runs to record commit/branch metadata for each target repo. Already aligned with the proof script. **Confidence: HIGH** |
| `python3` stdlib (`json`, `subprocess`, `tempfile`) | Current system tool | Manifest/assertion processing and regression fixtures | Use for proof-side automation only; keep all product behavior in C. **Confidence: HIGH** |
| Godot version metadata from manifest/`project.godot` | Current practice | Qualification of 4.x proof targets | Track Godot version as proof metadata. Use manifest-declared versions first; parse `project.godot` only as a supporting signal, not as the sole authority. **Confidence: MEDIUM** |

## Validation Infrastructure to Keep or Add

### Keep
- **Vendored grammar + native passes**: validate the existing extraction stack, not a shadow parser.
- **`scripts/gdscript-proof.sh`** as the top-level operator command.
- **Manifest-driven assertions** for pinned repos, pinned commits, and exact expected counts/edges.
- **Isolated per-run state** under `.artifacts/gdscript-proof/` with `HOME`, `XDG_CONFIG_HOME`, and `XDG_CACHE_HOME` redirected per repo.
- **Per-query JSON wrappers** plus human-readable summaries.
- **Small Python regression scripts** for edge cases the manifest should protect, like same-script calls.

### Add or tighten
- **One explicit corpus lane for official Godot demos** and **one supplemental external repo lane** only when official demos do not cover a behavior.
- **Stable project-subpath support** for monorepos/corpus repos like `godot-demo-projects` so each proof target is a real project, not the whole umbrella checkout.
- **Version qualification discipline**: only count repos toward final acceptance when confirmed as Godot 4.x.
- **Parallel/sequential parity checks** where possible, because the design spec explicitly requires parity for GDScript resolution behavior.
- **A narrow CI lane** that validates the proof harness itself (manifest parsing, wrapper generation, local fixture proof), while leaving heavyweight real-repo proof as an operator workflow.

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| Corpus selection | Official `godotengine/godot-demo-projects` + pinned external supplemental repo | Random GitHub search over many community projects | Too noisy and non-reproducible for acceptance gating. Use curated pinned repos instead. |
| Parser validation | Existing Tree-sitter GDScript integration | Godot editor/LSP as the primary parser oracle | Useful as occasional spot-checking, but wrong as the main stack: it changes the contract from "our parser-through-MCP path works" to "some other parser works." |
| Persistence/query proof | SQLite + MCP queries | Direct SQLite-only assertions | Too low-level for acceptance. The product promise is MCP usefulness, so query through the tool surface and use SQLite only as the underlying store. |
| Automation language | Bash + focused Python helpers | Rewriting proof orchestration in Node/TypeScript | Adds an unnecessary runtime to a native proof workflow and duplicates working brownfield tooling. |
| Test corpus storage | Local ignored artifacts + external repos by path | Vendoring full Godot demo repos into this repository | Bloats the repo and creates update churn. Keep proof inputs external and pinned by manifest. |
| Verification scope | Real-project end-to-end proof plus a few targeted regressions | Large UI/browser automation suite | The UI is not the acceptance surface for this milestone. Browser automation would add cost without improving parser-through-MCP confidence proportionally. |

## What NOT to Introduce

- **No new database** (Postgres, DuckDB, Neo4j): SQLite is the shipped graph store and the thing that must be proven.
- **No alternate parsing stack** (LSP-based extraction, editor plugins, ANTLR rewrite): the repo already standardized on Tree-sitter in C.
- **No Docker-first validation requirement** unless later needed for CI portability: local path-based repo validation is already the intended operator workflow.
- **No full Godot editor runtime automation** as a dependency for proof: this milestone validates static indexing/query behavior, not engine execution.
- **No TypeScript/Node proof framework rewrite**: Node is already needed for the optional UI build, but not for the core validation harness.
- **No broad schema expansion** just to make GDScript look more "native": the existing specs explicitly constrain this work to current node/edge types.

## Installation

```bash
# Build the native binary under test
make -f Makefile.cbm cbm

# Run the repo-owned proof harness against pinned real projects
bash scripts/gdscript-proof.sh \
  --manifest docs/superpowers/proofs/gdscript-good-tier-manifest.json \
  --repo "/absolute/path/to/godot-demo-projects/3d/squash_the_creeps" \
  --label "/absolute/path/to/godot-demo-projects/3d/squash_the_creeps=squash-the-creeps" \
  --repo "/absolute/path/to/godot-demo-projects/networking/webrtc_signaling" \
  --label "/absolute/path/to/godot-demo-projects/networking/webrtc_signaling=webrtc-signaling"

# Run targeted proof-harness regression tests
python3 scripts/test_gdscript_proof_same_script_calls.py
```

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Native validation stack fit | HIGH | Strong repo evidence from architecture, stack docs, proof script, and specs. |
| Corpus recommendation | HIGH | Official Godot demo repo is authoritative and already used by this repo's manifest. |
| Exact future CI shape | MEDIUM | Repo evidence supports a light CI lane, but full external-repo CI remains explicitly deferred. |
| Godot-version qualification workflow | MEDIUM | Manifest-based metadata is solid; automatic version detection remains intentionally limited. |

## Sources

- Repo architecture: `.planning/codebase/ARCHITECTURE.md` — HIGH confidence
- Repo stack: `.planning/codebase/STACK.md` — HIGH confidence
- Project scope: `.planning/PROJECT.md` — HIGH confidence
- GDScript design spec: `docs/superpowers/specs/2026-03-30-gdscript-support-design.md` — HIGH confidence
- Real-project proof design: `docs/superpowers/specs/2026-04-04-gdscript-real-project-proof-design.md` — HIGH confidence
- Existing proof runbook: `docs/superpowers/proofs/gdscript-real-project-validation.md` — HIGH confidence
- Existing proof manifest: `docs/superpowers/proofs/gdscript-good-tier-manifest.json` — HIGH confidence
- Existing automation: `scripts/gdscript-proof.sh`, `scripts/test_gdscript_proof_same_script_calls.py` — HIGH confidence
- Godot docs, GDScript landing page: https://docs.godotengine.org/en/stable/tutorials/scripting/gdscript/index.html — HIGH confidence
- Godot docs, GDScript grammar reference: https://docs.godotengine.org/en/stable/engine_details/file_formats/gdscript_grammar.html — MEDIUM confidence (official, but page itself warns it may be outdated for 4.6)
- Official Godot demo corpus: https://github.com/godotengine/godot-demo-projects — HIGH confidence
- Tree-sitter parser usage guide: https://tree-sitter.github.io/tree-sitter/using-parsers — HIGH confidence

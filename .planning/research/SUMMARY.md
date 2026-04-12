# Project Research Summary

**Project:** GDScript support verification for codebase-memory-mcp using real Godot demo projects
**Domain:** Brownfield verification infrastructure for GDScript indexing/query support
**Researched:** 2026-04-11
**Confidence:** HIGH

## Executive Summary

This initiative is not a greenfield product build; it is a brownfield verification effort for an existing C11 + Tree-sitter + SQLite + MCP code-intelligence stack. The strongest recommendation across the research is to prove GDScript support through the shipped binary and MCP surface on real Godot 4.x repositories, not through fixtures, editor automation, or alternate parsers. Experts would treat this as a black-box proof pipeline layered on top of the current product architecture: build the native binary, index pinned real repos, run a fixed query suite, capture raw JSON evidence, then synthesize aggregate pass/fail/incomplete results.

The roadmap should therefore start by locking the proof contract and curated corpus, then harden runtime isolation and query correctness, and only then scale to aggregate real-repo acceptance. The core stack is already sufficient: keep the vendored GDScript grammar, existing pipeline passes, SQLite persistence, MCP JSON-RPC tools, Bash orchestration, and small Python helpers. Table stakes are real `.gd` discovery, class/method extraction, stable script-anchor naming, same-script call resolution, queryable inheritance/import/signal validation, and parity across sequential and parallel paths.

The biggest risks are false confidence and non-reproducible evidence. The research repeatedly warns against proving only fixtures, trusting the harness more than the graph, contaminating runs with personal state, and overclaiming semantics beyond the verified v1 contract. Mitigation is clear: use pinned multi-repo Godot 4.x targets, isolate all proof state under `.artifacts/gdscript-proof/`, preserve raw query wrappers as the source of truth, require pass/fail/incomplete semantics, and document scope limits honestly.

## Key Findings

### Recommended Stack

The recommended stack is the existing repository stack, not a replacement. Verification should stay entirely inside the current native pipeline and use the same acceptance boundary users rely on: local binary build, SQLite-backed persistence, and MCP queries against indexed real repos.

**Core technologies:**
- **C11 native engine (`codebase-memory-mcp`)**: system under test — proves the shipped binary instead of shadow implementations.
- **Tree-sitter 0.24.4**: parsing runtime — already repo-pinned and integrated into `internal/cbm/`.
- **Vendored GDScript grammar shim**: Godot 4.x syntax coverage — validates real grammar-backed extraction already wired into the product.
- **SQLite 3.49.1**: graph persistence — acceptance must reflect what the real store persists and MCP can query.
- **MCP JSON-RPC tools**: end-to-end contract — `index_repository`, `list_projects`, and `query_graph` are the proof surface.
- **`Makefile.cbm` + `scripts/gdscript-proof.sh` + focused Python 3 helpers**: build and orchestration — reuse the existing brownfield verification path.

**Critical version requirements:**
- Tree-sitter **0.24.4** (repo-pinned)
- SQLite **3.49.1** (repo-pinned)
- Proof targets must be confirmed **Godot 4.x** to count toward final acceptance

### Expected Features

For this milestone, “features” are really proof capabilities. The table-stakes scope is narrow but strict: show that real GDScript projects survive the normal parser-to-store-to-MCP path and return useful graph answers for the semantics users actually care about.

**Must have (table stakes):**
- Real repo `.gd` discovery and indexing
- Non-zero class and method extraction with reviewable samples
- Stable script-anchor model for top-level script members
- Same-script call resolution
- Queryable `extends` inheritance validation
- Queryable `.gd` `preload` / `load` dependency validation
- Signal declaration plus conservative signal-call validation
- Sequential/parallel pipeline parity for core GDScript behavior
- Reproducible proof workflow with isolated local state
- Fixed query suite with machine-readable outputs

**Should have (differentiators):**
- Manifest-driven curated proof lane with pinned repos and commits
- Multi-repo aggregate coverage rules
- Per-category gating assertions in summaries
- Repo metadata qualification such as Godot version, label, and commit
- Extra edge-level samples for reviewer confidence

**Defer (v2+ / out of scope):**
- Godot app/runtime validation
- New graph schema or signal-specific edge types
- Non-GDScript language expansion
- Godot 3.x compatibility work
- Asset graph modeling for scenes/resources/textures
- Broad engine semantic inference or UI redesign
- External-repo CI automation as a hard requirement

### Architecture Approach

The recommended architecture is a proof pipeline layered over the existing product pipeline: **proof target selection → isolated proof runtime → local binary build → repository indexing → project ID resolution → fixed MCP query suite → evidence synthesis → aggregate acceptance decision**. Keep proof-only concerns outside core indexing code, treat wrapper JSON as authoritative, isolate each repo within a shared run, and preserve a three-state outcome model (`pass`, `fail`, `incomplete`).

**Major components:**
1. **Proof target definition** — pins repo/subpath/label/version/coverage expectations.
2. **Proof orchestrator (`scripts/gdscript-proof.sh`)** — owns run lifecycle, binary invocation, and artifact capture.
3. **Isolated runtime state** — redirects HOME/XDG/cache/store paths per run and repo.
4. **Product pipeline under test** — performs discovery, extraction, pipeline passes, and SQLite persistence.
5. **Project resolution + query capture** — resolves project IDs and runs the fixed MCP query suite into raw wrapper JSON.
6. **Evidence synthesis + acceptance evaluator** — produces per-repo and aggregate summaries with explicit pass/fail/incomplete semantics.

### Critical Pitfalls

1. **Proving the fixture, not the language support** — avoid one-repo or fixture-only evidence; require curated real Godot 4.x coverage mapped to explicit behaviors.
2. **Trusting the proof harness more than the graph** — harden the harness with query-level regressions and edge-level assertions, because the harness itself can mis-measure support.
3. **Losing comparability across real repo layouts** — pin commits, require labels, and record `project_subpath` for shared upstream corpora like `godot-demo-projects`.
4. **Contaminating proof runs with personal MCP state** — force all state under `.artifacts/gdscript-proof/` and record exact binary/worktree metadata.
5. **Overclaiming beyond the v1 contract** — validate only Godot 4.x, repo-local `.gd` imports/inheritance, and conservative signals; do not claim full Godot semantics.

## Implications for Roadmap

Based on research, suggested phase structure:

### Phase 1: Proof Contract and Corpus Definition
**Rationale:** Dependencies start here; without a fixed contract and curated targets, every later proof result is arguable.
**Delivers:** Required query suite, artifact layout, pass/fail/incomplete semantics, pinned Godot 4.x repos, labels, commits, and `project_subpath` rules.
**Addresses:** Reproducible proof workflow, fixed query suite, real-repo indexing scope, multi-repo coverage setup.
**Avoids:** Proving one happy-path repo, losing comparability, and counting unknown-version repos as acceptance evidence.

### Phase 2: Harness Hardening and Runtime Isolation
**Rationale:** The harness must be trustworthy before it can validate product behavior.
**Delivers:** Stable `gdscript-proof.sh` execution, isolated HOME/XDG/cache/store paths, raw wrapper JSON capture, and narrow regressions for known query-shape risks.
**Uses:** Existing Bash orchestration, Python helpers, SQLite store, and MCP CLI surface.
**Implements:** Isolated proof runtime, query capture layer, and proof-only artifact generation.
**Avoids:** Personal-state contamination and false positives/negatives from broken proof queries.

### Phase 3: Core Semantic Verification on Real Repos
**Rationale:** Once the contract and harness are stable, verify the actual table-stakes semantics in dependency order.
**Delivers:** Evidence for `.gd` discovery, class/method extraction, script-anchor naming, same-script calls, inheritance, `.gd` imports, signals, and sequential/parallel parity.
**Addresses:** All core table-stakes from FEATURES.md.
**Avoids:** Interpreting “no evidence” as failure or success, and shipping support claims from incomplete runs.

### Phase 4: Aggregate Acceptance and Promotion Guardrails
**Rationale:** Final acceptance should synthesize repo-level evidence into an honest promotion decision, not a pile of raw runs.
**Delivers:** Aggregate summaries, per-category gating, manifest-grade comparability, scoped promotion language, and regression lock-in for any proof misses found.
**Addresses:** Differentiators around maintainability and long-term regression checking.
**Avoids:** Overclaiming semantics, fragile acceptance logic, and repeated proof regressions.

### Phase Ordering Rationale

- Contract first, because architecture and pitfalls both show that proof validity depends on pinned targets, fixed queries, and explicit outcome semantics.
- Harness second, because broken isolation or broken queries invalidate all later semantic evidence.
- Semantic verification third, because features depend on stable indexing, project resolution, and trustworthy query capture.
- Aggregate acceptance last, because summaries and promotion gates should be derived from validated raw evidence rather than acting as the source of truth.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 1:** Curated corpus selection and per-repo behavior ownership may need a fresh check against current Godot demo coverage.
- **Phase 3:** Sequential vs parallel parity and signal/import edge cases may require targeted follow-up if real repos expose gaps not covered by the current manifest.

Phases with standard patterns (skip research-phase):
- **Phase 2:** Runtime isolation, artifact capture, and proof-script hardening follow well-understood repo-local patterns already documented in the proof script and architecture notes.
- **Phase 4:** Summary generation and aggregate evaluation are straightforward once the contract and raw evidence format are fixed.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | Strong repo-local evidence and explicit pinning in STACK.md; recommendation is to stay on the shipped stack. |
| Features | HIGH | Scope is tightly defined by design specs and proof docs, with clear v1 boundaries and dependencies. |
| Architecture | HIGH | Architecture guidance is concrete, repo-specific, and aligned with existing scripts and codemap structure. |
| Pitfalls | HIGH | Pitfalls are grounded in this repo’s actual proof history, including a documented same-script-call false negative. |

**Overall confidence:** HIGH

### Gaps to Address

- **Corpus sufficiency:** Confirm the curated Godot 4.x repo set actually covers all required behaviors without leaning on one demo project.
- **Parity evidence depth:** Decide how much sequential/parallel parity evidence is required for acceptance versus informational reporting.
- **Godot version qualification:** Use manifest-declared versions as the primary source and define how unknown-version repos are excluded or labeled.
- **CI scope:** Keep heavyweight external-repo proof local for now, but decide whether a lightweight harness smoke test belongs in CI during planning.

## Sources

### Primary (HIGH confidence)
- `.planning/research/STACK.md` — stack recommendation, pinned versions, and tooling constraints
- `.planning/research/FEATURES.md` — table stakes, differentiators, anti-features, and dependency ordering
- `.planning/research/ARCHITECTURE.md` — proof-pipeline architecture, component boundaries, and phase order
- `.planning/research/PITFALLS.md` — failure modes, prevention strategies, and promotion guardrails
- `codemap.md` — repository composition root and entry-point confirmation
- `docs/superpowers/specs/2026-03-30-gdscript-support-design.md` — v1 semantic contract
- `docs/superpowers/specs/2026-04-04-gdscript-real-project-proof-design.md` — proof workflow and acceptance semantics
- `docs/superpowers/proofs/gdscript-real-project-validation.md` — runbook and operator workflow
- `docs/superpowers/proofs/gdscript-good-tier-manifest.json` — curated proof target contract
- `scripts/gdscript-proof.sh` — actual orchestration, query suite, and artifact model

### Secondary (MEDIUM confidence)
- Godot GDScript grammar reference — official but version-drift caveat noted in STACK.md
- Godot version metadata inferred from `project.godot` — useful supporting signal, but not the sole authority
- GitHub Actions as future-proofing for lightweight harness checks — recommended cautiously, not required for v1

### Tertiary (LOW confidence)
- None identified; the key roadmap decisions are backed by repo-local materials and official Godot sources.

---
*Research completed: 2026-04-11*
*Ready for roadmap: yes*

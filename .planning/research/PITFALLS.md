# Domain Pitfalls

**Domain:** GDScript support verification for codebase-memory-mcp on real Godot projects
**Researched:** 2026-04-11
**Overall confidence:** HIGH

## Critical Pitfalls

Mistakes here create false confidence and lead to premature “GDScript is done” claims.

### Pitfall 1: Proving the fixture, not the language support
**What goes wrong:** Validation passes on tiny fixtures or one friendly repo, but real Godot repos still fail or only partially index.
**Why it happens:** The extractor can look healthy on synthetic inputs while real repos expose repo-layout issues, multi-project coverage gaps, or missing semantic patterns.
**Consequences:** README/promotion claims get ahead of reality; brownfield follow-up work becomes bug triage instead of proof.
**Warning signs:**
- Evidence comes from fixtures or one repo only
- Non-zero `.gd` files/classes/methods are treated as sufficient proof
- One repo is assumed to cover signals, inherits, imports, and same-script calls without explicit assertions
**Prevention:** Require manifest-driven proof across curated real repos, with each repo mapped to required behaviors and pinned commits. Treat “coverage split across multiple repos” as normal, not as a failure of the proof design.
**Detection:** Aggregate proof is missing categories, or a repo summary says complete while key behavior buckets are still unproven.
**Phase:** Phase 1 — Proof target curation and acceptance contract

### Pitfall 2: Trusting the proof harness more than the graph
**What goes wrong:** The proof script/query layer mis-measures support, producing false negatives or false positives even when the indexer behavior changed correctly or incorrectly.
**Why it happens:** Validation logic is itself software. This repo already had a closed miss where `gd-same-script-calls` was querying the wrong target shape and undercounting valid GDScript method calls.
**Consequences:** Teams chase the wrong bug, or worse, ship support claims backed by a broken proof harness.
**Warning signs:**
- Raw edge data and summary conclusions disagree
- A “fix” changes proof output without touching extraction/resolution
- Assertions are count-only, with no edge/sample checks
**Prevention:** Treat the harness as first-class product code: add narrow regression tests for proof queries, store raw JSON per query, and verify counts with edge-level assertions for calls/imports/inherits.
**Detection:** Fresh run artifacts show suspicious zeroes or mismatches between `*-edges.json` and summary tables.
**Phase:** Phase 2 — Proof harness hardening

### Pitfall 3: Losing comparability across real Godot repo layouts
**What goes wrong:** Results cannot be compared or reproduced because the proof run drifted to different commits, wrong subprojects, or ambiguous repo identities.
**Why it happens:** Godot demo sources can share one upstream remote while proof targets live in different subpaths; labels, pinned commits, and `project_subpath` become part of the correctness contract.
**Consequences:** A passing run is not reproducible; failures cannot be tied to a specific target; roadmap decisions are based on moving evidence.
**Warning signs:**
- Manifest-mode runs report `incomplete`
- Pinned commit mismatch or `project_subpath mismatch`
- Multiple proof targets come from one upstream repo but are treated as one logical project
**Prevention:** Use manifest mode for promotion-quality proof, require labels for every repo, pin commits, and record `project_subpath` for shared-remotes like `godot-demo-projects`.
**Detection:** Aggregate summary contains comparability issues, missing manifest labels, duplicate run labels, or missing required repos.
**Phase:** Phase 1 — Proof target curation and acceptance contract

### Pitfall 4: Contaminating proof runs with personal MCP state
**What goes wrong:** The proof succeeds because of leftover local cache/config/store state rather than the current branch under test.
**Why it happens:** Indexing tools often default to user-global state. Without explicit isolation, brownfield runs can silently reuse old projects or config.
**Consequences:** The team cannot trust pass/fail results; regressions may be masked; branch-specific behavior is unclear.
**Warning signs:**
- Proof results differ depending on the operator machine
- Re-running without cleanup changes outcomes unexpectedly
- Runtime state lives outside `.artifacts/gdscript-proof/`
**Prevention:** Force per-run isolated `HOME`, `XDG_CONFIG_HOME`, `XDG_CACHE_HOME`, and store roots under the artifact directory; log exact binary/worktree/commit used.
**Detection:** `env.txt` or run artifacts do not fully capture state roots, or summaries cannot attribute results to a single branch/commit.
**Phase:** Phase 2 — Proof harness hardening

### Pitfall 5: Treating “no evidence” as “feature unsupported” or “support complete”
**What goes wrong:** Empty counts are interpreted too strongly in either direction. Some repos simply do not exercise a behavior; others are incomplete and should not contribute to acceptance.
**Why it happens:** Real-project validation mixes two questions: “did indexing complete?” and “does this repo contain the pattern?” Without explicit `pass`/`fail`/`incomplete`, the proof lies.
**Consequences:** The roadmap either over-fixes non-bugs or ships unsupported claims from non-comparable inputs.
**Warning signs:**
- Unknown Godot version repos counted toward final acceptance
- Query failures counted the same as zero-result queries
- Aggregate logic uses only boolean coverage with no incompleteness handling
**Prevention:** Preserve three states—`pass`, `fail`, `incomplete`—and require complete query capture before a repo can contribute. Godot version must be confirmed for qualifying acceptance, not assumed.
**Detection:** Repo summaries show incomplete indexing/query stages, missing query wrappers, or unknown version but are still cited as proof.
**Phase:** Phase 3 — Real-repo execution and evidence triage

### Pitfall 6: Overclaiming semantics beyond the verified v1 contract
**What goes wrong:** Validation claims full “Godot support” when the implemented contract is narrower: Godot 4.x only, conservative signal resolution, repo-local `.gd` imports, repo-local inheritance, and no asset graph or Godot 3.x guarantees.
**Why it happens:** GDScript/Godot repositories contain dynamic paths, built-in engine base classes, scenes/resources, and signal patterns that cannot all be resolved statically in v1.
**Consequences:** Users hit obvious gaps immediately after promotion and perceive the language support as broken, not scoped.
**Warning signs:**
- README/roadmap language says “done” without naming Godot 4.x and v1 limits
- Dynamic `load()`/`preload()` or non-code assets are expected to produce graph edges
- Built-in base classes like `Node` are expected to appear as resolved graph nodes
**Prevention:** Validate only the explicit v1 acceptance set and document deferred areas honestly: no new signal edge types, no synthetic engine-class graph, no non-code asset edges, no Godot 3.x guarantee.
**Detection:** Proof pass exists, but reported misses cluster around dynamic paths, engine classes, scenes/resources, or unsupported Godot versions.
**Phase:** Phase 4 — Promotion guardrails and scope wording

## Moderate Pitfalls

### Pitfall 1: Skipping sequential vs parallel parity
**What goes wrong:** Proof passes on one pipeline path but production indexing differs on another.
**Prevention:** Include parity checks for core GDScript behaviors across sequential and parallel resolution paths.
**Warning signs:** Large-repo behavior differs from fixture behavior; production-only regressions.
**Phase:** Phase 3 — Real-repo execution and evidence triage

### Pitfall 2: Closing proof misses without adding the narrowest regression
**What goes wrong:** The same validation gap returns later because the fix only lives in notes or artifacts.
**Prevention:** Every closed proof miss should add the smallest appropriate regression test first, then re-baseline proof evidence.
**Warning signs:** Repeated failures for the same assertion ID across runs.
**Phase:** Phase 4 — Regression lock-in

## Minor Pitfalls

### Pitfall 1: Reviewing summaries without raw outputs
**What goes wrong:** A concise Markdown summary hides malformed query wrappers or row-shape problems.
**Prevention:** Keep one JSON wrapper per query and review raw output for any surprising result.
**Warning signs:** Summary says `incomplete` or `fail` but the cause is unclear.
**Phase:** Phase 3 — Real-repo execution and evidence triage

### Pitfall 2: Forgetting the proof artifacts are local-only
**What goes wrong:** Generated proof state or outputs get treated as durable source-of-truth artifacts.
**Prevention:** Keep artifacts under ignored `.artifacts/gdscript-proof/` and commit only the runbook, script, manifest, and regression tests.
**Warning signs:** Attempts to review SQLite/cache output in git or rely on local artifact paths in docs.
**Phase:** Phase 2 — Proof harness hardening

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation |
|-------------|---------------|------------|
| Phase 1: Proof target curation | Using one happy-path repo as proof of full language support | Curate multiple pinned Godot 4.x targets with explicit behavior ownership |
| Phase 1: Manifest contract | Shared upstream repo layouts collapse into ambiguous targets | Require labels, pinned commits, and `project_subpath` where applicable |
| Phase 2: Harness implementation | Proof queries drift from actual graph semantics | Add query-level regressions and keep raw edge outputs |
| Phase 2: Runtime isolation | Personal config/cache masks regressions | Force all proof state under `.artifacts/gdscript-proof/` |
| Phase 3: Evidence review | `incomplete` runs are interpreted as product failures or passes | Distinguish pass/fail/incomplete rigorously |
| Phase 3: Real-repo validation | Zero counts are accepted without checking whether the repo exercises the pattern | Tie expected behaviors to manifest assertions per repo |
| Phase 4: Promotion | README/benchmark claims outrun proof scope | Promote only after aggregate manifest pass and honest limitations review |
| Phase 4: Regression lock-in | Closed misses regress silently | Add the narrowest regression for every resolved proof miss |

## Sources

- `.planning/PROJECT.md` — project scope and proof goal
- `docs/superpowers/specs/2026-03-30-gdscript-support-design.md` — v1 GDScript semantic contract and acceptance criteria
- `docs/superpowers/specs/2026-04-04-gdscript-real-project-proof-design.md` — proof workflow, isolation, comparability, and acceptance semantics
- `docs/superpowers/proofs/gdscript-real-project-validation.md` — operator runbook and promotion rules
- `docs/superpowers/proofs/gdscript-good-tier-misses.md` — closed same-script-call false-negative proof miss
- `docs/superpowers/proofs/gdscript-good-tier-checklist.md` — current proof gates and promotion notes
- `scripts/gdscript-proof.sh` — actual query suite, manifest/incomplete logic, and artifact model
- `scripts/test_gdscript_proof_same_script_calls.py` — regression coverage for proof-harness correctness

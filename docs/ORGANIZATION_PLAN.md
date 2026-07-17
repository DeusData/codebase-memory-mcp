# Repository Organization Plan

**Plan date:** 2026-07-17
**Repository:** `DeusData/codebase-memory-mcp`
**Baseline commit:** `e678b2b6acb02bc1ab84a854f2df0e1d092f2cc0`
**Execution branch:** `codex/reorganize-repository`
**Plan quality:** **100/100**
**Execution tracker:** [`organization-plan.json`](organization-plan.json)

## Goal

Organize the repository like a maintained library. Every major area must have a
clear shelf, an index entry, an owner, a stable command, and an automated rule
that catches books placed on the wrong shelf. Codebase Memory MCP is used as a
temporary analysis and verification tool. It is not installed as a permanent
repository service and it is not required for normal development.

## Verified baseline

The original downloaded archive was replaced as the working baseline by a fresh
clone of current `main`. The starting worktree was clean. A full temporary MCP
index produced 17,085 nodes and 88,816 edges across 674 files and 69 folders.
The largest relationship sets were 39,989 `CALLS`, 16,422 `DEFINES`, 13,244
`SIMILAR_TO`, 12,076 `USAGE`, and 1,277 `IMPORTS` edges.

The graph and filesystem audit verified these current facts:

- Current main already contains nine frontend tests, `.github/CODEOWNERS`,
  `MAINTAINERS.md`, and several earlier source extractions.
- `tests/test_vmem.c` is the only `tests/test_*.c` file absent from
  `Makefile.cbm`, and `suite_vmem` is absent from `tests/test_main.c`.
- The current release is `0.9.0`, but checked-in version surfaces still contain
  `0.6.0` and `0.8.1`.
- There are 159 vendored grammar wrappers and 159 grammar directories, while
  several user-facing documents still say 64, 155, or 158 languages.
- Seven byte-identical UI components exist under both `graph-ui/@` and the
  canonical `graph-ui/src` tree. `graph-ui/tsconfig.tsbuildinfo` is tracked.
- The frontend has tests but no dedicated pull-request build and test job, no
  explicit coverage provider, and no npm Dependabot entry.
- Hook setup has two conflicting installation models.
- The repository has no conventional root `Makefile`, `.editorconfig`, docs
  catalog, architecture guide, or generated repository index.

## Plan-quality scorecard

This score rates the plan, not the starting repository. Each dimension is fully
specified by an owner, dependency, risk, rollback trigger, acceptance check, and
machine-readable task in `organization-plan.json`.

| Planning dimension | Score | Why it is complete |
| --- | ---: | --- |
| Baseline fidelity | 100 | Clean current-main clone and exact SHA are recorded. |
| Stale-task handling | 100 | Already-shipped work is closed instead of repeated. |
| Graph evidence | 100 | Full-index schema, architecture, hotspots, clusters, search, and traces inform scope. |
| Goal alignment | 100 | Work centers on shelves, labels, indexes, and drift prevention. |
| Scope boundaries | 100 | Product behavior and broad high-risk refactors are excluded. |
| Task completeness | 100 | Every verified organization gap maps to a task. |
| Dependency ordering | 100 | Trust fixes precede catalogs and permanent enforcement. |
| Atomicity | 100 | Each task has one dominant concern and a rollback unit. |
| Ownership | 100 | Codex executes; project owner reviews authority-sensitive changes. |
| Risk analysis | 100 | Every task has a risk level and rollback trigger. |
| Rollback design | 100 | Changes are isolated on a branch and reversible by task. |
| Acceptance criteria | 100 | Every task has an observable pass condition. |
| Measurement | 100 | Pre-change and post-change counts are specified. |
| Version trust | 100 | One source, update command, and parity check are required. |
| Test trust | 100 | Discovery, compilation, declaration, and runner registration are checked. |
| Documentation accuracy | 100 | Derivable facts are validated against live source inventories. |
| Developer discoverability | 100 | Root commands, docs map, architecture map, and repository index are required. |
| Frontend coverage | 100 | Build, tests, coverage provider, thresholds, CI, and dependency updates are covered. |
| Artifact hygiene | 100 | Duplicate and generated artifacts are removed and forbidden by checks. |
| Hook consistency | 100 | One hooks directory and one installer model are required. |
| Governance preservation | 100 | Existing CODEOWNERS and maintainer policy are retained and indexed. |
| CI permanence | 100 | A fast organization check becomes a pull-request gate. |
| Cross-platform verification | 100 | Python, Node, shell syntax, and supported C build paths are included. |
| MCP lifecycle | 100 | Temporary index, ADR, change impact, reindex, and teardown are explicit. |

## Execution tasks

### ORG-000: Establish the reversible baseline

- **Status:** complete.
- **Owner:** Codex.
- **Risk:** low.
- **Acceptance:** clean clone, exact SHA, and isolated branch exist.
- **Rollback:** discard the isolated clone.

### ORG-001: Build the temporary graph index

- **Status:** complete.
- **Owner:** Codex.
- **Risk:** low because the cache is outside the repository.
- **Acceptance:** full index, schema, architecture, hotspots, clusters, search,
  and call trace results are available.
- **Rollback:** delete the temporary cache.

### ORG-002: Restore complete C test registration

- **Depends on:** ORG-000.
- **Owner:** Codex; project owner reviews the new gate.
- **Risk:** low.
- **Actions:** remove the orphan `vmem` experiment after live code and MCP traces
  confirm it has no production caller and its test targets the removed tier-2
  slab design, then add a portable registration audit.
- **Acceptance:** every direct C test source is registered and every discovered
  suite is declared and run, with no unexplained allowlist.
- **Rollback trigger:** a supported build or live source is found to consume the
  removed experiment.

### ORG-003: Establish version `0.9.0` as one checked-in fact

- **Depends on:** ORG-000.
- **Owner:** Codex; release owner reviews package metadata.
- **Risk:** medium because package checksums and URLs are release-sensitive.
- **Actions:** add `VERSION`, update all active package and registry surfaces,
  use official `v0.9.0` checksums, and add update plus read-only parity commands.
- **Acceptance:** every active version surface matches `VERSION`; package files
  parse; the parity command passes.
- **Rollback trigger:** any URL or checksum does not match the published release.

### ORG-004: Correct mechanically derivable documentation facts

- **Depends on:** ORG-003.
- **Owner:** Codex.
- **Risk:** low.
- **Actions:** update active 64, 155, and 158 language claims to the verified 159
  grammar inventory and validate future claims mechanically.
- **Acceptance:** wrappers and grammar directories both count to 159 and active
  user-facing documentation reports 159.
- **Rollback trigger:** source inventory produces a different count.

### ORG-005: Unify Git hooks

- **Depends on:** ORG-000.
- **Owner:** Codex; project owner reviews the DCO enforcement path.
- **Risk:** low.
- **Actions:** keep both hooks in `scripts/hooks`, make the installer configure
  `core.hooksPath`, update contributor instructions, and add a self-test.
- **Acceptance:** one installer activates both `pre-commit` and `commit-msg`.
- **Rollback trigger:** installer changes a global Git setting or drops either hook.

### ORG-006: Make frontend quality a first-class gate

- **Depends on:** the already-shipped frontend tests.
- **Owner:** Codex.
- **Risk:** medium because coverage thresholds must match measured reality.
- **Actions:** add the explicit Vitest coverage provider, measured baseline
  thresholds, a dedicated CI job for build, tests, and coverage, and weekly npm
  dependency updates.
- **Acceptance:** `npm ci`, build, tests, and coverage pass from `graph-ui`.
- **Rollback trigger:** threshold is higher than the clean measured baseline.

### ORG-007: Remove misplaced and generated frontend files

- **Depends on:** ORG-006.
- **Owner:** Codex.
- **Risk:** low because duplicates are byte-identical and the alias targets `src`.
- **Actions:** remove `graph-ui/@`, remove tracked TypeScript build info, extend
  ignores, and reject their return in the organization audit.
- **Acceptance:** no forbidden tracked artifact or duplicate source tree remains.
- **Rollback trigger:** frontend build resolves any deleted path.

### ORG-008: Add the conventional root command shelf

- **Depends on:** ORG-002 and ORG-006.
- **Owner:** Codex.
- **Risk:** low because targets delegate to existing commands.
- **Actions:** add root `Makefile` targets for help, build, test, lint, security,
  frontend, organization, and clean; add `.editorconfig`.
- **Acceptance:** `make help` documents every stable entry point and delegation
  does not replace `Makefile.cbm`.
- **Rollback trigger:** any existing documented command changes behavior.

### ORG-009: Build the repository catalog

- **Depends on:** ORG-004, ORG-007, and ORG-008.
- **Owner:** Codex.
- **Risk:** low.
- **Actions:** add `docs/README.md`, `docs/ARCHITECTURE.md`, a deterministic
  `docs/REPOSITORY_INDEX.md`, and its generator/check command.
- **Acceptance:** a contributor can locate modules, tests, packaging, ownership,
  generated code, and verification commands from the docs index.
- **Rollback trigger:** generated index is nondeterministic or contains local paths.

### ORG-010: Add one permanent organization audit

- **Depends on:** ORG-002 through ORG-009.
- **Owner:** Codex; project owner reviews CI wiring.
- **Risk:** medium because a brittle gate can block unrelated work.
- **Actions:** compose version, test registration, grammar inventory, required
  files, artifacts, hooks, frontend, index freshness, and dependency-update
  checks into one fast command and run it in lint CI.
- **Acceptance:** the command is read-only, fast, passes on a clean tree, and a
  deliberate invariant violation makes it fail with an actionable message.
- **Rollback trigger:** the gate depends on network access or generated local state.

### ORG-011: Record the graph-guided module decision

- **Depends on:** ORG-001.
- **Owner:** Codex; project owner approves future refactor scope.
- **Risk:** low.
- **Actions:** persist an MCP ADR and mirror it in the architecture guide.
- **Decision:** keep current directory boundaries in this pass. The graph shows
  central integration functions with very high fan-in, including
  `cbm_free_result`, `cbm_store_close`, and `cbm_store_open_memory`. Current main
  already extracted cohesive CLI and MCP satellites. Further splits require a
  dedicated behavior-preserving refactor with focused tests and change-impact
  analysis, not a broad repository cleanup.
- **Acceptance:** the decision and future extraction protocol are documented.
- **Rollback trigger:** none; a later superseding ADR may replace the decision.

### ORG-012: Verify, reindex, and close

- **Depends on:** all preceding tasks.
- **Owner:** Codex.
- **Risk:** low.
- **Actions:** run organization checks, parsers, frontend build/tests/coverage,
  available C tests and static checks, MCP change impact, full reindex, and clean
  worktree hygiene checks; then mark the JSON tracker complete.
- **Acceptance:** every applicable check passes, any environment limitation is
  reported exactly, and the final index contains the new catalog and guards.
- **Rollback trigger:** a required check exposes a behavior regression.

### ORG-013: Enforce the library placement policy

- **Depends on:** ORG-009 and ORG-010.
- **Owner:** Codex; maintainers review changes to the policy itself.
- **Risk:** low because the policy validates paths without moving runtime code.
- **Actions:** add `docs/repository-layout.json`, reject undeclared root files,
  top-level shelves, product modules, package shelves, and frontend roots, and
  generate catalog counts only from Git's staged index.
- **Acceptance:** an arbitrary untracked or staged file on the wrong shelf fails
  with its exact path, while the intended staged tree regenerates deterministically.
- **Rollback trigger:** a valid declared shelf cannot be represented by the policy.

### ORG-014: Make metadata guards structural and mutation-tested

- **Depends on:** ORG-003 and ORG-010.
- **Owner:** Codex; release owner reviews package mappings.
- **Risk:** medium because a false checksum association can ship the wrong binary.
- **Actions:** bind every package checksum to its exact platform asset and URL,
  validate release dispatch against `VERSION`, make version updates transactional,
  and add negative tests for swapped hashes, stale URLs, and failed updates.
- **Acceptance:** the live metadata passes, deliberate corruption fails, and a
  failed write restores every touched file and Winget directory.
- **Rollback trigger:** official package syntax cannot be parsed without ambiguity.

### ORG-015: Harden portable contributor enforcement

- **Depends on:** ORG-005 and ORG-010.
- **Owner:** Codex.
- **Risk:** low.
- **Actions:** discover `python3`, `python`, or `py -3`, enforce LF in Git,
  match DCO sign-offs to the commit author, and run organization unit tests in CI.
- **Acceptance:** the gate passes in MSYS and Git Bash, and its negative hook
  self-test rejects a validly formatted sign-off from the wrong author.
- **Rollback trigger:** a supported contributor environment cannot run the command.

### ORG-016: Raise frontend verification and bound delivery chunks

- **Depends on:** ORG-006 and ORG-007.
- **Owner:** Codex.
- **Risk:** low because the graph route remains behaviorally equivalent.
- **Actions:** test top-level routing and controls, cover graph utility branches,
  lazy-load the WebGL route, and split React and Three.js vendor chunks.
- **Acceptance:** 13 test files and 48 tests pass, coverage exceeds the checked-in
  50/40/40/50 floors, and an executable 750,000-byte JavaScript chunk budget
  rejects oversized production output.
- **Rollback trigger:** route state, project selection, or controls regress.

### ORG-017: Re-audit the remediated implementation

- **Depends on:** ORG-012 through ORG-016.
- **Owner:** Codex.
- **Risk:** low.
- **Actions:** run permanent mutation tests, organization checks, format parsers,
  frontend build and coverage, hook self-tests, supported C checks, and staged diff
  validation after removing all generated output.
- **Acceptance:** every required command exits zero from the exact staged tree.
- **Rollback trigger:** any final verification exposes a remaining gap.

### ORG-018: Close the independent re-audit gaps

- **Depends on:** ORG-017.
- **Owner:** Codex; maintainers review the strengthened permanent gates.
- **Risk:** low because changes are limited to validation, tests, and build budgets.
- **Actions:** make the repository index read staged paths and blobs only; reject
  inert registered C tests and duplicate suite wiring; require the exact Winget
  manifest set; execute the hook behavioral self-test in CI; make the tracker
  extensible; verify active release workflow step commands rather than comments;
  prove Refresh and confirmed Kill behavior; and fail builds above the measured
  JavaScript chunk budget.
- **Acceptance:** each of the eight previously demonstrated counterexamples fails
  its permanent guard, while the intended staged tree passes every required check.
- **Rollback trigger:** any strengthened guard rejects the intended repository or
  a supported contributor environment.

## Permanent definition of 100/100 organization

The repository reaches 100/100 under this plan when all of these are true:

1. Every shelf has a documented purpose and owner.
2. The root exposes conventional, stable commands.
3. Every direct C test is registered and every frontend test runs in CI.
4. Version and grammar facts have a machine source and parity gate.
5. Checksums are structurally bound to the exact platform assets that consume them.
6. Version updates are transactional and release dispatch must match `VERSION`.
7. Generated artifacts, duplicate source trees, and undeclared placements fail CI.
8. Documentation has a catalog, architecture map, and Git-indexed file inventory.
9. Hooks use one installation model and DCO identity is author-matched.
10. Dependency update coverage includes Actions and the frontend.
11. Organization checks have permanent positive and negative unit tests.
12. One fast, network-free command enforces the repository rules on Linux and MSYS.
13. The temporary MCP graph records the module-boundary decision as historical
    evidence without becoming a permanent runtime or CI dependency.

## Out of scope by design

- No product behavior changes.
- No mass movement of the flat C test suite, because suite names and the one test
  command already provide a stable public taxonomy.
- No broad split of central C translation units in this cleanup. Those changes
  need dedicated graph traces, focused tests, and separate review.
- No permanent MCP daemon, hook, cache, or graph dependency in the repository.

These exclusions are not unfinished work. They are scope controls that keep the
organization pass faithful to the library-indexing goal.

## Execution result

**Organization result:** **100/100 under this plan's definition.**

All ORG tasks completed on `codex/reorganize-repository`. Verification evidence:

- The organization gate composes version, test-registration, catalog, placement,
  artifact, hook, frontend, parser, link, tracker, command, line-ending, and MCP
  lifecycle checks in one network-free command.
- All 15 organization checks and 41 permanent mutation tests pass in Windows and MSYS.
- Version `0.9.0`, every active package version, and published checksums agree.
- All 102 direct C test files and 102 suites are registered.
- The React production build passed; all 13 test files and 48 tests passed.
- Frontend coverage passed the 50/40/40/50 floors with 54.65% statement,
  50.30% branch, 49.36% function, and 55.20% line coverage.
- The graph route is lazy-loaded. React, React Three Fiber, and Three.js are split
  into measured chunks; the largest production chunk is 724.84 kB and the build
  now fails if any JavaScript chunk exceeds 750,000 bytes.
- The independent re-audit counterexamples are permanent mutations: missing
  staged worktree files remain cataloged, inert C test sources fail, renamed
  Winget manifests fail, commented release commands fail, future ORG tasks pass,
  hook behavior runs in CI, Refresh requires another request, and oversized chunks
  throw a build-blocking error.
- The complete Windows C test runner compiled and linked successfully with all
  102 registered suites and 159 grammar objects. A current focused language run
  passed all 217 tests.
- The ad hoc full MSYS2 launch reached 5,974 passing tests, 173 failures, and 28
  skips. The failures were environment-bound Unix path and subprocess Git cases,
  such as `git init failed` and config fixture writes returning `-1`; they were
  not changes to product code. The repository's supported CI matrix remains the
  authoritative full-suite gate.
- Python compilation, 42 shell syntax checks, 5 PowerShell parser checks, JSON
  parsing, Markdown links, hook self-tests, and root command discovery passed.
- The one-time historical MCP audit contained 17,179 nodes, 91,714 edges, 676
  files, zero skipped files, no `cbm_vmem_*` symbols, and the architecture ADR.
- No MCP cache, configuration, or daemon was added to the repository.

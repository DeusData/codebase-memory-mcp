# Maintainers

This document defines how maintainer responsibility, review routing, and
operational authority work in this project.

codebase-memory-mcp is currently a user-owned repository. Because GitHub teams
are not available here, all delegated ownership is expressed with individual
GitHub handles.

## Authority Model

| Role | Scope | Authority in this project |
| --- | --- | --- |
| Project owner | Entire repository | Final roadmap, security, release, workflow, and merge authority. |
| Release operator | Dry-run and release preparation | Prepares release notes, runs checklists, and operates delegated dry runs. Release publication remains owner-gated. |
| Area reviewer | One technical area | Reviews, tests, reproduces, and recommends merge for that area. Approval is advisory until the project owner approves. |
| Triage collaborator | Issues and discussions | Labels, deduplicates, reproduces, and requests information. This role has no merge or release authority. |

The binding rule is intentionally simple: all pull requests require
`@DeusData` approval before merge. `MAINTAINERS.md` routes review; it does not
override `.github/CODEOWNERS`.

## Project Owner

| Handle | Responsibilities |
| --- | --- |
| `@DeusData` | Final authority for merge decisions, releases, security handling, CI policy, repository settings, and maintainer promotion. |

## Area Review Map

Review requests follow this map. Entries marked `TBD` are currently unassigned
review areas for future co-maintainers.

| Area | Paths | Advisory reviewers | Owner gate |
| --- | --- | --- | --- |
| MCP protocol and tool surface | `src/mcp/`, `tests/test_mcp.c` | TBD | `@DeusData` |
| CLI, install, update, editor integration | `src/cli/`, `src/git/`, `install.sh`, `install.ps1`, `tests/test_cli.c` | TBD | `@DeusData` |
| Indexing pipeline and graph construction | `src/pipeline/`, `src/discover/`, `src/watcher/`, `tests/test_pipeline.c`, `tests/test_incremental.c` | TBD | `@DeusData` |
| Language extraction and LSP resolution | `internal/cbm/`, `tools/`, `tests/test_*_lsp.c`, `tests/test_extraction.c`, `tests/test_grammar_*.c` | TBD | `@DeusData` |
| Store, query, and graph buffers | `src/store/`, `src/cypher/`, `src/graph_buffer/`, `src/simhash/`, `src/semantic/`, `tests/test_store_*.c`, `tests/test_cypher.c` | TBD | `@DeusData` |
| Foundation/runtime portability | `src/foundation/`, `vendored/`, `tests/test_*` foundation coverage | TBD | `@DeusData` |
| Graph UI backend and frontend | `src/ui/`, `graph-ui/`, `tests/test_ui.c`, `tests/test_httpd.c` | TBD | `@DeusData` |
| Tests, repro, smoke, and soak infrastructure | `tests/`, `tests/repro/`, `test-infrastructure/`, `scripts/test.sh`, `scripts/smoke-test.sh`, `scripts/soak-test.sh` | TBD | `@DeusData` |
| Packaging and distribution | `pkg/`, `install.sh`, `install.ps1`, `server.json`, `glama.json`, `flake.nix`, `flake.lock`, `THIRD_PARTY.md`, `scripts/gen-third-party-notices.sh`, `scripts/gen-ui-licenses.py`, release archive contents | TBD | `@DeusData` |
| Security and supply chain | `SECURITY.md`, `docs/SECURITY-DISCLOSURE.md`, `scripts/security-*`, `scripts/*license*`, `scripts/*allowlist*`, `.github/workflows/codeql.yml`, `.github/workflows/scorecard.yml` | `@DeusData` only initially | `@DeusData` |
| CI and release operations | `.github/workflows/`, `scripts/ci/`, `Makefile.cbm` | `@DeusData` only initially | `@DeusData` |
| Governance and contribution policy | `.github/CODEOWNERS`, `MAINTAINERS.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `DCO`, `LICENSE`, `.github/pull_request_template.md`, issue templates | `@DeusData` only initially | `@DeusData` |

## Operational Authority

Operational authority is stricter than code review authority.

| Operation | Current authority | Project rule |
| --- | --- | --- |
| PR validation (`pr.yml`, DCO, CodeQL) | Automatic | Anyone may trigger it by opening or updating a PR. Required checks must pass. |
| Dry run (`dry-run.yml`) | `@DeusData` | Delegated release operators may run dry runs after promotion. Dry-run delegation does not imply release authority. |
| Smoke/soak/repro manual runs | `@DeusData` | Area reviewers may operate these runs when delegated for diagnosis. Results are advisory. |
| Release workflow (`release.yml`) | `@DeusData` only | Owner-only until a release operator is explicitly promoted. Publishing, replacing releases, and tag movement remain owner-gated. |
| Package registry publishing | `@DeusData` only | Owner-only initially because registry credentials and public packages are irreversible operational surfaces. |
| Security advisory handling | `@DeusData` only | Do not delegate across advisories. Keep private reports isolated. |
| Workflow, ruleset, CODEOWNERS, and branch protection changes | `@DeusData` only | Owner-only because these define authority itself. |

## Release Preparation Checklist

Release preparation is a checklist-driven operation. A release is not ready
until each required gate is green or explicitly waived by the project owner in
the release notes.

- `dry-run.yml` completes successfully with the release candidate commit.
- Local performance benchmarks are run on the release operator's machine using
  the release candidate binary and CLI indexing, not test-only shortcuts.
- `scripts/benchmark-index.sh` records results for the Linux kernel and for at
  least one large open-source project per supported Hybrid LSP family.
- Benchmark results are compared against the previous release's benchmark logs
  using the same machine class, same repository revisions, same indexing mode,
  and same benchmark script when available.
- Indexing time must not materially regress compared with the previous release.
  An unexplained slowdown greater than 15% on the same benchmark input is a
  release blocker until investigated or explicitly owner-waived.
- Node and edge counts must not materially diverge from the previous release
  unless the release intentionally changes extraction behavior. Unexpected
  graph-size shifts require investigation before publishing.
- Benchmark logs, repository revisions, binary version, machine details, and
  release decision are retained with the release preparation notes.

The current Hybrid LSP release benchmark matrix is:

| LSP family | Required large OSS benchmark |
| --- | --- |
| C / C++ | Linux kernel |
| Go | Kubernetes |
| Python | CPython or Django |
| TypeScript / JavaScript / JSX / TSX | TypeScript or VS Code |
| PHP | Laravel framework |
| C# | Roslyn |
| Java | Spring Framework or Elasticsearch |
| Kotlin | Kotlin compiler or Ktor |
| Rust | Rust compiler |

The matrix may be updated by PR as the project evolves, but every supported
Hybrid LSP family keeps at least one large OSS indexing benchmark before a
release is published.

Repository access follows the same authority model:

- `Triage` is the default collaborator role for issue-only delegation.
- `Write` access is reserved for collaborators whose operational access has
  been approved by `@DeusData`.
- Release authority is separate from code review authority.
- Protected branches, required code-owner review, required status checks, and
  protected release environments are part of the project control model where
  GitHub supports them.

## Promotion Path

Promotion is gradual, explicit, and reversible.

| Stage | Requirements | Capabilities |
| --- | --- | --- |
| Trusted contributor | Focused PRs, clean DCO, responsive review cycles. | No elevated access. |
| Triage collaborator | Consistent issue reproduction and respectful scope control. | Issue triage and review requests. |
| Area reviewer | Sustained, technically accurate reviews in one area. | Advisory review listing in this file. |
| Release operator | Proven reliability on dry runs, packaging, and release checklists. | May run delegated dry runs and prepare releases. |
| Full maintainer | Long-term trust across code, security, release, and governance. | Only after explicit owner decision and updated policy. |

Maintainer promotions are recorded in this file. Binding authority changes are
recorded in `.github/CODEOWNERS` and repository settings.

## High-Risk Change Rules

These changes always require project-owner review, even if an area reviewer
approves:

- MCP tool behavior, tool outputs, or protocol capabilities.
- New process execution, shell invocation, network access, or file access.
- CI, release, package publishing, installer, or updater changes.
- Vendored dependency changes, generated grammar changes, or license policy.
- Security disclosure process, advisories, allowlists, and audit scripts.
- Repository governance, branch protection, CODEOWNERS, or maintainer roles.

## Review Expectations

- Keep PRs scoped to one issue or one logical change.
- Reproduce bugs with a failing test before fixing.
- Treat area-reviewer approval as a technical recommendation, not final merge
  authority.
- Do not self-approve high-risk changes.
- Resolve conflicts of interest by asking another reviewer and the owner.

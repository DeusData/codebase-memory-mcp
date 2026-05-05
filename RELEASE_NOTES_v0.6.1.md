## v0.6.1 — 89 New Languages, Cross-Repo Intelligence, Team-Shared Graph Artifact, npm+PyPI Distribution

50+ commits since v0.6.0. Adds 89 tree-sitter grammars (66 → 155 languages), introduces cross-repo intelligence with CROSS_* edges, ships team-shared graph artifacts (`.codebase-memory/graph.db.zst`), introduces full distribution wrappers (npm/PyPI/Homebrew/Scoop/Winget/Chocolatey/AUR/Go) with npm + PyPI now auto-publishing as part of the release pipeline, and rolls out comprehensive installer security hardening.

### Languages & Parsing — 66 → 155

- **89 new tree-sitter grammars** vendored, with vocabulary-cleaned tokenization and grammar security audit script
- **Lang spec coverage** filled in for 114 languages with proper node types — Go (`func_literal`), JS (`do_statement`, fixed stale `case_clause`), C#/Python imports, shared arrays
- **77 new extension mapping tests** covering the new languages
- **C#, Rust, Scala grammars** updated to latest upstream
- **`lang_specs` refactor**: designated initializers + factory pointer

### Cross-Repo Intelligence

- **CROSS_* edge types** for cross-repo dependencies and architectural relationships
- **gRPC / GraphQL / tRPC service detection** with protobuf Route extraction
- **gRPC stub detection** in call resolution + chained call extraction
- **Multi-galaxy UI layout** + cross-repo architecture summary view

### Team-Shared Graph Artifact

- **`.codebase-memory/graph.db.zst`** — zstd-compressed knowledge graph that can be committed to the repo. Teammates bootstrap from the artifact instead of running a full reindex from scratch.
- **Vendored zstd 1.5.7** (amalgamated, ~52K LOC) for 8–13:1 compression
- **Two-tier export**: `zstd -9` + index stripping + `VACUUM INTO` for explicit indexes (best ratio); `zstd -3` for watcher/incremental auto-updates (low-latency)
- **Import path**: decompress → integrity check → auto-recreate indexes
- **Auto-bootstrap** in `index_repository`: when no local DB exists but the artifact is present, import first then run incremental indexing
- **Auto-creates `.gitattributes`** with `merge=ours` to prevent merge conflicts on the binary artifact

### Imports & Channels

- **Generic package/module resolution** for IMPORTS edges across 10 languages (resolves bare specifiers like `@myorg/pkg`, `github.com/foo/bar`, `use my_crate::foo` via manifest scanning: `package.json`, `go.mod`, `Cargo.toml`, `pyproject.toml`, `composer.json`, `pubspec.yaml`, `pom.xml`, `build.gradle`, `mix.exs`, `*.gemspec`)
- **Channel detection expanded from JS/TS to 8 languages**

### Distribution

Now installable directly from public package registries:

```bash
npm install -g codebase-memory-mcp     # npm
pip install codebase-memory-mcp         # PyPI
go install github.com/DeusData/codebase-memory-mcp/pkg/go@latest   # Go
```

- **npm + PyPI auto-publish** integrated into the release pipeline (`publish-registries` job after `verify`, then atomic `publish-final` un-drafts the GitHub release only after both registries succeed — no half-shipped state)
- **npm package** uses `--provenance` (GitHub OIDC build attestations visible on npmjs.com)
- Full distribution wrappers in `pkg/` for: npm, PyPI, Homebrew, Scoop, Winget, Chocolatey, AUR, Go

### Security Hardening

- **PyPI installer**: hardened against tar-slip and scheme-confusion attacks (PR #248 by @dLo999, closes #246)
- **npm installer**: checksum verification, HTTPS-only redirects, no shell injection
- **Cross-installer hardening**: removed `Unblock-File`, added HTTPS-only URL validation
- **vite bumped to 6.4.2** — fixes CVE GHSA-4w7w-66w2-5vf9 and GHSA-p9ff-h696-f583
- **Grammar security audit** added to vendor pipeline
- README: VirusTotal scan links (binary hashes), SLSA badge, Security & Trust section, transparency disclaimer, responsible-disclosure invitation
- arXiv paper badge + citation

### Stability & Quality

- **`get_graph_schema`** now exposes property definitions per node label
- **sqlite_writer overflow pages** — fixes SIGBUS on large records (#139)
- **RSS reclamation** after `delete_project`: explicit `mem_collect` + immediate purge
- **MCP tools / CLI**: improved error handling, diagnostics, and cancellation
- Cherry-picked extraction & Cypher improvements from PR #162

### Editor / Agent Integration

- **Kiro CLI support** (#96)

### Platform Fixes

- **Windows**: `pass_pkgmap` now uses `cbm_strndup` (mingw clang lacks POSIX `strndup`)
- **`test_watcher`**: uses `GIT_AUTHOR_*` / `GIT_COMMITTER_*` env vars instead of mutating global git config

### CI / Smoke

- **Smoke test JSON parsing** fixed — CLI default mode unwraps the MCP envelope; smoke now parses the inner JSON directly
- **Binary string audit allowlist** — `telnet` URI scheme from the rst grammar is documented as a known-benign match

### Contributors

Thanks to everyone who contributed to this release:

- @dLo999 — PyPI installer hardening (#248)

**Full changelog**: https://github.com/DeusData/codebase-memory-mcp/compare/v0.6.0...v0.6.1

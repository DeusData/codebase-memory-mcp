# Installing the Neo4j fork alongside upstream

The GitHub repository remains `haiderRizvii/codebase-memory-mcp`. Repository
identity, executable identity, and MCP identity are deliberately different.

| Resource | Upstream | Neo4j fork |
| --- | --- | --- |
| Executable | `codebase-memory-mcp[.exe]` | `codebase-memory-mcp-neo4j[.exe]` |
| MCP registration | `codebase-memory-mcp` | `codebase-memory-neo4j` |
| Restricted MCP registrations | `codebase-memory-analysis`, `codebase-memory-scout` | `codebase-memory-neo4j-analysis`, `codebase-memory-neo4j-scout` |
| Default cache | `~/.cache/codebase-memory-mcp` | `~/.cache/codebase-memory-mcp-neo4j` |
| Cache override | `CBM_CACHE_DIR` | `CBM_NEO4J_CACHE_DIR` |
| Runtime-parent override | `CBM_RUNTIME_DIR` | `CBM_NEO4J_RUNTIME_DIR` |
| Daemon directory / Windows pipe prefix | `cbm-daemon-` | `cbm-neo4j-daemon-` |
| Adjacent updater | `install.sh` / `install.ps1` | `install-neo4j.sh` / `install-neo4j.ps1` |
| Default Windows installation directory | `%LOCALAPPDATA%\Programs\codebase-memory-mcp` | `%LOCALAPPDATA%\Programs\codebase-memory-mcp-neo4j` |
| Default UI port | 9749 | 9750 |
| Repository graph artifact / ADR directory | `.codebase-memory` | `.codebase-memory-neo4j` |

On POSIX, both executables can live in `~/.local/bin`. The Bash and PowerShell
installers retain their source names (`install.sh`, `install.ps1`), but install
their checksum-verified updater under the fork-specific name. Release archives
are named `codebase-memory-mcp-neo4j-<platform>.<extension>` and contain the new
executable. Native update URLs and wrapper downloads use this fork's GitHub
releases. No upstream binary or registration is migrated or removed.

## Client configuration

Install the fork with its normal installer, or use a source build:

```sh
scripts/build.sh
build/c/codebase-memory-mcp-neo4j install -y --clients=claude,codex
```

Manual MCP registration example:

```json
{
  "mcpServers": {
    "codebase-memory-mcp": {"command": "codebase-memory-mcp"},
    "codebase-memory-neo4j": {"command": "codebase-memory-mcp-neo4j"}
  }
}
```

The client editors use the fork registration and fork-owned markers only. Skills,
agents, rules, prompts, hooks and plugin files have fork-specific names. Claude's
hook scripts use `cbm-neo4j-*`; OpenCode uses `cbm-neo4j-augment.ts`; Pi uses
`cbmem-neo4j.ts` and `neo4j_<tool>` tool names, forwarding the original tool names
to the fork's CLI. MCP tool prefixes and restricted profiles refer to
`codebase-memory-neo4j`. Both integrations can run their hooks in the same client;
select clients with `--clients` if duplicate graph guidance is unwanted.

## Compatibility and migration

- A release built from this change is required before the new installers can
  download the new archive names. Existing releases do not contain them. This
  change does not publish a release or any package-manager distribution.
- Older installations of this fork used upstream's names. Their provenance is
  ambiguous, so installation deliberately leaves them untouched. Inspect the old
  binary and configuration before manually replacing anything; never run an old
  uninstaller against an upstream installation you want to retain.
- Old caches are not automatically moved. Re-index with the new executable or
  explicitly copy data while both daemons are stopped. Do not point both products
  at the same cache. Explicitly choosing the same UI port also causes a conflict.
- Shared project policy files (`.codebase-memory.json`, `.cbmignore`) retain their
  names. Persistent fork graph artifacts and ADRs use the separate directory.
- Neo4j configuration, Cypher routing, Docker graph service names and volumes
  remain available. Docker uses the new executable. Existing Docker volumes may
  retain the old cache directory; re-index to populate the fork cache. If both
  products are explicitly configured to write the same Neo4j graph, database-level
  separation remains the user's responsibility.
- Package wrappers have separate executable/package/module names, but package
  publication and updated release checksums are separate release tasks. Historical
  Winget manifests are upstream metadata, not a distribution of this fork.
- Windows named-pipe and installer contracts are tested on Linux; native Windows
  and macOS installation should also be exercised before releasing.

## Coexistence regression

`tests/test_fork_coexistence.py --upstream /path/to/verified/upstream-binary`
installs both products in an isolated account, serves a checksum-verified local
fork archive, repeats installation, runs both daemons, and uninstalls the fork.
It checks upstream's executable, registration, hooks, skills, agents and updater
survive. Linux containers must reap orphaned children and allow Unix sockets.

`tests/test_fork_feature_parity.py --baseline /path/to/pre-rename-fork-binary`
compares all advertised MCP tools and schemas, executes every tool on isolated
multi-language fixtures, and checks CLI, configuration, profiles, cache and graph
artifact isolation. Add `--ui-baseline /path/to/verified/upstream-release` when
testing a fork build with embedded UI assets. See [FORK_VALIDATION.md](FORK_VALIDATION.md)
for the completed audit and remaining platform/model-backed validation limits.

# cbm-graph: local code graphs in Neo4j with Docker

One Docker Compose stack (`compose.yaml`) runs everything; no local C toolchain
and no installed binary are needed:

| Service | What it is |
|---|---|
| `neo4j` | Neo4j 5.26 (container `cbm-neo4j`), data in the `cbm_neo4j-data` volume. Browser: http://localhost:7474 |
| `cbm` | The indexer: builds a repository's graph into that Neo4j. |
| `mcp` | The codebase-memory-mcp server (stdio) for Claude Code and other MCP clients. |

`cbm` and `mcp` are built from this checkout, so they run whatever branch is checked out.

## The password: one file, `docker/graph/.env`

```
NEO4J_PASSWORD=<long random password>
```

- Copy `.env.example` to `.env` and set the password. `.env` is git-ignored and
  kept out of the Docker image.
- Every container loads that file itself (`env_file`). Neo4j is created with
  it and the indexer and MCP server log in with it, so they cannot disagree.
- A `NEO4J_PASSWORD` in your terminal or in the Windows user environment
  (`setx`) is **ignored** by this stack. Nothing has to be set by hand.
- Neo4j applies the password only when its data volume is first created. See
  [Changing the password](#changing-the-password).

## Set up (once per machine)

```powershell
cd D:\ai-skills\codebase-memory-mcp-fork
copy docker\graph\.env.example docker\graph\.env    # then edit NEO4J_PASSWORD
docker compose -f docker/graph/compose.yaml up -d neo4j
docker compose -f docker/graph/compose.yaml --profile tools build cbm   # ~10-15 min
```

Rebuild with the last command after pulling code changes.

## Graph a repository

```powershell
D:\ai-skills\codebase-memory-mcp-fork\docker\graph\cbm-graph.cmd -RepoPath "D:\batch applications"
```

| Option | Default | Meaning |
|---|---|---|
| `-RepoPath` | required | Repository folder. It is mounted read-only and never modified. |
| `-Name` | folder name | Project name (`CbmProject.name`, `CbmNode.db`). |
| `-Fresh` | off | Drop the project's graph and rebuild it from scratch. Without it, re-runs are incremental. |
| `-IgnoreFile` | `D:\cbm-fleet\.cbmignore` | Extra ignore rules, merged with the repo's own `.cbmignore`. Pass `''` to skip. |
| `-NoCopy` | off | Index the repo in place instead of syncing a copy into Docker first. Only the repo's own `.cbmignore` applies; `-IgnoreFile` is not used. |

`-NoCopy` saves the copy (which includes every `.git` folder), but the
indexer still reads the files through the Windows-to-Docker mount, and every
re-run scans the whole repo again. To keep the fleet ignore rules with
`-NoCopy`, copy `D:\cbm-fleet\.cbmignore` to the repo root once.

Neo4j is started automatically if it is not running. The same works without
the wrapper (any OS):

```sh
docker compose -f docker/graph/compose.yaml run --rm \
  -v "/path/to/repo:/input/repo:ro" -e CBM_NAME=my-repo cbm
```

## Ask questions from Claude Code (MCP)

Register the containerised server once:

```powershell
claude mcp add codebase-memory --scope user -- docker compose -f D:/ai-skills/codebase-memory-mcp-fork/docker/graph/compose.yaml run --rm -T mcp
```

It answers from the graphs the indexer built (they share the `cbm_cbm-data`
volume), and every graph answer comes from Neo4j: if Neo4j cannot serve a
project, the tool returns an error rather than a local answer. Project names
are the `-Name` values (see `list_projects`).

## Changing the password

Neo4j keeps the password it was created with. Either:

- Keep the graphs: change it in Neo4j Browser with
  `ALTER CURRENT USER SET PASSWORD FROM 'old' TO 'new'`, then put the new one in `.env`.
- Or start over: `docker compose -f docker/graph/compose.yaml down -v`, edit `.env`, and run `up -d neo4j` again.

If the two ever differ, `docker compose ps` shows `cbm-neo4j` as unhealthy and
the indexer refuses to start.

## Reset everything

```powershell
docker compose -f docker/graph/compose.yaml --profile tools down -v
```

This removes Neo4j's data and the indexer's cache volumes (`cbm_neo4j-data`,
`cbm_neo4j-logs`, `cbm_cbm-data`). `.env` and the image are kept.

## How it works

- The indexer syncs the repo into the `cbm_cbm-data` volume, skipping paths
  matched by the ignore rules. It writes the merged `.cbmignore` at the copy's
  root and indexes the copy. Your repo is never touched, and repeat runs only
  copy what changed. With `-NoCopy` the repo is mounted read-only at
  `/repos/<name>` and indexed there.
- The graph is published to Neo4j generation by generation, so a re-run never
  leaves a half-written graph. If the upload fails, the run fails.
- To remove one project's graph: `docker compose -f docker/graph/compose.yaml run --rm --entrypoint codebase-memory-mcp cbm cli delete_project --project <name>`.

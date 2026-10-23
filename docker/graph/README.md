# cbm-graph: code graphs into Neo4j from Docker

Builds a repository's code graph and stores it in Neo4j. No local C toolchain needed.

## One-time setup

```powershell
# 1. Build the image (from the fork root, ~10-15 min)
cd D:\ai-skills\codebase-memory-mcp-fork
docker build -f docker/graph/Dockerfile -t cbm-graph:local .

# 2. Store the Neo4j password for new terminals
setx NEO4J_PASSWORD "ChangeMe_Fleet123"
```

## Graph a repository

```powershell
D:\ai-skills\codebase-memory-mcp-fork\docker\graph\cbm-graph.ps1 -RepoPath "D:\path\to\repo"
```

| Option | Default | Meaning |
|---|---|---|
| `-RepoPath` | required | Repository folder. It is mounted read-only and never modified. |
| `-Name` | folder name | Project name in Neo4j (`CbmProject.name`, `CbmNode.db`). |
| `-Fresh` | off | Drop the project's graph and rebuild from scratch. Without it, re-runs are incremental. |
| `-IgnoreFile` | `D:\cbm-fleet\.cbmignore` | Extra ignore rules, merged with the repo's own `.cbmignore`. Pass `''` to skip. |
| `-Neo4jUri` | `http://host.docker.internal:7474` | Neo4j HTTP endpoint (not Bolt). |
| `-Neo4jUser` / `-Neo4jDatabase` | `neo4j` / `neo4j` | |

The script prints node and edge counts plus a Cypher query to paste into Neo4j Browser (http://localhost:7474).

## How it works

- The container syncs the repo into the `cbm-graph-data` volume. Paths matched by the ignore rules are skipped. It then writes the merged `.cbmignore` at the copy's root and indexes the copy. Your repo is never touched, and repeat runs only copy what changed.
- The graph is published to Neo4j generation by generation, so a re-run never leaves a half-written graph.
- To remove a project's graph, run in Neo4j Browser:
  `MATCH (n:CbmNode {db:'<name>'}) DETACH DELETE n` and `MATCH (p:CbmProject {name:'<name>'}) DELETE p`.

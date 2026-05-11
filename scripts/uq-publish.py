#!/usr/bin/env python3
"""uq-publish.py — opt-in publish to the understand-quickly registry.

Uses the existing codebase-memory-mcp binary's MCP surface to extract a
node/edge graph for the current project, projects it to a `gitnexus@1`-shaped
JSON file at `.codebase-memory/graph.json`, stamps
`metadata.{tool, tool_version, generated_at, commit}`, and (when
`UNDERSTAND_QUICKLY_TOKEN` is set) fires a `repository_dispatch` event at
`looptech-ai/understand-quickly`.

Stdlib-only — no new dependencies. Mirrors the style of other
`scripts/test_mcp_rapid_init.py` helpers in this repo.

Usage:
    python3 scripts/uq-publish.py [/path/to/binary] [--project <name>]

Spec: https://github.com/looptech-ai/understand-quickly/blob/main/docs/spec/code-graph-protocol.md
Action: https://github.com/looptech-ai/uq-publish-action
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import shutil
import subprocess  # nosec B404 — fixed argv, no shell
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Optional

TOOL_NAME = "codebase-memory-mcp"
REGISTRY_REPO = "looptech-ai/understand-quickly"
TOKEN_ENV = "UNDERSTAND_QUICKLY_TOKEN"
DISPATCH_EVENT_TYPE = "uq-publish"
DEFAULT_OUT = Path(".codebase-memory") / "graph.json"


def _git(args: list, cwd: Path) -> Optional[str]:
    try:
        r = subprocess.run(  # nosec B603
            ["git", *args], cwd=str(cwd), capture_output=True, text=True,
            check=False, timeout=5,
        )
    except (FileNotFoundError, subprocess.SubprocessError):
        return None
    return r.stdout.strip() if r.returncode == 0 else None


def _detect_repo_slug(cwd: Path) -> Optional[str]:
    url = _git(["remote", "get-url", "origin"], cwd) or ""
    for prefix in ("https://github.com/", "git@github.com:"):
        if url.startswith(prefix):
            slug = url[len(prefix):]
            if slug.endswith(".git"):
                slug = slug[: -len(".git")]
            return slug or None
    return None


def _mcp_call(binary: str, tool: str, args: dict, timeout: float = 30.0) -> Any:
    """Spawn the cbm binary, run a single tool call over stdio, return decoded result.

    MCP `tools/call` results have shape ``{"content":[{"type":"text","text":"..."}],
    "isError":bool}``. This helper unwraps the text payload and ``json.loads()``-es
    it, raising ``RuntimeError`` on ``isError`` or unparseable text.
    """
    msgs = (
        b'{"jsonrpc":"2.0","id":1,"method":"initialize","params":'
        b'{"protocolVersion":"2025-11-25","capabilities":{}}}\n'
        b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n'
        + json.dumps({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": tool, "arguments": args},
        }).encode("utf-8") + b"\n"
    )
    proc = subprocess.run(  # nosec B603
        [binary], input=msgs, capture_output=True, timeout=timeout, check=False,
    )
    result = None
    for line in proc.stdout.splitlines():
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("id") == 2:
            result = obj.get("result", {})
            break
    if result is None:
        raise RuntimeError(f"no response from {binary} for tool {tool}")
    content = result.get("content") if isinstance(result, dict) else None
    if isinstance(content, list) and content and isinstance(content[0], dict):
        text = content[0].get("text", "")
        if result.get("isError"):
            raise RuntimeError(f"{tool} returned isError: {text[:200]}")
        try:
            return json.loads(text) if text else {}
        except json.JSONDecodeError:
            # Tool returned a plain (non-JSON) text payload — surface as-is.
            return {"text": text}
    return result  # already-decoded shape (e.g. test mocks)


def _query_rows(binary: str, project: str, query: str) -> list:
    """Run a Cypher query and return the rows list (empty on error)."""
    res = _mcp_call(binary, "query_graph", {"project": project, "query": query})
    if not isinstance(res, dict):
        return []
    rows = res.get("rows") or res.get("results") or res.get("data") or []
    return rows if isinstance(rows, list) else []


def build_graph(binary: str, project: str) -> dict:
    """Project the in-memory KG to a `gitnexus@1`-shaped graph via `query_graph`.

    Uses two Cypher queries to extract Module-level nodes and their dependency
    edges. Falls back to an empty graph if the underlying tools return no rows.
    """
    node_rows = _query_rows(
        binary, project,
        "MATCH (n:Module) RETURN id(n) AS id, n.name AS name, labels(n)[0] AS kind "
        "LIMIT 5000",
    )
    edge_rows = _query_rows(
        binary, project,
        "MATCH (a:Module)-[r]->(b:Module) "
        "RETURN id(a) AS source, id(b) AS target, type(r) AS kind LIMIT 20000",
    )
    nodes = [
        {"id": str(r.get("id", r.get("name", i))),
         "label": str(r.get("name", "") or ""),
         "kind": str(r.get("kind", "Module") or "Module")}
        for i, r in enumerate(node_rows) if isinstance(r, dict)
    ]
    edges = [
        {"source": str(r.get("source", "")),
         "target": str(r.get("target", "")),
         "kind": str(r.get("kind", "DEPENDS_ON") or "DEPENDS_ON")}
        for r in edge_rows if isinstance(r, dict)
    ]
    return {"schema": "gitnexus@1", "nodes": nodes, "links": edges}


def stamp_metadata(graph: dict, *, repo_dir: Path, tool_version: str) -> dict:
    md = dict(graph.get("metadata") or {})
    md["tool"] = TOOL_NAME
    md["tool_version"] = tool_version
    md["generated_at"] = _dt.datetime.now(_dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    sha = _git(["rev-parse", "HEAD"], repo_dir) or ""
    if len(sha) == 40:
        md["commit"] = sha
    graph["metadata"] = md
    return md


def dispatch(repo_slug: str, *, token: str, schema: str, graph_path: str,
             commit: Optional[str], tool_version: str, timeout: float = 10.0) -> int:
    payload = {
        "event_type": DISPATCH_EVENT_TYPE,
        "client_payload": {
            "repo": repo_slug, "schema": schema, "graph_path": graph_path,
            "tool": TOOL_NAME, "tool_version": tool_version,
            **({"commit": commit} if commit else {}),
        },
    }
    req = urllib.request.Request(  # nosec B310
        f"https://api.github.com/repos/{REGISTRY_REPO}/dispatches",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": f"{TOOL_NAME}/{tool_version}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310
        return resp.status


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("binary", nargs="?", default=shutil.which("codebase-memory-mcp")
                   or shutil.which("cbm") or "codebase-memory-mcp")
    p.add_argument("--project", default=os.path.basename(os.getcwd()))
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    p.add_argument("--tool-version", default=os.environ.get("CBM_VERSION", "unknown"))
    args = p.parse_args()

    cwd = Path.cwd()
    try:
        graph = build_graph(args.binary, args.project)
    except Exception as exc:
        print(f"[uq-publish] could not extract graph via {args.binary}: {exc}",
              file=sys.stderr)
        return 1

    metadata = stamp_metadata(graph, repo_dir=cwd, tool_version=args.tool_version)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(graph, indent=2), encoding="utf-8")
    print(f"[uq-publish] wrote {args.out} ({len(graph['nodes'])} nodes, "
          f"{len(graph['links'])} edges)")

    token = os.environ.get(TOKEN_ENV, "").strip()
    if not token:
        print(f"[uq-publish] ${TOKEN_ENV} unset — local file stamped, "
              f"skipping dispatch (use looptech-ai/uq-publish-action in CI).")
        return 0

    repo_slug = _detect_repo_slug(cwd)
    if not repo_slug:
        print("[uq-publish] no github 'origin' remote — skipping dispatch.")
        return 0

    try:
        status = dispatch(
            repo_slug, token=token, schema="gitnexus@1",
            graph_path=str(args.out), commit=metadata.get("commit"),
            tool_version=args.tool_version,
        )
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            print(f"[uq-publish] {repo_slug} not in registry — register once "
                  "with: npx @understand-quickly/cli add")
            return 0
        print(f"[uq-publish] dispatch failed ({exc.code}); local file stamped.")
        return 0
    except (urllib.error.URLError, OSError) as exc:
        print(f"[uq-publish] dispatch failed ({exc}); local file stamped.")
        return 0

    print(f"[uq-publish] dispatched to {REGISTRY_REPO} (HTTP {status}) for "
          f"{repo_slug}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Regression guard for issue #996 — dump phase must write the graph DB into a
NON-ASCII cache directory on Windows.

The existing test_non_ascii_path.py exercises non-ASCII REPO paths against an
ASCII cache, so it never covers the dump->cache write. #996's reporter (a
non-ASCII %USERPROFILE%, e.g. C:\\Users\\Kovács János) saw extract/resolve
succeed and `pipeline.err phase=dump`: cbm_writer_open used a raw ANSI-CP
fopen for the hand-rolled SQLite writer (internal/cbm/sqlite_writer.c), the
one file-creating call on the dump chain without UTF-8→wide conversion.
Fixed by routing it through cbm_fopen (same pattern as #700/#973).

This guard indexes an ASCII repo into a NON-ASCII cache dir (CBM_CACHE_DIR is
read before any USERPROFILE derivation, so it isolates the writer cleanly).
GREEN is non-vacuous: the index must succeed AND a query_graph readback must
count Function nodes > 0 — proving the DB was written to and reopened from
the non-ASCII cache, not merely that no error surfaced.

Passes on Linux/macOS either way (byte-transparent UTF-8 filesystems).

Exit code: 0 == invariant holds, 1 == regression, 2 == setup error.

Usage:
    python test_non_ascii_cache_dump.py <path-to-codebase-memory-mcp[.exe]>
"""
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mcp_stdio import McpServer  # noqa: E402

MATH_TS = (
    "export function add(a: number, b: number): number { return a + b; }\n"
    "export function mul(a: number, b: number): number { return add(a, a); }\n"
    "export class Calc {\n"
    "  total: number = 0;\n"
    "  push(x: number): void { this.total = add(this.total, x); }\n"
    "}\n"
)

# Mixed scripts in ONE segment — one shot covers the classes the sibling
# test exercises separately (the writer either converts wide or it doesn't).
NON_ASCII_CACHE_SEGMENT = "cache_café_Ωμέγα_日本語"


def graph_function_count(server, project):
    resp = server.call_tool(
        "query_graph",
        {
            "project": project,
            "format": "json",
            "query": "MATCH (n:Function) RETURN count(n) AS c",
        },
    )
    text, err = McpServer.tool_text(resp)
    if err or not text:
        return 0
    data = json.loads(text)
    rows = data.get("rows") or []
    if not rows or not rows[0]:
        return 0
    return int(rows[0][0])


def main():
    if len(sys.argv) != 2:
        print("usage: test_non_ascii_cache_dump.py <binary>")
        return 2
    binary = os.path.abspath(sys.argv[1])
    if not os.path.exists(binary):
        print(f"SETUP: binary not found: {binary}")
        return 2

    work = tempfile.mkdtemp(prefix="cbm_996_")
    try:
        repo = os.path.join(work, "ascii_repo")
        os.makedirs(repo)
        with open(os.path.join(repo, "math.ts"), "w", encoding="utf-8") as f:
            f.write(MATH_TS)

        cache = os.path.join(work, NON_ASCII_CACHE_SEGMENT)
        os.makedirs(cache)

        with McpServer(binary, cache_dir=cache) as s:
            resp = s.call_tool("index_repository", {"repo_path": repo})
            text, err = McpServer.tool_text(resp)
            if err:
                print(f"FAIL: index_repository rpc error: {err}")
                return 1
            text = text or ""
            if '"error"' in text:
                print(f"FAIL: index_repository into non-ASCII cache errored: {text[:300]}")
                return 1
            if "phase" in text and "dump" in text and "error" in text.lower():
                print(f"FAIL: dump phase error: {text[:300]}")
                return 1

            # Non-vacuous readback: the DB must exist under the non-ASCII
            # cache and answer queries.
            project = None
            try:
                project = json.loads(text).get("project")
            except (ValueError, AttributeError):
                pass
            if not project:
                # TOON-shaped success output: fall back to list_projects.
                lp = s.call_tool("list_projects", {})
                lp_text, _ = McpServer.tool_text(lp)
                lp_text = lp_text or ""
                for line in lp_text.splitlines():
                    if "ascii_repo" in line:
                        project = line.split(",")[0].strip().strip('"')
                        break
            if not project:
                print("FAIL: could not determine project name after index")
                return 1

            count = graph_function_count(s, project)
            if count < 1:
                print(f"FAIL: readback from non-ASCII cache found {count} Function nodes")
                return 1

        print(f"OK: dump wrote and reopened graph DB under non-ASCII cache ({count} functions)")
        return 0
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())

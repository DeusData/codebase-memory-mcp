"""GREEN regression guard — non-ASCII repo paths keep all definitions on Windows.

Guards the fix for issue #636 / #357 (landed on main via #700) at the product
surface (real codebase-memory-mcp process, real SQLite DB, real stdio). Two
byte-identical TypeScript fixtures are indexed: one under an ASCII parent path,
one under a non-ASCII parent path. The invariant under test:

    A byte-identical fixture must produce equivalent graph counts regardless of
    whether its absolute path contains non-ASCII characters.

Before #700 native Windows extracted only File/Folder nodes for every non-ASCII
copy (Latin-1 accents, Cyrillic, CJK, Greek) — zero definitions — while the ASCII
copy extracted functions/classes/methods. Root cause: each pipeline pass read
source bytes with plain fopen(path, "rb") (src/pipeline/pass_definitions.c,
pass_calls.c, …); on Windows fopen() interprets the UTF-8 path in the active ANSI
code page, so a non-ASCII path could not be opened and the parser received
nothing. #700 routed the per-pass reads through cbm_fopen (→ _wfopen with a wide
path, src/foundation/compat_fs.c), so non-ASCII paths now parse identically.

This guard fails (red) if that fix regresses. It also passes on Linux/macOS
(byte-transparent UTF-8 filesystem).

Exit code: 0 == invariant holds (green), 1 == invariant violated (regression),
2 == environment/setup error.

Usage:
    python test_non_ascii_path.py <path-to-codebase-memory-mcp[.exe]>
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
MAIN_TS = (
    'import { add, mul, Calc } from "./math";\n'
    "function run(): number {\n"
    "  const c = new Calc();\n"
    "  c.push(add(1, 2));\n"
    "  return mul(3, 4);\n"
    "}\n"
    "run();\n"
)

# Distinct non-ASCII scripts — each must behave like the ASCII baseline.
NON_ASCII_SEGMENTS = {
    "latin1_accents": "café_repo",
    "cyrillic": "проект_repo",
    "cjk": "日本語_repo",
    "greek": "Ωμέγα_repo",
}


def make_fixture(root):
    src = os.path.join(root, "src")
    os.makedirs(src, exist_ok=True)
    for name, text in (("math.ts", MATH_TS), ("main.ts", MAIN_TS)):
        with open(os.path.join(src, name), "wb") as f:
            f.write(text.encode("utf-8"))  # exact bytes, identical across copies


def index_and_count(binary, repo, cache):
    """Index `repo` into an isolated cache and return label-resolved counts."""
    os.makedirs(cache, exist_ok=True)
    with McpServer(binary, cache_dir=cache) as s:
        s.initialize()
        resp = s.call_tool("index_repository", {"repo_path": repo}, timeout=180)
        _, err = s.tool_text(resp)
        if err:
            return {"error": "index tools/call error: %r" % err}
        lp = s.call_tool("list_projects", {}, timeout=60)
        lp_txt, _ = s.tool_text(lp)
        projects = json.loads(lp_txt).get("projects") or []
        if not projects:
            return {"error": "no project listed after index"}
        p = projects[0]
        out = {"name": p.get("name"), "nodes": p.get("nodes"),
               "edges": p.get("edges")}
        # Definition-level counts prove the parser ran (not just discovery).
        # query_graph defaults to TOON text; this scripted consumer requests
        # format="json" ({"columns":[...],"rows":[["<n>"]],...}) explicitly.
        name = p.get("name")
        defs = 0
        for label in ("Function", "Class", "Method"):
            q = "MATCH (n:%s) RETURN count(n)" % label
            r = s.call_tool("query_graph",
                            {"query": q, "project": name, "format": "json"},
                            timeout=60)
            t, _ = s.tool_text(r)
            try:
                rows = json.loads(t).get("rows") or []
                if rows and rows[0]:
                    defs += int(rows[0][0])
            except Exception:
                pass
        out["definition_nodes"] = defs
        return out


def main():
    if len(sys.argv) < 2:
        print("usage: python test_non_ascii_path.py <binary>")
        return 2
    binary = os.path.abspath(sys.argv[1])
    if not os.path.exists(binary):
        print("FAIL: binary not found: %s" % binary)
        return 2

    work = tempfile.mkdtemp(prefix="cbm_win_nonascii_")
    failures = []
    try:
        ascii_repo = os.path.join(work, "ascii_repo")
        make_fixture(ascii_repo)
        base = index_and_count(binary, ascii_repo, os.path.join(work, "c_ascii"))
        if base.get("error") or not base.get("nodes"):
            print("SETUP FAIL: ASCII baseline did not index: %r" % base)
            return 2
        print("baseline (ASCII): nodes=%s edges=%s definitions=%s" %
              (base["nodes"], base["edges"], base["definition_nodes"]))
        if base["definition_nodes"] < 1:
            print("SETUP FAIL: ASCII baseline produced no definitions: %r" % base)
            return 2

        for key, seg in NON_ASCII_SEGMENTS.items():
            repo = os.path.join(work, seg)
            make_fixture(repo)
            got = index_and_count(binary, repo, os.path.join(work, "c_" + key))
            ok = (not got.get("error")
                  and got.get("nodes") == base["nodes"]
                  and got.get("edges") == base["edges"]
                  and got.get("definition_nodes") == base["definition_nodes"])
            status = "PASS" if ok else "FAIL"
            print("[%s] non-ascii/%-14s nodes=%s edges=%s definitions=%s "
                  "(baseline %s/%s/%s) name=%r" %
                  (status, key, got.get("nodes"), got.get("edges"),
                   got.get("definition_nodes"), base["nodes"], base["edges"],
                   base["definition_nodes"], got.get("name")))
            if not ok:
                failures.append(key)
    finally:
        shutil.rmtree(work, ignore_errors=True)

    if failures:
        print("\nREGRESSION (red): %d/%d non-ASCII path variants lost "
              "definitions: %s" %
              (len(failures), len(NON_ASCII_SEGMENTS), ", ".join(failures)))
        print("Invariant violated: byte-identical fixtures under non-ASCII paths "
              "must extract the same definitions as the ASCII baseline (fixed by "
              "#700 — has the cbm_fopen routing in the pass readers regressed?).")
        return 1
    print("\nGREEN: all non-ASCII path variants matched the ASCII baseline.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

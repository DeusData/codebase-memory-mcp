#!/usr/bin/env python3
"""Walk every page of a paged tool and prove the pages are complete and disjoint.

    python3 evals/harness/paging_check.py --binary BIN --project P --tool search_graph \\
        --args '{"label": "Function"}' --page 5 [--max-pages 500]
    python3 evals/harness/paging_check.py --binary BIN --project P --tool query_graph \\
        --args '{"query": "MATCH (m:Method) RETURN m.qualified_name ORDER BY m.qualified_name"}' --page 1000
    python3 evals/harness/paging_check.py --binary BIN --project P --tool trace_path \\
        --args '{"function_name": "hub", "direction": "inbound", "depth": 1}' --page 5

search_graph pages with offset/limit, query_graph and trace_path with
next_cursor. The identity of a row is its qn (search_graph, trace_path) or
the whole row (query_graph). Prints one JSON line and "pages_ok" when:
  - no identity appears on two pages,
  - the union size equals the reported total (when the total is exact),
  - every page but the last is full, and has_more is false only on the last.
Exit 1 otherwise.
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from assertions import flatten_table, get_path  # noqa: E402
from mcp_client import Mcp, reply_structured, reply_text  # noqa: E402


def rows_of(tool, s):
    if tool == "search_graph":
        cols, rows = flatten_table(s)
        qi = cols.index("qn") if "qn" in cols else None
        return [r[qi] if qi is not None else json.dumps(r) for r in rows]
    if tool == "trace_path":
        table = s.get("callers") or s.get("callees") or s.get("paths") or s
        cols, rows = flatten_table(table)
        qi = cols.index("qn") if "qn" in cols else None
        return [r[qi] if qi is not None else json.dumps(r) for r in rows]
    cols, rows = flatten_table(s)
    return [json.dumps(r, ensure_ascii=False) for r in rows]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--binary", required=True)
    ap.add_argument("--project", required=True)
    ap.add_argument("--tool", default="search_graph")
    ap.add_argument("--args", default="{}")
    ap.add_argument("--page", type=int, default=5)
    ap.add_argument("--max-pages", type=int, default=500)
    ap.add_argument("--wait", type=int, default=300)
    ap.add_argument("--budget", type=int, default=1000000, help="max_output_tokens per page (default: effectively unbounded)")
    a = ap.parse_args()
    base = json.loads(a.args)
    base.update(project=a.project, format="json", max_output_tokens=a.budget)
    m = Mcp(a.binary)
    seen = {}
    pages = 0
    total = None
    total_exact = True
    problems = []
    cursor = None
    offset = 0
    while pages < a.max_pages:
        args = dict(base)
        if a.tool == "search_graph":
            args.update(limit=a.page, offset=offset)
        elif a.tool == "query_graph":
            args["max_rows"] = a.page
            if cursor:
                args["cursor"] = cursor
        else:
            args["limit"] = a.page
            if cursor:
                args["cursor"] = cursor
        status, reply = m.call(a.tool, args, a.wait)
        if status != "ok":
            problems.append(f"page {pages}: {status} {reply_text(reply)[:200]}")
            break
        s = reply_structured(reply) or {}
        ids = rows_of(a.tool, s)
        for i in ids:
            if i in seen:
                problems.append(f"duplicate {i!r} on pages {seen[i]} and {pages}")
            seen[i] = pages
        t = s.get("total", s.get("callers_total", s.get("callees_total")))
        rel = s.get("total_relation", s.get("callers_total_relation", "eq"))
        if t is not None:
            total = t
            total_exact = total_exact and rel == "eq"
        has_more = s.get("has_more", s.get("callers_has_more", s.get("callees_has_more")))
        nxt = s.get("next_cursor") or s.get("next") or (s.get("callers") or {}).get("next_cursor") \
            if isinstance(s.get("callers"), dict) else s.get("next_cursor") or s.get("next")
        pages += 1
        if not ids:
            if has_more:
                problems.append(f"page {pages - 1}: empty page but has_more=true")
            break
        budget_cut = s.get("truncation_reason") not in (None, "page_limit")
        if has_more and len(ids) < a.page and not budget_cut:
            problems.append(f"page {pages - 1}: short page ({len(ids)}<{a.page}) but has_more=true")
        if a.tool == "search_graph":
            # follow the server's continuation, never assume a full page was returned
            offset = s.get("next_offset", offset + len(ids))
            if not has_more:
                break
        else:
            if not nxt or has_more is False:
                break
            cursor = nxt
    m.close()
    if total is not None and total_exact and len(seen) != total:
        problems.append(f"union {len(seen)} != total {total}")
    out = {"tool": a.tool, "pages": pages, "unique_rows": len(seen), "total": total, "total_exact": total_exact,
           "problems": problems}
    print(json.dumps(out, ensure_ascii=False))
    print("pages_ok" if not problems else "pages_bad")
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())

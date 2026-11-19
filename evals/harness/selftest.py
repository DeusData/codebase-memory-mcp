#!/usr/bin/env python3
"""Self-test of the assertion DSL and the table flattening, with no server.

    python3 evals/harness/selftest.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import assertions as A  # noqa: E402

SEARCH_GRAPH = {
    "cols": ["name", "label", "lines", "in", "out"],
    "groups": [
        {"qn_prefix": "p.hub.hub", "file": "hub/hub.py", "rows": [["hub", "Function", "4-6", 12, 0], ["caller_01", "Function", "9-10", 0, 1]]},
        {"qn_prefix": "", "file": None, "rows": [["root", "Project", "", 0, 0]]},
    ],
    "total": 3, "returned": 3, "has_more": False,
    "graph_backend": {"kind": "neo4j", "status": "serving", "served_by": "neo4j", "remote_queries": 1},
}
QUERY_GRAPH = {"columns": ["c.name", "count(c)"], "rows": [["hub", 12], ["unused_helper", 0]], "returned": 2, "total": 2}
PROJECTS = {"projects": [{"name": "a", "root_path": "/a"}, {"name": "b"}], "total": 2}


def ok(name, cond):
    print(("PASS " if cond else "FAIL ") + name)
    return cond


def main():
    good = True
    cols, rows = A.flatten_table(SEARCH_GRAPH)
    good &= ok("grouped table adds qn/file", cols[-2:] == ["qn", "file"] and rows[0][-2] == "p.hub.hub.hub" and rows[2][-2] == "root")
    rec = {"status": "ok", "text": "graph_backend: neo4j", "structured": SEARCH_GRAPH, "ms": 120}
    good &= ok("rows includes/count/unique", not A.check({"rows": {"table": "", "col": "name", "includes": ["hub", "caller_01"], "count_eq": 3, "unique": "qn"}}, rec))
    good &= ok("rows excludes fails when present", bool(A.check({"rows": {"table": "", "col": "name", "excludes": "hub"}}, rec)))
    good &= ok("numeric col includes 12", not A.check({"rows": {"table": "", "col": "in", "includes": 12}}, rec))
    good &= ok("served_by neo4j", not A.check({"served_by": "neo4j"}, rec))
    good &= ok("served_by sqlite fails on neo4j reply", bool(A.check({"served_by": "sqlite"}, rec)))
    good &= ok("json ops", not A.check({"json": [{"path": "total", "op": "gte", "value": 3}, {"path": "has_more", "op": "eq", "value": False},
                                                 {"path": "graph_backend.remote_queries", "op": "gte", "value": 1}, {"path": "nope", "op": "missing"}]}, rec))
    good &= ok("json missing path fails", bool(A.check({"json": [{"path": "nope", "op": "eq", "value": 1}]}, rec)))
    good &= ok("deep_contains", not A.check({"deep_contains": ["hub/hub.py", "caller_01"], "deep_not_contains": "vendor"}, rec))
    good &= ok("ms_under", not A.check({"ms_under": 200}, rec) and bool(A.check({"ms_under": 100}, rec)))
    good &= ok("ok:false mismatch", bool(A.check({"ok": False}, rec)))
    good &= ok("any_of", not A.check({"any_of": [{"ok": False}, {"rows": {"table": "", "count_eq": 3}}]}, rec))
    good &= ok("any_of all fail", bool(A.check({"any_of": [{"ok": False}, {"rows": {"table": "", "count_eq": 99}}]}, rec)))
    rec2 = {"status": "ok", "text": "", "structured": QUERY_GRAPH, "ms": 5}
    good &= ok("contains_row with dotted col", not A.check({"rows": {"table": "", "contains_row": [{"c.name": "hub", "count(c)": 12}]}}, rec2))
    good &= ok("rows col 'name' tolerates 'c.name'", not A.check({"rows": {"table": "", "col": "name", "includes": "hub"}}, rec2))
    good &= ok("json rows eq list", not A.check({"json": [{"path": "rows.0", "op": "eq", "value": ["hub", 12]}]}, rec2))
    rec3 = {"status": "ok", "text": "", "structured": PROJECTS, "ms": 5}
    good &= ok("list-of-dicts table", not A.check({"rows": {"table": "projects", "col": "name", "includes": ["a", "b"], "excludes": "c"}}, rec3))
    rec4 = {"status": "ok", "rows": [["cbm_node_db", "ONLINE"], ["cbm_node_id", "ONLINE"]], "columns": ["name", "state"],
            "plan": {"operators": ["ProduceResults@neo4j", "NodeIndexSeek@neo4j"], "details": "RANGE INDEX n:CbmNode(db, gen, label) WHERE db = $db"}}
    good &= ok("cypher rows/cells/plan", not A.check({"rows_eq": 2, "cell_contains": ["cbm_node_db", "ONLINE"], "cell_not_contains": "FAILED",
                                                      "plan_has_operator": "NodeIndexSeek", "plan_uses_index": "CbmNode(db, gen, label)",
                                                      "plan_not_operator": ["NodeByLabelScan", "Sort@"]}, rec4))
    good &= ok("row_eq", not A.check({"row_eq": [["cbm_node_db", "ONLINE"], ["cbm_node_id", "ONLINE"]]}, rec4))
    rec5 = {"status": "error", "text": "search_graph: this project is Neo4j-backed and Neo4j cannot serve it",
            "structured": {"error": "x", "graph_backend": {"kind": "neo4j", "status": "unavailable", "reason": "no response"}}, "ms": 900}
    good &= ok("error with structured status", not A.check({"ok": False, "ms_under": 20000, "error_contains": "neo4j",
                                                            "json": [{"path": "graph_backend.status", "op": "eq", "value": "unavailable"}]}, rec5))
    rec6 = {"status": "ok", "rc": 0, "stdout": "pages_ok\n", "stderr": ""}
    good &= ok("shell stdout", not A.check({"rc": 0, "stdout_contains": "pages_ok", "stdout_regex": "^pages"}, rec6))
    print("selftest_ok" if good else "selftest_bad")
    return 0 if good else 1


if __name__ == "__main__":
    sys.exit(main())

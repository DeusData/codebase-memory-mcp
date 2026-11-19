"""Neo4j HTTP API helper for the evals (no driver needed).

Uses the transactional endpoint `POST /db/<database>/tx/commit`, the same
transport the product uses. Statements that start with EXPLAIN or PROFILE
return a plan, which `flatten_plan` reduces to the operator list and the
details text that `assertions.py` checks (`plan_has_operator`,
`plan_uses_index`).

    from neo4j_http import Neo4j
    n = Neo4j("http://localhost:7474", "neo4j", password, "neo4j")
    cols, rows, plan = n.run("SHOW INDEXES YIELD name, state RETURN name, state")
"""
import base64
import json
import urllib.error
import urllib.request


class Neo4jError(Exception):
    pass


class Neo4j:
    def __init__(self, uri, user, password, database="neo4j", timeout=120):
        self.uri = uri.rstrip("/")
        self.database = database or "neo4j"
        self.timeout = timeout
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Basic {token}",
        }

    def run(self, statement, params=None):
        """Run one statement. Returns (columns, rows, plan|None). Raises Neo4jError."""
        body = {"statements": [{"statement": statement, "parameters": params or {}}]}
        req = urllib.request.Request(
            f"{self.uri}/db/{self.database}/tx/commit",
            data=json.dumps(body).encode("utf-8"),
            headers=self.headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            raise Neo4jError(f"HTTP {e.code}: {e.read()[:300]!r}") from e
        except urllib.error.URLError as e:
            raise Neo4jError(f"connect: {e.reason}") from e
        errors = payload.get("errors") or []
        if errors:
            raise Neo4jError("; ".join(f"{x.get('code')}: {x.get('message')}" for x in errors))
        results = payload.get("results") or []
        if not results:
            return [], [], None
        r = results[0]
        cols = r.get("columns") or []
        rows = [d.get("row") for d in r.get("data") or []]
        plan = r.get("plan") or r.get("profile")
        return cols, rows, (flatten_plan(plan) if plan else None)

    def ping(self):
        self.run("RETURN 1")
        return True


def flatten_plan(plan):
    """Reduce a Neo4j plan tree to {"operators": [...], "details": "..."}."""
    root = plan.get("root", plan)
    ops = []
    details = []

    def walk(node):
        if not isinstance(node, dict):
            return
        op = node.get("operatorType")
        if op:
            ops.append(op)
        args = node.get("args") or {}
        for key in ("Details", "details", "Index", "index", "IndexName", "LabelName"):
            v = args.get(key)
            if v:
                details.append(str(v))
        for child in node.get("children") or []:
            walk(child)

    walk(root)
    # Neo4j 5's HTTP API puts index names only in the rendered plan table.
    for node in (plan, root):
        text = node.get("string-representation") if isinstance(node, dict) else None
        if text:
            details.append(text)
    return {"operators": ops, "details": "\n".join(details)}


INDEX_NAMES = [
    "cbm_node_db",
    "cbm_node_id",
    "cbm_node_qn",
    "cbm_node_name",
    "cbm_node_file",
    "cbm_node_label",
]
CONSTRAINT_NAME = "cbm_project_name"


def show_indexes(n):
    """{name: {state, type, labels, properties, population}} for every index."""
    cols, rows, _ = n.run(
        "SHOW INDEXES YIELD name, state, type, labelsOrTypes, properties, populationPercent "
        "RETURN name, state, type, labelsOrTypes, properties, populationPercent"
    )
    out = {}
    for name, state, typ, labels, props, pop in rows:
        out[name] = {
            "state": state,
            "type": typ,
            "labels": labels,
            "properties": props,
            "population": pop,
        }
    return out


if __name__ == "__main__":
    import os
    import sys

    uri = os.environ.get("CBM_EVAL_NEO4J_URI", "http://localhost:7474")
    pw = os.environ.get("NEO4J_PASSWORD", "")
    n = Neo4j(uri, os.environ.get("NEO4J_USER", "neo4j"), pw, os.environ.get("NEO4J_DATABASE", "neo4j"))
    stmt = " ".join(sys.argv[1:]) or "SHOW INDEXES YIELD name, state RETURN name, state"
    cols, rows, plan = n.run(stmt)
    print(json.dumps({"columns": cols, "rows": rows, "plan": plan}, indent=2))

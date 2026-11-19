"""The `expect` block of an eval case, evaluated against one step's record.

A record is the dict `run_cases.py` writes to the results JSONL:

    {"id", "kind", "status" (ok|error|HANG|EXIT|skipped), "ms",
     "text", "structured", "stdout", "stderr", "rc", "rows", "columns", "plan"}

Every key of `expect` is one check; all checks must hold for the step to pass.
Checks return a list of failure strings (empty = pass).

Tool-reply checks
  ok: true|false                 reply must (not) be an error
  error_contains: str|[str]      error text contains each substring (case-insensitive)
  text_contains / text_not_contains: str|[str]
  text_regex: str
  ms_under: int                  wall time of the call in milliseconds
  served_by: "neo4j"|"neo4j+local"|"sqlite"
                                 structured.graph_backend.served_by; "sqlite" means
                                 the reply has no graph_backend.served_by, or kind sqlite
  json: [{path, op, value}]      path is dotted into `structured` ("a.b.0.c");
                                 op: eq ne gt gte lt lte contains not_contains in
                                     exists missing regex len_eq len_gte len_lte truthy falsy
  deep_contains / deep_not_contains: str|[str]
                                 substring of ANY string leaf anywhere in `structured`
  rows: {...}                    see rows_check(); understands the {cols, rows},
                                 {columns, rows} and {cols, groups:[{qn_prefix,file,rows}]}
                                 table shapes every graph tool uses in json format
  same_as: [...]                 evaluated by run_cases.py (needs earlier records);
                                 optional "factor": this == prev * factor, "tolerance": abs diff
  any_of: [expect, expect]       passes when any alternative passes (e.g. error OR empty)
  unordered: true                hint for compare_backends.py: sort rows before comparing

Shell / cypher / sqlite step checks
  rc: int
  stdout_contains / stdout_not_contains / stdout_regex
  rows_eq / rows_gte / rows_lte: int    number of result rows
  cell_contains: str|[str]              substring of any cell
  row_eq: [[...]]                       exact rows (order-sensitive)
  plan_has_operator: str|[str]          EXPLAIN/PROFILE plan contains the operator
  plan_uses_index: str|[str]            plan mentions the index name (details text)
  plan_not_operator: str|[str]
"""
import json
import re


def _as_list(v):
    if v is None:
        return []
    return v if isinstance(v, list) else [v]


def get_path(obj, path):
    """Dotted path lookup; list indexes are integers. Returns (found, value)."""
    cur = obj
    if path in ("", None):
        return True, cur
    for part in str(path).split("."):
        if isinstance(cur, dict):
            if part not in cur:
                return False, None
            cur = cur[part]
        elif isinstance(cur, list):
            try:
                cur = cur[int(part)]
            except (ValueError, IndexError):
                return False, None
        else:
            return False, None
    return True, cur


def walk_strings(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield str(k)
            yield from walk_strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from walk_strings(v)
    elif isinstance(obj, str):
        yield obj
    elif obj is not None:
        yield str(obj)


def flatten_table(table):
    """Return (cols, rows) from any of the tool table shapes.

    {cols|columns, rows}                      -> as is
    {cols, groups:[{qn_prefix, file, rows}]}  -> rows get qn/file columns added
    list of dicts                             -> union of keys as cols
    """
    if isinstance(table, list):
        if table and isinstance(table[0], dict):
            cols = []
            for r in table:
                for k in r:
                    if k not in cols:
                        cols.append(k)
            return cols, [[r.get(c) for c in cols] for r in table]
        return [], [r if isinstance(r, list) else [r] for r in table]
    if not isinstance(table, dict):
        return [], []
    cols = list(table.get("cols") or table.get("columns") or [])
    if "groups" in table:
        out = []
        extra = ["qn", "file"]
        for g in table.get("groups") or []:
            prefix = g.get("qn_prefix", "") or ""
            gfile = g.get("file")
            for row in g.get("rows") or []:
                row = list(row)
                name = row[0] if row else ""
                qn = name if prefix == "" else f"{prefix}.{name}"
                out.append(row + [qn, gfile])
        return cols + extra, out
    rows = table.get("rows") or []
    return cols, [list(r) if isinstance(r, list) else [r] for r in rows]


def _col_index(cols, col):
    if isinstance(col, int):
        return col
    if col in cols:
        return cols.index(col)
    # tolerate "n.name" vs "name"
    for i, c in enumerate(cols):
        if c.split(".")[-1] == col:
            return i
    return None


def rows_check(spec, structured):
    """rows: {table: path, col: name, includes: [], excludes: [], count_eq/gte/lte: n,
             unique: col, all_match: regex, any_match: regex, contains_row: {col: v, ...}}"""
    fails = []
    found, table = get_path(structured, spec.get("table", ""))
    if not found:
        return [f"rows: table path {spec.get('table')!r} not found"]
    cols, rows = flatten_table(table)
    n = len(rows)
    for key, op in (("count_eq", "=="), ("count_gte", ">="), ("count_lte", "<=")):
        if key in spec:
            want = spec[key]
            ok = {"==": n == want, ">=": n >= want, "<=": n <= want}[op]
            if not ok:
                fails.append(f"rows: count {n} not {op} {want}")
    col = spec.get("col")
    values = None
    if col is not None:
        ci = _col_index(cols, col)
        if ci is None and not rows:
            ci = -1  # empty table: no columns to name; includes fail below, excludes hold
        elif ci is None:
            return fails + [f"rows: column {col!r} not in {cols}"]
        values = [r[ci] if 0 <= ci < len(r) else None for r in rows]
        sval = [str(v) for v in values]
        for want in _as_list(spec.get("includes")):
            if str(want) not in sval:
                fails.append(f"rows: {col}={want!r} missing (have {sval[:12]}...)")
        for bad in _as_list(spec.get("excludes")):
            if str(bad) in sval:
                fails.append(f"rows: {col}={bad!r} present but excluded")
        if "all_match" in spec:
            rx = re.compile(spec["all_match"])
            for v in sval:
                if not rx.search(v):
                    fails.append(f"rows: {col}={v!r} does not match {spec['all_match']!r}")
                    break
        if "any_match" in spec:
            rx = re.compile(spec["any_match"])
            if not any(rx.search(v) for v in sval):
                fails.append(f"rows: no {col} matches {spec['any_match']!r}")
    uniq = spec.get("unique")
    if uniq is not None:
        ci = _col_index(cols, uniq)
        if ci is None:
            fails.append(f"rows: unique column {uniq!r} not in {cols}")
        else:
            seen = [str(r[ci]) for r in rows if ci < len(r)]
            if len(seen) != len(set(seen)):
                dupes = sorted({s for s in seen if seen.count(s) > 1})
                fails.append(f"rows: duplicate {uniq} values {dupes[:5]}")
    for want_row in _as_list(spec.get("contains_row")):
        hit = False
        for r in rows:
            match = True
            for k, v in want_row.items():
                ci = _col_index(cols, k)
                if ci is None or ci >= len(r) or str(r[ci]) != str(v):
                    match = False
                    break
            if match:
                hit = True
                break
        if not hit:
            fails.append(f"rows: no row with {want_row}")
    return fails


def _cmp(op, actual, want):
    if op == "eq":
        return actual == want or str(actual) == str(want)
    if op == "ne":
        return not (actual == want or str(actual) == str(want))
    if op in ("gt", "gte", "lt", "lte"):
        try:
            a, w = float(actual), float(want)
        except (TypeError, ValueError):
            return False
        return {"gt": a > w, "gte": a >= w, "lt": a < w, "lte": a <= w}[op]
    if op == "contains":
        if isinstance(actual, (list, dict)):
            return want in actual
        return str(want) in str(actual)
    if op == "not_contains":
        if isinstance(actual, (list, dict)):
            return want not in actual
        return str(want) not in str(actual)
    if op == "in":
        return actual in want
    if op == "regex":
        return re.search(str(want), str(actual)) is not None
    if op == "len_eq":
        return len(actual) == want
    if op == "len_gte":
        return len(actual) >= want
    if op == "len_lte":
        return len(actual) <= want
    if op == "truthy":
        return bool(actual)
    if op == "falsy":
        return not bool(actual)
    raise ValueError(f"unknown op {op}")


def check(expect, rec, backend=None):
    """Evaluate every check in `expect` against record `rec`. Returns failures."""
    fails = []
    if not expect:
        return fails
    if "any_of" in expect:
        # passes when at least one alternative passes; reports the first one's reasons otherwise
        alternatives = [check(alt, rec, backend) for alt in expect["any_of"]]
        if not any(not f for f in alternatives):
            fails.append("any_of: no alternative held: " + " | ".join("; ".join(f) for f in alternatives))
        rest = {k: v for k, v in expect.items() if k != "any_of"}
        return fails + check(rest, rec, backend)
    status = rec.get("status")
    text = rec.get("text") or ""
    structured = rec.get("structured")
    low = text.lower()

    if "ok" in expect:
        want_ok = bool(expect["ok"])
        is_ok = status == "ok"
        if status in ("HANG", "EXIT"):
            fails.append(f"call {status}")
        elif want_ok != is_ok:
            fails.append(f"expected {'success' if want_ok else 'an error'}, got {status}: {text[:200]!r}")
    for s in _as_list(expect.get("error_contains")):
        if s.lower() not in low:
            fails.append(f"error text lacks {s!r}: {text[:200]!r}")
    for s in _as_list(expect.get("text_contains")):
        if s not in text:
            fails.append(f"text lacks {s!r}")
    for s in _as_list(expect.get("text_not_contains")):
        if s in text:
            fails.append(f"text contains forbidden {s!r}")
    if "text_regex" in expect and not re.search(expect["text_regex"], text, re.S):
        fails.append(f"text does not match {expect['text_regex']!r}")
    if "ms_under" in expect and rec.get("ms") is not None and rec["ms"] >= expect["ms_under"]:
        fails.append(f"took {rec['ms']} ms, limit {expect['ms_under']} ms")

    if "served_by" in expect:
        want = expect["served_by"]
        gb = (structured or {}).get("graph_backend") if isinstance(structured, dict) else None
        got = (gb or {}).get("served_by")
        kind = (gb or {}).get("kind")
        if want == "sqlite":
            if got not in (None, "sqlite", "local") and kind != "sqlite":
                fails.append(f"served_by {got!r}, expected a local SQLite answer")
        elif got != want:
            fails.append(f"served_by {got!r}, expected {want!r}")

    for item in _as_list(expect.get("json")):
        found, val = get_path(structured, item.get("path", ""))
        op = item.get("op", "eq")
        if op == "exists":
            if not found:
                fails.append(f"json: {item['path']} missing")
            continue
        if op == "missing":
            if found:
                fails.append(f"json: {item['path']} present ({val!r})")
            continue
        if not found:
            fails.append(f"json: {item['path']} missing")
            continue
        try:
            if not _cmp(op, val, item.get("value")):
                fails.append(f"json: {item['path']}={val!r} not {op} {item.get('value')!r}")
        except Exception as e:  # noqa: BLE001
            fails.append(f"json: {item['path']} {op}: {e}")

    if expect.get("deep_contains") or expect.get("deep_not_contains"):
        leaves = list(walk_strings(structured)) if structured is not None else []
        blob = "\n".join(leaves)
        for s in _as_list(expect.get("deep_contains")):
            if s not in blob:
                fails.append(f"deep: {s!r} not found anywhere in structured output")
        for s in _as_list(expect.get("deep_not_contains")):
            if s in blob:
                fails.append(f"deep: forbidden {s!r} found in structured output")

    for spec in _as_list(expect.get("rows")):
        if structured is None:
            fails.append("rows: no structured output")
        else:
            fails.extend(rows_check(spec, structured))

    # shell / cypher / sqlite
    if "rc" in expect and rec.get("rc") != expect["rc"]:
        fails.append(f"rc {rec.get('rc')} != {expect['rc']}: {(rec.get('stderr') or '')[-300:]}")
    out = rec.get("stdout") or ""
    for s in _as_list(expect.get("stdout_contains")):
        if s not in out:
            fails.append(f"stdout lacks {s!r}")
    for s in _as_list(expect.get("stdout_not_contains")):
        if s in out:
            fails.append(f"stdout contains forbidden {s!r}")
    if "stdout_regex" in expect and not re.search(expect["stdout_regex"], out, re.S):
        fails.append(f"stdout does not match {expect['stdout_regex']!r}")

    rows = rec.get("rows")
    if rows is not None:
        n = len(rows)
        if "rows_eq" in expect and n != expect["rows_eq"]:
            fails.append(f"{n} rows != {expect['rows_eq']}")
        if "rows_gte" in expect and n < expect["rows_gte"]:
            fails.append(f"{n} rows < {expect['rows_gte']}")
        if "rows_lte" in expect and n > expect["rows_lte"]:
            fails.append(f"{n} rows > {expect['rows_lte']}")
        cells = "\n".join(str(c) for r in rows for c in (r if isinstance(r, list) else [r]))
        for s in _as_list(expect.get("cell_contains")):
            if s not in cells:
                fails.append(f"no cell contains {s!r}")
        for s in _as_list(expect.get("cell_not_contains")):
            if s in cells:
                fails.append(f"a cell contains forbidden {s!r}")
        if "row_eq" in expect and [list(r) for r in rows] != expect["row_eq"]:
            fails.append(f"rows {json.dumps(rows)[:300]} != {json.dumps(expect['row_eq'])[:300]}")
    elif any(k in expect for k in ("rows_eq", "rows_gte", "rows_lte", "cell_contains", "row_eq")):
        fails.append("no rows in record (step failed?)")

    plan = rec.get("plan")
    if any(k in expect for k in ("plan_has_operator", "plan_uses_index", "plan_not_operator", "plan_db_hits_lte")):
        if not plan:
            fails.append("no query plan in record (statement needs EXPLAIN/PROFILE)")
        else:
            ops = plan.get("operators", [])
            details = plan.get("details", "")
            for o in _as_list(expect.get("plan_has_operator")):
                if not any(o in op for op in ops):
                    fails.append(f"plan lacks operator {o!r}: {ops}")
            for o in _as_list(expect.get("plan_not_operator")):
                # exact operator name ("Sort" does not match "PartialSort"); "@neo4j" suffix optional
                if any(op.split("@")[0] == o.split("@")[0] for op in ops):
                    fails.append(f"plan has forbidden operator {o!r}: {ops}")
            for idx in _as_list(expect.get("plan_uses_index")):
                if idx not in details:
                    fails.append(f"plan does not mention index {idx!r}")
            if "plan_db_hits_lte" in expect:
                m = re.search(r"Total database accesses:\s*(\d+)", details)
                if not m:
                    fails.append("plan has no 'Total database accesses' (statement needs PROFILE)")
                elif int(m.group(1)) > expect["plan_db_hits_lte"]:
                    fails.append(f"{m.group(1)} database accesses > {expect['plan_db_hits_lte']}")
    return fails

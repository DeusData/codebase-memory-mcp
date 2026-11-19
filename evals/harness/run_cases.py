#!/usr/bin/env python3
"""Run one eval case file against one backend profile.

    python3 evals/harness/run_cases.py \\
        --profile evals/profiles/neo4j.json \\
        --cases   evals/cases/common/10_search_graph_structural.json \\
        --out     evals/results/neo4j/10_search_graph_structural.jsonl \\
        [--binary PATH] [--var key=value ...] [--only ID,ID] [--golden DIR] [--stop-on-fail]

Case file
  {"suite": "SG", "title": "...", "spec": "specs/05_search_graph.md",
   "backends": ["sqlite", "neo4j"],         # profiles this file applies to
   "cases": [ step, step, ... ]}

Step kinds (one per object; `id`, `tier`, `title`, `expect`, `skip_if` are common)
  tool     {"tool": "search_graph", "args": {...}, "wait": 120}
  shell    {"shell": "cmd ...", "wait": 300}            runs through the shell; cwd = repo root
  cypher   {"cypher": "MATCH ...", "params": {...}}     Neo4j HTTP (profile's graph_db / credentials)
  sqlite   {"sqlite": "SELECT ...", "db": "{{db_path}}"}  python sqlite3, read-only
  fs       {"fs": "write|append|delete|mkdir|copy", "path": "...", "content": "...", "src": "..."}
                                                        path relative to {{repo}}
  session  {"session": "restart", "env": {...}, "daemon_stop": true}
                                                        new MCP session; daemon_stop runs
                                                        `<binary> daemon stop` first so the env
                                                        reaches the daemon too
  sleep    {"sleep": 2}
  note     {"note": "..."}                               recorded, never fails

Cross-step fields (any step)
  use_prev: [{"step": "ID", "path": "structured.next_cursor", "arg": "cursor"}]
           copies a value from an earlier step's record into this step's args
           (tool steps) or params (cypher steps) before it runs
  expect.same_as: [{"step": "ID", "path": "structured.total", "this_path": "rows.0.0",
                    "tolerance": 0}]
           this step's value must equal the earlier step's value (numeric tolerance
           optional); paths are evaluated on the result records themselves

Templates: every string in a step may use {{name}} from the profile's `vars`,
from --var, and from the built-ins {{project}} {{repo}} {{binary}} {{cache_dir}}
{{db_path}} {{backend}} {{LONG_NAME}} {{fixture_dir}} {{evals_dir}}.

Results: one JSON line per step with the fields assertions.py documents, plus
"pass", "fails", "tier". Passwords are masked. Exit code 1 when any tier-1
step fails, 2 when the session could not start.
"""
import argparse
import datetime as dt
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
EVALS = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import assertions  # noqa: E402
from mcp_client import Mcp, McpError, reply_structured, reply_text  # noqa: E402

LONG_NAME = ("very_long_function_name_" + "0" * 200)[:200]
TEMPLATE = re.compile(r"\{\{\s*([A-Za-z0-9_]+)\s*\}\}")


def render(obj, vars_):
    if isinstance(obj, str):
        def sub(m):
            key = m.group(1)
            if key not in vars_:
                raise KeyError(f"unknown template variable {{{{{key}}}}}")
            return str(vars_[key])
        return TEMPLATE.sub(sub, obj)
    if isinstance(obj, list):
        return [render(x, vars_) for x in obj]
    if isinstance(obj, dict):
        return {k: render(v, vars_) for k, v in obj.items()}
    return obj


def mask(obj, secrets):
    if not secrets:
        return obj
    if isinstance(obj, str):
        for s in secrets:
            if s:
                obj = obj.replace(s, "***")
        return obj
    if isinstance(obj, list):
        return [mask(x, secrets) for x in obj]
    if isinstance(obj, dict):
        return {k: ("***" if k in ("graph_password", "password") else mask(v, secrets)) for k, v in obj.items()}
    return obj


def load_profile(path):
    with open(path, encoding="utf-8") as f:
        p = json.load(f)
    p.setdefault("vars", {})
    # environment overrides for secrets so they never need to live in the file
    pw_env = p.get("neo4j_password_env", "NEO4J_PASSWORD")
    if p.get("backend") == "neo4j" and not p["vars"].get("neo4j_password"):
        p["vars"]["neo4j_password"] = os.environ.get(pw_env, "")
    return p


def skip_reason(step, vars_):
    cond = step.get("skip_if")
    if not cond:
        return None
    if isinstance(cond, str):
        cond = [cond]
    for c in cond:
        if c == "sqlite" and vars_.get("backend") == "sqlite":
            return "not for sqlite"
        if c == "neo4j" and vars_.get("backend") == "neo4j":
            return "not for neo4j"
        if c == "windows" and os.name == "nt":
            return "not on windows"
        if c == "posix" and os.name != "nt":
            return "not on posix"
        if c.startswith("no_var:") and not vars_.get(c.split(":", 1)[1]):
            return f"variable {c.split(':', 1)[1]} not set"
        if c.startswith("mode!=") and vars_.get("index_mode", "full") != c.split("=", 1)[1]:
            return f"index_mode is not {c.split('=', 1)[1]}"
    return None


class Runner:
    def __init__(self, a):
        self.a = a
        self.profile = load_profile(a.profile)
        self.backend = self.profile["backend"]
        self.binary = a.binary or self.profile.get("binary") or "codebase-memory-mcp-neo4j"
        v = dict(self.profile["vars"])
        for kv in a.var or []:
            k, _, val = kv.partition("=")
            v[k] = val
        v.setdefault("backend", self.backend)
        v.setdefault("binary", self.binary)
        v.setdefault("evals_dir", EVALS)
        v.setdefault("fixture_dir", os.path.join(EVALS, "fixtures", "eval-small"))
        v.setdefault("LONG_NAME", LONG_NAME)
        v.setdefault("python", sys.executable)
        v.setdefault("cache_dir", os.environ.get("CBM_NEO4J_CACHE_DIR")
                     or os.path.join(os.path.expanduser("~"), ".cache", "codebase-memory-mcp-neo4j"))
        v.setdefault("db_path", os.path.join(v["cache_dir"], f"{v.get('project', 'eval-small')}.db"))
        self.vars = v
        self.secrets = [v.get("neo4j_password", "")]
        self.env = dict(self.profile.get("env") or {})
        if self.backend == "neo4j" and v.get("neo4j_password"):
            self.env.setdefault("NEO4J_PASSWORD", v["neo4j_password"])
        self.mcp = None
        self.neo = None
        self.records = {}
        os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
        self.out = open(os.devnull if a.dry_run else a.out, "w", encoding="utf-8")
        self.stderr_path = a.out + ".stderr"
        self.summary = {"pass": 0, "fail": 0, "skip": 0, "tier1_fail": 0, "ids_failed": []}

    # ── sessions ────────────────────────────────────────────────
    def start_session(self, extra_env=None, daemon_stop=False):
        if self.mcp:
            self.mcp.close()
            self.mcp = None
        if daemon_stop:
            subprocess.run([self.binary, "daemon", "stop"], capture_output=True, timeout=120,
                           env={**os.environ, **self.env})
            time.sleep(1)
        env = dict(self.env)
        if extra_env:
            for k, val in render(extra_env, self.vars).items():
                if val is None:  # null unsets the variable for the new session
                    env.pop(k, None)
                    os.environ.pop(k, None)
                else:
                    env[k] = str(val)
            self.env = env
        self.mcp = Mcp(self.binary, env=env, stderr_path=self.stderr_path)

    def neo4j(self):
        if self.neo is None:
            from neo4j_http import Neo4j
            self.neo = Neo4j(self.vars["graph_db"], self.vars.get("neo4j_user", "neo4j"),
                             self.vars.get("neo4j_password", ""), self.vars.get("neo4j_database", "neo4j"))
        return self.neo

    # ── steps ───────────────────────────────────────────────────
    def run_step(self, step):
        rec = {"id": step.get("id"), "tier": step.get("tier", 2), "title": step.get("title", "")}
        reason = skip_reason(step, self.vars)
        if reason:
            rec.update(kind="skip", status="skipped", reason=reason)
            return rec
        step = render(step, self.vars)
        t0 = time.time()
        pre_fails = []
        for up in step.get("use_prev") or []:
            prev = self.records.get(up.get("step"))
            found, val = assertions.get_path(prev, up.get("path", "")) if prev else (False, None)
            if not found:
                pre_fails.append(f"use_prev: {up.get('step')}.{up.get('path')} not available")
                continue
            target = "params" if "cypher" in step else "args"
            step.setdefault(target, {})[up["arg"]] = val
        if pre_fails:
            rec.update(kind="tool" if "tool" in step else "step", status="error", text="; ".join(pre_fails),
                       ms=0, fails=pre_fails, **{"pass": False})
            return rec
        try:
            if "tool" in step:
                rec["kind"] = "tool"
                rec["tool"] = step["tool"]
                rec["args"] = mask(step.get("args", {}), self.secrets)
                status, reply = self.mcp.call(step["tool"], step.get("args", {}), step.get("wait", 300))
                rec["status"] = status
                rec["text"] = mask(reply_text(reply), self.secrets)
                rec["structured"] = mask(reply_structured(reply), self.secrets)
                if status in ("HANG", "EXIT"):
                    self.mcp.kill()
                    self.start_session()
            elif "shell" in step:
                rec["kind"] = "shell"
                rec["cmd"] = mask(step["shell"], self.secrets)
                p = subprocess.run(step["shell"], shell=True, capture_output=True, text=True,
                                   timeout=step.get("wait", 600), cwd=step.get("cwd") or self.vars.get("repo") or None,
                                   env={**os.environ, **self.env})
                rec.update(status="ok" if p.returncode == 0 else "error", rc=p.returncode,
                           stdout=mask(p.stdout[-20000:], self.secrets), stderr=mask(p.stderr[-4000:], self.secrets))
            elif "cypher" in step:
                rec["kind"] = "cypher"
                rec["cypher"] = step["cypher"]
                try:
                    cols, rows, plan = self.neo4j().run(step["cypher"], step.get("params"))
                    rec.update(status="ok", columns=cols, rows=rows, plan=plan)
                except Exception as e:  # noqa: BLE001
                    rec.update(status="error", text=mask(str(e), self.secrets))
            elif "sqlite" in step:
                rec["kind"] = "sqlite"
                rec["sql"] = step["sqlite"]
                db = step.get("db") or self.vars["db_path"]
                try:
                    uri = "file:" + os.path.abspath(db).replace("\\", "/") + "?mode=ro"
                    con = sqlite3.connect(uri, uri=True, timeout=30)
                    try:
                        cur = con.execute(step["sqlite"])
                        rec["columns"] = [d[0] for d in cur.description] if cur.description else []
                        rec["rows"] = [list(r) for r in cur.fetchmany(step.get("max_rows", 5000))]
                        rec["status"] = "ok"
                    finally:
                        con.close()
                except Exception as e:  # noqa: BLE001
                    rec.update(status="error", text=str(e))
            elif "fs" in step:
                rec["kind"] = "fs"
                rec["status"] = "ok"
                path = os.path.join(step.get("root") or self.vars["repo"], step["path"])
                op = step["fs"]
                if op == "write":
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, "w", encoding="utf-8", newline="\n") as f:
                        f.write(step.get("content", ""))
                elif op == "append":
                    with open(path, "a", encoding="utf-8", newline="\n") as f:
                        f.write(step.get("content", ""))
                elif op == "delete":
                    if os.path.isdir(path):
                        shutil.rmtree(path)
                    elif os.path.exists(path):
                        os.remove(path)
                elif op == "mkdir":
                    os.makedirs(path, exist_ok=True)
                elif op == "copy":
                    shutil.copy2(os.path.join(self.vars["repo"], step["src"]), path)
                else:
                    rec.update(status="error", text=f"unknown fs op {op}")
            elif "session" in step:
                rec["kind"] = "session"
                self.start_session(step.get("env"), step.get("daemon_stop", False))
                rec["status"] = "ok"
            elif "sleep" in step:
                rec["kind"] = "sleep"
                time.sleep(float(step["sleep"]))
                rec["status"] = "ok"
            elif "note" in step:
                rec.update(kind="note", status="ok", text=step["note"])
            else:
                rec.update(kind="unknown", status="error", text=f"unknown step: {list(step)}")
        except subprocess.TimeoutExpired:
            rec.update(status="HANG", text="shell timeout")
        except McpError as e:
            rec.update(status="EXIT", text=str(e))
        except Exception as e:  # noqa: BLE001 - a broken step must not end the suite
            rec.update(status="error", text=f"harness: {type(e).__name__}: {e}")
        rec["ms"] = int((time.time() - t0) * 1000)
        expect = step.get("expect") or {}
        rec["fails"] = assertions.check(expect, rec, self.backend)
        same_as = expect.get("same_as") or []
        for sa in (same_as if isinstance(same_as, list) else [same_as]):
            prev = self.records.get(sa.get("step"))
            if not prev:
                rec["fails"].append(f"same_as: step {sa.get('step')} has no record")
                continue
            f1, v1 = assertions.get_path(prev, sa.get("path", ""))
            f2, v2 = assertions.get_path(rec, sa.get("this_path", sa.get("path", "")))
            if not f1 or not f2:
                rec["fails"].append(f"same_as: path missing (prev {f1}, this {f2})")
                continue
            tol = sa.get("tolerance")
            factor = sa.get("factor")
            same = str(v1) == str(v2) and factor in (None, 1)
            if not same and (tol is not None or factor is not None):
                try:
                    want = float(v1) * float(factor if factor is not None else 1)
                    same = abs(want - float(v2)) <= float(tol or 0)
                except (TypeError, ValueError):
                    same = False
            if not same:
                rec["fails"].append(f"same_as: {sa.get('step')} has {v1!r}, this step has {v2!r}")
        rec["pass"] = not rec["fails"]
        if rec.get("id"):
            self.records[rec["id"]] = rec
        return rec

    def dry_run(self, suite):
        """Render every step with the profile's variables; report unknown templates and
        malformed steps without starting a session."""
        problems = 0
        kinds = ("tool", "shell", "cypher", "sqlite", "fs", "session", "sleep", "note")
        ids = set()
        for step in suite["cases"]:
            sid = step.get("id")
            if sid in ids:
                print(f"DUP  {sid}")
                problems += 1
            ids.add(sid)
            if not any(k in step for k in kinds):
                print(f"BAD  {sid}: no step kind in {sorted(step)}")
                problems += 1
            try:
                render(step, self.vars)
            except KeyError as e:
                print(f"TMPL {sid}: {e}")
                problems += 1
            for up in step.get("use_prev") or []:
                if up.get("step") not in ids:
                    print(f"PREV {sid}: use_prev refers to {up.get('step')} which does not run before it")
                    problems += 1
            for sa in (step.get("expect") or {}).get("same_as") or []:
                if isinstance(sa, dict) and sa.get("step") not in ids:
                    print(f"SAME {sid}: same_as refers to {sa.get('step')} which does not run before it")
                    problems += 1
        print(f"dry-run {suite.get('suite')} on {self.backend}: {len(suite['cases'])} steps, {problems} problem(s)")
        return 1 if problems else 0

    def run(self):
        with open(self.a.cases, encoding="utf-8") as f:
            suite = json.load(f)
        if self.backend not in suite.get("backends", ["sqlite", "neo4j"]):
            print(f"{self.a.cases}: not for backend {self.backend}; nothing to do")
            return 0
        if self.a.dry_run:
            return self.dry_run(suite)
        only = set(self.a.only.split(",")) if self.a.only else None
        try:
            self.start_session()
        except Exception as e:  # noqa: BLE001
            print(f"cannot start MCP session with {self.binary}: {e}")
            return 2
        header = {"suite": suite.get("suite"), "title": suite.get("title"), "spec": suite.get("spec"),
                  "backend": self.backend, "binary": self.binary, "started": dt.datetime.now(dt.timezone.utc).isoformat(),
                  "server": getattr(self.mcp, "server_info", {}), "vars": mask({k: v for k, v in self.vars.items()
                                                                                if k != "neo4j_password"}, self.secrets)}
        self.out.write(json.dumps({"_header": header}, ensure_ascii=False) + "\n")
        rc = 0
        for step in suite["cases"]:
            if only and step.get("id") not in only:
                continue
            rec = self.run_step(step)
            self.out.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
            self.out.flush()
            if rec["status"] == "skipped":
                self.summary["skip"] += 1
                tag = "SKIP"
            elif rec["pass"]:
                self.summary["pass"] += 1
                tag = "PASS"
            else:
                self.summary["fail"] += 1
                self.summary["ids_failed"].append(rec["id"])
                tag = "FAIL"
                if rec.get("tier", 2) == 1:
                    self.summary["tier1_fail"] += 1
                    rc = 1
            msg = (rec.get("reason") or "; ".join(rec.get("fails") or []) or (rec.get("text") or "")[:100]).replace("\n", " ")
            print(f"{tag:4} {rec.get('id'):<14} {rec.get('ms', 0):>7} ms  {msg[:140]}", flush=True)
            if tag == "FAIL" and self.a.stop_on_fail:
                break
        self.out.write(json.dumps({"_summary": self.summary}, ensure_ascii=False) + "\n")
        self.out.close()
        if self.mcp:
            self.mcp.close()
        s = self.summary
        print(f"\n{suite.get('suite')} on {self.backend}: {s['pass']} pass, {s['fail']} fail "
              f"({s['tier1_fail']} tier-1), {s['skip']} skipped -> {self.a.out}")
        return rc


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", required=True)
    ap.add_argument("--cases", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--binary")
    ap.add_argument("--var", action="append", help="key=value template variable (repeatable)")
    ap.add_argument("--only", help="comma-separated step ids")
    ap.add_argument("--stop-on-fail", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="render and validate the steps; start nothing")
    a = ap.parse_args()
    return Runner(a).run()


if __name__ == "__main__":
    sys.exit(main())

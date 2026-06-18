#!/usr/bin/env python3
"""fablize spec ledger — a self-contained, stdlib-only locked-spec store.

Purpose (behavior only):
  - After clarifying an ambiguous task, lock the agreed spec to a ledger (.fablize/) so a
    later session (after compaction or restart) reads it instead of re-asking the user.
  - Prevents doing the same clarification — and the same work — two or three times.

Usage:
  spec.py lock --req "..." [--req ...] [--constraint "..."] [--decision "question::answer"] [--brief "..."]
  spec.py show     # first command when resuming — prints the locked spec
State directory: ./.fablize/ (run from the repo root)
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

DIR = Path(".fablize")
SPEC = DIR / "spec.json"
LEDGER = DIR / "ledger.jsonl"
GLOBAL_LOG = Path.home() / ".fablize" / "events.jsonl"


def now():
    return datetime.now(timezone.utc).isoformat()


def log(event, **kw):
    DIR.mkdir(exist_ok=True)
    rec = {"ts": now(), "event": event, **kw}
    with open(LEDGER, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    try:
        GLOBAL_LOG.parent.mkdir(exist_ok=True)
        with open(GLOBAL_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps({**rec, "tool": "spec", "cwd": str(Path.cwd())}, ensure_ascii=False) + "\n")
    except OSError:
        pass


def cmd_lock(a):
    reqs = [r.strip() for r in a.req if r.strip()]
    if not reqs and not a.decision and not a.constraint:
        sys.exit("fablize: lock needs at least one --req, --constraint, or --decision.")
    decisions = []
    for d in a.decision:
        if "::" not in d:
            sys.exit(f"fablize: --decision format is 'question::answer' — invalid: {d}")
        q, ans = d.split("::", 1)
        decisions.append({"question": q.strip(), "answer": ans.strip()})
    spec = {
        "brief": a.brief,
        "locked": now(),
        "requirements": reqs,
        "constraints": [c.strip() for c in a.constraint if c.strip()],
        "decisions": decisions,
    }
    DIR.mkdir(exist_ok=True)
    SPEC.write_text(json.dumps(spec, ensure_ascii=False, indent=1), encoding="utf-8")
    log("spec_locked", reqs=len(reqs), constraints=len(spec["constraints"]), decisions=len(decisions))
    print(f"fablize: spec locked — {len(reqs)} requirement(s), {len(decisions)} decision(s) → {SPEC}")
    print("fablize: build against this; do not re-ask the user what is recorded here.")


def cmd_show(a):
    if not SPEC.exists():
        print("fablize: no locked spec yet. After clarifying, record it with `spec.py lock`.")
        return
    spec = json.loads(SPEC.read_text(encoding="utf-8"))
    print(f"fablize: locked spec — {spec.get('brief') or '(no brief)'}  [locked {spec.get('locked','?')}]")
    if spec.get("requirements"):
        print("Requirements:")
        for r in spec["requirements"]:
            print(f"  • {r}")
    if spec.get("constraints"):
        print("Constraints:")
        for c in spec["constraints"]:
            print(f"  • {c}")
    if spec.get("decisions"):
        print("Decisions:")
        for d in spec["decisions"]:
            print(f"  • {d['question']} → {d['answer']}")


def main():
    p = argparse.ArgumentParser(prog="spec.py")
    sub = p.add_subparsers(dest="cmd", required=True)
    lk = sub.add_parser("lock")
    lk.add_argument("--brief", default="")
    lk.add_argument("--req", action="append", default=[])
    lk.add_argument("--constraint", action="append", default=[])
    lk.add_argument("--decision", action="append", default=[])
    sub.add_parser("show")
    a = p.parse_args()
    {"lock": cmd_lock, "show": cmd_show}[a.cmd](a)


if __name__ == "__main__":
    main()

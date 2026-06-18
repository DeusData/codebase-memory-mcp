#!/usr/bin/env python3
"""fablize metrics — summarize the cross-project event stream (~/.fablize/events.jsonl).

This is the observability layer: it turns the raw event log written by goals.py / spec.py
into real, queryable numbers (how many plans, completion rate, how often work hit the
escalation gate, how many specs were locked). It gives the "verified-only" philosophy
actual data to decide on, instead of self-assessment.

Usage:
  metrics.py              # human-readable summary
  metrics.py --json       # machine-readable
  metrics.py --since 2026-06-01   # only events on/after this ISO date
"""
import argparse
import json
from collections import Counter
from pathlib import Path

GLOBAL_LOG = Path.home() / ".fablize" / "events.jsonl"


def read_events(since=""):
    if not GLOBAL_LOG.exists():
        return []
    out = []
    for line in GLOBAL_LOG.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if since and rec.get("ts", "") < since:
            continue
        out.append(rec)
    return out


def summarize(events):
    ev = Counter(e.get("event") for e in events)
    checkpoints = [e for e in events if e.get("event") == "checkpoint"]
    statuses = Counter(c.get("status") for c in checkpoints)
    completed = statuses.get("complete", 0)
    total_ck = len(checkpoints)
    projects = {e.get("cwd") for e in events if e.get("cwd")}
    return {
        "events_total": len(events),
        "plans_created": ev.get("plan_created", 0),
        "stories_started": ev.get("story_started", 0),
        "checkpoints": total_ck,
        "checkpoint_status": dict(statuses),
        "completion_rate": round(completed / total_ck, 3) if total_ck else None,
        "escalations": ev.get("escalation_triggered", 0),
        "specs_locked": ev.get("spec_locked", 0),
        "projects": len(projects),
    }


def main():
    p = argparse.ArgumentParser(prog="metrics.py")
    p.add_argument("--json", action="store_true")
    p.add_argument("--since", default="")
    a = p.parse_args()
    s = summarize(read_events(a.since))
    if a.json:
        print(json.dumps(s, ensure_ascii=False, indent=2))
        return
    if not s["events_total"]:
        print("fablize: no events yet (~/.fablize/events.jsonl is empty). Run a goals/spec flow first.")
        return
    print(f"fablize metrics{(' since ' + a.since) if a.since else ''} — {s['events_total']} events across {s['projects']} project(s)")
    print(f"  plans created     : {s['plans_created']}")
    print(f"  stories started   : {s['stories_started']}")
    print(f"  checkpoints       : {s['checkpoints']}  {s['checkpoint_status']}")
    rate = f"{s['completion_rate']*100:.1f}%" if s["completion_rate"] is not None else "n/a"
    print(f"  completion rate   : {rate}")
    print(f"  escalation gate   : {s['escalations']} hit(s)")
    print(f"  specs locked      : {s['specs_locked']}")


if __name__ == "__main__":
    main()

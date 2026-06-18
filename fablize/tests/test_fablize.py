#!/usr/bin/env python3
"""fablize test suite — stdlib unittest, no third-party deps (portable everywhere).

Runs the engines as real subprocesses in an isolated temp HOME/CWD so the suite never
touches the developer's real ~/.fablize or repo state. Covers the invariants that ARE
the product: evidence-gated completion, the final verification gate, the bounded
self-correction → escalation counter, and the metrics summary.
"""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GOALS = str(ROOT / "scripts" / "goals.py")
SPEC = str(ROOT / "scripts" / "spec.py")
METRICS = str(ROOT / "scripts" / "metrics.py")
GUARD = str(ROOT / "hooks" / "destructive_guard.py")


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.env = dict(os.environ, HOME=self.tmp)

    def run_script(self, script, *args, stdin=None):
        return subprocess.run(
            [sys.executable, script, *args],
            cwd=self.tmp, env=self.env, input=stdin,
            capture_output=True, text=True,
        )


class GoalsTests(Base):
    def _create(self):
        return self.run_script(GOALS, "create", "--brief", "demo",
                               "--goal", "build::do the thing",
                               "--goal", "verify::prove it works")

    def test_create_and_status(self):
        r = self._create()
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("2 stories", r.stdout)
        s = self.run_script(GOALS, "status")
        self.assertIn("0/2 complete", s.stdout)

    def test_complete_requires_evidence(self):
        self._create()
        self.run_script(GOALS, "next")
        r = self.run_script(GOALS, "checkpoint", "--id", "G001", "--status", "complete", "--evidence", "")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("non-empty --evidence", r.stderr)

    def test_checkpoint_requires_active(self):
        self._create()
        # G001 never activated via `next`
        r = self.run_script(GOALS, "checkpoint", "--id", "G001", "--status", "complete", "--evidence", "x")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("not active", r.stderr)

    def test_final_story_verification_gate(self):
        self._create()
        self.run_script(GOALS, "next")
        self.run_script(GOALS, "checkpoint", "--id", "G001", "--status", "complete", "--evidence", "built")
        self.run_script(GOALS, "next")  # activates final G002
        # final without verify args must fail
        r = self.run_script(GOALS, "checkpoint", "--id", "G002", "--status", "complete", "--evidence", "done")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("verification gate", r.stderr)
        # with verify args it succeeds
        ok = self.run_script(GOALS, "checkpoint", "--id", "G002", "--status", "complete",
                             "--evidence", "done", "--verify-cmd", "pytest", "--verify-evidence", "12 passed")
        self.assertEqual(ok.returncode, 0, ok.stderr)
        self.assertIn("all stories complete", ok.stdout)

    def test_bounded_escalation(self):
        self._create()
        # attempt 1
        self.run_script(GOALS, "next")
        r1 = self.run_script(GOALS, "checkpoint", "--id", "G001", "--status", "blocked", "--evidence", "stuck")
        self.assertNotIn("escalation gate", r1.stdout)
        # retry → attempt 2 → escalation
        rt = self.run_script(GOALS, "retry", "--id", "G001")
        self.assertIn("attempt 2", rt.stdout)
        r2 = self.run_script(GOALS, "checkpoint", "--id", "G001", "--status", "blocked", "--evidence", "still stuck")
        self.assertIn("escalation gate", r2.stdout)
        self.assertIn("effort xhigh", r2.stdout)

    def test_global_event_log_written(self):
        self._create()
        log = Path(self.tmp) / ".fablize" / "events.jsonl"
        self.assertTrue(log.exists())
        lines = [json.loads(x) for x in log.read_text().splitlines() if x.strip()]
        self.assertTrue(any(e["event"] == "plan_created" and e["tool"] == "goals" for e in lines))


class SpecTests(Base):
    def test_lock_needs_something(self):
        r = self.run_script(SPEC, "lock", "--brief", "x")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("at least one", r.stderr)

    def test_lock_and_show(self):
        r = self.run_script(SPEC, "lock", "--brief", "auth", "--req", "use OAuth",
                            "--decision", "db::postgres")
        self.assertEqual(r.returncode, 0, r.stderr)
        s = self.run_script(SPEC, "show")
        self.assertIn("use OAuth", s.stdout)
        self.assertIn("postgres", s.stdout)

    def test_show_empty(self):
        s = self.run_script(SPEC, "show")
        self.assertIn("no locked spec", s.stdout)


class MetricsTests(Base):
    def test_summary_after_flow(self):
        self.run_script(GOALS, "create", "--brief", "m", "--goal", "a::x", "--goal", "v::y")
        self.run_script(SPEC, "lock", "--req", "r1")
        r = self.run_script(METRICS, "--json")
        data = json.loads(r.stdout)
        self.assertEqual(data["plans_created"], 1)
        self.assertEqual(data["specs_locked"], 1)

    def test_empty_metrics(self):
        r = self.run_script(METRICS)
        self.assertIn("no events yet", r.stdout)


class GuardTests(Base):
    def _check(self, command):
        payload = json.dumps({"tool_name": "Bash", "tool_input": {"command": command}})
        return self.run_script(GUARD, stdin=payload)

    def test_blocks_rm_rf(self):
        r = self._check("rm -rf /tmp/stuff")
        self.assertIn("permissionDecision", r.stdout)
        self.assertIn("ask", r.stdout)

    def test_blocks_force_push(self):
        r = self._check("git push origin main --force")
        self.assertIn("ask", r.stdout)

    def test_allows_safe_command(self):
        r = self._check("ls -la && git status")
        self.assertEqual(r.stdout.strip(), "")

    def test_ignores_non_bash(self):
        payload = json.dumps({"tool_name": "Read", "tool_input": {"file_path": "/x"}})
        r = self.run_script(GUARD, stdin=payload)
        self.assertEqual(r.stdout.strip(), "")


if __name__ == "__main__":
    unittest.main(verbosity=2)

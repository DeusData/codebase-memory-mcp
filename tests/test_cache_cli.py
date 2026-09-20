"""Product regression: inspection never needs daemon coordination, even on a TTY.

Run: python3 tests/test_cache_cli.py build/c/codebase-memory-mcp
"""
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest


BINARY = str(Path(sys.argv.pop(1)).resolve()) if len(sys.argv) > 1 else str(
    Path("build/c/codebase-memory-mcp").resolve()
)


class CacheCLI(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="cbm-cache-cli-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cache = self.root / "cache"
        self.cache.mkdir()
        self.source = self.root / "source"
        self.source.mkdir()
        # Deliberately unusable rendezvous: ANY coordination attempt must fail.
        # This is independent of the developer's live daemon and installed version.
        blocked_runtime = self.root / "not-a-directory"
        blocked_runtime.write_text("do not touch")
        self.env = dict(os.environ, CBM_CACHE_DIR=str(self.cache),
                        CBM_RUNTIME_DIR=str(blocked_runtime))
        self.env.pop("CBM_TEST_DAEMON_RUNTIME_PARENT", None)
        for name, root in (("live", self.source), ("gone", self.root / "gone")):
            with sqlite3.connect(self.cache / f"{name}.db") as db:
                db.execute("CREATE TABLE projects(name TEXT, root_path TEXT, indexed_at TEXT)")
                db.execute("INSERT INTO projects VALUES(?,?,?)",
                           (name, str(root), "2020-01-01T00:00:00Z"))

    def snapshot(self):
        return {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in self.cache.iterdir() if p.is_file()}

    def run_cache(self, *args):
        master = slave = None
        if os.name != "nt":
            master, slave = os.openpty()
        try:
            result = subprocess.run([BINARY, "cache", *args], cwd=self.source,
                                    env=self.env, stdin=subprocess.DEVNULL,
                                    stdout=subprocess.PIPE,
                                    stderr=slave if slave is not None else subprocess.PIPE,
                                    timeout=30)
            if slave is not None:
                os.close(slave)
                slave = None
                chunks = []
                while True:
                    try:
                        chunk = os.read(master, 65536)
                    except OSError as exc:
                        if exc.errno == errno.EIO:
                            break
                        raise
                    if not chunk:
                        break
                    chunks.append(chunk)
                result.stderr = b"".join(chunks)
            return result
        finally:
            if slave is not None:
                os.close(slave)
            if master is not None:
                os.close(master)

    def test_stats_is_local_and_quiet_on_terminal(self):
        before = self.snapshot()
        result = self.run_cache("stats")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, b"")
        report = json.loads(result.stdout)
        self.assertEqual(report["project_count"], 2)
        self.assertEqual(report["missing_root_count"], 1)
        self.assertEqual(report["returned"], len(report["projects"]))
        self.assertFalse(report["has_more"])
        self.assertEqual(sum(p["root_status"] == "missing" for p in report["projects"]), 1)
        inventory = report["directory_inventory"]
        self.assertEqual(inventory["entry_count"], len(list(self.cache.iterdir())))
        self.assertEqual(inventory["entry_count"],
                         sum(c["entry_count"] for c in inventory["categories"].values()))
        self.assertEqual(self.snapshot(), before)

    def test_dry_run_is_local_and_preserves_files(self):
        before = self.snapshot()
        for flag in ("--dry-run", "--dry-run=true"):
            with self.subTest(flag=flag):
                result = self.run_cache("prune", "--missing-root", "--older-than", "30d", flag)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stderr, b"")
                report = json.loads(result.stdout)
                self.assertEqual(report["candidate_count"], 1)
                self.assertEqual(report["deleted_count"], 0)
                self.assertEqual(self.snapshot(), before)

    def test_real_prune_removes_only_missing_roots_and_is_repeatable(self):
        # Isolate BOTH storage and coordination from the developer's sessions.
        runtime = self.root / "runtime"
        runtime.mkdir(mode=0o700)
        self.env["CBM_RUNTIME_DIR"] = str(runtime)
        live_before = (self.cache / "live.db").read_bytes()
        preview = self.run_cache("prune", "--missing-root", "--dry-run")
        self.assertEqual(preview.returncode, 0, preview.stderr)
        self.assertEqual(json.loads(preview.stdout)["candidate_count"], 1)
        result = self.run_cache("prune", "--missing-root", "--confirm-missing-root")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn(b"Preparing one-shot", result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["deleted_count"], 1)
        self.assertEqual(report["failed_count"], 0)
        self.assertFalse((self.cache / "gone.db").exists())
        self.assertEqual((self.cache / "live.db").read_bytes(), live_before)
        self.assertTrue(self.source.is_dir())
        again = self.run_cache("prune", "--missing-root", "--confirm-missing-root")
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertEqual(json.loads(again.stdout)["deleted_count"], 0)

    def test_orphan_sidecars_preview_and_real_deletion(self):
        runtime = self.root / "runtime"
        runtime.mkdir(mode=0o700)
        self.env["CBM_RUNTIME_DIR"] = str(runtime)
        for name in ("orphan.db-wal", "orphan.db-shm", "single.db-shm"):
            (self.cache / name).write_bytes(b"orphan")
        before = self.snapshot()
        preview = self.run_cache("prune", "--orphan-sidecars", "--dry-run")
        self.assertEqual(preview.returncode, 0, preview.stderr)
        report = json.loads(preview.stdout)
        self.assertEqual(report["candidate_count"], 3)
        self.assertEqual(len(report["sidecars"]), 3)
        self.assertEqual(report["count_unit"], "sidecar_files")
        self.assertEqual(self.snapshot(), before)
        result = self.run_cache("prune", "--orphan-sidecars")
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["deleted_count"], 3)
        self.assertEqual(report["removed_bytes"], 18)
        for name in ("live.db", "gone.db"):
            self.assertEqual(self.snapshot()[name], before[name])
        self.assertFalse(list(self.cache.glob("*.db-wal")))
        self.assertFalse(list(self.cache.glob("*.db-shm")))

    def test_real_prune_refuses_running_daemon_without_stopping_it(self):
        runtime = self.root / "runtime"
        runtime.mkdir(mode=0o700)
        self.env["CBM_RUNTIME_DIR"] = str(runtime)
        start = subprocess.run([BINARY, "daemon", "start"], cwd=self.source,
                               env=self.env, capture_output=True, timeout=30)
        self.assertEqual(start.returncode, 0, start.stderr)
        try:
            result = self.run_cache("prune", "--missing-root", "--confirm-missing-root")
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn(b"cache is busy", result.stderr)
            prefixed = subprocess.run(
                [BINARY, "--quiet", "cache", "prune", "--missing-root", "--confirm-missing-root"],
                cwd=self.source, env=self.env, capture_output=True, timeout=30)
            self.assertNotEqual(prefixed.returncode, 0, prefixed.stdout)
            self.assertIn(b"cache is busy", prefixed.stderr)
            self.assertTrue((self.cache / "gone.db").exists())
            self.assertTrue((self.cache / "live.db").exists())
            status = subprocess.run([BINARY, "daemon", "status"], cwd=self.source,
                                    env=self.env, capture_output=True, timeout=30)
            self.assertEqual(status.returncode, 0, status.stderr)
            preview = self.run_cache("prune", "--missing-root", "--dry-run")
            self.assertEqual(preview.returncode, 0, preview.stderr)
        finally:
            stop = subprocess.run([BINARY, "daemon", "stop"], cwd=self.source,
                                  env=self.env, capture_output=True, timeout=30)
            self.assertEqual(stop.returncode, 0, stop.stderr)
        retry = self.run_cache("prune", "--missing-root", "--confirm-missing-root")
        self.assertEqual(retry.returncode, 0, retry.stderr)
        self.assertEqual(json.loads(retry.stdout)["deleted_count"], 1)

    def test_destructive_or_ambiguous_arguments_cannot_use_stateless_deletion(self):
        before = self.snapshot()
        for args in (("--missing-root",),
                     ("--missing-root", "--dry-run=false"),
                     ("--missing-root", "--dry-run", "--dry-run=false"),
                     ("--older-than", "--dry-run")):
            with self.subTest(args=args):
                result = self.run_cache("prune", *args)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertNotIn(b"Preparing one-shot", result.stderr)
                self.assertEqual(self.snapshot(), before)

    def test_wal_inspection_never_creates_shm_or_reads_stale_metadata(self):
        seed = self.root / "seed.db"
        with sqlite3.connect(seed) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA wal_autocheckpoint=0")
            db.execute("CREATE TABLE projects(name TEXT, root_path TEXT, indexed_at TEXT)")
            db.execute("INSERT INTO projects VALUES(?,?,?)",
                       ("walcase", str(self.root / "missing"), "2020-01-01T00:00:00Z"))
            db.commit()
            shutil.copy2(seed, self.cache / "walcase.db")
            shutil.copy2(str(seed) + "-wal", self.cache / "walcase.db-wal")
        before = self.snapshot()
        for args in (("stats",), ("prune", "--missing-root", "--dry-run")):
            result = self.run_cache(*args)
            self.assertEqual(result.returncode, 0, result.stderr)
            record = next(p for p in json.loads(result.stdout)["projects"]
                          if p["db_file"] == "walcase.db")
            self.assertEqual(record["status"], "journal_or_unavailable_database")
            self.assertEqual(self.snapshot(), before)

    def test_journal_backed_database_is_preserved_even_for_real_pruning(self):
        runtime = self.root / "runtime"
        runtime.mkdir(mode=0o700)
        self.env["CBM_RUNTIME_DIR"] = str(runtime)
        (self.cache / "gone.db-journal").write_bytes(b"unrecovered journal")
        before = self.snapshot()
        result = self.run_cache("prune", "--missing-root", "--confirm-missing-root")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["deleted_count"], 0)
        self.assertEqual(self.snapshot(), before)

    def test_missing_root_confirmation_and_adr_protection(self):
        runtime = self.root / "runtime"
        runtime.mkdir(mode=0o700)
        self.env["CBM_RUNTIME_DIR"] = str(runtime)
        with sqlite3.connect(self.cache / "gone.db") as db:
            db.execute("CREATE TABLE project_summaries(project TEXT, summary TEXT)")
            db.execute("INSERT INTO project_summaries VALUES('gone', 'keep my ADR')")
        before = self.snapshot()
        rejected = self.run_cache("prune", "--missing-root")
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn(b"confirm-missing-root", rejected.stdout)
        kept = self.run_cache("prune", "--missing-root", "--confirm-missing-root")
        self.assertEqual(kept.returncode, 0, kept.stderr)
        self.assertEqual(json.loads(kept.stdout)["deleted_count"], 0)
        self.assertEqual(self.snapshot(), before)
        removed = self.run_cache("prune", "--missing-root", "--confirm-missing-root", "--include-adrs")
        self.assertEqual(removed.returncode, 0, removed.stderr)
        self.assertEqual(json.loads(removed.stdout)["deleted_count"], 1)

    def test_non_directory_and_unresolved_roots_are_not_missing(self):
        not_directory = self.root / "root-file"
        not_directory.write_text("not a source directory")
        with sqlite3.connect(self.cache / "gone.db") as db:
            db.execute("UPDATE projects SET root_path=?", (str(not_directory),))
        if os.name != "nt":
            unresolved = self.root / "unresolved-root"
            unresolved.symlink_to(self.root / "unmounted")
            with sqlite3.connect(self.cache / "live.db") as db:
                db.execute("UPDATE projects SET root_path=?", (str(unresolved),))
        before = self.snapshot()
        preview = self.run_cache("prune", "--missing-root", "--dry-run")
        self.assertEqual(preview.returncode, 0, preview.stderr)
        report = json.loads(preview.stdout)
        self.assertEqual(report["candidate_count"], 0)
        self.assertEqual(report["missing_root_count"], 0)
        self.assertEqual(self.snapshot(), before)

    def test_unicode_and_uri_characters_in_cache_path(self):
        renamed = self.root / ("cache 日本語 #%" + ("?" if os.name != "nt" else ""))
        self.cache.rename(renamed)
        self.cache = renamed
        self.env["CBM_CACHE_DIR"] = str(renamed)
        before = self.snapshot()
        result = self.run_cache("stats")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["project_count"], 2)
        self.assertEqual(self.snapshot(), before)


if __name__ == "__main__":
    unittest.main()

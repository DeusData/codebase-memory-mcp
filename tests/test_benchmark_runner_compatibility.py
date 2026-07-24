import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
EXPERIMENTS_SCRIPT = REPO_ROOT / "benchmarks" / "run_experiments.py"


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


EXPERIMENTS = _load(EXPERIMENTS_SCRIPT, "run_benchmark_experiments_direct")


def _git(*args: str, cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    )


class BenchmarkExperimentsEntryPointTest(unittest.TestCase):
    def test_experiments_script_is_the_canonical_implementation(self) -> None:
        self.assertTrue(EXPERIMENTS_SCRIPT.is_file())
        self.assertTrue(hasattr(EXPERIMENTS, "main"))
        self.assertTrue(hasattr(EXPERIMENTS, "parse_arguments"))
        self.assertEqual(
            EXPERIMENTS.DEFAULT_CANDIDATE_REFS,
            (
                ("upstream-main", "upstream/main"),
                ("pre-today-major", "api-consolidation-stable-2026-07-16-semantic-v2"),
                ("pre-upstream-merge", "pre-upstream-main-merge-2026-07-19"),
                ("latest", "HEAD"),
            ),
        )

    def test_experiment_root_flag_accepts_retained_campaign_root_spelling(self) -> None:
        via_current = EXPERIMENTS.parse_arguments(
            ["--experiment-root", "runs-here", "--plan", "plan.json"]
        )
        via_retained = EXPERIMENTS.parse_arguments(
            ["--campaign-root", "runs-here", "--plan", "plan.json"]
        )
        self.assertEqual(via_current.experiment_root, Path("runs-here"))
        self.assertEqual(via_current.experiment_root, via_retained.experiment_root)

    def test_allow_temporary_experiment_root_flag_is_an_alias(self) -> None:
        via_current = EXPERIMENTS.parse_arguments(
            [
                "--allow-temporary-experiment-root",
                "--plan",
                "p.json",
                "--experiment-root",
                "r",
            ]
        )
        via_retained = EXPERIMENTS.parse_arguments(
            [
                "--allow-temporary-campaign-root",
                "--plan",
                "p.json",
                "--campaign-root",
                "r",
            ]
        )
        self.assertTrue(via_current.allow_temporary_experiment_root)
        self.assertTrue(via_retained.allow_temporary_experiment_root)


class CandidateRefOverrideTest(unittest.TestCase):
    def test_candidate_ref_override_parses_known_label(self) -> None:
        self.assertEqual(
            EXPERIMENTS.parse_candidate_ref_override("upstream-main=origin/main"),
            ("upstream-main", "origin/main"),
        )

    def test_candidate_ref_override_rejects_unknown_label(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be one of"):
            EXPERIMENTS.parse_candidate_ref_override("not-a-label=origin/main")

    def test_candidate_ref_override_rejects_missing_equals_or_empty_side(self) -> None:
        for value in ("upstream-main", "upstream-main=", "=origin/main"):
            with self.assertRaisesRegex(ValueError, "must be LABEL=REF"):
                EXPERIMENTS.parse_candidate_ref_override(value)

    def test_candidate_ref_cli_flag_populates_args_and_rejects_unknown_label(
        self,
    ) -> None:
        args = EXPERIMENTS.parse_arguments(
            ["--quick", "--candidate-ref", "latest=HEAD~1"]
        )
        self.assertEqual(args.candidate_ref, {"latest": "HEAD~1"})
        with self.assertRaises(SystemExit):
            EXPERIMENTS.parse_arguments(["--quick", "--candidate-ref", "bogus=HEAD"])

    def test_candidate_ref_flag_rejects_use_without_automatic_preset(self) -> None:
        with self.assertRaises(SystemExit):
            EXPERIMENTS.parse_arguments(
                [
                    "--plan",
                    "p.json",
                    "--campaign-root",
                    "r",
                    "--candidate-ref",
                    "latest=HEAD~1",
                ]
            )

    def test_resolve_default_candidate_ref_only_touches_upstream_main(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            _git("init", "-q", str(repo), cwd=Path.cwd())
            self.assertEqual(
                EXPERIMENTS.resolve_default_candidate_ref(
                    repo, "pre-today-major", "some-tag"
                ),
                "some-tag",
            )

    def test_resolve_default_candidate_ref_falls_back_from_upstream_to_origin_main(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            _git("init", "-q", str(repo), cwd=Path.cwd())
            _git("config", "user.email", "test@example.invalid", cwd=repo)
            _git("config", "user.name", "Benchmark Test", cwd=repo)
            (repo / "f.txt").write_text("x\n", encoding="utf-8")
            _git("add", "f.txt", cwd=repo)
            _git("commit", "-qm", "fixture", cwd=repo)
            # No "upstream" remote exists in this fixture, but a local branch
            # named "origin/main" stands in for a resolvable fallback target.
            _git("branch", "origin/main", cwd=repo)

            resolved = EXPERIMENTS.resolve_default_candidate_ref(
                repo, "upstream-main", "upstream/main"
            )

            self.assertEqual(resolved, "origin/main")

    def test_resolve_default_candidate_ref_returns_original_ref_when_nothing_resolves(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            _git("init", "-q", str(repo), cwd=Path.cwd())
            _git("config", "user.email", "test@example.invalid", cwd=repo)
            _git("config", "user.name", "Benchmark Test", cwd=repo)
            (repo / "f.txt").write_text("x\n", encoding="utf-8")
            _git("add", "f.txt", cwd=repo)
            _git("commit", "-qm", "fixture", cwd=repo)
            # Neither "upstream/main", "origin/main", nor "main" resolve here
            # (the default branch in this fixture is whatever `git init` picked
            # and was never named any of those three refs).
            current_branch = _git("branch", "--show-current", cwd=repo).stdout.strip()
            if current_branch in {"upstream/main", "origin/main", "main"}:
                self.skipTest(
                    "git init default branch collides with fallback ref under test"
                )

            resolved = EXPERIMENTS.resolve_default_candidate_ref(
                repo, "upstream-main", "upstream/main"
            )

            # Fail-closed: unresolved fallback returns the original ref so the
            # existing materialize_candidate error path still fires with a clear
            # message instead of silently substituting something else.
            self.assertEqual(resolved, "upstream/main")


if __name__ == "__main__":
    unittest.main()

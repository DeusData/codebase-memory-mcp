import importlib.util
import tempfile
import unittest
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "benchmarks" / "autotune.py"
SPEC = importlib.util.spec_from_file_location("autotune", SCRIPT)
assert SPEC and SPEC.loader
AUTOTUNE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AUTOTUNE)


class AutotuneTest(unittest.TestCase):
    def test_cli_requires_exact_build_provenance(self) -> None:
        with redirect_stderr(StringIO()), self.assertRaises(SystemExit):
            AUTOTUNE.parse_args([])

        args = AUTOTUNE.parse_args(
            [
                "--build-target",
                "make -f Makefile.cbm cbm",
                "--compiler",
                "Apple clang version 21.0.0",
                "--cflags",
                "-O3 -DNDEBUG",
            ]
        )
        self.assertEqual(args.cflags, "-O3 -DNDEBUG")

    def test_matrix_uses_versioned_rank_fixture_and_auditable_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            binary = Path(tmpdir) / "cbm"
            binary.write_bytes(b"optimized binary")
            spec = AUTOTUNE.build_matrix_spec(
                binary=binary,
                revision="a" * 40,
                repetitions=3,
                timeout_seconds=120,
                transports=["cli", "mcp"],
                build={"target": "make cbm", "compiler": "clang 18", "cflags": "-O3"},
            )

        self.assertEqual(spec["capability_quality"], "rank")
        self.assertEqual(spec["execution_order"], "paired_interleaved")
        self.assertEqual(spec["accepted_exit_codes"], [0, 1])
        self.assertEqual(spec["repetitions"], 3)
        self.assertEqual(spec["candidates"][0]["revision"], "a" * 40)
        self.assertEqual(spec["candidates"][0]["build"]["cflags"], "-O3")
        labels = [profile["label"] for profile in spec["profiles"]]
        self.assertEqual(labels[:2], ["candidate-default", "rank-disabled"])
        self.assertTrue(
            any(profile.get("config_overrides") for profile in spec["profiles"])
        )

    def test_generated_plan_is_accepted_by_shared_experiment_runner(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            binary = Path(tmpdir) / "cbm"
            binary.write_bytes(b"optimized binary")
            spec = AUTOTUNE.build_matrix_spec(
                binary=binary,
                revision="b" * 40,
                repetitions=1,
                timeout_seconds=60,
                transports=["mcp"],
                build={"target": "make cbm", "compiler": "clang 18", "cflags": "-O3"},
            )
            plan = AUTOTUNE.load_experiment_runner().expand_matrix_spec(spec)

        self.assertEqual(len(plan["cells"]), len(AUTOTUNE.TUNING_PROFILES))
        self.assertTrue(
            all(cell["scenario"] == "rank_quality" for cell in plan["cells"])
        )
        self.assertTrue(all(cell["transport"] == "mcp" for cell in plan["cells"]))
        self.assertTrue(
            all(
                cell["parameters"]["cell_name"]
                and cell["parameters"]["informs_hypotheses"]
                and cell["parameters"]["questions"]
                for cell in plan["cells"]
            )
        )
        self.assertTrue(
            all(
                "benchmark_script_sha256" in cell["parameters"]
                for cell in plan["cells"]
            )
        )

    def test_quick_preflight_reuses_the_first_scientific_cell(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            binary = Path(tmpdir) / "cbm"
            binary.write_bytes(b"optimized binary")
            full = AUTOTUNE.build_matrix_spec(
                binary=binary,
                revision="c" * 40,
                repetitions=1,
                timeout_seconds=60,
                transports=["mcp"],
                build={"target": "make cbm", "compiler": "clang 18", "cflags": "-O3"},
            )
            preflight = AUTOTUNE.build_preflight_spec(full)
            runner = AUTOTUNE.load_experiment_runner()
            full_plan = runner.expand_matrix_spec(full)
            preflight_plan = runner.expand_matrix_spec(preflight)

        self.assertEqual(len(preflight["profiles"]), 1)
        self.assertEqual(preflight["profiles"][0], full["profiles"][0])
        self.assertEqual(
            runner.cell_identity(preflight_plan["cells"][0]),
            runner.cell_identity(full_plan["cells"][0]),
        )
        self.assertEqual(len(full["profiles"]), len(AUTOTUNE.TUNING_PROFILES))

    def test_source_has_no_legacy_global_or_resource_path(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        self.assertNotIn("resources/read", source)
        self.assertNotIn("atexit", source)
        self.assertNotIn("Path.home()", source)
        self.assertNotIn("delete_project_db", source)
        self.assertIn(".worktrees", source)


if __name__ == "__main__":
    unittest.main()

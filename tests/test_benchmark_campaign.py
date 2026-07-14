import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run-benchmark-campaign.py"
SPEC = importlib.util.spec_from_file_location("run_benchmark_campaign", SCRIPT)
assert SPEC and SPEC.loader
CAMPAIGN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CAMPAIGN)


def cell(command: list[str], **overrides: object) -> dict:
    value = {
        "label": "latest-rank-off-r1",
        "revision": "a" * 40,
        "binary_sha256": "b" * 64,
        "build": {"target": "cbm", "cflags": "-O2"},
        "capabilities": {"rank_enabled": "false"},
        "transport": "mcp",
        "scenario": "self_dogfood",
        "repetition": 1,
        "harness_version": "quality-v1",
        "command": command,
        "accepted_exit_codes": [0],
    }
    value.update(overrides)
    return value


class BenchmarkCampaignTest(unittest.TestCase):
    def test_cell_identity_covers_binary_config_scenario_and_repetition(self) -> None:
        base = cell(["benchmark", "{result_path}"])
        base_id = CAMPAIGN.cell_identity(base)
        for key, changed in (
            ("binary_sha256", "c" * 64),
            ("capabilities", {"rank_enabled": "true"}),
            ("scenario", "matrix"),
            ("repetition", 2),
            ("environment", {"CBM_TEST_SEED": "2"}),
        ):
            variant = dict(base)
            variant[key] = changed
            self.assertNotEqual(base_id, CAMPAIGN.cell_identity(variant), key)

    def test_successful_cell_resumes_without_second_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            command = [
                sys.executable,
                "-c",
                (
                    "import json,sys; "
                    "json.dump({'binary_metadata':{'sha256':'" + "b" * 64 +
                    "'},'derived':{'passed':True}},open(sys.argv[1],'w'))"
                ),
                "{result_path}",
            ]
            planned = cell(command)
            first = CAMPAIGN.run_cell(root, planned, minimum_free_bytes=0)
            second = CAMPAIGN.run_cell(root, planned, minimum_free_bytes=0)
            cell_root = root / "runs" / CAMPAIGN.cell_identity(planned)
            self.assertEqual(first["status"], "completed")
            self.assertEqual(second["status"], "resumed")
            self.assertTrue((cell_root / "complete.json").is_file())
            self.assertEqual(len(list((cell_root / "attempts").iterdir())), 1)

    def test_failed_attempt_retains_logs_without_completion_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            planned = cell([sys.executable, "-c", "import sys; print('bad'); sys.exit(3)"])
            result = CAMPAIGN.run_cell(root, planned, minimum_free_bytes=0)
            cell_root = root / "runs" / CAMPAIGN.cell_identity(planned)
            attempts = list((cell_root / "attempts").iterdir())
            self.assertEqual(result["status"], "failed")
            self.assertFalse((cell_root / "complete.json").exists())
            self.assertEqual(len(attempts), 1)
            self.assertIn("bad", (attempts[0] / "stdout.log").read_text())
            self.assertTrue((attempts[0] / "attempt.json").is_file())

    def test_setup_error_releases_cell_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            planned = cell(["benchmark"], environment={"BAD": 3})
            with self.assertRaisesRegex(ValueError, "cell environment"):
                CAMPAIGN.run_cell(root, planned, minimum_free_bytes=0)
            cell_root = root / "runs" / CAMPAIGN.cell_identity(planned)
            self.assertFalse((cell_root / "running.lock").exists())

    def test_scan_reports_corrupt_and_unplanned_run_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            planned = cell(["benchmark"])
            expected_id = CAMPAIGN.cell_identity(planned)
            expected = root / "runs" / expected_id
            expected.mkdir(parents=True)
            (expected / "complete.json").write_text("not-json")
            (root / "runs" / "unplanned-cell").mkdir()
            audit = CAMPAIGN.scan_campaign(root, [planned])
            self.assertEqual(audit["counts"]["corrupt"], 1)
            self.assertEqual(audit["counts"]["unplanned"], 1)
            self.assertEqual(audit["unplanned"], ["unplanned-cell"])

    def test_atomic_json_roundtrip_leaves_no_temporary_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "manifest.json"
            CAMPAIGN.atomic_write_json(path, {"ok": True})
            self.assertEqual(json.loads(path.read_text()), {"ok": True})
            self.assertEqual(list(path.parent.glob(".manifest.json.*.tmp")), [])

    def test_atomic_bytes_preserve_exact_plan_content(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "plan.json"
            payload = b'{ "schema_version": 1 }\n'
            CAMPAIGN.atomic_write_bytes(path, payload)
            self.assertEqual(path.read_bytes(), payload)

    def test_completed_inputs_group_repetitions_under_candidate_label(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            planned = cell(["benchmark"])
            identity = CAMPAIGN.cell_identity(planned)
            result = root / "runs" / identity / "attempts" / "one" / "result.json"
            result.parent.mkdir(parents=True)
            result.write_text(
                json.dumps({"binary_metadata": {"sha256": "b" * 64}}), encoding="utf-8"
            )
            CAMPAIGN.atomic_write_json(
                root / "runs" / identity / "complete.json",
                {
                    "cell_identity": identity,
                    "result_path": str(result.relative_to(root / "runs" / identity)),
                    "result_sha256": CAMPAIGN.file_sha256(result),
                },
            )
            inputs = CAMPAIGN.completed_report_inputs(root, [planned])
            self.assertEqual(inputs, [("latest-rank-off-r1", result.resolve())])
            report = CAMPAIGN.generate_report(root, [planned], root / "reports" / "summary.md")
            self.assertEqual(report["input_count"], 1)
            self.assertTrue((root / "reports" / "summary.md").is_file())
            self.assertIn("latest-rank-off-r1", (root / "reports" / "summary.md").read_text())


if __name__ == "__main__":
    unittest.main()

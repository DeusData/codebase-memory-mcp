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
    def test_campaign_root_rejects_os_temporary_tree_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            temporary_root = Path(tmpdir) / "system-temp"
            campaign_root = temporary_root / "lost-after-reboot"
            with self.assertRaisesRegex(ValueError, "campaign root is temporary"):
                CAMPAIGN.validate_campaign_root(
                    campaign_root,
                    temporary_root=temporary_root,
                )
            self.assertEqual(
                CAMPAIGN.validate_campaign_root(
                    campaign_root,
                    allow_temporary=True,
                    temporary_root=temporary_root,
                ),
                campaign_root.resolve(),
            )

    def test_campaign_root_accepts_durable_path_outside_temporary_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            temporary_root = base / "system-temp"
            campaign_root = base / "repository" / ".worktrees" / "benchmark-campaign"
            self.assertEqual(
                CAMPAIGN.validate_campaign_root(
                    campaign_root,
                    temporary_root=temporary_root,
                ),
                campaign_root.resolve(),
            )

    def test_cell_identity_covers_binary_config_scenario_and_repetition(self) -> None:
        base = cell(["benchmark", "{result_path}"])
        base_id = CAMPAIGN.cell_identity(base)
        for key, changed in (
            ("binary_sha256", "c" * 64),
            ("capabilities", {"rank_enabled": "true"}),
            ("scenario", "matrix"),
            ("repetition", 2),
            ("environment", {"CBM_TEST_SEED": "2"}),
            ("parameters", {"frontier_files": 64, "exact_cap": 128}),
            ("capability_support", {"rank": False}),
        ):
            variant = dict(base)
            variant[key] = changed
            self.assertNotEqual(base_id, CAMPAIGN.cell_identity(variant), key)

    def test_matrix_spec_expands_structured_frontier_cap_and_repetition_cells(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            binary = root / "cbm"
            binary.write_bytes(b"optimized-binary")
            benchmark = root / "benchmark-incremental-speed.py"
            benchmark.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            spec = {
                "schema_version": 1,
                "harness_version": "frontier-v2",
                "benchmark_script": str(benchmark),
                "cwd": str(root),
                "timeout_seconds": 300,
                "repetitions": 2,
                "transports": ["cli"],
                "candidates": [
                    {
                        "label": "latest",
                        "revision": "a" * 40,
                        "binary": str(binary),
                        "build": {"target": "cbm", "cflags": "-O2"},
                        "capability_support": {"rank": True, "dependencies": True},
                    }
                ],
                "profiles": [
                    {
                        "label": "minimal",
                        "config_profile": "minimal_indexing",
                        "capabilities": {"rank_enabled": "false"},
                        "config_overrides": {"auto_index_deps": "false"},
                    }
                ],
                "scenarios": [
                    {
                        "name": "go_inbound_frontier",
                        "frontier_files": [4, 16],
                        "exact_caps": [4, 64],
                    }
                ],
            }

            plan = CAMPAIGN.expand_matrix_spec(spec)

            self.assertEqual(plan["schema_version"], 1)
            self.assertEqual(len(plan["cells"]), 8)
            self.assertEqual(len({CAMPAIGN.cell_identity(item) for item in plan["cells"]}), 8)
            first = plan["cells"][0]
            self.assertEqual(first["binary_sha256"], CAMPAIGN.file_sha256(binary))
            self.assertEqual(first["parameters"]["frontier_files"], 4)
            self.assertEqual(first["parameters"]["exact_cap"], 4)
            self.assertEqual(
                first["capability_support"], {"dependencies": True, "rank": True}
            )
            self.assertEqual(
                first["parameters"]["benchmark_script_sha256"], CAMPAIGN.file_sha256(benchmark)
            )
            self.assertIn("--frontier-files", first["command"])
            self.assertIn("incremental_exact_max_affected_paths=4", first["command"])

    def test_matrix_spec_rejects_candidate_sha_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            binary = Path(tmpdir) / "cbm"
            binary.write_bytes(b"binary")
            benchmark = Path(tmpdir) / "benchmark.py"
            benchmark.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            spec = {
                "schema_version": 1,
                "harness_version": "frontier-v2",
                "benchmark_script": str(benchmark),
                "cwd": tmpdir,
                "repetitions": 1,
                "transports": ["cli"],
                "candidates": [
                    {
                        "label": "latest",
                        "revision": "a" * 40,
                        "binary": str(binary),
                        "binary_sha256": "0" * 64,
                        "build": {"target": "cbm", "cflags": "-O2"},
                    }
                ],
                "profiles": [
                    {
                        "label": "minimal",
                        "config_profile": "minimal_indexing",
                        "capabilities": {},
                    }
                ],
                "scenarios": [
                    {"name": "go_inbound_frontier", "frontier_files": [4], "exact_caps": [8]}
                ],
            }

            with self.assertRaisesRegex(ValueError, "binary_sha256 does not match"):
                CAMPAIGN.expand_matrix_spec(spec)

    def test_matrix_spec_null_cap_preserves_candidate_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            binary = root / "cbm"
            binary.write_bytes(b"binary")
            benchmark = root / "benchmark.py"
            benchmark.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            spec = {
                "schema_version": 1,
                "harness_version": "default-v1",
                "benchmark_script": str(benchmark),
                "cwd": str(root),
                "repetitions": 1,
                "transports": ["mcp"],
                "candidates": [
                    {
                        "label": "candidate",
                        "revision": "a" * 40,
                        "binary": str(binary),
                        "build": {"cflags": "-O2"},
                    }
                ],
                "profiles": [
                    {"label": "default", "config_profile": "default", "capabilities": {}}
                ],
                "scenarios": [
                    {"name": "go_modify_1", "frontier_files": [16], "exact_caps": [None]}
                ],
            }

            cell = CAMPAIGN.expand_matrix_spec(spec)["cells"][0]

            self.assertEqual(cell["label"], "candidate.default.mcp.go_modify_1.f16.capdefault")
            self.assertIsNone(cell["parameters"]["exact_cap"])
            self.assertNotIn("incremental_exact_max_affected_paths", cell["capabilities"])
            self.assertFalse(
                any("incremental_exact_max_affected_paths=" in item for item in cell["command"])
            )

    def test_successful_cell_resumes_without_second_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            command = [
                sys.executable,
                "-c",
                (
                    "import json,sys; "
                    "json.dump({'binary_metadata':{'sha256':'" + "b" * 64 +
                    "'},'derived':{'passed':True},'cases':[{'passed':True}]},open(sys.argv[1],'w'))"
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

    def test_report_input_adds_candidate_support_without_mutating_raw_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw = root / "raw.json"
            raw_document = {
                "parameters": {"config_profile": "default"},
                "binary_metadata": {"sha256": "b" * 64},
                "cases": [{"passed": True}],
            }
            raw.write_text(json.dumps(raw_document), encoding="utf-8")
            planned = cell(
                ["benchmark", "{result_path}"],
                capability_support={"rank": False, "dependencies": False},
            )

            derived = CAMPAIGN.materialize_report_input(root, planned, raw)

            self.assertEqual(json.loads(raw.read_text()), raw_document)
            document = json.loads(derived.read_text())
            self.assertEqual(
                document["parameters"]["capability_support"],
                {"dependencies": False, "rank": False},
            )
            self.assertEqual(document["campaign_provenance"]["source_result_sha256"],
                             CAMPAIGN.file_sha256(raw))
            self.assertEqual(document["campaign_provenance"]["cell_identity"],
                             CAMPAIGN.cell_identity(planned))

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

    @unittest.skipIf(sys.platform == "win32", "POSIX signal-group assertion")
    def test_timeout_stops_descendant_and_retains_failed_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            marker = root / "descendant-terminated"
            child = (
                "import pathlib,signal,sys,time; "
                "signal.signal(signal.SIGTERM, lambda *_: "
                "(pathlib.Path(sys.argv[1]).write_text('terminated'), sys.exit(0))); "
                "time.sleep(60)"
            )
            parent = (
                "import subprocess,sys,time; "
                f"subprocess.Popen([sys.executable,'-c',{child!r},sys.argv[1]]); "
                "time.sleep(60)"
            )
            planned = cell(
                [sys.executable, "-c", parent, str(marker)], timeout_seconds=0.5
            )

            outcome = CAMPAIGN.run_cell(root, planned, minimum_free_bytes=0)
            cell_root = root / "runs" / CAMPAIGN.cell_identity(planned)
            attempt = next((cell_root / "attempts").iterdir())
            record = json.loads((attempt / "attempt.json").read_text())

            self.assertEqual(outcome["status"], "failed")
            self.assertEqual(record["status"], "failed")
            self.assertIn("timed out", record["error"])
            self.assertTrue(marker.is_file())
            self.assertFalse((cell_root / "running.lock").exists())
            self.assertFalse((cell_root / "complete.json").exists())

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
                json.dumps(
                    {
                        "binary_metadata": {"sha256": "b" * 64},
                        "derived": {"passed": True},
                        "cases": [{"passed": True}],
                    }
                ),
                encoding="utf-8",
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
            self.assertEqual(inputs[0][0], "latest-rank-off-r1")
            self.assertNotEqual(inputs[0][1], result.resolve())
            self.assertEqual(
                json.loads(inputs[0][1].read_text())["campaign_provenance"]["source_result_sha256"],
                CAMPAIGN.file_sha256(result),
            )
            report = CAMPAIGN.generate_report(root, [planned], root / "reports" / "summary.md")
            self.assertEqual(report["input_count"], 1)
            self.assertTrue((root / "reports" / "summary.md").is_file())
            self.assertIn("latest-rank-off-r1", (root / "reports" / "summary.md").read_text())

    def test_harness_error_report_is_not_marked_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = {
                "binary_metadata": {"sha256": "b" * 64},
                "derived": {"passed": False},
                "cases": [],
                "error": "RuntimeError: index failed",
            }
            command = [
                sys.executable,
                "-c",
                "import json,sys; json.dump(json.loads(sys.argv[2]),open(sys.argv[1],'w'))",
                "{result_path}",
                json.dumps(payload),
            ]
            planned = cell(command, accepted_exit_codes=[0, 1])
            outcome = CAMPAIGN.run_cell(root, planned, minimum_free_bytes=0)
            cell_root = root / "runs" / CAMPAIGN.cell_identity(planned)
            self.assertEqual(outcome["status"], "failed")
            self.assertIn("contains an error", outcome["error"])
            self.assertFalse((cell_root / "complete.json").exists())


if __name__ == "__main__":
    unittest.main()

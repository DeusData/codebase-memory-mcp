#!/usr/bin/env python3
"""Contract tests for the isolated benchmark-container coordinator."""

from __future__ import annotations

import importlib.util
import json
import math
import tempfile
import unittest
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parents[1] / "benchmarks" / "run_container_experiment.py"
)
SPEC = importlib.util.spec_from_file_location("run_container_experiment", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
CONTAINER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONTAINER)


class BenchmarkContainerContractTest(unittest.TestCase):
    def test_native_platform_mapping_is_explicit(self) -> None:
        self.assertEqual(CONTAINER.native_linux_platform("arm64"), "linux/arm64")
        self.assertEqual(CONTAINER.native_linux_platform("aarch64"), "linux/arm64")
        self.assertEqual(CONTAINER.native_linux_platform("x86_64"), "linux/amd64")
        with self.assertRaisesRegex(ValueError, "unsupported host architecture"):
            CONTAINER.native_linux_platform("riscv64")

    def test_resources_are_required_and_workers_cannot_exceed_cpu_budget(self) -> None:
        self.assertEqual(
            CONTAINER.validate_resources(4.0, "8g", 4),
            {"cpus": 4.0, "memory": "8g", "workers": 4},
        )
        for cpus, memory, workers, message in (
            (0.0, "8g", 1, "CPUs"),
            (math.nan, "8g", 1, "CPUs"),
            (math.inf, "8g", 1, "CPUs"),
            (1.0, "", 1, "memory"),
            (2.0, "8g", 3, "workers"),
        ):
            with (
                self.subTest(cpus=cpus, memory=memory, workers=workers),
                self.assertRaisesRegex(ValueError, message),
            ):
                CONTAINER.validate_resources(cpus, memory, workers)

    def test_bundle_excludes_stash_and_recovery_namespaces(self) -> None:
        arguments = CONTAINER.bundle_revision_arguments()
        self.assertEqual(arguments, ["HEAD", "--branches", "--tags", "--remotes"])
        self.assertNotIn("--all", arguments)

    def test_forwarded_arguments_cannot_replace_coordinator_owned_paths(self) -> None:
        for arguments in (
            ["--experiment-root", "/tmp/other"],
            ["--experiment-root=/tmp/other"],
            ["--candidate-root=/tmp/other"],
            ["--matrix-spec", "/tmp/other.json"],
            ["--product-env=CBM_WORKERS=99"],
        ):
            with (
                self.subTest(arguments=arguments),
                self.assertRaisesRegex(ValueError, "coordinator-owned"),
            ):
                CONTAINER.validate_forwarded_arguments(arguments)

    def test_matrix_worker_budget_is_explicit_and_conflicts_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source.json"
            effective = root / "effective.json"
            source.write_text('{"schema_version": 1}\n', encoding="utf-8")

            digest = CONTAINER.materialize_container_matrix_spec(source, effective, 4)

            self.assertEqual(digest, CONTAINER.file_sha256(effective))
            self.assertEqual(
                json.loads(effective.read_text(encoding="utf-8"))[
                    "product_environment"
                ],
                {"CBM_WORKERS": "4"},
            )
            source.write_text(
                '{"product_environment": {"CBM_WORKERS": "8"}}\n',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "conflicts"):
                CONTAINER.materialize_container_matrix_spec(source, effective, 4)

    def test_measured_command_uses_only_named_volumes(self) -> None:
        command = CONTAINER.build_measured_command(
            docker="docker",
            image="cbm-benchmark-runtime:abc",
            platform_name="linux/arm64",
            resources={"cpus": 4.0, "memory": "8g", "workers": 4},
            container_name="cbm-benchmark-measured-abc",
            work_volume="cbm-benchmark-work-abc",
            results_volume="cbm-benchmark-results-abc",
            source_revision="a" * 40,
            bundle_name="repo.bundle",
            runner_arguments=[
                "--full",
                "--transport",
                "mcp",
                "--product-env",
                "CBM_WORKERS=4",
            ],
            uid=501,
            gid=20,
        )

        self.assertIn("--rm", command)
        self.assertIn("type=volume,src=cbm-benchmark-work-abc,dst=/benchmark", command)
        self.assertIn("type=volume,src=cbm-benchmark-results-abc,dst=/results", command)
        self.assertIn("CBM_WORKERS=4", command)
        self.assertNotIn("type=bind", " ".join(command))
        self.assertNotIn("/Users/", " ".join(command))

    def test_export_merge_is_idempotent_and_rejects_changed_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            staged = root / "staged"
            destination = root / "history"
            (staged / "reports").mkdir(parents=True)
            (staged / "reports" / "result.json").write_text(
                '{"passed": true}\n', encoding="utf-8"
            )

            CONTAINER.merge_exported_tree(staged, destination)
            CONTAINER.merge_exported_tree(staged, destination)
            (staged / "reports" / "result.json").write_text(
                '{"passed": false}\n', encoding="utf-8"
            )

            with self.assertRaisesRegex(RuntimeError, "different bytes"):
                CONTAINER.merge_exported_tree(staged, destination)


if __name__ == "__main__":
    unittest.main()

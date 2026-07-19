import hashlib
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
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
    def test_filename_datetime_is_sortable_explicit_utc_and_filename_safe(self) -> None:
        stamp = CAMPAIGN.filename_datetime(datetime(2026, 7, 19, 21, 13, 58, 123456, tzinfo=timezone.utc))

        self.assertEqual(stamp, "2026-07-19-211358.123456Z")

    def test_campaign_names_separate_definition_runset_source_and_generation_identity(
        self,
    ) -> None:
        source = {
            "revision": "a" * 40,
            "commit_datetime_slug": "2026-07-19-1642",
            "tree": "b" * 40,
        }
        runset = CAMPAIGN.runset_identity(b'{"stable":"spec"}\n')
        generated = datetime(2026, 7, 19, 21, 13, 58, 123456, tzinfo=timezone.utc)

        root = CAMPAIGN.automatic_campaign_name("quick", source, runset)
        spec = CAMPAIGN.automatic_spec_name("quick", runset)
        report = CAMPAIGN.generated_artifact_name(
            "report", runset, ".md", preset="quick", moment=generated
        )
        manifest = CAMPAIGN.generated_artifact_name(
            "manifest", runset, ".json", moment=generated, nonce="c0ffee12"
        )

        self.assertEqual(
            root,
            "v0001-quick-commit-2026-07-19-1642-aaaaaaaaaaaa-runset-ff40aae6b1de",
        )
        self.assertEqual(spec, "spec-v0001-quick-runset-ff40aae6b1de.json")
        self.assertEqual(
            report,
            "report-v0001-quick-runset-ff40aae6b1de-generated-2026-07-19-211358.123456Z.md",
        )
        self.assertEqual(
            manifest,
            "manifest-v0001-runset-ff40aae6b1de-generated-2026-07-19-211358.123456Z-c0ffee12.json",
        )
        self.assertLessEqual(max(map(len, (root, spec, report, manifest))), 96)

    def test_runset_identity_is_stable_for_resume_and_changes_with_spec_bytes(self) -> None:
        first = CAMPAIGN.runset_identity(b'{"preset":"quick"}\n')
        resumed = CAMPAIGN.runset_identity(b'{"preset":"quick"}\n')
        changed = CAMPAIGN.runset_identity(b'{"preset":"full"}\n')

        self.assertEqual(resumed, first)
        self.assertNotEqual(changed, first)
        self.assertRegex(first, r"^[0-9a-f]{12}$")

    def test_automatic_runset_identity_ignores_path_remapping_but_not_binary_changes(
        self,
    ) -> None:
        spec = {
            "schema_version": 1,
            "campaign_version": "v0001",
            "harness_version": "runner-deadbeef",
            "benchmark_script": "/checkout-a/scripts/benchmark.py",
            "cwd": "/checkout-a",
            "repository_background": {
                "repo": "/corpus-a",
                "revision": "a" * 40,
                "tree": "b" * 40,
            },
            "candidates": [
                {
                    "label": "latest",
                    "revision": "c" * 40,
                    "binary": "/checkout-a/build/cbm",
                    "binary_sha256": "d" * 64,
                    "build": {"compiler": "clang 18", "cflags": "-O2"},
                }
            ],
            "profiles": [{"label": "default", "capabilities": {}}],
        }
        remapped = json.loads(json.dumps(spec))
        remapped["benchmark_script"] = "/checkout-b/scripts/benchmark.py"
        remapped["cwd"] = "/checkout-b"
        remapped["repository_background"]["repo"] = "/corpus-b"
        remapped["candidates"][0]["binary"] = "/checkout-b/build/cbm"
        changed = json.loads(json.dumps(remapped))
        changed["candidates"][0]["binary_sha256"] = "e" * 64

        self.assertEqual(
            CAMPAIGN.automatic_runset_identity(remapped),
            CAMPAIGN.automatic_runset_identity(spec),
        )
        self.assertNotEqual(
            CAMPAIGN.automatic_runset_identity(changed),
            CAMPAIGN.automatic_runset_identity(spec),
        )

    def test_identity_v2_resumes_after_canonical_path_remap_without_changing_legacy_ids(
        self,
    ) -> None:
        original = cell(
            [
                "/checkout-a/scripts/benchmark.py",
                "--binary",
                "/candidate-a/cbm",
                "--repo-root",
                "/corpus-a",
                "--out",
                "{result_path}",
            ],
            cwd="/checkout-a",
            identity_version=2,
            parameters={
                "benchmark_script_sha256": "c" * 64,
                "repository_background": {
                    "repo": "/corpus-a",
                    "revision": "d" * 40,
                    "tree": "e" * 40,
                },
            },
        )
        remapped = json.loads(json.dumps(original))
        remapped["command"][0] = "/checkout-b/scripts/benchmark.py"
        remapped["command"][2] = "/candidate-b/cbm"
        remapped["command"][4] = "/corpus-b"
        remapped["cwd"] = "/checkout-b"
        remapped["parameters"]["repository_background"]["repo"] = "/corpus-b"
        legacy_original = {key: value for key, value in original.items() if key != "identity_version"}
        legacy_remapped = {key: value for key, value in remapped.items() if key != "identity_version"}
        legacy_document = {
            key: legacy_original.get(key)
            for key in CAMPAIGN.IDENTITY_FIELDS
            if key != "identity_version"
        }
        legacy_expected = hashlib.sha256(CAMPAIGN.canonical_json(legacy_document)).hexdigest()[:24]

        self.assertEqual(CAMPAIGN.cell_identity(remapped), CAMPAIGN.cell_identity(original))
        self.assertEqual(CAMPAIGN.cell_identity(legacy_original), legacy_expected)
        self.assertNotEqual(
            CAMPAIGN.cell_identity(legacy_remapped),
            CAMPAIGN.cell_identity(legacy_original),
        )

    def test_no_arguments_selects_quick_preset_and_full_flag_selects_full_matrix(
        self,
    ) -> None:
        quick = CAMPAIGN.parse_arguments([])
        full = CAMPAIGN.parse_arguments(["--full"])
        explicit = CAMPAIGN.parse_arguments(
            ["--matrix-spec", "legacy-spec.json", "--campaign-root", "legacy-results"]
        )

        self.assertEqual(quick.preset, "quick")
        self.assertEqual(full.preset, "full")
        self.assertIsNone(explicit.preset)
        self.assertEqual(explicit.matrix_spec, Path("legacy-spec.json"))
        with self.assertRaises(SystemExit):
            CAMPAIGN.parse_arguments(["--full", "--matrix-spec", "spec.json"])

    def test_default_candidates_use_current_upstream_stable_run_premerge_and_head(
        self,
    ) -> None:
        self.assertEqual(
            CAMPAIGN.DEFAULT_CANDIDATE_REFS,
            (
                ("upstream-main", "upstream/main"),
                (
                    "pre-today-major",
                    "api-consolidation-stable-2026-07-16-semantic-v2",
                ),
                ("pre-upstream-merge", "pre-upstream-main-merge-2026-07-19"),
                ("latest", "HEAD"),
            ),
        )

    def test_automatic_specs_keep_quick_small_and_full_capability_complete(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(root),
                    "config",
                    "user.email",
                    "test@example.invalid",
                ],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(root), "config", "user.name", "Benchmark Test"],
                check=True,
            )
            benchmark = root / "benchmark.py"
            benchmark.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(root), "add", "benchmark.py"], check=True)
            subprocess.run(["git", "-C", str(root), "commit", "-qm", "fixture"], check=True)
            candidates = []
            for index, label in enumerate(("upstream-main", "pre-today-major", "pre-upstream-merge", "latest")):
                binary = root / f"cbm-{label}"
                binary.write_bytes(label.encode())
                candidates.append(
                    {
                        "label": label,
                        "revision": str(index) * 40,
                        "binary": str(binary),
                        "binary_sha256": CAMPAIGN.file_sha256(binary),
                        "build": {"target": "make cbm", "cflags": "-O2"},
                    }
                )

            quick = CAMPAIGN.build_automatic_spec(root, benchmark, candidates, preset="quick")
            full = CAMPAIGN.build_automatic_spec(root, benchmark, candidates, preset="full")

            self.assertEqual(quick["repetitions"], 1)
            self.assertEqual(quick["index_mode"], "fast")
            self.assertIn("commit_datetime", quick["repository_background"])
            self.assertEqual([item["label"] for item in quick["profiles"]], ["default"])
            self.assertEqual(full["repetitions"], 3)
            self.assertEqual(full["index_mode"], "moderate")
            self.assertEqual(
                [item["label"] for item in full["profiles"]],
                [
                    "default",
                    "upstream-equivalent",
                    "eager-derived-freshness",
                    "rank-disabled",
                    "dependency-disabled",
                    "similarity-disabled",
                    "semantic-edges-disabled",
                    "git-history-disabled",
                    "http-links-disabled",
                    "optional-graph-disabled",
                    "minimal-indexing",
                ],
            )
            self.assertTrue(all(item.get("candidate_labels") == ["latest"] for item in full["profiles"][1:]))
            expanded = CAMPAIGN.expand_matrix_spec(quick)
            self.assertEqual(
                expanded["cells"][0]["parameters"]["repository_background"][
                    "commit_datetime"
                ],
                quick["repository_background"]["commit_datetime"],
            )

    def test_materialize_candidate_resolves_tag_builds_and_reuses_detached_worktree(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            repo = base / "repo"
            repo.mkdir()
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo),
                    "config",
                    "user.email",
                    "test@example.invalid",
                ],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(repo), "config", "user.name", "Benchmark Test"],
                check=True,
            )
            (repo / "candidate.sh").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            (repo / "Makefile.cbm").write_text(
                "CFLAGS_PROD = -O3 -DFIXTURE_PRODUCTION=1\n"
                "cbm:\n\tmkdir -p build/c\n\tcp candidate.sh build/c/codebase-memory-mcp\n"
                "\tchmod +x build/c/codebase-memory-mcp\n",
                encoding="utf-8",
            )
            subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "fixture"], check=True)
            subprocess.run(["git", "-C", str(repo), "tag", "stable"], check=True)
            candidate_root = base / "candidates"

            first = CAMPAIGN.materialize_candidate(repo, candidate_root, "stable-candidate", "stable", jobs=1)
            build_logs_after_first = sorted((candidate_root / "build-logs").glob("*.log"))
            second = CAMPAIGN.materialize_candidate(repo, candidate_root, "stable-candidate", "stable", jobs=1)

            expected_revision = subprocess.run(
                ["git", "-C", str(repo), "rev-parse", "stable^{commit}"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            self.assertEqual(first["revision"], expected_revision)
            self.assertEqual(second, first)
            self.assertEqual(second["binary_sha256"], first["binary_sha256"])
            self.assertEqual(second["binary"], first["binary"])
            self.assertEqual(second["build"]["cflags"], "-O3 -DFIXTURE_PRODUCTION=1")
            self.assertTrue(Path(first["binary"]).is_file())
            self.assertEqual(
                sorted((candidate_root / "build-logs").glob("*.log")),
                build_logs_after_first,
            )
            Path(second["binary"]).write_bytes(b"tampered")
            rebuilt = CAMPAIGN.materialize_candidate(
                repo, candidate_root, "stable-candidate", "stable", jobs=1
            )
            self.assertEqual(rebuilt, first)
            self.assertEqual(
                len(list((candidate_root / "build-logs").glob("*.log"))),
                len(build_logs_after_first) + 1,
            )
            worktrees = subprocess.run(
                ["git", "-C", str(repo), "worktree", "list", "--porcelain"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
            self.assertEqual(worktrees.count(expected_revision), 2)

    def test_clean_tree_check_rejects_tracked_edits_but_allows_untracked_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(
                ["git", "-C", str(repo), "config", "user.email", "test@example.invalid"],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(repo), "config", "user.name", "Benchmark Test"],
                check=True,
            )
            tracked = repo / "tracked.txt"
            tracked.write_text("committed\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(repo), "add", "tracked.txt"], check=True)
            subprocess.run(["git", "-C", str(repo), "commit", "-qm", "fixture"], check=True)
            (repo / "retained.log").write_text("untracked evidence\n", encoding="utf-8")

            CAMPAIGN.ensure_clean_tracked_worktree(repo, "fixture")
            tracked.write_text("modified\n", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "fixture has tracked modifications"):
                CAMPAIGN.ensure_clean_tracked_worktree(repo, "fixture")

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

    def test_matrix_spec_expands_structured_frontier_cap_and_repetition_cells(
        self,
    ) -> None:
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
                "accepted_exit_codes": [0, 1],
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
            self.assertEqual(first["accepted_exit_codes"], [0, 1])
            self.assertEqual(first["capability_support"], {"dependencies": True, "rank": True})
            self.assertEqual(
                first["parameters"]["benchmark_script_sha256"],
                CAMPAIGN.file_sha256(benchmark),
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
                    {
                        "name": "go_inbound_frontier",
                        "frontier_files": [4],
                        "exact_caps": [8],
                    }
                ],
            }

            with self.assertRaisesRegex(ValueError, "binary_sha256 does not match"):
                CAMPAIGN.expand_matrix_spec(spec)

    def test_default_profile_rejects_disabled_capability_without_config_override(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            binary = root / "cbm"
            binary.write_bytes(b"binary")
            benchmark = root / "benchmark.py"
            benchmark.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            spec = {
                "schema_version": 1,
                "harness_version": "capability-claims-v1",
                "benchmark_script": str(benchmark),
                "workload": "self_dogfood",
                "repository_background": {
                    "repo": str(root),
                    "revision": "c" * 40,
                    "tree": "d" * 40,
                },
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
                    {
                        "label": "dependency-disabled",
                        "config_profile": "default",
                        "capabilities": {"auto_index_deps": "false"},
                    }
                ],
                "scenarios": [{"name": "c_new_leaf"}],
            }

            with self.assertRaisesRegex(ValueError, "capabilities claims auto_index_deps=false"):
                CAMPAIGN.expand_matrix_spec(spec)

    def test_matrix_spec_expands_capability_quality_without_frontier_axes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            binary = root / "cbm"
            binary.write_bytes(b"optimized-binary")
            benchmark = root / "benchmark-incremental-speed.py"
            benchmark.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            spec = {
                "schema_version": 1,
                "harness_version": "quality-v2",
                "benchmark_script": str(benchmark),
                "capability_quality": "rank",
                "index_mode": "moderate",
                "cwd": str(root),
                "timeout_seconds": 300,
                "accepted_exit_codes": [0, 1],
                "repetitions": 2,
                "transports": ["cli", "mcp"],
                "candidates": [
                    {
                        "label": "latest",
                        "revision": "a" * 40,
                        "binary": str(binary),
                        "build": {"target": "cbm", "cflags": "-O2"},
                        "capability_support": {"rank": True},
                    }
                ],
                "profiles": [
                    {
                        "label": "rank-disabled",
                        "config_profile": "rank_disabled",
                        "capabilities": {"rank_enabled": "false"},
                    }
                ],
            }

            plan = CAMPAIGN.expand_matrix_spec(spec)

            self.assertEqual(len(plan["cells"]), 4)
            first = plan["cells"][0]
            self.assertEqual(first["scenario"], "rank_quality")
            self.assertEqual(first["label"], "latest.rank-disabled.cli.rank_quality")
            self.assertEqual(first["parameters"]["capability_quality"], "rank")
            self.assertEqual(first["parameters"]["index_mode"], "moderate")
            self.assertNotIn("frontier_files", first["parameters"])
            self.assertIn("--capability-quality", first["command"])
            self.assertIn("--index-mode", first["command"])
            self.assertIn("moderate", first["command"])
            self.assertNotIn("--matrix", first["command"])
            self.assertEqual(first["accepted_exit_codes"], [0, 1])

    def test_matrix_spec_scopes_branch_only_profiles_to_named_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            benchmark = root / "benchmark.py"
            benchmark.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            candidates = []
            for label in ("upstream-main", "latest"):
                binary = root / f"cbm-{label}"
                binary.write_bytes(label.encode())
                candidates.append(
                    {
                        "label": label,
                        "revision": ("a" if label == "upstream-main" else "b") * 40,
                        "binary": str(binary),
                        "build": {"cflags": "-O2"},
                    }
                )
            spec = {
                "schema_version": 1,
                "harness_version": "candidate-profile-scope-v1",
                "benchmark_script": str(benchmark),
                "cwd": str(root),
                "repetitions": 1,
                "transports": ["cli"],
                "candidates": candidates,
                "profiles": [
                    {
                        "label": "default",
                        "config_profile": "default",
                        "capabilities": {},
                    },
                    {
                        "label": "rank-disabled",
                        "config_profile": "rank_disabled",
                        "capabilities": {"rank_enabled": "false"},
                        "candidate_labels": ["latest"],
                    },
                ],
                "scenarios": [{"name": "go_modify_1", "frontier_files": [4], "exact_caps": [None]}],
            }

            plan = CAMPAIGN.expand_matrix_spec(spec)

            self.assertEqual(
                [item["label"] for item in plan["cells"]],
                [
                    "upstream-main.default.cli.go_modify_1.f4.capdefault",
                    "latest.default.cli.go_modify_1.f4.capdefault",
                    "latest.rank-disabled.cli.go_modify_1.f4.capdefault",
                ],
            )

    def test_matrix_spec_expands_pinned_self_dogfood_repository_workload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            binary = root / "cbm"
            binary.write_bytes(b"optimized-binary")
            benchmark = root / "benchmark.py"
            benchmark.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            spec = {
                "schema_version": 1,
                "harness_version": "large-repository-v1",
                "benchmark_script": str(benchmark),
                "workload": "self_dogfood",
                "repository_background": {
                    "repo": str(root),
                    "revision": "c" * 40,
                    "tree": "d" * 40,
                },
                "index_mode": "moderate",
                "cwd": str(root),
                "repetitions": 2,
                "transports": ["mcp"],
                "candidates": [
                    {
                        "label": "latest",
                        "revision": "a" * 40,
                        "binary": str(binary),
                        "build": {"cflags": "-O2"},
                    }
                ],
                "profiles": [
                    {
                        "label": "default",
                        "config_profile": "default",
                        "capabilities": {},
                    }
                ],
                "scenarios": [{"name": "route_handler"}],
            }

            plan = CAMPAIGN.expand_matrix_spec(spec)

            self.assertEqual(len(plan["cells"]), 2)
            first = plan["cells"][0]
            self.assertEqual(first["label"], "latest.default.mcp.route_handler")
            self.assertEqual(first["scenario"], "route_handler")
            self.assertIn("--self-dogfood", first["command"])
            self.assertIn("--repo-root", first["command"])
            self.assertIn("--repo-revision", first["command"])
            self.assertIn("--self-dogfood-scenarios", first["command"])
            self.assertEqual(
                first["parameters"]["repository_background"],
                {"repo": str(root.resolve()), "revision": "c" * 40, "tree": "d" * 40},
            )
            self.assertNotIn("--matrix", first["command"])

    def test_paired_interleaved_order_runs_one_repetition_block_at_a_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            benchmark = root / "benchmark.py"
            benchmark.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            candidates = []
            for index in range(2):
                binary = root / f"cbm-{index}"
                binary.write_bytes(f"binary-{index}".encode())
                candidates.append(
                    {
                        "label": f"candidate-{index}",
                        "revision": str(index) * 40,
                        "binary": str(binary),
                        "build": {"cflags": "-O2"},
                    }
                )
            spec = {
                "schema_version": 1,
                "harness_version": "paired-v1",
                "benchmark_script": str(benchmark),
                "capability_quality": "similarity",
                "index_mode": "moderate",
                "execution_order": "paired_interleaved",
                "quality_background": {
                    "repo": str(root),
                    "revision": "c" * 40,
                    "tree": "d" * 40,
                },
                "cwd": str(root),
                "repetitions": 2,
                "transports": ["cli"],
                "candidates": candidates,
                "profiles": [
                    {
                        "label": "default",
                        "config_profile": "default",
                        "capabilities": {},
                    },
                    {
                        "label": "eager",
                        "config_profile": "incremental_semantic_freshness_eager",
                        "capabilities": {"incremental_derived_refresh": "eager"},
                    },
                ],
            }

            plan = CAMPAIGN.expand_matrix_spec(spec)

            self.assertEqual(plan["execution_order"], "paired_interleaved")
            self.assertEqual([cell["repetition"] for cell in plan["cells"]], [1, 1, 1, 1, 2, 2, 2, 2])
            self.assertEqual(
                [cell["parameters"]["execution_position"] for cell in plan["cells"]],
                list(range(1, 9)),
            )
            self.assertEqual(
                [cell["parameters"]["execution_block"] for cell in plan["cells"]],
                [1, 1, 1, 1, 2, 2, 2, 2],
            )
            self.assertIn("--quality-background-repo", plan["cells"][0]["command"])
            self.assertIn("--quality-background-revision", plan["cells"][0]["command"])
            self.assertEqual(
                plan["cells"][0]["parameters"]["quality_background"],
                {"repo": str(root.resolve()), "revision": "c" * 40, "tree": "d" * 40},
            )

    def test_resource_snapshot_records_load_disk_and_host_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot = CAMPAIGN.resource_snapshot(Path(tmpdir))

        self.assertIn("load_average", snapshot)
        self.assertGreater(snapshot["disk"]["total_bytes"], 0)
        self.assertGreaterEqual(snapshot["disk"]["free_bytes"], 0)
        self.assertIn("physical_memory_bytes", snapshot)

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
                    {
                        "label": "default",
                        "config_profile": "default",
                        "capabilities": {},
                    }
                ],
                "scenarios": [
                    {
                        "name": "go_modify_1",
                        "frontier_files": [16],
                        "exact_caps": [None],
                    }
                ],
            }

            cell = CAMPAIGN.expand_matrix_spec(spec)["cells"][0]

            self.assertEqual(cell["label"], "candidate.default.mcp.go_modify_1.f16.capdefault")
            self.assertIsNone(cell["parameters"]["exact_cap"])
            self.assertNotIn("incremental_exact_max_affected_paths", cell["capabilities"])
            self.assertFalse(any("incremental_exact_max_affected_paths=" in item for item in cell["command"]))

    def test_successful_cell_resumes_without_second_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            command = [
                sys.executable,
                "-c",
                (
                    "import json,sys; "
                    "json.dump({'binary_metadata':{'sha256':'"
                    + "b" * 64
                    + "'},'derived':{'passed':True},'cases':[{'passed':True}]},open(sys.argv[1],'w'))"
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
            attempt = next((cell_root / "attempts").iterdir())
            command_record = json.loads((attempt / "command.json").read_text())
            attempt_record = json.loads((attempt / "attempt.json").read_text())
            self.assertIn("resource_before", command_record)
            self.assertIn("resource_after", attempt_record)

    def test_successful_cell_hashes_durable_artifacts_created_by_child(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            command = [
                sys.executable,
                "-c",
                (
                    "import json,os,pathlib,sys; "
                    "artifact=pathlib.Path(os.environ['CBM_BENCHMARK_ARTIFACT_DIR'])/'worker.log.gz'; "
                    "artifact.parent.mkdir(parents=True); artifact.write_bytes(b'audit-log'); "
                    "json.dump({'binary_metadata':{'sha256':'"
                    + "b" * 64
                    + "'},'derived':{'passed':True},'cases':[{'passed':True}]},open(sys.argv[1],'w'))"
                ),
                "{result_path}",
            ]

            planned = cell(command)
            result = CAMPAIGN.run_cell(root, planned, minimum_free_bytes=0)
            cell_root = root / "runs" / CAMPAIGN.cell_identity(planned)
            attempt = next((cell_root / "attempts").iterdir())
            record = json.loads((attempt / "attempt.json").read_text())

            self.assertEqual(result["status"], "completed")
            self.assertEqual(record["artifacts"]["file_count"], 1)
            item = record["artifacts"]["files"][0]
            self.assertEqual(item["path"], "worker.log.gz")
            self.assertEqual(item["size_bytes"], 9)
            self.assertEqual(len(item["sha256"]), 64)

    def test_scan_rejects_changed_missing_or_unlisted_completed_artifacts(self) -> None:
        for mutation in ("changed", "missing", "unlisted"):
            with (
                self.subTest(mutation=mutation),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                root = Path(tmpdir)
                command = [
                    sys.executable,
                    "-c",
                    (
                        "import json,os,pathlib,sys; "
                        "artifact=pathlib.Path(os.environ['CBM_BENCHMARK_ARTIFACT_DIR'])/"
                        "'worker.log.gz'; "
                        "artifact.parent.mkdir(parents=True); artifact.write_bytes(b'audit-log'); "
                        "json.dump({'binary_metadata':{'sha256':'"
                        + "b"
                        * 64
                        + "'},'derived':{'passed':True},'cases':[{'passed':True}]},"
                        "open(sys.argv[1],'w'))"
                    ),
                    "{result_path}",
                ]
                planned = cell(command)
                outcome = CAMPAIGN.run_cell(root, planned, minimum_free_bytes=0)
                cell_root = root / "runs" / CAMPAIGN.cell_identity(planned)
                attempt_root = next((cell_root / "attempts").iterdir())
                artifact_root = attempt_root / "artifacts"
                artifact = artifact_root / "worker.log.gz"

                self.assertEqual(outcome["status"], "completed")
                if mutation == "changed":
                    artifact.write_bytes(b"tampered")
                elif mutation == "missing":
                    artifact.unlink()
                else:
                    (artifact_root / "unlisted.log.gz").write_bytes(b"extra")

                audit = CAMPAIGN.scan_campaign(root, [planned])
                self.assertEqual(audit["counts"]["corrupt"], 1)
                self.assertIn("artifact manifest", audit["cells"][0]["error"])

    def test_report_input_adds_candidate_support_without_mutating_raw_result(
        self,
    ) -> None:
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
            planned["parameters"] = {
                "execution_order": "paired_interleaved",
                "execution_block": 2,
                "execution_position": 7,
            }

            derived = CAMPAIGN.materialize_report_input(root, planned, raw)

            self.assertEqual(json.loads(raw.read_text()), raw_document)
            document = json.loads(derived.read_text())
            self.assertEqual(
                document["parameters"]["capability_support"],
                {"dependencies": False, "rank": False},
            )
            self.assertEqual(document["parameters"]["execution_order"], "paired_interleaved")
            self.assertEqual(document["parameters"]["execution_block"], 2)
            self.assertEqual(document["parameters"]["execution_position"], 7)
            self.assertEqual(
                document["campaign_provenance"]["source_result_sha256"],
                CAMPAIGN.file_sha256(raw),
            )
            self.assertEqual(
                document["campaign_provenance"]["cell_identity"],
                CAMPAIGN.cell_identity(planned),
            )

    def test_result_rejects_background_revision_or_tree_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            planned = cell(["benchmark", "{result_path}"])
            planned["parameters"] = {
                "quality_background": {
                    "repo": str(root),
                    "revision": "a" * 40,
                    "tree": "b" * 40,
                }
            }
            result = root / "result.json"
            result.write_text(
                json.dumps(
                    {
                        "binary_metadata": {"sha256": "b" * 64},
                        "derived": {"passed": True},
                        "cases": [
                            {
                                "passed": True,
                                "background_repository": {
                                    "revision": "c" * 40,
                                    "tree": "b" * 40,
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "background repository revision mismatch"):
                CAMPAIGN.validate_result(result, planned)

    def test_result_rejects_self_dogfood_repository_tree_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            planned = cell(["benchmark", "{result_path}"])
            planned["parameters"] = {
                "repository_background": {
                    "repo": str(root),
                    "revision": "a" * 40,
                    "tree": "b" * 40,
                }
            }
            result = root / "result.json"
            result.write_text(
                json.dumps(
                    {
                        "binary_metadata": {"sha256": "b" * 64},
                        "derived": {"passed": True},
                        "cases": [{"passed": True}],
                        "repository_background": {
                            "revision": "a" * 40,
                            "tree": "c" * 40,
                        },
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "repository background tree mismatch"):
                CAMPAIGN.validate_result(result, planned)

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
            planned = cell([sys.executable, "-c", parent, str(marker)], timeout_seconds=0.5)

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

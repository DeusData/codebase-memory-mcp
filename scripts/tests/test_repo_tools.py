from __future__ import annotations

import importlib.util
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]


def load_script(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class LayoutPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.organization = load_script("organization_checks", "check-repo-organization.py")
        cls.layout = json.loads((ROOT / "docs" / "repository-layout.json").read_text(encoding="utf-8"))

    def valid_paths(self) -> list[str]:
        paths = list(self.layout["rootFiles"])
        for shelf in self.layout["shelves"]:
            if shelf not in {"src", "pkg", "graph-ui"}:
                paths.append(f"{shelf}/catalog-entry.txt")
        paths.extend(f"src/{module}/module.c" for module in self.layout["srcModules"])
        paths.extend(f"src/{name}" for name in self.layout["srcRootFiles"])
        paths.extend(f"pkg/{shelf}/manifest.txt" for shelf in self.layout["packageShelves"])
        paths.extend(f"graph-ui/{name}" for name in self.layout["graphUiRootFiles"])
        paths.append("graph-ui/src/main.tsx")
        return paths

    def test_declared_layout_passes(self) -> None:
        self.assertEqual(self.organization.layout_policy_errors(self.valid_paths(), self.layout), [])

    def test_random_root_file_is_rejected(self) -> None:
        issues = self.organization.layout_policy_errors(
            self.valid_paths() + ["RANDOM_UNCATEGORIZED_BOOK.xyz"],
            self.layout,
        )
        self.assertTrue(any("undeclared root files" in issue for issue in issues))

    def test_unknown_source_module_is_rejected(self) -> None:
        issues = self.organization.layout_policy_errors(
            self.valid_paths() + ["src/random_module/file.c"],
            self.layout,
        )
        self.assertTrue(any("undeclared src modules" in issue for issue in issues))

    def test_random_top_level_shelf_is_rejected(self) -> None:
        issues = self.organization.layout_policy_errors(
            self.valid_paths() + ["random-shelf/uncataloged.txt"],
            self.layout,
        )
        self.assertTrue(any("undeclared shelves" in issue for issue in issues))

    def test_malformed_layout_schema_is_rejected_without_crashing(self) -> None:
        layout = dict(self.layout)
        layout["schemaVersion"] = 2
        layout["rootFiles"] = "README.md"
        layout["graphUiDirectories"] = ["src", "src"]
        issues = self.organization.layout_policy_errors(self.valid_paths(), layout)
        self.assertTrue(any("schemaVersion" in issue for issue in issues))
        self.assertTrue(any("rootFiles must be an array" in issue for issue in issues))
        self.assertTrue(any("graphUiDirectories must not contain duplicates" in issue for issue in issues))


class OrganizationInvariantMutationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.organization = load_script("organization_invariant_mutations", "check-repo-organization.py")

    def invoke(self, function) -> list[str]:
        self.organization.ERRORS.clear()
        self.organization.PASSES.clear()
        function()
        return list(self.organization.ERRORS)

    def test_missing_required_files_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            self.organization, "ROOT", Path(temporary)
        ):
            issues = self.invoke(self.organization.check_required_files)
        self.assertTrue(any("required repository files missing" in issue for issue in issues))

    def test_mismatched_grammar_inventory_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            grammar_root = root / "internal" / "cbm" / "vendored" / "grammars"
            grammar_root.mkdir(parents=True)
            (root / "internal" / "cbm" / "grammar_example.c").write_text("", encoding="utf-8")
            with mock.patch.object(self.organization, "ROOT", root):
                issues = self.invoke(self.organization.check_derived_facts)
        self.assertTrue(any("grammar inventory mismatch" in issue for issue in issues))

    def test_forbidden_artifacts_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ignore = root / ".gitignore"
            ignore.write_text("*.tsbuildinfo\n__pycache__/\n*.py[cod]\ngraph-ui/coverage/\n", encoding="utf-8")
            artifact = root / "graph-ui" / "@" / "duplicate.tsx"
            with (
                mock.patch.object(self.organization, "ROOT", root),
                mock.patch.object(self.organization, "git_visible_files", return_value=[artifact]),
            ):
                issues = self.invoke(self.organization.check_artifacts)
        self.assertTrue(any("forbidden source or generated artifacts" in issue for issue in issues))

    def test_hook_self_test_must_be_executed_by_ci(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "scripts" / "hooks").mkdir(parents=True)
            (root / ".github" / "workflows").mkdir(parents=True)
            (root / "scripts" / "hooks" / "pre-commit").write_text("#!/bin/sh\n", encoding="utf-8")
            (root / "scripts" / "hooks" / "commit-msg").write_text(
                "git interpret-trailers --parse\ngit var GIT_AUTHOR_IDENT\n",
                encoding="utf-8",
            )
            (root / "scripts" / "install-git-hooks.sh").write_text(
                "core.hooksPath scripts/hooks\n--check\nmismatched sign-off\nbash -n\n",
                encoding="utf-8",
            )
            (root / "CONTRIBUTING.md").write_text("install hooks\n", encoding="utf-8")
            (root / ".github" / "workflows" / "_lint.yml").write_text("jobs: {}\n", encoding="utf-8")
            with mock.patch.object(self.organization, "ROOT", root):
                issues = self.invoke(self.organization.check_hooks)
        self.assertIn("lint CI must execute the Git hook behavioral self-test", issues)

    def test_frontend_requires_a_hard_chunk_budget_and_mutation_test(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            src = root / "graph-ui" / "src"
            src.mkdir(parents=True)
            (root / ".github").mkdir()
            (root / ".github" / "workflows").mkdir()
            package = {
                "devDependencies": {"@vitest/coverage-v8": "1"},
                "scripts": {"test:coverage": "vitest run --coverage"},
            }
            (root / "graph-ui" / "package.json").write_text(json.dumps(package), encoding="utf-8")
            (root / "graph-ui" / "vite.config.ts").write_text(
                "thresholds statements: 50 branches: 40 functions: 40 lines: 50 manualChunks",
                encoding="utf-8",
            )
            (src / "App.tsx").write_text("lazy(() => import('./GraphTab'))", encoding="utf-8")
            for number in range(13):
                (src / f"case_{number}.test.ts").write_text("", encoding="utf-8")
            (root / ".github" / "workflows" / "_test.yml").write_text(
                "working-directory: graph-ui\nnpm ci\nnpm run build\nnpm run test:coverage\n",
                encoding="utf-8",
            )
            (root / ".github" / "dependabot.yml").write_text(
                'package-ecosystem: "npm"\ndirectory: "/graph-ui"\n',
                encoding="utf-8",
            )
            with mock.patch.object(self.organization, "ROOT", root):
                issues = self.invoke(self.organization.check_frontend)
        self.assertTrue(any("failing production JavaScript chunk budget" in issue for issue in issues))
        self.assertTrue(any("chunk-budget mutation tests" in issue for issue in issues))

    def test_invalid_first_party_json_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            invalid = root / "broken.json"
            invalid.write_text("{", encoding="utf-8")
            with (
                mock.patch.object(self.organization, "ROOT", root),
                mock.patch.object(self.organization, "git_visible_files", return_value=[invalid]),
            ):
                issues = self.invoke(self.organization.check_json_files)
        self.assertTrue(any("invalid first-party JSON" in issue for issue in issues))

    def test_missing_markdown_target_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "docs").mkdir()
            (root / "pkg").mkdir()
            (root / "README.md").write_text("[missing](missing.md)\n", encoding="utf-8")
            with mock.patch.object(self.organization, "ROOT", root):
                issues = self.invoke(self.organization.check_markdown_links)
        self.assertTrue(any("missing local Markdown links" in issue for issue in issues))

    def test_missing_root_commands_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "Makefile").write_text("help:\n\t@echo help\n", encoding="utf-8")
            with mock.patch.object(self.organization, "ROOT", root):
                issues = self.invoke(self.organization.check_command_surface)
        self.assertTrue(any("root Makefile targets missing" in issue for issue in issues))

    def test_crlf_in_the_index_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / ".gitattributes").write_text("* text=auto eol=lf\n", encoding="utf-8")
            completed = mock.Mock(returncode=0, stdout=b"i/crlf  w/crlf  attr/text\tbad.txt\0")
            with (
                mock.patch.object(self.organization, "ROOT", root),
                mock.patch.object(self.organization.subprocess, "run", return_value=completed),
            ):
                issues = self.invoke(self.organization.check_line_endings)
        self.assertTrue(any("tracked text has CRLF" in issue for issue in issues))

    def test_permanent_mcp_state_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / ".mcp.json").write_text("{}\n", encoding="utf-8")
            with mock.patch.object(self.organization, "ROOT", root):
                issues = self.invoke(self.organization.check_no_permanent_mcp_state)
        self.assertTrue(any("temporary MCP state" in issue for issue in issues))


class TrackerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.organization = load_script("organization_tracker_checks", "check-repo-organization.py")

    @staticmethod
    def valid_tracker() -> dict[str, object]:
        return {
            "schemaVersion": 1,
            "planDate": "2026-07-17",
            "baselineCommit": "e678b2b6acb02bc1ab84a854f2df0e1d092f2cc0",
            "branch": "codex/reorganize-repository",
            "planQualityScore": 100,
            "targetOrganizationScore": 100,
            "executionStatus": "complete",
            "scoreBasis": "verified",
            "tasks": [
                {
                    "id": f"ORG-{number:03d}",
                    "status": "complete",
                    "dependsOn": [] if number == 0 else [f"ORG-{number - 1:03d}"],
                }
                for number in range(18)
            ],
            "historicalMcpAudit": {
                "status": "historical",
                "ephemeral": True,
                "nodes": 1,
                "edges": 1,
                "files": 1,
                "skippedFiles": 0,
                "adrPresent": True,
            },
        }

    def test_complete_tracker_passes(self) -> None:
        self.assertEqual(self.organization.plan_tracker_errors(self.valid_tracker()), [])

    def test_in_progress_tracker_is_structurally_valid(self) -> None:
        tracker = self.valid_tracker()
        tracker["executionStatus"] = "in_progress"
        tracker["planQualityScore"] = 87
        tracker["tasks"][-1]["status"] = "in_progress"
        self.assertEqual(self.organization.plan_tracker_errors(tracker), [])

    def test_complete_tracker_rejects_incomplete_tasks(self) -> None:
        tracker = self.valid_tracker()
        tracker["tasks"][-1]["status"] = "pending"
        issues = self.organization.plan_tracker_errors(tracker)
        self.assertTrue(any("incomplete tasks" in issue for issue in issues))

    def test_tracker_accepts_a_new_well_formed_task(self) -> None:
        tracker = self.valid_tracker()
        tracker["tasks"].append({"id": "ORG-018", "status": "complete", "dependsOn": ["ORG-017"]})
        self.assertEqual(self.organization.plan_tracker_errors(tracker), [])

    def test_live_mcp_metric_claim_is_rejected(self) -> None:
        tracker = self.valid_tracker()
        tracker["finalIndex"] = {"nodes": 1}
        issues = self.organization.plan_tracker_errors(tracker)
        self.assertTrue(any("finalIndex" in issue for issue in issues))

    def test_dependency_cycle_is_rejected(self) -> None:
        tracker = self.valid_tracker()
        tracker["tasks"][0]["dependsOn"] = ["ORG-001"]
        issues = self.organization.plan_tracker_errors(tracker)
        self.assertTrue(any("acyclic" in issue for issue in issues))

    def test_invalid_task_id_and_score_are_rejected(self) -> None:
        tracker = self.valid_tracker()
        tracker["planQualityScore"] = 101
        tracker["tasks"][-1]["id"] = "BOOK-017"
        issues = self.organization.plan_tracker_errors(tracker)
        self.assertTrue(any("planQualityScore" in issue for issue in issues))
        self.assertTrue(any("ORG-NNN" in issue for issue in issues))

    def test_invalid_tracker_provenance_is_rejected(self) -> None:
        tracker = self.valid_tracker()
        tracker["schemaVersion"] = 2
        tracker["baselineCommit"] = "short"
        tracker["historicalMcpAudit"]["nodes"] = -1
        issues = self.organization.plan_tracker_errors(tracker)
        self.assertTrue(any("schemaVersion" in issue for issue in issues))
        self.assertTrue(any("full Git SHA" in issue for issue in issues))
        self.assertTrue(any("nonnegative integers" in issue for issue in issues))


class VersionGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.version = load_script("version_checks", "sync-version.py")

    def test_swapped_aur_architecture_hashes_are_rejected(self) -> None:
        data = json.loads((ROOT / "pkg" / "release-checksums.json").read_text(encoding="utf-8"))
        x64 = data["assets"]["codebase-memory-mcp-linux-amd64.tar.gz"]
        arm = data["assets"]["codebase-memory-mcp-linux-arm64.tar.gz"]
        original_read = self.version.read
        pkgbuild = original_read("pkg/aur/PKGBUILD")
        swapped = pkgbuild.replace(x64, "__X64__").replace(arm, x64).replace("__X64__", arm)

        def fake_read(path):
            normalized = str(path).replace("\\", "/")
            return swapped if normalized == "pkg/aur/PKGBUILD" else original_read(path)

        with mock.patch.object(self.version, "read", side_effect=fake_read):
            issues = self.version.collect_checksum_errors("0.9.0")
        self.assertTrue(any("PKGBUILD x86_64" in issue for issue in issues))
        self.assertTrue(any("PKGBUILD aarch64" in issue for issue in issues))

    def test_stale_winget_urls_are_rejected(self) -> None:
        original_read = self.version.read

        def fake_read(path):
            text = original_read(path)
            if str(path).endswith(".yaml") and "CodebaseMemoryMcp" in str(path):
                return text.replace("/v0.9.0/", "/v9.9.9/").replace("/tag/v0.9.0", "/tag/v9.9.9")
            return text

        with mock.patch.object(self.version, "read", side_effect=fake_read):
            issues = self.version.collect_version_errors("0.9.0")
        self.assertIn("Winget installer URL does not match VERSION", issues)
        self.assertIn("Winget release-notes URL does not match VERSION", issues)

    def test_renamed_winget_locale_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            winget_root = Path(temporary) / "CodebaseMemoryMcp"
            source = ROOT / "pkg" / "winget" / "manifests" / "d" / "DeusData" / "CodebaseMemoryMcp" / "0.9.0"
            target = winget_root / "0.9.0"
            shutil.copytree(source, target)
            locale = target / "DeusData.CodebaseMemoryMcp.locale.en-US.yaml"
            locale.rename(target / "Unexpected.yaml")
            with mock.patch.object(self.version, "WINGET_ROOT", winget_root):
                issues = self.version.collect_version_errors("0.9.0")
        self.assertTrue(any("manifest set is invalid" in issue for issue in issues))

    def test_commented_release_validation_command_is_rejected(self) -> None:
        original_read = self.version.read

        def fake_read(path):
            text = original_read(path)
            if str(path).replace("\\", "/") == ".github/workflows/release.yml":
                return text.replace(
                    "          python3 scripts/sync-version.py",
                    "          # python3 scripts/sync-version.py",
                    1,
                )
            return text

        with mock.patch.object(self.version, "read", side_effect=fake_read):
            issues = self.version.collect_version_errors("0.9.0")
        self.assertTrue(any("Match release input to VERSION" in issue for issue in issues))

    def test_release_workflow_metadata_rewrite_is_rejected(self) -> None:
        original_read = self.version.read

        def fake_read(path):
            text = original_read(path)
            if str(path).replace("\\", "/") == ".github/workflows/release.yml":
                return text.replace(
                    "          echo \"registry packages match VERSION $V\"",
                    "          mv pkg/npm/package.tmp pkg/npm/package.json\n"
                    "          echo \"registry packages match VERSION $V\"",
                    1,
                )
            return text

        with mock.patch.object(self.version, "read", side_effect=fake_read):
            issues = self.version.collect_version_errors("0.9.0")
        self.assertIn("release workflow must verify checked-in versions without rewriting them", issues)

    def test_duplicate_checksum_asset_is_rejected(self) -> None:
        checksum = "a" * 64
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "checksums.txt"
            required = [
                "codebase-memory-mcp-darwin-amd64.tar.gz",
                "codebase-memory-mcp-darwin-arm64.tar.gz",
                "codebase-memory-mcp-linux-amd64.tar.gz",
                "codebase-memory-mcp-linux-arm64.tar.gz",
                "codebase-memory-mcp-windows-amd64.zip",
            ]
            path.write_text(
                "\n".join(f"{checksum}  {name}" for name in required + [required[0]]) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "duplicate assets"):
                self.version.parse_release_checksums(path)

    def test_homebrew_hashes_are_bound_to_their_asset_urls(self) -> None:
        data = json.loads((ROOT / "pkg" / "release-checksums.json").read_text(encoding="utf-8"))
        arm_asset = "codebase-memory-mcp-darwin-arm64.tar.gz"
        x64_asset = "codebase-memory-mcp-darwin-amd64.tar.gz"
        arm = data["assets"][arm_asset]
        x64 = data["assets"][x64_asset]
        original_read = self.version.read
        formula = original_read("pkg/homebrew/Formula/codebase-memory-mcp.rb")
        swapped = formula.replace(arm, "__ARM__").replace(x64, arm).replace("__ARM__", x64)

        def fake_read(path):
            normalized = str(path).replace("\\", "/")
            return swapped if normalized == "pkg/homebrew/Formula/codebase-memory-mcp.rb" else original_read(path)

        with mock.patch.object(self.version, "read", side_effect=fake_read):
            issues = self.version.collect_checksum_errors("0.9.0")
        self.assertTrue(any(arm_asset in issue for issue in issues))
        self.assertTrue(any(x64_asset in issue for issue in issues))

    def test_windows_package_hashes_are_bound_to_the_windows_asset(self) -> None:
        data = json.loads((ROOT / "pkg" / "release-checksums.json").read_text(encoding="utf-8"))
        windows = data["assets"]["codebase-memory-mcp-windows-amd64.zip"]
        wrong = data["assets"]["codebase-memory-mcp-darwin-amd64.tar.gz"]
        original_read = self.version.read
        consumers = {
            "pkg/scoop/codebase-memory-mcp.json",
            "pkg/chocolatey/tools/chocolateyInstall.ps1",
            "pkg/winget/manifests/d/DeusData/CodebaseMemoryMcp/0.9.0/DeusData.CodebaseMemoryMcp.installer.yaml",
        }

        def fake_read(path):
            normalized = str(path).replace("\\", "/")
            text = original_read(path)
            return text.replace(windows, wrong) if normalized in consumers else text

        with mock.patch.object(self.version, "read", side_effect=fake_read):
            issues = self.version.collect_checksum_errors("0.9.0")
        self.assertTrue(any("pkg/scoop" in issue for issue in issues))
        self.assertTrue(any("pkg/chocolatey" in issue for issue in issues))
        self.assertTrue(any("pkg/winget" in issue for issue in issues))

    def test_srcinfo_hashes_are_bound_to_their_architectures(self) -> None:
        data = json.loads((ROOT / "pkg" / "release-checksums.json").read_text(encoding="utf-8"))
        x64 = data["assets"]["codebase-memory-mcp-linux-amd64.tar.gz"]
        arm = data["assets"]["codebase-memory-mcp-linux-arm64.tar.gz"]
        original_read = self.version.read
        srcinfo = original_read("pkg/aur/.SRCINFO")
        swapped = srcinfo.replace(x64, "__X64__").replace(arm, x64).replace("__X64__", arm)

        def fake_read(path):
            normalized = str(path).replace("\\", "/")
            return swapped if normalized == "pkg/aur/.SRCINFO" else original_read(path)

        with mock.patch.object(self.version, "read", side_effect=fake_read):
            issues = self.version.collect_checksum_errors("0.9.0")
        self.assertTrue(any(".SRCINFO x86_64" in issue for issue in issues))
        self.assertTrue(any(".SRCINFO aarch64" in issue for issue in issues))

    def test_failed_update_rolls_back_every_surface(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            mutated = ("VERSION", "metadata.txt")
            for relative in mutated:
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("original\n", encoding="utf-8")
            winget = root / "winget"
            (winget / "0.9.0").mkdir(parents=True)
            (winget / "0.9.0" / "manifest.yaml").write_text("original\n", encoding="utf-8")

            def fail_update(_version, _checksums):
                (root / "VERSION").write_text("changed\n", encoding="utf-8")
                shutil.rmtree(winget)
                raise RuntimeError("deliberate failure")

            with (
                mock.patch.object(self.version, "ROOT", root),
                mock.patch.object(self.version, "WINGET_ROOT", winget),
                mock.patch.object(self.version, "MUTATED_FILES", mutated),
                mock.patch.object(self.version, "update_versions", side_effect=fail_update),
            ):
                with self.assertRaisesRegex(RuntimeError, "deliberate failure"):
                    self.version.update_versions_transactionally("1.0.0", root / "checksums.txt")
            self.assertEqual((root / "VERSION").read_text(encoding="utf-8"), "original\n")
            self.assertEqual((winget / "0.9.0" / "manifest.yaml").read_text(encoding="utf-8"), "original\n")

    def test_failed_post_update_parity_check_rolls_back_every_surface(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            mutated = ("VERSION", "metadata.txt")
            for relative in mutated:
                (root / relative).write_text("original\n", encoding="utf-8")
            winget = root / "winget"
            (winget / "0.9.0").mkdir(parents=True)
            (winget / "0.9.0" / "manifest.yaml").write_text("original\n", encoding="utf-8")

            def update(_version, _checksums):
                (root / "VERSION").write_text("changed\n", encoding="utf-8")
                (root / "metadata.txt").write_text("changed\n", encoding="utf-8")

            with (
                mock.patch.object(self.version, "ROOT", root),
                mock.patch.object(self.version, "WINGET_ROOT", winget),
                mock.patch.object(self.version, "MUTATED_FILES", mutated),
                mock.patch.object(self.version, "update_versions", side_effect=update),
                mock.patch.object(self.version, "check", return_value=1),
            ):
                with self.assertRaisesRegex(RuntimeError, "did not pass parity"):
                    self.version.update_versions_transactionally("1.0.0", root / "checksums.txt")
            self.assertEqual((root / "VERSION").read_text(encoding="utf-8"), "original\n")
            self.assertEqual((root / "metadata.txt").read_text(encoding="utf-8"), "original\n")
            self.assertEqual((winget / "0.9.0" / "manifest.yaml").read_text(encoding="utf-8"), "original\n")


class IndexGeneratorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.generator = load_script("repository_index", "generate-repository-index.py")

    def test_inventory_reads_only_the_staged_git_index(self) -> None:
        completed = mock.Mock(returncode=0, stdout=b"README.md\0DOES_NOT_EXIST.md\0", stderr=b"")
        with mock.patch.object(self.generator.subprocess, "run", return_value=completed) as run:
            self.assertEqual(self.generator.tracked_files(), ["DOES_NOT_EXIST.md", "README.md"])
        command = run.call_args.args[0]
        self.assertIn(Path(command[0]).name.lower(), {"git", "git.exe"})
        self.assertEqual(command[1:], ["ls-files", "--cached", "-z"])

    def test_staged_blob_reader_does_not_open_the_working_tree(self) -> None:
        completed = mock.Mock(returncode=0, stdout=b'{"schemaVersion": 1}\n', stderr=b"")
        with (
            mock.patch.object(self.generator.subprocess, "run", return_value=completed) as run,
            mock.patch.object(Path, "read_text", side_effect=AssertionError("working tree read")),
        ):
            self.assertEqual(
                self.generator.read_index_text("docs/repository-layout.json"),
                '{"schemaVersion": 1}\n',
            )
        self.assertEqual(run.call_args.args[0][1:], ["show", ":docs/repository-layout.json"])

    def test_git_resolves_from_standard_windows_install_without_path(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            program_files = Path(temporary)
            executable = program_files / "Git" / "cmd" / "git.exe"
            executable.parent.mkdir(parents=True)
            executable.touch()
            with (
                mock.patch.dict(self.generator.os.environ, {"ProgramFiles": str(program_files)}, clear=True),
                mock.patch.object(self.generator.shutil, "which", return_value=None),
            ):
                self.assertEqual(self.generator.resolve_git(), str(executable))


class TestRegistrationGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registration = load_script("test_registration_checks", "check-test-registration.py")

    def run_fixture(self, test_source: str, main_source: str) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            tests = root / "tests"
            tests.mkdir()
            (root / "Makefile.cbm").write_text(
                "TEST_SRCS := tests/test_main.c tests/test_example.c\n",
                encoding="utf-8",
            )
            (tests / "test_main.c").write_text(main_source, encoding="utf-8")
            (tests / "test_example.c").write_text(test_source, encoding="utf-8")
            with (
                mock.patch.object(self.registration, "ROOT", root),
                mock.patch.object(self.registration, "TESTS", tests),
                mock.patch("builtins.print") as output,
            ):
                result = self.registration.main()
        messages = "\n".join(str(call.args[0]) for call in output.call_args_list if call.args)
        return result, messages

    def test_registered_file_without_a_suite_is_rejected(self) -> None:
        result, messages = self.run_fixture("/* inert */\n", "/* runner */\n")
        self.assertEqual(result, 1)
        self.assertIn("defines no suite", messages)

    def test_registered_suite_declared_and_run_once_passes(self) -> None:
        result, _ = self.run_fixture(
            "SUITE(example) { }\n",
            "extern void suite_example(void);\nRUN_SELECTED_SUITE(example);\n",
        )
        self.assertEqual(result, 0)

    def test_duplicate_suite_definition_is_rejected(self) -> None:
        result, messages = self.run_fixture(
            "SUITE(example) { }\nSUITE(example) { }\n",
            "extern void suite_example(void);\nRUN_SELECTED_SUITE(example);\n",
        )
        self.assertEqual(result, 1)
        self.assertIn("suite definitions must be unique", messages)

    def test_duplicate_runner_call_is_rejected(self) -> None:
        result, messages = self.run_fixture(
            "SUITE(example) { }\n",
            "extern void suite_example(void);\n"
            "RUN_SELECTED_SUITE(example);\nRUN_SELECTED_SUITE(example);\n",
        )
        self.assertEqual(result, 1)
        self.assertIn("runner calls suites more than once", messages)


if __name__ == "__main__":
    unittest.main()

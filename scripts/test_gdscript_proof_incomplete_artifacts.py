#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def fail(message: str) -> None:
    print(f"FAIL: {message}")
    sys.exit(1)


def write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def git(*args: str, cwd: Path, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def extract_run_root(output: str) -> Path:
    match = re.search(r"Proof run root: (.+)", output)
    if not match:
        fail(f"proof run root not found in output:\n{output}")
    return Path(match.group(1).strip())


def build_fixture_repo(temp_root: Path) -> tuple[Path, str]:
    fixture_repo = temp_root / "fixture-repo"
    fixture_repo.mkdir()
    write_file(
        fixture_repo / "project.godot",
        """; Engine configuration file.
[application]
config/name=\"Proof Fixture\"
config/features=PackedStringArray(\"4.3\")
""",
    )
    write_file(
        fixture_repo / "PlayerWalking.gd",
        """class_name PlayerWalking

signal hit

func Update():
    Move()

func Move():
    Transition(\"Idle\")

func Transition(_state):
    pass
""",
    )
    git("init", cwd=fixture_repo)
    git("add", ".", cwd=fixture_repo)
    commit_env = os.environ.copy()
    commit_env.update(
        {
            "GIT_AUTHOR_NAME": "CBM Test",
            "GIT_AUTHOR_EMAIL": "cbm-test@example.invalid",
            "GIT_COMMITTER_NAME": "CBM Test",
            "GIT_COMMITTER_EMAIL": "cbm-test@example.invalid",
        }
    )
    git("commit", "-m", "fixture", cwd=fixture_repo, env=commit_env)
    return fixture_repo, git("rev-parse", "HEAD", cwd=fixture_repo)


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    proof_script = repo_root / "scripts" / "gdscript-proof.sh"
    if not proof_script.is_file():
        fail(f"proof script not found at {proof_script}")

    with tempfile.TemporaryDirectory(prefix="gd-proof-incomplete-artifacts-") as tmpdir:
        temp_root = Path(tmpdir)
        repo_path, pinned_commit = build_fixture_repo(temp_root)

        manifest_path = temp_root / "manifest.json"
        manifest = {
            "version": 1,
            "language": "gdscript",
            "promotion_target": "good",
            "minimum_repo_count": 1,
            "repos": [
                {
                    "label": "fixture-repo",
                    "remote": "https://example.invalid/fixture-repo.git",
                    "pinned_commit": pinned_commit,
                    "godot_version": "4.3",
                    "required_for": ["same-script-calls"],
                    "assertions": [
                        {
                            "id": "calls.same_script_edges",
                            "query": "gd-same-script-calls",
                            "classification": "gating",
                            "expected": {"count": 2},
                        }
                    ],
                }
            ],
        }
        manifest_path.write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )

        env = os.environ.copy()
        env["GDSCRIPT_PROOF_INJECT_INVALID_QUERY"] = "gd-classes"
        result = subprocess.run(
            [
                str(proof_script),
                "--manifest",
                str(manifest_path),
                "--repo",
                str(repo_path),
                "--label",
                f"{repo_path}=fixture-repo",
            ],
            cwd=repo_root,
            env=env,
            capture_output=True,
            text=True,
        )
        output = result.stdout + result.stderr
        run_root = extract_run_root(output)
        if result.returncode == 0:
            fail(f"incomplete proof run unexpectedly passed:\n{output}")

        repo_dirs = [
            path
            for path in run_root.iterdir()
            if path.is_dir() and (path / "repo-meta.json").is_file()
        ]
        if len(repo_dirs) != 1:
            fail(f"expected 1 repo artifact dir, found {len(repo_dirs)} in {run_root}")

        repo_dir = repo_dirs[0]
        repo_meta = json.loads(
            (repo_dir / "repo-meta.json").read_text(encoding="utf-8")
        )
        run_index = json.loads(
            (run_root / "run-index.json").read_text(encoding="utf-8")
        )

        if repo_meta.get("task5", {}).get("status") != "incomplete":
            fail(f"repo-meta task5 status should be incomplete: {repo_meta}")
        if (repo_meta.get("task5", {}).get("message") or "").find("gd-classes") == -1:
            fail(f"repo-meta missing failed query details: {repo_meta}")

        gd_files_wrapper = repo_dir / "queries" / "gd-files.json"
        if not gd_files_wrapper.is_file():
            fail(f"expected partial wrapper evidence at {gd_files_wrapper}")

        repo_index = (run_index.get("repos") or {}).get(repo_meta.get("artifact_slug"))
        if not isinstance(repo_index, dict):
            fail(f"run index missing repo entry: {run_index}")

        if (
            repo_index.get("artifacts", {}).get("repo_meta")
            != f"{repo_meta['artifact_slug']}/repo-meta.json"
        ):
            fail(f"run index repo_meta path incorrect: {repo_index}")

        query_artifacts = repo_index.get("artifacts", {}).get("queries") or {}
        gd_files_entry = query_artifacts.get("gd-files") or {}
        if gd_files_entry.get("status") != "present":
            fail(f"gd-files wrapper should be marked present: {gd_files_entry}")
        if (
            gd_files_entry.get("path")
            != f"{repo_meta['artifact_slug']}/queries/gd-files.json"
        ):
            fail(f"gd-files wrapper path incorrect: {gd_files_entry}")

        gd_classes_entry = query_artifacts.get("gd-classes") or {}
        if gd_classes_entry.get("status") != "failed":
            fail(f"gd-classes wrapper should be marked failed: {gd_classes_entry}")
        if gd_classes_entry.get("path") is not None:
            fail(
                f"failed gd-classes wrapper should not have a path: {gd_classes_entry}"
            )

        missing_queries = repo_index.get("query_wrappers_missing") or []
        if "gd-classes" not in missing_queries:
            fail(f"run index missing query_wrappers_missing metadata: {repo_index}")

        shutil.rmtree(run_root, ignore_errors=True)
        print("PASS")


if __name__ == "__main__":
    main()

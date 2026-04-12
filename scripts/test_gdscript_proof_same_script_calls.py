#!/usr/bin/env python3
"""Regression test for gdscript-proof same-script call evidence.

Builds a tiny temporary GDScript repo, runs scripts/gdscript-proof.sh in
manifest mode, and asserts the proof passes only when same-script method CALLS
are counted while signal targets are excluded.

Exit codes:
    0 - PASS
    1 - FAIL
"""

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


def load_run_index(run_root: Path) -> dict:
    run_index_path = run_root / "run-index.json"
    if not run_index_path.is_file():
        fail(f"run index missing at {run_index_path}")
    return json.loads(run_index_path.read_text(encoding="utf-8"))


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    proof_script = repo_root / "scripts" / "gdscript-proof.sh"
    if not proof_script.is_file():
        fail(f"proof script not found at {proof_script}")

    with tempfile.TemporaryDirectory(prefix="gd-proof-same-script-") as tmpdir:
        temp_root = Path(tmpdir)
        fixture_repo = temp_root / "fixture-repo"
        fixture_repo.mkdir()

        write_file(
            fixture_repo / "project.godot",
            """; Engine configuration file.
[application]
config/name="Proof Fixture"
config/features=PackedStringArray("4.3")
""",
        )
        write_file(
            fixture_repo / "PlayerWalking.gd",
            """class_name PlayerWalking

signal hit

func Update():
    Move()
    self.hit.emit()

func Move():
    Transition("Idle")

func Transition(_state):
    pass

func _on_hit():
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
        pinned_commit = git("rev-parse", "HEAD", cwd=fixture_repo)

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
                            "expected": {
                                "count": 2,
                                "contains_edges": [
                                    "PlayerWalking.gd:Update->Move",
                                    "PlayerWalking.gd:Move->Transition",
                                ],
                            },
                        }
                    ],
                }
            ],
        }
        manifest_path.write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )

        cmd = [
            str(proof_script),
            "--manifest",
            str(manifest_path),
            "--repo",
            str(fixture_repo),
            "--label",
            f"{fixture_repo}=fixture-repo",
            "--godot-version",
            f"{fixture_repo}=4.3",
        ]
        result = subprocess.run(cmd, cwd=repo_root, capture_output=True, text=True)
        output = result.stdout + result.stderr

        run_root = extract_run_root(output)
        if result.returncode != 0:
            fail(
                f"proof command exited {result.returncode}; run root: {run_root}\n{output}"
            )

        aggregate_summary = run_root / "aggregate-summary.md"
        if not aggregate_summary.is_file():
            fail(f"aggregate summary missing at {aggregate_summary}")
        summary_text = aggregate_summary.read_text(encoding="utf-8")
        if "Final outcome: `pass`" not in summary_text:
            fail(f"aggregate summary did not report pass:\n{summary_text}")

        run_index = load_run_index(run_root)
        semantic_pairs = run_index.get("semantic_pairs") or {}
        fixture_pair = semantic_pairs.get("fixture-repo") or {}
        requested_modes = fixture_pair.get("requested_modes") or {}
        if sorted(requested_modes) != ["parallel", "sequential"]:
            fail(f"semantic_pairs missing sequential/parallel entries: {fixture_pair}")

        for requested_mode in ("sequential", "parallel"):
            artifact_slug = (requested_modes.get(requested_mode) or {}).get(
                "artifact_slug"
            )
            if not artifact_slug:
                fail(
                    f"artifact slug missing for requested mode {requested_mode}: {fixture_pair}"
                )
            repo_summary = run_root / artifact_slug / "summary.md"
            if not repo_summary.is_file():
                fail(f"repo summary missing at {repo_summary}")
            repo_summary_text = repo_summary.read_text(encoding="utf-8")
            if (
                "calls.same_script_edges" not in repo_summary_text
                or "| calls.same_script_edges | gd-same-script-calls | `pass` |"
                not in repo_summary_text
            ):
                fail(
                    f"same-script assertion did not pass in {requested_mode} repo summary:\n{repo_summary_text}"
                )

        shutil.rmtree(run_root, ignore_errors=True)
        print("PASS")


if __name__ == "__main__":
    main()

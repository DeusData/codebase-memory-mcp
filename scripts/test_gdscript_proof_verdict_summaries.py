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


def manifest_payload(pinned_commit: str, same_script_count: int) -> dict:
    return {
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
                            "count": same_script_count,
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


def run_manifest_case(
    repo_root: Path,
    proof_script: Path,
    repo_path: Path,
    manifest: dict,
    *,
    injected_invalid_query: str | None = None,
) -> tuple[subprocess.CompletedProcess[str], Path]:
    with tempfile.TemporaryDirectory(prefix="gd-proof-verdict-case-") as tmpdir:
        temp_root = Path(tmpdir)
        manifest_path = temp_root / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )

        env = os.environ.copy()
        if injected_invalid_query:
            env["GDSCRIPT_PROOF_INJECT_INVALID_QUERY"] = injected_invalid_query

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
        persisted_run_root = repo_root / ".artifacts" / "gdscript-proof-test-copy"
        if persisted_run_root.exists():
            shutil.rmtree(persisted_run_root, ignore_errors=True)
        shutil.copytree(run_root, persisted_run_root)
        return result, persisted_run_root


def repo_summaries(run_root: Path) -> list[str]:
    return [
        (repo_dir / "summary.md").read_text(encoding="utf-8")
        for repo_dir in run_root.iterdir()
        if repo_dir.is_dir() and (repo_dir / "summary.md").is_file()
    ]


def require_contains(text: str, needle: str, context: str) -> None:
    if needle not in text:
        fail(f"missing {needle!r} in {context}:\n{text}")


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    proof_script = repo_root / "scripts" / "gdscript-proof.sh"
    if not proof_script.is_file():
        fail(f"proof script not found at {proof_script}")

    with tempfile.TemporaryDirectory(prefix="gd-proof-verdict-summaries-") as tmpdir:
        temp_root = Path(tmpdir)
        repo_path, pinned_commit = build_fixture_repo(temp_root)

        pass_result, pass_root = run_manifest_case(
            repo_root,
            proof_script,
            repo_path,
            manifest_payload(pinned_commit, same_script_count=2),
        )
        if pass_result.returncode != 0:
            fail(f"pass manifest run failed:\n{pass_result.stdout}{pass_result.stderr}")
        pass_aggregate = (pass_root / "aggregate-summary.md").read_text(
            encoding="utf-8"
        )
        require_contains(
            pass_aggregate, "## Promotion decision", "pass aggregate summary"
        )
        require_contains(
            pass_aggregate,
            "Promotion answer: `qualified-support-only`",
            "pass aggregate summary",
        )
        require_contains(
            pass_aggregate,
            "Claim scope: approved manifest corpus only; current commit only",
            "pass aggregate summary",
        )
        for summary in repo_summaries(pass_root):
            require_contains(summary, "## Verdict", "pass repo summary")
            require_contains(summary, "Repo verdict: `pass`", "pass repo summary")
            require_contains(
                summary,
                "Approval contribution: `counts-toward-qualified-support`",
                "pass repo summary",
            )

        fail_result, fail_root = run_manifest_case(
            repo_root,
            proof_script,
            repo_path,
            manifest_payload(pinned_commit, same_script_count=3),
        )
        if fail_result.returncode == 0:
            fail("failing manifest run unexpectedly passed")
        fail_aggregate = (fail_root / "aggregate-summary.md").read_text(
            encoding="utf-8"
        )
        require_contains(
            fail_aggregate,
            "Promotion answer: `do-not-promote`",
            "fail aggregate summary",
        )
        require_contains(
            fail_aggregate, "Final outcome: `fail`", "fail aggregate summary"
        )
        for summary in repo_summaries(fail_root):
            require_contains(summary, "Repo verdict: `fail`", "fail repo summary")
            require_contains(
                summary,
                "Approval contribution: `does-not-count-toward-promotion`",
                "fail repo summary",
            )
            require_contains(
                summary,
                "| calls.same_script_edges | gd-same-script-calls | `fail` |",
                "fail repo summary",
            )

        incomplete_result, incomplete_root = run_manifest_case(
            repo_root,
            proof_script,
            repo_path,
            manifest_payload(pinned_commit, same_script_count=2),
            injected_invalid_query="gd-classes",
        )
        if incomplete_result.returncode == 0:
            fail("incomplete manifest run unexpectedly passed")
        incomplete_aggregate = (incomplete_root / "aggregate-summary.md").read_text(
            encoding="utf-8"
        )
        require_contains(
            incomplete_aggregate,
            "Promotion answer: `do-not-promote`",
            "incomplete aggregate summary",
        )
        require_contains(
            incomplete_aggregate,
            "Final outcome: `incomplete`",
            "incomplete aggregate summary",
        )
        for summary in repo_summaries(incomplete_root):
            require_contains(
                summary,
                "Repo verdict: `incomplete`",
                "incomplete repo summary",
            )
            require_contains(
                summary,
                "Approval contribution: `does-not-count-toward-promotion`",
                "incomplete repo summary",
            )
            require_contains(
                summary,
                "## Comparability issues",
                "incomplete repo summary",
            )
            require_contains(summary, "gd-classes", "incomplete repo summary")

        shutil.rmtree(pass_root, ignore_errors=True)
        shutil.rmtree(fail_root, ignore_errors=True)
        shutil.rmtree(incomplete_root, ignore_errors=True)
        print("PASS")


if __name__ == "__main__":
    main()

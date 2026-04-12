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
        fixture_repo / "subproject" / "PlayerWalking.gd",
        """class_name PlayerWalking

signal hit

func Update():
    Move()
    self.hit.emit()

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
    return fixture_repo / "subproject", git("rev-parse", "HEAD", cwd=fixture_repo)


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    proof_script = repo_root / "scripts" / "gdscript-proof.sh"
    if not proof_script.is_file():
        fail(f"proof script not found at {proof_script}")

    with tempfile.TemporaryDirectory(prefix="gd-proof-semantic-parity-") as tmpdir:
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
                    "project_subpath": "subproject",
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
            capture_output=True,
            text=True,
        )
        output = result.stdout + result.stderr
        run_root = extract_run_root(output)
        if result.returncode != 0:
            fail(f"manifest run failed: {result.returncode}\n{output}")

        semantic_parity_json = run_root / "semantic-parity.json"
        semantic_parity_md = run_root / "semantic-parity.md"
        if not semantic_parity_json.is_file():
            fail(f"semantic parity json missing at {semantic_parity_json}")
        if not semantic_parity_md.is_file():
            fail(f"semantic parity markdown missing at {semantic_parity_md}")

        payload = json.loads(semantic_parity_json.read_text(encoding="utf-8"))
        pair = (payload.get("semantic_pairs") or {}).get("fixture-repo") or {}
        requirements = pair.get("requirements") or {}
        missing = [
            key
            for key in ["SEM-01", "SEM-02", "SEM-03", "SEM-04", "SEM-05", "SEM-06"]
            if key not in requirements
        ]
        if missing:
            fail(f"semantic parity payload missing requirements: {missing}")

        sem01 = requirements["SEM-01"]
        class_count = (sem01.get("class_count") or {}).get("count") or {}
        method_count = (sem01.get("method_count") or {}).get("count") or {}
        if class_count.get("sequential", 0) <= 0 or class_count.get("parallel", 0) <= 0:
            fail(f"SEM-01 class counts must be non-zero: {sem01}")
        if (
            method_count.get("sequential", 0) <= 0
            or method_count.get("parallel", 0) <= 0
        ):
            fail(f"SEM-01 method counts must be non-zero: {sem01}")

        class_samples = (sem01.get("class_sample") or {}).get(
            "representative_samples"
        ) or {}
        method_samples = (sem01.get("method_sample") or {}).get(
            "representative_samples"
        ) or {}
        if not class_samples.get("sequential") or not class_samples.get("parallel"):
            fail(f"SEM-01 class samples missing representative values: {sem01}")
        if not method_samples.get("sequential") or not method_samples.get("parallel"):
            fail(f"SEM-01 method samples missing representative values: {sem01}")

        for requirement_id in ["SEM-02", "SEM-03", "SEM-04", "SEM-05"]:
            requirement = requirements[requirement_id]
            if "count" not in requirement:
                fail(f"{requirement_id} missing count comparison: {requirement}")
            if "representative_edges" not in requirement:
                fail(
                    f"{requirement_id} missing representative edge comparison: {requirement}"
                )

        markdown = semantic_parity_md.read_text(encoding="utf-8")
        for needle in [
            "SEM-01",
            "SEM-02",
            "SEM-03",
            "SEM-04",
            "SEM-05",
            "SEM-06",
            "gd-class-sample",
            "gd-method-sample",
            "sequential",
            "parallel",
        ]:
            if needle not in markdown:
                fail(f"semantic parity markdown missing {needle!r}:\n{markdown}")

        shutil.rmtree(run_root, ignore_errors=True)
        print("PASS")


if __name__ == "__main__":
    main()

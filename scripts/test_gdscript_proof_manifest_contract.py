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


def load_run_index(run_root: Path) -> dict:
    run_index_path = run_root / "run-index.json"
    if not run_index_path.is_file():
        fail(f"run index missing at {run_index_path}")
    return json.loads(run_index_path.read_text(encoding="utf-8"))


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

    with tempfile.TemporaryDirectory(prefix="gd-proof-manifest-contract-") as tmpdir:
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

        manifest_result = subprocess.run(
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
        manifest_output = manifest_result.stdout + manifest_result.stderr
        manifest_root = extract_run_root(manifest_output)
        if manifest_result.returncode != 0:
            fail(
                f"manifest run failed: {manifest_result.returncode}\n{manifest_output}"
            )

        repo_dirs = [
            p
            for p in manifest_root.iterdir()
            if p.is_dir() and (p / "repo-meta.json").is_file()
        ]
        if len(repo_dirs) != 2:
            fail(f"expected 2 manifest repo artifact dirs, found {len(repo_dirs)}")
        manifest_metas = {
            (meta.get("requested_mode") or ""): meta
            for meta in (
                json.loads((repo_dir / "repo-meta.json").read_text(encoding="utf-8"))
                for repo_dir in repo_dirs
            )
        }
        manifest_meta = manifest_metas.get("sequential")
        if manifest_meta is None or "parallel" not in manifest_metas:
            fail(f"manifest dual-mode metadata missing: {manifest_metas}")
        manifest_summary = (
            manifest_root / f"{manifest_meta['artifact_slug']}" / "summary.md"
        ).read_text(encoding="utf-8")
        run_index = load_run_index(manifest_root)

        identity = manifest_meta.get("canonical_identity") or {}
        if identity.get("remote") != "https://example.invalid/fixture-repo.git":
            fail(f"manifest canonical remote missing: {manifest_meta}")
        if identity.get("pinned_commit") != pinned_commit:
            fail(f"manifest canonical commit missing: {manifest_meta}")
        if identity.get("project_subpath") != "subproject":
            fail(f"manifest canonical project_subpath missing: {manifest_meta}")
        if identity.get("godot_version") != "4.3":
            fail(f"manifest canonical godot_version missing: {manifest_meta}")
        if manifest_meta.get("approval_status") != "canonical-approval-bearing":
            fail(f"manifest approval status incorrect: {manifest_meta}")
        if manifest_meta.get("qualification_status") != "godot-4.x-qualifying":
            fail(f"manifest qualification status incorrect: {manifest_meta}")
        if manifest_meta.get("comparison_label") != "fixture-repo":
            fail(f"comparison label missing from repo meta: {manifest_meta}")
        if manifest_meta.get("requested_mode") != "sequential":
            fail(f"requested mode missing from repo meta: {manifest_meta}")
        if manifest_meta.get("actual_mode") != "sequential":
            fail(f"actual mode missing from repo meta: {manifest_meta}")
        if manifest_metas["parallel"].get("requested_mode") != "parallel":
            fail(
                f"parallel requested mode missing from repo meta: {manifest_metas['parallel']}"
            )
        if manifest_metas["parallel"].get("actual_mode") != "parallel":
            fail(
                f"parallel actual mode missing from repo meta: {manifest_metas['parallel']}"
            )
        if (
            "canonical-approval-bearing" not in manifest_summary
            or "godot-4.x-qualifying" not in manifest_summary
        ):
            fail(
                f"manifest summary missing canonical/qualifying labels:\n{manifest_summary}"
            )

        query_suite = run_index.get("query_suite") or []
        if "gd-same-script-calls" not in query_suite:
            fail(f"run index query suite missing gd-same-script-calls: {run_index}")

        repo_index = (run_index.get("repos") or {}).get(
            manifest_meta.get("artifact_slug")
        )
        if not isinstance(repo_index, dict):
            fail(
                f"run index missing repo entry for {manifest_meta.get('artifact_slug')}: {run_index}"
            )
        if repo_index.get("requested_mode") != "sequential":
            fail(f"run index requested_mode incorrect: {repo_index}")
        if repo_index.get("actual_mode") != "sequential":
            fail(f"run index actual_mode incorrect: {repo_index}")

        if (
            repo_index.get("artifacts", {}).get("repo_meta")
            != f"{manifest_meta['artifact_slug']}/repo-meta.json"
        ):
            fail(f"run index repo_meta path incorrect: {repo_index}")
        if (
            repo_index.get("artifacts", {}).get("summary")
            != f"{manifest_meta['artifact_slug']}/summary.md"
        ):
            fail(f"run index summary path incorrect: {repo_index}")

        query_artifacts = repo_index.get("artifacts", {}).get("queries") or {}
        same_script_wrapper = query_artifacts.get("gd-same-script-calls") or {}
        if same_script_wrapper.get("status") != "present":
            fail(f"run index wrapper status should be present: {same_script_wrapper}")
        if (
            same_script_wrapper.get("path")
            != f"{manifest_meta['artifact_slug']}/queries/gd-same-script-calls.json"
        ):
            fail(f"run index wrapper path incorrect: {same_script_wrapper}")

        semantic_pairs = run_index.get("semantic_pairs") or {}
        fixture_pair = semantic_pairs.get("fixture-repo") or {}
        requested_modes = fixture_pair.get("requested_modes") or {}
        if sorted(requested_modes) != ["parallel", "sequential"]:
            fail(f"semantic_pairs missing sequential/parallel entries: {fixture_pair}")
        if (
            requested_modes["sequential"].get("artifact_slug")
            != manifest_meta["artifact_slug"]
        ):
            fail(f"semantic_pairs sequential artifact mismatch: {fixture_pair}")
        if requested_modes["parallel"].get("actual_mode") != "parallel":
            fail(f"semantic_pairs parallel actual mode mismatch: {fixture_pair}")
        if not (manifest_root / "semantic-parity.json").is_file():
            fail("semantic-parity.json missing from manifest run")
        if not (manifest_root / "semantic-parity.md").is_file():
            fail("semantic-parity.md missing from manifest run")
        semantic_parity = json.loads(
            (manifest_root / "semantic-parity.json").read_text(encoding="utf-8")
        )
        if "semantic_pairs" not in semantic_parity:
            fail(f"semantic-parity.json missing semantic_pairs: {semantic_parity}")
        if "fixture-repo" not in (semantic_parity.get("semantic_pairs") or {}):
            fail(f"semantic-parity.json missing fixture-repo pair: {semantic_parity}")

        ad_hoc_result = subprocess.run(
            [
                str(proof_script),
                "--repo",
                str(repo_path),
                "--godot-version",
                f"{repo_path}=4.3",
            ],
            cwd=repo_root,
            capture_output=True,
            text=True,
        )
        ad_hoc_output = ad_hoc_result.stdout + ad_hoc_result.stderr
        ad_hoc_root = extract_run_root(ad_hoc_output)

        repo_dirs = [
            p
            for p in ad_hoc_root.iterdir()
            if p.is_dir() and (p / "repo-meta.json").is_file()
        ]
        if len(repo_dirs) != 1:
            fail(f"expected 1 ad hoc repo artifact dir, found {len(repo_dirs)}")
        ad_hoc_meta = json.loads(
            (repo_dirs[0] / "repo-meta.json").read_text(encoding="utf-8")
        )
        ad_hoc_summary = (repo_dirs[0] / "summary.md").read_text(encoding="utf-8")

        if ad_hoc_meta.get("approval_status") != "non-canonical-debug-only":
            fail(f"ad hoc approval status incorrect: {ad_hoc_meta}")
        if ad_hoc_meta.get("qualification_status") != "non-qualifying":
            fail(f"ad hoc qualification status incorrect: {ad_hoc_meta}")
        if (
            "non-canonical-debug-only" not in ad_hoc_summary
            or "non-qualifying" not in ad_hoc_summary
        ):
            fail(
                f"ad hoc summary missing non-canonical/non-qualifying labels:\n{ad_hoc_summary}"
            )

        shutil.rmtree(manifest_root, ignore_errors=True)
        shutil.rmtree(ad_hoc_root, ignore_errors=True)
        print("PASS")


if __name__ == "__main__":
    main()

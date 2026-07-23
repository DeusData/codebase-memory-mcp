#!/usr/bin/env python3
"""Run an immutable, resumable benchmark experiment plan and retain an auditable disk trail.

This is the canonical entry point. The legacy `run-benchmark-campaign.py` filename,
flags, persisted keys, and `.worktrees/benchmark-campaign/` directory remain readable
for compatibility; new interfaces and records use "experiment" consistently.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CONFIG_SPELLING_SPEC_PATH = Path(__file__).with_name(
    "benchmark-config-spellings-v1.json"
)
with CONFIG_SPELLING_SPEC_PATH.open(encoding="utf-8") as stream:
    CONFIG_SPELLING_SPEC = json.load(stream)
if CONFIG_SPELLING_SPEC.get("schema_version") != 1:
    raise RuntimeError(
        f"unsupported benchmark config spelling schema: {CONFIG_SPELLING_SPEC_PATH}"
    )
DERIVED_RESULTS_AT_PUBLISH_PROFILE = CONFIG_SPELLING_SPEC["profiles"][
    "derived_results_refresh_at_publish"
]["canonical"]
DERIVED_RESULTS_AT_PUBLISH_EXPERIMENT_LABEL = CONFIG_SPELLING_SPEC["experiment_labels"][
    "derived_results_refresh_at_publish"
]["canonical"]
CONFIG_OVERRIDE_SPELLINGS = {
    entry["id"]: entry for entry in CONFIG_SPELLING_SPEC["config_overrides"]
}
DERIVED_RESULTS_AT_PUBLISH_OVERRIDE = CONFIG_OVERRIDE_SPELLINGS[
    "incremental_derived_results_refresh_at_publish"
]["canonical"]


SCHEMA_VERSION = 1
EXPERIMENT_DEFINITION_VERSION = 1
DEFAULT_MINIMUM_FREE_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_STALE_LOCK_SECONDS = 6 * 60 * 60
FILENAME_DATETIME_FORMAT = "%Y-%m-%d-%H%M%S.%fZ"
DEFAULT_CANDIDATE_REFS = (
    ("upstream-main", "upstream/main"),
    ("pre-today-major", "api-consolidation-stable-2026-07-16-semantic-v2"),
    ("pre-upstream-merge", "pre-upstream-main-merge-2026-07-19"),
    ("latest", "HEAD"),
)
# Fallback chain tried only for the built-in "upstream/main" baseline default, which
# requires a remote literally named "upstream". Era-pinned tags (pre-today-major,
# pre-upstream-merge) and every --candidate-ref override stay fail-closed: an
# unresolvable ref raises rather than silently substituting a different comparison
# point. Once api-consolidation merges to main and "upstream" stops existing, this
# lets --quick/--full keep working without editing DEFAULT_CANDIDATE_REFS.
UPSTREAM_MAIN_FALLBACK_REFS = ("upstream/main", "origin/main", "main")
IDENTITY_FIELDS = (
    "identity_version",
    "revision",
    "binary_sha256",
    "build",
    "capabilities",
    "capability_support",
    "transport",
    "scenario",
    "repetition",
    "harness_version",
    "command",
    "cwd",
    "environment",
    "parameters",
    "timeout_seconds",
    "accepted_exit_codes",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def filename_datetime(moment: datetime | None = None) -> str:
    """Return a sortable, filename-safe UTC datetime with collision-level precision."""
    current = moment or datetime.now(timezone.utc)
    return current.astimezone(timezone.utc).strftime(FILENAME_DATETIME_FORMAT)


def experiment_version() -> str:
    """Return the sortable version of the experiment definition, not a run number."""
    return f"v{EXPERIMENT_DEFINITION_VERSION:04d}"


def read_experiment_version(document: dict[str, Any]) -> str | None:
    """Read the current key or its legacy on-disk spelling without writing the legacy key."""
    current = document.get("experiment_version")
    legacy = document.get("campaign_version")
    if current is not None and legacy is not None and current != legacy:
        raise ValueError("experiment_version conflicts with legacy campaign_version")
    value = current if current is not None else legacy
    if value is not None and value != experiment_version():
        raise ValueError(f"experiment_version must be {experiment_version()}")
    return value


def runset_identity(spec_payload: bytes) -> str:
    """Identify an immutable runset so preparing the same spec resumes in place."""
    return hashlib.sha256(spec_payload).hexdigest()[:12]


def automatic_runset_identity(spec: dict[str, Any]) -> str:
    """Hash semantic inputs while allowing an identical runset to be path-remapped."""
    normalized = json.loads(json.dumps(spec))
    normalized.pop("runset_id", None)
    normalized.pop("benchmark_script", None)
    normalized.pop("cwd", None)
    for background_key in ("repository_background", "quality_background"):
        background = normalized.get(background_key)
        if isinstance(background, dict):
            background.pop("repo", None)
    candidates = normalized.get("candidates")
    if isinstance(candidates, list):
        for candidate in candidates:
            if isinstance(candidate, dict):
                candidate.pop("binary", None)
    return runset_identity(canonical_json(normalized))


def _validate_runset_identity(runset: str) -> str:
    if len(runset) != 12 or any(char not in "0123456789abcdef" for char in runset):
        raise ValueError(
            f"runset identity must be 12 lowercase hexadecimal characters: {runset!r}"
        )
    return runset


def automatic_experiment_name(preset: str, source: dict[str, str], runset: str) -> str:
    """Name a resumable experiment without confusing source and execution datetimes."""
    if preset not in {"quick", "full"}:
        raise ValueError(f"automatic preset must be quick or full: {preset!r}")
    revision = source.get("revision", "")
    commit_datetime = source.get("commit_datetime_slug", "")
    if len(revision) != 40 or not commit_datetime:
        raise ValueError("source must contain a full revision and commit_datetime_slug")
    return (
        f"{experiment_version()}-{preset}-commit-{commit_datetime}-{revision[:12]}-"
        f"runset-{_validate_runset_identity(runset)}"
    )


def automatic_spec_name(preset: str, runset: str) -> str:
    if preset not in {"quick", "full"}:
        raise ValueError(f"automatic preset must be quick or full: {preset!r}")
    return f"spec-{experiment_version()}-{preset}-runset-{_validate_runset_identity(runset)}.json"


def generated_artifact_name(
    kind: str,
    runset: str,
    suffix: str,
    *,
    preset: str | None = None,
    moment: datetime | None = None,
    nonce: str | None = None,
) -> str:
    """Name generated evidence while keeping its stable runset identity visible."""
    if not kind or any(not (char.isalnum() or char == "-") for char in kind):
        raise ValueError(f"artifact kind is not path-safe: {kind!r}")
    if preset is not None and preset not in {"quick", "full", "custom"}:
        raise ValueError(f"artifact preset is invalid: {preset!r}")
    if not suffix.startswith(".") or "/" in suffix:
        raise ValueError(f"artifact suffix is invalid: {suffix!r}")
    if nonce is not None and (
        not nonce or any(not (char.isalnum() or char in "-_") for char in nonce)
    ):
        raise ValueError(f"artifact nonce is not path-safe: {nonce!r}")
    parts = [kind, experiment_version()]
    if preset is not None:
        parts.append(preset)
    parts.extend(
        (
            "runset",
            _validate_runset_identity(runset),
            "generated",
            filename_datetime(moment),
        )
    )
    if nonce is not None:
        parts.append(nonce)
    return "-".join(parts) + suffix


def _run_text(command: list[str], *, cwd: Path) -> str:
    process = subprocess.run(
        command, cwd=cwd, capture_output=True, text=True, check=False
    )
    if process.returncode != 0:
        detail = process.stderr.strip() or process.stdout.strip() or "no diagnostic"
        raise RuntimeError(
            f"command failed ({process.returncode}): {' '.join(command)}: {detail}"
        )
    return process.stdout.strip()


def resolve_commit(repository: Path, ref: str) -> str:
    """Peel a branch, tag, or commit ref to the full commit object ID."""
    revision = _run_text(
        ["git", "rev-parse", "--verify", f"{ref}^{{commit}}"],
        cwd=repository,
    )
    if len(revision) != 40 or any(
        char not in "0123456789abcdef" for char in revision.lower()
    ):
        raise RuntimeError(
            f"git resolved {ref!r} to an invalid commit ID: {revision!r}"
        )
    return revision.lower()


def commit_identity(repository: Path, ref: str) -> dict[str, str]:
    """Return a peeled commit, its repository datetime, and its exact tree."""
    revision = resolve_commit(repository, ref)
    committed_at = _run_text(
        ["git", "show", "-s", "--format=%cI", revision], cwd=repository
    )
    try:
        parsed = datetime.fromisoformat(committed_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise RuntimeError(
            f"git returned an invalid commit datetime for {revision}: {committed_at!r}"
        ) from error
    tree = _run_text(
        ["git", "rev-parse", "--verify", f"{revision}^{{tree}}"], cwd=repository
    )
    return {
        "revision": revision,
        "committed_at": parsed.isoformat(),
        "commit_datetime_slug": parsed.strftime("%Y-%m-%d-%H%M"),
        "tree": tree,
    }


def resolve_default_candidate_ref(repository: Path, label: str, ref: str) -> str:
    """Resolve a default candidate ref, retrying survivable baseline aliases only.

    Only the built-in "upstream/main" baseline gets a fallback chain (see
    UPSTREAM_MAIN_FALLBACK_REFS), because it is the one default expected to age past
    a merge: the remote may be renamed or absent in a fresh clone. Era-pinned tag
    defaults and explicit --candidate-ref overrides are not touched here and remain
    fail-closed in materialize_candidate: an unresolvable ref raises a clear error
    instead of silently running a different comparison.
    """
    del label
    if ref != "upstream/main":
        return ref
    for candidate_ref in UPSTREAM_MAIN_FALLBACK_REFS:
        try:
            resolve_commit(repository, candidate_ref)
        except RuntimeError:
            continue
        return candidate_ref
    return ref


def parse_candidate_ref_override(value: str) -> tuple[str, str]:
    """Parse one repeatable --candidate-ref LABEL=REF argument."""
    label, separator, ref = value.partition("=")
    if not separator or not label or not ref:
        raise ValueError(f"--candidate-ref must be LABEL=REF: {value!r}")
    known_labels = {default_label for default_label, _ in DEFAULT_CANDIDATE_REFS}
    if label not in known_labels:
        raise ValueError(
            f"--candidate-ref label must be one of {sorted(known_labels)}: {label!r}"
        )
    return label, ref


def _path_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _candidate_slug(label: str) -> str:
    if not label or any(not (char.isalnum() or char in "-_") for char in label):
        raise ValueError(f"candidate label is not path-safe: {label!r}")
    return label


def ensure_clean_tracked_worktree(repository: Path, role: str) -> None:
    """Reject tracked edits while allowing ignored retained evidence and build output."""
    tracked_status = _run_text(
        ["git", "status", "--porcelain", "--untracked-files=no"], cwd=repository
    )
    if tracked_status:
        raise RuntimeError(
            f"{role} has tracked modifications; commit or restore them before measurement: "
            f"{repository}"
        )


def _registered_candidate_worktrees(
    repository: Path, candidate_root: Path, revision: str
) -> list[Path]:
    listing = _run_text(["git", "worktree", "list", "--porcelain"], cwd=repository)
    matches: list[Path] = []
    for block in listing.split("\n\n"):
        fields: dict[str, str] = {}
        for line in block.splitlines():
            key, separator, value = line.partition(" ")
            if separator:
                fields[key] = value
        path_value = fields.get("worktree")
        if fields.get("HEAD") == revision and path_value:
            candidate = Path(path_value).resolve()
            if _path_within(candidate, candidate_root):
                matches.append(candidate)
    return sorted(matches)


def _compiler_identity(worktree: Path) -> str:
    try:
        return _run_text(["cc", "--version"], cwd=worktree).splitlines()[0]
    except (OSError, RuntimeError, IndexError):
        return "unknown (see datetime-named build log)"


def _production_cflags(worktree: Path) -> str:
    """Read the candidate Makefile's canonical production flags without duplicating them."""
    target = "cbm-print-production-flags"
    definition = f"{target}:\n\t@printf '%s\\n' '$(CFLAGS_PROD)'\n"
    try:
        process = subprocess.run(
            [
                "make",
                "-s",
                "-f",
                "Makefile.cbm",
                "-f",
                "-",
                target,
            ],
            cwd=worktree,
            input=definition,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return "unknown (see candidate Makefile.cbm and build log)"
    if process.returncode != 0:
        return "unknown (see candidate Makefile.cbm and build log)"
    value = process.stdout.strip()
    return value or "not declared by candidate Makefile.cbm"


def _candidate_capability_support(label: str) -> dict[str, bool]:
    if label == "upstream-main":
        return {
            "rank": False,
            "dependencies": False,
            "similarity": True,
            "semantic_edges": True,
            "git_history": True,
            "http_links": False,
        }
    return {
        "rank": True,
        "dependencies": True,
        "similarity": True,
        "semantic_edges": True,
        "git_history": True,
        "http_links": True,
    }


def materialize_candidate(
    repository: Path,
    candidate_root: Path,
    label: str,
    ref: str,
    *,
    jobs: int = 2,
) -> dict[str, Any]:
    """Resolve, isolate, production-build, and hash one benchmark candidate."""
    repository = repository.expanduser().resolve()
    candidate_root = candidate_root.expanduser().resolve()
    safe_label = _candidate_slug(label)
    if jobs <= 0:
        raise ValueError("build jobs must be positive")
    source_identity = commit_identity(repository, ref)
    revision = source_identity["revision"]
    candidate_root.mkdir(parents=True, exist_ok=True)
    intended = candidate_root / f"{safe_label}-{revision[:12]}"
    matches = _registered_candidate_worktrees(repository, candidate_root, revision)
    if intended in matches:
        worktree = intended
    elif matches:
        worktree = matches[0]
    else:
        if intended.exists():
            raise RuntimeError(
                f"candidate path exists but is not the registered {revision} worktree: {intended}"
            )
        process = subprocess.run(
            ["git", "worktree", "add", "--detach", str(intended), revision],
            cwd=repository,
            capture_output=True,
            text=True,
            check=False,
        )
        if process.returncode != 0:
            detail = process.stderr.strip() or process.stdout.strip() or "no diagnostic"
            raise RuntimeError(
                f"could not create candidate worktree {intended}: {detail}"
            )
        worktree = intended
    actual_revision = resolve_commit(worktree, "HEAD")
    if actual_revision != revision:
        raise RuntimeError(
            f"candidate worktree HEAD mismatch: expected={revision} actual={actual_revision} path={worktree}"
        )
    ensure_clean_tracked_worktree(worktree, "candidate worktree")

    binary = worktree / "build" / "c" / "codebase-memory-mcp"
    stable_build = {
        "target": f"make -j{jobs} -f Makefile.cbm cbm",
        "compiler": _compiler_identity(worktree),
        "cflags": _production_cflags(worktree),
        "source_commit_datetime": source_identity["committed_at"],
        "source_tree": source_identity["tree"],
    }
    cache_path = (
        candidate_root
        / "cache"
        / f"candidate-{experiment_version()}-{safe_label}-commit-{revision[:12]}.json"
    )
    if cache_path.is_file() and binary.is_file():
        try:
            cached = read_json_object(cache_path).get("candidate")
            if (
                isinstance(cached, dict)
                and cached.get("label") == safe_label
                and cached.get("revision") == revision
                and cached.get("binary") == str(binary)
                and cached.get("build") == stable_build
                and cached.get("tree") == source_identity["tree"]
                and cached.get("binary_sha256") == file_sha256(binary)
            ):
                return cached
        except (OSError, ValueError, json.JSONDecodeError):
            pass

    stamp = filename_datetime()
    log_root = candidate_root / "build-logs"
    log_root.mkdir(parents=True, exist_ok=True)
    build_log = (
        log_root / f"generated-{stamp}-for-{safe_label}-commit-{revision[:12]}.log"
    )
    command = ["make", f"-j{jobs}", "-f", "Makefile.cbm", "cbm"]
    with build_log.open("w", encoding="utf-8") as stream:
        stream.write(f"started_at_utc={utc_now()}\n")
        stream.write(f"revision={revision}\n")
        stream.write(f"command={' '.join(command)}\n")
        stream.flush()
        process = subprocess.run(
            command,
            cwd=worktree,
            stdout=stream,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        stream.write(f"finished_at_utc={utc_now()}\n")
        stream.write(f"exit_code={process.returncode}\n")
    if process.returncode != 0:
        raise RuntimeError(
            f"candidate production build failed ({process.returncode}); see {build_log}"
        )
    if not binary.is_file():
        raise RuntimeError(f"candidate build did not produce {binary}; see {build_log}")
    candidate = {
        "label": safe_label,
        "revision": revision,
        "binary": str(binary),
        "binary_sha256": file_sha256(binary),
        "build": stable_build,
        "capability_support": _candidate_capability_support(safe_label),
        "commit_datetime": source_identity["committed_at"],
        "tree": source_identity["tree"],
    }
    metadata_root = candidate_root / "metadata"
    metadata_root.mkdir(parents=True, exist_ok=True)
    atomic_write_json(
        metadata_root
        / f"generated-{stamp}-for-{safe_label}-commit-{revision[:12]}.json",
        {
            **candidate,
            "ref": ref,
            "worktree": str(worktree),
            "build_log": str(build_log),
            "recorded_at_utc": utc_now(),
        },
    )
    atomic_write_json(
        cache_path,
        {
            "schema_version": SCHEMA_VERSION,
            "experiment_version": experiment_version(),
            "candidate": candidate,
        },
    )
    return candidate


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def artifact_manifest(root: Path) -> dict[str, Any]:
    files = []
    total_bytes = 0
    if root.is_dir():
        for path in sorted(item for item in root.rglob("*") if item.is_file()):
            size = path.stat().st_size
            total_bytes += size
            files.append(
                {
                    "path": path.relative_to(root).as_posix(),
                    "size_bytes": size,
                    "sha256": file_sha256(path),
                }
            )
    return {"file_count": len(files), "total_bytes": total_bytes, "files": files}


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    payload = json.dumps(value, indent=2, sort_keys=True) + "\n"
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            # Some filesystems do not support directory fsync. The file itself
            # is still synced before the atomic replacement.
            pass
    finally:
        if temporary.exists():
            temporary.unlink()


def atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    try:
        with temporary.open("wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def read_json_object(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def build_automatic_spec(
    repository: Path,
    benchmark_script: Path,
    candidates: list[dict[str, Any]],
    *,
    preset: str,
) -> dict[str, Any]:
    """Build the canonical safe quick or repeated full capability matrix."""
    if preset not in {"quick", "full"}:
        raise ValueError("preset must be quick or full")
    repository = repository.expanduser().resolve()
    benchmark_script = benchmark_script.expanduser().resolve()
    if not benchmark_script.is_file():
        raise ValueError(f"benchmark script does not exist: {benchmark_script}")
    expected_labels = [label for label, _ in DEFAULT_CANDIDATE_REFS]
    actual_labels = [candidate.get("label") for candidate in candidates]
    if actual_labels != expected_labels:
        raise ValueError(
            f"automatic candidates must be ordered {expected_labels}, got {actual_labels}"
        )
    repository_identity = commit_identity(repository, "HEAD")
    repository_revision = repository_identity["revision"]
    repository_tree = repository_identity["tree"]
    runner_sha = file_sha256(Path(__file__).resolve())
    benchmark_sha = file_sha256(benchmark_script)
    latest_labels = [label for label, ref in DEFAULT_CANDIDATE_REFS if ref == "HEAD"]
    native_candidate_labels = [
        label for label, ref in DEFAULT_CANDIDATE_REFS if ref != "HEAD"
    ]
    product_defaults = {
        "auto_index_deps": "false",
        "rank_enabled": "true",
        "similarity_enabled": "true",
        "semantic_edges_enabled": "true",
        "githistory_enabled": "true",
        "httplinks_enabled": "true",
    }

    def capabilities(**changes: str) -> dict[str, str]:
        values = dict(product_defaults)
        values.update(changes)
        return values

    profiles: list[dict[str, Any]] = [
        {
            "label": "candidate-native-configuration",
            "config_profile": "candidate_native_configuration",
            "candidate_labels": native_candidate_labels,
            "capabilities": {},
        },
        {
            "label": "automatic-dependency-source-indexing-disabled",
            "config_profile": "automatic_dependency_source_indexing_disabled",
            "candidate_labels": latest_labels,
            "capabilities": capabilities(),
        },
    ]
    if preset == "full":
        profiles.extend(
            (
                {
                    "label": "automatic-dependency-source-indexing-enabled",
                    "config_profile": "automatic_dependency_source_indexing_enabled",
                    "candidate_labels": latest_labels,
                    "capabilities": capabilities(auto_index_deps="true"),
                },
                {
                    "label": "upstream-equivalent",
                    "config_profile": "automatic_dependency_source_indexing_disabled",
                    "candidate_labels": latest_labels,
                    "capabilities": capabilities(
                        rank_enabled="false", httplinks_enabled="false"
                    ),
                    "config_overrides": {
                        "auto_index_deps": "false",
                        "rank_enabled": "false",
                        "httplinks_enabled": "false",
                    },
                },
                {
                    "label": DERIVED_RESULTS_AT_PUBLISH_EXPERIMENT_LABEL,
                    "config_profile": DERIVED_RESULTS_AT_PUBLISH_PROFILE,
                    "candidate_labels": latest_labels,
                    "capabilities": {
                        **capabilities(),
                        DERIVED_RESULTS_AT_PUBLISH_OVERRIDE[
                            "key"
                        ]: DERIVED_RESULTS_AT_PUBLISH_OVERRIDE["value"],
                    },
                },
                {
                    "label": "rank-disabled",
                    "config_profile": "rank_disabled",
                    "candidate_labels": latest_labels,
                    "capabilities": capabilities(rank_enabled="false"),
                },
                {
                    "label": "similarity-disabled",
                    "config_profile": "similarity_disabled",
                    "candidate_labels": latest_labels,
                    "capabilities": capabilities(similarity_enabled="false"),
                },
                {
                    "label": "semantic-edges-disabled",
                    "config_profile": "semantic_edges_disabled",
                    "candidate_labels": latest_labels,
                    "capabilities": capabilities(semantic_edges_enabled="false"),
                },
                {
                    "label": "git-history-disabled",
                    "config_profile": "git_history_disabled",
                    "candidate_labels": latest_labels,
                    "capabilities": capabilities(githistory_enabled="false"),
                },
                {
                    "label": "http-links-disabled",
                    "config_profile": "http_links_disabled",
                    "candidate_labels": latest_labels,
                    "capabilities": capabilities(httplinks_enabled="false"),
                },
                {
                    "label": "optional-graph-disabled",
                    "config_profile": "optional_graph_disabled",
                    "candidate_labels": latest_labels,
                    "capabilities": capabilities(
                        rank_enabled="false",
                        similarity_enabled="false",
                        semantic_edges_enabled="false",
                        githistory_enabled="false",
                        httplinks_enabled="false",
                    ),
                },
                {
                    "label": "minimal-indexing",
                    "config_profile": "minimal_indexing",
                    "candidate_labels": latest_labels,
                    "capabilities": capabilities(
                        rank_enabled="false",
                        similarity_enabled="false",
                        semantic_edges_enabled="false",
                        githistory_enabled="false",
                        httplinks_enabled="false",
                    ),
                },
            )
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "experiment_version": experiment_version(),
        "identity_version": 2,
        "harness_version": (
            f"automatic-{preset}:benchmark-{benchmark_sha}:runner-{runner_sha}"
        ),
        "benchmark_script": str(benchmark_script),
        "workload": "self_dogfood",
        "repository_background": {
            "repo": str(repository),
            "revision": repository_revision,
            "tree": repository_tree,
            "commit_datetime": repository_identity["committed_at"],
        },
        "index_mode": "fast" if preset == "quick" else "moderate",
        "execution_order": "paired_interleaved",
        "cwd": str(repository),
        "timeout_seconds": 900,
        "cell_timeout_seconds": 1800,
        "accepted_exit_codes": [0, 1],
        "repetitions": 1 if preset == "quick" else 3,
        "transports": ["mcp"],
        "candidates": candidates,
        "profiles": profiles,
        "scenarios": [{"name": "c_new_leaf"}],
    }


def identity_document(cell: dict[str, Any]) -> dict[str, Any]:
    if cell.get("identity_version") != 2:
        return {
            key: cell.get(key) for key in IDENTITY_FIELDS if key != "identity_version"
        }
    document = {key: cell.get(key) for key in IDENTITY_FIELDS}

    command = list(document.get("command") or [])
    if command:
        command[0] = "{benchmark_script}"
    for flag, replacement in (
        ("--binary", "{candidate_binary}"),
        ("--repo-root", "{repository_root}"),
        ("--quality-background-repo", "{quality_background_root}"),
    ):
        for index, token in enumerate(command[:-1]):
            if token == flag:
                command[index + 1] = replacement
    document["command"] = command
    if document.get("cwd") is not None:
        document["cwd"] = "{working_directory}"
    parameters = json.loads(json.dumps(document.get("parameters") or {}))
    for background_key in ("repository_background", "quality_background"):
        background = parameters.get(background_key)
        if isinstance(background, dict) and "repo" in background:
            background["repo"] = f"{{{background_key}_root}}"
    document["parameters"] = parameters
    return document


def cell_identity(cell: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(identity_document(cell))).hexdigest()[:24]


def validate_cell(cell: dict[str, Any], index: int) -> None:
    required = {
        "label": str,
        "revision": str,
        "binary_sha256": str,
        "build": dict,
        "capabilities": dict,
        "transport": str,
        "scenario": str,
        "repetition": int,
        "harness_version": str,
        "command": list,
    }
    for key, expected_type in required.items():
        if not isinstance(cell.get(key), expected_type):
            raise ValueError(f"cells[{index}].{key} must be {expected_type.__name__}")
    if not cell["label"] or "=" in cell["label"]:
        raise ValueError(
            f"cells[{index}].label must be non-empty and cannot contain '='"
        )
    if len(cell["revision"]) != 40:
        raise ValueError(
            f"cells[{index}].revision must be a full 40-character commit hash"
        )
    if len(cell["binary_sha256"]) != 64:
        raise ValueError(f"cells[{index}].binary_sha256 must be a full SHA-256")
    if not cell["command"] or not all(
        isinstance(item, str) for item in cell["command"]
    ):
        raise ValueError(f"cells[{index}].command must be a non-empty string array")
    if not _is_positive_json_integer(cell["repetition"]):
        raise ValueError(f"cells[{index}].repetition must be a positive integer")
    timeout_seconds = cell.get("timeout_seconds")
    if timeout_seconds is not None and not _is_positive_json_number(timeout_seconds):
        raise ValueError(
            f"cells[{index}].timeout_seconds must be a positive finite number"
        )
    identity_version = cell.get("identity_version", 1)
    if not _is_json_integer(identity_version) or identity_version not in {1, 2}:
        raise ValueError(f"cells[{index}].identity_version must be 1 or 2")
    accepted = cell.get("accepted_exit_codes", [0])
    if (
        not isinstance(accepted, list)
        or not accepted
        or not all(_is_json_integer(code) for code in accepted)
    ):
        raise ValueError(
            f"cells[{index}].accepted_exit_codes must be a non-empty integer array"
        )
    support = cell.get("capability_support")
    if support is not None and (
        not isinstance(support, dict)
        or not all(
            isinstance(key, str) and isinstance(value, bool)
            for key, value in support.items()
        )
    ):
        raise ValueError(
            f"cells[{index}].capability_support must be a string-to-boolean object"
        )


def validate_plan(plan: dict[str, Any]) -> list[dict[str, Any]]:
    if (
        not _is_json_integer(plan.get("schema_version"))
        or plan.get("schema_version") != SCHEMA_VERSION
    ):
        raise ValueError(f"schema_version must be {SCHEMA_VERSION}")
    cells = plan.get("cells")
    if not isinstance(cells, list) or not cells:
        raise ValueError("cells must be a non-empty array")
    typed_cells: list[dict[str, Any]] = []
    identities: set[str] = set()
    for index, value in enumerate(cells):
        if not isinstance(value, dict):
            raise ValueError(f"cells[{index}] must be an object")
        validate_cell(value, index)
        identity = cell_identity(value)
        if identity in identities:
            raise ValueError(f"duplicate cell identity at cells[{index}]: {identity}")
        identities.add(identity)
        typed_cells.append(value)
    return typed_cells


def _string_map(value: Any, field: str) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in value.items()
    ):
        raise ValueError(f"{field} must be a string-to-string object")
    return dict(value)


def _is_json_integer(value: Any) -> bool:
    """Return whether a decoded JSON value is an integer rather than a boolean."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_positive_json_integer(value: Any) -> bool:
    return _is_json_integer(value) and value > 0


def _is_positive_json_number(value: Any) -> bool:
    """Accept finite positive JSON numbers while keeping booleans distinct."""
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def _nonempty_list(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field} must be a non-empty array")
    return value


def _optional_iso_datetime(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be an ISO 8601 datetime string")
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} must be an ISO 8601 datetime string") from error
    return value


def expand_matrix_spec(spec: dict[str, Any]) -> dict[str, Any]:
    """Expand a compact benchmark grid into immutable experiment cells."""
    if (
        not _is_json_integer(spec.get("schema_version"))
        or spec.get("schema_version") != SCHEMA_VERSION
    ):
        raise ValueError(f"schema_version must be {SCHEMA_VERSION}")
    harness_version = spec.get("harness_version")
    benchmark_script = spec.get("benchmark_script")
    cwd = spec.get("cwd")
    repetitions = spec.get("repetitions")
    benchmark_timeout = spec.get("timeout_seconds", 240)
    index_mode = spec.get("index_mode", "fast")
    accepted_exit_codes = spec.get("accepted_exit_codes", [0])
    capability_quality = spec.get("capability_quality")
    workload = spec.get("workload", "matrix")
    identity_version = spec.get("identity_version", 1)
    execution_order = spec.get("execution_order")
    quality_background = spec.get("quality_background")
    repository_background = spec.get("repository_background")
    if not isinstance(harness_version, str) or not harness_version:
        raise ValueError("harness_version must be a non-empty string")
    if not isinstance(benchmark_script, str) or not benchmark_script:
        raise ValueError("benchmark_script must be a non-empty string")
    if not isinstance(cwd, str) or not cwd:
        raise ValueError("cwd must be a non-empty string")
    if not _is_positive_json_integer(repetitions):
        raise ValueError("repetitions must be a positive integer")
    if not _is_positive_json_integer(benchmark_timeout):
        raise ValueError("timeout_seconds must be a positive integer")
    if index_mode not in {"fast", "moderate", "full"}:
        raise ValueError("index_mode must be fast, moderate, or full")
    if execution_order not in {None, "grouped", "paired_interleaved"}:
        raise ValueError("execution_order must be grouped or paired_interleaved")
    if capability_quality is not None and (
        not isinstance(capability_quality, str)
        or not capability_quality
        or "=" in capability_quality
    ):
        raise ValueError("capability_quality must be a non-empty argument value")
    if workload not in {"matrix", "self_dogfood"}:
        raise ValueError("workload must be matrix or self_dogfood")
    if not _is_json_integer(identity_version) or identity_version not in {1, 2}:
        raise ValueError("identity_version must be 1 or 2")
    if capability_quality is not None and workload != "matrix":
        raise ValueError(
            "capability_quality cannot be combined with a self_dogfood workload"
        )
    if quality_background is not None:
        if capability_quality not in {"similarity", "semantic_edges"}:
            raise ValueError(
                "quality_background requires capability_quality similarity or semantic_edges"
            )
        if not isinstance(quality_background, dict):
            raise ValueError("quality_background must be an object")
        background_repo = quality_background.get("repo")
        background_revision = quality_background.get("revision")
        background_tree = quality_background.get("tree")
        background_datetime = _optional_iso_datetime(
            quality_background.get("commit_datetime"),
            "quality_background.commit_datetime",
        )
        if not isinstance(background_repo, str) or not Path(background_repo).is_dir():
            raise ValueError("quality_background.repo must be an existing directory")
        if not isinstance(background_revision, str) or len(background_revision) != 40:
            raise ValueError("quality_background.revision must be a full commit hash")
        if not isinstance(background_tree, str) or len(background_tree) != 40:
            raise ValueError("quality_background.tree must be a full tree hash")
        quality_background = {
            "repo": str(Path(background_repo).expanduser().resolve()),
            "revision": background_revision,
            "tree": background_tree,
        }
        if background_datetime is not None:
            quality_background["commit_datetime"] = background_datetime
    if repository_background is not None:
        if workload != "self_dogfood":
            raise ValueError("repository_background requires workload self_dogfood")
        if not isinstance(repository_background, dict):
            raise ValueError("repository_background must be an object")
        background_repo = repository_background.get("repo")
        background_revision = repository_background.get("revision")
        background_tree = repository_background.get("tree")
        background_datetime = _optional_iso_datetime(
            repository_background.get("commit_datetime"),
            "repository_background.commit_datetime",
        )
        if not isinstance(background_repo, str) or not Path(background_repo).is_dir():
            raise ValueError("repository_background.repo must be an existing directory")
        if not isinstance(background_revision, str) or len(background_revision) != 40:
            raise ValueError(
                "repository_background.revision must be a full commit hash"
            )
        if not isinstance(background_tree, str) or len(background_tree) != 40:
            raise ValueError("repository_background.tree must be a full tree hash")
        repository_background = {
            "repo": str(Path(background_repo).expanduser().resolve()),
            "revision": background_revision,
            "tree": background_tree,
        }
        if background_datetime is not None:
            repository_background["commit_datetime"] = background_datetime
    elif workload == "self_dogfood":
        raise ValueError("workload self_dogfood requires repository_background")
    if (
        not isinstance(accepted_exit_codes, list)
        or not accepted_exit_codes
        or not all(_is_json_integer(code) for code in accepted_exit_codes)
    ):
        raise ValueError("accepted_exit_codes must be a non-empty integer array")
    cell_timeout = spec.get("cell_timeout_seconds", benchmark_timeout * 4)
    if not _is_positive_json_integer(cell_timeout):
        raise ValueError("cell_timeout_seconds must be a positive integer")
    benchmark_path = Path(benchmark_script).expanduser().resolve()
    if not benchmark_path.is_file():
        raise ValueError(f"benchmark_script does not exist: {benchmark_path}")
    benchmark_sha256 = file_sha256(benchmark_path)

    candidates = _nonempty_list(spec.get("candidates"), "candidates")
    candidate_labels = {
        item.get("label") for item in candidates if isinstance(item, dict)
    }
    profiles = _nonempty_list(spec.get("profiles"), "profiles")
    scenarios = (
        [{"name": f"{capability_quality}_quality"}]
        if capability_quality is not None
        else _nonempty_list(spec.get("scenarios"), "scenarios")
    )
    transports = _nonempty_list(spec.get("transports"), "transports")
    if not all(isinstance(item, str) and item for item in transports):
        raise ValueError("transports must contain non-empty strings")
    common_environment = _string_map(spec.get("environment"), "environment")

    cells: list[dict[str, Any]] = []
    for candidate_index, candidate in enumerate(candidates):
        if not isinstance(candidate, dict):
            raise ValueError(f"candidates[{candidate_index}] must be an object")
        candidate_label = candidate.get("label")
        revision = candidate.get("revision")
        binary_value = candidate.get("binary")
        build = candidate.get("build")
        if (
            not isinstance(candidate_label, str)
            or not candidate_label
            or "=" in candidate_label
        ):
            raise ValueError(f"candidates[{candidate_index}].label is invalid")
        if not isinstance(revision, str) or len(revision) != 40:
            raise ValueError(
                f"candidates[{candidate_index}].revision must be a full commit hash"
            )
        if not isinstance(binary_value, str) or not binary_value:
            raise ValueError(
                f"candidates[{candidate_index}].binary must be a path string"
            )
        if not isinstance(build, dict):
            raise ValueError(f"candidates[{candidate_index}].build must be an object")
        binary = Path(binary_value).expanduser().resolve()
        if not binary.is_file():
            raise ValueError(f"candidate binary does not exist: {binary}")
        binary_sha = file_sha256(binary)
        declared_sha = candidate.get("binary_sha256")
        if declared_sha is not None and declared_sha != binary_sha:
            raise ValueError(
                f"candidates[{candidate_index}].binary_sha256 does not match {binary}"
            )
        candidate_environment = _string_map(
            candidate.get("environment"), f"candidates[{candidate_index}].environment"
        )
        candidate_support = candidate.get("capability_support")
        if candidate_support is not None and (
            not isinstance(candidate_support, dict)
            or not all(
                isinstance(key, str) and isinstance(value, bool)
                for key, value in candidate_support.items()
            )
        ):
            raise ValueError(
                f"candidates[{candidate_index}].capability_support must be a string-to-boolean object"
            )

        for profile_index, profile in enumerate(profiles):
            if not isinstance(profile, dict):
                raise ValueError(f"profiles[{profile_index}] must be an object")
            profile_label = profile.get("label")
            config_profile = profile.get("config_profile")
            capabilities = profile.get("capabilities")
            if (
                not isinstance(profile_label, str)
                or not profile_label
                or "=" in profile_label
            ):
                raise ValueError(f"profiles[{profile_index}].label is invalid")
            if not isinstance(config_profile, str) or not config_profile:
                raise ValueError(f"profiles[{profile_index}].config_profile is invalid")
            if not isinstance(capabilities, dict):
                raise ValueError(
                    f"profiles[{profile_index}].capabilities must be an object"
                )
            scoped_candidates = profile.get("candidate_labels")
            if scoped_candidates is not None:
                if (
                    not isinstance(scoped_candidates, list)
                    or not scoped_candidates
                    or not all(
                        isinstance(item, str) and item for item in scoped_candidates
                    )
                ):
                    raise ValueError(
                        f"profiles[{profile_index}].candidate_labels must be a non-empty string array"
                    )
                unknown_candidates = set(scoped_candidates) - candidate_labels
                if unknown_candidates:
                    raise ValueError(
                        f"profiles[{profile_index}].candidate_labels contains unknown candidates: "
                        f"{', '.join(sorted(unknown_candidates))}"
                    )
                if candidate_label not in scoped_candidates:
                    continue
            overrides = _string_map(
                profile.get("config_overrides"),
                f"profiles[{profile_index}].config_overrides",
            )
            if config_profile == "candidate_native_configuration":
                for key, claimed_value in capabilities.items():
                    expected = str(claimed_value).strip().lower()
                    if overrides.get(key, "").strip().lower() != expected:
                        raise ValueError(
                            f"profiles[{profile_index}].capabilities claims {key}={expected} "
                            "but the candidate-native profile does not apply that setting; add the "
                            "same value to config_overrides"
                        )
            if "incremental_exact_max_affected_paths" in overrides:
                raise ValueError(
                    "exact cap belongs in scenarios[].exact_caps, not profile overrides"
                )
            profile_environment = _string_map(
                profile.get("environment"), f"profiles[{profile_index}].environment"
            )

            for scenario_index, scenario in enumerate(scenarios):
                if not isinstance(scenario, dict):
                    raise ValueError(f"scenarios[{scenario_index}] must be an object")
                scenario_name = scenario.get("name")
                if not isinstance(scenario_name, str) or not scenario_name:
                    raise ValueError(f"scenarios[{scenario_index}].name is invalid")
                if capability_quality is not None or workload == "self_dogfood":
                    frontier_values: list[int | None] = [None]
                    cap_values: list[int | None] = [None]
                else:
                    frontier_values = _nonempty_list(
                        scenario.get("frontier_files"),
                        f"scenarios[{scenario_index}].frontier_files",
                    )
                    cap_values = _nonempty_list(
                        scenario.get("exact_caps"),
                        f"scenarios[{scenario_index}].exact_caps",
                    )
                    if not all(
                        _is_positive_json_integer(item) for item in frontier_values
                    ):
                        raise ValueError(
                            "frontier_files must contain positive integers"
                        )
                    if not all(
                        item is None or _is_positive_json_integer(item)
                        for item in cap_values
                    ):
                        raise ValueError(
                            "exact_caps must contain positive integers or null"
                        )

                for transport_index, transport in enumerate(transports):
                    for frontier_files in frontier_values:
                        for exact_cap in cap_values:
                            effective_capabilities = dict(capabilities)
                            effective_capabilities.update(overrides)
                            if capability_quality is not None:
                                command = [
                                    str(benchmark_path),
                                    "--binary",
                                    str(binary),
                                    "--capability-quality",
                                    capability_quality,
                                    "--transport",
                                    transport,
                                    "--config-profile",
                                    config_profile,
                                    "--index-mode",
                                    index_mode,
                                ]
                                if quality_background is not None:
                                    command.extend(
                                        (
                                            "--quality-background-repo",
                                            quality_background["repo"],
                                            "--quality-background-revision",
                                            quality_background["revision"],
                                        )
                                    )
                            elif workload == "self_dogfood":
                                assert repository_background is not None
                                command = [
                                    str(benchmark_path),
                                    "--binary",
                                    str(binary),
                                    "--self-dogfood",
                                    "--repo-root",
                                    repository_background["repo"],
                                    "--repo-revision",
                                    repository_background["revision"],
                                    "--self-dogfood-scenarios",
                                    scenario_name,
                                    "--transport",
                                    transport,
                                    "--config-profile",
                                    config_profile,
                                    "--index-mode",
                                    index_mode,
                                ]
                            else:
                                command = [
                                    str(benchmark_path),
                                    "--binary",
                                    str(binary),
                                    "--matrix",
                                    "--matrix-scenarios",
                                    scenario_name,
                                    "--frontier-files",
                                    str(frontier_files),
                                    "--transport",
                                    transport,
                                    "--config-profile",
                                    config_profile,
                                    "--index-mode",
                                    index_mode,
                                ]
                            cap_label = "default"
                            if isinstance(exact_cap, int):
                                effective_capabilities[
                                    "incremental_exact_max_affected_paths"
                                ] = str(exact_cap)
                                command.extend(
                                    (
                                        "--config",
                                        f"incremental_exact_max_affected_paths={exact_cap}",
                                    )
                                )
                                cap_label = str(exact_cap)
                            for key, value in sorted(overrides.items()):
                                command.extend(("--config", f"{key}={value}"))
                            if (
                                capability_quality is not None
                                or workload == "self_dogfood"
                            ):
                                command.append("--include-logs")
                            command.extend(
                                (
                                    "--timeout",
                                    str(benchmark_timeout),
                                    "--out",
                                    "{result_path}",
                                )
                            )
                            parameters = {
                                "config_profile": config_profile,
                                "config_overrides": dict(sorted(overrides.items())),
                                "benchmark_script_sha256": benchmark_sha256,
                                "index_mode": index_mode,
                            }
                            if capability_quality is not None:
                                parameters["capability_quality"] = capability_quality
                                if quality_background is not None:
                                    parameters["quality_background"] = (
                                        quality_background
                                    )
                                label = f"{candidate_label}.{profile_label}.{transport}.{scenario_name}"
                            elif workload == "self_dogfood":
                                assert repository_background is not None
                                parameters["repository_background"] = (
                                    repository_background
                                )
                                label = f"{candidate_label}.{profile_label}.{transport}.{scenario_name}"
                            else:
                                parameters["frontier_files"] = frontier_files
                                parameters["exact_cap"] = exact_cap
                                label = (
                                    f"{candidate_label}.{profile_label}.{transport}."
                                    f"{scenario_name}.f{frontier_files}.cap{cap_label}"
                                )
                            environment = {
                                **common_environment,
                                **candidate_environment,
                                **profile_environment,
                            }
                            for repetition in range(1, repetitions + 1):
                                cell = {
                                    "label": label,
                                    "revision": revision,
                                    "binary_sha256": binary_sha,
                                    "build": build,
                                    "capabilities": effective_capabilities,
                                    "transport": transport,
                                    "scenario": scenario_name,
                                    "repetition": repetition,
                                    "harness_version": harness_version,
                                    "command": command,
                                    "cwd": str(Path(cwd).expanduser().resolve()),
                                    "parameters": parameters,
                                    "timeout_seconds": cell_timeout,
                                    "accepted_exit_codes": list(accepted_exit_codes),
                                }
                                if identity_version == 2:
                                    cell["identity_version"] = 2
                                if environment:
                                    cell["environment"] = environment
                                if isinstance(candidate_support, dict):
                                    cell["capability_support"] = dict(
                                        sorted(candidate_support.items())
                                    )
                                cell["_design"] = {
                                    "candidate_index": candidate_index,
                                    "profile_index": profile_index,
                                    "scenario_index": scenario_index,
                                    "transport_index": transport_index,
                                    "grouped_position": len(cells),
                                }
                                cells.append(cell)
    if execution_order == "paired_interleaved":
        cells.sort(
            key=lambda cell: (
                cell["repetition"],
                cell["_design"]["scenario_index"],
                cell["_design"]["transport_index"],
                cell["_design"]["candidate_index"],
                cell["_design"]["profile_index"],
                cell["_design"]["grouped_position"],
            )
        )
        for position, cell in enumerate(cells, start=1):
            cell["parameters"] = {
                **cell["parameters"],
                "execution_order": execution_order,
                "execution_block": cell["repetition"],
                "execution_position": position,
            }
    for cell in cells:
        cell.pop("_design", None)
    plan = {"schema_version": SCHEMA_VERSION, "cells": cells}
    experiment_definition = read_experiment_version(spec)
    if experiment_definition is not None:
        plan["experiment_version"] = experiment_definition
    runset = spec.get("runset_id")
    if runset is not None:
        plan["runset_id"] = _validate_runset_identity(runset)
    if execution_order is not None:
        plan["execution_order"] = execution_order
    validate_plan(plan)
    return plan


def ensure_disk_space(root: Path, minimum_free_bytes: int) -> None:
    root.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(root).free
    if free < minimum_free_bytes:
        raise RuntimeError(
            f"insufficient experiment disk space: free={free} required={minimum_free_bytes} root={root}"
        )


def resource_snapshot(path: Path) -> dict[str, Any]:
    disk = shutil.disk_usage(path)
    try:
        load_average: list[float] | None = [
            round(value, 6) for value in os.getloadavg()
        ]
    except (AttributeError, OSError):
        load_average = None
    physical_memory_bytes: int | None = None
    try:
        pages = int(os.sysconf("SC_PHYS_PAGES"))
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        if pages > 0 and page_size > 0:
            physical_memory_bytes = pages * page_size
    except (AttributeError, OSError, TypeError, ValueError):
        pass
    return {
        "captured_at_utc": utc_now(),
        "hostname": socket.gethostname(),
        "load_average": load_average,
        "cpu_count": os.cpu_count(),
        "physical_memory_bytes": physical_memory_bytes,
        "disk": {
            "path": str(path.resolve()),
            "total_bytes": disk.total,
            "used_bytes": disk.used,
            "free_bytes": disk.free,
        },
    }


def validate_experiment_root(
    root: Path, *, allow_temporary: bool = False, temporary_root: Path | None = None
) -> Path:
    """Require retained experiment state to live outside the OS temporary tree."""
    resolved = root.expanduser().resolve()
    temp = (temporary_root or Path(tempfile.gettempdir())).expanduser().resolve()
    if not allow_temporary and (resolved == temp or temp in resolved.parents):
        raise ValueError(
            f"experiment root is temporary and may be lost after a crash or reboot: {resolved}; "
            "choose a durable ignored path, or pass --allow-temporary-experiment-root only "
            "for disposable tests"
        )
    return resolved


def process_is_live(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def acquire_lock(
    cell_root: Path, stale_after_seconds: int
) -> tuple[Path, dict[str, Any] | None]:
    cell_root.mkdir(parents=True, exist_ok=True)
    lock_path = cell_root / "running.lock"
    stale_record: dict[str, Any] | None = None
    if lock_path.exists():
        try:
            existing = read_json_object(lock_path)
        except (OSError, ValueError, json.JSONDecodeError):
            existing = {"invalid": True}
        try:
            started_epoch = float(existing.get("started_epoch", 0.0))
            pid = int(existing.get("pid", -1))
        except (TypeError, ValueError):
            started_epoch = 0.0
            pid = -1
        age = time.time() - started_epoch
        same_host = existing.get("hostname") == socket.gethostname()
        live = same_host and process_is_live(pid)
        if live or age < stale_after_seconds:
            raise RuntimeError(f"benchmark cell is already locked: {lock_path}")
        stale_record = {"recovered_at_utc": utc_now(), "previous_lock": existing}
        stale_path = (
            cell_root / f"stale-lock-{filename_datetime()}-{uuid.uuid4().hex[:8]}.json"
        )
        atomic_write_json(stale_path, stale_record)
        lock_path.unlink()
    document = {
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "started_at_utc": utc_now(),
        "started_epoch": time.time(),
    }
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(lock_path, flags, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(json.dumps(document, indent=2, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    return lock_path, stale_record


def resolve_result_path(cell_root: Path, completion: dict[str, Any]) -> Path:
    relative = completion.get("result_path")
    if not isinstance(relative, str):
        raise ValueError("completion result_path is missing")
    candidate = (cell_root / relative).resolve()
    if cell_root.resolve() not in candidate.parents:
        raise ValueError("completion result_path escapes the cell directory")
    return candidate


def validate_result(path: Path, cell: dict[str, Any]) -> dict[str, Any]:
    result = read_json_object(path)
    metadata = result.get("binary_metadata")
    actual_sha = metadata.get("sha256") if isinstance(metadata, dict) else None
    if actual_sha != cell["binary_sha256"]:
        raise ValueError(
            f"result binary SHA-256 mismatch: expected={cell['binary_sha256']} actual={actual_sha}"
        )
    if result.get("error"):
        raise ValueError(f"benchmark result contains an error: {result['error']}")
    run_context = result.get("benchmark_run_context")
    if run_context is not None:
        if not isinstance(run_context, dict):
            raise ValueError("benchmark_run_context must be an object")
        expected_context = {
            "cell_identity": cell_identity(cell),
            "label": cell["label"],
            "revision": cell["revision"],
            "repetition": cell["repetition"],
            "build": cell["build"],
            "capabilities": cell["capabilities"],
            "capability_support": cell.get("capability_support", {}),
            "harness_version": cell["harness_version"],
        }
        if run_context != expected_context:
            raise ValueError("benchmark_run_context does not match the experiment cell")
    derived = result.get("derived")
    if not isinstance(derived, dict) or not isinstance(derived.get("passed"), bool):
        raise ValueError("benchmark result must contain derived.passed as a boolean")
    cases = result.get("cases")
    measurements = result.get("measurements")
    if not (isinstance(cases, list) and cases) and not isinstance(measurements, dict):
        raise ValueError(
            "benchmark result must contain non-empty cases or measurements"
        )
    expected_background = cell.get("parameters", {}).get("quality_background")
    if expected_background is not None:
        first_case = cases[0] if isinstance(cases, list) and cases else None
        actual_background = (
            first_case.get("background_repository")
            if isinstance(first_case, dict)
            else None
        )
        if not isinstance(actual_background, dict):
            raise ValueError(
                "benchmark result is missing background_repository identity"
            )
        for key in ("revision", "tree"):
            if actual_background.get(key) != expected_background.get(key):
                raise ValueError(
                    f"background repository {key} mismatch: "
                    f"expected={expected_background.get(key)} actual={actual_background.get(key)}"
                )
    expected_repository = cell.get("parameters", {}).get("repository_background")
    if expected_repository is not None:
        actual_repository = result.get("repository_background")
        if not isinstance(actual_repository, dict):
            raise ValueError(
                "benchmark result is missing repository_background identity"
            )
        for key in ("revision", "tree"):
            if actual_repository.get(key) != expected_repository.get(key):
                raise ValueError(
                    f"repository background {key} mismatch: "
                    f"expected={expected_repository.get(key)} "
                    f"actual={actual_repository.get(key)}"
                )
    return result


def validate_attempt_artifacts(cell_root: Path, completion: dict[str, Any]) -> None:
    """Re-hash a completed attempt's archived evidence before trusting its audit status."""
    attempt_id = completion.get("attempt")
    if attempt_id is None:
        # Historical hand-authored plans may predate per-attempt evidence. Their
        # result hash remains validated, but there is no artifact claim to check.
        return
    if (
        not isinstance(attempt_id, str)
        or not attempt_id
        or Path(attempt_id).name != attempt_id
        or attempt_id in {".", ".."}
    ):
        raise ValueError("completion attempt identifier is invalid")
    attempt_root = cell_root / "attempts" / attempt_id
    attempt = read_json_object(attempt_root / "attempt.json")
    if attempt.get("cell_identity") != completion.get("cell_identity"):
        raise ValueError("attempt cell identity does not match the completion")
    if attempt.get("status") != "completed":
        raise ValueError("completed cell references a non-completed attempt")
    expected = attempt.get("artifacts")
    if not isinstance(expected, dict):
        raise ValueError("completed attempt artifact manifest is missing")
    actual = artifact_manifest(attempt_root / "artifacts")
    if actual != expected:
        raise ValueError(
            "completed attempt artifact manifest does not match retained files"
        )


def valid_completion(cell_root: Path, cell: dict[str, Any]) -> dict[str, Any] | None:
    completion_path = cell_root / "complete.json"
    if not completion_path.is_file():
        return None
    completion = read_json_object(completion_path)
    if completion.get("cell_identity") != cell_identity(cell):
        raise ValueError("completion cell identity does not match the plan")
    result_path = resolve_result_path(cell_root, completion)
    validate_result(result_path, cell)
    if file_sha256(result_path) != completion.get("result_sha256"):
        raise ValueError("completion result SHA-256 does not match the retained result")
    validate_attempt_artifacts(cell_root, completion)
    return completion


def expanded_command(
    command: list[str], attempt_root: Path, result_path: Path
) -> list[str]:
    replacements = {
        "{attempt_dir}": str(attempt_root),
        "{result_path}": str(result_path),
    }
    return [replacements.get(item, item) for item in command]


def cell_process_group_options() -> dict[str, Any]:
    if os.name == "nt":
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    return {"start_new_session": True}


def stop_cell_process_tree(
    process: subprocess.Popen[bytes], initial_signal: int, grace_seconds: float = 30.0
) -> int | None:
    """Stop an isolated benchmark process group, allowing harness cleanup first."""
    if process.poll() is not None:
        return process.returncode
    try:
        if os.name == "nt":
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            os.killpg(process.pid, initial_signal)
    except (OSError, ProcessLookupError):
        pass
    try:
        return process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        pass
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            pass
    try:
        return process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        return process.poll()


def run_cell(
    experiment_root: Path,
    cell: dict[str, Any],
    *,
    minimum_free_bytes: int = DEFAULT_MINIMUM_FREE_BYTES,
    stale_lock_seconds: int = DEFAULT_STALE_LOCK_SECONDS,
) -> dict[str, Any]:
    validate_cell(cell, 0)
    ensure_disk_space(experiment_root, minimum_free_bytes)
    identity = cell_identity(cell)
    cell_root = experiment_root / "runs" / identity
    try:
        completion = valid_completion(cell_root, cell)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {
            "cell_identity": identity,
            "label": cell["label"],
            "status": "corrupt",
            "error": str(exc),
        }
    if completion is not None:
        return {"cell_identity": identity, "label": cell["label"], "status": "resumed"}

    lock_path, stale_record = acquire_lock(cell_root, stale_lock_seconds)
    try:
        attempt_id = filename_datetime() + f"-{uuid.uuid4().hex[:8]}"
        attempt_root = cell_root / "attempts" / attempt_id
        attempt_root.mkdir(parents=True)
        artifact_root = attempt_root / "artifacts"
        result_path = attempt_root / "result.json"
        command = expanded_command(cell["command"], attempt_root, result_path)
        cwd = Path(cell.get("cwd") or Path.cwd()).expanduser().resolve()
        environment = dict(os.environ)
        overrides = cell.get("environment", {})
        if not isinstance(overrides, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in overrides.items()
        ):
            raise ValueError("cell environment must be a string-to-string object")
        environment.update(overrides)
        environment["CBM_BENCHMARK_ARTIFACT_DIR"] = str(artifact_root)
        benchmark_run_context = {
            "cell_identity": identity,
            "label": cell["label"],
            "revision": cell["revision"],
            "repetition": cell["repetition"],
            "build": cell["build"],
            "capabilities": cell["capabilities"],
            "capability_support": cell.get("capability_support", {}),
            "harness_version": cell["harness_version"],
        }
        environment["CBM_BENCHMARK_RUN_CONTEXT"] = canonical_json(
            benchmark_run_context
        ).decode("utf-8")
        command_record = {
            "cell_identity": identity,
            "identity": identity_document(cell),
            "label": cell["label"],
            "command": command,
            "cwd": str(cwd),
            "environment_overrides": overrides,
            "benchmark_run_context": benchmark_run_context,
            "artifact_directory": "artifacts",
            "started_at_utc": utc_now(),
            "stale_lock_recovered": stale_record is not None,
            "resource_before": resource_snapshot(experiment_root),
        }
        atomic_write_json(attempt_root / "command.json", command_record)
        started = time.monotonic()
        returncode: int | None = None
        error: str | None = None
        interrupted = False
    except Exception:
        if lock_path.exists():
            lock_path.unlink()
        raise
    try:
        with (
            (attempt_root / "stdout.log").open("wb") as stdout,
            (attempt_root / "stderr.log").open("wb") as stderr,
        ):
            try:
                process = subprocess.Popen(
                    command,
                    cwd=cwd,
                    env=environment,
                    stdout=stdout,
                    stderr=stderr,
                    **cell_process_group_options(),
                )
                returncode = process.wait(timeout=cell.get("timeout_seconds"))
            except subprocess.TimeoutExpired as exc:
                error = f"command timed out after {exc.timeout} seconds"
                returncode = stop_cell_process_tree(process, signal.SIGTERM)
            except KeyboardInterrupt:
                error = "command interrupted by SIGINT"
                interrupted = True
                returncode = stop_cell_process_tree(process, signal.SIGINT)
        accepted_codes = cell.get("accepted_exit_codes", [0])
        if error is None and returncode not in accepted_codes:
            error = f"command exited with {returncode}; accepted={accepted_codes}"
        result: dict[str, Any] | None = None
        if error is None:
            try:
                result = validate_result(result_path, cell)
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                error = str(exc)
        attempt_record = {
            **command_record,
            "finished_at_utc": utc_now(),
            "elapsed_seconds": round(time.monotonic() - started, 6),
            "returncode": returncode,
            "status": "completed" if error is None else "failed",
            "error": error,
            "resource_after": resource_snapshot(experiment_root),
            "artifacts": artifact_manifest(artifact_root),
        }
        atomic_write_json(attempt_root / "attempt.json", attempt_record)
        if interrupted:
            raise KeyboardInterrupt
        if error is not None:
            return {
                "cell_identity": identity,
                "label": cell["label"],
                "status": "failed",
                "error": error,
                "attempt": attempt_id,
            }
        assert result is not None
        derived = result.get("derived")
        benchmark_passed = derived.get("passed") if isinstance(derived, dict) else None
        completion = {
            "cell_identity": identity,
            "label": cell["label"],
            "completed_at_utc": utc_now(),
            "attempt": attempt_id,
            "result_path": str(result_path.relative_to(cell_root)),
            "result_sha256": file_sha256(result_path),
            "returncode": returncode,
            "benchmark_passed": benchmark_passed,
        }
        atomic_write_json(cell_root / "complete.json", completion)
        return {
            "cell_identity": identity,
            "label": cell["label"],
            "status": "completed",
        }
    finally:
        if lock_path.exists():
            lock_path.unlink()


def scan_experiment(
    experiment_root: Path, cells: list[dict[str, Any]]
) -> dict[str, Any]:
    expected = {cell_identity(cell): cell for cell in cells}
    entries: list[dict[str, Any]] = []
    counts = {
        "complete": 0,
        "missing": 0,
        "corrupt": 0,
        "duplicate_attempts": 0,
        "unplanned": 0,
    }
    for identity, cell in expected.items():
        cell_root = experiment_root / "runs" / identity
        attempts_root = cell_root / "attempts"
        attempt_count = (
            sum(1 for path in attempts_root.iterdir() if path.is_dir())
            if attempts_root.is_dir()
            else 0
        )
        if attempt_count > 1:
            counts["duplicate_attempts"] += attempt_count - 1
        status = "missing"
        error = None
        try:
            if valid_completion(cell_root, cell) is not None:
                status = "complete"
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            status = "corrupt"
            error = str(exc)
        counts[status] += 1
        entries.append(
            {
                "cell_identity": identity,
                "label": cell["label"],
                "status": status,
                "attempts": attempt_count,
                "error": error,
            }
        )
    runs_root = experiment_root / "runs"
    actual = (
        {path.name for path in runs_root.iterdir() if path.is_dir()}
        if runs_root.is_dir()
        else set()
    )
    unplanned = sorted(actual - set(expected))
    counts["unplanned"] = len(unplanned)
    return {"counts": counts, "cells": entries, "unplanned": unplanned}


def environment_snapshot(plan_path: Path) -> dict[str, Any]:
    return {
        "captured_at_utc": utc_now(),
        "plan_path": str(plan_path.resolve()),
        "plan_sha256": file_sha256(plan_path),
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python": sys.version,
        "cpu_count": os.cpu_count(),
        "resources": resource_snapshot(plan_path.parent),
    }


def completed_report_inputs(
    experiment_root: Path, cells: list[dict[str, Any]]
) -> list[tuple[str, Path]]:
    inputs: list[tuple[str, Path]] = []
    for cell in cells:
        cell_root = experiment_root / "runs" / cell_identity(cell)
        completion = valid_completion(cell_root, cell)
        if completion is not None:
            result_path = resolve_result_path(cell_root, completion)
            inputs.append(
                (
                    cell["label"],
                    materialize_report_input(experiment_root, cell, result_path),
                )
            )
    return inputs


def materialize_report_input(
    experiment_root: Path, cell: dict[str, Any], result_path: Path
) -> Path:
    """Create a deterministic derived input with candidate metadata beside immutable raw results."""
    document = read_json_object(result_path)
    parameters = document.get("parameters")
    if not isinstance(parameters, dict):
        parameters = {}
        document["parameters"] = parameters
    support = cell.get("capability_support")
    if isinstance(support, dict):
        parameters["capability_support"] = dict(sorted(support.items()))
    cell_parameters = cell.get("parameters")
    if isinstance(cell_parameters, dict):
        for key in ("execution_order", "execution_block", "execution_position"):
            if key in cell_parameters:
                parameters[key] = cell_parameters[key]
    source_sha = file_sha256(result_path)
    identity = cell_identity(cell)
    document["experiment_provenance"] = {
        "cell_identity": identity,
        "source_result": str(result_path),
        "source_result_sha256": source_sha,
    }
    output = (
        experiment_root / "reports" / "inputs" / f"{identity}-{source_sha[:12]}.json"
    )
    atomic_write_json(output, document)
    return output


def generate_report(
    experiment_root: Path, cells: list[dict[str, Any]], output: Path
) -> dict[str, Any]:
    inputs = completed_report_inputs(experiment_root, cells)
    if not inputs:
        raise RuntimeError(
            "cannot generate a report without completed experiment cells"
        )
    summarizer = Path(__file__).resolve().with_name("summarize-benchmark-results.py")
    command = [sys.executable, str(summarizer)]
    for label, result_path in inputs:
        command.extend(("--input", f"{label}={result_path}"))
    command.extend(("--out", str(output)))
    process = subprocess.run(command, capture_output=True, text=True, check=False)
    if process.returncode != 0:
        raise RuntimeError(
            f"report generator exited with {process.returncode}: {process.stderr.strip()}"
        )
    return {
        "path": str(output),
        "sha256": file_sha256(output),
        "input_count": len(inputs),
        "generator": str(summarizer),
    }


def write_manifest(
    experiment_root: Path,
    plan_path: Path,
    cells: list[dict[str, Any]],
    report: dict[str, Any] | None = None,
    *,
    runset: str | None = None,
) -> Path:
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "plan_sha256": file_sha256(plan_path),
        "audit": scan_experiment(experiment_root, cells),
        "generated_report": report,
    }
    effective_runset = runset or file_sha256(plan_path)[:12]
    name = generated_artifact_name(
        "manifest",
        effective_runset,
        ".json",
        nonce=uuid.uuid4().hex[:8],
    )
    path = experiment_root / "manifests" / name
    atomic_write_json(path, manifest)
    return path


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--plan", type=Path, help="Fully expanded immutable experiment plan."
    )
    source.add_argument(
        "--matrix-spec",
        type=Path,
        help="Compact deterministic grid expanded and archived before execution.",
    )
    source.add_argument(
        "--quick",
        dest="preset",
        action="store_const",
        const="quick",
        help="Automatically prepare and run the safe one-repetition smoke (default).",
    )
    source.add_argument(
        "--full",
        dest="preset",
        action="store_const",
        const="full",
        help="Automatically prepare and run the repeated capability matrix.",
    )
    parser.add_argument(
        "--experiment-root",
        "--campaign-root",
        dest="experiment_root",
        type=Path,
        help=(
            "Durable result root (--campaign-root is a legacy alias). "
            "Automatic modes default to a versioned, commit-qualified, "
            "content-addressed runset directory under "
            ".worktrees/benchmark-campaign (legacy path retained for existing runsets)."
        ),
    )
    parser.add_argument(
        "--candidate-root",
        type=Path,
        help=(
            "Automatic candidate worktree/build root (default: .worktrees/benchmark-candidates)."
        ),
    )
    parser.add_argument("--build-jobs", type=int, default=2)
    parser.add_argument(
        "--allow-temporary-experiment-root",
        "--allow-temporary-campaign-root",
        dest="allow_temporary_experiment_root",
        action="store_true",
        help="Allow disposable experiment state under the OS temporary directory.",
    )
    parser.add_argument(
        "--candidate-ref",
        dest="candidate_ref_overrides",
        action="append",
        default=[],
        metavar="LABEL=REF",
        help=(
            "Override one automatic candidate's git ref by label (repeatable), e.g. "
            "--candidate-ref upstream-main=origin/main. Valid labels: "
            + ", ".join(label for label, _ in DEFAULT_CANDIDATE_REFS)
            + ". Only applies to --quick/--full; explicit --plan/--matrix-spec already "
            "accept any resolvable ref directly in the spec. An override is an explicit "
            "request and stays fail-closed: an unresolvable override ref raises rather "
            "than falling back."
        ),
    )
    parser.add_argument("--minimum-free-gb", type=float, default=2.0)
    parser.add_argument("--stale-lock-hours", type=float, default=6.0)
    parser.add_argument("--audit-only", action="store_true")
    parser.add_argument(
        "--report-out",
        type=Path,
        help="Generated Markdown path (default: versioned runset report under EXPERIMENT_ROOT/reports).",
    )
    args = parser.parse_args(argv)
    if args.plan is None and args.matrix_spec is None and args.preset is None:
        args.preset = "quick"
    if args.preset is None and args.experiment_root is None:
        parser.error(
            "--experiment-root (legacy alias: --campaign-root) is required with --plan or --matrix-spec"
        )
    if args.build_jobs <= 0:
        parser.error("--build-jobs must be positive")
    candidate_ref_overrides: dict[str, str] = {}
    for value in args.candidate_ref_overrides:
        try:
            label, ref = parse_candidate_ref_override(value)
        except ValueError as error:
            parser.error(str(error))
        candidate_ref_overrides[label] = ref
    if candidate_ref_overrides and args.preset is None:
        parser.error("--candidate-ref only applies to --quick/--full")
    args.candidate_ref = candidate_ref_overrides
    return args


def _commit_datetime_slug(repository: Path, revision: str) -> str:
    return commit_identity(repository, revision)["commit_datetime_slug"]


def prepare_automatic_experiment(
    args: argparse.Namespace,
) -> tuple[Path, Path]:
    repository = Path(__file__).resolve().parents[1]
    ensure_clean_tracked_worktree(repository, "benchmark source worktree")
    candidate_root = (
        args.candidate_root.expanduser().resolve()
        if args.candidate_root
        else repository / ".worktrees" / "benchmark-candidates"
    )
    ensure_disk_space(candidate_root, max(0, int(args.minimum_free_gb * 1024**3)))
    candidate_ref_overrides: dict[str, str] = getattr(args, "candidate_ref", {}) or {}
    effective_candidate_refs = [
        (label, candidate_ref_overrides[label])
        if label in candidate_ref_overrides
        else (label, resolve_default_candidate_ref(repository, label, ref))
        for label, ref in DEFAULT_CANDIDATE_REFS
    ]
    candidates = [
        materialize_candidate(
            repository,
            candidate_root,
            label,
            ref,
            jobs=args.build_jobs,
        )
        for label, ref in effective_candidate_refs
    ]
    benchmark_script = repository / "scripts" / "benchmark-incremental-speed.py"
    spec = build_automatic_spec(
        repository,
        benchmark_script,
        candidates,
        preset=args.preset,
    )
    revision = spec["repository_background"]["revision"]
    tree = spec["repository_background"]["tree"]
    commit_datetime = _commit_datetime_slug(repository, revision)
    runset = automatic_runset_identity(spec)
    spec["runset_id"] = runset
    spec_payload = (json.dumps(spec, indent=2, sort_keys=True) + "\n").encode("utf-8")
    source_identity = {
        "revision": revision,
        "commit_datetime_slug": commit_datetime,
        "tree": tree,
    }
    experiment_root = (
        args.experiment_root.expanduser().resolve()
        if args.experiment_root
        else repository
        / ".worktrees"
        / "benchmark-campaign"
        / automatic_experiment_name(args.preset, source_identity, runset)
    )
    experiment_root = validate_experiment_root(
        experiment_root,
        allow_temporary=args.allow_temporary_experiment_root,
    )
    spec_path = experiment_root / "inputs" / automatic_spec_name(args.preset, runset)
    if spec_path.exists():
        if spec_path.read_bytes() != spec_payload:
            raise RuntimeError(
                f"automatic spec path contains different bytes: {spec_path}"
            )
    else:
        atomic_write_bytes(spec_path, spec_payload)
    return experiment_root, spec_path


def main(argv: list[str] | None = None) -> int:
    args = parse_arguments(argv)

    if args.preset is not None:
        experiment_root, matrix_spec = prepare_automatic_experiment(args)
        args.matrix_spec = matrix_spec
    else:
        assert args.experiment_root is not None
        experiment_root = validate_experiment_root(
            args.experiment_root,
            allow_temporary=args.allow_temporary_experiment_root,
        )

    minimum_free_bytes = max(0, int(args.minimum_free_gb * 1024**3))
    stale_lock_seconds = max(1, int(args.stale_lock_hours * 3600))
    ensure_disk_space(experiment_root, minimum_free_bytes)
    if args.matrix_spec:
        spec_path = args.matrix_spec.expanduser().resolve()
        spec = read_json_object(spec_path)
        plan = expand_matrix_spec(spec)
        plan["matrix_spec_sha256"] = file_sha256(spec_path)
        archived_spec = experiment_root / "specs" / f"{file_sha256(spec_path)}.json"
        if not archived_spec.exists():
            atomic_write_bytes(archived_spec, spec_path.read_bytes())
        plan_payload = (json.dumps(plan, indent=2, sort_keys=True) + "\n").encode(
            "utf-8"
        )
        plan_digest = hashlib.sha256(plan_payload).hexdigest()
        plan_path = experiment_root / "plans" / f"{plan_digest}.json"
        if not plan_path.exists():
            atomic_write_bytes(plan_path, plan_payload)
    else:
        plan_path = args.plan.expanduser().resolve()
        plan = read_json_object(plan_path)
        archived_plan = experiment_root / "plans" / f"{file_sha256(plan_path)}.json"
        if not archived_plan.exists():
            atomic_write_bytes(archived_plan, plan_path.read_bytes())
        plan_path = archived_plan
    cells = validate_plan(plan)
    runset = plan.get("runset_id", file_sha256(plan_path)[:12])
    runset = _validate_runset_identity(runset)
    snapshot_name = generated_artifact_name("environment", runset, ".json")
    atomic_write_json(
        experiment_root / "environments" / snapshot_name,
        environment_snapshot(plan_path),
    )

    failures = 0
    if not args.audit_only:
        for cell in cells:
            outcome = run_cell(
                experiment_root,
                cell,
                minimum_free_bytes=minimum_free_bytes,
                stale_lock_seconds=stale_lock_seconds,
            )
            print(json.dumps(outcome, sort_keys=True), flush=True)
            failures += int(outcome["status"] in {"failed", "corrupt"})
    audit = scan_experiment(experiment_root, cells)
    report_metadata = None
    if audit["counts"]["complete"]:
        report_path = (
            args.report_out.expanduser().resolve()
            if args.report_out
            else experiment_root
            / "reports"
            / generated_artifact_name(
                "report",
                runset,
                ".md",
                preset=args.preset or "custom",
            )
        )
        report_metadata = generate_report(experiment_root, cells, report_path)
    manifest_path = write_manifest(
        experiment_root,
        plan_path,
        cells,
        report_metadata,
        runset=runset,
    )
    print(
        json.dumps(
            {"manifest": str(manifest_path), "audit": audit}, indent=2, sort_keys=True
        )
    )
    return (
        1 if failures or audit["counts"]["missing"] or audit["counts"]["corrupt"] else 0
    )


if __name__ == "__main__":
    raise SystemExit(main())

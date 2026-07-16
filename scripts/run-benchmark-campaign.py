#!/usr/bin/env python3
"""Run an immutable, resumable benchmark plan and retain an auditable disk trail."""

from __future__ import annotations

import argparse
import hashlib
import json
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


SCHEMA_VERSION = 1
DEFAULT_MINIMUM_FREE_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_STALE_LOCK_SECONDS = 6 * 60 * 60
IDENTITY_FIELDS = (
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


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def identity_document(cell: dict[str, Any]) -> dict[str, Any]:
    return {key: cell.get(key) for key in IDENTITY_FIELDS}


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
        raise ValueError(f"cells[{index}].label must be non-empty and cannot contain '='")
    if len(cell["revision"]) != 40:
        raise ValueError(f"cells[{index}].revision must be a full 40-character commit hash")
    if len(cell["binary_sha256"]) != 64:
        raise ValueError(f"cells[{index}].binary_sha256 must be a full SHA-256")
    if not cell["command"] or not all(isinstance(item, str) for item in cell["command"]):
        raise ValueError(f"cells[{index}].command must be a non-empty string array")
    accepted = cell.get("accepted_exit_codes", [0])
    if not isinstance(accepted, list) or not accepted or not all(
        isinstance(code, int) for code in accepted
    ):
        raise ValueError(f"cells[{index}].accepted_exit_codes must be a non-empty integer array")
    support = cell.get("capability_support")
    if support is not None and (
        not isinstance(support, dict)
        or not all(isinstance(key, str) and isinstance(value, bool) for key, value in support.items())
    ):
        raise ValueError(f"cells[{index}].capability_support must be a string-to-boolean object")


def validate_plan(plan: dict[str, Any]) -> list[dict[str, Any]]:
    if plan.get("schema_version") != SCHEMA_VERSION:
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


def _nonempty_list(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field} must be a non-empty array")
    return value


def expand_matrix_spec(spec: dict[str, Any]) -> dict[str, Any]:
    """Expand a compact benchmark grid into immutable campaign cells."""
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"schema_version must be {SCHEMA_VERSION}")
    harness_version = spec.get("harness_version")
    benchmark_script = spec.get("benchmark_script")
    cwd = spec.get("cwd")
    repetitions = spec.get("repetitions")
    benchmark_timeout = spec.get("timeout_seconds", 240)
    index_mode = spec.get("index_mode", "fast")
    accepted_exit_codes = spec.get("accepted_exit_codes", [0])
    capability_quality = spec.get("capability_quality")
    execution_order = spec.get("execution_order")
    if not isinstance(harness_version, str) or not harness_version:
        raise ValueError("harness_version must be a non-empty string")
    if not isinstance(benchmark_script, str) or not benchmark_script:
        raise ValueError("benchmark_script must be a non-empty string")
    if not isinstance(cwd, str) or not cwd:
        raise ValueError("cwd must be a non-empty string")
    if not isinstance(repetitions, int) or repetitions <= 0:
        raise ValueError("repetitions must be a positive integer")
    if not isinstance(benchmark_timeout, int) or benchmark_timeout <= 0:
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
    if (
        not isinstance(accepted_exit_codes, list)
        or not accepted_exit_codes
        or not all(
            isinstance(code, int) and not isinstance(code, bool)
            for code in accepted_exit_codes
        )
    ):
        raise ValueError("accepted_exit_codes must be a non-empty integer array")
    cell_timeout = spec.get("cell_timeout_seconds", benchmark_timeout * 4)
    if not isinstance(cell_timeout, int) or cell_timeout <= 0:
        raise ValueError("cell_timeout_seconds must be a positive integer")
    benchmark_path = Path(benchmark_script).expanduser().resolve()
    if not benchmark_path.is_file():
        raise ValueError(f"benchmark_script does not exist: {benchmark_path}")
    benchmark_sha256 = file_sha256(benchmark_path)

    candidates = _nonempty_list(spec.get("candidates"), "candidates")
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
        if not isinstance(candidate_label, str) or not candidate_label or "=" in candidate_label:
            raise ValueError(f"candidates[{candidate_index}].label is invalid")
        if not isinstance(revision, str) or len(revision) != 40:
            raise ValueError(f"candidates[{candidate_index}].revision must be a full commit hash")
        if not isinstance(binary_value, str) or not binary_value:
            raise ValueError(f"candidates[{candidate_index}].binary must be a path string")
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
                f"candidates[{candidate_index}].capability_support must be a "
                "string-to-boolean object"
            )

        for profile_index, profile in enumerate(profiles):
            if not isinstance(profile, dict):
                raise ValueError(f"profiles[{profile_index}] must be an object")
            profile_label = profile.get("label")
            config_profile = profile.get("config_profile")
            capabilities = profile.get("capabilities")
            if not isinstance(profile_label, str) or not profile_label or "=" in profile_label:
                raise ValueError(f"profiles[{profile_index}].label is invalid")
            if not isinstance(config_profile, str) or not config_profile:
                raise ValueError(f"profiles[{profile_index}].config_profile is invalid")
            if not isinstance(capabilities, dict):
                raise ValueError(f"profiles[{profile_index}].capabilities must be an object")
            overrides = _string_map(
                profile.get("config_overrides"),
                f"profiles[{profile_index}].config_overrides",
            )
            if "incremental_exact_max_affected_paths" in overrides:
                raise ValueError("exact cap belongs in scenarios[].exact_caps, not profile overrides")
            profile_environment = _string_map(
                profile.get("environment"), f"profiles[{profile_index}].environment"
            )

            for scenario_index, scenario in enumerate(scenarios):
                if not isinstance(scenario, dict):
                    raise ValueError(f"scenarios[{scenario_index}] must be an object")
                scenario_name = scenario.get("name")
                if not isinstance(scenario_name, str) or not scenario_name:
                    raise ValueError(f"scenarios[{scenario_index}].name is invalid")
                if capability_quality is not None:
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
                    if not all(isinstance(item, int) and item > 0 for item in frontier_values):
                        raise ValueError("frontier_files must contain positive integers")
                    if not all(
                        item is None
                        or (isinstance(item, int) and not isinstance(item, bool) and item > 0)
                        for item in cap_values
                    ):
                        raise ValueError("exact_caps must contain positive integers or null")

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
                            if capability_quality is not None:
                                command.append("--include-logs")
                            command.extend(("--timeout", str(benchmark_timeout), "--out", "{result_path}"))
                            parameters = {
                                "config_profile": config_profile,
                                "config_overrides": dict(sorted(overrides.items())),
                                "benchmark_script_sha256": benchmark_sha256,
                                "index_mode": index_mode,
                            }
                            if capability_quality is not None:
                                parameters["capability_quality"] = capability_quality
                                label = (
                                    f"{candidate_label}.{profile_label}.{transport}."
                                    f"{scenario_name}"
                                )
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
    if execution_order is not None:
        plan["execution_order"] = execution_order
    validate_plan(plan)
    return plan


def ensure_disk_space(root: Path, minimum_free_bytes: int) -> None:
    root.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(root).free
    if free < minimum_free_bytes:
        raise RuntimeError(
            f"insufficient campaign disk space: free={free} required={minimum_free_bytes} root={root}"
        )


def resource_snapshot(path: Path) -> dict[str, Any]:
    disk = shutil.disk_usage(path)
    try:
        load_average: list[float] | None = [round(value, 6) for value in os.getloadavg()]
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


def validate_campaign_root(
    root: Path, *, allow_temporary: bool = False, temporary_root: Path | None = None
) -> Path:
    """Require retained campaign state to live outside the OS temporary tree."""
    resolved = root.expanduser().resolve()
    temp = (temporary_root or Path(tempfile.gettempdir())).expanduser().resolve()
    if not allow_temporary and (resolved == temp or temp in resolved.parents):
        raise ValueError(
            f"campaign root is temporary and may be lost after a crash or reboot: {resolved}; "
            "choose a durable ignored path, or pass --allow-temporary-campaign-root only "
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


def acquire_lock(cell_root: Path, stale_after_seconds: int) -> tuple[Path, dict[str, Any] | None]:
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
        stale_path = cell_root / f"stale-lock-{int(time.time())}-{uuid.uuid4().hex[:8]}.json"
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
    derived = result.get("derived")
    if not isinstance(derived, dict) or not isinstance(derived.get("passed"), bool):
        raise ValueError("benchmark result must contain derived.passed as a boolean")
    cases = result.get("cases")
    measurements = result.get("measurements")
    if not (isinstance(cases, list) and cases) and not isinstance(measurements, dict):
        raise ValueError("benchmark result must contain non-empty cases or measurements")
    return result


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
    return completion


def expanded_command(command: list[str], attempt_root: Path, result_path: Path) -> list[str]:
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
    campaign_root: Path,
    cell: dict[str, Any],
    *,
    minimum_free_bytes: int = DEFAULT_MINIMUM_FREE_BYTES,
    stale_lock_seconds: int = DEFAULT_STALE_LOCK_SECONDS,
) -> dict[str, Any]:
    validate_cell(cell, 0)
    ensure_disk_space(campaign_root, minimum_free_bytes)
    identity = cell_identity(cell)
    cell_root = campaign_root / "runs" / identity
    try:
        completion = valid_completion(cell_root, cell)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {"cell_identity": identity, "label": cell["label"], "status": "corrupt", "error": str(exc)}
    if completion is not None:
        return {"cell_identity": identity, "label": cell["label"], "status": "resumed"}

    lock_path, stale_record = acquire_lock(cell_root, stale_lock_seconds)
    try:
        attempt_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ") + f"-{uuid.uuid4().hex[:8]}"
        attempt_root = cell_root / "attempts" / attempt_id
        attempt_root.mkdir(parents=True)
        result_path = attempt_root / "result.json"
        command = expanded_command(cell["command"], attempt_root, result_path)
        cwd = Path(cell.get("cwd") or Path.cwd()).expanduser().resolve()
        environment = dict(os.environ)
        overrides = cell.get("environment", {})
        if not isinstance(overrides, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in overrides.items()
        ):
            raise ValueError("cell environment must be a string-to-string object")
        environment.update(overrides)
        command_record = {
            "cell_identity": identity,
            "identity": identity_document(cell),
            "label": cell["label"],
            "command": command,
            "cwd": str(cwd),
            "environment_overrides": overrides,
            "started_at_utc": utc_now(),
            "stale_lock_recovered": stale_record is not None,
            "resource_before": resource_snapshot(campaign_root),
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
        with (attempt_root / "stdout.log").open("wb") as stdout, (
            attempt_root / "stderr.log"
        ).open("wb") as stderr:
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
            "resource_after": resource_snapshot(campaign_root),
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
        return {"cell_identity": identity, "label": cell["label"], "status": "completed"}
    finally:
        if lock_path.exists():
            lock_path.unlink()


def scan_campaign(campaign_root: Path, cells: list[dict[str, Any]]) -> dict[str, Any]:
    expected = {cell_identity(cell): cell for cell in cells}
    entries: list[dict[str, Any]] = []
    counts = {"complete": 0, "missing": 0, "corrupt": 0, "duplicate_attempts": 0, "unplanned": 0}
    for identity, cell in expected.items():
        cell_root = campaign_root / "runs" / identity
        attempts_root = cell_root / "attempts"
        attempt_count = sum(1 for path in attempts_root.iterdir() if path.is_dir()) if attempts_root.is_dir() else 0
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
            {"cell_identity": identity, "label": cell["label"], "status": status, "attempts": attempt_count, "error": error}
        )
    runs_root = campaign_root / "runs"
    actual = {path.name for path in runs_root.iterdir() if path.is_dir()} if runs_root.is_dir() else set()
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
    campaign_root: Path, cells: list[dict[str, Any]]
) -> list[tuple[str, Path]]:
    inputs: list[tuple[str, Path]] = []
    for cell in cells:
        cell_root = campaign_root / "runs" / cell_identity(cell)
        completion = valid_completion(cell_root, cell)
        if completion is not None:
            result_path = resolve_result_path(cell_root, completion)
            inputs.append(
                (cell["label"], materialize_report_input(campaign_root, cell, result_path))
            )
    return inputs


def materialize_report_input(
    campaign_root: Path, cell: dict[str, Any], result_path: Path
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
    source_sha = file_sha256(result_path)
    identity = cell_identity(cell)
    document["campaign_provenance"] = {
        "cell_identity": identity,
        "source_result": str(result_path),
        "source_result_sha256": source_sha,
    }
    output = campaign_root / "reports" / "inputs" / f"{identity}-{source_sha[:12]}.json"
    atomic_write_json(output, document)
    return output


def generate_report(campaign_root: Path, cells: list[dict[str, Any]], output: Path) -> dict[str, Any]:
    inputs = completed_report_inputs(campaign_root, cells)
    if not inputs:
        raise RuntimeError("cannot generate a report without completed campaign cells")
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
    campaign_root: Path,
    plan_path: Path,
    cells: list[dict[str, Any]],
    report: dict[str, Any] | None = None,
) -> Path:
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now(),
        "plan_sha256": file_sha256(plan_path),
        "audit": scan_campaign(campaign_root, cells),
        "generated_report": report,
    }
    name = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ") + f"-{uuid.uuid4().hex[:8]}.json"
    path = campaign_root / "manifests" / name
    atomic_write_json(path, manifest)
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--plan", type=Path, help="Fully expanded immutable campaign plan.")
    source.add_argument(
        "--matrix-spec",
        type=Path,
        help="Compact deterministic grid expanded and archived before execution.",
    )
    parser.add_argument("--campaign-root", required=True, type=Path)
    parser.add_argument(
        "--allow-temporary-campaign-root",
        action="store_true",
        help="Allow disposable campaign state under the OS temporary directory.",
    )
    parser.add_argument("--minimum-free-gb", type=float, default=2.0)
    parser.add_argument("--stale-lock-hours", type=float, default=6.0)
    parser.add_argument("--audit-only", action="store_true")
    parser.add_argument(
        "--report-out",
        type=Path,
        help="Generated Markdown path (default: CAMPAIGN_ROOT/reports/summary.md).",
    )
    args = parser.parse_args()

    campaign_root = validate_campaign_root(
        args.campaign_root,
        allow_temporary=args.allow_temporary_campaign_root,
    )
    minimum_free_bytes = max(0, int(args.minimum_free_gb * 1024**3))
    stale_lock_seconds = max(1, int(args.stale_lock_hours * 3600))
    ensure_disk_space(campaign_root, minimum_free_bytes)
    if args.matrix_spec:
        spec_path = args.matrix_spec.expanduser().resolve()
        spec = read_json_object(spec_path)
        plan = expand_matrix_spec(spec)
        plan["matrix_spec_sha256"] = file_sha256(spec_path)
        archived_spec = campaign_root / "specs" / f"{file_sha256(spec_path)}.json"
        if not archived_spec.exists():
            atomic_write_bytes(archived_spec, spec_path.read_bytes())
        plan_payload = (json.dumps(plan, indent=2, sort_keys=True) + "\n").encode("utf-8")
        plan_digest = hashlib.sha256(plan_payload).hexdigest()
        plan_path = campaign_root / "plans" / f"{plan_digest}.json"
        if not plan_path.exists():
            atomic_write_bytes(plan_path, plan_payload)
    else:
        plan_path = args.plan.expanduser().resolve()
        plan = read_json_object(plan_path)
        archived_plan = campaign_root / "plans" / f"{file_sha256(plan_path)}.json"
        if not archived_plan.exists():
            atomic_write_bytes(archived_plan, plan_path.read_bytes())
        plan_path = archived_plan
    cells = validate_plan(plan)
    snapshot_name = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ") + ".json"
    atomic_write_json(
        campaign_root / "environments" / snapshot_name,
        environment_snapshot(plan_path),
    )

    failures = 0
    if not args.audit_only:
        for cell in cells:
            outcome = run_cell(
                campaign_root,
                cell,
                minimum_free_bytes=minimum_free_bytes,
                stale_lock_seconds=stale_lock_seconds,
            )
            print(json.dumps(outcome, sort_keys=True), flush=True)
            failures += int(outcome["status"] in {"failed", "corrupt"})
    audit = scan_campaign(campaign_root, cells)
    report_metadata = None
    if audit["counts"]["complete"]:
        report_path = (
            args.report_out.expanduser().resolve()
            if args.report_out
            else campaign_root / "reports" / "summary.md"
        )
        report_metadata = generate_report(campaign_root, cells, report_path)
    manifest_path = write_manifest(campaign_root, plan_path, cells, report_metadata)
    print(json.dumps({"manifest": str(manifest_path), "audit": audit}, indent=2, sort_keys=True))
    return 1 if failures or audit["counts"]["missing"] or audit["counts"]["corrupt"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

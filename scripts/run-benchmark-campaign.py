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
    "transport",
    "scenario",
    "repetition",
    "harness_version",
    "command",
    "cwd",
    "environment",
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


def ensure_disk_space(root: Path, minimum_free_bytes: int) -> None:
    root.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(root).free
    if free < minimum_free_bytes:
        raise RuntimeError(
            f"insufficient campaign disk space: free={free} required={minimum_free_bytes} root={root}"
        )


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
    }


def completed_report_inputs(
    campaign_root: Path, cells: list[dict[str, Any]]
) -> list[tuple[str, Path]]:
    inputs: list[tuple[str, Path]] = []
    for cell in cells:
        cell_root = campaign_root / "runs" / cell_identity(cell)
        completion = valid_completion(cell_root, cell)
        if completion is not None:
            inputs.append((cell["label"], resolve_result_path(cell_root, completion)))
    return inputs


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
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--campaign-root", required=True, type=Path)
    parser.add_argument("--minimum-free-gb", type=float, default=2.0)
    parser.add_argument("--stale-lock-hours", type=float, default=6.0)
    parser.add_argument("--audit-only", action="store_true")
    parser.add_argument(
        "--report-out",
        type=Path,
        help="Generated Markdown path (default: CAMPAIGN_ROOT/reports/summary.md).",
    )
    args = parser.parse_args()

    plan = read_json_object(args.plan)
    cells = validate_plan(plan)
    campaign_root = args.campaign_root.expanduser().resolve()
    minimum_free_bytes = max(0, int(args.minimum_free_gb * 1024**3))
    stale_lock_seconds = max(1, int(args.stale_lock_hours * 3600))
    ensure_disk_space(campaign_root, minimum_free_bytes)
    archived_plan = campaign_root / "plans" / f"{file_sha256(args.plan)}.json"
    if not archived_plan.exists():
        atomic_write_bytes(archived_plan, args.plan.read_bytes())
    snapshot_name = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ") + ".json"
    atomic_write_json(
        campaign_root / "environments" / snapshot_name,
        environment_snapshot(args.plan),
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
    manifest_path = write_manifest(campaign_root, args.plan, cells, report_metadata)
    print(json.dumps({"manifest": str(manifest_path), "audit": audit}, indent=2, sort_keys=True))
    return 1 if failures or audit["counts"]["missing"] or audit["counts"]["corrupt"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

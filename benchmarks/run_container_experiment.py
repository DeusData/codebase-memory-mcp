#!/usr/bin/env python3
"""Run the existing benchmark matrix in a native, resource-bounded Docker cohort.

The coordinator owns Docker isolation and artifact transfer only. Candidate
resolution, measurements, correctness gates, immutable plans, and reports remain
implemented by run_experiments.py and run_benchmark.py.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = ROOT / "test-infrastructure" / "Dockerfile"
OWNED_RUNNER_FLAGS = frozenset(
    {
        "--candidate-root",
        "--experiment-root",
        "--matrix-spec",
        "--plan",
        "--product-env",
        "--quick",
        "--full",
    }
)
MEMORY_LIMIT_PATTERN = re.compile(r"^[1-9][0-9]*(?:b|k|m|g|t)$", re.IGNORECASE)
CONTAINER_SCRIPT = r"""
set -euo pipefail
source_revision=$1
bundle_name=$2
source_key=$3
shift 3
export HOME=/benchmark/home
mkdir -p "$HOME" /benchmark/sources /benchmark/candidates/build-logs
repository=/benchmark/sources/$source_key
if [ ! -d "$repository/.git" ]; then
  git clone --quiet "/benchmark/$bundle_name" "$repository"
fi
git -C "$repository" checkout --quiet --detach "$source_revision"
if [ -n "$(git -C "$repository" status --porcelain --untracked-files=no)" ]; then
  echo "benchmark source clone has tracked changes: $repository" >&2
  exit 65
fi
cd "$repository"
exec python3 benchmarks/run_experiments.py "$@"
""".strip()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_container_manifest(
    experiment_root: Path,
    source_revision: str,
    bundle_sha256: str,
    manifest: dict[str, Any],
) -> Path:
    """Write an immutable, content-addressed environment record."""
    payload = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
    manifest_sha = hashlib.sha256(payload).hexdigest()
    path = (
        experiment_root
        / "manifests"
        / (
            f"container-environment-{source_revision[:12]}-{bundle_sha256[:12]}-"
            f"{manifest_sha[:12]}.json"
        )
    )
    if path.exists() and path.read_bytes() != payload:
        raise RuntimeError(f"manifest hash collision at {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def failure_log_export_root(experiment_root: Path, source_revision: str) -> Path:
    """Return the commit-keyed destination for failed candidate build logs."""
    return experiment_root / "container-failures" / source_revision[:12] / "build-logs"


def native_linux_platform(machine: str) -> str:
    normalized = machine.strip().lower()
    if normalized in {"arm64", "aarch64"}:
        return "linux/arm64"
    if normalized in {"amd64", "x86_64"}:
        return "linux/amd64"
    raise ValueError(
        f"unsupported host architecture {machine!r}; use a native arm64 or amd64 "
        "host for performance measurements"
    )


def validate_resources(cpus: float, memory: str, workers: int) -> dict[str, Any]:
    if not math.isfinite(cpus) or cpus <= 0:
        raise ValueError("benchmark CPUs must be a finite value greater than zero")
    if not MEMORY_LIMIT_PATTERN.fullmatch(memory):
        raise ValueError(
            "benchmark memory must be an explicit Docker limit such as 8g or 16384m"
        )
    if workers <= 0:
        raise ValueError("benchmark workers must be greater than zero")
    if workers > cpus:
        raise ValueError(
            f"benchmark workers ({workers}) cannot exceed the CPU budget ({cpus:g})"
        )
    return {"cpus": cpus, "memory": memory.lower(), "workers": workers}


def validate_forwarded_arguments(arguments: list[str]) -> list[str]:
    values = list(arguments)
    if values[:1] == ["--"]:
        values.pop(0)
    conflicts = sorted(
        item for item in values if item.partition("=")[0] in OWNED_RUNNER_FLAGS
    )
    if conflicts:
        raise ValueError(
            "runner arguments cannot replace coordinator-owned flags: "
            + ", ".join(conflicts)
        )
    return values


def volume_mount(name: str, destination: str) -> str:
    return f"type=volume,src={name},dst={destination}"


def bundle_revision_arguments() -> list[str]:
    """Include benchmarkable refs without copying stash or recovery namespaces."""
    return ["HEAD", "--branches", "--tags", "--remotes"]


def materialize_container_matrix_spec(
    source: Path, destination: Path, workers: int
) -> str:
    try:
        document = json.loads(source.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"matrix spec is not valid JSON: {source}") from error
    if not isinstance(document, dict):
        raise ValueError("matrix spec must be a JSON object")
    product_environment = document.get("product_environment", {})
    if not isinstance(product_environment, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in product_environment.items()
    ):
        raise ValueError("matrix spec product_environment must be string-to-string")
    declared_workers = product_environment.get("CBM_WORKERS")
    expected_workers = str(workers)
    if declared_workers not in {None, expected_workers}:
        raise ValueError(
            "matrix spec CBM_WORKERS conflicts with the container resource budget: "
            f"spec={declared_workers} coordinator={expected_workers}"
        )
    document["product_environment"] = {
        **product_environment,
        "CBM_WORKERS": expected_workers,
    }
    payload = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode("utf-8")
    destination.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def build_measured_command(
    *,
    docker: str,
    image: str,
    platform_name: str,
    resources: dict[str, Any],
    container_name: str,
    work_volume: str,
    results_volume: str,
    source_revision: str,
    bundle_name: str,
    runner_arguments: list[str],
    uid: int | None,
    gid: int | None,
) -> list[str]:
    command = [
        docker,
        "run",
        "--rm",
        "--name",
        container_name,
        "--platform",
        platform_name,
        "--cpus",
        f"{resources['cpus']:g}",
        "--memory",
        resources["memory"],
        "--mount",
        volume_mount(work_volume, "/benchmark"),
        "--mount",
        volume_mount(results_volume, "/results"),
        "--entrypoint",
        "/bin/bash",
    ]
    if uid is not None and gid is not None:
        command.extend(("--user", f"{uid}:{gid}"))
    source_key = hashlib.sha256(
        f"{source_revision}\0{bundle_name}".encode("utf-8")
    ).hexdigest()[:20]
    command.extend(
        (
            image,
            "-c",
            CONTAINER_SCRIPT,
            "cbm-benchmark-container",
            source_revision,
            bundle_name,
            source_key,
            *runner_arguments,
            "--experiment-root",
            "/results",
            "--candidate-root",
            "/benchmark/candidates",
        )
    )
    return command


def merge_exported_tree(source: Path, destination: Path) -> None:
    """Merge immutable exported artifacts without replacing different bytes."""
    destination.mkdir(parents=True, exist_ok=True)
    for source_path in sorted(source.rglob("*")):
        relative = source_path.relative_to(source)
        destination_path = destination / relative
        if source_path.is_symlink():
            raise RuntimeError(f"export contains an unsupported symlink: {source_path}")
        if source_path.is_dir():
            destination_path.mkdir(parents=True, exist_ok=True)
            continue
        if destination_path.exists():
            if not destination_path.is_file():
                raise RuntimeError(
                    f"export destination is not a file: {destination_path}"
                )
            if file_sha256(source_path) != file_sha256(destination_path):
                raise RuntimeError(
                    f"export destination contains different bytes: {destination_path}"
                )
            continue
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination_path.with_name(
            f".{destination_path.name}.container-export-{os.getpid()}"
        )
        shutil.copy2(source_path, temporary)
        os.replace(temporary, destination_path)


def run_command(
    command: list[str],
    *,
    cwd: Path | None = None,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    process = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        capture_output=capture,
        check=False,
    )
    if process.returncode != 0:
        detail = ""
        if capture:
            detail = (process.stderr or process.stdout).strip()
        suffix = f": {detail}" if detail else ""
        raise RuntimeError(
            f"command failed with exit {process.returncode}: "
            f"{' '.join(command[:4])}{suffix}"
        )
    return process


def docker_json(docker: str, arguments: list[str]) -> dict[str, Any]:
    process = run_command([docker, *arguments], capture=True)
    try:
        value = json.loads(process.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(
            f"Docker returned invalid JSON for {' '.join(arguments)}"
        ) from error
    if not isinstance(value, dict):
        raise RuntimeError(f"Docker returned non-object JSON for {' '.join(arguments)}")
    return value


def remove_container(docker: str, name: str) -> None:
    subprocess.run(
        [docker, "rm", "--force", name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )


def ensure_volume(docker: str, name: str, role: str) -> None:
    inspect = subprocess.run(
        [docker, "volume", "inspect", name, "--format", "{{json .Labels}}"],
        text=True,
        capture_output=True,
        check=False,
    )
    expected = {
        "com.codebase-memory-mcp.benchmark": "true",
        "com.codebase-memory-mcp.role": role,
    }
    if inspect.returncode == 0:
        labels = json.loads(inspect.stdout)
        if labels != expected:
            raise RuntimeError(
                f"Docker volume {name} exists without the expected benchmark "
                f"ownership labels; choose a different experiment root"
            )
        return
    run_command(
        [
            docker,
            "volume",
            "create",
            "--label",
            "com.codebase-memory-mcp.benchmark=true",
            "--label",
            f"com.codebase-memory-mcp.role={role}",
            name,
        ]
    )


def copy_to_volume(
    docker: str,
    image: str,
    volume: str,
    destination: str,
    source: Path,
    container_name: str,
) -> None:
    copy_source = f"{source}{os.sep}." if source.is_dir() else str(source)
    remove_container(docker, container_name)
    try:
        run_command(
            [
                docker,
                "create",
                "--name",
                container_name,
                "--mount",
                volume_mount(volume, destination),
                "--entrypoint",
                "/bin/true",
                image,
            ]
        )
        run_command([docker, "cp", copy_source, f"{container_name}:{destination}"])
    finally:
        remove_container(docker, container_name)


def export_results(
    docker: str,
    image: str,
    results_volume: str,
    destination: Path,
    container_name: str,
) -> None:
    remove_container(docker, container_name)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".cbm-container-export-", dir=destination.parent
    ) as tmpdir:
        staging = Path(tmpdir)
        try:
            run_command(
                [
                    docker,
                    "create",
                    "--name",
                    container_name,
                    "--mount",
                    volume_mount(results_volume, "/results"),
                    "--entrypoint",
                    "/bin/true",
                    image,
                ]
            )
            run_command([docker, "cp", f"{container_name}:/results/.", str(staging)])
        finally:
            remove_container(docker, container_name)
        merge_exported_tree(staging, destination)


def export_volume_subtree(
    docker: str,
    image: str,
    volume: str,
    volume_destination: str,
    subtree: str,
    destination: Path,
    container_name: str,
) -> None:
    """Export one named-volume subtree through the immutable history merge."""
    remove_container(docker, container_name)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".cbm-container-subtree-export-", dir=destination.parent
    ) as tmpdir:
        staging = Path(tmpdir)
        try:
            run_command(
                [
                    docker,
                    "create",
                    "--name",
                    container_name,
                    "--mount",
                    volume_mount(volume, volume_destination),
                    "--entrypoint",
                    "/bin/true",
                    image,
                ]
            )
            run_command([docker, "cp", f"{container_name}:{subtree}/.", str(staging)])
        finally:
            remove_container(docker, container_name)
        merge_exported_tree(staging, destination)


def parse_bundle_heads(bundle: Path) -> list[dict[str, str]]:
    process = run_command(
        ["git", "bundle", "list-heads", str(bundle)], cwd=ROOT, capture=True
    )
    heads: list[dict[str, str]] = []
    for line in process.stdout.splitlines():
        revision, separator, ref = line.partition(" ")
        if separator and len(revision) == 40:
            heads.append({"revision": revision, "ref": ref})
    return heads


def git_output(arguments: list[str]) -> str:
    return run_command(["git", *arguments], cwd=ROOT, capture=True).stdout.strip()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--matrix-spec", type=Path)
    source.add_argument("--quick", action="store_true")
    source.add_argument("--full", action="store_true")
    parser.add_argument("--experiment-root", type=Path, required=True)
    parser.add_argument("--cpus", type=float, required=True)
    parser.add_argument("--memory", required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--image")
    parser.add_argument("--docker", default="docker")
    parser.add_argument(
        "runner_arguments",
        nargs=argparse.REMAINDER,
        help="Additional run_experiments.py arguments after --.",
    )
    return parser


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.quick and not args.full and args.matrix_spec is None:
        args.quick = True
    try:
        args.resources = validate_resources(args.cpus, args.memory, args.workers)
        args.runner_arguments = validate_forwarded_arguments(args.runner_arguments)
        args.platform = native_linux_platform(platform.machine())
    except ValueError as error:
        parser.error(str(error))
    args.experiment_root = args.experiment_root.expanduser().resolve()
    if args.matrix_spec is not None:
        args.matrix_spec = args.matrix_spec.expanduser().resolve()
        if not args.matrix_spec.is_file():
            parser.error(f"matrix spec does not exist: {args.matrix_spec}")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_arguments(argv)
    tracked = git_output(["status", "--porcelain", "--untracked-files=no"])
    if tracked:
        raise RuntimeError(
            "benchmark source worktree has tracked changes; commit or preserve them "
            "before creating the immutable container input"
        )
    source_revision = git_output(["rev-parse", "HEAD"])
    docker_info = docker_json(args.docker, ["info", "--format", "{{json .}}"])
    server_platform = native_linux_platform(str(docker_info.get("Architecture", "")))
    if server_platform != args.platform:
        raise RuntimeError(
            f"Docker server architecture {server_platform} does not match native "
            f"host platform {args.platform}; emulation is not valid for performance"
        )

    dockerfile_sha = file_sha256(DOCKERFILE)
    image = args.image or (
        f"cbm-benchmark-runtime:{dockerfile_sha[:12]}-{args.platform.rsplit('/', 1)[1]}"
    )
    if args.image is None:
        with tempfile.TemporaryDirectory(
            prefix="cbm-benchmark-empty-build-context-"
        ) as empty_context:
            run_command(
                [
                    args.docker,
                    "build",
                    "--platform",
                    args.platform,
                    "--file",
                    str(DOCKERFILE),
                    "--tag",
                    image,
                    empty_context,
                ]
            )
    image_metadata = docker_json(
        args.docker,
        [
            "image",
            "inspect",
            image,
            "--format",
            "{{json .}}",
        ],
    )
    if (
        f"{image_metadata.get('Os')}/{image_metadata.get('Architecture')}"
        != args.platform
    ):
        raise RuntimeError(
            f"image platform {image_metadata.get('Os')}/"
            f"{image_metadata.get('Architecture')} does not match {args.platform}"
        )

    history_key = hashlib.sha256(str(args.experiment_root).encode("utf-8")).hexdigest()[
        :16
    ]
    work_volume = f"cbm-benchmark-work-{history_key}"
    results_volume = f"cbm-benchmark-results-{history_key}"
    ensure_volume(args.docker, work_volume, "work")
    ensure_volume(args.docker, results_volume, "results")

    name_prefix = f"cbm-benchmark-{history_key}-{os.getpid()}"
    seed_name = f"{name_prefix}-seed"
    measured_name = f"{name_prefix}-measured"
    export_name = f"{name_prefix}-export"
    try:
        with tempfile.TemporaryDirectory(prefix="cbm-benchmark-input-") as tmpdir:
            input_root = Path(tmpdir)
            bundle = input_root / "repository.bundle"
            run_command(
                [
                    "git",
                    "bundle",
                    "create",
                    str(bundle),
                    *bundle_revision_arguments(),
                ],
                cwd=ROOT,
            )
            run_command(
                ["git", "bundle", "verify", str(bundle)], cwd=ROOT, capture=True
            )
            bundle_sha = file_sha256(bundle)
            bundle_name = f"repository-{bundle_sha}.bundle"
            copied_bundle = input_root / bundle_name
            bundle.replace(copied_bundle)
            copy_to_volume(
                args.docker,
                image,
                work_volume,
                "/benchmark",
                copied_bundle,
                seed_name,
            )

            runner_arguments: list[str]
            matrix_sha: str | None = None
            effective_matrix_sha: str | None = None
            if args.matrix_spec is not None:
                matrix_sha = file_sha256(args.matrix_spec)
                provisional_matrix = input_root / "matrix-effective.json"
                effective_matrix_sha = materialize_container_matrix_spec(
                    args.matrix_spec,
                    provisional_matrix,
                    args.resources["workers"],
                )
                matrix_name = f"matrix-{effective_matrix_sha}.json"
                copied_matrix = input_root / matrix_name
                provisional_matrix.replace(copied_matrix)
                copy_to_volume(
                    args.docker,
                    image,
                    work_volume,
                    "/benchmark",
                    copied_matrix,
                    seed_name,
                )
                runner_arguments = ["--matrix-spec", f"/benchmark/{matrix_name}"]
            elif args.full:
                runner_arguments = [
                    "--full",
                    "--product-env",
                    f"CBM_WORKERS={args.resources['workers']}",
                ]
            else:
                runner_arguments = [
                    "--quick",
                    "--product-env",
                    f"CBM_WORKERS={args.resources['workers']}",
                ]
            runner_arguments.extend(args.runner_arguments)

            manifest = {
                "schema_version": 1,
                "recorded_at_utc": utc_now(),
                "source_revision": source_revision,
                "source_tree": git_output(["rev-parse", "HEAD^{tree}"]),
                "bundle_sha256": bundle_sha,
                "bundle_heads": parse_bundle_heads(copied_bundle),
                "matrix_spec_sha256": matrix_sha,
                "effective_matrix_spec_sha256": effective_matrix_sha,
                "image": image,
                "image_id": image_metadata.get("Id"),
                "image_repo_digests": image_metadata.get("RepoDigests") or [],
                "docker_server": {
                    key: docker_info.get(key)
                    for key in (
                        "Architecture",
                        "Driver",
                        "MemTotal",
                        "NCPU",
                        "OperatingSystem",
                        "OSType",
                        "ServerVersion",
                    )
                },
                "platform": args.platform,
                "resources": args.resources,
                "work_volume": work_volume,
                "results_volume": results_volume,
                "volumes_retained_for_resume": True,
                "runner_arguments": runner_arguments,
            }
            manifest_path = write_container_manifest(
                input_root,
                source_revision,
                bundle_sha,
                manifest,
            )
            copy_to_volume(
                args.docker,
                image,
                results_volume,
                "/results/manifests",
                manifest_path.parent,
                seed_name,
            )

            uid = os.getuid() if hasattr(os, "getuid") else None
            gid = os.getgid() if hasattr(os, "getgid") else None
            if uid is not None and gid is not None:
                run_command(
                    [
                        args.docker,
                        "run",
                        "--rm",
                        "--mount",
                        volume_mount(work_volume, "/benchmark"),
                        "--mount",
                        volume_mount(results_volume, "/results"),
                        "--entrypoint",
                        "/bin/chown",
                        image,
                        "-R",
                        f"{uid}:{gid}",
                        "/benchmark",
                        "/results",
                    ]
                )
            measured = build_measured_command(
                docker=args.docker,
                image=image,
                platform_name=args.platform,
                resources=args.resources,
                container_name=measured_name,
                work_volume=work_volume,
                results_volume=results_volume,
                source_revision=source_revision,
                bundle_name=bundle_name,
                runner_arguments=runner_arguments,
                uid=uid,
                gid=gid,
            )
            measured_process = subprocess.run(measured, text=True, check=False)
            export_results(
                args.docker,
                image,
                results_volume,
                args.experiment_root,
                export_name,
            )
            if measured_process.returncode != 0:
                failure_logs = failure_log_export_root(
                    args.experiment_root, source_revision
                )
                try:
                    export_volume_subtree(
                        args.docker,
                        image,
                        work_volume,
                        "/benchmark",
                        "/benchmark/candidates/build-logs",
                        failure_logs,
                        export_name,
                    )
                    failure_log_detail = (
                        f"; candidate build logs exported to {failure_logs}"
                    )
                except RuntimeError as log_error:
                    failure_log_detail = (
                        "; candidate build logs could not be exported automatically "
                        f"({log_error}); inspect {work_volume} at "
                        "/benchmark/candidates/build-logs"
                    )
                raise RuntimeError(
                    "container benchmark failed with exit "
                    f"{measured_process.returncode}; partial immutable results were "
                    f"exported to {args.experiment_root}{failure_log_detail}"
                )
    except Exception as error:
        raise RuntimeError(
            f"{error}; benchmark volumes retained for inspection or resume: "
            f"{work_volume}, {results_volume}"
        ) from error
    finally:
        for name in (seed_name, measured_name, export_name):
            remove_container(args.docker, name)

    print(
        json.dumps(
            {
                "status": "complete",
                "experiment_root": str(args.experiment_root),
                "work_volume": work_volume,
                "results_volume": results_volume,
                "volumes_retained_for_resume": True,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

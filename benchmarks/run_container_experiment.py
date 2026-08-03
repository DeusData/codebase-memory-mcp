#!/usr/bin/env python3
"""Run the existing benchmark matrix in a native, resource-bounded Docker cohort.

The coordinator owns Docker isolation and artifact transfer only. Candidate
resolution, measurements, correctness gates, immutable plans, and reports remain
implemented by run_experiments.py and run_benchmark.py.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = ROOT / "test-infrastructure" / "Dockerfile"
# Staged corpora live beside the bundle on the work volume, outside /results, because
# they are measured input rather than exported output.
CONTAINER_CORPUS_ROOT = "/benchmark/corpora"
CORPUS_FLAG = "--corpus"
DEFAULT_BUILD_ENVIRONMENT = {
    "CC": "clang-18",
    "CXX": "clang++-18",
}
OWNED_RUNNER_FLAGS = frozenset(
    {
        "--candidate-root",
        "--candidate-search-root",
        "--build-jobs",
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
mkdir -p "$HOME" /benchmark/sources
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


def repository_snapshot_sha256(
    source_revision: str, bundle_heads: list[dict[str, str]]
) -> str:
    """Hash Git commit/ref content independently of bundle pack bytes."""
    identity = {
        "source_revision": source_revision,
        "bundle_heads": sorted(
            (
                {"ref": head["ref"], "revision": head["revision"]}
                for head in bundle_heads
            ),
            key=lambda head: (head["ref"], head["revision"]),
        ),
    }
    payload = json.dumps(identity, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    return hashlib.sha256(payload).hexdigest()


def runtime_image_sha256(metadata: dict[str, Any]) -> str:
    """Hash runtime-relevant image bytes without BuildKit attestation metadata.

    Docker's top-level OCI index digest can change when BuildKit regenerates provenance
    even though the selected platform's configuration and root filesystem layers are
    byte-identical. Conversely, an explicit image can retain its tag while changing the
    compiler or filesystem. The resumable run key therefore uses the stable runtime
    projection below rather than either the mutable tag or the attestation-bearing index.
    """
    architecture = metadata.get("Architecture")
    operating_system = metadata.get("Os")
    config = metadata.get("Config")
    rootfs = metadata.get("RootFS")
    if not isinstance(architecture, str) or not architecture:
        raise ValueError("Docker image metadata lacks a non-empty Architecture")
    if not isinstance(operating_system, str) or not operating_system:
        raise ValueError("Docker image metadata lacks a non-empty Os")
    if not isinstance(config, dict):
        raise ValueError("Docker image metadata Config must be an object")
    if not isinstance(rootfs, dict):
        raise ValueError("Docker image metadata RootFS must be an object")
    layers = rootfs.get("Layers")
    if (
        not isinstance(layers, list)
        or not layers
        or not all(isinstance(layer, str) and layer for layer in layers)
    ):
        raise ValueError(
            "Docker image metadata RootFS.Layers must be non-empty strings"
        )
    identity = {
        "architecture": architecture,
        "os": operating_system,
        "config": config,
        "rootfs": rootfs,
    }
    payload = json.dumps(identity, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    return hashlib.sha256(payload).hexdigest()


def container_run_key(
    *,
    source_revision: str,
    repository_snapshot_sha256: str,
    matrix_spec_sha256: str | None,
    runtime_image_sha256: str,
    resources: dict[str, Any],
    runner_arguments: list[str],
    corpora: list[dict[str, str]] | None = None,
) -> str:
    """Identify one resumable measurement cohort inside a named history."""
    identity = {
        "source_revision": source_revision,
        "repository_snapshot_sha256": repository_snapshot_sha256,
        "matrix_spec_sha256": matrix_spec_sha256,
        "runtime_image_sha256": runtime_image_sha256,
        "resources": resources,
        # Audit-only changes execution, not the measured plan or environment.
        "runner_arguments": [
            argument for argument in runner_arguments if argument != "--audit-only"
        ],
    }
    # Staged corpora are measured input: two pins sharing a run key would resume into
    # one runset and merge cells taken against different source trees. Absent rather
    # than empty when nothing is staged, so run keys recorded before corpus staging
    # existed still resolve to the same cohort.
    if corpora:
        identity["corpora"] = sorted(
            ({"id": entry["id"], "revision": entry["revision"]} for entry in corpora),
            key=lambda entry: entry["id"],
        )
    payload = json.dumps(identity, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    return hashlib.sha256(payload).hexdigest()[:24]


def load_benchmark_module() -> Any:
    """Reuse run_benchmark.py's corpus logic instead of restating it here.

    Same importlib pattern autotune.py uses for run_experiments.py helpers. The
    resolution ladder, the pin check and the environment-variable spelling stay defined
    once, in the module that also reads them inside the container.
    """
    cached = sys.modules.get("run_benchmark")
    if cached is not None:
        return cached
    path = Path(__file__).resolve().with_name("run_benchmark.py")
    spec = importlib.util.spec_from_file_location("run_benchmark", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load the benchmark harness from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_benchmark"] = module
    spec.loader.exec_module(module)
    return module


def load_experiment_module() -> Any:
    """Reuse the canonical product-environment parser at the outer boundary."""
    cached = sys.modules.get("run_experiments")
    if cached is not None:
        return cached
    path = Path(__file__).resolve().with_name("run_experiments.py")
    spec = importlib.util.spec_from_file_location("run_experiments", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load the experiment harness from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_experiments"] = module
    spec.loader.exec_module(module)
    return module


def corpus_env_key(corpus_id: str) -> str:
    """Spelled by run_benchmark.py so the writer and the reader cannot disagree."""
    return load_benchmark_module().corpus_env_key(corpus_id)


def container_corpus_path(corpus_id: str, revision: str) -> str:
    """Immutable, pin-keyed location for a staged corpus inside the work volume.

    The work volume is retained so a runset can resume, and `docker cp` merges into an
    existing directory rather than replacing it. Keying only by corpus id would leave
    files from a previous pin in place and index a tree matching no commit.
    """
    return f"{CONTAINER_CORPUS_ROOT}/{corpus_id}-{revision[:12]}"


def staged_corpus_revision(entry: dict[str, Any], source: Path, timeout: int) -> str:
    """The commit a staged corpus is keyed and reported by.

    A pinned entry supplies its own 40-character sha. The four mined-workload corpora
    declare "resolve-at-runtime" instead, so their commit is read from the checkout;
    keying on the literal would give every state of that repository one directory and a
    retained work volume would merge two different trees under one name.

    Reading HEAD is sufficient rather than approximate: copy_git_revision_to_dir
    materializes the corpus with `git archive`, which only ever sees committed content,
    so uncommitted edits in the source checkout cannot reach the measured tree.
    """
    harness = load_benchmark_module()
    revision = entry.get("revision") or ""
    if len(revision) == 40:
        return revision
    if revision != harness.UNPINNED_REVISION:
        # Same rule as verify_corpus_pin. Accepting any non-sha here would let a typo'd
        # revision be staged as "whatever HEAD is" and then recorded as the measured
        # commit, which is the failure the sentinel exists to make explicit.
        raise RuntimeError(
            f"corpus {entry.get('id')!r} declares revision {revision!r}, which is "
            f"neither a 40-character commit hash nor {harness.UNPINNED_REVISION!r}"
        )
    return harness.command_stdout(["git", "rev-parse", "HEAD"], timeout, source).strip()


def corpora_in_arguments(arguments: list[str]) -> list[str]:
    """Corpus ids named by one benchmark_args list, in either flag spelling."""
    found: list[str] = []
    for index, argument in enumerate(arguments):
        if argument == CORPUS_FLAG and index + 1 < len(arguments):
            found.append(arguments[index + 1])
        elif argument.startswith(f"{CORPUS_FLAG}="):
            found.append(argument.split("=", 1)[1])
    return found


def corpora_required_by_spec(document: Any) -> list[str]:
    """Corpus ids named by any benchmark_args list anywhere in a matrix spec.

    run_experiments.py accepts benchmark_args at four levels — spec, candidates,
    profiles and scenarios — and concatenates them (:1677-1680). The scan is recursive
    rather than four fixed lookups so a level added later is covered without an edit
    here, and so a corpus named only inside one profile is still staged.
    """
    found: list[str] = []

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "benchmark_args" and isinstance(value, list):
                    found.extend(
                        item
                        for item in corpora_in_arguments(value)
                        if isinstance(item, str)
                    )
                else:
                    visit(value)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(document)
    return sorted(set(found))


def stage_corpora(
    *,
    docker: str,
    image: str,
    work_volume: str,
    corpus_ids: list[str],
    corpus_repo: list[str],
    corpus_manifest: str,
    allow_clone: bool,
    timeout: int,
    container_name: str,
) -> list[dict[str, str]]:
    """Copy each pinned corpus into the work volume and report what was staged.

    The measured container has no network and no host bind mount, so a corpus has to be
    resolved and pin-verified on the host and then carried in. Resolution is
    run_benchmark.py's own ladder (--corpus-repo, CBM_BENCH_CORPUS_<ID>, the shared
    cache, then an explicitly allowed clone), so the host and the container agree on
    which checkout a corpus id means.

    Time and I/O are linear in the staged bytes, once per corpus per run; the work
    volume is retained, but each pin lands in its own directory so a re-pin cannot
    merge into stale files.
    """
    if not corpus_ids:
        return []
    harness = load_benchmark_module()
    registry = harness.load_corpora_manifest(corpus_manifest)
    resolution_args = argparse.Namespace(
        corpus_repo=list(corpus_repo),
        clone_missing_real_repos=allow_clone,
        timeout=timeout,
    )
    staged: list[dict[str, str]] = []
    for corpus_id in corpus_ids:
        if corpus_id not in registry:
            raise RuntimeError(
                f"corpus {corpus_id!r} is absent from the corpus registry; known: "
                + ", ".join(sorted(registry))
            )
        entry = registry[corpus_id]
        source = harness.resolve_corpus_source(
            corpus_id, entry["url"], entry["revision"], resolution_args
        )
        # Verify on the host, where the failure is cheap and the message is visible,
        # rather than after a candidate build has already run inside the container.
        harness.verify_corpus_pin(source, entry, timeout)
        revision = staged_corpus_revision(entry, source, timeout)
        destination = container_corpus_path(corpus_id, revision)
        copy_to_volume(
            docker,
            image,
            work_volume,
            "/benchmark",
            source,
            container_name,
            copy_destination=destination,
        )
        staged.append(
            {
                "id": corpus_id,
                "revision": revision,
                "registry_revision": entry.get("revision", ""),
                "tree": entry.get("tree", ""),
                "host_path": str(source),
                "container_path": destination,
            }
        )
    return staged


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


def resolve_build_jobs(cpus: float, requested: int | None) -> int:
    """Use the container's complete declared CPU capacity unless overridden."""
    if requested is not None:
        if requested <= 0:
            raise ValueError("benchmark build jobs must be greater than zero")
        return requested
    return max(1, math.ceil(cpus))


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
    build_environment = document.get("build_environment", {})
    if not isinstance(build_environment, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in build_environment.items()
    ):
        raise ValueError("matrix spec build_environment must be string-to-string")
    declared_compilers = {
        key for key in DEFAULT_BUILD_ENVIRONMENT if key in build_environment
    }
    if declared_compilers and declared_compilers != set(DEFAULT_BUILD_ENVIRONMENT):
        raise ValueError("container matrix specs must declare CC and CXX together")
    document["build_environment"] = {
        **DEFAULT_BUILD_ENVIRONMENT,
        **build_environment,
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
    repository_snapshot_sha256: str,
    runner_arguments: list[str],
    experiment_root: str,
    uid: int | None,
    gid: int | None,
    corpus_environment: dict[str, str] | None = None,
) -> list[str]:
    command = [
        docker,
        "run",
        "--rm",
        # Candidate CLIs can detach supervised workers. Docker's init forwards
        # stop signals and reaps each exited descendant in O(children) total
        # work, preventing earlier cells from polluting later process tables.
        "--init",
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
    for key, value in DEFAULT_BUILD_ENVIRONMENT.items():
        command.extend(("--env", f"{key}={value}"))
    # Rung 2 of run_benchmark.py's resolution ladder. Pointing at the staged copy this
    # way means the container needs no new flag, no network, and no second resolver.
    for key, value in sorted((corpus_environment or {}).items()):
        command.extend(("--env", f"{key}={value}"))
    source_key = repository_snapshot_sha256[:20]
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
            experiment_root,
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
    volume_destination: str,
    source: Path,
    container_name: str,
    *,
    copy_destination: str | None = None,
) -> None:
    destination = copy_destination or volume_destination
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
                volume_mount(volume, volume_destination),
                "--entrypoint",
                "/bin/mkdir",
                image,
                "-p",
                destination,
            ]
        )
        run_command([docker, "start", "--attach", container_name])
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
    parser.add_argument(
        "--build-jobs",
        type=int,
        help=(
            "candidate build parallelism; defaults to the complete --cpus budget "
            "rounded up"
        ),
    )
    parser.add_argument("--image")
    parser.add_argument("--docker", default="docker")
    parser.add_argument(
        "--invocation-surface",
        choices=("run_container_experiment.py", "run_experiments.py --container"),
        default="run_container_experiment.py",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--product-env",
        action="append",
        default=[],
        metavar="CBM_KEY=VALUE",
        help="Candidate environment for automatic --quick/--full container presets.",
    )
    parser.add_argument(
        "--corpus",
        action="append",
        default=[],
        metavar="ID",
        help=(
            "Stage this corpus into the container in addition to any named by the "
            "matrix spec's benchmark_args, which are detected automatically. Repeatable."
        ),
    )
    parser.add_argument(
        "--corpus-repo",
        action="append",
        default=[],
        metavar="ID=PATH",
        help=(
            "Resolve a corpus id from an existing local checkout, repeatable. Same "
            "spelling and precedence as run_benchmark.py: this wins, then "
            "CBM_BENCH_CORPUS_<ID>, then ~/.cache/codebase-memory-mcp/bench-repos/<id>."
        ),
    )
    parser.add_argument(
        "--corpus-manifest",
        default="",
        help="Corpus registry; defaults to benchmarks/corpora-v1.json.",
    )
    parser.add_argument(
        "--clone-missing-real-repos",
        action="store_true",
        help=(
            "Allow cloning a pinned corpus that is not already available. Cloning "
            "happens on the host before the measured container starts, so the "
            "measurement itself never depends on the network."
        ),
    )
    parser.add_argument(
        "--corpus-timeout",
        type=int,
        default=1800,
        help="Seconds allowed for each host-side corpus resolution or clone.",
    )
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
        args.build_jobs = resolve_build_jobs(args.cpus, args.build_jobs)
        args.runner_arguments = validate_forwarded_arguments(args.runner_arguments)
        args.platform = native_linux_platform(platform.machine())
        experiment_module = load_experiment_module()
        args.product_environment = (
            experiment_module.parse_product_environment_arguments(args.product_env)
        )
        declared_workers = args.product_environment.pop("CBM_WORKERS", None)
        if declared_workers not in {None, str(args.resources["workers"])}:
            raise ValueError(
                "--product-env CBM_WORKERS conflicts with the container resource "
                f"budget: environment={declared_workers} "
                f"coordinator={args.resources['workers']}"
            )
        if args.matrix_spec is not None and args.product_environment:
            raise ValueError(
                "--product-env only applies to container --quick/--full; put explicit "
                "matrix environments in the matrix spec"
            )
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
    runtime_image_identity = runtime_image_sha256(image_metadata)

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
            bundle_heads = parse_bundle_heads(copied_bundle)
            repository_snapshot = repository_snapshot_sha256(
                source_revision, bundle_heads
            )
            source_key = repository_snapshot[:20]
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
            for key, value in sorted(args.product_environment.items()):
                runner_arguments.extend(("--product-env", f"{key}={value}"))
            runner_arguments.extend(("--build-jobs", str(args.build_jobs)))
            runner_arguments.extend(args.runner_arguments)

            # A campaign spec names its corpora in benchmark_args, so the caller does
            # not have to restate them here and cannot accidentally stage a different
            # set from the one the cells will ask for. --corpus adds to that.
            spec_corpora = (
                corpora_required_by_spec(
                    json.loads(copied_matrix.read_text(encoding="utf-8"))
                )
                if args.matrix_spec is not None
                else []
            )
            corpus_ids = sorted(set(spec_corpora) | set(args.corpus))
            staged_corpora = stage_corpora(
                docker=args.docker,
                image=image,
                work_volume=work_volume,
                corpus_ids=corpus_ids,
                corpus_repo=args.corpus_repo,
                corpus_manifest=args.corpus_manifest,
                allow_clone=args.clone_missing_real_repos,
                timeout=args.corpus_timeout,
                container_name=seed_name,
            )
            corpus_environment = {
                corpus_env_key(entry["id"]): entry["container_path"]
                for entry in staged_corpora
            }

            run_key = container_run_key(
                source_revision=source_revision,
                repository_snapshot_sha256=repository_snapshot,
                matrix_spec_sha256=effective_matrix_sha,
                runtime_image_sha256=runtime_image_identity,
                resources=args.resources,
                runner_arguments=runner_arguments,
                corpora=staged_corpora,
            )
            container_experiment_root = f"/results/runsets/{run_key}"

            manifest = {
                "schema_version": 1,
                "recorded_at_utc": utc_now(),
                "source_revision": source_revision,
                "source_tree": git_output(["rev-parse", "HEAD^{tree}"]),
                "bundle_sha256": bundle_sha,
                "bundle_heads": bundle_heads,
                "repository_snapshot_sha256": repository_snapshot,
                "matrix_spec_sha256": matrix_sha,
                "effective_matrix_spec_sha256": effective_matrix_sha,
                "image": image,
                "image_id": image_metadata.get("Id"),
                "image_repo_digests": image_metadata.get("RepoDigests") or [],
                "runtime_image_sha256": runtime_image_identity,
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
                "build_jobs": args.build_jobs,
                "default_build_environment": DEFAULT_BUILD_ENVIRONMENT,
                "invocation_surface": args.invocation_surface,
                "work_volume": work_volume,
                "results_volume": results_volume,
                "volumes_retained_for_resume": True,
                "runner_arguments": runner_arguments,
                "run_key": run_key,
                "container_experiment_root": container_experiment_root,
                "container_repository": f"/benchmark/sources/{source_key}",
                # Which corpus bytes were measured, pin-verified on the host before the
                # container started. Without this a published number names a corpus id
                # but not the commit behind it.
                "staged_corpora": staged_corpora,
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
                "/results",
                manifest_path.parent,
                seed_name,
                copy_destination="/results/manifests",
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
                repository_snapshot_sha256=repository_snapshot,
                runner_arguments=runner_arguments,
                experiment_root=container_experiment_root,
                uid=uid,
                gid=gid,
                corpus_environment=corpus_environment,
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
                        (
                            f"/benchmark/sources/{source_key}/.worktrees/"
                            "benchmark-candidates/build-logs"
                        ),
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
                        f"/benchmark/sources/{source_key}/.worktrees/"
                        "benchmark-candidates/build-logs"
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

#!/usr/bin/env python3
"""Measure fast-mode exact incremental indexing against a fresh full rebuild.

This is an explicit opt-in performance gate. It creates a synthetic Go repo in
a temporary work root, uses an isolated CBM_CACHE_DIR, enables disk incremental
indexing only for that cache, and removes only paths it created.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import queue
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


BENCHMARK_ARTIFACT_DIR_ENV = "CBM_BENCHMARK_ARTIFACT_DIR"


DEFAULT_FILE_COUNT = 240
DEFAULT_FUNCTIONS_PER_FILE = 12
DEFAULT_CHANGED_FILES = 2
DEFAULT_MIN_SPEEDUP = 10.0
DEFAULT_TIMEOUT_SECONDS = 240
RANK_REFRESH_CANDIDATE_DEFAULT = "candidate_default"
DEFAULT_RANK_REFRESH = RANK_REFRESH_CANDIDATE_DEFAULT
DEFAULT_OVERHEAD_PROBES = 0
DEFAULT_OVERHEAD_TOOL = "index_status"
DEFAULT_FRONTIER_FILES = 16
DEFAULT_LIST_PROJECT_COUNTS = "1,16,64"
DEFAULT_LIST_PROJECT_FIXTURE_MAX_MB = 512
LIST_PROJECT_DISK_RESERVE_BYTES = 2 * 1024 * 1024 * 1024
LIST_PROJECT_DISK_RESERVE_FRACTION = 0.05
SEARCH_PROJECTION_INTERNAL_FIELDS = frozenset({"fp", "sp", "bt"})
SEARCH_PROJECTION_CORE_FIELDS = frozenset(
    {
        "name",
        "qualified_name",
        "label",
        "file_path",
        "pagerank",
        "in_degree",
        "out_degree",
        "source",
        "package",
        "read_only",
        "connected",
    }
)
DEFAULT_FASTAPI_URL = "https://github.com/fastapi/fastapi.git"
CONFIG_PROFILE_DEFAULT = "default"
CONFIG_PROFILE_RANK_DISABLED = "rank_disabled"
CONFIG_PROFILE_SIMILARITY_DISABLED = "similarity_disabled"
CONFIG_PROFILE_SEMANTIC_EDGES_DISABLED = "semantic_edges_disabled"
CONFIG_PROFILE_GIT_HISTORY_DISABLED = "git_history_disabled"
CONFIG_PROFILE_HTTP_LINKS_DISABLED = "http_links_disabled"
CONFIG_PROFILE_OPTIONAL_GRAPH_DISABLED = "optional_graph_disabled"
CONFIG_PROFILE_DEPENDENCY_DISABLED = "dependency_disabled"
CONFIG_PROFILE_INCREMENTAL_SEMANTIC_FRESHNESS_EAGER = "incremental_semantic_freshness_eager"
CONFIG_PROFILE_MINIMAL_INDEXING = "minimal_indexing"
DERIVED_REFRESH_CANDIDATE_DEFAULT = "candidate_default"
CONFIG_PROFILES: dict[str, dict[str, str]] = {
    CONFIG_PROFILE_DEFAULT: {},
    CONFIG_PROFILE_RANK_DISABLED: {"rank_enabled": "false"},
    CONFIG_PROFILE_SIMILARITY_DISABLED: {"similarity_enabled": "false"},
    CONFIG_PROFILE_SEMANTIC_EDGES_DISABLED: {"semantic_edges_enabled": "false"},
    CONFIG_PROFILE_GIT_HISTORY_DISABLED: {"githistory_enabled": "false"},
    CONFIG_PROFILE_HTTP_LINKS_DISABLED: {"httplinks_enabled": "false"},
    CONFIG_PROFILE_DEPENDENCY_DISABLED: {"auto_index_deps": "false"},
    CONFIG_PROFILE_INCREMENTAL_SEMANTIC_FRESHNESS_EAGER: {
        "incremental_derived_refresh": "eager"
    },
    CONFIG_PROFILE_OPTIONAL_GRAPH_DISABLED: {
        "githistory_enabled": "false",
        "httplinks_enabled": "false",
        "rank_enabled": "false",
        "semantic_edges_enabled": "false",
        "similarity_enabled": "false",
    },
    CONFIG_PROFILE_MINIMAL_INDEXING: {
        "auto_index_deps": "false",
        "githistory_enabled": "false",
        "httplinks_enabled": "false",
        "rank_enabled": "false",
        "semantic_edges_enabled": "false",
        "similarity_enabled": "false",
    },
}
INDEX_MODES = ("fast", "moderate", "full")
PROJECT_DB_SUFFIX = ".db"
CONFIG_DB_NAME = "_config.db"
LOG_TAIL_LINES = 24
FAILURE_TAIL_LINES = 80
FAILURE_ARTIFACT_DIRNAME = "failures"
FAILURE_FALLBACK_DIRNAME = "cbm-benchmark-failures"
FAILURE_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%SZ"
MCP_INIT_PROTOCOL_VERSION = "2024-11-05"
MATRIX_SCENARIOS_DEFAULT = "go_modify_1,go_modify_2,go_create,go_delete,go_rename,go_new_folder,route_decorator,python_reexport"
MATRIX_REAL_REPO_SCENARIOS = frozenset({"fastapi_insert_probe"})
CAPABILITY_QUALITY_CASES = ("rank", "dependencies", "similarity", "semantic_edges")
CROSS_FILE_RESOLVER_LANGUAGES = (
    "go",
    "c",
    "cpp",
    "cuda",
    "python",
    "javascript",
    "typescript",
    "tsx",
    "php",
    "csharp",
    "java",
    "kotlin",
    "rust",
)
SCOPED_EXACT_FRONTIER_LANGUAGES = frozenset({"go", "c", "cpp", "cuda", "python"})
MATRIX_FRONTIER_SCENARIOS = {
    "go_inbound_frontier": "go",
    "python_inbound_frontier": "python",
    "c_header_inbound_frontier": "c_header",
    "cpp_inbound_frontier": "cpp",
    "cuda_inbound_frontier": "cuda",
    "javascript_inbound_frontier": "javascript",
    "typescript_inbound_frontier": "typescript",
    "tsx_inbound_frontier": "tsx",
    "php_inbound_frontier": "php",
    "csharp_inbound_frontier": "csharp",
    "java_inbound_frontier": "java",
    "kotlin_inbound_frontier": "kotlin",
    "rust_inbound_frontier": "rust",
}
SELF_DOGFOOD_SCENARIOS_DEFAULT = "noop,one_source_file,route_handler,store_pipeline_batch,multi_file_small"
SELF_DOGFOOD_MARKER_PREFIX = "cbm_pan4_oracle"
SELF_DOGFOOD_REPO_SUBDIR = "repo"
SELF_DOGFOOD_CACHE_SUBDIR = "cache"
FASTAPI_PROBE_REL_PATH = "fastapi/routing.py"
FASTAPI_PROBE_INSERT_BEFORE = "\n    def add_api_route(\n"
FASTAPI_PROBE_RETURN_VALUE = 64
PUBLISH_FULL = "full"
PUBLISH_INCREMENTAL_NOOP = "incremental_noop"
PUBLISH_INCREMENTAL_EXACT = "incremental_exact"
PUBLISH_INCREMENTAL_OVERLAY = "incremental_overlay"
PUBLISH_INCREMENTAL_CONTAINMENT = "incremental_containment"
OVERLAY_STATUS_READY = "overlay_ready"  # CBM_STORE_OVERLAY_STATUS_READY
OVERLAY_TOMBSTONE_FILE = "file"  # CBM_STORE_OVERLAY_TOMBSTONE_FILE
OVERLAY_TOMBSTONE_ACTIVE = 1  # STORE_OVERLAY_TOMBSTONE_ACTIVE
OVERLAY_ROW_OWNED = 1  # STORE_OVERLAY_ROW_OWNED
SOURCE_SPAN_LABELS = frozenset(
    {
        "Function",
        "Method",
        "Class",
        "Struct",
        "Interface",
        "Enum",
        "Type",
        "Trait",
        "Module",
    }
)
LOG_MARKER_PIPELINE_DONE = "pipeline.done"
LOG_MARKER_INCREMENTAL_DONE = "incremental.done"
LOG_MARKER_EXACT_DONE = "incremental.exact.done"
LOG_MARKER_EXACT_FRONTIER = "incremental.exact.frontier"
LOG_MARKER_EXACT_FALLBACK = "incremental.exact.fallback"
LOG_MARKER_EXACT_DELETE_FALLBACK = "incremental.exact.delete.fallback"
LOG_MARKER_EXACT_SKIP = "incremental.exact.skip"
LOG_MARKER_DEP_AUTO_INDEX = "sub=dep_auto_index"
LOG_MARKER_RANK_REFRESH = "phase=index_repository sub=rank_refresh"
LOG_MARKER_INDEX_WORKER_TOTAL = "phase=index_repository sub=TOTAL"


class BenchmarkCommandError(RuntimeError):
    def __init__(self, message: str, detail: dict[str, Any]) -> None:
        super().__init__(message)
        self.detail = detail


def now_ms() -> float:
    return time.perf_counter() * 1000.0


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def archive_measurement_log(source: Path, artifact_dir: Path) -> dict[str, Any]:
    """Stream one worker log into a content-addressed reproducible gzip artifact."""
    artifact_dir.mkdir(parents=True, exist_ok=True)
    temporary = artifact_dir / f".worker-log-{os.getpid()}-{time.time_ns()}.tmp"
    source_digest = hashlib.sha256()
    source_bytes = 0
    try:
        with source.open("rb") as input_stream, temporary.open("wb") as output_stream:
            with gzip.GzipFile(filename="", mode="wb", fileobj=output_stream, mtime=0) as compressed:
                for chunk in iter(lambda: input_stream.read(1024 * 1024), b""):
                    source_digest.update(chunk)
                    source_bytes += len(chunk)
                    compressed.write(chunk)
            output_stream.flush()
            os.fsync(output_stream.fileno())
        source_sha256 = source_digest.hexdigest()
        artifact_name = f"{source_sha256}.log.gz"
        destination = artifact_dir / artifact_name
        if destination.exists():
            temporary.unlink()
        else:
            os.replace(temporary, destination)
        return {
            "artifact_name": artifact_name,
            "source_name": source.name,
            "source_bytes": source_bytes,
            "source_sha256": source_sha256,
            "artifact_bytes": destination.stat().st_size,
            "artifact_sha256": file_sha256(destination),
            "compression": "gzip-mtime-0",
        }
    finally:
        if temporary.exists():
            temporary.unlink()


def go_file_content(index: int, revision: int, funcs_per_file: int) -> str:
    lines = ["package main", ""]
    for func_index in range(funcs_per_file):
        value = index * funcs_per_file + func_index + revision
        lines.extend(
            [
                f"func Func{index:04d}_{func_index:02d}() int {{",
                f"\treturn {value}",
                "}",
                "",
            ]
        )
    return "\n".join(lines)


def create_repo(repo_dir: Path, file_count: int, funcs_per_file: int) -> None:
    write_text(repo_dir / "go.mod", "module example.com/cbmbench\n\ngo 1.22\n")
    write_text(repo_dir / "main.go", "package main\n\nfunc main() {}\n")
    for index in range(file_count):
        write_text(repo_dir / f"pkg/file_{index:04d}.go", go_file_content(index, 0, funcs_per_file))


def modify_existing_files(repo_dir: Path, changed_files: int, funcs_per_file: int) -> list[str]:
    changed: list[str] = []
    for index in range(changed_files):
        rel = Path("pkg") / f"file_{index:04d}.go"
        write_text(repo_dir / rel, go_file_content(index, 1000, funcs_per_file))
        changed.append(rel.as_posix())
    return changed


def create_python_reexport_repo(repo_dir: Path) -> None:
    write_text(repo_dir / "fastapi" / "__init__.py", "from .param_functions import Header\n")
    write_text(repo_dir / "fastapi" / "param_functions.py", "def Header(default=None):\n    return default\n")
    write_text(repo_dir / "fastapi" / "openapi" / "models.py", "class Header:\n    pass\n")
    write_text(
        repo_dir / "docs_src" / "app" / "main.py",
        "from fastapi import Header\n\n"
        "def create_item():\n"
        "    return Header(None)\n",
    )


def create_route_repo(repo_dir: Path, route_path: str) -> None:
    write_text(
        repo_dir / "routes.py",
        "from fastapi import FastAPI\n\n"
        "app = FastAPI()\n\n"
        f"@app.get('{route_path}')\n"
        "def orders():\n"
        "    return {'ok': True}\n",
    )


def create_rank_quality_repo(repo_dir: Path) -> dict[str, Any]:
    """Create a lexical-decoy graph where structural rank identifies the useful result."""
    write_text(
        repo_dir / "order_core.py",
        "def zz_order_core(order):\n"
        "    \"\"\"Validate and persist the canonical order workflow.\"\"\"\n"
        "    return {'accepted': bool(order)}\n",
    )
    decoy_names = [f"a{letter}_order_stub" for letter in "abcdefgh"]
    write_text(
        repo_dir / "order_stubs.py",
        "\n\n".join(
            f"def {name}(order):\n    return order" for name in decoy_names
        )
        + "\n",
    )
    for index in range(8):
        write_text(
            repo_dir / f"caller_{index}.py",
            "from order_core import zz_order_core\n\n"
            f"def workflow_{index}(order):\n"
            "    return zz_order_core(order)\n",
        )
    return {
        "fixture_version": 1,
        "capability": "rank",
        "language": "python",
        "relevant_symbol": "zz_order_core",
        "lexical_decoys": decoy_names,
        "ranking_signal": "eight distinct callers target the relevant symbol",
    }


def create_dependency_quality_repo(repo_dir: Path) -> dict[str, Any]:
    """Create a local npm dependency whose source can be auto-indexed without I/O."""
    package_name = "cbmbenchdep"
    symbol = "canonicalDependencyAPI"
    write_text(
        repo_dir / "package.json",
        json.dumps(
            {
                "name": "cbm-dependency-quality-fixture",
                "version": "1.0.0",
                "dependencies": {package_name: "1.0.0"},
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    write_text(
        repo_dir / "src" / "app.js",
        f"import {{ {symbol} }} from '{package_name}';\n\n"
        f"export function useDependency(value) {{ return {symbol}(value); }}\n",
    )
    write_text(
        repo_dir / "node_modules" / package_name / "package.json",
        json.dumps(
            {"name": package_name, "version": "1.0.0", "main": "index.js"},
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    write_text(
        repo_dir / "node_modules" / package_name / "index.js",
        f"export function {symbol}(value) {{ return {{ accepted: Boolean(value) }}; }}\n",
    )
    return {
        "fixture_version": 1,
        "capability": "dependencies",
        "language": "javascript",
        "package_manager": "npm",
        "package": package_name,
        "relevant_symbol": symbol,
        "source_resolution": f"node_modules/{package_name}",
        "network_required": False,
    }


def canonical_json_sha256(value: Any) -> str:
    payload = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def create_pair_quality_repo(repo_dir: Path, capability: str) -> dict[str, Any]:
    task_root = Path(__file__).resolve().parents[1] / "benchmarks" / "semantic-pairs-v1"
    manifest_path = task_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1:
        raise ValueError("semantic pair manifest schema_version must be 1")
    cases = manifest.get("cases")
    case = cases.get(capability) if isinstance(cases, dict) else None
    if not isinstance(case, dict):
        raise ValueError(f"semantic pair manifest has no case for {capability}")
    source_paths = case.get("source_paths")
    if not isinstance(source_paths, list) or not source_paths:
        raise ValueError(f"semantic pair case {capability} requires source_paths")
    source_sha256: dict[str, str] = {}
    for relative in source_paths:
        if (
            not isinstance(relative, str)
            or not relative
            or Path(relative).is_absolute()
            or ".." in Path(relative).parts
        ):
            raise ValueError("semantic pair source path must be relative")
        source = task_root / relative
        payload = source.read_bytes()
        target = repo_dir / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        source_sha256[relative] = hashlib.sha256(payload).hexdigest()
    mutation = case.get("mutation")
    if not isinstance(mutation, dict):
        raise ValueError(f"semantic pair case {capability} requires mutation")
    replacement_relative = mutation.get("replacement_source_path")
    target_relative = mutation.get("target_path")
    if (
        not isinstance(replacement_relative, str)
        or not replacement_relative
        or Path(replacement_relative).is_absolute()
        or ".." in Path(replacement_relative).parts
        or target_relative not in source_paths
    ):
        raise ValueError(f"semantic pair case {capability} has invalid mutation paths")
    replacement_payload = (task_root / replacement_relative).read_bytes()
    mutation = {
        **mutation,
        "replacement_source_sha256": hashlib.sha256(replacement_payload).hexdigest(),
    }
    task_set = {
        "schema_version": manifest["schema_version"],
        "task_set_version": manifest["task_set_version"],
        "ground_truth_scope": manifest["ground_truth_scope"],
        "query_name_marker": manifest["query_name_marker"],
        **case,
        "mutation": mutation,
        "source_sha256": source_sha256,
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
    }
    return {**task_set, "task_set_sha256": canonical_json_sha256(task_set)}


def create_similarity_quality_repo(repo_dir: Path) -> dict[str, Any]:
    return create_pair_quality_repo(repo_dir, "similarity")


def create_semantic_edges_quality_repo(repo_dir: Path) -> dict[str, Any]:
    return create_pair_quality_repo(repo_dir, "semantic_edges")


def apply_pair_quality_mutation(
    repo_dir: Path, fixture: dict[str, Any]
) -> dict[str, Any]:
    mutation = fixture.get("mutation")
    if not isinstance(mutation, dict):
        raise ValueError("pair quality fixture has no mutation")
    target_relative = str(mutation["target_path"])
    replacement_relative = str(mutation["replacement_source_path"])
    target = repo_dir / target_relative
    before_payload = target.read_bytes()
    before_sha256 = hashlib.sha256(before_payload).hexdigest()
    expected_before = fixture.get("source_sha256", {}).get(target_relative)
    if before_sha256 != expected_before:
        raise ValueError(
            f"pair quality mutation source hash mismatch for {target_relative}"
        )
    task_root = Path(__file__).resolve().parents[1] / "benchmarks" / "semantic-pairs-v1"
    replacement_payload = (task_root / replacement_relative).read_bytes()
    after_sha256 = hashlib.sha256(replacement_payload).hexdigest()
    if after_sha256 != mutation.get("replacement_source_sha256"):
        raise ValueError("pair quality replacement source hash mismatch")
    atomic_write_text(target, replacement_payload.decode("utf-8"))
    return {
        "description": mutation["description"],
        "changed_paths": [target_relative],
        "before_sha256": before_sha256,
        "after_sha256": after_sha256,
        "post_judgments": list(mutation["post_judgments"]),
    }


def create_inbound_frontier_repo(
    repo_dir: Path, language: str, dependent_files: int
) -> dict[str, Any]:
    """Create one definition file with a requested number of inbound dependents."""
    if dependent_files <= 0:
        raise ValueError("frontier files must be positive")
    dependent_paths: list[str] = []
    if language == "go":
        write_text(repo_dir / "go.mod", "module example.com/cbmfrontier\n\ngo 1.22\n")
        write_text(repo_dir / "leaf.go", "package frontier\n\nfunc Leaf() int { return 1 }\n")
        for index in range(dependent_files):
            relative = f"caller_{index:04d}.go"
            write_text(
                repo_dir / relative,
                "package frontier\n\n"
                f"func Caller{index:04d}() int {{ return Leaf() + {index} }}\n",
            )
            dependent_paths.append(relative)
        changed_path = "leaf.go"
    elif language == "python":
        write_text(repo_dir / "leaf.py", "def leaf():\n    return 1\n")
        for index in range(dependent_files):
            relative = f"caller_{index:04d}.py"
            write_text(
                repo_dir / relative,
                "from leaf import leaf\n\n"
                f"def caller_{index:04d}():\n    return leaf() + {index}\n",
            )
            dependent_paths.append(relative)
        changed_path = "leaf.py"
    elif language == "c_header":
        write_text(
            repo_dir / "shared.h",
            "#ifndef SHARED_H\n"
            "#define SHARED_H\n"
            "static int shared_value(void) { return 1; }\n"
            "#endif\n",
        )
        for index in range(dependent_files):
            relative = f"consumer_{index:04d}.c"
            write_text(
                repo_dir / relative,
                '#include "shared.h"\n\n'
                f"int consumer_{index:04d}(void) {{ return shared_value() + {index}; }}\n",
            )
            dependent_paths.append(relative)
        changed_path = "shared.h"
    elif language in {"cpp", "cuda"}:
        header_ext, source_ext = ("hpp", "cpp") if language == "cpp" else ("cuh", "cu")
        changed_path = f"shared.{header_ext}"
        write_text(repo_dir / changed_path, "inline int shared_value() { return 1; }\n")
        for index in range(dependent_files):
            relative = f"consumer_{index:04d}.{source_ext}"
            write_text(
                repo_dir / relative,
                f'#include "{changed_path}"\n\n'
                f"int consumer_{index:04d}() {{ return shared_value() + {index}; }}\n",
            )
            dependent_paths.append(relative)
    elif language in {"javascript", "typescript", "tsx"}:
        extension = {"javascript": "js", "typescript": "ts", "tsx": "tsx"}[language]
        changed_path = f"leaf.{extension}"
        return_type = "" if language == "javascript" else ": number"
        write_text(repo_dir / changed_path, f"export function leaf(){return_type} {{ return 1; }}\n")
        for index in range(dependent_files):
            relative = f"caller_{index:04d}.{extension}"
            import_suffix = ".js" if language == "javascript" else ""
            write_text(
                repo_dir / relative,
                f"import {{ leaf }} from './leaf{import_suffix}';\n\n"
                f"export function caller{index:04d}(){return_type} "
                f"{{ return leaf() + {index}; }}\n",
            )
            dependent_paths.append(relative)
    elif language == "php":
        changed_path = "Leaf.php"
        write_text(
            repo_dir / changed_path,
            "<?php\nnamespace Frontier;\nfunction leaf_value(): int { return 1; }\n",
        )
        for index in range(dependent_files):
            relative = f"Caller{index:04d}.php"
            write_text(
                repo_dir / relative,
                "<?php\nnamespace Frontier;\nrequire_once __DIR__ . '/Leaf.php';\n"
                f"function caller_{index:04d}(): int {{ return leaf_value() + {index}; }}\n",
            )
            dependent_paths.append(relative)
    elif language == "csharp":
        changed_path = "Leaf.cs"
        write_text(
            repo_dir / changed_path,
            "namespace Frontier { public static class LeafApi { "
            "public static int Value() { return 1; } } }\n",
        )
        for index in range(dependent_files):
            relative = f"Caller{index:04d}.cs"
            write_text(
                repo_dir / relative,
                "namespace Frontier { "
                f"public class Caller{index:04d} {{ public int Call() "
                f"{{ return LeafApi.Value() + {index}; }} }} }}\n",
            )
            dependent_paths.append(relative)
    elif language == "java":
        changed_path = "Leaf.java"
        write_text(
            repo_dir / changed_path,
            "package frontier;\npublic class Leaf { public static int value() { return 1; } }\n",
        )
        for index in range(dependent_files):
            relative = f"Caller{index:04d}.java"
            write_text(
                repo_dir / relative,
                "package frontier;\n"
                f"class Caller{index:04d} {{ int call() {{ return Leaf.value() + {index}; }} }}\n",
            )
            dependent_paths.append(relative)
    elif language == "kotlin":
        changed_path = "Leaf.kt"
        write_text(repo_dir / changed_path, "package frontier\n\nfun leafValue(): Int = 1\n")
        for index in range(dependent_files):
            relative = f"Caller{index:04d}.kt"
            write_text(
                repo_dir / relative,
                f"package frontier\n\nfun caller{index:04d}(): Int = leafValue() + {index}\n",
            )
            dependent_paths.append(relative)
    elif language == "rust":
        changed_path = "leaf.rs"
        write_text(repo_dir / "Cargo.toml", "[package]\nname='cbm-frontier'\nversion='0.1.0'\n")
        write_text(repo_dir / changed_path, "pub fn leaf_value() -> i32 { 1 }\n")
        modules = ["mod leaf;"]
        for index in range(dependent_files):
            module = f"caller_{index:04d}"
            relative = f"{module}.rs"
            modules.append(f"mod {module};")
            write_text(
                repo_dir / relative,
                "use crate::leaf::leaf_value;\n"
                f"pub fn caller_{index:04d}() -> i32 {{ leaf_value() + {index} }}\n",
            )
            dependent_paths.append(relative)
        write_text(repo_dir / "lib.rs", "\n".join(modules) + "\n")
    else:
        raise ValueError(f"unsupported frontier language: {language}")
    resolver_language = "c" if language == "c_header" else language
    metadata = {
        "source": "synthetic_inbound_frontier",
        "language": language,
        "cross_file_resolver_language": resolver_language,
        "changed_path": changed_path,
        "requested_inbound_dependents": dependent_files,
        "dependent_paths": dependent_paths,
    }
    if resolver_language in SCOPED_EXACT_FRONTIER_LANGUAGES:
        metadata.update(
            {
                "incremental_contract": "exact_frontier",
                "expected_minimum_affected_files": dependent_files + 1,
            }
        )
    else:
        metadata.update(
            {
                "incremental_contract": "safe_full_rebuild",
                "expected_publish_kind": PUBLISH_FULL,
                "expected_reason": "scoped_lsp_gap",
            }
        )
    return metadata


def mutate_inbound_frontier_repo(repo_dir: Path, language: str) -> list[str]:
    if language == "go":
        changed_path = "leaf.go"
        content = (
            "package frontier\n\n"
            "func Leaf() int { return 2 }\n\n"
            "func LeafExtra() int { return Leaf() + 1 }\n"
        )
    elif language == "python":
        changed_path = "leaf.py"
        content = (
            "def leaf():\n    return 2\n\n"
            "def leaf_extra():\n    return leaf() + 1\n"
        )
    elif language == "c_header":
        changed_path = "shared.h"
        content = (
            "#ifndef SHARED_H\n"
            "#define SHARED_H\n"
            "static int shared_value(void) { return 2; }\n"
            "static int shared_extra(void) { return shared_value() + 1; }\n"
            "#endif\n"
        )
    elif language in {"cpp", "cuda"}:
        header_ext = "hpp" if language == "cpp" else "cuh"
        changed_path = f"shared.{header_ext}"
        content = (
            "inline int shared_value() { return 2; }\n"
            "inline int shared_extra() { return shared_value() + 1; }\n"
        )
    elif language in {"javascript", "typescript", "tsx"}:
        extension = {"javascript": "js", "typescript": "ts", "tsx": "tsx"}[language]
        changed_path = f"leaf.{extension}"
        return_type = "" if language == "javascript" else ": number"
        content = (
            f"export function leaf(){return_type} {{ return 2; }}\n"
            f"export function leafExtra(){return_type} {{ return leaf() + 1; }}\n"
        )
    elif language == "php":
        changed_path = "Leaf.php"
        content = (
            "<?php\nnamespace Frontier;\nfunction leaf_value(): int { return 2; }\n"
            "function leaf_extra(): int { return leaf_value() + 1; }\n"
        )
    elif language == "csharp":
        changed_path = "Leaf.cs"
        content = (
            "namespace Frontier { public static class LeafApi { "
            "public static int Value() { return 2; } "
            "public static int Extra() { return Value() + 1; } } }\n"
        )
    elif language == "java":
        changed_path = "Leaf.java"
        content = (
            "package frontier;\npublic class Leaf { public static int value() { return 2; } "
            "public static int extra() { return value() + 1; } }\n"
        )
    elif language == "kotlin":
        changed_path = "Leaf.kt"
        content = (
            "package frontier\n\nfun leafValue(): Int = 2\n"
            "fun leafExtra(): Int = leafValue() + 1\n"
        )
    elif language == "rust":
        changed_path = "leaf.rs"
        content = (
            "pub fn leaf_value() -> i32 { 2 }\n"
            "pub fn leaf_extra() -> i32 { leaf_value() + 1 }\n"
        )
    else:
        raise ValueError(f"unsupported frontier language: {language}")
    write_text(repo_dir / changed_path, content)
    return [changed_path]


def command_result(
    cmd: list[str],
    env: dict[str, str],
    timeout: int,
    cwd: Path | None = None,
) -> tuple[subprocess.CompletedProcess[str], float]:
    start = now_ms()
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return proc, now_ms() - start


def parse_list_project_counts(raw: str) -> list[int]:
    """Parse a strictly increasing positive scaling series."""
    items = raw.split(",") if raw else []
    try:
        counts = [int(item.strip()) for item in items if item.strip()]
    except ValueError as exc:
        raise ValueError("list project counts must be comma-separated integers") from exc
    if not counts or any(count <= 0 for count in counts):
        raise ValueError("list project counts must contain positive integers")
    if any(left >= right for left, right in zip(counts, counts[1:])):
        raise ValueError("list project counts must be strictly increasing")
    return counts


def list_project_fixture_budget(
    *,
    seed_bytes: int,
    maximum_projects: int,
    maximum_fixture_mb: int,
    disk_free_bytes: int,
) -> dict[str, Any]:
    """Return a deterministic disk gate before cloning list-project fixtures."""
    if min(seed_bytes, maximum_projects, maximum_fixture_mb, disk_free_bytes) <= 0:
        raise ValueError("list-project fixture budget inputs must be positive")
    mib = 1024 * 1024
    projected_bytes = seed_bytes * maximum_projects
    cap_bytes = maximum_fixture_mb * mib
    reserved_bytes = max(
        LIST_PROJECT_DISK_RESERVE_BYTES,
        math.ceil(disk_free_bytes * LIST_PROJECT_DISK_RESERVE_FRACTION),
    )
    available_after_reserve = max(0, disk_free_bytes - reserved_bytes)
    reason = ""
    if projected_bytes > cap_bytes:
        reason = "projected fixture exceeds configured cap"
    elif projected_bytes > available_after_reserve:
        reason = "projected fixture violates free-space reserve"
    return {
        "passed": not reason,
        "reason": reason or None,
        "seed_bytes": seed_bytes,
        "maximum_projects": maximum_projects,
        "projected_fixture_bytes": projected_bytes,
        "configured_cap_bytes": cap_bytes,
        "disk_free_bytes": disk_free_bytes,
        "reserved_free_bytes": reserved_bytes,
        "available_after_reserve_bytes": available_after_reserve,
    }


def text_tail(text: str, max_lines: int = FAILURE_TAIL_LINES) -> list[str]:
    lines = text.splitlines()
    return lines[-max_lines:]


def failure_artifact_dir(env: dict[str, str]) -> Path:
    cache_dir = env.get("CBM_CACHE_DIR")
    if cache_dir:
        return Path(cache_dir).expanduser().parent / FAILURE_ARTIFACT_DIRNAME
    return Path(tempfile.gettempdir()) / FAILURE_FALLBACK_DIRNAME


def command_failure(
    label: str,
    cmd: list[str],
    env: dict[str, str],
    proc: subprocess.CompletedProcess[str],
    elapsed_ms: float,
) -> BenchmarkCommandError:
    safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "_", label).strip("_") or "command"
    stamp = datetime.now(timezone.utc).strftime(FAILURE_TIMESTAMP_FORMAT)
    prefix = failure_artifact_dir(env) / f"{stamp}-{safe_label}"
    stdout_path = Path(f"{prefix}.stdout.txt")
    stderr_path = Path(f"{prefix}.stderr.txt")
    meta_path = Path(f"{prefix}.meta.json")

    write_text(stdout_path, proc.stdout)
    write_text(stderr_path, proc.stderr)
    detail: dict[str, Any] = {
        "label": label,
        "returncode": proc.returncode,
        "elapsed_ms": round(elapsed_ms, 3),
        "stdout_bytes": len(proc.stdout.encode("utf-8")),
        "stderr_bytes": len(proc.stderr.encode("utf-8")),
        "stdout_tail": text_tail(proc.stdout),
        "stderr_tail": text_tail(proc.stderr),
        "artifacts": {
            "stdout": str(stdout_path),
            "stderr": str(stderr_path),
            "meta": str(meta_path),
        },
    }
    write_text(
        meta_path,
        json.dumps({"cmd": cmd, **detail}, indent=2, sort_keys=True) + "\n",
    )
    return BenchmarkCommandError(
        f"{label} failed with rc={proc.returncode}; artifacts={detail['artifacts']}",
        detail,
    )


def record_report_error(report: dict[str, Any], exc: Exception) -> None:
    report["error"] = f"{type(exc).__name__}: {exc}"
    if isinstance(exc, BenchmarkCommandError):
        report["error_detail"] = exc.detail


def command_stdout(cmd: list[str], timeout: int, cwd: Path | None = None) -> str:
    proc, _ = command_result(cmd, dict(os.environ), timeout, cwd)
    if proc.returncode != 0:
        rendered = " ".join(cmd)
        raise RuntimeError(f"{rendered} failed: {proc.stderr.strip()}")
    return proc.stdout.strip()


def command_stdout_bytes(cmd: list[str], timeout: int, cwd: Path | None = None) -> bytes:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=dict(os.environ),
        capture_output=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        rendered = " ".join(cmd)
        stderr = proc.stderr.decode("utf-8", "replace").strip()
        raise RuntimeError(f"{rendered} failed: {stderr}")
    return proc.stdout


def append_text(path: Path, text: str) -> None:
    current = path.read_text(encoding="utf-8")
    path.write_text(current + text, encoding="utf-8")


def unwrap_cli_json(stdout: str) -> dict[str, Any]:
    outer = json.loads(stdout)
    if "content" in outer:
        return json.loads(outer["content"][0]["text"])
    return outer


def unwrap_mcp_result(response: dict[str, Any]) -> dict[str, Any]:
    result = response.get("result", {})
    if "content" in result:
        return json.loads(result["content"][0]["text"])
    return result


def cli_result_text(stdout: str) -> str:
    outer = json.loads(stdout)
    if "content" in outer:
        return str(outer["content"][0]["text"])
    return json.dumps(outer, separators=(",", ":"), sort_keys=True)


def mcp_result_text(response: dict[str, Any]) -> str:
    result = response.get("result", {})
    if "content" in result:
        return str(result["content"][0]["text"])
    return json.dumps(result, separators=(",", ":"), sort_keys=True)


TOKEN_ESTIMATOR = "utf8_bytes_div_4_ceil"


def canonical_response_bytes(data: dict[str, Any]) -> bytes:
    """Serialize the tool payload independently of CLI/MCP envelopes."""
    return json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")


def estimate_response_tokens(payload: bytes) -> int:
    """Return a deterministic, dependency-free byte/4 token estimate."""
    return (len(payload) + 3) // 4


def build_search_projection_observation(
    variant: str,
    data: dict[str, Any],
    mcp_envelope_bytes: int,
    elapsed_ms: float,
    transport_survived: bool,
) -> dict[str, Any]:
    results = data.get("results")
    typed_results = [item for item in results if isinstance(item, dict)] if isinstance(results, list) else []
    result_keys = {str(key) for item in typed_results for key in item}
    property_fields = sorted(result_keys - SEARCH_PROJECTION_CORE_FIELDS)
    internal_fields = sorted(result_keys & SEARCH_PROJECTION_INTERNAL_FIELDS)
    qualified_names = [
        str(item["qualified_name"])
        for item in typed_results
        if isinstance(item.get("qualified_name"), str)
    ]
    payload = canonical_response_bytes(data)
    return {
        "variant": variant,
        "returned_count": len(typed_results),
        "qualified_names": qualified_names,
        "property_fields": property_fields,
        "internal_fields": internal_fields,
        "response_bytes": len(payload),
        "response_token_estimate": estimate_response_tokens(payload),
        "mcp_envelope_bytes": mcp_envelope_bytes,
        "elapsed_ms": round(elapsed_ms, 3),
        "transport_survived": transport_survived,
        "passed": isinstance(results, list) and not internal_fields and transport_survived,
    }


def process_rss_kb(pid: int) -> int | None:
    """Read resident memory after a call; this is not a peak-RSS measurement."""
    try:
        proc = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if proc.returncode != 0:
            return None
        return int(proc.stdout.strip())
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return None


def tool_schema_sha256(tool: dict[str, Any]) -> str:
    schema = tool.get("inputSchema")
    payload = json.dumps(schema, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def tool_schema_properties(tool: dict[str, Any] | None) -> set[str]:
    if not isinstance(tool, dict):
        return set()
    schema = tool.get("inputSchema")
    properties = schema.get("properties") if isinstance(schema, dict) else None
    return set(map(str, properties)) if isinstance(properties, dict) else set()


def tool_schema_required(tool: dict[str, Any] | None) -> set[str]:
    if not isinstance(tool, dict):
        return set()
    schema = tool.get("inputSchema")
    required = schema.get("required") if isinstance(schema, dict) else None
    return set(map(str, required)) if isinstance(required, list) else set()


def compare_mcp_tool_surfaces(
    pre_reveal: list[dict[str, Any]],
    post_reveal: list[dict[str, Any]],
    classic: list[dict[str, Any]],
    *,
    pre_dispatch: dict[str, bool],
    list_changed_observed: bool,
) -> dict[str, Any]:
    """Compare discovery and callable coverage without conflating hidden with absent."""
    pre_by_name = {str(tool.get("name")): tool for tool in pre_reveal if tool.get("name")}
    post_by_name = {str(tool.get("name")): tool for tool in post_reveal if tool.get("name")}
    classic_by_name = {str(tool.get("name")): tool for tool in classic if tool.get("name")}
    classic_names = set(classic_by_name)
    advertised_pre = sorted(classic_names & set(pre_by_name))
    hidden_pre = sorted(classic_names - set(pre_by_name))
    dispatch_recognized_pre = sorted(
        name for name in classic_names if pre_dispatch.get(name) is True
    )
    missing_post = sorted(classic_names - set(post_by_name))
    schema_mismatches = sorted(
        name
        for name in classic_names & set(post_by_name)
        if tool_schema_sha256(classic_by_name[name]) != tool_schema_sha256(post_by_name[name])
    )
    name_parity = not missing_post
    schema_parity = name_parity and not schema_mismatches
    dispatch_parity = len(dispatch_recognized_pre) == len(classic_names) and bool(classic_names)
    alias_streamlined = pre_by_name.get("get_code")
    alias_classic = classic_by_name.get("get_code_snippet")
    streamlined_properties = tool_schema_properties(alias_streamlined)
    classic_properties = tool_schema_properties(alias_classic)
    streamlined_required = tool_schema_required(alias_streamlined)
    classic_required = tool_schema_required(alias_classic)
    alias = {
        "streamlined_name": "get_code",
        "classic_name": "get_code_snippet",
        "both_advertised_in_compared_surfaces": bool(alias_streamlined and alias_classic),
        "schema_equal": bool(alias_streamlined and alias_classic)
        and tool_schema_sha256(alias_streamlined) == tool_schema_sha256(alias_classic),
        "property_names_equal": streamlined_properties == classic_properties,
        "shared_properties": sorted(streamlined_properties & classic_properties),
        "streamlined_only_properties": sorted(streamlined_properties - classic_properties),
        "classic_only_properties": sorted(classic_properties - streamlined_properties),
        "streamlined_required": sorted(streamlined_required),
        "classic_required": sorted(classic_required),
        "required_names_equal": streamlined_required == classic_required,
    }
    return {
        "comparison_scope": {
            "advertised_parity": "tool names and input-schema hashes",
            "dispatch_parity": (
                "handler recognition from bounded empty-argument calls; this does not claim "
                "end-to-end behavioral equality"
            ),
        },
        "pre_reveal": {
            "advertised_classic_tools": f"{len(advertised_pre)}/{len(classic_names)}",
            "advertised_classic_tool_names": advertised_pre,
            "intentionally_hidden_classic_tools": hidden_pre,
            "dispatch_recognized_classic_tools": (
                f"{len(dispatch_recognized_pre)}/{len(classic_names)}"
            ),
            "dispatch_recognized_classic_tool_names": dispatch_recognized_pre,
            "classic_dispatch_parity": dispatch_parity,
            "get_code_alias": alias,
        },
        "post_reveal": {
            "classic_name_parity": name_parity,
            "missing_classic_tools": missing_post,
            "classic_schema_parity": schema_parity,
            "schema_mismatches": schema_mismatches,
            "tools_list_changed_observed": list_changed_observed,
        },
        "passed": dispatch_parity and name_parity and schema_parity and list_changed_observed,
    }


def capture_tool_surface(client: "McpClient") -> tuple[dict[str, Any], list[dict[str, Any]]]:
    start = now_ms()
    response = client._request("tools/list", {})
    elapsed_ms = now_ms() - start
    result = response.get("result")
    tools = result.get("tools") if isinstance(result, dict) else None
    if not isinstance(tools, list) or not all(isinstance(tool, dict) for tool in tools):
        raise RuntimeError("MCP tools/list did not return an object array")
    typed_tools = list(tools)
    payload = json.dumps(response, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return (
        {
            "tool_count": len(typed_tools),
            "tool_names": [str(tool.get("name")) for tool in typed_tools],
            "input_schema_sha256": {
                str(tool.get("name")): tool_schema_sha256(tool)
                for tool in typed_tools
                if tool.get("name")
            },
            "list_elapsed_ms": round(elapsed_ms, 3),
            "response_bytes": len(payload),
            "response_token_estimate": estimate_response_tokens(payload),
            "token_estimator": TOKEN_ESTIMATOR,
        },
        typed_tools,
    )


class McpClient:
    def __init__(self, binary: Path, env: dict[str, str], timeout: int) -> None:
        self.binary = binary
        self.env = env
        self.timeout = timeout
        self.next_id = 1
        self.notifications: list[dict[str, Any]] = []
        self.stderr_lines: list[str] = []
        self.stderr_lock = threading.Lock()
        self.stdout_queue: queue.Queue[str | None] = queue.Queue()
        self.proc: subprocess.Popen[str] | None = None
        self.stdout_thread: threading.Thread | None = None
        self.stderr_thread: threading.Thread | None = None

    def __enter__(self) -> "McpClient":
        self.proc = subprocess.Popen(
            [str(self.binary)],
            env=self.env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self.stdout_thread = threading.Thread(target=self._read_stdout, daemon=True)
        self.stderr_thread = threading.Thread(target=self._read_stderr, daemon=True)
        self.stdout_thread.start()
        self.stderr_thread.start()
        self._initialize()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if not self.proc:
            return
        proc = self.proc
        try:
            try:
                if proc.stdin:
                    proc.stdin.close()
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
        finally:
            reader_threads = tuple(
                thread for thread in (self.stdout_thread, self.stderr_thread) if thread
            )
            for thread in reader_threads:
                thread.join(timeout=5)
            alive_threads = [thread for thread in reader_threads if thread.is_alive()]
            for stream in (proc.stdout, proc.stderr):
                if stream:
                    stream.close()
            for thread in alive_threads:
                thread.join(timeout=1)
            readers_still_alive = any(thread.is_alive() for thread in alive_threads)
            self.proc = None
            self.stdout_thread = None
            self.stderr_thread = None
            if readers_still_alive and exc_type is None:
                raise RuntimeError("MCP reader thread did not stop after process exit")

    def _read_stdout(self) -> None:
        assert self.proc and self.proc.stdout
        for line in self.proc.stdout:
            self.stdout_queue.put(line)
        self.stdout_queue.put(None)

    def _read_stderr(self) -> None:
        assert self.proc and self.proc.stderr
        for line in self.proc.stderr:
            with self.stderr_lock:
                self.stderr_lines.append(line.rstrip("\n"))

    def _stderr_mark(self) -> int:
        with self.stderr_lock:
            return len(self.stderr_lines)

    def _stderr_since(self, mark: int) -> str:
        with self.stderr_lock:
            return "\n".join(self.stderr_lines[mark:])

    def _send(self, message: dict[str, Any]) -> None:
        if not self.proc or not self.proc.stdin:
            raise RuntimeError("MCP server is not running")
        self.proc.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
        self.proc.stdin.flush()

    def _request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        req_id = self.next_id
        self.next_id += 1
        message: dict[str, Any] = {"jsonrpc": "2.0", "id": req_id, "method": method}
        if params is not None:
            message["params"] = params
        self._send(message)

        deadline = time.monotonic() + self.timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"MCP request timed out: {method}")
            line = self.stdout_queue.get(timeout=remaining)
            if line is None:
                raise RuntimeError(f"MCP server exited before response: {method}")
            try:
                response = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"non-JSON MCP stdout line: {line[:200]!r}") from exc
            if "id" not in response and isinstance(response.get("method"), str):
                self.notifications.append(response)
                continue
            if response.get("id") == req_id:
                if "error" in response:
                    raise RuntimeError(f"MCP request failed: {response['error']}")
                return response

    def _notification(self, method: str, params: dict[str, Any] | None = None) -> None:
        message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        self._send(message)

    def _initialize(self) -> None:
        self._request(
            "initialize",
            {
                "protocolVersion": MCP_INIT_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "cbm-incr-speed", "version": "1.0"},
            },
        )
        self._notification("notifications/initialized")

    def call_tool(
        self, name: str, arguments: dict[str, Any]
    ) -> tuple[dict[str, Any], str, int, float]:
        text, stderr, stdout_bytes, elapsed_ms = self.call_tool_text(name, arguments)
        return json.loads(text), stderr, stdout_bytes, elapsed_ms

    def call_tool_text(
        self, name: str, arguments: dict[str, Any]
    ) -> tuple[str, str, int, float]:
        mark = self._stderr_mark()
        start = now_ms()
        response = self._request("tools/call", {"name": name, "arguments": arguments})
        elapsed_ms = now_ms() - start
        stderr = self._stderr_since(mark)
        stdout_bytes = len(json.dumps(response, separators=(",", ":")).encode("utf-8"))
        return mcp_result_text(response), stderr, stdout_bytes, elapsed_ms


def run_mcp_surface_parity(
    args: argparse.Namespace, binary: Path
) -> tuple[dict[str, Any], int]:
    auto_root = not bool(args.work_root)
    work_root = (
        Path(args.work_root).expanduser()
        if args.work_root
        else Path(tempfile.mkdtemp(prefix="cbm-mcp-surface-"))
    )
    work_root.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "binary": str(binary),
        "binary_metadata": binary_metadata(binary),
        "mode": "mcp_surface_parity",
        "protocol_version": MCP_INIT_PROTOCOL_VERSION,
        "work_root": str(work_root),
        "cleanup": {"requested": auto_root and not args.keep_work_root, "removed": False},
    }
    exit_code = 1
    try:
        base_env = build_env(work_root / "cache")
        base_env["CBM_AUTO_INDEX"] = "false"

        classic_env = dict(base_env)
        classic_env["CBM_TOOL_MODE"] = "classic"
        with McpClient(binary, classic_env, args.timeout) as classic_client:
            classic_summary, classic_tools = capture_tool_surface(classic_client)

        streamlined_env = dict(base_env)
        streamlined_env["CBM_TOOL_MODE"] = "streamlined"
        with McpClient(binary, streamlined_env, args.timeout) as streamlined_client:
            pre_summary, pre_tools = capture_tool_surface(streamlined_client)
            pre_dispatch: dict[str, bool] = {}
            dispatch_bytes: dict[str, int] = {}
            for tool in classic_tools:
                name = str(tool.get("name") or "")
                if not name:
                    continue
                text, _, response_bytes, _ = streamlined_client.call_tool_text(name, {})
                pre_dispatch[name] = "unknown tool" not in text.lower()
                dispatch_bytes[name] = response_bytes
            streamlined_client.call_tool_text("_hidden_tools", {})
            post_summary, post_tools = capture_tool_surface(streamlined_client)
            list_changed_observed = any(
                item.get("method") == "notifications/tools/list_changed"
                for item in streamlined_client.notifications
            )

        comparison = compare_mcp_tool_surfaces(
            pre_tools,
            post_tools,
            classic_tools,
            pre_dispatch=pre_dispatch,
            list_changed_observed=list_changed_observed,
        )
        pre_summary["classic_dispatch_recognized"] = pre_dispatch
        pre_summary["dispatch_response_bytes"] = dispatch_bytes
        report.update(
            {
                "surfaces": {
                    "streamlined_pre_reveal": pre_summary,
                    "streamlined_post_reveal": post_summary,
                    "classic": classic_summary,
                },
                "comparison": comparison,
                "derived": {"passed": comparison["passed"]},
            }
        )
        exit_code = 0 if comparison["passed"] else 1
    except Exception as exc:
        record_report_error(report, exc)
    finally:
        if auto_root and not args.keep_work_root:
            shutil.rmtree(work_root, ignore_errors=True)
            report["cleanup"]["removed"] = not work_root.exists()
        rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
        if args.out:
            atomic_write_text(Path(args.out).expanduser(), rendered)
        print(rendered, end="")
    return report, exit_code


def run_list_projects_scaling(
    args: argparse.Namespace, binary: Path
) -> tuple[dict[str, Any], int]:
    counts = parse_list_project_counts(args.list_project_counts)
    auto_root = not bool(args.work_root)
    work_root = (
        Path(args.work_root).expanduser()
        if args.work_root
        else Path(tempfile.mkdtemp(prefix="cbm-list-projects-scaling-"))
    )
    cache_dir = work_root / "cache"
    seed_repo = work_root / "seed-repo"
    cache_dir.mkdir(parents=True, exist_ok=True)
    seed_repo.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc)
    metadata = binary_metadata(binary)
    run_id = (
        f"list-projects-{generated_at.strftime('%Y%m%dT%H%M%SZ')}-"
        f"{metadata['sha256'][:12]}-{os.getpid()}"
    )
    report: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": generated_at.isoformat(),
        "binary": str(binary),
        "binary_metadata": metadata,
        "source_revision": git_metadata(Path(__file__).resolve().parents[1], args.timeout),
        "mode": "list_projects_scaling",
        "parameters": {
            "project_counts": counts,
            "maximum_fixture_mb": args.list_project_fixture_max_mb,
            "timeout_seconds": args.timeout,
            "process_isolation": "fresh_mcp_server_per_count",
            "seed_index_mode": "fast",
            "seed_config_profile": CONFIG_PROFILE_MINIMAL_INDEXING,
            "list_projects_arguments": {"all": True},
            "inventory_mode": "explicit_full_compatibility",
            "token_estimator": TOKEN_ESTIMATOR,
            "rss_measurement": "post_call_resident_kb_not_peak",
        },
        "work_root": str(work_root),
        "cleanup": {"requested": auto_root and not args.keep_work_root, "removed": False},
        "observations": [],
        "completion": {"status": "running"},
    }
    exit_code = 1
    try:
        create_repo(seed_repo, 1, 1)
        env = build_env(cache_dir)
        env.pop("CBM_PROFILE", None)
        apply_config_overrides(
            binary, env, CONFIG_PROFILES[CONFIG_PROFILE_MINIMAL_INDEXING], args.timeout
        )
        with McpClient(binary, env, args.timeout) as client:
            seed_result, _, _, _ = client.call_tool(
                "index_repository",
                {**index_tool_arguments(seed_repo, "fast"), "auto_index_deps": False},
            )
        seed_db = find_project_db(cache_dir)
        seed_project = str(seed_result.get("project") or seed_db.stem)
        disk = shutil.disk_usage(work_root)
        budget = list_project_fixture_budget(
            seed_bytes=seed_db.stat().st_size,
            maximum_projects=counts[-1],
            maximum_fixture_mb=args.list_project_fixture_max_mb,
            disk_free_bytes=disk.free,
        )
        report["fixture"] = {
            "seed_project": seed_project,
            "seed_db": str(seed_db),
            "budget": budget,
        }
        if not budget["passed"]:
            raise RuntimeError(str(budget["reason"]))

        created_projects = 1
        for requested_count in counts:
            for fixture_index in range(created_projects, requested_count):
                project = f"list-project-{fixture_index:06d}"
                destination = cache_dir / f"{project}{PROJECT_DB_SUFFIX}"
                root_path = work_root / "roots" / project
                clone_list_project_db(seed_db, destination, project, str(root_path))
            created_projects = requested_count

            client = McpClient(binary, env, args.timeout)
            with client:
                data, stderr, stdout_bytes, elapsed_ms = client.call_tool(
                    "list_projects", {"all": True}
                )
                projects = data.get("projects")
                returned_count = len(projects) if isinstance(projects, list) else None
                transport_start = now_ms()
                tools_response = client._request("tools/list", {})
                transport_probe_ms = now_ms() - transport_start
                transport_survived = isinstance(tools_response.get("result"), dict)
                rss_kb = process_rss_kb(client.proc.pid) if client.proc else None
            server_reaped = (
                client.proc is None
                and client.stdout_thread is None
                and client.stderr_thread is None
            )
            payload = canonical_response_bytes(data)
            db_bytes = sum(
                path.stat().st_size
                for path in cache_dir.glob(f"*{PROJECT_DB_SUFFIX}")
                if path.name != CONFIG_DB_NAME
            )
            report["observations"].append(
                {
                    "requested_projects": requested_count,
                    "returned_projects": returned_count,
                    "response_bytes": len(payload),
                    "response_token_estimate": estimate_response_tokens(payload),
                    "mcp_envelope_bytes": stdout_bytes,
                    "elapsed_ms": round(elapsed_ms, 3),
                    "post_call_rss_kb": rss_kb,
                    "transport_probe_ms": round(transport_probe_ms, 3),
                    "transport_survived": transport_survived,
                    "server_reaped": server_reaped,
                    "fixture_db_bytes": db_bytes,
                    "stderr_bytes": len(stderr.encode("utf-8")),
                    "passed": (
                        returned_count == requested_count
                        and transport_survived
                        and server_reaped
                    ),
                }
            )

        observations = report["observations"]
        first = observations[0]
        last = observations[-1]
        count_delta = last["requested_projects"] - first["requested_projects"]
        byte_delta = last["response_bytes"] - first["response_bytes"]
        report["derived"] = {
            "passed": all(item["passed"] for item in observations),
            "largest_response_bytes": last["response_bytes"],
            "largest_response_token_estimate": last["response_token_estimate"],
            "incremental_response_bytes_per_project": (
                round(byte_delta / count_delta, 3) if count_delta > 0 else None
            ),
            "claim_boundary": (
                "Measures list_projects alone in isolated caches; does not attribute combined "
                "multi-tool response size or claim peak RSS."
            ),
        }
        exit_code = 0 if report["derived"]["passed"] else 1
        report["completion"] = {"status": "complete", "exit_code": exit_code}
    except Exception as exc:
        record_report_error(report, exc)
        report["completion"] = {"status": "failed", "exit_code": 1}
        exit_code = 1
    finally:
        if auto_root and not args.keep_work_root:
            shutil.rmtree(work_root, ignore_errors=True)
            report["cleanup"]["removed"] = not work_root.exists()
        rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
        if args.out:
            out_path = Path(args.out).expanduser()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_text(out_path, rendered)
        print(rendered, end="")
    return report, exit_code


def run_search_projection(args: argparse.Namespace, binary: Path) -> tuple[dict[str, Any], int]:
    if args.search_projection_results <= 0:
        raise ValueError("search projection results must be positive")
    auto_root = not bool(args.work_root)
    work_root = (
        Path(args.work_root).expanduser()
        if args.work_root
        else Path(tempfile.mkdtemp(prefix="cbm-search-projection-"))
    )
    cache_dir = work_root / "cache"
    repo_dir = work_root / "repo"
    cache_dir.mkdir(parents=True, exist_ok=True)
    repo_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc)
    metadata = binary_metadata(binary)
    report: dict[str, Any] = {
        "schema_version": 1,
        "run_id": (
            f"search-projection-{generated_at.strftime('%Y%m%dT%H%M%SZ')}-"
            f"{metadata['sha256'][:12]}-{os.getpid()}"
        ),
        "generated_at_utc": generated_at.isoformat(),
        "binary_metadata": metadata,
        "source_revision": git_metadata(Path(__file__).resolve().parents[1], args.timeout),
        "mode": "search_projection",
        "parameters": {
            "requested_results": args.search_projection_results,
            "format": "json",
            "index_mode": "fast",
            "config_profile": CONFIG_PROFILE_MINIMAL_INDEXING,
            "process_isolation": "fresh_mcp_server_per_variant",
            "token_estimator": TOKEN_ESTIMATOR,
            "rss_measurement": "post_call_resident_kb_not_peak",
        },
        "work_root": str(work_root),
        "cleanup": {"requested": auto_root and not args.keep_work_root, "removed": False},
        "observations": [],
        "completion": {"status": "running"},
    }
    variants: tuple[tuple[str, dict[str, Any]], ...] = (
        ("compact_default", {}),
        ("compact_true", {"compact": True}),
        (
            "compact_selected_fields",
            {"compact": True, "fields": ["complexity", "signature"]},
        ),
        ("compact_false", {"compact": False}),
    )
    exit_code = 1
    try:
        file_count = min(4, args.search_projection_results)
        funcs_per_file = math.ceil(args.search_projection_results / file_count)
        create_repo(repo_dir, file_count, funcs_per_file)
        env = build_env(cache_dir)
        env.pop("CBM_PROFILE", None)
        apply_config_overrides(
            binary, env, CONFIG_PROFILES[CONFIG_PROFILE_MINIMAL_INDEXING], args.timeout
        )
        with McpClient(binary, env, args.timeout) as client:
            index_result, _, _, _ = client.call_tool(
                "index_repository",
                {**index_tool_arguments(repo_dir, "fast"), "auto_index_deps": False},
            )
        project = str(index_result.get("project") or "")
        if not project:
            raise RuntimeError("projection fixture index response omitted project")

        for variant, overrides in variants:
            arguments: dict[str, Any] = {
                "project": project,
                "name_pattern": "Func",
                "limit": args.search_projection_results,
                "sort_by": "name",
                "include_dependencies": False,
                "format": "json",
                **overrides,
            }
            client = McpClient(binary, env, args.timeout)
            with client:
                data, _, envelope_bytes, elapsed_ms = client.call_tool(
                    "search_graph", arguments
                )
                tools_response = client._request("tools/list", {})
                transport_survived = isinstance(tools_response.get("result"), dict)
                rss_kb = process_rss_kb(client.proc.pid) if client.proc else None
            server_reaped = (
                client.proc is None
                and client.stdout_thread is None
                and client.stderr_thread is None
            )
            observation = build_search_projection_observation(
                variant, data, envelope_bytes, elapsed_ms, transport_survived
            )
            observation["post_call_rss_kb"] = rss_kb
            observation["server_reaped"] = server_reaped
            observation["passed"] = bool(observation["passed"] and server_reaped)
            report["observations"].append(observation)

        observations = report["observations"]
        baseline_names = observations[0]["qualified_names"]
        by_variant = {item["variant"]: item for item in observations}
        for observation in observations:
            observation["identity_equal_to_default"] = (
                observation["qualified_names"] == baseline_names
            )
            fields = set(observation["property_fields"])
            variant = observation["variant"]
            if variant in {"compact_default", "compact_true"}:
                projection_met = not fields
            elif variant == "compact_selected_fields":
                projection_met = bool(fields) and fields <= {"complexity", "signature"}
            else:
                projection_met = bool(fields)
            observation["projection_contract_met"] = projection_met
            observation["passed"] = bool(
                observation["passed"]
                and observation["identity_equal_to_default"]
                and projection_met
            )
        compact_bytes = int(by_variant["compact_true"]["response_bytes"])
        selected_bytes = int(by_variant["compact_selected_fields"]["response_bytes"])
        verbose_bytes = int(by_variant["compact_false"]["response_bytes"])
        report["derived"] = {
            "passed": all(bool(item["passed"]) for item in observations),
            "identity_parity": all(
                bool(item["identity_equal_to_default"]) for item in observations
            ),
            "internal_fields_absent": all(not item["internal_fields"] for item in observations),
            "compact_bytes": compact_bytes,
            "selected_fields_bytes": selected_bytes,
            "non_compact_bytes": verbose_bytes,
            "non_compact_over_compact_ratio": (
                round(verbose_bytes / compact_bytes, 3) if compact_bytes else None
            ),
            "projection_order_expected": compact_bytes <= selected_bytes <= verbose_bytes,
            "claim_boundary": (
                "Measures response projection for identical ranked results after one small FAST "
                "index; one latency observation per variant is descriptive only."
            ),
        }
        report["derived"]["passed"] = bool(
            report["derived"]["passed"] and report["derived"]["projection_order_expected"]
        )
        exit_code = 0 if report["derived"]["passed"] else 1
        report["completion"] = {"status": "complete", "exit_code": exit_code}
    except Exception as exc:
        record_report_error(report, exc)
        report["completion"] = {"status": "failed", "exit_code": 1}
        exit_code = 1
    finally:
        if auto_root and not args.keep_work_root:
            shutil.rmtree(work_root, ignore_errors=True)
            report["cleanup"]["removed"] = not work_root.exists()
        rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
        if args.out:
            atomic_write_text(Path(args.out).expanduser(), rendered)
        print(rendered, end="")
    return report, exit_code


def log_tail(stderr: str) -> list[str]:
    lines = stderr.splitlines()
    return lines[-LOG_TAIL_LINES:]


def log_has(stderr: str, marker: str) -> bool:
    return marker in stderr


def response_publish_kind(data: dict[str, Any]) -> str:
    publish_kind = data.get("publish_kind")
    return publish_kind if isinstance(publish_kind, str) else ""


def response_publish_reason(data: dict[str, Any]) -> str:
    publish_reason = data.get("publish_reason")
    return publish_reason if isinstance(publish_reason, str) else ""


def response_freshness(data: dict[str, Any]) -> dict[str, Any] | None:
    freshness = data.get("freshness")
    return freshness if isinstance(freshness, dict) else None


def response_freshness_state(data: dict[str, Any]) -> str:
    freshness = response_freshness(data)
    if not freshness:
        return ""
    state = freshness.get("state")
    return state if isinstance(state, str) else ""


def declared_stale_views(oracles: dict[str, Any]) -> list[str]:
    """Return the sorted union of derived views explicitly reported stale."""
    views: set[str] = set()
    for oracle in oracles.values():
        if not isinstance(oracle, dict):
            continue
        freshness = oracle.get("freshness")
        if not isinstance(freshness, dict) or freshness.get("state") != "stale_with_warning":
            continue
        stale = freshness.get("stale_views")
        if isinstance(stale, list):
            views.update(item for item in stale if isinstance(item, str) and item)
    return sorted(views)


def is_incremental_publish_kind(publish_kind: str) -> bool:
    return publish_kind in {
        PUBLISH_INCREMENTAL_NOOP,
        PUBLISH_INCREMENTAL_EXACT,
        PUBLISH_INCREMENTAL_OVERLAY,
        PUBLISH_INCREMENTAL_CONTAINMENT,
    }


def is_explicit_incremental_route(publish_kind: str | None, reason: str | None = None) -> bool:
    return is_incremental_publish_kind(publish_kind or "") or bool(reason)


def parse_logged_elapsed_ms(stderr: str, marker: str) -> int | None:
    return parse_log_int_field(stderr, marker, "elapsed_ms")


def parse_log_int_field(stderr: str, marker: str, field: str) -> int | None:
    prefix = f"{field}="
    for line in stderr.splitlines():
        if marker not in line:
            continue
        for item in line.split():
            if item.startswith(prefix):
                try:
                    return int(item.split("=", 1)[1])
                except ValueError:
                    return None
    return None


def parse_log_max_int_field(stderr: str, marker: str, field: str) -> int | None:
    prefix = f"{field}="
    maximum: int | None = None
    for line in stderr.splitlines():
        if marker not in line:
            continue
        for item in line.split():
            if not item.startswith(prefix):
                continue
            try:
                value = int(item.split("=", 1)[1])
            except ValueError:
                continue
            maximum = value if maximum is None else max(maximum, value)
    return maximum


def parse_log_text_field(stderr: str, marker: str, field: str) -> str | None:
    prefix = f"{field}="
    for line in reversed(stderr.splitlines()):
        if marker not in line:
            continue
        for item in line.split():
            if item.startswith(prefix):
                return item[len(prefix) :]
    return None


def parse_exact_reason(stderr: str) -> str | None:
    detail = parse_exact_route_detail(stderr)
    reason = detail.get("reason")
    return reason if isinstance(reason, str) and reason else None


def parse_exact_route_detail(stderr: str) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "frontier_changed_files": parse_log_int_field(stderr, LOG_MARKER_EXACT_FRONTIER, "changed"),
        "frontier_expanded_files": parse_log_int_field(stderr, LOG_MARKER_EXACT_FRONTIER, "expanded"),
        "exact_done_files": parse_log_int_field(stderr, LOG_MARKER_EXACT_DONE, "files"),
        "event": None,
        "reason": None,
    }
    reason_markers = (
        (LOG_MARKER_EXACT_FALLBACK, "fallback"),
        (LOG_MARKER_EXACT_DELETE_FALLBACK, "delete_fallback"),
        (LOG_MARKER_EXACT_SKIP, "skip"),
    )
    for line in stderr.splitlines():
        for marker, event in reason_markers:
            prefix = f"msg={marker} reason="
            if prefix not in line:
                continue
            reason = line.split(prefix, 1)[1].split()[0]
            detail["event"] = event
            detail["reason"] = reason or None
            return detail
    if detail["exact_done_files"] is not None:
        detail["event"] = "exact"
    elif detail["frontier_expanded_files"] is not None:
        detail["event"] = "frontier_observed"
    return detail


def response_exact_delta(data: dict[str, Any]) -> dict[str, Any]:
    exact_delta = data.get("exact_delta")
    return exact_delta if isinstance(exact_delta, dict) else {}


def merge_exact_route_detail(
    detail: dict[str, Any],
    data: dict[str, Any],
    publish_kind: str,
    publish_reason: str,
) -> dict[str, Any]:
    exact_delta = response_exact_delta(data)
    field_map = {
        "changed_paths": "frontier_changed_files",
        "affected_paths": "frontier_expanded_files",
        "published_paths": "exact_done_files",
    }
    for response_key, detail_key in field_map.items():
        value = exact_delta.get(response_key)
        if detail.get(detail_key) is None and isinstance(value, int):
            detail[detail_key] = value
    if not detail.get("reason") and publish_reason:
        detail["reason"] = publish_reason
    if not detail.get("event"):
        published = detail.get("exact_done_files")
        if isinstance(published, int) and published > 0:
            detail["event"] = "exact"
        elif isinstance(published, int) and published == 0:
            detail["event"] = "noop"
        elif publish_reason:
            detail["event"] = "fallback"
        elif publish_kind == PUBLISH_INCREMENTAL_EXACT:
            detail["event"] = "exact"
        elif publish_kind == PUBLISH_INCREMENTAL_OVERLAY:
            detail["event"] = "overlay"
        elif publish_kind == PUBLISH_INCREMENTAL_NOOP:
            detail["event"] = "noop"
    return detail


def indexed_work_elapsed_ms(logged_elapsed_ms: dict[str, int | None]) -> int | None:
    incremental_ms = logged_elapsed_ms.get("incremental_done")
    if incremental_ms is not None:
        return incremental_ms
    return logged_elapsed_ms.get("pipeline_done")


def run_config_set(binary: Path, env: dict[str, str], key: str, value: str, timeout: int) -> None:
    cmd = [str(binary), "config", "set", key, value]
    proc, elapsed_ms = command_result(cmd, env, timeout)
    if proc.returncode != 0:
        raise command_failure(f"config_set_{key}", cmd, env, proc, elapsed_ms)


def parse_config_overrides(items: list[str]) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for item in items:
        key, sep, value = item.partition("=")
        if not sep or not key or not value:
            raise SystemExit(f"error: --config must be key=value, got {item!r}")
        overrides[key] = value
    return overrides


def resolve_config_overrides(profile: str, items: list[str]) -> dict[str, str]:
    """Return one explicit benchmark profile plus higher-priority per-key overrides."""
    if profile not in CONFIG_PROFILES:
        raise ValueError(f"unknown config profile: {profile}")
    overrides = dict(CONFIG_PROFILES[profile])
    overrides.update(parse_config_overrides(items))
    return overrides


def index_tool_arguments(repo_dir: Path, index_mode: str) -> dict[str, str]:
    if index_mode not in INDEX_MODES:
        raise ValueError(f"unsupported index mode: {index_mode}")
    return {"repo_path": str(repo_dir), "mode": index_mode}


def index_mode_capability_applicability(index_mode: str) -> dict[str, dict[str, Any]]:
    if index_mode not in INDEX_MODES:
        raise ValueError(f"unsupported index mode: {index_mode}")
    available = {"applicable": True, "reason": f"available in {index_mode} mode"}
    result = {
        name: dict(available)
        for name in (
            "rank",
            "similarity",
            "semantic_edges",
            "git_history",
            "http_links",
            "dependencies",
        )
    }
    if index_mode == "fast":
        result["similarity"] = {
            "applicable": False,
            "reason": "SIMILAR_TO generation requires full or moderate mode",
        }
        result["semantic_edges"] = {
            "applicable": False,
            "reason": "SEMANTICALLY_RELATED generation requires full or moderate mode",
        }
    return result


def apply_config_overrides(
    binary: Path, env: dict[str, str], overrides: dict[str, str], timeout: int
) -> None:
    for key, value in overrides.items():
        run_config_set(binary, env, key, value, timeout)


def apply_rank_refresh_override(
    binary: Path, env: dict[str, str], policy: str, timeout: int
) -> bool:
    """Apply an explicit rank policy while preserving each candidate's default."""
    if policy == RANK_REFRESH_CANDIDATE_DEFAULT:
        return False
    run_config_set(binary, env, "rank_refresh", policy, timeout)
    return True


def build_index_result(
    data: dict[str, Any],
    stderr: str,
    stdout_bytes: int,
    elapsed_ms: float,
    include_logs: bool,
) -> dict[str, Any]:
    measurement_log_markers: list[str] = []
    measurement_log_artifacts: list[dict[str, Any]] = []
    logfiles: list[str] = []
    supervisor_log = parse_log_text_field(stderr, "index.supervisor.profile_log", "log")
    if supervisor_log:
        logfiles.append(supervisor_log)
    response_log = data.get("logfile")
    if isinstance(response_log, str) and response_log and response_log not in logfiles:
        logfiles.append(response_log)
    for logfile in logfiles:
        log_path = Path(logfile)
        artifact_dir_value = os.environ.get(BENCHMARK_ARTIFACT_DIR_ENV)
        if artifact_dir_value and log_path.is_file():
            measurement_log_artifacts.append(
                archive_measurement_log(log_path, Path(artifact_dir_value))
            )
        try:
            with log_path.open(encoding="utf-8", errors="replace") as stream:
                for line in stream:
                    if any(
                        marker in line
                        for marker in (
                            "msg=mem.phase",
                            "msg=pipeline.done",
                            "msg=incremental.done",
                            LOG_MARKER_DEP_AUTO_INDEX,
                            LOG_MARKER_RANK_REFRESH,
                            LOG_MARKER_INDEX_WORKER_TOTAL,
                        )
                    ):
                        measurement_log_markers.append(line.rstrip("\n"))
                        if len(measurement_log_markers) >= 512:
                            break
        except OSError:
            continue
        if measurement_log_markers:
            break
    measurement_text = "\n".join((stderr, *measurement_log_markers))
    elapsed_ms_int = int(elapsed_ms)
    publish_kind = response_publish_kind(data)
    logged_elapsed_ms = {
        "pipeline_done": parse_logged_elapsed_ms(measurement_text, LOG_MARKER_PIPELINE_DONE),
        "incremental_done": parse_logged_elapsed_ms(measurement_text, LOG_MARKER_INCREMENTAL_DONE),
    }
    indexed_ms = indexed_work_elapsed_ms(logged_elapsed_ms)
    publish_reason = response_publish_reason(data)
    exact_route_detail = merge_exact_route_detail(
        parse_exact_route_detail(stderr), data, publish_kind, publish_reason
    )
    freshness = response_freshness(data)
    freshness_state = response_freshness_state(data)
    peak_candidates = [
        parse_log_max_int_field(measurement_text, marker, "peak_mb")
        for marker in ("mem.phase", LOG_MARKER_PIPELINE_DONE, LOG_MARKER_INCREMENTAL_DONE)
    ]
    peak_rss_mb = max((value for value in peak_candidates if value is not None), default=None)
    dependency_phase_ms = parse_log_int_field(
        measurement_text, LOG_MARKER_DEP_AUTO_INDEX, "ms"
    )
    rank_refresh_ms = parse_log_int_field(measurement_text, LOG_MARKER_RANK_REFRESH, "ms")
    worker_elapsed_ms = parse_log_int_field(
        measurement_text, LOG_MARKER_INDEX_WORKER_TOTAL, "ms"
    )
    known_elapsed_ms = worker_elapsed_ms
    if known_elapsed_ms is None:
        known_components = [
            value
            for value in (indexed_ms, dependency_phase_ms, rank_refresh_ms)
            if value is not None
        ]
        known_elapsed_ms = sum(known_components) if known_components else None
    process_overhead_ms = (
        max(0, elapsed_ms_int - known_elapsed_ms)
        if known_elapsed_ms is not None
        else None
    )
    dependencies_indexed = data.get("dependencies_indexed")
    dependency_packages = (
        dependencies_indexed
        if isinstance(dependencies_indexed, int) and dependencies_indexed >= 0
        else None
    )
    result: dict[str, Any] = {
        "elapsed_ms": elapsed_ms_int,
        "peak_rss_mb": peak_rss_mb,
        "measurement_log_markers": measurement_log_markers,
        "measurement_log_artifacts": measurement_log_artifacts,
        "indexed_work_elapsed_ms": indexed_ms,
        "worker_elapsed_ms": worker_elapsed_ms,
        "process_overhead_ms": process_overhead_ms,
        # Backwards-compatible field: now excludes every measured worker phase,
        # not just the main pipeline. Prefer process_overhead_ms in new reports.
        "unlogged_overhead_ms": process_overhead_ms,
        "timing_components_ms": {
            "main_index": indexed_ms,
            "dependency_index": dependency_phase_ms,
            "rank_refresh": rank_refresh_ms,
            "worker_total": worker_elapsed_ms,
            "cold_process_and_supervisor": process_overhead_ms,
        },
        "response": data,
        "publish_kind": publish_kind or None,
        "freshness_state": freshness_state or None,
        "freshness": freshness,
        "stdout_bytes": stdout_bytes,
        "dependency_indexing": {
            "measurement_status": (
                "measured"
                if dependency_phase_ms is not None or dependency_packages is not None
                else "unknown"
            ),
            "phase_elapsed_ms": dependency_phase_ms,
            "packages_indexed": dependency_packages,
        },
        "markers": {
            "incremental_exact_done": log_has(stderr, LOG_MARKER_EXACT_DONE)
            or publish_kind == PUBLISH_INCREMENTAL_EXACT,
            "incremental_done": log_has(stderr, LOG_MARKER_INCREMENTAL_DONE)
            or is_incremental_publish_kind(publish_kind),
            "pagerank_done": log_has(stderr, "pagerank.done"),
            "pagerank_defer": log_has(stderr, "pagerank.defer"),
            "full_route": log_has(stderr, "pipeline.route path=full")
            or publish_kind == PUBLISH_FULL,
            "incremental_route": log_has(stderr, "pipeline.route path=incremental")
            or is_incremental_publish_kind(publish_kind),
        },
        "logged_elapsed_ms": logged_elapsed_ms,
        "exact_reason": publish_reason or exact_route_detail.get("reason"),
        "exact_route_detail": exact_route_detail,
        "stderr_tail": log_tail(stderr),
    }
    if include_logs:
        result["stderr"] = stderr
    return result


def run_index(
    binary: Path,
    env: dict[str, str],
    repo_dir: Path,
    timeout: int,
    include_logs: bool,
    index_mode: str = "fast",
) -> dict[str, Any]:
    args = json.dumps(index_tool_arguments(repo_dir, index_mode))
    cmd = [str(binary), "cli", "--json", "index_repository", args]
    proc, elapsed_ms = command_result(
        cmd,
        env,
        timeout,
    )
    if proc.returncode != 0:
        raise command_failure("index_repository", cmd, env, proc, elapsed_ms)
    data = unwrap_cli_json(proc.stdout)
    return build_index_result(
        data, proc.stderr, len(proc.stdout.encode("utf-8")), elapsed_ms, include_logs
    )


def run_index_mcp(
    client: McpClient,
    repo_dir: Path,
    include_logs: bool,
    index_mode: str = "fast",
) -> dict[str, Any]:
    data, stderr, stdout_bytes, elapsed_ms = client.call_tool(
        "index_repository", index_tool_arguments(repo_dir, index_mode)
    )
    return build_index_result(data, stderr, stdout_bytes, elapsed_ms, include_logs)


def build_tool_probe_result(
    data: dict[str, Any],
    stderr: str,
    stdout_bytes: int,
    elapsed_ms: float,
    include_logs: bool,
) -> dict[str, Any]:
    elapsed_ms_value = round(elapsed_ms, 3)
    result: dict[str, Any] = {
        "elapsed_ms": elapsed_ms_value,
        "stdout_bytes": stdout_bytes,
        "response_keys": sorted(str(key) for key in data.keys()),
        "stderr_tail": log_tail(stderr),
    }
    if include_logs:
        result["stderr"] = stderr
    return result


def run_cli_tool_probe(
    binary: Path,
    env: dict[str, str],
    tool_name: str,
    timeout: int,
    include_logs: bool,
) -> dict[str, Any]:
    cmd = [str(binary), "cli", "--json", tool_name, "{}"]
    proc, elapsed_ms = command_result(cmd, env, timeout)
    if proc.returncode != 0:
        raise command_failure(f"{tool_name}_probe", cmd, env, proc, elapsed_ms)
    data = unwrap_cli_json(proc.stdout)
    return build_tool_probe_result(
        data, proc.stderr, len(proc.stdout.encode("utf-8")), elapsed_ms, include_logs
    )


def run_mcp_tool_probe(
    client: McpClient,
    tool_name: str,
    include_logs: bool,
) -> dict[str, Any]:
    data, stderr, stdout_bytes, elapsed_ms = client.call_tool(tool_name, {})
    return build_tool_probe_result(data, stderr, stdout_bytes, elapsed_ms, include_logs)


def build_tool_call_result(
    data: dict[str, Any],
    stderr: str,
    stdout_bytes: int,
    elapsed_ms: float,
    include_logs: bool,
    response_payload: bytes | None = None,
) -> dict[str, Any]:
    quality_payload = canonical_response_bytes(data)
    payload = response_payload if response_payload is not None else quality_payload
    result: dict[str, Any] = {
        "elapsed_ms": round(elapsed_ms, 3),
        # Preserve the historical field while separating transport framing from
        # the canonical payload used for cross-transport comparisons.
        "stdout_bytes": stdout_bytes,
        "transport_response_bytes": stdout_bytes,
        "response_bytes": len(payload),
        "response_token_estimate": estimate_response_tokens(payload),
        "token_estimator": TOKEN_ESTIMATOR,
        "response_encoding": "tool_default" if response_payload is not None else "canonical_json",
        "quality_response_bytes": len(quality_payload),
        "response": data,
        "freshness_state": response_freshness_state(data) or None,
        "freshness": response_freshness(data),
        "stderr_tail": log_tail(stderr),
    }
    if include_logs:
        result["stderr"] = stderr
    return result


def run_cli_tool_call(
    binary: Path,
    env: dict[str, str],
    tool_name: str,
    arguments: dict[str, Any],
    timeout: int,
    include_logs: bool,
) -> dict[str, Any]:
    encoded = json.dumps(arguments, separators=(",", ":"))
    cmd = [str(binary), "cli", "--json", tool_name, encoded]
    proc, elapsed_ms = command_result(cmd, env, timeout)
    if proc.returncode != 0:
        raise command_failure(f"{tool_name}_call", cmd, env, proc, elapsed_ms)
    raw_payload = cli_result_text(proc.stdout).encode("utf-8")
    quality_arguments = dict(arguments)
    quality_arguments["format"] = "json"
    quality_cmd = [
        str(binary), "cli", "--json", tool_name,
        json.dumps(quality_arguments, separators=(",", ":")),
    ]
    quality_proc, quality_elapsed_ms = command_result(quality_cmd, env, timeout)
    if quality_proc.returncode != 0:
        raise command_failure(
            f"{tool_name}_quality_call", quality_cmd, env, quality_proc, quality_elapsed_ms
        )
    data = unwrap_cli_json(quality_proc.stdout)
    result = build_tool_call_result(
        data, proc.stderr, len(proc.stdout.encode("utf-8")), elapsed_ms, include_logs, raw_payload
    )
    result["quality_probe_elapsed_ms"] = round(quality_elapsed_ms, 3)
    return result


def run_mcp_tool_call(
    client: McpClient,
    tool_name: str,
    arguments: dict[str, Any],
    include_logs: bool,
) -> dict[str, Any]:
    raw_text, stderr, stdout_bytes, elapsed_ms = client.call_tool_text(tool_name, arguments)
    quality_arguments = dict(arguments)
    quality_arguments["format"] = "json"
    data, _, _, quality_elapsed_ms = client.call_tool(tool_name, quality_arguments)
    result = build_tool_call_result(
        data, stderr, stdout_bytes, elapsed_ms, include_logs, raw_text.encode("utf-8")
    )
    result["quality_probe_elapsed_ms"] = round(quality_elapsed_ms, 3)
    return result


def run_tool_call_for_transport(
    transport: str,
    binary: Path,
    env: dict[str, str],
    tool_name: str,
    arguments: dict[str, Any],
    timeout: int,
    include_logs: bool,
    client: McpClient | None = None,
) -> dict[str, Any]:
    if transport == "mcp":
        if client is None:
            raise RuntimeError("MCP transport requires an active client")
        return run_mcp_tool_call(client, tool_name, arguments, include_logs)
    return run_cli_tool_call(binary, env, tool_name, arguments, timeout, include_logs)


def summarize_elapsed_ms(probes: list[dict[str, Any]]) -> dict[str, Any]:
    elapsed = sorted(float(probe["elapsed_ms"]) for probe in probes)
    if not elapsed:
        return {"count": 0}
    return {
        "count": len(elapsed),
        "min_ms": elapsed[0],
        "median_ms": elapsed[len(elapsed) // 2],
        "max_ms": elapsed[-1],
    }


def measure_cli_overhead_probes(
    binary: Path,
    env: dict[str, str],
    tool_name: str,
    count: int,
    timeout: int,
    include_logs: bool,
) -> dict[str, Any] | None:
    if count <= 0:
        return None
    probes = [
        run_cli_tool_probe(binary, env, tool_name, timeout, include_logs)
        for _ in range(count)
    ]
    return {"tool": tool_name, "trials": probes, "summary": summarize_elapsed_ms(probes)}


def measure_mcp_overhead_probes(
    client: McpClient,
    tool_name: str,
    count: int,
    include_logs: bool,
) -> dict[str, Any] | None:
    if count <= 0:
        return None
    probes = [run_mcp_tool_probe(client, tool_name, include_logs) for _ in range(count)]
    return {"tool": tool_name, "trials": probes, "summary": summarize_elapsed_ms(probes)}


def remove_project_dbs(cache_dir: Path) -> list[str]:
    removed: list[str] = []
    for path in cache_dir.iterdir():
        if not path.is_file():
            continue
        if path.name == CONFIG_DB_NAME or not path.name.endswith(PROJECT_DB_SUFFIX):
            continue
        path.unlink()
        removed.append(path.name)
        for suffix in ("-wal", "-shm"):
            sidecar = cache_dir / f"{path.name}{suffix}"
            if sidecar.exists():
                sidecar.unlink()
                removed.append(sidecar.name)
    return removed


def find_project_db(cache_dir: Path) -> Path:
    dbs = sorted(
        path
        for path in cache_dir.iterdir()
        if path.is_file() and path.name != CONFIG_DB_NAME and path.name.endswith(PROJECT_DB_SUFFIX)
    )
    if len(dbs) != 1:
        names = ", ".join(path.name for path in dbs)
        raise RuntimeError(f"expected one project DB in {cache_dir}, found {len(dbs)}: {names}")
    return dbs[0]


def remove_sqlite_sidecars(path: Path) -> None:
    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{path}{suffix}")
        if sidecar.exists():
            sidecar.unlink()


def copy_sqlite_snapshot(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        destination.unlink()
    remove_sqlite_sidecars(destination)
    uri = f"{source.resolve().as_uri()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as src, sqlite3.connect(str(destination)) as dst:
        src.backup(dst)


def clone_list_project_db(source: Path, destination: Path, project: str, root_path: str) -> None:
    """Clone one valid project DB and rekey rows used by list_projects."""
    copy_sqlite_snapshot(source, destination)
    with sqlite3.connect(str(destination)) as con:
        project_rows = con.execute("SELECT name FROM projects").fetchall()
        if len(project_rows) != 1:
            raise RuntimeError(
                f"list-project fixture seed must contain one project, found {len(project_rows)}"
            )
        old_project = str(project_rows[0][0])
        con.execute(
            "UPDATE projects SET name = ?, root_path = ? WHERE name = ?",
            (project, root_path, old_project),
        )
        con.execute("UPDATE nodes SET project = ? WHERE project = ?", (project, old_project))
        con.execute("UPDATE edges SET project = ? WHERE project = ?", (project, old_project))


def decode_sqlite_text(data: bytes) -> str:
    return data.decode("utf-8", "surrogateescape")


def sqlite_cbm_source_span_label(label: str | None) -> int:
    return int(label in SOURCE_SPAN_LABELS)


def query_rows(db_path: Path, sql: str, params: tuple[Any, ...]) -> list[str]:
    con = sqlite3.connect(str(db_path))
    con.text_factory = decode_sqlite_text
    con.create_function("cbm_source_span_label", 1, sqlite_cbm_source_span_label)
    try:
        rows = [str(row[0]) for row in con.execute(sql, params)]
    finally:
        con.close()
    return rows


def canonical_query_rows(db_path: Path, project: str, sql: str) -> list[str]:
    return query_rows(db_path, sql, (project,))


def stream_query_fingerprint(
    db_path: Path, sql: str, params: tuple[Any, ...]
) -> dict[str, Any]:
    """Hash an ordered single-column query with O(1) Python memory."""
    digest = hashlib.sha256()
    row_count = 0
    con = sqlite3.connect(str(db_path))
    con.text_factory = decode_sqlite_text
    con.create_function("cbm_source_span_label", 1, sqlite_cbm_source_span_label)
    try:
        for row in con.execute(sql, params):
            value = row[0]
            payload = (
                value
                if isinstance(value, bytes)
                else str(value).encode("utf-8", "surrogateescape")
            )
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
            row_count += 1
    finally:
        con.close()
    return {"row_count": row_count, "sha256": digest.hexdigest()}


def first_sorted_query_difference(
    left_db: Path,
    right_db: Path,
    left_sql: str,
    left_params: tuple[Any, ...],
    right_sql: str,
    right_params: tuple[Any, ...],
) -> tuple[str | None, str | None]:
    """Return the first merge difference from two ordered queries in O(1) memory."""
    left_con = sqlite3.connect(str(left_db))
    right_con = sqlite3.connect(str(right_db))
    for con in (left_con, right_con):
        con.text_factory = decode_sqlite_text
        con.create_function("cbm_source_span_label", 1, sqlite_cbm_source_span_label)
    try:
        left_rows = iter(left_con.execute(left_sql, left_params))
        right_rows = iter(right_con.execute(right_sql, right_params))
        left = next(left_rows, None)
        right = next(right_rows, None)
        while left is not None and right is not None:
            left_value = str(left[0])
            right_value = str(right[0])
            if left_value == right_value:
                left = next(left_rows, None)
                right = next(right_rows, None)
            elif left_value < right_value:
                return left_value, None
            else:
                return None, right_value
        return (
            str(left[0]) if left is not None else None,
            str(right[0]) if right is not None else None,
        )
    finally:
        left_con.close()
        right_con.close()


def compare_query_rows(
    left_db: Path,
    right_db: Path,
    kind: str,
    left_sql: str,
    left_params: tuple[Any, ...],
    right_sql: str,
    right_params: tuple[Any, ...],
) -> dict[str, Any]:
    left = stream_query_fingerprint(left_db, left_sql, left_params)
    right = stream_query_fingerprint(right_db, right_sql, right_params)
    if left != right:
        left_only, right_only = first_sorted_query_difference(
            left_db, right_db, left_sql, left_params, right_sql, right_params
        )
        return {
            "equal": False,
            "kind": kind,
            "left_count": left["row_count"],
            "right_count": right["row_count"],
            "left_sha256": left["sha256"],
            "right_sha256": right["sha256"],
            "left_only": left_only,
            "right_only": right_only,
        }
    return {"equal": True, "row_count": left["row_count"], "sha256": left["sha256"]}


CANONICAL_NODES_SQL = (
    "SELECT quote(label) || char(9) || quote(name) || char(9) || "
    "quote(qualified_name) || char(9) || quote(coalesce(file_path,'')) || char(9) || "
    "start_line || char(9) || end_line || char(9) || "
    "COALESCE((SELECT group_concat(item, char(30)) FROM ("
    "SELECT quote(je.key) || '=' || je.type || '=' || "
    "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
    "FROM json_each(n.properties) AS je "
    "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
    ")), '') "
    "FROM nodes n WHERE project = ?1 "
    "ORDER BY label, name, qualified_name, coalesce(file_path,''), start_line, end_line, "
    "COALESCE((SELECT group_concat(item, char(30)) FROM ("
    "SELECT quote(je.key) || '=' || je.type || '=' || "
    "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
    "FROM json_each(n.properties) AS je "
    "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
    ")), '')"
)

def build_canonical_edges_sql(edge_predicate: str = "") -> str:
    return (
        "SELECT quote(s.label) || char(9) || quote(s.qualified_name) || char(9) || "
        "quote(coalesce(s.file_path,'')) || char(9) || s.start_line || char(9) || "
        "s.end_line || char(9) || quote(t.label) || char(9) || quote(t.qualified_name) || "
        "char(9) || quote(coalesce(t.file_path,'')) || char(9) || t.start_line || char(9) || "
        "t.end_line || char(9) || quote(e.type) || char(9) || "
        "COALESCE((SELECT group_concat(item, char(30)) FROM ("
        "SELECT quote(je.key) || '=' || je.type || '=' || "
        "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
        "FROM json_each(e.properties) AS je "
        "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
        ")), '') "
        "FROM edges e "
        "JOIN nodes s ON s.id = e.source_id "
        "JOIN nodes t ON t.id = e.target_id "
        f"WHERE e.project = ?1 {edge_predicate}"
        "ORDER BY s.label, s.qualified_name, coalesce(s.file_path,''), s.start_line, s.end_line, "
        "t.label, t.qualified_name, coalesce(t.file_path,''), t.start_line, t.end_line, "
        "e.type, COALESCE((SELECT group_concat(item, char(30)) FROM ("
        "SELECT quote(je.key) || '=' || je.type || '=' || "
        "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
        "FROM json_each(e.properties) AS je "
        "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
        ")), '')"
    )


CANONICAL_EDGES_SQL = build_canonical_edges_sql()
CANONICAL_EDGES_WITHOUT_SEMANTIC_SQL = build_canonical_edges_sql(
    "AND e.type <> 'SEMANTICALLY_RELATED' "
)

CANONICAL_HASHES_SQL = (
    "SELECT quote(rel_path) || char(9) || quote(sha256) || char(9) || mtime_ns || char(9) || "
    "size FROM file_hashes WHERE project = ?1 ORDER BY rel_path"
)

CONTENT_HASHES_SQL = (
    "SELECT quote(rel_path) || char(9) || quote(sha256) || char(9) || size "
    "FROM file_hashes WHERE project = ?1 ORDER BY rel_path"
)

STABLE_NODES_SQL = (
    "SELECT quote(label) || char(9) || "
    "quote(CASE WHEN label = 'Project' AND name = ?1 THEN '<project>' ELSE name END) || char(9) || "
    "quote(CASE WHEN qualified_name = ?1 THEN '<project>' "
    "WHEN substr(qualified_name, 1, length(?1) + 1) = ?1 || '.' "
    "THEN substr(qualified_name, length(?1) + 2) ELSE qualified_name END) || char(9) || "
    "quote(coalesce(file_path,'')) || char(9) || start_line || char(9) || end_line "
    "FROM nodes WHERE project = ?1 ORDER BY 1"
)

STABLE_EDGES_SQL = (
    "SELECT quote(CASE WHEN s.qualified_name = ?1 THEN '<project>' "
    "WHEN substr(s.qualified_name, 1, length(?1) + 1) = ?1 || '.' "
    "THEN substr(s.qualified_name, length(?1) + 2) ELSE s.qualified_name END) || char(9) || "
    "quote(CASE WHEN t.qualified_name = ?1 THEN '<project>' "
    "WHEN substr(t.qualified_name, 1, length(?1) + 1) = ?1 || '.' "
    "THEN substr(t.qualified_name, length(?1) + 2) ELSE t.qualified_name END) || char(9) || "
    "quote(e.type) FROM edges e "
    "JOIN nodes s ON s.id = e.source_id JOIN nodes t ON t.id = e.target_id "
    "WHERE e.project = ?1 ORDER BY 1"
)

STABLE_SEMANTIC_SCORES_SQL = (
    "SELECT quote(CASE WHEN s.qualified_name = ?1 THEN '<project>' "
    "WHEN substr(s.qualified_name, 1, length(?1) + 1) = ?1 || '.' "
    "THEN substr(s.qualified_name, length(?1) + 2) ELSE s.qualified_name END) || char(9) || "
    "quote(CASE WHEN t.qualified_name = ?1 THEN '<project>' "
    "WHEN substr(t.qualified_name, 1, length(?1) + 1) = ?1 || '.' "
    "THEN substr(t.qualified_name, length(?1) + 2) ELSE t.qualified_name END) || char(9) || "
    "quote(e.type) || char(9) || "
    "coalesce(quote(CAST(json_extract(e.properties, '$.score') AS TEXT)), 'NULL') || char(9) || "
    "coalesce(quote(CAST(json_extract(e.properties, '$.jaccard') AS TEXT)), 'NULL') "
    "FROM edges e JOIN nodes s ON s.id = e.source_id JOIN nodes t ON t.id = e.target_id "
    "WHERE e.project = ?1 AND e.type IN ('SIMILAR_TO','SEMANTICALLY_RELATED') ORDER BY 1"
)

ACTIVE_OVERLAY_CTE_SQL = (
    "WITH active_overlay_files AS ("
    "  SELECT project, rel_path, MAX(overlay_generation) AS overlay_generation"
    "  FROM ("
    "    SELECT n.project, n.rel_path, n.overlay_generation"
    "    FROM overlay_nodes n"
    "    JOIN overlay_generations g"
    "      ON g.project = n.project AND g.overlay_generation = n.overlay_generation"
    "    WHERE g.status = ?1 AND n.project = ?4"
    "    UNION"
    "    SELECT e.project, e.rel_path, e.overlay_generation"
    "    FROM overlay_edges e"
    "    JOIN overlay_generations g"
    "      ON g.project = e.project AND g.overlay_generation = e.overlay_generation"
    "    WHERE g.status = ?1 AND e.project = ?4"
    "    UNION"
    "    SELECT t.project, t.rel_path, t.overlay_generation"
    "    FROM overlay_tombstones t"
    "    JOIN overlay_generations g"
    "      ON g.project = t.project AND g.overlay_generation = t.overlay_generation"
    "    WHERE g.status = ?1 AND t.active = ?3 AND t.project = ?4"
    "  ) overlay_files"
    "  GROUP BY project, rel_path"
    "), active_file_tombstones AS ("
    "  SELECT t.project, t.rel_path, MAX(t.overlay_generation) AS overlay_generation"
    "  FROM overlay_tombstones t"
    "  JOIN overlay_generations g"
    "    ON g.project = t.project AND g.overlay_generation = t.overlay_generation"
    "  WHERE g.status = ?1 AND t.entity_kind = ?2 AND t.active = ?3 AND t.project = ?4"
    "  GROUP BY t.project, t.rel_path"
    "), active_node_candidates AS ("
    "  SELECT 0 AS overlay_row, n.project, n.label, n.name, n.qualified_name, n.file_path,"
    "         n.start_line, n.end_line, n.properties"
    "  FROM nodes n"
    "  WHERE n.project = ?4"
    "    AND NOT EXISTS (SELECT 1 FROM active_file_tombstones af"
    "                    WHERE af.project = n.project AND af.rel_path = n.file_path)"
    "  UNION ALL"
    "  SELECT 1 AS overlay_row, n.project, n.label, n.name, n.qualified_name, n.file_path,"
    "         n.start_line, n.end_line, n.properties"
    "  FROM overlay_nodes n"
    "  JOIN active_overlay_files af"
    "    ON af.project = n.project AND af.rel_path = n.rel_path"
    "   AND af.overlay_generation = n.overlay_generation"
    "  WHERE n.owned = ?5"
    "), active_nodes AS ("
    "  SELECT project, label, name, qualified_name, file_path, start_line, end_line, properties"
    "  FROM ("
    "    SELECT c.*, ROW_NUMBER() OVER ("
    "      PARTITION BY c.project, c.qualified_name"
    "      ORDER BY cbm_source_span_label(c.label) DESC,"
    "               CASE WHEN cbm_source_span_label(c.label) = 1"
    "                    THEN CASE WHEN c.file_path <> '' THEN 1 ELSE 0 END"
    "                    ELSE c.overlay_row END DESC,"
    "               CASE WHEN cbm_source_span_label(c.label) = 1"
    "                         AND c.start_line > 0 AND c.end_line >= c.start_line"
    "                    THEN c.end_line - c.start_line + 1 ELSE 0 END DESC,"
    "               CASE WHEN cbm_source_span_label(c.label) = 1 THEN c.start_line ELSE 0 END ASC,"
    "               CASE WHEN cbm_source_span_label(c.label) = 1 THEN c.end_line ELSE 0 END DESC,"
    "               CASE WHEN cbm_source_span_label(c.label) = 1 THEN c.file_path ELSE '' END ASC,"
    "               c.overlay_row DESC"
    "    ) AS rn"
    "    FROM active_node_candidates c"
    "  ) ranked_nodes"
    "  WHERE rn = 1"
    "), active_edges AS ("
    "  SELECT e.project, s.qualified_name AS source_qn, t.qualified_name AS target_qn,"
    "         e.type, e.properties"
    "  FROM edges e"
    "  JOIN nodes s ON s.id = e.source_id"
    "  JOIN nodes t ON t.id = e.target_id"
    "  WHERE e.project = ?4"
    "    AND NOT EXISTS (SELECT 1 FROM active_file_tombstones af"
    "                    WHERE af.project = s.project AND af.rel_path = s.file_path)"
    "    AND NOT EXISTS (SELECT 1 FROM active_file_tombstones af"
    "                    WHERE af.project = t.project AND af.rel_path = t.file_path)"
    "  UNION"
    "  SELECT e.project, e.source_qn, e.target_qn, e.type, e.properties"
    "  FROM overlay_edges e"
    "  JOIN active_overlay_files af"
    "    ON af.project = e.project AND af.rel_path = e.rel_path"
    "   AND af.overlay_generation = e.overlay_generation"
    "  WHERE e.owned = ?5"
    ") "
)

ACTIVE_OVERLAY_NODES_SQL = (
    ACTIVE_OVERLAY_CTE_SQL
    + "SELECT quote(label) || char(9) || quote(name) || char(9) || "
    "quote(qualified_name) || char(9) || quote(coalesce(file_path,'')) || char(9) || "
    "start_line || char(9) || end_line || char(9) || "
    "COALESCE((SELECT group_concat(item, char(30)) FROM ("
    "SELECT quote(je.key) || '=' || je.type || '=' || "
    "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
    "FROM json_each(n.properties) AS je "
    "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
    ")), '') "
    "FROM active_nodes n WHERE project = ?4 "
    "ORDER BY label, name, qualified_name, coalesce(file_path,''), start_line, end_line, "
    "COALESCE((SELECT group_concat(item, char(30)) FROM ("
    "SELECT quote(je.key) || '=' || je.type || '=' || "
    "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
    "FROM json_each(n.properties) AS je "
    "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
    ")), '')"
)

ACTIVE_OVERLAY_EDGES_SQL = (
    ACTIVE_OVERLAY_CTE_SQL
    + "SELECT quote(s.label) || char(9) || quote(s.qualified_name) || char(9) || "
    "quote(coalesce(s.file_path,'')) || char(9) || s.start_line || char(9) || "
    "s.end_line || char(9) || quote(t.label) || char(9) || quote(t.qualified_name) || "
    "char(9) || quote(coalesce(t.file_path,'')) || char(9) || t.start_line || char(9) || "
    "t.end_line || char(9) || quote(e.type) || char(9) || "
    "COALESCE((SELECT group_concat(item, char(30)) FROM ("
    "SELECT quote(je.key) || '=' || je.type || '=' || "
    "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
    "FROM json_each(e.properties) AS je "
    "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
    ")), '') "
    "FROM active_edges e "
    "JOIN active_nodes s ON s.project = e.project AND s.qualified_name = e.source_qn "
    "JOIN active_nodes t ON t.project = e.project AND t.qualified_name = e.target_qn "
    "WHERE e.project = ?4 "
    "ORDER BY s.label, s.qualified_name, coalesce(s.file_path,''), s.start_line, s.end_line, "
    "t.label, t.qualified_name, coalesce(t.file_path,''), t.start_line, t.end_line, "
    "e.type, COALESCE((SELECT group_concat(item, char(30)) FROM ("
    "SELECT quote(je.key) || '=' || je.type || '=' || "
    "COALESCE(quote(CAST(je.value AS TEXT)), 'NULL') AS item "
    "FROM json_each(e.properties) AS je "
    "ORDER BY je.key, je.type, CAST(je.value AS TEXT)"
    ")), '')"
)


def stable_graph_fingerprint(db_path: Path, project: str) -> dict[str, Any]:
    """Return path-normalized experiment identity with O(1) Python memory."""
    components: dict[str, dict[str, Any]] = {}
    aggregate = hashlib.sha256()
    for name, sql in (
        ("nodes", STABLE_NODES_SQL),
        ("edges", STABLE_EDGES_SQL),
        ("semantic_scores", STABLE_SEMANTIC_SCORES_SQL),
        ("source_files", CONTENT_HASHES_SQL),
    ):
        fingerprint = stream_query_fingerprint(db_path, sql, (project,))
        components[name] = fingerprint
        name_payload = name.encode("ascii")
        aggregate.update(len(name_payload).to_bytes(8, "big"))
        aggregate.update(name_payload)
        aggregate.update(fingerprint["row_count"].to_bytes(8, "big"))
        aggregate.update(bytes.fromhex(fingerprint["sha256"]))
    return {"sha256": aggregate.hexdigest(), "components": components}


def compare_canonical_graph(left_db: Path, right_db: Path, project: str) -> dict[str, Any]:
    for kind, sql in (
        ("canonical nodes", CANONICAL_NODES_SQL),
        ("canonical edges", CANONICAL_EDGES_SQL),
        ("file hashes", CANONICAL_HASHES_SQL),
    ):
        result = compare_query_rows(left_db, right_db, kind, sql, (project,), sql, (project,))
        if not result["equal"]:
            return result
    return {"equal": True}


def compare_graph_excluding_declared_stale_views(
    left_db: Path,
    right_db: Path,
    project: str,
    stale_views: list[str],
) -> dict[str, Any] | None:
    """Verify strict graph equality after excluding only explicitly stale derived rows."""
    if "semantic_edges" not in stale_views:
        return None
    excluded_edge_types = ["SEMANTICALLY_RELATED"]
    for kind, sql in (
        ("canonical nodes excluding declared stale views", CANONICAL_NODES_SQL),
        (
            "canonical edges excluding declared stale views",
            CANONICAL_EDGES_WITHOUT_SEMANTIC_SQL,
        ),
        ("file hashes excluding declared stale views", CANONICAL_HASHES_SQL),
    ):
        result = compare_query_rows(
            left_db, right_db, kind, sql, (project,), sql, (project,)
        )
        if not result["equal"]:
            return {
                **result,
                "declared_stale_views": stale_views,
                "excluded_edge_types": excluded_edge_types,
            }
    return {
        "equal": True,
        "declared_stale_views": stale_views,
        "excluded_edge_types": excluded_edge_types,
    }


def compare_active_overlay_graph(left_db: Path, right_db: Path, project: str) -> dict[str, Any]:
    left_params = (
        OVERLAY_STATUS_READY,
        OVERLAY_TOMBSTONE_FILE,
        OVERLAY_TOMBSTONE_ACTIVE,
        project,
        OVERLAY_ROW_OWNED,
    )
    for kind, left_sql, right_sql in (
        ("active overlay nodes", ACTIVE_OVERLAY_NODES_SQL, CANONICAL_NODES_SQL),
        ("active overlay edges", ACTIVE_OVERLAY_EDGES_SQL, CANONICAL_EDGES_SQL),
    ):
        result = compare_query_rows(
            left_db, right_db, kind, left_sql, left_params, right_sql, (project,)
        )
        if not result["equal"]:
            return result
    return {"equal": True}


def graph_gate_for_publish_kind(
    canonical: dict[str, Any],
    publish_kind: str | None,
    oracle_passed: bool | None = None,
    active_overlay: dict[str, Any] | None = None,
    freshness_scoped: dict[str, Any] | None = None,
) -> dict[str, Any]:
    canonical_equal = bool(canonical.get("equal"))
    active_overlay_equal = bool(active_overlay and active_overlay.get("equal"))
    if publish_kind == PUBLISH_INCREMENTAL_OVERLAY and active_overlay is not None:
        return {
            "passed": active_overlay_equal,
            "policy": "overlay_active_graph",
            "canonical_equal": canonical_equal,
            "active_overlay_equal": active_overlay_equal,
            "reason": (
                "overlay publish leaves canonical rows unchanged; validate active overlay "
                "nodes and edges against a fresh full graph"
            ),
        }
    if publish_kind == PUBLISH_INCREMENTAL_OVERLAY and oracle_passed is not None:
        return {
            "passed": bool(oracle_passed),
            "policy": "overlay_active_oracles",
            "canonical_equal": canonical_equal,
            "reason": (
                "overlay publish leaves canonical rows unchanged; self-dogfood gates "
                "on active read oracles and freshness metadata"
            ),
        }
    if freshness_scoped is not None and freshness_scoped.get("equal") is True:
        return {
            "passed": True,
            "policy": "declared_stale_derived_views",
            "canonical_equal": canonical_equal,
            "freshness_scoped_equal": True,
            "declared_stale_views": freshness_scoped.get("declared_stale_views", []),
            "excluded_edge_types": freshness_scoped.get("excluded_edge_types", []),
            "reason": (
                "the full graph intentionally retains declared-stale derived rows; "
                "all non-stale canonical rows equal the fresh graph"
            ),
        }
    return {
        "passed": canonical_equal,
        "policy": "canonical_graph",
        "canonical_equal": canonical_equal,
    }


def frontier_coverage_gate(
    scenario_metadata: dict[str, Any],
    incremental: dict[str, Any],
    exact_cap: int | None = None,
) -> dict[str, Any]:
    expected_publish_kind = scenario_metadata.get("expected_publish_kind")
    expected_reason = scenario_metadata.get("expected_reason")
    if isinstance(expected_publish_kind, str) and isinstance(expected_reason, str):
        observed_publish_kind = incremental.get("publish_kind")
        observed_reason = incremental.get("exact_reason")
        passed = observed_publish_kind == expected_publish_kind and observed_reason == expected_reason
        result = {
            "passed": passed,
            "applicable": True,
            "contract": "safe_full_rebuild",
            "expected_publish_kind": expected_publish_kind,
            "observed_publish_kind": observed_publish_kind,
            "expected_reason": expected_reason,
            "observed_reason": observed_reason,
        }
        if not passed:
            result["reason"] = "observed fallback route does not match the fixture contract"
        return result
    expected = scenario_metadata.get("expected_minimum_affected_files")
    if not isinstance(expected, int):
        return {"passed": True, "applicable": False}
    exact_delta = incremental.get("response", {}).get("exact_delta", {})
    observed = exact_delta.get("affected_paths")
    if not isinstance(observed, int):
        observed = incremental.get("exact_route_detail", {}).get("frontier_expanded_files")
    if isinstance(exact_cap, int) and exact_cap < expected:
        observed_publish_kind = incremental.get("publish_kind")
        observed_reason = incremental.get("exact_reason")
        truncated = exact_delta.get("affected_paths_truncated") is True
        passed = (
            observed_publish_kind in {PUBLISH_FULL, PUBLISH_INCREMENTAL_CONTAINMENT}
            and observed_reason == "frontier_too_large"
            and truncated
        )
        result = {
            "passed": passed,
            "applicable": True,
            "contract": "configured_cap_fallback",
            "configured_exact_cap": exact_cap,
            "expected_minimum_affected_files": expected,
            "observed_affected_files": observed,
            "observed_publish_kind": observed_publish_kind,
            "observed_reason": observed_reason,
            "affected_paths_truncated": truncated,
        }
        if not passed:
            result["reason"] = (
                "configured cap fallback requires containment/full publication, "
                "frontier_too_large, and truncation evidence"
            )
        return result
    passed = isinstance(observed, int) and observed >= expected
    result = {
        "passed": passed,
        "applicable": True,
        "contract": "exact_frontier",
        "expected_minimum_affected_files": expected,
        "observed_affected_files": observed,
    }
    if not passed:
        result["reason"] = "observed frontier is smaller than the fixture contract"
    return result


def build_env(cache_dir: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["CBM_CACHE_DIR"] = str(cache_dir)
    env["CBM_AUTO_INDEX"] = "false"
    env["CBM_CONTEXT_INJECTION"] = "false"
    # The supervisor retains successful worker logs only in profile mode. The
    # harness streams their exact memory/timing markers before cleaning the cache.
    env["CBM_PROFILE"] = "1"
    return env


def prepare_matrix_scenario(
    name: str,
    repo_dir: Path,
    files: int,
    funcs_per_file: int,
    args: argparse.Namespace,
    case_root: Path,
) -> dict[str, Any]:
    frontier_language = MATRIX_FRONTIER_SCENARIOS.get(name)
    if frontier_language:
        return create_inbound_frontier_repo(repo_dir, frontier_language, args.frontier_files)
    if name in {
        "go_modify_1",
        "go_modify_2",
        "go_create",
        "go_delete",
        "go_rename",
        "go_new_folder",
    }:
        create_repo(repo_dir, files, funcs_per_file)
        return {"source": "synthetic_go"}
    if name == "route_decorator":
        create_route_repo(repo_dir, "/api/orders")
        return {"source": "synthetic_route"}
    if name == "python_reexport":
        create_python_reexport_repo(repo_dir)
        return {"source": "synthetic_python_reexport"}
    if name == "fastapi_insert_probe":
        return copy_fastapi_head_to_case(args, repo_dir, case_root)
    raise ValueError(f"unknown matrix scenario: {name}")


def mutate_matrix_scenario(name: str, repo_dir: Path, funcs_per_file: int) -> list[str]:
    frontier_language = MATRIX_FRONTIER_SCENARIOS.get(name)
    if frontier_language:
        return mutate_inbound_frontier_repo(repo_dir, frontier_language)
    if name == "go_modify_1":
        return modify_existing_files(repo_dir, 1, funcs_per_file)
    if name == "go_modify_2":
        return modify_existing_files(repo_dir, 2, funcs_per_file)
    if name == "go_create":
        rel = Path("pkg") / "file_created.go"
        write_text(repo_dir / rel, go_file_content(9999, 1, funcs_per_file))
        return [rel.as_posix()]
    if name == "go_delete":
        rel = Path("pkg") / "file_0000.go"
        (repo_dir / rel).unlink()
        return [rel.as_posix()]
    if name == "go_rename":
        old_rel = Path("pkg") / "file_0000.go"
        new_rel = Path("pkg") / "file_renamed.go"
        (repo_dir / old_rel).unlink()
        write_text(repo_dir / new_rel, go_file_content(9998, 1, funcs_per_file))
        return [old_rel.as_posix(), new_rel.as_posix()]
    if name == "go_new_folder":
        rel = Path("newpkg") / "leaf.go"
        write_text(repo_dir / rel, "package newpkg\n\nfunc NewFolderLeaf() int {\n\treturn 23\n}\n")
        return [rel.as_posix()]
    if name == "route_decorator":
        create_route_repo(repo_dir, "/api/items")
        return ["routes.py"]
    if name == "python_reexport":
        rel = Path("fastapi") / "__init__.py"
        write_text(repo_dir / rel, "from .openapi.models import Header\n")
        return [rel.as_posix()]
    if name == "fastapi_insert_probe":
        rel = Path(FASTAPI_PROBE_REL_PATH)
        path = repo_dir / rel
        source = path.read_text(encoding="utf-8")
        insert = (
            "\n"
            "    def cbm_frontier_noop_mask_probe(self) -> int:\n"
            f"        return {FASTAPI_PROBE_RETURN_VALUE}\n"
        )
        if FASTAPI_PROBE_INSERT_BEFORE not in source:
            raise RuntimeError(f"FastAPI probe insertion point not found: {rel.as_posix()}")
        mutated = source.replace(FASTAPI_PROBE_INSERT_BEFORE, insert + FASTAPI_PROBE_INSERT_BEFORE, 1)
        try:
            compile(mutated, rel.as_posix(), "exec")
        except SyntaxError as exc:
            raise RuntimeError(f"FastAPI probe mutation produced invalid Python: {exc}") from exc
        path.write_text(mutated, encoding="utf-8")
        return [rel.as_posix()]
    raise ValueError(f"unknown matrix scenario: {name}")


def resolve_git_repo_root(repo_root: Path, timeout: int) -> Path:
    root = repo_root.expanduser().resolve()
    return Path(command_stdout(["git", "rev-parse", "--show-toplevel"], timeout, root)).resolve()


def git_metadata(repo_root: Path, timeout: int) -> dict[str, Any]:
    def maybe(args: list[str]) -> str:
        try:
            return command_stdout(["git", *args], timeout, repo_root)
        except Exception as exc:  # noqa: BLE001 - metadata should not abort benchmark execution.
            return f"<error: {type(exc).__name__}: {exc}>"

    return {
        "repo_root": str(repo_root),
        "head": maybe(["rev-parse", "HEAD"]),
        "short_head": maybe(["rev-parse", "--short", "HEAD"]),
        "branch": maybe(["branch", "--show-current"]),
        "dirty_status_short": maybe(["status", "--short"]),
    }


def binary_metadata(binary: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with binary.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    stat = binary.stat()
    return {
        "path": str(binary.resolve()),
        "size_bytes": stat.st_size,
        "sha256": digest.hexdigest(),
    }


def clone_real_repo(url: str, target: Path, timeout: int) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    proc, _ = command_result(
        ["git", "clone", "--depth=1", url, str(target)],
        dict(os.environ),
        timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"git clone failed for {url}: {proc.stderr.strip()}")
    return target


def resolve_fastapi_source(args: argparse.Namespace, case_root: Path) -> Path:
    candidates: list[Path] = []
    if args.fastapi_repo:
        candidates.append(Path(args.fastapi_repo).expanduser())
    env_repo = os.environ.get("CBM_FASTAPI_REPO")
    if env_repo:
        candidates.append(Path(env_repo).expanduser())
    candidates.extend(
        [
            Path.home() / "source" / "fastapi",
            Path.home() / ".cache" / "codebase-memory-mcp" / "bench-repos" / "fastapi",
        ]
    )
    for candidate in candidates:
        if (candidate / FASTAPI_PROBE_REL_PATH).is_file():
            return resolve_git_repo_root(candidate, args.timeout)
    if not args.clone_missing_real_repos:
        searched = ", ".join(str(path) for path in candidates)
        raise RuntimeError(
            "fastapi_insert_probe requires --fastapi-repo, CBM_FASTAPI_REPO, "
            f"or --clone-missing-real-repos; searched: {searched}"
        )
    return clone_real_repo(args.fastapi_url, case_root / "source-fastapi", args.timeout)


def copy_git_head_to_dir(source_repo: Path, dest: Path, timeout: int) -> None:
    if dest.exists() and any(dest.iterdir()):
        raise RuntimeError(f"destination is not empty: {dest}")
    dest.mkdir(parents=True, exist_ok=True)
    raw = command_stdout_bytes(
        ["git", "ls-tree", "-r", "--name-only", "-z", "HEAD"], timeout, source_repo
    )
    rel_paths = [
        item.decode("utf-8", "surrogateescape")
        for item in raw.split(b"\0")
        if item
    ]
    for rel_path in rel_paths:
        blob = command_stdout_bytes(["git", "show", f"HEAD:{rel_path}"], timeout, source_repo)
        target = dest / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(blob)


def copy_git_revision_to_dir(
    source_repo: Path,
    dest: Path,
    revision: str,
    timeout: int,
    *,
    excluded_prefixes: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Materialize tracked files from one exact commit without source dirty state."""
    source_root = resolve_git_repo_root(source_repo, timeout)
    exact_revision = command_stdout(
        ["git", "rev-parse", f"{revision}^{{commit}}"], timeout, source_root
    )
    tree = command_stdout(
        ["git", "rev-parse", f"{exact_revision}^{{tree}}"], timeout, source_root
    )
    dirty_status = command_stdout(["git", "status", "--short"], timeout, source_root)
    if dest.exists() and any(dest.iterdir()):
        raise RuntimeError(f"destination is not empty: {dest}")
    dest.mkdir(parents=True, exist_ok=True)
    archive_path = dest.parent / f".cbm-background-{os.getpid()}-{time.time_ns()}.tar"
    try:
        proc, _ = command_result(
            [
                "git",
                "archive",
                "--format=tar",
                f"--output={archive_path}",
                exact_revision,
            ],
            dict(os.environ),
            timeout,
            cwd=source_root,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"git archive failed: {proc.stderr.strip()}")
        destination_root = dest.resolve()
        with tarfile.open(archive_path, mode="r:") as archive:
            excluded_roots = tuple(prefix.rstrip("/") for prefix in excluded_prefixes)
            members = [
                member
                for member in archive.getmembers()
                if not any(
                    member.name == root or member.name.startswith(f"{root}/")
                    for root in excluded_roots
                )
            ]
            for member in members:
                target = (dest / member.name).resolve()
                if target != destination_root and destination_root not in target.parents:
                    raise RuntimeError(f"git archive member escapes destination: {member.name}")
            archive.extractall(dest, members=members, filter="data")
    finally:
        if archive_path.exists():
            archive_path.unlink()
    return {
        "source_repo": str(source_root),
        "revision": exact_revision,
        "tree": tree,
        "source_dirty_status_short": dirty_status,
        "excluded_prefixes": list(excluded_prefixes),
        "copy_policy": "git_archive_tracked_files_from_exact_commit",
    }


def copy_fastapi_head_to_case(
    args: argparse.Namespace, repo_dir: Path, case_root: Path
) -> dict[str, Any]:
    source_repo = resolve_fastapi_source(args, case_root)
    copy_git_head_to_dir(source_repo, repo_dir, args.timeout)
    return {
        "source_repo": str(source_repo),
        "source_git": git_metadata(source_repo, args.timeout),
        "copy_policy": "git_tracked_files_from_HEAD",
    }


def create_self_dogfood_worktree(
    source_repo: Path,
    case_root: Path,
    timeout: int,
    revision: str,
) -> Path:
    repo_dir = case_root / SELF_DOGFOOD_REPO_SUBDIR
    if repo_dir.exists():
        raise RuntimeError(f"self-dogfood worktree already exists: {repo_dir}")
    proc, _ = command_result(
        ["git", "worktree", "add", "--detach", str(repo_dir), revision],
        dict(os.environ),
        timeout,
        source_repo,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"git worktree add failed: {proc.stderr.strip()}")
    return repo_dir


def remove_self_dogfood_worktree(source_repo: Path, repo_dir: Path, timeout: int) -> dict[str, Any]:
    cleanup: dict[str, Any] = {"requested": True, "path": str(repo_dir), "removed": False}
    proc, _ = command_result(
        ["git", "worktree", "remove", "--force", str(repo_dir)],
        dict(os.environ),
        timeout,
        source_repo,
    )
    if proc.returncode != 0:
        cleanup["git_worktree_remove_error"] = proc.stderr.strip()
        shutil.rmtree(repo_dir, ignore_errors=True)
    cleanup["removed"] = not repo_dir.exists()
    return cleanup


def self_dogfood_marker(name: str) -> str:
    return f"{SELF_DOGFOOD_MARKER_PREFIX}_{name}"


def append_c_marker_function(repo_dir: Path, rel_path: str, marker: str, value: int) -> str:
    append_text(
        repo_dir / rel_path,
        (
            "\n"
            f"static int {marker}(void) {{\n"
            f"    return {value};\n"
            "}\n"
        ),
    )
    return rel_path


def create_c_marker_file(repo_dir: Path, rel_path: str, marker: str, value: int) -> str:
    path = repo_dir / rel_path
    if path.exists():
        raise RuntimeError(f"benchmark new-file mutation target already exists: {rel_path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"static int {marker}(void) {{\n    return {value};\n}}\n",
        encoding="utf-8",
    )
    return rel_path


def mutate_self_dogfood_scenario(name: str, repo_dir: Path) -> dict[str, Any]:
    marker = self_dogfood_marker(name)
    changed: list[str] = []
    scenario_paths = {
        "noop": [],
        "one_source_file": ["src/pipeline/pipeline_internal.h"],
        "route_handler": ["src/ui/http_server.c"],
        "c_new_leaf": ["src/cbm_benchmark_leaf.c"],
        "store_pipeline_batch": ["src/store/store.h", "src/pipeline/pipeline_internal.h"],
        "multi_file_small": ["src/mcp/mcp.c", "tests/test_mcp.c"],
    }
    paths = scenario_paths.get(name)
    if paths is None:
        raise ValueError(f"unknown self-dogfood scenario: {name}")
    before_hashes = {
        path: file_sha256(repo_dir / path) if (repo_dir / path).is_file() else None
        for path in paths
    }

    def finish(document: dict[str, Any]) -> dict[str, Any]:
        document["source_hashes"] = [
            {
                "path": path,
                "before_sha256": before_hashes[path],
                "after_sha256": (
                    file_sha256(repo_dir / path) if (repo_dir / path).is_file() else None
                ),
            }
            for path in paths
        ]
        return document

    if name == "noop":
        return finish(
            {"marker": None, "changed_paths": changed, "description": "no source mutation"}
        )
    if name == "one_source_file":
        changed.append(
            append_c_marker_function(repo_dir, "src/pipeline/pipeline_internal.h", marker, 4101)
        )
        return finish(
            {"marker": marker, "changed_paths": changed, "description": "single C header edit"}
        )
    if name == "route_handler":
        append_text(
            repo_dir / "src/ui/http_server.c",
            (
                "\n"
                f"static int {marker}(const char *path) {{\n"
                '    return cbm_http_path_match(path, "/api/pan4-oracle");\n'
                "}\n"
            ),
        )
        changed.append("src/ui/http_server.c")
        return finish(
            {
                "marker": marker,
                "changed_paths": changed,
                "description": "HTTP UI handler source edit with route literal oracle",
            }
        )
    if name == "c_new_leaf":
        changed.append(
            create_c_marker_file(
                repo_dir,
                "src/cbm_benchmark_leaf.c",
                marker,
                4102,
            )
        )
        return finish(
            {
                "marker": marker,
                "changed_paths": changed,
                "description": "new isolated C source file",
            }
        )
    if name == "store_pipeline_batch":
        changed.append(append_c_marker_function(repo_dir, "src/store/store.h", marker, 4103))
        second_marker = f"{marker}_pipeline"
        changed.append(
            append_c_marker_function(repo_dir, "src/pipeline/pipeline_internal.h", second_marker, 4104)
        )
        return finish(
            {
                "marker": marker,
                "secondary_marker": second_marker,
                "changed_paths": changed,
                "description": "small store plus pipeline header batch",
            }
        )
    if name == "multi_file_small":
        changed.append(append_c_marker_function(repo_dir, "src/mcp/mcp.c", marker, 4105))
        second_marker = f"{marker}_test"
        changed.append(append_c_marker_function(repo_dir, "tests/test_mcp.c", second_marker, 4106))
        return finish(
            {
                "marker": marker,
                "secondary_marker": second_marker,
                "changed_paths": changed,
                "description": "small production plus test source batch",
            }
        )


def oracle_passed(tool_result: dict[str, Any], marker: str | None) -> bool:
    if not marker:
        return True
    response = tool_result.get("response")
    return marker in json.dumps(response, sort_keys=True)


def canonical_pair(source: str, target: str) -> tuple[str, str]:
    """Return an order-independent pair identity without losing endpoint names."""
    if not isinstance(source, str) or not source or not isinstance(target, str) or not target:
        raise ValueError("pair endpoints must be non-empty strings")
    if source == target:
        raise ValueError("pair endpoints must be distinct")
    return (source, target) if source < target else (target, source)


def score_pair_classification(
    observed_pairs: list[dict[str, Any]],
    judgments: list[dict[str, Any]],
) -> dict[str, Any]:
    """Score unordered observed pairs against explicit positive/negative judgments.

    Natural large-repository results outside the bounded judgment set are retained as
    unjudged observations. They are intentionally excluded from the confusion matrix:
    incomplete ground truth cannot turn an unknown pair into a false positive.
    """
    judgment_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    for judgment in judgments:
        if not isinstance(judgment, dict):
            raise ValueError("pair judgment must be an object")
        pair = canonical_pair(judgment.get("source"), judgment.get("target"))
        if pair in judgment_by_pair:
            raise ValueError(f"duplicate pair judgment: {pair[0]} <-> {pair[1]}")
        expected = judgment.get("expected")
        if not isinstance(expected, bool):
            raise ValueError("pair judgment expected must be boolean")
        category = judgment.get("category", "uncategorized")
        if not isinstance(category, str) or not category:
            raise ValueError("pair judgment category must be a non-empty string")
        judgment_by_pair[pair] = {
            **judgment,
            "source": pair[0],
            "target": pair[1],
            "expected": expected,
            "category": category,
        }

    observed_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    for observed in observed_pairs:
        if not isinstance(observed, dict):
            raise ValueError("observed pair must be an object")
        pair = canonical_pair(observed.get("source"), observed.get("target"))
        observed_by_pair.setdefault(
            pair,
            {**observed, "source": pair[0], "target": pair[1]},
        )

    confusion = {"tp": 0, "fp": 0, "fn": 0, "tn": 0}
    witnesses: dict[str, list[dict[str, Any]]] = {key: [] for key in confusion}
    categories: dict[str, dict[str, int]] = {}
    for pair, judgment in judgment_by_pair.items():
        observed = observed_by_pair.get(pair)
        if judgment["expected"]:
            outcome = "tp" if observed is not None else "fn"
        else:
            outcome = "fp" if observed is not None else "tn"
        confusion[outcome] += 1
        category = judgment["category"]
        category_counts = categories.setdefault(
            category,
            {"tp": 0, "fp": 0, "fn": 0, "tn": 0},
        )
        category_counts[outcome] += 1
        witnesses[outcome].append(
            {
                "source": pair[0],
                "target": pair[1],
                "category": category,
                "observed": observed,
            }
        )

    unjudged_observed = [
        observed
        for pair, observed in sorted(observed_by_pair.items())
        if pair not in judgment_by_pair
    ]
    precision_denominator = confusion["tp"] + confusion["fp"]
    recall_denominator = confusion["tp"] + confusion["fn"]
    negative_denominator = confusion["fp"] + confusion["tn"]
    precision = (
        confusion["tp"] / precision_denominator if precision_denominator else None
    )
    recall = confusion["tp"] / recall_denominator if recall_denominator else None
    f1 = (
        2.0 * precision * recall / (precision + recall)
        if precision is not None and recall is not None and precision + recall > 0
        else None
    )
    false_positive_rate = (
        confusion["fp"] / negative_denominator if negative_denominator else None
    )
    return {
        "judgment_count": len(judgment_by_pair),
        "observed_pair_count": len(observed_by_pair),
        "confusion": confusion,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "false_positive_rate": false_positive_rate,
        "categories": categories,
        "witnesses": witnesses,
        "unjudged_observed_count": len(unjudged_observed),
        "unjudged_observed": unjudged_observed,
        "passed": recall_denominator > 0 and confusion["fp"] == 0 and confusion["fn"] == 0,
        "ground_truth_boundary": (
            "Only explicit judgments enter TP/FP/FN/TN; unjudged observed pairs are retained "
            "but excluded because natural-repository ground truth is incomplete."
        ),
    }


def score_ranked_relevance(
    ranked_items: list[Any],
    judgments: list[dict[str, Any]],
    *,
    cutoff: int = 5,
) -> dict[str, Any]:
    """Score a bounded ranking against explicit graded substring judgments."""
    if cutoff <= 0:
        raise ValueError("relevance cutoff must be positive")
    valid_judgments = [
        {
            "expected": str(item["expected_substring"]),
            "required": [
                str(value)
                for value in item.get("required_substrings", [])
                if isinstance(value, str) and value
            ],
            "grade": float(item["relevance"]),
        }
        for item in judgments
        if isinstance(item, dict)
        and isinstance(item.get("expected_substring"), str)
        and item["expected_substring"]
        and isinstance(item.get("relevance"), (int, float))
        and float(item["relevance"]) > 0
    ]
    all_relevance: list[float | int] = []
    for ranked_item in ranked_items:
        serialized = json.dumps(ranked_item, separators=(",", ":"), sort_keys=True)
        relevance = max(
            (
                item["grade"]
                for item in valid_judgments
                if item["expected"] in serialized
                and all(required in serialized for required in item["required"])
            ),
            default=0.0,
        )
        all_relevance.append(
            int(relevance) if relevance.is_integer() else relevance
        )
    first_relevant_rank = next(
        (index for index, relevance in enumerate(all_relevance, start=1) if relevance > 0),
        None,
    )
    matched_relevance = all_relevance[:cutoff]

    def discounted_gain(grades: list[float | int]) -> float:
        return sum(
            (2.0 ** float(relevance) - 1.0) / math.log2(position + 1)
            for position, relevance in enumerate(grades, start=1)
        )

    dcg = discounted_gain(matched_relevance)
    ideal_relevance = sorted(
        (item["grade"] for item in valid_judgments), reverse=True
    )[:cutoff]
    idcg = discounted_gain(ideal_relevance)
    ndcg = dcg / idcg if idcg > 0 else None
    result = {
        "cutoff": cutoff,
        "judgment_count": len(valid_judgments),
        "first_relevant_rank": first_relevant_rank,
        "reciprocal_rank": 1.0 / first_relevant_rank if first_relevant_rank else 0.0,
        "hit_at_1": first_relevant_rank == 1,
        "hit_at_5": first_relevant_rank is not None and first_relevant_rank <= 5,
        "dcg": dcg,
        "ideal_dcg": idcg,
        "ndcg": ndcg,
        "matched_relevance": matched_relevance,
    }
    result[f"dcg_at_{cutoff}"] = dcg
    result[f"ideal_dcg_at_{cutoff}"] = idcg
    result[f"ndcg_at_{cutoff}"] = ndcg
    return result


def score_quality_oracles(
    oracles: dict[str, Any],
    expectations: dict[str, Any],
) -> dict[str, Any]:
    """Attach auditable per-oracle verdicts and summarize applicable checks."""
    applicable_count = 0
    passed_count = 0
    reciprocal_rank_total = 0.0
    hit_at_1_count = 0
    hit_at_5_count = 0
    ndcg_total = 0.0
    ndcg_applicable_count = 0
    for name, result in oracles.items():
        if not isinstance(result, dict):
            continue
        expectation = expectations.get(name, (None, "no quality criterion"))
        graded = isinstance(expectation, dict)
        if graded:
            criterion = str(expectation.get("criterion") or "no quality criterion")
            judgments = expectation.get("judgments")
            judgments = judgments if isinstance(judgments, list) else []
            cutoff = expectation.get("cutoff", 5)
            cutoff = int(cutoff) if isinstance(cutoff, int) else 5
            positive_judgments = [
                item
                for item in judgments
                if isinstance(item, dict)
                and isinstance(item.get("expected_substring"), str)
                and isinstance(item.get("relevance"), (int, float))
                and float(item["relevance"]) > 0
            ]
            expected = (
                str(max(positive_judgments, key=lambda item: float(item["relevance"]))[
                    "expected_substring"
                ])
                if positive_judgments
                else None
            )
            required_substrings = (
                list(
                    max(
                        positive_judgments,
                        key=lambda item: float(item["relevance"]),
                    ).get("required_substrings", [])
                )
                if positive_judgments
                else []
            )
        else:
            expected, criterion = expectation
            judgments = []
            cutoff = 5
            required_substrings = []
        applicable = bool(judgments) if graded else expected is not None
        passed = False
        rank: int | None = None
        returned_count: int | None = None
        ndcg: float | None = None
        if applicable:
            applicable_count += 1
            response = result.get("response")
            ranked_items = (
                response.get("results")
                if isinstance(response, dict) and isinstance(response.get("results"), list)
                else response if isinstance(response, list) else [response]
            )
            returned_count = len(ranked_items)
            if graded:
                ranking = score_ranked_relevance(ranked_items, judgments, cutoff=cutoff)
                rank = ranking["first_relevant_rank"]
                reciprocal_rank = float(ranking["reciprocal_rank"])
                ndcg_value = ranking["ndcg"]
                ndcg = float(ndcg_value) if isinstance(ndcg_value, (int, float)) else None
                passed = bool(ranking["hit_at_5"])
                if ndcg is not None:
                    ndcg_total += ndcg
                    ndcg_applicable_count += 1
            else:
                passed = expected in json.dumps(
                    response, separators=(",", ":"), sort_keys=True
                )
                for position, item in enumerate(ranked_items, start=1):
                    if expected in json.dumps(item, separators=(",", ":"), sort_keys=True):
                        rank = position
                        break
                reciprocal_rank = 1.0 / rank if rank is not None else 0.0
            passed_count += int(passed)
            reciprocal_rank_total += reciprocal_rank
            hit_at_1_count += int(rank == 1)
            hit_at_5_count += int(rank is not None and rank <= 5)
        else:
            reciprocal_rank = None
        result["quality"] = {
            "applicable": applicable,
            "passed": passed if applicable else None,
            "criterion": criterion,
            "expected_substring": expected,
            "required_substrings": required_substrings,
            "rank": rank,
            "returned_count": returned_count,
            "reciprocal_rank": reciprocal_rank,
            "hit_at_1": rank == 1 if applicable else None,
            "hit_at_5": rank is not None and rank <= 5 if applicable else None,
            "relevance_judgments": len(judgments) if graded else None,
            "relevance_cutoff": cutoff if graded else None,
            "ndcg_at_5": ndcg if graded and cutoff == 5 else None,
        }
    mean_reciprocal_rank = (
        reciprocal_rank_total / applicable_count if applicable_count else None
    )
    return {
        "passed": passed_count == applicable_count,
        "passed_count": passed_count,
        "applicable_count": applicable_count,
        "binary_pass_rate": round(passed_count / applicable_count, 6) if applicable_count else None,
        "mean_reciprocal_rank": (
            round(mean_reciprocal_rank, 6) if mean_reciprocal_rank is not None else None
        ),
        "hit_at_1": round(hit_at_1_count / applicable_count, 6) if applicable_count else None,
        "hit_at_5": round(hit_at_5_count / applicable_count, 6) if applicable_count else None,
        "mean_ndcg_at_5": (
            round(ndcg_total / ndcg_applicable_count, 6)
            if ndcg_applicable_count
            else None
        ),
        "ndcg_applicable_count": ndcg_applicable_count,
        "score": round(mean_reciprocal_rank, 6) if mean_reciprocal_rank is not None else None,
    }


def run_self_dogfood_oracles(
    transport: str,
    binary: Path,
    env: dict[str, str],
    project: str,
    mutation: dict[str, Any],
    args: argparse.Namespace,
    client: McpClient | None = None,
) -> dict[str, Any]:
    marker = mutation.get("marker")
    changed_paths = list(mutation.get("changed_paths") or [])
    first_changed = changed_paths[0] if changed_paths else ""
    oracles: dict[str, Any] = {}
    expectations: dict[str, tuple[str | None, str]] = {}
    if marker:
        search_code_args: dict[str, Any] = {"project": project, "pattern": marker, "limit": 5}
        if first_changed:
            search_code_args["file_pattern"] = Path(first_changed).name
            search_code_args["path_filter"] = f"^{re.escape(first_changed)}$"
        oracles["marker_search_graph"] = run_tool_call_for_transport(
            transport,
            binary,
            env,
            "search_graph",
            {"project": project, "name_pattern": marker, "limit": 5},
            args.timeout,
            args.include_logs,
            client,
        )
        expectations["marker_search_graph"] = (marker, "mutated symbol appears in graph search")
        oracles["marker_search_code"] = run_tool_call_for_transport(
            transport,
            binary,
            env,
            "search_code",
            search_code_args,
            args.timeout,
            args.include_logs,
            client,
        )
        expectations["marker_search_code"] = (marker, "mutated symbol appears in source search")
    if first_changed:
        oracles["changed_file_query_graph"] = run_tool_call_for_transport(
            transport,
            binary,
            env,
            "query_graph",
            {
                "project": project,
                "query": (
                    "MATCH (n) WHERE n.file_path CONTAINS "
                    f"'{first_changed}' RETURN n.name, n.label, n.file_path LIMIT 10"
                ),
            },
            args.timeout,
            args.include_logs,
            client,
        )
        expectations["changed_file_query_graph"] = (
            first_changed,
            "changed file path appears in graph query",
        )
        oracles["scoped_architecture"] = run_tool_call_for_transport(
            transport,
            binary,
            env,
            "get_architecture",
            {"project": project, "path": first_changed, "aspects": ["all"]},
            args.timeout,
            args.include_logs,
            client,
        )
        expectations["scoped_architecture"] = (
            first_changed,
            "changed file path appears in scoped architecture",
        )
    route_expected = "/api/pan4-oracle" if mutation.get("description", "").startswith(
        "HTTP UI handler"
    ) else None
    route_arguments: dict[str, Any] = {"project": project, "label": "Route", "limit": 5}
    if route_expected:
        route_arguments["name_pattern"] = "pan4-oracle"
    oracles["route_freshness_probe"] = run_tool_call_for_transport(
        transport,
        binary,
        env,
        "search_graph",
        route_arguments,
        args.timeout,
        args.include_logs,
        client,
    )
    expectations["route_freshness_probe"] = (
        route_expected,
        "new route literal appears in route search" if route_expected else "route mutation not applicable",
    )
    quality = score_quality_oracles(oracles, expectations)
    oracles["quality"] = quality
    oracles["passed"] = quality["passed"]
    return oracles


def run_rank_quality_oracles(
    transport: str,
    binary: Path,
    env: dict[str, str],
    project: str,
    args: argparse.Namespace,
    client: McpClient | None = None,
) -> dict[str, Any]:
    oracles = {
        "central_order_search": run_tool_call_for_transport(
            transport,
            binary,
            env,
            "search_graph",
            {
                "project": project,
                "label": "Function",
                "name_pattern": "order",
                "limit": 10,
            },
            args.timeout,
            args.include_logs,
            client,
        )
    }
    expectations = {
        "central_order_search": {
            "criterion": (
                "rank the structurally central order workflow ahead of lexical-only decoys"
            ),
            "cutoff": 5,
            "judgments": [
                {"expected_substring": "zz_order_core", "relevance": 3},
            ],
        }
    }
    quality = score_quality_oracles(oracles, expectations)
    oracles["quality"] = quality
    oracles["passed"] = quality["passed"]
    return oracles


def run_dependency_quality_oracles(
    transport: str,
    binary: Path,
    env: dict[str, str],
    project: str,
    args: argparse.Namespace,
    client: McpClient | None = None,
) -> dict[str, Any]:
    symbol = "canonicalDependencyAPI"
    package_name = "cbmbenchdep"
    oracles = {
        "dependency_api_search": run_tool_call_for_transport(
            transport,
            binary,
            env,
            "search_graph",
            {
                "project": project,
                "label": "Function",
                "name_pattern": symbol,
                "include_dependencies": True,
                "limit": 10,
            },
            args.timeout,
            args.include_logs,
            client,
        )
    }
    expectations = {
        "dependency_api_search": {
            "criterion": (
                "retrieve the imported dependency API with dependency, package, and read-only "
                "provenance on the same result"
            ),
            "cutoff": 5,
            "judgments": [
                {
                    "expected_substring": symbol,
                    "required_substrings": [
                        '\"source\":\"dependency\"',
                        f'\"package\":\"{package_name}\"',
                        '\"read_only\":true',
                    ],
                    "relevance": 3,
                }
            ],
        }
    }
    quality = score_quality_oracles(oracles, expectations)
    oracles["quality"] = quality
    oracles["passed"] = quality["passed"]
    return oracles


def observed_pairs_from_query_response(tool_result: dict[str, Any]) -> list[dict[str, Any]]:
    response = tool_result.get("response")
    if not isinstance(response, dict):
        return []
    columns = response.get("columns")
    rows = response.get("rows")
    if not isinstance(columns, list) or not isinstance(rows, list):
        return []
    column_names = [str(value) for value in columns]
    observed: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 2:
            continue
        score = row[2] if len(row) > 2 else None
        if isinstance(score, str):
            try:
                score = float(score)
            except ValueError:
                pass
        values = {
            column_names[index]: value
            for index, value in enumerate(row)
            if index < len(column_names)
        }
        observed.append(
            {
                "source": str(row[0]),
                "target": str(row[1]),
                "score": score,
                "source_path": row[3] if len(row) > 3 else None,
                "target_path": row[4] if len(row) > 4 else None,
                "row": values,
            }
        )
    return observed


def compare_pair_oracle_outputs(
    incremental: dict[str, Any], fresh: dict[str, Any]
) -> dict[str, Any]:
    def canonical(output: dict[str, Any]) -> set[tuple[str, str, float | str | None]]:
        result: set[tuple[str, str, float | str | None]] = set()
        for item in output.get("observed_pairs", []):
            source, target = canonical_pair(item.get("source"), item.get("target"))
            result.add((source, target, item.get("score")))
        return result

    incremental_pairs = canonical(incremental)
    fresh_pairs = canonical(fresh)

    def render(values: set[tuple[str, str, float | str | None]]) -> list[dict[str, Any]]:
        return [
            {"source": source, "target": target, "score": score}
            for source, target, score in sorted(values)
        ]

    return {
        "passed": incremental_pairs == fresh_pairs,
        "incremental_only": render(incremental_pairs - fresh_pairs),
        "fresh_only": render(fresh_pairs - incremental_pairs),
        "incremental_pair_count": len(incremental_pairs),
        "fresh_pair_count": len(fresh_pairs),
    }


def evaluate_pair_incremental_policy(
    config_overrides: dict[str, str],
    incremental_index: dict[str, Any],
    incremental_oracles: dict[str, Any],
    canonical_graph: dict[str, Any],
    pair_equality: dict[str, Any],
) -> dict[str, Any]:
    explicit_policy = config_overrides.get("incremental_derived_refresh")
    policy = explicit_policy or DERIVED_REFRESH_CANDIDATE_DEFAULT
    policy_source = "explicit_override" if explicit_policy else "candidate_default"
    warnings = incremental_oracles.get("edge_query", {}).get("response", {}).get(
        "warnings", []
    )
    warnings = warnings if isinstance(warnings, list) else []
    stale_warning_present = any(
        isinstance(warning, str)
        and "semantic_edges derived view is stale" in warning
        for warning in warnings
    )
    pair_freshness_met = bool(
        incremental_oracles.get("passed") and pair_equality.get("passed")
    )
    immediate_freshness_met = bool(pair_freshness_met and canonical_graph.get("equal"))
    if explicit_policy:
        immediate_freshness_expected: bool | None = policy == "eager"
        policy_conformance_met = (
            immediate_freshness_met and not stale_warning_present
            if immediate_freshness_expected
            else immediate_freshness_met or stale_warning_present
        )
        observed_behavior = (
            "immediate_full_freshness"
            if immediate_freshness_met and not stale_warning_present
            else "deferred_with_warning"
            if stale_warning_present
            else "unreported_stale"
        )
    elif stale_warning_present:
        immediate_freshness_expected = False
        policy_conformance_met = True
        observed_behavior = "deferred_with_warning"
    elif pair_freshness_met:
        immediate_freshness_expected = True
        policy_conformance_met = True
        observed_behavior = (
            "immediate_full_freshness"
            if immediate_freshness_met
            else "immediate_pair_freshness"
        )
    else:
        immediate_freshness_expected = None
        policy_conformance_met = False
        observed_behavior = "unreported_stale"
    return {
        "policy": policy,
        "policy_source": policy_source,
        "observed_behavior": observed_behavior,
        "publish_kind": incremental_index.get("publish_kind"),
        "immediate_freshness_expected": immediate_freshness_expected,
        "pair_freshness_met": pair_freshness_met,
        "immediate_freshness_met": immediate_freshness_met,
        "stale_warning_present": stale_warning_present,
        "policy_conformance_met": policy_conformance_met,
        "interpretation": (
            "eager policy requires canonical fresh semantic/similarity results"
            if explicit_policy == "eager"
            else "explicit deferred policy requires a stale warning or canonical freshness"
            if explicit_policy
            else "candidate default is classified from observed pair freshness and warnings"
        ),
    }


def run_relation_quality_oracles(
    transport: str,
    binary: Path,
    env: dict[str, str],
    project: str,
    fixture: dict[str, Any],
    args: argparse.Namespace,
    client: McpClient | None = None,
) -> dict[str, Any]:
    relationship = str(fixture["relationship"])
    score_property = str(fixture["score_property"])
    marker = str(fixture["query_name_marker"])
    query = (
        f"MATCH (a)-[r:{relationship}]->(b) "
        f"WHERE a.name CONTAINS '{marker}' OR b.name CONTAINS '{marker}' "
        f"RETURN a.name, b.name, r.{score_property}, a.file_path, b.file_path LIMIT 1000"
    )
    edge_query = run_tool_call_for_transport(
        transport,
        binary,
        env,
        "query_graph",
        {
            "project": project,
            "query": query,
            "format": "json",
            "max_output_bytes": 1024 * 1024,
        },
        args.timeout,
        args.include_logs,
        client,
    )
    observed_pairs = observed_pairs_from_query_response(edge_query)
    pair_classification = score_pair_classification(
        observed_pairs,
        list(fixture["judgments"]),
    )
    true_positive_witnesses = pair_classification["witnesses"]["tp"]
    score_witness_count = sum(
        1
        for witness in true_positive_witnesses
        if isinstance(witness.get("observed"), dict)
        and isinstance(witness["observed"].get("score"), (int, float))
    )
    score_coverage = (
        score_witness_count / len(true_positive_witnesses)
        if true_positive_witnesses
        else None
    )
    response = edge_query.get("response")
    response_quality = {
        "correctness": pair_classification["passed"],
        "relevance": pair_classification["precision"],
        "completeness": pair_classification["recall"],
        "actionable_witness_score_coverage": score_coverage,
        "protocol_shape_valid": (
            isinstance(response, dict)
            and isinstance(response.get("columns"), list)
            and isinstance(response.get("rows"), list)
        ),
        "truncated": response.get("truncated") if isinstance(response, dict) else None,
        "elapsed_ms": edge_query.get("elapsed_ms"),
        "response_bytes": edge_query.get("response_bytes"),
        "response_token_estimate": edge_query.get("response_token_estimate"),
        "hard_gate": (
            pair_classification["passed"]
            and score_coverage == 1.0
            and isinstance(response, dict)
            and isinstance(response.get("rows"), list)
            and not bool(response.get("truncated"))
        ),
    }
    return {
        "relationship": relationship,
        "edge_query": edge_query,
        "observed_pairs": observed_pairs,
        "pair_classification": pair_classification,
        "response_quality": response_quality,
        "passed": response_quality["hard_gate"],
    }


def run_index_for_transport(
    transport: str,
    binary: Path,
    env: dict[str, str],
    repo_dir: Path,
    timeout: int,
    include_logs: bool,
    client: McpClient | None = None,
    index_mode: str = "fast",
) -> dict[str, Any]:
    if transport == "mcp":
        if client is None:
            raise RuntimeError("MCP transport requires an active client")
        return run_index_mcp(client, repo_dir, include_logs, index_mode)
    return run_index(binary, env, repo_dir, timeout, include_logs, index_mode)


def run_pair_quality_lifecycle(
    args: argparse.Namespace,
    binary: Path,
    case_env: dict[str, str],
    repo_dir: Path,
    cache_dir: Path,
    work_root: Path,
    fixture: dict[str, Any],
) -> dict[str, Any]:
    run_config_set(binary, case_env, "incremental_reindex", "always", args.timeout)
    if args.transport == "mcp":
        with McpClient(binary, case_env, args.timeout) as client:
            initial_index = run_index_for_transport(
                args.transport,
                binary,
                case_env,
                repo_dir,
                args.timeout,
                args.include_logs,
                client,
                index_mode=args.index_mode,
            )
            project = str(initial_index.get("response", {}).get("project") or "repo")
            initial_oracles = run_relation_quality_oracles(
                args.transport, binary, case_env, project, fixture, args, client
            )
            initial_graph_fingerprint = stable_graph_fingerprint(
                find_project_db(cache_dir), project
            )
            mutation = apply_pair_quality_mutation(repo_dir, fixture)
            incremental_index = run_index_for_transport(
                args.transport,
                binary,
                case_env,
                repo_dir,
                args.timeout,
                args.include_logs,
                client,
                index_mode=args.index_mode,
            )
            post_fixture = {**fixture, "judgments": mutation["post_judgments"]}
            incremental_oracles = run_relation_quality_oracles(
                args.transport, binary, case_env, project, post_fixture, args, client
            )
    else:
        initial_index = run_index_for_transport(
            args.transport,
            binary,
            case_env,
            repo_dir,
            args.timeout,
            args.include_logs,
            index_mode=args.index_mode,
        )
        project = str(initial_index.get("response", {}).get("project") or "repo")
        initial_oracles = run_relation_quality_oracles(
            args.transport, binary, case_env, project, fixture, args
        )
        initial_graph_fingerprint = stable_graph_fingerprint(
            find_project_db(cache_dir), project
        )
        mutation = apply_pair_quality_mutation(repo_dir, fixture)
        incremental_index = run_index_for_transport(
            args.transport,
            binary,
            case_env,
            repo_dir,
            args.timeout,
            args.include_logs,
            index_mode=args.index_mode,
        )
        post_fixture = {**fixture, "judgments": mutation["post_judgments"]}
        incremental_oracles = run_relation_quality_oracles(
            args.transport, binary, case_env, project, post_fixture, args
        )

    incremental_db = find_project_db(cache_dir)
    incremental_snapshot = work_root / "incremental.db"
    copy_sqlite_snapshot(incremental_db, incremental_snapshot)

    fresh_cache = work_root / "fresh-cache"
    fresh_cache.mkdir(parents=True, exist_ok=True)
    fresh_env = build_env(fresh_cache)
    apply_rank_refresh_override(binary, fresh_env, args.rank_refresh, args.timeout)
    apply_config_overrides(binary, fresh_env, args.config_overrides, args.timeout)
    if args.transport == "mcp":
        with McpClient(binary, fresh_env, args.timeout) as client:
            fresh_index = run_index_for_transport(
                args.transport,
                binary,
                fresh_env,
                repo_dir,
                args.timeout,
                args.include_logs,
                client,
                index_mode=args.index_mode,
            )
            fresh_project = str(fresh_index.get("response", {}).get("project") or project)
            fresh_oracles = run_relation_quality_oracles(
                args.transport, binary, fresh_env, fresh_project, post_fixture, args, client
            )
    else:
        fresh_index = run_index_for_transport(
            args.transport,
            binary,
            fresh_env,
            repo_dir,
            args.timeout,
            args.include_logs,
            index_mode=args.index_mode,
        )
        fresh_project = str(fresh_index.get("response", {}).get("project") or project)
        fresh_oracles = run_relation_quality_oracles(
            args.transport, binary, fresh_env, fresh_project, post_fixture, args
        )
    fresh_db = find_project_db(fresh_cache)
    incremental_graph_fingerprint = stable_graph_fingerprint(incremental_snapshot, project)
    fresh_graph_fingerprint = stable_graph_fingerprint(fresh_db, fresh_project)
    canonical_graph = compare_canonical_graph(incremental_snapshot, fresh_db, project)
    pair_equality = compare_pair_oracle_outputs(incremental_oracles, fresh_oracles)
    incremental_policy = evaluate_pair_incremental_policy(
        args.config_overrides,
        incremental_index,
        incremental_oracles,
        canonical_graph,
        pair_equality,
    )
    return {
        "project": project,
        "initial_index": initial_index,
        "initial_oracles": initial_oracles,
        "mutation": mutation,
        "incremental_index": incremental_index,
        "incremental_oracles": incremental_oracles,
        "fresh_index": fresh_index,
        "fresh_oracles": fresh_oracles,
        "graph_fingerprints": {
            "initial": initial_graph_fingerprint,
            "incremental": incremental_graph_fingerprint,
            "fresh": fresh_graph_fingerprint,
        },
        "canonical_graph": canonical_graph,
        "pair_equality": pair_equality,
        "incremental_policy": incremental_policy,
        "policy_conformance_met": incremental_policy["policy_conformance_met"],
        "quality_target_met": bool(
            initial_oracles.get("passed")
            and incremental_oracles.get("passed")
            and fresh_oracles.get("passed")
            and canonical_graph.get("equal")
            and pair_equality.get("passed")
        ),
    }


def run_capability_quality(args: argparse.Namespace, binary: Path) -> tuple[dict[str, Any], int]:
    capability = args.capability_quality
    if capability not in CAPABILITY_QUALITY_CASES:
        raise ValueError(f"unsupported capability quality case: {capability}")
    auto_root = not bool(args.work_root)
    work_root = (
        Path(args.work_root).expanduser()
        if args.work_root
        else Path(tempfile.mkdtemp(prefix=f"cbm-quality-{capability}-"))
    )
    repo_dir = work_root / "repo"
    cache_dir = work_root / "cache"
    repo_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    case_env = build_env(cache_dir)
    report: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "binary": str(binary),
        "binary_metadata": binary_metadata(binary),
        "work_root": str(work_root),
        "mode": "capability_quality",
        "parameters": {
            "capability": capability,
            "rank_refresh": args.rank_refresh,
            "rank_refresh_override_applied": (
                args.rank_refresh != RANK_REFRESH_CANDIDATE_DEFAULT
            ),
            "index_mode": args.index_mode,
            "capability_applicability": index_mode_capability_applicability(args.index_mode),
            "config_profile": args.config_profile,
            "config_overrides": args.config_overrides,
            "transport": args.transport,
            "timeout": args.timeout,
            "quality_background_repo": args.quality_background_repo or None,
            "quality_background_revision": (
                (args.quality_background_revision or "HEAD")
                if args.quality_background_repo
                else None
            ),
        },
        "cleanup": {"requested": auto_root and not args.keep_work_root, "removed": False},
        "cases": [],
    }
    exit_code = 1
    try:
        background = None
        if args.quality_background_revision and not args.quality_background_repo:
            raise ValueError(
                "--quality-background-revision requires --quality-background-repo"
            )
        if args.quality_background_repo:
            if capability not in {"similarity", "semantic_edges"}:
                raise ValueError(
                    "quality background repository is supported only for similarity and semantic_edges"
                )
            background = copy_git_revision_to_dir(
                Path(args.quality_background_repo).expanduser(),
                repo_dir,
                args.quality_background_revision or "HEAD",
                args.timeout,
                excluded_prefixes=("benchmarks/semantic-pairs-v1/",),
            )
        fixture_factory = {
            "rank": create_rank_quality_repo,
            "dependencies": create_dependency_quality_repo,
            "similarity": create_similarity_quality_repo,
            "semantic_edges": create_semantic_edges_quality_repo,
        }[capability]
        fixture = fixture_factory(repo_dir)
        apply_rank_refresh_override(binary, case_env, args.rank_refresh, args.timeout)
        apply_config_overrides(binary, case_env, args.config_overrides, args.timeout)
        lifecycle = None
        if capability in {"similarity", "semantic_edges"}:
            lifecycle = run_pair_quality_lifecycle(
                args, binary, case_env, repo_dir, cache_dir, work_root, fixture
            )
            indexed = lifecycle["initial_index"]
            project = lifecycle["project"]
            oracles = lifecycle["initial_oracles"]
        elif args.transport == "mcp":
            with McpClient(binary, case_env, args.timeout) as client:
                indexed = run_index_for_transport(
                    args.transport,
                    binary,
                    case_env,
                    repo_dir,
                    args.timeout,
                    args.include_logs,
                    client,
                    index_mode=args.index_mode,
                )
                project = str(indexed.get("response", {}).get("project") or "repo")
                oracle_runner = {
                    "rank": run_rank_quality_oracles,
                    "dependencies": run_dependency_quality_oracles,
                }[capability]
                oracles = oracle_runner(
                    args.transport, binary, case_env, project, args, client
                )
        else:
            indexed = run_index_for_transport(
                args.transport,
                binary,
                case_env,
                repo_dir,
                args.timeout,
                args.include_logs,
                index_mode=args.index_mode,
            )
            project = str(indexed.get("response", {}).get("project") or "repo")
            oracle_runner = {
                "rank": run_rank_quality_oracles,
                "dependencies": run_dependency_quality_oracles,
            }[capability]
            oracles = oracle_runner(args.transport, binary, case_env, project, args)
        case = {
            "scenario": f"{capability}_quality",
            "project": project,
            "fixture": fixture,
            "background_repository": background,
            "initial_fast_full": indexed,
            "oracles": oracles,
            "pair_lifecycle": lifecycle,
            "execution_passed": True,
            "quality_target_met": (
                bool(lifecycle["quality_target_met"])
                if lifecycle is not None
                else bool(oracles.get("passed"))
            ),
            "passed": True,
        }
        report["cases"].append(case)
        report["derived"] = {
            "passed": True,
            "quality_target_met": case["quality_target_met"],
            "case_count": 1,
        }
        exit_code = 0
    except Exception as exc:
        record_report_error(report, exc)
    finally:
        if auto_root and not args.keep_work_root:
            shutil.rmtree(work_root, ignore_errors=True)
            report["cleanup"]["removed"] = not work_root.exists()
        rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
        if args.out:
            atomic_write_text(Path(args.out).expanduser(), rendered)
        print(rendered, end="")
    return report, exit_code


def run_matrix_case(
    scenario: str,
    binary: Path,
    env: dict[str, str],
    case_root: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    repo_dir = case_root / "repo"
    cache_dir = case_root / "cache"
    case_root.mkdir(parents=True, exist_ok=True)
    if scenario not in MATRIX_REAL_REPO_SCENARIOS:
        repo_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    case_env = dict(env)
    case_env["CBM_CACHE_DIR"] = str(cache_dir)
    run_config_set(binary, case_env, "incremental_reindex", "always", args.timeout)
    apply_rank_refresh_override(binary, case_env, args.rank_refresh, args.timeout)
    apply_config_overrides(binary, case_env, args.config_overrides, args.timeout)

    scenario_metadata = prepare_matrix_scenario(
        scenario, repo_dir, args.files, args.functions_per_file, args, case_root
    )
    if args.transport == "mcp":
        with McpClient(binary, case_env, args.timeout) as client:
            initial = run_index_for_transport(
                args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs, client,
                index_mode=args.index_mode,
            )
            changed_paths = mutate_matrix_scenario(scenario, repo_dir, args.functions_per_file)
            incremental = run_index_for_transport(
                args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs, client,
                index_mode=args.index_mode,
            )
    else:
        initial = run_index_for_transport(
            args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs,
            index_mode=args.index_mode,
        )
        changed_paths = mutate_matrix_scenario(scenario, repo_dir, args.functions_per_file)
        incremental = run_index_for_transport(
            args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs,
            index_mode=args.index_mode,
        )

    project_db = find_project_db(cache_dir)
    project = str(incremental.get("response", {}).get("project") or project_db.stem)
    incremental_snapshot = case_root / "incremental.db"
    copy_sqlite_snapshot(project_db, incremental_snapshot)
    removed_dbs = remove_project_dbs(cache_dir)

    if args.transport == "mcp":
        with McpClient(binary, case_env, args.timeout) as client:
            full_rebuild = run_index_for_transport(
                args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs, client,
                index_mode=args.index_mode,
            )
    else:
        full_rebuild = run_index_for_transport(
            args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs,
            index_mode=args.index_mode,
        )

    full_db = find_project_db(cache_dir)
    canonical = compare_canonical_graph(incremental_snapshot, full_db, project)
    incremental_reason = incremental.get("exact_reason")
    publish_kind = incremental.get("publish_kind")
    active_overlay = None
    if publish_kind == PUBLISH_INCREMENTAL_OVERLAY:
        active_overlay = compare_active_overlay_graph(incremental_snapshot, full_db, project)
    graph_gate = graph_gate_for_publish_kind(
        canonical, str(publish_kind or ""), active_overlay=active_overlay
    )
    configured_cap = args.config_overrides.get("incremental_exact_max_affected_paths")
    try:
        exact_cap = int(configured_cap) if configured_cap is not None else None
    except ValueError:
        exact_cap = None
    frontier_gate = frontier_coverage_gate(scenario_metadata, incremental, exact_cap=exact_cap)
    explicit_route = is_explicit_incremental_route(publish_kind, incremental_reason)
    passed = bool(graph_gate.get("passed")) and bool(frontier_gate.get("passed")) and explicit_route
    speedup = max(1, int(full_rebuild["elapsed_ms"])) / max(1, int(incremental["elapsed_ms"]))
    return {
        "scenario": scenario,
        "project": project,
        "changed_paths": changed_paths,
        "scenario_metadata": scenario_metadata,
        "removed_project_dbs": removed_dbs,
        "initial_fast_full": initial,
        "incremental": incremental,
        "fresh_fast_full_after_change": full_rebuild,
        "canonical_graph": canonical,
        "active_overlay_graph": active_overlay,
        "graph_gate": graph_gate,
        "frontier_coverage_gate": frontier_gate,
        "explicit_exact_or_fallback": explicit_route,
        "explicit_incremental_route": explicit_route,
        "exact_reason": incremental_reason,
        "speedup_full_rebuild_over_incremental": speedup,
        "passed": passed,
    }


def run_matrix(args: argparse.Namespace, binary: Path) -> tuple[dict[str, Any], int]:
    auto_root = not bool(args.work_root)
    work_root = Path(args.work_root).expanduser() if args.work_root else Path(
        tempfile.mkdtemp(prefix="cbm-incr-matrix-")
    )
    work_root.mkdir(parents=True, exist_ok=True)
    scenarios = [item.strip() for item in args.matrix_scenarios.split(",") if item.strip()]
    report: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "binary": str(binary),
        "binary_metadata": binary_metadata(binary),
        "work_root": str(work_root),
        "mode": "matrix",
        "parameters": {
            "files": args.files,
            "functions_per_file": args.functions_per_file,
            "frontier_files": args.frontier_files,
            "rank_refresh": args.rank_refresh,
            "rank_refresh_override_applied": (
                args.rank_refresh != RANK_REFRESH_CANDIDATE_DEFAULT
            ),
            "index_mode": args.index_mode,
            "capability_applicability": index_mode_capability_applicability(args.index_mode),
            "config_profile": args.config_profile,
            "config_overrides": args.config_overrides,
            "timeout": args.timeout,
            "transport": args.transport,
            "scenarios": scenarios,
        },
        "cleanup": {"requested": auto_root and not args.keep_work_root, "removed": False},
        "cases": [],
    }
    exit_code = 1
    try:
        base_env = build_env(work_root / "cache-base")
        for scenario in scenarios:
            case = run_matrix_case(scenario, binary, base_env, work_root / scenario, args)
            report["cases"].append(case)
        report["derived"] = {
            "passed": all(bool(case.get("passed")) for case in report["cases"]),
            "case_count": len(report["cases"]),
        }
        exit_code = 0 if report["derived"]["passed"] else 1
    except Exception as exc:
        record_report_error(report, exc)
        exit_code = 1
    finally:
        if auto_root and not args.keep_work_root:
            shutil.rmtree(work_root, ignore_errors=True)
            report["cleanup"]["removed"] = not work_root.exists()
        if args.out:
            atomic_write_text(
                Path(args.out).expanduser(),
                json.dumps(report, indent=2, sort_keys=True) + "\n",
            )
        print(json.dumps(report, indent=2, sort_keys=True))
    return report, exit_code


def run_self_dogfood_case(
    scenario: str,
    source_repo: Path,
    binary: Path,
    case_root: Path,
    args: argparse.Namespace,
    revision: str,
) -> dict[str, Any]:
    cache_dir = case_root / SELF_DOGFOOD_CACHE_SUBDIR
    cache_dir.mkdir(parents=True, exist_ok=True)
    repo_dir = create_self_dogfood_worktree(
        source_repo, case_root, args.timeout, revision
    )
    case_env = build_env(cache_dir)
    cleanup: dict[str, Any] = {"requested": not args.keep_work_root, "removed": False}
    result: dict[str, Any] | None = None
    try:
        run_config_set(binary, case_env, "incremental_reindex", "always", args.timeout)
        apply_rank_refresh_override(binary, case_env, args.rank_refresh, args.timeout)
        apply_config_overrides(binary, case_env, args.config_overrides, args.timeout)
        if args.transport == "mcp":
            with McpClient(binary, case_env, args.timeout) as client:
                initial = run_index_for_transport(
                    args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs, client,
                    index_mode=args.index_mode,
                )
                mutation = mutate_self_dogfood_scenario(scenario, repo_dir)
                incremental = run_index_for_transport(
                    args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs, client,
                    index_mode=args.index_mode,
                )
                project_db = find_project_db(cache_dir)
                project = str(incremental.get("response", {}).get("project") or project_db.stem)
                oracles = run_self_dogfood_oracles(
                    args.transport, binary, case_env, project, mutation, args, client
                )
        else:
            initial = run_index_for_transport(
                args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs,
                index_mode=args.index_mode,
            )
            mutation = mutate_self_dogfood_scenario(scenario, repo_dir)
            incremental = run_index_for_transport(
                args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs,
                index_mode=args.index_mode,
            )
            project_db = find_project_db(cache_dir)
            project = str(incremental.get("response", {}).get("project") or project_db.stem)
            oracles = run_self_dogfood_oracles(
                args.transport, binary, case_env, project, mutation, args
            )

        incremental_snapshot = case_root / "incremental.db"
        copy_sqlite_snapshot(project_db, incremental_snapshot)
        removed_dbs = remove_project_dbs(cache_dir)
        if args.transport == "mcp":
            with McpClient(binary, case_env, args.timeout) as client:
                full_rebuild = run_index_for_transport(
                    args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs, client,
                    index_mode=args.index_mode,
                )
        else:
            full_rebuild = run_index_for_transport(
                args.transport, binary, case_env, repo_dir, args.timeout, args.include_logs,
                index_mode=args.index_mode,
            )
        full_db = find_project_db(cache_dir)
        canonical = compare_canonical_graph(incremental_snapshot, full_db, project)
        stale_views = declared_stale_views(oracles)
        freshness_scoped = compare_graph_excluding_declared_stale_views(
            incremental_snapshot, full_db, project, stale_views
        )
        publish_kind = incremental.get("publish_kind")
        incremental_reason = incremental.get("exact_reason")
        active_overlay = None
        if publish_kind == PUBLISH_INCREMENTAL_OVERLAY:
            active_overlay = compare_active_overlay_graph(incremental_snapshot, full_db, project)
        explicit_route = is_explicit_incremental_route(publish_kind, incremental_reason)
        speedup = max(1, int(full_rebuild["elapsed_ms"])) / max(1, int(incremental["elapsed_ms"]))
        graph_gate = graph_gate_for_publish_kind(
            canonical,
            str(publish_kind or ""),
            bool(oracles.get("passed")),
            active_overlay=active_overlay,
            freshness_scoped=freshness_scoped,
        )
        passed = bool(graph_gate.get("passed")) and explicit_route and bool(oracles.get("passed"))
        result = {
            "scenario": scenario,
            "project": project,
            "repo_dir": str(repo_dir),
            "mutation": mutation,
            "removed_project_dbs": removed_dbs,
            "initial_fast_full": initial,
            "incremental": incremental,
            "fresh_fast_full_after_change": full_rebuild,
            "canonical_graph": canonical,
            "freshness_scoped_graph": freshness_scoped,
            "active_overlay_graph": active_overlay,
            "graph_gate": graph_gate,
            "oracles": oracles,
            "explicit_incremental_route": explicit_route,
            "exact_reason": incremental_reason,
            "speedup_full_rebuild_over_incremental": speedup,
            "passed": passed,
        }
    finally:
        if not args.keep_work_root:
            cleanup = remove_self_dogfood_worktree(source_repo, repo_dir, args.timeout)
        if cache_dir.exists() and not args.keep_work_root:
            shutil.rmtree(cache_dir, ignore_errors=True)
            cleanup["cache_removed"] = not cache_dir.exists()
        cleanup["case_root_removed"] = False
        if not args.keep_work_root and case_root.exists():
            shutil.rmtree(case_root, ignore_errors=True)
            cleanup["case_root_removed"] = not case_root.exists()
    if result is None:
        raise RuntimeError(f"self-dogfood case did not produce a result: {scenario}")
    result["cleanup"] = cleanup
    return result


def run_self_dogfood(args: argparse.Namespace, binary: Path) -> tuple[dict[str, Any], int]:
    auto_root = not bool(args.work_root)
    work_root = Path(args.work_root).expanduser() if args.work_root else Path(
        tempfile.mkdtemp(prefix="cbm-self-dogfood-")
    )
    work_root.mkdir(parents=True, exist_ok=True)
    source_repo = resolve_git_repo_root(Path(args.repo_root), args.timeout)
    source_revision = command_stdout(
        ["git", "rev-parse", f"{args.repo_revision}^{{commit}}"],
        args.timeout,
        source_repo,
    )
    source_tree = command_stdout(
        ["git", "rev-parse", f"{source_revision}^{{tree}}"],
        args.timeout,
        source_repo,
    )
    scenarios = [item.strip() for item in args.self_dogfood_scenarios.split(",") if item.strip()]
    report: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "binary": str(binary),
        "binary_metadata": binary_metadata(binary),
        "work_root": str(work_root),
        "source_repo": str(source_repo),
        "source_git": git_metadata(source_repo, args.timeout),
        "repository_background": {
            "repo": str(source_repo),
            "revision": source_revision,
            "tree": source_tree,
            "source_dirty_status_short": command_stdout(
                ["git", "status", "--short"], args.timeout, source_repo
            ),
            "copy_policy": "detached_worktree_from_exact_commit",
        },
        "mode": "self_dogfood",
        "parameters": {
            "rank_refresh": args.rank_refresh,
            "rank_refresh_override_applied": (
                args.rank_refresh != RANK_REFRESH_CANDIDATE_DEFAULT
            ),
            "index_mode": args.index_mode,
            "capability_applicability": index_mode_capability_applicability(args.index_mode),
            "config_profile": args.config_profile,
            "config_overrides": args.config_overrides,
            "timeout": args.timeout,
            "transport": args.transport,
            "scenarios": scenarios,
            "repo_revision": source_revision,
        },
        "cleanup": {"requested": auto_root and not args.keep_work_root, "removed": False},
        "cases": [],
    }
    exit_code = 1
    try:
        for scenario in scenarios:
            case = run_self_dogfood_case(
                scenario,
                source_repo,
                binary,
                work_root / scenario,
                args,
                source_revision,
            )
            report["cases"].append(case)
        report["derived"] = {
            "passed": all(bool(case.get("passed")) for case in report["cases"]),
            "case_count": len(report["cases"]),
        }
        exit_code = 0 if report["derived"]["passed"] else 1
    except Exception as exc:
        record_report_error(report, exc)
        exit_code = 1
    finally:
        if auto_root and not args.keep_work_root:
            shutil.rmtree(work_root, ignore_errors=True)
            report["cleanup"]["removed"] = not work_root.exists()
        if args.out:
            atomic_write_text(
                Path(args.out).expanduser(),
                json.dumps(report, indent=2, sort_keys=True) + "\n",
            )
        print(json.dumps(report, indent=2, sort_keys=True))
    return report, exit_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gate exact fast-mode incremental indexing against a fresh full rebuild."
    )
    parser.add_argument("--binary", default="build/c/codebase-memory-mcp")
    parser.add_argument("--work-root", default="")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--out", default="")
    parser.add_argument("--files", type=int, default=DEFAULT_FILE_COUNT)
    parser.add_argument("--functions-per-file", type=int, default=DEFAULT_FUNCTIONS_PER_FILE)
    parser.add_argument("--changed-files", type=int, default=DEFAULT_CHANGED_FILES)
    parser.add_argument("--min-speedup", type=float, default=DEFAULT_MIN_SPEEDUP)
    parser.add_argument(
        "--index-mode",
        choices=INDEX_MODES,
        default="fast",
        help=(
            "Indexing mode for every compared run. Use full or moderate when measuring "
            "SIMILAR_TO or SEMANTICALLY_RELATED quality; fast intentionally skips both."
        ),
    )
    parser.add_argument(
        "--rank-refresh",
        choices=(
            RANK_REFRESH_CANDIDATE_DEFAULT,
            "eager",
            "stale_on_exact",
            "stale_on_incremental",
        ),
        default=DEFAULT_RANK_REFRESH,
        help=(
            "Preserve the candidate's compiled/configured default unless an explicit policy "
            "is selected. This is independent of --config-profile."
        ),
    )
    parser.add_argument(
        "--config-profile",
        choices=tuple(CONFIG_PROFILES),
        default=CONFIG_PROFILE_DEFAULT,
        help=(
            "Named, auditable capability profile. dependency_disabled changes only "
            "auto_index_deps for a controlled ablation; minimal_indexing disables dependency "
            "indexing plus every optional graph/rank pass. Repeated --config KEY=VALUE "
            "arguments take priority over the selected profile."
        ),
    )
    parser.add_argument(
        "--config",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Additional config override; repeat to set multiple keys. Applied after built-in settings.",
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--keep-work-root", action="store_true")
    parser.add_argument("--include-logs", action="store_true")
    parser.add_argument(
        "--mcp-surface-parity",
        action="store_true",
        help=(
            "Measure classic startup, streamlined pre-reveal, and streamlined post-reveal "
            "tool discovery without indexing a repository."
        ),
    )
    parser.add_argument(
        "--list-projects-scaling",
        action="store_true",
        help=(
            "Measure list_projects alone against isolated cloned project databases using "
            "a fresh MCP server per configured count."
        ),
    )
    parser.add_argument(
        "--list-project-counts",
        default=DEFAULT_LIST_PROJECT_COUNTS,
        help="Strictly increasing positive project counts for --list-projects-scaling.",
    )
    parser.add_argument(
        "--list-project-fixture-max-mb",
        type=int,
        default=DEFAULT_LIST_PROJECT_FIXTURE_MAX_MB,
        help="Hard disk cap for cloned list-project fixtures before any clone is created.",
    )
    parser.add_argument(
        "--search-projection",
        action="store_true",
        help=(
            "Compare compact default/true, selected fields, and compact=false JSON projection "
            "for identical ranked results."
        ),
    )
    parser.add_argument(
        "--search-projection-results",
        type=int,
        default=30,
        help="Bounded matching result count for --search-projection.",
    )
    parser.add_argument(
        "--capability-quality",
        choices=CAPABILITY_QUALITY_CASES,
        default="",
        help=(
            "Run one isolated, deterministic capability-quality fixture. rank measures whether "
            "structural ranking lifts the central result above lexical decoys; dependencies "
            "measures local npm API retrieval with source/package/read-only provenance; similarity "
            "scores SIMILAR_TO structural-clone pairs and semantic_edges scores "
            "SEMANTICALLY_RELATED control-flow variants against explicit hard negatives."
        ),
    )
    parser.add_argument(
        "--quality-background-repo",
        default="",
        help=(
            "Optional Git repository whose tracked files at an exact revision form the realistic "
            "background for similarity or semantic_edges canaries. Dirty and untracked source "
            "state is excluded."
        ),
    )
    parser.add_argument(
        "--quality-background-revision",
        default="",
        help="Commit-ish copied by git archive for --quality-background-repo; campaigns should use a full hash.",
    )
    parser.add_argument("--matrix", action="store_true", help="Run the affected-frontier scenario matrix.")
    parser.add_argument(
        "--self-dogfood",
        action="store_true",
        help="Run isolated edit-loop scenarios against a detached worktree of --repo-root.",
    )
    parser.add_argument(
        "--repo-revision",
        default="HEAD",
        help=(
            "Exact commit used for --self-dogfood detached worktrees. Campaigns should pass "
            "a full hash so mutable source HEAD cannot change the measured corpus."
        ),
    )
    parser.add_argument(
        "--matrix-scenarios",
        default=MATRIX_SCENARIOS_DEFAULT,
        help="Comma-separated matrix scenarios to run.",
    )
    parser.add_argument(
        "--frontier-files",
        type=int,
        default=DEFAULT_FRONTIER_FILES,
        help=(
            "Number of inbound-dependent source files created by each *_inbound_frontier "
            "matrix scenario. The changed definition file is additional."
        ),
    )
    parser.add_argument(
        "--fastapi-repo",
        default="",
        help=(
            "Existing FastAPI checkout for matrix scenario fastapi_insert_probe. "
            "Defaults also check CBM_FASTAPI_REPO and common local cache/source paths."
        ),
    )
    parser.add_argument(
        "--fastapi-url",
        default=DEFAULT_FASTAPI_URL,
        help="Clone URL used only with --clone-missing-real-repos.",
    )
    parser.add_argument(
        "--clone-missing-real-repos",
        action="store_true",
        help="Clone missing real benchmark repos into the isolated work root.",
    )
    parser.add_argument(
        "--self-dogfood-scenarios",
        default=SELF_DOGFOOD_SCENARIOS_DEFAULT,
        help="Comma-separated real-repo edit-loop scenarios to run.",
    )
    parser.add_argument(
        "--transport",
        choices=("cli", "mcp"),
        default="cli",
        help="Measure cold CLI subprocess calls or persistent MCP tool-call latency.",
    )
    parser.add_argument(
        "--overhead-probes",
        type=int,
        default=DEFAULT_OVERHEAD_PROBES,
        help=(
            "Run N cheap tool-call probes before indexing to estimate invocation overhead; "
            "0 preserves the historical gate behavior."
        ),
    )
    parser.add_argument(
        "--overhead-tool",
        default=DEFAULT_OVERHEAD_TOOL,
        help="Existing MCP tool used by --overhead-probes.",
    )
    args = parser.parse_args()
    args.config_overrides = resolve_config_overrides(args.config_profile, args.config)
    return args


def resolve_binary_path(binary_arg: str) -> Path:
    binary = Path(binary_arg).expanduser()
    if binary.is_absolute():
        return binary.resolve()
    cwd_candidate = (Path.cwd() / binary).resolve()
    if cwd_candidate.is_file():
        return cwd_candidate
    script_candidate = (Path(__file__).resolve().parents[1] / binary).resolve()
    return script_candidate


def main() -> int:
    args = parse_args()
    binary = resolve_binary_path(args.binary)
    if not binary.is_file():
        print(f"error: binary not found: {binary}", file=sys.stderr)
        return 2
    if args.list_projects_scaling:
        _, list_exit_code = run_list_projects_scaling(args, binary)
        return list_exit_code
    if args.search_projection:
        _, projection_exit_code = run_search_projection(args, binary)
        return projection_exit_code
    if args.mcp_surface_parity:
        _, surface_exit_code = run_mcp_surface_parity(args, binary)
        return surface_exit_code
    if args.capability_quality:
        _, quality_exit_code = run_capability_quality(args, binary)
        return quality_exit_code
    if args.matrix:
        _, matrix_exit_code = run_matrix(args, binary)
        return matrix_exit_code
    if args.self_dogfood:
        _, self_dogfood_exit_code = run_self_dogfood(args, binary)
        return self_dogfood_exit_code

    auto_root = not bool(args.work_root)
    work_root = Path(args.work_root).expanduser() if args.work_root else Path(
        tempfile.mkdtemp(prefix="cbm-incr-speed-")
    )
    work_root.mkdir(parents=True, exist_ok=True)
    repo_dir = work_root / "repo"
    cache_dir = work_root / "cache"
    repo_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    report: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "binary": str(binary),
        "binary_metadata": binary_metadata(binary),
        "work_root": str(work_root),
        "parameters": {
            "files": args.files,
            "functions_per_file": args.functions_per_file,
            "changed_files": args.changed_files,
            "min_speedup": args.min_speedup,
            "rank_refresh": args.rank_refresh,
            "rank_refresh_override_applied": (
                args.rank_refresh != RANK_REFRESH_CANDIDATE_DEFAULT
            ),
            "index_mode": args.index_mode,
            "capability_applicability": index_mode_capability_applicability(args.index_mode),
            "config_profile": args.config_profile,
            "config_overrides": args.config_overrides,
            "timeout": args.timeout,
            "transport": args.transport,
            "overhead_probes": args.overhead_probes,
            "overhead_tool": args.overhead_tool,
        },
        "cleanup": {"requested": auto_root and not args.keep_work_root, "removed": False},
    }

    exit_code = 1
    try:
        create_repo(repo_dir, args.files, args.functions_per_file)
        env = build_env(cache_dir)
        run_config_set(binary, env, "incremental_reindex", "always", args.timeout)
        apply_rank_refresh_override(binary, env, args.rank_refresh, args.timeout)
        apply_config_overrides(binary, env, args.config_overrides, args.timeout)

        if args.transport == "mcp":
            with McpClient(binary, env, args.timeout) as client:
                overhead_probe = measure_mcp_overhead_probes(
                    client, args.overhead_tool, args.overhead_probes, args.include_logs
                )
                initial = run_index_mcp(client, repo_dir, args.include_logs, args.index_mode)
                changed_paths = modify_existing_files(
                    repo_dir, args.changed_files, args.functions_per_file
                )
                incremental = run_index_mcp(client, repo_dir, args.include_logs, args.index_mode)
            removed_dbs = remove_project_dbs(cache_dir)
            with McpClient(binary, env, args.timeout) as client:
                full_rebuild = run_index_mcp(
                    client, repo_dir, args.include_logs, args.index_mode
                )
        else:
            overhead_probe = measure_cli_overhead_probes(
                binary,
                env,
                args.overhead_tool,
                args.overhead_probes,
                args.timeout,
                args.include_logs,
            )
            initial = run_index(
                binary, env, repo_dir, args.timeout, args.include_logs, args.index_mode
            )
            changed_paths = modify_existing_files(
                repo_dir, args.changed_files, args.functions_per_file
            )
            incremental = run_index(
                binary, env, repo_dir, args.timeout, args.include_logs, args.index_mode
            )
            removed_dbs = remove_project_dbs(cache_dir)
            full_rebuild = run_index(
                binary, env, repo_dir, args.timeout, args.include_logs, args.index_mode
            )

        incr_ms = max(1, int(incremental["elapsed_ms"]))
        full_ms = max(1, int(full_rebuild["elapsed_ms"]))
        speedup = full_ms / incr_ms
        incremental_markers = incremental["markers"]
        explicit_incremental_route = is_incremental_publish_kind(
            str(incremental.get("publish_kind") or "")
        )
        defer_marker = bool(incremental_markers["pagerank_defer"])
        passed = speedup >= args.min_speedup and explicit_incremental_route

        report.update(
            {
                "changed_paths": changed_paths,
                "removed_project_dbs": removed_dbs,
                "measurements": {
                    "overhead_probe": overhead_probe,
                    "initial_fast_full": initial,
                    "incremental_exact": incremental,
                    "incremental": incremental,
                    "fresh_fast_full_after_change": full_rebuild,
                },
                "derived": {
                    "speedup_full_rebuild_over_incremental": speedup,
                    "exact_incremental_marker_seen": explicit_incremental_route,
                    "explicit_incremental_route_seen": explicit_incremental_route,
                    "rank_defer_marker_seen": defer_marker,
                    "passed": passed,
                },
            }
        )
        exit_code = 0 if passed else 1
    except Exception as exc:
        record_report_error(report, exc)
        exit_code = 1
    finally:
        if auto_root and not args.keep_work_root:
            shutil.rmtree(work_root, ignore_errors=True)
            report["cleanup"]["removed"] = not work_root.exists()
        if args.out:
            atomic_write_text(
                Path(args.out).expanduser(),
                json.dumps(report, indent=2, sort_keys=True) + "\n",
            )
        print(json.dumps(report, indent=2, sort_keys=True))

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Create or run an auditable PageRank tuning experiment.

This compatibility frontend uses the repository's versioned rank-quality fixture
and content-addressed experiment runner. It never changes the user's normal CBM
configuration or cache, and it retains every result under an ignored durable
experiment root rather than an operating-system temporary directory.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
BENCHMARK = ROOT / "scripts" / "benchmark-incremental-speed.py"
EXPERIMENT_RUNNER = ROOT / "scripts" / "run-benchmark-experiments.py"
DEFAULT_EXPERIMENT_ROOT = ROOT / ".worktrees" / "benchmark-experiments" / "autotune"

# Each row is an independently identified experiment profile. The first two are
# the essential capability ablation; the remaining rows preserve the useful
# parameter sweep from the former global-config autotuner.
TUNING_PROFILES: tuple[dict[str, Any], ...] = (
    {
        "label": "candidate-default",
        "config_profile": "automatic_dependency_source_indexing_disabled",
        "capabilities": {"rank_enabled": "candidate_default"},
    },
    {
        "label": "rank-disabled",
        "config_profile": "rank_disabled",
        "capabilities": {"rank_enabled": "false"},
    },
    {
        "label": "calls-boost",
        "config_profile": "automatic_dependency_source_indexing_disabled",
        "capabilities": {"rank_enabled": "true"},
        "config_overrides": {"edge_weight_calls": "2.0", "edge_weight_usage": "0.3"},
    },
    {
        "label": "usage-dampen",
        "config_profile": "automatic_dependency_source_indexing_disabled",
        "capabilities": {"rank_enabled": "true"},
        "config_overrides": {"edge_weight_usage": "0.3", "edge_weight_defines": "0.05"},
    },
    {
        "label": "tests-dampen",
        "config_profile": "automatic_dependency_source_indexing_disabled",
        "capabilities": {"rank_enabled": "true"},
        "config_overrides": {"edge_weight_tests": "0.01", "edge_weight_usage": "0.3"},
    },
    {
        "label": "calls-boost-tests-dampen",
        "config_profile": "automatic_dependency_source_indexing_disabled",
        "capabilities": {"rank_enabled": "true"},
        "config_overrides": {
            "edge_weight_calls": "2.0",
            "edge_weight_usage": "0.3",
            "edge_weight_tests": "0.01",
        },
    },
    {
        "label": "more-iterations",
        "config_profile": "automatic_dependency_source_indexing_disabled",
        "capabilities": {"rank_enabled": "true"},
        "config_overrides": {"pagerank_max_iter": "100"},
    },
)


def load_experiment_runner(path: Path = EXPERIMENT_RUNNER) -> ModuleType:
    spec = importlib.util.spec_from_file_location("cbm_benchmark_experiment", path)
    if not spec or not spec.loader:
        raise RuntimeError(f"cannot load experiment runner: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def git_revision(repo: Path) -> str:
    proc = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        text=True,
        capture_output=True,
        check=False,
    )
    revision = proc.stdout.strip()
    if proc.returncode != 0 or len(revision) != 40:
        raise ValueError(
            f"cannot resolve a full Git revision for {repo}: {proc.stderr.strip()}"
        )
    return revision


def build_matrix_spec(
    *,
    binary: Path,
    revision: str,
    repetitions: int,
    timeout_seconds: int,
    transports: list[str],
    build: dict[str, str],
) -> dict[str, Any]:
    if not binary.is_file():
        raise ValueError(f"binary does not exist: {binary}")
    if len(revision) != 40:
        raise ValueError("revision must be a full 40-character commit hash")
    if repetitions <= 0 or timeout_seconds <= 0:
        raise ValueError("repetitions and timeout_seconds must be positive")
    if not transports or not set(transports).issubset({"cli", "mcp"}):
        raise ValueError("transports must contain cli, mcp, or both")
    for key in ("target", "compiler", "cflags"):
        if not build.get(key):
            raise ValueError(f"build metadata requires non-empty {key}")

    runner = load_experiment_runner()
    return {
        "schema_version": 1,
        "harness_version": f"benchmark-incremental-speed.py:{runner.file_sha256(BENCHMARK)}",
        "benchmark_script": str(BENCHMARK),
        "capability_quality": "rank",
        "index_mode": "full",
        "cwd": str(ROOT),
        "timeout_seconds": timeout_seconds,
        "cell_timeout_seconds": timeout_seconds * 4,
        "accepted_exit_codes": [0, 1],
        "execution_order": "paired_interleaved",
        "repetitions": repetitions,
        "transports": transports,
        "candidates": [
            {
                "label": "candidate",
                "revision": revision,
                "binary": str(binary.resolve()),
                "build": dict(sorted(build.items())),
                "capability_support": {"rank": True},
            }
        ],
        "profiles": [dict(profile) for profile in TUNING_PROFILES],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary", type=Path, default=ROOT / "build" / "c" / "codebase-memory-mcp"
    )
    parser.add_argument(
        "--revision",
        default="",
        help="Full candidate commit; defaults to repository HEAD.",
    )
    parser.add_argument(
        "--experiment-root",
        "--campaign-root",
        dest="experiment_root",
        type=Path,
        default=DEFAULT_EXPERIMENT_ROOT,
        help="Durable result root (--campaign-root is a legacy alias).",
    )
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=1200)
    parser.add_argument("--transport", choices=("cli", "mcp", "both"), default="both")
    parser.add_argument(
        "--build-target",
        required=True,
        help="Exact build command/target used for the binary.",
    )
    parser.add_argument(
        "--compiler", required=True, help="Exact compiler identity/version."
    )
    parser.add_argument(
        "--cflags", required=True, help="Exact optimization/profiling flags."
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Write and validate the plan without running cells.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    binary = args.binary.expanduser().resolve()
    revision = args.revision or git_revision(ROOT)
    transports = ["cli", "mcp"] if args.transport == "both" else [args.transport]
    build = {
        "target": args.build_target,
        "compiler": args.compiler,
        "cflags": args.cflags,
    }
    spec = build_matrix_spec(
        binary=binary,
        revision=revision,
        repetitions=args.repetitions,
        timeout_seconds=args.timeout,
        transports=transports,
        build=build,
    )

    runner = load_experiment_runner()
    plan = runner.expand_matrix_spec(spec)
    experiment_root = args.experiment_root.expanduser().resolve()
    runner.validate_experiment_root(experiment_root)
    experiment_root.mkdir(parents=True, exist_ok=True)
    spec_path = experiment_root / "autotune-matrix-spec.json"
    plan_path = experiment_root / "autotune-plan.json"
    runner.atomic_write_json(spec_path, spec)
    runner.atomic_write_json(plan_path, plan)
    if args.plan_only:
        print(
            json.dumps(
                {"matrix_spec": str(spec_path), "plan": str(plan_path)}, indent=2
            )
        )
        return 0

    os.execv(
        sys.executable,
        [
            sys.executable,
            str(EXPERIMENT_RUNNER),
            "--plan",
            str(plan_path),
            "--experiment-root",
            str(experiment_root),
        ],
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

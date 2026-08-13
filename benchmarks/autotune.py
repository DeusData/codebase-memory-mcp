#!/usr/bin/env python3
"""Create or run an auditable PageRank tuning experiment.

This compatibility frontend uses the repository's versioned rank-quality fixture
and content-addressed experiment runner. It never changes the user's normal CBM
configuration or cache, and it retains every result under an ignored durable
experiment root rather than an operating-system temporary directory.
"""

from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
BENCHMARK = ROOT / "benchmarks" / "run_benchmark.py"
EXPERIMENT_RUNNER = ROOT / "benchmarks" / "run_experiments.py"
DEFAULT_EXPERIMENT_ROOT = ROOT / ".worktrees" / "benchmark-experiments" / "autotune"

try:  # Package import under tests; script-local import for direct execution.
    from benchmarks import rank_hypotheses as rank_evidence
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    import rank_hypotheses as rank_evidence


def tuning_profiles() -> tuple[dict[str, Any], ...]:
    """Return the DRY hypothesis profiles while retaining the v1 baseline label."""
    profiles = rank_evidence.quick_hypothesis_profiles()
    profiles[0]["label"] = "candidate-default"
    profiles[0]["cell_name"] = "RANK-CELL-01 — candidate default"
    return tuple(profiles)


# Compatibility export used by existing callers/tests. Values come from the canonical
# product declarations read by rank_hypotheses.py, not duplicated literals here.
TUNING_PROFILES = tuning_profiles()


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
    profiles: tuple[dict[str, Any], ...] | list[dict[str, Any]] | None = None,
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
    selected_profiles = list(profiles or TUNING_PROFILES)
    if not selected_profiles:
        raise ValueError("profiles must be non-empty")
    return {
        "schema_version": 1,
        "harness_version": f"run_benchmark.py:{runner.file_sha256(BENCHMARK)}",
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
        "profiles": [dict(profile) for profile in selected_profiles],
        "suite_name": rank_evidence.RANK_SUITE_NAME,
        "evidence_contract": rank_evidence.public_hypothesis_registry(),
    }


def write_spec_and_plan(
    runner: ModuleType,
    experiment_root: Path,
    spec: dict[str, Any],
    *,
    stem: str,
) -> tuple[Path, Path, dict[str, Any]]:
    plan = runner.expand_matrix_spec(spec)
    spec_path = experiment_root / f"{stem}-matrix-spec.json"
    plan_path = experiment_root / f"{stem}-plan.json"
    runner.atomic_write_json(spec_path, spec)
    runner.atomic_write_json(plan_path, plan)
    return spec_path, plan_path, plan


def build_preflight_spec(spec: dict[str, Any]) -> dict[str, Any]:
    """Reuse the first real cell as a fail-fast candidate/protocol preflight.

    This does not add an experiment or alter cell identity. A successful cell is
    content-addressed and reused when the complete plan runs; a failed cell prevents the
    remaining profiles from repeating the same environment or protocol failure.
    """
    profiles = spec.get("profiles")
    if not isinstance(profiles, list) or not profiles:
        raise ValueError("autotune preflight requires at least one profile")
    preflight = copy.deepcopy(spec)
    preflight["profiles"] = preflight["profiles"][:1]
    return preflight


def run_plan(plan_path: Path, experiment_root: Path) -> int:
    process = subprocess.run(
        [
            sys.executable,
            str(EXPERIMENT_RUNNER),
            "--plan",
            str(plan_path),
            "--experiment-root",
            str(experiment_root),
        ],
        cwd=ROOT,
        check=False,
    )
    return process.returncode


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
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
    parser.add_argument("--repetitions", type=int)
    parser.add_argument("--timeout", type=int, default=1200)
    parser.add_argument("--transport", choices=("cli", "mcp", "both"))
    parser.add_argument(
        "--build-target",
        required=True,
        help="Exact build command/target used for the measured binary.",
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
    parser.add_argument(
        "--profile",
        action="append",
        default=[],
        help="Run only this named profile; repeat in desired priority order.",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help=(
            "Run each fixed rank-evidence profile once over MCP. Actual duration is "
            "recorded; the suite has no wall-clock target or cutoff."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    binary = args.binary.expanduser().resolve()
    revision = args.revision or git_revision(ROOT)
    transport = args.transport or ("mcp" if args.quick else "both")
    repetitions = args.repetitions or (1 if args.quick else 3)
    transports = ["cli", "mcp"] if transport == "both" else [transport]
    build = {
        "target": args.build_target,
        "compiler": args.compiler,
        "cflags": args.cflags,
    }
    profiles_by_label = {profile["label"]: profile for profile in TUNING_PROFILES}
    unknown = [label for label in args.profile if label not in profiles_by_label]
    if unknown:
        raise ValueError(
            f"unknown profile(s) {unknown}; available: {', '.join(profiles_by_label)}"
        )
    requested_profiles = (
        tuple(profiles_by_label[label] for label in args.profile) or TUNING_PROFILES
    )
    runner = load_experiment_runner()
    experiment_root = args.experiment_root.expanduser().resolve()
    runner.validate_experiment_root(experiment_root)
    experiment_root.mkdir(parents=True, exist_ok=True)

    spec = build_matrix_spec(
        binary=binary,
        revision=revision,
        repetitions=repetitions,
        timeout_seconds=args.timeout,
        transports=transports,
        build=build,
        profiles=requested_profiles,
    )
    spec_path, plan_path, _plan = write_spec_and_plan(
        runner, experiment_root, spec, stem="autotune"
    )
    if args.plan_only:
        print(
            json.dumps(
                {"matrix_spec": str(spec_path), "plan": str(plan_path)}, indent=2
            )
        )
        return 0

    if args.quick:
        preflight_spec = build_preflight_spec(spec)
        _, preflight_plan_path, _ = write_spec_and_plan(
            runner, experiment_root, preflight_spec, stem="autotune-preflight"
        )
        preflight_status = run_plan(preflight_plan_path, experiment_root)
        if preflight_status != 0:
            print(
                json.dumps(
                    {
                        "status": "preflight_failed",
                        "remaining_cells_started": False,
                        "plan": str(preflight_plan_path),
                        "guidance": (
                            "Inspect the preserved attempt error. If it reports an "
                            "exact-build or cache-cohort conflict, close active CBM "
                            "sessions and rerun from a standalone terminal, or execute "
                            "the campaign in its isolated container environment."
                        ),
                    },
                    indent=2,
                ),
                file=sys.stderr,
            )
            return preflight_status

    return run_plan(plan_path, experiment_root)


if __name__ == "__main__":
    raise SystemExit(main())

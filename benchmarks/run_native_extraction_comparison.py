#!/usr/bin/env python3
"""Compare production-native extraction runners with interleaved samples."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shlex
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


STARTUP_SUITE = "startup_baseline"
SUITES = (STARTUP_SUITE, "shared_parse_baseline", "cs_lsp_bench", "py_lsp_bench", "py_lsp_scale")
OUTPUT_METRICS = (
    "lines",
    "definitions",
    "calls",
    "resolved",
    "usages",
    "type_refs",
    "read_write",
)
PERFORMANCE_METRICS = (
    "latency_ms",
    "small_ms",
    "medium_ms",
    "parse_ms",
    "non_lsp_ms",
    "lsp_ms",
    "total_extract_ms",
    "max_rss_bytes",
    "peak_footprint_bytes",
    "incremental_max_rss_bytes",
    "incremental_peak_footprint_bytes",
    "incremental_instructions",
    "incremental_cycles",
    "instructions",
    "cycles",
)
FIXED_RESULT_RE = re.compile(
    r"(?:cs )?bench: (?P<lines>\d+) lines, (?P<definitions>\d+) defs, "
    r"(?P<calls>\d+) calls, (?P<resolved>\d+) resolved \(\d+%\), "
    r"(?:\d+ high-conf \(\d+%\), )?"
    r"(?P<usages>\d+) usages, (?P<type_refs>\d+) type_refs, "
    r"(?P<read_write>\d+) rw, (?P<latency_ms>[0-9.]+) ms"
)
LEGACY_FIXED_RESULT_RE = re.compile(
    r"(?:cs )?bench: (?P<lines>\d+) lines, "
    r"(?P<calls>\d+) calls, (?P<resolved>\d+) resolved \(\d+%\), "
    r"(?:\d+ high-conf \(\d+%\), )?"
    r"(?P<latency_ms>[0-9.]+) ms"
)
SCALE_RESULT_RE = re.compile(
    r"scale: 100=(?P<small_ms>[0-9.]+)ms .*?"
    r"500=(?P<medium_ms>[0-9.]+)ms .*?"
    r"2000=(?P<latency_ms>[0-9.]+)ms "
    r"\(calls=(?P<calls>\d+) resolved=(?P<resolved>\d+)\)",
    re.DOTALL,
)
PROFILE_RE = re.compile(
    r"profile: files=(?P<files>\d+) parse=(?P<parse_ms>[0-9.]+)ms "
    r"non_lsp=(?P<non_lsp_ms>[0-9.]+)ms lsp=(?P<lsp_ms>[0-9.]+)ms "
    r"total_extract=(?P<total_extract_ms>[0-9.]+)ms"
)
DARWIN_RESOURCE_PATTERNS = {
    "max_rss_bytes": r"(?m)^\s*(\d+)\s+maximum resident set size$",
    "instructions": r"(?m)^\s*(\d+)\s+instructions retired$",
    "cycles": r"(?m)^\s*(\d+)\s+cycles elapsed$",
    "peak_footprint_bytes": r"(?m)^\s*(\d+)\s+peak memory footprint$",
}
LINUX_MAX_RSS_RE = re.compile(r"Maximum resident set size \(kbytes\):\s*(\d+)")


def parse_candidate(value: str) -> tuple[str, Path]:
    name, separator, raw_path = value.partition("=")
    if not separator or not name or not raw_path:
        raise argparse.ArgumentTypeError("candidate must be NAME=/absolute/path/to/runner")
    path = Path(raw_path).expanduser().resolve()
    if not path.is_file():
        raise argparse.ArgumentTypeError(f"candidate runner does not exist: {path}")
    return name, path


def _numeric_groups(match: re.Match[str]) -> dict[str, int | float]:
    parsed: dict[str, int | float] = {}
    for key, value in match.groupdict().items():
        parsed[key] = float(value) if key.endswith("_ms") else int(value)
    return parsed


def _parse_resource_metrics(output: str) -> dict[str, int]:
    metrics: dict[str, int] = {}
    for key, pattern in DARWIN_RESOURCE_PATTERNS.items():
        match = re.search(pattern, output)
        if match is not None:
            metrics[key] = int(match.group(1))
    linux_rss = LINUX_MAX_RSS_RE.search(output)
    if linux_rss is not None and "max_rss_bytes" not in metrics:
        metrics["max_rss_bytes"] = int(linux_rss.group(1)) * 1024
    return metrics


def parse_sample(suite: str, output: str) -> dict[str, int | float]:
    resource_metrics = _parse_resource_metrics(output)
    if suite == STARTUP_SUITE:
        if not resource_metrics:
            raise ValueError("could not parse startup resource metrics")
        return resource_metrics

    result_match = (SCALE_RESULT_RE if suite == "py_lsp_scale" else FIXED_RESULT_RE).search(output)
    if result_match is None and suite != "py_lsp_scale":
        result_match = LEGACY_FIXED_RESULT_RE.search(output)
    profile_match = PROFILE_RE.search(output)
    if result_match is None or profile_match is None:
        raise ValueError(f"could not parse {suite} benchmark output")

    metrics = _numeric_groups(result_match)
    metrics.update(_numeric_groups(profile_match))
    metrics.update(resource_metrics)
    return metrics


def timed_command(runner: Path, suite: str) -> list[str]:
    selector = "__cbm_startup_baseline__" if suite == STARTUP_SUITE else suite
    system = platform.system()
    if Path("/usr/bin/time").is_file() and system == "Darwin":
        return ["/usr/bin/time", "-lp", str(runner), selector]
    if Path("/usr/bin/time").is_file() and system == "Linux":
        return ["/usr/bin/time", "-v", str(runner), selector]
    return [str(runner), selector]


def run_sample(runner: Path, suite: str, timeout_seconds: float) -> tuple[dict[str, int | float], str]:
    command = timed_command(runner, suite)
    environment = os.environ.copy()
    environment["CBM_PROFILE"] = "1"
    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout_seconds,
        env=environment,
    )
    output = completed.stdout
    expected_returncodes = {0, 2} if suite == STARTUP_SUITE else {0}
    if completed.returncode not in expected_returncodes:
        raise RuntimeError(
            f"benchmark failed with exit {completed.returncode}: "
            f"{shlex.join(command)}\n{output}"
        )
    return parse_sample(suite, output), output


def add_incremental_resource_metrics(samples: list[dict[str, Any]]) -> None:
    """Subtract each process's matching no-work startup resource counters."""
    startup_by_run = {
        (sample["candidate"], sample["repetition"]): sample["metrics"]
        for sample in samples
        if sample["suite"] == STARTUP_SUITE
    }
    for sample in samples:
        if sample["suite"] == STARTUP_SUITE:
            continue
        startup = startup_by_run.get((sample["candidate"], sample["repetition"]))
        if startup is None:
            continue
        for metric in (
            "max_rss_bytes",
            "peak_footprint_bytes",
            "instructions",
            "cycles",
        ):
            if metric in sample["metrics"] and metric in startup:
                sample["metrics"][f"incremental_{metric}"] = max(
                    0, sample["metrics"][metric] - startup[metric]
                )


def summarize(samples: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for candidate in sorted({sample["candidate"] for sample in samples}):
        summary[candidate] = {}
        for suite in SUITES:
            selected = [
                sample["metrics"]
                for sample in samples
                if sample["candidate"] == candidate and sample["suite"] == suite
            ]
            keys = sorted(set.intersection(*(set(metrics) for metrics in selected)))
            summary[candidate][suite] = {
                key: statistics.median(metrics[key] for metrics in selected) for key in keys
            }
    return summary


def compare_candidates(samples: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = sorted({sample["candidate"] for sample in samples})
    by_run = {
        (sample["candidate"], sample["suite"], sample["repetition"]): sample["metrics"]
        for sample in samples
    }
    comparisons: dict[str, Any] = {}
    for candidate in candidates:
        for baseline in candidates:
            if candidate == baseline:
                continue
            comparison_key = f"{candidate}_vs_{baseline}"
            comparisons[comparison_key] = {}
            for suite in SUITES:
                candidate_runs = sorted(
                    repetition
                    for name, sample_suite, repetition in by_run
                    if name == candidate and sample_suite == suite
                )
                paired = [
                    (
                        by_run[(candidate, suite, repetition)],
                        by_run[(baseline, suite, repetition)],
                    )
                    for repetition in candidate_runs
                    if (baseline, suite, repetition) in by_run
                ]
                output_differences: dict[str, Any] = {}
                for metric in OUTPUT_METRICS:
                    values = {
                        (candidate_metrics.get(metric), baseline_metrics.get(metric))
                        for candidate_metrics, baseline_metrics in paired
                    }
                    if any(left != right for left, right in values):
                        output_differences[metric] = [
                            {"candidate": left, "baseline": right}
                            for left, right in sorted(values, key=repr)
                        ]

                percent_changes: dict[str, float] = {}
                for metric in PERFORMANCE_METRICS:
                    changes = [
                        (candidate_metrics[metric] - baseline_metrics[metric])
                        / baseline_metrics[metric]
                        * 100.0
                        for candidate_metrics, baseline_metrics in paired
                        if metric in candidate_metrics
                        and metric in baseline_metrics
                        and baseline_metrics[metric] != 0
                    ]
                    if changes:
                        percent_changes[metric] = statistics.median(changes)

                comparisons[comparison_key][suite] = {
                    "paired_samples": len(paired),
                    "output_parity": not output_differences,
                    "output_differences": output_differences,
                    "paired_median_percent_change": percent_changes,
                }
    return comparisons


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--candidate",
        action="append",
        required=True,
        type=parse_candidate,
        metavar="NAME=RUNNER",
        help="named production-native benchmark executable; repeat for each candidate",
    )
    parser.add_argument("--repetitions", type=int, default=15)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.repetitions < 1 or args.warmups < 0:
        parser.error("repetitions must be positive and warmups must be non-negative")
    names = [candidate[0] for candidate in args.candidate]
    if len(names) != len(set(names)):
        parser.error("candidate names must be unique")

    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_path = output_dir / "native-extraction-raw.txt"
    json_path = output_dir / "native-extraction-results.json"

    candidates = list(args.candidate)
    for suite_index, suite in enumerate(SUITES):
        for warmup in range(args.warmups):
            offset = (suite_index + warmup) % len(candidates)
            for _, runner in candidates[offset:] + candidates[:offset]:
                run_sample(runner, suite, args.timeout_seconds)

    samples: list[dict[str, Any]] = []
    with raw_path.open("w", encoding="utf-8") as raw:
        for suite_index, suite in enumerate(SUITES):
            for repetition in range(args.repetitions):
                offset = (suite_index + repetition) % len(candidates)
                ordered = candidates[offset:] + candidates[:offset]
                for candidate, runner in ordered:
                    metrics, output = run_sample(runner, suite, args.timeout_seconds)
                    sample = {
                        "candidate": candidate,
                        "suite": suite,
                        "repetition": repetition + 1,
                        "metrics": metrics,
                    }
                    samples.append(sample)
                    raw.write(
                        f"BENCH candidate={candidate} suite={suite} "
                        f"run={repetition + 1}\n{output.rstrip()}\n"
                    )
                    raw.flush()
                    print(
                        f"{suite} {repetition + 1}/{args.repetitions} {candidate}: "
                        + (
                            f"{metrics['latency_ms']:.3f} ms"
                            if "latency_ms" in metrics
                            else "resource baseline"
                        ),
                        flush=True,
                    )

    add_incremental_resource_metrics(samples)
    result = {
        "schema_version": 2,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "repetitions": args.repetitions,
        "warmups": args.warmups,
        "candidates": {name: str(path) for name, path in candidates},
        "samples": samples,
        "medians": summarize(samples),
        "comparisons": compare_candidates(samples),
    }
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"raw: {raw_path}")
    print(f"results: {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

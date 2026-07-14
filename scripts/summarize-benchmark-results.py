#!/usr/bin/env python3
"""Aggregate existing CBM benchmark JSON into a quality-first Markdown table."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return float(ordered[index])


def ratio(passed: int, applicable: int) -> str:
    return f"{passed}/{applicable}" if applicable else "n/a"


def cases_from_report(report: dict[str, Any]) -> list[dict[str, Any]]:
    cases = report.get("cases")
    if isinstance(cases, list):
        return [case for case in cases if isinstance(case, dict)]
    measurements = report.get("measurements")
    derived = report.get("derived")
    if isinstance(measurements, dict) and isinstance(derived, dict):
        return [
            {
                "passed": derived.get("passed"),
                "incremental": measurements.get("incremental", {}),
                "fresh_fast_full_after_change": measurements.get(
                    "fresh_fast_full_after_change", {}
                ),
                "speedup_full_rebuild_over_incremental": derived.get(
                    "speedup_full_rebuild_over_incremental"
                ),
            }
        ]
    return []


def config_label(reports: list[dict[str, Any]]) -> str:
    labels: set[str] = set()
    for report in reports:
        parameters = report.get("parameters", {})
        overrides = parameters.get("config_overrides", {}) if isinstance(parameters, dict) else {}
        if isinstance(overrides, dict) and overrides:
            labels.add(", ".join(f"{key}={overrides[key]}" for key in sorted(overrides)))
        else:
            labels.add("defaults")
    return " / ".join(sorted(labels))


def summarize_group(label: str, reports: list[dict[str, Any]]) -> dict[str, Any]:
    cases = [case for report in reports for case in cases_from_report(report)]
    canonical = [
        bool(case["canonical_graph"].get("equal"))
        for case in cases
        if isinstance(case.get("canonical_graph"), dict)
    ]
    oracles = [
        bool(case["oracles"].get("passed"))
        for case in cases
        if isinstance(case.get("oracles"), dict)
    ]
    case_passes = [bool(case.get("passed")) for case in cases]
    incremental_ms: list[float] = []
    full_ms: list[float] = []
    speedups: list[float] = []
    peak_rss: list[int] = []
    for case in cases:
        incremental = case.get("incremental", {})
        full = case.get("fresh_fast_full_after_change", {})
        if isinstance(incremental, dict):
            if isinstance(incremental.get("elapsed_ms"), (int, float)):
                incremental_ms.append(float(incremental["elapsed_ms"]))
            if isinstance(incremental.get("peak_rss_mb"), int):
                peak_rss.append(incremental["peak_rss_mb"])
        if isinstance(full, dict):
            if isinstance(full.get("elapsed_ms"), (int, float)):
                full_ms.append(float(full["elapsed_ms"]))
            if isinstance(full.get("peak_rss_mb"), int):
                peak_rss.append(full["peak_rss_mb"])
        if isinstance(case.get("speedup_full_rebuild_over_incremental"), (int, float)):
            speedups.append(float(case["speedup_full_rebuild_over_incremental"]))

    quality_failed = any(not value for value in canonical) or any(not value for value in oracles)
    if quality_failed:
        decision = "REJECT: quality/correctness"
    elif case_passes and not all(case_passes):
        decision = "REJECT: benchmark gate"
    elif not cases:
        decision = "REJECT: no cases"
    else:
        decision = "PASS"

    cleanup_passes = sum(
        bool(report.get("cleanup", {}).get("removed"))
        for report in reports
        if isinstance(report.get("cleanup"), dict)
    )
    hashes = sorted(
        {
            str(report.get("binary_metadata", {}).get("sha256", ""))
            for report in reports
            if isinstance(report.get("binary_metadata"), dict)
            and report.get("binary_metadata", {}).get("sha256")
        }
    )
    return {
        "candidate": label,
        "decision": decision,
        "cases": ratio(sum(case_passes), len(case_passes)),
        "canonical": ratio(sum(canonical), len(canonical)),
        "oracles": ratio(sum(oracles), len(oracles)),
        "capabilities": config_label(reports),
        "incremental_p50_ms": percentile(incremental_ms, 0.50),
        "incremental_p95_ms": percentile(incremental_ms, 0.95),
        "full_p50_ms": percentile(full_ms, 0.50),
        "speedup_p50": float(statistics.median(speedups)) if speedups else None,
        "peak_rss_mb": max(peak_rss) if peak_rss else None,
        "cleanup": ratio(cleanup_passes, len(reports)),
        "binary_sha256": ", ".join(value[:12] for value in hashes) or "n/a",
    }


def display(value: Any, digits: int = 1) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value).replace("|", "\\|")


def render_markdown(rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Codebase Memory performance and quality summary",
        "",
        "| Candidate | Decision | Cases | Canonical | Task oracles | Capabilities | "
        "Incremental p50 ms | Incremental p95 ms | Full p50 ms | Speedup p50 | "
        "Peak RSS MB | Cleanup | Binary SHA-256 |",
        "|---|---|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                (
                    display(row["candidate"]),
                    display(row["decision"]),
                    display(row["cases"]),
                    display(row["canonical"]),
                    display(row["oracles"]),
                    display(row["capabilities"]),
                    display(row["incremental_p50_ms"]),
                    display(row["incremental_p95_ms"]),
                    display(row["full_p50_ms"]),
                    display(row["speedup_p50"], 2),
                    display(row["peak_rss_mb"]),
                    display(row["cleanup"]),
                    display(row["binary_sha256"]),
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "A speedup is accepted only when the case gate and every applicable canonical-graph "
            "and task-oracle check pass. `n/a` means the input artifact did not measure that axis.",
        )
    )
    return "\n".join(lines) + "\n"


def parse_input(value: str) -> tuple[str, Path]:
    label, separator, raw_path = value.partition("=")
    if not separator or not label or not raw_path:
        raise argparse.ArgumentTypeError("--input expects LABEL=PATH")
    return label, Path(raw_path).expanduser()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", action="append", required=True, type=parse_input)
    parser.add_argument("--out", default="")
    args = parser.parse_args()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for label, path in args.input:
        with path.open(encoding="utf-8") as stream:
            document = json.load(stream)
        if not isinstance(document, dict):
            raise SystemExit(f"error: expected JSON object in {path}")
        grouped[label].append(document)
    markdown = render_markdown(
        [summarize_group(label, reports) for label, reports in grouped.items()]
    )
    if args.out:
        output = Path(args.out).expanduser()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(markdown, encoding="utf-8")
    print(markdown, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

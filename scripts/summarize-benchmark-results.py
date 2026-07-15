#!/usr/bin/env python3
"""Aggregate existing CBM benchmark JSON into a quality-first Markdown table."""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import uuid
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
        overrides = (
            parameters.get("config_overrides", {}) if isinstance(parameters, dict) else {}
        )
        profile = parameters.get("config_profile") if isinstance(parameters, dict) else None
        if isinstance(overrides, dict) and overrides:
            expanded = ", ".join(f"{key}={overrides[key]}" for key in sorted(overrides))
            labels.add(
                f"{profile} ({expanded})"
                if isinstance(profile, str) and profile
                else expanded
            )
        else:
            labels.add(str(profile) if isinstance(profile, str) and profile else "defaults")
    return " / ".join(sorted(labels))


def quality_oracle_details(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for case_index, case in enumerate(cases, start=1):
        scenario = str(case.get("scenario") or f"case {case_index}")
        case_oracles = case.get("oracles")
        if not isinstance(case_oracles, dict):
            continue
        for name, oracle in case_oracles.items():
            if name == "quality" or not isinstance(oracle, dict):
                continue
            quality = oracle.get("quality")
            if not isinstance(quality, dict):
                continue
            applicable = quality.get("applicable") is not False
            passed = quality.get("passed")
            rank = quality.get("rank")
            returned = quality.get("returned_count")
            if not applicable:
                result = "N/A"
            elif passed is True and isinstance(rank, int) and isinstance(returned, int):
                result = f"PASS (rank {rank} of {returned})"
            elif passed is True:
                result = "PASS"
            elif isinstance(returned, int):
                result = f"FAIL (not found in {returned})"
            else:
                result = "FAIL"
            details.append(
                {
                    "scenario": scenario,
                    "oracle": str(name),
                    "criterion": str(quality.get("criterion") or "unspecified"),
                    "expected": str(quality.get("expected_substring") or "n/a"),
                    "result": result,
                    "reciprocal_rank": quality.get("reciprocal_rank"),
                    "hit_at_1": quality.get("hit_at_1"),
                    "hit_at_5": quality.get("hit_at_5"),
                }
            )
    return details


def compact_witness(value: Any, limit: int = 96) -> str:
    if not isinstance(value, str) or not value:
        return ""
    single_line = " ".join(value.split())
    return single_line if len(single_line) <= limit else single_line[: limit - 1] + "…"


def correctness_findings(cases: list[dict[str, Any]]) -> list[str]:
    findings: list[str] = []
    for case in cases:
        canonical = case.get("canonical_graph")
        if isinstance(canonical, dict) and canonical.get("equal") is False:
            kind = canonical.get("kind") or "canonical graph"
            detail = (
                f"{kind} mismatch (incremental={canonical.get('left_count', 'n/a')}, "
                f"fresh={canonical.get('right_count', 'n/a')})"
            )
            witnesses = [
                compact_witness(canonical.get("left_only")),
                compact_witness(canonical.get("right_only")),
            ]
            witnesses = [value for value in witnesses if value]
            if witnesses:
                detail += "; witness: " + " vs ".join(witnesses)
            findings.append(detail)

        case_oracles = case.get("oracles")
        if not isinstance(case_oracles, dict):
            continue
        for name, oracle in case_oracles.items():
            if not isinstance(oracle, dict) or name == "quality":
                continue
            quality = oracle.get("quality")
            if isinstance(quality, dict) and quality.get("passed") is False:
                expected = compact_witness(quality.get("expected_substring"))
                finding = f"{name} failed"
                if expected:
                    finding += f" (expected {expected})"
                findings.append(finding)
    return list(dict.fromkeys(findings))


def summarize_group(label: str, reports: list[dict[str, Any]]) -> dict[str, Any]:
    cases = [case for report in reports for case in cases_from_report(report)]
    canonical = [
        bool(case["canonical_graph"].get("equal"))
        for case in cases
        if isinstance(case.get("canonical_graph"), dict)
    ]
    oracles: list[bool] = []
    for case in cases:
        case_oracles = case.get("oracles")
        if not isinstance(case_oracles, dict):
            continue
        verdict = case_oracles.get("passed")
        if not isinstance(verdict, bool):
            quality = case_oracles.get("quality")
            verdict = quality.get("passed") if isinstance(quality, dict) else None
        if isinstance(verdict, bool):
            oracles.append(verdict)
    case_passes = [bool(case.get("passed")) for case in cases]
    incremental_ms: list[float] = []
    full_ms: list[float] = []
    speedups: list[float] = []
    peak_rss: list[int] = []
    query_latency_ms: list[float] = []
    query_response_bytes: list[float] = []
    query_response_tokens: list[float] = []
    quality_passed = 0
    quality_applicable = 0
    quality_score_weighted = 0.0
    quality_score_count = 0
    hit_at_1_weighted = 0.0
    hit_at_5_weighted = 0.0
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
        case_oracles = case.get("oracles", {})
        if isinstance(case_oracles, dict):
            quality = case_oracles.get("quality", {})
            if isinstance(quality, dict):
                applicable = int(quality.get("applicable_count") or 0)
                quality_passed += int(quality.get("passed_count") or 0)
                quality_applicable += applicable
                score = quality.get("score")
                hit_at_1 = quality.get("hit_at_1")
                hit_at_5 = quality.get("hit_at_5")
                if applicable and isinstance(score, (int, float)):
                    quality_score_weighted += float(score) * applicable
                    quality_score_count += applicable
                if applicable and isinstance(hit_at_1, (int, float)):
                    hit_at_1_weighted += float(hit_at_1) * applicable
                if applicable and isinstance(hit_at_5, (int, float)):
                    hit_at_5_weighted += float(hit_at_5) * applicable
            for oracle in case_oracles.values():
                if not isinstance(oracle, dict):
                    continue
                if isinstance(oracle.get("elapsed_ms"), (int, float)):
                    query_latency_ms.append(float(oracle["elapsed_ms"]))
                if isinstance(oracle.get("response_bytes"), (int, float)):
                    query_response_bytes.append(float(oracle["response_bytes"]))
                if isinstance(oracle.get("response_token_estimate"), (int, float)):
                    query_response_tokens.append(float(oracle["response_token_estimate"]))

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
    retrieval_score = (
        quality_score_weighted / quality_score_count
        if quality_score_count
        else quality_passed / quality_applicable if quality_applicable else None
    )
    graph_fidelity_score = sum(canonical) / len(canonical) if canonical else None
    task_success_score = (
        quality_passed / quality_applicable
        if quality_applicable
        else sum(oracles) / len(oracles) if oracles else None
    )
    quality_categories = (retrieval_score, graph_fidelity_score, task_success_score)
    overall_quality_score = (
        math.prod(quality_categories) ** (1.0 / len(quality_categories))
        if all(isinstance(value, (int, float)) for value in quality_categories)
        else None
    )
    return {
        "candidate": label,
        "decision": decision,
        "cases": ratio(sum(case_passes), len(case_passes)),
        "canonical": ratio(sum(canonical), len(canonical)),
        "oracles": ratio(sum(oracles), len(oracles)),
        "quality_score": retrieval_score,
        "overall_quality_score": overall_quality_score,
        "graph_fidelity_score": graph_fidelity_score,
        "task_success_score": task_success_score,
        "hit_at_1": hit_at_1_weighted / quality_score_count if quality_score_count else None,
        "hit_at_5": hit_at_5_weighted / quality_score_count if quality_score_count else None,
        "quality_checks": ratio(quality_passed, quality_applicable),
        "query_response_p50_bytes": percentile(query_response_bytes, 0.50),
        "query_response_p50_tokens": percentile(query_response_tokens, 0.50),
        "query_latency_p50_ms": percentile(query_latency_ms, 0.50),
        "capabilities": config_label(reports),
        "incremental_p50_ms": percentile(incremental_ms, 0.50),
        "incremental_p95_ms": percentile(incremental_ms, 0.95),
        "full_p50_ms": percentile(full_ms, 0.50),
        "speedup_p50": float(statistics.median(speedups)) if speedups else None,
        "peak_rss_mb": max(peak_rss) if peak_rss else None,
        "cleanup": ratio(cleanup_passes, len(reports)),
        "binary_sha256": ", ".join(value[:12] for value in hashes) or "n/a",
        "findings": correctness_findings(cases),
        "quality_details": quality_oracle_details(cases),
        "pareto": "unclassified",
        "pareto_reason": "not evaluated",
    }


PARETO_MINIMIZE = (
    "incremental_p50_ms",
    "query_latency_p50_ms",
    "query_response_p50_tokens",
    "peak_rss_mb",
)


def dominates(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_quality = left.get("overall_quality_score")
    right_quality = right.get("overall_quality_score")
    if not isinstance(left_quality, (int, float)) or not isinstance(
        right_quality, (int, float)
    ):
        return False
    left_values = [left.get(key) for key in PARETO_MINIMIZE]
    right_values = [right.get(key) for key in PARETO_MINIMIZE]
    if not all(isinstance(value, (int, float)) for value in left_values + right_values):
        return False
    no_worse = left_quality >= right_quality and all(
        left_value <= right_value
        for left_value, right_value in zip(left_values, right_values, strict=True)
    )
    strictly_better = left_quality > right_quality or any(
        left_value < right_value
        for left_value, right_value in zip(left_values, right_values, strict=True)
    )
    return no_worse and strictly_better


def mark_pareto_frontier(rows: list[dict[str, Any]]) -> None:
    """Mark correctness-admissible, fully measured non-dominated candidates."""
    eligible = [
        row
        for row in rows
        if row.get("decision") == "PASS"
        and isinstance(row.get("overall_quality_score"), (int, float))
        and all(isinstance(row.get(key), (int, float)) for key in PARETO_MINIMIZE)
    ]
    for row in rows:
        row["pareto"] = "ineligible"
        missing = [
            key
            for key in ("overall_quality_score", *PARETO_MINIMIZE)
            if not isinstance(row.get(key), (int, float))
        ]
        reasons = []
        if row.get("decision") != "PASS":
            reasons.append(str(row.get("decision")))
        if missing:
            reasons.append("missing " + ", ".join(missing))
        row["pareto_reason"] = "; ".join(reasons) or "not eligible"
    for row in eligible:
        dominators = [
            other for other in eligible if other is not row and dominates(other, row)
        ]
        if dominators:
            dominator = dominators[0]
            row["pareto"] = f"dominated by {dominator['candidate']}"
            row["pareto_reason"] = (
                f"{dominator['candidate']} has overall quality "
                f"{dominator['overall_quality_score']:.3f} >= "
                f"{row['overall_quality_score']:.3f} and is no slower/larger on every cost axis"
            )
        else:
            row["pareto"] = "frontier"
            row["pareto_reason"] = (
                "no passing, fully measured candidate is at least as good on overall quality "
                "and every cost axis while being strictly better on one or more axes"
            )


def display(value: Any, digits: int = 1) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value).replace("|", "\\|")


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def render_markdown(rows: list[dict[str, Any]]) -> str:
    mark_pareto_frontier(rows)
    lines = [
        "# Codebase Memory performance and quality summary",
        "",
        "| Candidate | Decision | Overall quality† | Retrieval MRR | Hit@1 | Hit@5 | "
        "Graph fidelity | Task success | Evidence counts (R/G/S) | "
        "Response p50 bytes | Response p50 tokens* | Query p50 ms | Incremental p50 ms | "
        "Peak RSS MB | Pareto |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                (
                    display(row["candidate"]),
                    display(row["decision"]),
                    display(row["overall_quality_score"], 3),
                    display(row["quality_score"], 3),
                    display(row["hit_at_1"], 3),
                    display(row["hit_at_5"], 3),
                    display(row["graph_fidelity_score"], 3),
                    display(row["task_success_score"], 3),
                    display(
                        f"{row['quality_checks']} / {row['canonical']} / {row['oracles']}"
                    ),
                    display(row["query_response_p50_bytes"]),
                    display(row["query_response_p50_tokens"]),
                    display(row["query_latency_p50_ms"]),
                    display(row["incremental_p50_ms"]),
                    display(row["peak_rss_mb"]),
                    display(row["pareto"]),
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "## Performance and provenance",
            "",
            "| Candidate | Cases | Capabilities | Incremental p95 ms | Full p50 ms | "
            "Speedup p50 | Cleanup | Binary SHA-256 |",
            "|---|---:|---|---:|---:|---:|---:|---|",
        )
    )
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                (
                    display(row["candidate"]),
                    display(row["cases"]),
                    display(row["capabilities"]),
                    display(row["incremental_p95_ms"]),
                    display(row["full_p50_ms"]),
                    display(row["speedup_p50"], 2),
                    display(row["cleanup"]),
                    display(row["binary_sha256"]),
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "## Named quality-oracle breakdown",
            "",
            "| Candidate | Scenario | Oracle | Criterion | Expected evidence | Result | RR | Hit@1 | Hit@5 |",
            "|---|---|---|---|---|---|---:|---:|---:|",
        )
    )
    detail_count = 0
    for row in rows:
        for detail in row["quality_details"]:
            detail_count += 1
            lines.append(
                "| "
                + " | ".join(
                    (
                        display(row["candidate"]),
                        display(detail["scenario"]),
                        display(detail["oracle"]),
                        display(detail["criterion"]),
                        display(detail["expected"]),
                        display(detail["result"]),
                        display(detail["reciprocal_rank"], 3),
                        display(detail["hit_at_1"]),
                        display(detail["hit_at_5"]),
                    )
                )
                + " |"
            )
    if not detail_count:
        lines.append(
            "| all | n/a | n/a | No per-oracle quality evidence recorded | n/a | "
            "N/A | n/a | n/a | n/a |"
        )
    lines.extend(
        (
            "",
            "## Correctness and quality findings",
            "",
            "| Candidate | Evidence |",
            "|---|---|",
        )
    )
    for row in rows:
        evidence = row["findings"] or ["All applicable canonical-graph and task-oracle checks passed."]
        lines.append(f"| {display(row['candidate'])} | {display('; '.join(evidence))} |")
    lines.extend(
        (
            "",
            "## Pareto eligibility and dominance",
            "",
            "| Candidate | Status | Explanation |",
            "|---|---|---|",
        )
    )
    for row in rows:
        lines.append(
            f"| {display(row['candidate'])} | {display(row['pareto'])} | "
            f"{display(row['pareto_reason'])} |"
        )
    lines.extend(
        (
            "",
            "* Response tokens use the recorded `utf8_bytes_div_4_ceil` deterministic estimate; "
            "bytes remain the exact default tool-response payload measurement.",
            "",
            "Retrieval MRR is the mean reciprocal rank of the first expected result over applicable "
            "ranked probes; a missing expected result contributes zero. Hit@1 and Hit@5 are the "
            "fractions of those same applicable probes whose first expected result appears by the "
            "stated cutoff. N/A probes are excluded from every retrieval denominator. These definitions "
            "follow the official TREC treatment of reciprocal rank and Success@n: "
            "[TREC 2005 Enterprise/QA overview](https://trec.nist.gov/pubs/trec14/papers/hummingbird.qa.robust.tera.pdf).",
            "",
            "Graph fidelity is the fraction of mutation cases whose incremental canonical graph equals "
            "a fresh FAST rebuild. Task success is the fraction of applicable probes that find their "
            "required evidence. Evidence counts show retrieval probes / graph comparisons / strict "
            "whole-scenario passes. The named breakdown above shows why a result is, for example, 4/5 "
            "rather than hiding the failed task.",
            "",
            "† Overall quality is a custom descriptive score: the equal-weight geometric mean of "
            "Retrieval MRR, graph fidelity, and task success. It is N/A unless all three categories are "
            "measured. It never overrides the correctness gate: any canonical or task-oracle failure "
            "still makes Decision=REJECT. Category values remain visible so the aggregate cannot hide "
            "which capability changed.",
            "",
            "Query p50 aggregates the recorded default-response oracle calls. Indexing p50/p95 use only "
            "the recorded indexing observations; consult Cases and the immutable campaign manifest before "
            "treating a small pilot as a population estimate.",
            "",
            "Pareto status considers only candidates that pass correctness/quality and have every "
            "axis measured. It maximizes overall quality while minimizing incremental and query latency, "
            "response-token estimate, and peak RSS.",
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
    markdown = render_markdown([summarize_group(label, reports) for label, reports in grouped.items()])
    if args.out:
        output = Path(args.out).expanduser()
        atomic_write_text(output, markdown)
    print(markdown, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

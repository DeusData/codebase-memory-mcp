#!/usr/bin/env python3
"""Aggregate existing CBM benchmark JSON into a quality-first Markdown table."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
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


def evidence_lifecycle(reports: list[dict[str, Any]]) -> str:
    """Describe retained evidence separately from requested cleanup outcomes."""
    disposed = 0
    retained = 0
    failed = 0
    unknown = 0
    for report in reports:
        cleanup = report.get("cleanup")
        if not isinstance(cleanup, dict) or not isinstance(
            cleanup.get("requested"), bool
        ):
            unknown += 1
        elif cleanup["requested"] is False:
            retained += 1
        elif cleanup.get("removed") is True:
            disposed += 1
        else:
            failed += 1
    if failed:
        requested = disposed + failed
        return f"CLEANUP FAILED {failed}/{requested}"
    if unknown:
        return f"unknown {unknown}/{len(reports)}"
    if disposed and retained:
        return f"disposed {disposed}/{len(reports)}; retained by request {retained}/{len(reports)}"
    if disposed:
        return f"disposed {disposed}/{len(reports)}"
    return f"retained by request {retained}/{len(reports)}"


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
            parameters.get("config_overrides", {})
            if isinstance(parameters, dict)
            else {}
        )
        profile = (
            parameters.get("config_profile") if isinstance(parameters, dict) else None
        )
        if isinstance(overrides, dict) and overrides:
            expanded = ", ".join(f"{key}={overrides[key]}" for key in sorted(overrides))
            labels.add(
                f"{profile} ({expanded})"
                if isinstance(profile, str) and profile
                else expanded
            )
        else:
            labels.add(
                str(profile) if isinstance(profile, str) and profile else "defaults"
            )
    return " / ".join(sorted(labels))


def config_signature(
    reports: list[dict[str, Any]],
) -> tuple[tuple[str, str], ...] | None:
    signatures: set[tuple[tuple[str, str], ...]] = set()
    for report in reports:
        parameters = report.get("parameters")
        overrides = (
            parameters.get("config_overrides", {})
            if isinstance(parameters, dict)
            else {}
        )
        if not isinstance(overrides, dict):
            return None
        signatures.add(
            tuple(sorted((str(key), str(value)) for key, value in overrides.items()))
        )
    return next(iter(signatures)) if len(signatures) == 1 else None


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
            elif isinstance(rank, int) and isinstance(returned, int):
                result = f"BELOW CUTOFF (rank {rank} of {returned})"
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
                    "ndcg_at_5": quality.get("ndcg_at_5"),
                    "judgments": (
                        f"{quality['relevance_judgments']} judgments"
                        if isinstance(quality.get("relevance_judgments"), int)
                        else "n/a"
                    ),
                }
            )
    return details


def compact_witness(value: Any, limit: int = 96) -> str:
    if not isinstance(value, str) or not value:
        return ""
    single_line = " ".join(value.split())
    return single_line if len(single_line) <= limit else single_line[: limit - 1] + "…"


def canonical_mismatch_finding(
    canonical: Any,
    *,
    graph_gate: Any = None,
) -> str | None:
    if not isinstance(canonical, dict) or canonical.get("equal") is not False:
        return None
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
    if (
        isinstance(graph_gate, dict)
        and graph_gate.get("policy") == "declared_stale_derived_views"
        and graph_gate.get("passed") is True
    ):
        views = graph_gate.get("declared_stale_views")
        view_text = (
            ", ".join(str(value) for value in views)
            if isinstance(views, list)
            else "unknown"
        )
        detail = f"declared stale derived views ({view_text}); " + detail
    return detail


def correctness_findings(
    cases: list[dict[str, Any]],
    *,
    capability_quality: bool = False,
    disabled_pair_capabilities: set[str] | None = None,
) -> list[str]:
    findings: list[str] = []
    disabled_pair_capabilities = disabled_pair_capabilities or set()
    for case in cases:
        canonical = case.get("canonical_graph")
        detail = canonical_mismatch_finding(
            canonical,
            graph_gate=case.get("graph_gate"),
        )
        if detail:
            findings.append(detail)

        case_oracles = case.get("oracles")
        if isinstance(case_oracles, dict):
            for name, oracle in case_oracles.items():
                if not isinstance(oracle, dict) or name == "quality":
                    continue
                quality = oracle.get("quality")
                if isinstance(quality, dict) and quality.get("passed") is False:
                    expected = compact_witness(quality.get("expected_substring"))
                    rank = quality.get("rank")
                    cutoff = quality.get("relevance_cutoff")
                    if (
                        capability_quality
                        and isinstance(rank, int)
                        and isinstance(cutoff, int)
                    ):
                        finding = f"{name} below quality cutoff (rank {rank}, cutoff {cutoff})"
                    elif capability_quality:
                        finding = f"{name} did not meet the quality target"
                    else:
                        finding = f"{name} failed"
                    if expected:
                        finding += f" (expected {expected})"
                    findings.append(finding)

        lifecycle = case.get("pair_lifecycle")
        fixture = case.get("fixture")
        capability = (
            str(fixture.get("capability") or "") if isinstance(fixture, dict) else ""
        )
        if not isinstance(lifecycle, dict) or capability in disabled_pair_capabilities:
            continue
        lifecycle_canonical = lifecycle.get("canonical_graph")
        if lifecycle_canonical is not canonical:
            detail = canonical_mismatch_finding(lifecycle_canonical)
            if detail:
                findings.append(detail)
        policy = lifecycle.get("incremental_policy")
        immediate_expected = (
            isinstance(policy, dict)
            and policy.get("immediate_freshness_expected") is True
        )
        for stage, key, required in (
            ("Initial", "initial_oracles", True),
            ("Post-edit", "incremental_oracles", immediate_expected),
            ("Fresh", "fresh_oracles", True),
        ):
            oracle = lifecycle.get(key)
            if (
                not required
                or not isinstance(oracle, dict)
                or oracle.get("passed") is not False
            ):
                continue
            classification = oracle.get("pair_classification")
            confusion = (
                classification.get("confusion")
                if isinstance(classification, dict)
                else None
            )
            finding = f"{stage} semantic-pair quality missed the declared target"
            if isinstance(confusion, dict):
                tp = int(confusion.get("tp") or 0)
                tn = int(confusion.get("tn") or 0)
                fp = int(confusion.get("fp") or 0)
                fn = int(confusion.get("fn") or 0)
                finding += f": TP={tp}, TN={tn}, FP={fp}, FN={fn}"
                consequences = []
                if fn:
                    consequences.append(f"{fn} expected positive absent")
                if fp:
                    consequences.append(f"{fp} unexpected positive present")
                if consequences:
                    finding += " (" + ", ".join(consequences) + ")"
            findings.append(finding)
    return list(dict.fromkeys(findings))


def mutation_reindex_details(
    cases: list[dict[str, Any]],
    *,
    disabled_pair_capabilities: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Aggregate repeated measurements without hiding the mutated source or publish route."""
    disabled_pair_capabilities = disabled_pair_capabilities or set()
    grouped: dict[str, dict[str, Any]] = {}
    for case_index, case in enumerate(cases, start=1):
        scenario = str(case.get("scenario") or f"case {case_index}")
        group = grouped.setdefault(
            scenario,
            {
                "descriptions": set(),
                "changed_paths": set(),
                "routes": set(),
                "reasons": set(),
                "incremental_ms": [],
                "work_ms": [],
                "full_ms": [],
                "speedups": [],
                "canonical": [],
            },
        )
        lifecycle = case.get("pair_lifecycle")
        lifecycle = lifecycle if isinstance(lifecycle, dict) else {}
        mutation = lifecycle.get("mutation", case.get("mutation"))
        if isinstance(mutation, dict):
            description = mutation.get("description")
            if isinstance(description, str) and description:
                group["descriptions"].add(description)
            changed_paths = mutation.get("changed_paths")
            if isinstance(changed_paths, list):
                group["changed_paths"].update(
                    str(path)
                    for path in changed_paths
                    if isinstance(path, str) and path
                )
        # Matrix artifacts predate the self-dogfood mutation object and retain
        # their changed paths at the case root. Consume both schemas so an
        # auditable path is never rendered as "not reported".
        case_changed_paths = case.get("changed_paths")
        if isinstance(case_changed_paths, list):
            group["changed_paths"].update(
                str(path)
                for path in case_changed_paths
                if isinstance(path, str) and path
            )
        scenario_metadata = case.get("scenario_metadata")
        if not group["descriptions"] and isinstance(scenario_metadata, dict):
            if scenario_metadata.get("source") == "synthetic_inbound_frontier":
                language = scenario_metadata.get("cross_file_resolver_language")
                if not isinstance(language, str) or not language:
                    language = scenario_metadata.get("language")
                if isinstance(language, str) and language:
                    group["descriptions"].add(
                        f"synthetic {language} inbound-frontier definition edit"
                    )
        incremental = lifecycle.get("incremental_index", case.get("incremental"))
        if isinstance(incremental, dict):
            if isinstance(incremental.get("elapsed_ms"), (int, float)):
                group["incremental_ms"].append(float(incremental["elapsed_ms"]))
            if isinstance(incremental.get("indexed_work_elapsed_ms"), (int, float)):
                group["work_ms"].append(float(incremental["indexed_work_elapsed_ms"]))
            route = incremental.get("publish_kind")
            if isinstance(route, str) and route:
                group["routes"].add(route)
            reason = incremental.get("exact_reason")
            if isinstance(reason, str) and reason:
                group["reasons"].add(reason)
        full = lifecycle.get("fresh_index", case.get("fresh_fast_full_after_change"))
        if isinstance(full, dict) and isinstance(full.get("elapsed_ms"), (int, float)):
            group["full_ms"].append(float(full["elapsed_ms"]))
        speedup = case.get("speedup_full_rebuild_over_incremental")
        if isinstance(speedup, (int, float)):
            group["speedups"].append(float(speedup))
        canonical = lifecycle.get("canonical_graph", case.get("canonical_graph"))
        if isinstance(canonical, dict) and isinstance(canonical.get("equal"), bool):
            group["canonical"].append(canonical["equal"])
        fixture = case.get("fixture")
        capability = (
            str(fixture.get("capability") or "") if isinstance(fixture, dict) else ""
        )
        policy = lifecycle.get("incremental_policy")
        if capability in disabled_pair_capabilities:
            group["canonical_policy"] = "capability disabled"
        elif (
            isinstance(policy, dict)
            and policy.get("immediate_freshness_expected") is False
            and policy.get("policy_conformance_met") is True
            and policy.get("stale_warning_present") is True
        ):
            group["canonical_policy"] = "deferred with warning"

    details: list[dict[str, Any]] = []
    for scenario, group in grouped.items():
        routes = sorted(group["routes"])
        reasons = sorted(group["reasons"])
        route = ", ".join(routes) if routes else "not reported"
        if reasons:
            route += " (" + ", ".join(reasons) + ")"
        canonical = group["canonical"]
        details.append(
            {
                "scenario": scenario,
                "mutation": "; ".join(sorted(group["descriptions"])) or "not reported",
                "changed_paths": ", ".join(sorted(group["changed_paths"]))
                or "not reported",
                "publication": route,
                "incremental_p50_ms": percentile(group["incremental_ms"], 0.50),
                "work_p50_ms": percentile(group["work_ms"], 0.50),
                "full_p50_ms": percentile(group["full_ms"], 0.50),
                "speedup_p50": (
                    float(statistics.median(group["speedups"]))
                    if group["speedups"]
                    else None
                ),
                "canonical": group.get("canonical_policy")
                or ratio(sum(canonical), len(canonical)),
            }
        )
    return details


def marker_int(lines: Any, marker: str, field: str) -> int | None:
    if not isinstance(lines, list):
        return None
    prefix = f"{field}="
    for line in lines:
        if not isinstance(line, str) or marker not in line:
            continue
        for item in line.split():
            if item.startswith(prefix):
                try:
                    return int(item.split("=", 1)[1])
                except ValueError:
                    return None
    return None


def dependency_observation(index_result: Any) -> tuple[int | None, int | None]:
    """Read dependency cost/count from current and retained benchmark result shapes."""
    if not isinstance(index_result, dict):
        return None, None
    dependency = index_result.get("dependency_indexing")
    phase_ms = None
    packages = None
    if isinstance(dependency, dict):
        raw_phase = dependency.get("phase_elapsed_ms")
        raw_packages = dependency.get("packages_indexed")
        phase_ms = int(raw_phase) if isinstance(raw_phase, (int, float)) else None
        packages = int(raw_packages) if isinstance(raw_packages, (int, float)) else None
    if phase_ms is None:
        phase_ms = marker_int(
            index_result.get("measurement_log_markers"), "sub=dep_auto_index", "ms"
        )
    response = index_result.get("response")
    if packages is None and isinstance(response, dict):
        raw_packages = response.get("dependencies_indexed")
        packages = int(raw_packages) if isinstance(raw_packages, (int, float)) else None
    return phase_ms, packages


def dependency_mode(
    reports: list[dict[str, Any]], observed_packages: list[float]
) -> str:
    support: set[bool] = set()
    overrides: set[str] = set()
    for report in reports:
        parameters = report.get("parameters")
        if not isinstance(parameters, dict):
            continue
        capability_support = parameters.get("capability_support")
        if isinstance(capability_support, dict):
            raw_support = capability_support.get(
                "dependencies", capability_support.get("auto_index_deps")
            )
            if isinstance(raw_support, bool):
                support.add(raw_support)
        config = parameters.get("config_overrides")
        if isinstance(config, dict) and "auto_index_deps" in config:
            overrides.add(str(config["auto_index_deps"]).lower())
    if support == {False}:
        return "unsupported"
    if overrides and overrides <= {"false", "0", "off"}:
        return "disabled (explicit)"
    if overrides and overrides <= {"true", "1", "on"}:
        return "enabled (explicit)"
    if observed_packages and max(observed_packages) > 0:
        return "enabled (observed)"
    return "unknown"


ALGORITHM_CAPABILITIES = (
    "rank",
    "similarity",
    "semantic_edges",
    "git_history",
    "http_links",
    "dependencies",
)


def summarize_capability_applicability(
    reports: list[dict[str, Any]],
) -> dict[str, str]:
    summarized: dict[str, str] = {}
    for capability in ALGORITHM_CAPABILITIES:
        states: set[tuple[bool, str]] = set()
        support: set[bool] = set()
        for report in reports:
            parameters = report.get("parameters")
            capability_support = (
                parameters.get("capability_support")
                if isinstance(parameters, dict)
                else None
            )
            if isinstance(capability_support, dict) and isinstance(
                capability_support.get(capability), bool
            ):
                support.add(capability_support[capability])
            applicability = (
                parameters.get("capability_applicability")
                if isinstance(parameters, dict)
                else None
            )
            state = (
                applicability.get(capability)
                if isinstance(applicability, dict)
                else None
            )
            if isinstance(state, dict) and isinstance(state.get("applicable"), bool):
                states.add(
                    (state["applicable"], str(state.get("reason") or "unspecified"))
                )
        if support == {False}:
            summarized[capability] = "unsupported by candidate"
        elif len(support) > 1:
            summarized[capability] = "mixed support"
        elif not states:
            summarized[capability] = "unknown"
        elif len(states) > 1:
            summarized[capability] = "mixed"
        else:
            applicable, reason = next(iter(states))
            summarized[capability] = "applicable" if applicable else f"N/A: {reason}"
    return summarized


def quality_miss_is_explicit_ablation(
    report: dict[str, Any], case: dict[str, Any]
) -> bool:
    fixture = case.get("fixture")
    capability = fixture.get("capability") if isinstance(fixture, dict) else None
    parameters = report.get("parameters")
    overrides = (
        parameters.get("config_overrides") if isinstance(parameters, dict) else None
    )
    if not isinstance(overrides, dict):
        return False
    key = "rank_enabled" if capability == "rank" else "auto_index_deps"
    if capability not in {"rank", "dependencies"}:
        return False
    value = overrides.get(key)
    return value is False or (isinstance(value, str) and value.lower() == "false")


def semantic_pair_quality_details(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for case in cases:
        lifecycle = case.get("pair_lifecycle")
        fixture = case.get("fixture")
        if not isinstance(lifecycle, dict) or not isinstance(fixture, dict):
            continue
        policy = lifecycle.get("incremental_policy")
        policy = policy if isinstance(policy, dict) else {}

        def classification(stage: str) -> tuple[dict[str, int] | None, float | None]:
            oracles = lifecycle.get(f"{stage}_oracles")
            pair = (
                oracles.get("pair_classification")
                if isinstance(oracles, dict)
                else None
            )
            confusion = pair.get("confusion") if isinstance(pair, dict) else None
            f1 = pair.get("f1") if isinstance(pair, dict) else None
            return (
                confusion if isinstance(confusion, dict) else None,
                float(f1) if isinstance(f1, (int, float)) else None,
            )

        initial_confusion, initial_f1 = classification("initial")
        incremental_confusion, incremental_f1 = classification("incremental")
        fresh_confusion, fresh_f1 = classification("fresh")
        if policy.get("immediate_freshness_met") is True:
            freshness = "fresh and canonical"
        elif (
            policy.get("immediate_freshness_expected") is False
            and policy.get("stale_warning_present") is True
        ):
            freshness = "deferred with warning"
        else:
            freshness = "unexpected stale or non-canonical"
        background = case.get("background_repository")
        details.append(
            {
                "capability": fixture.get("capability"),
                "relationship": fixture.get("relationship"),
                "task_sha256": fixture.get("task_set_sha256"),
                "background_revision": (
                    background.get("revision") if isinstance(background, dict) else None
                ),
                "background_tree": (
                    background.get("tree") if isinstance(background, dict) else None
                ),
                "initial_confusion": initial_confusion,
                "initial_f1": initial_f1,
                "incremental_confusion": incremental_confusion,
                "incremental_f1": incremental_f1,
                "fresh_confusion": fresh_confusion,
                "fresh_f1": fresh_f1,
                "freshness_policy": policy.get("policy"),
                "freshness": freshness,
                "policy_conformance_met": policy.get("policy_conformance_met"),
                "immediate_freshness_expected": policy.get(
                    "immediate_freshness_expected"
                ),
                "immediate_freshness_met": policy.get("immediate_freshness_met"),
            }
        )
    return details


def summarize_group(label: str, reports: list[dict[str, Any]]) -> dict[str, Any]:
    cases = [case for report in reports for case in cases_from_report(report)]
    report_modes = {str(report.get("mode") or "") for report in reports}
    capability_quality = report_modes == {"capability_quality"}
    canonical: list[bool] = []
    core_graph: list[bool] = []
    for case in cases:
        canonical_graph = case.get("canonical_graph")
        if isinstance(canonical_graph, dict):
            canonical_equal = bool(canonical_graph.get("equal"))
            canonical.append(canonical_equal)
            graph_gate = case.get("graph_gate")
            core_graph.append(
                bool(graph_gate.get("passed"))
                if isinstance(graph_gate, dict)
                and isinstance(graph_gate.get("passed"), bool)
                else canonical_equal
            )
            continue
        lifecycle = case.get("pair_lifecycle")
        if not isinstance(lifecycle, dict):
            continue
        policy = lifecycle.get("incremental_policy")
        lifecycle_graph = lifecycle.get("canonical_graph")
        if (
            isinstance(policy, dict)
            and policy.get("immediate_freshness_expected") is True
            and isinstance(lifecycle_graph, dict)
        ):
            lifecycle_equal = bool(lifecycle_graph.get("equal"))
            canonical.append(lifecycle_equal)
            core_graph.append(lifecycle_equal)
    oracles: list[bool] = []
    quality_miss_ablation_states: list[bool] = []
    for report in reports:
        for case in cases_from_report(report):
            case_oracles = case.get("oracles")
            if not isinstance(case_oracles, dict):
                continue
            verdict = case_oracles.get("passed")
            if not isinstance(verdict, bool):
                quality = case_oracles.get("quality")
                verdict = quality.get("passed") if isinstance(quality, dict) else None
            if isinstance(verdict, bool):
                oracles.append(verdict)
                if verdict is False and case.get("quality_target_met") is False:
                    quality_miss_ablation_states.append(
                        quality_miss_is_explicit_ablation(report, case)
                    )
    pair_quality_details = semantic_pair_quality_details(cases)
    signature = config_signature(reports)
    override_map = dict(signature) if signature is not None else {}
    capability_config_keys = {
        "similarity": "similarity_enabled",
        "semantic_edges": "semantic_edges_enabled",
    }
    for detail in pair_quality_details:
        config_key = capability_config_keys.get(str(detail.get("capability")))
        configured = override_map.get(config_key) if config_key else None
        if isinstance(configured, str) and configured.lower() == "false":
            detail["capability_state"] = "disabled"
            detail["freshness"] = "capability disabled"
        else:
            detail["capability_state"] = "enabled or default"
    case_passes: list[bool] = []
    for case in cases:
        lifecycle = case.get("pair_lifecycle")
        if isinstance(lifecycle, dict):
            policy = lifecycle.get("incremental_policy")
            policy = policy if isinstance(policy, dict) else {}
            initial_oracles = lifecycle.get("initial_oracles")
            incremental_oracles = lifecycle.get("incremental_oracles")
            fresh_oracles = lifecycle.get("fresh_oracles")
            passed = bool(
                isinstance(initial_oracles, dict)
                and initial_oracles.get("passed")
                and isinstance(fresh_oracles, dict)
                and fresh_oracles.get("passed")
                and policy.get("policy_conformance_met")
            )
            if policy.get("immediate_freshness_expected") is True:
                passed = passed and bool(
                    isinstance(incremental_oracles, dict)
                    and incremental_oracles.get("passed")
                )
            case_passes.append(passed)
        elif capability_quality and isinstance(case.get("quality_target_met"), bool):
            case_passes.append(bool(case.get("quality_target_met")))
        else:
            case_passes.append(bool(case.get("passed")))
    incremental_ms: list[float] = []
    incremental_work_ms: list[float] = []
    incremental_peak_rss: list[float] = []
    initial_full_ms: list[float] = []
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
    ndcg_weighted = 0.0
    ndcg_count = 0
    dependency_initial_ms: list[float] = []
    dependency_incremental_ms: list[float] = []
    dependency_fresh_ms: list[float] = []
    dependency_packages: list[float] = []
    for case in cases:
        lifecycle = case.get("pair_lifecycle")
        if isinstance(lifecycle, dict):
            initial = lifecycle.get("initial_index", {})
            incremental = lifecycle.get("incremental_index", {})
            full = lifecycle.get("fresh_index", {})
            relation_oracles = [
                lifecycle.get("initial_oracles"),
                lifecycle.get("incremental_oracles"),
                lifecycle.get("fresh_oracles"),
            ]
        else:
            initial = case.get("initial_fast_full", {})
            incremental = case.get("incremental", {})
            full = case.get("fresh_fast_full_after_change", {})
            relation_oracles = []
        if isinstance(initial, dict):
            if isinstance(initial.get("elapsed_ms"), (int, float)):
                initial_full_ms.append(float(initial["elapsed_ms"]))
            if isinstance(initial.get("peak_rss_mb"), (int, float)):
                peak_rss.append(int(initial["peak_rss_mb"]))
        if isinstance(incremental, dict):
            if isinstance(incremental.get("elapsed_ms"), (int, float)):
                incremental_ms.append(float(incremental["elapsed_ms"]))
            if isinstance(incremental.get("indexed_work_elapsed_ms"), (int, float)):
                incremental_work_ms.append(
                    float(incremental["indexed_work_elapsed_ms"])
                )
            if isinstance(incremental.get("peak_rss_mb"), (int, float)):
                incremental_peak_rss.append(float(incremental["peak_rss_mb"]))
                peak_rss.append(int(incremental["peak_rss_mb"]))
        if isinstance(full, dict):
            if isinstance(full.get("elapsed_ms"), (int, float)):
                full_ms.append(float(full["elapsed_ms"]))
            if isinstance(full.get("peak_rss_mb"), int):
                peak_rss.append(full["peak_rss_mb"])
        if isinstance(case.get("speedup_full_rebuild_over_incremental"), (int, float)):
            speedups.append(float(case["speedup_full_rebuild_over_incremental"]))
        elif (
            (
                not isinstance(lifecycle, dict)
                or isinstance(lifecycle.get("incremental_policy"), dict)
                and lifecycle["incremental_policy"].get("immediate_freshness_met")
                is True
            )
            and isinstance(full, dict)
            and isinstance(full.get("elapsed_ms"), (int, float))
            and isinstance(incremental, dict)
            and isinstance(incremental.get("elapsed_ms"), (int, float))
            and float(incremental["elapsed_ms"]) > 0
        ):
            speedups.append(
                float(full["elapsed_ms"]) / float(incremental["elapsed_ms"])
            )
        for index_result, timings in (
            (initial, dependency_initial_ms),
            (incremental, dependency_incremental_ms),
            (full, dependency_fresh_ms),
        ):
            phase_ms, packages = dependency_observation(index_result)
            if phase_ms is not None:
                timings.append(float(phase_ms))
            if packages is not None:
                dependency_packages.append(float(packages))
        case_oracles = case.get("oracles", {})
        if isinstance(case_oracles, dict) and not isinstance(lifecycle, dict):
            quality = case_oracles.get("quality", {})
            if isinstance(quality, dict):
                applicable = int(quality.get("applicable_count") or 0)
                quality_passed += int(quality.get("passed_count") or 0)
                quality_applicable += applicable
                score = quality.get("score")
                hit_at_1 = quality.get("hit_at_1")
                hit_at_5 = quality.get("hit_at_5")
                mean_ndcg_at_5 = quality.get("mean_ndcg_at_5")
                ndcg_applicable = int(quality.get("ndcg_applicable_count") or 0)
                if applicable and isinstance(score, (int, float)):
                    quality_score_weighted += float(score) * applicable
                    quality_score_count += applicable
                if applicable and isinstance(hit_at_1, (int, float)):
                    hit_at_1_weighted += float(hit_at_1) * applicable
                if applicable and isinstance(hit_at_5, (int, float)):
                    hit_at_5_weighted += float(hit_at_5) * applicable
                if ndcg_applicable and isinstance(mean_ndcg_at_5, (int, float)):
                    ndcg_weighted += float(mean_ndcg_at_5) * ndcg_applicable
                    ndcg_count += ndcg_applicable
            for oracle in case_oracles.values():
                if not isinstance(oracle, dict):
                    continue
                if isinstance(oracle.get("elapsed_ms"), (int, float)):
                    query_latency_ms.append(float(oracle["elapsed_ms"]))
                if isinstance(oracle.get("response_bytes"), (int, float)):
                    query_response_bytes.append(float(oracle["response_bytes"]))
                if isinstance(oracle.get("response_token_estimate"), (int, float)):
                    query_response_tokens.append(
                        float(oracle["response_token_estimate"])
                    )
        for relation in relation_oracles:
            response_quality = (
                relation.get("response_quality") if isinstance(relation, dict) else None
            )
            if not isinstance(response_quality, dict):
                continue
            if isinstance(response_quality.get("elapsed_ms"), (int, float)):
                query_latency_ms.append(float(response_quality["elapsed_ms"]))
            if isinstance(response_quality.get("response_bytes"), (int, float)):
                query_response_bytes.append(float(response_quality["response_bytes"]))
            if isinstance(
                response_quality.get("response_token_estimate"), (int, float)
            ):
                query_response_tokens.append(
                    float(response_quality["response_token_estimate"])
                )

    canonical_failed = any(not value for value in core_graph)
    oracle_target_missed = any(not value for value in oracles)
    required_pair_stage_missed = False
    for case in cases:
        lifecycle = case.get("pair_lifecycle")
        if not isinstance(lifecycle, dict):
            continue
        policy = lifecycle.get("incremental_policy")
        immediate_expected = (
            isinstance(policy, dict)
            and policy.get("immediate_freshness_expected") is True
        )
        required = [lifecycle.get("initial_oracles"), lifecycle.get("fresh_oracles")]
        if immediate_expected:
            required.append(lifecycle.get("incremental_oracles"))
        if any(
            isinstance(oracle, dict) and oracle.get("passed") is False
            for oracle in required
        ):
            required_pair_stage_missed = True
            break
    quality_target_missed = oracle_target_missed or required_pair_stage_missed
    missed_oracle_count = sum(1 for value in oracles if not value)
    explicit_ablation_miss = (
        bool(quality_miss_ablation_states)
        and len(quality_miss_ablation_states) == missed_oracle_count
        and all(quality_miss_ablation_states)
    )
    deferred_freshness = any(
        detail["freshness"] == "deferred with warning"
        for detail in pair_quality_details
    )
    declared_stale_views = any(
        isinstance((gate := case.get("graph_gate")), dict)
        and gate.get("policy") == "declared_stale_derived_views"
        and gate.get("passed") is True
        for case in cases
    )
    freshness_policy_failed = any(
        detail.get("policy_conformance_met") is False
        for detail in pair_quality_details
        if detail.get("capability_state") != "disabled"
    )
    if canonical_failed:
        decision = "REJECT: graph correctness"
    elif freshness_policy_failed:
        decision = "REJECT: freshness policy"
    elif quality_target_missed and (capability_quality or explicit_ablation_miss):
        decision = "BELOW QUALITY TARGET"
    elif quality_target_missed:
        decision = "REJECT: task correctness"
    elif case_passes and not all(case_passes) and not capability_quality:
        decision = "REJECT: benchmark gate"
    elif not cases:
        decision = "REJECT: no cases"
    elif declared_stale_views:
        decision = "PASS: DECLARED STALE VIEWS"
    elif deferred_freshness:
        decision = "PASS: DEFERRED FRESHNESS"
    else:
        decision = "PASS"

    hashes = sorted(
        {
            str(report.get("binary_metadata", {}).get("sha256", ""))
            for report in reports
            if isinstance(report.get("binary_metadata"), dict)
            and report.get("binary_metadata", {}).get("sha256")
        }
    )
    pair_f1_values = [
        value
        for detail in pair_quality_details
        for value in (detail.get("initial_f1"), detail.get("fresh_f1"))
        if isinstance(value, (int, float))
    ]
    retrieval_score = (
        quality_score_weighted / quality_score_count
        if quality_score_count
        else quality_passed / quality_applicable
        if quality_applicable
        else None
    )
    pair_f1_score = statistics.mean(pair_f1_values) if pair_f1_values else None
    graph_fidelity_score = sum(canonical) / len(canonical) if canonical else None
    core_graph_fidelity_score = (
        sum(core_graph) / len(core_graph) if core_graph else None
    )
    task_success_score = (
        quality_passed / quality_applicable
        if quality_applicable
        else sum(oracles) / len(oracles)
        if oracles
        else None
    )
    result_quality_values = [
        value
        for value in (retrieval_score, pair_f1_score)
        if isinstance(value, (int, float))
    ]
    result_quality_score = (
        statistics.mean(result_quality_values) if result_quality_values else None
    )
    quality_categories = (
        result_quality_score,
        graph_fidelity_score,
        task_success_score,
    )
    overall_quality_score = (
        math.prod(quality_categories) ** (1.0 / len(quality_categories))
        if all(isinstance(value, (int, float)) for value in quality_categories)
        else None
    )
    scenarios = {str(case.get("scenario")) for case in cases if case.get("scenario")}
    workload_backgrounds: set[str] = set()
    workload_tasks: set[str] = set()
    for report in reports:
        parameters = report.get("parameters")
        if isinstance(parameters, dict):
            for key in ("repository_background", "quality_background"):
                background = parameters.get(key)
                if isinstance(background, dict):
                    workload_backgrounds.add(
                        json.dumps(background, separators=(",", ":"), sort_keys=True)
                    )
    for case in cases:
        background = case.get("background_repository")
        if isinstance(background, dict):
            workload_backgrounds.add(
                json.dumps(background, separators=(",", ":"), sort_keys=True)
            )
        fixture = case.get("fixture")
        if isinstance(fixture, dict) and fixture.get("task_set_sha256"):
            workload_tasks.add(str(fixture["task_set_sha256"]))
    contracts = {
        str(gate.get("contract"))
        for case in cases
        if isinstance((gate := case.get("frontier_coverage_gate")), dict)
        and gate.get("contract")
    }
    frontier_files = {
        parameters.get("frontier_files")
        for report in reports
        if isinstance((parameters := report.get("parameters")), dict)
        and isinstance(parameters.get("frontier_files"), int)
    }
    exact_caps: set[int] = set()
    index_modes = {
        str(parameters.get("index_mode"))
        for report in reports
        if isinstance((parameters := report.get("parameters")), dict)
        and parameters.get("index_mode")
    }
    execution_orders = {
        str(parameters.get("execution_order") or "grouped")
        for report in reports
        if isinstance((parameters := report.get("parameters")), dict)
    }
    for report in reports:
        parameters = report.get("parameters")
        if not isinstance(parameters, dict):
            continue
        overrides = parameters.get("config_overrides")
        if not isinstance(overrides, dict):
            continue
        raw_cap = overrides.get("incremental_exact_max_affected_paths")
        try:
            exact_caps.add(int(raw_cap))
        except (TypeError, ValueError):
            pass
    full_values = full_ms or initial_full_ms
    disabled_pair_capabilities = {
        str(detail.get("capability"))
        for detail in pair_quality_details
        if detail.get("capability_state") == "disabled"
    }
    findings = correctness_findings(
        cases,
        capability_quality=capability_quality,
        disabled_pair_capabilities=disabled_pair_capabilities,
    )
    if freshness_policy_failed:
        findings.append(
            "The incremental semantic result did not conform to the recorded freshness policy; "
            "the post-edit pair result and whole-graph canonical comparison must be interpreted "
            "together."
        )
    disabled_pair_controls = [
        detail
        for detail in pair_quality_details
        if detail.get("capability_state") == "disabled"
    ]
    if disabled_pair_controls:
        findings.append(
            "The explicit capability-off control omitted the judged positive in initial, "
            "post-edit, and fresh results; this is the expected ablation contrast, not a "
            "freshness deferral or execution failure."
        )
    if deferred_freshness:
        findings.insert(
            0,
            "Immediate semantic freshness was intentionally deferred under the recorded policy; "
            "the structured stale warning was present and initial/fresh pair tasks passed",
        )
    return {
        "candidate": label,
        "decision": decision,
        "cases": ratio(sum(case_passes), len(case_passes)),
        "canonical": ratio(sum(canonical), len(canonical)),
        "core_graph": ratio(sum(core_graph), len(core_graph)),
        "oracles": ratio(sum(oracles), len(oracles)),
        "quality_score": retrieval_score,
        "pair_f1_score": pair_f1_score,
        "overall_quality_score": overall_quality_score,
        "graph_fidelity_score": graph_fidelity_score,
        "core_graph_fidelity_score": core_graph_fidelity_score,
        "task_success_score": task_success_score,
        "hit_at_1": hit_at_1_weighted / quality_score_count
        if quality_score_count
        else None,
        "hit_at_5": hit_at_5_weighted / quality_score_count
        if quality_score_count
        else None,
        "ndcg_at_5": ndcg_weighted / ndcg_count if ndcg_count else None,
        "quality_checks": ratio(quality_passed, quality_applicable),
        "query_response_p50_bytes": percentile(query_response_bytes, 0.50),
        "query_response_p50_tokens": percentile(query_response_tokens, 0.50),
        "query_latency_p50_ms": percentile(query_latency_ms, 0.50),
        "query_observations": len(query_latency_ms),
        "query_range_ms": (min(query_latency_ms), max(query_latency_ms))
        if query_latency_ms
        else None,
        "incremental_observations": len(incremental_ms),
        "incremental_range_ms": (min(incremental_ms), max(incremental_ms))
        if incremental_ms
        else None,
        "full_observations": len(full_values),
        "full_range_ms": (min(full_values), max(full_values)) if full_values else None,
        "capabilities": config_label(reports),
        "capability_signature": config_signature(reports),
        "incremental_p50_ms": percentile(incremental_ms, 0.50),
        "incremental_work_p50_ms": percentile(incremental_work_ms, 0.50),
        "incremental_peak_p50_mb": percentile(incremental_peak_rss, 0.50),
        "incremental_p95_ms": percentile(incremental_ms, 0.95),
        "full_p50_ms": percentile(full_values, 0.50),
        "speedup_p50": float(statistics.median(speedups)) if speedups else None,
        "peak_rss_mb": max(peak_rss) if peak_rss else None,
        "dependency_mode": dependency_mode(reports, dependency_packages),
        "dependency_packages_p50": percentile(dependency_packages, 0.50),
        "dependency_initial_p50_ms": percentile(dependency_initial_ms, 0.50),
        "dependency_incremental_p50_ms": percentile(dependency_incremental_ms, 0.50),
        "dependency_fresh_p50_ms": percentile(dependency_fresh_ms, 0.50),
        "index_modes": ", ".join(sorted(index_modes)) if index_modes else "unknown",
        "execution_orders": ", ".join(sorted(execution_orders))
        if execution_orders
        else "unknown",
        "capability_applicability": summarize_capability_applicability(reports),
        "lifecycle": evidence_lifecycle(reports),
        "binary_sha256": ", ".join(value[:12] for value in hashes) or "n/a",
        "findings": findings,
        "quality_details": quality_oracle_details(cases),
        "pair_quality_details": pair_quality_details,
        "mutation_details": mutation_reindex_details(
            cases,
            disabled_pair_capabilities=disabled_pair_capabilities,
        ),
        "scenario": next(iter(scenarios)) if len(scenarios) == 1 else None,
        "pareto_workload": json.dumps(
            {
                "backgrounds": sorted(workload_backgrounds),
                "index_modes": sorted(index_modes),
                "report_modes": sorted(report_modes),
                "scenarios": sorted(scenarios),
                "task_sets": sorted(workload_tasks),
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
        "frontier_files": next(iter(frontier_files))
        if len(frontier_files) == 1
        else None,
        "exact_cap": next(iter(exact_caps)) if len(exact_caps) == 1 else None,
        "frontier_contract": next(iter(contracts)) if len(contracts) == 1 else None,
        "pareto": "unclassified",
        "pareto_reason": "not evaluated",
    }


def historical_delta_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_by_signature = {
        row.get("capability_signature"): row
        for row in rows
        if str(row.get("candidate", "")).startswith("latest-")
        and row.get("capability_signature") is not None
    }
    comparisons: list[dict[str, Any]] = []
    for baseline in rows:
        if str(baseline.get("candidate", "")).startswith("latest-"):
            continue
        latest = latest_by_signature.get(baseline.get("capability_signature"))
        if latest is None:
            continue

        accepted_decisions = {
            "PASS",
            "PASS: DEFERRED FRESHNESS",
            "PASS: DECLARED STALE VIEWS",
        }
        baseline_decision = baseline.get("decision")
        latest_decision = latest.get("decision")
        if (
            baseline_decision not in accepted_decisions
            or latest_decision not in accepted_decisions
        ):
            comparison_status = "not comparable: correctness/quality gate"
        elif baseline_decision != latest_decision:
            comparison_status = "not comparable: freshness/quality decision differs"
        else:
            quality_axes = (
                "overall_quality_score",
                "pair_f1_score",
                "graph_fidelity_score",
                "task_success_score",
            )
            quality_matches = all(
                (baseline.get(axis) is None and latest.get(axis) is None)
                or (
                    isinstance(baseline.get(axis), (int, float))
                    and isinstance(latest.get(axis), (int, float))
                    and math.isclose(
                        float(baseline[axis]),
                        float(latest[axis]),
                        rel_tol=1e-9,
                        abs_tol=1e-12,
                    )
                )
                for axis in quality_axes
            )
            if not quality_matches:
                comparison_status = "not comparable: measured quality differs"
            else:
                minimum_observations = min(
                    int(baseline.get(key) or 0)
                    for key in (
                        "incremental_observations",
                        "full_observations",
                        "query_observations",
                    )
                )
                minimum_observations = min(
                    minimum_observations,
                    *(
                        int(latest.get(key) or 0)
                        for key in (
                            "incremental_observations",
                            "full_observations",
                            "query_observations",
                        )
                    ),
                )
                comparison_status = (
                    "quality-matched repeated evidence"
                    if minimum_observations >= 3
                    else f"descriptive only: minimum matched observation count {minimum_observations}"
                )
        comparable = not comparison_status.startswith("not comparable")

        def speedup(metric: str) -> float | None:
            if not comparable:
                return None
            old = baseline.get(metric)
            new = latest.get(metric)
            return (
                old / new
                if isinstance(old, (int, float))
                and isinstance(new, (int, float))
                and new > 0
                else None
            )

        comparisons.append(
            {
                "latest": latest["candidate"],
                "baseline": baseline["candidate"],
                "incremental_speedup": speedup("incremental_p50_ms"),
                "full_speedup": speedup("full_p50_ms"),
                "query_speedup": speedup("query_latency_p50_ms"),
                "latest_quality": latest.get("overall_quality_score"),
                "baseline_quality": baseline.get("overall_quality_score"),
                "baseline_decision": baseline.get("decision"),
                "comparison_status": comparison_status,
            }
        )
    return comparisons


def frontier_crossover_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Pair the closest configured fallback and exact run for each frontier."""
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        scenario = row.get("scenario")
        frontier_files = row.get("frontier_files")
        if isinstance(scenario, str) and isinstance(frontier_files, int):
            grouped[(scenario, frontier_files)].append(row)

    crossovers: list[dict[str, Any]] = []
    for (scenario, frontier_files), candidates in sorted(grouped.items()):
        fallbacks = [
            row
            for row in candidates
            if row.get("frontier_contract") == "configured_cap_fallback"
            and isinstance(row.get("exact_cap"), int)
        ]
        exact = [
            row
            for row in candidates
            if row.get("frontier_contract") == "exact_frontier"
            and isinstance(row.get("exact_cap"), int)
        ]
        if not fallbacks or not exact:
            continue
        fallback = max(fallbacks, key=lambda row: row["exact_cap"])
        exact_run = min(exact, key=lambda row: row["exact_cap"])
        fallback_elapsed = fallback.get("incremental_p50_ms")
        exact_elapsed = exact_run.get("incremental_p50_ms")
        ratio_value = (
            exact_elapsed / fallback_elapsed
            if isinstance(exact_elapsed, (int, float))
            and isinstance(fallback_elapsed, (int, float))
            and fallback_elapsed > 0
            else None
        )
        if ratio_value is None:
            conclusion = "not measured"
        elif ratio_value < 0.95:
            conclusion = "exact faster"
        elif ratio_value <= 1.05:
            conclusion = "tied"
        else:
            conclusion = "fallback faster"
        crossovers.append(
            {
                "scenario": scenario,
                "affected_files": frontier_files + 1,
                "fallback_cap": fallback["exact_cap"],
                "fallback_p50_ms": fallback_elapsed,
                "fallback_work_p50_ms": fallback.get("incremental_work_p50_ms"),
                "fallback_rss_p50_mb": fallback.get("incremental_peak_p50_mb"),
                "exact_cap": exact_run["exact_cap"],
                "exact_p50_ms": exact_elapsed,
                "exact_work_p50_ms": exact_run.get("incremental_work_p50_ms"),
                "exact_rss_p50_mb": exact_run.get("incremental_peak_p50_mb"),
                "full_p50_ms": exact_run.get("full_p50_ms"),
                "exact_fallback_ratio": ratio_value,
                "conclusion": conclusion,
            }
        )
    return crossovers


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
            other
            for other in eligible
            if other is not row
            and other.get("pareto_workload") == row.get("pareto_workload")
            and dominates(other, row)
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
                "within the same workload, no passing, fully measured candidate is at least as "
                "good on overall quality and every cost axis while being strictly better on one "
                "or more axes"
            )


def display(value: Any, digits: int = 1) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value).replace("|", "\\|")


def display_range(value: Any) -> str:
    if not isinstance(value, tuple) or len(value) != 2:
        return "n/a"
    return f"[{display(value[0])}, {display(value[1])}]"


def display_confusion(value: Any) -> str:
    if not isinstance(value, dict):
        return "n/a"
    return "/".join(str(value.get(key, "n/a")) for key in ("tp", "tn", "fp", "fn"))


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


def render_search_projection(document: dict[str, Any]) -> str:
    if document.get("mode") != "search_projection":
        raise ValueError("expected a search_projection result document")
    observations = document.get("observations")
    derived = document.get("derived")
    if not isinstance(observations, list) or not isinstance(derived, dict):
        raise ValueError(
            "search-projection result is missing observations or derived data"
        )
    completion = document.get("completion")
    status = (
        str(completion.get("status")) if isinstance(completion, dict) else "unknown"
    )
    labels = {
        "compact_default": "compact default",
        "compact_true": "compact true",
        "compact_selected_fields": "compact + selected fields",
        "compact_false": "non-compact",
    }
    lines = [
        "## search_graph JSON projection",
        "",
        f"Outcome: {status} — ranked-result identity parity="
        f"{str(bool(derived.get('identity_parity'))).lower()}, internal fields absent="
        f"{str(bool(derived.get('internal_fields_absent'))).lower()}.",
        "",
        "| Variant | Results | Ranked identities | Property fields | Payload bytes | "
        "Estimated tokens* | Call ms† | Post-call RSS MiB‡ | Transport | Cleanup |",
        "|---|---:|---|---|---:|---:|---:|---:|---|---|",
    ]
    non_compact_fields: list[str] = []
    for item in observations:
        if not isinstance(item, dict):
            continue
        fields = item.get("property_fields")
        typed_fields = list(map(str, fields)) if isinstance(fields, list) else []
        fields_text = (
            f"{len(typed_fields)} fields"
            if len(typed_fields) > 4
            else (", ".join(typed_fields) if typed_fields else "none")
        )
        if item.get("variant") == "compact_false":
            non_compact_fields = typed_fields
        rss_kb = item.get("post_call_rss_kb")
        lines.append(
            "| "
            + " | ".join(
                (
                    labels.get(str(item.get("variant")), str(item.get("variant"))),
                    f"{int(item['returned_count']):,}",
                    "Equal" if item.get("identity_equal_to_default") else "Different",
                    fields_text,
                    f"{int(item['response_bytes']):,}",
                    f"{int(item['response_token_estimate']):,}",
                    f"{float(item['elapsed_ms']):.3f}",
                    f"{float(rss_kb) / 1024:.1f}"
                    if isinstance(rss_kb, (int, float))
                    else "n/a",
                    "Survived" if item.get("transport_survived") else "Interrupted",
                    "Reaped" if item.get("server_reaped") else "Incomplete",
                )
            )
            + " |"
        )
    compact_bytes = derived.get("compact_bytes")
    verbose_bytes = derived.get("non_compact_bytes")
    savings = (
        100.0 * (1.0 - float(compact_bytes) / float(verbose_bytes))
        if isinstance(compact_bytes, (int, float))
        and isinstance(verbose_bytes, (int, float))
        and verbose_bytes
        else None
    )
    binary = document.get("binary_metadata")
    sha = binary.get("sha256") if isinstance(binary, dict) else None
    cleanup = document.get("cleanup")
    cleanup_removed = cleanup.get("removed") if isinstance(cleanup, dict) else None
    lines.extend(
        (
            "",
            "### Interpretation and audit boundary",
            "",
            (
                "- Non-compact property fields: " + ", ".join(non_compact_fields) + "."
                if non_compact_fields
                else "- Non-compact property fields: none."
            ),
            f"- Compact output uses {savings:.1f}% fewer payload bytes than non-compact output."
            if savings is not None
            else "- Compact versus non-compact byte savings were not measured.",
            "- No fp, sp, or bt indexing fields appear in any variant."
            if derived.get("internal_fields_absent")
            else "- One or more internal indexing fields were observed.",
            f"- Claim boundary: {derived.get('claim_boundary', 'projection-only comparison')}",
            f"- Run ID: {document.get('run_id', 'n/a')}; binary SHA-256: {sha or 'n/a'}.",
            f"- Auto-created fixture cleanup confirmed: {str(cleanup_removed).lower()}.",
            "",
            "* Tokens are the deterministic ceil(UTF-8 payload bytes / 4) estimate, not a "
            "model-tokenizer count.",
            "",
            "† There is one observation per variant. The table is a response-projection and "
            "ranked-identity check, not a latency comparison.",
            "",
            "‡ RSS is sampled after each call and is not peak RSS.",
        )
    )
    return "\n".join(lines) + "\n"


def render_list_projects_scaling(document: dict[str, Any]) -> str:
    if document.get("mode") != "list_projects_scaling":
        raise ValueError("expected a list_projects_scaling result document")
    observations = document.get("observations")
    derived = document.get("derived")
    if not isinstance(observations, list) or not isinstance(derived, dict):
        raise ValueError(
            "list-project scaling result is missing observations or derived data"
        )
    completion = document.get("completion")
    completion_status = (
        str(completion.get("status")) if isinstance(completion, dict) else "unknown"
    )
    all_valid = bool(derived.get("passed"))
    outcome_detail = (
        "all requested inventories returned, follow-up MCP requests succeeded, and server "
        "resources were reaped"
        if all_valid
        else "one or more inventory, transport, or teardown checks were incomplete"
    )
    lines = [
        "## `list_projects` response scaling",
        "",
        f"Outcome: {completion_status} — {outcome_detail}.",
        "",
        "| Requested projects | Returned projects | Payload bytes | Estimated tokens* | "
        "Call ms† | Post-call RSS MiB‡ | Transport | Server cleanup | Fixture DB MiB |",
        "|---:|---:|---:|---:|---:|---:|---|---|---:|",
    ]
    for item in observations:
        if not isinstance(item, dict):
            continue
        rss_kb = item.get("post_call_rss_kb")
        fixture_bytes = item.get("fixture_db_bytes")
        lines.append(
            "| "
            + " | ".join(
                (
                    f"{int(item['requested_projects']):,}",
                    f"{int(item['returned_projects']):,}",
                    f"{int(item['response_bytes']):,}",
                    f"{int(item['response_token_estimate']):,}",
                    f"{float(item['elapsed_ms']):.3f}",
                    f"{float(rss_kb) / 1024:.1f}"
                    if isinstance(rss_kb, (int, float))
                    else "n/a",
                    "Survived" if item.get("transport_survived") else "Interrupted",
                    "Reaped" if item.get("server_reaped") else "Incomplete",
                    (
                        f"{float(fixture_bytes) / (1024 * 1024):.1f}"
                        if isinstance(fixture_bytes, (int, float))
                        else "n/a"
                    ),
                )
            )
            + " |"
        )
    growth = derived.get("incremental_response_bytes_per_project")
    claim_boundary = derived.get("claim_boundary")
    binary = document.get("binary_metadata")
    sha = binary.get("sha256") if isinstance(binary, dict) else None
    cleanup = document.get("cleanup")
    cleanup_removed = cleanup.get("removed") if isinstance(cleanup, dict) else None
    lines.extend(
        (
            "",
            "### Interpretation and audit boundary",
            "",
            f"- Observed payload growth: {float(growth):.1f} bytes per added project."
            if isinstance(growth, (int, float))
            else "- Observed payload growth: not measured.",
            f"- Claim boundary: {claim_boundary}"
            if isinstance(claim_boundary, str)
            else "- Claim boundary: this measures `list_projects` alone.",
            f"- Run ID: `{document.get('run_id', 'n/a')}`; binary SHA-256: `{sha or 'n/a'}`.",
            f"- Auto-created fixture cleanup confirmed: {str(cleanup_removed).lower()}.",
            "",
            "* Tokens are the deterministic `ceil(UTF-8 payload bytes / 4)` estimate, not a "
            "model-tokenizer count.",
            "",
            "† This pilot has one observation per project count. Latency is descriptive and must "
            "not be presented as a population estimate or regression threshold.",
            "",
            "‡ RSS is sampled after each call and is not peak RSS. Fixture DB size is transient "
            "isolated-test storage, not response memory or a recommended cache size.",
        )
    )
    return "\n".join(lines) + "\n"


def render_mcp_surface_parity(document: dict[str, Any]) -> str:
    if document.get("mode") != "mcp_surface_parity":
        raise ValueError("expected an mcp_surface_parity result document")
    surfaces = document.get("surfaces")
    comparison = document.get("comparison")
    if not isinstance(surfaces, dict) or not isinstance(comparison, dict):
        raise ValueError("MCP surface result is missing surfaces or comparison")

    classic = surfaces.get("classic")
    pre = surfaces.get("streamlined_pre_reveal")
    post = surfaces.get("streamlined_post_reveal")
    pre_comparison = comparison.get("pre_reveal")
    post_comparison = comparison.get("post_reveal")
    if not all(
        isinstance(value, dict)
        for value in (classic, pre, post, pre_comparison, post_comparison)
    ):
        raise ValueError("MCP surface result is missing one or more parity states")

    assert isinstance(classic, dict)
    assert isinstance(pre, dict)
    assert isinstance(post, dict)
    assert isinstance(pre_comparison, dict)
    assert isinstance(post_comparison, dict)
    capability_parity = comparison.get("capability_parity")
    if not isinstance(capability_parity, list):
        capability_parity = []
    classic_count = classic.get("tool_count")
    post_classic = (
        f"{classic_count}/{classic_count}"
        if post_comparison.get("classic_name_parity") and isinstance(classic_count, int)
        else "incomplete"
    )
    rows = (
        (
            "Pure classic",
            classic,
            f"{classic_count}/{classic_count}"
            if isinstance(classic_count, int)
            else "n/a",
            "n/a (advertised directly)",
        ),
        (
            "Streamlined before reveal",
            pre,
            pre_comparison.get("advertised_classic_tools"),
            pre_comparison.get("dispatch_recognized_classic_tools"),
        ),
        (
            "Same streamlined process after reveal",
            post,
            post_classic,
            "n/a (advertised after reveal)",
        ),
    )
    lines = [
        "## MCP tool-surface parity",
        "",
        "These are three separate discovery states. The post-reveal row comes from the same "
        "streamlined server process as the pre-reveal row.",
        "",
        "### Capability outcomes",
        "",
        "| Capability outcome | Classic advertised | Streamlined before reveal | "
        "Streamlined after reveal | Evidence boundary |",
        "|---|---|---|---|---|",
    ]
    for item in capability_parity:
        if not isinstance(item, dict):
            continue
        pre_state = (
            "advertised"
            if item.get("streamlined_pre_reveal_advertised")
            else "callable but hidden"
            if item.get("streamlined_pre_reveal_callable")
            else "not demonstrated"
        )
        lines.append(
            "| "
            + " | ".join(
                (
                    str(item.get("outcome") or item.get("capability") or "unknown"),
                    "yes" if item.get("classic_advertised") else "no",
                    pre_state,
                    "advertised"
                    if item.get("streamlined_post_reveal_advertised")
                    else "not demonstrated",
                    str(item.get("evidence") or "surface evidence only"),
                )
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "### Discovery and response cost",
            "",
            "| State | Advertised tools | Advertised classic names | Classic handlers recognized* | "
            "tools/list bytes | Estimated tokens† | tools/list ms‡ |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for label, surface, advertised_classic, dispatch in rows:
        lines.append(
            "| "
            + " | ".join(
                (
                    label,
                    display(surface.get("tool_count")),
                    display(advertised_classic),
                    display(dispatch),
                    display(surface.get("response_bytes")),
                    display(surface.get("response_token_estimate")),
                    display(surface.get("list_elapsed_ms"), 3),
                )
            )
            + " |"
        )

    hidden = pre_comparison.get("intentionally_hidden_classic_tools")
    alias = pre_comparison.get("get_code_alias")
    hidden_count = len(hidden) if isinstance(hidden, list) else "unknown"
    alias_text = "not measured"
    if isinstance(alias, dict):
        alias_text = (
            f"property names equal={str(bool(alias.get('property_names_equal'))).lower()}, "
            f"required names equal={str(bool(alias.get('required_names_equal'))).lower()}, "
            f"validation shape equal={str(bool(alias.get('validation_shape_equal'))).lower()}, "
            f"complete advertised schema identical={str(bool(alias.get('schema_equal'))).lower()}"
        )
    lines.extend(
        (
            "",
            "### Parity checks",
            "",
            f"- Pre-reveal intentionally hidden classic names: {hidden_count}.",
            f"- Post-reveal classic name parity: {str(bool(post_comparison.get('classic_name_parity'))).lower()}.",
            f"- Post-reveal classic input-schema parity: {str(bool(post_comparison.get('classic_schema_parity'))).lower()}.",
            f"- Post-reveal full MCP contract parity: "
            f"{str(bool(post_comparison.get('classic_contract_parity'))).lower()}.",
            f"- `notifications/tools/list_changed` observed after reveal: "
            f"{str(bool(post_comparison.get('tools_list_changed_observed'))).lower()}.",
            f"- MCP processes and reader threads reaped: "
            f"{str(bool(comparison.get('lifecycle_passed'))).lower()}.",
            f"- `get_code` versus classic `get_code_snippet`: {alias_text}.",
            "",
            "\\* Handler recognition uses bounded empty-argument `tools/call` requests and only proves "
            "that dispatch did not return `unknown tool`; it does not prove successful execution or "
            "end-to-end behavioral parity. Behavioral parity requires capability fixtures.",
            "",
            "† Estimated as `ceil(UTF-8 response bytes / 4)`; this is not a model-tokenizer count.",
            "",
            "‡ Each state currently has one `tools/list` observation, so latency is descriptive only "
            "and has no confidence interval.",
        )
    )
    return "\n".join(lines) + "\n"


def render_markdown(rows: list[dict[str, Any]]) -> str:
    mark_pareto_frontier(rows)
    lines = [
        "# Codebase Memory performance and quality summary",
        "",
        "| Candidate | Decision | Overall quality† | Retrieval MRR | Pair F1 | Hit@1 | Hit@5 | nDCG@5 | "
        "Core graph | Full graph freshness | Task success | Evidence counts (R/Core/Full/S) | "
        "Response p50 bytes | Response p50 tokens* | Query p50 ms | Incremental p50 ms | "
        "Peak RSS MB | Pareto |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
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
                    display(row["pair_f1_score"], 3),
                    display(row["hit_at_1"], 3),
                    display(row["hit_at_5"], 3),
                    display(row["ndcg_at_5"], 3),
                    display(row["core_graph_fidelity_score"], 3),
                    display(row["graph_fidelity_score"], 3),
                    display(row["task_success_score"], 3),
                    display(
                        f"{row['quality_checks']} / {row['core_graph']} / "
                        f"{row['canonical']} / {row['oracles']}"
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
    pair_detail_count = sum(len(row["pair_quality_details"]) for row in rows)
    if pair_detail_count:
        lines.extend(
            (
                "",
                "## Semantic pair quality and freshness",
                "",
                "Confusion columns are TP/TN/FP/FN over the explicit bounded judgments. "
                "Natural background pairs outside the judgment set remain unjudged.",
                "",
                "| Candidate | Capability | Relationship | Initial TP/TN/FP/FN | Initial F1 | "
                "Post-edit TP/TN/FP/FN | Post-edit F1 | Fresh TP/TN/FP/FN | Fresh F1 | "
                "Freshness policy | Freshness result | Policy conforming | Task SHA | Background commit/tree |",
                "|---|---|---|---:|---:|---:|---:|---:|---:|---|---|---|---|---|",
            )
        )
        for row in rows:
            for detail in row["pair_quality_details"]:
                revision = detail.get("background_revision")
                tree = detail.get("background_tree")
                background = (
                    f"{str(revision)[:12]}/{str(tree)[:12]}"
                    if revision and tree
                    else "synthetic fixture"
                )
                lines.append(
                    "| "
                    + " | ".join(
                        (
                            display(row["candidate"]),
                            display(detail["capability"]),
                            display(detail["relationship"]),
                            display_confusion(detail["initial_confusion"]),
                            display(detail["initial_f1"], 3),
                            display_confusion(detail["incremental_confusion"]),
                            display(detail["incremental_f1"], 3),
                            display_confusion(detail["fresh_confusion"]),
                            display(detail["fresh_f1"], 3),
                            display(detail["freshness_policy"]),
                            display(detail["freshness"]),
                            display(detail["policy_conformance_met"]),
                            display(str(detail.get("task_sha256") or "")[:12]),
                            display(background),
                        )
                    )
                    + " |"
                )
    comparisons = historical_delta_rows(rows)
    if comparisons:
        lines.extend(
            (
                "",
                "## Quality-constrained cross-version timing",
                "",
                "Rows first require matching capability overrides, accepted and identical lifecycle "
                "decisions, and equal measured quality categories. Ratios are suppressed when those "
                "conditions differ. Speedup is baseline latency divided by latest latency, so values "
                "above 1× favor latest; fewer than three matched observations remain descriptive.",
                "",
                "| Latest | Baseline | Incremental speedup | Fresh rebuild speedup | "
                "Query speedup | Latest quality | Baseline quality | Baseline gate | Evidence status |",
                "|---|---|---:|---:|---:|---:|---:|---|---|",
            )
        )
        for comparison in comparisons:

            def multiple(value: Any) -> str:
                return f"{value:.2f}×" if isinstance(value, (int, float)) else "n/a"

            lines.append(
                "| "
                + " | ".join(
                    (
                        display(comparison["latest"]),
                        display(comparison["baseline"]),
                        multiple(comparison["incremental_speedup"]),
                        multiple(comparison["full_speedup"]),
                        multiple(comparison["query_speedup"]),
                        display(comparison["latest_quality"], 3),
                        display(comparison["baseline_quality"], 3),
                        display(comparison["baseline_decision"]),
                        display(comparison["comparison_status"]),
                    )
                )
                + " |"
            )
    lines.extend(
        (
            "",
            "## Incremental mutation and reindex breakdown",
            "",
            "| Candidate | Scenario | Source mutation | Changed paths | Publication route/reason | "
            "Incremental p50 ms | Indexing work p50 ms | Fresh rebuild p50 ms | "
            "Fresh / incremental | Canonical equality |",
            "|---|---|---|---|---|---:|---:|---:|---:|---:|",
        )
    )
    for row in rows:
        for detail in row["mutation_details"]:
            lines.append(
                "| "
                + " | ".join(
                    (
                        display(row["candidate"]),
                        display(detail["scenario"]),
                        display(detail["mutation"]),
                        display(detail["changed_paths"]),
                        display(detail["publication"]),
                        display(detail["incremental_p50_ms"]),
                        display(detail["work_p50_ms"]),
                        display(detail["full_p50_ms"]),
                        display(detail["speedup_p50"], 2),
                        display(detail["canonical"]),
                    )
                )
                + " |"
            )
    lines.extend(
        (
            "",
            "Incremental p50 is the end-to-end response time after applying the named source "
            "mutation. Indexing work p50 isolates indexing work reported inside that response. "
            "Fresh rebuild p50 indexes a separate copy of the same post-mutation tree; canonical "
            "equality compares the incremental graph with that fresh reference graph.",
        )
    )
    lines.extend(
        (
            "",
            "## Dependency-indexing capability and cost",
            "",
            "| Candidate | Dependency mode | Packages indexed p50 | Initial dependency p50 ms | "
            "Incremental dependency p50 ms | Fresh-after-mutation dependency p50 ms |",
            "|---|---|---:|---:|---:|---:|",
        )
    )
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                (
                    display(row["candidate"]),
                    display(row["dependency_mode"]),
                    display(row["dependency_packages_p50"]),
                    display(row["dependency_initial_p50_ms"]),
                    display(row["dependency_incremental_p50_ms"]),
                    display(row["dependency_fresh_p50_ms"]),
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "## Observation ranges",
            "",
            "| Candidate | Incremental n | Incremental p50 ms | Incremental min–max ms | "
            "Query n | Query p50 ms | Query min–max ms | Full n | Full p50 ms | Full min–max ms |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        )
    )
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                (
                    display(row["candidate"]),
                    display(row["incremental_observations"]),
                    display(row["incremental_p50_ms"]),
                    display_range(row["incremental_range_ms"]),
                    display(row["query_observations"]),
                    display(row["query_latency_p50_ms"]),
                    display_range(row["query_range_ms"]),
                    display(row["full_observations"]),
                    display(row["full_p50_ms"]),
                    display_range(row["full_range_ms"]),
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "These are descriptive min–max ranges, not confidence intervals. Campaigns run "
            "sequentially to avoid resource contention. Rows record grouped or paired-interleaved "
            "execution explicitly; interleaving reduces configuration-aligned drift but does not "
            "by itself create an effect-size confidence interval. Medians and ratios remain "
            "descriptive until sufficient paired repetitions are measured.",
        )
    )
    lines.extend(
        (
            "",
            "`enabled (observed)` requires a positive recorded package count. `disabled "
            "(explicit)` and `enabled (explicit)` come from exact config overrides; `unsupported` "
            "requires explicit capability-support metadata. `unknown` is intentionally not guessed "
            "from an old artifact that lacks those signals.",
        )
    )
    lines.extend(
        (
            "",
            "## Algorithm-quality applicability",
            "",
            "| Candidate | Index mode | Rank | Similarity | Semantic edges | Git history | HTTP links | Dependencies |",
            "|---|---|---|---|---|---|---|---|",
        )
    )
    for row in rows:
        applicability = row["capability_applicability"]
        lines.append(
            "| "
            + " | ".join(
                (
                    display(row["candidate"]),
                    display(row["index_modes"]),
                    *(display(applicability[name]) for name in ALGORITHM_CAPABILITIES),
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "Applicability is separate from enabled/disabled state. In particular, FAST mode "
            "does not generate `SIMILAR_TO` or `SEMANTICALLY_RELATED`, so those quality effects "
            "must be N/A rather than zero, pass, or failure. Retained artifacts without explicit "
            "mode metadata remain `unknown`.",
        )
    )
    crossovers = frontier_crossover_rows(rows)
    if crossovers:
        lines.extend(
            (
                "",
                "## Exact-frontier cap crossover",
                "",
                "Each row compares the largest cap that deliberately selected bounded full-index "
                "fallback with the smallest measured cap that admitted exact incremental work for "
                "the same mutation. Affected files include the changed root plus the generated frontier.",
                "",
                "| Scenario | Affected files | Fallback cap | Fallback p50 ms | Fallback work p50 ms | "
                "Fallback RSS p50 MB | Exact cap | Exact p50 ms | Exact work p50 ms | "
                "Exact RSS p50 MB | Fresh full p50 ms | Exact / fallback | Conclusion |",
                "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
            )
        )
        for crossover in crossovers:
            ratio_value = crossover["exact_fallback_ratio"]
            rendered_ratio = (
                f"{ratio_value:.2f}×"
                if isinstance(ratio_value, (int, float))
                else "n/a"
            )
            lines.append(
                "| "
                + " | ".join(
                    (
                        display(crossover["scenario"]),
                        display(crossover["affected_files"]),
                        display(crossover["fallback_cap"]),
                        display(crossover["fallback_p50_ms"]),
                        display(crossover["fallback_work_p50_ms"]),
                        display(crossover["fallback_rss_p50_mb"]),
                        display(crossover["exact_cap"]),
                        display(crossover["exact_p50_ms"]),
                        display(crossover["exact_work_p50_ms"]),
                        display(crossover["exact_rss_p50_mb"]),
                        display(crossover["full_p50_ms"]),
                        rendered_ratio,
                        display(crossover["conclusion"]),
                    )
                )
                + " |"
            )
        lines.extend(
            (
                "",
                "`p50 ms` is end-to-end incremental response latency; `work p50 ms` isolates the "
                "indexing work reported inside that response. RSS is recorded only in benchmark "
                "profiling mode. Ratios within ±5% are labelled tied.",
            )
        )
    lines.extend(
        (
            "",
            "## Performance and provenance",
            "",
            "| Candidate | Cases meeting gate/target | Capabilities | Execution order | Observations (incremental/full) | Incremental p95 ms | Full p50 ms | "
            "Speedup p50 | Evidence lifecycle | Binary SHA-256 |",
            "|---|---:|---|---|---:|---:|---:|---:|---:|---|",
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
                    display(row["execution_orders"]),
                    display(
                        f"{row['incremental_observations']}/{row['full_observations']}"
                    ),
                    display(row["incremental_p95_ms"]),
                    display(row["full_p50_ms"]),
                    display(row["speedup_p50"], 2),
                    display(row["lifecycle"]),
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
            "| Candidate | Scenario | Oracle | Criterion | Expected evidence | Judgments | Result | RR | Hit@1 | Hit@5 | nDCG@5 |",
            "|---|---|---|---|---|---|---|---:|---:|---:|---:|",
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
                        display(detail["judgments"]),
                        display(detail["result"]),
                        display(detail["reciprocal_rank"], 3),
                        display(detail["hit_at_1"]),
                        display(detail["hit_at_5"]),
                        display(detail["ndcg_at_5"], 3),
                    )
                )
                + " |"
            )
    if not detail_count:
        lines.append(
            "| all | n/a | n/a | No per-oracle quality evidence recorded | n/a | n/a | "
            "N/A | n/a | n/a | n/a | n/a |"
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
        if row["findings"]:
            evidence = row["findings"]
        elif str(row["decision"]).startswith("PASS"):
            evidence = ["All applicable canonical-graph and task-oracle checks passed."]
        else:
            evidence = [
                f"{row['decision']}: no stage-level witness was recorded; inspect the retained "
                "raw result before drawing a causal conclusion."
            ]
        lines.append(
            f"| {display(row['candidate'])} | {display('; '.join(evidence))} |"
        )
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
            "follow NIST's official TREC QA definition: "
            "[TREC QA evaluation data](https://trec.nist.gov/data/qa.html).",
            "Graded probes additionally report nDCG@5, which rewards placing more-relevant "
            "evidence earlier while normalizing against the ideal judged ordering. MRR and Hit@k "
            "remain visible because they answer the distinct first-useful-result question. This "
            "follows Järvelin and Kekäläinen's primary definition in "
            "[Cumulated Gain-based Evaluation of IR Techniques]"
            "(https://doi.org/10.1145/582415.582418).",
            "",
            "Graph fidelity is split into two visible categories. Core graph is the fraction of "
            "mutation cases whose non-stale canonical rows equal a "
            "matching-mode fresh rebuild. A declared-stale gate can pass only when the harness removes "
            "the specifically named derived rows and every remaining canonical node, edge, property, "
            "and file hash still matches. Full graph freshness requires unfiltered canonical equality. "
            "Task success is the fraction of applicable probes that find "
            "their required evidence. Pair F1 is the mean of the explicit initial and fresh semantic-"
            "pair classification tasks; an expected deferred post-edit view remains visible separately "
            "and does not masquerade as retrieval MRR. Evidence counts show retrieval probes / graph comparisons / strict "
            "whole-scenario passes. The named breakdown above shows why a result is, for example, 4/5 "
            "rather than hiding the failed task.",
            "",
            "† Overall quality is a custom descriptive score: the equal-weight geometric mean of "
            "result quality, full graph freshness, and task success. Result quality is Pair F1 or "
            "MRR when only one is measured, and their arithmetic mean when both are measured. It is "
            "N/A unless all three categories are measured. It never overrides a graph-correctness gate. "
            "A required mutation oracle can "
            "reject a correctness benchmark; an algorithm-ablation oracle that misses its declared "
            "cutoff is labelled BELOW QUALITY TARGET instead of being called broken. Category values "
            "remain visible so the aggregate cannot hide which capability changed.",
            "",
            "Query p50 aggregates the recorded default-response oracle calls. Indexing p50/p95 use only "
            "the recorded indexing observations; consult Cases and the immutable campaign manifest before "
            "treating a small pilot as a population estimate.",
            "Performance ratios require matched experiment identities and enough independent repetitions "
            "for an effect-size confidence interval. This report shows observation counts and does not "
            "invent an interval for one- or three-observation pilots. The experiment-design rationale "
            "follows [Kalibera and Jones, Quantifying Performance Changes with Effect Size Confidence "
            "Intervals](https://arxiv.org/abs/2007.10899).",
            "",
            "Pareto status considers only candidates that meet the declared quality target, pass "
            "correctness, and have every axis measured. It maximizes overall quality while minimizing "
            "incremental and query latency, response-token estimate, and peak RSS. This is exact "
            "pairwise nondominance over the measured candidates, using the Pareto relation described "
            "by [Deb et al.](https://doi.org/10.1109/4235.996017); it does not run NSGA-II.",
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


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_campaign_runner() -> Any:
    path = Path(__file__).resolve().with_name("run-benchmark-campaign.py")
    spec = importlib.util.spec_from_file_location(
        "run_benchmark_campaign_for_summary", path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load campaign runner: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _composition_path(base: Path, value: Any, field: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty path string")
    path = Path(value).expanduser()
    return (base / path).resolve() if not path.is_absolute() else path.resolve()


def load_composition_groups(
    composition_path: Path, campaign_runner: Any | None = None
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    """Resolve completed campaign cells into exact cross-scenario report groups."""
    composition_path = composition_path.expanduser().resolve()
    with composition_path.open(encoding="utf-8") as stream:
        composition = json.load(stream)
    if not isinstance(composition, dict) or composition.get("schema_version") != 1:
        raise ValueError("composition schema_version must be 1")
    groups = composition.get("groups")
    if not isinstance(groups, list) or not groups:
        raise ValueError("composition groups must be a non-empty array")
    campaigns = composition.get("campaigns")
    if not isinstance(campaigns, dict) or not campaigns:
        raise ValueError("composition campaigns must be a non-empty object")
    runner = campaign_runner or load_campaign_runner()
    base = composition_path.parent
    resolved_campaigns: dict[str, tuple[list[dict[str, Any]], Path]] = {}
    campaign_records: list[dict[str, Any]] = []
    for campaign_name, campaign in campaigns.items():
        if (
            not isinstance(campaign_name, str)
            or not campaign_name
            or not isinstance(campaign, dict)
        ):
            raise ValueError(
                "composition campaign entries must have non-empty names and objects"
            )
        prefix = f"campaigns.{campaign_name}"
        matrix_value = campaign.get("matrix_spec")
        plan_value = campaign.get("plan")
        if (matrix_value is None) == (plan_value is None):
            raise ValueError(
                f"{prefix} must declare exactly one of matrix_spec or plan"
            )
        campaign_root = _composition_path(
            base, campaign.get("campaign_root"), f"{prefix}.campaign_root"
        )
        if plan_value is not None:
            source_path = _composition_path(base, plan_value, f"{prefix}.plan")
            with source_path.open(encoding="utf-8") as stream:
                plan = json.load(stream)
            cells = runner.validate_plan(plan)
            source_kind = "immutable_plan"
        else:
            source_path = _composition_path(base, matrix_value, f"{prefix}.matrix_spec")
            with source_path.open(encoding="utf-8") as stream:
                matrix_spec = json.load(stream)
            plan = runner.expand_matrix_spec(matrix_spec)
            cells = plan.get("cells") if isinstance(plan, dict) else None
            if not isinstance(cells, list):
                raise ValueError(f"{prefix}.matrix_spec did not expand to cells")
            source_kind = "live_matrix_expansion"
        resolved_campaigns[campaign_name] = (cells, campaign_root)
        campaign_records.append(
            {
                "campaign": campaign_name,
                "source_kind": source_kind,
                "source_path": str(source_path),
                "source_sha256": file_sha256(source_path),
            }
        )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    input_records: list[dict[str, Any]] = []
    seen_labels: set[str] = set()
    for group_index, group in enumerate(groups):
        if not isinstance(group, dict):
            raise ValueError(f"groups[{group_index}] must be an object")
        label = group.get("label")
        if not isinstance(label, str) or not label or label in seen_labels:
            raise ValueError(
                f"groups[{group_index}].label must be non-empty and unique"
            )
        seen_labels.add(label)
        inputs = group.get("inputs")
        if not isinstance(inputs, list) or not inputs:
            raise ValueError(f"groups[{group_index}].inputs must be a non-empty array")
        for source_index, source in enumerate(inputs):
            if not isinstance(source, dict):
                raise ValueError(
                    f"groups[{group_index}].inputs[{source_index}] must be an object"
                )
            prefix = f"groups[{group_index}].inputs[{source_index}]"
            campaign_name = source.get("campaign")
            if (
                not isinstance(campaign_name, str)
                or campaign_name not in resolved_campaigns
            ):
                raise ValueError(f"{prefix}.campaign must name a declared campaign")
            cells, campaign_root = resolved_campaigns[campaign_name]
            cell_labels = source.get("cell_labels")
            if (
                not isinstance(cell_labels, list)
                or not cell_labels
                or not all(isinstance(item, str) and item for item in cell_labels)
            ):
                raise ValueError(
                    f"{prefix}.cell_labels must be a non-empty string array"
                )
            requested = set(cell_labels)
            selected = [cell for cell in cells if cell.get("label") in requested]
            found = {cell.get("label") for cell in selected}
            missing_labels = sorted(requested - found)
            if missing_labels:
                raise ValueError(
                    f"{prefix} cell labels not found: {', '.join(missing_labels)}"
                )
            inputs = runner.completed_report_inputs(campaign_root, selected)
            if len(inputs) != len(selected):
                raise ValueError(
                    f"{prefix} has {len(inputs)} validated completions for {len(selected)} cells"
                )
            for cell_label, input_path in inputs:
                with input_path.open(encoding="utf-8") as stream:
                    document = json.load(stream)
                if not isinstance(document, dict):
                    raise ValueError(f"expected JSON object in {input_path}")
                grouped[label].append(document)
                input_records.append(
                    {
                        "group": label,
                        "cell_label": cell_label,
                        "input_path": str(input_path.resolve()),
                        "input_sha256": file_sha256(input_path),
                    }
                )
    provenance = {
        "schema_version": 1,
        "spec_path": str(composition_path),
        "spec_sha256": file_sha256(composition_path),
        "campaigns": campaign_records,
        "input_count": len(input_records),
        "inputs": input_records,
    }
    return grouped, provenance


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", action="append", default=[], type=parse_input)
    parser.add_argument(
        "--composition-spec",
        type=Path,
        help="Compose exact labels from validated cells in multiple durable campaigns.",
    )
    parser.add_argument(
        "--mcp-surface-parity",
        action="append",
        default=[],
        type=Path,
        help="Append a three-state MCP surface section from a retained parity JSON result.",
    )
    parser.add_argument(
        "--list-projects-scaling",
        action="append",
        default=[],
        type=Path,
        help="Append a list_projects response-scaling section from retained JSON.",
    )
    parser.add_argument(
        "--search-projection",
        action="append",
        default=[],
        type=Path,
        help="Append a search_graph compact-projection section from retained JSON.",
    )
    parser.add_argument("--out", default="")
    args = parser.parse_args()
    if (
        not args.input
        and not args.composition_spec
        and not args.mcp_surface_parity
        and not args.list_projects_scaling
        and not args.search_projection
    ):
        parser.error(
            "at least one --input, --composition-spec, --mcp-surface-parity, "
            "--list-projects-scaling, or --search-projection is required"
        )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    composition_provenance: dict[str, Any] | None = None
    for label, path in args.input:
        with path.open(encoding="utf-8") as stream:
            document = json.load(stream)
        if not isinstance(document, dict):
            raise SystemExit(f"error: expected JSON object in {path}")
        grouped[label].append(document)
    if args.composition_spec:
        try:
            composed, composition_provenance = load_composition_groups(
                args.composition_spec
            )
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise SystemExit(f"error: invalid composition spec: {exc}") from exc
        for label, documents in composed.items():
            grouped[label].extend(documents)
    sections: list[str] = []
    if grouped:
        sections.append(
            render_markdown(
                [summarize_group(label, reports) for label, reports in grouped.items()]
            ).rstrip()
        )
    for raw_path in args.mcp_surface_parity:
        path = raw_path.expanduser()
        with path.open(encoding="utf-8") as stream:
            document = json.load(stream)
        if not isinstance(document, dict):
            raise SystemExit(f"error: expected JSON object in {path}")
        sections.append(render_mcp_surface_parity(document).rstrip())
    for raw_path in args.list_projects_scaling:
        path = raw_path.expanduser()
        with path.open(encoding="utf-8") as stream:
            document = json.load(stream)
        if not isinstance(document, dict):
            raise SystemExit(f"error: expected JSON object in {path}")
        sections.append(render_list_projects_scaling(document).rstrip())
    for raw_path in args.search_projection:
        path = raw_path.expanduser()
        with path.open(encoding="utf-8") as stream:
            document = json.load(stream)
        if not isinstance(document, dict):
            raise SystemExit(f"error: expected JSON object in {path}")
        sections.append(render_search_projection(document).rstrip())
    if composition_provenance:
        sections.append(
            "\n".join(
                (
                    "## Composition provenance",
                    "",
                    f"- Spec: `{composition_provenance['spec_path']}`",
                    f"- Spec SHA-256: `{composition_provenance['spec_sha256']}`",
                    f"- Validated campaign inputs: {composition_provenance['input_count']}",
                    "- Per-input paths and SHA-256 values are retained in the sidecar manifest.",
                )
            )
        )
    markdown = "\n\n".join(sections) + "\n"
    if args.out:
        output = Path(args.out).expanduser()
        atomic_write_text(output, markdown)
        if composition_provenance:
            manifest_output = output.with_name(output.name + ".manifest.json")
            atomic_write_text(
                manifest_output,
                json.dumps(composition_provenance, indent=2, sort_keys=True) + "\n",
            )
    print(markdown, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

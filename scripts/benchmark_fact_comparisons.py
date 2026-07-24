#!/usr/bin/env python3
"""Derive auditable comparison and lifecycle views from benchmark fact bundles."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile
from typing import Any, Iterable


SCHEMA_VERSION = 1
SCHEMA_URI = "docs/schema/benchmark-comparisons-v1.schema.json"
FACT_SCHEMA_URIS = {
    1: "docs/schema/benchmark-facts-v1.schema.json",
    2: "docs/schema/benchmark-facts-v2.schema.json",
}
PARITY_JOIN_ID = "parity_manifest_and_contract_v1"
CAPABILITY_DELTA_JOIN_ID = "capability_delta_manifest_v1"
MEDIAN_FORMULA_ID = "median_elapsed_ms_v1"
RATIO_FORMULA_ID = "left_elapsed_divided_by_right_elapsed_v1"
LIFECYCLE_STEP_IDS = (
    "initial_index",
    "incremental_index",
    "clean_rebuild_index",
)
REPO_ROOT = Path(__file__).resolve().parents[1]
TERMINOLOGY_PATH = REPO_ROOT / "docs" / "benchmark-terminology.json"


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")


def content_id(value: Any, length: int = 24) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()[:length]


def is_unknown(value: Any) -> bool:
    if isinstance(value, dict):
        if value.get("status") == "unknown":
            return True
        return any(is_unknown(child) for child in value.values())
    if isinstance(value, list):
        return any(is_unknown(child) for child in value)
    return False


def load_fact_bundle(path: Path) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError(f"fact bundle must be an object: {path}")
    version = document.get("schema_version")
    expected_uri = FACT_SCHEMA_URIS.get(version)
    if expected_uri is None or document.get("$schema") != expected_uri:
        raise ValueError(f"unsupported or mismatched fact schema in {path}")
    for table in ("runs", "steps", "results", "artifacts"):
        if not isinstance(document.get(table), list):
            raise ValueError(f"fact bundle {table} must be an array: {path}")
    if len(document["runs"]) != 1 or not isinstance(document["runs"][0], dict):
        raise ValueError(f"fact bundle must contain one run row: {path}")
    run_id = document["runs"][0].get("run_id")
    if not isinstance(run_id, str):
        raise ValueError(f"fact bundle run_id is invalid: {path}")
    for field, expected_type in (
        ("cell_label", str),
        ("mode", str),
        ("implementation", dict),
        ("capabilities", dict),
        ("scope", dict),
        ("cache", dict),
        ("host", dict),
        ("harness", dict),
    ):
        if not isinstance(document["runs"][0].get(field), expected_type):
            raise ValueError(f"fact bundle run {field} is invalid: {path}")
    if version == 2:
        for field in (
            "terminology_version",
            "terminology_sha256",
            "generator_revision",
        ):
            if not isinstance(document.get(field), str):
                raise ValueError(f"fact bundle {field} is invalid: {path}")
    for table in ("steps", "results", "artifacts"):
        if any(
            not isinstance(row, dict) or row.get("run_id") != run_id
            for row in document[table]
        ):
            raise ValueError(f"fact bundle {table} has a foreign run_id: {path}")
    return document


def result_contract_projection(
    results: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    projection = []
    for row in results:
        value = row.get("value")
        contract: Any = None
        if isinstance(value, dict):
            contract = {
                key: value[key]
                for key in (
                    "scenario",
                    "criterion",
                    "expected_substring",
                    "applicable",
                    "policy",
                    "declared_stale_views",
                    "excluded_edge_types",
                )
                if key in value
            }
        projection.append(
            {
                "result_id": row.get("result_id"),
                "kind": row.get("kind"),
                "contract": contract,
            }
        )
    return sorted(projection, key=lambda row: (str(row["result_id"]), str(row["kind"])))


def implementation_projection(implementation: Any) -> dict[str, Any]:
    if not isinstance(implementation, dict):
        return {}
    binary = implementation.get("binary")
    return {
        "revision": implementation.get("revision"),
        "binary": (
            {
                key: binary.get(key)
                for key in ("sha256", "size_bytes")
                if key in binary
            }
            if isinstance(binary, dict)
            else {}
        ),
        "build": implementation.get("build"),
    }


def capability_projection(capabilities: Any) -> dict[str, Any]:
    if not isinstance(capabilities, dict):
        return {}
    return {
        key: capabilities.get(key)
        for key in ("values", "completeness")
        if key in capabilities
    }


def harness_projection(harness: Any) -> dict[str, Any]:
    if not isinstance(harness, dict):
        return {}
    return {
        key: harness.get(key)
        for key in ("fact_schema_version", "sha256")
        if key in harness
    }


def benchmark_contract_projection(
    bundle: dict[str, Any], run: dict[str, Any]
) -> dict[str, Any]:
    return {
        "fact_schema_version": bundle.get("schema_version"),
        "terminology_version": bundle.get(
            "terminology_version",
            {"status": "unknown", "reason": "fact_schema_v1_did_not_record_it"},
        ),
        "terminology_sha256": bundle.get(
            "terminology_sha256",
            {"status": "unknown", "reason": "fact_schema_v1_did_not_record_it"},
        ),
        "generator_revision": bundle.get(
            "generator_revision",
            {"status": "unknown", "reason": "fact_schema_v1_did_not_record_it"},
        ),
        "harness": harness_projection(run.get("harness")),
    }


def cell_group_identity(
    bundle: dict[str, Any], run: dict[str, Any], results: list[dict[str, Any]]
) -> dict[str, Any]:
    return {
        "label": run.get("cell_label"),
        "mode": run.get("mode"),
        "implementation": implementation_projection(run.get("implementation")),
        "capabilities": capability_projection(run.get("capabilities")),
        "scope": run.get("scope"),
        "cache": run.get("cache"),
        "host": run.get("host"),
        "benchmark_contract": benchmark_contract_projection(bundle, run),
        "result_contract": result_contract_projection(results),
    }


def aggregate_steps(step_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in step_rows:
        step_id = row.get("step_id")
        elapsed = row.get("elapsed_ms")
        if (
            isinstance(step_id, str)
            and isinstance(elapsed, (int, float))
            and not isinstance(elapsed, bool)
        ):
            grouped.setdefault(step_id, []).append(row)
    aggregates = []
    for step_id, rows in sorted(grouped.items()):
        values = [float(row["elapsed_ms"]) for row in rows]
        aggregates.append(
            {
                "step_id": step_id,
                "formula_id": MEDIAN_FORMULA_ID,
                "count": len(values),
                "median_elapsed_ms": statistics.median(values),
                "min_elapsed_ms": min(values),
                "max_elapsed_ms": max(values),
                "source_occurrence_ids": sorted(
                    str(row["occurrence_id"]) for row in rows
                ),
            }
        )
    return aggregates


def aggregate_results(result_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in result_rows:
        result_id = row.get("result_id")
        kind = row.get("kind")
        if isinstance(result_id, str) and isinstance(kind, str):
            grouped.setdefault((result_id, kind), []).append(row)
    aggregates = []
    for (result_id, kind), rows in sorted(grouped.items()):
        statuses = [str(row.get("status")) for row in rows]
        aggregates.append(
            {
                "result_id": result_id,
                "kind": kind,
                "statuses": statuses,
                "all_passed": bool(statuses)
                and all(status == "passed" for status in statuses),
                "source_run_ids": sorted(str(row["run_id"]) for row in rows),
            }
        )
    return aggregates


def aggregate_cells(
    sources: list[tuple[Path, dict[str, Any]]],
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    seen_run_ids: dict[str, Path] = {}
    for path, bundle in sources:
        run = bundle["runs"][0]
        run_id = run["run_id"]
        previous_path = seen_run_ids.get(run_id)
        if previous_path is not None:
            raise ValueError(
                f"duplicate fact run_id {run_id}: {previous_path} and {path}"
            )
        seen_run_ids[run_id] = path
        identity = cell_group_identity(bundle, run, bundle["results"])
        group_id = content_id(identity)
        group = groups.setdefault(
            group_id,
            {
                "cell_group_id": group_id,
                "label": run.get("cell_label"),
                "mode": run.get("mode"),
                "implementation": identity["implementation"],
                "capabilities": identity["capabilities"],
                "scope": run.get("scope"),
                "cache": run.get("cache"),
                "host": run.get("host"),
                "benchmark_contract": identity["benchmark_contract"],
                "result_contract": identity["result_contract"],
                "source_run_ids": [],
                "source_fact_paths": [],
                "_steps": [],
                "_results": [],
            },
        )
        group["source_run_ids"].append(run["run_id"])
        group["source_fact_paths"].append(str(path))
        group["_steps"].extend(bundle["steps"])
        group["_results"].extend(bundle["results"])
    cells = []
    for group in groups.values():
        group["source_run_ids"].sort()
        group["source_fact_paths"].sort()
        group["step_aggregates"] = aggregate_steps(group.pop("_steps"))
        group["result_aggregates"] = aggregate_results(group.pop("_results"))
        cells.append(group)
    return sorted(
        cells,
        key=lambda cell: (
            str(cell.get("label")),
            str(cell.get("implementation", {}).get("revision")),
            cell["cell_group_id"],
        ),
    )


def capability_differences(left: Any, right: Any) -> list[dict[str, Any]]:
    left_values = left.get("values", {}) if isinstance(left, dict) else {}
    right_values = right.get("values", {}) if isinstance(right, dict) else {}
    if not isinstance(left_values, dict) or not isinstance(right_values, dict):
        return []
    differences = []
    for key in sorted(set(left_values) | set(right_values)):
        if left_values.get(key) != right_values.get(key):
            differences.append(
                {
                    "capability_id": key,
                    "left": left_values.get(key),
                    "right": right_values.get(key),
                }
            )
    return differences


def manifest_equal(left: dict[str, Any], right: dict[str, Any], key: str) -> bool:
    return canonical_json_bytes(left.get(key)) == canonical_json_bytes(right.get(key))


def capabilities_complete(cell: dict[str, Any]) -> bool:
    capabilities = cell.get("capabilities")
    return (
        isinstance(capabilities, dict)
        and capabilities.get("completeness") == "complete_declared_cell"
        and not is_unknown(capabilities)
    )


def common_step_ratios(
    left: dict[str, Any], right: dict[str, Any]
) -> list[dict[str, Any]]:
    left_steps = {row["step_id"]: row for row in left["step_aggregates"]}
    right_steps = {row["step_id"]: row for row in right["step_aggregates"]}
    rows = []
    for step_id in sorted(set(left_steps) & set(right_steps)):
        numerator = left_steps[step_id]["median_elapsed_ms"]
        denominator = right_steps[step_id]["median_elapsed_ms"]
        rows.append(
            {
                "step_id": step_id,
                "formula_id": RATIO_FORMULA_ID,
                "left_median_elapsed_ms": numerator,
                "right_median_elapsed_ms": denominator,
                "left_elapsed_divided_by_right_elapsed": (
                    numerator / denominator if denominator > 0 else None
                ),
                "left_source_occurrence_ids": left_steps[step_id][
                    "source_occurrence_ids"
                ],
                "right_source_occurrence_ids": right_steps[step_id][
                    "source_occurrence_ids"
                ],
            }
        )
    return rows


def classify_pair(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    same_mode = left.get("mode") == right.get("mode")
    same_scope = manifest_equal(left, right, "scope")
    same_cache = manifest_equal(left, right, "cache")
    same_host = manifest_equal(left, right, "host")
    same_benchmark_contract = manifest_equal(left, right, "benchmark_contract")
    same_contract = manifest_equal(left, right, "result_contract")
    same_capabilities = manifest_equal(left, right, "capabilities")
    complete = capabilities_complete(left) and capabilities_complete(right)
    manifest_unknown = any(
        is_unknown(cell.get(key))
        for cell in (left, right)
        for key in (
            "scope",
            "cache",
            "host",
            "benchmark_contract",
            "result_contract",
        )
    )
    common = {
        "comparison_id": content_id(
            {
                "left": left["cell_group_id"],
                "right": right["cell_group_id"],
            }
        ),
        "left_cell_group_id": left["cell_group_id"],
        "right_cell_group_id": right["cell_group_id"],
        "left_source_run_ids": left["source_run_ids"],
        "right_source_run_ids": right["source_run_ids"],
    }
    if (
        same_mode
        and same_scope
        and same_cache
        and same_host
        and same_benchmark_contract
        and same_contract
        and same_capabilities
        and complete
        and not manifest_unknown
    ):
        return {
            **common,
            "comparison_kind": "parity_comparison",
            "join_id": PARITY_JOIN_ID,
            "ratio_allowed": True,
            "capability_differences": [],
            "step_comparisons": common_step_ratios(left, right),
            "limitations": [],
        }
    differences = capability_differences(
        left.get("capabilities"), right.get("capabilities")
    )
    if (
        same_mode
        and same_scope
        and same_cache
        and same_host
        and same_benchmark_contract
        and same_contract
        and complete
        and differences
    ):
        limitations = []
        if manifest_unknown:
            limitations.append(
                "scope, cache, host, benchmark-contract, or correctness metadata "
                "contains unknown values"
            )
        return {
            **common,
            "comparison_kind": "capability_delta_comparison",
            "join_id": CAPABILITY_DELTA_JOIN_ID,
            "ratio_allowed": False,
            "capability_differences": differences,
            "step_comparisons": [],
            "limitations": limitations,
        }
    reasons = []
    for matched, reason in (
        (same_mode, "workload mode differs"),
        (same_scope, "scope manifest differs"),
        (same_cache, "cache manifest differs"),
        (same_host, "host manifest differs"),
        (same_benchmark_contract, "benchmark contract differs"),
        (same_contract, "correctness contract differs"),
        (complete, "capability manifest is incomplete or unknown"),
    ):
        if not matched:
            reasons.append(reason)
    if manifest_unknown:
        reasons.append("required manifest or benchmark contract contains unknown values")
    return {
        **common,
        "comparison_kind": "not_eligible",
        "join_id": None,
        "ratio_allowed": False,
        "capability_differences": differences,
        "step_comparisons": [],
        "limitations": sorted(set(reasons)),
    }


def generate_comparison_document(
    sources: list[tuple[Path, dict[str, Any]]],
    *,
    generated_at_utc: str,
    terminology_version: str,
    terminology_sha256: str,
) -> dict[str, Any]:
    cells = aggregate_cells(sources)
    comparisons = [
        classify_pair(cells[left], cells[right])
        for left in range(len(cells))
        for right in range(left + 1, len(cells))
    ]
    source_bundles = [
        {
            "path": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "schema_version": bundle["schema_version"],
            "run_id": bundle["runs"][0]["run_id"],
        }
        for path, bundle in sorted(sources, key=lambda item: str(item[0]))
    ]
    return {
        "$schema": SCHEMA_URI,
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated_at_utc,
        "terminology_version": terminology_version,
        "terminology_sha256": terminology_sha256,
        "joins": [
            {
                "join_id": PARITY_JOIN_ID,
                "fields": [
                    "mode",
                    "capabilities",
                    "scope",
                    "cache",
                    "host",
                    "benchmark_contract",
                    "result_contract",
                ],
                "unknown_values_allowed": False,
                "ratio_allowed": True,
            },
            {
                "join_id": CAPABILITY_DELTA_JOIN_ID,
                "fields": [
                    "mode",
                    "scope",
                    "cache",
                    "host",
                    "benchmark_contract",
                    "result_contract",
                ],
                "unknown_values_allowed": True,
                "ratio_allowed": False,
            },
        ],
        "formulas": [
            {
                "formula_id": MEDIAN_FORMULA_ID,
                "expression": "versioned median of elapsed_ms for one cell group and step_id",
            },
            {
                "formula_id": RATIO_FORMULA_ID,
                "expression": "left median elapsed_ms / right median elapsed_ms",
            },
        ],
        "source_bundles": source_bundles,
        "cell_groups": cells,
        "comparisons": comparisons,
        "lifecycle_rows": [
            {
                "cell_group_id": cell["cell_group_id"],
                "label": cell["label"],
                "source_run_ids": cell["source_run_ids"],
                "steps": [
                    step
                    for step in cell["step_aggregates"]
                    if step["step_id"] in LIFECYCLE_STEP_IDS
                ],
                "wall_time_rule": (
                    "each listed lifecycle step uses its recorded outer elapsed_ms; "
                    "component spans are not summed"
                ),
            }
            for cell in cells
        ],
    }


def render_markdown(document: dict[str, Any]) -> str:
    comparisons = document["comparisons"]
    parity = [
        row for row in comparisons if row["comparison_kind"] == "parity_comparison"
    ]
    deltas = [
        row
        for row in comparisons
        if row["comparison_kind"] == "capability_delta_comparison"
    ]
    rejected = [row for row in comparisons if row["comparison_kind"] == "not_eligible"]
    lines = [
        "## Fact-derived comparison audit",
        "",
        f"- Source fact bundles: {len(document['source_bundles'])}",
        f"- Cell groups: {len(document['cell_groups'])}",
        f"- Apples-to-apples parity pairs: {len(parity)}",
        f"- Apples-to-oranges capability-delta pairs: {len(deltas)}",
        f"- Ineligible pairs: {len(rejected)}",
        "",
        "Ratios appear only for parity pairs. Capability-delta rows deliberately carry no "
        "cross-implementation speed ratio. Component spans remain separate when their "
        "recorded boundaries cannot prove serial execution.",
        "",
        "### Lifecycle fact table",
        "",
        "| Configuration | Step | Median ms | Repetitions | Source occurrence IDs |",
        "|---|---|---:|---:|---|",
    ]
    for lifecycle in document["lifecycle_rows"]:
        for step in lifecycle["steps"]:
            lines.append(
                f"| {lifecycle['label']} | `{step['step_id']}` | "
                f"{step['median_elapsed_ms']:.3f} | {step['count']} | "
                f"`{', '.join(step['source_occurrence_ids'])}` |"
            )
    lines.extend(
        (
            "",
            "The adjacent comparison JSON retains every join ID, formula ID, source run ID, "
            "result contract, step occurrence ID, capability manifest, scope manifest, and "
            "cache manifest used to classify these rows.",
            "",
        )
    )
    return "\n".join(lines)


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fact",
        action="append",
        type=Path,
        required=True,
        help="Fact bundle to include; repeat for every completed run.",
    )
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--markdown-out", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        sources = [(path, load_fact_bundle(path)) for path in args.fact]
        terminology = json.loads(TERMINOLOGY_PATH.read_text(encoding="utf-8"))
        document = generate_comparison_document(
            sources,
            generated_at_utc=datetime.now(timezone.utc).isoformat(),
            terminology_version=terminology["terminology_version"],
            terminology_sha256=hashlib.sha256(
                canonical_json_bytes(terminology)
            ).hexdigest(),
        )
        atomic_write_text(
            args.out, json.dumps(document, indent=2, sort_keys=True) + "\n"
        )
        atomic_write_text(args.markdown_out, render_markdown(document))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: cannot generate fact comparisons: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

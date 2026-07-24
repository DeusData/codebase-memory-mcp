#!/usr/bin/env python3
"""Validate benchmark terminology and render its checked-in derived views."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = Path(__file__).with_name("terminology.json")
MARKDOWN_PATH = REPO_ROOT / "docs" / "BENCHMARK_TERMINOLOGY.md"
HEADER_PATH = REPO_ROOT / "src" / "foundation" / "profile_terms_generated.h"
REQUIRED_ENTRY_FIELDS = (
    "term_id",
    "display_name",
    "definition",
    "status",
    "kind",
    "data_type",
    "allowed_values_or_range",
    "unit",
    "clock_or_cpu_scope",
    "boundary_semantics",
    "aggregation_rule",
    "concurrency_rule",
    "missing_or_unsupported_behavior",
    "configuration_precedence",
    "capability_or_freshness_implications",
    "source_anchors",
    "introduced_version",
    "deprecated_replacement",
    "examples",
)
TERM_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")


def load_registry(path: Path = REGISTRY_PATH) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        registry = json.load(stream)
    validate_registry(registry)
    return registry


def validate_registry(registry: dict[str, Any]) -> None:
    if registry.get("schema_version") != 1:
        raise ValueError("benchmark terminology schema_version must equal 1")
    version = registry.get("terminology_version")
    if not isinstance(version, str) or not re.fullmatch(r"[1-9]\d*\.\d+\.\d+", version):
        raise ValueError("terminology_version must be a semantic version")
    entries = registry.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ValueError("benchmark terminology entries must be a non-empty array")
    seen: set[str] = set()
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError(f"terminology entry {index} must be an object")
        missing = [field for field in REQUIRED_ENTRY_FIELDS if field not in entry]
        if missing:
            raise ValueError(
                f"terminology entry {index} is missing fields: {', '.join(missing)}"
            )
        term_id = entry["term_id"]
        if not isinstance(term_id, str) or not TERM_ID_PATTERN.fullmatch(term_id):
            raise ValueError(f"invalid term_id at entry {index}: {term_id!r}")
        if term_id in seen:
            raise ValueError(f"duplicate benchmark term_id: {term_id}")
        seen.add(term_id)
        if entry["status"] not in {"existing", "proposed", "deprecated"}:
            raise ValueError(f"{term_id}: invalid status {entry['status']!r}")
        for field in (
            "display_name",
            "definition",
            "kind",
            "data_type",
            "allowed_values_or_range",
            "unit",
            "clock_or_cpu_scope",
            "boundary_semantics",
            "aggregation_rule",
            "concurrency_rule",
            "missing_or_unsupported_behavior",
            "configuration_precedence",
            "capability_or_freshness_implications",
            "introduced_version",
        ):
            if not isinstance(entry[field], str) or not entry[field].strip():
                raise ValueError(f"{term_id}: {field} must be a non-empty string")
        if not isinstance(entry["source_anchors"], list) or not entry["source_anchors"]:
            raise ValueError(f"{term_id}: source_anchors must be a non-empty array")
        if not all(
            isinstance(anchor, str) and anchor.strip()
            for anchor in entry["source_anchors"]
        ):
            raise ValueError(f"{term_id}: source_anchors contains an invalid anchor")
        if not isinstance(entry["examples"], list):
            raise ValueError(f"{term_id}: examples must be an array")
        replacement = entry["deprecated_replacement"]
        if replacement is not None and (
            not isinstance(replacement, str)
            or not TERM_ID_PATTERN.fullmatch(replacement)
        ):
            raise ValueError(f"{term_id}: deprecated_replacement is invalid")
        if entry["status"] == "deprecated" and replacement is None:
            raise ValueError(f"{term_id}: deprecated term requires a replacement")
    for entry in entries:
        replacement = entry["deprecated_replacement"]
        if replacement is not None and replacement not in seen:
            raise ValueError(
                f"{entry['term_id']}: replacement {replacement!r} is not registered"
            )
    step_id_order = registry.get("step_id_order")
    if not isinstance(step_id_order, list) or not step_id_order:
        raise ValueError("step_id_order must be a non-empty array")
    if len(step_id_order) != len(set(step_id_order)):
        raise ValueError("step_id_order contains a duplicate")
    registered_step_ids = {
        entry["term_id"] for entry in entries if entry["kind"] == "step_id"
    }
    if set(step_id_order) != registered_step_ids:
        raise ValueError(
            "step_id_order must contain every registered step_id exactly once"
        )


def registry_sha256(registry: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(registry)).hexdigest()


def markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def render_markdown(registry: dict[str, Any]) -> str:
    digest = registry_sha256(registry)
    lines = [
        "# Benchmark terminology",
        "",
        "<!-- Generated by benchmarks/generate_terminology.py; do not edit. -->",
        "",
        f"- Terminology version: `{registry['terminology_version']}`",
        "- Canonical registry: `benchmarks/terminology.json`",
        f"- Canonical-content SHA-256: `{digest}`",
        "",
        "Every definition below is normative. Parent relations describe containment, "
        "not execution order; overlapping elapsed spans are work-time evidence and must "
        "not be summed into lifecycle wall time.",
        "",
    ]
    by_kind: dict[str, list[dict[str, Any]]] = {}
    for entry in registry["entries"]:
        by_kind.setdefault(entry["kind"], []).append(entry)
    for kind in sorted(by_kind):
        lines.extend((f"## {kind.replace('_', ' ').title()}", ""))
        lines.extend(
            (
                "| ID | Normative definition | Status; type; unit | "
                "Boundaries; aggregation; concurrency | Missing/configuration/effect | Sources |",
                "|---|---|---|---|---|---|",
            )
        )
        for entry in sorted(by_kind[kind], key=lambda item: item["term_id"]):
            identity = f"`{entry['term_id']}`<br>{entry['display_name']}"
            type_cell = (
                f"{entry['status']}; {entry['data_type']}; "
                f"{entry['allowed_values_or_range']}; {entry['unit']}; "
                f"scope: {entry['clock_or_cpu_scope']}"
            )
            timing_cell = (
                f"boundaries: {entry['boundary_semantics']}; "
                f"aggregation: {entry['aggregation_rule']}; "
                f"concurrency: {entry['concurrency_rule']}"
            )
            behavior_cell = (
                f"missing/unsupported: {entry['missing_or_unsupported_behavior']}; "
                f"configuration: {entry['configuration_precedence']}; "
                f"effect: {entry['capability_or_freshness_implications']}"
            )
            sources = ", ".join(f"`{anchor}`" for anchor in entry["source_anchors"])
            lines.append(
                "| "
                + " | ".join(
                    markdown_cell(value)
                    for value in (
                        identity,
                        entry["definition"],
                        type_cell,
                        timing_cell,
                        behavior_cell,
                        sources,
                    )
                )
                + " |"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_header(registry: dict[str, Any]) -> str:
    step_ids = registry["step_id_order"]
    rows = " \\\n".join(
        f'    X({term_id.upper()}, "{term_id}")' for term_id in step_ids
    )
    return (
        "/* Generated by benchmarks/generate_terminology.py; do not edit. */\n"
        "#ifndef CBM_PROFILE_TERMS_GENERATED_H\n"
        "#define CBM_PROFILE_TERMS_GENERATED_H\n\n"
        f'#define CBM_BENCHMARK_TERMINOLOGY_VERSION "{registry["terminology_version"]}"\n'
        f'#define CBM_BENCHMARK_TERMINOLOGY_SHA256 "{registry_sha256(registry)}"\n\n'
        "#define CBM_BENCHMARK_STEP_IDS(X) \\\n"
        f"{rows}\n\n"
        "#endif /* CBM_PROFILE_TERMS_GENERATED_H */\n"
    )


def check_or_write(path: Path, expected: str, check: bool) -> bool:
    actual = path.read_text(encoding="utf-8") if path.exists() else None
    if actual == expected:
        return True
    if check:
        print(f"stale generated benchmark terminology file: {path}", file=sys.stderr)
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(expected, encoding="utf-8")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if generated Markdown or C step-ID definitions are stale.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        registry = load_registry()
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"invalid benchmark terminology registry: {exc}", file=sys.stderr)
        return 1
    ok = check_or_write(MARKDOWN_PATH, render_markdown(registry), args.check)
    ok = check_or_write(HEADER_PATH, render_header(registry), args.check) and ok
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

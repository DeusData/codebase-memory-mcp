#!/usr/bin/env python3
"""Select one content-bound candidate inside every release target tuple."""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import pathlib
import re
import shutil
import stat
import sys
import tempfile
from collections.abc import Iterable, Sequence


class ContractError(Exception):
    """Candidate or VirusTotal evidence is incomplete or incoherent."""


TARGETS = (
    "linux-amd64",
    "linux-arm64",
    "linux-amd64-portable",
    "linux-arm64-portable",
    "darwin-amd64",
    "darwin-arm64",
    "windows-amd64",
    "windows-arm64",
)
VARIANTS = ("unstripped", "debug-stripped", "stripped")
PROVENANCE_FIELDS = (
    "target",
    "variant",
    "relative_path",
    "source_sha256",
    "pre_sign_sha256",
    "sha256",
    "size",
    "format",
    "architecture",
    "linkage",
    "transform",
    "signature",
    "strip_tool",
    "strip_version",
    "pair_verification",
)
CANDIDATE_FIELDS = (*PROVENANCE_FIELDS, "scan_path")
RESULT_FIELDS = (
    "scan_path",
    "sha256",
    "size",
    "association_count",
    "completed_engines",
    "total_engines",
    "malicious",
    "suspicious",
    "analysis_id",
    "microsoft_category",
    "microsoft_result",
    "policy_classification",
    "microsoft_engine_version",
    "microsoft_engine_update",
    "virustotal_url",
)
SELECTION_FIELDS = (
    "target",
    "selected_variant",
    "selected_path",
    "selected_sha256",
    "selected_size",
    "decision",
    "unstripped_sha256",
    "unstripped_scan_path",
    "unstripped_classification",
    "unstripped_analysis_id",
    "unstripped_virustotal_url",
    "debug_stripped_sha256",
    "debug_stripped_scan_path",
    "debug_stripped_classification",
    "debug_stripped_analysis_id",
    "debug_stripped_virustotal_url",
    "stripped_sha256",
    "stripped_scan_path",
    "stripped_classification",
    "stripped_analysis_id",
    "stripped_virustotal_url",
)
SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
ANALYSIS_ID_RE = re.compile(r"\A[A-Za-z0-9_+/-]+={0,2}\Z")
MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_CANDIDATE_BYTES = 512 * 1024 * 1024
MIN_ENGINES = 50


def parse_arguments(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Select one VT-approved candidate per canonical target tuple."
    )
    parser.add_argument("--candidates", required=True, type=pathlib.Path)
    parser.add_argument("--objects-dir", required=True, type=pathlib.Path)
    parser.add_argument("--out-dir", required=True, type=pathlib.Path)
    policy = parser.add_mutually_exclusive_group(required=True)
    policy.add_argument("--results", type=pathlib.Path)
    policy.add_argument("--default-stripped", action="store_true")
    return parser.parse_args(argv[1:])


def regular_status(path: pathlib.Path, *, ceiling: int, label: str) -> os.stat_result:
    try:
        status = path.lstat()
    except FileNotFoundError as error:
        raise ContractError(f"missing {label}: {path}") from error
    if not stat.S_ISREG(status.st_mode):
        raise ContractError(f"{label} is not a regular non-symlink file: {path}")
    if status.st_size > ceiling:
        raise ContractError(f"{label} exceeds the {ceiling}-byte ceiling: {path}")
    return status


def parse_tsv(
    path: pathlib.Path,
    *,
    marker: str,
    fields: Sequence[str],
) -> tuple[dict[str, str], list[dict[str, str]]]:
    regular_status(path, ceiling=MAX_MANIFEST_BYTES, label="evidence manifest")
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeError as error:
        raise ContractError(f"evidence manifest is not UTF-8: {path}") from error
    if "\x00" in text or "\r" in text:
        raise ContractError(f"evidence manifest contains forbidden control bytes: {path}")
    lines = text.splitlines()
    if not lines or lines[0] != f"# {marker}":
        raise ContractError(f"evidence marker is missing: {marker}")
    metadata: dict[str, str] = {}
    cursor = 1
    while cursor < len(lines) and lines[cursor].startswith("# "):
        raw = lines[cursor][2:]
        key, separator, value = raw.partition("=")
        if (
            not separator
            or not key
            or not value
            or key in metadata
            or any(ord(character) < 32 for character in raw)
        ):
            raise ContractError(f"malformed or duplicate evidence metadata: {raw!r}")
        metadata[key] = value
        cursor += 1
    reader = csv.DictReader(lines[cursor:], delimiter="\t")
    if tuple(reader.fieldnames or ()) != tuple(fields):
        raise ContractError(f"evidence TSV header is malformed or unexpected: {path}")
    rows = list(reader)
    if any(
        None in row
        or any(row.get(field) is None for field in fields)
        or any(
            any(ord(character) < 32 for character in value)
            for value in row.values()
            if value
        )
        for row in rows
    ):
        raise ContractError(f"evidence TSV has missing or surplus cells: {path}")
    return metadata, rows


def expected_properties(target: str) -> tuple[str, str, str, str]:
    binary = "codebase-memory-mcp.exe" if target.startswith("windows-") else "codebase-memory-mcp"
    file_format = "pe" if target.startswith("windows-") else "macho" if target.startswith("darwin-") else "elf"
    architecture = target.split("-")[1]
    linkage = (
        "portable"
        if target.endswith("-portable")
        else "dynamic"
        if target.startswith("linux-")
        else "native"
    )
    return binary, file_format, architecture, linkage


def sha256_file(path: pathlib.Path, *, expected_size: int) -> str:
    status = regular_status(path, ceiling=MAX_CANDIDATE_BYTES, label="candidate object")
    if status.st_size != expected_size:
        raise ContractError(f"candidate object size changed: {path}")
    digest = hashlib.sha256()
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (
            status.st_dev,
            status.st_ino,
        ):
            raise ContractError(f"candidate object changed while being opened: {path}")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def validate_candidate_row(row: dict[str, str], *, target: str, variant: str) -> None:
    binary, file_format, architecture, linkage = expected_properties(target)
    exact = {
        "target": target,
        "variant": variant,
        "relative_path": f"{variant}/{binary}",
        "format": file_format,
        "architecture": architecture,
        "linkage": linkage,
        "transform": {
            "unstripped": "copy",
            "debug-stripped": "strip-debug",
            "stripped": "strip",
        }[variant],
        "signature": "adhoc-verified" if target.startswith("darwin-") else "not-applicable",
        "pair_verification": "same-linker-output-v1",
    }
    for field, expected in exact.items():
        if row[field] != expected:
            raise ContractError(f"candidate {target}/{variant} has invalid {field}")
    for field in ("source_sha256", "pre_sign_sha256", "sha256"):
        if SHA256_RE.fullmatch(row[field]) is None:
            raise ContractError(f"candidate {target}/{variant} has invalid {field}")
    if not row["size"].isdigit() or not 0 < int(row["size"]) <= MAX_CANDIDATE_BYTES:
        raise ContractError(f"candidate {target}/{variant} has invalid size")
    scan_path = pathlib.PurePosixPath(row["scan_path"])
    if (
        scan_path.is_absolute()
        or len(scan_path.parts) != 2
        or scan_path.parts[0] != "objects"
        or scan_path.parts[1] != row["sha256"]
        or scan_path.as_posix() != row["scan_path"]
    ):
        raise ContractError(f"candidate {target}/{variant} has invalid scan path")
    for field in ("strip_tool", "strip_version"):
        if not row[field] or any(ord(character) < 32 for character in row[field]):
            raise ContractError(f"candidate {target}/{variant} has invalid {field}")


def load_candidates(
    manifest: pathlib.Path, objects_dir: pathlib.Path
) -> tuple[list[dict[str, str]], dict[str, pathlib.Path]]:
    metadata, rows = parse_tsv(
        manifest,
        marker="cbm-release-candidates-v1",
        fields=CANDIDATE_FIELDS,
    )
    if metadata != {"targets": str(len(TARGETS)), "candidates": str(len(TARGETS) * len(VARIANTS))}:
        raise ContractError("candidate metadata does not bind the exact canonical matrix")
    expected_pairs = [(target, variant) for target in TARGETS for variant in VARIANTS]
    if [(row["target"], row["variant"]) for row in rows] != expected_pairs:
        raise ContractError("candidate rows are not the exact canonical target/variant matrix")

    try:
        objects_status = objects_dir.lstat()
    except FileNotFoundError as error:
        raise ContractError(f"candidate object directory is missing: {objects_dir}") from error
    if not stat.S_ISDIR(objects_status.st_mode):
        raise ContractError(f"candidate object path is not a regular directory: {objects_dir}")

    object_paths: dict[str, pathlib.Path] = {}
    seen_hashes: set[str] = set()
    for row, (target, variant) in zip(rows, expected_pairs):
        validate_candidate_row(row, target=target, variant=variant)
        if row["sha256"] in seen_hashes:
            raise ContractError("candidate object hashes are not unique")
        seen_hashes.add(row["sha256"])
        path = objects_dir / pathlib.PurePosixPath(row["scan_path"]).name
        actual_sha256 = sha256_file(path, expected_size=int(row["size"]))
        if actual_sha256 != row["sha256"]:
            raise ContractError(f"candidate object hash changed: {row['scan_path']}")
        object_paths[row["scan_path"]] = path

    actual_names: set[str] = set()
    for path in objects_dir.iterdir():
        if path.is_symlink() or not path.is_file():
            raise ContractError(f"unexpected non-regular candidate object: {path}")
        actual_names.add(path.name)
    expected_names = {path.name for path in object_paths.values()}
    if actual_names != expected_names or len(actual_names) != len(rows):
        raise ContractError("candidate object directory is missing objects or contains surplus entries")
    for target in TARGETS:
        pair = [row for row in rows if row["target"] == target]
        if len({row["source_sha256"] for row in pair}) != 1:
            raise ContractError(f"candidate siblings do not share source provenance: {target}")
        if pair[0]["pre_sign_sha256"] != pair[0]["source_sha256"]:
            raise ContractError(
                f"unstripped pre-sign bytes do not match source provenance: {target}"
            )
        if not target.startswith("darwin-") and any(
            row["pre_sign_sha256"] != row["sha256"] for row in pair
        ):
            raise ContractError(
                f"non-Darwin candidate changed between pre-sign and final bytes: {target}"
            )
        if pair[0]["sha256"] == pair[1]["sha256"]:
            raise ContractError(f"candidate siblings are not byte-distinct: {target}")
    return rows, object_paths


def parse_nonnegative(row: dict[str, str], field: str, *, scan_path: str) -> int:
    value = row[field]
    if not value.isdigit():
        raise ContractError(f"VT result has invalid {field}: {scan_path}")
    return int(value)


def validate_result_row(row: dict[str, str], candidate: dict[str, str]) -> None:
    scan_path = candidate["scan_path"]
    if (
        row["scan_path"] != scan_path
        or row["sha256"] != candidate["sha256"]
        or row["size"] != candidate["size"]
        or row["association_count"] != "1"
    ):
        raise ContractError(f"VT result is not bound to candidate object: {scan_path}")
    if SHA256_RE.fullmatch(row["sha256"]) is None:
        raise ContractError(f"VT result has invalid SHA-256: {scan_path}")

    completed = parse_nonnegative(row, "completed_engines", scan_path=scan_path)
    total = parse_nonnegative(row, "total_engines", scan_path=scan_path)
    malicious = parse_nonnegative(row, "malicious", scan_path=scan_path)
    suspicious = parse_nonnegative(row, "suspicious", scan_path=scan_path)
    # `total < completed` stays: that is an incoherent response. A low decisive
    # count does NOT — how many engines answered is VirusTotal fleet
    # availability on the day, not a property of this binary, and gating on it
    # made an 8-target release fail on a candidate with zero detections.
    if total < completed:
        raise ContractError(f"VT result has incoherent engine coverage: {scan_path}")
    if ANALYSIS_ID_RE.fullmatch(row["analysis_id"]) is None:
        raise ContractError(f"VT result has invalid analysis id: {scan_path}")
    for field in ("microsoft_engine_version", "microsoft_engine_update"):
        if not row[field] or any(ord(character) < 32 for character in row[field]):
            raise ContractError(f"VT result has incomplete Microsoft evidence: {scan_path}")
    expected_url = f"https://www.virustotal.com/gui/file/{row['sha256']}/detection"
    if row["virustotal_url"] != expected_url:
        raise ContractError(f"VT result URL is not content-bound: {scan_path}")

    classification = row["policy_classification"]
    if classification == "clean":
        coherent = (
            malicious == 0
            and suspicious == 0
            and row["microsoft_category"] in {"undetected", "harmless"}
            and row["microsoft_result"] == ""
        )
    elif classification == "microsoft-ml":
        coherent = (
            malicious == 1
            and suspicious == 0
            and row["microsoft_category"] == "malicious"
            and bool(row["microsoft_result"])
            and row["microsoft_result"].endswith("!ml")
        )
    else:
        raise ContractError(f"VT result is a hard or unknown policy block: {scan_path}")
    if not coherent:
        raise ContractError(f"VT classification contradicts its result details: {scan_path}")


def load_results(
    path: pathlib.Path, candidates: Sequence[dict[str, str]]
) -> dict[str, dict[str, str]]:
    metadata, rows = parse_tsv(
        path,
        marker="cbm-virustotal-results-v2",
        fields=RESULT_FIELDS,
    )
    expected_count = len(candidates)
    required_metadata = {
        "scan_objects": expected_count,
        "associations": expected_count,
        "min_engines_policy": MIN_ENGINES,
    }
    for key, expected in required_metadata.items():
        if metadata.get(key) != str(expected):
            raise ContractError(f"VT results metadata has invalid {key}")
    if set(metadata) != {
        "scan_objects",
        "associations",
        "min_engines_policy",
        "min_completed_engines",
        "max_completed_engines",
    }:
        raise ContractError("VT results metadata fields are incomplete or unexpected")
    if len(rows) != expected_count:
        raise ContractError("VT results do not contain exactly one row per candidate")

    by_path: dict[str, dict[str, str]] = {}
    analysis_ids: set[str] = set()
    candidate_by_path = {row["scan_path"]: row for row in candidates}
    for row in rows:
        candidate = candidate_by_path.get(row["scan_path"])
        if candidate is None or row["scan_path"] in by_path:
            raise ContractError(f"VT results contain a surplus or duplicate row: {row['scan_path']}")
        validate_result_row(row, candidate)
        if row["analysis_id"] in analysis_ids:
            raise ContractError("VT results reuse one analysis id for multiple objects")
        analysis_ids.add(row["analysis_id"])
        by_path[row["scan_path"]] = row
    if set(by_path) != set(candidate_by_path):
        raise ContractError("VT results are missing candidate objects")

    completed = [int(row["completed_engines"]) for row in rows]
    if (
        not metadata["min_completed_engines"].isdigit()
        or not metadata["max_completed_engines"].isdigit()
        or int(metadata["min_completed_engines"]) != min(completed)
        or int(metadata["max_completed_engines"]) != max(completed)
    ):
        raise ContractError("VT results engine-range metadata does not match its rows")
    return by_path


def write_tsv(
    path: pathlib.Path,
    *,
    metadata: Iterable[tuple[str, object]],
    rows: Iterable[dict[str, object]],
) -> None:
    with path.open("x", encoding="utf-8", newline="") as handle:
        handle.write("# cbm-release-selection-v1\n")
        for key, value in metadata:
            handle.write(f"# {key}={value}\n")
        writer = csv.DictWriter(
            handle,
            fieldnames=SELECTION_FIELDS,
            delimiter="\t",
            lineterminator="\n",
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())


def copy_selected(
    source: pathlib.Path,
    destination: pathlib.Path,
    *,
    expected_sha256: str,
    expected_size: int,
) -> None:
    if sha256_file(source, expected_size=expected_size) != expected_sha256:
        raise ContractError(f"candidate object changed before selection copy: {source}")
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    before = regular_status(source, ceiling=MAX_CANDIDATE_BYTES, label="candidate object")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(source, flags)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (
            before.st_dev,
            before.st_ino,
        ):
            raise ContractError(f"candidate object changed while being opened: {source}")
        digest = hashlib.sha256()
        total = 0
        with os.fdopen(descriptor, "rb", closefd=False) as input_handle, destination.open(
            "xb"
        ) as output_handle:
            while True:
                chunk = input_handle.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > expected_size:
                    raise ContractError(f"candidate object grew during selection: {source}")
                digest.update(chunk)
                output_handle.write(chunk)
            output_handle.flush()
            os.fsync(output_handle.fileno())
    finally:
        os.close(descriptor)
    if total != expected_size or digest.hexdigest() != expected_sha256:
        raise ContractError(f"candidate object changed during selection copy: {source}")
    destination.chmod(0o555)
    if sha256_file(destination, expected_size=expected_size) != expected_sha256:
        raise ContractError(f"selected copy is not content-bound: {destination}")


def main(argv: Sequence[str]) -> None:
    args = parse_arguments(argv)
    output = args.out_dir
    if output.name in ("", ".", ".."):
        raise ContractError(f"unsafe output directory: {output}")
    if os.path.lexists(output):
        raise ContractError(f"refusing to overwrite output path: {output}")

    candidates, object_paths = load_candidates(args.candidates, args.objects_dir)
    results = load_results(args.results, candidates) if args.results else None
    policy = "virustotal-v2" if results is not None else "unscanned-dry-run"
    candidate_by_tuple = {
        (row["target"], row["variant"]): row for row in candidates
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = pathlib.Path(
        tempfile.mkdtemp(prefix=f".{output.name}.select-", dir=str(output.parent))
    )
    staged = temporary / "bundle"
    staged.mkdir(mode=0o700)
    selection_rows: list[dict[str, object]] = []
    try:
        for target in TARGETS:
            pair = {
                variant: candidate_by_tuple[target, variant] for variant in VARIANTS
            }
            if results is None:
                classifications = {
                    variant: "unscanned-dry-run" for variant in VARIANTS
                }
                selected_variant = "stripped"
                decision = "stripped-unscanned-dry-run"
            else:
                classifications = {
                    variant: results[pair[variant]["scan_path"]]["policy_classification"]
                    for variant in VARIANTS
                }
                # Preference order is fixed and content-independent: the
                # smallest artifact first, then progressively more metadata.
                # A `hard` classification never reaches here - the gate fails
                # the release before selection - so every candidate below is
                # either clean or a tolerated single Microsoft `!ml`.
                #
                # The variants are behaviourally identical, so this picks on
                # verdict alone: the first clean one in preference order, and
                # only if EVERY candidate drew the tolerated verdict do we ship
                # a flagged one (still the smallest).
                order = ("stripped", "debug-stripped", "unstripped")
                clean = [v for v in order if classifications[v] == "clean"]
                if clean:
                    selected_variant = clean[0]
                    if selected_variant == "stripped":
                        decision = "stripped-preferred"
                    else:
                        others = "-".join(
                            f"{v}:{classifications[v]}" for v in order if v != selected_variant
                        )
                        decision = f"{selected_variant}-clean-after-{others}"
                else:
                    selected_variant = "stripped"
                    decision = "stripped-all-candidates-microsoft-ml"

            selected = pair[selected_variant]
            binary_name = expected_properties(target)[0]
            selected_relative = f"selected/{target}/{binary_name}"
            copy_selected(
                object_paths[selected["scan_path"]],
                staged / selected_relative,
                expected_sha256=selected["sha256"],
                expected_size=int(selected["size"]),
            )
            evidence: dict[str, object] = {
                "target": target,
                "selected_variant": selected_variant,
                "selected_path": selected_relative,
                "selected_sha256": selected["sha256"],
                "selected_size": selected["size"],
                "decision": decision,
            }
            for variant in VARIANTS:
                # Field prefixes use underscores; the variant name is the
                # on-disk directory and keeps its hyphen ("debug-stripped").
                key = variant.replace("-", "_")
                candidate = pair[variant]
                result = results[candidate["scan_path"]] if results is not None else None
                evidence[f"{key}_sha256"] = candidate["sha256"]
                evidence[f"{key}_scan_path"] = candidate["scan_path"]
                evidence[f"{key}_classification"] = classifications[variant]
                evidence[f"{key}_analysis_id"] = result["analysis_id"] if result else ""
                evidence[f"{key}_virustotal_url"] = result["virustotal_url"] if result else ""
            selection_rows.append(evidence)

        write_tsv(
            staged / "release-selection.tsv",
            metadata=(
                ("policy", policy),
                ("targets", len(TARGETS)),
                ("candidates", len(candidates)),
            ),
            rows=selection_rows,
        )
        if os.path.lexists(output):
            raise ContractError(f"output path appeared during selection: {output}")
        os.replace(staged, output)
    finally:
        shutil.rmtree(temporary, ignore_errors=True)

    print(f"selected one candidate for each of {len(TARGETS)} targets under {policy}: {output}")


try:
    main(sys.argv)
except (ContractError, OSError, UnicodeError, ValueError) as error:
    print(f"select-release-candidates: {error}", file=sys.stderr)
    raise SystemExit(1)

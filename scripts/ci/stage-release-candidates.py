#!/usr/bin/env python3
"""Validate and atomically stage the exact release-candidate scan set."""

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
    """The candidate boundary is incomplete, unsafe, or incoherent."""


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
SCAN_SET_FIELDS = (
    "scan_path",
    "sha256",
    "size",
    "association_count",
    "association_kinds",
)
ASSOCIATION_FIELDS = (
    "association_type",
    "archive",
    "archive_sha256",
    "variant",
    "kind",
    "member",
    "asset_path",
    "mime",
    "scan_path",
    "object_sha256",
    "size",
)
SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_CANDIDATE_BYTES = 512 * 1024 * 1024
MAX_NOTICE_BYTES = 16 * 1024 * 1024


def parse_arguments(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage the exact eight-target, sixteen-candidate VT scan set."
    )
    parser.add_argument("source_dir", type=pathlib.Path)
    parser.add_argument("output_dir", type=pathlib.Path)
    parser.add_argument("--expect-targets", type=int, required=True)
    parser.add_argument("--expect-candidates", type=int, required=True)
    args = parser.parse_args(argv[1:])
    if args.expect_targets != len(TARGETS):
        parser.error(f"--expect-targets must be exactly {len(TARGETS)}")
    if args.expect_candidates != len(TARGETS) * len(VARIANTS):
        parser.error(
            f"--expect-candidates must be exactly {len(TARGETS) * len(VARIANTS)}"
        )
    return args


def require_regular(path: pathlib.Path, *, ceiling: int, label: str) -> os.stat_result:
    try:
        status = path.lstat()
    except FileNotFoundError as error:
        raise ContractError(f"missing {label}: {path}") from error
    if not stat.S_ISREG(status.st_mode):
        raise ContractError(f"{label} is not a regular non-symlink file: {path}")
    if status.st_size > ceiling:
        raise ContractError(f"{label} exceeds the {ceiling}-byte ceiling: {path}")
    return status


def ensure_clean_source_tree(source: pathlib.Path) -> list[pathlib.Path]:
    try:
        root_status = source.lstat()
    except FileNotFoundError as error:
        raise ContractError(f"candidate source directory does not exist: {source}") from error
    if not stat.S_ISDIR(root_status.st_mode):
        raise ContractError(f"candidate source is not a regular directory: {source}")

    manifests: list[pathlib.Path] = []
    for directory, names, files in os.walk(source, followlinks=False):
        parent = pathlib.Path(directory)
        for name in names:
            child = parent / name
            if child.is_symlink():
                raise ContractError(f"symlinked directory in candidate source: {child}")
        for name in files:
            child = parent / name
            if child.is_symlink():
                raise ContractError(f"symlinked file in candidate source: {child}")
            if name == "candidate-provenance.tsv":
                manifests.append(child)
    return sorted(manifests, key=lambda item: item.as_posix())


def read_provenance(path: pathlib.Path) -> list[dict[str, str]]:
    require_regular(path, ceiling=MAX_MANIFEST_BYTES, label="candidate provenance")
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeError as error:
        raise ContractError(f"candidate provenance is not UTF-8: {path}") from error
    if "\x00" in text or "\r" in text:
        raise ContractError(f"candidate provenance contains forbidden control bytes: {path}")
    lines = text.splitlines()
    if not lines or lines[0] != "# cbm-release-candidate-provenance-v1":
        raise ContractError(f"candidate provenance marker is missing: {path}")
    if len(lines) < 2 or lines[1].startswith("# "):
        raise ContractError(f"candidate provenance has unexpected metadata: {path}")
    reader = csv.DictReader(lines[1:], delimiter="\t")
    if tuple(reader.fieldnames or ()) != PROVENANCE_FIELDS:
        raise ContractError(f"candidate provenance header changed: {path}")
    rows = list(reader)
    if len(rows) != len(VARIANTS):
        raise ContractError(f"candidate provenance must contain exactly two rows: {path}")
    if any(
        None in row
        or any(row.get(field) is None for field in PROVENANCE_FIELDS)
        or any(
            any(ord(character) < 32 for character in value)
            for value in row.values()
            if value
        )
        for row in rows
    ):
        raise ContractError(f"candidate provenance has missing or surplus cells: {path}")
    return rows


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


def validate_row_shape(row: dict[str, str], *, target: str, variant: str) -> pathlib.PurePosixPath:
    binary, file_format, architecture, linkage = expected_properties(target)
    expected_relative = f"{variant}/{binary}"
    relative = pathlib.PurePosixPath(row["relative_path"])
    if (
        relative.is_absolute()
        or relative.as_posix() != expected_relative
        or any(part in ("", ".", "..") for part in relative.parts)
    ):
        raise ContractError(
            f"candidate {target}/{variant} has unsafe or noncanonical relative path"
        )
    exact = {
        "target": target,
        "variant": variant,
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
            raise ContractError(
                f"candidate {target}/{variant} has invalid {field}: {row[field]!r}"
            )
    for field in ("source_sha256", "pre_sign_sha256", "sha256"):
        if SHA256_RE.fullmatch(row[field]) is None:
            raise ContractError(f"candidate {target}/{variant} has invalid {field}")
    if not row["size"].isdigit() or not 0 < int(row["size"]) <= MAX_CANDIDATE_BYTES:
        raise ContractError(f"candidate {target}/{variant} has invalid size")
    for field in ("strip_tool", "strip_version"):
        value = row[field]
        if not value or any(ord(character) < 32 for character in value):
            raise ContractError(f"candidate {target}/{variant} has invalid {field}")
    return relative


def validate_artifact_inventory(root: pathlib.Path, *, target: str) -> None:
    binary = expected_properties(target)[0]
    expected_root = {
        "candidate-provenance.tsv",
        "THIRD_PARTY_NOTICES.md",
        *VARIANTS,
    }
    actual_root = {path.name for path in root.iterdir()}
    if actual_root != expected_root:
        raise ContractError(
            f"candidate artifact inventory is not exact for {target}: "
            f"missing={sorted(expected_root - actual_root)}, "
            f"surplus={sorted(actual_root - expected_root)}"
        )
    for variant in VARIANTS:
        variant_dir = root / variant
        try:
            mode = variant_dir.lstat().st_mode
        except FileNotFoundError as error:
            raise ContractError(f"missing candidate variant directory: {target}/{variant}") from error
        if not stat.S_ISDIR(mode):
            raise ContractError(f"candidate variant path is not a directory: {target}/{variant}")
        if {path.name for path in variant_dir.iterdir()} != {binary}:
            raise ContractError(
                f"candidate variant inventory is not exact: {target}/{variant}"
            )


def ingest_file(
    source: pathlib.Path,
    destination: pathlib.Path,
    *,
    expected_sha256: str,
    expected_size: int,
    ceiling: int,
    label: str,
) -> None:
    before = require_regular(source, ceiling=ceiling, label=label)
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    digest = hashlib.sha256()
    total = 0
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(source, flags)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (
            before.st_dev,
            before.st_ino,
        ):
            raise ContractError(f"{label} changed while being opened: {source}")
        with os.fdopen(descriptor, "rb", closefd=False) as input_handle, destination.open(
            "xb"
        ) as output_handle:
            while True:
                chunk = input_handle.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > ceiling:
                    raise ContractError(f"{label} exceeds the {ceiling}-byte ceiling: {source}")
                digest.update(chunk)
                output_handle.write(chunk)
            output_handle.flush()
            os.fsync(output_handle.fileno())
    finally:
        os.close(descriptor)
    if total != expected_size or digest.hexdigest() != expected_sha256:
        raise ContractError(f"{label} does not match its provenance: {source}")
    destination.chmod(0o600)


def write_tsv(
    path: pathlib.Path,
    *,
    marker: str,
    metadata: Iterable[tuple[str, object]],
    fields: Sequence[str],
    rows: Iterable[dict[str, object]],
) -> None:
    with path.open("x", encoding="utf-8", newline="") as handle:
        handle.write(f"# {marker}\n")
        for key, value in metadata:
            handle.write(f"# {key}={value}\n")
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
            delimiter="\t",
            lineterminator="\n",
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())


def publish_atomically(staged: pathlib.Path, output: pathlib.Path) -> None:
    if os.path.lexists(output):
        raise ContractError(f"refusing to overwrite output path: {output}")
    os.replace(staged, output)


def main(argv: Sequence[str]) -> None:
    args = parse_arguments(argv)
    source = args.source_dir
    output = args.output_dir
    if output.name in ("", ".", ".."):
        raise ContractError(f"unsafe output directory: {output}")
    if os.path.lexists(output):
        raise ContractError(f"refusing to overwrite output path: {output}")

    manifests = ensure_clean_source_tree(source)
    if len(manifests) != args.expect_targets:
        raise ContractError(
            f"candidate matrix has {len(manifests)} provenance manifests; expected {args.expect_targets}"
        )

    by_target: dict[str, tuple[pathlib.Path, list[dict[str, str]]]] = {}
    for manifest in manifests:
        rows = read_provenance(manifest)
        targets = {row["target"] for row in rows}
        if len(targets) != 1:
            raise ContractError(f"candidate provenance mixes targets: {manifest}")
        target = next(iter(targets))
        if target not in TARGETS or target in by_target:
            raise ContractError(f"unexpected or duplicate candidate target: {target}")
        if tuple(row["variant"] for row in rows) != VARIANTS:
            raise ContractError(f"candidate variants are not in canonical order: {manifest}")
        validate_artifact_inventory(manifest.parent, target=target)
        by_target[target] = (manifest.parent, rows)
    if tuple(target for target in TARGETS if target in by_target) != TARGETS:
        missing = [target for target in TARGETS if target not in by_target]
        raise ContractError(f"candidate matrix is missing canonical targets: {missing}")

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = pathlib.Path(
        tempfile.mkdtemp(prefix=f".{output.name}.stage-", dir=str(output.parent))
    )
    staged = temporary / "bundle"
    staged.mkdir(mode=0o700)
    try:
        candidate_rows: list[dict[str, object]] = []
        association_rows: list[dict[str, object]] = []
        seen_hashes: set[str] = set()
        for target in TARGETS:
            artifact_root, rows = by_target[target]
            row_by_variant = {row["variant"]: row for row in rows}
            source_hashes = {row["source_sha256"] for row in rows}
            if len(source_hashes) != 1:
                raise ContractError(f"candidate siblings do not share source provenance: {target}")
            if rows[0]["pre_sign_sha256"] != rows[0]["source_sha256"]:
                raise ContractError(
                    f"unstripped pre-sign bytes do not match linker-output provenance: {target}"
                )
            if not target.startswith("darwin-") and any(
                row["pre_sign_sha256"] != row["sha256"] for row in rows
            ):
                raise ContractError(
                    f"non-Darwin candidate changed between pre-sign and final bytes: {target}"
                )
            for variant in VARIANTS:
                row = row_by_variant[variant]
                relative = validate_row_shape(row, target=target, variant=variant)
                candidate = artifact_root.joinpath(*relative.parts)
                sha256 = row["sha256"]
                if sha256 in seen_hashes:
                    raise ContractError(f"candidate hashes are not unique across the exact scan set: {sha256}")
                seen_hashes.add(sha256)
                scan_path = f"objects/{sha256}"
                object_path = staged / scan_path
                ingest_file(
                    candidate,
                    object_path,
                    expected_sha256=sha256,
                    expected_size=int(row["size"]),
                    ceiling=MAX_CANDIDATE_BYTES,
                    label=f"candidate {target}/{variant}",
                )
                candidate_rows.append({**row, "scan_path": scan_path})
                association_rows.append(
                    {
                        "association_type": "member",
                        "archive": target,
                        "archive_sha256": row["source_sha256"],
                        "variant": variant,
                        "kind": "binary",
                        "member": row["relative_path"],
                        "asset_path": "",
                        "mime": "",
                        "scan_path": scan_path,
                        "object_sha256": sha256,
                        "size": row["size"],
                    }
                )
            if rows[0]["sha256"] == rows[1]["sha256"]:
                raise ContractError(f"candidate siblings are not byte-distinct: {target}")
            notice_source = artifact_root / "THIRD_PARTY_NOTICES.md"
            notice_status = require_regular(
                notice_source, ceiling=MAX_NOTICE_BYTES, label=f"third-party notice for {target}"
            )
            notice_destination = staged / "notices" / target / "THIRD_PARTY_NOTICES.md"
            notice_destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            notice_sha = hashlib.sha256(notice_source.read_bytes()).hexdigest()
            ingest_file(
                notice_source,
                notice_destination,
                expected_sha256=notice_sha,
                expected_size=notice_status.st_size,
                ceiling=MAX_NOTICE_BYTES,
                label=f"third-party notice for {target}",
            )
            notice_destination.chmod(0o444)

        if len(candidate_rows) != args.expect_candidates:
            raise ContractError(
                f"candidate matrix has {len(candidate_rows)} rows; expected {args.expect_candidates}"
            )
        write_tsv(
            staged / "candidates.tsv",
            marker="cbm-release-candidates-v1",
            metadata=(("targets", len(TARGETS)), ("candidates", len(candidate_rows))),
            fields=CANDIDATE_FIELDS,
            rows=candidate_rows,
        )
        write_tsv(
            staged / "scan-set.tsv",
            marker="cbm-release-scan-set-v2",
            metadata=(("scan_objects", len(candidate_rows)), ("associations", len(candidate_rows))),
            fields=SCAN_SET_FIELDS,
            rows=(
                {
                    "scan_path": row["scan_path"],
                    "sha256": row["sha256"],
                    "size": row["size"],
                    "association_count": 1,
                    "association_kinds": "binary",
                }
                for row in candidate_rows
            ),
        )
        write_tsv(
            staged / "associations.tsv",
            marker="cbm-release-scan-associations-v3",
            metadata=(
                ("archives", len(TARGETS)),
                ("associations", len(candidate_rows)),
                ("scan_objects", len(candidate_rows)),
            ),
            fields=ASSOCIATION_FIELDS,
            rows=association_rows,
        )
        for object_path in (staged / "objects").iterdir():
            object_path.chmod(0o444)
        publish_atomically(staged, output)
    finally:
        shutil.rmtree(temporary, ignore_errors=True)

    print(
        f"staged exact {len(TARGETS)}-target/{len(candidate_rows)}-candidate scan set: {output}"
    )


try:
    main(sys.argv)
except (ContractError, OSError, UnicodeError, ValueError) as error:
    print(f"stage-release-candidates: {error}", file=sys.stderr)
    raise SystemExit(1)

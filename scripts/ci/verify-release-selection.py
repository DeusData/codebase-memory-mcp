#!/usr/bin/env python3
"""Reconcile every canonical public release container to selected bytes."""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import pathlib
import re
import stat
import sys
import tarfile
import zipfile
from collections.abc import Sequence
from typing import BinaryIO, Optional


class ContractError(Exception):
    """The release-container namespace or selected-byte binding is invalid."""


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
FIELD_KEY = {variant: variant.replace("-", "_") for variant in VARIANTS}
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
MAX_ARCHIVE_BYTES = 1024 * 1024 * 1024
MAX_MEMBER_BYTES = 512 * 1024 * 1024
MAX_TOTAL_MEMBER_BYTES = 2 * 1024 * 1024 * 1024
MAX_MEMBERS = 128


def archive_specs() -> dict[str, tuple[str, str]]:
    specs: dict[str, tuple[str, str]] = {}
    for target in TARGETS:
        binary = "codebase-memory-mcp.exe" if target.startswith("windows-") else "codebase-memory-mcp"
        suffix = ".zip" if target.startswith("windows-") else ".tar.gz"
        specs[f"codebase-memory-mcp-{target}{suffix}"] = (target, binary)
        if (
            target.startswith(("darwin-", "windows-"))
            or target.endswith("-portable")
        ):
            specs[f"codebase-memory-mcp-{target}.mcpb"] = (
                target,
                f"server/{binary}",
            )
    return specs


ARCHIVES = archive_specs()


def parse_arguments(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bind all fourteen canonical release containers to release-selection.tsv."
    )
    parser.add_argument("--selection", required=True, type=pathlib.Path)
    parser.add_argument("--archive-dir", required=True, type=pathlib.Path)
    parser.add_argument(
        "--require-policy",
        choices=("virustotal-v2", "unscanned-dry-run"),
        help="reject a selection produced under any other policy",
    )
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


def load_selection(
    path: pathlib.Path, *, require_policy: Optional[str] = None
) -> dict[str, dict[str, str]]:
    regular_status(path, ceiling=MAX_MANIFEST_BYTES, label="release selection")
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeError as error:
        raise ContractError("release selection is not UTF-8") from error
    if "\x00" in text or "\r" in text:
        raise ContractError("release selection contains forbidden control bytes")
    lines = text.splitlines()
    if not lines or lines[0] != "# cbm-release-selection-v1":
        raise ContractError("release selection marker is missing")
    metadata: dict[str, str] = {}
    cursor = 1
    while cursor < len(lines) and lines[cursor].startswith("# "):
        raw = lines[cursor][2:]
        key, separator, value = raw.partition("=")
        if not separator or not key or not value or key in metadata:
            raise ContractError(f"malformed or duplicate release-selection metadata: {raw!r}")
        metadata[key] = value
        cursor += 1
    if set(metadata) != {"policy", "targets", "candidates"}:
        raise ContractError("release selection metadata fields are incomplete or unexpected")
    if metadata["policy"] not in {"virustotal-v2", "unscanned-dry-run"}:
        raise ContractError("release selection has an invalid policy")
    if require_policy is not None and metadata["policy"] != require_policy:
        raise ContractError(
            f"release selection policy is {metadata['policy']!r}; required {require_policy!r}"
        )
    if metadata["targets"] != str(len(TARGETS)) or metadata["candidates"] != str(
        len(TARGETS) * len(VARIANTS)
    ):
        raise ContractError("release selection metadata does not bind the canonical matrix")
    reader = csv.DictReader(lines[cursor:], delimiter="\t")
    if tuple(reader.fieldnames or ()) != SELECTION_FIELDS:
        raise ContractError("release selection header is malformed or unexpected")
    rows = list(reader)
    if any(
        None in row
        or any(row.get(field) is None for field in SELECTION_FIELDS)
        or any(
            any(ord(character) < 32 for character in value)
            for value in row.values()
            if value
        )
        for row in rows
    ):
        raise ContractError("release selection has missing or surplus cells")
    if [row["target"] for row in rows] != list(TARGETS):
        raise ContractError("release selection is not the exact canonical target matrix")

    selections: dict[str, dict[str, str]] = {}
    analysis_ids: set[str] = set()
    for row in rows:
        target = row["target"]
        binary = "codebase-memory-mcp.exe" if target.startswith("windows-") else "codebase-memory-mcp"
        classifications = {
            variant: row[f"{FIELD_KEY[variant]}_classification"] for variant in VARIANTS
        }
        if metadata["policy"] == "virustotal-v2":
            if any(
                classification not in {"clean", "microsoft-ml"}
                for classification in classifications.values()
            ):
                raise ContractError(f"release selection contains a hard classification: {target}")
            for variant in VARIANTS:
                sha256 = row[f"{FIELD_KEY[variant]}_sha256"]
                analysis_id = row[f"{FIELD_KEY[variant]}_analysis_id"]
                expected_url = f"https://www.virustotal.com/gui/file/{sha256}/detection"
                if ANALYSIS_ID_RE.fullmatch(analysis_id) is None:
                    raise ContractError(f"release selection lacks VT analysis evidence: {target}")
                if analysis_id in analysis_ids:
                    raise ContractError("release selection reuses one VT analysis id")
                analysis_ids.add(analysis_id)
                if row[f"{FIELD_KEY[variant]}_virustotal_url"] != expected_url:
                    raise ContractError(f"release selection has unbound VT URL: {target}")
            # Re-derived here independently of the selector: smallest artifact
            # first, first CLEAN one wins, and only if every candidate drew the
            # tolerated verdict do we ship the smallest flagged one.
            order = ("stripped", "debug-stripped", "unstripped")
            clean = [v for v in order if classifications[v] == "clean"]
            if clean:
                expected_variant = clean[0]
                if expected_variant == "stripped":
                    expected_decision = "stripped-preferred"
                else:
                    others = "-".join(
                        f"{v}:{classifications[v]}" for v in order if v != expected_variant
                    )
                    expected_decision = f"{expected_variant}-clean-after-{others}"
            else:
                expected_variant = "stripped"
                expected_decision = "stripped-all-candidates-microsoft-ml"
        else:
            if any(
                classification != "unscanned-dry-run"
                for classification in classifications.values()
            ):
                raise ContractError(f"dry-run selection disguises unscanned evidence: {target}")
            if any(
                row[f"{FIELD_KEY[variant]}_{suffix}"]
                for variant in VARIANTS
                for suffix in ("analysis_id", "virustotal_url")
            ):
                raise ContractError(f"dry-run selection contains invented VT evidence: {target}")
            expected_variant = "stripped"
            expected_decision = "stripped-unscanned-dry-run"
        if (
            row["selected_variant"] != expected_variant
            or row["decision"] != expected_decision
        ):
            raise ContractError(f"release selection contradicts the policy truth table: {target}")
        selected_variant = expected_variant
        if row["selected_path"] != f"selected/{target}/{binary}":
            raise ContractError(f"release selection has unsafe selected path: {target}")
        if (
            SHA256_RE.fullmatch(row["selected_sha256"]) is None
            or row["selected_sha256"] != row[f"{FIELD_KEY[selected_variant]}_sha256"]
            or not row["selected_size"].isdigit()
            or not 0 < int(row["selected_size"]) <= MAX_MEMBER_BYTES
        ):
            raise ContractError(f"release selection has invalid selected content: {target}")
        for variant in VARIANTS:
            sha256 = row[f"{FIELD_KEY[variant]}_sha256"]
            scan_path = pathlib.PurePosixPath(row[f"{FIELD_KEY[variant]}_scan_path"])
            if (
                SHA256_RE.fullmatch(sha256) is None
                or scan_path.as_posix() != f"objects/{sha256}"
            ):
                raise ContractError(f"release selection has invalid sibling binding: {target}")
        if row["unstripped_sha256"] == row["stripped_sha256"]:
            raise ContractError(f"release selection siblings are not byte-distinct: {target}")
        selections[target] = row
    return selections


def safe_member_name(name: str) -> bool:
    if not name or "\\" in name or "\x00" in name or name.startswith("/"):
        return False
    pure = pathlib.PurePosixPath(name)
    return (
        not pure.is_absolute()
        and pure.as_posix() == name
        and all(part not in ("", ".", "..") for part in pure.parts)
    )


def hash_stream(stream: BinaryIO, *, declared_size: int, label: str) -> tuple[str, int]:
    if declared_size < 0 or declared_size > MAX_MEMBER_BYTES:
        raise ContractError(f"archive member has an invalid size: {label}")
    digest = hashlib.sha256()
    total = 0
    while True:
        chunk = stream.read(min(1024 * 1024, declared_size - total + 1))
        if not chunk:
            break
        total += len(chunk)
        if total > declared_size or total > MAX_MEMBER_BYTES:
            raise ContractError(f"archive member exceeded its declared size: {label}")
        digest.update(chunk)
    if total != declared_size:
        raise ContractError(f"archive member size does not match metadata: {label}")
    return digest.hexdigest(), total


def verify_tar(
    path: pathlib.Path, *, required_member: str, expected_sha256: str,
    expected_size: int, require_executable_mode: bool
) -> None:
    matches = 0
    try:
        with tarfile.open(path, mode="r:gz") as archive:
            members = archive.getmembers()
            if not members or len(members) > MAX_MEMBERS:
                raise ContractError(f"tar member count is invalid: {path.name}")
            if sum(member.size for member in members) > MAX_TOTAL_MEMBER_BYTES:
                raise ContractError(f"tar exceeds total member-size ceiling: {path.name}")
            seen: set[str] = set()
            for member in members:
                if not safe_member_name(member.name) or member.name in seen:
                    raise ContractError(f"tar has unsafe or duplicate member: {path.name}:{member.name}")
                seen.add(member.name)
                if member.issym() or member.islnk() or member.isdev() or member.isfifo():
                    raise ContractError(f"tar contains a special member: {path.name}:{member.name}")
                if member.size > MAX_MEMBER_BYTES:
                    raise ContractError(f"tar member exceeds size ceiling: {path.name}:{member.name}")
                if member.name != required_member:
                    continue
                if not member.isfile():
                    raise ContractError(f"required tar executable is not regular: {path.name}")
                if require_executable_mode and stat.S_IMODE(member.mode) != 0o755:
                    raise ContractError(
                        f"required tar executable has wrong mode: {path.name}:{member.name}"
                    )
                matches += 1
                handle = archive.extractfile(member)
                if handle is None:
                    raise ContractError(f"required tar executable is unreadable: {path.name}")
                with handle:
                    actual_sha256, actual_size = hash_stream(
                        handle, declared_size=member.size, label=f"{path.name}:{member.name}"
                    )
                if actual_sha256 != expected_sha256 or actual_size != expected_size:
                    raise ContractError(f"tar executable does not match selection: {path.name}")
    except (tarfile.TarError, EOFError) as error:
        raise ContractError(f"malformed tar archive: {path.name}") from error
    if matches != 1:
        raise ContractError(f"tar must contain exactly one required executable: {path.name}")


def zip_entry_kind(info: zipfile.ZipInfo) -> str:
    mode = (info.external_attr >> 16) & 0xFFFF
    if mode and stat.S_IFMT(mode) not in (0, stat.S_IFREG, stat.S_IFDIR):
        return "special"
    return "directory" if info.is_dir() else "regular"


def verify_zip(
    path: pathlib.Path, *, required_member: str, expected_sha256: str,
    expected_size: int, require_executable_mode: bool
) -> None:
    matches = 0
    try:
        with zipfile.ZipFile(path, mode="r") as archive:
            members = archive.infolist()
            if not members or len(members) > MAX_MEMBERS:
                raise ContractError(f"zip member count is invalid: {path.name}")
            if sum(member.file_size for member in members) > MAX_TOTAL_MEMBER_BYTES:
                raise ContractError(f"zip exceeds total member-size ceiling: {path.name}")
            seen: set[str] = set()
            for member in members:
                name = member.filename
                if not safe_member_name(name) or name in seen:
                    raise ContractError(f"zip has unsafe or duplicate member: {path.name}:{name}")
                seen.add(name)
                if zip_entry_kind(member) == "special":
                    raise ContractError(f"zip contains a special member: {path.name}:{name}")
                if member.file_size > MAX_MEMBER_BYTES:
                    raise ContractError(f"zip member exceeds size ceiling: {path.name}:{name}")
                if name != required_member:
                    continue
                if zip_entry_kind(member) != "regular":
                    raise ContractError(f"required zip executable is not regular: {path.name}")
                mode = (member.external_attr >> 16) & 0xFFFF
                if require_executable_mode and stat.S_IMODE(mode) != 0o755:
                    raise ContractError(
                        f"required zip executable has wrong mode: {path.name}:{name}"
                    )
                matches += 1
                with archive.open(member, mode="r") as handle:
                    actual_sha256, actual_size = hash_stream(
                        handle, declared_size=member.file_size, label=f"{path.name}:{name}"
                    )
                if actual_sha256 != expected_sha256 or actual_size != expected_size:
                    raise ContractError(f"zip executable does not match selection: {path.name}")
    except (zipfile.BadZipFile, EOFError, RuntimeError) as error:
        raise ContractError(f"malformed zip archive: {path.name}") from error
    if matches != 1:
        raise ContractError(f"zip must contain exactly one required executable: {path.name}")


def discover_archives(root: pathlib.Path) -> dict[str, pathlib.Path]:
    try:
        root_status = root.lstat()
    except FileNotFoundError as error:
        raise ContractError(f"archive directory does not exist: {root}") from error
    if not stat.S_ISDIR(root_status.st_mode):
        raise ContractError(f"archive path is not a regular directory: {root}")

    discovered: dict[str, pathlib.Path] = {}
    archive_like: list[pathlib.Path] = []
    for directory, names, files in os.walk(root, followlinks=False):
        parent = pathlib.Path(directory)
        for name in names:
            child = parent / name
            if child.is_symlink():
                raise ContractError(f"symlinked directory in archive tree: {child}")
        for name in files:
            child = parent / name
            if child.is_symlink():
                raise ContractError(f"symlinked file in archive tree: {child}")
            if name.endswith((".tar.gz", ".zip", ".mcpb")):
                archive_like.append(child)
                if name in discovered:
                    raise ContractError(f"duplicate archive basename: {name}")
                discovered[name] = child
    actual_names = set(discovered)
    expected_names = set(ARCHIVES)
    if actual_names != expected_names or len(archive_like) != len(expected_names):
        missing = sorted(expected_names - actual_names)
        surplus = sorted(actual_names - expected_names)
        raise ContractError(
            f"archive namespace is not the exact canonical set; missing={missing}, surplus={surplus}"
        )
    return discovered


def main(argv: Sequence[str]) -> None:
    args = parse_arguments(argv)
    selections = load_selection(args.selection, require_policy=args.require_policy)
    archives = discover_archives(args.archive_dir)
    for name, (target, member) in ARCHIVES.items():
        path = archives[name]
        regular_status(path, ceiling=MAX_ARCHIVE_BYTES, label="release archive")
        selection = selections[target]
        verify = verify_tar if name.endswith(".tar.gz") else verify_zip
        verify(
            path,
            required_member=member,
            expected_sha256=selection["selected_sha256"],
            expected_size=int(selection["selected_size"]),
            require_executable_mode=not target.startswith("windows-"),
        )
    print(f"verified all {len(ARCHIVES)} canonical containers against selected bytes")


try:
    main(sys.argv)
except (ContractError, OSError, UnicodeError, ValueError) as error:
    print(f"verify-release-selection: {error}", file=sys.stderr)
    raise SystemExit(1)

#!/usr/bin/env bash
# Validate the complete canonical release matrix and atomically build a
# content-deduplicated VirusTotal scan bundle.
#
# Usage:
#   extract-release-archives.sh <archive-dir> <output-dir> \
#     [--expect-archives=N] [--expect-binaries=N] \
#     [--expect-runtime-files=N]
#
# The output directory is published as one atomic bundle:
#   objects/          one file per distinct byte sequence
#   associations.tsv  every extracted archive member -> scan object
#   scan-set.tsv      the exact path/hash/size set the VT action must return
#
# Every archive is validated and hashed for provenance, but downloadable
# .tar.gz/.zip release containers are not scanned. Their exact members are
# covered instead. Identical bytes are uploaded once, but no extracted member
# association is discarded.
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <archive-dir> <output-dir> [--expect-archives=N] [--expect-binaries=N] [--expect-runtime-files=N]" >&2
  exit 2
fi

command -v python3 >/dev/null 2>&1 || {
  echo "FAIL: python3 is required to validate release archives" >&2
  exit 1
}

python3 - "$@" <<'PY'
from __future__ import annotations

import csv
import hashlib
import json
import os
import pathlib
import re
import stat
import struct
import sys
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass, field
from typing import BinaryIO, Dict, Iterable, List, Optional, Sequence, Tuple


class ContractError(Exception):
    pass


MIB = 1024 * 1024
GIB = 1024 * MIB
READ_CHUNK = MIB
MAX_ARCHIVE_BYTES = 512 * MIB
MAX_MEMBER_BYTES = 512 * MIB
MAX_TOTAL_MEMBER_BYTES = 8 * GIB
MAX_PACK_BYTES = 64 * MIB
PACK_HEADER_BYTES = 80
PACK_ENTRY_BYTES = 24
PACK_MAX_FILES = 1024
PACK_MAX_PATH_BYTES = 255

UNIX_TARGETS = (
    "linux-amd64",
    "linux-arm64",
    "darwin-amd64",
    "darwin-arm64",
    "linux-amd64-portable",
    "linux-arm64-portable",
)
WINDOWS_TARGETS = ("windows-amd64", "windows-arm64")
# MCPB bundles exist for darwin/windows and the STATIC linux builds only —
# the same eligibility rule scripts/package-release.sh encodes.
MCPB_TARGETS = (
    "darwin-amd64",
    "darwin-arm64",
    "linux-amd64-portable",
    "linux-arm64-portable",
    "windows-amd64",
    "windows-arm64",
)
CANONICAL_ARCHIVES = frozenset(
    [f"codebase-memory-mcp-{target}.tar.gz" for target in UNIX_TARGETS]
    + [f"codebase-memory-mcp-{target}.zip" for target in WINDOWS_TARGETS]
    + [f"codebase-memory-mcp-{target}.mcpb" for target in MCPB_TARGETS]
)
# One composition ships; the association column is retained so the schema stays
# stable for the gate and release-notes consumers.
RELEASE_VARIANT = "release"
SAFE_LABEL = re.compile(r"[^A-Za-z0-9._-]+")
SAFE_ASSET_PATH = re.compile(rb"\A[A-Za-z0-9._/-]+\Z")
COUNT_OPTIONS = {
    "--expect-archives": "archives",
    "--expect-binaries": "binaries",
    "--expect-runtime-files": "runtime_files",
}
MIME_BY_EXTENSION = {
    ".html": (1, "text/html"),
    ".js": (2, "application/javascript"),
    ".mjs": (2, "application/javascript"),
    ".css": (3, "text/css"),
    ".json": (4, "application/json"),
    ".svg": (5, "image/svg+xml"),
    ".png": (6, "image/png"),
    ".jpg": (7, "image/jpeg"),
    ".jpeg": (7, "image/jpeg"),
    ".webp": (8, "image/webp"),
    ".avif": (9, "image/avif"),
    ".ico": (10, "image/x-icon"),
    ".woff2": (11, "font/woff2"),
    ".woff": (12, "font/woff"),
    ".wasm": (13, "application/wasm"),
}
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
SCAN_SET_FIELDS = (
    "scan_path",
    "sha256",
    "size",
    "association_count",
    "association_kinds",
)


@dataclass
class ScanObject:
    path: pathlib.Path
    scan_path: str
    sha256: str
    size: int
    kinds: set[str] = field(default_factory=set)
    association_count: int = 0


@dataclass(frozen=True)
class PackAsset:
    path: str
    mime: str
    offset: int
    size: int


class SliceReader:
    def __init__(self, handle: BinaryIO, offset: int, size: int) -> None:
        self.handle = handle
        self.remaining = size
        handle.seek(offset)

    def read(self, length: int = -1) -> bytes:
        if self.remaining == 0:
            return b""
        wanted = self.remaining if length < 0 else min(length, self.remaining)
        data = self.handle.read(wanted)
        if not data:
            raise ContractError("UI pack payload ended before its declared asset length")
        self.remaining -= len(data)
        return data


def files_equal(left: pathlib.Path, right: pathlib.Path) -> bool:
    """Exact comparison is mandatory even after the SHA-256+size grouping."""
    with left.open("rb") as left_handle, right.open("rb") as right_handle:
        while True:
            left_chunk = left_handle.read(READ_CHUNK)
            right_chunk = right_handle.read(READ_CHUNK)
            if left_chunk != right_chunk:
                return False
            if not left_chunk:
                return True


def safe_object_label(label: str) -> str:
    cleaned = SAFE_LABEL.sub("-", pathlib.PurePosixPath(label).name).strip("-.")
    return (cleaned or "object")[:96]


class ObjectStore:
    def __init__(self, directory: pathlib.Path) -> None:
        self.directory = directory
        self.directory.mkdir(mode=0o700)
        self.groups: Dict[Tuple[str, int], List[ScanObject]] = {}
        self.objects: List[ScanObject] = []
        self.candidate_index = 0

    def ingest_stream(
        self,
        source: BinaryIO,
        *,
        declared_size: int,
        ceiling: int,
        label: str,
    ) -> ScanObject:
        if declared_size < 0 or declared_size > ceiling:
            raise ContractError(
                f"object exceeds {ceiling} byte ceiling ({label}: {declared_size})"
            )
        self.candidate_index += 1
        candidate = self.directory / f".candidate-{self.candidate_index:06d}"
        digest = hashlib.sha256()
        total = 0
        try:
            with candidate.open("xb") as output:
                while True:
                    chunk = source.read(min(READ_CHUNK, ceiling - total + 1))
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > ceiling:
                        raise ContractError(f"object exceeds {ceiling} byte ceiling ({label})")
                    digest.update(chunk)
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
            if total != declared_size:
                raise ContractError(
                    f"object length mismatch ({label}: declared {declared_size}, read {total})"
                )

            sha256 = digest.hexdigest()
            group = self.groups.setdefault((sha256, total), [])
            for existing in group:
                if files_equal(candidate, existing.path):
                    candidate.unlink()
                    return existing

            collision = len(group) + 1
            collision_part = "" if collision == 1 else f"-collision-{collision}"
            name = (
                f"scan-{sha256}{collision_part}--{safe_object_label(label)}"
            )
            destination = self.directory / name
            if os.path.lexists(destination):
                raise ContractError(f"internal scan-object name collision: {name}")
            os.replace(candidate, destination)
            destination.chmod(0o600)
            result = ScanObject(
                path=destination,
                scan_path=f"objects/{name}",
                sha256=sha256,
                size=total,
            )
            group.append(result)
            self.objects.append(result)
            return result
        finally:
            if os.path.lexists(candidate):
                candidate.unlink()

    def ingest_path(self, path: pathlib.Path, *, ceiling: int, label: str) -> ScanObject:
        mode = path.lstat().st_mode
        if not stat.S_ISREG(mode):
            raise ContractError(f"scan input is not a regular file: {path}")
        size = path.stat().st_size
        with path.open("rb") as source:
            return self.ingest_stream(
                source,
                declared_size=size,
                ceiling=ceiling,
                label=label,
            )

    def ingest_slice(
        self,
        path: pathlib.Path,
        *,
        offset: int,
        size: int,
        label: str,
    ) -> ScanObject:
        with path.open("rb") as handle:
            source = SliceReader(handle, offset, size)
            result = self.ingest_stream(
                source,
                declared_size=size,
                ceiling=MAX_PACK_BYTES,
                label=label,
            )
            if source.remaining != 0:
                raise ContractError(f"UI pack asset was not read completely: {label}")
            return result


def parse_arguments(
    argv: Sequence[str],
) -> Tuple[pathlib.Path, pathlib.Path, Dict[str, Optional[int]]]:
    archive_dir = pathlib.Path(argv[1]).absolute()
    output_dir = pathlib.Path(argv[2]).absolute()
    expected: Dict[str, Optional[int]] = {value: None for value in COUNT_OPTIONS.values()}
    seen: set[str] = set()
    for argument in argv[3:]:
        option, separator, raw_value = argument.partition("=")
        if not separator or option not in COUNT_OPTIONS:
            raise ContractError(f"unknown option: {argument}")
        key = COUNT_OPTIONS[option]
        if key in seen:
            raise ContractError(f"duplicate option: {option}")
        if not raw_value.isdigit():
            raise ContractError(f"{option} requires a non-negative integer")
        expected[key] = int(raw_value)
        seen.add(key)
    return archive_dir, output_dir, expected


def validate_namespace(archive_name: str, names: Iterable[str]) -> Dict[str, str]:
    names_list = list(names)
    if len(names_list) != len(set(names_list)):
        duplicate = next(name for name in names_list if names_list.count(name) > 1)
        raise ContractError(f"duplicate archive member in {archive_name}: {duplicate}")
    # The archive name is already validated against CANONICAL_ARCHIVES, so
    # platform detection by name is sound for every container kind.
    windows = "-windows-" in archive_name
    if archive_name.endswith(".mcpb"):
        binary = "server/codebase-memory-mcp.exe" if windows else "server/codebase-memory-mcp"
        fixed = {
            "manifest.json": "runtime",
            binary: "binary",
            "server/LICENSE": "runtime",
            "server/THIRD_PARTY_NOTICES.md": "runtime",
        }
    else:
        binary = "codebase-memory-mcp.exe" if windows else "codebase-memory-mcp"
        installer = "install.ps1" if windows else "install.sh"
        fixed = {
            binary: "binary",
            "LICENSE": "runtime",
            installer: "runtime",
            "THIRD_PARTY_NOTICES.md": "runtime",
        }
    name_set = set(names_list)
    extras = name_set - set(fixed)
    if extras:
        raise ContractError(f"unexpected archive member in {archive_name}: {sorted(extras)[0]}")
    if name_set != set(fixed):
        missing = sorted(set(fixed) - name_set)
        raise ContractError(
            f"member namespace mismatch in {archive_name}: expected exactly 4 members; missing={missing}"
        )
    return fixed


def validate_mcpb_manifest(path: pathlib.Path, *, archive_name: str) -> None:
    """A structurally broken bundle must fail the matrix, not ship.

    The namespace check above proves manifest.json EXISTS; this proves it
    actually describes the binary the bundle carries. Full schema validation
    belongs to MCPB hosts — the gate pins only what a wrong build would break.
    """
    with zipfile.ZipFile(path, "r") as archive:
        try:
            manifest = json.loads(archive.read("manifest.json"))
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ContractError(f"manifest.json in {archive_name} is not valid JSON: {error}")
        if not isinstance(manifest, dict):
            raise ContractError(f"manifest.json in {archive_name} must be a JSON object")
        if not manifest.get("version"):
            raise ContractError(f"manifest.json in {archive_name} lacks a version")
        server = manifest.get("server")
        if not isinstance(server, dict) or server.get("type") != "binary":
            raise ContractError(f"manifest.json in {archive_name} must declare a binary server")
        entry = server.get("entry_point")
        if entry not in set(archive.namelist()):
            raise ContractError(
                f"manifest entry_point is not a member of {archive_name}: {entry}"
            )
        mcp_config = server.get("mcp_config")
        command = mcp_config.get("command") if isinstance(mcp_config, dict) else None
        if not isinstance(command, str) or not command.endswith(entry):
            raise ContractError(
                f"manifest mcp_config.command does not target the entry_point in {archive_name}"
            )


def add_association(
    rows: List[Dict[str, object]],
    scan_object: ScanObject,
    *,
    association_type: str,
    archive: str,
    archive_sha256: str,
    kind: str,
    member: str = "",
    asset_path: str = "",
    mime: str = "",
) -> None:
    scan_object.association_count += 1
    scan_object.kinds.add(kind)
    rows.append(
        {
            "association_type": association_type,
            "archive": archive,
            "archive_sha256": archive_sha256,
            "variant": RELEASE_VARIANT,
            "kind": kind,
            "member": member,
            "asset_path": asset_path,
            "mime": mime,
            "scan_path": scan_object.scan_path,
            "object_sha256": scan_object.sha256,
            "size": scan_object.size,
        }
    )


def validate_member_metadata(archive_name: str, names_and_sizes: Iterable[Tuple[str, int]]) -> int:
    total = 0
    for name, size in names_and_sizes:
        if not name or "\t" in name or "\n" in name or "\r" in name:
            raise ContractError(f"invalid archive member name in {archive_name}")
        if size < 0 or size > MAX_MEMBER_BYTES:
            raise ContractError(
                f"archive member exceeds {MAX_MEMBER_BYTES} byte ceiling in {archive_name}: {name}"
            )
        total += size
        if total > MAX_TOTAL_MEMBER_BYTES:
            raise ContractError("release matrix exceeds total uncompressed byte ceiling")
    return total


def process_member(
    *,
    store: ObjectStore,
    rows: List[Dict[str, object]],
    source: BinaryIO,
    size: int,
    member: str,
    kind: str,
    archive_name: str,
    archive_sha256: str,
    variant: str = RELEASE_VARIANT,
) -> None:
    scan_object = store.ingest_stream(
        source,
        declared_size=size,
        ceiling=MAX_MEMBER_BYTES,
        label=member,
    )
    add_association(
        rows,
        scan_object,
        association_type="member",
        archive=archive_name,
        archive_sha256=archive_sha256,
        kind=kind,
        member=member,
    )


def process_tar(
    path: pathlib.Path,
    *,
    archive_name: str,
    store: ObjectStore,
    rows: List[Dict[str, object]],
    archive_sha256: str,
    remaining_member_bytes: int,
) -> Tuple[str, Dict[str, str], int]:
    with tarfile.open(path, "r:gz") as archive:
        infos = archive.getmembers()
        seen: set[str] = set()
        for info in infos:
            if info.name in seen:
                raise ContractError(f"duplicate archive member in {archive_name}: {info.name}")
            seen.add(info.name)
            if not info.isfile():
                raise ContractError(f"non-regular archive member in {archive_name}: {info.name}")
        total = validate_member_metadata(archive_name, ((info.name, info.size) for info in infos))
        if total > remaining_member_bytes:
            raise ContractError("release matrix exceeds total uncompressed byte ceiling")
        kinds = validate_namespace(archive_name, [info.name for info in infos])
        for info in sorted(infos, key=lambda item: item.name):
            source = archive.extractfile(info)
            if source is None:
                raise ContractError(f"could not read archive member in {archive_name}: {info.name}")
            with source:
                process_member(
                    store=store,
                    rows=rows,
                    source=source,
                    size=info.size,
                    member=info.name,
                    kind=kinds[info.name],
                    archive_name=archive_name,
                    archive_sha256=archive_sha256,
                )
        return kinds, total


def process_zip(
    path: pathlib.Path,
    *,
    archive_name: str,
    store: ObjectStore,
    rows: List[Dict[str, object]],
    archive_sha256: str,
    remaining_member_bytes: int,
) -> Tuple[str, Dict[str, str], int]:
    with zipfile.ZipFile(path, "r") as archive:
        infos = archive.infolist()
        seen: set[str] = set()
        for info in infos:
            name = info.filename
            if name in seen:
                raise ContractError(f"duplicate archive member in {archive_name}: {name}")
            seen.add(name)
            unix_mode = (info.external_attr >> 16) & 0xFFFF
            file_type = stat.S_IFMT(unix_mode)
            if info.is_dir() or name.endswith("/") or file_type not in (0, stat.S_IFREG):
                raise ContractError(f"non-regular archive member in {archive_name}: {name}")
            if info.flag_bits & 0x1:
                raise ContractError(f"encrypted archive member in {archive_name}: {name}")
        total = validate_member_metadata(archive_name, ((info.filename, info.file_size) for info in infos))
        if total > remaining_member_bytes:
            raise ContractError("release matrix exceeds total uncompressed byte ceiling")
        kinds = validate_namespace(archive_name, [info.filename for info in infos])
        for info in sorted(infos, key=lambda item: item.filename):
            with archive.open(info, "r") as source:
                process_member(
                    store=store,
                    rows=rows,
                    source=source,
                    size=info.file_size,
                    member=info.filename,
                    kind=kinds[info.filename],
                    archive_name=archive_name,
                    archive_sha256=archive_sha256,
                )
        return kinds, total


def write_tsv(
    path: pathlib.Path,
    *,
    marker: str,
    metadata: Iterable[Tuple[str, int]],
    fields: Sequence[str],
    rows: Iterable[Dict[str, object]],
) -> None:
    with path.open("x", encoding="utf-8", newline="") as handle:
        handle.write(f"# {marker}\n")
        for key, value in metadata:
            handle.write(f"# {key}={value}\n")
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())


def main(argv: Sequence[str]) -> None:
    archive_dir, output_dir, expected = parse_arguments(argv)
    if not archive_dir.is_dir() or archive_dir.is_symlink():
        raise ContractError(f"archive directory is not a regular directory: {archive_dir}")
    if output_dir.name in ("", ".", ".."):
        raise ContractError(f"unsafe output directory: {output_dir}")
    if os.path.lexists(output_dir):
        if output_dir.is_symlink() or not output_dir.is_dir():
            raise ContractError(f"output path is not a regular directory: {output_dir}")
        if any(output_dir.iterdir()):
            raise ContractError(f"output directory must be empty: {output_dir}")

    archive_paths = sorted(archive_dir.iterdir(), key=lambda candidate: candidate.name)
    actual_names = {path.name for path in archive_paths}
    expected_names = CANONICAL_ARCHIVES
    if actual_names != expected_names or len(archive_paths) != len(expected_names):
        missing = sorted(expected_names - actual_names)
        unexpected = sorted(actual_names - expected_names)
        raise ContractError(
            f"archive namespace mismatch: expected exact canonical {len(expected_names)} "
            f"missing={missing}, unexpected={unexpected}"
        )
    for path in archive_paths:
        mode = path.lstat().st_mode
        if not stat.S_ISREG(mode):
            raise ContractError(f"archive is not a regular file: {path.name}")
        if path.stat().st_size > MAX_ARCHIVE_BYTES:
            raise ContractError(
                f"archive exceeds {MAX_ARCHIVE_BYTES} byte ceiling: {path.name}"
            )

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    rows: List[Dict[str, object]] = []
    counts = {
        "archives": 0,
        "binaries": 0,
        "runtime_files": 0,
        "associations": 0,
        "scan_objects": 0,
    }
    total_members = 0

    with tempfile.TemporaryDirectory(prefix=".cbm-release-scan-", dir=str(output_dir.parent)) as temporary:
        staged_output = pathlib.Path(temporary) / "bundle"
        staged_output.mkdir(mode=0o700)
        store = ObjectStore(staged_output / "objects")
        archive_store = ObjectStore(pathlib.Path(temporary) / "archives")
        for archive_path in archive_paths:
            archive_name = archive_path.name
            archive_object = archive_store.ingest_path(
                archive_path,
                ceiling=MAX_ARCHIVE_BYTES,
                label=archive_name,
            )
            archive_sha256 = archive_object.sha256
            kinds, member_total = (
                process_tar(
                    archive_object.path,
                    archive_name=archive_name,
                    store=store,
                    rows=rows,
                    archive_sha256=archive_sha256,
                    remaining_member_bytes=MAX_TOTAL_MEMBER_BYTES - total_members,
                )
                if archive_name.endswith(".tar.gz")
                else process_zip(
                    archive_object.path,
                    archive_name=archive_name,
                    store=store,
                    rows=rows,
                    archive_sha256=archive_sha256,
                    remaining_member_bytes=MAX_TOTAL_MEMBER_BYTES - total_members,
                )
            )
            total_members += member_total
            if total_members > MAX_TOTAL_MEMBER_BYTES:
                raise ContractError("release matrix exceeds total uncompressed byte ceiling")
            if archive_name.endswith(".mcpb"):
                validate_mcpb_manifest(archive_object.path, archive_name=archive_name)
            counts["archives"] += 1
            counts["binaries"] += sum(kind == "binary" for kind in kinds.values())
            counts["runtime_files"] += sum(kind == "runtime" for kind in kinds.values())

        counts["associations"] = len(rows)
        counts["scan_objects"] = len(store.objects)
        for key in ("archives", "binaries", "runtime_files"):
            wanted = expected[key]
            if wanted is not None and counts[key] != wanted:
                display = key.replace("_files", " files")
                raise ContractError(
                    f"count contract failed: {display} expected {wanted}, got {counts[key]}"
                )

        rows.sort(
            key=lambda row: (
                str(row["archive"]),
                str(row["member"]),
                str(row["asset_path"]),
            )
        )
        metadata_order = (
            "archives",
            "binaries",
            "runtime_files",
            "associations",
            "scan_objects",
        )
        write_tsv(
            staged_output / "associations.tsv",
            marker="cbm-release-scan-associations-v3",
            metadata=((key, counts[key]) for key in metadata_order),
            fields=ASSOCIATION_FIELDS,
            rows=rows,
        )
        scan_rows = [
            {
                "scan_path": item.scan_path,
                "sha256": item.sha256,
                "size": item.size,
                "association_count": item.association_count,
                "association_kinds": ",".join(sorted(item.kinds)),
            }
            for item in sorted(store.objects, key=lambda item: item.scan_path)
        ]
        write_tsv(
            staged_output / "scan-set.tsv",
            marker="cbm-release-scan-set-v2",
            metadata=((key, counts[key]) for key in ("scan_objects", "associations")),
            fields=SCAN_SET_FIELDS,
            rows=scan_rows,
        )
        for item in store.objects:
            item.path.chmod(0o644)

        # The objects and both manifests become visible together. A rejected
        # archive matrix can therefore never leave a partial set for the action.
        if output_dir.exists():
            if any(output_dir.iterdir()):
                raise ContractError(f"output directory became non-empty: {output_dir}")
            output_dir.rmdir()
        os.replace(staged_output, output_dir)

    print(
        f"validated exact {counts['archives']}-archive matrix: "
        f"{counts['binaries']} binaries and {counts['runtime_files']} runtime files"
    )
    print(
        f"scan bundle: {counts['scan_objects']} distinct byte objects cover "
        f"{counts['associations']} extracted member/asset associations"
    )
    print(f"associations: {output_dir / 'associations.tsv'}")
    print(f"expected scan set: {output_dir / 'scan-set.tsv'}")


try:
    main(sys.argv)
except (
    ContractError,
    OSError,
    OverflowError,
    struct.error,
    tarfile.TarError,
    zipfile.BadZipFile,
    zipfile.LargeZipFile,
) as error:
    print(f"FAIL: {error}", file=sys.stderr)
    raise SystemExit(1)
PY

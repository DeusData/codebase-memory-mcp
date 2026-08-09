#!/usr/bin/env bash
# Contract for the release scan boundary: archives are validated and hashed;
# only exact extracted members and independently parsed UI-pack assets enter
# the byte-deduplicated, atomically published scan set.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

python3 - "$ROOT" "$BASH" <<'PY'
from __future__ import annotations

import csv
import hashlib
import io
import pathlib
import stat
import struct
import subprocess
import sys
import tarfile
import tempfile
import warnings
import zipfile
from typing import Dict, List, Optional, Tuple


root = pathlib.Path(sys.argv[1])
bash_executable = pathlib.Path(sys.argv[2])
extractor = root / "scripts/ci/extract-release-archives.sh"
unix_targets = (
    "linux-amd64",
    "linux-arm64",
    "darwin-amd64",
    "darwin-arm64",
    "linux-amd64-portable",
    "linux-arm64-portable",
)
windows_targets = ("windows-amd64", "windows-arm64")
archive_names = tuple(
    sorted(
        [
            f"codebase-memory-mcp{suffix}-{target}.tar.gz"
            for suffix in ("", "-ui")
            for target in unix_targets
        ]
        + [
            f"codebase-memory-mcp{suffix}-{target}.zip"
            for suffix in ("", "-ui")
            for target in windows_targets
        ]
    )
)
ui_archive_names = tuple(name for name in archive_names if name.startswith("codebase-memory-mcp-ui-"))
expected_base_counts = {
    "archives": 16,
    "binaries": 16,
    "packs": 8,
    "runtime_files": 64,
}
count_args = [
    f"--expect-{key.replace('_', '-')}={value}"
    for key, value in expected_base_counts.items()
]
ui_expected_base_counts = {
    "archives": 8,
    "binaries": 8,
    "packs": 8,
    "runtime_files": 32,
}
ui_count_args = [
    "--archive-scope=ui",
    *[
        f"--expect-{key.replace('_', '-')}={value}"
        for key, value in ui_expected_base_counts.items()
    ],
]
asset_payloads = {
    "/assets/app.css": b"body{color:#123}\n",
    "/assets/app.js": b"globalThis.__CBM_UI__ = true;\n",
    "/index.html": b"<!doctype html><script src='/assets/app.js'></script>\n",
}
mime_ids = {".html": 1, ".js": 2, ".css": 3}


def fail(message: str) -> None:
    raise AssertionError(message)


def is_ui(name: str) -> bool:
    return name.startswith("codebase-memory-mcp-ui-")


def make_pack(payloads: Dict[str, bytes]) -> bytes:
    entries = sorted(payloads.items())
    paths = [path.encode("ascii") for path, _ in entries]
    index_size = len(entries) * 24
    paths_size = sum(len(path) for path in paths)
    payload_size = sum(len(data) for _, data in entries)
    paths_offset = 80 + index_size
    payload_offset = paths_offset + paths_size
    total = payload_offset + payload_size
    pack = bytearray(total)
    struct.pack_into(
        "<8sHHIIIQQQQQQQ",
        pack,
        0,
        b"CBMUIPK\0",
        1,
        80,
        0,
        len(entries),
        24,
        80,
        index_size,
        paths_offset,
        paths_size,
        payload_offset,
        payload_size,
        total,
    )
    path_cursor = 0
    payload_cursor = 0
    for index, ((path, data), raw_path) in enumerate(zip(entries, paths)):
        extension = pathlib.PurePosixPath(path).suffix
        struct.pack_into(
            "<IHBBQQ",
            pack,
            80 + index * 24,
            path_cursor,
            len(raw_path),
            mime_ids[extension],
            1 if path == "/index.html" else 2,
            payload_cursor,
            len(data),
        )
        pack[paths_offset + path_cursor : paths_offset + path_cursor + len(raw_path)] = raw_path
        pack[payload_offset + payload_cursor : payload_offset + payload_cursor + len(data)] = data
        path_cursor += len(raw_path)
        payload_cursor += len(data)
    return bytes(pack)


valid_pack = make_pack(asset_payloads)


def mutate_pack(kind: str) -> bytes:
    data = bytearray(valid_pack)
    if kind == "bad_pack_magic":
        data[0] = ord("X")
    elif kind == "bad_pack_bounds":
        struct.pack_into("<Q", data, 72, len(data) + 1)
    elif kind == "bad_pack_path":
        _, _, _, _, _, _, _, index_size, paths_offset, _, _, _, _ = struct.unpack_from(
            "<8sHHIIIQQQQQQQ", data, 0
        )
        del index_size
        first_length = struct.unpack_from("<H", data, 84)[0]
        replacement = b"/evilxx/app.css"
        if len(replacement) != first_length:
            fail("bad-path fixture length drifted")
        data[paths_offset : paths_offset + first_length] = replacement
    elif kind == "bad_pack_mime":
        data[86] = 2  # first entry is app.css, whose closed MIME id is 3
    else:
        fail(f"unknown pack mutation: {kind}")
    return bytes(data)


def regular(name: str, data: bytes) -> Dict[str, object]:
    return {"name": name, "kind": "regular", "data": data}


def archive_entries(name: str, mutation: Optional[str] = None) -> List[Dict[str, object]]:
    marker = name.encode("ascii")
    windows = name.endswith(".zip")
    binary = "codebase-memory-mcp.exe" if windows else "codebase-memory-mcp"
    installer = "install.ps1" if windows else "install.sh"
    entries = [
        regular(binary, b"binary bytes from " + marker + b"\n"),
        regular("cbm-integrations.json", b'{"schema":1,"integrations":[]}\n'),
        regular("LICENSE", b"shared release license\n"),
        regular(
            installer,
            b"Write-Output 'shared installer'\n"
            if windows
            else b"#!/bin/sh\n# shared installer\n",
        ),
        regular("THIRD_PARTY_NOTICES.md", b"shared third-party notices\n"),
    ]
    if is_ui(name):
        pack = mutate_pack(mutation) if mutation and mutation.startswith("bad_pack_") else valid_pack
        entries.append(regular(f"cbm-ui-{hashlib.sha256(pack).hexdigest()}.pack", pack))
    return entries


def write_archive(path: pathlib.Path, entries: List[Dict[str, object]]) -> None:
    if path.name.endswith(".tar.gz"):
        with tarfile.open(path, "w:gz") as archive:
            for entry in entries:
                name = str(entry["name"])
                kind = str(entry["kind"])
                info = tarfile.TarInfo(name)
                info.mtime = 1
                if kind == "regular":
                    data = bytes(entry["data"])
                    info.mode = 0o755 if name == "codebase-memory-mcp" else 0o644
                    info.size = len(data)
                    archive.addfile(info, io.BytesIO(data))
                elif kind == "directory":
                    info.type = tarfile.DIRTYPE
                    info.mode = 0o755
                    archive.addfile(info)
                elif kind == "symlink":
                    info.type = tarfile.SYMTYPE
                    info.linkname = str(entry["target"])
                    archive.addfile(info)
                else:
                    fail(f"unknown fixture entry kind: {kind}")
        return
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for entry in entries:
                name = str(entry["name"])
                kind = str(entry["kind"])
                info = zipfile.ZipInfo(name)
                info.create_system = 3
                if kind == "regular":
                    mode = 0o755 if name == "codebase-memory-mcp.exe" else 0o644
                    info.external_attr = (stat.S_IFREG | mode) << 16
                    archive.writestr(info, bytes(entry["data"]))
                elif kind == "directory":
                    if not name.endswith("/"):
                        info.filename += "/"
                    info.external_attr = ((stat.S_IFDIR | 0o755) << 16) | 0x10
                    archive.writestr(info, b"")
                elif kind == "symlink":
                    info.external_attr = (stat.S_IFLNK | 0o777) << 16
                    archive.writestr(info, str(entry["target"]).encode("ascii"))
                else:
                    fail(f"unknown fixture entry kind: {kind}")


Association = Tuple[str, str, str, str]


def build_matrix(
    archive_dir: pathlib.Path,
    mutation: Optional[str] = None,
    selected_names: Tuple[str, ...] = archive_names,
) -> Dict[Association, bytes]:
    archive_dir.mkdir(parents=True)
    expected: Dict[Association, bytes] = {}
    targets = {
        "bad_hash": "codebase-memory-mcp-ui-linux-amd64.tar.gz",
        "bad_pack_magic": "codebase-memory-mcp-ui-linux-amd64.tar.gz",
        "bad_pack_bounds": "codebase-memory-mcp-ui-linux-arm64.tar.gz",
        "bad_pack_path": "codebase-memory-mcp-ui-darwin-amd64.tar.gz",
        "bad_pack_mime": "codebase-memory-mcp-ui-darwin-arm64.tar.gz",
        "directory": "codebase-memory-mcp-linux-amd64.tar.gz",
        "extra_pack": "codebase-memory-mcp-ui-linux-arm64-portable.tar.gz",
        "missing_pack": "codebase-memory-mcp-ui-windows-arm64.zip",
        "nested_member": "codebase-memory-mcp-darwin-amd64.tar.gz",
        "tar_symlink": "codebase-memory-mcp-linux-arm64.tar.gz",
        "tar_duplicate": "codebase-memory-mcp-ui-linux-amd64-portable.tar.gz",
        "zip_symlink": "codebase-memory-mcp-windows-amd64.zip",
        "zip_directory": "codebase-memory-mcp-windows-arm64.zip",
        "zip_duplicate": "codebase-memory-mcp-ui-windows-amd64.zip",
    }
    target = targets.get(mutation)
    for name in selected_names:
        if mutation == "missing_archive" and name == "codebase-memory-mcp-linux-amd64.tar.gz":
            continue
        pack_mutation = mutation if name == target and mutation and mutation.startswith("bad_pack_") else None
        entries = archive_entries(name, pack_mutation)
        if name == target:
            if mutation == "bad_hash":
                entries = [entry for entry in entries if not str(entry["name"]).endswith(".pack")]
                entries.append(regular(f"cbm-ui-{'0' * 64}.pack", valid_pack))
            elif mutation in ("directory", "zip_directory"):
                entries.append({"name": "nested/", "kind": "directory"})
            elif mutation in ("tar_duplicate", "zip_duplicate"):
                entries.append(regular("LICENSE", b"second license body\n"))
            elif mutation == "extra_pack":
                second = make_pack({
                    "/assets/other.js": b"console.log('other')\n",
                    "/index.html": b"<!doctype html>other\n",
                })
                entries.append(regular(f"cbm-ui-{hashlib.sha256(second).hexdigest()}.pack", second))
            elif mutation == "missing_pack":
                entries = [entry for entry in entries if not str(entry["name"]).endswith(".pack")]
            elif mutation == "nested_member":
                entries.append(regular("nested/payload", b"unexpected nested member\n"))
            elif mutation in ("tar_symlink", "zip_symlink"):
                installer = "install.ps1" if name.endswith(".zip") else "install.sh"
                entries = [entry for entry in entries if entry["name"] != installer]
                entries.append({"name": installer, "kind": "symlink", "target": "LICENSE"})
        path = archive_dir / name
        write_archive(path, entries)
        if mutation is None:
            for entry in entries:
                if entry["kind"] != "regular":
                    continue
                member = str(entry["name"])
                data = bytes(entry["data"])
                expected[("member", name, member, "")] = data
                if member.endswith(".pack"):
                    for asset_path, payload in asset_payloads.items():
                        expected[("pack_asset", name, member, asset_path)] = payload
    if mutation == "unexpected_archive":
        unexpected = "codebase-memory-mcp-freebsd-amd64.tar.gz"
        write_archive(archive_dir / unexpected, archive_entries("codebase-memory-mcp-linux-amd64.tar.gz"))
    if mutation == "oversized_archive":
        with (archive_dir / "codebase-memory-mcp-linux-amd64.tar.gz").open("r+b") as handle:
            handle.truncate(512 * 1024 * 1024 + 1)
    return expected


def invoke(
    archive_dir: pathlib.Path,
    output_dir: pathlib.Path,
    args: Optional[List[str]] = None,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            str(bash_executable),
            str(extractor),
            str(archive_dir),
            str(output_dir),
            *(args or count_args),
        ],
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )


def parse_manifest(path: pathlib.Path, marker: str) -> Tuple[Dict[str, int], List[Dict[str, str]]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != f"# {marker}":
        fail(f"{path.name} is missing marker {marker}")
    metadata: Dict[str, int] = {}
    cursor = 1
    while cursor < len(lines) and lines[cursor].startswith("# "):
        key, value = lines[cursor][2:].split("=", 1)
        metadata[key] = int(value)
        cursor += 1
    return metadata, list(csv.DictReader(lines[cursor:], delimiter="\t"))


def assert_clean_failure(case_root: pathlib.Path, mutation: str, message: str) -> None:
    archive_dir = case_root / "archives"
    output_dir = case_root / "scan"
    build_matrix(archive_dir, mutation)
    result = invoke(archive_dir, output_dir)
    if result.returncode == 0 or message not in result.stdout:
        fail(
            f"{mutation}: extractor must fail with {message!r}; "
            f"rc={result.returncode}, output={result.stdout.strip()}"
        )
    if output_dir.exists():
        fail(f"{mutation}: rejected archives left a partial atomic scan bundle")


if len(archive_names) != 16 or len(ui_archive_names) != 8:
    fail("fixture matrix no longer describes 16 canonical archives / eight UI archives")

with tempfile.TemporaryDirectory(prefix="cbm-release-scan-contract-") as temporary:
    work = pathlib.Path(temporary)
    valid = work / "valid"
    expected = build_matrix(valid / "archives")
    output = valid / "scan"
    result = invoke(valid / "archives", output)
    if result.returncode != 0:
        fail(f"canonical release matrix was rejected: {result.stdout.strip()}")
    if {path.name for path in output.iterdir()} != {"objects", "associations.tsv", "scan-set.tsv"}:
        fail("scan bundle must publish exactly objects/ plus its two manifests")

    association_metadata, rows = parse_manifest(
        output / "associations.tsv", "cbm-release-scan-associations-v3"
    )
    expected_pack_assets = 8 * len(asset_payloads)
    expected_associations = 88 + expected_pack_assets
    expected_metadata = {
        **expected_base_counts,
        "pack_assets": expected_pack_assets,
        "associations": expected_associations,
        "scan_objects": len(set(expected.values())),
    }
    if association_metadata != expected_metadata:
        fail(f"association metadata mismatch: {association_metadata!r}")
    if len(rows) != expected_associations:
        fail("association manifest does not contain every extracted member/asset association")
    actual_associations = {
        (row["association_type"], row["archive"], row["member"], row["asset_path"]): row
        for row in rows
    }
    if any(row["association_type"] == "archive" or row["kind"] == "archive" for row in rows):
        fail("archive containers must not appear in the VirusTotal association set")
    if set(actual_associations) != set(expected):
        fail("association manifest keys differ from exact shipped associations")
    archive_hashes = {
        name: hashlib.sha256((valid / "archives" / name).read_bytes()).hexdigest()
        for name in archive_names
    }
    for association, data in expected.items():
        row = actual_associations[association]
        object_path = output / row["scan_path"]
        if object_path.read_bytes() != data:
            fail(f"scan object changed bytes for association {association!r}")
        if row["object_sha256"] != hashlib.sha256(data).hexdigest() or int(row["size"]) != len(data):
            fail(f"association digest/size is wrong for {association!r}")
        if row["archive_sha256"] != archive_hashes[row["archive"]]:
            fail(f"association archive provenance is wrong for {association!r}")

    object_paths = sorted((output / "objects").iterdir())
    if len(object_paths) != len(set(expected.values())):
        fail("byte-identical archive members/assets were not deduplicated")
    object_bytes = [path.read_bytes() for path in object_paths]
    if len(object_bytes) != len(set(object_bytes)):
        fail("scan set contains duplicate byte sequences")
    license_rows = [row for row in rows if row["member"] == "LICENSE"]
    if len(license_rows) != 16 or len({row["scan_path"] for row in license_rows}) != 1:
        fail("shared LICENSE bytes must map 16 associations to one scan object")
    pack_rows = [row for row in rows if row["kind"] == "pack"]
    if len(pack_rows) != 8 or len({row["scan_path"] for row in pack_rows}) != 1:
        fail("shared UI pack bytes must map eight associations to one scan object")
    script_rows = [row for row in rows if row["asset_path"] == "/assets/app.js"]
    if len(script_rows) != 8 or len({row["scan_path"] for row in script_rows}) != 1:
        fail("each pack's JavaScript association must reach one exact deduplicated script object")

    scan_metadata, scan_rows = parse_manifest(output / "scan-set.tsv", "cbm-release-scan-set-v2")
    if scan_metadata != {
        "scan_objects": expected_metadata["scan_objects"],
        "associations": expected_associations,
    }:
        fail(f"scan-set metadata mismatch: {scan_metadata!r}")
    if len(scan_rows) != len(object_paths):
        fail("scan-set row count differs from staged distinct objects")
    scan_paths = {row["scan_path"] for row in scan_rows}
    if scan_paths != {f"objects/{path.name}" for path in object_paths}:
        fail("scan-set paths differ from staged object paths")
    if sum(int(row["association_count"]) for row in scan_rows) != expected_associations:
        fail("scan-set association counts do not cover the association manifest")
    for row in scan_rows:
        path = output / row["scan_path"]
        data = path.read_bytes()
        if row["sha256"] != hashlib.sha256(data).hexdigest() or int(row["size"]) != len(data):
            fail(f"scan-set digest/size does not bind {row['scan_path']}")

    ui_only = work / "ui-only"
    ui_expected = build_matrix(ui_only / "archives", selected_names=ui_archive_names)
    ui_output = ui_only / "scan"
    ui_result = invoke(ui_only / "archives", ui_output, ui_count_args)
    if ui_result.returncode != 0:
        fail(f"canonical UI-only matrix was rejected: {ui_result.stdout.strip()}")
    ui_metadata, ui_rows = parse_manifest(
        ui_output / "associations.tsv", "cbm-release-scan-associations-v3"
    )
    ui_associations = 48 + expected_pack_assets
    if ui_metadata != {
        **ui_expected_base_counts,
        "pack_assets": expected_pack_assets,
        "associations": ui_associations,
        "scan_objects": len(set(ui_expected.values())),
    }:
        fail(f"UI-only association metadata mismatch: {ui_metadata!r}")
    if len(ui_rows) != ui_associations or {row["variant"] for row in ui_rows} != {"ui"}:
        fail("UI-only scan scope does not cover exactly the eight UI archives")
    ui_as_all_output = ui_only / "scan-as-all"
    ui_as_all = invoke(ui_only / "archives", ui_as_all_output, count_args)
    if (
        ui_as_all.returncode == 0
        or "expected exact canonical 16 (all)" not in ui_as_all.stdout
        or ui_as_all_output.exists()
    ):
        fail("UI-only archives must require an explicit closed archive scope")

    failure_cases = {
        "bad_hash": "UI pack digest does not match its filename",
        "bad_pack_magic": "CBMUIPK header/offset contract failed",
        "bad_pack_bounds": "CBMUIPK header/offset contract failed",
        "bad_pack_path": "CBMUIPK asset path is invalid",
        "bad_pack_mime": "CBMUIPK MIME/path contract failed",
        "directory": "non-regular archive member",
        "extra_pack": "exactly one UI pack",
        "missing_archive": "expected exact canonical 16 (all)",
        "missing_pack": "exactly one UI pack",
        "nested_member": "unexpected archive member",
        "oversized_archive": "archive exceeds 536870912 byte ceiling",
        "tar_symlink": "non-regular archive member",
        "tar_duplicate": "duplicate archive member",
        "unexpected_archive": "expected exact canonical 16 (all)",
        "zip_directory": "non-regular archive member",
        "zip_duplicate": "duplicate archive member",
        "zip_symlink": "non-regular archive member",
    }
    for mutation, message in failure_cases.items():
        case_root = work / mutation
        case_root.mkdir()
        assert_clean_failure(case_root, mutation, message)

    wrong_count = work / "wrong-count"
    wrong_count.mkdir()
    build_matrix(wrong_count / "archives")
    wrong_args = [arg for arg in count_args if not arg.startswith("--expect-packs=")]
    wrong_args.append("--expect-packs=9")
    wrong_output = wrong_count / "scan"
    wrong = invoke(wrong_count / "archives", wrong_output, wrong_args)
    if wrong.returncode == 0 or "packs expected 9, got 8" not in wrong.stdout or wrong_output.exists():
        fail(f"explicit expected-count mismatch did not fail atomically: {wrong.stdout.strip()}")

source = extractor.read_text(encoding="utf-8")
if "files_equal(candidate, existing.path)" not in source:
    fail("deduplication no longer exact-compares bytes after SHA-256+size grouping")
if source.count("archive_object.path,") < 2:
    fail("members are not parsed from the same retained archive bytes used for provenance")
for ceiling in ("MAX_ARCHIVE_BYTES", "MAX_MEMBER_BYTES", "MAX_TOTAL_MEMBER_BYTES"):
    if ceiling not in source:
        fail(f"extractor lost required size ceiling {ceiling}")

release = (root / ".github/workflows/release.yml").read_text(encoding="utf-8")
dry_run = (root / ".github/workflows/dry-run.yml").read_text(encoding="utf-8")
build = (root / ".github/workflows/_build.yml").read_text(encoding="utf-8")
smoke = (root / ".github/workflows/_smoke.yml").read_text(encoding="utf-8")
if "ui_only" in build or "Build standard binary" in build:
    fail("canonical build workflow must not retain a non-UI release path")
if build.count("--variant ui") != 4:
    fail("canonical build workflow must package exactly its four UI-enabled build families")
if "ui_only" in smoke or "VARIANTS='[\"ui\"]'" not in smoke or "standard" in smoke.split("VARIANTS=", 1)[1].split("\n", 1)[0]:
    fail("release smoke matrix must expose only the UI-enabled runtime")
if "ui_only" in dry_run or "--archive-scope=ui" not in dry_run:
    fail("dry-run must use the canonical UI-only build/smoke/extraction path")
if "--archive-scope=ui" not in release:
    fail("release verification must accept only the shipped UI-enabled archives")
if (
    "path: ${{ runner.temp }}/release-archives" not in dry_run
    or 'extract-release-archives.sh "$RUNNER_TEMP/release-archives" binaries' not in dry_run
    or "path: assets" in dry_run
    or "extract-release-archives.sh assets binaries" in dry_run
):
    fail("dry-run must isolate downloaded archives from tracked checkout assets")
if (
    'ARCHIVE_DIR="$RUNNER_TEMP/release-archives"' not in release
    or '--dir "$ARCHIVE_DIR"' not in release
    or 'extract-release-archives.sh "$ARCHIVE_DIR" binaries' not in release
    or "--dir assets" in release
    or "extract-release-archives.sh assets binaries" in release
):
    fail("release verification must isolate downloaded archives from tracked checkout assets")
for workflow_name, workflow, arguments in (
    ("release", release, ui_count_args),
    ("dry-run", dry_run, ui_count_args),
):
    for argument in arguments:
        if argument not in workflow:
            fail(f"{workflow_name} workflow does not assert {argument}")
    for required in (
        "files: binaries/objects/*",
        "request_rate: 4",
        "VT_EXPECTED_SCAN_SET: binaries/scan-set.tsv",
        "VT_ASSOCIATIONS: binaries/associations.tsv",
        "VT_RESULTS_PATH: binaries/vt-results.tsv",
        "MIN_ENGINES: 50",
        "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
        "virustotal-evidence-",
    ):
        if required not in workflow:
            fail(f"{workflow_name} workflow is missing exact scan/gate contract: {required}")
    lowered = workflow.lower()
    for forbidden in ("false-positive", "false positive", "submission", "reputation"):
        if forbidden in lowered:
            fail(f"{workflow_name} workflow prescribes forbidden scan-cleaning policy: {forbidden}")
    if "naturally clean" not in lowered:
        fail(f"{workflow_name} workflow must state the naturally-clean baseline")

notes = (root / "scripts/ci/append-vt-notes.sh").read_text(encoding="utf-8")
for required in (
    "cbm-security-verification:start",
    "cbm-security-verification:end",
    "binaries/associations.tsv",
    "binaries/vt-results.tsv",
):
    if required not in notes:
        fail(f"release-note updater is missing {required}")
if "70+" in notes or "...`" in notes:
    fail("release notes must use measured engine counts and full SHA-256 values")

print("release archive extractor contract: OK")
PY

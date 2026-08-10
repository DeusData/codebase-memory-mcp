#!/usr/bin/env bash
# Contract: scripts/ci/extract-release-archives.sh produces the exact scan bundle
# the VirusTotal gate consumes.
#
# The gate is only as good as this manifest. If the extractor silently skipped a
# member, mis-deduplicated bytes, or let an archive container into the scan set,
# check-virustotal.sh would still report a confident green over an incomplete
# set. These assertions are what make "0 detections" mean the whole release.
#
# The pack-format half of the previous contract is gone with the UI pack itself;
# everything asserted here applies to the four-member single-composition archive.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIX="$(mktemp -d "${TMPDIR:-/tmp}/cbm-extract-contract.XXXXXX")"
trap 'rm -rf "$FIX"' EXIT

# "$BASH" by explicit argv: BASH is a shell variable, NOT exported, so reading
# it from os.environ inside Python silently falls back — and on native Windows a
# bare "bash" resolves to the WSL stub, which fails every invocation with
# "Windows Subsystem for Linux has no installed distributions".
python3 - "$ROOT" "$FIX" "$BASH" <<'PY'
import hashlib
import pathlib
import subprocess
import sys
import tarfile
import zipfile

root = pathlib.Path(sys.argv[1])
fixtures = pathlib.Path(sys.argv[2])
bash_executable = sys.argv[3]
extractor = root / "scripts" / "ci" / "extract-release-archives.sh"

UNIX = ("linux-amd64", "linux-arm64", "darwin-amd64", "darwin-arm64",
        "linux-amd64-portable", "linux-arm64-portable")
WINDOWS = ("windows-amd64", "windows-arm64")
ARCHIVES = tuple(f"codebase-memory-mcp-{t}.tar.gz" for t in UNIX) + \
           tuple(f"codebase-memory-mcp-{t}.zip" for t in WINDOWS)

# Deliberately byte-IDENTICAL across every archive: the extractor must collapse
# them to one scan object each, or the gate pays to scan the same bytes 8 times
# and the association count stops meaning "distinct bytes".
SHARED_LICENSE = b"shared release license\n"
SHARED_NOTICES = b"shared third-party notices\n"
SHARED_SH = b"#!/bin/sh\n# shared installer\n"
SHARED_PS1 = b"Write-Output 'shared installer'\n"

failures = []


def fail(message):
    failures.append(message)


def members(archive):
    windows = archive.endswith(".zip")
    binary = "codebase-memory-mcp.exe" if windows else "codebase-memory-mcp"
    installer = "install.ps1" if windows else "install.sh"
    return {
        binary: b"binary bytes of " + archive.encode(),   # unique per archive
        "LICENSE": SHARED_LICENSE,
        installer: SHARED_PS1 if windows else SHARED_SH,
        "THIRD_PARTY_NOTICES.md": SHARED_NOTICES,
    }


def write_archive(directory, archive, extra=None, drop=None):
    entries = members(archive)
    if drop:
        entries.pop(drop, None)
    if extra:
        entries[extra] = b"unexpected\n"
    path = directory / archive
    if archive.endswith(".zip"):
        with zipfile.ZipFile(path, "w") as zf:
            for name, data in entries.items():
                zf.writestr(name, data)
    else:
        with tarfile.open(path, "w:gz") as tf:
            for name, data in entries.items():
                info = tarfile.TarInfo(name)
                info.size = len(data)
                tf.addfile(info, __import__("io").BytesIO(data))
    return path


def build_matrix(directory, **kwargs):
    directory.mkdir(parents=True, exist_ok=True)
    for archive in ARCHIVES:
        write_archive(directory, archive, **kwargs)
    return directory


def run_extractor(archive_dir, out_dir, *args):
    return subprocess.run(
        [bash_executable, str(extractor), str(archive_dir), str(out_dir), *args],
        capture_output=True, text=True,
    )


def read_manifest(path, marker):
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != f"# {marker}":
        fail(f"{path.name} does not start with its marker {marker!r}")
        return {}, []
    meta, rows, header = {}, [], None
    for line in lines[1:]:
        if line.startswith("# "):
            key, _, value = line[2:].partition("=")
            meta[key] = int(value) if value.isdigit() else value
        elif header is None:
            header = line.split("\t")
        else:
            rows.append(dict(zip(header, line.split("\t"))))
    return meta, rows


# ── 1. The happy path: exact matrix in, exact bundle out ────────────────────
good = build_matrix(fixtures / "good" / "archives")
out = fixtures / "good" / "scan"
result = run_extractor(good, out, "--expect-archives=8", "--expect-binaries=8",
                       "--expect-runtime-files=24")
if result.returncode != 0:
    fail(f"exact release matrix was rejected: {result.stdout[-600:]}{result.stderr[-600:]}")
else:
    published = sorted(p.name for p in out.iterdir())
    if published != ["associations.tsv", "objects", "scan-set.tsv"]:
        fail(f"scan bundle must publish exactly objects/ plus its two manifests: {published}")

    assoc_meta, assoc = read_manifest(out / "associations.tsv",
                                      "cbm-release-scan-associations-v3")
    set_meta, scan_set = read_manifest(out / "scan-set.tsv", "cbm-release-scan-set-v2")

    # Every member of every archive is covered — 8 archives x 4 members.
    if len(assoc) != 32:
        fail(f"association manifest must cover all 32 extracted members, got {len(assoc)}")

    # An archive container must never be scanned as if it were a member.
    if any(row["member"] in ARCHIVES for row in assoc):
        fail("archive containers must not appear in the association set")

    # Deduplication: identical bytes collapse, unique bytes do not.
    objects = {row["scan_path"] for row in assoc}
    if len(scan_set) != len(objects):
        fail(f"scan-set rows ({len(scan_set)}) differ from distinct objects ({len(objects)})")
    licences = {row["scan_path"] for row in assoc if row["member"] == "LICENSE"}
    if len(licences) != 1:
        fail(f"8 byte-identical LICENSE members must map to ONE scan object, got {len(licences)}")
    binaries = {row["scan_path"] for row in assoc if row["kind"] == "binary"}
    if len(binaries) != 8:
        fail(f"8 distinct binaries must stay 8 scan objects, got {len(binaries)}")

    # The counts the gate reads back must agree with the rows.
    if assoc_meta.get("associations") != len(assoc):
        fail("association metadata disagrees with its own rows")
    if set_meta.get("scan_objects") != len(scan_set):
        fail("scan-set metadata disagrees with its own rows")

    # Every staged object must exist with the hash the manifest claims.
    for row in scan_set:
        staged = out / row["scan_path"]
        if not staged.is_file():
            fail(f"scan-set names a missing object: {row['scan_path']}")
        elif hashlib.sha256(staged.read_bytes()).hexdigest() != row["sha256"]:
            fail(f"staged object does not match its recorded hash: {row['scan_path']}")

# ── 2. Fail-closed cases — each must be REFUSED, not silently scanned ───────
for label, kwargs, expect in (
    ("an unexpected extra member", {"extra": "surprise.txt"}, "unexpected archive member"),
    ("a missing required member", {"drop": "LICENSE"}, "member namespace mismatch"),
):
    case = fixtures / label.replace(" ", "_")
    bad = build_matrix(case / "archives", **kwargs)
    result = run_extractor(bad, case / "scan")
    if result.returncode == 0:
        fail(f"extractor accepted {label}")
    elif expect not in (result.stdout + result.stderr):
        fail(f"{label} was rejected without naming the contract: "
             f"{(result.stdout + result.stderr)[-300:]}")

# An incomplete matrix must never satisfy an exact count.
short = fixtures / "short" / "archives"
short.mkdir(parents=True)
write_archive(short, ARCHIVES[0])
result = run_extractor(short, fixtures / "short" / "scan", "--expect-archives=8")
if result.returncode == 0:
    fail("extractor accepted a 1-archive matrix under --expect-archives=8")

if failures:
    print("RELEASE ARCHIVE EXTRACTOR CONTRACT VIOLATED:")
    for message in failures:
        print(f"  - {message}")
    sys.exit(1)

print("release archive extractor contract OK "
      "(8-archive matrix, 32 member associations, dedup exact, fail-closed on "
      "surplus/missing members and short matrices)")
PY

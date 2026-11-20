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
MCPB = ("darwin-amd64", "darwin-arm64", "linux-amd64-portable",
        "linux-arm64-portable", "windows-amd64", "windows-arm64")
ARCHIVES = tuple(f"codebase-memory-mcp-{t}.tar.gz" for t in UNIX) + \
           tuple(f"codebase-memory-mcp-{t}.zip" for t in WINDOWS) + \
           tuple(f"codebase-memory-mcp-{t}.mcpb" for t in MCPB)

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


def target_of(archive):
    return archive.rsplit(".", 1)[0].removesuffix(".tar")


def make_manifest(entry_point):
    return (
        '{"manifest_version": "0.3", "name": "codebase-memory-mcp",'
        ' "version": "0.0.0-test", "server": {"type": "binary",'
        f' "entry_point": "{entry_point}",'
        ' "mcp_config": {"command": "${__dirname}/' + entry_point + '", "args": []}}}'
    ).encode()


def members(archive):
    windows = "-windows-" in archive
    # Binary bytes keyed by TARGET, not archive: production repackages the
    # SAME staged binary into the archive and the .mcpb, and the dedup
    # assertion below depends on that.
    binary_bytes = b"binary bytes of " + target_of(archive).encode()
    if archive.endswith(".mcpb"):
        binary = "server/codebase-memory-mcp.exe" if windows else "server/codebase-memory-mcp"
        return {
            "manifest.json": make_manifest(binary),
            binary: binary_bytes,
            "server/LICENSE": SHARED_LICENSE,
            "server/THIRD_PARTY_NOTICES.md": SHARED_NOTICES,
        }
    binary = "codebase-memory-mcp.exe" if windows else "codebase-memory-mcp"
    installer = "install.ps1" if windows else "install.sh"
    return {
        binary: binary_bytes,
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
    if archive.endswith((".zip", ".mcpb")):
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
result = run_extractor(good, out, "--expect-archives=14", "--expect-binaries=14",
                       "--expect-runtime-files=42")
if result.returncode != 0:
    fail(f"exact release matrix was rejected: {result.stdout[-600:]}{result.stderr[-600:]}")
else:
    published = sorted(p.name for p in out.iterdir())
    if published != ["associations.tsv", "objects", "scan-set.tsv"]:
        fail(f"scan bundle must publish exactly objects/ plus its two manifests: {published}")

    assoc_meta, assoc = read_manifest(out / "associations.tsv",
                                      "cbm-release-scan-associations-v3")
    set_meta, scan_set = read_manifest(out / "scan-set.tsv", "cbm-release-scan-set-v2")

    # Every member of every archive is covered — 14 containers x 4 members.
    if len(assoc) != 56:
        fail(f"association manifest must cover all 56 extracted members, got {len(assoc)}")

    # An archive container must never be scanned as if it were a member.
    if any(row["member"] in ARCHIVES for row in assoc):
        fail("archive containers must not appear in the association set")

    # Deduplication: identical bytes collapse, unique bytes do not.
    objects = {row["scan_path"] for row in assoc}
    if len(scan_set) != len(objects):
        fail(f"scan-set rows ({len(scan_set)}) differ from distinct objects ({len(objects)})")
    licences = {row["scan_path"] for row in assoc
                if row["member"] in ("LICENSE", "server/LICENSE")}
    if len(licences) != 1:
        fail(f"14 byte-identical LICENSE members must map to ONE scan object, got {len(licences)}")
    # 14 binary MEMBERS but 8 distinct byte sequences: each .mcpb repackages
    # its source archive's binary, and the gate must not scan those bytes twice.
    binaries = {row["scan_path"] for row in assoc if row["kind"] == "binary"}
    if len(binaries) != 8:
        fail(f"8 distinct binaries must stay 8 scan objects, got {len(binaries)}")
    binary_members = [row for row in assoc if row["kind"] == "binary"]
    if len(binary_members) != 14:
        fail(f"14 binary members must all be associated, got {len(binary_members)}")

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
result = run_extractor(short, fixtures / "short" / "scan", "--expect-archives=14")
if result.returncode == 0:
    fail("extractor accepted a 1-archive matrix under --expect-archives=14")

# ── 3. MCPB manifest contract — a structurally broken bundle must not ship ──
BROKEN_MCPB = "codebase-memory-mcp-darwin-arm64.mcpb"


def rewrite_mcpb(directory, manifest_bytes):
    entries = members(BROKEN_MCPB)
    entries["manifest.json"] = manifest_bytes
    with zipfile.ZipFile(directory / BROKEN_MCPB, "w") as zf:
        for name, data in entries.items():
            zf.writestr(name, data)


# Case directories use SHORT slugs: the staged scan-object name embeds the
# fixture path plus a 64-hex digest, and a descriptive directory name pushes
# the total past Windows' 260-char MAX_PATH — os.replace then fails before
# the contract error under test can fire.
for label, slug, manifest_bytes, expect in (
    ("unparseable manifest.json", "m1", b"{not json", "not valid JSON"),
    ("manifest without a version", "m2",
     make_manifest("server/codebase-memory-mcp").replace(b'"version": "0.0.0-test", ', b""),
     "lacks a version"),
    ("manifest entry_point outside the bundle", "m3",
     make_manifest("server/other-binary"),
     "not a member"),
    ("manifest command not targeting the entry_point", "m4",
     make_manifest("server/codebase-memory-mcp").replace(
         b'${__dirname}/server/codebase-memory-mcp', b"/usr/bin/env"),
     "does not target the entry_point"),
):
    case = fixtures / slug
    bad = build_matrix(case / "archives")
    rewrite_mcpb(case / "archives", manifest_bytes)
    result = run_extractor(bad, case / "scan")
    if result.returncode == 0:
        fail(f"extractor accepted a bundle with {label}")
    elif expect not in (result.stdout + result.stderr):
        fail(f"{label} was rejected without naming the contract: "
             f"{(result.stdout + result.stderr)[-300:]}")

if failures:
    print("RELEASE ARCHIVE EXTRACTOR CONTRACT VIOLATED:")
    for message in failures:
        print(f"  - {message}")
    sys.exit(1)

print("release archive extractor contract OK "
      "(14-container matrix incl. 6 MCPB bundles, 56 member associations, "
      "dedup exact incl. bundle/archive binary collapse, fail-closed on "
      "surplus/missing members, short matrices and broken MCPB manifests)")
PY

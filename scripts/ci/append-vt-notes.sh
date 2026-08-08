#!/usr/bin/env bash
# Idempotently publish the exact VirusTotal scope and measured result evidence
# after check-virustotal.sh has passed.
set -euo pipefail

: "${GH_TOKEN:?append-vt-notes: GH_TOKEN is required}"
: "${VERSION:?append-vt-notes: VERSION is required}"
: "${GITHUB_REPOSITORY:?append-vt-notes: GITHUB_REPOSITORY is required}"

VT_ASSOCIATIONS="${VT_ASSOCIATIONS:-binaries/associations.tsv}"
VT_RESULTS_PATH="${VT_RESULTS_PATH:-binaries/vt-results.tsv}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/cbm-vt-notes.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

gh release view "$VERSION" \
  --json body --jq '.body // ""' --repo "$GITHUB_REPOSITORY" > "$WORK/current.md"

python3 - "$VT_ASSOCIATIONS" "$VT_RESULTS_PATH" "$WORK/current.md" "$WORK/updated.md" \
  "$GITHUB_REPOSITORY" "$VERSION" <<'PY'
from __future__ import annotations

import csv
import pathlib
import re
import sys
import urllib.parse
from typing import Dict, List, Sequence, Tuple


START = "<!-- cbm-security-verification:start -->"
END = "<!-- cbm-security-verification:end -->"
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
    "microsoft_engine_version",
    "microsoft_engine_update",
    "virustotal_url",
)


def fail(message: str) -> None:
    raise SystemExit(f"append-vt-notes: {message}")


def read_tsv(
    path: pathlib.Path,
    marker: str,
    fields: Sequence[str],
) -> Tuple[Dict[str, int], List[Dict[str, str]]]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 16 * 1024 * 1024:
        fail(f"missing, unsafe or oversized evidence file: {path}")
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != f"# {marker}":
        fail(f"wrong evidence marker in {path}")
    metadata: Dict[str, int] = {}
    cursor = 1
    while cursor < len(lines) and lines[cursor].startswith("# "):
        key, separator, raw_value = lines[cursor][2:].partition("=")
        if not separator or not key or key in metadata or not raw_value.isdigit():
            fail(f"malformed evidence metadata in {path}")
        metadata[key] = int(raw_value)
        cursor += 1
    reader = csv.DictReader(lines[cursor:], delimiter="\t")
    if tuple(reader.fieldnames or ()) != tuple(fields):
        fail(f"unexpected TSV header in {path}")
    rows = list(reader)
    if any(None in row or any(row.get(field) is None for field in fields) for row in rows):
        fail(f"TSV row has missing or surplus cells in {path}")
    if not rows:
        fail(f"empty evidence rows in {path}")
    return metadata, rows


association_path = pathlib.Path(sys.argv[1])
results_path = pathlib.Path(sys.argv[2])
current_path = pathlib.Path(sys.argv[3])
updated_path = pathlib.Path(sys.argv[4])
repository = sys.argv[5]
version = sys.argv[6]
if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) is None or not version:
    fail("unsafe repository or version for public evidence links")
asset_base = (
    f"https://github.com/{repository}/releases/download/"
    f"{urllib.parse.quote(version, safe='')}"
)
association_meta, associations = read_tsv(
    association_path,
    "cbm-release-scan-associations-v2",
    ASSOCIATION_FIELDS,
)
result_meta, results = read_tsv(
    results_path,
    "cbm-virustotal-results-v1",
    RESULT_FIELDS,
)
if association_meta.get("associations") != len(associations):
    fail("association manifest count does not match its rows")
if result_meta.get("scan_objects") != len(results):
    fail("VirusTotal result count does not match its rows")
if result_meta.get("associations") != len(associations):
    fail("VirusTotal results do not cover the association manifest")

results_by_path: Dict[str, Dict[str, str]] = {}
association_counts: Dict[str, int] = {}
executable_paths = {
    association["scan_path"]
    for association in associations
    if association["kind"] == "binary"
}
for association in associations:
    scan_path = association["scan_path"]
    association_counts[scan_path] = association_counts.get(scan_path, 0) + 1
for result in results:
    scan_path = result["scan_path"]
    if scan_path in results_by_path:
        fail(f"duplicate VirusTotal result path: {scan_path}")
    for numeric in (
        "size",
        "association_count",
        "completed_engines",
        "total_engines",
        "malicious",
        "suspicious",
    ):
        if not result[numeric].isdigit():
            fail(f"malformed numeric result field {numeric}: {scan_path}")
    if int(result["malicious"]) != 0 or int(result["suspicious"]) != 0:
        fail(f"detected object reached release-note publication: {scan_path}")
    if (
        re.fullmatch(r"[0-9a-f]{64}", result["sha256"]) is None
        or result["virustotal_url"]
        != f"https://www.virustotal.com/gui/file/{result['sha256']}/detection"
    ):
        fail(f"result SHA/link is malformed or mismatched: {scan_path}")
    if int(result["association_count"]) != association_counts.get(scan_path):
        fail(f"result association count mismatch: {scan_path}")
    if scan_path in executable_paths:
        if result["microsoft_category"] not in {"undetected", "harmless"}:
            fail(f"executable result lacks a decisive clean Microsoft verdict: {scan_path}")
        if not result["microsoft_engine_version"] or not result["microsoft_engine_update"]:
            fail(f"executable result lacks Microsoft version evidence: {scan_path}")
    results_by_path[scan_path] = result
if set(results_by_path) != set(association_counts):
    fail("VirusTotal result paths differ from association paths")
for association in associations:
    result = results_by_path[association["scan_path"]]
    if (
        result["sha256"] != association["object_sha256"]
        or result["size"] != association["size"]
    ):
        fail(f"result hash/size differs from association: {association['scan_path']}")

archive_rows = [row for row in associations if row["association_type"] == "archive"]
member_rows = [row for row in associations if row["association_type"] == "member"]
asset_rows = [row for row in associations if row["association_type"] == "pack_asset"]
if len(archive_rows) != association_meta.get("archives"):
    fail("downloadable archive association count is inconsistent")
archive_names = [row["archive"] for row in archive_rows]
if len(set(archive_names)) != len(archive_names):
    fail("downloadable archive association is duplicated")
archive_hashes = {row["archive"]: row["archive_sha256"] for row in archive_rows}
if any(row["archive_sha256"] != row["object_sha256"] for row in archive_rows):
    fail("downloadable archive SHA is not bound to its scan object")
if any(archive_hashes.get(row["archive"]) != row["archive_sha256"] for row in associations):
    fail("member/asset association is not bound to its downloadable archive")

engine_counts = [int(row["completed_engines"]) for row in results]
minimum = min(engine_counts)
maximum = max(engine_counts)
policy = result_meta.get("min_engines_policy")
if policy is None or minimum < policy:
    fail("result engine counts do not satisfy their recorded policy")
engine_range = str(minimum) if minimum == maximum else f"{minimum}–{maximum}"

section = [
    START,
    "## Security Verification",
    "",
    (
        f"VirusTotal completed **{len(results)} distinct byte objects** covering "
        f"**{len(associations)} exact release associations**: "
        f"{len(archive_rows)} downloadable archives, {len(member_rows)} archive members, "
        f"and {len(asset_rows)} independently extracted UI-pack assets."
    ),
    "",
    (
        f"Every object returned **0 malicious and 0 suspicious** verdicts with "
        f"{engine_range} decisive engine results (required minimum: {policy})."
    ),
    (
        f"Microsoft returned a decisive clean verdict for all "
        f"{len(executable_paths)} executable objects."
    ),
    "",
    (
        "Durable public evidence: "
        f"[associations]({asset_base}/virustotal-associations.tsv), "
        f"[exact scan set]({asset_base}/virustotal-scan-set.tsv), "
        f"[per-object results and report links]({asset_base}/virustotal-results.tsv), "
        f"[evidence checksums]({asset_base}/virustotal-evidence-checksums.txt)."
    ),
    "",
    "| Downloadable archive | SHA-256 | Engines | VirusTotal |",
    "|---|---|---:|---|",
]
for association in sorted(archive_rows, key=lambda row: row["archive"]):
    result = results_by_path[association["scan_path"]]
    sha256 = result["sha256"]
    section.append(
        f"| `{association['archive']}` | `{sha256}` | "
        f"{result['completed_engines']} | [0 detections]({result['virustotal_url']}) |"
    )
section.append(END)
replacement = "\n".join(section)

current = current_path.read_text(encoding="utf-8")
start_count = current.count(START)
end_count = current.count(END)
if start_count != end_count or start_count > 1:
    fail("existing release notes contain malformed verification markers")
if start_count == 1:
    if current.index(START) >= current.index(END):
        fail("existing release-note verification markers are reversed")
    pattern = re.compile(re.escape(START) + r".*?" + re.escape(END), re.DOTALL)
    updated, replacements = pattern.subn(replacement, current, count=1)
    if replacements != 1:
        fail("existing release-note verification section could not be replaced")
else:
    updated = current.rstrip() + ("\n\n" if current.strip() else "") + replacement + "\n"
updated_path.write_text(updated, encoding="utf-8")
PY

gh release edit "$VERSION" \
  --notes-file "$WORK/updated.md" --repo "$GITHUB_REPOSITORY"

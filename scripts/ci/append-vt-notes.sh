#!/usr/bin/env bash
# Publish the already-completed candidate scans and tuple-local selections.
set -euo pipefail

: "${GH_TOKEN:?append-vt-notes: GH_TOKEN is required}"
: "${VERSION:?append-vt-notes: VERSION is required}"
: "${GITHUB_REPOSITORY:?append-vt-notes: GITHUB_REPOSITORY is required}"

VT_CANDIDATES="${VT_CANDIDATES:-release-candidates.tsv}"
VT_RESULTS_PATH="${VT_RESULTS_PATH:-virustotal-candidate-results.tsv}"
RELEASE_SELECTION="${RELEASE_SELECTION:-release-selection.tsv}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/cbm-vt-notes.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

gh release view "$VERSION" --json body --jq '.body // ""' \
  --repo "$GITHUB_REPOSITORY" > "$WORK/current.md"

python3 - "$VT_CANDIDATES" "$VT_RESULTS_PATH" "$RELEASE_SELECTION" \
  "$WORK/current.md" "$WORK/updated.md" "$GITHUB_REPOSITORY" "$VERSION" <<'PY'
from __future__ import annotations

import csv
import pathlib
import re
import sys
import urllib.parse


START = "<!-- cbm-security-verification:start -->"
END = "<!-- cbm-security-verification:end -->"
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
CANDIDATE_FIELDS = (
    "target", "variant", "relative_path", "source_sha256", "pre_sign_sha256",
    "sha256", "size", "format", "architecture", "linkage", "transform",
    "signature", "strip_tool", "strip_version", "pair_verification", "scan_path",
)
RESULT_FIELDS = (
    "scan_path", "sha256", "size", "association_count", "completed_engines",
    "total_engines", "malicious", "suspicious", "analysis_id",
    "microsoft_category", "microsoft_result", "policy_classification",
    "microsoft_engine_version", "microsoft_engine_update", "virustotal_url",
)
SELECTION_FIELDS = (
    "target", "selected_variant", "selected_path", "selected_sha256",
    "selected_size", "decision", "unstripped_sha256", "unstripped_scan_path",
    "unstripped_classification", "unstripped_analysis_id",
    "unstripped_virustotal_url",
    "debug_stripped_sha256", "debug_stripped_scan_path",
    "debug_stripped_classification", "debug_stripped_analysis_id",
    "debug_stripped_virustotal_url",
    "stripped_sha256", "stripped_scan_path",
    "stripped_classification", "stripped_analysis_id", "stripped_virustotal_url",
)
SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def fail(message: str) -> None:
    raise SystemExit(f"append-vt-notes: {message}")


def read_tsv(path: pathlib.Path, marker: str, fields: tuple[str, ...]):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 16 * 1024 * 1024:
        fail(f"missing, unsafe or oversized evidence file: {path}")
    text = path.read_text(encoding="utf-8")
    if "\x00" in text or "\r" in text:
        fail(f"forbidden control bytes in evidence file: {path}")
    lines = text.splitlines()
    if not lines or lines[0] != f"# {marker}":
        fail(f"wrong evidence marker in {path}")
    metadata: dict[str, str] = {}
    cursor = 1
    while cursor < len(lines) and lines[cursor].startswith("# "):
        key, separator, value = lines[cursor][2:].partition("=")
        if not separator or not key or not value or key in metadata:
            fail(f"malformed evidence metadata in {path}")
        metadata[key] = value
        cursor += 1
    reader = csv.DictReader(lines[cursor:], delimiter="\t")
    if tuple(reader.fieldnames or ()) != fields:
        fail(f"unexpected TSV header in {path}")
    rows = list(reader)
    if any(None in row or any(row.get(field) is None for field in fields) for row in rows):
        fail(f"missing or surplus TSV cells in {path}")
    return metadata, rows


candidates_path = pathlib.Path(sys.argv[1])
results_path = pathlib.Path(sys.argv[2])
selection_path = pathlib.Path(sys.argv[3])
current_path = pathlib.Path(sys.argv[4])
updated_path = pathlib.Path(sys.argv[5])
repository = sys.argv[6]
version = sys.argv[7]
if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) is None or not version:
    fail("unsafe repository or version")

candidate_meta, candidates = read_tsv(
    candidates_path, "cbm-release-candidates-v1", CANDIDATE_FIELDS
)
result_meta, results = read_tsv(
    results_path, "cbm-virustotal-results-v2", RESULT_FIELDS
)
selection_meta, selections = read_tsv(
    selection_path, "cbm-release-selection-v1", SELECTION_FIELDS
)
expected_pairs = [(target, variant) for target in TARGETS for variant in VARIANTS]
if candidate_meta != {"targets": str(len(TARGETS)), "candidates": str(len(TARGETS) * len(VARIANTS))}:
    fail("candidate metadata does not bind the canonical target/variant matrix")
if [(row["target"], row["variant"]) for row in candidates] != expected_pairs:
    fail("candidate rows are not the canonical target/variant matrix")
expected_scans = str(len(TARGETS) * len(VARIANTS))
if result_meta.get("scan_objects") != expected_scans or result_meta.get("associations") != expected_scans:
    fail("VirusTotal results do not cover every candidate")
if selection_meta != {"policy": "virustotal-v2", "targets": str(len(TARGETS)), "candidates": str(len(TARGETS) * len(VARIANTS))}:
    fail("selection was not produced by the scanned release policy")
if [row["target"] for row in selections] != list(TARGETS):
    fail("selection rows are not the canonical target matrix")

candidate_by_pair = {(row["target"], row["variant"]): row for row in candidates}
candidate_by_path = {row["scan_path"]: row for row in candidates}
expected_candidates = len(TARGETS) * len(VARIANTS)
if len(candidate_by_pair) != expected_candidates or len(candidate_by_path) != expected_candidates:
    fail("candidate evidence contains duplicates")

results_by_path: dict[str, dict[str, str]] = {}
for result in results:
    candidate = candidate_by_path.get(result["scan_path"])
    if candidate is None or result["scan_path"] in results_by_path:
        fail("VirusTotal evidence contains an unknown or duplicate candidate")
    for field in ("size", "completed_engines", "total_engines", "malicious", "suspicious"):
        if not result[field].isdigit():
            fail(f"malformed numeric result for {result['scan_path']}")
    if (
        result["sha256"] != candidate["sha256"]
        or result["size"] != candidate["size"]
        or result["association_count"] != "1"
        or SHA256.fullmatch(result["sha256"]) is None
        or result["virustotal_url"]
        != f"https://www.virustotal.com/gui/file/{result['sha256']}/detection"
    ):
        fail(f"VirusTotal result is not bound to candidate bytes: {result['scan_path']}")
    completed = int(result["completed_engines"])
    total = int(result["total_engines"])
    if completed < 50 or total < completed:
        fail(f"incomplete VirusTotal result: {result['scan_path']}")
    classification = result["policy_classification"]
    clean = (
        classification == "clean"
        and result["malicious"] == "0"
        and result["suspicious"] == "0"
        and result["microsoft_category"] in {"undetected", "harmless"}
        and result["microsoft_result"] == ""
    )
    microsoft_ml = (
        classification == "microsoft-ml"
        and result["malicious"] == "1"
        and result["suspicious"] == "0"
        and result["microsoft_category"] == "malicious"
        and result["microsoft_result"].endswith("!ml")
    )
    if not clean and not microsoft_ml:
        fail(f"blocked VirusTotal result reached release notes: {result['scan_path']}")
    if not result["analysis_id"] or not result["microsoft_engine_version"] or not result["microsoft_engine_update"]:
        fail(f"incomplete VirusTotal evidence: {result['scan_path']}")
    results_by_path[result["scan_path"]] = result
if set(results_by_path) != set(candidate_by_path):
    fail("VirusTotal results are missing candidates")

selection_by_target: dict[str, dict[str, str]] = {}
for selection in selections:
    target = selection["target"]
    pair = {variant: candidate_by_pair[target, variant] for variant in VARIANTS}
    pair_results = {variant: results_by_path[pair[variant]["scan_path"]] for variant in VARIANTS}
    classes = {v: pair_results[v]["policy_classification"] for v in VARIANTS}
    order = ("stripped", "debug-stripped", "unstripped")
    clean = [v for v in order if classes[v] == "clean"]
    expected_variant = clean[0] if clean else "stripped"
    selected = pair[expected_variant]
    if (
        selection["selected_variant"] != expected_variant
        or selection["selected_sha256"] != selected["sha256"]
        or selection["selected_size"] != selected["size"]
    ):
        fail(f"selection contradicts candidate results: {target}")
    for variant in VARIANTS:
        result = pair_results[variant]
        if (
            selection[f"{FIELD_KEY[variant]}_sha256"] != pair[variant]["sha256"]
            or selection[f"{FIELD_KEY[variant]}_scan_path"] != pair[variant]["scan_path"]
            or selection[f"{FIELD_KEY[variant]}_classification"] != result["policy_classification"]
            or selection[f"{FIELD_KEY[variant]}_analysis_id"] != result["analysis_id"]
            or selection[f"{FIELD_KEY[variant]}_virustotal_url"] != result["virustotal_url"]
        ):
            fail(f"selection evidence is not bound to both candidate verdicts: {target}")
    selection_by_target[target] = selection

asset_base = (
    f"https://github.com/{repository}/releases/download/"
    f"{urllib.parse.quote(version, safe='')}"
)


# Release notes report the SHIPPED binary and nothing else.
#
# Several candidates per product are scanned so the selector has an alternative
# when an opaque classifier flags one of them, but a reader installing cbm cares
# about the bytes they receive, not about the ones we discarded. The rejected
# candidates' verdicts stay in the published evidence TSVs for anyone auditing
# the selection, and they matter to US in development as a signal; they are
# noise in a changelog.
def shipped_result(target: str) -> dict:
    selection = selection_by_target[target]
    variant = selection["selected_variant"]
    return results_by_path[candidate_by_pair[target, variant]["scan_path"]]


def verdict_cell(target: str) -> str:
    result = shipped_result(target)
    label = "clean" if result["policy_classification"] == "clean" else "Microsoft `!ml`"
    return f"[{label}]({result['virustotal_url']})"


shipped = [shipped_result(target) for target in TARGETS]
ml_count = sum(result["policy_classification"] == "microsoft-ml" for result in shipped)
engine_counts = [int(result["completed_engines"]) for result in shipped]
engine_range = str(min(engine_counts)) if min(engine_counts) == max(engine_counts) else f"{min(engine_counts)}–{max(engine_counts)}"
section = [
    START,
    "## Security Verification",
    "",
    (
        f"Every binary published below was scanned by VirusTotal before smoke and soak "
        f"testing, and the verdict for the exact shipped bytes is linked per product "
        f"(decisive engines: {engine_range})."
    ),
    (
        "Every shipped binary was clean."
        if ml_count == 0
        else f"**{ml_count} shipped binary/binaries** carried only the documented single Microsoft machine-learning `!ml` result; no other decisive engine reported malicious or suspicious."
    ),
    "",
    "| Product | Shipped binary | VirusTotal verdict |",
    "|---|---|---|",
]
for target in TARGETS:
    selection = selection_by_target[target]
    section.append(
        f"| `{target}` | `{selection['selected_sha256']}` | {verdict_cell(target)} |"
    )
section.extend(
    [
        "",
        (
            "Selection is tuple-local and defaults to stripped. The selected executable SHA-256 "
            "was verified again after packaging; archive containers were not redundantly submitted "
            "to VirusTotal. Their hashes remain available in `checksums.txt`."
        ),
        "",
        (
            "Durable evidence: "
            f"[candidate provenance]({asset_base}/release-candidates.tsv), "
            f"[candidate VirusTotal results]({asset_base}/virustotal-candidate-results.tsv), "
            f"[selection decisions]({asset_base}/release-selection.tsv)."
        ),
        END,
    ]
)
replacement = "\n".join(section)
current = current_path.read_text(encoding="utf-8")
if current.count(START) != current.count(END) or current.count(START) > 1:
    fail("existing release notes contain malformed verification markers")
if START in current:
    if current.index(START) >= current.index(END):
        fail("existing verification markers are reversed")
    updated, count = re.subn(
        re.escape(START) + r".*?" + re.escape(END),
        replacement,
        current,
        count=1,
        flags=re.DOTALL,
    )
    if count != 1:
        fail("could not replace existing verification section")
else:
    updated = current.rstrip() + ("\n\n" if current.strip() else "") + replacement + "\n"
updated_path.write_text(updated, encoding="utf-8")
PY

gh release edit "$VERSION" --notes-file "$WORK/updated.md" --repo "$GITHUB_REPOSITORY"

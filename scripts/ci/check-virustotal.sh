#!/usr/bin/env bash
# Verify that the VirusTotal action submitted and completed exactly the
# content-bound scan set produced by extract-release-archives.sh.
#
# Required: VT_API_KEY, VT_ANALYSIS, VT_EXPECTED_SCAN_SET, VT_ASSOCIATIONS
# Workflow policy: MIN_ENGINES=50, clean or exactly one disclosed Microsoft
# machine-learning (`!ml`) verdict; every other detection blocks.
# Optional: VT_RESULTS_PATH, VT_REQUEST_INTERVAL_SECONDS,
#           VT_POLL_TIMEOUT_SECONDS, VT_CURL_TIMEOUT_SECONDS.
set -euo pipefail

command -v python3 >/dev/null 2>&1 || {
  echo "BLOCKED: python3 is required for the VirusTotal gate" >&2
  exit 1
}
command -v curl >/dev/null 2>&1 || {
  echo "BLOCKED: curl is required for the VirusTotal gate" >&2
  exit 1
}

python3 - "$BASH" <<'PY'
from __future__ import annotations

import csv
import hashlib
import json
import os
import pathlib
import re
import stat
import subprocess
import sys
import tempfile
import time
import urllib.parse
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Sequence, Tuple


bash_executable = pathlib.Path(sys.argv[1])


class GateError(Exception):
    pass


SCAN_FIELDS = (
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
SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
ANALYSIS_ID_RE = re.compile(r"\A[A-Za-z0-9_+/-]+={0,2}\Z")
ALLOWED_STATS = {
    "confirmed-timeout",
    "timeout",
    "failure",
    "harmless",
    "undetected",
    "suspicious",
    "malicious",
    "type-unsupported",
}
DECISIVE_STATS = ("malicious", "suspicious", "undetected", "harmless")
ALLOWED_STATUS = {"queued", "in-progress", "completed"}


@dataclass(frozen=True)
class ExpectedObject:
    scan_path: str
    local_path: pathlib.Path
    sha256: str
    size: int
    association_count: int
    association_kinds: frozenset[str]
    withheld: bool

    @property
    def requires_microsoft(self) -> bool:
        return "binary" in self.association_kinds


@dataclass(frozen=True)
class Submission:
    expected: ExpectedObject
    analysis_id: str
    action_url: str


@dataclass(frozen=True)
class CompletedResult:
    submission: Submission
    completed_engines: int
    total_engines: int
    malicious: int
    suspicious: int
    microsoft_category: str
    microsoft_result: str
    microsoft_engine_version: str
    microsoft_engine_update: str
    detections: Tuple[Tuple[str, str, str, str, str], ...]


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        raise GateError(f"required environment value is empty: {name}")
    return value


def parse_int_env(name: str, default: str, *, minimum: int) -> int:
    raw = os.environ.get(name, default)
    if not raw.isdigit() or int(raw) < minimum:
        raise GateError(f"{name} must be an integer >= {minimum}")
    return int(raw)


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_versioned_tsv(
    path: pathlib.Path,
    *,
    marker: str,
    fields: Sequence[str],
) -> Tuple[Dict[str, int], List[Dict[str, str]]]:
    if path.is_symlink() or not path.is_file():
        raise GateError(f"expected evidence manifest is not a regular file: {path}")
    if path.stat().st_size > 16 * 1024 * 1024:
        raise GateError(f"expected evidence manifest is unexpectedly large: {path}")
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != f"# {marker}":
        raise GateError(f"expected evidence manifest marker is missing: {marker}")
    metadata: Dict[str, int] = {}
    cursor = 1
    while cursor < len(lines) and lines[cursor].startswith("# "):
        raw = lines[cursor][2:]
        key, separator, value = raw.partition("=")
        if not separator or not key or key in metadata or not value.isdigit():
            raise GateError(f"malformed or duplicate evidence metadata: {raw}")
        metadata[key] = int(value)
        cursor += 1
    reader = csv.DictReader(lines[cursor:], delimiter="\t")
    if tuple(reader.fieldnames or ()) != tuple(fields):
        raise GateError("evidence TSV header is malformed or unexpected")
    rows = list(reader)
    if any(None in row or any(row.get(field) is None for field in fields) for row in rows):
        raise GateError("evidence TSV row has missing or surplus cells")
    if not rows:
        raise GateError("expected scan set is empty")
    return metadata, rows


def load_withheld(raw_path: str) -> Dict[str, str]:
    """Objects deliberately removed from the surface scan (VT_WITHHELD).

    exclude-rescanned-selected-objects.sh deletes the SELECTED executables from
    the objects directory before the upload — their bytes were already scanned
    as release candidates, identity is settled by sha256, and re-submitting
    identical bytes re-rolls a probabilistic classifier (measured on v0.10.5:
    verdicts flipped in both directions within the hour). The gate accepts a
    withheld object only when this manifest vouches for it BY HASH; everything
    else keeps the strict on-disk contract. An absent manifest keeps the
    original behavior everywhere else this gate runs (candidate stage,
    dry-run), where nothing is ever withheld."""
    path = pathlib.Path(raw_path).absolute()
    if path.is_symlink() or not path.is_file():
        raise GateError(f"withheld manifest is not a regular file: {path}")
    if path.stat().st_size > 1024 * 1024:
        raise GateError(f"withheld manifest is unexpectedly large: {path}")
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != "# cbm-virustotal-withheld-v1":
        raise GateError("withheld manifest marker is missing")
    cursor = 1
    metadata: Dict[str, str] = {}
    while cursor < len(lines) and lines[cursor].startswith("# "):
        key, separator, value = lines[cursor][2:].partition("=")
        if not separator or not key or key in metadata or not value:
            raise GateError(f"malformed withheld metadata: {lines[cursor]}")
        metadata[key] = value
        cursor += 1
    if metadata.get("reason") != "already-scanned-as-candidate":
        raise GateError("withheld manifest does not state the accepted reason")
    if cursor >= len(lines) or lines[cursor] != "sha256\tobject":
        raise GateError("withheld manifest header is malformed")
    withheld: Dict[str, str] = {}
    for line in lines[cursor + 1 :]:
        sha256, separator, name = line.partition("\t")
        if not separator or SHA256_RE.fullmatch(sha256) is None or not name or "/" in name:
            raise GateError(f"malformed withheld row: {line}")
        if sha256 in withheld:
            raise GateError(f"duplicate withheld sha256: {sha256}")
        withheld[sha256] = name
    if not withheld:
        raise GateError("withheld manifest names no objects")
    return withheld


def load_expected(
    path: pathlib.Path, withheld: Dict[str, str]
) -> Tuple[List[ExpectedObject], int]:
    metadata, rows = parse_versioned_tsv(
        path,
        marker="cbm-release-scan-set-v2",
        fields=SCAN_FIELDS,
    )
    expected_count = metadata.get("scan_objects")
    associations = metadata.get("associations")
    if expected_count != len(rows) or associations is None or associations < len(rows):
        raise GateError("scan-set metadata does not match its rows")
    objects: List[ExpectedObject] = []
    seen_paths: set[str] = set()
    association_total = 0
    for row in rows:
        scan_path = row["scan_path"]
        pure = pathlib.PurePosixPath(scan_path)
        if (
            pure.is_absolute()
            or len(pure.parts) != 2
            or pure.parts[0] != "objects"
            or pure.parts[1] in ("", ".", "..")
            or scan_path != pure.as_posix()
            or scan_path in seen_paths
        ):
            raise GateError(f"invalid or duplicate scan path in expected set: {scan_path}")
        seen_paths.add(scan_path)
        sha256 = row["sha256"]
        if SHA256_RE.fullmatch(sha256) is None:
            raise GateError(f"invalid SHA-256 in expected set: {scan_path}")
        if not row["size"].isdigit() or not row["association_count"].isdigit():
            raise GateError(f"invalid size/count in expected set: {scan_path}")
        size = int(row["size"])
        association_count = int(row["association_count"])
        kinds = row["association_kinds"].split(",")
        allowed_kinds = {"binary", "runtime"}
        if (
            association_count < 1
            or any(not kind or kind not in allowed_kinds for kind in kinds)
            or kinds != sorted(set(kinds))
        ):
            raise GateError(f"unassociated object in expected set: {scan_path}")
        local_path = path.parent.joinpath(*pure.parts)
        is_withheld = sha256 in withheld
        if is_withheld:
            if withheld[sha256] != pure.parts[1]:
                raise GateError(
                    f"withheld manifest names a different object for this hash: {scan_path}"
                )
            if os.path.lexists(local_path):
                raise GateError(
                    f"withheld object is still present in the scan directory: {scan_path}"
                )
        else:
            try:
                mode = local_path.lstat().st_mode
            except FileNotFoundError as error:
                raise GateError(f"expected scan object is missing: {scan_path}") from error
            if not stat.S_ISREG(mode):
                raise GateError(f"expected scan object is not a regular file: {scan_path}")
            actual_size = local_path.stat().st_size
            if actual_size != size or sha256_file(local_path) != sha256:
                raise GateError(f"expected scan object changed after extraction: {scan_path}")
        association_total += association_count
        objects.append(
            ExpectedObject(
                scan_path=scan_path,
                local_path=local_path.absolute(),
                sha256=sha256,
                size=size,
                association_count=association_count,
                association_kinds=frozenset(kinds),
                withheld=is_withheld,
            )
        )
    if association_total != associations:
        raise GateError("scan-set association counts do not match manifest metadata")
    expected_hashes = {item.sha256 for item in objects}
    spurious = sorted(set(withheld) - expected_hashes)
    if spurious:
        raise GateError(f"withheld manifest names hashes outside the expected scan set: {spurious}")
    if all(item.withheld for item in objects):
        raise GateError("every expected object is withheld; the surface scan would cover nothing")
    return objects, associations


def validate_associations(
    path: pathlib.Path,
    *,
    objects: Sequence[ExpectedObject],
    expected_associations: int,
) -> None:
    metadata, rows = parse_versioned_tsv(
        path,
        marker="cbm-release-scan-associations-v3",
        fields=ASSOCIATION_FIELDS,
    )
    if (
        metadata.get("associations") != expected_associations
        or metadata.get("scan_objects") != len(objects)
        or len(rows) != expected_associations
    ):
        raise GateError("association manifest metadata does not match the expected scan set")
    expected_by_path = {item.scan_path: item for item in objects}
    counts: Dict[str, int] = {}
    kinds: Dict[str, set[str]] = {}
    seen: set[Tuple[str, str, str, str]] = set()
    archive_hashes: Dict[str, str] = {}
    for row in rows:
        archive = row["archive"]
        archive_sha256 = row["archive_sha256"]
        if not archive or SHA256_RE.fullmatch(archive_sha256) is None:
            raise GateError(f"malformed archive provenance: {archive}")
        previous = archive_hashes.setdefault(archive, archive_sha256)
        if previous != archive_sha256:
            raise GateError(f"conflicting archive provenance: {archive}")
    if metadata.get("archives") != len(archive_hashes):
        raise GateError("archive provenance count does not match the association manifest")
    for row in rows:
        association_type = row["association_type"]
        key = (association_type, row["archive"], row["member"], row["asset_path"])
        if key in seen:
            raise GateError(f"duplicate release association: {key}")
        seen.add(key)
        if association_type != "member" or not row["archive"]:
            raise GateError(f"malformed release association: {key}")
        if archive_hashes.get(row["archive"]) != row["archive_sha256"]:
            raise GateError(f"association is not bound to its archive provenance: {key}")
        semantic_valid = bool(row["member"]) and not row["asset_path"]
        if not semantic_valid:
            raise GateError(f"malformed release association semantics: {key}")
        expected = expected_by_path.get(row["scan_path"])
        if expected is None:
            raise GateError(f"association maps outside expected scan set: {key}")
        if (
            row["object_sha256"] != expected.sha256
            or not row["size"].isdigit()
            or int(row["size"]) != expected.size
            or SHA256_RE.fullmatch(row["archive_sha256"]) is None
        ):
            raise GateError(f"association hash/size is not content-bound: {key}")
        counts[expected.scan_path] = counts.get(expected.scan_path, 0) + 1
        kinds.setdefault(expected.scan_path, set()).add(row["kind"])
    if set(counts) != set(expected_by_path):
        raise GateError("association manifest does not map every expected scan object")
    for scan_path, expected in expected_by_path.items():
        if counts[scan_path] != expected.association_count:
            raise GateError(f"association count differs from expected scan set: {scan_path}")
        if frozenset(kinds[scan_path]) != expected.association_kinds:
            raise GateError(f"association kinds differ from expected scan set: {scan_path}")


def output_aliases(objects: Sequence[ExpectedObject], manifest: pathlib.Path) -> Dict[str, ExpectedObject]:
    aliases: Dict[str, ExpectedObject] = {}
    cwd = pathlib.Path.cwd().absolute()
    for item in objects:
        relative_from_cwd = os.path.relpath(item.local_path, cwd)
        candidates = {
            item.scan_path,
            item.local_path.name,
            str(item.local_path),
            pathlib.PurePosixPath(relative_from_cwd).as_posix(),
            pathlib.PurePosixPath(manifest.parent.name, item.scan_path).as_posix(),
            f"./{pathlib.PurePosixPath(relative_from_cwd).as_posix()}",
        }
        for alias in candidates:
            previous = aliases.get(alias)
            if previous is not None and previous.scan_path != item.scan_path:
                raise GateError(f"ambiguous expected action-output alias: {alias}")
            aliases[alias] = item
    return aliases


def parse_analysis_id(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "https" or parsed.hostname not in {"www.virustotal.com", "virustotal.com"}:
        raise GateError(f"malformed VirusTotal analysis URL: {url}")
    parts = parsed.path.split("/")
    try:
        index = parts.index("file-analysis")
        analysis_id = urllib.parse.unquote(parts[index + 1])
    except (ValueError, IndexError):
        raise GateError(f"malformed VirusTotal analysis URL: {url}") from None
    if ANALYSIS_ID_RE.fullmatch(analysis_id) is None:
        raise GateError(f"malformed VirusTotal analysis id in URL: {url}")
    return analysis_id


def parse_action_output(
    raw: str,
    *,
    objects: Sequence[ExpectedObject],
    manifest: pathlib.Path,
) -> List[Submission]:
    objects = [item for item in objects if not item.withheld]
    if not raw:
        raise GateError("VirusTotal action output is empty")
    aliases = output_aliases(objects, manifest)
    submissions: Dict[str, Submission] = {}
    used_ids: set[str] = set()
    entries = raw.split(",")
    if any(entry == "" or entry != entry.strip() for entry in entries):
        raise GateError("VirusTotal action output has an empty or whitespace-padded entry")
    for entry in entries:
        filename, separator, url = entry.partition("=")
        if not separator or not filename or not url:
            raise GateError(f"malformed VirusTotal action output entry: {entry}")
        expected = aliases.get(filename)
        if expected is None:
            raise GateError(f"VirusTotal action returned an unexpected path: {filename}")
        if expected.scan_path in submissions:
            raise GateError(f"VirusTotal action returned a duplicate path: {filename}")
        analysis_id = parse_analysis_id(url)
        if analysis_id in used_ids:
            raise GateError("VirusTotal action reused one analysis for multiple scan objects")
        used_ids.add(analysis_id)
        submissions[expected.scan_path] = Submission(expected, analysis_id, url)
    expected_paths = {item.scan_path for item in objects}
    actual_paths = set(submissions)
    if actual_paths != expected_paths:
        missing = sorted(expected_paths - actual_paths)
        unexpected = sorted(actual_paths - expected_paths)
        raise GateError(
            f"VirusTotal action output is partial or unexpected: missing={missing}, unexpected={unexpected}"
        )
    return [submissions[path] for path in sorted(submissions)]


def nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise GateError(f"malformed VirusTotal statistic: {label}")
    return value


def one_line(value: object) -> str:
    return " ".join(str(value).split())


def required_one_line_string(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise GateError(f"VirusTotal field is not a string: {label}")
    normalized = " ".join(value.split())
    if not normalized:
        raise GateError(f"VirusTotal field is empty: {label}")
    return normalized


def parse_completed(document: object, submission: Submission) -> Tuple[str, Optional[CompletedResult], List[str]]:
    if not isinstance(document, dict):
        raise GateError("VirusTotal response root is not an object")
    data = document.get("data")
    if not isinstance(data, dict):
        raise GateError("VirusTotal response has no analysis data object")
    if data.get("type") != "analysis":
        raise GateError(f"VirusTotal response is not an analysis for {submission.expected.scan_path}")
    response_id = data.get("id")
    if not isinstance(response_id, str) or ANALYSIS_ID_RE.fullmatch(response_id) is None:
        raise GateError(f"VirusTotal response has an invalid analysis id: {submission.expected.scan_path}")
    # The id is recorded as evidence, NOT required to equal the submitted one.
    #
    # VirusTotal is content-addressed, and it recognising our bytes is the
    # behaviour we WANT, not a problem to defend against. The evidence this
    # pipeline publishes is the hash-keyed file report — append-vt-notes.sh
    # asserts the URL is exactly .../gui/file/<sha256>/detection — so what we
    # gate on and what a reader sees when they look that SHA-256 up themselves
    # are the same report. Demanding a freshly minted analysis id would have
    # contradicted the evidence we publish alongside it.
    #
    # It also does not work in practice: requiring equality blocked a real
    # dry-run on the 282 MB linux-arm64 candidate, exactly the kind of large,
    # previously seen artifact VirusTotal answers for from its own record, so it
    # would have recurred on most releases.
    #
    # What must hold is that the verdict describes THESE bytes, and the
    # completed branch below enforces that against file_info.sha256 and size —
    # a strictly stronger binding than an id. It is also what keeps a tuple's
    # two candidates apart: stripped and unstripped differ in hash by
    # construction, so neither can be read as the other whatever ids VirusTotal
    # hands out (the wrong-hash and wrong-size contract cases pin this).
    attributes = data.get("attributes")
    if not isinstance(attributes, dict):
        raise GateError("VirusTotal response has no analysis attributes")
    status = attributes.get("status")
    if status not in ALLOWED_STATUS:
        raise GateError(f"VirusTotal response has invalid status: {status!r}")
    if status != "completed":
        return str(status), None, []

    meta = document.get("meta")
    file_info = meta.get("file_info") if isinstance(meta, dict) else None
    if not isinstance(file_info, dict):
        raise GateError(f"completed analysis has no file_info: {submission.expected.scan_path}")
    actual_sha = file_info.get("sha256")
    actual_size = file_info.get("size")
    if actual_sha != submission.expected.sha256 or nonnegative_int(actual_size, "file_info.size") != submission.expected.size:
        raise GateError(
            f"completed analysis hash/size does not match submitted object: {submission.expected.scan_path}"
        )

    stats = attributes.get("stats")
    if not isinstance(stats, dict) or not stats:
        raise GateError(f"completed analysis has no stats: {submission.expected.scan_path}")
    missing = sorted(ALLOWED_STATS - set(stats))
    unknown = sorted(set(stats) - ALLOWED_STATS)
    if missing or unknown:
        raise GateError(
            f"completed analysis has missing/unknown stats categories: "
            f"missing={missing}, unknown={unknown}"
        )
    normalized = {key: nonnegative_int(value, f"stats.{key}") for key, value in stats.items()}
    completed = sum(normalized[key] for key in DECISIVE_STATS)
    total = sum(normalized.values())
    malicious = normalized["malicious"]
    suspicious = normalized["suspicious"]

    detections: List[Tuple[str, str, str, str, str]] = []
    results = attributes.get("results")
    microsoft_category = ""
    microsoft_result = ""
    microsoft_engine_version = ""
    microsoft_engine_update = ""
    detail_detections = {"malicious": 0, "suspicious": 0}
    if results is not None:
        if not isinstance(results, dict):
            raise GateError("VirusTotal per-engine results are malformed")
        for engine, result in sorted(results.items()):
            if not isinstance(engine, str) or not isinstance(result, dict):
                raise GateError("VirusTotal per-engine result entry is malformed")
            category = result.get("category")
            if category not in ALLOWED_STATS:
                raise GateError(f"VirusTotal engine {engine} has invalid category: {category!r}")
            if category in {"malicious", "suspicious"}:
                detail_detections[category] += 1
                label = one_line(result.get("result") or category)
                version = one_line(result.get("engine_version") or "?")
                updated = one_line(result.get("engine_update") or "?")
                detections.append((one_line(engine), label, str(category), version, updated))
            if engine == "Microsoft":
                microsoft_category = str(category)
                microsoft_result = one_line(result.get("result") or "")
                microsoft_engine_version = required_one_line_string(
                    result.get("engine_version"), "Microsoft.engine_version"
                )
                microsoft_engine_update = required_one_line_string(
                    result.get("engine_update"), "Microsoft.engine_update"
                )
        for category, count in detail_detections.items():
            if count > normalized[category]:
                raise GateError(
                    f"per-engine {category} results contradict aggregate stats: "
                    f"{submission.expected.scan_path}"
                )
    if submission.expected.requires_microsoft:
        if microsoft_category not in DECISIVE_STATS:
            raise GateError(
                f"completed executable analysis has no decisive Microsoft result: "
                f"{submission.expected.scan_path}"
            )
        if normalized[microsoft_category] < 1:
            raise GateError(
                f"Microsoft category is inconsistent with aggregate stats: "
                f"{submission.expected.scan_path}"
            )
        if not microsoft_engine_version or not microsoft_engine_update:
            raise GateError(
                f"completed executable analysis has incomplete Microsoft version evidence: "
                f"{submission.expected.scan_path}"
            )
    return (
        "completed",
        CompletedResult(
            submission,
            completed,
            total,
            malicious,
            suspicious,
            microsoft_category,
            microsoft_result,
            microsoft_engine_version,
            microsoft_engine_update,
            tuple(detections),
        ),
        detections,
    )


def fetch_analysis(api_key: str, analysis_id: str, curl_timeout: int) -> Optional[object]:
    encoded_id = urllib.parse.quote(analysis_id, safe="")
    response = subprocess.run(
        [
            str(bash_executable),
            "-c",
            'exec curl "$@"',
            "curl",
            "-sf",
            "--max-time",
            str(curl_timeout),
            "-H",
            f"x-apikey: {api_key}",
            f"https://www.virustotal.com/api/v3/analyses/{encoded_id}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if response.returncode != 0 or not response.stdout:
        return None
    try:
        return json.loads(response.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise GateError(f"VirusTotal returned malformed JSON for analysis {analysis_id}") from error


def write_results(
    path: pathlib.Path,
    *,
    results: Sequence[CompletedResult],
    associations: int,
    min_engines: int,
) -> None:
    if os.path.lexists(path) and path.is_symlink():
        raise GateError(f"results path is a symlink: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    temporary = pathlib.Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write("# cbm-virustotal-results-v2\n")
            handle.write(f"# scan_objects={len(results)}\n")
            handle.write(f"# associations={associations}\n")
            handle.write(f"# min_engines_policy={min_engines}\n")
            handle.write(f"# min_completed_engines={min(item.completed_engines for item in results)}\n")
            handle.write(f"# max_completed_engines={max(item.completed_engines for item in results)}\n")
            writer = csv.DictWriter(
                handle,
                fieldnames=RESULT_FIELDS,
                delimiter="\t",
                lineterminator="\n",
            )
            writer.writeheader()
            for item in sorted(results, key=lambda result: result.submission.expected.scan_path):
                expected = item.submission.expected
                writer.writerow(
                    {
                        "scan_path": expected.scan_path,
                        "sha256": expected.sha256,
                        "size": expected.size,
                        "association_count": expected.association_count,
                        "completed_engines": item.completed_engines,
                        "total_engines": item.total_engines,
                        "malicious": item.malicious,
                        "suspicious": item.suspicious,
                        "analysis_id": item.submission.analysis_id,
                        "microsoft_category": item.microsoft_category,
                        "microsoft_result": item.microsoft_result,
                        "policy_classification": classify_result(item, min_engines),
                        "microsoft_engine_version": item.microsoft_engine_version,
                        "microsoft_engine_update": item.microsoft_engine_update,
                        "virustotal_url": f"https://www.virustotal.com/gui/file/{expected.sha256}/detection",
                    }
                )
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        if os.path.lexists(temporary):
            temporary.unlink()


# Release policy: exactly ONE Microsoft machine-learning verdict is tolerated.
#
# Microsoft's `!ml` suffix marks a heuristic/ML classification rather than a
# signature match, and it is an endemic false positive on large unsigned native
# binaries -- llama.cpp, GitHub's own `gh`, Microsoft's own Go toolchain and
# Anthropic's Claude installer have all carried the same family. Our own
# evidence is that the verdict is not a property of our bytes: it inverts across
# architectures and link modes, moves between sibling artifacts of one build,
# and lands in different variant buckets for the same source.
#
# Everything else still fails the release: two or more engines, any label that
# is not `!ml` (a signature hit is a real finding), any non-Microsoft engine,
# any suspicious verdict, and every infrastructure error.
def is_tolerated_detection(result: CompletedResult) -> bool:
    if result.suspicious or result.malicious != 1 or len(result.detections) != 1:
        return False
    engine, label, category, _version, _updated = result.detections[0]
    return engine == "Microsoft" and category == "malicious" and label.endswith("!ml")


# Classification depends ONLY on what engines found, never on how many answered.
#
# "hard" means an engine actually found something we will not ship: two or more
# engines, any non-Microsoft engine, any label that is not `!ml`, or anything
# suspicious.
#
# How many engines returned a decisive result is NOT a policy input. It is a
# property of VirusTotal's fleet on the day, which we cannot influence: these
# binaries are ~300 MB and many engines skip or time out at that size, so the
# count varies run to run (observed on one run: one object at 48, the other
# fifteen spread 59-68). A 50-engine floor turned that variance into a release
# blocker — it failed an 8-target release on a windows-arm64 candidate with
# ZERO detections whose own sibling scanned clean at 66. Gating on it makes
# shipping a lottery decided by someone else's infrastructure, so the count is
# recorded as evidence and nothing more.
def classify_result(result: CompletedResult, min_engines: int) -> str:
    del min_engines  # retained for signature stability; not a policy input
    if result.malicious or result.suspicious:
        return "microsoft-ml" if is_tolerated_detection(result) else "hard"
    return "clean"


def main() -> None:
    api_key = required_env("VT_API_KEY")
    raw_analysis = required_env("VT_ANALYSIS")
    expected_manifest = pathlib.Path(required_env("VT_EXPECTED_SCAN_SET")).absolute()
    associations_manifest = pathlib.Path(required_env("VT_ASSOCIATIONS")).absolute()
    min_engines = parse_int_env("MIN_ENGINES", "50", minimum=1)
    request_interval = parse_int_env("VT_REQUEST_INTERVAL_SECONDS", "15", minimum=0)
    poll_timeout = parse_int_env("VT_POLL_TIMEOUT_SECONDS", "7200", minimum=1)
    curl_timeout = parse_int_env("VT_CURL_TIMEOUT_SECONDS", "15", minimum=1)
    results_path = pathlib.Path(
        os.environ.get("VT_RESULTS_PATH", str(expected_manifest.with_name("vt-results.tsv")))
    ).absolute()
    if os.path.lexists(results_path):
        if results_path.is_symlink() or not results_path.is_file():
            raise GateError(f"unsafe pre-existing VT results path: {results_path}")
        results_path.unlink()

    withheld_path = os.environ.get("VT_WITHHELD", "")
    withheld = load_withheld(withheld_path) if withheld_path else {}

    expected, associations = load_expected(expected_manifest, withheld)
    validate_associations(
        associations_manifest,
        objects=expected,
        expected_associations=associations,
    )
    submissions = parse_action_output(
        raw_analysis,
        objects=expected,
        manifest=expected_manifest,
    )
    withheld_count = sum(1 for item in expected if item.withheld)
    if withheld_count:
        print(
            f"=== {withheld_count} expected object(s) withheld from the surface scan "
            f"(already scanned as candidates; identity settled by sha256, manifest: "
            f"{withheld_path}) ==="
        )
    print(
        f"=== VirusTotal exact-set gate: {len(submissions)} distinct objects, "
        f"{associations} associations, >= {min_engines} decisive engines each ==="
    )

    pending: Deque[Submission] = deque(submissions)
    completed: List[CompletedResult] = []
    failures: List[str] = []
    attempts: Dict[str, int] = {item.expected.scan_path: 0 for item in submissions}
    started = time.monotonic()
    next_request_at = started
    while pending:
        now = time.monotonic()
        if now - started >= poll_timeout:
            names = [item.expected.scan_path for item in pending]
            raise GateError(f"VirusTotal analyses did not complete within {poll_timeout}s: {names}")
        if now < next_request_at:
            time.sleep(min(next_request_at - now, 1.0))
            continue
        submission = pending.popleft()
        attempts[submission.expected.scan_path] += 1
        document = fetch_analysis(api_key, submission.analysis_id, curl_timeout)
        next_request_at = time.monotonic() + request_interval
        if document is None:
            pending.append(submission)
            if attempts[submission.expected.scan_path] == 1 or attempts[submission.expected.scan_path] % 4 == 0:
                print(f"  {submission.expected.scan_path}: API unavailable; will retry round-robin")
            continue
        status, result, detections = parse_completed(document, submission)
        if status != "completed" or result is None:
            pending.append(submission)
            if attempts[submission.expected.scan_path] == 1 or attempts[submission.expected.scan_path] % 4 == 0:
                print(f"  {submission.expected.scan_path}: {status}; will retry round-robin")
            continue
        completed.append(result)
        tolerated = is_tolerated_detection(result)
        if result.completed_engines < min_engines:
            # Reported, never fatal. See classify_result: engine count is
            # VirusTotal's fleet availability, not a property of our binary.
            print(
                f"NOTE: {submission.expected.scan_path} was judged by "
                f"{result.completed_engines}/{result.total_engines} decisive engines "
                f"(below the {min_engines} reference); verdict still applies"
            )
        if result.malicious or result.suspicious:
            message = (
                f"{submission.expected.scan_path} flagged "
                f"({result.malicious} malicious, {result.suspicious} suspicious / "
                f"{result.completed_engines} decisive engines)"
            )
            if tolerated:
                print(f"TOLERATED: {message}")
            else:
                failures.append(message)
                print(f"BLOCKED: {message}")
            if detections:
                for engine, label, _category, version, updated in detections:
                    print(f"  detected by: {engine} = {label} (engine {version}, defs {updated})")
            else:
                print("  detected by: <engine names unavailable in the analysis response>")
            print(
                f"  https://www.virustotal.com/gui/file/"
                f"{submission.expected.sha256}/detection"
            )
        else:
            print(
                f"OK: {submission.expected.scan_path} clean "
                f"({result.completed_engines}/{result.total_engines} decisive/total engines)"
            )

    completed_paths = {item.submission.expected.scan_path for item in completed}
    expected_paths = {item.expected.scan_path for item in submissions}
    if completed_paths != expected_paths or len(completed) != len(submissions):
        raise GateError("completed VirusTotal set differs from the exact submitted set")
    write_results(
        results_path,
        results=completed,
        associations=associations,
        min_engines=min_engines,
    )
    if failures:
        print(f"VirusTotal completed-set evidence: {results_path}")
        raise GateError(f"{len(failures)} VirusTotal object(s) failed release policy")
    print(f"=== All {len(completed)} exact VirusTotal objects passed; results: {results_path} ===")


try:
    main()
except (GateError, OSError, subprocess.SubprocessError) as error:
    print(f"BLOCKED: {error}", file=sys.stderr)
    raise SystemExit(1)
PY

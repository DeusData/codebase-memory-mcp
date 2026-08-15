#!/usr/bin/env bash
# Contract: candidate staging, VT-policy selection, and final archive
# reconciliation form one content-bound fail-closed boundary.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIX="$(mktemp -d "${TMPDIR:-/tmp}/cbm-vt-select.XXXXXX")"
trap 'rm -rf "$FIX"' EXIT

STAGE="$ROOT/scripts/ci/stage-release-candidates.py"
SELECT="$ROOT/scripts/ci/select-release-candidates.py"
VERIFY="$ROOT/scripts/ci/verify-release-selection.py"
for tool in "$STAGE" "$SELECT" "$VERIFY"; do
    [[ -x "$tool" ]] || { echo "FAIL: missing executable candidate-boundary tool: $tool" >&2; exit 1; }
done

python3 - "$ROOT" "$FIX" <<'PY'
from __future__ import annotations

import csv
import hashlib
import io
import os
import pathlib
import random
import shutil
import stat
import subprocess
import sys
import tarfile
import zipfile

root = pathlib.Path(sys.argv[1])
fix = pathlib.Path(sys.argv[2])
stage_tool = root / "scripts/ci/stage-release-candidates.py"
select_tool = root / "scripts/ci/select-release-candidates.py"
verify_tool = root / "scripts/ci/verify-release-selection.py"

TARGETS = (
    "linux-amd64", "linux-arm64", "linux-amd64-portable",
    "linux-arm64-portable", "darwin-amd64", "darwin-arm64",
    "windows-amd64", "windows-arm64",
)
VARIANTS = ("unstripped", "debug-stripped", "stripped")
ORDER = ("stripped", "debug-stripped", "unstripped")
FIELDS = (
    "target", "variant", "relative_path", "source_sha256", "pre_sign_sha256",
    "sha256", "size", "format", "architecture", "linkage", "transform",
    "signature", "strip_tool", "strip_version", "pair_verification",
)
RESULT_FIELDS = (
    "scan_path", "sha256", "size", "association_count", "completed_engines",
    "total_engines", "malicious", "suspicious", "analysis_id",
    "microsoft_category", "microsoft_result", "policy_classification",
    "microsoft_engine_version", "microsoft_engine_update", "virustotal_url",
)
ARCHIVES = tuple(
    f"codebase-memory-mcp-{target}.tar.gz"
    for target in TARGETS if not target.startswith("windows-")
) + tuple(
    f"codebase-memory-mcp-{target}.zip"
    for target in TARGETS if target.startswith("windows-")
) + tuple(
    f"codebase-memory-mcp-{target}.mcpb"
    for target in TARGETS
    if target.startswith(("darwin-", "windows-")) or target.endswith("-portable")
)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def run(*argv: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, *(str(item) for item in argv)],
                          text=True, capture_output=True)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def manifest(path: pathlib.Path) -> tuple[dict[str, str], list[dict[str, str]]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    marker = lines[0]
    metadata: dict[str, str] = {}
    cursor = 1
    while cursor < len(lines) and lines[cursor].startswith("# "):
        key, separator, value = lines[cursor][2:].partition("=")
        require(bool(separator), f"malformed metadata in {path}")
        metadata[key] = value
        cursor += 1
    return {"marker": marker, **metadata}, list(csv.DictReader(lines[cursor:], delimiter="\t"))


def write_tsv(path: pathlib.Path, marker: str, metadata: dict[str, object],
              fields: tuple[str, ...], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        handle.write(f"# {marker}\n")
        for key, value in metadata.items():
            handle.write(f"# {key}={value}\n")
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def candidate_bytes(target: str, variant: str) -> bytes:
    linked = f"codebase-memory-mcp:{target}:same-linker-output\n".encode()
    suffix = {
        "unstripped": b"debug-and-local-symbols\n",
        "debug-stripped": b"local-symbols-only\n",
        "stripped": b"stripped\n",
    }[variant]
    return linked + suffix


def make_artifacts(directory: pathlib.Path, *, reverse: bool = False) -> dict[tuple[str, str], bytes]:
    objects: dict[tuple[str, str], bytes] = {}
    targets = list(reversed(TARGETS)) if reverse else list(TARGETS)
    for index, target in enumerate(targets):
        artifact = directory / f"download-{index:02d}" / "nested" / target
        artifact.mkdir(parents=True)
        rows = []
        source_hash = digest(candidate_bytes(target, "unstripped"))
        for variant in VARIANTS:
            name = "codebase-memory-mcp.exe" if target.startswith("windows-") else "codebase-memory-mcp"
            data = candidate_bytes(target, variant)
            objects[target, variant] = data
            path = artifact / variant / name
            path.parent.mkdir()
            path.write_bytes(data)
            path.chmod(0o500)
            rows.append({
                "target": target,
                "variant": variant,
                "relative_path": f"{variant}/{name}",
                "source_sha256": source_hash,
                "pre_sign_sha256": digest(data),
                "sha256": digest(data),
                "size": len(data),
                "format": "pe" if target.startswith("windows-") else "macho" if target.startswith("darwin-") else "elf",
                "architecture": target.split("-")[1],
                "linkage": "portable" if target.endswith("-portable") else "dynamic" if target.startswith("linux-") else "native",
                "transform": {"unstripped": "copy", "debug-stripped": "strip-debug", "stripped": "strip"}[variant],
                "signature": "adhoc-verified" if target.startswith("darwin-") else "not-applicable",
                "strip_tool": "contract-strip",
                "strip_version": "1.0",
                "pair_verification": "same-linker-output-v1",
            })
        write_tsv(artifact / "candidate-provenance.tsv",
                  "cbm-release-candidate-provenance-v1", {}, FIELDS, rows)
        # Required target-bound notice adjacent to each downloaded artifact.
        (artifact / "THIRD_PARTY_NOTICES.md").write_text(
            f"third-party notice for {target}\n", encoding="utf-8")
    return objects


def invoke_stage(source: pathlib.Path, output: pathlib.Path) -> subprocess.CompletedProcess[str]:
    return run(stage_tool, source, output, "--expect-targets", 8, "--expect-candidates", 24)


artifacts = fix / "artifacts"
candidate_data = make_artifacts(artifacts)
staged = fix / "scan"
result = invoke_stage(artifacts, staged)
require(result.returncode == 0, f"valid 8-target candidate set rejected: {result.stdout}{result.stderr}")
require(sorted(path.name for path in staged.iterdir()) ==
        ["associations.tsv", "candidates.tsv", "notices", "objects", "scan-set.tsv"],
        "stager output is not the exact atomic bundle")

cmeta, candidates = manifest(staged / "candidates.tsv")
require(cmeta["marker"] == "# cbm-release-candidates-v1", "candidate marker changed")
require(cmeta.get("targets") == "8" and cmeta.get("candidates") == "24", "candidate counts not bound")
require([(row["target"], row["variant"]) for row in candidates] ==
        [(target, variant) for target in TARGETS for variant in VARIANTS],
        "candidate rows are not in canonical target/variant order")
for target in TARGETS:
    notice = staged / "notices" / target / "THIRD_PARTY_NOTICES.md"
    require(notice.read_text(encoding="utf-8") == f"third-party notice for {target}\n",
            f"wrong/missing target notice: {target}")

# Discovery/row order must not affect any staged manifest bytes.
reverse_artifacts = fix / "reverse-artifacts"
make_artifacts(reverse_artifacts, reverse=True)
reverse_stage = fix / "reverse-scan"
result = invoke_stage(reverse_artifacts, reverse_stage)
require(result.returncode == 0, f"reverse-order artifact set rejected: {result.stdout}{result.stderr}")
for name in ("candidates.tsv", "scan-set.tsv", "associations.tsv"):
    require((staged / name).read_bytes() == (reverse_stage / name).read_bytes(),
            f"{name} depends on discovery/row order")


def expect_stage_failure(label: str, mutate) -> None:
    case = fix / f"bad-stage-{label}"
    shutil.copytree(artifacts, case, symlinks=True)
    mutate(case)
    out = fix / f"bad-stage-out-{label}"
    result = invoke_stage(case, out)
    require(result.returncode != 0, f"stager accepted {label}")
    require(not out.exists(), f"failed stage {label} published a partial output")


def provenance_paths(root_path: pathlib.Path) -> list[pathlib.Path]:
    return sorted(root_path.rglob("candidate-provenance.tsv"))


expect_stage_failure("missing", lambda path: provenance_paths(path)[0].unlink())
expect_stage_failure("surplus", lambda path: shutil.copytree(
    provenance_paths(path)[0].parent, path / "ninth" / "nested" / "extra"))


def replace_candidate_with_symlink(path: pathlib.Path) -> None:
    provenances = provenance_paths(path)
    binary = next(provenances[0].parent.glob("unstripped/*"))
    # Windows maps the fixture's read-only mode to FILE_ATTRIBUTE_READONLY,
    # which must be cleared before unlink; Unix only needs a writable parent.
    binary.chmod(0o700)
    binary.unlink()
    target = provenances[1].parent / "unstripped" / (
        "codebase-memory-mcp.exe" if "windows" in str(provenances[1])
        else "codebase-memory-mcp"
    )
    binary.symlink_to(target)


expect_stage_failure("symlink", replace_candidate_with_symlink)


def tamper_candidate(path: pathlib.Path) -> None:
    candidate = next(provenance_paths(path)[0].parent.glob("stripped/*"))
    candidate.chmod(0o700)
    candidate.write_bytes(b"tampered")


expect_stage_failure("tamper", tamper_candidate)


def result_rows(classifications: dict[tuple[str, str], str], *, shuffle: bool = False) -> list[dict[str, object]]:
    rows = []
    for row in candidates:
        classification = classifications[row["target"], row["variant"]]
        clean = classification == "clean"
        rows.append({
            "scan_path": row["scan_path"], "sha256": row["sha256"], "size": row["size"],
            "association_count": 1, "completed_engines": 71, "total_engines": 71,
            "malicious": 0 if clean else 1, "suspicious": 0,
            "analysis_id": "analysis-" + row["sha256"][:24],
            "microsoft_category": "undetected" if clean else "malicious",
            "microsoft_result": "" if clean else "Trojan:Win32/Wacatac.B!ml",
            "policy_classification": classification,
            "microsoft_engine_version": "1.1.1", "microsoft_engine_update": "20260813",
            "virustotal_url": "https://www.virustotal.com/gui/file/" + row["sha256"] + "/detection",
        })
    if shuffle:
        random.Random(8675309).shuffle(rows)
    return rows


def write_results(path: pathlib.Path, rows: list[dict[str, object]]) -> None:
    write_tsv(path, "cbm-virustotal-results-v2", {
        "scan_objects": 24, "associations": 24, "min_engines_policy": 50,
        "min_completed_engines": 71, "max_completed_engines": 71,
    }, RESULT_FIELDS, rows)


# Keys are (stripped, debug-stripped, unstripped) in the policy's preferred
# order. Three variants x two tolerated classifications is exactly eight
# combinations, so the eight targets below cover the table EXHAUSTIVELY.
TRUTH_KEYS = [
    (a, b, c)
    for a in ("clean", "microsoft-ml")
    for b in ("clean", "microsoft-ml")
    for c in ("clean", "microsoft-ml")
]


def expected_choice(key: tuple[str, str, str]) -> tuple[str, str]:
    """Return (selected_variant, decision) for a (stripped, debug, unstripped) key."""
    by_variant = dict(zip(ORDER, key))
    clean = [v for v in ORDER if by_variant[v] == "clean"]
    if not clean:
        return "stripped", "stripped-all-candidates-microsoft-ml"
    chosen = clean[0]
    if chosen == "stripped":
        return "stripped", "stripped-preferred"
    others = "-".join(f"{v}:{by_variant[v]}" for v in ORDER if v != chosen)
    return chosen, f"{chosen}-clean-after-{others}"


def selection_rows(directory: pathlib.Path) -> tuple[dict[str, str], list[dict[str, str]]]:
    return manifest(directory / "release-selection.tsv")


# Exhaustive truth table: distribute all four pairs across eight targets and
# repeat once, proving exact decision behavior and both result bindings.
classes: dict[tuple[str, str], str] = {}
expected: dict[str, str] = {}
expected_decisions: dict[str, str] = {}
require(len(TRUTH_KEYS) == len(TARGETS), "truth table must map one combination per target")
for index, target in enumerate(TARGETS):
    key = TRUTH_KEYS[index]
    for variant, classification in zip(ORDER, key):
        classes[target, variant] = classification
    expected[target], expected_decisions[target] = expected_choice(key)
results_file = fix / "vt-results.tsv"
write_results(results_file, result_rows(classes, shuffle=True))
selected = fix / "selected"
result = run(select_tool, "--candidates", staged / "candidates.tsv",
             "--objects-dir", staged / "objects", "--out-dir", selected,
             "--results", results_file)
require(result.returncode == 0, f"valid truth-table results rejected: {result.stdout}{result.stderr}")
smeta, selections = selection_rows(selected)
require(smeta["marker"] == "# cbm-release-selection-v1", "selection marker changed")
require(smeta.get("policy") == "virustotal-v2", "scanned selection policy not recorded")
require([row["target"] for row in selections] == list(TARGETS), "selection order is not canonical")
for row in selections:
    target = row["target"]
    require(row["selected_variant"] == expected[target], f"truth-table decision wrong for {target}")
    expected_decision = expected_decisions[target]
    require(row["decision"] == expected_decision,
            f"truth-table decision reason wrong for {target}: {row['decision']!r}")
    chosen = next(c for c in candidates if c["target"] == target and c["variant"] == expected[target])
    path = selected / "selected" / target / ("codebase-memory-mcp.exe" if target.startswith("windows-") else "codebase-memory-mcp")
    require(path.read_bytes() == candidate_data[target, expected[target]], f"wrong selected bytes for {target}")
    require(row["selected_sha256"] == chosen["sha256"], f"selection hash wrong for {target}")

# Same logical results in canonical/reverse order produce identical evidence.
ordered_results = fix / "ordered-results.tsv"
write_results(ordered_results, result_rows(classes, shuffle=False))
selected_ordered = fix / "selected-ordered"
result = run(select_tool, "--candidates", staged / "candidates.tsv",
             "--objects-dir", staged / "objects", "--out-dir", selected_ordered,
             "--results", ordered_results)
require(result.returncode == 0, "canonical-order results unexpectedly rejected")
require((selected / "release-selection.tsv").read_bytes() ==
        (selected_ordered / "release-selection.tsv").read_bytes(),
        "selection evidence depends on VT result row order")

# Default policy is explicit dry-run evidence, not a fake clean result.
defaulted = fix / "default-selected"
result = run(select_tool, "--candidates", staged / "candidates.tsv",
             "--objects-dir", staged / "objects", "--out-dir", defaulted,
             "--default-stripped")
require(result.returncode == 0, f"default-stripped dry-run rejected: {result.stdout}{result.stderr}")
dmeta, drows = selection_rows(defaulted)
require(dmeta.get("policy") == "unscanned-dry-run", "default policy is not visibly marked unscanned")
require(all(row["selected_variant"] == "stripped" and
            row["unstripped_classification"] == "unscanned-dry-run" and
            row["stripped_classification"] == "unscanned-dry-run" for row in drows),
        "default policy does not bind every target to visibly unscanned stripped bytes")


def expect_select_failure(label: str, mutate) -> None:
    rows = result_rows({key: "clean" for key in candidate_data})
    mutate(rows)
    evidence = fix / f"bad-results-{label}.tsv"
    write_results(evidence, rows)
    out = fix / f"bad-selected-{label}"
    result = run(select_tool, "--candidates", staged / "candidates.tsv",
                 "--objects-dir", staged / "objects", "--out-dir", out,
                 "--results", evidence)
    require(result.returncode != 0, f"selector accepted {label}")
    require(not out.exists(), f"failed selection {label} published partial output")


expect_select_failure("hard", lambda rows: rows[0].update(policy_classification="hard"))
expect_select_failure("incomplete", lambda rows: rows[0].update(completed_engines=49))
expect_select_failure("missing", lambda rows: rows.pop())
expect_select_failure("surplus", lambda rows: rows.append(dict(rows[0], analysis_id="extra-analysis")))
expect_select_failure("tampered-hash", lambda rows: rows[0].update(sha256="0" * 64))
expect_select_failure("cross-target", lambda rows: (
    rows[0].update(scan_path=rows[2]["scan_path"], sha256=rows[2]["sha256"], size=rows[2]["size"]),
    rows[2].update(scan_path=candidates[0]["scan_path"], sha256=candidates[0]["sha256"], size=candidates[0]["size"]),
))

# Reconcile all 14 canonical containers, recursively, against the selected
# eight hashes. Eligible MCPBs repeat exactly the matching selected bytes.
archives = fix / "containers"
archive_for: dict[str, bytes] = {}
for row in selections:
    target = row["target"]
    archive_for[target] = candidate_data[target, row["selected_variant"]]
for name in ARCHIVES:
    target = next(target for target in TARGETS if name.startswith(f"codebase-memory-mcp-{target}."))
    destination = archives / target / ("mcpb" if name.endswith(".mcpb") else "archive") / name
    destination.parent.mkdir(parents=True, exist_ok=True)
    binary_name = "codebase-memory-mcp.exe" if target.startswith("windows-") else "codebase-memory-mcp"
    member = f"server/{binary_name}" if name.endswith(".mcpb") else binary_name
    if name.endswith((".zip", ".mcpb")):
        with zipfile.ZipFile(destination, "w") as handle:
            info = zipfile.ZipInfo(member)
            info.create_system = 3
            permissions = 0o755 if not target.startswith("windows-") else 0o644
            info.external_attr = (stat.S_IFREG | permissions) << 16
            handle.writestr(info, archive_for[target])
    else:
        with tarfile.open(destination, "w:gz") as handle:
            info = tarfile.TarInfo(member)
            info.size = len(archive_for[target])
            info.mode = 0o755
            handle.addfile(info, io.BytesIO(archive_for[target]))

result = run(verify_tool, "--selection", selected / "release-selection.tsv",
             "--archive-dir", archives)
require(result.returncode == 0, f"exact canonical containers rejected: {result.stdout}{result.stderr}")


def expect_verify_failure(label: str, mutate) -> None:
    case = fix / f"bad-containers-{label}"
    shutil.copytree(archives, case)
    mutate(case)
    result = run(verify_tool, "--selection", selected / "release-selection.tsv",
                 "--archive-dir", case)
    require(result.returncode != 0, f"archive verifier accepted {label}")


expect_verify_failure("missing", lambda path: next(path.rglob("*.mcpb")).unlink())
expect_verify_failure("surplus", lambda path: (path / "surplus.zip").write_bytes(b"not allowed"))


def mutate_zip(path: pathlib.Path) -> None:
    archive = next(path.rglob("codebase-memory-mcp-windows-amd64.zip"))
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr("codebase-memory-mcp.exe", b"mutated-selected-binary")


expect_verify_failure("mutation", mutate_zip)


def remove_unix_executable_mode(path: pathlib.Path) -> None:
    archive = next(path.rglob("codebase-memory-mcp-darwin-arm64.mcpb"))
    with zipfile.ZipFile(archive, "r") as handle:
        entries = [(info, handle.read(info)) for info in handle.infolist()]
    with zipfile.ZipFile(archive, "w") as handle:
        for info, payload in entries:
            if info.filename == "server/codebase-memory-mcp":
                info.create_system = 3
                info.external_attr = (stat.S_IFREG | 0o644) << 16
            handle.writestr(info, payload)


expect_verify_failure("unix-executable-mode", remove_unix_executable_mode)


def wrong_target(path: pathlib.Path) -> None:
    archive = next(path.rglob("codebase-memory-mcp-linux-amd64.tar.gz"))
    with tarfile.open(archive, "w:gz") as handle:
        data = archive_for["linux-arm64"]
        info = tarfile.TarInfo("codebase-memory-mcp")
        info.size = len(data)
        handle.addfile(info, io.BytesIO(data))


expect_verify_failure("wrong-target", wrong_target)

print("PASS: exact candidate staging, exhaustive VT truth table, fail-closed selection, deterministic evidence, dry-run marking, and 14-container reconciliation")
PY

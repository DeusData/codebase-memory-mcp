#!/usr/bin/env bash
# Contract: release notes reuse the exact pre-smoke candidate verdicts.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT="$ROOT/scripts/ci/append-vt-notes.sh"
FIX="$(mktemp -d "${TMPDIR:-/tmp}/vt-notes-contract.XXXXXX")"
trap 'rm -rf "$FIX"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

mkdir -p "$FIX/bin" "$FIX/evidence"
python3 - "$FIX/evidence" <<'PY'
import csv
import hashlib
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
targets = (
    "linux-amd64", "linux-arm64", "linux-amd64-portable",
    "linux-arm64-portable", "darwin-amd64", "darwin-arm64",
    "windows-amd64", "windows-arm64",
)
variants = ("unstripped", "debug-stripped", "stripped")
field_key = {v: v.replace("-", "_") for v in variants}
candidate_fields = (
    "target", "variant", "relative_path", "source_sha256", "pre_sign_sha256",
    "sha256", "size", "format", "architecture", "linkage", "transform",
    "signature", "strip_tool", "strip_version", "pair_verification", "scan_path",
)
result_fields = (
    "scan_path", "sha256", "size", "association_count", "completed_engines",
    "total_engines", "malicious", "suspicious", "analysis_id",
    "microsoft_category", "microsoft_result", "policy_classification",
    "microsoft_engine_version", "microsoft_engine_update", "virustotal_url",
)
selection_fields = (
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


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def write(path, marker, metadata, fields, rows):
    with path.open("w", encoding="utf-8", newline="") as handle:
        handle.write(f"# {marker}\n")
        for key, value in metadata.items():
            handle.write(f"# {key}={value}\n")
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


classes = {(target, variant): "clean" for target in targets for variant in variants}
classes["linux-amd64", "stripped"] = "microsoft-ml"
classes["darwin-arm64", "stripped"] = "microsoft-ml"
classes["darwin-arm64", "unstripped"] = "microsoft-ml"
# windows-amd64 draws the tolerated verdict on ALL THREE candidates, so the
# SHIPPED binary carries it and the notes must disclose exactly that one. The
# two targets above keep a clean sibling, so their flagged candidates are
# rejected and must NOT appear in the notes at all.
for _variant in variants:
    classes["windows-amd64", _variant] = "microsoft-ml"
candidates = []
results = []
by_pair = {}
for target in targets:
    source_sha = digest(f"{target}:linker-output")
    for variant in variants:
        sha = digest(f"{target}:{variant}")
        name = "codebase-memory-mcp.exe" if target.startswith("windows-") else "codebase-memory-mcp"
        row = {
            "target": target, "variant": variant, "relative_path": f"{variant}/{name}",
            "source_sha256": source_sha, "pre_sign_sha256": sha, "sha256": sha,
            "size": "1234", "format": "pe" if target.startswith("windows-") else "macho" if target.startswith("darwin-") else "elf",
            "architecture": target.split("-")[1],
            "linkage": "portable" if target.endswith("-portable") else "dynamic" if target.startswith("linux-") else "native",
            "transform": {"unstripped": "copy", "debug-stripped": "strip-debug", "stripped": "strip"}[variant],
            "signature": "adhoc-verified" if target.startswith("darwin-") else "not-applicable",
            "strip_tool": "strip", "strip_version": "contract",
            "pair_verification": "same-linker-output-v1", "scan_path": f"objects/{sha}",
        }
        candidates.append(row)
        by_pair[target, variant] = row
        classification = classes[target, variant]
        clean = classification == "clean"
        results.append({
            "scan_path": row["scan_path"], "sha256": sha, "size": row["size"],
            "association_count": "1", "completed_engines": "61", "total_engines": "64",
            "malicious": "0" if clean else "1", "suspicious": "0",
            "analysis_id": f"analysis-{sha[:20]}",
            "microsoft_category": "undetected" if clean else "malicious",
            "microsoft_result": "" if clean else "Trojan:Win32/Wacatac.B!ml",
            "policy_classification": classification,
            "microsoft_engine_version": "1.1.1", "microsoft_engine_update": "20260813",
            "virustotal_url": f"https://www.virustotal.com/gui/file/{sha}/detection",
        })

result_by_pair = {(row["target"], row["variant"]): result for row, result in zip(candidates, results)}
selections = []
for target in targets:
    order = ("stripped", "debug-stripped", "unstripped")
    clean = [v for v in order if classes[target, v] == "clean"]
    chosen = clean[0] if clean else "stripped"
    selected = by_pair[target, chosen]
    if not clean:
        decision = "stripped-all-candidates-microsoft-ml"
    elif chosen == "stripped":
        decision = "stripped-preferred"
    else:
        others = "-".join(f"{v}:{classes[target, v]}" for v in order if v != chosen)
        decision = f"{chosen}-clean-after-{others}"
    row = {
        "target": target, "selected_variant": chosen,
        "selected_path": f"selected/{target}/" + ("codebase-memory-mcp.exe" if target.startswith("windows-") else "codebase-memory-mcp"),
        "selected_sha256": selected["sha256"], "selected_size": selected["size"],
        "decision": decision,
    }
    for variant in variants:
        candidate = by_pair[target, variant]
        result = result_by_pair[target, variant]
        row.update({
            f"{field_key[variant]}_sha256": candidate["sha256"],
            f"{field_key[variant]}_scan_path": candidate["scan_path"],
            f"{field_key[variant]}_classification": result["policy_classification"],
            f"{field_key[variant]}_analysis_id": result["analysis_id"],
            f"{field_key[variant]}_virustotal_url": result["virustotal_url"],
        })
    selections.append(row)

write(root / "release-candidates.tsv", "cbm-release-candidates-v1",
      {"targets": 8, "candidates": 24}, candidate_fields, candidates)
write(root / "virustotal-candidate-results.tsv", "cbm-virustotal-results-v2",
      {"scan_objects": 24, "associations": 24, "min_engines_policy": 50,
       "min_completed_engines": 61, "max_completed_engines": 61}, result_fields, results)
write(root / "release-selection.tsv", "cbm-release-selection-v1",
      {"policy": "virustotal-v2", "targets": 8, "candidates": 24},
      selection_fields, selections)
PY

cat > "$FIX/current.md" <<'EOF'
Intro text.

<!-- cbm-security-verification:start -->
stale data
<!-- cbm-security-verification:end -->

Outro text.
EOF

cat > "$FIX/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [ "$1" = release ] && [ "$2" = view ]; then
  cat "$STUB_RELEASE_BODY"
  exit 0
fi
if [ "$1" = release ] && [ "$2" = edit ]; then
  while [ "$#" -gt 0 ]; do
    if [ "$1" = --notes-file ]; then
      cp "$2" "$STUB_CAPTURE"
      exit 0
    fi
    shift
  done
fi
exit 2
EOF
chmod +x "$FIX/bin/gh"

run_notes() {
  (cd "$FIX" &&
    PATH="$FIX/bin:$PATH" \
      GH_TOKEN=stub VERSION=v1.0.0 \
      GITHUB_REPOSITORY=DeusData/codebase-memory-mcp \
      VT_CANDIDATES=evidence/release-candidates.tsv \
      VT_RESULTS_PATH=evidence/virustotal-candidate-results.tsv \
      RELEASE_SELECTION=evidence/release-selection.tsv \
      STUB_RELEASE_BODY="$1" STUB_CAPTURE="$2" \
      bash "$SCRIPT")
}

run_notes "$FIX/current.md" "$FIX/first.md"
[ "$(grep -c 'cbm-security-verification:start' "$FIX/first.md")" = 1 ] || fail "start marker duplicated"
[ "$(grep -c 'cbm-security-verification:end' "$FIX/first.md")" = 1 ] || fail "end marker duplicated"
grep -q 'Intro text.' "$FIX/first.md" || fail "content before section was lost"
grep -q 'Outro text.' "$FIX/first.md" || fail "content after section was lost"
! grep -q 'stale data' "$FIX/first.md" || fail "stale section was appended"
# Release notes report ONLY the shipped bytes. Rejected candidates stay in the
# published evidence TSVs; they are development signal, not changelog content.
grep -q 'verdict for the exact shipped bytes is linked per product' "$FIX/first.md" || \
  fail "shipped-binary scan scope missing"
[ "$(grep -c '^| `' "$FIX/first.md")" = "8" ] || fail "product scope missing: expected one row per release product"
grep -q '1 shipped binary.*Microsoft' "$FIX/first.md" || fail "tolerated finding on a shipped binary not disclosed"
grep -q 'candidate(s)' "$FIX/first.md" && fail "notes must not count rejected candidates"
grep -q 'no other decisive engine reported malicious or suspicious' "$FIX/first.md" || fail "non-decisive engines are overstated as clean"
! grep -q 'every other engine was clean' "$FIX/first.md" || fail "non-decisive engines are incorrectly described as clean"
grep -q '| Product | Shipped binary | VirusTotal verdict |' "$FIX/first.md" || fail "shipped-binary table missing"
grep -q 'Stripped candidate' "$FIX/first.md" && \
  fail "release notes must not enumerate rejected candidates"
grep -q '| `linux-amd64` |' "$FIX/first.md" || fail "Linux product row missing"
grep -q '| `windows-arm64` |' "$FIX/first.md" || fail "Windows product row missing"
grep -q 'archive containers were not redundantly submitted' "$FIX/first.md" || fail "no-rescan boundary missing"
for asset in release-candidates.tsv virustotal-candidate-results.tsv release-selection.tsv; do
  grep -q "releases/download/v1.0.0/$asset" "$FIX/first.md" || fail "evidence link missing: $asset"
done

run_notes "$FIX/first.md" "$FIX/second.md"
python3 -c 'import sys;sys.exit(open(sys.argv[1],"rb").read()!=open(sys.argv[2],"rb").read())' \
  "$FIX/first.md" "$FIX/second.md" || fail "release-note update is not idempotent"

cp "$FIX/evidence/release-selection.tsv" "$FIX/evidence/release-selection.clean.tsv"
python3 - "$FIX/evidence/release-selection.tsv" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1])
text = p.read_text()
p.write_text(text.replace("\t1234\tstripped-preferred", "\t9999\tstripped-preferred", 1))
PY
if run_notes "$FIX/current.md" "$FIX/tampered.md"; then
  fail "selection hash/size mismatch must fail closed"
fi
mv "$FIX/evidence/release-selection.clean.tsv" "$FIX/evidence/release-selection.tsv"

cat > "$FIX/reversed.md" <<'EOF'
<!-- cbm-security-verification:end -->
stale reversed data
<!-- cbm-security-verification:start -->
EOF
if run_notes "$FIX/reversed.md" "$FIX/reversed-capture.md"; then
  fail "reversed verification markers must fail closed"
fi

echo 'PASS: release notes report the shipped binary only, disclose selection, and avoid a duplicate scan'

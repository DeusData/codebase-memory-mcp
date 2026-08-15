#!/usr/bin/env bash
# Publish the complete, already-gated VirusTotal evidence as durable release
# assets. Workflow artifacts are useful diagnostics but expire and may require
# repository access; these fixed-name files remain attached to the public
# release whose exact objects they describe.
set -euo pipefail

: "${GH_TOKEN:?publish-vt-evidence: GH_TOKEN is required}"
: "${VERSION:?publish-vt-evidence: VERSION is required}"
: "${GITHUB_REPOSITORY:?publish-vt-evidence: GITHUB_REPOSITORY is required}"

VT_ASSOCIATIONS="${VT_ASSOCIATIONS:-binaries/associations.tsv}"
VT_EXPECTED_SCAN_SET="${VT_EXPECTED_SCAN_SET:-binaries/scan-set.tsv}"
VT_RESULTS_PATH="${VT_RESULTS_PATH:-binaries/vt-results.tsv}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/cbm-vt-public.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

publish_copy() {
    local source="$1" marker="$2" destination="$3" first_line size
    if [ ! -f "$source" ] || [ -L "$source" ]; then
        echo "publish-vt-evidence: missing or unsafe evidence file: $source" >&2
        exit 1
    fi
    size=$(wc -c < "$source" | tr -d '[:space:]')
    case "$size" in
    ''|*[!0-9]*) echo "publish-vt-evidence: cannot measure evidence file: $source" >&2; exit 1 ;;
    esac
    if [ "$size" -le 0 ] || [ "$size" -gt 16777216 ]; then
        echo "publish-vt-evidence: empty or oversized evidence file: $source" >&2
        exit 1
    fi
    IFS= read -r first_line < "$source" || true
    if [ "$first_line" != "# $marker" ]; then
        echo "publish-vt-evidence: wrong evidence marker in $source" >&2
        exit 1
    fi
    cp "$source" "$WORK/$destination"
    chmod 0644 "$WORK/$destination"
}

publish_copy "$VT_ASSOCIATIONS" cbm-release-scan-associations-v3 virustotal-associations.tsv
publish_copy "$VT_EXPECTED_SCAN_SET" cbm-release-scan-set-v2 virustotal-scan-set.tsv
publish_copy "$VT_RESULTS_PATH" cbm-virustotal-results-v2 virustotal-results.tsv

for name in virustotal-associations.tsv virustotal-results.tsv virustotal-scan-set.tsv; do
    if command -v sha256sum >/dev/null 2>&1; then
        digest=$(sha256sum "$WORK/$name" | awk '{print $1}')
    elif command -v shasum >/dev/null 2>&1; then
        digest=$(shasum -a 256 "$WORK/$name" | awk '{print $1}')
    else
        echo "publish-vt-evidence: no SHA-256 tool is available" >&2
        exit 1
    fi
    printf '%s  %s\n' "$digest" "$name" >> "$WORK/virustotal-evidence-checksums.txt"
done
chmod 0644 "$WORK/virustotal-evidence-checksums.txt"

gh release upload "$VERSION" \
    "$WORK/virustotal-associations.tsv" \
    "$WORK/virustotal-scan-set.tsv" \
    "$WORK/virustotal-results.tsv" \
    "$WORK/virustotal-evidence-checksums.txt" \
    --clobber --repo "$GITHUB_REPOSITORY"

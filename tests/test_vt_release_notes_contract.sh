#!/usr/bin/env bash
# Contract for idempotent, evidence-bound VirusTotal release-note publication.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT="$ROOT/scripts/ci/append-vt-notes.sh"
PUBLISH="$ROOT/scripts/ci/publish-vt-evidence.sh"
FIX="$(mktemp -d "${TMPDIR:-/tmp}/vt-notes-contract.XXXXXX")"
trap 'rm -rf "$FIX"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }

mkdir -p "$FIX/bin" "$FIX/binaries"
SHA_A='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
SHA_B='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
cat > "$FIX/binaries/associations.tsv" <<EOF
# cbm-release-scan-associations-v2
# archives=1
# binaries=1
# packs=0
# runtime_files=0
# pack_assets=1
# associations=3
# scan_objects=2
association_type	archive	archive_sha256	variant	kind	member	asset_path	mime	scan_path	object_sha256	size
archive	codebase-memory-mcp-linux-amd64.tar.gz	$SHA_A	standard	archive				objects/archive	$SHA_A	100
member	codebase-memory-mcp-linux-amd64.tar.gz	$SHA_A	standard	binary	codebase-memory-mcp			objects/archive	$SHA_A	100
pack_asset	codebase-memory-mcp-linux-amd64.tar.gz	$SHA_A	ui	ui_asset	cbm-ui-$SHA_B.pack	/assets/app.js	application/javascript	objects/script	$SHA_B	20
EOF
cat > "$FIX/binaries/vt-results.tsv" <<EOF
# cbm-virustotal-results-v1
# scan_objects=2
# associations=3
# min_engines_policy=50
# min_completed_engines=59
# max_completed_engines=61
scan_path	sha256	size	association_count	completed_engines	total_engines	malicious	suspicious	analysis_id	microsoft_category	microsoft_engine_version	microsoft_engine_update	virustotal_url
objects/archive	$SHA_A	100	2	59	62	0	0	analysis-a	undetected	1.26070	20260808	https://www.virustotal.com/gui/file/$SHA_A/detection
objects/script	$SHA_B	20	1	61	64	0	0	analysis-b				https://www.virustotal.com/gui/file/$SHA_B/detection
EOF
cat > "$FIX/current.md" <<'EOF'
Intro text.

<!-- cbm-security-verification:start -->
## Security Verification

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
if [ "$1" = release ] && [ "$2" = upload ]; then
  shift 3 # release upload <tag>
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --clobber) shift ;;
      --repo) shift 2 ;;
      *) cp "$1" "$STUB_UPLOAD_DIR/$(basename "$1")"; shift ;;
    esac
  done
  exit 0
fi
exit 2
EOF
chmod +x "$FIX/bin/gh"

run_notes() { # current capture
  (cd "$FIX" &&
    PATH="$FIX/bin:$PATH" \
      GH_TOKEN=stub \
      VERSION=v1.0.0 \
      GITHUB_REPOSITORY=DeusData/codebase-memory-mcp \
      VT_ASSOCIATIONS=binaries/associations.tsv \
      VT_RESULTS_PATH=binaries/vt-results.tsv \
      STUB_RELEASE_BODY="$1" \
      STUB_CAPTURE="$2" \
      bash "$SCRIPT")
}

run_notes "$FIX/current.md" "$FIX/first.md"
[ "$(grep -c 'cbm-security-verification:start' "$FIX/first.md")" = 1 ] || fail "start marker duplicated"
[ "$(grep -c 'cbm-security-verification:end' "$FIX/first.md")" = 1 ] || fail "end marker duplicated"
grep -q 'Intro text.' "$FIX/first.md" || fail "content before marked section was lost"
grep -q 'Outro text.' "$FIX/first.md" || fail "content after marked section was lost"
! grep -q 'stale data' "$FIX/first.md" || fail "stale marked section was appended instead of replaced"
grep -q '2 distinct byte objects' "$FIX/first.md" || fail "unique-object scope missing"
grep -q '3 exact release associations' "$FIX/first.md" || fail "association scope missing"
grep -q '59–61 decisive engine results' "$FIX/first.md" || fail "measured engine range missing"
grep -q 'Microsoft returned a decisive clean verdict for all 1 executable objects' "$FIX/first.md" || \
  fail "executable Microsoft coverage is not stated"
grep -q "$SHA_A" "$FIX/first.md" || fail "full archive SHA-256 missing"
grep -q "gui/file/$SHA_A/detection" "$FIX/first.md" || fail "archive VirusTotal link missing"
for asset in virustotal-associations.tsv virustotal-scan-set.tsv virustotal-results.tsv virustotal-evidence-checksums.txt; do
  grep -q "releases/download/v1.0.0/$asset" "$FIX/first.md" || fail "public evidence link missing: $asset"
done
! grep -q '70+' "$FIX/first.md" || fail "unmeasured engine claim returned"

run_notes "$FIX/first.md" "$FIX/second.md"
cmp -s "$FIX/first.md" "$FIX/second.md" || fail "marked release-note update is not idempotent"

cp "$FIX/binaries/vt-results.tsv" "$FIX/binaries/vt-results.clean.tsv"
printf '\textra-cell\n' >> "$FIX/binaries/vt-results.tsv"
if run_notes "$FIX/current.md" "$FIX/surplus-capture.md"; then
  fail "surplus result TSV cells must fail closed"
fi
mv "$FIX/binaries/vt-results.clean.tsv" "$FIX/binaries/vt-results.tsv"

cat > "$FIX/reversed.md" <<'EOF'
Intro text.

<!-- cbm-security-verification:end -->
stale reversed data
<!-- cbm-security-verification:start -->
EOF
if run_notes "$FIX/reversed.md" "$FIX/reversed-capture.md"; then
  fail "reversed verification markers must fail closed"
fi

mkdir -p "$FIX/public"
run_publish() {
  (cd "$FIX" &&
    PATH="$FIX/bin:$PATH" \
      GH_TOKEN=stub \
      VERSION=v1.0.0 \
      GITHUB_REPOSITORY=DeusData/codebase-memory-mcp \
      VT_ASSOCIATIONS=binaries/associations.tsv \
      VT_EXPECTED_SCAN_SET=binaries/scan-set.tsv \
      VT_RESULTS_PATH=binaries/vt-results.tsv \
      STUB_UPLOAD_DIR="$FIX/public" \
      bash "$PUBLISH")
}
cat > "$FIX/binaries/scan-set.tsv" <<EOF
# cbm-release-scan-set-v1
# scan_objects=2
# associations=3
scan_path	sha256	size	association_count	association_kinds
objects/archive	$SHA_A	100	2	archive,binary
objects/script	$SHA_B	20	1	ui_asset
EOF
run_publish
for asset in virustotal-associations.tsv virustotal-scan-set.tsv virustotal-results.tsv virustotal-evidence-checksums.txt; do
  [ -s "$FIX/public/$asset" ] || fail "public evidence asset was not uploaded: $asset"
done
[ "$(wc -l < "$FIX/public/virustotal-evidence-checksums.txt" | tr -d ' ')" = 3 ] || \
  fail "public evidence checksum inventory is incomplete"
cp "$FIX/binaries/scan-set.tsv" "$FIX/binaries/scan-set.clean.tsv"
sed '1s/cbm-release-scan-set-v1/wrong-marker/' "$FIX/binaries/scan-set.clean.tsv" > "$FIX/binaries/scan-set.tsv"
if run_publish; then
  fail "public evidence publication must reject a wrong scan-set marker"
fi
mv "$FIX/binaries/scan-set.clean.tsv" "$FIX/binaries/scan-set.tsv"

echo 'PASS: VT release evidence is durable, scoped, measured, full-hash and idempotent'

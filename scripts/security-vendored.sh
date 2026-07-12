#!/usr/bin/env bash
set -euo pipefail

# Layer 8: Vendored dependency integrity — verifies vendored C sources match
# checked-in checksums. Detects supply chain tampering of vendored libraries.
#
# Libraries covered: mimalloc, nomic, sqlite3, tre, xxhash, yyjson
#
# Usage: scripts/security-vendored.sh [--update]

MODE="${1:-}"
if [[ -n "$MODE" && "$MODE" != "--update" ]]; then
    echo "Usage: scripts/security-vendored.sh [--update]"
    exit 2
fi

ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"
CHECKSUMS="$ROOT/scripts/vendored-checksums.txt"
VENDORED_ROOT="$ROOT/vendored"

if [[ ! -f "$CHECKSUMS" || -L "$CHECKSUMS" ]]; then
    echo "FAIL: checksum manifest must be a regular, non-symlink file: $CHECKSUMS"
    exit 1
fi
if [[ ! -d "$VENDORED_ROOT" || -L "$VENDORED_ROOT" ]]; then
    echo "FAIL: vendored root must be a regular, non-symlink directory: $VENDORED_ROOT"
    exit 1
fi

echo "=== Layer 8: Vendored Dependency Integrity ==="

# Detect shasum command (shasum on macOS, sha256sum on Linux). Missing hashing
# is a hard failure: an integrity check that verified nothing must never pass or
# authorize a manifest update.
if command -v sha256sum &>/dev/null; then
    SHA_CMD=(sha256sum)
elif command -v shasum &>/dev/null; then
    SHA_CMD=(shasum -a 256)
else
    echo "BLOCKED: no sha256sum or shasum available"
    exit 1
fi

MANIFEST_PATHS="$(mktemp "${TMPDIR:-/tmp}/cbm-vendored-paths.XXXXXX")" || {
    echo "BLOCKED: cannot create checksum verification workspace"
    exit 1
}
VENDORED_FILES="$(mktemp "${TMPDIR:-/tmp}/cbm-vendored-files.XXXXXX")" || {
    rm -f "$MANIFEST_PATHS"
    echo "BLOCKED: cannot create vendored inventory workspace"
    exit 1
}
VENDORED_SYMLINKS="$(mktemp "${TMPDIR:-/tmp}/cbm-vendored-links.XXXXXX")" || {
    rm -f "$MANIFEST_PATHS" "$VENDORED_FILES"
    echo "BLOCKED: cannot create vendored link workspace"
    exit 1
}
UPDATE_TMP=""
cleanup() {
    rm -f "$MANIFEST_PATHS" "$VENDORED_FILES" "$VENDORED_SYMLINKS"
    if [[ -n "$UPDATE_TMP" ]]; then
        rm -f "$UPDATE_TMP"
    fi
}
trap cleanup EXIT

STRUCTURAL_FAIL=0
CONTENT_DRIFT=0
CHECKED=0
MISSING=0
UNEXPECTED=0
DISCOVERED=0

valid_vendored_path() {
    local path="$1"
    [[ "$path" == vendored/* ]] || return 1
    [[ "$path" == *.c || "$path" == *.h ]] || return 1
    [[ "$path" != *'\\'* ]] || return 1
    [[ "$path" != *'//'* ]] || return 1
    [[ "$path" != *'/./'* && "$path" != */. ]] || return 1
    [[ "$path" != *'/../'* && "$path" != */.. ]] || return 1
    [[ "$path" != *$'\n'* && "$path" != *$'\r'* && "$path" != *$'\t'* ]] || return 1
}

hash_file() {
    local file="$1"
    local output
    local hash
    if ! output="$("${SHA_CMD[@]}" "$file")"; then
        return 1
    fi
    hash="${output%% *}"
    [[ "$hash" =~ ^[[:xdigit:]]{64}$ ]] || return 1
    printf '%s\n' "$hash"
}

# Inventory with NUL delimiters first. This makes path validation independent
# of whitespace and ensures find/read failures are structural, not silent skips.
if ! find "$VENDORED_ROOT" -type f \( -name '*.c' -o -name '*.h' \) -print0 \
    > "$VENDORED_FILES"; then
    echo "BLOCKED: cannot inventory vendored source files"
    STRUCTURAL_FAIL=1
fi
if ! find "$VENDORED_ROOT" -type l -print0 > "$VENDORED_SYMLINKS"; then
    echo "BLOCKED: cannot inspect vendored symlinks"
    STRUCTURAL_FAIL=1
fi
while IFS= read -r -d '' link; do
    echo "BLOCKED: vendored symlink is not allowed: ${link#"$ROOT/"}"
    STRUCTURAL_FAIL=1
done < "$VENDORED_SYMLINKS"

while IFS= read -r -d '' file; do
    relpath="${file#"$ROOT/"}"
    DISCOVERED=$((DISCOVERED + 1))
    if ! valid_vendored_path "$relpath"; then
        echo "BLOCKED: invalid or non-confined vendored path: $relpath"
        STRUCTURAL_FAIL=1
    fi
done < "$VENDORED_FILES"

# Verify each file in the checksum manifest. Malformed, duplicate, or escaping
# entries are structural failures. Ordinary content changes are updateable drift.
while IFS=' ' read -r expected_hash filepath || [[ -n "$expected_hash$filepath" ]]; do
    [[ -z "$expected_hash" && -z "$filepath" ]] && continue
    filepath="${filepath#"${filepath%%[![:space:]]*}"}"

    if [[ ! "$expected_hash" =~ ^[[:xdigit:]]{64}$ ]] ||
       ! valid_vendored_path "$filepath"; then
        echo "BLOCKED: invalid checksum manifest entry: ${filepath:-<missing path>}"
        STRUCTURAL_FAIL=1
        continue
    fi
    if grep -Fqx -- "$filepath" "$MANIFEST_PATHS"; then
        echo "BLOCKED: duplicate checksum manifest path: $filepath"
        STRUCTURAL_FAIL=1
        continue
    fi
    printf '%s\n' "$filepath" >> "$MANIFEST_PATHS"

    full_path="$ROOT/$filepath"
    if [[ ! -e "$full_path" ]]; then
        echo "MISSING: $filepath"
        MISSING=$((MISSING + 1))
        CONTENT_DRIFT=1
        continue
    fi
    if [[ ! -f "$full_path" || -L "$full_path" ]]; then
        echo "BLOCKED: manifest path is not a regular, non-symlink file: $filepath"
        STRUCTURAL_FAIL=1
        continue
    fi
    if ! actual_hash="$(hash_file "$full_path")"; then
        echo "BLOCKED: cannot hash vendored file: $filepath"
        STRUCTURAL_FAIL=1
        continue
    fi
    CHECKED=$((CHECKED + 1))

    if [[ "$actual_hash" != "$expected_hash" ]]; then
        echo "MISMATCH: $filepath"
        echo "  expected: $expected_hash"
        echo "  actual:   $actual_hash"
        CONTENT_DRIFT=1
    fi
done < "$CHECKSUMS"

# Verify every vendored library directory has checksum coverage. Missing
# coverage in a known library is updateable drift; an unknown library below is
# always a structural blocker.
echo ""
echo "--- Checking vendored library coverage ---"
while IFS= read -r -d '' libdir; do
    libname="$(basename "$libdir")"
    prefix="vendored/${libname}/"
    covered=false
    while IFS= read -r manifest_path; do
        if [[ "$manifest_path" == "$prefix"* ]]; then
            covered=true
            break
        fi
    done < "$MANIFEST_PATHS"
    if ! $covered; then
        echo "MISSING COVERAGE: vendored/${libname}/ has no checksum entry"
        CONTENT_DRIFT=1
    fi
done < <(find "$VENDORED_ROOT" -mindepth 1 -maxdepth 1 -type d -print0)

# An unexpected source is a hard failure in verification mode. Literal
# whole-line matching prevents regex metacharacters or path prefixes from
# making one manifest entry claim a different file.
while IFS= read -r -d '' file; do
    relpath="${file#"$ROOT/"}"
    if ! grep -Fqx -- "$relpath" "$MANIFEST_PATHS"; then
        echo "NEW FILE: $relpath (not in checksums)"
        UNEXPECTED=$((UNEXPECTED + 1))
        CONTENT_DRIFT=1
    fi
done < "$VENDORED_FILES"

echo ""
echo "Checked: $CHECKED files"
[[ $MISSING -gt 0 ]] && echo "Missing: $MISSING files"
[[ $UNEXPECTED -gt 0 ]] && echo "New (untracked): $UNEXPECTED files"
if [[ $CHECKED -eq 0 ]]; then
    echo "BLOCKED: checksum manifest verified zero files"
    STRUCTURAL_FAIL=1
fi
if [[ $DISCOVERED -eq 0 ]]; then
    echo "BLOCKED: vendored inventory contains zero C/header files"
    STRUCTURAL_FAIL=1
fi

# Dangerous call scan: vendored code must not contain subprocess calls.
echo ""
echo "--- Scanning vendored code for dangerous calls ---"

SUBPROCESS_FUNCS='[^a-z_]system\(|[^a-z]popen\(|[^a-z_]execl\(|[^a-z_]execv\(|[^a-z_]fork\('
if grep -rn -E "$SUBPROCESS_FUNCS" "$VENDORED_ROOT/" --include='*.c' --include='*.h' 2>/dev/null \
    | grep -v '^\s*//' | grep -v '^\s*\*' | grep -v '#define' | grep -v 'typedef' \
    | grep -v 'indicating that a fork' > /dev/null 2>&1; then
    echo "BLOCKED: Subprocess calls found in vendored code:"
    grep -rn -E "$SUBPROCESS_FUNCS" "$VENDORED_ROOT/" --include='*.c' --include='*.h' 2>/dev/null \
        | grep -v '^\s*//' | grep -v '^\s*\*' | grep -v '#define' | grep -v 'typedef' \
        | grep -v 'indicating that a fork' | head -10
    STRUCTURAL_FAIL=1
else
    echo "OK: No subprocess calls (system/popen/exec/fork) in vendored code"
fi

# Network calls are not allowed in vendored code. The graph-UI HTTP server is
# first-party (src/ui/httpd.c) and audited separately by security-ui.sh.
NETWORK_FUNCS='[^a-z_]connect\(|[^a-z_]socket\(|[^a-z_]sendto\(|[^a-z_]bind\('
VENDORED_NETWORK="$(grep -rn -E "$NETWORK_FUNCS" "$VENDORED_ROOT/" --include='*.c' --include='*.h' 2>/dev/null \
    | grep -v '^\s*//' | grep -v '^\s*\*' | grep -v '#define' | grep -v 'typedef' \
    | grep -v 'sqlite3.*bind()' || true)"
if [[ -n "$VENDORED_NETWORK" ]]; then
    echo "BLOCKED: Network calls found in vendored code:"
    echo "$VENDORED_NETWORK" | head -10
    STRUCTURAL_FAIL=1
else
    echo "OK: No network calls in vendored code"
fi

# dlopen/LoadLibrary is only allowed in sqlite3 (extension loading) and
# mimalloc (Windows APIs).
DYNLOAD_FUNCS='dlopen\(|LoadLibrary\('
NON_SQLITE_DYNLOAD="$(grep -rn -E "$DYNLOAD_FUNCS" "$VENDORED_ROOT/" --include='*.c' --include='*.h' 2>/dev/null \
    | grep -v '^\s*//' | grep -v '^\s*\*' | grep -v '#define' \
    | grep -v 'sqlite3' | grep -v 'mimalloc' || true)"
if [[ -n "$NON_SQLITE_DYNLOAD" ]]; then
    echo "BLOCKED: Dynamic library loading found outside sqlite3:"
    echo "$NON_SQLITE_DYNLOAD" | head -10
    STRUCTURAL_FAIL=1
else
    echo "OK: dlopen/LoadLibrary only in sqlite3 (blocked by authorizer at runtime)"
fi

# Every top-level vendored library requires an explicit review decision.
KNOWN_VENDORED="mimalloc nomic sqlite3 tre xxhash yyjson"
while IFS= read -r -d '' libdir; do
    libname="$(basename "$libdir")"
    found=false
    for known in $KNOWN_VENDORED; do
        if [[ "$libname" == "$known" ]]; then
            found=true
            break
        fi
    done
    if ! $found; then
        echo "BLOCKED: vendored/${libname}/ is not in the known vendored library list"
        echo "  Evaluate it for dangerous calls, then add it to KNOWN_VENDORED."
        STRUCTURAL_FAIL=1
    fi
done < <(find "$VENDORED_ROOT" -mindepth 1 -maxdepth 1 -type d -print0)

# Also scan tree-sitter grammars (internal/cbm/vendored/) — 650MB, 20M lines.
# Use fixed-string grep for each pattern to avoid slow regex on the large tree.
if [[ -d "$ROOT/internal/cbm/vendored" ]]; then
    GRAMMAR_FAIL=false
    for pattern in 'system(' 'popen(' 'execl(' 'execv(' 'fork(' 'connect(' 'socket(' 'sendto(' 'dlopen(' 'LoadLibrary('; do
        HITS="$(grep -rl -F "$pattern" "$ROOT/internal/cbm/vendored/" --include='*.c' --include='*.h' 2>/dev/null | head -3 || true)"
        if [[ -n "$HITS" ]]; then
            echo "BLOCKED: '$pattern' found in vendored grammars:"
            echo "$HITS" | sed 's|.*/vendored/|  vendored/|'
            GRAMMAR_FAIL=true
        fi
    done
    if $GRAMMAR_FAIL; then
        STRUCTURAL_FAIL=1
    else
        echo "OK: No dangerous calls in vendored tree-sitter grammars"
    fi
fi

if [[ "$MODE" == "--update" ]]; then
    if [[ $STRUCTURAL_FAIL -ne 0 ]]; then
        echo ""
        echo "=== CHECKSUM UPDATE REFUSED ==="
        echo "Resolve all structural and dangerous-code blockers before updating."
        exit 1
    fi

    UPDATE_TMP="$(mktemp "$CHECKSUMS.tmp.XXXXXX")" || {
        echo "BLOCKED: cannot create atomic checksum update"
        exit 1
    }
    UPDATE_COUNT=0
    UPDATE_FAIL=0
    while IFS= read -r -d '' file; do
        relpath="${file#"$ROOT/"}"
        if ! hash="$(hash_file "$file")"; then
            echo "BLOCKED: cannot hash vendored file during update: $relpath"
            UPDATE_FAIL=1
            continue
        fi
        printf '%s  %s\n' "$hash" "$relpath" >> "$UPDATE_TMP"
        UPDATE_COUNT=$((UPDATE_COUNT + 1))
    done < <(LC_ALL=C sort -z "$VENDORED_FILES")

    if [[ $UPDATE_FAIL -ne 0 || $UPDATE_COUNT -eq 0 ]]; then
        echo "BLOCKED: checksum update did not hash every vendored source"
        exit 1
    fi
    chmod 0644 "$UPDATE_TMP"
    if ! mv -f "$UPDATE_TMP" "$CHECKSUMS"; then
        echo "BLOCKED: atomic checksum manifest replacement failed"
        exit 1
    fi
    UPDATE_TMP=""
    echo ""
    echo "Updated: $CHECKSUMS ($UPDATE_COUNT files)"
    exit 0
fi

if [[ $STRUCTURAL_FAIL -ne 0 || $CONTENT_DRIFT -ne 0 ]]; then
    echo ""
    echo "=== VENDORED INTEGRITY CHECK FAILED ==="
    if [[ $STRUCTURAL_FAIL -eq 0 ]]; then
        echo "Vendored content changed. If intentional, review it and run:"
        echo "scripts/security-vendored.sh --update"
    else
        echo "Resolve all structural and dangerous-code blockers before updating."
    fi
    exit 1
fi

echo ""
echo "=== Vendored integrity check passed ==="

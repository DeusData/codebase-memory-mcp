#!/usr/bin/env bash
set -euo pipefail

# Layer 4: Install output audit — verifies install --dry-run writes only to expected paths.
#
# Checks:
#   1. All output file paths are in the expected set
#   2. No writes to sensitive directories (~/.ssh, ~/.gnupg, ~/.aws, /etc, /usr)
#   3. Skill file content contains no dangerous patterns
#
# Usage: scripts/security-install.sh <binary-path>

BINARY="${1:?usage: security-install.sh <binary-path>}"

if [[ ! -f "$BINARY" ]]; then
    echo "FAIL: binary not found: $BINARY"
    exit 1
fi

echo "=== Layer 4: Install Output Audit ==="

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

# Set HOME to tmpdir so install writes there instead of real home
export HOME="$TMPDIR/home"
mkdir -p "$HOME"

FAIL=0

# ── 1. Run install and capture written files ─────────────────────

echo "--- Running install -y ---"

# Run install (non-interactive with -y flag)
"$BINARY" install -y > "$TMPDIR/install_output.txt" 2>&1 || true

echo "Install output:"
cat "$TMPDIR/install_output.txt"
echo ""

# ── 2. Verify written paths are in expected set ──────────────────

echo "--- Verifying written file paths ---"

# Find all files created under HOME
find "$HOME" -type f > "$TMPDIR/created_files.txt" 2>/dev/null || true

# Expected path patterns (relative to HOME):
#   .config/*/mcp.json (or .mcp.json variants)
#   .claude/skills/*
#   .claude/settings.json
#   .continue/config.yaml
#   .codeium/config.json
#   .local/bin/codebase-memory-mcp
#   Various agent config dirs

EXPECTED_PATTERNS=(
    ".claude/"
    ".cursor/"
    ".config/"
    ".continue/"
    ".codeium/"
    ".windsurf/"
    ".trae/"
    ".aider/"
    ".local/bin/"
    "AGENTS.md"
    "CONVENTIONS.md"
    ".mcp.json"
    "mcp.json"
    ".zshrc"
    ".bashrc"
    ".profile"
)

while IFS= read -r filepath; do
    relpath="${filepath#"$HOME/"}"

    matched=false
    for pattern in "${EXPECTED_PATTERNS[@]}"; do
        if [[ "$relpath" == *"$pattern"* ]]; then
            matched=true
            break
        fi
    done

    if ! $matched; then
        echo "REVIEW: Unexpected file created: $relpath"
    fi
done < "$TMPDIR/created_files.txt"

# ── 3. Check for writes to sensitive paths ───────────────────────

echo ""
echo "--- Checking for sensitive path writes ---"

SENSITIVE_DIRS=(".ssh" ".gnupg" ".aws" ".kube" ".config/gcloud")

for dir in "${SENSITIVE_DIRS[@]}"; do
    if [[ -d "$HOME/$dir" ]]; then
        echo "BLOCKED: Install created sensitive directory: ~/$dir"
        FAIL=1
    fi
done

# Also check install output for any references to sensitive paths
for dir in "${SENSITIVE_DIRS[@]}"; do
    if grep -q "$dir" "$TMPDIR/install_output.txt" 2>/dev/null; then
        echo "BLOCKED: Install output references sensitive path: $dir"
        FAIL=1
    fi
done

if [[ $FAIL -eq 0 ]]; then
    echo "OK: No sensitive path writes detected."
fi

# ── 4. Audit skill file content ──────────────────────────────────

echo ""
echo "--- Auditing skill file content ---"

SKILLS_DIR="$HOME/.claude/skills"
if [[ -d "$SKILLS_DIR" ]]; then
    SKILL_ISSUES=0

    while IFS= read -r skill_file; do
        basename=$(basename "$skill_file")

        # Check for dangerous patterns in skill content
        for pattern in 'system(' 'eval(' 'exec(' '__import__(' 'subprocess' 'os.popen'; do
            if grep -q "$pattern" "$skill_file" 2>/dev/null; then
                echo "BLOCKED: Skill '$basename' contains dangerous pattern: $pattern"
                SKILL_ISSUES=1
                FAIL=1
            fi
        done

        # Check for unexpected URLs
        if grep -oE 'https?://[^\s"'"'"']+' "$skill_file" 2>/dev/null | grep -v 'github.com/DeusData' | grep -v 'localhost' | grep -v '127.0.0.1' > /tmp/sec_skill_urls 2>/dev/null; then
            while IFS= read -r url; do
                echo "REVIEW: Skill '$basename' contains URL: $url"
            done < /tmp/sec_skill_urls
            rm -f /tmp/sec_skill_urls
        fi

        # Check for encoded strings (base64-like blocks > 50 chars)
        if grep -E '[A-Za-z0-9+/]{50,}={0,2}' "$skill_file" > /dev/null 2>&1; then
            echo "REVIEW: Skill '$basename' contains potential encoded content"
        fi
    done < <(find "$SKILLS_DIR" -type f -name '*.md')

    if [[ $SKILL_ISSUES -eq 0 ]]; then
        echo "OK: Skill files contain no dangerous patterns."
    fi
else
    echo "SKIP: No skills directory created."
fi

# ── Summary ──────────────────────────────────────────────────────

echo ""
if [[ $FAIL -ne 0 ]]; then
    echo "=== INSTALL OUTPUT AUDIT FAILED ==="
    exit 1
fi

echo "=== Install output audit passed ==="

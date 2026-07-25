#!/usr/bin/env bash
# vm-smoke.sh — run the PR smoke inside the Windows VM exactly as CI does.
#
# Mirrors .github/workflows/pr.yml's windows pr-smoke step: stage the release
# pair (launcher as codebase-memory-mcp.exe + CLI as .payload.exe) in a
# profile-rooted temp dir — MSYS2 /tmp is intentionally rejected by the
# launcher (shared writable ancestor), and so is anything under the msys
# install tree — then run scripts/smoke-test.sh against the staged launcher.
#
# Run inside the VM's CLANGARM64 shell from the repo root:
#   bash test-infrastructure/vm/vm-smoke.sh
# Or from the host: test-infrastructure/vm/win.sh smoke-install
set -euo pipefail

cd "$(dirname "$0")/../.."
[ -x build/c/codebase-memory-mcp.exe ] || { echo "build first (win.sh build)" >&2; exit 2; }

PROFILE_ROOT="$(cygpath -u "$USERPROFILE")"
SMOKE_DIR="$(mktemp -d "$PROFILE_ROOT/cbm-vm-smoke.XXXXXX")"
trap 'rm -rf "$SMOKE_DIR"' EXIT

cp build/c/codebase-memory-mcp-launcher.exe "$SMOKE_DIR/codebase-memory-mcp.exe"
cp build/c/codebase-memory-mcp.exe "$SMOKE_DIR/codebase-memory-mcp.payload.exe"

CBM_CACHE_DIR="$(cygpath -m "$SMOKE_DIR/cache")" \
    SMOKE_TEMP_ROOT="$SMOKE_DIR" \
    scripts/smoke-test.sh "$SMOKE_DIR/codebase-memory-mcp.exe"

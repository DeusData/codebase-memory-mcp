#!/usr/bin/env bash
# env.sh — Shared environment detection for all build scripts.
#
# Sourced by test.sh, build.sh, lint.sh. Not meant to run standalone.
#
# Exports:
#   ARCH        — target architecture (arm64 / x86_64)
#   ARCH_PREFIX — "arch -arm64" on macOS, empty on Linux/Windows
#   NPROC       — number of CPU cores
#   OS          — darwin / linux / windows

set -euo pipefail

# ── Detect OS ──────────────────────────────────────────────────
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$OS" in
    darwin)  OS="darwin" ;;
    linux)   OS="linux" ;;
    mingw*|msys*|cygwin*) OS="windows" ;;
    *)       OS="unknown" ;;
esac

# ── Detect / override architecture ─────────────────────────────
# Default: native HARDWARE architecture (not Rosetta-translated).
# On macOS under Rosetta, uname -m returns x86_64 even on Apple Silicon.
# We use sysctl to detect the true hardware.
HW_ARCH="$(uname -m)"
if [[ "$(uname -s)" == "Darwin" ]] && sysctl -n hw.optional.arm64 2>/dev/null | grep -q 1; then
    HW_ARCH="arm64"
fi
case "$HW_ARCH" in
    aarch64|arm64) HW_ARCH="arm64" ;;
    x86_64|amd64)  HW_ARCH="x86_64" ;;
esac

# CBM_ARCH env var or --arch flag override (parsed by calling script)
ARCH="${CBM_ARCH:-$HW_ARCH}"

# ── Build arch prefix (macOS only) ─────────────────────────────
ARCH_PREFIX=""
if [[ "$OS" == "darwin" ]]; then
    ARCH_PREFIX="arch -${ARCH}"
fi

# ── Detect parallelism ─────────────────────────────────────────
NPROC=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# ── Verify compiler is available for target arch ───────────────
verify_compiler() {
    local compiler="$1"
    local bin
    bin="$(command -v "$compiler" 2>/dev/null || true)"

    if [[ -z "$bin" ]]; then
        echo "ERROR: compiler '$compiler' not found in PATH" >&2
        exit 1
    fi

    if [[ "$OS" == "darwin" ]]; then
        local file_info
        file_info="$(file "$bin" 2>/dev/null)"

        if [[ "$ARCH" == "arm64" ]] && ! echo "$file_info" | grep -qE "arm64|universal"; then
            echo "WARNING: $compiler ($bin) is x86_64 only — cannot build native arm64" >&2
            echo "  Install arm64 GCC: arch -arm64 /opt/homebrew/bin/brew install gcc" >&2
            echo "  Or override: scripts/test.sh --arch x86_64" >&2
            exit 1
        fi

        if [[ "$ARCH" == "x86_64" ]] && ! echo "$file_info" | grep -qE "x86_64|universal"; then
            echo "WARNING: $compiler ($bin) is arm64 only — cannot build x86_64" >&2
            exit 1
        fi
    fi
}

# ── Default compiler selection ─────────────────────────────────
# macOS: cc (Apple Clang). Linux/Windows: gcc (system default).
# CI overrides via CC=gcc CXX=g++ args. Local macOS overrides via CC=cc.
if [[ -z "${CC:-}" ]]; then
    if [[ "$OS" == "darwin" ]]; then
        export CC=cc CXX=c++
    else
        export CC=gcc CXX=g++
    fi
fi

# ── Print environment summary ──────────────────────────────────
print_env() {
    local context="$1"
    echo "=== $context: os=$OS arch=$ARCH cores=$NPROC cc=${CC:-default} cxx=${CXX:-default} ==="
}

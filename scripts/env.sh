#!/bin/bash
# env.sh — Shared environment detection for all build scripts.
#
# Sourced by test.sh, build.sh, lint.sh. Not meant to run standalone.
#
# Exports:
#   ARCH        — target architecture (arm64 / x86_64)
#   ARCHFLAGS   — "-arch <arch>" on macOS (target slice for clang/ld), empty elsewhere
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

# ── Target-architecture flags (macOS only) ─────────────────────
# Select the target slice explicitly with clang/ld's -arch instead of
# relaunching make under `arch -<arch>`. Explicit -arch is the only approach
# that works across toolchains: a Nix/Homebrew clang wrapper has a fixed
# target triple, so `arch -x86_64 make` would still emit native arm64. It also
# lets host tools (node, codegen) keep running natively during a cross-build.
# Makefile.cbm folds $(ARCHFLAGS) into CC/CXX so it reaches every compile and
# link, including the vendored objects. Empty on Linux/Windows.
ARCHFLAGS=""
if [[ "$OS" == "darwin" ]]; then
    ARCHFLAGS="-arch ${ARCH}"
fi
export ARCHFLAGS

# ── Detect parallelism ─────────────────────────────────────────
NPROC=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# ── Verify compiler can build for the target arch ──────────────
# On macOS, PROBE actual capability instead of inspecting the compiler's own
# Mach-O header. The driver is often a wrapper script (Nix, ccache, Homebrew)
# or a clang that cross-compiles, so `file` on the binary says nothing about
# what it can target — it just sees a shell script or a host-arch executable.
# Compiling + linking a trivial program with -arch <target> is the truth.
verify_compiler() {
    local compiler="$1"
    local bin
    bin="$(command -v "$compiler" 2>/dev/null || true)"

    if [[ -z "$bin" ]]; then
        echo "ERROR: compiler '$compiler' not found in PATH" >&2
        exit 1
    fi

    if [[ "$OS" == "darwin" ]]; then
        local probe_out
        probe_out="$(mktemp -t cbm-archprobe.XXXXXX)"
        if ! printf 'int main(void){return 0;}\n' \
            | "$compiler" -arch "$ARCH" -x c - -o "$probe_out" >/dev/null 2>&1; then
            rm -f "$probe_out"
            echo "ERROR: $compiler cannot build for -arch $ARCH ($bin)" >&2
            echo "  A trivial $ARCH compile + link failed with this toolchain." >&2
            if [[ "$ARCH" != "$HW_ARCH" ]]; then
                echo "  Cross-building $HW_ARCH -> $ARCH needs the $ARCH SDK slice + runtime;" >&2
                echo "  to build for this machine instead, re-run with: --arch $HW_ARCH" >&2
            fi
            exit 1
        fi
        rm -f "$probe_out"
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

#!/usr/bin/env bash
# env.sh — Shared environment detection for all build scripts.
#
# Sourced by test.sh, build.sh, lint.sh. Not meant to run standalone.
#
# Exports:
#   ARCH        — target architecture (arm64 / x86_64)
#   ARCHFLAGS   — "-arch <arch>" on macOS (target slice for clang/ld), empty elsewhere
#   MAKE        — make program for Makefile.cbm (gmake on FreeBSD, make elsewhere)
#   NPROC       — number of CPU cores
#   OS          — darwin / linux / windows / freebsd

set -euo pipefail

# ── Detect OS ──────────────────────────────────────────────────
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$OS" in
    darwin)  OS="darwin" ;;
    linux)   OS="linux" ;;
    freebsd*) OS="freebsd" ;;
    mingw*|msys*|cygwin*) OS="windows" ;;
    *)       OS="unknown" ;;
esac

# ── Select the make program ────────────────────────────────────
# Makefile.cbm is written in GNU make syntax. FreeBSD ships BSD make as its
# default make, so builds there run gmake (pkg install gmake); every other
# platform runs make. Not exported: build.sh sources this file and runs
# $MAKE in the same shell.
if [[ "$OS" == "freebsd" ]]; then
    MAKE="gmake"
else
    MAKE="make"
fi

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

# ── Verified compiler cache (ccache, opt-out CBM_NO_CCACHE=1) ──
# Activated through ccache's masquerade directories so $CC keeps its plain
# name everywhere (verify_compiler, make, link lines are untouched).
# Zero-staleness guarantee: CCACHE_COMPILERCHECK=content keys every entry on
# the CONTENT of the compiler binary plus the fully preprocessed translation
# unit, so a cache hit is provably the identical compilation — a stale or
# foreign cache can only MISS, never return wrong output. No CCACHE_BASEDIR
# and no path rewriting: debug-info and sanitizer report paths stay exact.
if [[ "${CBM_NO_CCACHE:-0}" != "1" ]] && command -v ccache >/dev/null 2>&1; then
    for _cbm_ccache_masq in \
        /usr/lib/ccache \
        /opt/homebrew/opt/ccache/libexec \
        /usr/local/opt/ccache/libexec \
        /clang64/lib/ccache/bin \
        /clangarm64/lib/ccache/bin; do
        if [[ -d "$_cbm_ccache_masq" ]]; then
            case ":$PATH:" in
            *":$_cbm_ccache_masq:"*) ;;
            *) PATH="$_cbm_ccache_masq:$PATH" ;;
            esac
        fi
    done
    unset _cbm_ccache_masq
    export PATH
    export CCACHE_COMPILERCHECK=content
fi

# ── CPU summary (model + SHA-256 instructions) ─────────────────
# src/foundation/sha256.c hashes on x86 SHA-NI / ARMv8 SHA2 when the CPU has
# them and on its portable transform otherwise. A sanitized test runner
# fingerprints its own ~800 MB image that way, so on a runner without those
# instructions every fingerprint is many times slower; this makes the CPU a
# hosted job drew visible in its log. Best effort and never fatal: anything
# unreadable prints "unknown".
cpu_summary() {
    local model="" flags="" key="sha_ni" has="unknown"
    if [[ "$HW_ARCH" == "arm64" || "${MSYSTEM:-}" == "CLANGARM64" ]]; then
        key="sha2"
    fi
    if [[ "$OS" == "darwin" ]]; then
        model="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)"
        if [[ "$key" == "sha2" ]]; then
            flags="$(sysctl -n hw.optional.arm.FEAT_SHA256 2>/dev/null || true)"
            case "$flags" in
                1) has="yes" ;;
                0) has="no" ;;
            esac
        else
            flags="$(sysctl -n machdep.cpu.leaf7_features 2>/dev/null || true)"
            if [[ -n "$flags" ]]; then
                case " $flags " in
                    *" SHA "*) has="yes" ;;
                    *) has="no" ;;
                esac
            fi
        fi
    elif [[ -r /proc/cpuinfo ]]; then
        model="$(awk -F': *' '/^model name/ {print $2; exit}' /proc/cpuinfo 2>/dev/null || true)"
        # MSYS2's own runtime is x86-64 even on Windows ARM64, so there its
        # /proc/cpuinfo flags describe the emulated CPU, not the real one.
        if [[ "${MSYSTEM:-}" != "CLANGARM64" ]]; then
            flags="$(awk '/^(flags|Features)[[:space:]]*:/ {print; exit}' /proc/cpuinfo 2>/dev/null || true)"
        fi
        if [[ -n "$flags" ]]; then
            case " $flags " in
                *" $key "*) has="yes" ;;
                *) has="no" ;;
            esac
        fi
    fi
    if [[ -z "$model" ]] && command -v lscpu >/dev/null 2>&1; then
        model="$(lscpu 2>/dev/null | awk -F': *' '/^Model name/ {print $2; exit}' || true)"
    fi
    if [[ "$model" == "-" ]]; then
        model="" # lscpu's placeholder for a CPU part it has no name for
    fi
    printf 'cpu="%s" %s=%s' "${model:-unknown}" "$key" "$has"
}

# ── Print environment summary ──────────────────────────────────
# print_env CONTEXT [--cpu]: --cpu appends cpu_summary to the same line.
print_env() {
    local context="$1"
    local cpu=""
    if [[ "${2:-}" == "--cpu" ]]; then
        cpu=" $(cpu_summary)"
    fi
    echo "=== $context: os=$OS arch=$ARCH cores=$NPROC cc=${CC:-default} cxx=${CXX:-default} make=$MAKE$cpu ==="
}

#!/usr/bin/env bash
# Contract: the setup scripts install through the installers' verified path.
#
# scripts/setup.sh and scripts/setup-windows.ps1 are the README's "Automated
# download + install" one-liners. They used to resolve the latest release,
# download the archive and unpack it on their own -- with none of the checks
# install.sh and install.ps1 make before anything is installed or run:
# checksums.txt must download, must list the archive, a hash tool must exist,
# and the digest must match. Both setup scripts now hand the download to the
# installer, so there is exactly ONE implementation of download + verify.
#
# Functional leg (macOS/Linux host): setup.sh runs against a loopback fixture
# release built from a harmless stub "binary", in four variants. A correct
# checksums.txt installs (file mode and `curl | bash` pipe mode); a wrong
# digest, a missing checksums.txt and a PATH without any hash tool each stop
# the script with a non-zero exit, nothing installed and the stub never run.
# Everything lives in a scratch HOME under a private work directory; no real
# release is ever downloaded or executed, and the network is unreachable for
# the whole run (every proxy variable points at a closed loopback port; the
# fixture is reached with --noproxy).
#
# Static leg (every host): setup-windows.ps1 must delegate to install.ps1,
# must not resolve, download or unpack the release itself, must stage in a
# fresh directory of its own (not directly under %TEMP%), must honour CBM_DOWNLOAD_URL, and must stay
# pure ASCII (Windows PowerShell 5.1 reads a BOM-less file as ANSI). The
# functional equivalent is the Windows VM leg, which serves the same kind of
# fixture to install.ps1 (test-infrastructure/vm/vm-smoke.sh).

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SETUP_SH="$ROOT/scripts/setup.sh"
SETUP_PS1="$ROOT/scripts/setup-windows.ps1"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

# ── Static contract: scripts/setup.sh ──────────────────────────────────────
#
# These gate the functional leg on purpose: a setup.sh that ignores
# CBM_DOWNLOAD_URL cannot be pointed at the fixture and would reach for the
# real release. The contract refuses to run that script at all.
grep -q 'CBM_DOWNLOAD_URL' "$SETUP_SH" ||
    fail "scripts/setup.sh must honour CBM_DOWNLOAD_URL (the installers' download-base override)"
grep -q 'install\.sh' "$SETUP_SH" ||
    fail "scripts/setup.sh must install through install.sh"
if grep -Eq 'tar +-[a-zA-Z]*x' "$SETUP_SH"; then
    fail "scripts/setup.sh must not unpack the release archive itself"
fi
if grep -Eq 'api\.github\.com|releases/download' "$SETUP_SH"; then
    fail "scripts/setup.sh must not resolve or download the release itself (install.sh owns that)"
fi

# ── Static contract: scripts/setup-windows.ps1 ─────────────────────────────
if LC_ALL=C grep -n $'[^ -~\t]' "$SETUP_PS1"; then
    fail "scripts/setup-windows.ps1 must be pure ASCII (PS 5.1 reads BOM-less files as ANSI)"
fi
if grep -q $'\r' "$SETUP_PS1"; then
    fail "scripts/setup-windows.ps1 must use LF line endings in the repository"
fi
grep -q 'install\.ps1' "$SETUP_PS1" ||
    fail "scripts/setup-windows.ps1 must install through install.ps1"
grep -q 'CBM_DOWNLOAD_URL' "$SETUP_PS1" ||
    fail "scripts/setup-windows.ps1 must honour CBM_DOWNLOAD_URL (the installers' download-base override)"
if grep -q 'Expand-Archive' "$SETUP_PS1"; then
    fail "scripts/setup-windows.ps1 must not unpack the release archive itself"
fi
if grep -Eq 'api\.github\.com|releases/download' "$SETUP_PS1"; then
    fail "scripts/setup-windows.ps1 must not resolve or download the release itself (install.ps1 owns that)"
fi
if grep -q 'env:TEMP' "$SETUP_PS1"; then
    fail "scripts/setup-windows.ps1 must stage in a fresh directory of its own, not directly under %TEMP%"
fi
grep -q 'GetTempPath' "$SETUP_PS1" && grep -q 'Get-Random' "$SETUP_PS1" ||
    fail "scripts/setup-windows.ps1 must stage in a freshly created private directory (as install.ps1 does)"

# No PowerShell parser is available on every host; a bracket balance over the
# file with comments and quoted strings removed still catches a truncated or
# mis-pasted script before the Windows leg does.
python3 - "$SETUP_PS1" <<'PY' || fail "scripts/setup-windows.ps1 has unbalanced brackets"
import re
import sys

text = open(sys.argv[1], encoding="ascii").read()
text = re.sub(r"<#.*?#>", "", text, flags=re.S)
text = re.sub(r"#[^\n]*", "", text)
text = re.sub(r"'(?:[^']|'')*'", "''", text)
text = re.sub(r'"(?:[^"\\`]|`.|\\.)*"', '""', text)
for open_ch, close_ch in (("{", "}"), ("(", ")"), ("[", "]")):
    if text.count(open_ch) != text.count(close_ch):
        print(f"unbalanced {open_ch}{close_ch}: {text.count(open_ch)} vs {text.count(close_ch)}")
        sys.exit(1)
PY

echo "OK: static contract (setup.sh delegates to install.sh, setup-windows.ps1 to install.ps1)"

# ── Functional leg: setup.sh against a loopback fixture release ────────────
case "$(uname -s)" in
    Darwin) OS=darwin ;;
    Linux) OS=linux ;;
    *)
        # WHY: setup.sh is the macOS/Linux one-liner; on a Windows host it would
        # select no archive at all. The Windows setup script's functional
        # coverage is the VM leg that serves a fixture to install.ps1 (the
        # script it delegates to). Tried: running the POSIX leg under MSYS --
        # install.sh then targets a windows-*.zip, which is not what setup.sh
        # ever installs, so the run would prove nothing about this contract.
        echo "SKIP: functional leg needs a macOS/Linux host ($(uname -s)); static contract passed"
        exit 0
        ;;
esac
case "$(uname -m)" in
    arm64 | aarch64) ARCH=arm64 ;;
    x86_64 | amd64)
        # Match install.sh: a Rosetta shell reports x86_64 on Apple Silicon.
        if [ "$OS" = "darwin" ] &&
            sysctl -n machdep.cpu.brand_string 2>/dev/null | grep -qi apple; then
            ARCH=arm64
        else
            ARCH=amd64
        fi
        ;;
    *) fail "unsupported host architecture $(uname -m)" ;;
esac
PORTABLE=""
[ "$OS" = "linux" ] && PORTABLE="-portable"
ARCHIVE="codebase-memory-mcp-${OS}-${ARCH}${PORTABLE}.tar.gz"

for tool in python3 curl tar shasum; do
    command -v "$tool" >/dev/null 2>&1 || command -v sha256sum >/dev/null 2>&1 ||
        fail "functional leg needs $tool"
done

WORK=$(mktemp -d "${TMPDIR:-/tmp}/cbm-setup-contract-XXXXXX")
chmod 700 "$WORK"
SERVER_PID=""
cleanup() {
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT

FIXTURE="$WORK/fixture"
STAGE="$WORK/stage"
mkdir -p "$FIXTURE/good" "$FIXTURE/bad" "$FIXTURE/none" "$STAGE" "$WORK/tmp" \
    "$WORK/cache" "$WORK/runtime" "$WORK/trace" "$WORK/cwd"

# The fixture "binary": a stub that answers --version and performs the minimal
# `install` the installer asks of the product (copy itself into --dir). It
# records every invocation so the failure cases can prove it never ran.
cat > "$STAGE/codebase-memory-mcp" <<'STUB'
#!/bin/sh
set -eu
if [ -n "${CBM_SETUP_STUB_TRACE:-}" ]; then
    printf '%s\n' "$*" >> "$CBM_SETUP_STUB_TRACE"
fi
case "${1:-}" in
    --version)
        echo "codebase-memory-mcp setup-contract-stub 0.0.0"
        ;;
    install)
        dir=""
        for arg in "$@"; do
            case "$arg" in --dir=*) dir="${arg#--dir=}" ;; esac
        done
        [ -n "$dir" ] || { echo "stub: install needs --dir=" >&2; exit 2; }
        mkdir -p "$dir"
        cp "$0" "$dir/codebase-memory-mcp"
        chmod 755 "$dir/codebase-memory-mcp"
        ;;
    *)
        echo "stub: unexpected invocation: $*" >&2
        exit 2
        ;;
esac
STUB
chmod 755 "$STAGE/codebase-memory-mcp"
cp "$ROOT/LICENSE" "$ROOT/install.sh" "$STAGE/"
printf 'fixture notices\n' > "$STAGE/THIRD_PARTY_NOTICES.md"

# Member set and order mirror scripts/package-release.sh; install.sh refuses
# any other inventory.
tar -czf "$STAGE/$ARCHIVE" -C "$STAGE" \
    codebase-memory-mcp LICENSE install.sh THIRD_PARTY_NOTICES.md
if command -v sha256sum >/dev/null 2>&1; then
    DIGEST=$(sha256sum "$STAGE/$ARCHIVE" | awk '{print $1}')
else
    DIGEST=$(shasum -a 256 "$STAGE/$ARCHIVE" | awk '{print $1}')
fi

for variant in good bad none; do
    cp "$STAGE/$ARCHIVE" "$ROOT/install.sh" "$FIXTURE/$variant/"
done
printf '%s  %s\n' "$DIGEST" "$ARCHIVE" > "$FIXTURE/good/checksums.txt"
WRONG_DIGEST=$(printf '%064d' 0)
printf '%s  %s\n' "$WRONG_DIGEST" "$ARCHIVE" > "$FIXTURE/bad/checksums.txt"
# "none" ships no checksums.txt at all.

# The server owns the ephemeral bind and prints its banner only after the
# port is published; reading that banner from a FIFO is the readiness signal
# (no polling, no timer). Request logs go to a file so nothing is written to
# the FIFO after the banner.
mkfifo "$WORK/server.fifo"
python3 "$ROOT/scripts/smoke-fixture-server.py" \
    --directory "$FIXTURE" --port-file "$WORK/port" \
    > "$WORK/server.fifo" 2> "$WORK/server.log" &
SERVER_PID=$!
IFS= read -r SERVER_BANNER < "$WORK/server.fifo" ||
    fail "fixture server exited before publishing its port"
PORT=$(tr -d '[:space:]' < "$WORK/port")
case "$PORT" in
    '' | *[!0-9]*) fail "fixture server published no usable port ($SERVER_BANNER)" ;;
esac

# Every tool setup.sh, install.sh and the stub may need, EXCEPT a hash tool.
# The no-hash-tool variant runs with PATH restricted to this directory.
mkdir -p "$WORK/tools"
for tool in bash sh env curl wget mktemp uname sysctl grep wc tr awk tar \
    xattr codesign chmod cp mv rm mkdir cat sed head dirname tput; do
    if resolved=$(command -v "$tool" 2>/dev/null) && [ -n "$resolved" ]; then
        ln -s "$resolved" "$WORK/tools/$tool"
    fi
done

# run_setup <label> <variant> <mode: file|pipe> <path> -> exit status in
# RUN_RC, output in $WORK/<label>.log, scratch HOME in $WORK/home-<label>.
run_setup() {
    local label="$1" variant="$2" mode="$3" path="$4"
    local home="$WORK/home-$label" log="$WORK/$label.log"
    local trace="$WORK/trace/$label"
    mkdir -p "$home"
    RUN_HOME="$home"
    RUN_LOG="$log"
    RUN_TRACE="$trace"
    RUN_RC=0
    # Run from a directory that is not a checkout: setup.sh activates git hooks
    # when it finds scripts/hooks in the current directory.
    if [ "$mode" = "pipe" ]; then
        cat "$SETUP_SH" | (cd "$WORK/cwd" && env \
            HOME="$home" USERPROFILE="$home" \
            XDG_CONFIG_HOME="$home/.config" CLAUDE_CONFIG_DIR="$home/.claude" \
            TMPDIR="$WORK/tmp" TEMP="$WORK/tmp" TMP="$WORK/tmp" \
            CBM_CACHE_DIR="$WORK/cache" CBM_RUNTIME_DIR="$WORK/runtime" \
            CBM_DOWNLOAD_URL="http://127.0.0.1:$PORT/$variant" \
            CBM_SETUP_STUB_TRACE="$trace" \
            http_proxy=http://127.0.0.1:1 https_proxy=http://127.0.0.1:1 \
            HTTP_PROXY=http://127.0.0.1:1 HTTPS_PROXY=http://127.0.0.1:1 \
            no_proxy= NO_PROXY= \
            PATH="$path" "$BASH") > "$log" 2>&1 || RUN_RC=$?
    else
        printf 'n\n' | (cd "$WORK/cwd" && env \
            HOME="$home" USERPROFILE="$home" \
            XDG_CONFIG_HOME="$home/.config" CLAUDE_CONFIG_DIR="$home/.claude" \
            TMPDIR="$WORK/tmp" TEMP="$WORK/tmp" TMP="$WORK/tmp" \
            CBM_CACHE_DIR="$WORK/cache" CBM_RUNTIME_DIR="$WORK/runtime" \
            CBM_DOWNLOAD_URL="http://127.0.0.1:$PORT/$variant" \
            CBM_SETUP_STUB_TRACE="$trace" \
            http_proxy=http://127.0.0.1:1 https_proxy=http://127.0.0.1:1 \
            HTTP_PROXY=http://127.0.0.1:1 HTTPS_PROXY=http://127.0.0.1:1 \
            no_proxy= NO_PROXY= \
            PATH="$path" "$BASH" "$SETUP_SH") > "$log" 2>&1 || RUN_RC=$?
    fi
}

show_log() {
    echo "--- $RUN_LOG ---" >&2
    cat "$RUN_LOG" >&2
    echo "--- server log ---" >&2
    cat "$WORK/server.log" >&2
}

assert_installed() {
    local label="$1" installed="$RUN_HOME/.local/bin/codebase-memory-mcp"
    if [ "$RUN_RC" -ne 0 ]; then
        show_log
        fail "$label: setup.sh exited $RUN_RC with a correct checksums.txt"
    fi
    [ -x "$installed" ] || { show_log; fail "$label: nothing installed at $installed"; }
    cmp -s "$STAGE/codebase-memory-mcp" "$installed" ||
        { show_log; fail "$label: installed file is not the fixture stub"; }
    grep -q 'Checksum verified\.' "$RUN_LOG" ||
        { show_log; fail "$label: the installer's checksum verification did not run"; }
    [ -s "$RUN_TRACE" ] || { show_log; fail "$label: the installed stub never ran"; }
}

assert_refused() {
    local label="$1" message="$2"
    if [ "$RUN_RC" -eq 0 ]; then
        show_log
        fail "$label: setup.sh exited 0; it must refuse to install"
    fi
    grep -q "$message" "$RUN_LOG" ||
        { show_log; fail "$label: expected the message '$message'"; }
    if [ -n "$(find "$RUN_HOME" -type f 2>/dev/null)" ]; then
        show_log
        fail "$label: setup.sh wrote into HOME although verification failed"
    fi
    [ ! -e "$RUN_TRACE" ] ||
        { show_log; fail "$label: the fixture binary ran although verification failed"; }
}

run_setup good-file good file "$PATH"
assert_installed "good/file"
grep -q '"GET /good/install.sh HTTP/1.1" 200' "$WORK/server.log" ||
    { show_log; fail "good/file: setup.sh did not fetch install.sh from the download base"; }
echo "OK: correct checksums.txt -> installed through install.sh (file mode)"

run_setup good-pipe good pipe "$PATH"
assert_installed "good/pipe"
echo "OK: correct checksums.txt -> installed through install.sh (curl | bash mode)"

run_setup bad bad pipe "$PATH"
assert_refused "bad" "CHECKSUM MISMATCH"
echo "OK: wrong digest -> refused, nothing installed, stub never ran"

run_setup none none pipe "$PATH"
assert_refused "none" "could not download checksums.txt"
echo "OK: missing checksums.txt -> refused, nothing installed, stub never ran"

run_setup no-hash-tool good pipe "$WORK/tools"
assert_refused "no-hash-tool" "sha256sum or shasum is required"
echo "OK: no hash tool on PATH -> refused, nothing installed, stub never ran"

echo "Setup-script contract passed"

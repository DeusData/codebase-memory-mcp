#!/usr/bin/env bash
# provision-windows.sh — provision the Windows test VM from the macOS host.
#
# Idempotent: safe to re-run at any time; every step skips work already done.
# Recovers a fresh VM (or a lost disk) to fully-built state in one command.
#
# Prerequisites (one-time, manual — see README.md):
#   1. UTM VM with Windows 11 ARM64 installed, CLONED as a backup before
#      drivers, SPICE guest tools installed (network), and
#      windows-bootstrap.ps1 run inside the VM (OpenSSH + authorized key).
#   2. Host config in ~/.claude/cbm-vm/config (KEY=VALUE), values printed by
#      windows-bootstrap.ps1 at the end of its run:
#        CBM_VM_HOST=<vm ip>
#        CBM_VM_USER=<vm user>
#        CBM_VM_HOST_KEY_SHA256=<SHA256:... host-key fingerprint>
#      Key at ~/.claude/cbm-vm/id_ed25519 (see README.md to generate your own;
#      never commit keys or this config).
#
# Usage:
#   test-infrastructure/vm/provision-windows.sh            # full provision
#   test-infrastructure/vm/provision-windows.sh --update   # repo+build only
set -euo pipefail

CONFIG="${HOME}/.claude/cbm-vm/config"
KEY="${HOME}/.claude/cbm-vm/id_ed25519"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# The fixed host-local config is intentionally outside this repository.
# shellcheck source=/dev/null
[ -f "$CONFIG" ] && . "$CONFIG"
HOST="${CBM_VM_HOST:?set CBM_VM_HOST in ~/.claude/cbm-vm/config}"
USER_="${CBM_VM_USER:-test}"
HOST_KEY="${CBM_VM_HOST_KEY_SHA256:?set CBM_VM_HOST_KEY_SHA256 in ~/.claude/cbm-vm/config}"
LOCAL_BRANCH="$(git -C "$ROOT" branch --show-current)"
BRANCH="${CBM_VM_BRANCH:-${LOCAL_BRANCH:-main}}"
REPO_URL="${CBM_VM_REPO:-https://github.com/DeusData/codebase-memory-mcp.git}"
MSYS2_SFX_URL="https://github.com/msys2/msys2-installer/releases/download/2026-06-11/msys2-base-x86_64-20260611.sfx.exe"
MSYS2_SFX_SHA256="c105946e64e08f099ac0e4647461ce762b95333ad211777666476a9a41451d65"

# shellcheck source=test-infrastructure/vm/ssh-common.sh
source "$SCRIPT_DIR/ssh-common.sh"
cbm_vm_require_safe_branch "$BRANCH"
cbm_vm_prepare_known_hosts "$HOST" "$HOST_KEY"
trap cbm_vm_cleanup_known_hosts EXIT
SSH_OPTIONS=(-i "$KEY" -o IdentitiesOnly=yes -o HostKeyAlgorithms=ssh-ed25519 \
             -o StrictHostKeyChecking=yes -o UserKnownHostsFile="$CBM_VM_KNOWN_HOSTS" \
             -o ConnectTimeout=10 -o BatchMode=yes)
SSH=(ssh "${SSH_OPTIONS[@]}" "${USER_}@${HOST}")
# Expand inside the remote MSYS2 shell, not on the macOS host.
# shellcheck disable=SC2016
JOBS='$(nproc)'

# Run a command inside the given msys2 environment (clangarm64|clang64|msys).
vm() { # vm <env> <command...>
    local env="$1"; shift
    "${SSH[@]}" "C:\\msys64\\msys2_shell.cmd -defterm -no-start -${env} -c \"set -e -o pipefail; $*\""
}
vm_cmd() { "${SSH[@]}" "$@"; } # plain cmd.exe

step() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }

step "0/6 reachability"
vm_cmd "echo VM_OK & ver" | grep -q VM_OK || {
    echo "FATAL: cannot reach ${USER_}@${HOST} over ssh." >&2
    echo "Run windows-bootstrap.ps1 inside the VM first (see README.md)." >&2
    exit 1
}

if [ "${1:-}" != "--update" ]; then
    step "1/6 msys2 base (skip if present)"
    if ! vm_cmd "if exist C:\\msys64\\usr\\bin\\bash.exe echo MSYS2_PRESENT" | grep -q MSYS2_PRESENT; then
        vm_cmd "powershell -NoProfile -Command \"\$ProgressPreference='SilentlyContinue'; [Net.ServicePointManager]::SecurityProtocol='Tls12'; Invoke-WebRequest -Uri '${MSYS2_SFX_URL}' -OutFile C:\\msys2.sfx.exe; \$actual=(Get-FileHash -LiteralPath C:\\msys2.sfx.exe -Algorithm SHA256).Hash.ToLowerInvariant(); if (\$actual -ne '${MSYS2_SFX_SHA256}') { Remove-Item -LiteralPath C:\\msys2.sfx.exe -Force; throw 'MSYS2 installer SHA-256 mismatch' }\""
        vm_cmd "C:\\msys2.sfx.exe -y -oC:\\" >/dev/null
        vm_cmd "del C:\\msys2.sfx.exe"
        vm "msys" "pacman-key --init && pacman-key --populate msys2"
    fi

    step "2/6 pacman full update (official two-pass sequence)"
    vm "msys" "pacman --noconfirm --noprogressbar -Syuu"
    vm "msys" "pacman --noconfirm --noprogressbar -Syuu"

    step "3/6 toolchains: CLANGARM64 (native, fast) + CLANG64 (x86_64 = CI arch, ASan)"
    vm "msys" "pacman -S --noconfirm --noprogressbar --needed \
        git make coreutils \
        mingw-w64-clang-aarch64-clang mingw-w64-clang-aarch64-zlib \
        mingw-w64-clang-aarch64-python mingw-w64-clang-aarch64-ccache \
        mingw-w64-clang-x86_64-clang mingw-w64-clang-x86_64-compiler-rt \
        mingw-w64-clang-x86_64-zlib mingw-w64-clang-x86_64-ccache"

    step "3b/6 official Node.js + VC++ runtime (guards UI build)"
    # MSVC-built npm native modules (rollup) cannot resolve Node-API symbols
    # against MSYS2's mingw node, and they need the VC++ runtime. Pinned
    # official builds, hash/signature-verified.
    NODE_VERSION="v22.23.1"
    NODE_SHA256="b470fdfe3502c05151656e06d495e3f47544f2ee8b1d9c8705090f2dd5996bd0"
    if ! vm_cmd "if exist C:\\node\\node.exe echo NODE_PRESENT" | grep -q NODE_PRESENT; then
        vm_cmd "powershell -NoProfile -Command \"\$ProgressPreference='SilentlyContinue'; Invoke-WebRequest -Uri 'https://nodejs.org/dist/${NODE_VERSION}/node-${NODE_VERSION}-win-arm64.zip' -OutFile C:\\node.zip; \$actual=(Get-FileHash -LiteralPath C:\\node.zip -Algorithm SHA256).Hash.ToLowerInvariant(); if (\$actual -ne '${NODE_SHA256}') { Remove-Item C:\\node.zip; throw 'node SHA-256 mismatch' }; Expand-Archive -LiteralPath C:\\node.zip -DestinationPath C:\\; Move-Item C:\\node-${NODE_VERSION}-win-arm64 C:\\node; Remove-Item C:\\node.zip\""
    fi
    if ! vm_cmd "if exist C:\\Windows\\System32\\vcruntime140.dll echo VCR_PRESENT" | grep -q VCR_PRESENT; then
        vm_cmd "powershell -NoProfile -Command \"\$ProgressPreference='SilentlyContinue'; Invoke-WebRequest -Uri 'https://aka.ms/vs/17/release/vc_redist.arm64.exe' -OutFile C:\\vc_redist.arm64.exe; \$sig = Get-AuthenticodeSignature C:\\vc_redist.arm64.exe; if (\$sig.Status -ne 'Valid') { throw 'vc_redist unsigned' }; Start-Process C:\\vc_redist.arm64.exe -ArgumentList '/install','/quiet','/norestart' -Wait; Remove-Item C:\\vc_redist.arm64.exe\""
    fi
fi

step "4/6 repo clone/update -> /c/cbm @ ${BRANCH}"
if vm "clangarm64" "test -d /c/cbm/.git && echo REPO_PRESENT" | grep -q REPO_PRESENT; then
    vm "clangarm64" "cd /c/cbm && git fetch origin ${BRANCH} && git reset --hard FETCH_HEAD && git clean -fdx && git log --oneline -1"
else
    vm "clangarm64" "git clone --branch ${BRANCH} --single-branch --depth 200 ${REPO_URL} /c/cbm && cd /c/cbm && git log --oneline -1"
fi

step "5/6 build: native ARM64 binary + launcher + test-runner (no ASan on arm64)"
vm "clangarm64" "cd /c/cbm && make -j${JOBS} -f Makefile.cbm CC=clang CXX=clang++ SANITIZE= cbm build/c/test-runner > /tmp/provision-build.log 2>&1 && echo BUILD_OK || (echo BUILD_FAIL; tail -15 /tmp/provision-build.log; exit 1)"

step "6/6 smoke: binary + test-runner start"
vm "clangarm64" "cd /c/cbm && ./build/c/codebase-memory-mcp.exe --version && ./build/c/test-runner --list-suites | head -3"

printf '\n\033[1mPROVISION COMPLETE\033[0m — daily driving: test-infrastructure/vm/win.sh\n'

#!/usr/bin/env bash
# Shared pinned-host verification for the local Windows VM drivers.

cbm_vm_require_safe_branch() {
    local branch="${1-}"
    if [[ ! "$branch" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*$ ]] ||
        ! git check-ref-format --branch "$branch" >/dev/null 2>&1; then
        echo "FATAL: CBM_VM_BRANCH is not a safe Git branch name: $branch" >&2
        return 1
    fi
}

cbm_vm_prepare_known_hosts() {
    local host="$1"
    local expected="$2"
    local actual

    if [[ ! "$expected" =~ ^SHA256:[A-Za-z0-9+/]{43}$ ]]; then
        echo "FATAL: CBM_VM_HOST_KEY_SHA256 is missing or malformed." >&2
        echo "  Copy the SHA256:... Ed25519 fingerprint printed by windows-bootstrap.ps1." >&2
        return 1
    fi

    CBM_VM_KNOWN_HOSTS="$(mktemp "${TMPDIR:-/tmp}/cbm-vm-known-hosts.XXXXXX")"
    chmod 600 "$CBM_VM_KNOWN_HOSTS"
    if ! ssh-keyscan -T 10 -t ed25519 "$host" >"$CBM_VM_KNOWN_HOSTS" 2>/dev/null; then
        echo "FATAL: could not read the Windows VM Ed25519 host key at $host." >&2
        cbm_vm_cleanup_known_hosts
        return 1
    fi
    actual="$(ssh-keygen -lf "$CBM_VM_KNOWN_HOSTS" -E sha256 2>/dev/null | awk '{print $2}')"
    if [ "$actual" != "$expected" ]; then
        echo "FATAL: Windows VM SSH host-key mismatch." >&2
        echo "  expected: $expected" >&2
        echo "  observed: ${actual:-unreadable}" >&2
        echo "  Stop: verify the fingerprint from the VM console; do not accept it blindly." >&2
        cbm_vm_cleanup_known_hosts
        return 1
    fi
}

cbm_vm_cleanup_known_hosts() {
    if [ -n "${CBM_VM_KNOWN_HOSTS:-}" ]; then
        rm -f -- "$CBM_VM_KNOWN_HOSTS"
        CBM_VM_KNOWN_HOSTS=""
    fi
}

# Send PowerShell source without exposing its metacharacters to the remote
# account shell. OpenSSH joins remote argv into one command string; raw
# parentheses, pipes, and semicolons can therefore be reinterpreted before
# PowerShell sees them. Windows PowerShell's encoded-command contract is
# UTF-16LE base64, produced locally from trusted repository-owned source.
# Usage: cbm_vm_run_powershell '<source>' "${SSH[@]}"
cbm_vm_run_powershell() {
    local source="${1-}"
    shift || true
    if [ "$#" -eq 0 ]; then
        echo "FATAL: encoded PowerShell requires an SSH command" >&2
        return 1
    fi
    local encoded
    if ! encoded="$(printf '%s' "$source" | iconv -f UTF-8 -t UTF-16LE | base64 | tr -d '\r\n')"; then
        echo "FATAL: could not encode PowerShell source for the Windows VM." >&2
        return 1
    fi
    "$@" "powershell.exe -NoLogo -NoProfile -NonInteractive -EncodedCommand $encoded"
}

# UTM guests can resume with their wall clock tens of minutes behind the host.
# Besides invalidating TLS and timeout evidence, that makes freshly mirrored
# source files appear "from the future", so make can repeatedly rebuild or
# incorrectly reason about dependencies. Set the dedicated test VM from the
# trusted local host clock, then fail unless the observed UTC epoch is close.
# Arguments are the complete pinned SSH command array.
cbm_vm_sync_windows_clock() {
    if [ "$#" -eq 0 ]; then
        echo "FATAL: Windows VM clock sync requires an SSH command" >&2
        return 1
    fi
    local host_utc
    local host_epoch
    local guest_output
    local guest_epoch
    local skew
    local powershell_source
    local attempt

    # A host sleep can pause QEMU after host_utc is captured but before the
    # guest is set and validated. Retry from a fresh host timestamp instead of
    # accepting the stale clock or requiring a manual VM restart.
    for attempt in 1 2 3; do
        host_utc="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        powershell_source="Set-Date -Date ([DateTimeOffset]::Parse('${host_utc}', [Globalization.CultureInfo]::InvariantCulture, [Globalization.DateTimeStyles]::AssumeUniversal).LocalDateTime) | Out-Null; [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()"
        if ! guest_output="$(cbm_vm_run_powershell "$powershell_source" "$@")"; then
            echo "FATAL: could not synchronize the Windows VM clock from the host." >&2
            return 1
        fi
        guest_epoch="$(printf '%s\n' "$guest_output" | tr -d '\r' | tail -n 1)"
        case "$guest_epoch" in
            '' | *[!0-9]*)
                echo "FATAL: Windows VM clock sync returned an invalid epoch: $guest_epoch" >&2
                return 1
                ;;
        esac
        host_epoch="$(date -u '+%s')"
        skew=$((host_epoch - guest_epoch))
        if [ "$skew" -lt 0 ]; then
            skew=$((-skew))
        fi
        if [ "$skew" -le 15 ]; then
            echo "WINDOWS_CLOCK_OK skew=${skew}s"
            return 0
        fi
    done
    echo "FATAL: Windows VM clock remains ${skew}s from the host after synchronization." >&2
    return 1
}

cbm_vm_write_untracked_manifest() {
    local root="$1"
    local manifest="$2"
    local symlinks
    local entry
    local mode
    local relative
    local link
    local nested

    symlinks="$(mktemp "${TMPDIR:-/tmp}/cbm-vm-symlinks.XXXXXX")"
    : >"$manifest"
    while IFS= read -r -d '' entry; do
        mode="${entry%% *}"
        relative="${entry#*$'\t'}"
        if [ "$mode" = "120000" ]; then
            printf '%s\0' "$relative" >>"$symlinks"
        fi
    done < <(git -C "$root" ls-files --stage -z)

    while IFS= read -r -d '' relative; do
        nested=false
        while IFS= read -r -d '' link; do
            case "$relative" in
                "$link"/*) nested=true; break ;;
            esac
        done <"$symlinks"
        if ! $nested && [ -L "$root/$relative" ]; then
            echo "FATAL: untracked symlink cannot be mirrored to Windows: $relative" >&2
            echo "  Stage the symlink so the binary Git patch carries its platform semantics." >&2
            rm -f -- "$symlinks"
            return 1
        fi
        if ! $nested && [ -e "$root/$relative" ]; then
            printf '%s\0' "$relative" >>"$manifest"
        fi
    done < <(git -C "$root" ls-files --others --exclude-standard -z)
    rm -f -- "$symlinks"
}

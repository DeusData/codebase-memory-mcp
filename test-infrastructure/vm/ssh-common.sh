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

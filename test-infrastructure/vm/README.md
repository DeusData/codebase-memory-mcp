# Real-Windows local-CI leg

Windows kernel semantics cannot be reproduced by the container/Wine legs. The
UTM VM is a first-class part of the mandatory local-CI gate and is driven over
local ssh by the reproducible scripts in this directory. The canonical macOS
leg remains the native host; Linux runs through Colima.

## Windows VM — real ACL/token/owner semantics (script-driven)

Wine only compile-checks Windows. Kernel-real security semantics — object
ownership, DACLs, token identity, the Administrators-default-owner policy on
CI runners — need real Windows. The VM turns blind CI round-trips into a
seconds-fast local loop with full stderr/debugger visibility.

### Files

| File | Runs where | Purpose |
|---|---|---|
| `windows-bootstrap.ps1` | inside the VM, once | OpenSSH + host key + CI owner-policy mirror |
| `provision-windows.sh`  | on the host | zero → fully-built, idempotent (disk-loss recovery) |
| `win.sh`                | on the host | daily driver: build / test / guards / install-E2E / shell |

Host-local config (never committed): `~/.claude/cbm-vm/config` with
`CBM_VM_HOST=<ip>`, `CBM_VM_USER=<user>`, and the
`CBM_VM_HOST_KEY_SHA256=<SHA256:...>` fingerprint printed by the bootstrap.
`CBM_VM_BRANCH=<branch>` is optional; both drivers otherwise use the current
local Git branch, falling back to `main` only for a detached worktree.
The drivers verify that fingerprint before every connection. The ssh keypair is at
`~/.claude/cbm-vm/id_ed25519` (generate: `ssh-keygen -t ed25519 -N "" -C
claude-cbm-vm -f ~/.claude/cbm-vm/id_ed25519`; include the public half beside
`windows-bootstrap.ps1` on the setup ISO as documented in that script).

### One-time VM creation (~30 min interactive, do it once, snapshot it)

1. `brew install --cask utm` (free/open source).
2. Download the official **Windows 11 ARM64** ISO:
   https://www.microsoft.com/software-download/windows11arm64
3. UTM → New VM → Virtualize → Windows → the ISO → **all host cores**,
   12-16 GB RAM, 64+ GB disk. vCPUs are host-scheduled, not pinned: idle
   guest cores cost nothing and nothing is seized from parallel work — no
   artificial limits, speed first. RAM is the one bounded number (Windows
   commits it eagerly).
4. Windows setup. At "Let's connect you to a network": Shift+Fn+F10 →
   `start ms-cxh:localonly` → create a local account (no Microsoft account,
   no network needed).
5. **⭐ Shut down and CLONE the VM in UTM now** ("Windows-CLEAN-backup").
   Driver installs can brick the boot; the clone turns that into a 10-second
   rollback instead of a reinstall. Boot the original afterwards.
6. Install the SPICE guest tools (network driver): download
   https://github.com/utmapp/qemu/releases/download/v7.0.0-utm/spice-guest-tools-0.164.4.iso
   on the host, mount via UTM's CD icon, run as administrator inside the VM,
   restart. A brief "Display output is not active" during restart is normal.
7. Get `windows-bootstrap.ps1` into the VM via the ISO trick in its header
   (clipboard and shared folders DO NOT work on Windows-ARM guests — the
   SPICE vdagent/webdavd services don't exist there; don't chase them),
   run it, note the printed ip/user, write `~/.claude/cbm-vm/config`,
   reboot the VM once (owner policy).
8. From the host: `test-infrastructure/vm/provision-windows.sh` — installs
   msys2 + both toolchains, clones the repo, builds everything, smoke-checks.
   Re-run it any time; it is the single recovery command if the disk is lost.

### Daily loop

```
vm/win.sh update            # sync VM repo to the pushed current branch + rebuild
vm/win.sh sync              # mirror the uncommitted worktree and rebuild before local gates
vm/win.sh test cli daemon_ipc         # run suites natively, seconds
vm/win.sh guards                      # the Windows guard scripts
vm/win.sh smoke-install               # managed-install E2E, stderr VISIBLE
vm/win.sh sh "cd /c/cbm && gdb ..."   # anything, interactively
vm/win.sh push-file src/cli/cli.c /c/cbm/src/cli/cli.c   # WIP iteration
```

`sync` requires the VM to already have the exact pushed base commit (`update`
does that); it then mirrors the host's uncommitted tracked and untracked changes,
cleans stale build outputs, and rebuilds.

### Toolchains & honest limits

- **CLANGARM64 (native, default)**: fast on all cores; OS-semantics bugs
  (ACL/owner/paths/locks — the entire Windows tail class) reproduce
  faithfully. Build with `SANITIZE=` (no ASan runtime exists for
  aarch64-windows).
- **CLANG64 (x86_64 = CI's arch, emulated)**: arch-parity checks and
  **UBSan, which WORKS under emulation** (validated: builds, runs, and
  reports a planted signed-overflow with file:line) — `win.sh ubsan-build`
  + `ubsan-test <suites>`. UBSan needs no interceptors, which is why it
  survives emulation.
- **ASan does NOT exist on this VM in either arch** — verified, don't
  retry: the aarch64-windows compiler-rt ships no sanitizer runtimes at
  all (MSVC's ARM64 ASan is a VS-2026 preview tied to the MSVC CRT), and
  the x86_64 ASan runtime faults during process-init under x64-on-ARM
  emulation. ASan coverage stays a CI-only leg; note the CI windows
  failures have historically been logic failures, not ASan aborts.
- **PageHeap** (`win.sh pageheap on|off`): OS-level page-granular heap
  overflow/UAF detection for the native ARM64 test-runner — a
  toolchain-agnostic partial ASan substitute that works on real Windows.
- The bootstrap mirrors the GitHub-runner default-owner policy
  (`NoDefaultAdminOwner=0`) so CI-only ownership refusals reproduce locally.
- Environment shape (runner temp paths, runneradmin profile) is
  approximated, not identical — CI on the final SHA remains the proof.

## Honesty rules

- These legs never silently skip: absent tooling prints setup guidance and
  exits with a distinct status.
- VM results complement, never replace, the CI matrix on the final SHA.
- Everything runs over the local UTM/host network only — nothing about the
  VM loop touches the internet except the VM's own package/repo downloads.

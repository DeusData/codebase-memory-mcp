# Windows troubleshooting

This page collects confirmed Windows-specific symptoms that are still tracked in the issue
tracker. It is intentionally brief: each entry records the affected version reported by users,
a current workaround when one is known, and the canonical issue. Check the linked issue before
assuming the same cause applies to a newer release.

## Non-ASCII paths in CBM_CACHE_DIR or CBM_ALLOWED_ROOT

**Symptom:** configuration appears not to persist, the config database cannot be opened, or
repositories that should be inside `CBM_ALLOWED_ROOT` are rejected.

**Reported on:** v0.9.0.

**Workaround:** avoid non-ASCII characters in these two environment-variable paths. Repository
paths passed directly as CLI arguments were not affected in the original report.

**Tracking:** #1165.

## Upgrade completes but the Windows binary version does not change

**Symptom:** an upgrade appears to download successfully, but
`codebase-memory-mcp --version` still reports the old version.

**Reported on:** v0.8.1.

**Workaround:** use the current `install.ps1` flow to reinstall/update the binary, then verify
the version explicitly. The current README also treats the install script as the supported update
path on Windows.

**Tracking:** #1168.

## Index worker hangs, crashes, or leaves an empty worker log

**Symptom:** `index_repository` repeatedly reports worker hangs/crashes, or the referenced
`.worker-*.log` is empty.

**Reported on:** v0.9.0.

**Workaround:** for repositories containing large binary/media/model directories, exclude those
paths with `.cbmignore` before retrying; that workaround was confirmed in #1132. If the project
is small or mostly source, keep the failing tree intact for diagnosis and follow the canonical
worker issues instead of assuming the same trigger.

**Tracking:** #1130, #1132, #1145.

## Antigravity 2.0 configuration is not picked up

**Symptom:** installation completes but Antigravity 2.0 does not see the MCP server/hooks, while
legacy Gemini configuration is present; duplicate large binaries may also be left on disk.

**Reported on:** v0.9.0.

**Workaround:** verify the configuration files that Antigravity is actually loading before
re-running installation, and avoid keeping duplicate binary copies once you have confirmed which
one is active.

**Tracking:** #1161.

## OpenCode is installed but not detected

**Symptom:** `codebase-memory-mcp install` detects other clients but skips OpenCode.

**Reported on:** v0.9.0 on Windows.

**Workaround:** use the README's manual MCP configuration path for OpenCode while automatic
detection is being tracked.

**Tracking:** #1167.

---

When a release resolves one of these issues, update or remove the corresponding entry rather than
letting this page become a second issue tracker.

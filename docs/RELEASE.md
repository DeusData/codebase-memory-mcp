# Release Runbook

The checked-in Claude Code plugin is a release artifact. Its manifest, MCP
server command, and hooks must use the same version as every `version` field
in `server.json`.

## Prepare a release

1. Choose the bare semantic version, for example:

   ```bash
   RELEASE_VERSION=0.9.0
   ```

2. Update every `version` field in `server.json` to
   `$RELEASE_VERSION`.

3. Regenerate the plugin and review the result:

   ```bash
   scripts/sync-plugin.sh "$RELEASE_VERSION"
   git diff -- server.json plugin/
   ```

4. Run the same preflight used by release CI:

   ```bash
   scripts/check-plugin-drift.sh "v$RELEASE_VERSION"
   ```

5. Commit the version metadata and generated plugin together, then dispatch
   the Release workflow with `version=v$RELEASE_VERSION`.

The release workflow runs the drift preflight before platform artifact builds.
It refuses a release when the dispatch version, `server.json`, or generated
plugin differ.

# Frontend configuration

Open **Config** at the right of the top bar. Settings are grouped by indexing,
resources, server, diagnostics, and browser. The **External inputs** reference
covers repository files, installation, launch flags, integration paths, and
development settings whose owner is outside the running daemon.

## Daemon overrides

Daemon edits are staged until **Save changes**. A batch is validated and committed
atomically to `_config.db` in the CBM cache directory. Invalid keys or values reject
the whole batch. Each row exposes its source, default, saved override, current
process value, and application timing. **Reset** removes the saved override; it
does not overwrite the inherited environment or replace it with a hardcoded default.
Legacy environment values outside the registry's canonical format are shown as
runtime-interpreted inputs, rather than guessed effective values. Saving an override
requires a validated value.

Supported environment overrides are loaded once when a process starts, ahead of
the inherited environment and built-in defaults. They do not mutate or export the
process environment. Restart CBM to apply changes throughout the installation;
newly launched index workers can load saved values before the existing daemon is
restarted. Explicit command flags, per-index options, and supervisor recovery caps
retain their documented precedence.

The existing `auto_index`, `auto_index_limit`, `auto_watch`, `watcher_enabled`, and
`ui-lang` settings share their canonical SQLite values with CLI configuration.
UI enablement and port overrides use the same transactional store, with legacy
UI `config.json` as a fallback. Explicit CLI/UI saves update that store too.
Concurrent edits use a revision check; a stale save fails without changing data.

Launch locations, filesystem access grants, arbitrary output destinations,
installer settings, test injection, and supervisor controls are references rather
than editable process overrides. Platform-specific or unlinked functionality is
marked unavailable. No generic environment editor or secret enumeration is exposed.

## Browser preferences

Browser changes apply immediately. Graph display and edge motion use their existing
preference stores. Galaxy budgets, coverage shadow, and architecture appearance
share a versioned, per-project browser store with the controls in those views.
Changing these values preserves the current selection. If storage is unavailable,
preferences remain usable for the current session.

Local agent configuration opens the existing model controls. Opening Config never
downloads weights or starts a model. Repository extension mappings and ignore files
remain repository-owned and take effect on subsequent indexing.

## Local HTTP API

`GET /api/config` returns `{ revision, settings }`. Each setting declares its type,
validation bounds or enum choices, lifecycle, provenance, and editability.
The registry lives in `src/cli/runtime_settings.c`; consumer locations are included
in its metadata. Browser response validation lives in `src/settings/config-model.ts`.

`POST /api/config` accepts JSON with the revision just read and a nonempty changes
object. Values are canonical strings; `null` removes an override:

```json
{
  "revision": "12",
  "changes": {
    "CBM_WORKERS": "4",
    "CBM_SEMANTIC_ENABLED": "false",
    "CBM_SQLITE_MMAP_SIZE": null
  }
}
```

Success returns the updated snapshot. Invalid batches return 400, conflicting
revisions return 409, and storage failures return 500. Existing loopback Host,
same-origin, and JSON-content checks protect the endpoint. Frontend drafts survive
errors so users can inspect the conflict before reloading or discarding them.

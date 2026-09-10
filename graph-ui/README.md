# CodeAtlasWeb

## In this repository

This directory is the frontend that `codebase-memory-mcp --ui=true` serves.
It came in as CodeAtlasWeb by Bernhard Jackiewicz (design and tracking in
issue #1964); the text below this section is his README, kept as written.
What the maintainers added on top:

- **Build and embed.** `make -f Makefile.cbm cbm-with-ui` runs `npm ci`
  (with `PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1`) and `npm run build` here, then
  embeds `dist/` into the binary. The dev server (`npm run dev`) listens on
  5173 and proxies `/rpc` and `/api` to a server on 9749, which is the
  contract `tests/test_ui_dev_proxy_security.sh` pins.
- **CSP.** The served page may reach the server itself plus the loopback
  service explicitly allowed for optional local inference (`src/ui/http_server.h`,
  `CBM_UI_CSP_VALUE`). Nothing else, and a test holds that.
- **The `[p]rojects` panel** (alt+p, `src/projects/`): index a repository,
  check or remove an index, edit the decision record, read the server's
  processes and log. It is the one surface that asks the server to write,
  and it names every route it uses.
- **The frontend log** (`src/app/ui-log.ts`, installed in `src/main.tsx`):
  the console keeps printing, and a copy of every console line, uncaught
  error, unhandled rejection and failed `/rpc` or `/api` call is batched
  to `POST /api/ui-log`, which the server appends as JSON lines to
  `<cache_dir>/logs/ui.log` (rotating once at 5 MiB). `GET /api/ui-log`
  tails it, the projects panel shows it under "This server", and a bug
  report attaches the file. The caught request failures reach the log
  through `src/provider/error-observer.ts`, the seam the two clients
  announce on before they throw.
- **Gates in CI** (`.github/workflows/_test.yml`, job `test-ui`, which
  runs `scripts/ci/test-ui.sh`; run that script locally for the identical
  leg): `npm run test:unit`, `npm run check:style`,
  `npm run check:promises`, `npm run test:acceptance` and
  `npm run build`. `test:acceptance` is
  every frozen check except the two release-binding files
  (`release-current`, `release-proof-binding`), which tie the recorded
  release report to the commits of the original repository and cannot
  pass here. The browser proofs (`npm run smoke:*`) need a running server
  and Playwright browsers and stay a local venue; run them by hand with
  `npx playwright install chromium` first.

Eine lokale Lese-IDE fuer ein indiziertes Repository: Baum, Editor,
Galaxie, Erklaerungen. Sie laeuft im Browser, sie hat kein eigenes Backend, und
sie ist per Vorgabe abgeschottet. Der genehmigte Plan steht in `PLAN.md`, die
Projektregeln in `CLAUDE.md`, der aufgenommene Fremdcode in `THIRD_PARTY.md`.

## Was hier laeuft

```
npm run build          # tsc + vite, Ergebnis in dist/
npm run test:unit      # vitest
node --test tests/scaffold/    # die eingefrorenen Abnahmetests
npm run check:style    # lange Striche, Attribution, hartkodierte Texte
npm run check:promises # Saetze, die etwas zusagen, und Flaechen ohne Wirkung
```

Die Beweislaeufe je Zyklus heissen `npm run smoke:<zyklus>` und schreiben ihr
Ergebnis nach `verification/`. Jeder von ihnen laeuft unter einem
Netz-Deny-Gate; was dabei an Verbindungen hinausging, steht als
`outboundViolations` im Artefakt.

Der Analyse-Server (`cbm/`) wird getrennt gebaut und indiziert die Repositories.
Diese Oberflaeche indiziert nichts selbst: sie liest `/api` und `/rpc`.

## Die Vorschau: nach jedem Bau neu starten

Eine Vorschau-Instanz besteht aus zwei Prozessen: dem C-Server auf einem
eigenen Port und einem kleinen Auslieferer, der `dist/` bedient und `/api` sowie
`/rpc` an den Server weiterreicht (`tools/lib/static-proxy.mjs`).

**Der Auslieferer KOPIERT `dist/` beim Start.** Er tut das mit Absicht: sonst
liefe ein `npm run build` mitten in eine offene Seite hinein und ersetzte
Dateien, waehrend der Browser sie laedt. Die Folge ist aber, dass er spaetere
Bauten **nie** sieht. Wer baut und dann die alte Vorschau neu laedt, sieht den
Stand von vorhin, und nichts an der Oberflaeche sagt das.

Also gilt, ohne Ausnahme:

1. `npm run build`
2. die laufende Vorschau beenden (SIGTERM an ihren Prozess)
3. die Vorschau neu starten
4. **pruefen, was wirklich ausgeliefert wird**, bevor irgendetwas beurteilt wird

Der vierte Schritt ist der, den man weglassen moechte, und genau der, der den
Fehler findet. Er vergleicht die Bundle-Kennung im ausgelieferten `index.html`
mit der im gebauten `dist/`:

```
ls dist/assets/index-*.js                       # was der Bau erzeugt hat
curl -s http://127.0.0.1:<ui-port>/ | grep -o 'index-[A-Za-z0-9_-]*\.js'
```

Stimmen die beiden Kennungen nicht ueberein, liefert die Vorschau einen alten
Bau aus, und jeder Befund an ihr ist ein Befund ueber Code von vorhin.

Warum das hier steht: zwischen 13:08 und 23:10 am 2026-08-29 sind fuenf Zyklen
gebaut und nicht gesehen worden, weil die Vorschau vom Vormittag weiterlief. Ein
Ablauf, der nur im Kopf einer Sitzung steht, ist beim naechsten Mal weg.

## Die Agentenebene: was live arbeitende Agenten auf dem Graphen zeigen

Arbeiten KI-Agenten in demselben Repository, kann die Galaxie sie zeigen: je
Agent ein kleiner leuchtender Koerper, der den Symbolknoten umkreist, an dem er
gerade arbeitet, und unten rechts ein kompaktes Instrument, das erklaert, was
man sieht. Der Weg dorthin hat drei Stationen.

### Recorded tool activity through the daemon

`agents/hooks/atlas-trace.py` is the supported PostToolUse producer. Configure
`ATLAS_PROJECT` to the exact indexed project name, `ATLAS_AGENT_NAME` for its
label, and (only for a custom port) `ATLAS_DAEMON_URL=http://127.0.0.1:PORT`.
The **Connect your agent** disclosure in Agent activity downloads this exact
hook and generates a project-specific install command using `/api/repo-info`.
On macOS/Linux with Python 3, run the reviewed command:

```sh
python3 ~/Downloads/cbm-atlas-trace.py --install-claude --root /absolute/repository --project INDEXED_NAME --daemon-url http://127.0.0.1:9749
```

Installation is explicit and local: it preserves unrelated Claude settings,
adds a PostToolUse entry to `.claude/settings.local.json`, and copies the hook
into `.claude/hooks/`. It refuses conflicting hooks, symlinks and malformed
configuration. It makes no network request or account change. Start a new Claude
Code session after installation, then load recorded activity in the browser.
Other coding clients require their own event adapter; they are not automatically
observed. For manual configuration, run `python3 <path>/atlas-trace.py` in the
client's PostToolUse hook. Its JSON tool event arrives on stdin. During normal
hook execution it always exits zero and does not change the tool call's result.

The hook records timestamp, tool name, run ID, sequence number, path and optional
line span/command. The hook records no file contents and no tool output. Commands
and paths may still contain sensitive information: the entire data path stays
on this workstation. The hook refuses external destinations, proxies and redirects.

A transactional SQLite outbox at `~/.atlas-trace/events.jsonl.outbox.sqlite3`
allocates sequence numbers safely across concurrent hooks. `ATLAS_TRACE_FILE`
changes this path prefix. Up to 10,000 pending events survive daemon downtime;
old pending entries expire at that limit. Delivery retries on the next hook,
or explicitly with `python3 <repo>/graph-ui/agents/hooks/atlas-trace.py --flush`.
A flush delivers up to 100 pending events from one project. No background bridge
or additional listener is required. The old JSONL watcher and replay bridge remain
historical fixture tools; they are not connected to the application by default.

Daemon log provenance is explicit. `GET /api/logs?project=NAME&min_level=warn`
returns only records attributed to that exact indexed project; the filter applies
before pagination and counts. Omitting `project` keeps the daemon-wide history;
`scope=unattributed` shows records without known project ownership. Migration v3
adds a nullable project column without assigning old records to the current
repository. Paths, page URLs and message text are never used to guess ownership.
Each record exposes `project: string | null`; responses expose `scope` and the
selected `project`. A failed scoped database read returns an error instead of
substituting unrelated memory-ring logs. New UI entries capture optional project
ownership when recorded, before buffering; an explicit empty/null entry project
stays unattributed even if a batch carries a project.

`POST /api/agent-events` accepts `{project, events: [...]}` with at most 100 events.
Required event fields are `ts`, `agent`, `run`, integer `seq`, `phase: start|end`
and `tool`. Optional fields include `path`, `lines`, `detail`, `source`, `intent`
(self-report), and explicit `replay`. Unknown fields are discarded.

The daemon owns `<cache>/activity.db`, independent of replaceable project indexes.
Its additive migrations use SQLite WAL and a busy timeout. Events have monotonic
daemon cursors and unique `(project, run, seq)` identities. A small per-run retired sequence floor prevents expired events from reappearing after a delayed retry.
10,000 events are retained globally; expired history is explicitly incomplete.
A database generation distinguishes a restart (same state) from a replaced database.

### The view and logs

Agent activity is off until enabled by the reader. It then polls
`GET /api/agent-events?project=NAME&after=CURSOR&limit=200` over the same origin and
port as the graph and MCP HTTP API. Failed requests retain the last accepted
cursor; reconnects do not duplicate retained rows. Changing repositories resets
the browser's activity state. `?agents=PORT` no longer selects another server.

Daemon log callbacks and every accepted frontend log level persist in the same SQLite
journal. `GET /api/logs` preserves its `lines`/`total` response and adds structured
`records`, receipt timestamps, severity, source, durable IDs and a database
generation. `min_level=warn` selects warnings and errors, `min_level=error` only errors.
`q=TEXT` applies a literal, ASCII case-insensitive substring search to timestamps,
sources, messages and explicit project names. Scope, severity, search, counts and
cursors all apply before the display limit. System Logs uses these same filters; 2,000 warning/error records
are reserved independently of 3,000 routine records. `after=CURSOR` reads forward,
while requests without a cursor return a recent snapshot. A failed SQLite store
is reported as `persistent:false`, with the bounded in-memory fallback available.
`GET /api/ui-log` now reads frontend JSON lines from that same journal. Its
`path` names `activity.db`, `export_path` names the compatibility JSONL file, and
`source: "sqlite"` / `persistent: true` identify the source. Version 4 adds explicit
frontend origin and retains full bounded JSON evidence. On first open, at most the
last 256 KiB of each old `ui.log.1` and `ui.log` are imported in one transaction;
original receipt timestamps are preserved, project ownership remains unknown,
and a partial import is disclosed. No log is reassigned to the selected project.

UI posts retain the existing `accepted`, `dropped`, `path` and `file_error` fields,
adding `duplicates`, `persistent` and `storage_error`; a failed journal write
returns HTTP 503 for retry. A retired per-session sequence floor prevents expired
UI events from returning on an old retry. JSONL remains a rotating local export
for existing `tail` workflows; changing it does not change the HTTP history.
No local report or event is published externally by these routes.

Was die Ansicht zeigt und was sie ausdruecklich nicht zeigt:

- **Ein Agent ist ein Koerper, kein Knoten.** Die Ebene legt sich ueber den
  Graphen; Knoten- und Kantenfarben bleiben unangetastet. Die Farbe eines
  Agenten kommt aus seiner Kennung und ist nach einem Reload dieselbe; dazu
  traegt jeder Koerper einen Buchstaben, damit die Unterscheidung nicht allein
  an der Farbe haengt.
- **Die Art der Arbeit steht in Form und Verhalten**: lesen ein weiter, ruhiger
  Orbit; schreiben ein enger; suchen kurze Pings an den Knoten, deren Name das
  Suchmuster traegt; testen ein pulsierender Koerper mit einer gestrichelten
  Linie zum geprueften Bereich, sofern der Befehl eine Datei nennt, die der
  Index kennt.
- **Die Zuordnung ist so genau, wie sie sein kann.** Datei plus Zeilenbereich
  trifft den engsten passenden Symbolknoten, nur Datei den Modulknoten, ein
  Knoten ohne Endzeile wird als unsicher gekennzeichnet, und was sich nicht
  verorten laesst, steht im Instrument samt dem Rohereignis, statt zu
  verschwinden.
- **Nichts wird gedeutet.** Kein Fortschritt, keine Prozentzahl, keine
  Bewertung. Eine Absichtszeile erscheint nur, wenn das Ereignis ein
  `intent`-Feld mitbringt, das der Agent selbst geschrieben hat, und dann
  gekennzeichnet als Selbstauskunft.
- **Der Leser ist auch ein Akteur.** Welches Symbol er oeffnet, laeuft als
  eigener Akteur "you" durch dieselbe Ebene. Diese Ereignisse entstehen im
  Browser und gehen NICHT in die Ereignisdatei.

Der Schalter, der die ganze Ebene abschaltet, liegt im Einstellungen-Panel in
der Gruppe "Drawing, and what it costs", zusammen mit allem anderen, was
Rechenzeit kostet, und nennt dort seinen auf dieser Maschine gemessenen Effekt
auf die Bildrate.

Die aufgezeichnete Datei, gegen die der Beweislauf faehrt, liegt unter
`fixtures/agent-events/`; woraus sie entstanden ist und was daran geaendert
wurde, steht vollstaendig in `fixtures/agent-events/HERKUNFT.md`.

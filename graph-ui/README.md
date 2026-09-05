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
- **CSP.** The served page may reach the server itself plus two loopback
  services the reader can start: the local-model sidecar on 127.0.0.1:4141
  and the agent bridge on 127.0.0.1:4142 (`src/ui/http_server.h`,
  `CBM_UI_CSP_VALUE`). Nothing else, and a test holds that.
- **The `[p]rojects` panel** (alt+p, `src/projects/`): index a repository,
  check or remove an index, edit the decision record, read the server's
  processes and log. It is the one surface that asks the server to write,
  and it names every route it uses.
- **Gates in CI** (`.github/workflows/_test.yml`, job `test-ui`):
  `npm run test:unit`, `npm run check:style`, `npm run check:promises`,
  `npm run test:acceptance` and `npm run build`. `test:acceptance` is
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

### 1. Die Quelle: eine Ereignisdatei

Ein Werkzeug-Hook schreibt eine JSON-Zeile je Werkzeugaufruf, angehaengt an eine
Datei (JSONL). Die Hook-Skripte dieses Repositories liegen unter
`agents/hooks/`:

- `agents/hooks/atlas-trace.py` als PostToolUse-Hook. Er endet unter allen
  Umstaenden mit 0: ein Protokoll, das die Arbeit aufhalten kann, waere
  schlimmer als kein Protokoll.
- `agents/hooks/atlas-trace-watch.mjs` als zweite Quelle. Sie beobachtet
  Schreibvorgaenge auf der Platte, sieht also **nur** Schreiben und weder Lesen
  noch Suchen noch Testlaeufe, und kennzeichnet jedes Ereignis entsprechend
  (`source: "fs"`).

Wohin geschrieben wird, sagt `ATLAS_TRACE_FILE`; ohne die Variable ist es
`~/.atlas-trace/events.jsonl`. Wie der Agent heisst, sagt `ATLAS_AGENT_NAME`.

Einrichtung in der Hook-Konfiguration des Agenten-Werkzeugs (bei Werkzeugen mit
einem `PostToolUse`-Hook ist das eine `settings.json` in deren
Heimatverzeichnis):

```json
{
  "hooks": {
    "PostToolUse": [
      { "matcher": "*",
        "hooks": [{ "type": "command",
                    "command": "python3 <repo>/agents/hooks/atlas-trace.py" }] }
    ]
  }
}
```

**Das Format**, eine Zeile je Ereignis:

| Feld | Bedeutung |
| --- | --- |
| `ts` | Millisekunden seit 1970 |
| `agent` | Anzeigename des Agenten |
| `run` | Kennung eines Laufs |
| `seq` | fortlaufend je Lauf |
| `phase` | `start` oder `end` |
| `tool` | Werkzeugname |
| `path` | optional, repo-relativer Pfad |
| `lines` | optional, `[von, bis]` |
| `detail` | optional, Befehl oder Suchmuster |
| `intent` | optional, Selbstauskunft des Agenten |

Unbekannte Felder werden durchgereicht und nicht als Fehler behandelt: jede
andere Quelle, die dasselbe Format schreibt, wird gelesen.

**Die Ereignisdatei enthaelt Pfade, Zeilenbereiche und Werkzeugnamen und
keine Dateiinhalte.** Kein Quelltext, kein Ausgabetext, kein Diff. Das ist die
Grenze des Formats und keine Einstellung daran: wer die Datei liest, liest, WO
gearbeitet wurde, und nicht, WAS dort steht. Sie kann trotzdem verraten, an
welchen Dateien jemand arbeitet und welche Befehle er faehrt; sie gehoert
deshalb nicht in ein Repository und steht in `.gitignore`.

### 2. Die Bruecke

```
node tools/agent-bridge.mjs                  # Vorgabe: ~/.atlas-trace/events.jsonl, Port 4142
node tools/agent-bridge.mjs --file <pfad> --port <port>
node tools/agent-bridge.mjs --replay fixtures/agent-events/w11a-replay.jsonl --port <port>
```

Sie verfolgt die Datei und reicht sie als Server-Sent-Events weiter. Sie **liest
nur**: es gibt keine Route, die etwas hinzufuegt, sie legt die Datei nicht an
und sie loescht sie nicht. Sie spricht ausschliesslich Loopback und weist jede
Verbindung ab, die nicht von 127.0.0.1 kommt.

Der Wiedergabemodus (`--replay`) liest die Datei einmal und schweigt danach, bis
jemand `POST /replay/advance?count=N` ruft. Das ist die Naht, mit der der
Beweislauf den Takt setzt, statt auf eine Wanduhr zu warten.

### 3. Die Ansicht

Der Live-Modus ist **aus**, bis der Leser ihn einschaltet: im Menue mit
`[g] live agents` (alt+g) oder mit der Zeile `live agents` in der
Kommandozeile. Solange er aus ist, geht keine einzige Anfrage an die Bruecke.
Eingeschaltet ohne laufende Bruecke sagt das Instrument den Zustand und nennt
den Befehl, der sie startet.

Einen anderen Port als 4142 nennt die Adresszeile: `?agents=<port>`.

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

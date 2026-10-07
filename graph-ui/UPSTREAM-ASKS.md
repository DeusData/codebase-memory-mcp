# Upstream-Asks an Martin (CBM, PR 1860 / feat/atlas-r1)

Stand 2026-08-28, abgeleitet aus INVENTAR.md (Belege dort, Datei:Zeile
relativ zum Clone). WICHTIG: Diese Liste wird ausschliesslich lokal gepflegt.
Bernhard reicht alles selbst haendisch bei Martin ein; aus diesem Projekt
heraus gibt es keine Pushes, PRs, Issues oder Kommentare Richtung
DeusData/codebase-memory-mcp.

## Ask 1: Datei-Streaming-Endpoint (PRIO 1, blockiert den Reader)

Was fehlt: Ein Read-only-Endpoint, der eine indexierte Projektdatei komplett
liefert (relativer Pfad rein, Bytes + mtime raus), z. B.
GET /api/file?project=...&path=....
Befund: Es gibt keinen solchen Endpoint (Routenliste http_server.c:2545-2714).
/api/browse ist nur ein Verzeichnis-Picker; get_code_snippet verlangt
qualified_name (symbolweise, harter 500-Zeilen-Cap, realpath-Containment);
search_code full ist auf ein 60-Zeilen-Fenster gekappt. mtime prueft heute
kein Pfad.
Wunsch: gleiche Sicherheitsmechanik wie get_code_snippet (Containment gegen
die Projekt-Root), plus mtime/size im Header fuer Cache-Invalidierung.
NACHTRAG 2026-08-28: get_code_snippet auf dem Modul-Knoten liefert bereits
die ganze Datei; der Reader nutzt das als Interim-Weg. Die Ask bleibt fuer
den Rest bestehen: mtime/size fuer Cache-Invalidierung, verlaessliche
Fenster-Semantik fuer Dateien ueber dem 500-Zeilen-Cap, und nicht
indexierte Dateien (z.B. reine Assets), die kein Modul-Knoten abdeckt.

## Ask 2: Traces-Query-Surface via /rpc (PRIO 2)

Was fehlt: Beobachtete Kanten pro Symbol abfragbar ueber die /rpc-Flaeche
(z. B. eigenes Read-Tool oder r.observed in query_graph).
Befund: observed_calls/observed_paths werden nur von GET /api/trace und
GET /api/flow angehaengt (cbm_atlas_attach_observed, Memo-Cap 64 Knoten);
kein observed-Flag an Graph-Kanten, keine Cypher-Exposition; /rpc verbietet
ingest_traces (richtig so), bietet aber auch keinen Lese-Ersatz.
Zusatzbefund fuer Martin: observed_paths ist aktuell write-only (kein SELECT
im ganzen src/); die im Schema-Kommentar (store.c:328-330) angekuendigte
Ganz-Pfad-Anzeige existiert noch nicht.
Interim bei uns: BUG-Wizard und Twin lesen observed nur ueber /api/trace und
/api/flow; alles andere zeigt 'unsupported'.
NACHTRAG 2026-08-28 (W4-Befund, verschaerft die Ask): cbm_atlas_attach_observed
haengt observed NUR an statisch benachbarte Hops. Eine ingestierte Kante OHNE
statisches Index-Gegenstueck (unser Testfall: listUsers -> validateUser) wird
gespeichert (pairs_stored zaehlt sie), ist danach aber ueber KEINE Route mehr
lesbar. Genau diese Kanten sind fuer die Bug-Jagd die interessantesten
(beobachtet, aber vom Index nicht erwartet). Wunsch praezisiert: ein Read-Weg
fuer observed-Kanten UNABHAENGIG von statischen Nachbarschaften (z.B. eigenes
Tool observed_edges(project, symbol?) oder r.observed in query_graph).

## Ask 3: Offizieller Platz fuer Zusatz-Frontends unter --ui (PRIO 2)

Was fehlt: Ein gesegneter Weg, zusaetzliche statische Frontend-Routen mit
auszuliefern (unser Reader/Twin-UI), ohne den graph-ui-Quellbaum anzufassen.
Befund: embed-frontend.sh pinnt KEINE Asset-Liste (find ueber dist/), neue
Dateien in dist/ werden automatisch eingebettet und serviert; CORS/Origin
sind strikt same-server (http_server.c:84-119), ein anderer localhost-Port
ist bewusst ein fremdes Principal (403 "forbidden origin"). Ein
Cross-Port-Frontend ist damit ausgeschlossen; same-origin ist der einzige
Produktweg.
Wunsch: entweder ein dokumentierter extra-Assets-Ordner fuer den embed-Step
oder eine offizielle Route fuer ein zweites SPA-Bundle.
Interim bei uns: Dev/Tests ueber Vite-Proxy mit Origin-Rewrite (PR-Vorbild,
graph-ui/vite.config.ts); Produktbeweis durch zusaetzliche Dateien in dist/
vor dem embed (reine Build-Artefakte, kein Source-Change im Clone).

## Ask 4: Symbol-Aufloesung Cursor -> Deklaration (PRIO 3, vermutlich erfuellt)

Bedarf: Unser resolveSymbolNamed (Suche + Deklarationszeile in EINEM Call).
Befund: search_graph liefert bereits qualified_name, Label, Datei und
Zeilenbereich in einem Roundtrip (empirisch: "createUser ... Function
src/services/userService.ts 23-36"). Das deckt den Bedarf voraussichtlich;
offen ist nur, ob bei Namenskollisionen ein exakter Name-Filter noetig wird
(exclude_entry_points/include_connected existieren bereits im Schema,
mcp.c:441-442).
Kein Handlungsbedarf, solange W1/W2 keine Gegenbeispiele finden.

## Ask 5: search_graph ohne is_test/is_exported in der flachen Form (PRIO 3)

Befund aus dem W1-Port: die flache, maschinenlesbare search_graph-Form
(Parameter query, BM25 mit search_mode/rank) traegt KEINE is_test- und
is_exported-Spalten; die name_pattern-Form liefert sie vermutlich, kommt
aber als nach Modulen gruppierte Anzeigeform, die sich nicht sauber
maschinell lesen laesst. Wunsch: entweder die flache Form um is_test und
is_exported ergaenzen oder eine format=json-Variante fuer name_pattern.
Interim bei uns: Suche laeuft ueber BM25; die beiden Flags bleiben ehrlich
undefined statt erraten.
NACHTRAG 2026-08-28 (W3): dieselbe flache Form traegt auch kein Fan-in. Der
Rangfolge fehlt damit ihr Gleichstandsbrecher. Wir nehmen ihn ersatzweise aus
`in_calls` der Layout-Antwort (/api/layout), weil dieselbe Oberflaeche sie
ohnehin geladen hat; ohne geladenes Layout hat jeder Kandidat kein Fan-in,
was nur bei Gleichstand etwas aendert. Ein `fan_in` in der Suchzeile waere
der saubere Weg und wuerde die Kopplung zwischen Suche und Galaxie loesen.

## Bereits erfuellt (keine Asks, nur Anerkennung)

- Bindung strikt auf 127.0.0.1: httpd.c:248 (htonl(0x7F000001)), dazu
  Host-Header-Allowlist gegen DNS-Rebinding und Origin-Gate vor OPTIONS.
  Unser Air-gap-Ask aus der Planungsphase ist damit gegenstandslos.
- /rpc-Read-only-Allowlist (12 Tools + manage_adr get/sections) mit
  Mutations-Guard und deaktivierten Background-Tasks: genau die Flaeche, die
  unser Provider-Port braucht (query_graph inkl. r.line/r.args/r.confidence).
- ingest_traces ist echt (v0.9.0-Stub ersetzt), qn-keyed und reindex-fest.

## Kleine Hinweise (fuer Martins Review, keine Blocker fuer uns)

- /api/why mit guards=1 parst Caller-Dateien von Pfaden aus der DB ohne
  eigenen within-root-Check (Kontrast: get_code_snippet kanonisiert per
  realpath). Vermutlich unkritisch (DB ist lokal), aber inkonsistent.
- traces.h:5-6 nennt OTLP-Helfer "Used by the MCP ingest_traces handler",
  der Handler nutzt sie nicht (nur test-gedeckter Code).
- TabBar.tsx in graph-ui wird nirgends importiert (toter Baustein).

## Stand 2026-08-29 (Fortschreibung nach W4 bis W7)

Diese Liste wurde zuletzt am 2026-08-28 geschrieben; das Inventar hat
seither dazugelernt. Was sich geaendert hat, in der Reihenfolge der Asks:

- **Ask 1 (Datei-Streaming)**: unveraendert offen, aber entschaerft. Der
  Reader laedt ganze Dateien ueber den Modul-Knoten
  (get_code_snippet mit der Modul-QN); der 500-Zeilen-Cap greift und die
  Fenster-Argumente werden vom Server ignoriert (gemessen, siehe
  verification/w2/reader.json: windowSemantics "ignored"). Der Rest der
  Ask bleibt: mtime und Groesse fuer Cache-Invalidierung, verlaessliche
  Fenster-Semantik oberhalb des Caps, und Dateien ohne Modul-Knoten.
- **Ask 2 (Traces lesen)**: durch W4b praezisiert und wichtiger geworden.
  cbm_atlas_attach_observed haengt beobachtete Aufrufe NUR an statisch
  benachbarte Hops. Eine ingestierte Kante ohne statisches Gegenstueck
  (unser Testfall listUsers -> validateUser) wird gespeichert
  (pairs_stored zaehlt sie), ist danach aber ueber keine Route mehr
  lesbar. Genau diese Kanten sind bei der Fehlersuche die
  interessantesten: beobachtet, aber vom Index nicht erwartet. Der
  BUG-Wizard nennt seine zweite Liste deshalb ehrlich "Observed, not on
  the expected chains" und fragt den Index je Zeile einzeln.
- **Ask 3 (Platz fuer Zusatz-Frontends)**: unveraendert offen und
  weiterhin der Grund, warum unsere Flaechen und die PR-1860-Views nicht
  in einem Chrome zusammenliegen (so auch im unabhaengigen Audit als
  bewusst akzeptierte Teil-Erfuellung dokumentiert).
- **Ask 4 (Symbol-Aufloesung)**: erledigt, kein Handlungsbedarf.
  search_graph liefert Name, QN, Datei und Zeilenbereich in einem
  Roundtrip; W1 bis W7 haben kein Gegenbeispiel gefunden.
- **Ask 5 (Suchzeile ohne Flags)**: unveraendert offen; wir nehmen den
  Fan-in ersatzweise aus dem geladenen Layout, was Suche und Galaxie
  koppelt.

Neu dazugelernt, ohne dass eine Ask noetig waere (Anerkennung):

- Der Coverage-Store ist eine eigene, vollstaendige Quelle neben dem
  Code-Graphen. check_index_coverage mit scopes ["."] listet paginiert,
  index_status fuehrt parse_partial, skipped und not_indexed, und die
  Metadaten zaehlen ignorierte Dateien mit. Gemessen in W4d: eine
  gitignorierte Datei erscheint mit Grund "gitignore" und status
  "excluded"; eine kaputte TS-Datei landet mit Zeilenbereichen in
  parse_partial. Damit steht der Explorer auf der Datei-Wahrheit und
  nicht auf dem Graphen. Martins Einschaetzung dazu war richtig, unsere
  urspruengliche Vermutung einer Luecke war es nicht.
- ingest_traces speichert qn-keyed und ueberlebt Reindexing; die
  Zaehler und Run-Label kommen im Wizard und an den Flow-Hops an.

Uebergabe bleibt wie gehabt: Bernhard reicht alles selbst haendisch ein.

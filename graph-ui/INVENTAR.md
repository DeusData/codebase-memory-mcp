# INVENTAR: CBM Atlas (PR 1860, Branch feat/atlas-r1)

Stand 2026-08-28. Quelle: lokaler Clone unter ./cbm (Commit bda46086), gebaut
mit `make -f Makefile.cbm cbm-with-ui` auf macOS arm64 (Apple Clang, Node
v20.19.5, npm 10.8.2). Alle Belege sind Datei:Zeile relativ zu ./cbm.
Empirische Befunde wurden gegen den laufenden Dev-Server (Port 4200,
isoliertes HOME) verifiziert. Dieses Inventar ist die Entscheidungsgrundlage
fuer W1; die daraus folgenden Luecken stehen in UPSTREAM-ASKS.md.

## 1. Frontend-Framework und Toolchain (graph-ui)

- React 19 SPA in TypeScript 5.7, gebaut mit Vite 6 (`tsc -b && vite build`,
  graph-ui/package.json:8), Tailwind CSS 4, shadcn-Style-Primitives unter
  graph-ui/src/components/ui (components.json: style new-york, lucide).
- 3D-Stack: three ~0.183 via @react-three/fiber 9.5, @react-three/drei 10.7,
  @react-three/postprocessing (graph-ui/package.json:14-27).
- Tests: Vitest 4 + Testing Library + jsdom (graph-ui/vite.config.ts:35-39),
  ko-lokalisiert neben den Komponenten, inkl. zh-Suiten.
- Kein Router, kein State-Framework: App.tsx macht eigenes Query-Param-Routing
  (tab, project, node, region, sym, path, flow, #wiki-Hash) via
  pushState/popstate. Tabs sind eine geschlossene TabId-Union (overview,
  modules, graph, flows, changes, dashboard, symbol, stats, control;
  graph-ui/src/lib/types.ts:121-130). Ein neuer Tab heisst: Union erweitern,
  TAB_IDS in App.tsx:22-32, i18n-Label, Render-Zweig. Keine Registry.
- i18n handgerollt: EIN typisiertes messages-Objekt mit en- und zh-Baeumen
  (graph-ui/src/lib/i18n.ts:5-727, `as const` erzwingt Paritaet), Sprachwahl
  ueber /api/ui-config-Override, sonst Accept-Language-Ranking
  (i18n.ts:731-776). Keine separaten Locale-Dateien.
- Serverzugriff zweigleisig: callTool() postet JSON-RPC auf /rpc
  (graph-ui/src/api/rpc.ts:15-47, parst MCP result.content[0].text), REST-GETs
  auf die /api-Routen (graph-ui/src/lib/atlas.ts). Das Frontend selbst ruft
  nur 7 MCP-Tools ueber /rpc.
- Dev-Modus des PR selbst: Vite-Proxy fuer /rpc und /api auf 127.0.0.1:9749
  mit Origin-Rewrite (graph-ui/vite.config.ts). Das ist das Vorbild fuer
  unseren eigenen Dev-Betrieb.
- Dark-Mode hart verdrahtet (graph-ui/index.html:2), kein Theme-Umschalter.

## 2. /rpc-Allowlist und Request-Format

- Endpoint: POST /rpc, Content-Type application/json, Body im MCP-Format
  `{"jsonrpc":"2.0","id":N,"method":"tools/call","params":{"name":...,
  "arguments":{...}}}`. Nur method == "tools/call" passiert den Filter
  (src/ui/http_server.c:2360).
- Allowlist UI_RPC_READ_TOOLS (src/ui/http_server.c:2310-2314), exakt 12
  Read-Tools: list_projects, get_code_snippet, get_graph_schema,
  search_graph, search_code, trace_path, trace_call_path, get_architecture,
  query_graph, detect_changes, check_index_coverage, index_status.
- Sonderfall manage_adr: nur in den Read-Modi get und sections erlaubt
  (rpc_manage_adr_is_read, http_server.c:2327-2347).
- Alle Schreib-Tools (index_repository, delete_project, ingest_traces,
  manage_adr update) sind fuer /rpc gesperrt; Antwort 403 mit -32601
  "UI RPC method is not allowed" (empirisch bestaetigt).
- Duplizierte Top-Level-Keys im Body werden via json_unique_member als
  mehrdeutig abgelehnt; leerer/uebergrosser Body: 400, -32600.
- Die HTTP-MCP-Instanz laeuft ohne Background-Tasks
  (http_server.c:2763) und mit installiertem Daemon-Mutations-Guard
  (http_server.c:2922-2923).
- query_graph dahinter: read-only Cypher-Subset (src/cypher/cypher.c) mit
  MATCH/OPTIONAL MATCH, Multi-Typ-Relationen, variablen Hops, WHERE-Baum,
  Aggregaten, ORDER BY, SKIP/LIMIT, WITH, UNION, UNWIND, CASE; Ceiling 100k
  Zeilen; r.line/r.args/r.confidence generisch via json_extract auf
  properties_json. CREATE/SET/MERGE werden abgelehnt.
- get_code_snippet: NUR ueber indexierte Symbole (qualified_name ist
  Pflicht, empirisch bestaetigt), Zeilenfenster mit hartem 500-Zeilen-Cap,
  Pfad-Containment via realpath gegen die Projekt-Root. search_code full:
  60-Zeilen-Fenster.
- NACHTRAG 2026-08-28 (empirisch): get_code_snippet auf dem Modul-Knoten
  (<projekt>.<pfad.ohne.endung>, Label Module) liefert die GANZE Datei
  (source mit start_line 1..N). Der Reader laedt Dateien darum ueber
  Modul-Snippets; die Fenster-Semantik oberhalb des 500-Zeilen-Caps wird
  im W2-Smoke gemessen und in verification/w2/reader.json dokumentiert.
- MCP-Kern insgesamt: 15 Tools in statischer TOOLS[]-Tabelle
  (src/mcp/mcp.c:376-711) plus Alias trace_call_path (mcp.c:11564); drei
  Tool-Profile all/analysis/scout; tools/list paginiert (Seitengroesse 8).

## 3. /api-Routen (28 benannte Routen plus / und /assets/*)

GET-Routen (src/ui/http_server.c:2545-2714): /__cbm/ui-readiness
(HMAC-Challenge), /api/layout, /api/tree, /api/symbol-history, /api/who,
/api/symbol, /api/metrics, /api/why, /api/handout, /api/blast, /api/bridges,
/api/scent, /api/trace, /api/impact, /api/flows, /api/flow, /api/repo-info,
/api/index-status, /api/ui-config, /api/browse, /api/adr,
/api/project-health, /api/processes, /api/logs. POST: /rpc, /api/index,
/api/adr. DELETE: /api/project. Dazu / und /assets/* aus dem Embedding.
- /api/tree liefert den indexierten Datei-Baum aus dem Graphen (empirisch
  bestaetigt), NICHT das Dateisystem.
- /api/browse ist ein Verzeichnis-Picker fuer die Index-Anlage (empirisch:
  liefert dirs/parent/roots, keine Dateiinhalte).
- Einen Datei-Streaming-Endpoint gibt es NICHT (kein /api/file; Luecke, siehe
  UPSTREAM-ASKS.md). mtime wird nirgends geprueft.
- Nicht gematchte Pfade: 404; ohne eingebettetes Frontend antwortet GET /
  mit 404 "no frontend embedded".

## 4. Server-Betrieb

- Ein Binary, drei Betriebsarten: (a) ohne Argumente stdio-MCP-Server als
  Thin-Client eines per-Account-Daemons, (b) `daemon start|stop|status`,
  (c) `cli <tool>` als daemonlose Einmal-Kommandos (stdin-JSON).
- Die UI gehoert dem Daemon. `daemon start --open --port=9749` startet
  permanent, aktiviert UI-Config und oeffnet den Browser; alternativ
  persistieren `--ui=true --port=N` beim MCP-Start dieselbe Config.
  Default-Port 9749 (src/ui/config.h:13).
- Empirisch: im stdio-Modus startet der HTTP-Server erst NACH einem
  MCP-initialize-Handshake auf stdin; stdin-EOF beendet den Prozess. Fuer
  Tests: stdin offen halten, initialize senden, dann auf GET / == 200 pollen.
- WICHTIG fuer Tests: --ui/--port sind PERSISTIERTE Settings. Testlaeufe
  brauchen ein isoliertes HOME, sonst landet die Config im echten
  Nutzerprofil. Der Index liegt als <projekt>.db unter
  ${CBM_CACHE_DIR:-~/.cache/codebase-memory-mcp}.
- Indexiert wird per MCP-Tool, `cli index_repository`, POST /api/index oder
  optional auto_index (default aus). Git-basierter Polling-Watcher fuer
  Inkremental-Reindexing im Daemon.
- Fixture-Messung (Korrektur 2026-08-29, Audit-Befund 16: UNSERE Kopie
  mit HERKUNFT.md, 10 .ts- plus Konfig-Dateien): index 76 Knoten /
  178 Kanten, deutlich
  unter 1s; Server-Readiness nach Handshake ~2-3s.

## 5. CORS, Host-Politik, Bindung

- Bindung hart auf 127.0.0.1 (httpd.c:248, htonl(0x7F000001)); httpd.c ist
  ein eigener single-threaded HTTP/1.1-Transport.
- /rpc und /api verlangen Host-Header exakt 127.0.0.1:PORT oder
  localhost:PORT (Schutz gegen DNS-Rebinding, http_server.c:2403-2414).
- Origin-Politik: NUR die eigene Server-Origin wird reflektiert; ein anderer
  localhost-Port ist bewusst ein anderes Principal und bekommt 403
  "forbidden origin" (http_server.c:84-119; empirisch bestaetigt).
  Preflight-OPTIONS laeuft ebenfalls erst nach dem Host/Origin-Gate.
- Keine Auth auf /api und /rpc; Schutzmodell = Loopback-Bind + Host-Check +
  Origin-Check + Content-Type-Zwang.
- Konsequenz fuer CodeAtlasWeb: same-origin Auslieferung durch den C-Server
  (Embedding) oder Dev-Proxy nach dem Vorbild des PR (Vite-Proxy mit
  Origin-Rewrite). Direkter Cross-Port-fetch ist unmoeglich.

## 6. Traces (ingest_traces, beobachtete Kanten)

- ingest_traces ist auf diesem Branch ECHT (v0.9.0 war ein Stub): Array aus
  Call-Pfaden qualifizierter Namen (2..256 Hops) oder caller/callee-Paaren,
  optional count und Run-Label. Nur Paare, deren beide Endpunkte indexierte
  Symbole sind, werden gespeichert; bis 10 unaufgeloeste Namen werden
  zurueckgemeldet.
- Ablage qn-keyed (ueberlebt Reindexing) in observed_calls (count,
  first_seen, last_seen, Label) und observed_paths (path_json), Cap 200000
  Paare pro Projekt mit Ganz-Run-Eviction; Ingest in einer Transaktion
  (mcp.c:11433-11514).
- Lesepfad HEUTE: ausschliesslich GET /api/trace und GET /api/flow haengen
  pro Hop ein observed-Objekt {count,label,last_seen} an
  (cbm_atlas_attach_observed, http_server.c; Memo-Cap 64 Knoten). KEIN
  observed-Flag an Graph-Kanten, KEINE query_graph/Cypher-Exposition (kein
  r.observed), /rpc erlaubt ingest_traces nicht.
- observed_paths ist aktuell write-only (kein SELECT im ganzen src/): die im
  Schema-Kommentar angekuendigte Ganz-Pfad-Anzeige existiert noch nicht.
- Frontend: FlowsTab zeigt ObservedChip "observed xN", eine
  Alle-Hops-beobachtet-Zeile und einen Freshness-Footer
  (graph-ui/src/lib/observed.ts, FlowsTab.tsx).

## 7. Galaxy (3D-Graph)

- Rendering: three.js via @react-three/fiber, Bloom via postprocessing; kein
  Client-Force-Layout. ALLE Positionen kommen fertig vom Server:
  GET /api/layout aus layout3d.c (Verzeichnis-Cluster-Ring, Z aus Call-Tiefe,
  ankergebundenes ForceAtlas2 mit Barnes-Hut-Octree), Regionen aus
  layout_regions.c (geseedetes Leiden, golden-angle Kugel-Seed).
- Determinismus: Hash-geseedete Jitter-LCG, stabile Knotenreihenfolge
  (ORDER BY name,id), geseedetes Leiden; Regionen serverseitig pro
  (project, indexed_at) gecacht. Stabil ueber Reloads bei unveraendertem
  Index. Ein expliziter Determinismus-Test fehlt (nur Konstruktion).
- Interaktion heute: Raycast-Klick (Highlight + Nachbarn + Detail-Panel +
  URL-Deep-Link ?node=&region=), Hover, Sidebar-Suche, Region-Scent,
  OrbitControls mit Damping, Idle-Autorotation, semantischer Zoom
  (ApproachWatcher), 2D-Minimap mit Kamera-Marker.
- Es gibt BEREITS eine flyTo-Kamera-Animation: CameraAnimator lerpt Kamera
  und OrbitControls-Target mit ease-out cubic. Focus-Follow (Anforderung 3)
  kann darauf aufsetzen.
- Kapselung: GraphScene ist eigenstaendig und rein props-gesteuert (eigener
  Canvas, cameraTarget/highlightedIds als Props); GraphTab haelt die Szene
  nach erstem Besuch gemountet (invisible statt display:none, WebGL-Kontext
  bleibt erhalten; App.tsx:81-87). Kein Event-Bus: Fokus von aussen geht
  heute nur ueber Props oder URL-Parameter. ErrorBoundary um die Szene.
- Fuer ein zweites Panel relevant: DPR geklemmt auf [1,1.5], antialias aus,
  Composer-Multisampling 0 (GraphScene.tsx:73-74).

## 8. Embedding und Auslieferung (fuer unsere Integration)

- Build-Kette: `make -f Makefile.cbm cbm-with-ui` = frontend (npm ci + vite
  build) -> scripts/embed-frontend.sh dist -> Objektdateien + generiertes
  src/ui/embedded_assets.c -> Link (Makefile.cbm:1068-1081).
- embed-frontend.sh sammelt ALLE Dateien unter dist/ per find ein: es gibt
  KEINE gepinnte Asset-Liste. Neue Dateien in dist/ werden beim naechsten
  embed automatisch eingebettet und vom Server ausgeliefert
  (strcmp-Lookup-Tabelle in embedded_assets.c).
- Auslieferung: GET / = index.html (no-cache), /assets/* immutable, strikte
  Airgap-CSP ohne externe Hosts, nosniff.
- Integrationspfad fuer CodeAtlasWeb ohne jede Quell-Aenderung am Clone:
  eigene Assets zusaetzlich nach cbm/graph-ui/dist/ legen und embed + Link
  neu laufen lassen (Build-Artefakte, kein Source-Change, git-Tree des
  Clones bleibt clean). Offizieller Upstream-Weg bleibt Ask 5.

## 9. Konsequenzen fuer W1 (Architektur-Festlegungen aus dem Inventar)

1. Unser Stack passt sich dem PR an: React 19 + TypeScript + Vite 6
   (+ Tailwind 4 optional; unser Terminal-Look ist ohnehin eigenes CSS).
2. Transport: fetch auf /rpc (tools/call) + /api-Routen; Dev und Tests ueber
   Vite-Proxy mit Origin-Rewrite nach PR-Vorbild; Ports >= 4200.
3. Quelltext fuer den Reader kommt bis zur Erfuellung von Ask 1 per
   get_code_snippet (symbolweise, 500-Zeilen-Cap) plus ehrlicher
   KnowledgeState-Anzeige fuer nicht abgedeckte Dateiteile.
4. Focus-Follow baut auf CameraAnimator/props-gesteuertem GraphScene auf
   (Portierung der Anbindung, nicht der Szene).
5. Traces fuer den BUG-Wizard kommen aus /api/trace und /api/flow, nicht aus
   Cypher (bis Ask 4 erfuellt ist).

# CodeAtlasWeb: Plan v1 (2026-08-28)

Das lokale Verstehens-Frontend auf dem CBM-Stack. Unabhängiges Projekt,
eigener Ordner, eigenes Repo. CodeAtlasIDE dient NUR als Referenz und wird
von hier aus nie verändert.

## 1. Mission

Martins Zielbild umsetzen: Das Produkt läuft komplett lokal über localhost,
ausgeliefert vom CBM-C-Server (PR 1860, Branch feat/atlas-r1), ohne Electron,
air-gapped per Default. Es sieht aus wie eine IDE, ist aber eine Lese-IDE:
Man guckt, versteht und navigiert; editiert wird woanders. Der Look ist der
CODEATLAS-Terminal-Stil (siehe Abschnitt 4). Die Verstehens-Logik kommt als
Portierung aus CodeAtlasIDE, nicht als Neuerfindung.

Vier Sonderanforderungen von Bernhard/Martin (2026-08-28), alle verbindlich:

1. **Komplett lokal, kleine Modelle**: kein teures Cloud-LLM. Zwei lokale
   Modellklassen (etwa 1B und etwa 4B) plus ein Modus, in dem LLM komplett
   aus ist und alles deterministisch bleibt.
2. **Optimierte LLM-Übergaben**: Das kleine Modell ist dumm; der Graph ist
   schlau. Ein Context-Compiler bereitet die Graph-Fakten so als Text auf,
   dass auch ein 1B-Modell kluge, kontextüberblickende Antworten geben kann.
3. **Graph-Fenster mit Focus-Follow**: Der 3D-Graph (Galaxy aus PR 1860) ist
   dauerhaft in einem Fenster/Panel. Klick auf eine Funktion im Code oder im
   Twin dreht/zoomt die Kamera zu genau diesem Knoten.
4. **Mehrere Einstiegsmodi statt Lernkurs**: Ziel ist nicht Coding lernen.
   Modus A: Projekt von Grund auf verstehen (geführte Reihenfolge, Configs
   zuerst). Modus B: Maintainer wählt selbst einen Entry Point (Route,
   Symbol, Suche) und startet DORT, ohne Config-Pflichtprogramm. Dazu die
   bestehenden Frage-Modi (Bug, Change) als Einstiege.

Arbeitsweise: komplett testgetrieben (icca-harness gilt, siehe Abschnitt 9).

## 2. Referenzen (lesen, nie verändern)

- **Referenz-Repo**: /Users/bernhard/Desktop/CodeAtlasIDE (Theia-IDE,
  v1.0.0-Stand). Von dort werden Logik-Module portiert. NIE von hier aus
  editieren, committen oder bauen. Die wichtigsten portierbaren Module:
  - theia-extensions/codeatlas-views/src/browser/twin/render-model.ts
    (Twin-Ansichtsmodell, 4 Tiefen, Ehrlichkeitsregeln)
  - .../pseudocode/pseudocode-builder.ts und flow-model.ts
  - .../diagrams/mermaid-builder.ts, .../impact/risk-rules.ts
  - .../search/semantic-search.ts (deterministisches Ranking)
  - theia-extensions/codeatlas-core/src/common/semantic-ir.ts
    (Fact/KnowledgeState/Evidence: das Ehrlichkeits-Typsystem)
  - theia-extensions/codeatlas-intelligence/src/node/ir/semantic-ir-builder.ts
    und provider/cypher.ts (die Fragen an den Graphen; werden auf /rpc
    umgezogen)
  - .../ai/data-policy-enforcer.ts (Payload-Bau, Sanitisierung, Leak-Scan)
  - .../tours/tour-generator.ts (deterministischer Topsort, Modus A)
  - tests/scaffold/*.test.mjs und tools/smoke-*.mjs als Muster für
    eingefrorene Abnahmetests und Playwright-Beweisläufe
- **CBM Atlas (PR 1860)**: https://github.com/DeusData/codebase-memory-mcp/pull/1860
  Branch feat/atlas-r1. Beschaffen mit:
  `gh repo clone DeusData/codebase-memory-mcp cbm -- --branch feat/atlas-r1`
  (alternativ `git fetch origin pull/1860/head:atlas-r1`). Liefert: C-Server
  mit --ui, /rpc-Read-only-Allowlist, Galaxy (3D), Flows/Trace, Symbol-View,
  Changes, Dashboard, Metric-Wiki, i18n, echtes ingest_traces. Build:
  `make -f Makefile.cbm cbm-with-ui`. ERSTE AUFGABE: Frontend-Framework und
  /rpc-Surface inventarisieren, bevor irgendetwas geplant festgezurrt wird.
- **Engine (Release-Stand)**: /Users/bernhard/.local/bin/codebase-memory-mcp
  (v0.9.0, CLI-per-Call). Gilt nur als Fallback; das Zielsystem ist der
  Server aus dem PR-Branch.
- **Look-Referenz**: zwei Screenshots vom 2026-08-28 (17:15/17:16), Original
  unter /var/folders/.../NSIRD_screencaptureui_*/Bildschirmfoto*.png, besser:
  Bernhard legt Kopien nach ./design/ ab. Beschreibung in Abschnitt 4.

## 3. Architektur

```
Browser (localhost)                     lokale Prozesse
+--------------------------------+     +---------------------------+
| SPA im CBM-Atlas-Frontend      | <-> | CBM-C-Server (PR 1860)    |
|  Reader (Monaco, read-only)    |     |  --ui  (statisches UI)    |
|  Semantic Twin + Steps         |     |  /rpc  (Graph-Queries)    |
|  Galaxy (Focus-Follow)         |     |  /api/file (Datei-Stream) |
|  Flows/Wizard/Suche/Modi       |     +---------------------------+
|  Atlas-Chat (lokal)            | <-> | llama-server (Sidecar)    |
+--------------------------------+     |  1B + 4B GGUF, optional   |
                                       +---------------------------+
```

Grundsätze:
- **Kein eigenes Node-Backend.** Provider, IR-Builder und Context-Compiler
  laufen als TypeScript im Browser; Transport ist fetch auf /rpc und die
  Datei-Streaming-Route des C-Servers. Die Provider-Abstraktion aus
  CodeAtlasIDE wird übernommen (Interface identisch, Transport neu), damit
  die portierten Module unverändert funktionieren.
- **C-Server bleibt Martins Hoheit.** Fehlende Endpoints werden als
  Upstream-Asks gesammelt (Abschnitt 8), nicht selbst in C gehackt. Bis ein
  Ask erfüllt ist, wird die Lücke ehrlich im UI benannt (KnowledgeState
  'unsupported'), niemals umschifft durch Behauptungen.
- **Monaco read-only** als Code-Reader (MIT, läuft in jedem Browser):
  Syntax-Highlighting client-seitig, unsere Schritt-Badges als Decorations,
  Cursor-/Klick-Events an den Twin. Kein LSP, kein Editieren in v1.
- **Air-gapped per Default**: Ohne Zutun des Nutzers verlässt kein Byte die
  Maschine. Der llama-Sidecar ist localhost-only. Ein Netz-Deny-Gate wie in
  CodeAtlasIDE (lsof-Sampling, 0 outbound) wird von Anfang an mitgeführt.
- **Ports**: UI/RPC nutzt den Port des C-Servers; llama-server auf 4141;
  Test-Instanzen ab 4200. NIEMALS 3000-3019 und niemals 3001 (belegt durch
  das Nachbarprojekt CodeAtlasIDE).

## 4. Look and Feel (aus den Referenz-Screenshots)

Terminal-Ästhetik, ernst und ruhig, keine Verspieltheit:
- Grundton fast schwarz (#0b0f0e-Bereich), Panels minimal abgesetzt,
  1px-Rahmen in dunklem Grün-Grau.
- Akzent: Phosphor-Grün für Marke ("CODEATLAS"), aktive Tabs, Primäraktionen
  ([finish]), Schritt-Badges; Sekundärakzent Cyan/Blau für Symbolnamen,
  Rot/Magenta sparsam für Fehlerpfade ("may raise").
- ALLES monospace. Menüzeile im Stil `[f]ile [e]dit [v]iew [a]tlas
  [t]erminal [?]help`: jeder Menüpunkt trägt seinen Buchstaben-Shortcut,
  Keyboard-first ist Pflicht, Maus ist optional.
  NACHTRAG 2026-08-29 (Bernhards Anweisung "entferne alles, was Dinge
  verspricht, die wir nicht können"): Die Zeile führt nur noch Punkte,
  hinter denen etwas liegt. `[f]ile`, `[e]dit`, `[v]iew` und `[t]erminal`
  sind entfernt, weil dieses Produkt eine Lese-IDE ist: es editiert
  nicht, es führt nichts aus, es hat kein Terminal. Geblieben sind
  `[a]tlas` mit seinen vier Aktionen und `[?]help`, das eine eingebaute
  Hilfeseite öffnet, welche genau diese Grenzen zuerst benennt. Die
  Buchstaben-Shortcuts liegen seither auf Alt/Option, damit Tippen in
  der Kommandozeile ankommt statt versehentlich ein Panel zu öffnen.
- Kopfzeile: Marke + Versions-Chip (z.B. `v0.1.0-dirty`), rechts
  Status-Chips. Fußzeile: dauerhafte Kommandozeile
  `> type a command or ask the atlas...` (Command-Palette und Chat-Eingang
  in einem), darunter Statusleiste.
- Editor-Gutter mit nummerierten Schritt-Badges (1), (2) an den Call-Sites,
  synchron zum STEPS-Panel rechts (CodeAtlasIDE-P6-Verhalten als Vorbild).
- Rechtes Panel "SEMANTIC_TWIN": flow(symbol)-Kopf, ASCII/SVG-Sequenzkasten,
  prev/next-Stepper, STEPS-Liste, aufklappbare Fakten (query, may raise).
- Untere Karte für Touren/Modi: Schrittzähler `STEP 10/10` mit
  Blockzeichen-Fortschritt, Prosa-Erklärung, Aktionen `[<-] prev
  [Enter] finish [q] exit [d] diagram`.
- Ehrlichkeitszeilen bleiben sichtbar wie im Vorbild ("Nobody wrote that
  description. CodeAtlas put it together from what the index recorded...").

## 5. Lokale LLM-Strategie (Anforderung 1)

- **Runtime**: llama.cpp `llama-server` (MIT) als einziger Inferenzprozess,
  OpenAI-kompatibles HTTP auf localhost:4141. Start/Stop übernimmt das
  Produkt (Sidecar-Manager im UI sichtbar: Modell, Status, RAM). Kein
  Cloud-Fallback. Erste Iteration darf ein lokal installiertes Ollama als
  Alternative erkennen, aber llama-server ist der Referenzpfad.
- **Modellwahl per Recherche, nicht per Vorurteil (Nutzeranweisung
  2026-08-28)**: Welche konkreten Modelle die 1B- und die 4B-Klasse
  besetzen, entscheidet eine eigene Recherche zu Beginn von W5 (Stand
  August 2026), NICHT eine Vorfestlegung. Vielleicht ist es Google/Gemma,
  vielleicht Qwen, vielleicht etwas Neues. Vorgehen:
  - Web-Recherche über seriöse Quellen (offizielle Modell-Releases,
    lmarena/LMSYS, Hugging-Face-Leaderboards, unabhängige Evals; SEO-
    Statistikseiten zählen nicht) über die Kandidatenfelder der Klassen
    ~0.5-2B und ~3-5B (z.B. Qwen3, Gemma 3, Llama 3.x, Phi-4-mini,
    SmolLM3, und was August 2026 sonst aktuell ist).
  - Kriterien in dieser Reihenfolge: (1) Instruction-Following- und
    Text-Verständnis-Benchmarks der Klasse, (2) kommerzielle Lizenz
    (Apache/MIT bevorzugt; Gemma-Terms und Llama-Lizenz sind dokumentierte
    Findings, kein K.O., aber begründungspflichtig), (3) GGUF-
    Verfügbarkeit und Qualität quantisiert (Q4_K_M), (4) Tempo auf Apple
    Silicon, (5) Deutsch- und Englisch-Qualität.
  - Ergebnis als ADR mit Quellen committen; zusätzlich muss die eigene
    Eval-Suite (Abschnitt 6, 40 goldene Fragen) die Top-2-Kandidaten je
    Klasse GEGENEINANDER messen, und der Sieger der eigenen Eval schlägt
    den Benchmark-Ruf. Modell-Downloads sind der einzige erlaubte
    Netzzugriff des Projekts und passieren einmalig, dokumentiert, vor dem
    Air-gap-Beweis.
  - Bis die Recherche gelaufen ist, gilt Qwen3 (1.7B/4B, Apache-2.0) nur
    als Platzhalter für Architektur-Annahmen (Kontextfenster-Budgets).
- **Opt-out ist ein Produktmodus**: Schalter "LLM: aus" (Default bei
  Erststart). Aus heißt aus: Sidecar wird nicht gestartet, kein Prozess,
  keine Ports, UI zeigt überall die deterministischen Texte. Die
  Policy-Mechanik aus CodeAtlasIDE (committete .codeatlas/policy.json
  schlägt Nutzerpräferenz) wird sinngemäß übernommen: eine Firma kann
  LLM-aus erzwingen.
- **Budget-Realismus**: 1B bekommt maximal ~3k Token Kontext, 4B maximal
  ~8k. Der Context-Compiler (Abschnitt 6) MUSS darunter bleiben; Antworten
  sind kurz, extraktiv, zitierend. Kein Agent, keine Tools, kein Editieren.
- **Modellbeschaffung: der Leser wählt, das Programm liefert nichts mit
  (Nachtrag W10, 2026-08-29, Anforderung Martin)**. Zwei Sätze weiter oben
  stehen "Start/Stop übernimmt das Produkt" und Qwen3 als Platzhalter. Beides
  ist überholt und bleibt als Planungsstand stehen; hier steht, was gilt:
  - Das Produkt startet keinen Prozess. Eine SPA ohne eigenes Backend kann
    das nicht (ADR 0001, Abschnitt "Engine-Entscheidung"); `llm/start.sh`
    startet, die Oberfläche erkennt, zeigt und leitet an.
  - `llm/start.sh` fährt in der Vorgabe den Router-Modus über ein
    Cache-Verzeichnis (`--models-dir`, `--models-max`, `--models-autoload`)
    und hat keinen fest verdrahteten Dateinamen mehr in seiner Vorgabe. Das
    Modell kommt als Parameter oder aus der Umgebung
    (`ATLAS_MODEL`, `ATLAS_MODELS_DIR`, `ATLAS_CTX`).
  - Das Einstellungen-Panel zeigt, was im Cache des Lesers liegt, schaltet
    ohne Neustart zwischen diesen Modellen um (Feld `model` im Request) und
    stellt für alles andere den fertigen `-hf`-Aufruf bereit, mit dem Satz
    dazu, dass er ins Netz geht und wohin er lädt. Die Anwendung selbst lädt
    nichts herunter und zeigt deshalb auch keinen Fortschritt.
  - Die sechs Kandidaten der W5-Eval bleiben als Vorschlag mit ihren
    gemessenen Zahlen; sie sind Empfehlung, nicht Festlegung. Die Wahlen
    `class-a` bis `class-b-gemma` bleiben als Reproduktionsgriffe der Eval.
- **Alles, was Rechenzeit kostet, an einem Ort (Nachtrag W10, 2026-08-29,
  Nutzerwunsch)**: "2D/3D oder sowas sollte immer zentral in einem
  Settings-Menü drin sein, nicht alles auf einer Oberfläche, wegen
  Rechenleistung falls jemand keine so starke Maschine hat." Der 2D/3D-
  Umschalter, die einzelnen Effekte (Leuchthöfe, Kantendichte,
  Beschriftungsentfernung), das Sparprofil und der Bildratendeckel sitzen im
  Einstellungen-Panel und sonst nirgends. Jede dieser Einstellungen nennt
  ihren Effekt in Zahlen, die auf der Maschine des Lesers an derselben Szene
  gemessen wurden (Bildrate vorher und nachher), nie als Versprechen; wo
  nichts messbar ist, sagt das Panel genau das. Schalter ohne Rechenkosten
  (Legende, Twin-Detailstufe, Kantenart-Filter) bleiben, wo sie sind:
  gebündelt wird das Teure, nicht alles.

## 6. Context-Compiler (Anforderung 2, das Kern-IP dieses Projekts)

Prinzip: Das Modell denkt nicht, es formuliert. Alles Denken passiert
deterministisch im Graphen, der Compiler übersetzt es in dichte, eindeutige
Textform. Bausteine:

1. **Frage-Klassifikation ohne LLM**: Regelbasiert (Frageform, @mentions,
   aktiver Fokus, aktiver Modus) auf eine von ~8 Frageklassen (Was-ist-das,
   Wer-ruft, Was-passiert-wenn, Wo-Entry, Warum-Fehler, Vergleich,
   Überblick, Sonstiges).
2. **Fakten-Paket pro Frageklasse**: Jede Klasse hat ein festes Rezept,
   welche Graph-Fakten geholt werden (IR des Fokus, Callers/Callees mit
   Zeilen, Routen, Fehlerpfade, Trace-Beobachtungen, Closure-Auszug). Die
   Rezepte portieren die semantic-ir-builder-Fragen auf /rpc.
3. **Kompression in Karten**: Fakten werden zu nummerierten Karten
   verdichtet (K1..Kn), je Karte maximal 3 Zeilen, kanonisches Vokabular
   ("K3: createUser (services/userService.ts:23) ruft validateUser [Zeile
   24], wirft ValidationError [beobachtet 2x zur Laufzeit]"). Zahlenbudget
   pro Modellklasse hart im Code, Überschuss wird mit ehrlicher Notiz
   gekappt ("7 weitere Aufrufer nicht gelistet").
4. **Antwortvertrag im Prompt**: Das Modell darf nur Karten zitieren
   (Format: Aussage [K3]), muss bei fehlender Karte "dazu liegt keine Karte
   vor, hole X mit @X" sagen, darf nichts erfinden, keine Ratschläge, keine
   Code-Änderungen. UI rendert [K3]-Zitate als Klick auf die Quelle.
5. **Eval-Suite von Anfang an**: ~40 goldene Fragen gegen das
   Demo-Fixture mit erwarteten Kernaussagen (Substring-Checks), gefahren
   gegen 1B und 4B als Regressionstest; zusätzlich der harte Test, dass
   JEDE Behauptung ein [K]-Zitat trägt (Antworten ohne Zitat = rot).
   Deterministisch via Temperatur 0 und festem Seed.

## 7. Die weiteren Kernstücke

- **Galaxy mit Focus-Follow (Anforderung 3)**: Die 3D-Galaxy aus PR 1860
  wird eingebettet, nicht neu gebaut. Neu: eine Selektions-API (Kamera
  fliegt/zoomt animiert zum Knoten, Knoten pulsiert kurz), gefüttert von
  Reader-Klicks, Twin-Steps, Suche und Chat-Zitaten; Rückrichtung: Klick im
  Graph fokussiert Reader+Twin. Stabiles Layout (im PR vorhanden) ist
  Voraussetzung, sonst ist die Kamerafahrt wertlos.
- **Einstiegsmodi (Anforderung 4)**, beim ersten Öffnen und jederzeit per
  Befehl:
  - "Neu im Projekt": Topsort-Tour (Port aus tour-generator.ts), Configs
    und Typen zuerst, wie gehabt.
  - "Ich kenne das Projekt": Entry-Point-Wahl (Liste der Entry Points und
    Routen + Bedeutungssuche), Tour startet DORT als Vorwärts-Closure-Walk
    statt beim Fundament. Kein Config-Pflichtprogramm.
  - "Bug jagen": der BUG-Wizard (statisch vs beobachtet, Port aus C26).
  - "Change abschätzen": Blast-Radius (Changes-Tab aus PR 1860 + unsere
    Risk-Rules).
  Wortwahl neutral halten (keine Lern-/Schul-Vokabeln); die Checkliste
  bleibt als stille Fortschrittsanzeige erhalten, wird aber nicht als
  Lernfunktion vermarktet.
- **Bestehende PR-1860-Views** (Overview, Dashboard, Wiki, Flows) bleiben
  unangetastet nutzbar; CodeAtlasWeb ergänzt Reader+Twin+Modi+Chat als neue
  Tabs im selben Chrome und übernimmt dessen i18n- und Ehrlichkeits-
  Standards (Wort-Budgets, Denominatoren, Caps).
- **Keine Lernversion in diesem Projekt** (Nutzerentscheidung 2026-08-28):
  Das P8-Teaching-System (Assignments, Quiz-Engine, Lehrer-Fluss) wird
  ausschliesslich im IDE-Projekt (CodeAtlasIDE) gebaut und ist hier
  explizit Non-Goal. CodeAtlasWeb behaelt nur die neutralen Einstiegsmodi
  aus diesem Abschnitt.

## 8. Upstream-Asks an Martin (früh klären, blockieren sonst)

1. Datei-Streaming-Endpoint (Datei lesen, relativer Pfad, Bytes + mtime;
   sein "über den C-Server auf die Platte streamen").
2. /rpc-Abdeckung für die IR-Fragen (Callers/Callees mit r.line/args/
   confidence, USAGE, RAISES, CONFIGURES, Routen, IMPORTS, TESTS,
   detect_changes). Inventar in W0 ergibt die konkrete Lückenliste.
3. Symbol-Deklarations-Auflösung (unser resolveSymbolNamed-Bedarf:
   search + Deklarationszeile in einem Call, sonst 2 Roundtrips).
4. ingest_traces-Query-Surface (beobachtete Kanten pro Symbol abfragbar).
5. Static-Serving der neuen Frontend-Routen unter --ui (falls sein Build
   die Asset-Liste pinnt).
6. CORS/localhost-Bindung: /rpc strikt auf 127.0.0.1 (air-gap).

## 9. Arbeitsregeln (verbindlich)

- icca-harness für jeden Zyklus: Contract, eingefrorene Abnahmetests, Red
  bestätigt, Delegation an Opus-Implementierer, Gates, Commit erst nach
  Commit-Gate. Keine Ausnahmen für "nur Frontend".
- Eigenes privates GitHub-Repo unter BernhardJackiewicz (bei Erstellung
  bestätigen lassen), Push nach jedem Commit-Gate. NIEMALS ins
  CodeAtlasIDE-Repo pushen. Martins Repo (DeusData/codebase-memory-mcp)
  ist absolut push-tabu: keine Pushes, keine PRs, keine Kommentare, keine
  Issues durch den Agenten, unter keinen Umständen. Upstream-Material wird
  ausschließlich lokal vorbereitet und an Bernhard übergeben; er reicht
  alles (auch Beiträge zu PR 1860) selbst händisch ein.
- Referenz-Repo und dessen Vorschau (Port 3001!) sind tabu: nichts killen,
  nichts bauen, nichts ändern. Kopieren von Modulen: nur per Datei-Kopie
  hierher, mit Herkunftsnotiz im Header (gleicher Urheber, kein
  Lizenzproblem, aber Nachvollziehbarkeit).
- Playwright-Klickstrecken + Screenshot-Baselines + Netz-Deny-Gate ab dem
  ersten UI-Zyklus (Muster aus tools/ des Referenz-Repos).
- Deutsch antworten, keine em/en-Dashes, keine Claude/AI-Attribution im
  Repo, Anthropic-Key existiert in diesem Projekt NICHT (es gibt keinen
  Cloud-LLM-Pfad).

## 10. Phasenplan (Zyklen, jeder mit eigenem Contract)

- **W0 Inventar + Spike (zuerst, alles weitere hängt daran)**: PR-Branch
  clonen, `make -f Makefile.cbm cbm-with-ui` bauen, Frontend-Framework und
  /rpc-Allowlist dokumentieren; Spike: eine Datei via C-Server streamen und
  in Monaco read-only mit einer Decoration anzeigen; Lückenliste für
  Abschnitt 8 schreiben. AC: INVENTAR.md committet, Spike-Screenshot,
  Upstream-Ask-Liste an Bernhard übergeben.
- **W1 Fundament**: Repo-Gerüst (Vite + Framework passend zum PR-Frontend),
  RpcTransport + Provider-Port + IR-Builder-Port gegen /rpc, Fact/
  KnowledgeState übernommen; Unit-Suite läuft; Netz-Deny-Gate steht.
- **W2 Reader + Twin**: Monaco read-only, Datei-Baum, Cursor->Symbol->Twin
  (Render-Model-Port, 4 Tiefen), Schritt-Badges + STEPS-Sync, Look nach
  Abschnitt 4 als Design-System (Tokens + Baselines).
- **W3 Galaxy Focus-Follow + Suche**: Einbettung, Selektions-API,
  Kamerafahrt, Rückrichtung; Bedeutungssuche (Port) im Fußzeilen-Command.
- **W4 Einstiegsmodi**: Modus-Dialog, Topsort-Tour-Port, Entry-Point-Modus
  (Vorwärts-Walk), BUG-Wizard-Port (mit Trace-Anzeige aus dem echten
  ingest_traces des Branches), Changes/Blast-Radius-Anbindung.
- **W5 Lokal-LLM**: Sidecar-Manager, Opt-out-Modus, Context-Compiler v1
  (Frageklassen, Karten, Zitate), Atlas-Eingabezeile als Chat, Eval-Suite
  (40 goldene Fragen, beide Modelle).
- **W6 Härtung + Abnahme**: Air-gap-Beweis (0 outbound über gesamten
  Klickpfad), Performance-Budgets (Twin warm < 800ms via /rpc), i18n-Angleich
  an PR-Standards, unabhängiges Audit mit frischem Kontext, Demo-Video-
  Klickstrecke für Martin.

Reihenfolge W2/W3 darf tauschen, W5 braucht W1+W2. Jede Phase endet mit
Bericht + Tag wie im Referenzprojekt. Das P8-Teaching-System ist bewusst
NICHT Teil dieses Projekts (lebt im IDE-Projekt).

## 11. Erste Aufgabe der frischen Session

W0 starten: Lies zuerst diese Datei und CLAUDE.md komplett, dann PR-Branch
beschaffen, bauen, Inventar schreiben. Bei Repo-Anlage (GitHub, privat)
einmal bestätigen lassen. Nichts am Referenz-Repo und nichts an Port 3001
anfassen.

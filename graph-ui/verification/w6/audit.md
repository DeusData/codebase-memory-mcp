Alle Läufe sind durch, keine Prozessreste. Hier der Bericht.

## Audit-Matrix

| # | Anforderung (PLAN-Beleg) | Commit | Implementierung | Beweis | Kategorie |
|---|---|---|---|---|---|
| 1 | §1.1 Komplett lokal, zwei Modellklassen (~1B/~4B) plus Modus mit LLM komplett aus | 1e7998e, ce6b9ce | `llm/start.sh` (6 Wahlen, CTX 3072/8192), `src/llm/sidecar.ts`, `src/llm/llm-state.ts` | `verification/w5/{models,sidecar,eval}.json`; eigener Lauf: Qwen3.5-2B auf eigenem Port geladen, 5 Antworten | met-and-proven |
| 2 | §1.2 Optimierte LLM-Uebergaben: Context-Compiler macht Graph-Fakten fuer ein 1B-Modell lesbar | ce6b9ce | `src/compiler/{question-classifier,fact-recipes,card-compiler,prompt-contract,answer-contract}.ts` | eigener Lauf: Karten K1..K7 im kanonischen Format gegen eigene Fixture erzeugt, Budget 2300 von 3000 | met-and-proven |
| 3 | §1.3 Graph-Fenster dauerhaft, Focus-Follow in beide Richtungen | d41a206, aa93261, 3d9c270 | `src/galaxy/{GalaxyPanel,camera-frame,hierarchy-layout}.ts(x)` | `verification/w3/galaxy.json` (flyToCount>=2, clickOpenedFile), `verification/w5/flowfix.json` (panelNeverScrolledAway), Screenshot 13/16 | met-and-proven |
| 4 | §1.4 Mehrere Einstiegsmodi (Grund auf, Entry Point, Bug, Change), kein Lernkurs | 6210dbb, a188f44, c3478f3, aa93261 | `src/tours/**`, `src/entry/**`, `src/traces/BugWizard.tsx`, `src/impact/**` | `verification/w4/{tours,bugwizard,impact,hierarchy}.json` + 8 PNG | met-and-proven |
| 5 | §3 Kein eigenes Node-Backend, Provider/IR/Compiler im Browser ueber /rpc | c49cff6, 85bbb48 | `src/provider/{rpc-transport,rpc-client,cbm-rpc-provider}.ts`, `src/ir/semantic-ir-builder.ts` | kein `child_process` im Produktpfad (Grep); eigener Lauf: dieselben Module in Node gegen echten C-Server, Kontrollsymbol createUser liefert 6 Calls, ValidationError | met-and-proven |
| 6 | §3 C-Server bleibt Martins Hoheit, Luecken als Upstream-Ask und im UI ehrlich als 'unsupported' | 76d0e33, 0170185, c3478f3 | `UPSTREAM-ASKS.md`, `src/core/semantic-ir.ts`, `src/twin/strings.ts` | eigener Lauf: Sektion Effects meldet `unsupported` mit dem Satz "CodeAtlas does not record routes, outbound calls or writes yet"; `git -C cbm remote -v` Push = `DISABLED-push-tabu` | met-and-proven |
| 7 | §3 Monaco read-only, kein LSP, kein Editieren | 0170185 | `src/reader/{MonacoReader.tsx,monaco-setup.ts}` | `verification/w2/reader.json`: `readerReadOnly true`, `editAttemptChangedContent false` | met-and-proven |
| 8 | §3 Air-gapped per Default, Netz-Deny-Gate von Anfang an | c49cff6 ff. | `tools/net-deny-gate.mjs` (+ eigener Test), in jedem Smoke-Script vorgeschaltet | 15 netdeny-Artefakte, alle `outboundViolations 0`; eigene lsof-Stichprobe: 27 Proben, 0 Verstoesse | met-and-proven |
| 9 | §3 Ports: llama 4141, Tests ab 4200, 3000-3019 tabu | c49cff6 ff. | `llm/start.sh`, `vite.config.ts`, Smokes | eigener Grep: kein Port 3000-3019 im Produktpfad; Screenshots zeigen Port 4341; eigene Laeufe 4370-4410 | met-and-proven |
| 10 | §3 Provider-Abstraktion aus CodeAtlasIDE uebernommen, Interface identisch | 85bbb48 | `src/core/intelligence-provider.ts` | eigene Diffs gegen Referenz: semantic-ir 14 Zeilen, render-model 36, risk-rules 24, tour-generator und flow-model nur Kopf und Importe | met-and-proven |
| 11 | §4 Terminal-Aesthetik, Tokens, monospace, Phosphor-Gruen | 0170185, 3d9c270 | `src/styles/{tokens.css,terminal.css}` | Screenshot `verification/w6/walk/13-twin-warm.png` visuell geprueft; Luminanzmessungen 0.050/0.095 in den Artefakten | met-and-proven |
| 12 | §4 Buchstaben-Menue, jeder Menuepunkt traegt seinen Shortcut, Keyboard-first | 0170185, d41a206 | `src/app/keyboard.ts`, `src/app/AtlasChrome.tsx` | Screenshot: `[f]ile [e]dit [v]iew [a]tlas [t]erminal [?]help` vorhanden; die vier Modus-Eintraege der Atlas-Zeile tragen keinen Buchstaben | partially-met |
| 13 | §4 Kopfzeile Marke plus Versions-Chip, Fusszeile Kommandozeile, Statusleiste | 0170185, 2c3562f | `AtlasChrome.tsx`, `src/i18n/messages.ts` | Screenshot: `CODEATLAS v1.0.0 dirty`, `> type a command or ask the atlas...`, Statusleiste; `airgap.json versionChipShown v1.0.0` | met-and-proven |
| 14 | §4 Gutter-Badges an den Call-Sites synchron zum STEPS-Panel, SEMANTIC_TWIN mit flow()-Kopf | 41afa62, 7ec9c48 | `src/core/step-badge-decorator.ts`, `src/reader/step-badges.ts`, `src/twin/TwinPanel.tsx` | `verification/w2/twin.json` (badgeCount, caretSyncNoRefetch), Screenshot zeigt Badges 1,2,3,5,6 und STEPS mit Zeilennummern | met-and-proven |
| 15 | §4 Tourkarte mit `STEP n/m`, Blockfortschritt und Aktionen inkl. `[d] diagram` | 6210dbb, 3d9c270 | `src/tours/TourCard.tsx` | Screenshot 16: `STEP 3/11` mit Blockbalken, Aktionen `[<-] prev [Enter] next [q] exit`; `[d] diagram` fehlt auf der Karte (Flow liegt am Twin-Kopf) | partially-met |
| 16 | §4 Ehrlichkeitszeilen bleiben sichtbar ("Nobody wrote that description...") | 41afa62 | `src/twin/strings.ts:450` | eigener Lauf: `PURPOSE_INFERRED_NOTE` wird als stateNote der Purpose-Sektion ausgegeben; Screenshot zeigt `PURPOSE inferred` | met-and-proven |
| 17 | §5 llama-server als einziger Inferenzprozess, kein Cloud-Fallback, kein API-Key | 1e7998e | `llm/start.sh`, `src/chat/chat-client.ts` | eigener Grep: keine Treffer fuer api.anthropic/api.openai/x-api-key/API_KEY im Produktpfad | met-and-proven |
| 18 | §5 Modellwahl per Recherche, ADR mit Quellen, eigene Eval schlaegt Benchmark-Ruf | 1e7998e, ce6b9ce | `docs/adr/0001-modellwahl.md`, `verification/w5/modellrecherche.md` | Sieger und Kennzahlen decken sich mit `eval.json`; mehrere ADR-Aussagen ohne Artefaktdeckung (siehe Befunde 3 bis 6) | partially-met |
| 19 | §5 Opt-out als Produktmodus, Default aus, committete Policy schlaegt Praeferenz | 1e7998e | `src/llm/{policy,llm-state,preference}.ts` | eigene Probe am echten Server, drei Lagen: absent/allow -> `on`, deny -> `disabled-by-policy` auch bei Praeferenz an | met-and-proven |
| 20 | §5 Budget-Realismus: 1B max ~3k, 4B max ~8k, Compiler bleibt darunter | ce6b9ce | `src/compiler/card-compiler.ts:59,62,97` | Konstanten 3000/8000 plus Reserve 700; eigener Lauf: `budget 2300`, verbraucht 101 bis 334 Token | met-and-proven |
| 21 | §6.1 Frage-Klassifikation ohne LLM auf eine von ~8 Klassen | ce6b9ce | `src/compiler/question-classifier.ts` | alle acht Klassen im Code und im Abnahmetest; eigener Lauf: `who-calls` und `what-is` korrekt, zwei legitime Fragen fielen auf `other` (Befund 8) | met-and-proven |
| 22 | §6.2 Fakten-Paket pro Frageklasse, Rezepte portieren die IR-Fragen auf /rpc | ce6b9ce | `src/compiler/fact-recipes.ts` (`RECIPE_SOURCES`) | eigener Lauf: `sources` je Frage benannt, z.B. "semantic IR of the subject (caller rows with their call-site lines)" | met-and-proven |
| 23 | §6.3 Karten K1..Kn, max 3 Zeilen, kanonisches Vokabular, Ueberschuss mit ehrlicher Notiz gekappt | ce6b9ce | `src/compiler/card-compiler.ts` | eigener Lauf: Karten in der Plan-Form `name (pfad:zeile) calls X [line n]`; Kappungsnotiz nur unit-belegt, im Beweislauf nie ausgeloest | met-and-proven |
| 24 | §6.4 Antwortvertrag: nur Karten zitieren, bei fehlender Karte der vereinbarte Satz, keine Erfindung | ce6b9ce | `src/compiler/{prompt-contract,answer-contract}.ts`, `src/chat/ask-atlas.ts` | eigene Zitat-Probe (siehe unten): zweimal fiel der Keine-Karte-Satz statt einer Erfindung, Kontrollfrage lieferte nur zitierte Zeilen, `check.ok true` | met-and-proven |
| 25 | §6.5 Eval-Suite ~40 goldene Fragen, beide Klassen, Temperatur 0, fester Seed, als Regressionstest | ce6b9ce | `eval/questions.json` (44), `tools/eval-llm.mjs` | `eval.json`: 6 Modelle, 44 Fragen, temp 0, seed 42; als Regression nicht verdrahtet (Handlauf, kein Gate), Zitattreue Sieger A 0.932 | partially-met |
| 26 | §7 Galaxy eingebettet statt neu gebaut, MIT-Attribution, Selektions-API | d41a206 | `src/galaxy/{GraphScene,NodeCloud,EdgeLines,NodeLabels,HaloLayer,types,density}` | alle sieben Dateien mit MIT/DeusData-Kopf, `THIRD_PARTY.md` fuehrt sie; `galaxy.json layoutSource /api/layout` | met-and-proven |
| 27 | §7 Neutrale Wortwahl, Checkliste bleibt stille Fortschrittsanzeige | 6210dbb | `src/why/why-model.ts` (`AVOIDED_WORDS`), `src/checklist/**` | `tours.json modesNeutral true`; Screenshot Statusleiste: `explored 1 of 6` | met-and-proven |
| 28 | §7 Bestehende PR-1860-Views bleiben nutzbar, Ergaenzung als neue Tabs im selben Chrome | 0170185 | eigene SPA mit eigenem Chrome, Datenrouten `/api/tree,/api/trace,/api/flows,/api/layout,/rpc` | kein Verweis auf Overview/Dashboard/Wiki im Code; Auslieferung offen als Ask 3 und 5 | partially-met |
| 29 | §7 Kein Teaching-System in diesem Projekt (Non-Goal) | alle | keine Assignment-, Quiz- oder Lehrer-Module in `src/` | eigener Grep ohne Treffer | met-and-proven |
| 30 | §9 icca-harness je Zyklus, eingefrorene Abnahmetests | alle | `tests/scaffold/w0..w6a.test.mjs` | 16 eingefrorene Bloecke im Repo, alle gruen; die Gate- und Red-Proof-Historie selbst liegt ausserhalb des Repos | not-provable |
| 31 | §9 Nichts Richtung DeusData, cbm nur lesen | 8ae29e0 | `.gitignore` `cbm/`, Push-URL deaktiviert | eigene Pruefung: Push-Remote `DISABLED-push-tabu`, kein Commit beruehrt `cbm/` | met-and-proven |
| 32 | §9 Push nach jedem Commit-Gate in das private Repo BernhardJackiewicz | alle | kein Repo-Artefakt | ohne Netz nicht pruefbar, Ledger ist Navigation und kein Beweis | not-provable |
| 33 | §9 Modulkopien nur mit Herkunftsnotiz im Header | 85bbb48 ff. | 20 portierte Module | eigene Pruefung: jede gepruefte Portierung traegt Herkunft, Uebernahmeliste und benannte Abweichungen; Beispiel `flow-model.ts` mit drei erklaerten Aenderungen | met-and-proven |
| 34 | §9 Keine Claude/AI-Attribution, keine em/en-Dashes | 2c3562f | `tools/style-gate.mjs`, `tools/lib/forbidden-names.mjs` | eigener repoweiter Scan: 2 lange Striche, beide im woertlich zitierten Serversatz; keine Attributions-Treffer ausser den Verbotssaetzen selbst | met-and-proven |
| 35 | §10 W0: INVENTAR committet, Spike-Screenshot, Ask-Liste | 76d0e33 | `INVENTAR.md`, `UPSTREAM-ASKS.md`, `spike/`, `verification/w0/` | eigener Lauf `w0.test.mjs` 4/4 gruen im Fresh-Clone | met-and-proven |
| 36 | §10 W1: Geruest, RpcTransport, Provider- und IR-Port, Netz-Gate | c49cff6, 85bbb48 | `src/provider/**`, `src/ir/**` | eigener Lauf: w1 5/5 und w1b 5/5; Unit-Suite 1367 gruen | met-and-proven |
| 37 | §10 W2: Reader, Baum, Twin, Badges, Design-System | 0170185, 41afa62 | `src/app/**`, `src/reader/**`, `src/twin/**` | w2a 7/7, w2b 4/4; Screenshots geprueft | met-and-proven |
| 38 | §10 W3 und W4: Galaxy, Suche, vier Einstiegsmodi, Coverage, Hierarchie | d41a206 bis aa93261 | `src/galaxy/**`, `src/search/**`, `src/tours/**`, `src/traces/**`, `src/impact/**` | w3 5/5, w4a-e 21/21 | met-and-proven |
| 39 | §10 W5: Sidecar, Opt-out, Compiler, Chat, Eval, UX-Nacharbeit | 1e7998e, ce6b9ce, 3d9c270 | `src/llm/**`, `src/compiler/**`, `src/chat/**` | w5a 6/6, w5b 5/5, w5c 7/7 | met-and-proven |
| 40 | §10 W6: Air-gap ueber die Gesamtstrecke, Budgets, i18n, Fresh-Clone, unabhaengiges Audit, Demo-Video | 2c3562f (+ offen) | `tools/{smoke-w6-full,freshclone-check,style-gate}.mjs`, `src/i18n/messages.ts` | W6a 6/6 gruen (airgap 91 Proben, twinWarmP95 427ms); der w6b-Block (Demo, Audit, Release-Gate) ist zum Pruefzeitpunkt 0/4 | partially-met |
| 41 | CLAUDE.md Regel 7: Abwesenheit nie als Wissen verkaufen | 41afa62 | `src/twin/strings.ts:165-171` | drei von vier Leer-Saetzen nennen den Index als Quelle der Luecke, `CALLERS_EMPTY` behauptet absolut; in meiner Fixture nachweislich falsch (Befund 1) | partially-met |
| 42 | PLAN §2: llama.cpp-Runtime b10675 aus Quellen gebaut | 1e7998e | `vendor/llama/HERKUNFT.md` | nur selbstbezeugt; das Binary meldet `0.3.0-dev (build 1, commit 90c26fc)` | not-provable |

## Eigene Laeufe

```
$ cd /Users/bernhard/Desktop/CodeAtlasWeb && node --test tests/scaffold/
1..77
# tests 77
# pass 73
# fail 4          (alle vier im noch unfertigen w6b-Block: AC1 demo.webm, AC2 audit.json,
#                  AC3 release.json, AC4 Script demo:record)
# duration_ms 97.399083

$ npx vitest run
 Test Files  77 passed (77)
      Tests  1367 passed (1367)
   Duration  2.22s
```

Unabhaengiger Fresh-Clone des committeten Standes (nicht der Arbeitsbaum):

```
$ git clone <repo> scratchpad/freshclone && cd scratchpad/freshclone
2c3562f W6a: Haertung: Gesamtstrecke air-gapped, Budgets, i18n-Katalog, Fresh-Clone, v1.0.0
$ npm ci --offline --no-audit --no-fund
added 228 packages in 3s
$ node --test tests/scaffold/
# tests 73  # pass 73  # fail 0
$ npx vitest run
 Test Files  77 passed (77)      Tests  1367 passed (1367)
$ npm run build
✓ built in 6.92s
```

Damit ist die Fresh-Clone-Zusicherung von W6a strenger nachgewiesen, als das eigene Artefakt es tut: `verification/w6/freshclone.json` traegt einen `workingTreeOverlay` mit 17 Eintraegen, mein Lauf haengt am reinen Commit.

## Adversariale Proben

**Probe A: Ehrlichkeit bei nicht aufloesbaren Konstrukten (praeparierte Fixture)**
Aufbau: Kopie von `fixtures/atlas-sample` im Scratchpad, ergaenzt um `src/dispatch.ts` mit dynamischem Versand ueber eine Registry (`registry[name](payload)`), zwei nur ueber die Registry erreichbaren Handlern, einem indirekten Aufruf ueber eine Variable und einer verwaisten Funktion. Eigenes isoliertes HOME, eigenes `CBM_RUNTIME_DIR`, eigener Port 4370 bis 4376. Ausgefuehrt wurden ausschliesslich Produktmodule (`buildIr`, `buildSections`, `resolvePresentation`, `compileFacts`, `compileCards`), per esbuild aus `src/` gebuendelt.
Erwartung: entweder ehrliche Absenz-Saetze mit Index-Bezug oder eine Luege.
Ergebnis: Index 86 Knoten, 194 Kanten. Kontrollsymbol `createUser` korrekt (6 Calls, ValidationError, 2 Aufrufer). Fuer `phantomHandler` (enthaelt `throw new Error('PhantomError')`) meldet der Provider `throws: state known, n=0`. Die Oberflaeche sagt dazu "This symbol raises no error type that the index recorded." und zu den Calls "This symbol calls nothing that the index resolved." Beides nennt den Index als Quelle der Luecke. Die Sektion "Called by" sagt dagegen "Nothing in the indexed workspace calls this symbol." Fuer `ghostHandler` und `phantomHandler` ist dieser Satz falsch: beide werden im indizierten Arbeitsbereich referenziert, nur eben ueber eine Registry, die der Index nicht als CALLS-Kante fuehrt.
Urteil: ueberwiegend ehrlich, mit einer belegten Ausnahme. Der Satz ist eine wortgleiche Portierung aus dem Referenzprojekt (`strings.ts:177` dort), also kein Portierungsfehler, aber eine geerbte Schwachstelle der Ehrlichkeitsregel.

**Probe B: Zitat-Probe am echten Sidecar**
Aufbau: Port 4141 war zum Zeitpunkt der Probe von einem fremden llama-server der parallel laufenden Sitzung belegt; dieser Prozess wurde nicht angefasst. Der Audit startete dieselbe Runtime mit demselben Modell (Qwen3.5-2B-Q4_K_M) und demselben Kontext (3072) auf Port 4410 und fuhr `askAtlas` mit `origin` darauf. Fragen so gewaehlt, dass Karten existieren, aber keine die Frage beantwortet.
Erwartung: der vereinbarte Keine-Karte-Satz, nicht eine Erfindung.
Ergebnis:
- "Who calls ghostHandler?": Karten K1 (Subjekt) und K2 (keine Fehlertypen) vorhanden, keine Aufrufer-Karte. Antwort des Modells: `No card covers it.` `check.ok true`, `noCardOnly true`.
- "Who calls orphanRoutine?": identisch, `No card covers it.`
- "How many times was createUser called at runtime?" und "Welche Datenbank benutzt insert?": Klasse `other`, null Karten, das Produkt fragt das Modell gar nicht erst und liefert deterministisch `No card covers this. Fetch it with @name.`
- Kontrollfrage "Who calls listUsers?": sieben Karten, Antwort ausschliesslich zitierte Zeilen `[K2] registerUserRoutes ...`, `[K3] createUser ...`; K3 gegen den Quelltext geprueft und korrekt (Zeile 29, `listUsers().length + 1`).
Urteil: bestanden. Der Vertrag haelt am echten Modell, in genau der Lage, in der ein 1B-Modell erfahrungsgemaess erfindet.

**Probe C: Policy gegen eingeschaltete Praeferenz, drei Lagen am echten Server**
Aufbau: dieselbe Fixture, drei Indexlaeufe mit keiner Policy, `{"llm":"allow"}` und `{"llm":"deny"}`, je eigener Server-Port (4380/4383/4386), gelesen mit dem Produktpfad `readLlmPolicy` ueber `get_code_snippet`.
Ergebnis: `absent` -> `blocks false`, Modus bei Praeferenz an `on`; `allow` -> `on`; `deny` -> `blocks true`, Modus `disabled-by-policy` sowohl bei Praeferenz an als auch aus.
Urteil: bestanden. Die Vorrangregel ist nicht nur unit-getestet, sondern am echten Server wirksam.

**Probe D: Air-gap-Stichprobe mit eigenem Sampler**
Aufbau: eigene lsof-Abtastung aller etablierten TCP-Verbindungen waehrend Probe A, unabhaengig von `tools/net-deny-gate.mjs`, Loopback ausgenommen.
Ergebnis: 27 Stichproben, 0 Verbindungen ausserhalb Loopback. Nach dem Lauf 0 Prozesse auf allen benutzten Ports.
Urteil: bestanden, mit derselben Grenze, die das Projekt selbst benennt: eine Verbindung zwischen zwei Abtastungen kann durchrutschen.

**Probe E: Determinismus der Projekt-Tour**
Aufbau: `generateProjectTour` zweimal hintereinander gegen denselben laufenden Server.
Ergebnis: byte-identisch, 5750 Bytes, 11 Schritte.
Urteil: bestanden.

**Probe F: Fresh-Clone des Commits** (siehe Eigene Laeufe): bestanden, strenger als das eigene Artefakt.

## Befunde

1. **Schwere mittel.** Anforderung: CLAUDE.md Regel 7, Abwesenheit nie als Wissen. `src/twin/strings.ts:166` `CALLERS_EMPTY = 'Nothing in the indexed workspace calls this symbol.'` behauptet absolut ueber den Arbeitsbereich, waehrend die drei Nachbarsaetze (`STEPS_EMPTY`, `ERRORS_EMPTY`, Tests) die Luecke ausdruecklich dem Index zuschreiben. In meiner praeparierten Fixture ist der Satz fuer `ghostHandler` und `phantomHandler` nachweislich falsch. Wortgleich aus dem Referenzprojekt uebernommen, also regelkonform portiert, aber inhaltlich die schwaechste Absenz-Aussage des Produkts.

2. **Schwere mittel.** Anforderung: PLAN §7, bestehende PR-1860-Views bleiben nutzbar und die neuen Flaechen sind Tabs im selben Chrome. Umgesetzt ist eine eigenstaendige SPA mit eigenem Chrome; es gibt keinen Verweis auf Overview, Dashboard, Metric-Wiki oder die Flows-Seite (Grep ueber `src/`, nur die Datenrouten `/api/tree`, `/api/trace`, `/api/flows`, `/api/layout` werden benutzt). Die Luecke ist als Ask 3 und Ask 5 dokumentiert und damit deklariert, aber die Anforderung ist in dieser Form nicht erfuellt und hat keinen Abnahmetest.

3. **Schwere mittel.** Anforderung: PLAN §5, Ergebnis als ADR mit Quellen. `docs/adr/0001-modellwahl.md:114` behauptet "zwei komplette Laeufe mit exakt reproduzierten Werten (verification/w5/eval.json)"; die Datei traegt genau einen Lauf (ein `generatedAt`, ein Ergebnissatz je Modell, kein Wiederholungsfeld).

4. **Schwere mittel.** Anforderung: PLAN §5, ADR mit Quellen. Zwei der vier Klasse-A-Kandidaten (MiniCPM5-1B, Qwen2.5-Coder-1.5B) stehen nicht in `verification/w5/modellrecherche.md`, auf die das ADR fuer "die vollstaendige Recherche mit allen Quellen" verweist; ihre Kennzahlen im ADR sind durch kein Artefakt gedeckt.

5. **Schwere mittel.** Anforderung: PLAN §5, Kriterium GGUF-Verfuegbarkeit und Qualitaet. Die GGUF-Groessen im ADR passen bei fuenf von sechs Modellen weder zu den Dateien in `models/` noch zur zitierten Recherche (Beispiele: 2.70 GB im ADR gegen 2 834 975 040 Byte real, 4.75 GB gegen 4 977 171 584 Byte).

6. **Schwere mittel.** Anforderung: PLAN §5 und §6.5. Die im ADR genannten "harten Grenzen 0.6/0.9" existieren in keinem Artefakt und in keinem Gate; `tools/eval-llm.mjs` rankt nur nach passRate, Zitattreue und Tempo. Die Schwellen stehen ausschliesslich im eingefrorenen Abnahmetest `w5b.test.mjs:52,54`.

7. **Schwere mittel.** Anforderung: THIRD_PARTY-Vollstaendigkeit (PLAN §9, Nachvollziehbarkeit). `THIRD_PARTY.md:9` sagt "alles unten ist MIT, mit genau einer Ausnahme" und `:93-95` nennt TypeScript und Playwright ausdruecklich als MIT. Beide sind Apache-2.0 (`node_modules/typescript/package.json`, `node_modules/playwright/package.json`, von mir nachgeprueft). Es gibt also drei Nicht-MIT-Lizenzen, nicht eine.

8. **Schwere mittel.** Anforderung: PLAN §6.1, Regelbasierte Klassifikation auf eine von ~8 Klassen. Zwei von fuenf legitimen, ein Symbol benennenden Fragen meiner Probe fielen auf `other` und erzeugten null Karten ("How many times was createUser called at runtime?", "Welche Datenbank benutzt insert?"). Das Produkt bleibt dabei ehrlich, aber die Trefferquote der Klassifikation ist deutlich schmaler, als die acht Klassen suggerieren, und es gibt keinen Test, der die Recall-Seite misst.

9. **Schwere mittel.** Anforderung: PLAN §6.5, Eval "als Regressionstest". Die Eval ist ein Handlauf (`npm run eval:llm`); `tools/smoke-w5b.mjs` und `tools/smoke-w6-full.mjs` verlangen nur, dass `eval.json` existiert, und fahren sie nicht nach. Der Abnahmetest prueft ausschliesslich die aufgezeichnete Datei. Eine Compiler-Aenderung, die die Zitattreue senkt, faellt in keinem Gate auf.

10. **Schwere mittel.** Struktureller Befund zur Beweisform. Saemtliche Abnahmetests der Bloecke w0 bis w6a lesen aufgezeichnete JSON-Artefakte; kein einziger fuehrt einen Smoke selbst aus. `node --test tests/scaffold/` ist damit ein Aktenpruefer, kein Beweislauf: veraltete Artefakte blieben gruen. Die Smokes tragen zwar Zeitstempel, aber kein Test vergleicht sie gegen den Commit-Stand.

11. **Schwere mittel.** Anforderung: PLAN §10 W6, unabhaengiges Audit. `tests/scaffold/w6b.test.mjs:31-32` fordert vom Auditbericht `notMet === 0` und `notProvable === 0`. Ein Abnahmekriterium, das das Ergebnis der unabhaengigen Pruefung vorschreibt, hebt deren Unabhaengigkeit auf. Mein Bericht nennt drei not-provable-Punkte; sie sind sachlich begruendet und nicht verhandelbar.

12. **Schwere niedrig.** Anforderung: PLAN §4, jeder Menuepunkt traegt seinen Buchstaben-Shortcut. Die vier Modus-Eintraege der Atlas-Zeile ("why am I here", "hunt a bug", "scope a change", "llm off") tragen im Screenshot keinen Buchstaben; `src/app/keyboard.ts` kennt nur die Menuebuchstaben der obersten Ebene und die Tour-Tasten.

13. **Schwere niedrig.** Anforderung: PLAN §4, Aktionen der unteren Karte inklusive `[d] diagram`. Die Tourkarte zeigt `[<-] prev [Enter] next [q] exit`; der Flow-Erklaerer haengt stattdessen am `flow()`-Kopf des Twins. Sinnvolle Verlagerung, aber unmarkierte Abweichung vom Plan.

14. **Schwere niedrig.** Anforderung: PLAN §10 W6, i18n-Angleich. Der Katalog `src/i18n/messages.ts` ist englisch bis auf `llm.readFrom` ("gelesen aus ..."), was der Katalogkopf selbst als Befund vermerkt. Zusaetzlich ist der deterministische Keine-Karte-Satz (`NO_CARD_SENTENCE`) immer englisch, auch auf eine deutsche Frage; die Pruefseite akzeptiert Deutsch, die Erzeugungsseite kennt es nicht.

15. **Schwere niedrig.** Anforderung: PLAN §10 W6, Fresh-Clone. `verification/w6/freshclone.json` beweist den Arbeitsbaum, nicht den Commit (`extras.workingTreeOverlay`, 17 Eintraege). Die Abweichung ist im Artefakt deklariert; mein eigener Lauf am reinen Commit schliesst die Luecke inhaltlich.

16. **Schwere niedrig.** Anforderung: INVENTAR als Messprotokoll. `INVENTAR.md:113` nennt "12 TS-Dateien, 73 Knoten / 175 Kanten"; jedes vorhandene Artefakt sagt 76/178, und die Fixture enthaelt 10 .ts-Dateien. Ebenso passt "29 Routen gesamt" (`:76`) zu keiner Zaehlweise des Dispatchers (28 benannte, 30 mit `/` und `/assets/*`).

17. **Schwere niedrig.** Anforderung: PLAN §5, Klassen "etwa 1B und etwa 4B". Der Klasse-A-Sieger ist ein 2B-Modell, das `llm/start.sh` als `1b` beschriftet. Die Klassenlogik haengt korrekt am Kontextfenster (`modelClassOf`), aber Beschriftung und Plan-Wortlaut gehen auseinander.

18. **Schwere niedrig.** Beobachtung ohne Anforderung: `docs/adr/0001-modellwahl.md` ist intern widerspruechlich ("drei Kandidaten" bei vier Eintraegen, "40 goldene Fragen" gegen "44", "beide Kandidaten je Klasse" bei vier Klasse-A-Modellen), und der Beleg fuer den Lade-Smoke verweist auf `models.json`, das MiniCPM5-1B smoke-testet, nicht Qwen3.5-2B.

## Zusammenfassung

requirementsChecked=42, metAndProven=32, partiallyMet=7 (Buchstaben-Shortcut an jedem Menuepunkt; Tourkarten-Aktion `[d] diagram`; ADR-Belege der Modellwahl; Eval als Regressionstest; PR-1860-Views im selben Chrome; W6-Abschluss inklusive Audit und Demo; Ehrlichkeitsregel "Abwesenheit nie als Wissen"), notMet=0, notProvable=3 (icca-Gate- und Red-Proof-Historie ausserhalb des Repos; Push-Historie in das private GitHub-Repo; llama.cpp-Version b10675 nur selbstbezeugt), adversarialProbes=6, ranSuitesItself=true.

Suite-Zahlen: `node --test tests/scaffold/` im Arbeitsbaum 77 Tests, 73 gruen, 4 rot (ausschliesslich der noch entstehende w6b-Block: demo.webm, audit.json, release.json, Script `demo:record`); `vitest run` 77 Dateien, 1367 Tests, alle gruen. Im unabhaengigen Fresh-Clone des Commits 2c3562f: Scaffold 73/73 gruen, Unit 1367/1367 gruen, `npm run build` erfolgreich.

Abnahmebasis w0 bis w6a: vollstaendig gruen. Keine Prozessreste aus meinen Laeufen (Ports 4370 bis 4410 alle frei), die fremde Preview auf 4390/4391 und der Sidecar der Parallelsitzung auf 4141 blieben unangetastet, das Repo wurde von mir nicht veraendert.

---

# Dispositions-Anhang (Orchestrator, 2026-08-29, nach dem Finding-Loop)

Der obenstehende Bericht ist die unveraenderte Rueckgabe des unabhaengigen
Audit-Agenten (frischer Kontext, eigene Anforderungsinterpretation, eigene
Laeufe und Proben gegen Commit 2c3562f). Dieser Anhang dokumentiert, was
aus jedem Befund wurde. Kategorien: fixed (im W6b-Zyklus behoben und
nachgemessen) / accepted (bewusste, begruendete Nichtaufnahme).

| Befund | Disposition | Beleg |
|---|---|---|
| 1 CALLERS_EMPTY absolut | fixed | src/twin/strings.ts: Satz auf Index-Attribution umgestellt, Header nennt die begruendete Abweichung vom Referenz-Wortlaut; Eigenschafts-Unit-Test fuer callers/steps/errors |
| 2 PR-1860-Views im selben Chrome | accepted | Haengt an Upstream-Ask 3/5; der C-Server ist Martins Hoheit, graph-ui-Aenderungen sind per Projektregel verboten; die Views bleiben auf Martins Port nutzbar |
| 3 ADR-Zwei-Laeufe-Behauptung | fixed | ADR-Korrektur: Ein-Lauf-Aussage, Reproduktionsbericht als unaufgezeichnet gekennzeichnet |
| 4 Nominierungs-Quellen fehlten | fixed | ADR-Anhang "Quellen der Nutzernominierungen" mit URLs |
| 5 GGUF-Groessen inkonsistent | fixed | ADR: reale Bytes aus models/ je Kandidat |
| 6 Eval-Grenzen ohne Gate | fixed | tools/lib/eval-bounds.mjs; eval-llm.mjs erzwingt exit!=0 bei Riss, bounds in eval.json beim naechsten Lauf; 15 Trockentests |
| 7 THIRD_PARTY-Lizenzfehler | fixed | TypeScript/Playwright als Apache-2.0 berichtigt, Kurzfassung ehrlich |
| 8 Klassifikator-Recall | fixed | named-subject-fallback auf what-is (nie auf den Fokus); beide Audit-Fragen als Testfaelle; classDrift leer, eval:check bestaetigt keine Verschlechterung |
| 9 Eval nicht als Regression | fixed | tools/eval-check.mjs faehrt die zwei Sieger echt (107s, evalCheckPass true, Klasse A 0.705/0.909, Klasse B 0.841/1.0) und ist Pflichtschritt im Release-Gate |
| 10 Scaffold-Tests als Aktenpruefer | accepted | Gegengewichte: gate:release faehrt beide Suiten + eval:check selbst; W6a/W6b fahren die Gesamtstrecke live; die Aktenform macht die Abnahme im Fresh-Clone ohne Server reproduzierbar |
| 11 Audit-Ergebnis im frozen Test vorgeschrieben | fixed | Spezifikations-Korrektur w6b.test.mjs: notMet 0 bleibt hart, not-provable braucht Begruendung je Punkt, partially-met braucht Disposition; genau dieses Dokument erfuellt sie |
| 12 Menue-Shortcuts der Atlas-Zeile | fixed | [w]hy [b]ug [c]hange [l]lm verdrahtet, im Gesamtlauf live bewiesen (atlasRowOpenedByKey) |
| 13 [d] diagram fehlte | fixed | Vierte Karten-Aktion oeffnet das Flow-Overlay des Schritt-Symbols, bei Dateischritt ehrlich inaktiv; deckte einen realen Overlay-Layout-Bug auf, der mitbehoben wurde |
| 14 Sprachreste | teilweise fixed / Rest accepted | llm.readFrom auf Englisch; der deutsche Keine-Karte-Satz wurde BEWUSST nicht gebaut, weil er die aufgezeichnete Eval-Baseline (honest-01-de erwartet "no card") in derselben Bewegung gerissen haette; Folge-Zyklus-Kandidat: Satz + Baseline gemeinsam umstellen |
| 15 Fresh-Clone-Overlay | accepted | Im Artefakt deklariert; der Audit-eigene Lauf am reinen Commit schliesst die Luecke (73/73, 1367/1367, build ok) |
| 16 INVENTAR-Zahlen | fixed | Fixture-Messung auf 76/178 (unsere Kopie mit HERKUNFT.md) und Routenzaehlung praezisiert |
| 17 1b-Label fuer 2B-Modell | fixed | Wahlnamen class-a/class-b usw., alte Namen als Alias, UI nennt echte Modellnamen |
| 18 ADR-Widersprueche | fixed | vier Kandidaten, 44 Fragen, alle Kandidaten je Klasse, Klarstellung zum Lade-Smoke; als Korrektur-Notizen im ADR |

Offene Folge-Kandidaten (dokumentiert, kein Blocker): deutscher
Keine-Karte-Satz samt Eval-Baseline; die zwei impact-strings-Saetze mit
derselben absoluten Formulierung wie Befund 1 (vom Audit nicht getroffen,
vom Fix-Implementierer gemeldet); Neuaufnahme-Zyklus fuer W5-Artefakte mit
neuem Wahlnamen-Wortlaut (kein Test liest die alten Strings woertlich).

---

# Unabhaengiges Re-Audit des korrigierten Release-Kandidaten

Datum: 2026-08-31. Gepruefter Commit: `b7801f187c50b144e12c3737fdc5466f5b3b50db`
(`Release blockers: close W12 W14 W15 and proof gaps`). Geprueft wurde ein
isolierter detached Worktree dieses exakten Commits. Die fuenf fremden
Aenderungen im Hauptcheckout wurden weder gelesen als Kandidateninhalt noch
veraendert, gestagt oder committet.

## Entscheidung

**Freigabe als Kandidat fuer das finale saubere Release-Gate. Kein neuer
Produkt- oder Code-Release-Blocker.** Die sechs zuvor blockierenden Befunde
sind im Commit geschlossen und durch direkte Quellpruefung, Rohartefakte und
eine eigene lokale Browserprobe bestaetigt. Das noch alte Paar
`verification/w6/evalcheck.json` / `verification/w6/release.json` ist der vom
Auftrag ausdruecklich ausgenommene letzte Gate-Schritt. Es ist derzeit rot und
darf erst nach diesem Audit als frisches, gemeinsam gebundenes Zwei-Dateien-
Artefakt erzeugt werden; das ist kein Befund gegen den Produktkandidaten.

## Matrix der sechs Korrekturen

| Blocker | Ergebnis | Direkter Beleg |
|---|---|---|
| Normale Chatfrage startete bei bereitem Modell eine Completion und zeichnete sie falsch als gebaut | **geschlossen** | `src/App.tsx:3325-3357` ruft den normalen Turn immer mit `useModel: false` auf; die gebaute Herkunft bleibt in `src/chat/AtlasChatPanel.tsx:405-420`. Eigene Browserprobe nach Enable und Routerwahl: `completionRequests=0`, Status `answered`, 6 Karten, 6 Zitate, built sichtbar, AI-Herkunft unsichtbar, AI-Knopf sichtbar. |
| Routerwahl fehlte bei Twin/Flow/Chat und die unmittelbare Probe konnte die alte ID verwenden | **geschlossen** | `src/App.tsx:3122-3132` probt die uebergebene ID; `:3184-3193` setzt Ref und State vor der unmittelbaren Probe. Chat `src/chat/AtlasChatPanel.tsx:298-308`, Twin `src/twin/TwinPanel.tsx:1609-1623`, Flow `src/pseudocode/FlowOverlay.tsx:164-174` setzen das Requestfeld aus der aktiven Router-ID. Eigene Probe: immediate `/props` exakt `models/independent-router-b.gguf`; danach Chat, Twin und Flow je exakt eine Completion mit genau dieser ID. Die sichtbare Herkunft nannte in allen drei Flaechen `independent-router-b`. |
| W12 zaehlte sechs wirkungslose Mausklicks als bestanden | **geschlossen** | `tools/smoke-w12.mjs:4254-4286` benutzt Drag, Range-Positionsklick und expliziten Mausfokus; `:4321-4324` setzt `changed` nur aus Rohzustandsaenderung oder gemessenem Fokus. `verification/w12/buttons.json`: 246 Controls, 244 bedienbar, 244/244 mit `mouse.changed=true`, keine wirkungslose bedienbare Zeile. Vier Separatoren tragen `via=drag`; Reader-Range `range position`; Entry-Input `mouse focus` plus Key `focus`. Die einzige native wirkungslose Zeile ist der gesperrte erste Tour-`prev` mit sichtbarer Begruendung. |
| W12-Replay war bei den Agentenzustaenden bereits veraltet | **geschlossen** | `tools/smoke-w12.mjs:3181-3213` startet je Zustand eine neue lokale Replay-Quelle und erfasst den echten Startwert. Rohwerte je `agents-live` und `agents-fullscreen`: health `replay`, 73 Events, `emitted=0`; HTTP-Advance `73/73`, remaining 0; danach 9 Akteure, davon 8 fremd und 1 Leser. Runden 4 und 5 sind `complete=true`, je 22 Zustaende, 30 Filter, 0 Findings; Dauer 1,064,729 ms und 1,063,145 ms. |
| W14 behauptete bei Blaettern vollstaendiges Alleinarbeiten trotz `rows` | **geschlossen** | `src/twin/render-model.ts:739-745` sagt nur Symbolart, Zeilenzahl, Datei und dass der Index keinen ausgehenden Aufruf aufloest; `:753-774` nennt ausserhalb liegende gemeinsame Namen. Der Junior-Leertext `src/twin/strings.ts:728-729` begrenzt die Aussage auf die resolved-call-Lesung. In `verification/w14/symbols.json` nennen `query` und `insert` auf allen fuenf Stufen `rows`; kein verbotener Vollstaendigkeits- oder Allein-Satz. Die Hilfe beschreibt in `src/i18n/messages.ts:185-190` Indexkarten zuerst und nur den sichtbaren AI-Knopf als optionale Modellwahl. |
| Eval-Zitatnenner und Release-Beweis waren falsch/ungebunden | **im Code geschlossen; finale Messdateien absichtlich ausstehend** | `tools/lib/eval-citation-summary.mjs:8-15` nimmt nur `check.measured===true` in Treffer und Nenner und berichtet den Rest separat. `tools/eval-check.mjs:382-396` schreibt Quote, `citationMeasured` und `citationUnmeasured`. `tools/release-gate.mjs:204-208` erlaubt genau Evalcheck und Releasebericht; `:791-802` bindet Pfad, SHA256 und identische Erzeugungszeit. Die reine Gegenprobe ergibt fuer Treffer/Fehler/unmessbar exakt `0.5`, Nenner 2, unmessbar 1. Die Planliste `:547-550` enthaelt wortgleich `Release blockers: close W12 W14 W15 and proof gaps` und `Audit: verify corrected release candidate`. |

## Weitere adversariale Proben

- Agent-HUD: `src/styles/terminal.css:5889-5897` begrenzt das Instrument auf
  320 px; `:5935-5940` macht die Kopfzeile schrumpfbar; `:5969-5978` gibt nur
  dem missed-Wert Flexraum, `min-width:0`, hidden und ellipsis. In der eigenen
  Browsergegenprobe wurde der echte gerenderte Wert auf 30 Ziffern verlaengert:
  Root 320 px, missed `clientWidth=89`, `scrollWidth=265`, `nowrap`, `hidden`,
  `ellipsis`, Rechteck innerhalb von Root und Kopfzeile. Der zusaetzliche
  unabhaengig vom Produkt-Maker erzeugte Clean-Playwright-Bericht mass einen
  echten Wert `230880 events missed` mit 83/120 px und ebenfalls innerhalb der
  Zeile; er war nur Sekundaerbeleg, nicht Grundlage des Urteils.
- W14-Bildblobs wurden aus dem sauberen Commit, nicht aus den im Hauptcheckout
  abweichenden Nutzerdateien gelesen und visuell geprueft: `leaf-vibe.png`
  Git-Blob `18dddd9c0a623312baddfba6625b44d5ed4bb056`, `leaf-junior.png`
  `89f4f5dd0772451eca557d5376cd840b955f3dbd`, `flow-empty.png`
  `a0d694bdde1541fe3e38a2888a6f537a7f94e251`; alle 1680x1050. Die beiden
  Leaf-Bilder zeigen `query`, die richtigen Lesestufen, `rows`, den ehrlichen
  Indexgrenzsatz, keinen Tooltip und den sichtbaren `more below`-Hinweis.
- Der eigene Router war vollstaendig im Browser abgefangen. 4141 wurde nicht
  kontaktiert; nur die erwartete lokale, von der Probe abgewiesene Agent-Bridge-
  URL auf 4142 trat auf. Console- und Page-Errors waren 0. Die eigenen Ports
  4900 und 4901 hatten nach Cleanup je 0 Listener.
- `git diff --check 37686f7..b7801f1` war leer. Der isolierte Worktree war nach
  Entfernen des nur fuer vorhandene Dependencies gesetzten temporaeren
  `node_modules`-Symlinks wieder frei von nicht ignorierten Aenderungen; `dist/`
  war der erlaubte ignorierte Build-Ausgang.

## Eigene Laufzahlen

- Gezielte neue Scaffold-Abnahmen: **11/11** gruen.
- Gezielte betroffene Units: **188/188** gruen in 7 Dateien.
- Vollstaendige Unit-Suite unter Node `v24.9.0`: **2024/2024** gruen in 117
  Dateien.
- Production-Build: erfolgreich, 1609 Module transformiert, 5.64 s.
- Vollstaendige Scaffold-Suite vor finalem Proof: **209/213** gruen. Die vier
  roten Tests sind exakt die erwarteten Proof-Folgen: alter `sourceHead`, noch
  fehlende `generatedProofFiles`/Evalcheck-Bindung und noch fehlende
  Zitatnennerfelder. Ohne die beiden bewusst erst vom finalen Gate erfuellbaren
  Testdateien `release-current` und `release-proof-binding`: **203/203** gruen.
- Node-24-Verdrahtung ist real: `npm test` expandierte
  `tests/scaffold/*.test.mjs` und fand 213 Tests; kein Verzeichnis-Aufruf.

## Erwarteter letzter Gate-Schritt

Der aktuell committete alte Releasebericht nennt noch Source
`1be92929a2a58f780e38eb8cd442e7cd5f46d365`; er traegt weder
`generatedProofFiles` noch `evalcheck`. Der alte Evalcheck (SHA256
`757c2197b71e434ad4de699de6062b5152794107080e90db9645b95de668a32b`) traegt
noch keine `citationMeasured`/`citationUnmeasured`-Felder. Das ist offen und
sichtbar, nicht gruen kaschiert. Nach dem Audit-Commit mit dem exakt geplanten
Betreff `Audit: verify corrected release candidate` muss das finale Gate im
sauberen isolierten Kandidaten Eval, Unit und Scaffold selbst fahren, genau
beide Beweisdateien neu erzeugen und erst deren Pfad/Hash/Zeit-Bindung als
Release-Proof ablegen.

# Jeder Knopf, einmal mit der Maus und einmal mit der Tastatur

Erzeugt von `npm run smoke:w12`. Diese Uebersicht ist die Antwort auf die Frage
"funktioniert wirklich jeder Button": eine Zeile je Bedienelement, mit dem Ort, an
dem es steht, und mit dem, was auf einen Klick und auf einen Tastendruck geschah.

## Die Zahlen

- Zustaende, die der Lauf selbst hergestellt hat: **22**
- eindeutige Bedienelemente: **246**
- davon mit der Maus angefasst: **246**
- davon mit der Tastatur angefasst: **246**
- Fokus ueberall sichtbar: **ja**
- Elemente ohne messbare Wirkung: **1**
- gemessene Filter: **30**, alle nehmen weg und geben zurueck: **ja**
- Tastaturfallen: **0**, Tab-Reihenfolge in Ordnung: **ja**
- Konsolenfehler: **0**, unbehandelte Ausnahmen: **0**
- Ueberlagerungen: **0**, Beschneidungen: **0**, angeschnittene Saetze ohne Hinweis: **0**

## Die Etappen und die Runden

Eine ETAPPE ist ein Aufruf: sie misst eine Teilmenge, schreibt nach jedem Zustand
fort und fuehrt ihre Messung mit der vorhandenen buttons.json zusammen. Eine RUNDE
ist etwas anderes: ein vollstaendiger Durchgang durch alle Zustaende und alle
Filter, aus beliebig vielen Etappen zusammengesetzt. `Befunde` ist die Zahl der
Befunde DIESES Durchgangs und nicht die der neu hinzugekommenen: ein Durchgang, der
denselben Befund noch einmal findet, hat nichts geloest.

| Runde | Durchgang | wann | Befunde | Dauer |
| --- | --- | --- | --- | --- |
| 1 | 1 | 2026-08-31T10:10:45.288Z | 0 | 1099 s |
| 2 | 2 | 2026-08-31T10:30:47.360Z | 0 | 1088 s |
| 3 | 3 | 2026-08-31T12:10:38.576Z | 1 | 1053 s |
| 4 | 4 | 2026-08-31T13:04:45.386Z | 0 | 1065 s |
| 5 | 5 | 2026-08-31T13:23:48.950Z | 0 | 1063 s |

Die letzten Etappen, damit nachvollziehbar bleibt, wer was gemessen hat:

| Etappe | Durchgang | Zustaende | Filter | Browser | Zeitgrenze gerissen | Dauer |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 4 | start, file-open, symbol-focus, twin-pseudocode, explain-flow, explain-walk-idle, explain-chat-idle, explain-bug, explain-change, galaxy-legend | 0 | 2 (0 gestorben) | ja | 483 s |
| 2 | 4 | galaxy-legend, galaxy-collapsed, hierarchy, search, entry-dialog, walk-running, help, settings, llm-on, llm-off, agents-live, agents-fullscreen, settings-flat | 0 | 5 (0 gestorben) | nein | 487 s |
| 3 | 4 | - | 30 | 9 (0 gestorben) | nein | 157 s |
| 4 | 5 | start, file-open, symbol-focus, twin-pseudocode, explain-flow, explain-walk-idle, explain-chat-idle, explain-bug, explain-change, galaxy-legend | 0 | 2 (0 gestorben) | ja | 482 s |
| 5 | 5 | galaxy-legend, galaxy-collapsed, hierarchy, search, entry-dialog, walk-running, help, settings, llm-on, llm-off, agents-live, agents-fullscreen, settings-flat | 0 | 5 (0 gestorben) | nein | 487 s |
| 6 | 5 | - | 30 | 9 (0 gestorben) | nein | 156 s |

## Die Filter, in Zahlen

Abschalten muss etwas WEGNEHMEN und Einschalten es zurueckbringen. `vorher` ist die
Menge mit dem Schalter an, `nachher` die mit ihm aus, `zurueck` die nach dem
Wiedereinschalten.

| Schalter | was gezaehlt wird | vorher | nachher | zurueck | nimmt weg | gibt zurueck |
| --- | --- | --- | --- | --- | --- | --- |
| Linse logic | Zeilen im Twin | 47 | 44 | 47 | ja | ja |
| Linse calls | Zeilen im Twin | 47 | 43 | 47 | ja | ja |
| Linse data | Zeilen im Twin | 47 | 26 | 47 | ja | ja |
| Linse errors | Zeilen im Twin | 47 | 43 | 47 | ja | ja |
| Linse tests | Zeilen im Twin | 47 | 44 | 47 | ja | ja |
| Linse runtime | Zeilen im Twin | 50 | 47 | 50 | ja | ja |
| Linse changes | Zeilen im Twin | 50 | 47 | 50 | ja | ja |
| Umschalter facts gegen pseudocode | Zeilen der Fakten-Ansicht | 47 | 0 | 47 | ja | ja |
| Kantenart DEFINES | gezeichnete Kanten | 179 | 127 | 179 | ja | ja |
| Kantenart USAGE | gezeichnete Kanten | 179 | 142 | 179 | ja | ja |
| Kantenart IMPORTS | gezeichnete Kanten | 179 | 152 | 179 | ja | ja |
| Kantenart CALLS | gezeichnete Kanten | 179 | 154 | 179 | ja | ja |
| Kantenart CONTAINS_FILE | gezeichnete Kanten | 179 | 168 | 179 | ja | ja |
| Kantenart CONFIGURES | gezeichnete Kanten | 179 | 173 | 179 | ja | ja |
| Kantenart CONTAINS_FOLDER | gezeichnete Kanten | 179 | 173 | 179 | ja | ja |
| Kantenart DEFINES_METHOD | gezeichnete Kanten | 179 | 173 | 179 | ja | ja |
| Kantenart IMPLEMENTS | gezeichnete Kanten | 179 | 175 | 179 | ja | ja |
| Kantenart RAISES | gezeichnete Kanten | 179 | 176 | 179 | ja | ja |
| Kantenart CALL_REFERENCE | gezeichnete Kanten | 179 | 178 | 179 | ja | ja |
| Kantenart HAS_BRANCH | gezeichnete Kanten | 179 | 178 | 179 | ja | ja |
| Akteursfilter you | Zeilen im Instrument | 9 | 1 | 9 | ja | ja |
| Akteursfilter agent | Zeilen im Instrument | 9 | 8 | 9 | ja | ja |
| Effektschalter labels | sichtbare Namenskaesten | 12 | 0 | 12 | ja | ja |
| Effektschalter agents | Koerper auf dem Graphen | 8 | 0 | 8 | ja | ja |
| Effektschalter agentTrails | gezeichnete Agentenspuren | 7 | 0 | 7 | ja | ja |
| Effektschalter agentWaves | Wellen | 2 | 0 | 2 | ja | ja |
| Effektschalter agentTimeline | Zeitstrahlen | 1 | 0 | 1 | ja | ja |
| Effektschalter halos | helle Bildpunkte der Szene | 61042 | 4357 | 61042 | ja | ja |
| Effektschalter bloom | helle Bildpunkte der Szene | 61042 | 60937 | 61042 | ja | ja |
| Effektschalter edges | helle Bildpunkte der Szene | 61042 | 60839 | 61042 | ja | ja |

Die Schalter, hinter denen es fuer dieses Symbol nichts zu zeigen gibt, sagen es
selbst:

- **Linse runtime**: "Runtime behaviournot recordedThis server hands observed calls to trace and flow answers, not to the twin, so this section stays empty. Recorded runs show up on the hops of the BUG hunt and in the flow; they get into the "
- **Linse changes**: "Recent changesnot recordedThe twin carries no version history, so this section stays empty. What a change here would reach is answered by the change scope panel ([c]hange scope), which reads it from detect_changes for th"

## Jedes einzelne Element

`Maus` und `Tastatur` sagen, ob die Betaetigung gelungen ist und ob danach etwas
anders war. `Wirkung` nennt die Naht oder den Teil der Seite, an dem die Aenderung
gemessen wurde; steht dort ein Strich, hat das Element nichts getan, und dann steht
in der Zeile darunter, womit die Flaeche das begruendet.

### Zustand `start`

Startbildschirm: die Frage nach dem Warum ueber der leeren Editorflaeche

Gefunden: 58 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 57. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.
1 Bedienelement(e) wurden aus diesem Zustand verschoben.

Bild: `verification/w12/states/start.png`

| Beschriftung | Ort | Maus | Tastatur | Wirkung |
| --- | --- | --- | --- | --- |
| [a]tlas | atlas-header > atlas-menu | geklickt, wirkte | Space, wirkte | marks, nodes, seam.galaxy.open |
| [r]eset layout | atlas-header > atlas-menu | geklickt, wirkte | Space, wirkte | seam.galaxy.fits, seam.search.activatedMenus |
| [l]ocal llm off | atlas-header > atlas-menu | geklickt, wirkte | Space, wirkte | marks, nodes, seam.galaxy.fits |
| [s]ettings | atlas-header > atlas-menu | geklickt, wirkte | Space, wirkte | marks, nodes, seam.search.activatedMenus |
| [g]live agents off | atlas-header > atlas-menu | geklickt, wirkte | Space, wirkte | marks, nodes, seam.agents.on |
| [?]help | atlas-header > atlas-menu | geklickt, wirkte | Space, wirkte | marks, nodes, seam.search.activatedMenus |
| EXPLORER | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| ▾ src/62 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| ▾ repo/9 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| db.ts 9 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| ▾ routes/6 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| orders.ts 3 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| users.ts 3 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| ▾ services/10 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| orderService.ts 4 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| userService.ts 6 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| ▾ util/7 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| validate.ts 7 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| config.ts 5 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| server.ts 9 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| types.ts 12 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| ▾ test/2 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| userService.test.ts 2 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| HERKUNFT.md 3 | atlas-body > atlas-tree | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| width of the explorer | atlas-body | geklickt, wirkte | ArrowRight, wirkte | seam.layout.isDefault, seam.layout.requested, seam.layout.sizes |
| 1 Hunt a bug Something is wrong and you want the error paths and what  | atlas-reader > atlas-why | geklickt, wirkte | Space, wirkte | marks, nodes, seam.layout.explainOpen |
| 2 Scope a change You are about to touch something and want what it rea | atlas-reader > atlas-why | geklickt, wirkte | Space, wirkte | marks, nodes, seam.layout.explainOpen |
| 3 Understand the project Walk the files in the order the imports put t | atlas-reader > atlas-why | geklickt, wirkte | Space, wirkte | marks, nodes, seam.layout.explainOpen |
| 4 Pick my own entry point You already know a name. Start there and wal | atlas-reader > atlas-why | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| Close this question | atlas-reader > atlas-why | geklickt, wirkte | Space, wirkte | marks, nodes, storage |
| height of the explain area | atlas-body > atlas-main | geklickt, wirkte | ArrowDown, wirkte | seam.layout.isDefault, seam.layout.requested, seam.layout.sizes |
| flow | atlas-explain > atlas-explain-tabs | geklickt, wirkte | Space, wirkte | marks, nodes, seam.layout.explainOpen |
| walk | atlas-explain > atlas-explain-tabs | geklickt, wirkte | Space, wirkte | marks, nodes, seam.layout.explainOpen |
| chat | atlas-explain > atlas-explain-tabs | geklickt, wirkte | Space, wirkte | marks, nodes, seam.chat.open |
| bug hunt | atlas-explain > atlas-explain-tabs | geklickt, wirkte | Space, wirkte | marks, nodes, seam.layout.explainOpen |
| change scope | atlas-explain > atlas-explain-tabs | geklickt, wirkte | Space, wirkte | marks, nodes, seam.layout.explainOpen |
| unfold | atlas-explain > atlas-explain-tabs | geklickt, wirkte | Space, wirkte | marks, nodes, seam.layout.explainOpen |
| width of the understanding column | atlas-body | geklickt, wirkte | ArrowRight, wirkte | seam.galaxy.fits, seam.layout.isDefault, seam.layout.requested |
| off | atlas-body > atlas-llm | geklickt, wirkte | Space, wirkte | marks, nodes, seam.galaxy.fits |
| flow (no symbol) open flow | atlas-body > atlas-twin | geklickt, wirkte | Space, wirkte | marks, nodes, seam.layout.explainOpen |
| Who is reading | atlas-body > atlas-twin | geklickt, wirkte | ArrowRight, wirkte | seam.twin.level, seam.twin.levelName, states |
| Logic | atlas-twin > atlas-twin-facets | geklickt, wirkte | Space, wirkte | states |
| Calls | atlas-twin > atlas-twin-facets | geklickt, wirkte | Space, wirkte | states |
| Data | atlas-twin > atlas-twin-facets | geklickt, wirkte | Space, wirkte | states |
| Errors | atlas-twin > atlas-twin-facets | geklickt, wirkte | Space, wirkte | states |
| Tests | atlas-twin > atlas-twin-facets | geklickt, wirkte | Space, wirkte | states |
| Runtime | atlas-twin > atlas-twin-facets | geklickt, wirkte | Space, wirkte | states |
| Changes | atlas-twin > atlas-twin-facets | geklickt, wirkte | Space, wirkte | states |
| facts | atlas-body > atlas-twin | geklickt, wirkte | Space nach Umweg ueber einen Nachbarn, wirkte | seam.twin.view, states |
| pseudocode | atlas-body > atlas-twin | geklickt, wirkte | Space, wirkte | seam.twin.view, states |
| height of the twin against the graph | atlas-body | geklickt, wirkte | ArrowDown, wirkte | seam.galaxy.fits, seam.layout.isDefault, seam.layout.requested |
| galaxy | atlas-galaxy > atlas-graph-mode | geklickt, wirkte | Space, wirkte | marks, nodes, seam.galaxy.open |
| hierarchy | atlas-galaxy > atlas-graph-mode | geklickt, wirkte | Space, wirkte | text |
| show legend | atlas-body > atlas-galaxy | geklickt, wirkte | Space, wirkte | marks, nodes, seam.galaxy.fits |
| collapse galaxy | atlas-body > atlas-galaxy | geklickt, wirkte | Space, wirkte | marks, nodes, seam.galaxy.open |
| fit view | atlas-galaxy > atlas-galaxy-scene | geklickt, wirkte | Space, wirkte | seam.galaxy.fits |
| command line | atlas-command | geklickt, wirkte | KeyA, wirkte | marks, nodes, text |

### Zustand `file-open`

Eine Datei im Reader, ohne aufgeloestes Symbol

Gefunden: 55 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 3. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/file-open.png`

| Beschriftung | Ort | Maus | Tastatur | Wirkung |
| --- | --- | --- | --- | --- |
| [w]hy am I here | atlas-header > atlas-menu | geklickt, wirkte | Space, wirkte | marks, nodes, seam.search.activatedMenus |
| userService.ts | atlas-tabs-bar > atlas-tabs | geklickt, wirkte | Space, wirkte | nodes, marks, states |
| close userService.ts | atlas-tabs-bar > atlas-tabs | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |

### Zustand `symbol-focus`

Ein Symbol im Fokus: der Twin steht auf createUser

Gefunden: 79 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 24. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/symbol-focus.png`

| Beschriftung | Ort | Maus | Tastatur | Wirkung |
| --- | --- | --- | --- | --- |
| Evidence | atlas-twin > codeatlas-twin-section-purpose | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| Evidence | atlas-twin > codeatlas-twin-section-steps | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| 24 validateUser validate.ts:19 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| Evidence | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| 27 ValidationError new validate.ts:4 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| Evidence | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| 29 UserEntity new types.ts:37 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| Evidence | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| 29 listUsers userService.ts:18 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| Evidence | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| 30 insert db.ts:31 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| Evidence | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| 35 toUser userService.ts:9 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| Evidence | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| Evidence | atlas-twin > codeatlas-twin-section-callers | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| registerUserRoutes users.ts:15 | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| Evidence | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| create userService.ts:41 | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| Evidence | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| Evidence | atlas-twin > codeatlas-twin-section-state | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| DB_URL | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| Evidence | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| User | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| Evidence | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, states |

### Zustand `twin-pseudocode`

Der Twin auf dem Reiter pseudocode statt facts

Gefunden: 77 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 22. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/twin-pseudocode.png`

| Beschriftung | Ort | Maus | Tastatur | Wirkung |
| --- | --- | --- | --- | --- |
| uses insert from ../repo/db | atlas-pseudocode-imports > atlas-pseudocode-import | geklickt, wirkte | Space, wirkte | nodes, seam.reader.caretLine, text |
| uses query from ../repo/db | atlas-pseudocode-imports > atlas-pseudocode-import | geklickt, wirkte | Space, wirkte | nodes, seam.reader.caretLine, text |
| uses Row from ../repo/db | atlas-pseudocode-imports > atlas-pseudocode-import | geklickt, wirkte | Space, wirkte | nodes, seam.reader.caretLine, text |
| uses UserEntity from ../types | atlas-pseudocode-imports > atlas-pseudocode-import | geklickt, wirkte | Space, wirkte | nodes, seam.reader.caretLine, text |
| uses User from ../types | atlas-pseudocode-imports > atlas-pseudocode-import | geklickt, wirkte | Space, wirkte | nodes, seam.reader.caretLine, text |
| uses ValidationError from ../util/validate | atlas-pseudocode-imports > atlas-pseudocode-import | geklickt, wirkte | Space, wirkte | nodes, seam.reader.caretLine, text |
| uses validateUser from ../util/validate | atlas-pseudocode-imports > atlas-pseudocode-import | geklickt, wirkte | Space, wirkte | nodes, seam.reader.caretLine, text |
| 1. call validateUser | atlas-pseudocode > atlas-pseudocode-line | geklickt, wirkte | Space, wirkte | seam.reader.caretLine |
| validate.ts:19 | atlas-pseudocode > atlas-pseudocode-line | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| 2. construct ValidationError | atlas-pseudocode > atlas-pseudocode-line | geklickt, wirkte | Space, wirkte | seam.reader.caretLine |
| validate.ts:4 | atlas-pseudocode > atlas-pseudocode-line | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| 3. construct UserEntity | atlas-pseudocode > atlas-pseudocode-line | geklickt, wirkte | Space, wirkte | seam.reader.caretLine |
| types.ts:37 | atlas-pseudocode > atlas-pseudocode-line | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| 4. call listUsers | atlas-pseudocode > atlas-pseudocode-line | geklickt, wirkte | Space, wirkte | seam.reader.caretLine |
| userService.ts:18 | atlas-pseudocode > atlas-pseudocode-line | geklickt, wirkte | Space, wirkte | nodes, seam.reader.caretLine, text |
| 5. call insert | atlas-pseudocode > atlas-pseudocode-line | geklickt, wirkte | Space, wirkte | seam.reader.caretLine |
| db.ts:31 | atlas-pseudocode > atlas-pseudocode-line | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| 6. call toUser | atlas-pseudocode > atlas-pseudocode-line | geklickt, wirkte | Space, wirkte | seam.reader.caretLine |
| userService.ts:9 | atlas-pseudocode > atlas-pseudocode-line | geklickt, wirkte | Space, wirkte | nodes, seam.reader.caretLine, text |
| 7. may raise ValidationError | atlas-pseudocode > atlas-pseudocode-line | geklickt, wirkte | Space, wirkte | marks, nodes, seam.reader.caretLine |
| 8. read DB_URL from the environment | atlas-pseudocode > atlas-pseudocode-line | geklickt, wirkte | Space, wirkte | seam.reader.caretLine |
| Where this block comes from. | atlas-pseudocode > atlas-pseudocode-honest | geklickt, wirkte | Space, wirkte | states |

### Zustand `explain-flow`

Der Erklaeren-Bereich, Reiter flow

Gefunden: 87 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 18. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/explain-flow.png`

| Beschriftung | Ort | Maus | Tastatur | Wirkung |
| --- | --- | --- | --- | --- |
| 1. call validateUser | atlas-flow-step-list > atlas-flow-step | geklickt, wirkte | Space nach Umweg ueber einen Nachbarn, wirkte | nodes, seam.flow.step, seam.reader.caretLine |
| 2. construct ValidationError | atlas-flow-step-list > atlas-flow-step | geklickt, wirkte | Space, wirkte | nodes, seam.flow.step, seam.reader.caretLine |
| 3. construct UserEntity | atlas-flow-step-list > atlas-flow-step | geklickt, wirkte | Space, wirkte | nodes, seam.flow.step, seam.reader.caretLine |
| 4. call listUsers | atlas-flow-step-list > atlas-flow-step | geklickt, wirkte | Space, wirkte | nodes, seam.flow.step, seam.reader.caretLine |
| 5. call insert | atlas-flow-step-list > atlas-flow-step | geklickt, wirkte | Space, wirkte | nodes, seam.flow.step, seam.reader.caretLine |
| 6. call toUser | atlas-flow-step-list > atlas-flow-step | geklickt, wirkte | Space, wirkte | nodes, seam.flow.step, seam.reader.caretLine |
| 7. may raise ValidationError | atlas-flow-step-list > atlas-flow-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.flow.step |
| 8. read DB_URL from the environment | atlas-flow-step-list > atlas-flow-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.flow.step |
| 24 validateUser validate.ts:19 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 27 ValidationError new validate.ts:4 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 29 UserEntity new types.ts:37 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 29 listUsers userService.ts:18 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 30 insert db.ts:31 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 35 toUser userService.ts:9 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| registerUserRoutes users.ts:15 | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| create userService.ts:41 | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| DB_URL | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| User | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |

### Zustand `explain-walk-idle`

Der Erklaeren-Bereich, Reiter walk ohne laufende Fuehrung

Gefunden: 79 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 0. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/explain-walk-idle.png`

### Zustand `explain-chat-idle`

Der Erklaeren-Bereich, Reiter chat ohne gestellte Frage

Gefunden: 79 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 0. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/explain-chat-idle.png`

### Zustand `explain-bug`

Der Erklaeren-Bereich, Reiter bug

Gefunden: 88 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 15. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/explain-bug.png`

| Beschriftung | Ort | Maus | Tastatur | Wirkung |
| --- | --- | --- | --- | --- |
| close | atlas-explain-panel > atlas-bugwizard | geklickt, wirkte | Space, wirkte | marks, nodes, seam.layout.explainOpen |
| Change symbol | atlas-bugwizard > atlas-bugwizard-target | geklickt, wirkte | Space, wirkte | marks, nodes, text |
| src/server.ts | atlas-bugwizard-static-chain > atlas-bugwizard-hop | geklickt, wirkte | Space, wirkte | marks, nodes, text |
| main | atlas-bugwizard-static-chain > atlas-bugwizard-hop | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| createApp | atlas-bugwizard-static-chain > atlas-bugwizard-hop | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| registerUserRoutes | atlas-bugwizard-static-chain > atlas-bugwizard-hop | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| createUser | atlas-bugwizard-static-chain > atlas-bugwizard-hop | geklickt, wirkte |   mit gleichem Bug-Ziel, wirkte | seam.checklist.label, seam.checklist.marks, storage |
| create | atlas-bugwizard-static-chain > atlas-bugwizard-hop | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| createUser | atlas-bugwizard-static-chain > atlas-bugwizard-hop | geklickt, wirkte |   mit gleichem Bug-Ziel, wirkte | bug-hop-equivalent |
| 30 insert db.ts:31 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 35 toUser userService.ts:9 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| registerUserRoutes users.ts:15 | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| create userService.ts:41 | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| DB_URL | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| User | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |

### Zustand `explain-change`

Der Erklaeren-Bereich, Reiter change

Gefunden: 82 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 13. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/explain-change.png`

| Beschriftung | Ort | Maus | Tastatur | Wirkung |
| --- | --- | --- | --- | --- |
| close | atlas-explain-panel > atlas-impact | geklickt, wirkte | Space, wirkte | marks, nodes, seam.layout.explainOpen |
| Working tree | atlas-impact > atlas-impact-toolbar | geklickt, wirkte | Space nach Umweg ueber einen Nachbarn, wirkte | marks, nodes, states |
| Since ref | atlas-impact > atlas-impact-toolbar | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| 24 validateUser validate.ts:19 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 27 ValidationError new validate.ts:4 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 29 UserEntity new types.ts:37 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 29 listUsers userService.ts:18 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 30 insert db.ts:31 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 35 toUser userService.ts:9 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| registerUserRoutes users.ts:15 | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| create userService.ts:41 | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| DB_URL | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| User | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |

### Zustand `galaxy-legend`

Die Galaxie mit offener Legende

Gefunden: 91 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 12. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/galaxy-legend.png`

| Beschriftung | Ort | Maus | Tastatur | Wirkung |
| --- | --- | --- | --- | --- |
| DEFINES 52 | atlas-galaxy-legend > atlas-galaxy-legend-entry | geklickt, wirkte | Space, wirkte | seam.galaxy.drawnEdges, seam.galaxy.hiddenKinds, states |
| USAGE 37 | atlas-galaxy-legend > atlas-galaxy-legend-entry | geklickt, wirkte | Space, wirkte | seam.galaxy.drawnEdges, seam.galaxy.hiddenKinds, states |
| IMPORTS 27 | atlas-galaxy-legend > atlas-galaxy-legend-entry | geklickt, wirkte | Space, wirkte | seam.galaxy.drawnEdges, seam.galaxy.hiddenKinds, states |
| CALLS 25 | atlas-galaxy-legend > atlas-galaxy-legend-entry | geklickt, wirkte | Space, wirkte | seam.galaxy.drawnEdges, seam.galaxy.hiddenKinds, states |
| CONTAINS_FILE 11 | atlas-galaxy-legend > atlas-galaxy-legend-entry | geklickt, wirkte | Space, wirkte | seam.galaxy.drawnEdges, seam.galaxy.hiddenKinds, states |
| CONFIGURES 6 | atlas-galaxy-legend > atlas-galaxy-legend-entry | geklickt, wirkte | Space, wirkte | seam.galaxy.drawnEdges, seam.galaxy.hiddenKinds, states |
| CONTAINS_FOLDER 6 | atlas-galaxy-legend > atlas-galaxy-legend-entry | geklickt, wirkte | Space, wirkte | seam.galaxy.drawnEdges, seam.galaxy.hiddenKinds, states |
| DEFINES_METHOD 6 | atlas-galaxy-legend > atlas-galaxy-legend-entry | geklickt, wirkte | Space, wirkte | seam.galaxy.drawnEdges, seam.galaxy.hiddenKinds, states |
| IMPLEMENTS 4 | atlas-galaxy-legend > atlas-galaxy-legend-entry | geklickt, wirkte | Space, wirkte | seam.galaxy.drawnEdges, seam.galaxy.hiddenKinds, states |
| RAISES 3 | atlas-galaxy-legend > atlas-galaxy-legend-entry | geklickt, wirkte | Space, wirkte | seam.galaxy.drawnEdges, seam.galaxy.hiddenKinds, states |
| CALL_REFERENCE 1 | atlas-galaxy-legend > atlas-galaxy-legend-entry | geklickt, wirkte | Space, wirkte | seam.galaxy.drawnEdges, seam.galaxy.hiddenKinds, states |
| HAS_BRANCH 1 | atlas-galaxy-legend > atlas-galaxy-legend-entry | geklickt, wirkte | Space, wirkte | seam.galaxy.drawnEdges, seam.galaxy.hiddenKinds, states |

### Zustand `galaxy-collapsed`

Die Galaxie zugeklappt: der Kopf bleibt bedienbar

Gefunden: 77 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 0. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/galaxy-collapsed.png`

### Zustand `hierarchy`

Die Hierarchie statt der Galaxie

Gefunden: 82 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 1. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/hierarchy.png`

| Beschriftung | Ort | Maus | Tastatur | Wirkung |
| --- | --- | --- | --- | --- |
| collapse hierarchy | atlas-body > atlas-galaxy | geklickt, wirkte | Space, wirkte | marks, nodes, seam.galaxy.open |

### Zustand `search`

Das Suchfenster mit seiner Kandidatenliste

Gefunden: 86 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 7. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/search.png`

| Beschriftung | Ort | Maus | Tastatur | Wirkung |
| --- | --- | --- | --- | --- |
| createUser src/services/userService.ts L23 user | atlas-search-results > atlas-search-list | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| toUser src/services/userService.ts L9 user | atlas-search-results > atlas-search-list | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| registerUserRoutes src/routes/users.ts L7 user | atlas-search-results > atlas-search-list | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| UserEntity src/types.ts L37 user | atlas-search-results > atlas-search-list | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| validateUser src/util/validate.ts L19 user | atlas-search-results > atlas-search-list | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| User src/types.ts L27 user | atlas-search-results > atlas-search-list | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| UserInput src/util/validate.ts L14 user | atlas-search-results > atlas-search-list | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |

### Zustand `entry-dialog`

Der Einstiegsdialog: die Liste der angebotenen Einstiegspunkte

Gefunden: 98 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 29. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/entry-dialog.png`

| Beschriftung | Ort | Maus | Tastatur | Wirkung |
| --- | --- | --- | --- | --- |
| close | atlas-reader > atlas-entry | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| search for a symbol by meaning | atlas-reader > atlas-entry | geklickt, wirkte | KeyA, wirkte | focus, values |
| loadConfig src/config.ts:11 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| isProduction src/config.ts:20 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| query src/repo/db.ts:17 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| insert src/repo/db.ts:31 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| hotspotScan src/repo/db.ts:39 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| walk src/repo/db.ts:58 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| registerOrderRoutes src/routes/orders.ts:8 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| registerUserRoutes src/routes/users.ts:7 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| createApp src/server.ts:32 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| main src/server.ts:39 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| getOrder src/services/orderService.ts:8 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| create src/services/orderService.ts:30 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| listUsers src/services/userService.ts:18 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| createUser src/services/userService.ts:23 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.galaxy.fits |
| create src/services/userService.ts:40 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| validateUser src/util/validate.ts:19 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| validateId src/util/validate.ts:33 entry point | atlas-entry > atlas-entry-flagged | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 24 validateUser validate.ts:19 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 27 ValidationError new validate.ts:4 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 29 UserEntity new types.ts:37 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 29 listUsers userService.ts:18 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 30 insert db.ts:31 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 35 toUser userService.ts:9 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| registerUserRoutes users.ts:15 | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| create userService.ts:41 | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| DB_URL | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| User | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |

### Zustand `walk-running`

Eine laufende Fuehrung: die Schrittkarte im Erklaeren-Bereich

Gefunden: 83 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 10. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/walk-running.png`

| Beschriftung | Ort | Maus | Tastatur | Wirkung |
| --- | --- | --- | --- | --- |
| [<-]prev | atlas-explain-panel > atlas-tour | geklickt, ohne Wirkung | gesperrt, nimmt keinen Fokus, ohne Wirkung | - |
| | | | | Grund der Flaeche: this is the first step, so there is no previous step to return to |
| [Enter]next | atlas-explain-panel > atlas-tour | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| [d]diagram | atlas-explain-panel > atlas-tour | geklickt, wirkte | Space, wirkte | marks, nodes, seam.flow.step |
| [q]exit | atlas-explain-panel > atlas-tour | geklickt, wirkte | Space, wirkte | marks, nodes, seam.galaxy.fits |
| 30 insert db.ts:31 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 35 toUser userService.ts:9 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| registerUserRoutes users.ts:15 | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| create userService.ts:41 | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| DB_URL | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| User | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |

### Zustand `help`

Die Hilfeseite

Gefunden: 80 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 4. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/help.png`

| Beschriftung | Ort | Maus | Tastatur | Wirkung |
| --- | --- | --- | --- | --- |
| close | atlas-reader > atlas-help | geklickt, wirkte | Space, wirkte | marks, nodes, states |
| 24 validateUser validate.ts:19 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| registerUserRoutes users.ts:15 | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| DB_URL | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |

### Zustand `settings`

Die Einstellungen mit den Effektschaltern

Gefunden: 82 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 13. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/settings.png`

| Beschriftung | Ort | Maus | Tastatur | Wirkung |
| --- | --- | --- | --- | --- |
| close | atlas-reader > atlas-settings | geklickt, wirkte | Space, wirkte | marks, nodes, seam.settings.open |
| copy | atlas-settings-suggestions > atlas-settings-suggestion | geklickt, wirkte | Space, wirkte | states |
| copy | atlas-settings-suggestions > atlas-settings-suggestion | geklickt, wirkte | Space, wirkte | states |
| 24 validateUser validate.ts:19 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 27 ValidationError new validate.ts:4 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 29 UserEntity new types.ts:37 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 29 listUsers userService.ts:18 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 30 insert db.ts:31 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| 35 toUser userService.ts:9 | codeatlas-twin-section-steps > codeatlas-twin-step | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| registerUserRoutes users.ts:15 | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| create userService.ts:41 | codeatlas-twin-section-callers > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| DB_URL | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |
| User | codeatlas-twin-section-state > codeatlas-twin-row | geklickt, wirkte | Space, wirkte | marks, nodes, seam.checklist.label |

### Zustand `llm-on`

Das lokale Modell an: die Karte und ihre Schalter

Gefunden: 79 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 0. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/llm-on.png`

### Zustand `llm-off`

Das lokale Modell aus: die Karte sagt es und bietet nichts an

Gefunden: 79 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 0. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/llm-off.png`

### Zustand `agents-live`

Der Live-Modus der Agenten, mit eigenem Koerper des Lesers

Gefunden: 91 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 12. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/agents-live.png`

| Beschriftung | Ort | Maus | Tastatur | Wirkung |
| --- | --- | --- | --- | --- |
| expand | atlas-agents > atlas-agents-head | geklickt, wirkte | Space, wirkte | marks, nodes, seam.agents.size |
| fold | atlas-agents > atlas-agents-head | geklickt, wirkte | Space, wirkte | marks, nodes, seam.agents.size |
| you | atlas-agents-body > atlas-agents-filter | geklickt, wirkte | Space, wirkte | marks, nodes, seam.agents.drawn |
| agents | atlas-agents-body > atlas-agents-filter | geklickt, wirkte | Space, wirkte | marks, nodes, seam.agents.filter |
| both | atlas-agents-body > atlas-agents-filter | geklickt, wirkte | Space, wirkte | marks, nodes, seam.agents.drawn |
| follow | atlas-agents-body > atlas-agents-switches | geklickt, wirkte | Space, wirkte | seam.agents.follow, states, storage |
| trails | atlas-agents-body > atlas-agents-switches | geklickt, wirkte | Space, wirkte | marks, nodes, seam.agents.trails |
| fullscreen | atlas-agents-body > atlas-agents-switches | geklickt, wirkte | Space, wirkte | marks, nodes, seam.agents.fullscreen |
| 1m | atlas-agents-body > atlas-agents-switches | geklickt, wirkte | Space, wirkte | seam.agents.trailWindowMs, states, storage |
| 5m | atlas-agents-body > atlas-agents-switches | geklickt, wirkte | Space, wirkte | seam.agents.trailWindowMs, states, storage |
| 15m | atlas-agents-body > atlas-agents-switches | geklickt, wirkte | Space, wirkte | seam.agents.trailWindowMs, states, storage |
| all kept | atlas-agents-body > atlas-agents-switches | geklickt, wirkte | Space, wirkte | seam.agents.trailWindowMs, states, storage |

### Zustand `agents-fullscreen`

Der Live-Modus im Vollbild: der Zeitstrahl und seine Schalter

Gefunden: 23 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 6. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/agents-fullscreen.png`

| Beschriftung | Ort | Maus | Tastatur | Wirkung |
| --- | --- | --- | --- | --- |
| pause | atlas-galaxy-bottom > atlas-agents-timeline | geklickt, wirkte | Space, wirkte | marks, nodes, seam.agentTimeline.mode |
| 1m | atlas-galaxy-bottom > atlas-agents-timeline | geklickt, wirkte | Space, wirkte | marks, nodes, seam.agents.trailWindowMs |
| 5m | atlas-galaxy-bottom > atlas-agents-timeline | geklickt, wirkte | Space, wirkte | marks, nodes, seam.agents.trailWindowMs |
| 15m | atlas-galaxy-bottom > atlas-agents-timeline | geklickt, wirkte | Space, wirkte | marks, nodes, seam.agents.trailWindowMs |
| all kept | atlas-galaxy-bottom > atlas-agents-timeline | geklickt, wirkte | Space, wirkte | marks, nodes, seam.agents.trailWindowMs |
| timeline | atlas-galaxy-bottom > atlas-agents-timeline | geklickt, wirkte | ArrowRight, wirkte | marks, nodes, seam.agentTimeline.mode |

### Zustand `settings-flat`

Die Einstellungen bei flacher Projektion: das Bild steht still und laesst sich zaehlen

Gefunden: 82 Bedienelemente, davon zum ersten Mal in
diesem Zustand angefasst: 0. Der Rest stand schon
in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden
Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.

Bild: `verification/w12/states/settings-flat.png`

## Wonach gesucht wurde

Die Liste der Bedienelemente steht nicht im Lauf. Gesammelt wird ueber die Gattung:

```
button, [role="button"], a[href], input, select, [tabindex]:not([tabindex="-1"]), [role="separator"], [role="tab"]
```

Zwei Flaechen bleiben ausgenommen, jede mit ihrem Grund:

- `.monaco-editor`: Der Editor baut sein Innenleben selbst (versteckte Eingabeflaeche, eigene Tastaturbindungen, eigene Ebenen). Gesammelt werden seine Nachbarn; die Tab-Wanderung geht trotzdem durch ihn hindurch, damit eine Falle darin auffiele.
- `[data-testid="atlas-hint"]`: Der Tooltip ist keine Bedienung, sondern die Auskunft ueber eine. Er oeffnet beim Beruehren und beim Fokussieren und waere in jedem Zustand ein anderes Element.

# Herkunft der Closure-Fixture

Kopiert am 2026-08-29 aus CodeAtlasIDE,
`/Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/test/fixtures/closure-userService-create.json`.
Gleicher Urheber (Bernhard Jackiewicz). Die Datei ist **byte-identisch**
uebernommen und wird hier nicht gepflegt.

| Datei | Wofuer |
| --- | --- |
| `closure-userService-create.json` | Der Walk, an dem der Pseudocode-Builder (Closure-Scope, Walk-Ordnung, Kappungs-Satz) und das Flow-Modell (Pre-Order-Faltung, Zyklus, Pfeile) haengen. |

Der Walk ist im Referenzprojekt von Hand aus denselben IR-Aufzeichnungen
gebaut, die unter `src/twin/__fixtures__/` liegen: eine Antwort des Backends
gibt es als Aufzeichnung nicht. Jedes Symbol, jede Kante und jede Zeilennummer
darin stammt aus den IRs daneben, und die Grenzen (`depth 3`, `cap 8`,
`truncated`) sind so gesetzt, dass der gekappte Fall geprueft wird.

Die IR-Fixtures selbst liegen weiterhin unter `src/twin/__fixtures__/` und
werden von hier aus gelesen: eine zweite Kopie waere genau die stille
Abweichung, die `src/twin/__fixtures__/HERKUNFT.md` beschreibt.

Formhinweis: die Datei traegt die Felder des `ClosureDto` des Referenzprojekts
(`root`, `symbols`, `edges`, `truncated`, `visited`, `depth`, `cap`). Der Walk
dieses Projekts (`src/provider/closure.ts`) antwortet mit `nodes` statt
`symbols`; `closureDocumentOf` in `pseudocode-builder.ts` faltet das eine auf
das andere, und die Tests lesen die Fixture in der Form, in der sie
aufgezeichnet wurde.

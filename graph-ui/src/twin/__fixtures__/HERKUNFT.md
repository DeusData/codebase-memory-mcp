# Herkunft der IR-Fixtures

Kopiert am 2026-08-28 aus CodeAtlasIDE,
`/Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/test/fixtures/`.
Gleicher Urheber (Bernhard Jackiewicz). Die Dateien sind **byte-identisch**
uebernommen und werden hier nicht gepflegt: sie sind Aufzeichnungen, die
`tools/record-ir-fixtures.mjs` des Referenzprojekts an der 0.9.0-Engine gemacht
hat, und eine handgepflegte Zweitfassung waere genau die stille Abweichung, die
die portierten Render-Regeln unbemerkt anders aussehen liesse.

| Datei | Wofuer |
| --- | --- |
| `create-user-ir.json` | Die eine IR, an der die Verhaltenstests des Render-Modells haengen: jede Faktenfamilie ist gefuellt, also hat jeder Zweig des Absatzbauers etwas zu tun. |
| `ir-*.json` (10 Stueck) | Das Netz ueber die Formen, die eine einzelne Fixture nicht hat: ein Blatt, das nichts ruft; ein Symbol, das niemand ruft; eins ohne Fehlerpfad; ein Knoten, den drei Dienste benutzen. |

Beides sind Aufzeichnungen ueber `fixtures/atlas-sample` des Referenzprojekts,
nicht ueber `fixtures/atlas-sample` dieses Projekts. Die beiden Sample-Baeume
sind derselbe Code; die qualifizierten Namen tragen aber das Projekt-Praefix der
Aufzeichnung (`codeatlas-atlas-sample-<hash>`), und die Datei-URIs sind
`file:///workspace/...`. Kein Pfad nennt eine Maschine; die Depth-Tests pruefen
das noch einmal selbst.

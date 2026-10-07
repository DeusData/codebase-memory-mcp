# Herkunft dieses Korpus

Generiert am 2026-08-28 fuer CodeAtlasWeb. Nichts daran ist kopiert:
`src/big.ts` ist deterministisch erzeugt (100 kleine exportierte Funktionen
plus ein `runAll`, das sie der Reihe nach faehrt), `src/support.ts` und
`src/index.ts` sind von Hand geschrieben und klein gehalten.

Zweck: **Cap-Messung**. `get_code_snippet` kappt jeden Schnipsel bei 500
Zeilen (`MCP_SNIPPET_MAX_LINES`, cbm/src/mcp/mcp.c) und meldet das mit
`source_clipped`. `src/big.ts` hat mit 717 Zeilen sicher mehr, damit der
Reader beweisen muss, dass er den fehlenden Rest ehrlich benennt statt eine
gekappte Datei als ganze Datei auszugeben.

Warum ein eigener Korpus und nicht `fixtures/atlas-sample`: an dessen Zahlen
haengt der W1b-Smoke (73 Knoten, 175 Kanten, 6 Callees, 3 Caller). Eine Datei
mehr dort wuerde diese Zahlen verschieben und einen bestandenen Beweislauf
umschreiben. Dieser Korpus wird nur fuer die Cap-Messung indiziert.

Nicht bauen, nicht installieren, nicht ausfuehren. Der Korpus ist
Messmaterial.

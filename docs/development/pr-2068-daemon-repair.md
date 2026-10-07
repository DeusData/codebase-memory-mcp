# Daemon- und Agent-Setup-Reparaturen nach dem Bedienvergleich

Der echte Vorher-/Nachher-Vergleich fand einen Bedienfehler: System → Logs filterte
nur die neuesten 200 Zeilen im Browser. 49 vorhandene SQLite-Fehler wurden dadurch
als 0 sichtbare Fehler dargestellt. System verwendet jetzt projektweiten,
daemonweiten oder unbekannten Scope und Schweregrad-Schwellen serverseitig vor
LIMIT; eine begrenzte Textsuche durchsucht ebenfalls den gespeicherten Bestand.
Ein Scopewechsel zeigt keine alten Scope-Daten, auch nicht vor Reacts Effekten.

Alle angenommenen Frontend-Loglevel landen jetzt im gemeinsamen SQLite-Journal.
Schema 4 ergänzt die Herkunft `frontend` und einen auslaufende Ereignisse
überdauernden Sequenzstand. `/api/ui-log` liest seine bisherigen JSON-Zeilen aus
dieser Quelle; `path` nennt jetzt `activity.db`, `export_path` den weitergeführten
lokalen JSONL-Kompatibilitätsexport. Der Export ist keine zweite Statusquelle.
Session/Sequenz-Wiederholungen erzeugen weder doppelte Journalzeilen noch doppelte
Exportzeilen, auch nach Payload-Retention. Ein fehlgeschlagener Journal-Write
antwortet mit HTTP 503. Einmalig werden höchstens die letzten 256 KiB von je
`ui.log.1` und `ui.log` importiert; Empfangszeiten bleiben alt und unbekannte
Projektzuordnung bleibt unbekannt. Teilweise Importe sind explizit gekennzeichnet.

Der Benutzer erhält unter Agents → **Connect your agent** den tatsächlichen
Python-Hook und einen anhand des vorhandenen `/api/repo-info` erzeugten lokalen
Installbefehl. Der Installer verändert nur die explizit gewählte Projektkonfiguration,
erhält fremde Einstellungen und verweigert widersprüchliche Hooks oder Symlinks.
Er startet keinen Client, ruft kein LLM auf und richtet kein Konto ein. Die Oberfläche
unterscheidet laufendes History-Polling vom Zeitpunkt der letzten aufgezeichneten
Werkzeugaktivität. Frühere Prüfeinträge werden nicht als gegenwärtige Arbeit bezeichnet.
Unterstützt ist hier Claude Code auf macOS/Linux mit Python 3; andere Clients benötigen
einen eigenen Adapter. Eine produktive neue LLM-Sitzung wurde nicht ausgeführt.

Ein zweiter echter Browserfehler war eine abgebrochene 11.070.807-Byte-Mapantwort
(`ERR_CONTENT_LENGTH_MISMATCH`). Nur Repository-Map-Antworten oberhalb 1 MiB
bekommen jetzt maximal fünf Sekunden Sendezeit. Normale Antworten behalten eine
Sekunde und das sofortige Stop-Interrupt-Verhalten. Die JSON-Map wird direkt
übertragen, ohne zusätzliche Formatierungskopie. Kompression und ein kleineres
semantisches Antwortformat bleiben mögliche spätere Optimierungen.

Die Repository-Map gibt zusätzlich gespeicherte Resolvermetadaten `strategy` und
`confidence` unverändert weiter. Fehlende oder falsch typisierte Werte werden
nicht ergänzt; sie sind keine Schadenswahrscheinlichkeit.

## Tatsächliche Prüfungen

- `build/c/test-runner httpd`: **83 bestanden, 0 fehlgeschlagen, 1 bestehender
  Windows-Skip**, mit ASan/UBSan. Abgedeckt sind vollständige große Antworten
  trotz vorübergehend blockiertem Leser, unveränderte Defaultfrist/Interrupt,
  Schema-/Legacy-Import, volle JSON-Evidenz, Retention/alte Retries,
  Scope/Suchfilter vor LIMIT und Neustarts.
  [Ungekürztes Laufprotokoll](../../graph-ui/verification/pr-2068/native-log-transport-repair.log.gz)
- `build/c/test-runner ui`: **40 bestanden**, einschließlich vorhandener Impact-
  und erweiterter Map-Evidenztests für `unique_name`, `confidence: 0.75`, rohe
  Zeilenmetadaten sowie fehlende/falsch typisierte Angaben.
  [Laufprotokoll](../../graph-ui/verification/pr-2068/native-map-resolution-repair.log.gz)
- System/Logadapter/kompakte Alerts: **41 Tests bestanden**, anschließend TypeScript
  ohne Fehler. Die erste Native-Prüfung fand vertauschte `duplicates`/`dropped`
  Antwortwerte; diese wurden korrigiert. Der Zwischenlauf mit dem irrtümlichen
  Flag `--suites` wird nicht als erfolgreicher Gesamtlauf gezählt.
- Der echte Indexer verarbeitete erneut das ausdrücklich markierte lokale Fixture.
  Nicht lesbares `unreadable.c` erzeugte neue fehlgeschlagene Jobs und sichtbare
  ERROR-Ereignisse **#8986 in System** sowie **#9018 in Galaxy**, jeweils ohne
  Neuladen. System zeigte **57/57** daemonweite Fehler, **11/11** projektbezogene
  Fehler und **3/3** Suchtreffer für `semantic_manifest.err`. Die Werte wurden
  am aufgezeichneten Ereignis-Cursor direkt in SQLite bestätigt.
  [Browser-/HTTP-/SQLite-Nachweis](../../graph-ui/verification/pr-2068/repair-errors-e2e.json)
- Lokale Diagnose, echte Parserzeile und ausschließlich bearbeiteter lokaler
  Download erneut geprüft. Keine externe Browseranfrage, kein Browserfehler;
  Dateirechte des Fixtures anschließend wiederhergestellt.

Die neuen Screenshots sind echte Aufnahmen des markierten Fehlerfixtures:
[gefilterte System-Historie](../../graph-ui/verification/pr-2068/repair-errors-system-filtered-history.png),
[Systemfehler](../../graph-ui/verification/pr-2068/repair-errors-after-system.png),
[Galaxyfehler](../../graph-ui/verification/pr-2068/repair-errors-after-galaxy.png),
[Diagnose](../../graph-ui/verification/pr-2068/repair-errors-local-diagnosis.png),
[lokaler Bericht](../../graph-ui/verification/pr-2068/repair-errors-local-report.png).

Die Hook-Konfiguration folgt der verifizierten
[offiziellen Claude-Hook-Schnittstelle](https://code.claude.com/docs/en/hooks).
Die Definition von PostToolUse wird benutzt, ohne aus aufgezeichneten Werkzeugaufrufen
Absichten, Ergebnisse oder eine aktive Agentensitzung abzuleiten.

Die abschließende Browserprüfung lief am 9. September 2026 um 21:45 UTC gegen
den neu gestarteten Daemon und den erneut indexierten Arbeitsbaum. Ein frischer
Browser öffnete das sichtbare Agenten-Setup; der tatsächlich heruntergeladene Hook
stimmte byteweise mit dem geprüften Source überein. Keine Clientkonfiguration
wurde verändert, kein Client oder LLM gestartet und kein Agentenereignis erzeugt.
Das frühere lokale TEST-Hookereignis **ID 8** war nach dem echten Daemonneustart
unverändert genau einmal in Schema 4 und in der HTTP-Abfrage vorhanden.

Der Browser war vor diesem Neustart noch nicht verbunden; der erste Versuch fiel
in die Unterbrechung und ist gesondert dokumentiert. Deshalb wurde die
Browserwiederverbindung anschließend durch bewusstes Offline/Online geprüft:
ohne Reload, identische Generation, Cursor und vier ausdrücklich markierte alte
Prüfereignisse, keine doppelten Zeilen. Die Verbindung erholte sich in diesem
einzelnen Lauf nach 2.855 ms. Das ist ein Ablaufnachweis, kein Benchmark.

Im selben Lauf entstand der lokale Diagnosebericht erst nach **Run local diagnosis**.
Der bearbeitete Download enthielt ausschließlich den absichtlich redigierten
Text. Die Prüfung erfasste keine externe oder Bridge-Anfrage und keinen
JavaScript-Fehler; vier bewusst provozierte Offline-Netzwerkfehler sind erhalten.
[Vollständiger Nachweis](../../graph-ui/verification/pr-2068/final-agent-diagnosis-restart.json),
[Setup und echter Download](../../graph-ui/verification/pr-2068/final-agents-setup-download.png),
[wiederverbundener Browser](../../graph-ui/verification/pr-2068/final-agents-connection-recovered.png),
[Diagnose vor Freigabe](../../graph-ui/verification/pr-2068/final-coverage-before-diagnosis-action.png),
[lokaler Bericht nach Aktion](../../graph-ui/verification/pr-2068/final-coverage-local-report-after-action.png).

# PR 2068: Korrekturen nach Prüfung der Fehleranzeige

Der spätere [Produkt-Audit](pr-2068-product-audit.md) ergänzt diesen Bericht:
Die Projekt-/Historienkorrektur ist implementiert, aber der bestehende
System-Logs-Filter und weitere Nutzerabläufe bleiben unvollständig.

Stand: 9. September 2026, Browsernachweise bis 20:16:25 UTC, abschließender
vollständiger Unit-Lauf um 20:17:59 UTC. Dieser Nachtrag
ergänzt den [ursprünglichen Implementierungsbericht](pr-2068-delivery.md).
Alle Prüfungen verwenden den lokalen Daemon auf `127.0.0.1:9749`.

## Implementiert

**Projektzuordnung und Historie.** `activity.db` migriert additiv auf
`user_version=3`: `daemon_logs.project` und ein Projekt-/Schweregrad-/ID-Index
ermöglichen Filterung vor der Mengenbegrenzung. Bestehende Einträge behalten
ihre IDs und bleiben ohne nachträglich erratene Projektzuordnung erhalten.
`GET /api/logs?project=…` liefert ausschließlich diesem Projekt zugeordnete
Zeilen; der Daemon-Verlauf und `scope=unattributed` bleiben ausdrücklich
abrufbar. Die vorhandene begrenzte Aufbewahrung bleibt wirksam.

Worker und Indexierungsabschluss liefern die Projektzuordnung an der Quelle.
Das Frontend erfasst sie pro Logeintrag vor Pufferung und Wiederholungen;
RPC-/API-Fehler behalten die Zuordnung ihrer Anfrage. Eine explizite Zuordnung
im Eintrag hat Vorrang vor dem Batch. Ein explizit leerer Wert bleibt unbekannt
und wird nicht dem gerade offenen Projekt zugewiesen. Die Anwendung meldet
auch ein automatisch ausgewähltes Projekt an den Logproduzenten, wenn kein
Projektparameter in der URL steht.

Die gemeinsame Ereignisleiste startet eingeklappt mit dem aktuellen Projekt.
Historische Fehler werden nicht als neu aufgetretene Frontendfehler angezeigt.
`History` öffnet die vorhandenen Einträge; `All daemon history` wechselt zum
aufgeklappten Verlauf aller Projekte mit sichtbarer Herkunft beziehungsweise
fehlender Zuordnung. `This project only` kehrt zur eingeklappten Projektsicht
zurück. Ein neu empfangener Projektfehler erscheint weiterhin unmittelbar rot,
mit ERROR, Zeitpunkt, Quelle und Ereignis-ID. `Acknowledge new events` entfernt
den Hinweis auf neue Ereignisse, löscht aber weder Historie noch Fehlerursache.
Beim Wechsel der Datenquelle werden alte Ergebnisse nicht als Daten des neuen
Projekts übernommen; fehlgeschlagene Aktualisierungen behalten nur die eigene
letzte Abfrage.

**Vollständigkeit der Diagnose.** Die gemeinsame Coverage-Quelle verarbeitet
die tatsächliche Pagination von `check_index_coverage`, einschließlich der
Fortsetzung im Antwort-Envelope. Ein vollständig gelesener Wurzelbereich ersetzt
die abgekürzten Listen von `index_status`; Duplikate, fehlende Zeilen,
Generationswechsel und erreichte Grenzen bleiben ausdrücklich erkennbar.
Für `cbm-pr2068` sind **281 erfasste Einträge geladen: 85 teilweise geparste
Pfade und 196 bewusste Ausschlüsse**. Die unzutreffende Warnung vor einer
unvollständigen Liste entfällt. Parserlücken und Ausschlüsse bleiben sichtbar;
eine vollständige Liste bescheinigt keine vollständige Repository-Indexierung
und ein partieller Parse keinen Programmfehler. Die
[separate Untersuchung](pr-2068-coverage-investigation.md) enthält konkrete
Quellbeispiele und die erfolgreiche echte Drei-Seiten-Abfrage.

Es wurden keine Fehlerlogs gelöscht, Diagnoseinhalte übertragen, Issues erstellt
oder zusätzlichen Listener für diese Funktionen eingeführt. Ein frischer
Browserlauf mit Workspace-Wechseln und zwei Reloads reproduzierte keinen
Frontendabsturz und keinen fehlgeschlagenen Request. Vorhandene
`Failed to fetch`-Einträge werden weiterhin als tatsächliche Anfragefehler
gespeichert; aus der Nachricht allein lässt sich ihre Ursache nicht sicher
ableiten. Es wurde keine pauschale Unterdrückung von Verbindungsfehlern ergänzt.

## Tatsächlich ausgeführte Prüfungen

| Prüfung | Ergebnis und Nachweis |
| --- | --- |
| Abschließender vollständiger Frontend-Unit-Lauf | **2.388 bestanden, 166 Dateien**, einschließlich des letzten Race-Regressionsfalls. [Finales Protokoll](../../graph-ui/verification/pr-2068/feedback-unit-final-tests.log.gz). Der vorherige vollständige Lauf mit [2.387 Fällen](../../graph-ui/verification/pr-2068/feedback-unit-tests.log.gz) bleibt separat erhalten. |
| Zusätzlicher gezielter Ereignisleisten-Lauf | **8 bestanden**, einschließlich verspäteter A-Antwort nach Wechsel zu B und anschließend fehlgeschlagener B-Aktualisierung. [Protokoll](../../graph-ui/verification/pr-2068/feedback-alert-final-tests.log.gz) |
| Native Scope-/Worker-Regressionen | **139 bestanden, 1 vorhandener Windows-Plattform-Skip**, ASan/UBSan; Suites `httpd`, `daemon_application`, `index_supervisor`. [Protokoll](../../graph-ui/verification/pr-2068/feedback-native-scope-tests.log.gz) |
| Frontend-Vertragstests | **203 bestanden, 0 fehlgeschlagen**. [Protokoll](../../graph-ui/verification/pr-2068/feedback-acceptance-tests.log.gz) |
| Builds und ergänzende Gates | TypeScript-Prüfung, Style-/Promise-Gates, [Frontend-Build](../../graph-ui/verification/pr-2068/feedback-ui-build.log.gz) und [eingebetteter nativer Build](../../graph-ui/verification/pr-2068/feedback-embed-build.log.gz) erfolgreich. Die bestehende Vite-Warnung zum großen Hauptbundle bleibt. |
| Zwei echte Projektfenster | Neuer ausdrücklich synthetischer Fehler **#4125** nur im Fixture-Projekt; Gegenprobe mit widersprechendem Batch-Projekt und explizit unbekannter Zuordnung. Quittierung behält denselben Datensatz, Reload macht ihn nicht erneut zum neuen Alarm. Alter unattributierter Fixture-Fehler **#747** bleibt erhalten. Null Page-Errors, Console-Errors, fehlgeschlagene oder externe Requests. [Browserbericht](../../graph-ui/verification/pr-2068/feedback-e2e.json) |
| Tatsächlicher Indexierungsfehler | Markiertes Fixture-Projekt, `unreadable.c` mit `chmod 000`, echtes `EACCES`, jeweils neuer fehlgeschlagener Indexierungsauftrag. **#4391** erschien live in Systems, **#4412** in Galaxy, beide korrekt `project=pr2068-error-fixture`. Keine Seitenneuladung. [Browser-, HTTP- und SQLite-Nachweis](../../graph-ui/verification/pr-2068/errors-e2e.json) |
| Bewusste lokale Diagnose | Öffnen erzeugt keinen Bericht. Erst `Run local diagnosis` erstellt den bearbeitbaren lokalen Text; echte Parserzeile `2-2` bleibt erhalten, Download enthält exakt den zuvor redigierten Text. Keine externen Requests. Fixture-Dateirechte anschließend nachweislich wieder `0600`. |

Der erste Feedback-Testlauf enthielt einen zu breiten Vergleich seines eigenen
Sessionmarkers mit vollständigen Log-JSONs: Eine korrekt zugeordnete normale
Warnung enthielt denselben Marker in ihrer Audit-URL. Der Test vergleicht jetzt
die vollständigen synthetischen Ereignismeldungen. Der erste Lauf ist mit dieser
Harness-Korrektur [separat dokumentiert](../../graph-ui/verification/pr-2068/feedback-e2e-first-run.json).
Die Produktzuordnung war dabei bereits korrekt.

Die ursprünglichen Fehler-Testberichte und Screenshots wurden vor dem erneuten
Lauf unverändert gesichert und unter
`graph-ui/verification/pr-2068/before-feedback-errors/` aufbewahrt. Die
folgenden Links zeigen den erneut geprüften Stand. Die Ereignisse aus beiden
Regressionen bleiben bewusst als gekennzeichnete Fixture-Historie erhalten.

## Echte Screenshots des geprüften Stands

- [Projektsicht ohne historische Fremdalarme](../../graph-ui/verification/pr-2068/feedback-after-project-events.png)
- [281 erfasste Coverage-Einträge vollständig geladen](../../graph-ui/verification/pr-2068/feedback-after-coverage.png)
- [Daemon-Historie mit alten, nicht zugeordneten Ereignissen](../../graph-ui/verification/pr-2068/feedback-after-daemon-history.png)
- [Neuer Fehler im Fixture-Projekt](../../graph-ui/verification/pr-2068/feedback-after-fixture-live-error.png)
- [Anderes Projekt bleibt ohne diesen Alarm](../../graph-ui/verification/pr-2068/feedback-after-other-project-unaffected.png)
- [Quittiertes Ereignis bleibt in der Historie](../../graph-ui/verification/pr-2068/feedback-after-acknowledged-history.png)
- [Echter Indexierungsfehler live in Systems](../../graph-ui/verification/pr-2068/errors-after-system.png)
- [Echter Indexierungsfehler live in Galaxy](../../graph-ui/verification/pr-2068/errors-after-galaxy.png)

Reproduzierbar mit `node tools/pr-2068-feedback-e2e.mjs` und anschließend
`node tools/pr-2068-errors-e2e.mjs` aus `graph-ui`. Beide erwarten den bestehenden
isolierten PR-Daemon; sie starten keinen Server. Der zweite Lauf verändert nur
das eindeutig markierte Testverzeichnis und stellt Dateirechte im `finally`
wieder her. Historien sind begrenzt, die realen Coverage-Zahlen beziehen sich
auf den dokumentierten Indexstand; spätere Reindexierungen oder abgelaufene
Fixture-Einträge erfordern entsprechend aktualisierte Testvorbedingungen.

## Vergleich mit dem ursprünglichen Frontend

Auf Nutzerwunsch läuft das unveränderte Frontend aus Commit
`80f4f41f9bcb9daf60afc5f607bcbcba24a4b196` separat auf
<http://127.0.0.1:9751/?project=cbm-pr2068>. Die aktuelle eingebettete Anwendung
bleibt auf <http://127.0.0.1:9749/?project=cbm-pr2068>. Die Vergleichsvorschau
verwendet einen isolierten Git-Export und denselben aktuellen Daemon/Index.
Sie vergleicht also die Frontends anhand derselben Daten; sie ist keine
Wiederherstellung des früheren Backend- oder Indexzustands. Der zusätzliche
Loopback-Port dient ausschließlich der ausdrücklich gewünschten Vergleichsansicht.

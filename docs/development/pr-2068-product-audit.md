# PR 2068: tatsächlicher Produktstand nach direktem Browservergleich

Stand: 9. September 2026. Dieser Audit korrigiert die Abnahmebewertung des
früheren Lieferberichts. Technische Bausteine sind implementiert, die verlangte
verständliche und handlungsorientierte Repository-Karte ist noch nicht fertig.
Erfolgreiche Unit-Tests und erreichbare Komponenten beweisen diese Produktqualität
nicht. In diesem Audit wurden keine Produktdateien geändert.

## Verfahren und Vergleichsgrenze

Mit Playwright wurden frische Browserkontexte auf 9751 und 9749 geöffnet,
der normale Willkommensdialog bedient und danach die sichtbaren UI-Einstiege
angeklickt. Keine Workspace-Vorauswahl über LocalStorage und keine gemockten
Graphantworten. Der Root-Lauf verwendet 1440 × 1000 Pixel und dokumentiert auch
notwendiges Scrollen. Die beiden Seiten liefern unterschiedliche Frontendbuilds.

- **Vorher, 9751:** Originalfrontend aus `80f4f41f9bcb9daf60afc5f607bcbcba24a4b196`.
- **Aktuell, 9749:** eingebetteter Build mit unseren Änderungen.
- Beide lesen denselben aktuellen Daemon und Repository-Index. Das ist ein
  Frontendvergleich, kein erneuter Betrieb der alten Backendarchitektur.
- Die vorhandenen vier Agentereignisse sind ausdrücklich gekennzeichnete
  frühere Prüfdaten. Der Audit hat keine neuen Ereignisfixtures erzeugt.

Geprüfte Wege: alle fünf Architecture-Tabs, Bereich → Beziehung → Quellzeile,
README-Einstieg, Galaxy, Explore-Auswahl, alter und neuer Impact-Einstieg,
Tools-Kontextauswahl, Agents sowie System/Logs/ERROR.

[Architecture-Ablauf und Geometrie](../../graph-ui/verification/pr-2068/audit-architecture.json)
sowie [Auswahl-/Impact-Vergleich](../../graph-ui/verification/pr-2068/audit-impact.json)
und [Agents-/System-Ablauf samt SQLite-Abgleich](../../graph-ui/verification/pr-2068/audit-daemon-views.json)
enthalten die tatsächlichen Klicks, Antworten und Aufnahmen. Neustart und
Fehlerinjektion wurden für diesen Audit nicht wiederholt; deren früher ausgeführte
Nachweise bleiben im Feedback-Bericht getrennt dokumentiert.

## Was gebaut wurde und was davon noch nicht reicht

| Anforderung | Tatsächlich neu vorhanden | Abnahmegrenze |
| --- | --- | --- |
| Branch und Bestand verstehen | PR-Stand, Daemon-/Frontend-Datenwege, Build-/Testkommandos und reale Ausgangsaufnahmen wurden untersucht. Keine fremden Branchänderungen oder Pushes. | Keine offene Implementierung in diesem Vorbereitungspunkt; keine neue Leistung durch bereits vorher vorhandene Ansichten behaupten. |
| Repository-Karte | Neuer Overview mit Bereich/Datei/Symbol-Hierarchie, README-Einstieg, echten gerichteten Beziehungen, Kanten-ID und Quellzeile. Bereich → CALLS-Kante → Quellzeile funktioniert im frischen Audit. | **Teilweise erfüllt.** Bereiche entstehen weiterhin aus Dateipfaden. Verständliche Verantwortlichkeiten, Projektzweck und benannte belegte Systemabläufe fehlen. Einstiegskandidaten sind unzureichend priorisiert. |
| Ein Daemon/Port, persistente Agentereignisse | Agenten-API, SQLite-WAL-Journal, Migration bis Version 3, Cursor/Deduplizierung und projektbezogene Logs. Aktuelles Agent-Polling verwendet 9749, der alte Client versucht 4142. | **Technische Grundlage implementiert.** Keine echte laufende Agentenarbeit im ausgewählten Projekt nachgewiesen: vorhanden sind vier ältere Prüfeinträge. Ein eingerichteter produktiver Hook bleibt erforderlich. Nicht sämtliche Frontendlogs sind auf die SQLite-Quelle vereinheitlicht; ein alter JSONL-Logweg bleibt vorhanden. |
| Auswahlbezogener Kontext | Caller, gerichtete Beziehungen, begrenzter statischer Einstiegspfad, getrennte aufgezeichnete Agentereignisse und Quelllinks. | **Teilweise erfüllt.** Zusätzlicher Inspector-Baustein, kein durchgängig erklärter aufgabenbezogener Ablauf. Der frühere Intent-Chooser bleibt umbenannt bestehen. Gleichzeitige unterschiedliche Einstiegspunkte und textreiche Ausgabe erschweren das Finden des Nutzens. |
| Fehler und Indexdiagnose | Persistente projektbezogene Fehlerhistorie, Live-Hinweise, bestehende Coverage-Abfrage mit korrigierter Pagination, bewusst gestarteter bearbeitbarer lokaler Bericht. 85 partielle Parses und 196 Ausschlüsse sind vollständig als erfasste Datensätze verfügbar. | **Teilweise erfüllt.** System → Logs → ERROR filtert weiterhin nur einen ungefilterten 200er-Tail. Im Audit waren dort null Treffer bei 49 gespeicherten Fehlern. Die neue Historienleiste bietet einen separaten Zugang, vereinheitlicht den naheliegenden Logs-Weg aber nicht. Parserlücken selbst sind nicht behoben. |
| Impact-Widget | Direkte/transitive strukturelle Befunde, getrennte Git-Co-Changes, begrenzte Analyse/Cache, heuristische Risikofaktoren, Datenqualität und Quellziele. | **Teilweise erfüllt.** Der sichtbare Einstieg `change scope` öffnet weiter das alte Panel. Das neue Widget ist separat. Keine Interpretation konkreter Diff-Hunks/geänderter Verträge und keine kompakte Priorisierung betroffener Subsysteme. Testbezüge sind Graphbefunde mit heuristischer Testklassifikation. |

## Konkrete Auffindbarkeits- und Verständlichkeitsprobleme

1. **Viel von dem sichtbaren Produkt gab es vorher bereits.** Navigation, Explore,
   Galaxy, Agentenlayout und die Architecture-Tabs bleiben weitgehend gleich.
   Die neue Karte sitzt im Overview, die übrigen Tabs wurden überwiegend ergänzt
   oder beibehalten. [Vorher](../../graph-ui/verification/pr-2068/audit-before-architecture-overview.png),
   [aktuell](../../graph-ui/verification/pr-2068/audit-after-architecture-overview.png).
2. **Die Übersicht erklärt keine fachlichen Rollen.** Die vier Startkarten sind
   `App`, `main`, `GalaxyPanel` und `graph-ui/tools/smoke-w10.mjs:main`.
   Die ersten Bereiche sind `tests`, `internal/cbm`, `src/foundation` und
   `src/daemon`. Beziehungen und Zählungen sind real; daraus folgt für einen
   Neuling noch keine Erklärung des Systems.
3. **Ein erfolgreicher Klick kann unsichtbar bleiben.** Nach `387 edges · inspect`
   beginnt die erzeugte Beziehungsevidenz bei ungefähr y=1302 im 1000 Pixel hohen
   Browserfenster. Es erfolgt kein Sprung oder Fokus auf das Ergebnis.
   [Direkt nach Klick](../../graph-ui/verification/pr-2068/audit-after-relationship-immediate.png),
   [erst nach Scrollen sichtbar](../../graph-ui/verification/pr-2068/audit-after-relationship-scrolled.png).
4. **Der README-Einstieg ist als Onboarding ungeeignet aufbereitet.** Er öffnet
   Markdown-Quelltext mit Badge-URLs im bestehenden Editor. Daneben erklärt der
   generische Kontext fehlende Graphabhängigkeiten und unbekanntes Risiko.
   Das hilft wenig beim ersten Verstehen des Projekts.
   [Echte Ansicht](../../graph-ui/verification/pr-2068/audit-after-readme-entry.png).
5. **Fehleransichten widersprechen sich im Bedienergebnis.** SQLite hält den
   Fehlerverlauf, der offensichtliche ERROR-Filter des Logs-Tabs findet ihn nicht.
   [Aktueller Leerzustand](../../graph-ui/verification/pr-2068/audit-after-system-errors-filter.png).
6. **Der prominente Impact-Einstieg bleibt defekt.** `change scope → Working tree`
   scheitert im ursprünglichen und im aktuellen Frontend am selben Backend-Weg:
   `detect_changes: base_branch or HEAD is not a commit`. Das neue Widget hat
   einen separaten Einstieg und ersetzt diesen naheliegenden Ablauf nicht.
   [Fehler im aktuellen Frontend](../../graph-ui/verification/pr-2068/audit-after-legacy-impact.png).
7. **Das neue Impact-Widget funktioniert, ist aber ungünstig untergebracht.**
   Für `application_project_lock_release_fully` in der tatsächlich lokal
   geänderten `src/daemon/application.c` zeigt es zwei direkte und 17 insgesamt
   erreichbare Betroffene, sieben heuristisch klassifizierte Testkandidaten und
   zwei Dateien aus einem gemeinsamen Commit. Der Graphpfad nennt eine echte
   Kanten-ID, ein Test-Button navigiert zu `tests/test_daemon_application.c`.
   Fünf Datenqualitätsgrenzen werden offen angezeigt. Die langen Befunde liegen
   im nur ungefähr 299 Pixel hohen eigenen Inspector-Scrollbereich; der übrige
   rechte Bereich bleibt vom Galaxy-Graphen belegt. Git-Evidenz ist lesbar, aber
   Commit-Hash und lokales Kommando haben dort keinen Link oder Kopierbutton.
   [Widget](../../graph-ui/verification/pr-2068/audit-after-impact.png),
   [Graphpfad](../../graph-ui/verification/pr-2068/audit-after-impact-path.png),
   [Git-Befund](../../graph-ui/verification/pr-2068/audit-after-impact-git.png).
8. **Der bestehende Datei-Inspector liefert widersprüchliche Teilergebnisse.**
   Bei `application.c` steht `No symbols found` mit angeblich fehlgeschlagenen
   Function-/Class-Abfragen, während der neue Graphkontext bereits 246
   Caller-Kanten aus fünf Dateien findet. Exakt wiederholte Backend-Abfragen
   liefern tatsächlich 127 Function- und acht Class-Zeilen mit `rows_refs`-
   Kompression. Eine Formatinkompatibilität im Frontend ist damit ein begründeter
   Verdacht; die Ursache wurde in diesem Audit nicht abschließend repariert.
   Der Reader lädt den Quelltext später;
   frühe Loading-Aufnahmen beweisen keinen dauerhaften Ladefehler. Vollständige
   Pfadsuche priorisiert zunächst `application_internal.h` vor der gesuchten
   Datei; der dokumentierte Vergleich weicht anschließend sichtbar auf den
   Explorer aus. Auch solche gewöhnlichen Einstiegsprobleme muss die integrierte
   Lösung behandeln, statt nur einen vorbereiteten Kartenpfad erfolgreich zu testen.

Zur Abgrenzung: `Since ref → HEAD` zeigte in der Browseraufnahme noch einen
Ladezustand. Die anschließende exakt wiederholte Backend-Abfrage lieferte
234 Änderungen und 252 Impact-Befunde mit begrenztem Antwortumfang. Daraus wird
kein Fehlen jeglicher Backend-Diffanalyse abgeleitet. Die bestätigte Lücke ist
deren fehlende Verbindung mit dem neuen Widget sowie der konkret fehlgeschlagene
Working-tree-Bedienweg.

## Was die Recherche tatsächlich beigetragen hat

Die Recherche fand statt; sie war in Tiefe und Umsetzung ungleichmäßig.
Archify und ein Graphify-Beispiel wurden tatsächlich mit Diagrammdaten bedient.
GitNexus und das vom Nutzer bestätigte Codegraph wurden überwiegend anhand
konkreter Frontend-/API-Quellen untersucht, nicht vollständig lokal mit einem
Repository ausprobiert. Eine vergleichende Onboarding-Aufgabe über dieselben
Repository-Daten oder eine Erstnutzerprüfung wurde nicht durchgeführt.

Übernommen wurden Hierarchie/Evidenznavigation, die Trennung von statischen
Befunden und abgeleiteten Gruppen sowie unabhängige historische Co-Changes.
Archifys sofort sichtbarer Pfadfokus bei erhaltenem Überblick, GitNexus-artige
Prozessnavigation und eine rollenbezogene Startübersicht wurden nicht geliefert.
Keine zusätzliche Viewer-/Framework-Abhängigkeit wurde übernommen.

Der [kritische Rechercheabgleich](pr-2068-research-audit.md) belegt diese Aussagen
pro Projekt mit Originalquellen, Code und echten Aufnahmen. Die wesentliche
Schwäche liegt in der unzureichenden Übersetzung der Recherche in zusammenhängende
Nutzerabläufe, nicht im vollständigen Fehlen von Recherche oder Implementierung.

## Noch zu implementierende Abschnitte, keine gelieferten Features

1. Eine erste Repository-Seite mit belegtem Projektzweck, verständlich benannten
   Verantwortlichkeiten und priorisierten Einstiegsmöglichkeiten. Anwendung,
   Infrastruktur, Tests und Entwicklungswerkzeuge müssen in ihrer Rolle erkennbar
   sein, statt pauschal nach Knotengrad zu erscheinen.
2. Ein sichtbarer nachvollziehbarer Weg vom Systembestandteil über Beziehungen
   bis zur Quelle, mit erhaltenem Überblick und sofort erkennbarem Klickergebnis.
3. Ein gemeinsamer Kontext-/Impact-Einstieg für dieselbe Datei, dasselbe Symbol
   und denselben Änderungsumfang; den konkurrierenden alten Weg zusammenführen.
4. Fehlerfilter über den persistierten Bestand vereinheitlichen und die reale
   Agentenquelle im Setup nachvollziehbar anschließen. Leere oder alte Aktivität
   muss eindeutig von tatsächlich aktueller Arbeit unterscheidbar sein.
5. Dieselben konkreten Entwickleraufgaben anschließend vom normalen Einstieg
   aus prüfen, einschließlich Auffindbarkeit, Rollenverständnis, Quellenprüfung
   und nächster Änderungshandlung. Bestehende technische Tests bleiben wichtig,
   ersetzen diese Prüfung jedoch nicht.

Der vollständige ursprüngliche Auftrag ist damit **nicht abgeschlossen**.

# PR 2068: kritischer Abgleich von Recherche und Produkt

Stand: 9. September 2026. Read-only-Audit des Produktcodes; nur dieser Bericht
wurde geschrieben. Verglichen wurden der aktuelle Working Tree und PR-HEAD
`80f4f41f9bcb9daf60afc5f607bcbcba24a4b196`, die geklonten Originalquellen der im
[Recherchebericht](pr-2068-research.md) festgehaltenen Revisionen sowie echte
Recherche- und CBM-Aufnahmen. Relevante GitHub-Originalquellen wurden erneut
geöffnet. Dieser Audit führte keinen zusätzlichen interaktiven CBM-Browserlauf
aus; die Browserbefunde stammen aus den verlinkten Aufnahmen und Protokollen.
Der Hauptauftrag hat während dieses Audits beide Frontendstände mit demselben
aktuellen Daemon/Index erneut bedient; dessen
[Vergleichslauf](../../graph-ui/verification/pr-2068/audit-architecture.json)
von 20:26–20:27 UTC wurde hier zusätzlich gelesen und die relevanten Aufnahmen
visuell geprüft.

**Urteil:** Die Recherche ist teilweise in echte Funktionen eingeflossen. Am
klarsten ist die Übertragung bei gerichteter Evidenznavigation und der Trennung
von Graphbefund und Git-Historie. Das zentrale Onboarding-Versprechen einer
verständlichen Systemkarte ist damit noch nicht eingelöst. Die wichtigsten
Bestandteile werden weiterhin überwiegend als Verzeichnisse und Symbolnamen
präsentiert; ihre Verantwortlichkeiten und ihr Zusammenwirken muss der Leser
selbst rekonstruieren. Das frühere Recherchefazit beschrieb eine sinnvolle
Kombination von Ideen, keine bereits erreichte Produktqualität. Es wäre falsch,
es im Abschluss als vollständige Umsetzung dieser Ideen zu verwenden.

## Was aus welcher Recherche tatsächlich angekommen ist

| Quelle und nachweislich untersuchte Interaktion | Umsetzung im aktuellen CBM-Code | Fehlender Teil / ursprünglicher Anspruch |
| --- | --- | --- |
| **Archify**, MIT. Im Browser tatsächlich Kapitel gewählt und eine gerichtete Route zwischen zwei Knoten geöffnet: [Aufnahme](../../graph-ui/verification/pr-2068/research-archify-route.png). Der [Originalvertrag](https://github.com/tt-a1i/archify/blob/10722002bb8777ecb639d93c49586fae4adf3ae4/archify/references/viewer-runtime.md) hält Kapitel, Fokus und Beziehungen am selben Diagrammmodell. Es ist eine autorenverfasste Demo. | **Teilweise.** [RepositoryMap](../../graph-ui/src/architecture/RepositoryMap.tsx) bietet Übersicht → Bereich → Datei → Symbole mit Breadcrumbs. [selection-context.ts](../../graph-ui/src/why/selection-context.ts) sucht einen tatsächlichen CALLS-Pfad zu einem Entry-Kandidaten, maximal vier Kanten / 500 Symbole; die UI verlinkt die Evidenz. | Kein benannter Systemablauf, keine Kapitel, keine Auswahl beliebiger Start-/Zielpunkte und kein hervorgehobener Pfad im erhaltenen Kartenüberblick. Die Pfadausgabe ist eine vertikale Kantenliste. Das Weglassen des Archify-Compilers erklärt die fehlende Runtime-Abhängigkeit, begründet aber nicht das Ausbleiben dieser eigenen Produktinteraktion. |
| **GitNexus**, [korrekte URL](https://github.com/abhigyanpatwari/GitNexus), PolyForm Noncommercial. Nur der öffentliche Einstieg wurde geöffnet; er wartete auf einen lokalen Server. Prozesssuche, Prozessschritte und Graphfokus wurden ausschließlich in [ProcessesPanel.tsx](https://github.com/abhigyanpatwari/GitNexus/blob/b60c21d05df6af6235ce3f61603ca86469fba8fc/gitnexus-web/src/components/ProcessesPanel.tsx) gelesen. | **Teilweise als allgemeine Idee.** Bereichsauswahl und Beziehungsliste begrenzen die angezeigte Information. Der Kontextpfad nutzt ausschließlich echte CALLS-Kanten; der im Fremdcode erwähnte lineare Ersatzpfad bei fehlenden Kanten wurde nicht nachgebaut. | Kein entsprechendes Prozessmodell oder Prozessbrowser in der neuen RepositoryMap und keine neue Prozessauswahl, die den Galaxy-Graphen auf einen zugehörigen Teilgraphen fokussiert. „Fokussierte Teilgraphen übernommen“ wäre als Beschreibung der sichtbaren Interaktion zu weitgehend. Ein funktionierender GitNexus-Graph wurde von uns nicht ausprobiert. |
| **Graphify-Labs/graphify**, Apache-2.0 mit weiteren Lizenzhinweisen. Das eingecheckte HTML-Beispiel wurde tatsächlich durchsucht und eine Datei ausgewählt: [Aufnahme](../../graph-ui/verification/pr-2068/research-graphify-example-selected.png). Der damalige Nachbarklick scheiterte; das ist kein Nachweis eines Fehlers im aktuellen Exporter. Dessen [Quellcode](https://github.com/Graphify-Labs/graphify/blob/3f82bf7f837a07fb0f7668fbdbd5662801906942/graphify/exporters/html.py) unterscheidet extrahierte, abgeleitete und aggregierte Kanten. | **Teilweise.** Die Karte bezeichnet Verzeichnisbereiche als abgeleitete Grenzen, nennt Beziehungstypen und erhält konkrete Endpunkte, Kanten-IDs und gegebenenfalls Quellzeilen. Keine neue CDN-/Viewer-Abhängigkeit. | Keine vergleichbare Herkunfts-/Konfidenzkennzeichnung jeder einzelnen Kante im Graphen. Die neue UI unterscheidet vor allem statische Indexkanten von heuristischer Gruppierung. Suche/Datei-Inspector und ein großer Graph waren in CBM bereits vorhanden; ihre Existenz ist kein neuer Research-Erfolg. |
| **colbymchenry/codegraph**, MIT, vom Nutzer ausdrücklich bestätigt. Tatsächlicher [HomeView](https://github.com/colbymchenry/codegraph/blob/3ed73bc127323e63153bf6ec8354afa82ce36aaf/ui/src/views/HomeView.svelte), Entry-API und [SymbolView](https://github.com/colbymchenry/codegraph/blob/3ed73bc127323e63153bf6ec8354afa82ce36aaf/ui/src/views/SymbolView.svelte) gelesen, kein laufender Reader mit Graphdaten ausprobiert. Startlisten unterscheiden Routen, ausführende Dateien, Tests und Hubs; Caller und Callees werden neben konkreten Codezeilen angeordnet. | **Teilweise.** „Where to start“, README-Einstieg, ausgewählte Symbole und eingehende/ausgehende Evidenz sind erreichbar. [repository-source.ts](../../graph-ui/src/architecture/repository-source.ts) löst die Auswahl gegen den aktuellen Snapshot auf. | Die neue Startliste trennt diese Rollen nicht: sie nimmt vier `status === 'entry'`-Kandidaten nach `out_calls`. Der reale Lauf bot App, main, GalaxyPanel und ein Smoke-Test-main nebeneinander an. Kein neues Caller–Code–Callee-Layout mit zeilenbezogenen Verbindungen. Die bestehenden Entry-/Routes-/Hotspots-Tabs sind kein neu übernommenes Feature. |
| **lzehrung/codegraph**, zusätzliche MIT-Quelle. [app.js](https://github.com/lzehrung/codegraph/blob/dd58ad3641007a4da90712aef0114d552a8aa492/src/viewer/app.js) verbindet Baumauswahl und Inspector mit gerichteten Referenzlisten; nur Quellcode untersucht. | **Weitgehend auf Komponentenebene.** Architecture und Galaxy erhalten denselben neuen Auswahlkontext und Impact-Baustein aus [App.tsx](../../graph-ui/src/App.tsx). Quellziele verwenden die vorhandene Reader-Navigation. | Kein durchgängiger visueller Kartenpfad: `navigateEvidence` wechselt in Explore. Der Kartenüberblick und die Quellstelle werden nicht gemeinsam präsentiert. Gemeinsame Daten und Navigation sind vorhanden, ein nahtloser orientierender Leseablauf daraus noch nicht vollständig. |
| **optave/ops-codegraph-tool**, zusätzliche Apache-2.0-Quelle. [cochange.ts](https://github.com/optave/ops-codegraph-tool/blob/427c96f6da08fed88559075ef3c3c9eaa2122319/src/features/cochange.ts) und diff-impact-Quelle gelesen, kein eigener CLI-/UI-Lauf. | **Konkret umgesetzt.** [atlas_impact.c](../../src/ui/atlas_impact.c) berechnet begrenzte rückwärts gerichtete CALLS-/IMPORTS-Pfade und separat lokale Co-Changes. [SelectionImpactWidget](../../graph-ui/src/impact/SelectionImpactWidget.tsx) zeigt Pfad-IDs, Quellziele, Commitreferenzen, Datenstand und Einschränkungen. | Keine Analyse einzelner Diff-Hunks oder semantisch geänderter Verträge. Die Auswahl ist Datei/Symbol; uncommittete Änderungen werden als Zustand erkannt. Historienbefunde sind im Daemon kurzzeitig im Speicher gecacht, nicht wie beim Fremdprojekt als Co-Change-Paare in SQLite persistiert. Der ursprüngliche Recherchetext darf nicht als Nachweis eines solchen persistenten Caches gelesen werden. |
| **Code Maat**, zusätzliche GPL-3.0-Quelle. [coupling_algos.clj](https://github.com/adamtornhill/code-maat/blob/50537abb8dffa1b1ba4e91a605ee5c558c01b224/src/code_maat/analysis/coupling_algos.clj) filtert große Changesets und berücksichtigt Mindestdaten; nur Quelle gelesen. | **Konkret umgesetzt.** CBM schließt Merge-Commits und Änderungen mit mehr als 30 Dateien aus, begrenzt auf 200 Commits / 548 Tage und nennt Ausschlüsse. [selectionRisk](../../graph-ui/src/impact/selection-impact.ts) benennt Regeln und Unsicherheit getrennt; keine Defektwahrscheinlichkeit. | Keine kalibrierte Prognose, keine Defekthistorie und keine historische Testwirksamkeitsanalyse. Das ist eine fachlich angemessene Begrenzung, kein noch fehlendes Prozent-Feature. Die Übertragbarkeit der Regeln ist belegt; ihre konkreten Schwellen sind eigene Heuristiken. |

Es gibt keinen neuen Eintrag in CBMs Frontend-Paketmanifest oder Lockfile. Die
Entscheidung gegen zusätzliche Viewer-/Framework-/Datenbank-Stacks wurde also
tatsächlich eingehalten. Daraus folgt weder, dass jede sinnvolle Interaktionsidee
umgesetzt ist, noch dass Lizenzen die fehlende eigene Umsetzung verhindert hätten.

## Warum der sichtbare Unterschied klein bleibt

Der PR-HEAD hatte bereits die fünf Architecture-Tabs, Modul-Karten, Layer- und
Community-Listen, ein Abhängigkeitsdiagramm und Quelllinks. Das ist in der echten
[Vorher-Aufnahme](../../graph-ui/verification/pr-2068/before-architecture.png) und
in `git show HEAD:graph-ui/src/architecture/ArchitecturePanel.tsx` überprüfbar.
Neu sind die RepositoryMap innerhalb des Overview-Tabs und konkrete
Kanten-Evidenz in Dependencies; die vorhandene Zusammenfassung liegt nun in einer
Disclosure. Rahmen, Raster, Farben und Tabstruktur bleiben weitgehend gleich.
In `GalaxyPanel.tsx` ergänzt der Produktdiff einen Auswahl-Aside, keine neue
Graphdarstellung.

Die älteren Vorher-/Nachher-Aufnahmen stammen aus verschiedenen Indexständen.
Ihre geänderten Knoten-/Dateizahlen messen keinen Produktfortschritt. Der neue
Vergleich hält den Daemon und Index gleich und zeigt erneut die beschriebene
Grenze: [Overview vorher](../../graph-ui/verification/pr-2068/audit-before-architecture-overview.png),
[Overview nachher](../../graph-ui/verification/pr-2068/audit-after-architecture-overview.png).

Der [aktuelle Aufnahmezustand nach der Log-Korrektur](../../graph-ui/verification/pr-2068/audit-after-architecture-overview.png)
zeigt die verbleibende Produktlücke deutlich: Namen wie `internal/cbm`,
`src/foundation` und `src/daemon` plus Zahlen und Funktionsnamen. In
[repository-map.ts](../../graph-ui/src/architecture/repository-map.ts) bildet
`areaOf` die Bereiche ausschließlich aus dem ersten beziehungsweise zweiten
Pfadsegment. Graphkanten beeinflussen Rangfolge und Beziehungen, nicht die
inhaltliche Benennung oder die Grenzen. Die optionale Layer-Zeile nach dem Öffnen
eines Bereichs ist ein zusätzliches Heuristikergebnis, keine Erklärung seiner
Verantwortlichkeit. Die UI kann bereits mehr als ein Dateibaum, beantwortet aber
„Was macht dieses System und wie arbeitet es?“ noch nicht auf Übersichtsebene.

Der neue Nutzen erscheint zudem überwiegend nach mehreren Aktionen: Bereich
öffnen, Relation wählen, Evidenz aufklappen oder Symbol auswählen. Dann entstehen
lange Listen. Im frischen Browserlauf blieb die neu erzeugte Beziehungsevidenz
nach „inspect“ unterhalb des sichtbaren Bereichs: Beginn ungefähr bei y=1302 in
einem 1000 Pixel hohen Viewport. Der Nutzer musste zusätzlich scrollen.
[Unmittelbare Aufnahme](../../graph-ui/verification/pr-2068/audit-after-relationship-immediate.png),
[nach zusätzlichem Scrollen](../../graph-ui/verification/pr-2068/audit-after-relationship-scrolled.png).
Das passt exakt zum Code: Der Klick ruft in `RepositoryMapView` nur `setEvidence`
auf und rendert die Liste unter dem Bereich; `scrollIntoView` reagiert allein
auf `selection?.id`. Archifys erhaltener Überblick plus sofort sichtbarer
Fokusbefund ist hier gerade nicht angekommen.

Auch der README-Einstieg ist funktional, aber als Onboarding schwach:
[aktuelle Aufnahme](../../graph-ui/verification/pr-2068/audit-after-readme-entry.png).
Er öffnet ungefilterten Markdown-Quelltext einschließlich vieler Badge-URLs in
Explore. Der daneben eingeblendete generische Hinweis auf fehlende Graphkanten
und unbewiesenes niedriges Risiko beantwortet beim Lesen der Projekteinführung
keine hilfreiche Frage. `onNavigate(readme)` und der bedingungslos für jede
ausgewählte Datei eingefügte Kontext erklären diesen sichtbaren Zustand.

[SelectionContext](../../graph-ui/src/why/SelectionContext.tsx)
liefert echte Caller-/Pfadinformation statt der früheren generischen Frage.
Es liest jedoch keinen aktuellen Aufgabentext und korreliert Agentenereignisse
nur mit derselben Datei; eine Erklärung „gelesen, anschließend abhängiges Symbol
geändert“ wird nicht berechnet. Das ist ehrlich, aber enger als das Zielbild.

Impact beantwortet eine konkrete Entwicklerfrage bereits substanzieller. Die
[reale Aufzeichnung](../../graph-ui/verification/pr-2068/map-e2e.json) enthält für
`cbm_daemon_application_new` einen statischen Einstiegspfad, direkte und
transitive Betroffene sowie getrennte Git-Befunde. Trotzdem bleibt die Ausgabe
symbolorientiert und textreich: keine priorisierte Zusammenfassung betroffener
Subsysteme, kein visueller Zusammenhang von Änderung und Systemablauf. Der
[Impact-Screenshot](../../graph-ui/verification/pr-2068/after-context-and-impact.png)
belegt diese Darstellung. Sein alter globaler Fehlerbanner ist inzwischen
korrigiert; er darf nicht als aktueller Fehlerzustand ausgegeben werden.

## Korrektur des bisherigen Abschlussanspruchs

- **Implementiert:** stufenweises Öffnen von Quellbereichen, konkrete gerichtete
  Beziehungsevidenz, ein Auswahlkontext mit echtem begrenztem CALLS-Pfad und
  getrennter Agentenbeobachtung, sowie Auswahl-Impact mit getrennten Graph- und
  Git-Befunden. Diese Aussagen sind im Code und in echten Ablaufprotokollen
  verankert.
- **Noch nicht eingelöst:** eine Übersicht, aus der ein Neuling die
  Verantwortlichkeiten und das Zusammenwirken der Hauptbestandteile erklären
  kann; gut unterschiedene Einstiegstypen; ein anschaulicher Ablauf-/Pfadfokus
  mit erhaltenem Überblick; eine kompakte Priorisierung der betroffenen
  Komponenten. Das Architect-Abnahmekriterium darf deshalb nicht als vollständig
  erfüllt gelten.
- **Recherchegrenze:** Zwei Fremdansichten wurden mit Graph-/Diagrammdaten
  tatsächlich bedient, die übrigen relevanten Interaktionen überwiegend anhand
  von Quellcode geprüft. Es gab keinen Vergleich mit Neulingen, keine gemessene
  Onboarding-Verbesserung und keinen belastbaren Vergleich über dieselbe
  Repository-Aufgabe in mehreren Tools. Testzahlen ersetzen keinen dieser
  Nachweise.

Der nächste Produktabschnitt müsste die bereits vorhandenen Beziehungen in eine
überschaubare, benannte Systemerklärung mit überprüfbaren Einstiegspfaden
übersetzen. Das ist eine verbleibende Implementierungsaufgabe, kein durch diesen
Audit geliefertes Feature. Weitere Bibliotheken oder automatisch erzeugter Text
allein würden die festgestellte Lücke nicht schließen.

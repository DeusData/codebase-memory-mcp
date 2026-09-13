# PR 2068: überprüfbare Produktrecherche

Recherche am 9. September 2026. Untersucht wurden öffentliche Original-Repositories,
deren konkrete Frontend-/Analysequellen und mehrere im Browser erreichbare Oberflächen.
Es wurden keine Accounts erstellt, keine privaten Repositorydaten hochgeladen und
keine Fremdimplementierungen oder neuen Laufzeitabhängigkeiten übernommen. Die
Entscheidungen unten sind Implementierungsleitlinien; sie sind kein Nachweis, dass
ein CBM-Abnahmekriterium bereits erfüllt ist.

## Namen und untersuchter Stand

| Nutzereingabe | Verifikation | Untersuchte Revision / Lizenz |
| --- | --- | --- |
| `gitnexus` | [abhigyanpatwari/GitNexus](https://github.com/abhigyanpatwari/GitNexus) ist das Originalrepository der gefundenen gleichnamigen Forks; die Projekt-Homepage verweist auf `gitnexus.vercel.app`. | `b60c21d05df6af6235ce3f61603ca86469fba8fc`; **PolyForm Noncommercial 1.0.0**, laut tatsächlicher [LICENSE](https://github.com/abhigyanpatwari/GitNexus/blob/b60c21d05df6af6235ce3f61603ca86469fba8fc/LICENSE). Kein permissiv lizenziertes Open-Source-Bauteil für CBM. |
| `gtaphyfy` | Für die exakte Schreibweise zunächst kein belastbarer Projekttreffer. **Im Gespräch ausdrücklich bestätigt:** [Graphify-Labs/graphify](https://github.com/Graphify-Labs/graphify). | Default-Branch `v8`, `3f82bf7f837a07fb0f7668fbdbd5662801906942`; Apache-2.0, zusätzliche MIT-Hinweise in `LICENSE-MIT`/`NOTICE`. |
| `coedgraph` | **Vom Nutzer mit konkreter URL aufgelöst:** [colbymchenry/codegraph](https://github.com/colbymchenry/codegraph). Repository, echte Svelte-Oberfläche und zugehörige Server-API-Quellen untersucht; siehe eigener Abschnitt. | `3ed73bc127323e63153bf6ec8354afa82ce36aaf`, MIT, HEAD vom 9. September 2026. |
| Zusätzliche Codegraph-Inspiration | Unabhängig untersucht: [lzehrung/codegraph](https://github.com/lzehrung/codegraph) und [optave/ops-codegraph-tool](https://github.com/optave/ops-codegraph-tool). Die alte URL `optave/codegraph` leitet auf Letzteres um. Beide sind **nicht** das vom Nutzer gemeinte CodeGraph. | lzehrung: `dd58ad3641007a4da90712aef0114d552a8aa492`, MIT. optave: `427c96f6da08fed88559075ef3c3c9eaa2122319`, Apache-2.0. |
| Explizite Archify-URL | [tt-a1i/archify](https://github.com/tt-a1i/archify), eindeutige Zuordnung. | `10722002bb8777ecb639d93c49586fae4adf3ae4`, MIT. |
| Zusätzliche Impact-Inspiration | [adamtornhill/code-maat](https://github.com/adamtornhill/code-maat), eigenständige Recherchequelle. | `50537abb8dffa1b1ba4e91a605ee5c558c01b224`, GPL-3.0 laut README, `project.clj` und Quelltextkopf. |

Die anfängliche Mehrdeutigkeit wurde nicht stillschweigend aufgelöst. Nach der
Klarstellung des Nutzers sind die drei gemeinten Projekte nun eindeutig zugeordnet.

## Was tatsächlich ausprobiert wurde

Alle Screenshots sind unveränderte Playwright-Aufnahmen mit Chromium, 1440×1000.
Sie zeigen **Fremdprojekte**, keine behauptete CBM-Implementierung. Metadaten und
Dateiprüfsummen stehen in [research-captures.json](../../graph-ui/verification/pr-2068/research-captures.json).

| Oberfläche / Daten | Tatsächliche Interaktion und Befund | Screenshot |
| --- | --- | --- |
| [Archify: Agent Tool Call Workflow](https://tt-a1i.github.io/archify/gallery/artifacts/agent-tool-call.workflow.html), öffentliches **autorenverfasstes Beispieldiagramm**, kein aus CBM extrahierter Ablauf | Übersicht geöffnet; Kapitel „Request to result“ gewählt und Anzeige `1 / 3` abgewartet. Sichtbare Verantwortlichkeitsbereiche und Knotennamen erklären die Abfolge. | [Übersicht](../../graph-ui/verification/pr-2068/research-archify-overview.png), [Kapitel](../../graph-ui/verification/pr-2068/research-archify-chapter.png) |
| Dieselbe Archify-Demo | PATH geöffnet, `User`, dann `Tool Call` geklickt. Ergebnis: sechs benannte Knoten, fünf gerichtete Schritte. Panel bezeichnet die Beziehung ausdrücklich als kürzesten **autorenverfassten** Pfad. | [Ausgewählte Route](../../graph-ui/verification/pr-2068/research-archify-route.png) |
| [GitNexus Web](https://gitnexus.vercel.app) | Öffentliche Seite geladen. Tatsächliche Oberfläche fordert einen lokalen `gitnexus serve` auf Port 4747 an und wartet auf ihn; keine anonyme Beispieldatenansicht angeboten. Kein Login verlangt. Lokalen Fremdserver nicht installiert. Graph-/Prozess-Interaktionen daher **nur im Quellcode untersucht**. | [Realer Einstiegszustand](../../graph-ui/verification/pr-2068/research-gitnexus-entry.png) |
| Graphify: eingechecktes [RSL Siege Manager Beispiel](https://github.com/Graphify-Labs/graphify/blob/3f82bf7f837a07fb0f7668fbdbd5662801906942/worked/rsl-siege-manager/graph.html), lokal als HTML geöffnet | Vorhandene Beispieldaten: 1886 Knoten, 3876 Kanten, 141 Communities. Suche `main.py`, exakten Treffer gewählt; Inspector nennt `bot/app/main.py` und fünf Nachbarn. Danach erster Nachbar geklickt: Inspector wechselte **nicht**, Browser meldete `Unexpected end of input` durch fehlerhaftes Inline-`onclick`. Der aktuelle Exporter enthält inzwischen event-listener-basierten Code; der Fehler ist nur am eingecheckten Beispiel reproduziert, nicht am aktuellen Exporter. | [Ausgewählte Beispieldatei](../../graph-ui/verification/pr-2068/research-graphify-example-selected.png) |
| [colbymchenry/codegraph Website](https://colbymchenry.github.io/codegraph/) | Im Browser geöffnet: öffentliche Produkt-/Dokumentationseinstiegsseite, kein interaktiver Reader mit Repositorydaten. Die relevanten Reader-Interaktionen wurden deshalb **im tatsächlichen Svelte- und API-Quellcode** untersucht; keine lokale Fremdindexierung gestartet. | Kein Screenshot als vermeintlicher Reader-Nachweis. |

Die Graphify-Beispieldatei lädt vis-network von unpkg. Es wurden dabei nur diese
öffentlichen Beispieldaten geöffnet. Diese CDN-Abhängigkeit ist für das lokale
CBM-Produkt kein geeignetes Integrationsmuster. Screenshot und DOM wurden geprüft;
eine vollständige Fremdprodukt-Testsuite oder Sicherheitsaudit wurde nicht ausgeführt.

## Beobachtung → Nutzerproblem → Entscheidung

### Archify: geführte Ebenen und gerichtete Beziehungen

**Beobachtung:** Das ausprobierte Diagramm verbindet sichtbare Bereiche, benannte
Knoten, auswählbare Kapitel und einen expliziten gerichteten Pfad. Übersicht und
Detail behalten denselben Kontext. Die [Runtime-Beschreibung](https://github.com/tt-a1i/archify/blob/10722002bb8777ecb639d93c49586fae4adf3ae4/archify/references/viewer-runtime.md)
unterscheidet Lesetiefen; die Demo enthält tatsächliche Knoten-/Kantenidentitäten.

**Gelöstes Problem:** „Wie komme ich vom Einstieg zu diesem Bestandteil?“ lässt
sich Schritt für Schritt erklären, ohne einen kompletten Graphen zu interpretieren.

**Übertragung / Aufwand / Lizenz:** Kleine eigene React-Übersicht mit benannten
Bereichen, Beziehungsliste und Evidenznavigation: mittlerer Aufwand. MIT ermöglicht
grundsätzlich Wiederverwendung mit Hinweisen; der separate JSON-Diagrammcompiler
würde jedoch eine zweite Modell-/Renderarchitektur einführen.

**Entscheidung:** Interaktionsidee übernehmen, Compiler nicht integrieren. CBM
muss aus Graphkanten belegte Pfade bilden und Cluster als Heuristik kennzeichnen.
Autorenverfasste Demo-Abläufe sind kein Beleg für automatisch erkannte Datenflüsse.

### GitNexus: Prozesse als navigierbare Teilgraphen

**Quellcodebeobachtung:** [ProcessesPanel.tsx](https://github.com/abhigyanpatwari/GitNexus/blob/b60c21d05df6af6235ce3f61603ca86469fba8fc/gitnexus-web/src/components/ProcessesPanel.tsx)
trennt Prozesse innerhalb und zwischen Communities, erlaubt Suche und fokussiert
Teilgraphen. Es lädt Prozessschritte mit Dateipfaden und fragt `CALLS`-Kanten ab.
Der Fehlerpfad erwähnt allerdings eine lineare Ersatzdarstellung, wenn Kantenabfragen
fehlschlagen. Das wurde im Code gelesen, nicht im laufenden Graphen getestet.

**Gelöstes Problem:** Ein benannter Einstieg in einen überschaubaren Teilgraphen
ist leichter handhabbar als eine Menge unbeschrifteter Punkte.

**Übertragung / Aufwand / Lizenz:** Eigene auf echten Kanten basierende
Beziehungsnavigation: mittel. PolyForm Noncommercial schließt eine ungeprüfte
Codeübernahme aus. Der Webstack enthält unter anderem Sigma/Graphology, Mermaid
und LangChain-Anbindungen; das wäre für die vorhandene CBM-Oberfläche unnötig groß.

**Entscheidung:** Fokussierte Teilgraphen als Produktidee nutzen. Kein Fremdcode,
kein LLM für Grundkontext, keine linearen Scheinpfade bei fehlenden Kanten.

### Graphify: Herkunft einer Kante sichtbar machen

**Beobachtung:** Der aktuelle [HTML-Exporter](https://github.com/Graphify-Labs/graphify/blob/3f82bf7f837a07fb0f7668fbdbd5662801906942/graphify/exporters/html.py)
unterscheidet extrahierte und abgeleitete Beziehungen durch Linien und Metadaten.
Bei großen Graphen aggregiert er Communities mit explizitem `AGGREGATED`-Typ.
Im tatsächlich geöffneten Beispiel führt die Suche zum Datei-Inspector; generische
Community-Nummern und die große Knotenmenge erklären allein keine Verantwortung.

**Gelöstes Problem:** „Was ist beobachtet, was ist abgeleitet?“; Suchtreffer
bekommen eine überprüfbare Herkunft.

**Übertragung / Aufwand / Lizenz:** Evidenzart als Text plus Linien-/Badge-Stil:
klein. Apache-2.0 mit vorhandenen Drittanbieterhinweisen; Python/NetworkX und
vis-network brauchen wir für diese Interaktion nicht zusätzlich.

**Entscheidung:** Evidenzkennzeichnung, Aggregationshinweis und Quellnavigation
übernehmen. Keine generischen Community-Nummern als alleinige Architekturbegriffe;
kein CDN oder Inline-HTML aus Repositorytext. Den reproduzierten Beispiel-Linkfehler
als Anlass nehmen, echte Navigationsaktionen statt nur sichtbarer Links zu testen.

### lzehrung/codegraph: Graph und Dateinavigation synchron halten

**Quellcodebeobachtung:** [app.js](https://github.com/lzehrung/codegraph/blob/dd58ad3641007a4da90712aef0114d552a8aa492/src/viewer/app.js)
verbindet Baumauswahl, Graphfokus und Inspector. Eingehende und ausgehende
Datei-/Symbolreferenzen sind getrennt und führen zurück zum jeweiligen Baumelement.
Externe Pakete und Symbole lassen sich explizit einblenden. Die Visualisierung
verwendet lokal mitgelieferte Sigma-/Graphology-Assets. Kein eigener Browserlauf.

**Gelöstes Problem:** „Was verwendet dieses Element und wer verwendet es?“ ohne
Orientierungsverlust beim Wechsel zwischen Abhängigkeiten.

**Übertragung / Aufwand / Lizenz:** Gemeinsame Elementauswahl und bidirektionale
Referenzlisten: klein bis mittel. MIT, zusätzliche vendorte Lizenzdateien. Die
eingebauten Graphbibliotheken wären eine weitere Rendertechnik in CBM.

**Entscheidung:** Einheitliche Auswahl und explizite Richtung umsetzen;
vorhandene React-/Graphkomponenten weiterverwenden. Kein Viewer-/Indexerimport.

### colbymchenry/codegraph: Einstieg und zeilengenaue Aufrufbeziehungen

**Quellcodebeobachtung:** [HomeView.svelte](https://github.com/colbymchenry/codegraph/blob/3ed73bc127323e63153bf6ec8354afa82ce36aaf/ui/src/views/HomeView.svelte)
gibt bereits ohne Auswahl konkrete Einstiege. Die zugehörige
[entrypoints.ts](https://github.com/colbymchenry/codegraph/blob/3ed73bc127323e63153bf6ec8354afa82ce36aaf/src/ui-server/api/entrypoints.ts)
liefert getrennt Routen, auf Dateiebene aufrufende Dateien, Tests und häufig
referenzierte Symbole. Sie begrenzt die Ergebniszahl je Ordner, kennzeichnet
Zählgrenzen und bindet den Cache an den Indexstand. Ein Hub wird dabei ausdrücklich
nicht als zuerst ausgeführter Code interpretiert.

**Gelöstes Problem:** „Wo fange ich an?“ bekommt überprüfbare Auswahlmöglichkeiten,
ohne Einstiegspunkt, Test und zentrale Abhängigkeit gleichzusetzen.

**Weitere belegte Interaktion:** [SymbolView.svelte](https://github.com/colbymchenry/codegraph/blob/3ed73bc127323e63153bf6ec8354afa82ce36aaf/ui/src/views/SymbolView.svelte)
ordnet Caller, Originalcode und Callees um konkrete Aufrufzeilen an. Bei veralteten
Symbol-IDs versucht es eine erneute Auflösung. Der gemeinsame
[Live-Adapter](https://github.com/colbymchenry/codegraph/blob/3ed73bc127323e63153bf6ec8354afa82ce36aaf/ui/src/lib/live.svelte.ts)
verteilt Indexänderungen über `/api/events`. Das beschreibt Quellcodeverhalten,
keinen von uns ausgeführten Reader-End-to-End-Test.

**Übertragung / Aufwand / Lizenz:** Einstiegsliste und gerichtete Referenz-Evidenz
sind im vorhandenen CBM-Modell mit mittlerem Aufwand möglich. MIT; der Reader nutzt
Svelte 5 und `@xyflow/svelte`, was gegenüber vorhandenen React-Komponenten eine
zusätzliche Framework-/Layoutabhängigkeit wäre.

**Entscheidung:** Konkrete Einstiege, gemeinsame Auswahl und revisionsbewusste
Navigation übernehmen; keine Svelte-Komponenten oder zusätzlichen Server
integrieren. Nicht übertragbar ohne weitere Evidenz: behauptete Testausführung,
garantierte Vollständigkeit und eine Rangfolge als tatsächlicher Laufzeitablauf.

### optave/ops-codegraph-tool: Historie als eigene Evidenzquelle

**Quellcodebeobachtung:** [cochange.ts](https://github.com/optave/ops-codegraph-tool/blob/427c96f6da08fed88559075ef3c3c9eaa2122319/src/features/cochange.ts)
liest Git ohne Merge-Commits, begrenzt Changesets, zählt gemeinsame Änderungen und
persistiert Paare samt Historienstand in SQLite. [diff-impact.ts](https://github.com/optave/ops-codegraph-tool/blob/427c96f6da08fed88559075ef3c3c9eaa2122319/src/domain/analysis/diff-impact.ts)
führt historischen und strukturellen Befund getrennt. Kein CLI-/UI-Lauf.

**Gelöstes Problem:** „Welche Dateien sollte ich zusätzlich prüfen, weil sie in
der Vergangenheit wiederholt zusammen geändert wurden?“

**Übertragung / Aufwand / Lizenz:** Begrenzter lokaler Git-Lauf plus Cache in der
vorhandenen SQLite-Struktur: mittel. Apache-2.0. Zweiter Tree-sitter-/SQLite-Stack
wäre Doppelarbeit; die unabhängige Implementierung benötigt keine neue Bibliothek.

**Entscheidung:** Historie getrennt ausweisen, Merge-/Massenänderungen ausfiltern,
Revision, Umfang und fehlende Historie anzeigen. Gemeinsame Änderungen nicht als
funktionale Abhängigkeit oder Ausfallwahrscheinlichkeit ausgeben.

### Code Maat: große Commits verfälschen Kopplung

**Quellcodebeobachtung:** [coupling_algos.clj](https://github.com/adamtornhill/code-maat/blob/50537abb8dffa1b1ba4e91a605ee5c558c01b224/src/code_maat/analysis/coupling_algos.clj)
filtert übergroße Changesets vor Paarbildung. Dokumentierte Mindestanzahlen für
Revisionen und gemeinsame Änderungen machen die Datenbasis sichtbar. Clojure-CLI,
keine grafische Oberfläche; nicht gestartet.

**Gelöstes Problem:** Ein Formatierungs-/Migrationscommit soll nicht praktisch alle
Dateien zu scheinbar eng gekoppelten Komponenten machen.

**Übertragung / Aufwand / Lizenz:** Eigene einfache Filter-/Zählregeln: klein.
GPL-3.0 und JVM/Clojure sind zusätzliche Gründe gegen Code-/Runtimeübernahme.

**Entscheidung:** Mindestdatenbasis und Ausschlüsse transparent anzeigen. Eine in
der README verwendete Risiko-/Chance-Deutung von Kopplungsprozenten wird bewusst
nicht übernommen: Co-change beschreibt vergangene Zusammenarbeit, keine Defekte.

## Wartung, Kompatibilität und Grenzen

Die geprüften HEAD-Commits liegen bei Archify, GitNexus, Graphify und den beiden
Codegraph-Projekten im September 2026; bei Code Maat im Juli 2025. Das sind
reproduzierbare Aktivitätsindikatoren, keine Qualitäts- oder Wartungsgarantie.
Lizenzdateien und relevante Manifeste wurden gelesen. Keine externe Abhängigkeit
wird hinzugefügt; deshalb war kein Abhängigkeitsaudit als Integrationsfreigabe
erforderlich. Es wurde insbesondere keine Sicherheit allein aus Popularität,
Sternen oder Marketing abgeleitet.

Die umsetzbare Kombination ist eine gemeinsame Auswahl mit stufenweiser
Repositoryübersicht, expliziten Relationstypen, überprüfbarer Quellnavigation,
separater historischer Evidenz und sichtbarer Unvollständigkeit. Die genannten
Ansätze lösen unterschiedliche Entwicklerfragen; keiner ersetzt den vollständigen
CBM-Datenweg und dessen eigene Integrationstests.

## User-requested local Archify preview

On 2026-09-09, after the user explicitly requested a separate browser port, served
the unmodified checked-out Archify `docs` directory at `127.0.0.1:9750`.
`GET /gallery/artifacts/agent-tool-call.workflow.html` returned HTTP 200, and
macOS `open` opened that URL in the user's browser. The gallery is at
`http://127.0.0.1:9750/gallery.html`. These are Archify-authored example diagrams,
not an analysis of Codebase Memory MCP. This optional comparison server is
separate from the application; the application remains on its one daemon port.

Reproduce the preview from the research checkout with:

```sh
python3 -m http.server 9750 --bind 127.0.0.1 --directory /tmp/cbm-pr2068-research/archify/docs
```

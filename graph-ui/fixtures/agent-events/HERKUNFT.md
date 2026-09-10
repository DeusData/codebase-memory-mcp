# Herkunft dieser Ereignisdatei

`w11a-replay.jsonl` ist die Aufzeichnung, gegen die der Beweislauf von W11a
faehrt (`npm run smoke:w11a`). Sie liegt im Repository, weil eine Ansicht ueber
laufende Agenten sonst nur dann messbar waere, wenn zufaellig ein Agent laeuft;
byte-stabil, weil zwei Laeufe an derselben Datei dieselben Bilder ergeben
muessen.

## Woraus sie entstanden ist

Aus einer **echten** Aufzeichnung: einer `events.jsonl` im Heimatverzeichnis
des Agenten-Werkzeugs, geschrieben von einem Werkzeug-Hook in der Nacht vom 29.
auf den 30. August 2026, waehrend an diesem Repository gearbeitet wurde. Die
Datei liegt nicht im Repository (sie enthaelt Pfade und Befehle einer fremden
Maschine); was von ihr uebernommen wurde, steht hier vollstaendig.

**Uebernommen, unveraendert:**

- Die **Form**: acht Felder je Zeile (`ts`, `agent`, `run`, `seq`, `phase`,
  `tool`, `path`, `detail`), eine Zeile JSON je Werkzeugaufruf, angehaengt in
  der Reihenfolge, in der die Aufrufe endeten.
- Die **Zeitabstaende**: 44 gemessene Abstaende aus einem zusammenhaengenden
  Fenster der Aufzeichnung (ab ihrer 57. Zeile, dem ersten Fenster von 45
  Ereignissen ohne eine Pause ueber zwei Minuten). Sie stehen in der Reihenfolge
  da, in der sie gemessen wurden: 10065, 13599, 115703, 5123, 18182, 6789, 1140,
  13387, 5243, 5570, 6676, 21168, 51146, 6330, 6339, 28257, 4029, 318, 2709,
  709, 83, 6176, 1116, 3373, 3642, 3149, 2275, 4175, 2884, 4066, 2310, 2028,
  2838, 33596, 5878, 17621, 826, 5788, 11062, 6988, 5724, 10001, 17767, 13461
  Millisekunden. Daher die ungleichmaessige Dichte: eine Kette aus drei
  Sekunden, dann eine halbe Minute Ruhe. Ein Takt aus gleichen Abstaenden waere
  ein Bild, das so nie entsteht.
- Die **Mischung der Werkzeuge**: in der Aufzeichnung ueberwiegen Befehle,
  danach kommen Aenderungen, dann Lesen. Dieselbe Ordnung steht hier.
- Der **Anker der Zeit**: `ts` der ersten Zeile ist der echte Zeitstempel des
  Fensteranfangs (1788038561000).

**Geaendert, und warum:**

1. **Die Pfade.** In der Aufzeichnung stehen absolute Pfade dieses Repositories
   und eines Arbeitsverzeichnisses unter `/private/tmp`. Ein Ereignis laesst
   sich nur verorten, wenn der Index den Pfad kennt, und der Index dieses
   Beweislaufs ist `fixtures/atlas-sample`. Jeder Pfad hier ist darum ein Pfad
   jener Fixture, repo-relativ, so wie `/api/layout` ihn fuehrt. Genau ein Pfad
   ist mit Absicht **nicht** darin enthalten (`package.json`): er belegt den
   Fall "nicht im Graphen verortbar", der im Instrument stehen bleiben muss,
   statt zu verschwinden.
2. **Die Namen der Agenten.** Die Aufzeichnung fuehrt einen einzigen Namen, den
   des Werkzeugs, das den Hook aufgerufen hat. Er steht hier aus zwei Gruenden
   nicht: das Repository nennt kein Werkzeug und kein Modell als Urheber
   (PLAN.md, CLAUDE.md), und die Ansicht ist erst dann messbar, wenn mehr als
   ein Koerper im Bild steht. Die drei Namen `implementer`, `checker` und
   `explorer` sind die Rollen, die in diesem Projekt wirklich getrennt
   arbeiten. Ihre `run`-Kennungen sind erfunden und haben die Laenge der echten
   (acht Zeichen).
3. **Die Zeilenbereiche (`lines`).** Der Hook der Aufzeichnung hat sie nicht
   geschrieben; der Hook dieses Repositories (`agents/hooks/atlas-trace.py`)
   schreibt sie, weil die genaue Zuordnung an ihnen haengt. Die Bereiche hier
   sind die echten Bereiche der Symbole in `fixtures/atlas-sample`, abgelesen
   an `/api/layout` (`start_line`, `end_line`), nicht geschaetzt.
4. **Eine Luecke in der Reihenfolge.** Der Lauf `b7e4d19c` zaehlt von 6 auf 9
   weiter. Zwei Ereignisse fehlen also, und das Instrument muss es sagen, statt
   eine lueckenlose Geschichte zu zeichnen. Die Luecke ist mit Absicht
   eingesetzt; in der Aufzeichnung gibt es keine, weil dort nichts abgerissen
   ist.
5. **Zwei `intent`-Felder.** In der Aufzeichnung kommt das Feld kein einziges
   Mal vor, weil kein Agent es geschrieben hat. Es steht hier genau zweimal,
   damit der Beweislauf beide Seiten der Ehrlichkeitsregel messen kann: ein
   Ereignis ohne `intent` erzeugt nie eine Absichtszeile, eines mit `intent`
   erzeugt sie immer, gekennzeichnet als Selbstauskunft des Agenten. Wer diese
   zwei Zeilen im Bild sieht, sieht eine Aussage des Agenten ueber sich selbst
   und keine Messung.
6. **Die `detail`-Texte.** Sie beschreiben, was an der Fixture getan wurde. In
   der Aufzeichnung stehen dort die echten Befehle und die Beschreibungen der
   Werkzeugaufrufe; sie waeren hier Saetze ueber Dateien, die der Index nicht
   kennt.

## Was NICHT darin steht

Kein Dateiinhalt. Weder die Aufzeichnung noch diese Datei traegt eine Zeile
Quelltext: was ein Ereignis sagt, ist welches Werkzeug welchen Pfad in welchem
Zeilenbereich beruehrt hat, und was der Befehl war. Das ist die Grenze des
Formats und nicht eine Einstellung daran.

## Die Zahlen dieser Datei

45 Zeilen, 3 Laeufe, 4 Arten von Arbeit (Lesen, Schreiben, Suchen, Testen),
1 Luecke von 2 Ereignissen, 2 Selbstauskuenfte, 1 Pfad ausserhalb des Index.

# `w11b-replay.jsonl`: die zweite Aufzeichnung

Sie ist die Datei, gegen die der Beweislauf von W11b faehrt
(`npm run smoke:w11b`). Der wichtigste Satz zuerst, damit niemand sie fuer
etwas haelt, was sie nicht ist:

**Diese Datei ist KONSTRUIERT und nicht gemessen.** Sie ist keine Aufzeichnung
einer echten Sitzung wie `w11a-replay.jsonl`. Sie stellt die Lagen her, die
W11b messen muss, und sie tut es absichtlich und nachlesbar. Ihre Zeitabstaende
sind gewaehlt und nicht aufgezeichnet.

## Was daran echt ist

- **Die Pfade und die Zeilenbereiche.** Jeder Pfad ist ein Pfad von
  `fixtures/atlas-sample`, repo-relativ, so wie `/api/layout` ihn fuehrt. Jeder
  Zeilenbereich ist der echte Bereich des Symbols in jener Fixture
  (`start_line`, `end_line`), abgelesen an der Quelle und nicht geschaetzt.
- **Die Form.** Dieselben acht Felder je Zeile wie in der echten Aufzeichnung,
  eine Zeile JSON je Werkzeugaufruf, in der Reihenfolge, in der die Aufrufe
  endeten.

## Was daran gebaut ist, und wofuer

1. **Acht Agenten** (`implementer`, `checker`, `explorer`, `reviewer`,
   `tracer`, `indexer`, `packer`, `planner`). AC7 verlangt die Bildrate bei
   acht Agenten; zusammen mit dem Leser sind es neun Akteure, und der Deckel
   der gezeichneten Koerper liegt bei acht. Damit wird er wirklich erreicht,
   und das Instrument muss es sagen, statt still den aeltesten Koerper fallen
   zu lassen.
2. **Zwei Ruhephasen, und warum es zwei sein muessen.** `explorer` liefert sein
   letztes Ereignis 75 Sekunden vor dem Ende, `planner` 85 Sekunden davor.
   Beide sind damit ruhig (ueber eine Minute ohne Ereignis) und trotzdem im
   Bild (unter drei Minuten). Zwei, weil der Deckel bei acht gezeichneten
   Koerpern liegt und den Akteur mit dem aeltesten Ereignis zurueckhaelt: mit
   nur einer Ruhephase waere genau der ruhige Akteur der ungezeichnete, und AC6
   liesse sich an seinem Koerper nicht messen. So traegt `planner` den Deckel
   und `explorer` die Ruhe.
3. **Ein Schreib-Bruch.** `implementer` aendert am Ende fuenfmal in 1.8
   Sekunden dieselbe Stelle (`createUser`, Zeilen 23 bis 36). AC3b verlangt
   daraus GENAU EINE Welle, und der Beweislauf zaehlt sie. Der Bruch steht am
   Ende der Datei, weil die Wiedergabe die Zeitstempel auf die Gegenwart
   schiebt: ein Bruch in der Mitte waere beim Messen laengst vorbei.
4. **Drei Akteure am selben Symbol.** `reviewer` (schreibt), `indexer` (sucht)
   und `tracer` (liest) enden alle drei auf `hotspotScan` in
   `src/repo/db.ts`, Zeilen 39 bis 54. Ihre Bahnradien muessen sich
   unterscheiden und deterministisch aus Art und Kennung folgen; der
   Beweislauf rechnet sie unabhaengig nach.
5. **Ein weiter Weg.** `implementer` beruehrt in der letzten Minute fuenfzehn
   verschiedene Symbole, also mehr als die zehn, die eine Spur traegt. Damit
   ist der Deckel der Spur (6 bis 10 Knoten) wirklich erreicht und nicht nur
   theoretisch.
6. **Viel und wenig Arbeit nebeneinander.** `implementer` liefert in der
   letzten Minute fuenfzehn Ereignisse, `packer` zwei. Der Puls haengt an
   dieser Zahl: einer atmet schnell und kraeftig, einer langsam und schwach,
   `planner` gar nicht. Ohne diesen Abstand waere AC3b nicht messbar, sondern
   nur behauptet.
7. **Ein Pfad ausserhalb des Index** (`package.json`, von `planner`). Er belegt
   weiter den Fall "nicht im Graphen verortbar", der im Instrument stehen
   bleiben muss, statt zu verschwinden.
8. **Eine Selbstauskunft.** Genau ein Ereignis traegt ein `intent`-Feld. Ein
   Ereignis ohne `intent` erzeugt nie eine Absichtszeile, eines mit `intent`
   immer, gekennzeichnet als Selbstauskunft des Agenten.

## Was NICHT darin steht

Kein Dateiinhalt, kein Fortschritt, keine Bewertung. Was ein Ereignis sagt, ist
welches Werkzeug welchen Pfad in welchem Zeilenbereich beruehrt hat.

## Die Zahlen dieser Datei

73 Zeilen, 8 Laeufe, 4 Arten von Arbeit, 1 Schreib-Bruch aus 5 Ereignissen,
2 Ruhephasen (75 und 85 Sekunden), 1 Pfad ausserhalb des Index,
1 Selbstauskunft. Die Datei umspannt 145 Sekunden. Die letzten Ereignisse
liegen, vom Ende her gerechnet, bei 0 (implementer), 3.8 (tracer), 4.4
(indexer), 5.0 (reviewer), 7.0 (packer), 10.0 (checker), 75.0 (explorer) und
85.0 Sekunden (planner). Die beiden ruhigen liegen dabei bewusst nicht weit
auseinander: ein Akteur faellt drei Minuten nach seinem letzten Ereignis ganz
aus dem Bild, und der Beweislauf muss beide vorher gemessen haben.

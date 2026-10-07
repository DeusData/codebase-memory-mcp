# Fremder Code in CodeAtlasWeb

Diese Datei nennt jeden Bestandteil dieses Repositorys, der nicht hier
entstanden ist, sagt woher er kommt, unter welcher Lizenz er steht und was an
ihm geaendert wurde. Sie ist keine Formalie: der Kern der 3D-Ansicht ist
uebernommener Code, und wer das aus den Dateien allein erschliessen muesste,
wuerde es nicht tun.

Kurzfassung: alles unten ist MIT oder Apache-2.0, mit einer Zlib-Ausnahme, und die ist
ausgewiesen (`postprocessing`, Zlib).

## 1. Die Galaxie: DeusData/codebase-memory-mcp, graph-ui

**Lizenz:** MIT License, Copyright (c) 2025 DeusData.
**Quelle:** https://github.com/DeusData/codebase-memory-mcp, Branch
`feat/atlas-r1` (lokaler Clone unter `cbm/`, Stand bda46086),
Verzeichnis `graph-ui/src`.
**Uebernommen am:** 2026-08-28.
**Lizenztext:** vollstaendig im Clone unter `cbm/LICENSE`. Er erlaubt die
Weiterverwendung samt Aenderung und verlangt, dass Copyright-Vermerk und
Lizenztext erhalten bleiben. Genau dafuer traegt jede der sieben Dateien unten
ihren eigenen Kopf mit Lizenz, Copyright, Quelle, Datum und Aenderungsliste;
diese Datei ist die Uebersicht darueber, nicht ihr Ersatz.

Uebernommen wurden sieben Dateien, die zusammen die Szene ergeben. Sie liegen
unter `src/galaxy/`:

| Datei hier | Original in `graph-ui/src` | Was sie tut |
|---|---|---|
| `src/galaxy/GraphScene.tsx` | `components/GraphScene.tsx` | Canvas, Licht, Bloom, OrbitControls, Kamerafahrt, `computeCameraTarget` |
| `src/galaxy/NodeCloud.tsx` | `components/NodeCloud.tsx` | die Knoten als Instanz-Kugeln oder Punkt-Sprites, samt Klick- und Hover-Raycast |
| `src/galaxy/EdgeLines.tsx` | `components/EdgeLines.tsx` | die Kanten als ein additiv gemischtes `lineSegments`, Farbe je Kantentyp |
| `src/galaxy/NodeLabels.tsx` | `components/NodeLabels.tsx` | Namensschilder als Canvas-Texturen auf den groessten Knoten |
| `src/galaxy/HaloLayer.tsx` | `components/HaloLayer.tsx` | Coronas auf den zwoelf groessten Knoten |
| `src/galaxy/types.ts` | `lib/types.ts` | die Datentypen der Layout-Antwort |
| `src/galaxy/density.ts` | `lib/density.ts` | die Dichte-Skalen, die verhindern, dass eine grosse Galaxie zu einem weissen Fleck wird |

**Nicht uebernommen** wurde `components/NodeTooltip.tsx`: die Hover-Karte des
Originals ist mit Tailwind-Klassen gebaut, deren Farben aus einem `@theme`-Block
kommen, den dieses Projekt nicht hat. An ihrer Stelle steht
`src/galaxy/NodeTooltipCard.tsx`, eigener Code in den Tokens dieses Projekts
(`src/styles/tokens.css`), eingehaengt ueber die `renderTooltip`-Prop, die das
Original selbst vorsieht.

**Die Aenderungen an den uebernommenen Dateien**, alle auch im Kopf der
jeweiligen Datei:

1. `GraphScene.tsx`: der Idle-Beobachter griff den Canvas mit
   `document.querySelector("canvas")` und damit den ersten im ganzen Dokument.
   In dieser Anwendung steht Monaco daneben, also greift er jetzt seinen
   eigenen ueber `useThree().gl.domElement`.
2. `GraphScene.tsx`: `sceneRadius` lief bei jedem Render ueber alle Knoten und
   liegt jetzt in `useMemo` an den Knoten.
3. `GraphScene.tsx`: die Zweige fuer `linked_projects` (Satelliten-Galaxien
   fremder Projekte) und `missed` (Geister-Cluster nicht indizierter Dateien)
   sind entfernt, samt der zugehoerigen Props und Typen.
4. `GraphScene.tsx`: `ApproachWatcher` (Semantic Zoom) und `ViewTargetReporter`
   (Minimap-Feed) sind entfernt, samt der Props `onApproachNode` und
   `onViewTarget`. Beide bedienen Panels, die dieses Projekt nicht uebernimmt.
5. `GraphScene.tsx`: der Import von `NodeTooltip` ist entfernt, siehe oben.
6. `types.ts`: nur die vier Typen der Szene sind geblieben.
7. `density.ts`: die localStorage-Funktionen sind entfernt; die Skalen selbst
   sind unveraendert.
8. Alle sieben: Importpfade, Formatierung (4 Leerzeichen, einfache
   Anfuehrungszeichen) und die langen Gedankenstriche in den Kommentaren. Kein
   Verhalten haengt daran.

Was NICHT geaendert wurde, ist die Rechnung: Farbgebung, Skalierung,
Kantenintensitaet, Kameraziel und Bloom sind Zeile fuer Zeile die des
Originals. Eine eigene Kurve waere eine zweite Wahrheit ueber dasselbe Bild.

## 2. Die 3D-Bibliotheken

Diese Pakete kommen ueber `package.json` und liegen nicht im Repository. Sie
sind exakt gepinnt, weil die uebernommene Szene gegen genau diese Fassungen
geschrieben ist (dieselben Fassungen, die `graph-ui/package-lock.json` fuehrt).

| Paket | Fassung | Lizenz | wofuer |
|---|---|---|---|
| `three` | 0.183.2 | MIT | die 3D-Bibliothek selbst |
| `@react-three/fiber` | 9.5.0 | MIT | `Canvas`, `useThree`, `useFrame` |
| `@react-three/drei` | 10.7.7 | MIT | nur `OrbitControls` und `Html` |
| `@react-three/postprocessing` | 3.0.4 | MIT | `EffectComposer`, `Bloom` |
| `postprocessing` | 6.38.3 | **Zlib** | Peer der Zeile darueber |
| `three-stdlib` | 2.36.1 | MIT | nur der Typ von `OrbitControls` (devDependency) |
| `@types/three` | 0.183.1 | MIT | Typen (devDependency) |

Die Zlib-Lizenz von `postprocessing` ist die einzige Nicht-MIT-Lizenz in dieser
Liste. Sie ist ebenfalls freizuegig und verlangt im Wesentlichen, die Herkunft
nicht falsch darzustellen und geaenderte Fassungen als geaendert zu kennzeichnen.
Dieses Projekt aendert das Paket nicht, es benutzt es.

Die uebrigen Abhaengigkeiten des Projekts stehen unter MIT (React, Vite,
Vitest, Monaco) beziehungsweise Apache-2.0 (TypeScript, Playwright;
Korrektur 2026-08-29, Audit-Befund 7) und sind kein uebernommener Code, sondern
Werkzeuge und Bibliotheken in ihrer veroeffentlichten Form.

## 3. Portierungen aus CodeAtlasIDE

`~/Desktop/CodeAtlasIDE` ist ein Projekt desselben Urhebers (Bernhard
Jackiewicz) und keine fremde Quelle; die Module, die von dort kommen, tragen
ihre Herkunftsnotiz im Dateikopf (Provider-Schicht, Semantic IR, Twin,
`src/search/semantic-search.ts`). Sie stehen hier nur der Vollstaendigkeit
halber: lizenzrechtlich ist daran nichts zu regeln.

## 4. Uebernommene Idee: der Topsort-Rundgang

`src/tours/tour-generator.ts` ist eine Portierung aus CodeAtlasIDE (Abschnitt 3)
und traegt dort wie hier eine zweite, schwaechere Herkunftsangabe im Kopf: die
Idee, einen Einstiegs-Rundgang aus der Struktur eines Projekts abzuleiten statt
ihn von einem Modell schreiben zu lassen, stammt dem Konzept nach von
Understand-Anything (MIT). Uebernommen ist der Gedanke, kein Quelltext: der
Kahn-Lauf, die Rollenworte, die Saetze und der Zyklusbruch sind eigener Code.
Die Angabe steht hier, weil eine Konzept-Anleihe nichts ist, was man erst dann
nennt, wenn jemand fragt.

## 5. Optionale lokale Quelltext-Erklaerung

Die Browser-Laufzeit wird lokal mit dem Frontend ausgeliefert; Modellgewichte
sind nicht enthalten. Ein Download erfolgt erst nach ausdruecklicher
Aktivierung im Browser. Der Quelltext bleibt fuer die Erklaerung im Browser.

| Bestandteil | Fassung | Lizenz | Verwendung |
|---|---|---|---|
| `@huggingface/transformers` | 4.2.0 | Apache-2.0 | Textgenerierung und Chat-Tokenizer im Web Worker |
| `onnxruntime-web` | 1.26.0-dev.20260416-b7804b056c | MIT | WebGPU-Laufzeit; MJS und WASM werden als lokale Assets gepackt |
| `sharp` | 0.35.0 | Apache-2.0 | Transitive Node-Abhaengigkeit mit gezieltem Override; nicht Teil der Browser-Laufzeit |

Das optionale Modell ist
[`onnx-community/Qwen2.5-Coder-0.5B-Instruct`](https://huggingface.co/onnx-community/Qwen2.5-Coder-0.5B-Instruct),
festgelegt auf Revision `f0292f665fd307846ff3c318a91a1bc29d091492`
und Variante `q4f16`. Die gewaehlte ONNX-Datei umfasst 554.935.833 Byte;
mit Konfiguration und Tokenizer sind es 566.366.194 Byte. Der in den
Hugging-Face-Metadaten angegebene SHA-256 der Gewichte ist
`60c076ac0d3910881fe0cad75997e803459a5f71bb74bcbcd82407ef83db7ba0`;
die Laufzeit berechnet diesen Hash nicht erneut.

Die Modellkarte der ONNX-Konvertierung nennt keine eigene Lizenz.
Das offizielle Basismodell
[`Qwen/Qwen2.5-Coder-0.5B-Instruct`](https://huggingface.co/Qwen/Qwen2.5-Coder-0.5B-Instruct)
ist unter Apache-2.0 veroeffentlicht. Der Download verwendet ausschliesslich
`huggingface.co` und dessen fuer diese Revision geprueften Weiterleitungs-Host
`us.aws.cdn.hf.co`. Remote-Skripte werden nicht geladen.

Weitere Browser-Modelle (Metadaten und Download-Hosts am 2026-09-08 geprueft):

| Modell | Feste Revision | Download inkl. Tokenizer/Konfiguration | Lizenz |
|---|---|---|---|
| `onnx-community/Qwen3-0.6B-ONNX` | `da1453100cf3ff33ef56d17983fc7a8648706db6` | 578.917.626 Byte | Apache-2.0 |
| `LiquidAI/LFM2.5-1.2B-Instruct-ONNX` | `10f72e70abf67ac0fd7ebf15bc5854726891d864` | 763.763.755 Byte | Liquid AI License; kommerzielle Bedingungen oberhalb 10 Mio. USD Jahresumsatz pruefen |
| `onnx-community/Qwen3.5-2B-ONNX-OPT` | `2ea7886f48b926aca97de8b0e041ffca7e3ebaa9` | 1.402.850.762 Byte | Apache-2.0 |

Alle Varianten verwenden `q4f16`. Fuer Qwen3.5 werden nur Decoder und
Text-Embeddings geladen; Vision-Dateien sind nicht in der Download-Freigabe.
Die vollstaendigen Dateilisten und veroeffentlichten LFS-Hashes stehen in
`src/browser-ai/model-policy.ts`. Hashes werden nicht erneut berechnet.
Die angegebenen Bytes sind keine Aussage ueber GPU-Speicherbedarf.

Die Modellarchitekturen werden von Transformers.js 4.2.0 unterstuetzt. Das
ist kein Benchmark oder Kompatibilitaetsnachweis fuer jedes Browser/GPU-Paar.
Es wurden fuer diese Aenderung keine Modellgewichte heruntergeladen und
keine Inferenz-Benchmarks ausgefuehrt. Der Browser begrenzt den gesamten
Kontext auf 8192 Token inklusive 512 reservierter Antwort-Token; gezaehlt
wird mit dem gewaehlten Chat-Template und Tokenizer. Eingaben werden nicht
abgeschnitten. Qwen3 und Qwen3.5 laufen standardmaessig ohne Thinking.

## 6. Markdown-Darstellung im lokalen Chat

Die Chat-Darstellung verwendet die folgenden unveraenderten npm-Pakete.
Die Lizenztexte stammen aus der jeweiligen installierten Datei `license`.

| Paket | Version | Lizenz | Copyright |
| --- | --- | --- | --- |
| react-markdown | 10.1.0 | MIT | Espen Hovlandsdal |
| remark-gfm | 4.0.1 | MIT | Titus Wormer |

### react-markdown

```text
The MIT License (MIT)

Copyright (c) Espen Hovlandsdal

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### remark-gfm

```text
(The MIT License)

Copyright (c) Titus Wormer <tituswormer@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```

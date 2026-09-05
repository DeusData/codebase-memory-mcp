# ADR 0001: Lokale Modellklassen fuer CodeAtlasWeb (Stand August 2026)

Status: Kandidaten festgelegt per Web-Recherche 2026-08-28; endgueltige
Besetzung entscheidet die eigene Eval-Suite (Head-to-Head, siehe unten).
Die vollstaendige Recherche mit allen Quellen liegt in
verification/w5/modellrecherche.md (36 nummerierte Quellen).

## Kontext

PLAN §5: zwei lokale Modellklassen (~1B und ~4B) fuer kurze, extraktive,
zitierende Antworten ueber Fakten-Karten des Context-Compilers; Runtime
llama.cpp llama-server auf Apple Silicon, GGUF Q4_K_M, Kontextbudget 3k
(Klasse A) und 8k (Klasse B) Token; Kriterienreihenfolge laut Plan:
(1) Instruction-Following/Verstaendnis, (2) kommerzielle Lizenz,
(3) GGUF-Qualitaet, (4) Tempo auf Apple Silicon, (5) Deutsch+Englisch.
Modellwahl per Recherche, nicht per Vorurteil (Nutzeranweisung 2026-08-28);
der Platzhalter Qwen3 aus der Planungsphase ist damit abgeloest.

## Lage August 2026 (Kurzfassung)

- Qwen3.5-Small (Maerz 2026, 0.8B/2B/4B/9B, Apache 2.0) und Gemma 4
  (April 2026, erstmals Apache 2.0) haben die Klasse umgewaelzt.
- Explizit geprueft und NICHT existent: SmolLM4, Phi-5 (nur Geruechte).
  Llama 3.2 ist fachlich ueberholt und lizenzseitig ohne Not.
- llama.cpp: Qwen3.5 braucht einen aktuellen Build (hybride
  Gated-DeltaNet-Attention); MTP-Speedup seit 2026-05-16 im Main.
  Konsequenz: llama-server b10675 aus Quellen gebaut (Metal, RPC aus),
  Lade-Smoke mit Qwen3.5-2B erfolgreich.

## Entscheidung: Kandidatenpaare fuer die Eval

Klasse A (~3k Budget), vier Kandidaten:
1. Qwen3.5-2B (Apache 2.0, Q4_K_M 1.28GB (models/, 1280835840 Bytes), IFEval 78.6 Thinking/61.2
   Non-Thinking, MMLU-Redux 79.6, 201 Sprachen)
2. LFM2.5-1.2B-Instruct (LFM Open License v1.0: frei unter 10 Mio. USD
   Jahresumsatz; klassenbestes IFEval 86.23; Q4_K_M 0.73GB (730895168 Bytes))
3. MiniCPM5-1B (NUTZERNOMINIERUNG Bernhard 2026-08-28, verifiziert
   2026-08-28: OpenBMB, Release 2026-05-19, Apache 2.0, LlamaForCausalLM
   nativ in llama.cpp, offizielles GGUF Q4_K_M 0.69GB (688065920 Bytes), IFEval 80.4,
   MMLU-Redux 70.1, Kontext 131k. RISIKO: offiziell nur EN/ZH,
   Deutsch unbelegt und anekdotisch schwach; die deutschen
   Eval-Fragen entscheiden das empirisch.)
4. Qwen2.5-Coder-1.5B-Instruct (NUTZERNOMINIERUNG Bernhard 2026-08-28:
   Code-Spezialist. Apache 2.0, offizielles GGUF Q4_K_M 1.12GB (1117320768 Bytes),
   Qwen2-Architektur nativ in llama.cpp, Kontext 32k. Aeltere
   Generation (2024) mit schwaecherem generellem Instruction-Following
   als die 2026er-Kandidaten; die Hypothese "Code-Spezialisierung
   hilft beim Formulieren aus Code-Fakten-Karten" wird empirisch
   gemessen statt verworfen.)
Fallback bei Lizenz-Aus: Granite 4.0-1B (Apache 2.0, IFEval 78.5).

Klasse B (~8k Budget):
1. Qwen3.5-4B (Apache 2.0, IFEval 89.8, MMLU-Pro 79.1, Q4_K_M 2.83GB (2834975040 Bytes), MTP)
2. Gemma 4 E4B-it (Apache 2.0 seit Gemma 4, MMLU-Pro 69.4, MMMLU 76.6,
   Q4_K_M 4.98GB (4977171584 Bytes), offizielle QAT-Quants)
Fallback bei Tempo-Aus des E4B: Ministral 3 3B Instruct 2512 (Apache 2.0,
2.15GB).

## Lizenz-Notizen

Apache 2.0: Qwen3.5, Gemma 4, Ministral 3, Granite. MIT: Phi-4-mini.
LFM Open License v1.0: kommerziell frei nur unter 10 Mio. USD Umsatz;
fuer die aktuelle Situation ausreichend, aber strategisches Risiko bei
Buendelung/Weiterverkauf: dokumentiert, nie Default ohne Eval-Sieg UND
bewusste Lizenzabnahme.

## Bekannte Risiken (in der Eval zu messen)

- Qwen3.5 Thinking-Modus: hoher Token-Verbrauch und dokumentierte
  Halluzinationsneigung ohne Kontext; unser Betrieb ist Non-Thinking mit
  Fakten-Karten; die Zitiertreue-Metrik (jede Aussage traegt [K]) ist der
  harte Filter.
- Fuer fast alle Kandidaten fehlen seriöse tok/s-Belege auf M-Serie
  (nur LFM2.5-2.6B: 220 tok/s auf M5 Max); Tempo misst die Eval selbst.
- Gemma 4 E4B: 8B Gesamtparameter, groesster Speicher der Klasse.

## Engine-Entscheidung (Recherche 2026-08-28, Martins Pruefauftrag)

Gefragt war (Martin, Voicemail 2): eine leichtgewichtige JS-/Browser-
Inferenz-Engine statt des llama.cpp-Sidecars. Benchmark-basierte
Antwort (Volltext mit Quellen: verification/w5/modellrecherche.md, Teil 2):

- ENTSCHEIDUNG: llama-server (Metal, 127.0.0.1:4141) bleibt der
  Referenzpfad. Gleiche Maschine, gleiches Modell: nativ ~1.5x beim
  Decode und ueber 2x beim Prefill schneller als der beste Browser-Weg
  (ggml-WebGPU-Paper 2026); unser Workload (Karten im Prompt, kurze
  Antworten) ist Prefill-dominiert. Dazu: GGUFs direkt nutzbar, kein
  2GB-Split fuers 4B-Modell, Prozess-Robustheit (Tab-Crash reisst
  nichts mit), triviale Air-gap-Story.
- ZUKUNFTSOPTION mit klarem Kandidaten: wllama mit dem offiziellen
  ggml-WebGPU-Backend (gleiche GGUFs, gleiche ggml-Basis, ~52 tok/s
  Decode 1B q4 auf M3 im Browser belegt). Neubewertungs-Trigger: die
  Prefill-Luecke schliesst sich UND 4B-Handling (Split + Buffer-Limits)
  ist im Release-Stand stabil. WebLLM (Konvertierungszwang, duenne
  Release-Kadenz), transformers.js (dokumentierte WebGPU-Korrektheits-
  bugs in der 1B-Klasse), MediaPipe (maintenance-only) und WebNN
  (Origin Trial) scheiden heute aus.
- Damit der Wechsel jederzeit billig bleibt: der Chat-Transport ist
  eine OpenAI-kompatible Abstraktion, die Engine ist Konfiguration.

## Konsequenz

Die Eval-Suite (44 goldene Fragen gegen das Demo-Fixture, Temperatur 0,
fester Seed, Zitat-Pflicht) faehrt alle Kandidaten je Klasse
GEGENEINANDER; der Eval-Sieger schlaegt den Benchmark-Ruf. Ergebnis wird
in diesem ADR nachgetragen (Abschnitt "Eval-Ergebnis", W5).

Downloads: einmalig am 2026-08-28 dokumentiert (SHA256SUMS in
models/SHA256SUMS), einziger Netzzugriff des Projekts neben npm/Clone
zur Bauzeit; danach air-gapped.

## Eval-Ergebnis (2026-08-29, Head-to-Head, bindend)

44 goldene Fragen (23 deutsch), Temperatur 0, Seed 42; Werte aus dem
aufgezeichneten Lauf (verification/w5/eval.json). Ein zweiter kompletter
Lauf reproduzierte passRate und Zitattreue laut Implementierungsbericht
exakt, ist aber NICHT als eigenes Artefakt aufgezeichnet (Korrektur
2026-08-29 nach Audit-Befund 3):

| Modell | Klasse | passRate | Zitattreue | tok/s |
|---|---|---|---|---|
| Qwen3.5-2B | A | 0.682 | 0.932 | 86 |
| LFM2.5-1.2B | A | 0.295 | 0.432 | 110 |
| MiniCPM5-1B (Nominierung) | A | 0.250 | 0.545 | 171 |
| Qwen2.5-Coder-1.5B (Nominierung) | A | 0.227 | 0.705 | 84 |
| Qwen3.5-4B | B | 0.818 | 0.955 | 38 |
| gemma-4-E4B | B | 0.841 | 1.000 | 32 |

ENTSCHEIDUNG: Klasse A = Qwen3.5-2B, Klasse B = gemma-4-E4B (beide ueber
den harten Grenzen 0.6/0.9). Die Nominierungen wurden vollstaendig
mitgefahren; MiniCPM5-1B ist das schnellste Modell des Feldes, faellt
aber bei Instruktionstreue und Zitierdisziplin ab; die Code-Spezialisten-
Hypothese des Coders bestaetigte sich nicht. Sechs dokumentierte
Compiler-Iterationen (eval.json extras.iterations), keine Frage wurde
entschaerft; der groesste Einzelsprung war, die Kartennummern selbst in
Zitat-Syntax [K1] zu setzen.

## Quellen der Nutzernominierungen (Nachtrag 2026-08-29, Audit-Befund 4)

Die zwei Nominierungen stehen nicht in verification/w5/modellrecherche.md
(die dokumentiert die urspruengliche Vier-Kandidaten-Recherche); ihre
Verifikation lief separat am 2026-08-28:
- MiniCPM5-1B: https://huggingface.co/openbmb/MiniCPM5-1B (Modellkarte:
  1.08B, Apache-2.0, LlamaForCausalLM, EN/ZH),
  https://huggingface.co/openbmb/MiniCPM5-1B-GGUF (Q4_K_M),
  https://github.com/OpenBMB/MiniCPM (Release 2026-05-19),
  https://benchlm.ai/models/minicpm5-1b (IFEval 80.4, MMLU-Redux 70.1).
  Deutsch offiziell unbelegt; die deutschen Eval-Fragen massen es.
- Qwen2.5-Coder-1.5B-Instruct:
  https://huggingface.co/Qwen/Qwen2.5-Coder-1.5B-Instruct-GGUF
  (offizielles GGUF, Apache-2.0, Qwen2-Architektur, 2024er-Generation).

## Nachtrag W10 (2026-08-29): entschieden wurde die Engine, nicht ein Modell

Dieser Nachtrag nimmt nichts von dem zurueck, was oben steht. Er sagt, was
oben eine Entscheidung war und was ein Messergebnis, weil beides bisher im
selben Abschnitt stand und darum gleich verbindlich aussah.

**Ausloeser.** Martin, 2026-08-29: "Fuer mich waere wichtig, dass das Modell
das Binary nicht gross verseucht. Man bietet in der UI das Feature so an,
dass man sich von zum Beispiel Hugging Face selber ein Modell aussuchen
kann, das wird dann im local cache gedownloaded. Dann sitzt das nicht im
Binary, und dann auch easy mit groesseren LLMs. Kann der Nutzer selber
konfigurieren. Also quasi im Settings-Menue." Der Nutzer stimmt zu:
"kannst qwen drin lassen, aber das muss auch noch geaendert werden".

**Was bindend bleibt.** Die Engine. llama-server auf 127.0.0.1 bleibt der
Referenzpfad, aus den oben gemessenen Gruenden (Prefill-Vorsprung, GGUF
direkt, kein 2GB-Split, Prozess-Robustheit, Air-gap). Der Chat-Transport
bleibt die OpenAI-kompatible Abstraktion, die Engine bleibt Konfiguration.
Die Neubewertungs-Trigger fuer wllama gelten unveraendert.

**Was ab hier Empfehlung ist und nicht Festlegung.** Die Zeile
"ENTSCHEIDUNG: Klasse A = Qwen3.5-2B, Klasse B = gemma-4-E4B" im Abschnitt
"Eval-Ergebnis". Sie war eine Entscheidung, solange das Programm ein Modell
mitbrachte. Das tut es nicht mehr. Die sechs Zeilen der Eval-Tabelle sind
weiterhin gemessen, weiterhin reproduzierbar und weiterhin das Beste, was
dieses Projekt ueber diese sechs Dateien sagen kann; sie sind ab jetzt die
Auskunft, die der Leser bekommt, wenn er im Einstellungen-Panel zwischen
Vorschlaegen waehlt. Sie sind nicht mehr die Antwort auf die Frage, welches
Modell laeuft. Diese Frage beantwortet der Leser.

**Warum das kein Rueckzieher ist.** Die Eval hat gemessen, was sie messen
konnte: sechs Dateien, 44 Fragen, eine Maschine, ein Tag. Sie hat nie
gemessen, was ein 9B-Modell auf einer groesseren Maschine tut oder was ein
Modell tut, das es im August 2026 noch nicht gab. Eine Zahl aus einem
Head-to-Head zur Produkteigenschaft zu machen hiesse, die Stichprobe fuer
die Welt zu halten. Als Empfehlung mit sichtbarer Herkunft sagt dieselbe
Zahl genau so viel, wie sie deckt.

**Was daraus folgt (umgesetzt in W10).** llm/start.sh faehrt in der Vorgabe
den Router-Modus ueber ein Cache-Verzeichnis (`--models-dir`, `--models-max`,
`--models-autoload`, gemessen an vendor/llama, llama-server 0.3.0-dev, build
b1-90c26fc) und hat keinen fest verdrahteten Dateinamen mehr in seiner
Vorgabe. Die sechs Wahlen `class-a` bis `class-b-gemma` bleiben als
Reproduktionsgriffe dieser Eval bestehen, damit die Laeufe von W5 und die
Regressionspruefung in tools/eval-check.mjs wiederholbar bleiben. Sie sind
Belege, keine Vorgabe.

**Grenze, die dabei nicht verschoben wurde.** Die Oberflaeche laedt nichts
herunter, weil sie es nicht kann: eine SPA ohne eigenes Backend startet
keinen Prozess und schreibt nichts auf die Platte. Sie waehlt aus, was im
Cache liegt, und stellt fuer alles andere den fertigen `-hf`-Aufruf bereit,
mit dem Satz dazu, dass er ins Netz geht und wohin er laedt. Ein
Fortschrittsbalken fuer einen Download, den ein anderer Prozess faehrt,
waere eine Anzeige ohne Messung.

**Offen geblieben.** Ob ein groesseres Modell (9B und darueber) auf dieser
Maschine im Budget dieses Produkts brauchbar ist, hat niemand gemessen.
Das Panel behauptet darueber nichts; es zeigt zu den sechs gemessenen
Kandidaten die gemessenen Zahlen und zu allem anderen nur den Weg, es
selbst zu probieren.

## Korrektur-Notizen (2026-08-29, nach dem unabhaengigen Audit)

- Groessenangaben oben auf die tatsaechlichen Dateien in models/
  umgestellt (Dezimal-GB + exakte Bytes; die fruehere Mischung aus
  HF-MiB-Angaben und Dezimal-GB war inkonsistent, Audit-Befund 5).
- Die harten Eval-Grenzen (Sieger je Klasse: passRate >= 0.6, Zitattreue
  >= 0.9) sind seit dem Finding-Fix in tools/eval-llm.mjs als
  erzwungenes Gate hinterlegt und in eval.json dokumentiert
  (Audit-Befund 6); tools/eval-check.mjs prueft die Sieger als
  Regressionslauf im Release-Gate (Audit-Befund 9).
- Der Lade-Smoke in verification/w5/models.json testet MiniCPM5-1B als
  kleinstes Modell; der Klasse-A-Sieger Qwen3.5-2B ist durch den
  Chat-Smoke und die Eval selbst belegt (Klarstellung zu Befund 18).

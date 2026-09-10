# Lokale LLM-Kandidaten fuer CodeAtlasWeb, Stand August 2026

**Wichtigste Lage-Aenderungen seit 2025:** Die Small-Model-Landschaft wurde 2026 zweimal umgewaelzt: Qwen3.5-Small (Maerz 2026, 0.8B/2B/4B/9B, Apache 2.0) [1][2] und Gemma 4 (April 2026, erstmals unter Apache 2.0 statt Gemma-Terms) [7][8]. Dazu kamen Ministral 3 (Dez 2025, Apache 2.0) [21], LFM2.5 (Jan-Aug 2026) [13][14] und Granite 4.1 (April 2026) [26][27].

**Geprueft und NICHT existent bzw. nicht relevant (Stand Aug 2026):**
- **SmolLM4: existiert nicht.** Neuestes Modell der Reihe bleibt SmolLM3-3B (Juli 2025) [31].
- **Phi-5: nicht offiziell released.** Nur SEO-/Geruechteseiten; Microsofts letztes Small-Phi bleibt Phi-4-mini (3.8B, Feb 2025), das neueste Phi ueberhaupt ist Phi-4-reasoning-vision-15B (Maerz 2026, zu gross) [30].
- **Meta: nichts Neues im Small-Bereich.** Llama 3.2 1B/3B (2024) ist weiter der Stand, Llama-Community-Lizenz, in Benchmarks von allen 2026er-Kandidaten ueberholt.
- **Qwen3.6/3.8 (April-Aug 2026): nur grosse offene Modelle** (27B, 35B-A3B, Max-Klasse). Die kleine offene Familie bleibt Qwen3.5 [33-Anm., 38].

**Wichtiger llama.cpp-Hinweis:** Qwen3.5-Small nutzt hybride Gated-DeltaNet-Attention; es braucht einen aktuellen llama.cpp-Build. Multi-Token-Prediction (MTP) fuer ~1.5-2x schnellere Generierung ist seit 16.05.2026 im llama.cpp-Main-Branch [5].

## Klasse A (0.5-2B)

| Modell | Params | Lizenz | IF / Verstaendnis (belegt) | Kontext | GGUF Q4_K_M | Deutsch | Apple-Silicon-Tempo |
|---|---|---|---|---|---|---|---|
| **Qwen3.5-2B** (Maerz 2026) | 2B | Apache 2.0 | IFEval 78.6 (Thinking) / 61.2 (Non-Thinking), MMLU-Redux 79.6/69.2, MMLU-Pro 66.5/55.3 [4] | 262k | `unsloth/Qwen3.5-2B-GGUF`, 1.28 GB [6] | 201 Sprachen; MMMLU 56.9, WMT24++ 45.8 [4] | nicht belegt (MTP-Speedup belegt [5]) |
| **LFM2.5-1.2B-Instruct** (Jan 2026) | 1.2B | LFM Open License v1.0 (frei < 10 Mio. USD Umsatz) [14] | IFEval 86.23, IFBench 47.33, MMLU-Pro 44.35 [13] | 128k (Familienangabe) [15] | `LiquidAI/LFM2.5-1.2B-Instruct-GGUF`, 731 MB [16] | Familie: 16 Sprachen inkl. Deutsch [15] | keine Mac-Zahl belegt; CPU (Ryzen HX370, Q4_0): 116 tok/s Decode [13]; MLX-Checkpoints vorhanden |
| **Granite 4.0-1B** (Okt 2025) | 1.6B (H-Variante ~1.5B) [18][17] | Apache 2.0, ISO-42001-zertifiziert [20] | IFEval 78.53 (H-1B: 78.5, vs. Qwen3-1.7B 73.1), MMLU 59.74 [18][20] | 128k [18] | `ibm-granite/granite-4.0-1b-GGUF`, 1.02 GB; Hinweis auf FP32-Praezisionsproblem in kleinen Quants [19] | 12 Sprachen inkl. Deutsch; MMMLU 45 [18] | nicht belegt |
| **Gemma 4 E2B-it** (April 2026) | 5.1B total / 2.3B effektiv [8] | Apache 2.0 [7][8] | IFEval nicht belegt; MMLU-Pro 60.0, GPQA 43.4 [8] | 128k | `unsloth/gemma-4-E2B-it-GGUF`, 3.11 GB (Ausreisser: 3x so gross wie echte 2B) [10] | 140+ Sprachen, Deutsch out-of-the-box; MMMLU 67.4 [8] | nicht belegt |
| **Qwen3.5-0.8B** (Maerz 2026) | 0.8B | Apache 2.0 | AA Intelligence Index 9 (vs. Qwen3-0.6B: 6.5); IFEval nicht belegt [2] | 262k | GGUF ueber Unsloth/Community; Groesse nicht belegt | 201 Sprachen (Familienangabe) [3] | nicht belegt |

**Ranking Klasse A** (nach Kriterienreihenfolge IF > Lizenz > GGUF > Tempo > De/En): 1. Qwen3.5-2B, 2. LFM2.5-1.2B, 3. Granite 4.0-1B, 4. Gemma 4 E2B, 5. Qwen3.5-0.8B. LFM2.5-1.2B hat das beste belegte IFEval der Klasse (86.23), faellt aber beim Lizenzkriterium hinter die Apache-Modelle zurueck. Gemma 4 E2B sprengt mit 3.11 GB das Klassenbudget.

## Klasse B (3-5B)

| Modell | Params | Lizenz | IF / Verstaendnis (belegt) | Kontext | GGUF Q4_K_M | Deutsch | Apple-Silicon-Tempo |
|---|---|---|---|---|---|---|---|
| **Qwen3.5-4B** (Maerz 2026) | 4B (Vision-Language) | Apache 2.0 | IFEval 89.8, MMLU-Pro 79.1, MMLU-Redux 88.8; AA-Index 27 (vs. Qwen3-4B-2507: 18) [3][2] | 262k nativ (bis 1M) [3] | `unsloth/Qwen3.5-4B-MTP-GGUF`, 2.83 GB [5] | 201 Sprachen; MMMLU 76.1, WMT24++ 66.6 [3] | nicht belegt; MTP-Speculative-Decoding ~1.5-2x [5] |
| **Gemma 4 E4B-it** (April 2026) | 8B total / 4.5B effektiv [8] | Apache 2.0 | MMLU-Pro 69.4, GPQA 58.6, MMMLU 76.6 [8]; IFEval 84.66 strict-P (Community-Messung, nicht offiziell) [12] | 128k | `unsloth/gemma-4-E4B-it-GGUF`, 4.98 GB; offizielle QAT-Quants vorhanden [9][11] | 140+ Sprachen, Deutsch out-of-the-box [8] | nicht belegt |
| **Ministral 3 3B Instruct 2512** (Dez 2025) | 3.4B LM + 0.4B Vision-Encoder [23] | Apache 2.0 [21][22] | IFEval nicht belegt; MATH 83.0, Wild Bench 56.8, Arena Hard 30.5 [24]; laut Mistral sehr token-effizient [21] | 256k [22] | `unsloth/Ministral-3-3B-Instruct-2512-GGUF`, 2.15 GB [23] | Deutsch explizit gelistet; Familie "40+ Sprachen" [23][21] | nicht belegt |
| **Granite 4.1-3B** (29.04.2026) | 3B dense | Apache 2.0 [26][27] | IFEval nicht belegt; AA-Index 9 (Non-Reasoning), extrem token-effizient: 2.7M Output-Tokens fuer den AA-Testlauf vs. 78M bei Qwen3.5-9B [26] | 512k [27] | `ibm-granite/granite-4.1-3b-GGUF` (Groesse nicht belegt) [28] | 4.0-Familie: 12 Sprachen inkl. Deutsch [18]; fuer 4.1 nicht separat belegt | nicht belegt |
| **LFM2.5-2.6B** (04.08.2026, Grenzfall 2.6B) | 2.69B, reines Reasoning-Modell (denkt immer vor) [15] | LFM Open License v1.0 (frei < 10 Mio. USD Umsatz) [14] | IFBench 59.17, IFStruct 85.49, ToolSandbox 77.83 (vs. Qwen3.5-9B: 76.44) [15][14] | 128k [15] | `LiquidAI/LFM2.5-2.6B-GGUF` inkl. QAD-Q4_0 (< 2.5 GB RAM) [14] | 16 Sprachen inkl. Deutsch [15] | **220 tok/s auf Apple M5 Max (belegt)** [14] |
| **Phi-4-mini-instruct** (Feb 2025, Alt-Kandidat) | 3.8B | MIT | IFEval-Zahl nicht oeffentlich belegt; MMLU 67.3, MMLU-Pro 52.8 [29] | 128k | Community-GGUFs (bartowski/unsloth), Groesse hier nicht verifiziert | 22 Sprachen inkl. Deutsch [29] | nicht belegt |
| **SmolLM3-3B** (Juli 2025, Alt-Kandidat) | 3B | Apache 2.0 [32] | IFEval nicht belegt; schlaegt Llama-3.2-3B und Qwen2.5-3B [31] | 64k (128k via YaRN) [31] | Community-GGUFs, Groesse hier nicht verifiziert | 6 Sprachen inkl. Deutsch [31] | nicht belegt |

**Ranking Klasse B:** 1. Qwen3.5-4B, 2. Gemma 4 E4B, 3. Ministral 3 3B, 4. LFM2.5-2.6B, 5. Granite 4.1-3B, dann Phi-4-mini und SmolLM3 als ueberholte Alt-Kandidaten. Qwen3.5-4B fuehrt alle belegten IF-/Verstaendnis-Benchmarks deutlich an. Zwei dokumentierte Schwaechen: hoher Token-Verbrauch im Thinking-Modus und 80-82% Halluzinationsrate auf AA-Omniscience (Wissensabfrage ohne Kontext) [2]; fuer den CodeAtlasWeb-Anwendungsfall (Context-Compiler liefert Fakten, Modell formuliert nur, Non-Thinking-Betrieb) ist beides beherrschbar, gehoert aber in die Zitiertreue-Evaluation.

## Lizenz-Notizen

- **Apache 2.0, kommerziell unproblematisch:** Qwen3.5 (alle Groessen) [3][4], **Gemma 4 (Neuerung 2026: Apache 2.0 statt Gemma-Terms; gilt fuer die 4er-Generation, Gemma 3 bleibt unter Gemma-Terms)** [7][8], Ministral 3 (alle Groessen inkl. 3B) [21][22], Granite 4.0/4.1 (zusaetzlich ISO 42001 und signierte Checkpoints) [20][26], SmolLM3 [32].
- **MIT:** Phi-4-mini [29].
- **LFM Open License v1.0 (LiquidAI):** kommerziell frei nur unter 10 Mio. USD Jahresumsatz, darueber Enterprise-Vertrag noetig [14]. Fuer die aktuelle GmbH-Situation formal ausreichend, aber ein strategisches Risiko, falls CodeAtlasWeb gebuendelt/weiterverkauft wird: dokumentieren, nicht als Default setzen.
- **Llama-Community-Lizenz** (Llama 3.2): kein technischer Grund mehr, sie in Kauf zu nehmen.

## Empfehlung Top-2 je Klasse

**Klasse A (~3k Token Budget):**
1. **Qwen3.5-2B**: bestes Gesamtpaket aus Verstaendnis (MMLU-Redux 79.6), solidem IFEval, Apache 2.0, 201 Sprachen und kleinem Q4_K_M (1.28 GB). Bezug: `unsloth/Qwen3.5-2B-GGUF` [6]. Caveat: Fuer kurze extraktive Antworten im Non-Thinking-Modus faellt IFEval auf 61.2; im Eval beide Modi testen.
2. **LFM2.5-1.2B-Instruct**: klassenbestes belegtes IFEval (86.23) bei nur 731 MB, GGUF direkt vom Hersteller mit Day-One-llama.cpp-Support, Deutsch in der Sprachliste. Bezug: `LiquidAI/LFM2.5-1.2B-Instruct-GGUF` [16]. Caveat: Lizenz-Umsatzschwelle; schwaches Weltwissen (MMLU-Pro 44) ist beim Fakten-Karten-Ansatz zweitrangig.
- Fallback, falls die LFM-Lizenz ausscheidet: `ibm-granite/granite-4.0-1b-GGUF` (Apache 2.0, IFEval 78.5) [19][18].

**Klasse B (~8k Token Budget):**
1. **Qwen3.5-4B**: klarer Klassenprimus bei Instruction-Following (IFEval 89.8) und Verstaendnis (MMLU-Pro 79.1), Apache 2.0, stark mehrsprachig, nur 2.83 GB, plus MTP-Speedup in aktuellem llama.cpp. Bezug: `unsloth/Qwen3.5-4B-MTP-GGUF` [5].
2. **Gemma 4 E4B-it**: zweitstaerkstes belegtes Verstaendnis der Klasse, jetzt Apache 2.0, offizielle QAT-Quants (gute Q4-Qualitaet), sehr gute Mehrsprachigkeit. Bezug: `unsloth/gemma-4-E4B-it-GGUF` (Q4_K_M 4.98 GB) [9]. Caveat: 8B Gesamtparameter, also groesster Speicher- und vermutlich langsamster Kandidat der Klasse; wenn er im Head-to-Head am Tempo scheitert, ist `unsloth/Ministral-3-3B-Instruct-2512-GGUF` (Apache 2.0, 2.15 GB, token-effizient) [23] der natuerliche Ersatz.

Beide Empfehlungspaare sind bewusst architektonisch verschieden (Qwen-Hybrid vs. Alternative), sodass das spaetere A/B-Eval echte Stilunterschiede beim extraktiven Zitieren misst.

## Quellen

1. https://www.marktechpost.com/2026/03/02/alibaba-just-released-qwen-3-5-small-models-a-family-of-0-8b-to-9b-parameters-built-for-on-device-applications/
2. https://artificialanalysis.ai/articles/qwen3-5-small-models
3. https://huggingface.co/Qwen/Qwen3.5-4B
4. https://huggingface.co/Qwen/Qwen3.5-2B
5. https://huggingface.co/unsloth/Qwen3.5-4B-MTP-GGUF
6. https://huggingface.co/unsloth/Qwen3.5-2B-GGUF
7. https://blog.google/innovation-and-ai/technology/developers-tools/gemma-4/
8. https://ai.google.dev/gemma/docs/core/model_card_4
9. https://huggingface.co/unsloth/gemma-4-E4B-it-GGUF
10. https://huggingface.co/unsloth/gemma-4-E2B-it-GGUF
11. https://unsloth.ai/docs/models/gemma-4/qat
12. https://huggingface.co/rpDungeon/Gemma-4-E4B-Luchador (Community-IFEval-Baseline fuer Stock-E4B-it)
13. https://www.liquid.ai/blog/introducing-lfm2-5-the-next-generation-of-on-device-ai
14. https://venturebeat.com/technology/no-cloud-no-gpus-no-problem-liquid-ais-new-model-lfm2-5-2-6b-brings-powerful-ai-agents-to-devices-as-small-as-a-raspberry-pi
15. https://huggingface.co/LiquidAI/LFM2.5-2.6B
16. https://huggingface.co/LiquidAI/LFM2.5-1.2B-Instruct-GGUF
17. https://huggingface.co/blog/ibm-granite/granite-4-nano
18. https://huggingface.co/ibm-granite/granite-4.0-1b
19. https://huggingface.co/ibm-granite/granite-4.0-1b-GGUF
20. https://venturebeat.com/ai/ibms-open-source-granite-4-0-nano-ai-models-are-small-enough-to-run-locally
21. https://mistral.ai/news/mistral-3/
22. https://docs.mistral.ai/models/ministral-3-3b-25-12
23. https://huggingface.co/unsloth/Ministral-3-3B-Instruct-2512-GGUF
24. https://llm-stats.com/models/compare/ministral-3-3b-instruct-2512-vs-qwen3.5-2b
25. https://arxiv.org/abs/2601.08584 (Ministral-3-Report; dort vermutlich IFEval-Zahlen, in dieser Recherche nicht extrahiert)
26. https://x.com/ArtificialAnlys/status/2049505499377193156
27. https://datanorth.ai/news/ibm-releases-granite-4-1
28. https://huggingface.co/ibm-granite/granite-4.1-3b-GGUF
29. https://huggingface.co/microsoft/Phi-4-mini-instruct
30. https://en.wikipedia.org/wiki/Phi_(language_model)
31. https://huggingface.co/blog/smollm3
32. https://huggingface.co/HuggingFaceTB/SmolLM3-3B
33. https://github.com/stared/benching-local-llms-on-apple-silicon (M5-Max-Benchmarks; nur 27B/35B-Qwen3.6, keine Small-Modelle)
34. https://venturebeat.com/technology/alibabas-small-open-source-qwen3-5-9b-beats-openais-gpt-oss-120b-and-can-run
35. https://simonwillison.net/2025/Dec/2/introducing-mistral-3/
36. https://www.latent.space/p/ainews-qwen-38-max24t-and-27b-new (Beleg: Qwen3.6/3.8 ohne offizielle Small-Modelle)

**Ehrliche Luecken:** Fuer Qwen3.5-4B/2B, Gemma 4 E4B/E2B, Ministral 3 3B und Granite gibt es derzeit keine seriös belegten tok/s-Werte auf M-Serie (nur LFM2.5-2.6B: 220 tok/s auf M5 Max). IFEval fehlt offiziell fuer Gemma 4, Ministral 3 3B (im arXiv-Report pruefen) und Granite 4.1. Diese Luecken schliesst am besten der eigene Eval-Lauf auf der Ziel-Hardware.

---

# Teil 2: Engine-Recherche (Browser-Inferenz vs. Sidecar), 2026-08-28

Der folgende Bericht beantwortet Martins Pruefauftrag (Voicemail 2) und ist die
Quelle des Abschnitts "Engine-Entscheidung" in docs/adr/0001-modellwahl.md.
Er steht hier als zweiter Teil und nicht als eigene Datei, damit die W5-Recherche
einen Ort hat.

Die Faktenlage ist ausreichend belegt. Hier der Recherchebericht.

# Browser-Inferenz vs. llama-server-Sidecar (Stand August 2026)

## Engines-Tabelle

| Engine | Format | Modelle (Kandidatenklasse 1-4B) | Belegte Apple-Silicon-Performance (Browser) | Grenzen |
|---|---|---|---|---|
| **WebLLM (MLC-AI)**, WebGPU | MLC-Format (konvertierte Weights + kompiliertes `model_lib`-WASM), **kein GGUF** ([Docs](https://webllm.mlc.ai/docs/), [GitHub](https://github.com/mlc-ai/web-llm)) | Prebuilt-Liste auf main enthaelt Qwen3.5 (0.8B/2B/4B/9B), Gemma 3 1B, Llama 3.2 1B/3B, Phi-4-mini, SmolLM2, DeepSeek-R1-Distill. **Kein MiniCPM, kein Gemma 4** ([config.ts](https://raw.githubusercontent.com/mlc-ai/web-llm/main/src/config.ts)) | M3 Max: Llama-3.1-8B q4 41.1 tok/s Decode (native MLC: 57.7, also 71%); Phi-3.5-mini 3.8B q4 71.1 tok/s (native 89.3, 80%). Prefill-Zahlen im Paper nicht ausgewiesen ([WebLLM-Paper](https://arxiv.org/html/2412.15803v2)) | Letztes GitHub-Release v0.2.83 vom April 2025, Docs auf 0.2.84; Repo lebt (Issues bis Aug 2026), Release-Kadenz aber duenn ([Releases](https://github.com/mlc-ai/web-llm/releases), [Issues](https://github.com/mlc-ai/web-llm/issues)). Self-Hosting von eigener Origin geht (model-URL + model_lib-URL) ([GitHub](https://github.com/mlc-ai/web-llm)). VRAM-Angaben pro Modell, z.B. Llama-3.2-1B q4f16: 879 MB ([config.ts](https://raw.githubusercontent.com/mlc-ai/web-llm/main/src/config.ts)) |
| **wllama / llama.cpp-WASM+WebGPU** | **GGUF direkt** (`loadModelFromUrl`), OPFS-Caching ([wllama](https://github.com/ngxson/wllama)) | Alles, was llama.cpp kann, also auch die vorhandenen GGUFs; Paper nennt ~177k verfuegbare GGUF-Modelle vs. ~400 fuer WebLLM ([Paper](https://arxiv.org/html/2605.20706v1)) | Offizieller ggml-WebGPU-Backend ("LlamaWeb", UCSC/Reese Levine, von Gerganov angekuendigt): Llama-3.2-1B q4_k_m auf Apple M3 in Chrome ~52 tok/s Decode; Decode +54% vs. WebLLM, +69% vs. transformers.js; **Prefill nur 49% von WebLLM**. Reines CPU-WASM-wllama auf Apple Silicon: **nicht belegt** ([Paper](https://arxiv.org/html/2605.20706v1), [ggerganov auf X](https://x.com/ggerganov/status/2057668450076520811)) | WASM: SIMD + Multithreading (braucht COOP/COEP-Header); **2GB pro Datei** (ArrayBuffer-Limit), groessere Modelle nur als Split-GGUF; WebGPU erst seit V3.1, sehr jung ([wllama-README](https://github.com/ngxson/wllama)). WASM-Heap ist grow-only, Speicher wird im Tab nie freigegeben ([Paper](https://arxiv.org/html/2605.20706v1)) |
| **transformers.js / ONNX Runtime Web** | ONNX (onnx-community-Konvertierungen), kein GGUF ([transformers.js v3](https://www.huggingface.co/blog/transformersjs-v3)) | Qwen3-0.6B, Qwen3.5-0.8B, Gemma-3 1B/270M als ONNX vorhanden ([gemma-3-1b-it-ONNX](https://huggingface.co/onnx-community/gemma-3-1b-it-ONNX), [Qwen3.5-0.8B-ONNX](https://huggingface.co/onnx-community/Qwen3.5-0.8B-ONNX)) | Keine soliden veroeffentlichten tok/s auf M-Serie gefunden: **nicht belegt**. Paper: Decode langsamste der drei Engines, Prefill dafuer stark ([Paper](https://arxiv.org/html/2605.20706v1)) | Korrektheitsprobleme genau in der Kandidatenklasse: JSEP-Crash mit Gemma-3-1b ([Issue #1469](https://github.com/huggingface/transformers.js/issues/1469)), fp16/q4f16-Overflow liefert kaputte Outputs auf WebGPU ([onnxruntime #26732](https://github.com/microsoft/onnxruntime/issues/26732)) |
| **MediaPipe LLM Inference (Web)** | `.litertlm`/`.task`/`.bin`, kein GGUF | Gemma-3n E2B/E4B, Gemma-3 1B | Keine Zahlen dokumentiert: **nicht belegt** | **Maintenance-only**; Google empfiehlt Migration auf LiteRT-LM JS API ([Google-Doku](https://developers.google.com/edge/mediapipe/solutions/genai/llm_inference/web_js)) |
| **WebNN** | (API, keine Engine) | - | - | W3C Candidate Recommendation aktualisiert 22.01.2026, Chrome 146 erst **Origin Trial**; fuer Produktions-LLM-Inferenz noch nicht relevant ([W3C](https://www.w3.org/TR/webnn/), [Phoronix](https://www.phoronix.com/news/Chrome-146-Beta)) |

Plattform-Rahmen: WebGPU ist seit Safari 26 (Sept 2025) auf macOS default-on und seit Maerz 2026 W3C Candidate Recommendation, ~84% globale Verbreitung ([WebKit-Blog](https://webkit.org/blog/16993/news-from-wwdc25-web-technology-coming-this-fall-in-safari-26-beta/), [tianpan.co](https://tianpan.co/blog/2026-04-17-browser-native-llm-inference-webgpu)). WebGPU-Buffer: Default maxBufferSize 256 MB, Desktop-Adapter bis 4 GB per `requiredLimits` anforderbar; auf Mobil deutlich weniger, Safari-iOS-Tabs unter 500 MB ([webgpufundamentals](https://webgpufundamentals.org/webgpu/lessons/webgpu-limits-and-features.html), [MDN](https://developer.mozilla.org/en-US/docs/Web/API/GPUSupportedLimits), [Paper](https://arxiv.org/html/2605.20706v1)). 2-5GB-Modelle im Tab sind auf Desktop-Macs also machbar, aber nicht garantiert.

## Sidecar-Referenzwerte (llama.cpp Metal, nativ)

- Kanonische Tabelle ([Discussion #4167](https://github.com/ggml-org/llama.cpp/discussions/4167), bis 2026 gepflegt, inkl. M5), 7B Q4_0: M1 PP512 118 / TG 14.2 tok/s; M2 179.6/21.9; M4 221.3/24.1; M1 Pro 266/36.4; M4 Pro 439.8/50.7; M2 Max 671/66.0; M3 Max 759.7/66.3; M4 Max 885.7/83.1. 1-4B-Modelle skalieren entsprechend hoeher.
- Kleinmodelle nativ, M3 Max 64GB, llama-server b9020: Gemma4-E2B Q4_K_M **108.3 tok/s** Decode (TTFT 59 ms), Gemma4-E4B **72.9 tok/s** (TTFT 96 ms) ([hiesch.eu](https://hiesch.eu/blog/llamacpp-benchmarks-speculative-decoding/)).
- Direkter Same-Device-Vergleich aus dem ggml-WebGPU-Paper: **Metal schlaegt WebGPU im Browser um ueber 2x beim Prefill und ~50% beim Decode** ([Paper](https://arxiv.org/html/2605.20706v1)). Das ist der sauberste verfuegbare Vergleichswert.

## Abwaegung fuer den konkreten Fall (1B+4B Q4, kurze extraktive Antworten, GGUFs vorhanden, air-gapped)

1. **Formatkompatibilitaet**: Die GGUFs liegen schon da. Nur wllama/llama.cpp-WebGPU laedt sie direkt. WebLLM erzwingt MLC-Konvertierung plus model_lib-Kompilat, transformers.js erzwingt ONNX-Export. Beides ist eine zweite Modell-Pipeline neben der bestehenden.
2. **Workload-Profil**: Extraktive Antworten heisst kurzer Output, aber Kontext im Prompt, also **Prefill-dominiert**. Genau dort ist der Abstand am groessten: Metal ueber 2x schneller als Browser-WebGPU, und der llama.cpp-WebGPU-Backend erreicht beim Prefill selbst nur 49% von WebLLM ([Paper](https://arxiv.org/html/2605.20706v1)). Beim Sidecar sind TTFTs von 59-96 ms fuer 2-4B belegt ([hiesch.eu](https://hiesch.eu/blog/llamacpp-benchmarks-speculative-decoding/)).
3. **4B-Modell**: ~2.3-2.5GB Q4 liegt ueber dem 2GB-Einzeldatei-Limit von wllama (Split-GGUF noetig) und braucht im Browser explizit angehobene WebGPU-Buffer-Limits; CPU-WASM fuer 4B ist ohne jeden Beleg und nach allen CPU-Referenzwerten unbrauchbar langsam. Nativ ist 4B Q4 auf jeder M-Serie unkritisch.
4. **Robustheit/Speicher**: Tab-Prozess mit grow-only WASM-Heap und Browser-Memory-Kill vs. eigener Prozess, den der C-Server ueberwachen und neu starten kann. Ein Engine-Crash im Sidecar reisst die SPA nicht mit.
5. **Air-gap/Serving**: Beides geht offline. Browser-Weg verlangt aber Same-Origin-Serving der Modelldateien mit COOP/COEP-Headern und OPFS-Caching im C-Server; Sidecar liest die GGUFs einfach vom Dateisystem.
6. **Plattformunabhaengigkeit**: Der einzige echte Punkt fuer Martin. WebGPU laeuft inzwischen ueberall default-on ([webkit.org](https://webkit.org/blog/16993/news-from-wwdc25-web-technology-coming-this-fall-in-safari-26-beta/)). Aber llama.cpp selbst ist genauso portabel (Metal/CUDA/Vulkan), nur mit Binary-Distribution als Preis.

## Empfehlung

**Sidecar (llama-server auf 127.0.0.1:4141, Metal) bleibt der Referenzpfad.** Benchmark-basiert: gleiche Maschine, gleiches Modell, nativ 1.5x beim Decode und ueber 2x beim Prefill schneller als der beste Browser-Weg, und der Workload ist Prefill-lastig. Dazu direkte GGUF-Nutzung, kein 2GB-Split, Prozess-Robustheit und triviale Air-gap-Story.

**Die Browser-Engine ist eine ernsthafte Zukunftsoption, aber heute nicht der Gewinner.** Falls sie spaeter gezogen wird, ist der Kandidat eindeutig **wllama mit dem offiziellen ggml-WebGPU-Backend** (nicht WebLLM, nicht transformers.js): gleiche GGUFs, gleiche ggml-Codebasis, seit 2026 offiziell im llama.cpp-Baum ([ggerganov](https://x.com/ggerganov/status/2057668450076520811)), belegte ~52 tok/s Decode fuer 1B q4 auf M3 im Browser. Sinnvoller Trigger fuer eine Neubewertung: wenn der WebGPU-Backend die Prefill-Luecke schliesst und das 4B-Handling (Split + Buffer-Limits) im wllama-Release-Stand stabil ist. WebLLM funktioniert, hat aber Konvertierungszwang und duenne Release-Kadenz (letztes Release April 2025); transformers.js hat aktuell dokumentierte WebGPU-Korrektheitsbugs genau in der 1B-Klasse; MediaPipe ist maintenance-only; WebNN ist noch Origin-Trial-Stadium.

Nicht belegt trotz Suche: tok/s fuer reines CPU-WASM-wllama auf Apple Silicon; transformers.js-tok/s auf M-Serie; Browser-Benchmarks fuer 4B-Q4-Modelle auf M-Serie.

## Quellen

- https://arxiv.org/html/2605.20706v1 (ggml-WebGPU-Paper "Llamas on the Web", Mai 2026)
- https://arxiv.org/html/2412.15803v2 (WebLLM-Paper, M3-Max-Zahlen)
- https://github.com/mlc-ai/web-llm und https://github.com/mlc-ai/web-llm/releases
- https://raw.githubusercontent.com/mlc-ai/web-llm/main/src/config.ts (Prebuilt-Modellliste)
- https://webllm.mlc.ai/docs/
- https://github.com/ngxson/wllama (GGUF, 2GB-Limit, V3.1-WebGPU)
- https://x.com/ggerganov/status/2057668450076520811 (offizieller WebGPU-Backend)
- https://github.com/ggml-org/llama.cpp/discussions/4167 (Metal-Referenztabelle)
- https://hiesch.eu/blog/llamacpp-benchmarks-speculative-decoding/ (2B/4B nativ auf M3 Max)
- https://huggingface.co/onnx-community/gemma-3-1b-it-ONNX, https://huggingface.co/onnx-community/Qwen3.5-0.8B-ONNX
- https://github.com/huggingface/transformers.js/issues/1469, https://github.com/microsoft/onnxruntime/issues/26732
- https://developers.google.com/edge/mediapipe/solutions/genai/llm_inference/web_js (maintenance-only)
- https://www.w3.org/TR/webnn/, https://www.phoronix.com/news/Chrome-146-Beta (WebNN-Status)
- https://webkit.org/blog/16993/news-from-wwdc25-web-technology-coming-this-fall-in-safari-26-beta/ (Safari-26-WebGPU)
- https://webgpufundamentals.org/webgpu/lessons/webgpu-limits-and-features.html, https://developer.mozilla.org/en-US/docs/Web/API/GPUSupportedLimits (Buffer-Limits)
- https://www.huggingface.co/blog/transformersjs-v3
- https://tianpan.co/blog/2026-04-17-browser-native-llm-inference-webgpu (WebGPU-Verbreitung; Sekundaerquelle)
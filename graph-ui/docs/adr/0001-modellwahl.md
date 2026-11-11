# ADR 0001: Optional local inference and model evidence

## Decision

CBM Lens offers optional browser inference. Weights are downloaded only after the
user opts in; they are not embedded in the executable. Model selection remains a
user choice. Browser model IDs, fixed revisions, allowed files and licenses live in
`src/browser-ai/model-policy.ts`; [THIRD_PARTY.md](../../THIRD_PARTY.md) documents them.

The older GGUF sidecar catalogue remains supported by local scripts and its recorded
evaluation. It is separate from the browser ONNX catalogue. Its measurements do not
predict browser performance, another machine's performance or a model's general rank.

## Sidecar model sources

The six Q4_K_M candidates below were recorded in August 2026. Class A used a 3072-token
context and class B used 8192 tokens. Exact download sizes refer to those evaluated
files, not runtime memory. Checksums are retained in `models/SHA256SUMS`.

| Model | Class | Download source | Recorded size | License recorded for the base model |
| --- | --- | --- | --- | --- |
| Qwen3.5-2B | A | [unsloth/Qwen3.5-2B-GGUF](https://huggingface.co/unsloth/Qwen3.5-2B-GGUF) | 1280835840 Bytes | Apache 2.0 |
| LFM2.5-1.2B-Instruct | A | [LiquidAI/LFM2.5-1.2B-Instruct-GGUF](https://huggingface.co/LiquidAI/LFM2.5-1.2B-Instruct-GGUF) | 730895168 Bytes | LFM Open License v1.0; commercial terms apply at $10M annual revenue |
| MiniCPM5-1B | A | [openbmb/MiniCPM5-1B-GGUF](https://huggingface.co/openbmb/MiniCPM5-1B-GGUF) | 688065920 Bytes | Apache 2.0 |
| Qwen2.5-Coder-1.5B-Instruct | A | [Qwen/Qwen2.5-Coder-1.5B-Instruct-GGUF](https://huggingface.co/Qwen/Qwen2.5-Coder-1.5B-Instruct-GGUF) | 1117320768 Bytes | Apache 2.0 |
| Qwen3.5-4B | B | [unsloth/Qwen3.5-4B-MTP-GGUF](https://huggingface.co/unsloth/Qwen3.5-4B-MTP-GGUF) | 2834975040 Bytes | Apache 2.0 |
| Gemma 4 E4B-it | B | [unsloth/gemma-4-E4B-it-GGUF](https://huggingface.co/unsloth/gemma-4-E4B-it-GGUF) | 4977171584 Bytes | Apache 2.0 |

Review current model-card terms before downloading or redistribution. The recorded
LFM license allows free commercial use below $10M annual revenue; use beyond that
threshold requires separate consideration. No evaluation result overrides licensing.

## Evaluation limits

`verification/w5/eval.json` records 44 fixture questions, including 23 in German,
with temperature 0 and seed 42. The figures below summarize that saved run. Citation
compliance measures the evaluation's citation rule, not factual correctness.

| Model | Class | Pass rate | Citation compliance | Mean tokens/s, rounded |
| --- | --- | --- | --- | --- |
| Qwen3.5-2B | A | 0.682 | 0.932 | 86 |
| LFM2.5-1.2B | A | 0.295 | 0.432 | 110 |
| MiniCPM5-1B | A | 0.250 | 0.545 | 171 |
| Qwen2.5-Coder-1.5B | A | 0.227 | 0.705 | 84 |
| Qwen3.5-4B | B | 0.818 | 0.955 | 38 |
| gemma-4-E4B | B | 0.841 | 1.000 | 32 |

The recorded class recommendations were Qwen3.5-2B and gemma-4-E4B, with pass-rate
and citation thresholds of 0.6 and 0.9. They are recommendations, not installed
defaults. A reported repeat run has no separate saved artifact and is not independent
reproduction evidence. The original record does not measure unscorable citations;
missing measurements must not be displayed as zero.

## Engine boundary

The sidecar evaluation used llama.cpp on Apple Silicon with Metal and GGUF files.
Its source/build record is in [runtime provenance](../../vendor/llama/HERKUNFT.md).
`llm/start.sh` also supports a local model cache and OpenAI-compatible transport;
the class-specific script options reproduce the recorded evaluation.

Browser inference uses a different runtime and format. Compare actual device and
workload behavior before drawing performance conclusions, especially prompt prefill,
GPU memory limits and large-file handling. Relevant engine sources retained from
the original investigation are [llama.cpp](https://github.com/ggml-org/llama.cpp),
[wllama](https://github.com/ngxson/wllama), [WebLLM](https://webllm.mlc.ai/docs/),
[Transformers.js](https://github.com/huggingface/transformers.js),
[WebGPU limits](https://developer.mozilla.org/en-US/docs/Web/API/GPUSupportedLimits),
and the [ggml WebGPU study](https://arxiv.org/html/2605.20706v1).
These references explain the formats and tradeoffs; the sidecar timings above are
not browser benchmarks.

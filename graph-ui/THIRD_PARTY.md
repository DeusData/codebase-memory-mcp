# CBM Lens third-party notices

This document records imported source, package licenses and optional model provenance.
Preserve the copyright and license headers in individual source files. The npm
lockfile records the exact installed dependency versions.

## Galaxy source

The initial scene was adapted from [DeusData/codebase-memory-mcp](https://github.com/DeusData/codebase-memory-mcp),
`feat/atlas-r1` at `bda46086`, on 2026-08-28. It is MIT licensed,
Copyright (c) 2025 DeusData; the full license is in the repository's `LICENSE`.

| Lens file | Original under `graph-ui/src` |
| --- | --- |
| `src/galaxy/GraphScene.tsx` | `components/GraphScene.tsx` |
| `src/galaxy/NodeCloud.tsx` | `components/NodeCloud.tsx` |
| `src/galaxy/EdgeLines.tsx` | `components/EdgeLines.tsx` |
| `src/galaxy/NodeLabels.tsx` | `components/NodeLabels.tsx` |
| `src/galaxy/HaloLayer.tsx` | `components/HaloLayer.tsx` |
| `src/galaxy/types.ts` | `lib/types.ts` |
| `src/galaxy/density.ts` | `lib/density.ts` |

Adaptations include component-local canvas access, memoized scene measurements,
frontend-specific props and types, and revised graph interaction/rendering.
`NodeTooltipCard.tsx` is a separate implementation using Lens styling rather than
the original Tailwind tooltip. File headers retain the initial import details;
subsequent changes are recorded in Git history.

## Packages

| Package | Recorded version | License |
| --- | --- | --- |
| `three` | 0.183.2 | MIT |
| `@react-three/fiber` | 9.5.0 | MIT |
| `@react-three/drei` | 10.7.7 | MIT |
| `@react-three/postprocessing` | 3.0.4 | MIT |
| `postprocessing` | 6.38.3 | Zlib |
| `three-stdlib` | 2.36.1 | MIT |
| `@types/three` | 0.183.1 | MIT |
| `@huggingface/transformers` | 4.2.0 | Apache-2.0 |
| `onnxruntime-web` | 1.26.0-dev.20260416-b7804b056c | MIT |
| `sharp` | 0.35.0 | Apache-2.0 |
| `react-markdown` | 10.1.0 | MIT |
| `remark-gfm` | 4.0.1 | MIT |

These packages are used in their published form. `postprocessing` requires accurate
origin attribution and identification of modified versions under its Zlib license.
React, Vite, Vitest and Monaco use MIT; TypeScript and Playwright use Apache-2.0.
`sharp` is a transitive Node dependency with a targeted override, not browser code.
Full Markdown renderer license texts appear below.

## CodeAtlasIDE provenance

Provider, semantic IR, twin and semantic-search modules were ported from Bernhard
Jackiewicz's CodeAtlasIDE project by the same author. Their source headers retain
that origin. They are original contributions from Bernhard, not imports from an
unrelated third party.

The concept of deriving an entry tour from project structure was inspired by
Understand-Anything (MIT). `src/tours/tour-generator.ts` contains original Kahn
traversal, role labels, wording and cycle handling; no source was copied from that
project. Its direct code origin is CodeAtlasIDE.

## Optional browser models

Transformers.js and ONNX Runtime assets are served locally. Weights are not bundled;
downloading them requires an explicit browser action. Source inference stays in the
browser. Model policy pins revisions and permitted files; it does not execute remote
model scripts.

The following metadata was recorded on 2026-09-08. Download sizes include tokenizer
and configuration files and do not describe GPU memory requirements.

| Model | Fixed revision | Download bytes | Recorded license |
| --- | --- | --- | --- |
| [onnx-community/Qwen2.5-Coder-0.5B-Instruct](https://huggingface.co/onnx-community/Qwen2.5-Coder-0.5B-Instruct) | `f0292f665fd307846ff3c318a91a1bc29d091492` | 566366194 | Apache-2.0 for the base model |
| [onnx-community/Qwen3-0.6B-ONNX](https://huggingface.co/onnx-community/Qwen3-0.6B-ONNX) | `da1453100cf3ff33ef56d17983fc7a8648706db6` | 578917626 | Apache-2.0 |
| [LiquidAI/LFM2.5-1.2B-Instruct-ONNX](https://huggingface.co/LiquidAI/LFM2.5-1.2B-Instruct-ONNX) | `10f72e70abf67ac0fd7ebf15bc5854726891d864` | 763763755 | Liquid AI license; review commercial terms above $10M annual revenue |
| [onnx-community/Qwen3.5-2B-ONNX-OPT](https://huggingface.co/onnx-community/Qwen3.5-2B-ONNX-OPT) | `2ea7886f48b926aca97de8b0e041ffca7e3ebaa9` | 1402850762 | Apache-2.0 |

All use `q4f16`. Qwen3.5 loads the text decoder and embeddings only, not vision
files. The Qwen2.5 ONNX weight file is 554935833 bytes; its published LFS SHA-256 is
`60c076ac0d3910881fe0cad75997e803459a5f71bb74bcbcd82407ef83db7ba0`.
The converter's model card does not declare a separate license; its
[official base model](https://huggingface.co/Qwen/Qwen2.5-Coder-0.5B-Instruct)
is Apache-2.0.

`src/browser-ai/model-policy.ts` retains complete file lists, published LFS hashes
and allowed hosts, including `huggingface.co` and the recorded redirect host
`us.aws.cdn.hf.co`. The loader does not recompute the published hashes.
The browser context limit is 8192 tokens including 512 reserved output tokens;
requests are counted with the selected template and tokenizer rather than silently
truncated. Qwen3 and Qwen3.5 default to non-thinking mode. Runtime support is not a
benchmark or a compatibility guarantee for every browser/GPU combination.

## Markdown renderer licenses

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

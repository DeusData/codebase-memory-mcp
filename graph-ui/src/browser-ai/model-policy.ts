/** Metadata verified against the pinned Hugging Face revision on 2026-09-07. */
export const BROWSER_MODEL = {
    id: 'onnx-community/Qwen2.5-Coder-0.5B-Instruct',
    revision: 'f0292f665fd307846ff3c318a91a1bc29d091492',
    displayName: 'Qwen2.5 Coder 0.5B',
    bytes: 566366194,
    weightBytes: 554935833,
    weightSha256: '60c076ac0d3910881fe0cad75997e803459a5f71bb74bcbcd82407ef83db7ba0',
    cacheName: 'cbm-browser-ai-qwen25-coder-f0292f6-v1',
    modelCard: 'https://huggingface.co/onnx-community/Qwen2.5-Coder-0.5B-Instruct',
    baseModelCard: 'https://huggingface.co/Qwen/Qwen2.5-Coder-0.5B-Instruct',
    license: 'Apache-2.0 (base model)',
    dtype: 'q4f16',
} as const;

export const MODEL_FILES = ['config.json', 'generation_config.json', 'tokenizer.json', 'tokenizer_config.json', 'onnx/model_q4f16.onnx'] as const;
export const MODEL_BASE_URL = `https://huggingface.co/${BROWSER_MODEL.id}/resolve/${BROWSER_MODEL.revision}/`;
export const MODEL_DOWNLOAD_ORIGINS = ['https://huggingface.co', 'https://us.aws.cdn.hf.co'] as const;
export const MAX_OUTPUT_TOKENS = 160;

export interface BrowserModel {
    id: string;
    revision: string;
    displayName: string;
    bytes: number;
    cacheName: string;
    modelCard: string;
    license: string;
    dtype: 'q4f16';
    /** Deliberately bounded browser context, including the reserved answer. */
    contextTokens: number;
    maxOutputTokens: number;
    availability: 'available' | 'unsupported';
    compatibilityNote: string;
    files: readonly string[];
    /** Published LFS hashes; revision pinning is enforced, hashes are not recomputed in the browser. */
    weightHashes: Readonly<Record<string, string>>;
}

/** Immutable manifests verified 2026-09-08. Bytes cover exactly the allowed files, not GPU memory.
 * Runtime architecture support is verified in Transformers.js 4.2.0; device performance is unbenchmarked.
 */
export const BROWSER_MODELS: readonly BrowserModel[] = [
    {
        ...BROWSER_MODEL, contextTokens: 8192, maxOutputTokens: 512, availability: 'available',
        compatibilityNote: 'Coding baseline · WebGPU with shader-f16 · 8K browser context',
        files: MODEL_FILES, weightHashes: { 'onnx/model_q4f16.onnx': BROWSER_MODEL.weightSha256 },
    },
    {
        id: 'onnx-community/Qwen3-0.6B-ONNX', revision: 'da1453100cf3ff33ef56d17983fc7a8648706db6',
        displayName: 'Qwen3 0.6B', bytes: 578917626, cacheName: 'cbm-browser-ai-qwen3-06-da14531-v1',
        modelCard: 'https://huggingface.co/onnx-community/Qwen3-0.6B-ONNX', license: 'Apache-2.0', dtype: 'q4f16',
        contextTokens: 8192, maxOutputTokens: 512, availability: 'available',
        compatibilityNote: 'Small download · WebGPU with shader-f16 · non-thinking · 8K browser context',
        files: MODEL_FILES, weightHashes: { 'onnx/model_q4f16.onnx': '9e33a5911974174761d0dfdcc0bec975d9c45af0eae5e9eb647b8ba9442a8f91' },
    },
    {
        id: 'LiquidAI/LFM2.5-1.2B-Instruct-ONNX', revision: '10f72e70abf67ac0fd7ebf15bc5854726891d864',
        displayName: 'LFM2.5 1.2B', bytes: 763763755, cacheName: 'cbm-browser-ai-lfm25-12-10f72e7-v1',
        modelCard: 'https://huggingface.co/LiquidAI/LFM2.5-1.2B-Instruct-ONNX',
        license: 'Liquid AI license · commercial terms apply above $10M annual revenue; review model card', dtype: 'q4f16',
        contextTokens: 8192, maxOutputTokens: 512, availability: 'available',
        compatibilityNote: 'Instruction-following candidate · WebGPU with shader-f16 · 8K browser context · not benchmarked in CBM',
        files: [...MODEL_FILES, 'onnx/model_q4f16.onnx_data'],
        weightHashes: {
            'onnx/model_q4f16.onnx': 'a9986ad188200507342ac32f727aa4691edb5428b0aa8c4a9fbdf0c85a6fe667',
            'onnx/model_q4f16.onnx_data': '46cfacc12941150620a3f644a5269e9baebd75d681cfa09cadeef71b8ed64ac2',
        },
    },
    {
        id: 'onnx-community/Qwen3.5-2B-ONNX-OPT', revision: '2ea7886f48b926aca97de8b0e041ffca7e3ebaa9',
        displayName: 'Qwen3.5 2B', bytes: 1402850762, cacheName: 'cbm-browser-ai-qwen35-2-2ea7886-v1',
        modelCard: 'https://huggingface.co/onnx-community/Qwen3.5-2B-ONNX-OPT', license: 'Apache-2.0', dtype: 'q4f16',
        contextTokens: 8192, maxOutputTokens: 512, availability: 'available',
        compatibilityNote: 'Text only · WebGPU with shader-f16 · non-thinking · 8K browser context · not benchmarked in CBM',
        files: ['config.json', 'generation_config.json', 'tokenizer.json', 'tokenizer_config.json',
            'onnx/decoder_model_merged_q4f16.onnx', 'onnx/decoder_model_merged_q4f16.onnx_data',
            'onnx/embed_tokens_q4f16.onnx', 'onnx/embed_tokens_q4f16.onnx_data'],
        weightHashes: {
            'onnx/decoder_model_merged_q4f16.onnx': 'c567d4d34dc97185e85bb40c9c30d6f73133858b1f8a32b90166b7fea4b653bf',
            'onnx/decoder_model_merged_q4f16.onnx_data': '06dd7841f90e5c4ecc029193a29478750ae9dcbfeaf8cfb223cf8b69cc5666d6',
            'onnx/embed_tokens_q4f16.onnx': '802a072ff21f540eda7f343aa71dbb0354c8859caaf34f09b3bf8117725d7de8',
            'onnx/embed_tokens_q4f16.onnx_data': '650aa8eb39b7404ca2c908d78243c82b6fd88321feeb8fca175745806c6b3a81',
        },
    },
];

export function getBrowserModel(modelId: string = BROWSER_MODEL.id): BrowserModel {
    const model = BROWSER_MODELS.find(candidate => candidate.id === modelId);
    if (!model) throw new Error('Unknown browser model. Choose a model from the catalog.');
    return model;
}

export function browserModelBaseUrl(model: BrowserModel): string {
    return `https://huggingface.co/${model.id}/resolve/${model.revision}/`;
}

export function isPinnedModelRequest(value: string, modelId: string = BROWSER_MODEL.id): boolean {
    try {
        const model = getBrowserModel(modelId);
        const url = new URL(value);
        return !url.username && !url.password && !url.hash && (!url.search || url.search === '?download=true')
            && model.files.some(file => `${url.origin}${url.pathname}` === `${browserModelBaseUrl(model)}${file}`);
    } catch { return false; }
}

export async function removeBrowserModelCache(modelId: string = BROWSER_MODEL.id): Promise<void> {
    const model = getBrowserModel(modelId);
    if (typeof caches !== 'undefined') await caches.delete(model.cacheName);
}

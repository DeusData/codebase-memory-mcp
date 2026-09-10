import { describe, expect, it, vi } from 'vitest';
import { BROWSER_MODEL, BROWSER_MODELS, browserModelBaseUrl, getBrowserModel, isPinnedModelRequest, MODEL_BASE_URL, MODEL_FILES, removeBrowserModelCache } from './model-policy';

describe('pinned browser model download policy', () => {
    it('allows only the verified model files at the exact immutable revision', () => {
        for (const file of MODEL_FILES) expect(isPinnedModelRequest(`${MODEL_BASE_URL}${file}`)).toBe(true);
        for (const url of [
            `${MODEL_BASE_URL}unexpected.onnx`,
            `${MODEL_BASE_URL}config.json?source=private`,
            `${MODEL_BASE_URL}config.json#fragment`,
            `${MODEL_BASE_URL.replace(BROWSER_MODEL.revision, 'main')}config.json`,
            `${MODEL_BASE_URL.replace('huggingface.co', 'huggingface.co.attacker.test')}config.json`,
            `${MODEL_BASE_URL.replace('https://', 'https://user:secret@')}config.json`,
            'file:///tmp/model.onnx',
        ]) expect(isPinnedModelRequest(url)).toBe(false);
    });

    it('deletes only the dedicated model cache', async () => {
        const remove = vi.fn(async () => true);
        vi.stubGlobal('caches', { delete: remove });
        try {
            await removeBrowserModelCache();
            expect(remove).toHaveBeenCalledExactlyOnceWith(BROWSER_MODEL.cacheName);
        } finally { vi.unstubAllGlobals(); }
    });

    it('allows each manifest only for its selected model and immutable revision', () => {
        for (const model of BROWSER_MODELS) {
            expect(model.revision).toMatch(/^[a-f0-9]{40}$/);
            for (const file of model.files) {
                const url = browserModelBaseUrl(model) + file;
                expect(isPinnedModelRequest(url, model.id)).toBe(true);
                expect(isPinnedModelRequest(url.replace(model.revision, 'main'), model.id)).toBe(false);
                for (const other of BROWSER_MODELS.filter(candidate => candidate.id !== model.id)) {
                    expect(isPinnedModelRequest(url, other.id)).toBe(false);
                }
            }
            expect(model.contextTokens).toBeGreaterThan(model.maxOutputTokens);
            expect(model.compatibilityNote).toContain('WebGPU');
        }
        expect(() => getBrowserModel('unknown')).toThrow('Unknown browser model');
        expect(isPinnedModelRequest(MODEL_BASE_URL + MODEL_FILES[0], 'unknown')).toBe(false);
    });

    it('keeps Qwen3.5 text-only and exposes the LFM license before downloading', () => {
        const qwen = BROWSER_MODELS.find(model => model.displayName === 'Qwen3.5 2B')!;
        expect(qwen.files).toContain('onnx/embed_tokens_q4f16.onnx_data');
        expect(qwen.files.some(file => /vision|processor/.test(file))).toBe(false);
        expect(isPinnedModelRequest(browserModelBaseUrl(qwen) + 'onnx/vision_encoder_q4f16.onnx', qwen.id)).toBe(false);
        expect(BROWSER_MODELS.find(model => model.displayName === 'LFM2.5 1.2B')!.license).toContain('$10M');
    });

    it('removes the selected cache without deleting another model', async () => {
        const remove = vi.fn(async () => true);
        vi.stubGlobal('caches', { delete: remove });
        try {
            await removeBrowserModelCache(BROWSER_MODELS[2].id);
            expect(remove).toHaveBeenCalledExactlyOnceWith(BROWSER_MODELS[2].cacheName);
        } finally { vi.unstubAllGlobals(); }
    });
});

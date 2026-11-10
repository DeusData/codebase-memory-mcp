import { describe, expect, it, vi } from 'vitest';
import { BrowserAiController } from './browser-ai-controller';
import type { BrowserAiSource } from './browser-ai-controller';

const source: BrowserAiSource = { text: 'function sum(a, b) { return a + b; }', path: 'src/math.ts', startLine: 8, project: 'sample' };
function fixture() {
    let resolvePrepare!: () => void;
    const runtime = {
        prepare: vi.fn(() => new Promise<void>(resolve => { resolvePrepare = resolve; })),
        explain: vi.fn(async (_source: BrowserAiSource) => 'Adds two values.'), dispose: vi.fn(),
    };
    const create = vi.fn(() => runtime);
    const remove = vi.fn(async () => {});
    const changed = vi.fn();
    return { runtime, create, remove, changed, controller: new BrowserAiController(create, remove, changed), ready: () => resolvePrepare() };
}

describe('browser AI consent and lifecycle', () => {
    it('does not create a worker or download anything until explicit preparation', async () => {
        const f = fixture();
        expect(f.controller.state.phase).toBe('off');
        await f.controller.explain(source);
        expect(f.create).not.toHaveBeenCalled();
        expect(f.runtime.explain).not.toHaveBeenCalled();
        const preparing = f.controller.prepare();
        expect(f.create).toHaveBeenCalledOnce();
        expect(f.controller.state.phase).toBe('preparing');
        f.ready(); await preparing;
        expect(f.controller.state.phase).toBe('ready');
    });

    it('terminates preparation immediately and ignores a late ready event', async () => {
        const f = fixture(); const preparing = f.controller.prepare();
        f.controller.cancel();
        expect(f.runtime.dispose).toHaveBeenCalledOnce();
        expect(f.controller.state.phase).toBe('off');
        f.ready(); await preparing;
        expect(f.controller.state.phase).toBe('off');
    });

    it('preserves the exact input and source attribution, leaving token limits to the runtime', async () => {
        const f = fixture(); const preparing = f.controller.prepare();
        expect(f.runtime.prepare).toHaveBeenCalledOnce(); f.ready(); await preparing;
        const text = '\t' + 'x'.repeat(6040) + '\r\n  ';
        await f.controller.explain({ ...source, text });
        expect(f.runtime.explain.mock.calls[0][0].text).toBe(text);
        expect(f.controller.state.phase).toBe('ready');
        expect(f.controller.state.output).toBe('Adds two values.');
        expect(f.controller.state.outputSource?.path).toBe(source.path);
        expect(f.controller.state.outputSource?.startLine).toBe(8);
    });

    it('discards a late generated result after cancel instead of presenting it as current', async () => {
        const f = fixture(); const preparing = f.controller.prepare();
        expect(f.runtime.prepare).toHaveBeenCalledOnce(); f.ready(); await preparing;
        let resolveOutput!: (value: string) => void;
        f.runtime.explain.mockImplementation(() => new Promise(resolve => { resolveOutput = resolve; }));
        const generating = f.controller.explain(source);
        f.controller.cancel(); resolveOutput('Late result'); await generating;
        expect(f.controller.state.output).toBeUndefined();
        expect(f.controller.state.phase).toBe('off');
    });

    it('removes only the dedicated model cache without creating a worker or downloading', async () => {
        const f = fixture(); await f.controller.remove();
        expect(f.remove).toHaveBeenCalledOnce();
        expect(f.create).not.toHaveBeenCalled();
        expect(f.controller.state.phase).toBe('off');
    });

    it('surfaces preparation failures and disposes the failed runtime', async () => {
        const f = fixture(); f.runtime.prepare.mockRejectedValueOnce(new Error('WebGPU unavailable'));
        await f.controller.prepare();
        expect(f.controller.state.phase).toBe('error');
        expect(f.controller.state.error).toContain('WebGPU unavailable');
        expect(f.runtime.dispose).toHaveBeenCalledOnce();
    });
});

// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest';
import BrowserAiPanel from './BrowserAiPanel';
import { browserAiText as text } from './strings';
import type { BrowserAiSource } from './browser-ai-controller';

let container: HTMLDivElement;
let root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); });

function fixture() {
    const runtime = { prepare: vi.fn(async () => {}), explain: vi.fn(async (_source: BrowserAiSource) => 'Returns the sum.'), dispose: vi.fn() };
    return { runtime, createRuntime: vi.fn(() => runtime), removeCache: vi.fn(async () => {}), onClose: vi.fn() };
}
const source = { text: 'return a + b;', path: 'src/add.ts', project: 'sample', startLine: 3 };
async function click(label: string): Promise<void> {
    const target = [...container.querySelectorAll('button')].find(button => button.textContent === label);
    expect(target).not.toBeUndefined(); await act(async () => target?.click());
}

describe('optional browser AI panel', () => {
    it('opens off with model size and explicit consent, without creating a worker', async () => {
        const props = fixture(); await act(async () => root.render(<BrowserAiPanel {...props} source={source} />));
        expect(props.createRuntime).not.toHaveBeenCalled();
        expect(props.runtime.prepare).not.toHaveBeenCalled();
        expect(container.textContent).toContain('566 MB');
        expect(container.textContent).toContain(text.download);
        expect(container.querySelector('pre')?.textContent).toBe(source.text);
    });

    it('waits for a separate Explain action after enable and labels the attributed output as unverified', async () => {
        const props = fixture(); await act(async () => root.render(<BrowserAiPanel {...props} source={source} />));
        await click(text.download);
        expect(props.runtime.prepare).toHaveBeenCalledOnce();
        expect(props.runtime.explain).not.toHaveBeenCalled();
        await click(text.explain);
        expect(props.runtime.explain).toHaveBeenCalledWith(source);
        expect(container.textContent).toContain(text.output);
        expect(container.textContent).toContain('src/add.ts:3');
        expect(container.querySelector('.atlas-browser-ai-output > p')?.textContent).toBe('sample · src/add.ts:3');
        expect(container.textContent).toContain('Returns the sum.');
    });

    it('shows the full exact source before generation', async () => {
        const props = fixture();
        const exactText = '\t' + 'x'.repeat(6025) + '\n  ';
        await act(async () => root.render(<BrowserAiPanel {...props} source={{ ...source, text: exactText }} />));
        expect(container.querySelector('pre')?.textContent).toBe(exactText);
        expect(container.textContent).not.toContain(text.truncated);
        expect(props.createRuntime).not.toHaveBeenCalled();
    });

    it('lets users remove the dedicated cache while remaining off', async () => {
        const props = fixture(); await act(async () => root.render(<BrowserAiPanel {...props} />));
        await click(text.remove);
        expect(props.removeCache).toHaveBeenCalledOnce();
        expect(props.createRuntime).not.toHaveBeenCalled();
        expect(container.textContent).toContain(text.noSource);
    });
});

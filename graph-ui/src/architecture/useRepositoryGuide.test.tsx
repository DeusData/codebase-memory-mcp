// @vitest-environment jsdom
import { act } from 'react';
import { createRoot } from 'react-dom/client';
import { afterEach, expect, it, vi } from 'vitest';
import { useRepositoryGuide } from './useRepositoryGuide';
import type { SourceReader } from './repository-guide';

(globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0)) await cleanup(); });
async function mount(paths: string[], readSource?: SourceReader) {
    const container = document.createElement('div'); document.body.append(container);
    const root = createRoot(container);
    let result: ReturnType<typeof useRepositoryGuide>;
    function Harness({ paths, reader }: { paths: string[]; reader?: SourceReader }) {
        result = useRepositoryGuide(paths, reader); return null;
    }
    const render = async (nextPaths = paths, reader = readSource) => {
        await act(async () => root.render(<Harness paths={nextPaths} reader={reader} />));
    };
    cleanups.push(async () => { await act(async () => root.unmount()); container.remove(); });
    await render();
    return { render, result: () => result! };
}

it('finishes an empty inspection without leaving a permanent loading state', async () => {
    const read = vi.fn(async () => ({ source: '' }));
    const ui = await mount([], read);
    expect(ui.result().finished).toBe(true);
    expect(read).not.toHaveBeenCalled();
});

it('does not transfer stale descriptions across a path or reader switch', async () => {
    const resolve: ((value: { source: string }) => void)[] = [];
    const reader = vi.fn(() => new Promise<{ source: string }>(done => resolve.push(done)));
    const ui = await mount(['README.md'], reader);
    await ui.render(['other/README.md']);
    await act(async () => resolve[0]({ source: 'A previous project description must never appear in the selected repository.' }));
    expect(ui.result().quotes).toEqual({});
    await act(async () => resolve[1]({ source: 'A current project description explains the selected repository to its readers.' }));
    expect(Object.keys(ui.result().quotes)).toEqual(['other/README.md']);
    const replacement = vi.fn(async () => { throw new Error('source unavailable'); });
    await ui.render(['other/README.md'], replacement);
    expect(ui.result()).toMatchObject({ quotes: {}, finished: true, failures: 1 });
});

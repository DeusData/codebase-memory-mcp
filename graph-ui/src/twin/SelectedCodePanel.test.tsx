// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { CREATE_USER_IR } from '../test-support/twin-fixtures';
import type { ImportsGroup } from '../pseudocode/imports-group';
import SelectedCodePanel, { type SelectedCodePanelProps } from './SelectedCodePanel';

let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
});

afterEach(async () => {
    await act(async () => root.unmount());
    container.remove();
});

function props(overrides: Partial<SelectedCodePanelProps> = {}): SelectedCodePanelProps {
    return {
        filePath: 'src/services/userService.ts',
        symbol: CREATE_USER_IR.symbol,
        ir: CREATE_USER_IR,
        status: 'ready',
        pinned: false,
        onTogglePin: vi.fn(),
        onAsk: vi.fn(),
        onShowGraph: vi.fn(),
        onFollow: vi.fn(),
        ...overrides,
    };
}

async function render(overrides: Partial<SelectedCodePanelProps> = {}) {
    const value = props(overrides);
    await act(async () => root.render(<SelectedCodePanel {...value} />));
    return value;
}

async function click(selector: string) {
    const element = container.querySelector<HTMLElement>(selector);
    expect(element).not.toBeNull();
    await act(async () => element?.click());
}

describe('Selected code inspector', () => {
    it('starts with one file-opening hint and no empty control wall', async () => {
        await render({ filePath: '', symbol: undefined, ir: undefined, status: 'empty' });
        expect(container.textContent).toContain('Open a file');
        expect(container.querySelector('.selected-code-actions')).toBeNull();
        expect(container.querySelector('.selected-code-pin')?.hasAttribute('disabled')).toBe(true);
        expect(container.querySelectorAll('.selected-code-fact')).toHaveLength(0);
    });

    it('offers actual file symbols before a caret has resolved a function', async () => {
        const value = await render({ symbol: undefined, ir: undefined, status: 'empty',
            fileSymbols: [CREATE_USER_IR.symbol], fileSymbolsStatus: 'ready',
            fileSymbolsMessage: 'Showing the first 100 indexed symbols.' });
        expect(container.textContent).toContain('Symbols in this file');
        expect(container.textContent).toContain('function · 23');
        expect(container.textContent).toContain('Showing the first 100 indexed symbols.');
        await click('.selected-code-file li button');
        expect(value.onFollow).toHaveBeenCalledWith(CREATE_USER_IR.symbol);
    });

    it('distinguishes unavailable file symbols from a completed empty result', async () => {
        await render({ symbol: undefined, ir: undefined, status: 'empty',
            fileSymbolsStatus: 'error', fileSymbolsMessage: 'Index service unavailable.' });
        expect(container.textContent).toContain('Index service unavailable.');
        expect(container.textContent).not.toContain('No symbols found');
        await render({ symbol: undefined, ir: undefined, status: 'empty',
            fileSymbolsStatus: 'ready', fileSymbols: [] });
        expect(container.textContent).toContain('No symbols found in the current index.');
    });

    it('keeps failed symbol resolution separate from still useful file navigation', async () => {
        await render({ symbol: undefined, ir: undefined, status: 'not-indexed',
            message: 'This file has not been indexed.', fileSymbolsStatus: 'ready', fileSymbols: [] });
        expect(container.querySelector('[role="status"]')?.textContent).toBe('This file has not been indexed.');
        expect(container.querySelector('.selected-code-actions')).not.toBeNull();
        expect(container.querySelector('[data-section="steps"]')).toBeNull();
    });

    it('preserves exact marked text and makes broader relationship scope explicit', async () => {
        const selection = { startLine: 24, startColumn: 3, endLine: 25, endColumn: 17,
            text: '  validateUser(input);\n\treturn `<User>`;' };
        await render({ selection });
        expect(container.querySelector('.selected-code-selection code')?.textContent).toBe(selection.text);
        expect(container.textContent).toContain('24:3 to 25:17');
        expect(container.textContent).toContain('Relationships below describe createUser');
        expect(container.querySelector('.selected-code-selection code User')).toBeNull();
    });

    it('offers explicit graph, chat and controlled pin actions without model setup', async () => {
        const value = await render();
        await click('.selected-code-ask');
        await click('.selected-code-actions button:last-child');
        await click('.selected-code-pin');
        expect(value.onAsk).toHaveBeenCalledOnce();
        expect(value.onShowGraph).toHaveBeenCalledOnce();
        expect(value.onTogglePin).toHaveBeenCalledOnce();
        expect(container.querySelector('.selected-code-pin')?.getAttribute('aria-pressed')).toBe('false');
        await render({ pinned: true });
        expect(container.querySelector('.selected-code-pin')?.getAttribute('aria-pressed')).toBe('true');
        expect(container.textContent).toContain('Context pinned.');
        expect(container.textContent).not.toMatch(/SEMANTIC_TWIN|Reading preferences|LOCAL LLM|start\.sh|Changes/);
    });

    it('does not pin transient loading context, but lets an existing pin be released', async () => {
        await render({ status: 'loading', ir: undefined });
        expect(container.querySelector('.selected-code-pin')?.hasAttribute('disabled')).toBe(true);
        await render({ status: 'loading', ir: undefined, pinned: true });
        expect(container.querySelector('.selected-code-pin')?.hasAttribute('disabled')).toBe(false);
    });

    it('waits for file symbols before pinning file context and still allows unpinning', async () => {
        const value = await render({ symbol: undefined, ir: undefined, status: 'empty', fileSymbolsStatus: 'loading' });
        expect(container.querySelector('.selected-code-pin')?.hasAttribute('disabled')).toBe(true);
        await click('.selected-code-pin');
        expect(value.onTogglePin).not.toHaveBeenCalled();
        const pinned = await render({ symbol: undefined, ir: undefined, status: 'empty', fileSymbolsStatus: 'loading', pinned: true });
        expect(container.querySelector('.selected-code-pin')?.hasAttribute('disabled')).toBe(false);
        await click('.selected-code-pin');
        expect(pinned.onTogglePin).toHaveBeenCalledOnce();
        await render({ symbol: undefined, ir: undefined, status: 'empty', fileSymbolsStatus: 'ready', fileSymbols: [] });
        expect(container.querySelector('.selected-code-pin')?.hasAttribute('disabled')).toBe(false);
    });

    it('keeps outgoing facts source-linked with the original citation', async () => {
        const value = await render();
        expect(container.querySelector('[data-section="steps"]')?.hasAttribute('open')).toBe(false);
        await click('[data-section="steps"] > summary');
        await click('[data-section="steps"] .atlas-twin-row button');
        expect(value.onFollow).toHaveBeenCalledWith(expect.objectContaining({ name: 'validateUser',
            uri: 'file:///workspace/atlas-sample/src/util/validate.ts',
            range: expect.objectContaining({ start: { line: 18, character: 0 } }) }));
        await click('[data-section="steps"] [data-factpath="steps[0]"]');
        expect(container.querySelector('.atlas-evidence-attribution')?.textContent).toContain('cbm');
        expect(container.querySelector('.atlas-evidence-loc')?.textContent).toContain('validate.ts');
    });

    it('does not turn an unavailable answer into a checked-empty assertion', async () => {
        await render({ ir: { ...CREATE_USER_IR,
            steps: { value: [], state: 'unknown', evidence: [] },
            tests: { value: [], state: 'notIndexed', evidence: [] } } });
        const calls = container.querySelector('[data-section="steps"]');
        expect(calls?.querySelector('summary')?.textContent).toContain('Unavailable');
        expect(calls?.textContent).toContain('unavailable from the current index');
        expect(container.querySelector('[data-section="tests"]')?.textContent).toContain('not indexed yet');
        expect(container.querySelector('[data-section="tests"]')?.textContent).toContain('not test results or measured coverage');
    });

    it('shows runtime only for real observations and retains optional outline navigation', async () => {
        await render();
        expect(container.querySelector('[data-section="runtime"]')).toBeNull();
        const onOpenFlow = vi.fn();
        await render({ onOpenFlow, callOutline: <div>Deterministic outline</div>, ir: {
            ...CREATE_USER_IR,
            runtime: { state: 'known', evidence: [], value: [{ targetName: 'validateUser', count: 3, unexpected: false }] },
        } });
        expect(container.querySelector('[data-section="runtime"]')?.textContent).toContain('validateUser');
        expect(container.querySelector('[data-section="outline"]')?.textContent).toContain('not complete control flow');
        await click('.selected-code-flow');
        expect(onOpenFlow).toHaveBeenCalledOnce();
    });

    it('retains imports and their source evidence for a file without a resolved symbol', async () => {
        const imports: ImportsGroup = { heading: 'Imports', entries: [{ id: 'import-0',
            factPath: 'imports[0]', label: 'validateUser', module: '../util/validate',
            usage: 'used', marker: 'used', text: 'Used by this file.', note: 'Matched an indexed call.',
            sourceRef: { uri: CREATE_USER_IR.symbol.uri, line: 4 }, finding: false, origin: 'source',
            evidence: [{ source: 'source-text', file: CREATE_USER_IR.symbol.uri,
                range: { startLine: 4, endLine: 4 }, engineGeneration: 2, providerId: 'codeatlas' }] }],
            hidden: 0, tally: '1 import', used: 1, unused: 0, unknown: 0, sourceRead: true };
        const value = await render({ symbol: undefined, ir: undefined, status: 'empty', imports });
        await click('[data-section="imports"] .atlas-twin-row-chip button');
        expect(value.onFollow).toHaveBeenCalledWith(expect.objectContaining({ uri: CREATE_USER_IR.symbol.uri,
            range: expect.objectContaining({ start: { line: 3, character: 0 } }) }));
        await click('[data-factpath="imports[0]"]');
        expect(container.querySelector('.atlas-evidence-loc')?.textContent).toContain('userService.ts:4');
    });
});

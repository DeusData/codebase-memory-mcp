// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import ConfigPanel from './ConfigPanel';
import { DEFAULT_GRAPH_DISPLAY, type GraphDisplaySettings } from '../galaxy/density';
import type { ConfigService, ConfigSetting, ConfigSnapshot } from './config-model';
import { viewPreferencesKey } from './view-preferences';

let container: HTMLDivElement, root: Root;
const setting: ConfigSetting = { key: 'CBM_WORKERS', label: 'Index workers', description: 'Parallel workers.', category: 'resources', type: 'integer', minimum: 1, maximum: 256,
    defaultValue: null, value: '4', override: null, effective: '4', source: 'environment', applyMode: 'restart', pendingRestart: false, editable: true };
const snapshot = (change: Partial<ConfigSetting> = {}): ConfigSnapshot => ({ revision: '1', settings: [{ ...setting, ...change }] });
let service: { configuration: ReturnType<typeof vi.fn<ConfigService['configuration']>>; saveConfiguration: ReturnType<typeof vi.fn<ConfigService['saveConfiguration']>> };
const close = vi.fn<() => void>(), model = vi.fn<() => void>(), display = vi.fn<(next: GraphDisplaySettings) => void>();
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    // Node exposes its own storage global; use the actual jsdom browser store.
    vi.stubGlobal('localStorage', (globalThis as unknown as { jsdom: { window: Window } }).jsdom.window.localStorage);
    window.localStorage.clear();
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
    service = { configuration: vi.fn().mockResolvedValue(snapshot()), saveConfiguration: vi.fn() };
    close.mockReset(); model.mockReset(); display.mockReset();
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });
const button = (text: string) => [...container.querySelectorAll<HTMLButtonElement>('button')].find(item => item.textContent === text)!;
const click = async (text: string) => { await act(async () => button(text).click()); };
const input = () => container.querySelector<HTMLInputElement>('#config-CBM_WORKERS')!;
const edit = async (value: string) => { await act(async () => { Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(input(), value); input().dispatchEvent(new Event('input', { bubbles: true })); }); };
async function render(embedded = false, hidden = false, active = true) { await act(async () => root.render(<div hidden={hidden}><ConfigPanel embedded={embedded} active={active} project="config-test" service={service} display={DEFAULT_GRAPH_DISPLAY} onDisplay={display} onOpenBrowserModels={model} onOpenDisplay={vi.fn()} onClose={close} /></div>)); }

describe('Config panel', () => {
    it('embeds without a backdrop, focus capture, close action, or modal keyboard handling', async () => {
        const outside = document.createElement('button');
        document.body.appendChild(outside);
        try {
            outside.focus();
            await render(true);
            expect(document.activeElement).toBe(outside);
            expect(container.querySelector('.cbm-config-backdrop')).toBeNull();
            expect(container.querySelector('[role="dialog"]')).toBeNull();
            expect(container.querySelector('[aria-modal]')).toBeNull();
            expect(container.querySelector('[aria-label="Close configuration"]')).toBeNull();
            expect(container.querySelector('[role="region"]')?.getAttribute('aria-labelledby')).toBe('cbm-config-title');
            const tab = new KeyboardEvent('keydown', { key: 'Tab', bubbles: true, cancelable: true });
            await act(async () => { button('Reload values').dispatchEvent(tab); input().dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true })); });
            expect(tab.defaultPrevented).toBe(false);
            expect(close).not.toHaveBeenCalled();
        } finally { outside.remove(); }
    });
    it('retains embedded drafts when hidden and preserves explicit discard and save behavior', async () => {
        service.saveConfiguration.mockResolvedValue({ ...snapshot({ override: '12', value: '12', pendingRestart: true }), revision: '2' });
        await render(true); await edit('8');
        const field = input();
        await render(true, true); await render(true);
        expect(input()).toBe(field);
        expect(input().value).toBe('8');
        expect(service.configuration).toHaveBeenCalledOnce();
        expect(service.saveConfiguration).not.toHaveBeenCalled();
        await click('Discard edits');
        expect(input().value).toBe('4');
        expect(button('Save changes').disabled).toBe(true);
        await edit('12'); await click('Save changes');
        expect(service.saveConfiguration).toHaveBeenCalledExactlyOnceWith('1', { CBM_WORKERS: '12' });
        expect(container.textContent).toContain('Restart pending');
        expect(close).not.toHaveBeenCalled();
    });
    it('loads only when activated and refreshes clean settings changed in another System page', async () => {
        await render(true, true, false);
        expect(service.configuration).not.toHaveBeenCalled();
        await render(true);
        expect(input().value).toBe('4');
        await render(true, true, false);
        service.configuration.mockResolvedValue({ ...snapshot({ value: '12', override: '12' }), revision: '2' });
        await render(true);
        expect(input().value).toBe('12');
        expect(service.configuration).toHaveBeenCalledTimes(2);
    });
    it('keeps unsaved drafts and their revision when returning from another System page', async () => {
        service.saveConfiguration.mockRejectedValue(new Error('HTTP 409: configuration changed'));
        await render(true); await edit('8');
        await render(true, true, false);
        service.configuration.mockResolvedValue({ ...snapshot({ value: '12', override: '12' }), revision: '2' });
        await render(true);
        expect(input().value).toBe('8');
        expect(service.configuration).toHaveBeenCalledOnce();
        await click('Save changes');
        expect(service.saveConfiguration).toHaveBeenCalledExactlyOnceWith('1', { CBM_WORKERS: '8' });
        expect(input().value).toBe('8');
        expect(container.textContent).toContain('409');
    });
    it('keeps noncanonical inherited inputs visible without inventing a resolved value', async () => {
        service.configuration.mockResolvedValue(snapshot({ value: '3junk', effective: '3junk', effectiveKnown: false }));
        await render();
        expect(container.textContent).toContain('Runtime interprets 3junk');
        expect(container.textContent).not.toContain('Using Automatic');
        expect(input().value).toBe('');
        await edit('4');
        expect(button('Save changes').disabled).toBe(false);
    });
    it('stages an override, persists once, and distinguishes saved from running', async () => {
        service.saveConfiguration.mockResolvedValue({ ...snapshot({ override: '8', value: '8', pendingRestart: true }), revision: '2' });
        await render(); await edit('8');
        expect(service.saveConfiguration).not.toHaveBeenCalled();
        expect(container.textContent).toContain('Using 4');
        await click('Save changes');
        expect(service.saveConfiguration).toHaveBeenCalledExactlyOnceWith('1', { CBM_WORKERS: '8' });
        expect(container.textContent).toContain('Restart pending');
        expect(container.textContent).toContain('Using 4');
        expect(container.textContent).toContain('Restart CBM');
        expect(button('Save changes').disabled).toBe(true);
    });
    it('resets an override with null rather than replacing it with a default', async () => {
        service.configuration.mockResolvedValue(snapshot({ override: '8', value: '8', effective: '8', source: 'override' }));
        service.saveConfiguration.mockResolvedValue(snapshot({ pendingRestart: true, effective: '8' }));
        await render(); await click('Reset'); await click('Save changes');
        expect(service.saveConfiguration).toHaveBeenCalledWith('1', { CBM_WORKERS: null });
    });
    it('blocks invalid values before writing and retains drafts on a server conflict', async () => {
        service.saveConfiguration.mockRejectedValue(new Error('HTTP 409: configuration changed'));
        await render(); await edit('999');
        expect(button('Save changes').disabled).toBe(true);
        await edit('8'); await click('Save changes');
        expect(input().value).toBe('8');
        expect(container.textContent).toContain('409');
        expect(container.textContent).toContain('1 unsaved change');
    });
    it('does not close or open model controls while daemon edits would be lost', async () => {
        await render(); await edit('8');
        await act(async () => container.querySelector<HTMLButtonElement>('[aria-label="Close configuration"]')!.click());
        expect(close).not.toHaveBeenCalled();
        await click('Keep editing'); await click('Browser'); await click('Configure local agent →');
        expect(model).not.toHaveBeenCalled();
        await click('Discard edits'); await click('Configure local agent →');
        expect(model).toHaveBeenCalledOnce();
    });
    it('keeps browser preferences usable without a compatible daemon', async () => {
        service.configuration.mockRejectedValue(new Error('/api/config HTTP 404'));
        await render();
        expect(container.textContent).toContain('404');
        const nodes = [...container.querySelectorAll<HTMLSelectElement>('select')].find(item => item.parentElement?.textContent?.startsWith('Node limit'))!;
        await act(async () => { nodes.value = '10000'; nodes.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(JSON.parse(window.localStorage.getItem(viewPreferencesKey('config-test'))!).preferences.galaxyNodes).toBe(10000);
        expect(service.saveConfiguration).not.toHaveBeenCalled();
    });
    it('renders immutable inputs as references without a writable control', async () => {
        service.configuration.mockResolvedValue(snapshot({ editable: false, readOnlyReason: 'Bootstrap input' }));
        await render();
        expect(input()).toBeNull();
        expect(container.textContent).toContain('Bootstrap input');
        await click('External inputs');
        expect(container.textContent).toContain('.cbmignore');
        expect(container.textContent).toContain('not editable overrides');
    });
    it('prevents overlapping save requests', async () => {
        let resolve!: (value: ConfigSnapshot) => void;
        service.saveConfiguration.mockImplementation(() => new Promise(done => { resolve = done; }));
        await render(); await edit('8');
        await act(async () => { button('Save changes').click(); button('Save changes')?.click(); });
        expect(service.saveConfiguration).toHaveBeenCalledOnce();
        await act(async () => resolve(snapshot({ override: '8' })));
    });
});

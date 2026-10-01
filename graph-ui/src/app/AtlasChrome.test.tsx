// @vitest-environment jsdom
/*
 * Das Chrome in jsdom: die Testmarken, die der Beweislauf im Browser sucht,
 * der Wortlaut der Kopfzeile und die Stellen, an denen die Oberflaeche zugibt,
 * dass sie etwas noch nicht kann.
 *
 * Monaco wird hier nicht geladen: der Reader kommt als Kind herein. Das ist der
 * ganze Grund fuer den Schnitt zwischen AtlasChrome und App.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import AtlasChrome, { splitMenuLabel } from './AtlasChrome';
import type { AtlasChromeProps } from './AtlasChrome';
import type { TreeRow } from './tree-model';

let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
});

afterEach(async () => {
    await act(async () => {
        root.unmount();
    });
    container.remove();
});

const fileRow: TreeRow = {
    name: 'userService.ts',
    path: 'src/services/userService.ts',
    kind: 'file',
    symbols: 6,
    files: 1,
    depth: 1,
    expanded: false,
    loaded: true,
};

function props(overrides: Partial<AtlasChromeProps> = {}): AtlasChromeProps {
    return {
        version: 'v0.0.0-dirty',
        chips: [{ label: 'project', value: 'atlas-sample' }],
        tabs: [],
        onSelectTab: vi.fn(),
        onCloseTab: vi.fn(),
        tree: {
            projectName: 'atlas-sample',
            rows: [fileRow],
            cursor: 0,
            activePath: '',
            note: '18 files, 71 symbols from /api/tree',
            onCursorChange: vi.fn(),
            onOpen: vi.fn(),
            onToggle: vi.fn(),
            onKeyDown: vi.fn(),
        },
        breadcrumb: [],
        children: <div data-testid="fake-reader" />,
        truncationNote: '',
        commandValue: '',
        onCommandChange: vi.fn(),
        commandHint: 'type 2 letters to search by meaning',
        status: [{ label: 'server', value: 'ready', state: 'ok' }],
        ...overrides,
    };
}

async function render(next: AtlasChromeProps): Promise<void> {
    await act(async () => {
        root.render(<AtlasChrome {...next} />);
    });
}

const testId = (id: string): HTMLElement | null => container.querySelector(`[data-testid="${id}"]`);

describe('AtlasChrome', () => {

    it('leaves general configuration out of the top bar', async () => {
        const onOpenConfig = vi.fn();
        await render(props({ onOpenConfig, configOpen: true, onOpenBrowserAi: vi.fn(), projectSwitcher: <button>Project</button> }));
        expect(testId('atlas-header')?.querySelector('[aria-label="Open configuration"]')).toBeNull();
        expect(testId('atlas-header')?.lastElementChild?.getAttribute('aria-label')).toBe('Local agent');
        expect(onOpenConfig).not.toHaveBeenCalled();
    });

    it('shows the selected model subtly in the agent status button and opens its configuration directly', async () => {
        const settings = vi.fn(), toggle = vi.fn();
        await render(props({ agentState: 'active', agentModelName: 'Qwen 3.5 0.8B', onOpenBrowserAi: settings, onToggleBrowserAi: toggle }));
        const button = container.querySelector<HTMLButtonElement>('[aria-label="Local agent settings"]')!;
        expect(button.textContent).toContain('Agent active');
        expect(button.querySelector('small')?.textContent).toBe('Qwen 3.5 0.8B');
        await act(async () => button.click());
        expect(settings).toHaveBeenCalledOnce();
        expect(toggle).not.toHaveBeenCalled();
        expect(container.querySelector('[role="menu"]')).toBeNull();
    });

    it('omits the model label when no model is selected', async () => {
        await render(props({ agentState: 'off', onOpenBrowserAi: vi.fn() }));
        expect(container.querySelector('.atlas-agent-model-name')).toBeNull();
    });

    it.each([['off', 'Enable agent'], ['active', 'Agent active'], ['busy', 'Agent working'], ['loading', 'Agent loading'], ['error', 'Agent error']] as const)('shows truthful local agent state %s and opens settings beside the project', async (agentState, label) => {
        const settings = vi.fn();
        await render(props({ agentState, onOpenBrowserAi: settings, projectSwitcher: <button>Project</button> }));
        const button = container.querySelector<HTMLButtonElement>('[aria-label="Local agent settings"]')!;
        expect(button.textContent).toBe(label);
        expect(button.parentElement?.previousElementSibling?.textContent).toBe('Project');
        await act(async () => button.click());
        expect(settings).toHaveBeenCalledOnce();
    });

    it.each(['explore', 'architecture', 'galaxy', 'system', 'adr', 'coverage', 'agents'] as const)('groups independent configuration and chat controls in the top bar for %s', async workspace => {
        const settings = vi.fn(), toggle = vi.fn();
        const next = (chatOpen: boolean) => props({ workspace, chatOpen, onOpenBrowserAi: settings, onToggleBrowserAi: toggle, chatDock: <input aria-label="Chat draft" defaultValue="conversation draft" /> });
        await render(next(false));
        const group = testId('atlas-header')?.querySelector('[role="group"][aria-label="Local agent"]');
        expect(group?.querySelectorAll('button')).toHaveLength(2);
        const draft = container.querySelector<HTMLInputElement>('[aria-label="Chat draft"]')!;
        draft.value = 'retained draft';
        const settingsButton = group?.querySelector<HTMLButtonElement>('[aria-label="Local agent settings"]')!;
        const openButton = group?.querySelector<HTMLButtonElement>('[aria-label="Open chat"]')!;
        expect(openButton.getAttribute('aria-expanded')).toBe('false');
        await act(async () => settingsButton.click());
        expect(settings).toHaveBeenCalledOnce();
        expect(toggle).not.toHaveBeenCalled();
        await act(async () => openButton.click());
        expect(toggle).toHaveBeenCalledOnce();
        expect(settings).toHaveBeenCalledOnce();
        await render(next(true));
        const hideButton = testId('atlas-header')?.querySelector<HTMLButtonElement>('[aria-label="Hide chat"]')!;
        expect(hideButton.getAttribute('aria-expanded')).toBe('true');
        await act(async () => hideButton.click());
        expect(toggle).toHaveBeenCalledTimes(2);
        expect(settings).toHaveBeenCalledOnce();
        await render(next(false));
        expect(container.querySelector('[aria-label="Chat draft"]')).toBe(draft);
        expect(draft.value).toBe('retained draft');
        expect(container.querySelector('.atlas-agent-reopen')).toBeNull();
        expect(container.querySelector('[role="menu"]')).toBeNull();
    });

    it('traegt jede Testmarke, an der der Beweislauf das Chrome erkennt', async () => {
        await render(props());
        for (const id of ['atlas-header', 'atlas-tabs',
            'atlas-statusbar', 'atlas-tree', 'atlas-breadcrumb']) {
            expect(testId(id), `${id} fehlt`).not.toBeNull();
        }
    });

    it('zeigt Marke und Versions-Chip in der Kopfzeile', async () => {
        await render(props());
        expect(container.querySelector('.atlas-brand')?.textContent).toBe('CODEATLAS');
        expect(testId('atlas-version')?.textContent).toBe('v0.0.0-dirty');
    });

    it('keeps project switching at the end of the header without a duplicate project chip', async () => {
        await render(props({
            chips: [{ label: 'project', value: 'atlas-sample' }, { label: 'sym', value: '76' }],
            ...{ projectSwitcher: <button type="button">Switch project: atlas-sample</button> },
        }));
        const header = testId('atlas-header');
        expect(header?.lastElementChild?.textContent).toBe('Switch project: atlas-sample');
        expect(header?.querySelector('[data-chip="project"]')).toBeNull();
        expect(header?.querySelector('[data-chip="sym"]')?.textContent).toContain('76');
    });

    /*
     * Der Zustand des Arbeitsbaums steht neben der Fassung, nicht in ihr.
     *
     * Der Chip beantwortet "welche Fassung ist das" und `v1.0.0-dirty` waere
     * darauf eine Antwort, die es als Release nicht gibt. Ohne Zusatz steht
     * daneben nichts: ein leeres Element waere ein Platz, an dem jemand eine
     * Angabe vermutet.
     */
    it('haelt den Bau-Zusatz getrennt vom Versions-Chip', async () => {
        await render(props({ version: 'v1.0.0', buildSuffix: 'dirty' }));
        expect(testId('atlas-version')?.textContent).toBe('v1.0.0');
        expect(testId('atlas-version-suffix')?.textContent).toBe('dirty');
    });

    it('zeigt gar kein Zusatz-Element, wenn der Baum sauber war', async () => {
        await render(props({ version: 'v1.0.0', buildSuffix: '' }));
        expect(testId('atlas-version')?.textContent).toBe('v1.0.0');
        expect(testId('atlas-version-suffix')).toBeNull();
    });

    it.each(['on', 'off'] as const)('keeps removed tool menus absent even when legacy actions are supplied (%s)', async state => {
        const onSelect = vi.fn(); const extra = vi.fn(); const help = vi.fn();
        await render(props({ menus: {
            a: { title: 'Legacy tools', state, onSelect, extras: [
                { key: 'why', label: '[w]hy am I here', title: 'Where to start', onSelect: extra },
                { key: 'bug', label: '[b]ug hunt', title: 'Bug hunt', onSelect: extra },
                { key: 'impact', label: '[c]hange scope', title: 'Impact', onSelect: extra },
                { key: 'llm', label: '[l]lm off', title: 'Model', onSelect: extra },
            ] },
            '?': { title: 'Help', state, onSelect: help },
        } }));
        expect(testId('atlas-menu')).toBeNull();
        expect(testId('atlas-menu-legend')).toBeNull();
        expect(container.querySelector('[data-menu]')).toBeNull();
        expect(container.querySelector('.atlas-tools-menu')).toBeNull();
        expect(testId('fake-reader')).not.toBeNull();
        expect(onSelect).not.toHaveBeenCalled();
        expect(extra).not.toHaveBeenCalled();
        expect(help).not.toHaveBeenCalled();
    });

    it('laesst ein Etikett ohne Klammer ganz stehen, statt eine zu erfinden', () => {
        expect(splitMenuLabel('[w]hy am I here')).toEqual({ key: 'w', rest: 'hy am I here' });
        expect(splitMenuLabel('[l]lm off')).toEqual({ key: 'l', rest: 'lm off' });
        expect(splitMenuLabel('plain')).toEqual({ key: '', rest: 'plain' });
    });

    it('does not render a global command bar, search dialog or legacy search results', async () => {
        const onCommandChange = vi.fn();
        await render(props({ onCommandChange, commandValue: 'old query', commandOverlay: <div data-testid="fake-overlay" /> }));
        expect(testId('atlas-command')).toBeNull();
        expect(testId('atlas-command-input')).toBeNull();
        expect(testId('fake-overlay')).toBeNull();
        expect(container.querySelector('dialog')).toBeNull();
        expect(container.querySelector('.atlas-command-hint')).toBeNull();
        expect(onCommandChange).not.toHaveBeenCalled();
    });

    it('preserves the reader and galaxy when obsolete command props are supplied', async () => {
        await render(props({ commandOverlay: <div data-testid="fake-overlay" />,
            twin: <aside data-testid="fake-twin" />, galaxy: <section data-testid="fake-galaxy" /> }));
        expect(testId('fake-reader')).not.toBeNull();
        expect(testId('fake-overlay')).toBeNull();
        const side = container.querySelector('.atlas-side');
        expect(side?.querySelector('[data-testid="fake-twin"]')).not.toBeNull();
        expect(side?.querySelector('[data-testid="fake-galaxy"]')).not.toBeNull();
    });

    it('baut keine dritte Spalte, wenn weder Twin noch Galaxie hereingereicht wurden', async () => {
        await render(props());
        expect(container.querySelector('.atlas-side')).toBeNull();
    });

    it('sagt ohne offene Datei, dass keine offen ist, statt eine leere Zeile zu zeigen', async () => {
        await render(props());
        expect(testId('atlas-tabs')?.textContent).toContain('no file open');
        expect(testId('atlas-breadcrumb')?.textContent).toContain('no file open');
    });

    it('schreibt die Breadcrumb mit dem Trenner des Vorbilds', async () => {
        await render(props({ breadcrumb: ['src', 'services', 'userService.ts'] }));
        const text = testId('atlas-breadcrumb')?.textContent ?? '';
        expect(text).toBe('src › services › userService.ts');
        expect(container.querySelector('.atlas-breadcrumb-leaf')?.textContent).toBe('userService.ts');
    });

    it('zeigt den aktiven Tab mit Punkt und meldet Auswahl und Schliessen', async () => {
        const onSelectTab = vi.fn();
        const onCloseTab = vi.fn();
        await render(props({
            tabs: [
                { path: 'src/types.ts', name: 'types.ts', active: false },
                { path: 'src/services/userService.ts', name: 'userService.ts', active: true },
            ],
            onSelectTab,
            onCloseTab,
        }));
        const tabs = testId('atlas-tabs')?.querySelectorAll('.atlas-tab') ?? [];
        expect(tabs).toHaveLength(2);
        expect(tabs[1]?.getAttribute('data-active')).toBe('true');
        expect(tabs[1]?.querySelector('.atlas-tab-dot')).not.toBeNull();

        await act(async () => {
            (tabs[0]?.querySelector('.atlas-tab-label') as HTMLButtonElement).click();
        });
        expect(onSelectTab).toHaveBeenCalledWith('src/types.ts');

        await act(async () => {
            (tabs[1]?.querySelector('.atlas-tab-close') as HTMLButtonElement).click();
        });
        expect(onCloseTab).toHaveBeenCalledWith('src/services/userService.ts');
    });

    /*
     * Die Tab-Leiste (W5c, Nutzerfeedback 2026-08-29).
     *
     * In jsdom faellt kein Layout an: `scrollWidth` und `clientWidth` sind
     * dort beide null, also kann hier nicht gemessen werden, dass die Leiste
     * ueberlaeuft. Das misst der Beweislauf im Browser. Geprueft wird hier,
     * was ein Browserlauf nur umstaendlich fragen koennte: dass die Leiste ein
     * eigener Bildlaufbereich mit Ueberlauf-Anzeigen ist, dass ein Rad-Ereignis
     * darin waagerecht bewegt und dass ein Tab seinen Pfad als Tooltip traegt.
     */
    it('gibt der Tab-Leiste einen eigenen Bildlaufbereich mit zwei Anzeigen', async () => {
        await render(props({
            tabs: [
                { path: 'src/types.ts', name: 'types.ts', active: true },
                { path: 'src/config.ts', name: 'config.ts', active: false },
            ],
        }));
        expect(testId('atlas-tabs-bar')).not.toBeNull();
        expect(testId('atlas-tabs')?.parentElement).toBe(testId('atlas-tabs-bar'));
        const marks = [...(testId('atlas-tabs-bar')?.querySelectorAll('[data-testid="atlas-tabs-overflow"]') ?? [])];
        expect(marks.map((mark) => mark.getAttribute('data-side'))).toEqual(['left', 'right']);
        // Ohne Ueberlauf leuchtet keine der beiden.
        expect(marks.every((mark) => mark.getAttribute('data-on') === 'false')).toBe(true);
    });

    it('nimmt eine Radumdrehung ueber der Leiste an sich, statt sie durchzulassen', async () => {
        await render(props({
            tabs: [{ path: 'src/types.ts', name: 'types.ts', active: true }],
        }));
        const bar = testId('atlas-tabs') as HTMLElement;
        // jsdom rechnet kein Layout: ohne diese zwei Zahlen weiss die Leiste
        // nicht, dass es etwas zu scrollen gibt, und laesst das Rad in Ruhe.
        Object.defineProperty(bar, 'scrollWidth', { value: 900, configurable: true });
        Object.defineProperty(bar, 'clientWidth', { value: 300, configurable: true });
        const wheel = new WheelEvent('wheel', { deltaY: 120, bubbles: true, cancelable: true });
        await act(async () => {
            bar.dispatchEvent(wheel);
        });
        // Abbestellt heisst: die Leiste bewegt sich genau einmal, naemlich hier,
        // und nicht zusaetzlich noch einmal durch die Vorgabe der Engine.
        expect(wheel.defaultPrevented).toBe(true);
    });

    it('laesst das Rad in Ruhe, solange nichts ueberlaeuft', async () => {
        await render(props({
            tabs: [{ path: 'src/types.ts', name: 'types.ts', active: true }],
        }));
        const bar = testId('atlas-tabs') as HTMLElement;
        const wheel = new WheelEvent('wheel', { deltaY: 120, bubbles: true, cancelable: true });
        await act(async () => {
            bar.dispatchEvent(wheel);
        });
        expect(wheel.defaultPrevented).toBe(false);
    });

    it('haengt den ganzen Pfad an den Tab, weil der Name allein mehrdeutig ist', async () => {
        await render(props({
            tabs: [{ path: 'src/services/userService.ts', name: 'userService.ts', active: true }],
        }));
        expect(testId('atlas-tab')?.getAttribute('data-hint')).toBe('src/services/userService.ts');
    });

    it('zeigt die Kappungszeile nur, wenn es etwas zu melden gibt', async () => {
        await render(props());
        expect(testId('atlas-truncation')).toBeNull();
        await render(props({ truncationNote: 'lines 501-718 not loaded: server snippet cap' }));
        expect(testId('atlas-truncation')?.textContent).toContain('501-718');
        expect(testId('atlas-truncation')?.textContent).toContain('incomplete');
    });

    it('stellt die Coverage-Notiz ueber den Editor und nicht darunter', async () => {
        await render(props());
        expect(testId('atlas-coverage-note')).toBeNull();
        await render(props({
            coverageNote: {
                state: 'partially parsed',
                text: 'src/broken.ts is only partially parsed: constructs inside lines 12-18 may be missing.',
            },
        }));
        const note = testId('atlas-coverage-note');
        expect(note?.textContent).toContain('12-18');
        expect(note?.getAttribute('data-coverage')).toBe('partially parsed');
        // Ueber dem Editor: die Notiz qualifiziert den Text, den man gleich
        // liest, und muss vor dem ersten Zeichen dastehen.
        const reader = testId('atlas-reader');
        const position = note === null || reader === null
            ? 0
            : note.compareDocumentPosition(reader);
        expect(position & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    });

    it('faerbt eine Abwesenheit anders als eine Zahl', async () => {
        await render(props({
            chips: [
                { label: 'project', value: 'no project', state: 'absent' },
                { label: 'sym', value: '76' },
            ],
        }));
        const absent = container.querySelector('[data-chip="project"]');
        expect(absent?.getAttribute('data-state')).toBe('absent');
        expect(container.querySelector('[data-chip="sym"]')?.getAttribute('data-state')).toBe('plain');
    });

    it('reicht die Reader-Flaeche durch, ohne sie zu kennen', async () => {
        await render(props());
        expect(testId('atlas-reader')?.querySelector('[data-testid="fake-reader"]')).not.toBeNull();
    });

    it('only reserves an explanation splitter while a tool is open, preserving the reader', async () => {
        const reader = <input data-testid="preserved-reader" defaultValue="selected source" />;
        const next = props({ children: reader, splitExplain: <div data-testid="explanation-splitter" /> });
        await render(next);
        const mountedReader = testId('preserved-reader');
        expect(testId('explanation-splitter')).toBeNull();

        await render({ ...next, explain: <section data-testid="opened-tool">Flow</section> });
        expect(testId('explanation-splitter')).not.toBeNull();
        expect(testId('opened-tool')).not.toBeNull();
        expect(testId('preserved-reader')).toBe(mountedReader);

        await render(next);
        expect(testId('explanation-splitter')).toBeNull();
        expect(testId('opened-tool')).toBeNull();
        expect(testId('preserved-reader')).toBe(mountedReader);
        expect((mountedReader as HTMLInputElement).value).toBe('selected source');
    });
});

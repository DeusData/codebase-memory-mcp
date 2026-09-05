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
import AtlasChrome, { COMMAND_PLACEHOLDER, MENU_ITEMS, splitMenuLabel } from './AtlasChrome';
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

    it('traegt jede Testmarke, an der der Beweislauf das Chrome erkennt', async () => {
        await render(props());
        for (const id of ['atlas-header', 'atlas-menu', 'atlas-tabs', 'atlas-command',
            'atlas-statusbar', 'atlas-tree', 'atlas-breadcrumb']) {
            expect(testId(id), `${id} fehlt`).not.toBeNull();
        }
    });

    it('zeigt Marke und Versions-Chip in der Kopfzeile', async () => {
        await render(props());
        expect(container.querySelector('.atlas-brand')?.textContent).toBe('CODEATLAS');
        expect(testId('atlas-version')?.textContent).toBe('v0.0.0-dirty');
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

    /*
     * Die Menuezeile nach W7a (Nutzerauftrag 2026-08-29): sie zeichnet nur, was
     * eine Verdrahtung hat. Ein Punkt ohne Verdrahtung ist kein blasser Knopf
     * mit Tooltip mehr, sondern gar kein Knopf.
     */
    it('zeichnet nur Menuepunkte, hinter denen etwas liegt', async () => {
        const onSelect = vi.fn();
        await render(props({
            menus: { a: { title: 'atlas: hide the galaxy panel (alt+a)', state: 'on', onSelect } },
        }));
        const buttons = [...(testId('atlas-menu')?.querySelectorAll('button') ?? [])];
        expect(buttons.map((button) => button.getAttribute('data-menu'))).toEqual(['a']);
        expect(buttons).toHaveLength(1);
        expect(MENU_ITEMS.length).toBeGreaterThan(1);
        expect(testId('atlas-menu')?.textContent).toContain('[a]tlas');
        expect(testId('atlas-menu')?.textContent).not.toContain('[?]help');
    });

    it('sagt in der Zeile, dass die Klammer Alt verlangt', async () => {
        await render(props());
        expect(testId('atlas-menu-legend')?.textContent).toContain('alt');
    });

    it('macht die verdrahteten Menuepunkte anklickbar', async () => {
        const onSelect = vi.fn();
        await render(props({
            menus: { a: { title: 'atlas: hide the galaxy panel (alt+a)', state: 'on', onSelect } },
        }));
        const atlas = container.querySelector('[data-menu="a"]') as HTMLButtonElement;
        expect(atlas.getAttribute('aria-disabled')).toBeNull();
        expect(atlas.getAttribute('data-state')).toBe('on');
        expect(atlas.getAttribute('aria-pressed')).toBe('true');
        expect(atlas.getAttribute('data-hint') ?? '').toContain('galaxy');
        await act(async () => {
            atlas.click();
        });
        expect(onSelect).toHaveBeenCalledTimes(1);
    });

    /*
     * Die Menuezeile nach W7b (Nutzerbefund 2026-08-29, Screenshot): `[a]tlas`
     * und `[?]help` trugen einen Rahmen, die vier Eintraege dazwischen einen
     * gepunkteten Unterstrich in kleinerer Schrift, und dadurch sahen sie aus
     * wie Beschriftungen zwischen zwei Bedienelementen. Sie sind aber alle
     * dasselbe, also sehen sie jetzt auch so aus. Geprueft wird die Struktur,
     * nicht die Farbe: eine Klasse, ein Element, ein Buchstabe in Klammern.
     */
    it('gibt den fensterweiten Eintraegen dieselbe Gestalt', async () => {
        const acts = Object.fromEntries(
            ['a', 'why', 'bug', 'impact', 'llm', 'help'].map((key) => [key, vi.fn()]),
        );
        await render(props({
            menus: {
                a: {
                    title: 'atlas: hide the galaxy panel (alt+a)',
                    state: 'on',
                    onSelect: acts['a'],
                    extras: [
                        { key: 'why', label: '[w]hy am I here', title: 'w', onSelect: acts['why'] },
                        { key: 'bug', label: '[b]ug hunt', title: 'b', onSelect: acts['bug'] },
                        { key: 'impact', label: '[c]hange scope', title: 'c', onSelect: acts['impact'] },
                        { key: 'llm', label: '[l]lm off', title: 'l', onSelect: acts['llm'] },
                    ],
                },
                '?': { title: 'help', state: 'off', onSelect: acts['help'] },
            },
        }));
        const entries = [...(testId('atlas-menu')?.querySelectorAll('[data-menu]') ?? [])];
        expect(entries.map((node) => node.getAttribute('data-menu')))
            .toEqual(['a', 'a-why', 'a-llm', '?']);
        for (const entry of entries) {
            expect(entry.tagName, entry.getAttribute('data-menu') ?? '').toBe('BUTTON');
            expect(entry.getAttribute('class'), entry.getAttribute('data-menu') ?? '')
                .toBe('atlas-menu-item');
            expect(entry.querySelector('.atlas-menu-key')?.textContent, entry.textContent ?? '')
                .toMatch(/^\[[a-z?]\]$/);
        }
    });

    it('laesst jeden sichtbaren Eintrag mit der Maus ausloesen', async () => {
        const acts = Object.fromEntries(
            ['a', 'why', 'bug', 'impact', 'llm', 'help'].map((key) => [key, vi.fn()]),
        );
        await render(props({
            menus: {
                a: {
                    title: 'atlas',
                    state: 'on',
                    onSelect: acts['a'],
                    extras: [
                        { key: 'why', label: '[w]hy am I here', title: 'w', onSelect: acts['why'] },
                        { key: 'bug', label: '[b]ug hunt', title: 'b', onSelect: acts['bug'] },
                        { key: 'impact', label: '[c]hange scope', title: 'c', onSelect: acts['impact'] },
                        { key: 'llm', label: '[l]lm off', title: 'l', onSelect: acts['llm'] },
                    ],
                },
                '?': { title: 'help', state: 'off', onSelect: acts['help'] },
            },
        }));
        for (const entry of [...(testId('atlas-menu')?.querySelectorAll('[data-menu]') ?? [])]) {
            await act(async () => {
                (entry as HTMLButtonElement).click();
            });
        }
        for (const [name, act_] of Object.entries(acts)) {
            expect(act_, name).toHaveBeenCalledTimes(name === 'bug' || name === 'impact' ? 0 : 1);
        }
    });

    it('laesst ein Etikett ohne Klammer ganz stehen, statt eine zu erfinden', () => {
        expect(splitMenuLabel('[w]hy am I here')).toEqual({ key: 'w', rest: 'hy am I here' });
        expect(splitMenuLabel('[l]lm off')).toEqual({ key: 'l', rest: 'lm off' });
        expect(splitMenuLabel('plain')).toEqual({ key: '', rest: 'plain' });
    });

    it('haelt die Kommandozeile fokussierbar und zeigt Platzhalter und Hinweis', async () => {
        const onCommandChange = vi.fn();
        await render(props({ onCommandChange }));
        const input = testId('atlas-command-input') as HTMLInputElement;
        expect(input.placeholder).toBe(COMMAND_PLACEHOLDER);
        expect(input.placeholder).toContain('type a command or ask the atlas');
        input.focus();
        expect(document.activeElement).toBe(input);
        expect(container.querySelector('.atlas-command-hint')?.textContent)
            .toContain('search by meaning');
    });

    /*
     * AC10 (Nutzer-Screenshot 2026-08-29): der native Tooltip legte sich beim
     * Zeigen ueber den Anfang der Zeile, also ueber den Text, den der Leser
     * gerade tippt. Was er sagte, steht jetzt in der Hilfe.
     */
    it('haengt keinen nativen Tooltip mehr an die Zeile', async () => {
        await render(props());
        expect((testId('atlas-command-input') as HTMLInputElement).hasAttribute('title')).toBe(false);
        expect(testId('atlas-command')?.hasAttribute('title')).toBe(false);
    });

    /*
     * AC9: die Zeile sagt, ob sie den Fokus hat. Ohne das war der Zustand, in
     * dem Tippen frueher ins Leere lief, unsichtbar.
     */
    it('zeigt im DOM, ob die Zeile den Fokus hat', async () => {
        await render(props());
        expect(testId('atlas-command')?.getAttribute('data-focused')).toBe('false');
        const input = testId('atlas-command-input') as HTMLInputElement;
        await act(async () => {
            input.focus();
        });
        expect(testId('atlas-command')?.getAttribute('data-focused')).toBe('true');
        await act(async () => {
            input.blur();
        });
        expect(testId('atlas-command')?.getAttribute('data-focused')).toBe('false');
    });

    it('reicht Tasten der Kommandozeile nach aussen und laesst Escape nur los, wenn niemand sie braucht', async () => {
        const handled = vi.fn((event: { preventDefault: () => void }) => event.preventDefault());
        await render(props({ onCommandKeyDown: handled as never }));
        const input = testId('atlas-command-input') as HTMLInputElement;
        input.focus();
        await act(async () => {
            input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
        });
        expect(handled).toHaveBeenCalled();
        // Der Aufrufer hat Escape verbraucht, also bleibt der Fokus stehen.
        expect(document.activeElement).toBe(input);

        await render(props());
        const plain = testId('atlas-command-input') as HTMLInputElement;
        plain.focus();
        await act(async () => {
            plain.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
        });
        expect(document.activeElement).not.toBe(plain);
    });

    it('zeigt das Suchfenster in der Kommandozeile und die Galaxie neben dem Twin', async () => {
        await render(props({
            commandOverlay: <div data-testid="fake-overlay" />,
            twin: <aside data-testid="fake-twin" />,
            galaxy: <section data-testid="fake-galaxy" />,
        }));
        expect(testId('atlas-command')?.querySelector('[data-testid="fake-overlay"]')).not.toBeNull();
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
});

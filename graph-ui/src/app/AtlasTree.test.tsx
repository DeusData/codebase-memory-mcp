// @vitest-environment jsdom
/*
 * Der Explorer in jsdom: Zeilen, Pfeil-Zeichen, aktive Datei, Klickwege.
 *
 * Die Tastenlogik selbst ist eine reine Funktion und wird in tree-model.test.ts
 * geprueft; hier wird nur bewiesen, dass die Liste sie ueberhaupt zu sehen
 * bekommt, also dass sie den Fokus nehmen kann und ihre Tasten weiterreicht.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import AtlasTree, { twistyFor } from './AtlasTree';
import type { AtlasTreeProps } from './AtlasTree';
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

const dir = (path: string, expanded: boolean, depth = 0): TreeRow => ({
    name: path.split('/').pop() ?? path,
    path,
    kind: 'dir',
    symbols: 10,
    files: 2,
    depth,
    expanded,
    loaded: true,
});

const file = (path: string, depth = 1): TreeRow => ({
    name: path.split('/').pop() ?? path,
    path,
    kind: 'file',
    symbols: 6,
    files: 1,
    depth,
    expanded: false,
    loaded: true,
});

function props(overrides: Partial<AtlasTreeProps> = {}): AtlasTreeProps {
    return {
        projectName: 'atlas-sample',
        rows: [dir('src', true), file('src/services/userService.ts')],
        cursor: 0,
        activePath: '',
        note: '18 files, 71 symbols from /api/tree',
        onCursorChange: vi.fn(),
        onOpen: vi.fn(),
        onToggle: vi.fn(),
        onKeyDown: vi.fn(),
        ...overrides,
    };
}

async function render(next: AtlasTreeProps): Promise<void> {
    await act(async () => {
        root.render(<AtlasTree {...next} />);
    });
}

const rows = (): HTMLButtonElement[] =>
    [...container.querySelectorAll<HTMLButtonElement>('[data-testid="atlas-tree-row"]')];

describe('AtlasTree', () => {

    it('zeigt EXPLORER, den Projektnamen als Wurzel und eine Zeile je Eintrag', async () => {
        await render(props());
        expect(container.querySelector('.atlas-tree-title')?.textContent).toBe('EXPLORER');
        expect(container.querySelector('[data-root="true"]')?.textContent).toContain('atlas-sample/');
        expect(rows()).toHaveLength(2);
    });

    it('haengt Ordnern einen Schraegstrich an und Dateien keinen', async () => {
        await render(props());
        expect(rows()[0]?.textContent).toContain('src/');
        expect(rows()[1]?.textContent).toContain('userService.ts');
        expect(rows()[1]?.textContent).not.toContain('userService.ts/');
    });

    it('zeigt den Zustand eines Ordners am Pfeil-Zeichen', () => {
        expect(twistyFor(dir('src', false))).toBe('▸');
        expect(twistyFor(dir('src', true))).toBe('▾');
        expect(twistyFor(file('a.ts'))).toBe(' ');
    });

    it('markiert die Datei, die gerade im Reader steht', async () => {
        await render(props({ activePath: 'src/services/userService.ts' }));
        expect(rows()[1]?.getAttribute('data-active')).toBe('true');
        expect(rows()[0]?.getAttribute('data-active')).toBe('false');
    });

    it('oeffnet eine Datei per Klick und klappt einen Ordner per Klick', async () => {
        const onOpen = vi.fn();
        const onToggle = vi.fn();
        const onCursorChange = vi.fn();
        await render(props({ onOpen, onToggle, onCursorChange }));

        await act(async () => {
            rows()[1]?.click();
        });
        expect(onOpen).toHaveBeenCalledWith(expect.objectContaining({ path: 'src/services/userService.ts' }));
        expect(onCursorChange).toHaveBeenCalledWith(1);

        await act(async () => {
            rows()[0]?.click();
        });
        expect(onToggle).toHaveBeenCalledWith(expect.objectContaining({ path: 'src' }));
    });

    it('nimmt den Fokus auf die Liste und reicht Tasten weiter', async () => {
        const onKeyDown = vi.fn();
        await render(props({ onKeyDown }));
        const list = container.querySelector<HTMLUListElement>('.atlas-tree-list');
        expect(list?.getAttribute('tabindex')).toBe('0');
        list?.focus();
        expect(document.activeElement).toBe(list);
        await act(async () => {
            list?.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true }));
        });
        expect(onKeyDown).toHaveBeenCalled();
    });

    it('rueckt tiefere Ebenen ein, statt sie flach zu zeigen', async () => {
        await render(props());
        expect(rows()[0]?.style.paddingLeft).toBe('8px');
        expect(rows()[1]?.style.paddingLeft).toBe('20px');
    });

    it('zeigt die Herkunftszeile des Baums und faerbt eine Abwesenheit', async () => {
        await render(props());
        expect(container.querySelector('.atlas-tree-note')?.textContent).toContain('/api/tree');
        await render(props({ note: 'tree unavailable: HTTP 404', noteIsAbsence: true }));
        expect(container.querySelector('.atlas-tree-note')?.getAttribute('data-state')).toBe('absent');
    });
});

/*
 * Die Coverage-Stufen im Baum.
 *
 * Geprueft wird, was ein Leser sieht: das Attribut, an dem die CSS-Regel
 * haengt, das Zeichen neben dem Namen und der Tooltip. Die Farbe selbst prueft
 * der Beweislauf im Browser; jsdom rechnet keine Kaskade.
 */

const withCoverage = (row: TreeRow, coverage: TreeRow['coverage'], reason?: string): TreeRow => ({
    ...row,
    coverage,
    ...(reason === undefined ? {} : { coverageReason: reason }),
});

describe('AtlasTree und die Coverage-Stufen', () => {

    const coveredRows = [
        withCoverage(dir('src', true), 'partial'),
        withCoverage(file('src/broken.ts'), 'partial', '12-18'),
        withCoverage(file('src/clean.ts'), 'indexed'),
        withCoverage(file('assets/beleg.png'), 'skipped', 'unsupported extension'),
        withCoverage(file('secret.env'), 'not-indexed', '.gitignore'),
    ];

    it('traegt die Stufe an jeder Zeile als Attribut', async () => {
        await render(props({ rows: coveredRows }));
        expect(rows().map((row) => row.getAttribute('data-coverage'))).toEqual([
            'partial',
            'partial',
            'indexed',
            'skipped',
            'not-indexed',
        ]);
    });

    it('gibt Dateien ein Zeichen und Ordnern einen Punkt', async () => {
        await render(props({ rows: coveredRows }));
        const marks = [...container.querySelectorAll('[data-testid="atlas-tree-mark"]')];
        expect(marks.map((mark) => mark.textContent)).toEqual(['●', '!', 'x', '-']);
    });

    it('laesst eine vollstaendig indizierte Zeile ohne Zeichen', async () => {
        await render(props({ rows: [withCoverage(file('a.ts'), 'indexed')] }));
        expect(container.querySelector('[data-testid="atlas-tree-mark"]')).toBeNull();
    });

    /*
     * Der Status-Punkt (W5c, Nutzerfeedback 2026-08-29).
     *
     * Das Zeichen sagt WELCHE Stoerung, der Punkt sagt OB eine da ist, und der
     * Punkt steht immer da. Vorher war ein sauber indizierter Baum von einem
     * Baum ohne Coverage-Antwort nicht zu unterscheiden.
     */
    it('gibt jeder Datei ihren Status-Punkt, auch der indizierten', async () => {
        await render(props({ rows: coveredRows }));
        const dots = [...container.querySelectorAll('[data-testid="atlas-tree-dot"]')];
        // Vier Dateien, ein Ordner: der Ordner traegt weiter seinen Punkt als
        // Zeichen, weil seine Aussage eine ueber seinen Inhalt ist.
        expect(dots).toHaveLength(4);
        expect(dots.map((dot) => dot.getAttribute('data-coverage')))
            .toEqual(['partial', 'indexed', 'skipped', 'not-indexed']);
        expect(dots.map((dot) => dot.getAttribute('data-tone')))
            .toEqual(['partial', 'indexed', 'absent', 'absent']);
    });

    it('legt Status und Grund in den Tooltip des Punktes', async () => {
        await render(props({ rows: coveredRows }));
        /*
         * Der Satz haengt seit W8b an der ZEILE und nicht mehr am Punkt darin.
         * Beide trugen bis dahin denselben `title`; als eigener Tooltip waeren
         * das zwei Kaesten fuer eine Auskunft, die sich beim Zeigen auch noch
         * gegenseitig verdecken. Die Zusicherung ist dieselbe geblieben: Stufe
         * und Grund sind einen Zeiger entfernt, an der Zeile, die sie betreffen.
         */
        expect([...container.querySelectorAll('[data-testid="atlas-tree-dot"]')]).toHaveLength(4);
        const carrying = rows();
        expect(carrying[1]?.getAttribute('data-hint')).toContain('partially parsed');
        expect(carrying[1]?.getAttribute('data-hint')).toContain('12-18');
        expect(carrying[2]?.getAttribute('data-hint')).toContain('no source recorded an issue');
    });

    it('erklaert in der Legende auch den Gutfall, mit seinem Punkt', async () => {
        await render(props({ rows: coveredRows }));
        const good = container.querySelector('[data-testid="atlas-tree-legend-entry"][data-coverage="indexed"]');
        expect(good).not.toBeNull();
        expect(good?.querySelector('[data-testid="atlas-tree-legend-dot"]')?.getAttribute('data-tone'))
            .toBe('indexed');
        expect(good?.textContent).toContain('in the graph, no recorded issue');
    });

    it('sagt an einem Ordner, ob er aufgeklappt ist, und an einer Datei nichts', async () => {
        await render(props({ rows: coveredRows }));
        expect(rows()[0]?.getAttribute('data-expanded')).toBe('true');
        expect(rows()[1]?.hasAttribute('data-expanded')).toBe(false);
    });

    it('behandelt eine Zeile ohne Stufe wie eine indizierte, ohne es zu behaupten', async () => {
        await render(props({ rows: [file('a.ts')] }));
        expect(rows()[0]?.getAttribute('data-coverage')).toBe('indexed');
        expect(container.querySelector('[data-testid="atlas-tree-mark"]')).toBeNull();
    });

    it('legt den Grund des Servers in den Tooltip', async () => {
        await render(props({ rows: coveredRows }));
        expect(rows()[1]?.getAttribute('data-hint')).toContain('12-18');
        expect(rows()[3]?.getAttribute('data-hint')).toContain('unsupported extension');
        expect(rows()[4]?.getAttribute('data-hint')).toContain('.gitignore');
    });

    it('spricht im Tooltip eines Ordners ueber dessen Inhalt', async () => {
        await render(props({ rows: coveredRows }));
        expect(rows()[0]?.getAttribute('data-hint')).toContain('worst stage below this folder');
    });

    it('zeigt eine Legende mit den vorkommenden Stufen und dem Quellensatz', async () => {
        await render(props({ rows: coveredRows }));
        const legend = container.querySelector('[data-testid="atlas-tree-legend"]');
        expect(legend).not.toBeNull();
        const entries = [...container.querySelectorAll('[data-testid="atlas-tree-legend-entry"]')]
            .map((entry) => entry.getAttribute('data-coverage'));
        expect(entries).toEqual(['indexed', 'partial', 'not-indexed', 'skipped', 'folder']);
        expect(container.querySelector('[data-testid="atlas-tree-legend-source"]')?.textContent)
            .toContain('files it never met are invisible');
    });

    it('erklaert keine Stufe, die im Baum nicht vorkommt', async () => {
        await render(props({ rows: [withCoverage(file('a.ts'), 'indexed')] }));
        const entries = [...container.querySelectorAll('[data-testid="atlas-tree-legend-entry"]')]
            .map((entry) => entry.getAttribute('data-coverage'));
        expect(entries).toEqual(['indexed']);
    });

    it('zeigt jede gekappte Liste als eigene ehrliche Zeile', async () => {
        await render(props({
            rows: coveredRows,
            truncations: ['the server cut the skipped list: 812 recorded, fewer listed'],
        }));
        const cut = [...container.querySelectorAll('[data-testid="atlas-tree-truncation"]')];
        expect(cut).toHaveLength(1);
        expect(cut[0]?.textContent).toContain('812 recorded');
        expect(cut[0]?.getAttribute('data-state')).toBe('absent');
    });

    it('oeffnet eine uebersprungene Datei wie jede andere, statt sie zu sperren', async () => {
        // Was danach passiert, entscheidet der Reader. Eine Zeile, die auf
        // einen Klick nicht reagiert, waere eine tote Flaeche ohne Erklaerung.
        const onOpen = vi.fn();
        await render(props({ rows: coveredRows, onOpen }));
        await act(async () => {
            rows()[3]?.click();
        });
        expect(onOpen).toHaveBeenCalledWith(expect.objectContaining({ path: 'assets/beleg.png' }));
    });
});

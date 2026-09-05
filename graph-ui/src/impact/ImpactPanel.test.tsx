// @vitest-environment jsdom
/**
 * Die Aenderungsansicht in jsdom.
 *
 * Zwei Pruefungen tragen hier mehr als alle anderen: dass neben dem Wort die
 * erfuellten Regeln als Saetze stehen, und dass jede Behauptung der Erzaehlung
 * ihre eigene Evidenz-Zeile hat. Beides ist der Unterschied zwischen einem
 * Urteil, dem man widersprechen kann, und einem, das man glauben muss.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import ImpactPanel from './ImpactPanel';
import { buildComplexityLookup, mapChangeImpact } from './impact-model';
import type { ImpactModel } from './impact-model';
import type { ArchitectureOverviewDto, ChangeImpactDto } from '../core/intelligence-provider';

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

const CHANGE: ChangeImpactDto = {
    changedFiles: ['src/services/userService.ts'],
    impacted: [],
    walkedDistance: 2,
    symbols: [
        {
            name: 'createUser',
            qualifiedName: 'p.src.services.userService.createUser',
            filePath: 'src/services/userService.ts',
            line: 22,
            kind: 'function',
            changeKind: 'declared',
            distance: 0,
        },
        {
            name: 'registerUserRoutes',
            qualifiedName: 'p.src.routes.users.registerUserRoutes',
            filePath: 'src/routes/users.ts',
            line: 7,
            kind: 'function',
            changeKind: 'caller',
            distance: 1,
        },
    ],
};

const ARCHITECTURE: ArchitectureOverviewDto = {
    totalSymbols: 76,
    totalRelations: 178,
    symbolKinds: [],
    relationKinds: [],
    languages: [],
    groups: [],
    entryPoints: [],
    routes: [
        { method: 'GET', path: '/users', filePath: 'src/routes/users.ts', line: 8, origin: 'source' },
        { method: 'POST', path: '/users', filePath: 'src/routes/users.ts', line: 13, origin: 'source' },
    ],
    clusters: [],
    layers: [],
    boundaries: [],
    hotspots: [],
    files: [],
};

const MODEL: ImpactModel = mapChangeImpact(
    CHANGE,
    ARCHITECTURE,
    buildComplexityLookup([], ARCHITECTURE),
    { bySymbol: new Map(), checked: new Set(['p.src.services.userService.createUser']) },
);

function handlerSet() {
    return {
        onMode: vi.fn<(mode: 'worktree' | 'since-ref') => void>(),
        onRefDraft: vi.fn<(value: string) => void>(),
        onGo: vi.fn<() => void>(),
        onOpen: vi.fn<(target: { name: string }) => void>(),
        onClose: vi.fn<() => void>(),
    };
}

type Handlers = ReturnType<typeof handlerSet>;

async function render(over: {
    model?: ImpactModel;
    mode?: 'worktree' | 'since-ref';
    refError?: string;
    refDraft?: string;
} = {}): Promise<Handlers> {
    const handlers: Handlers = handlerSet();
    await act(async () => {
        root.render(
            <ImpactPanel
                project="codeatlasweb-w4b"
                mode={over.mode ?? 'worktree'}
                onMode={handlers.onMode}
                refDraft={over.refDraft ?? ''}
                onRefDraft={handlers.onRefDraft}
                onGo={handlers.onGo}
                refError={over.refError ?? ''}
                model={'model' in over ? over.model : MODEL}
                status={'model' in over && over.model === undefined ? 'loading' : 'ready'}
                message=""
                routeNote="Routes were read off the text of 2 indexed source files."
                onOpen={handlers.onOpen}
                onClose={handlers.onClose}
            />,
        );
    });
    return handlers;
}

const at = (testid: string): HTMLElement | null =>
    container.querySelector<HTMLElement>(`[data-testid="${testid}"]`);

const all = (testid: string): HTMLElement[] =>
    [...container.querySelectorAll<HTMLElement>(`[data-testid="${testid}"]`)];

describe('ImpactPanel', () => {

    it('shows at least five tiles, one per figure the model carries', async () => {
        await render();
        const tiles = all('atlas-impact-tile');
        expect(tiles.length).toBeGreaterThanOrEqual(5);
        expect(tiles.map((tile) => tile.getAttribute('data-tile'))).toContain('changed files');
        expect(tiles.map((tile) => tile.getAttribute('data-tile'))).toContain('untested affected');
    });

    it('puts the rules that fired beside the word, as sentences', async () => {
        await render();
        expect(at('atlas-impact-badge')?.textContent).toBe('MEDIUM');
        const rules = all('atlas-impact-badge-rule').map((node) => node.textContent ?? '');
        expect(rules.join(' ')).toContain('2 endpoints are registered in a file this change reaches.');
        expect(rules.join(' ')).toContain('No test caller was found for 1 affected symbol.');
    });

    it('gives every claim of the narrative its own evidence row', async () => {
        await render();
        const rows = all('atlas-impact-evidence');
        expect(rows.length).toBe(MODEL.narrative.evidence.length);
        for (const row of rows) {
            expect(row.getAttribute('data-source')).toMatch(/detect_changes|architecture|facts/);
            expect((row.textContent ?? '').length).toBeGreaterThan(20);
        }
    });

    it('draws the four lists and names the endpoints it reached', async () => {
        await render();
        expect(all('atlas-impact-direct-row')).toHaveLength(1);
        expect(all('atlas-impact-downstream-row')).toHaveLength(1);
        const endpoints = all('atlas-impact-endpoint').map((node) => node.getAttribute('data-endpoint'));
        expect(endpoints).toEqual(['GET /users', 'POST /users']);
        expect(all('atlas-impact-endpoint')[0].getAttribute('data-via')).toBe('file');
        expect(all('atlas-impact-untested')).toHaveLength(1);
    });

    it('opens a row through the caller and refuses a row with no file', async () => {
        const handlers = await render();
        const button = all('atlas-impact-direct-row')[0].querySelector('button')!;
        await act(async () => {
            button.click();
        });
        expect(handlers.onOpen).toHaveBeenCalledTimes(1);
        expect(handlers.onOpen.mock.calls[0][0].name).toBe('createUser');
    });

    it('shows the ref field only in the since-ref mode', async () => {
        await render();
        expect(at('atlas-impact-ref-input')).toBeNull();
        await render({ mode: 'since-ref' });
        expect(at('atlas-impact-ref-input')).not.toBeNull();
    });

    it('shows a refused ref inline and says nothing was asked', async () => {
        await render({ mode: 'since-ref', refDraft: 'main..dev', refError: '"main..dev" is not a usable git ref: a git ref holds no two consecutive dots. Nothing was asked of the analysis backend.' });
        const error = at('atlas-impact-ref-error');
        expect(error?.getAttribute('role')).toBe('alert');
        expect(error?.textContent).toContain('Nothing was asked of the analysis backend');
    });

    it('says it is reading rather than drawing an empty page', async () => {
        await render({ model: undefined });
        expect(at('atlas-impact-loading')).not.toBeNull();
        expect(at('atlas-impact-tiles')).toBeNull();
        expect(at('atlas-impact-badge')).toBeNull();
    });

    it('says out loud what the route scan opened', async () => {
        await render();
        expect(at('atlas-impact-route-note')?.textContent).toContain('read off the text');
    });
});

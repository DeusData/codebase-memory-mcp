// @vitest-environment jsdom
/**
 * Der Assistent in jsdom.
 *
 * Geprueft wird, was der Browser-Lauf am fertigen Bild nur bestaetigen kann: dass
 * ohne Beobachtung der ehrliche Satz UND die Anleitung dastehen, dass die
 * Divergenz zwei Listen mit eigenen Ueberschriften ist und nie eine Zahl, dass
 * jede Zeile der zweiten Liste sagt, was der Index ueber genau diesen Aufruf
 * weiss, und dass ein Klick auf einen Hop nichts weiter tut, als den Hop
 * zurueckzugeben.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import BugWizard from './BugWizard';
import type { BugPathNode, BugPathsDto } from './bug-paths';
import {
    BUG_WIZARD_NO_DIVERGENCE,
    BUG_WIZARD_RUNTIME_ONLY_LABEL,
    BUG_WIZARD_STATIC_ONLY_LABEL,
} from './bug-wizard-strings';

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

const OBSERVED = { count: 3, label: 'smoke-run', lastSeen: '2026-08-28T19:42:14Z' };

const TARGET = {
    name: 'createUser',
    qualifiedName: 'p.src.services.userService.createUser',
    filePath: 'src/services/userService.ts',
};

const EMPTY: BugPathsDto = {
    target: TARGET,
    staticPaths: [[{ name: 'registerUserRoutes', entryPoint: true }, TARGET]],
    observedPaths: [],
    staticOnly: [{ from: { name: 'registerUserRoutes' }, to: TARGET }],
    runtimeOnly: [],
    truncated: false,
    observedEvents: 0,
    depth: 4,
    chains: 6,
    flowsRead: 2,
    flowsTruncated: false,
};

const FULL: BugPathsDto = {
    ...EMPTY,
    observedPaths: [[{ name: 'registerUserRoutes' }, { ...TARGET, observed: OBSERVED }]],
    observedEvents: 2,
    staticOnly: [{ from: { name: 'create' }, to: TARGET }],
    runtimeOnly: [
        {
            from: TARGET,
            to: { name: 'validateUser', filePath: 'src/util/validate.ts' },
            observed: OBSERVED,
            indexRecordsCall: true,
        },
        {
            from: { name: 'listUsers', filePath: 'src/services/userService.ts' },
            to: { name: 'validateUser', filePath: 'src/util/validate.ts' },
            observed: OBSERVED,
            indexRecordsCall: false,
        },
    ],
};

function handlerSet() {
    return {
        onHop: vi.fn<(hop: BugPathNode) => void>(),
        onChangeTarget: vi.fn<() => void>(),
        onClose: vi.fn<() => void>(),
    };
}

type Handlers = ReturnType<typeof handlerSet>;

async function render(paths: BugPathsDto | undefined, over: Partial<Handlers> = {}): Promise<Handlers> {
    const handlers: Handlers = { ...handlerSet(), ...over };
    await act(async () => {
        root.render(
            <BugWizard
                project="codeatlasweb-w4b"
                target={TARGET}
                paths={paths}
                status={paths === undefined ? 'loading' : 'ready'}
                message=""
                onHop={handlers.onHop}
                onChangeTarget={handlers.onChangeTarget}
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

describe('BugWizard', () => {

    it('draws the four steps in the order the anatomy fixes', async () => {
        await render(FULL);
        const steps = [...container.querySelectorAll<HTMLElement>('.atlas-bugwizard-step')];
        expect(steps.map((step) => step.getAttribute('data-step'))).toEqual(['1', '2', '3', '4']);
        expect(steps.map((step) => step.getAttribute('data-testid'))).toEqual([
            'atlas-bugwizard-target',
            'atlas-bugwizard-static',
            'atlas-bugwizard-observed',
            'atlas-bugwizard-divergence',
        ]);
    });

    it('labels the expected chains as a reading of the index', async () => {
        await render(FULL);
        expect(at('atlas-bugwizard-static-origin')?.textContent).toBe('from the index');
        expect(all('atlas-bugwizard-static-chain')).toHaveLength(1);
    });

    it('says so and instructs when nothing came back, rather than showing an empty list', async () => {
        await render(EMPTY);
        const message = at('atlas-bugwizard-no-traces-message');
        expect(message?.textContent).toContain('No observed call came back');
        expect(at('atlas-bugwizard-no-traces-how')?.textContent).toContain('CLI');
        const command = at('atlas-bugwizard-no-traces-command')?.textContent ?? '';
        expect(command).toContain('codebase-memory-mcp cli ingest_traces');
        expect(command).toContain('"project":"codeatlasweb-w4b"');
        expect(command).toContain('"path"');
        expect(at('atlas-bugwizard-no-traces-format')?.textContent).toContain('2 to 256 qualified names');
        // Und der eine Satz, der erklaert, warum die Liste leer bleiben kann,
        // obwohl jemand einen Lauf eingespielt hat.
        expect(at('atlas-bugwizard-no-traces-where')?.textContent).toContain('cannot be read again');
        // Der erwartete Pfad bleibt stehen: eine halbe Antwort ist eine Antwort.
        expect(all('atlas-bugwizard-static-chain')).toHaveLength(1);
    });

    it('shows the observed count, the run and when it was last seen', async () => {
        await render(FULL);
        expect(at('atlas-bugwizard-no-traces')).toBeNull();
        const hops = all('atlas-bugwizard-observed-hop');
        expect(hops).toHaveLength(1);
        expect(hops[0].getAttribute('data-count')).toBe('3');
        expect(hops[0].getAttribute('data-label')).toBe('smoke-run');
        expect(hops[0].getAttribute('data-last-seen')).toBe(OBSERVED.lastSeen);
        expect(hops[0].textContent).toContain('observed 3 times, run "smoke-run"');
    });

    it('draws the divergence as two headed lists and never as one number', async () => {
        await render(FULL);
        const staticOnly = at('atlas-bugwizard-static-only');
        const runtimeOnly = at('atlas-bugwizard-runtime-only');
        expect(staticOnly?.textContent).toContain(BUG_WIZARD_STATIC_ONLY_LABEL);
        expect(runtimeOnly?.textContent).toContain(BUG_WIZARD_RUNTIME_ONLY_LABEL);
        expect(staticOnly?.getAttribute('data-count')).toBe('1');
        expect(runtimeOnly?.getAttribute('data-count')).toBe('2');
        expect(at('atlas-bugwizard-no-divergence')).toBeNull();
    });

    it('says per row what the index knows about that one call', async () => {
        await render(FULL);
        const verdicts = all('atlas-bugwizard-edge-verdict').map((node) => node.textContent);
        expect(verdicts).toEqual([
            'the index records this call, it is simply not on a way in',
            'the index records no such call at all',
        ]);
    });

    it('says both lists are empty instead of drawing nothing', async () => {
        await render({ ...FULL, staticOnly: [], runtimeOnly: [] });
        expect(at('atlas-bugwizard-no-divergence')?.textContent).toBe(BUG_WIZARD_NO_DIVERGENCE);
        expect(at('atlas-bugwizard-static-only')).toBeNull();
        expect(at('atlas-bugwizard-runtime-only')).toBeNull();
    });

    it('announces a walk that stopped at its bound', async () => {
        await render({ ...FULL, truncated: true });
        expect(at('atlas-bugwizard-truncated')?.textContent).toContain('4 hops or 6 chains');
    });

    it('hands a clicked hop back and does nothing else', async () => {
        const handlers = await render(FULL);
        const hop = all('atlas-bugwizard-hop')[0].querySelector('button')!;
        await act(async () => {
            hop.click();
        });
        expect(handlers.onHop).toHaveBeenCalledTimes(1);
        expect(handlers.onHop.mock.calls[0][0].name).toBe('registerUserRoutes');
        expect(handlers.onClose).not.toHaveBeenCalled();
        expect(handlers.onChangeTarget).not.toHaveBeenCalled();
    });

    it('carries the counts on the root so a run can read them without parsing', async () => {
        await render(FULL);
        const panel = at('atlas-bugwizard')!;
        expect(panel.getAttribute('data-static-paths')).toBe('1');
        expect(panel.getAttribute('data-observed-paths')).toBe('1');
        expect(panel.getAttribute('data-static-only')).toBe('1');
        expect(panel.getAttribute('data-runtime-only')).toBe('2');
        expect(panel.getAttribute('data-events')).toBe('2');
    });

    it('shows neither list nor an absence while the reading is still running', async () => {
        await render(undefined);
        expect(at('atlas-bugwizard-busy')).not.toBeNull();
        expect(at('atlas-bugwizard-static-empty')).toBeNull();
        expect(at('atlas-bugwizard-no-divergence')).toBeNull();
    });
});

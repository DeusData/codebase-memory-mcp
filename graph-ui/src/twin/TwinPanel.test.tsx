// @vitest-environment jsdom
/*
 * Das Panel in jsdom: die Testmarken, die der Beweislauf im Browser sucht, die
 * Ehrlichkeitszeilen, die sichtbar dastehen muessen, und die Regel, dass eine
 * Zeile mit Ziel ein Knopf ist und eine ohne nicht.
 *
 * Monaco wird hier nicht geladen. Der Editor haengt nicht am Panel, und was er
 * mit den Badges macht, hat seine eigenen Tests neben dem Adapter.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { SemanticIR } from '../core/semantic-ir';
import { buildFlowView } from '../pseudocode/flow-view';
import type { FlowView } from '../pseudocode/flow-view';
import { buildImportsGroup } from '../pseudocode/imports-group';
import type { ImportsGroup } from '../pseudocode/imports-group';
import { buildPseudocode } from '../pseudocode/pseudocode-builder';
import type { ClosureDocument } from '../pseudocode/pseudocode-builder';
import { CORE_FACETS, CREATE_USER_IR, presentation } from '../test-support/twin-fixtures';
import TwinPanel, { TWIN_TITLE } from './TwinPanel';
import type { TwinPanelProps } from './TwinPanel';
import { Facet } from './presentation-profile';

const HERE = dirname(fileURLToPath(import.meta.url));

/** Der aufgezeichnete Walk und die IRs dazu, wie die Unit-Tests des Ports sie lesen. */
function flowFixture(): FlowView {
    const closure = JSON.parse(readFileSync(
        join(HERE, '..', 'pseudocode', '__fixtures__', 'closure-userService-create.json'),
        'utf8',
    )) as ClosureDocument;
    const ir = (name: string): SemanticIR => JSON.parse(readFileSync(
        join(HERE, '__fixtures__', `ir-${name}.json`),
        'utf8',
    )) as SemanticIR;
    return buildFlowView({
        closure,
        irs: [ir('userService-create'), ir('createUser'), ir('listUsers'), ir('validateUser'), ir('insert')],
    });
}

/** Zwei Importe aus derselben Anweisung: einer, den createUser benutzt, und einer nicht. */
function importsFixture(): ImportsGroup {
    return buildImportsGroup({
        imports: {
            entries: [
                { name: 'insert', module: '../repo/db', line: 4, origin: 'source', evidence: [] },
                { name: 'query', module: '../repo/db', line: 4, origin: 'source', evidence: [] },
            ],
            truncated: false,
            indexedTargets: ['src/repo/db.ts'],
            sourceRead: true,
            fileIrs: [CREATE_USER_IR],
        },
        irs: [CREATE_USER_IR],
        uri: CREATE_USER_IR.symbol.uri,
    });
}

/**
 * Ein Symbol, dessen einzigen Aufruf der Index nicht aufgeloest hat (W8c).
 *
 * Kein Zielort, kein qualifizierter Name des Ziels: genau der Fall, in dem eine
 * Schrittzeile frueher stumm blieb und seitdem sagt, dass der Index keine
 * Stelle kennt.
 */
const NO_TARGET_IR: SemanticIR = {
    ...CREATE_USER_IR,
    steps: { ...CREATE_USER_IR.steps, value: [{ targetName: 'mystery', line: 24 }], evidence: [] },
    throws: { ...CREATE_USER_IR.throws, value: [], evidence: [] },
    reads: { ...CREATE_USER_IR.reads, value: [], evidence: [] },
};

/**
 * Das geladene Layout, so weit dieser Test es braucht (W8c).
 *
 * Zwei Knoten und eine RAISES-Kante, in derselben Form, in der `/api/layout`
 * antwortet: `validateUser` erhebt `ValidationError`. Genau das ist die Sorte
 * Aussage, die der Leser dem Aufruf `validateUser(input)` nicht ansieht.
 */
const INSIGHT_GRAPH = {
    nodes: [
        {
            id: 1,
            name: 'validateUser',
            qualified_name: 'codeatlas-atlas-sample-b6fa2326.src.util.validate.validateUser',
            label: 'Function',
            file_path: 'src/util/validate.ts',
            start_line: 19,
        },
        {
            id: 2,
            name: 'ValidationError',
            qualified_name: 'codeatlas-atlas-sample-b6fa2326.src.util.validate.ValidationError',
            label: 'Class',
            file_path: 'src/util/validate.ts',
            start_line: 4,
        },
    ],
    edges: [{ source: 1, target: 2, type: 'RAISES' }],
};

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

function props(overrides: Partial<TwinPanelProps> = {}): TwinPanelProps {
    return {
        status: 'ready',
        message: '',
        symbolName: 'createUser',
        ir: CREATE_USER_IR,
        presentation: presentation(2, CORE_FACETS),
        onDepth: () => undefined,
        onToggleFacet: () => undefined,
        onFollow: () => undefined,
        flowOpen: false,
        onToggleFlow: () => undefined,
        flowStep: -1,
        view: 'facts',
        onView: () => undefined,
        ...overrides,
    };
}

async function render(overrides: Partial<TwinPanelProps> = {}): Promise<void> {
    await act(async () => {
        root.render(<TwinPanel {...props(overrides)} />);
    });
}

const q = (selector: string) => container.querySelector(selector);
const all = (selector: string) => [...container.querySelectorAll(selector)];
const text = () => container.textContent ?? '';

describe('Kopf und Regler', () => {
    it('nennt sich SEMANTIC_TWIN und sagt, um welchen Fluss es geht', async () => {
        await render();
        expect(q('[data-testid="atlas-twin"]')).not.toBeNull();
        expect(q('.atlas-twin-title')?.textContent).toBe(TWIN_TITLE);
        expect(q('[data-testid="atlas-twin-subject"]')?.textContent).toContain('createUser');
    });

    it('fragt nach dem Leser und bietet genau die fuenf Rasten', async () => {
        const slider = (await render(), q('[data-testid="atlas-twin-depth"]') as HTMLInputElement);
        expect(slider.type).toBe('range');
        expect(slider.min).toBe('0');
        expect(slider.max).toBe('4');
        expect(slider.value).toBe('2');
        // AC2: die Beschriftung fragt nach dem Leser, nicht nach der Menge,
        // und daneben steht der Name der gewaehlten Stufe.
        expect(q('.atlas-twin-depth-label')?.textContent).toBe('Who is reading');
        expect(slider.getAttribute('aria-label')).toBe('Who is reading');
        expect(q('[data-testid="atlas-twin-depth-name"]')?.textContent).toBe('medior');
    });

    it('meldet eine neue Tiefe geklemmt nach oben', async () => {
        const onDepth = vi.fn();
        await render({ onDepth });
        const slider = q('[data-testid="atlas-twin-depth"]') as HTMLInputElement;
        await act(async () => {
            // React fuehrt an einem kontrollierten Feld einen eigenen
            // Wert-Beobachter. Wer `slider.value` direkt setzt, aktualisiert
            // den Beobachter mit und React haelt das Ereignis fuer eine
            // Wiederholung. Der Setter des Prototyps geht daran vorbei, und
            // erst dann ist das `input` ein echter Wertwechsel.
            const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
            setter?.call(slider, '3');
            slider.dispatchEvent(new Event('input', { bubbles: true }));
        });
        expect(onDepth).toHaveBeenCalledWith(3);
    });

    it('bietet alle sieben Linsen stabil an und sagt, ob sie an sind', async () => {
        const onToggleFacet = vi.fn();
        await render({ onToggleFacet });
        const chips = all('.atlas-twin-facet');
        expect(chips.map((chip) => chip.getAttribute('data-facet'))).toEqual([
            'logic',
            'calls',
            'data',
            'errors',
            'tests',
            'runtime',
            'changes',
        ]);
        expect(q('.atlas-twin-facet[data-facet="logic"]')?.getAttribute('data-on')).toBe('true');
        expect(q('.atlas-twin-facet[data-facet="runtime"]')?.getAttribute('data-on')).toBe('false');
        expect(q('.atlas-twin-facet[data-facet="changes"]')?.getAttribute('data-on')).toBe('false');
        await act(async () => {
            (q('.atlas-twin-facet[data-facet="runtime"]') as HTMLButtonElement).click();
            (q('.atlas-twin-facet[data-facet="changes"]') as HTMLButtonElement).click();
        });
        expect(onToggleFacet.mock.calls).toEqual([[Facet.Runtime], [Facet.Changes]]);
    });

    it('meldet einen Linsenwechsel nach oben, statt selbst zu filtern', async () => {
        const onToggleFacet = vi.fn();
        await render({ onToggleFacet });
        await act(async () => {
            (q('.atlas-twin-facet[data-facet="logic"]') as HTMLButtonElement).click();
        });
        expect(onToggleFacet).toHaveBeenCalledWith(Facet.Logic);
    });
});

describe('die Sektionen der technischen Tiefe', () => {
    it('traegt an jeder Sektion ihre eigene Testmarke', async () => {
        await render();
        const names = all('[data-testid^="codeatlas-twin-section-"]').map((node) =>
            node.getAttribute('data-testid'),
        );
        expect(names).toEqual([
            'codeatlas-twin-section-purpose',
            'codeatlas-twin-section-steps',
            'codeatlas-twin-section-callers',
            'codeatlas-twin-section-state',
            'codeatlas-twin-section-errors',
            'codeatlas-twin-section-effects',
            'codeatlas-twin-section-tests',
        ]);
    });

    it('zeichnet die Schritte mit ihrer Aufrufzeile', async () => {
        await render();
        const steps = all('[data-testid="codeatlas-twin-step"]');
        expect(steps).toHaveLength(6);
        expect(steps.map((node) => node.getAttribute('data-line'))).toEqual([
            '24',
            '27',
            '29',
            '29',
            '30',
            '35',
        ]);
        expect(steps[0].textContent).toContain('validateUser');
    });

    it('markiert genau die Zeile, auf der der Caret steht', async () => {
        await render({ caretLine: 27 });
        const current = all('[data-testid="codeatlas-twin-step"][data-current="true"]');
        expect(current).toHaveLength(1);
        expect(current[0].getAttribute('data-line')).toBe('27');
    });

    it('macht eine Zeile mit Ziel zum Knopf und meldet den Klick nach oben', async () => {
        const onFollow = vi.fn();
        await render({ onFollow });
        const button = q('[data-testid="codeatlas-twin-step"] button') as HTMLButtonElement;
        await act(async () => {
            button.click();
        });
        expect(onFollow).toHaveBeenCalledTimes(1);
        expect(onFollow.mock.calls[0][0].uri).toContain('validate.ts');
    });

    it('zeigt die Ehrlichkeitszeilen sichtbar, nicht nur im Modell', async () => {
        await render();
        // Der Effekt-Block sagt, was in ihm staende, statt sich als "keine
        // Effekte" zu lesen; die Fehler-Sektion sagt, was der Index nicht sieht.
        expect(text()).toContain('records no routes, no outbound calls and no writes');
        expect(text()).toContain('Ask 2 in UPSTREAM-ASKS.md');
        expect(text()).toContain('Where these are handled is not visible to the index');
    });

    it('zeigt den ehrlichen Kein-Test-Satz, wenn kein Testaufrufer gefunden wurde', async () => {
        const noTests = {
            ...CREATE_USER_IR,
            tests: { ...CREATE_USER_IR.tests, value: [], evidence: [] },
        };
        await render({ ir: noTests });
        expect(q('[data-testid="codeatlas-twin-empty-tests"]')?.textContent).toContain(
            'No test callers found',
        );
    });

    it('setzt den Zustandsmarker samt seinem ganzen Satz als Titel', async () => {
        await render();
        const marker = q(
            '[data-testid="codeatlas-twin-section-tests"] [data-testid="codeatlas-twin-state-marker"]',
        );
        expect(marker?.textContent).toBe('inferred');
        expect(marker?.getAttribute('data-hint')).toContain('derived from indexed callers');
    });
});

describe('die Belege', () => {
    it('bietet keinen Beleg-Knopf ueber einer Behauptung, die nicht aufgestellt wurde', async () => {
        await render();
        const effects = q('[data-testid="codeatlas-twin-section-effects"]');
        expect(effects?.querySelector('[data-testid="codeatlas-evidence-btn"]')).toBeNull();
    });

    it('klappt auf Klick auf und nennt Relation, Ort und Urheber', async () => {
        await render();
        const button = q('[data-testid="codeatlas-evidence-btn"][data-factpath="steps[0]"]') as HTMLButtonElement;
        expect(q('[data-testid="codeatlas-evidence-popover"]')).toBeNull();
        await act(async () => {
            button.click();
        });
        const popover = q('[data-testid="codeatlas-evidence-popover"]');
        expect(popover).not.toBeNull();
        // Der Beleg nennt die Aufrufstelle (validate.ts:24 in der
        // Aufzeichnung), nicht die Deklarationszeile, zu der die Zeile fuehrt.
        // Genau diese Unterscheidung ist der Grund, warum `siteLine` und
        // `target` getrennt sind.
        expect(popover?.textContent).toContain('calls');
        expect(popover?.textContent).toContain('validate.ts:24');
        expect(popover?.textContent).toContain('index generation');
    });

    it('klappt beim zweiten Klick wieder zu', async () => {
        await render();
        const button = q('[data-testid="codeatlas-evidence-btn"][data-factpath="steps[0]"]') as HTMLButtonElement;
        await act(async () => {
            button.click();
        });
        await act(async () => {
            button.click();
        });
        expect(q('[data-testid="codeatlas-evidence-popover"]')).toBeNull();
    });
});

describe('die Tiefen im DOM', () => {
    it('zeigt an der Erzaehl-Tiefe Prosa, Chips und keine Sektion', async () => {
        await render({ presentation: presentation(0, CORE_FACETS) });
        expect(q('[data-testid="codeatlas-twin-prose"]')).not.toBeNull();
        expect(all('[data-testid="codeatlas-twin-chip"]')).toHaveLength(5);
        expect(all('[data-testid^="codeatlas-twin-section-"]')).toHaveLength(0);
        expect(text()).toContain('Nobody wrote that description');
        expect(text()).not.toMatch(/\w+\.\w+\.\w+/);
    });

    it('zeigt der Junior-Stufe die Reihenfolge und die erklaerten Woerter', async () => {
        await render({ presentation: presentation(1, CORE_FACETS, 'plain') });
        expect(all('[data-testid="codeatlas-twin-step"]')).toHaveLength(6);
        expect(all('[data-testid="codeatlas-twin-term"]').length).toBeGreaterThan(2);
        expect(text()).toContain('First it hands work to validateUser');
        expect(text()).toContain('Words used above, once each:');
    });

    it('zeigt der Senior-Stufe die Kosten und keine Sektion', async () => {
        await render({ presentation: presentation(3, CORE_FACETS) });
        expect(all('[data-testid^="codeatlas-twin-reader-"]').length).toBeGreaterThan(3);
        expect(all('[data-testid^="codeatlas-twin-section-"]')).toHaveLength(0);
        expect(text()).toContain('How it can fail');
        expect(text()).toContain('What moves with a change');
        expect(text()).not.toContain('confidence 0.90');
    });

    it('zeigt der Architekten-Stufe Grund, Herkunft und die Grenzen des Index', async () => {
        await render({ presentation: presentation(4, CORE_FACETS) });
        expect(text()).toContain('confidence 0.90');
        expect(text()).toContain('What it sits on');
        expect(q('[data-testid="codeatlas-twin-reader-limits"]')).not.toBeNull();
        expect(all('[data-testid="codeatlas-twin-limit"]').length).toBeGreaterThan(4);
        expect(text()).toContain('index generation 2');
    });

    /*
     * AC1 im DOM, gemessen wie der Beweislauf es misst: bei AUSGESCHALTETEM
     * Modell (dieser Testgriff kennt gar keins) muss jede Stufe einen anderen
     * Text ergeben, und jede muss ein Element tragen, das keine andere hat.
     */
    it('sagt auf keinen zwei Stufen dasselbe, auch ohne Modell', async () => {
        const seen: string[] = [];
        const marks: Set<string>[] = [];
        for (const level of [0, 1, 2, 3, 4] as const) {
            await render({ presentation: presentation(level, CORE_FACETS) });
            seen.push(text());
            marks.push(new Set(
                [...document.querySelectorAll('[data-testid]')]
                    .map((node) => node.getAttribute('data-testid') ?? ''),
            ));
        }
        expect(new Set(seen).size).toBe(5);
        for (const [index, set] of marks.entries()) {
            const others = marks.filter((_, at) => at !== index);
            const unique = [...set].filter((mark) => others.every((other) => !other.has(mark)));
            expect(unique.length, `Stufe ${index} hat kein eigenes Element`).toBeGreaterThan(0);
        }
    });

    it('stellt jeder Stufe ihre Frage voran', async () => {
        const questions: string[] = [];
        for (const level of [0, 1, 2, 3, 4] as const) {
            await render({ presentation: presentation(level, CORE_FACETS) });
            questions.push(q('[data-testid="codeatlas-twin-question"]')?.textContent ?? '');
        }
        expect(new Set(questions).size).toBe(5);
        expect(questions[0]).toBe('What happens here, and why does it matter to you?');
        expect(questions[4]).toBe('What does this sit on, and where does our knowledge end?');
    });

    it('sagt bei ausgeschaltetem Modell, dass die Saetze gebaut sind, und bietet keinen Knopf', async () => {
        await render({ presentation: presentation(0, CORE_FACETS) });
        expect(q('[data-testid="codeatlas-twin-voice-btn"]')).toBeNull();
        expect(q('[data-testid="codeatlas-twin-voice-note"]')?.textContent)
            .toContain('The local model is off');
    });

    it('bietet den Knopf, sobald das Modell bereit ist', async () => {
        await render({ presentation: presentation(0, CORE_FACETS), refineAvailable: true });
        expect(q('[data-testid="codeatlas-twin-voice-btn"]')?.textContent)
            .toBe('Say it for this reader');
    });
});

describe('wenn es nichts zu zeigen gibt', () => {
    it('sagt den Satz statt eine leere Flaeche zu zeigen', async () => {
        await render({
            status: 'not-indexed',
            ir: undefined,
            symbolName: 'nothing',
            message: 'This file is not indexed yet.',
            hint: 'CodeAtlas is still reading this workspace.',
        });
        const empty = q('[data-testid="atlas-twin-empty"]');
        expect(empty?.textContent).toContain('not indexed yet');
        expect(empty?.textContent).toContain('still reading this workspace');
        expect(q('[data-testid="atlas-twin"]')?.getAttribute('data-status')).toBe('not-indexed');
    });

    it('behaelt den Regler, damit die Tiefe nicht an einem Ladezustand haengt', async () => {
        await render({ status: 'loading', ir: undefined, message: 'Reading ...' });
        expect(q('[data-testid="atlas-twin-depth"]')).not.toBeNull();
    });
});

describe('der Griff fuer den Beweislauf', () => {
    it('nennt Subjekt, Sektionen und Schrittzeilen', async () => {
        await render();
        expect(globalThis.__atlasTwin?.symbol).toBe('createUser');
        expect(globalThis.__atlasTwin?.sectionNames).toContain('steps');
        expect(globalThis.__atlasTwin?.stepLines).toEqual([24, 27, 29, 29, 30, 35]);
    });
});

/*
 * W4c/W5c: der flow()-Kopf, die STEPS-Markierung des Steppers, die
 * Pseudocode-Ansicht und die wieder eingebaute Imports-Gruppe.
 *
 * Der Sequenzkasten selbst wird hier NICHT mehr geprueft: er ist seit W5c ein
 * Overlay ueber der Editorflaeche und hat seine eigenen Tests neben seiner
 * eigenen Datei (src/pseudocode/FlowOverlay.test.tsx). Was das Panel davon
 * behaelt, ist der Kopf, der das Overlay schaltet, und die Zeile der
 * STEPS-Liste, auf der der Stepper gerade steht.
 */
describe('der flow()-Kopf', () => {
    it('ist ein Knopf, den man mit der Tastatur erreicht', async () => {
        await render();
        const head = q('[data-testid="atlas-twin-subject"]') as HTMLElement | null;
        expect(head?.tagName).toBe('BUTTON');
        expect(head?.getAttribute('aria-expanded')).toBe('false');
        expect((head as HTMLButtonElement).disabled).toBe(false);
    });

    it('sagt am Knopf, ob der Kasten offen ist', async () => {
        await render({ flowOpen: true });
        expect(q('[data-testid="atlas-twin-subject"]')?.getAttribute('aria-expanded')).toBe('true');
    });

    it('meldet einen Klick, statt ihn zu schlucken', async () => {
        const onToggleFlow = vi.fn();
        await render({ onToggleFlow });
        await act(async () => {
            (q('[data-testid="atlas-twin-subject"]') as HTMLButtonElement).click();
        });
        expect(onToggleFlow).toHaveBeenCalledTimes(1);
    });

    it('zeichnet den Kasten nicht mehr selbst, auch nicht bei offenem Erklaerer', async () => {
        // Der Befund vom 2026-08-29: in dieser Spalte war der Kasten nicht zu
        // lesen. Er steht jetzt im Overlay, und dass hier nichts davon uebrig
        // ist, gehoert zu der Entscheidung dazu.
        await render({ flowOpen: true, flow: flowFixture() });
        expect(q('[data-testid="atlas-flow"]')).toBeNull();
        expect(q('[data-testid="atlas-flow-box"]')).toBeNull();
        expect(q('[data-testid="atlas-flow-position"]')).toBeNull();
    });
});

describe('die STEPS-Zeile, auf der der Stepper steht', () => {
    const flow = flowFixture();

    it('markiert die STEPS-Zeile, auf der der Stepper steht', async () => {
        // Die Fixture laeuft von `create` aus; dessen einzige Aufrufstelle ist
        // die erste Zeile der STEPS-Liste. Genau die muss markiert sein, und
        // keine zweite.
        const step = flow.stepRows.findIndex((row) => row === 0);
        expect(step).toBeGreaterThanOrEqual(0);
        await render({ flowOpen: true, flow, flowStep: step });
        const marked = all('[data-testid="codeatlas-twin-step"][data-current="true"]');
        expect(marked).toHaveLength(1);
        expect(marked[0]).toBe(all('[data-testid="codeatlas-twin-step"]')[0]);
    });

    it('laesst waehrend einer Sitzung nur den Stepper markieren, nie auch den Caret', async () => {
        const step = flow.stepRows.findIndex((row) => row === 0);
        // Der Caret steht auf der Aufrufstelle einer ANDEREN Zeile der Liste.
        await render({ flowOpen: true, flow, flowStep: step, caretLine: 29 });
        const marked = all('[data-testid="codeatlas-twin-step"][data-current="true"]');
        expect(marked).toHaveLength(1);
        expect(marked[0]).toBe(all('[data-testid="codeatlas-twin-step"]')[0]);
    });

    it('gibt die Markierung an den Caret zurueck, sobald die Sitzung endet', async () => {
        await render({ flowOpen: true, flow, flowStep: -1, caretLine: 29 });
        const marked = all('[data-testid="codeatlas-twin-step"][data-current="true"]');
        expect(marked.length).toBeGreaterThan(0);
        expect(marked.every((node) => node.getAttribute('data-line') === '29')).toBe(true);
    });

    it('markiert keine STEPS-Zeile fuer einen Schritt eines anderen Symbols', async () => {
        const step = flow.stepRows.findIndex((row) => row < 0);
        expect(step).toBeGreaterThanOrEqual(0);
        await render({ flowOpen: true, flow, flowStep: step });
        expect(all('[data-testid="codeatlas-twin-step"][data-current="true"]')).toHaveLength(0);
    });
});

describe('die Pseudocode-Ansicht', () => {
    const document = buildPseudocode(
        { kind: 'symbol', label: 'createUser' },
        { irs: [CREATE_USER_IR] },
    );
    const imports = importsFixture();

    it('zeigt die nummerierten Fakten-Zeilen des Symbols', async () => {
        await render({ view: 'pseudocode', pseudocode: document, imports });
        const lines = all('[data-testid="atlas-pseudocode-line"]');
        expect(lines.length).toBeGreaterThanOrEqual(6);
        expect(lines[0].textContent).toContain('1. call validateUser');
    });

    it('zeichnet "may raise" in der Alarm-Rolle statt wie jede andere Zeile', async () => {
        await render({ view: 'pseudocode', pseudocode: document, imports });
        const alarm = all('[data-testid="atlas-pseudocode-line"][data-alarm="true"]');
        expect(alarm).toHaveLength(1);
        expect(alarm[0].textContent).toContain('may raise');
    });

    it('fuehrt jede nummerierte Zeile an den Ort, aus dem sie gelesen wurde', async () => {
        const onOpenLine = vi.fn();
        await render({ view: 'pseudocode', pseudocode: document, imports, onOpenLine });
        await act(async () => {
            (q('[data-testid="atlas-pseudocode-line"] button') as HTMLButtonElement).click();
        });
        expect(onOpenLine).toHaveBeenCalledWith({ uri: CREATE_USER_IR.symbol.uri, line: 24 });
    });

    it('fuehrt die Imports-Gruppe als eigene Ueberschrift, mit ihren Markern', async () => {
        await render({ view: 'pseudocode', pseudocode: document, imports });
        const group = q('[data-testid="atlas-pseudocode-imports"]');
        expect(group?.getAttribute('data-entries')).toBe('2');
        expect(q('[data-testid="atlas-pseudocode-group"]')?.textContent).toContain('pulls in');
        expect(all('[data-testid="atlas-pseudocode-import"]')).toHaveLength(2);
        expect(text()).toContain('used here');
        expect(text()).toContain('not used here');
    });

    /*
     * W8c: die Saetze ueber den Block selbst stehen nicht mehr als Absaetze
     * unter ihm, sondern hinter dem Fragezeichen daneben. Geprueft wird
     * deshalb BEIDES: dass auf der Flaeche der eine kurze Satz steht, und dass
     * die drei anderen wortgleich erreichbar geblieben sind. Ein Zyklus, der
     * eine Ehrlichkeitszusage aufloest statt sie umzuraeumen, faellt hier
     * durch.
     */
    it('laesst unter dem Block einen Satz stehen und keine Textwand', async () => {
        await render({ view: 'pseudocode', pseudocode: document, imports });
        const honest = q('[data-testid="atlas-pseudocode-honest"]');
        expect(honest?.textContent).toContain('Only what the index reported');
        expect(honest?.textContent).not.toContain('Derived from the index and nothing else');
    });

    it('haelt die verschobenen Saetze wortgleich hinter dem Fragezeichen', async () => {
        await render({ view: 'pseudocode', pseudocode: document, imports });
        const hint = q('[data-testid="atlas-pseudocode-provenance"]')?.getAttribute('data-hint') ?? '';
        expect(hint).toContain('in scope contributed steps');
        expect(hint).toContain('Derived from the index and nothing else');
        expect(hint).toContain('not a callee that does none of them');
    });

    it('fuehrt mit dem Fund und stellt die Schrittliste dahinter', async () => {
        await render({ view: 'pseudocode', pseudocode: document, imports });
        const block = q('[data-testid="atlas-pseudocode"]');
        const order = [...(block?.querySelectorAll('[data-testid]') ?? [])]
            .map((node) => node.getAttribute('data-testid') ?? '');
        expect(order.indexOf('atlas-pseudocode-lead'))
            .toBeLessThan(order.indexOf('atlas-pseudocode-imports'));
        expect(order.indexOf('atlas-pseudocode-imports'))
            .toBeLessThan(order.indexOf('atlas-pseudocode-line'));
        // Der Fund dieser Fixture: `insert` steht im Kopf, weil createUser es
        // nach den aufgezeichneten Fakten nicht anfasst.
        expect(q('[data-testid="atlas-pseudocode-lead"]')?.getAttribute('data-kind'))
            .toBe('unused-imports');
        expect(q('[data-testid="atlas-pseudocode-lead"]')?.textContent)
            .toContain('as far as the index shows');
    });

    it('macht den ungenutzten Import als Fund kenntlich, mit seiner Grenze', async () => {
        await render({ view: 'pseudocode', pseudocode: document, imports });
        const marked = all('[data-testid="atlas-pseudocode-import"][data-finding="true"]');
        expect(marked).toHaveLength(1);
        expect(marked[0].textContent).toContain('not used anywhere in this file as far as the index shows');
        expect(q('[data-testid="atlas-pseudocode-group"]')?.textContent)
            .toContain('not used anywhere in this file as far as the index shows');
        expect(q('[data-testid="atlas-pseudocode-tally"]')?.textContent)
            .toContain('that CodeAtlas cannot check');
    });

    it('nennt an jeder Schrittzeile Datei und Zeile des aufgerufenen Symbols', async () => {
        await render({ view: 'pseudocode', pseudocode: document, imports });
        const steps = all('[data-testid="atlas-pseudocode-line"][data-kind="step"]');
        expect(steps.length).toBeGreaterThan(0);
        for (const step of steps) {
            const place = step.querySelector('[data-testid="atlas-pseudocode-target"]');
            expect(place).not.toBeNull();
            expect((place?.textContent ?? '').length).toBeGreaterThan(0);
        }
        expect(steps[0].querySelector('[data-testid="atlas-pseudocode-target"]')?.textContent)
            .toBe('validate.ts:19');
    });

    it('oeffnet mit dem Ziel die Stelle des Aufgerufenen und nicht die Aufrufstelle', async () => {
        const onOpenLine = vi.fn();
        await render({ view: 'pseudocode', pseudocode: document, imports, onOpenLine });
        await act(async () => {
            (q('[data-testid="atlas-pseudocode-line"][data-kind="step"] '
                + '[data-testid="atlas-pseudocode-target"]') as HTMLButtonElement).click();
        });
        expect(onOpenLine).toHaveBeenCalledWith({
            uri: CREATE_USER_IR.steps.value[0].targetFile,
            line: CREATE_USER_IR.steps.value[0].targetLine,
        });
    });

    it('sagt an einer Zeile ohne Ziel, dass der Index keine Stelle kennt', async () => {
        const blind = buildPseudocode({ kind: 'symbol', label: 'blind' }, { irs: [NO_TARGET_IR] });
        await render({ view: 'pseudocode', pseudocode: blind, imports });
        const place = q('[data-testid="atlas-pseudocode-target"]');
        expect(place?.getAttribute('data-known')).toBe('false');
        expect(place?.tagName).toBe('SPAN');
        expect(place?.textContent).toBe('the index records no place for this name');
    });

    it('schreibt an den Schritt, was der Index ueber das aufgerufene Symbol haelt', async () => {
        const enriched = buildPseudocode(
            { kind: 'symbol', label: 'createUser' },
            { irs: [CREATE_USER_IR], graph: INSIGHT_GRAPH },
        );
        await render({ view: 'pseudocode', pseudocode: enriched, imports });
        const notes = all('[data-testid="atlas-pseudocode-behind"]');
        expect(notes.length).toBeGreaterThan(0);
        expect(notes.map((node) => node.textContent)).toContain('may raise ValidationError');
        expect(globalThis.__atlasTwin?.pseudocode.enrichedSteps).toBeGreaterThan(0);
        expect(globalThis.__atlasTwin?.pseudocode.enrichment.usable.map((entry) => entry.kind))
            .toContain('raises');
        expect(globalThis.__atlasTwin?.pseudocode.enrichment.missing.length).toBeGreaterThan(0);
    });

    it('zeigt jeden Schritt, bei zwei Schritten wie bei sechs', async () => {
        // Die Gegenprobe zur Laengenschwelle, die dieser Zyklus ausdruecklich
        // nicht eingefuehrt hat: ein Block mit einem einzigen Schritt zeigt ihn
        // genauso wie der Block mit sechsen darueber.
        const short = buildPseudocode({ kind: 'symbol', label: 'blind' }, { irs: [NO_TARGET_IR] });
        await render({ view: 'pseudocode', pseudocode: short, imports });
        expect(all('[data-testid="atlas-pseudocode-line"]')).toHaveLength(short.lines.length);
        expect(q('[data-testid="atlas-pseudocode-steps-head"]')).not.toBeNull();
    });

    it('bleibt beim Fakten-Koerper, solange niemand umgeschaltet hat', async () => {
        await render({ pseudocode: document, imports });
        expect(q('[data-testid="atlas-pseudocode"]')).toBeNull();
        expect(q('[data-testid="codeatlas-twin-section-steps"]')).not.toBeNull();
    });
});

describe('die wieder eingebaute Imports-Gruppe im Twin', () => {
    it('steht im DATA-Block, neben dem, was das Symbol liest', async () => {
        await render({ imports: importsFixture() });
        const section = q('[data-testid="codeatlas-twin-section-imports"]');
        expect(section).not.toBeNull();
        expect(section?.getAttribute('data-block')).toBe('data');
        expect(section?.textContent).toContain('used in this file');
        expect(globalThis.__atlasTwin?.sectionNames).toContain('imports');
    });

    it('fehlt, solange keine Antwort da ist, statt eine leere Ueberschrift zu zeigen', async () => {
        await render();
        expect(q('[data-testid="codeatlas-twin-section-imports"]')).toBeNull();
    });
});

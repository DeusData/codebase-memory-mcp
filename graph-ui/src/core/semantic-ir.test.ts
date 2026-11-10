import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import type {
    CallSite,
    ChecklistItem,
    ComplexityFlags,
    Evidence,
    EvidenceSource,
    Fact,
    KnowledgeState,
    RuntimeCall,
    SemanticIR,
} from './semantic-ir';
import type { SymbolRef } from './focus-protocol';

/*
 * Der Port besteht ausschliesslich aus Typen; zur Laufzeit exportiert er
 * nichts. Die Invarianten werden deshalb doppelt geprueft: die typisierten
 * Beispiele unten scheitern beim Typecheck, wenn eine Form abweicht, und die
 * Quelltextpruefungen scheitern in vitest, das den Typecheck nicht faehrt.
 * Nur zusammen fangen sie ab, dass jemand 'unsupported' oder ein Feld still
 * entfernt.
 */
const SOURCE = readFileSync(new URL('./semantic-ir.ts', import.meta.url), 'utf8');

/** Alle sechs Zustaende des Originals, jeder einzeln benannt. */
const KNOWLEDGE_STATES: KnowledgeState[] = [
    'known',
    'inferred',
    'ambiguous',
    'unsupported',
    'notIndexed',
    'unknown',
];

/** Alle sechs Evidenzquellen des Originals. */
const EVIDENCE_SOURCES: EvidenceSource[] = [
    'graph-edge',
    'graph-node',
    'runtime-trace',
    'test',
    'git-history',
    'llm',
];

const symbol: SymbolRef = {
    name: 'createUser',
    qualifiedName: 'atlas.src.services.userService.createUser',
    kind: 'function',
    uri: 'file:///atlas/src/services/userService.ts',
    range: { start: { line: 22, character: 0 }, end: { line: 35, character: 1 } },
};

const graphEvidence: Evidence = {
    source: 'graph-edge',
    relation: 'CALLS',
    file: 'src/services/userService.ts',
    range: { startLine: 24, endLine: 24 },
    engineGeneration: 7,
    providerId: 'cbm-rpc',
};

const traceEvidence: Evidence = {
    source: 'runtime-trace',
    observations: 12,
    engineGeneration: 7,
    providerId: 'trace-import',
};

describe('Herkunft des Ports', () => {
    it('nennt CodeAtlasIDE als Quelle und den Portierungstag', () => {
        expect(SOURCE).toContain('CodeAtlasIDE');
        expect(SOURCE).toContain('semantic-ir.ts');
        expect(SOURCE).toContain('2026-08-28');
    });

    it('haelt den Import von SymbolRef auf das mitportierte Fokusprotokoll', () => {
        expect(SOURCE).toContain("import { SymbolRef } from './focus-protocol'");
    });
});

describe('KnowledgeState', () => {
    it('kennt alle sechs Zustaende, darunter unsupported', () => {
        expect(KNOWLEDGE_STATES).toHaveLength(6);
        expect(KNOWLEDGE_STATES).toContain('unsupported');
        expect(new Set(KNOWLEDGE_STATES).size).toBe(6);
    });

    it('fuehrt jeden Zustand einzeln im Quelltext, ohne Sammelbegriff', () => {
        for (const state of KNOWLEDGE_STATES) {
            expect(SOURCE).toContain(`| '${state}'`);
        }
    });

    it('kollabiert nicht zu einem Boolean', () => {
        expect(SOURCE).toContain('Never collapse these into a boolean');
    });
});

describe('Evidence', () => {
    it('traegt jede Quelle des Originals', () => {
        for (const source of EVIDENCE_SOURCES) {
            expect(SOURCE).toContain(`| '${source}'`);
        }
        expect(EVIDENCE_SOURCES).toHaveLength(6);
    });

    it('verlangt Generation und Provider, damit alte Evidenz erkennbar bleibt', () => {
        expect(graphEvidence.engineGeneration).toBe(7);
        expect(graphEvidence.providerId).toBe('cbm-rpc');
        expect(SOURCE).toMatch(/engineGeneration: number;/);
        expect(SOURCE).toMatch(/providerId: string;/);
    });

    it('laesst observations bei einer Graphkante weg, statt null zu zaehlen', () => {
        expect(graphEvidence.observations).toBeUndefined();
        expect(traceEvidence.observations).toBe(12);
        expect(SOURCE).toContain('observations?: number;');
    });
});

describe('Fact', () => {
    it('bindet Wert, Zustand und Zitate zusammen', () => {
        const purpose: Fact<string> = {
            value: 'Legt einen Nutzer an, nachdem die Eingabe validiert wurde.',
            state: 'inferred',
            evidence: [graphEvidence],
        };
        expect(purpose.state).toBe('inferred');
        expect(purpose.evidence).toHaveLength(1);
        expect(SOURCE).toContain('export interface Fact<T>');
    });

    it('erlaubt eine Behauptung ohne Zitate nur mit einem Zustand, der das sagt', () => {
        const missingTests: Fact<boolean> = { value: false, state: 'notIndexed', evidence: [] };
        expect(missingTests.evidence).toEqual([]);
        expect(missingTests.state).not.toBe('known');
    });
});

describe('CallSite und RuntimeCall bleiben getrennt', () => {
    it('trennt die Zeile der Aufrufstelle von der Zeile der Deklaration', () => {
        const call: CallSite = {
            targetName: 'validateUser',
            targetFile: 'src/util/validate.ts',
            line: 24,
            targetLine: 8,
        };
        expect(call.line).not.toBe(call.targetLine);
        expect(SOURCE).toContain('targetLine?: number;');
    });

    it('zaehlt beobachtete Aufrufe und markiert unerwartete', () => {
        const observed: RuntimeCall = {
            targetName: 'sendWelcomeMail',
            count: 3,
            unexpected: true,
        };
        expect(observed.count).toBeGreaterThan(0);
        expect(observed.unexpected).toBe(true);
        expect(SOURCE).toContain('export interface RuntimeCall');
        expect(SOURCE).toContain('Never merged with {@link CallSite}');
    });
});

describe('SemanticIR', () => {
    it('haelt eine vollstaendige IR zusammen und bleibt bei schemaVersion 1', () => {
        const complexity: ComplexityFlags = {
            cyclomatic: 4,
            cognitive: 5,
            loopDepth: 1,
            transitiveLoopDepth: 2,
            linearScanInLoop: false,
            allocInLoop: false,
            unguardedRecursion: false,
            recursive: false,
            isEntryPoint: false,
            isExported: true,
        };
        const checklist: ChecklistItem[] = [
            { id: 'core-1', category: 'core-logic', label: 'Validierung gelesen', done: false },
        ];
        const ir: SemanticIR = {
            schemaVersion: 1,
            symbol,
            generation: 7,
            purpose: { value: 'Legt einen Nutzer an.', state: 'inferred', evidence: [graphEvidence] },
            steps: { value: [], state: 'unsupported', evidence: [] },
            calls: { value: [{ targetName: 'validateUser', line: 24 }], state: 'known', evidence: [graphEvidence] },
            calledBy: { value: [], state: 'known', evidence: [] },
            reads: { value: [], state: 'known', evidence: [] },
            writes: { value: [], state: 'known', evidence: [] },
            throws: { value: [], state: 'known', evidence: [] },
            externalEffects: { value: [], state: 'known', evidence: [] },
            tests: { value: [], state: 'notIndexed', evidence: [] },
            missingTests: { value: true, state: 'notIndexed', evidence: [] },
            complexity: { value: complexity, state: 'known', evidence: [graphEvidence] },
            risks: [],
            checklist,
        };

        expect(ir.schemaVersion).toBe(1);
        expect(ir.symbol.name).toBe('createUser');
        expect(ir.steps.state).toBe('unsupported');
        expect(ir.runtime).toBeUndefined();
        expect(ir.typeRefs).toBeUndefined();
        expect(ir.checklist[0].category).toBe('core-logic');
    });

    it('haelt runtime und typeRefs optional, damit Abwesenheit nicht als Leere gilt', () => {
        expect(SOURCE).toContain('runtime?: Fact<RuntimeCall[]>;');
        expect(SOURCE).toContain('typeRefs?: Fact<DataRef[]>;');
    });

    it('exportiert zur Laufzeit nichts, ist also reine Beschreibung', async () => {
        const module = await import('./semantic-ir');
        expect(Object.keys(module)).toEqual([]);
    });
});

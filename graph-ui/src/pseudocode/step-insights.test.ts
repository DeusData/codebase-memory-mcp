/**
 * What may be said about the symbol behind a call, and what may not.
 *
 * Three kinds of assertion, and the second is the reason the suite exists.
 *
 * The first kind is that a note is a recorded relation and nothing else: a
 * RAISES edge becomes "may raise", a CALLS edge becomes a count, a CONFIGURES
 * edge becomes an environment read, and an edge of any other kind becomes
 * nothing at all.
 *
 * The second kind is about the join. This fixture has two symbols called
 * `create`, in two files, exactly as the sample workspace of this project does.
 * A note that landed under the wrong step would be the worst failure this
 * feature can have: a true sentence about the wrong symbol reads as a fact and
 * is one only by accident. So a callee is matched on the qualified name, and a
 * call the index did not resolve gets nothing.
 *
 * The third kind is the measurement. `usable` says what the loaded data really
 * gave, `missing` says what it cannot give, and neither is allowed to be a
 * promise: the proof run writes both into the artifact, and a list that
 * declared a kind nobody measured would put a promise into the record.
 */

import { describe, expect, it } from 'vitest';

import type { CallSite } from '../core/semantic-ir';
import {
    ENRICHMENT_MISSING,
    STEP_INSIGHT_CAP,
    enrichmentAvailabilityOf,
    stepInsightsOf,
} from './step-insights';
import type { InsightGraph } from './step-insights';

const GRAPH: InsightGraph = {
    nodes: [
        { id: 1, name: 'validateId', qualified_name: 'p.src.util.validate.validateId', label: 'Function' },
        { id: 2, name: 'ValidationError', qualified_name: 'p.src.util.validate.ValidationError', label: 'Class' },
        { id: 3, name: 'query', qualified_name: 'p.src.repo.db.query', label: 'Function' },
        { id: 4, name: 'rows', qualified_name: 'p.src.repo.db.rows', label: 'Variable' },
        { id: 5, name: 'createUser', qualified_name: 'p.src.services.userService.createUser', label: 'Function' },
        { id: 6, name: 'DB_URL', qualified_name: '__env__DB_URL', label: 'EnvVar' },
        { id: 7, name: 'create', qualified_name: 'p.src.services.userService.create', label: 'Function' },
        { id: 8, name: 'create', qualified_name: 'p.src.services.orderService.create', label: 'Function' },
    ],
    edges: [
        { source: 1, target: 2, type: 'CALLS' },
        { source: 1, target: 2, type: 'RAISES' },
        { source: 3, target: 4, type: 'USAGE' },
        { source: 5, target: 6, type: 'CONFIGURES' },
        { source: 7, target: 5, type: 'CALLS' },
    ],
};

const call = (name: string, qualified?: string): CallSite => ({
    targetName: name,
    ...(qualified === undefined ? {} : { targetQualifiedName: qualified }),
});

describe('what the loaded graph says about a callee', () => {
    it('turns a RAISES edge into the same words the block uses for a raise', () => {
        const notes = stepInsightsOf([call('validateId', 'p.src.util.validate.validateId')], GRAPH);
        expect(notes.get('p.src.util.validate.validateId')?.map((note) => note.text))
            .toEqual(['may raise ValidationError', 'makes 1 call of its own']);
    });

    it('counts what the callee calls in turn, without naming an order it does not know', () => {
        const notes = stepInsightsOf([call('create', 'p.src.services.userService.create')], GRAPH);
        expect(notes.get('p.src.services.userService.create')?.map((note) => note.kind))
            .toEqual(['calls-on']);
    });

    it('reads an environment key off the relation that records one', () => {
        const notes = stepInsightsOf([call('createUser', 'p.src.services.userService.createUser')], GRAPH);
        expect(notes.get('p.src.services.userService.createUser')?.map((note) => note.text))
            .toEqual(['reads DB_URL from the environment']);
    });

    it('says nothing at all from a USAGE edge, which records no direction and no kind', () => {
        const notes = stepInsightsOf([call('query', 'p.src.repo.db.query')], GRAPH);
        expect(notes.has('p.src.repo.db.query')).toBe(false);
    });

    it('never matches on the bare name, because two files can hold the same one', () => {
        // Der Aufruf nennt `create` und den qualifizierten Namen des ANDEREN
        // `create`. Ein Namensvergleich haette hier die Notiz des einen unter
        // den Schritt des anderen geschrieben.
        const notes = stepInsightsOf([call('create', 'p.src.services.orderService.create')], GRAPH);
        expect(notes.size).toBe(0);
    });

    it('says nothing about a call the index did not resolve', () => {
        expect(stepInsightsOf([call('mystery')], GRAPH).size).toBe(0);
    });

    it('says nothing when this window holds no graph', () => {
        expect(stepInsightsOf([call('validateId', 'p.src.util.validate.validateId')], undefined).size).toBe(0);
        expect(stepInsightsOf([call('validateId', 'p.src.util.validate.validateId')], { nodes: [], edges: [] }).size)
            .toBe(0);
    });

    it('counts the names past its display bound instead of dropping them', () => {
        const many: InsightGraph = {
            nodes: [
                { id: 1, name: 'thrower', qualified_name: 'p.thrower' },
                ...Array.from({ length: STEP_INSIGHT_CAP + 2 }, (_, at) => ({
                    id: at + 2,
                    name: `Error${at}`,
                    qualified_name: `p.Error${at}`,
                })),
            ],
            edges: Array.from({ length: STEP_INSIGHT_CAP + 2 }, (_, at) => ({
                source: 1,
                target: at + 2,
                type: 'RAISES',
            })),
        };
        const notes = stepInsightsOf([call('thrower', 'p.thrower')], many);
        expect(notes.get('p.thrower')?.[0].text).toContain('and 2 more');
    });
});

describe('the measurement that stays in the artifact', () => {
    it('reports only the kinds that really answered here', () => {
        const notes = stepInsightsOf(
            [
                call('validateId', 'p.src.util.validate.validateId'),
                call('createUser', 'p.src.services.userService.createUser'),
            ],
            GRAPH,
        );
        const measured = enrichmentAvailabilityOf(notes, GRAPH);
        expect(measured.usable.map((entry) => entry.kind).sort())
            .toEqual(['calls-on', 'raises', 'reads-env']);
        for (const entry of measured.usable) {
            expect(entry.symbols).toBeGreaterThan(0);
            expect(entry.source.length).toBeGreaterThan(0);
        }
    });

    it('keeps the standing list of what no loaded data can answer', () => {
        const measured = enrichmentAvailabilityOf(new Map(), GRAPH);
        expect(measured.usable).toEqual([]);
        expect(measured.missing).toEqual([...ENRICHMENT_MISSING]);
        for (const entry of measured.missing) {
            expect(entry.reason.length).toBeGreaterThan(40);
        }
    });

    it('separates a kind that had nothing to say here from one that cannot say anything', () => {
        const notes = stepInsightsOf([call('validateId', 'p.src.util.validate.validateId')], GRAPH);
        const measured = enrichmentAvailabilityOf(notes, GRAPH);
        expect(measured.usable.map((entry) => entry.kind).sort()).toEqual(['calls-on', 'raises']);
        expect(measured.silent.map((entry) => entry.kind)).toEqual(['reads-env']);
        expect(measured.missing.map((entry) => entry.kind)).not.toContain('reads-env');
    });

    it('puts the whole of it under missing when no graph was loaded', () => {
        const measured = enrichmentAvailabilityOf(new Map(), undefined);
        expect(measured.missing[0].kind).toBe('every kind below');
        expect(measured.missing).toHaveLength(ENRICHMENT_MISSING.length + 1);
    });
});

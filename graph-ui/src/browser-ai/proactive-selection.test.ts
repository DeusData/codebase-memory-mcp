import { describe, expect, it } from 'vitest';
import { jsonbAggEvidence, jsonbAggScope } from './galaxy-evidence.fixture';
import { explanationInput } from './proactive-selection';

const key = (scope: string, context = jsonbAggEvidence()) => explanationInput(scope, undefined, context)!.key;

describe('selection identity for explanations', () => {
    it('keys a Galaxy selection by project, workspace, scope, direction, edge types and depth only', () => {
        const base = key('django-demo:galaxy');
        // Counts, rendered sizes and loading state change while a scope loads; they are not identity.
        const fewer = jsonbAggEvidence({ edges: jsonbAggScope().edges.slice(0, 5), state: 'loading-partial-preview' });
        expect(key('django-demo:galaxy', fewer)).toBe(base);
        expect(key('cbm:galaxy')).not.toBe(base);
        expect(key('django-demo:explore')).not.toBe(base);
        expect(key('django-demo:galaxy', jsonbAggEvidence({ depth: 2 }))).not.toBe(base);
        expect(key('django-demo:galaxy', jsonbAggEvidence({ direction: 'inbound' }))).not.toBe(base);
        const filtered = JSON.parse(jsonbAggEvidence().text);
        filtered.evidence.scope.edgeTypes = ['CALLS'];
        expect(key('django-demo:galaxy', { id: 'f', label: 'JSONBAgg', text: JSON.stringify(filtered) })).not.toBe(base);
        const other = JSON.parse(jsonbAggEvidence().text);
        other.evidence.selected.scope = { kind: 'node', id: 7, name: 'OrderableAggMixin' };
        expect(key('django-demo:galaxy', { id: 'o', label: 'OrderableAggMixin', text: JSON.stringify(other) })).not.toBe(base);
    });

    it('fingerprints the evidence of a complete scope only, so a re-index can retire a cached explanation', () => {
        const complete = explanationInput('p:galaxy', undefined, jsonbAggEvidence())!;
        expect(complete.evidence).toBe(jsonbAggEvidence().id);
        expect(explanationInput('p:galaxy', undefined, jsonbAggEvidence({ state: 'loading-partial-preview' }))?.evidence).toBeUndefined();
        const reindexed = explanationInput('p:galaxy', undefined, jsonbAggEvidence({ edges: jsonbAggScope().edges.slice(2) }))!;
        expect(reindexed.key).toBe(complete.key);
        expect(reindexed.evidence).not.toBe(complete.evidence);
    });

    it('marks incomplete Galaxy scopes as waiting and leaves other graph evidence unchanged', () => {
        expect(explanationInput('p:galaxy', undefined, jsonbAggEvidence())?.waiting).toBeUndefined();
        expect(explanationInput('p:galaxy', undefined, jsonbAggEvidence({ state: 'loading-partial-preview' }))?.waiting).toBe('loading');
        expect(explanationInput('p:galaxy', undefined, jsonbAggEvidence({ state: 'partial' }))?.waiting).toBe('partial');
        // A layer that stopped at the render limit is settled: it is explained, and its facts say it is partial (C1).
        const limited = jsonbAggEvidence({ state: 'render-limit-partial', renderLimit: { layer: 2, kind: 'nodes', limit: 500 } });
        expect(explanationInput('p:galaxy', undefined, limited)).toMatchObject({ evidence: limited.id });
        expect(explanationInput('p:galaxy', undefined, limited)?.waiting).toBeUndefined();
        const architecture = { id: 'a', label: 'Component', text: JSON.stringify({ evidence: { kind: 'current-selection-evidence', view: 'architecture-structure', selected: { name: 'api' } } }) };
        expect(explanationInput('p:architecture', undefined, architecture)).toEqual({ key: JSON.stringify(['p:architecture', 'Component', architecture.text]), label: 'Component', graph: architecture });
    });
});

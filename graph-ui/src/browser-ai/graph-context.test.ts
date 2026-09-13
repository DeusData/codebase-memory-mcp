import { describe, expect, it } from 'vitest';
import type { SemanticIR } from '../core/semantic-ir';
import { browserGraphContext } from './graph-context';

function fixture(): SemanticIR {
    // Deliberately partial provider data exercises the serialization boundary.
    return {
        schemaVersion: 1,
        generation: 7,
        symbol: { name: 'sum', qualifiedName: 'math.sum', kind: 'function', uri: 'file:///repo/src/sum.ts', range: { start: { line: 2, character: 0 }, end: { line: 8, character: 1 } }, projectName: 'sample', nodeId: 'node-3' },
        calls: { state: 'known', value: [{ targetName: 'add', targetQualifiedName: 'math.add', targetFile: 'src/add.ts', line: 4, targetLine: 8, args: ['provider-truncated-arg…'], confidence: 0.7, strategy: 'same-module' }], evidence: [{ source: 'graph-edge', relation: 'CALLS', file: 'src/sum.ts', range: { startLine: 4, endLine: 4 }, confidence: 0.7, engineGeneration: 7, providerId: 'cbm' }] },
        calledBy: { state: 'unknown', value: [], evidence: [] },
    } as unknown as SemanticIR;
}

describe('optional browser graph context', () => {
    it('offers context only for the active file and matching project', () => {
        const ir = fixture();
        expect(browserGraphContext(undefined, 'sample', 'src/sum.ts', 'src/sum.ts')).toEqual([]);
        expect(browserGraphContext(ir, 'sample', '', '')).toEqual([]);
        expect(browserGraphContext(ir, 'sample', 'src/other.ts', 'src/sum.ts')).toEqual([]);
        expect(browserGraphContext(ir, 'different-project', 'src/sum.ts', 'src/sum.ts')).toEqual([]);
        expect(browserGraphContext(ir, 'sample', 'src/sum.ts', 'src/sum.ts')).toHaveLength(2);
    });

    it('retains provenance, exact fact state and evidence while omitting truncated arguments', () => {
        const ir = fixture(); const [calls, callers] = browserGraphContext(ir, 'sample', 'src/sum.ts', 'src/sum.ts');
        const payload = JSON.parse(calls.text);
        expect(payload).toMatchObject({ project: 'sample', path: 'src/sum.ts', twinPath: 'src/sum.ts', generation: 7, symbol: ir.symbol, relation: 'calls', fact: { present: true, state: 'known', evidence: ir.calls.evidence } });
        expect(payload.fact.value).toEqual([{ targetName: 'add', targetQualifiedName: 'math.add', targetFile: 'src/add.ts', line: 4, targetLine: 8, confidence: 0.7, strategy: 'same-module' }]);
        expect(calls.text).not.toContain('provider-truncated-arg');
        expect(payload.limitations).toContain('not an exhaustive call graph');
        expect(calls.id).not.toBe(callers.id);
    });

    it('keeps unknown, known-empty, and absent facts distinguishable', () => {
        const ir = fixture();
        const unknown = JSON.parse(browserGraphContext(ir, 'sample', 'src/sum.ts', 'src/sum.ts')[1].text);
        expect(unknown.fact).toEqual({ present: true, state: 'unknown', value: [], evidence: [] });
        ir.calledBy = { state: 'known', value: [], evidence: [] };
        const empty = JSON.parse(browserGraphContext(ir, 'sample', 'src/sum.ts', 'src/sum.ts')[1].text);
        expect(empty.fact).toEqual({ present: true, state: 'known', value: [], evidence: [] });
        delete (ir as Partial<SemanticIR>).calledBy;
        const absent = browserGraphContext(ir, 'sample', 'src/sum.ts', 'src/sum.ts')[1];
        expect(JSON.parse(absent.text).fact).toEqual({ present: false });
        expect(absent.label).toContain('unavailable');
    });

    it('preserves ambiguous or inferred relationships without upgrading confidence', () => {
        const ir = fixture();
        ir.calls.state = 'ambiguous';
        ir.calledBy = { state: 'inferred', value: [{ name: 'spec', file: 'test/sum.ts', line: 9, nodeId: 'test-7', isTest: true, sourceKind: 'module' }], evidence: [{ source: 'graph-node', strategy: 'test-name', engineGeneration: 6, providerId: 'cbm' }] };
        const context = browserGraphContext(ir, 'sample', 'src/sum.ts', 'src/sum.ts');
        expect(JSON.parse(context[0].text).fact.state).toBe('ambiguous');
        expect(JSON.parse(context[1].text).fact).toEqual({ present: true, ...ir.calledBy });
    });

    it('serializes snapshots independent of later provider mutations', () => {
        const ir = fixture(); const before = browserGraphContext(ir, 'sample', 'src/sum.ts', 'src/sum.ts');
        ir.calls.value[0].targetName = 'changed'; ir.generation = 8;
        expect(JSON.parse(before[0].text).fact.value[0].targetName).toBe('add');
        expect(JSON.parse(before[0].text).generation).toBe(7);
        expect(browserGraphContext(ir, 'sample', 'src/sum.ts', 'src/sum.ts')[0].id).not.toBe(before[0].id);
    });
});

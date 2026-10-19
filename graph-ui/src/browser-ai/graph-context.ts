import type { CallSite, CallerRef, Fact, SemanticIR } from '../core/semantic-ir';
import type { BrowserChatContext } from './chat-model';

function callWithoutArguments(call: CallSite): Omit<CallSite, 'args'> {
    return {
        targetName: call.targetName,
        targetQualifiedName: call.targetQualifiedName,
        targetFile: call.targetFile,
        line: call.line,
        targetLine: call.targetLine,
        confidence: call.confidence,
        strategy: call.strategy,
    };
}

/** Optional evidence choices for the active file; the dock decides what the user attaches. */
export function browserGraphContext(ir: SemanticIR | undefined, project: string, activePath: string, twinPath: string): BrowserChatContext[] {
    if (!ir || !activePath || activePath !== twinPath || (ir.symbol.projectName && ir.symbol.projectName !== project)) return [];
    const context = (relation: 'calls' | 'calledBy', fact: Fact<CallSite[]> | Fact<CallerRef[]> | undefined): BrowserChatContext => {
        const value = relation === 'calls' ? (fact?.value as CallSite[] | undefined)?.map(callWithoutArguments) : fact?.value;
        return {
            id: `graph:${JSON.stringify([project, activePath, ir.symbol.uri, ir.symbol.nodeId ?? ir.symbol.qualifiedName ?? ir.symbol.name, ir.generation, relation])}`,
            label: `${relation === 'calls' ? 'Calls' : 'Called by'} · ${ir.symbol.name} · ${fact?.state ?? 'unavailable'}`,
            text: JSON.stringify({
                kind: 'graph-context-snapshot',
                project,
                path: activePath,
                twinPath,
                generation: ir.generation,
                symbol: ir.symbol,
                coordinates: { symbolRanges: 'zero-based', callLines: 'one-based', evidenceRanges: 'one-based inclusive' },
                relation,
                fact: fact ? { present: true, state: fact.state, value, evidence: fact.evidence } : { present: false },
                limitations: 'Provider results are best-effort, not an exhaustive call graph. An absent or unknown fact does not mean there are no relationships. Call argument excerpts are omitted because the provider may truncate them.',
            }, null, 2),
        };
    };
    return [context('calls', ir.calls), context('calledBy', ir.calledBy)];
}

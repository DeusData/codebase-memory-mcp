/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-intelligence/src/node/ir/semantic-ir-builder.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen:
 * die vier Regeln im Kopf unten sind die Ehrlichkeitsregeln dieses Produkts,
 * und ein Bau, der eine davon abschwaecht, waere kein Port mehr.
 * Aenderungen gegenueber dem Original: die Importpfade zeigen auf die
 * portierten Dateien; sha256 kommt aus core/hash statt aus node:crypto und ist
 * dort asynchron, weshalb der Snippet-Hash awaited wird; IrFactSource ist an
 * der Schnittstelle deklariert und wird hier nur weiterexportiert.
 */
/**
 * Assembling one symbol's semantic IR out of the provider's fact families.
 *
 * Four rules shape everything below.
 *
 * Rule one: a build never fails. The IR is what a panel renders, and a panel
 * that renders nothing because one of six queries timed out is worse than a
 * panel that renders five families and says the sixth is unknown. Every fan-out
 * leg is therefore wrapped, and a failed leg produces a `Fact` in state
 * `unknown` with no evidence plus one warning naming the family.
 *
 * Rule two: the fan-out is parallel, because the families are independent and
 * the twin is on the interaction path. The only families issued together are
 * callers and tests, and only because the provider derives the tested-by
 * inference from the caller rows: asking separately would run the same query
 * twice and could return two answers that disagree.
 *
 * Rule three: nothing here invents a fact. The purpose sentence is a template
 * over counts the provider actually returned, and it says so by being
 * `inferred` with a named strategy rather than `known`. When the engine grows
 * a docstring relation this is the one function that changes.
 *
 * Rule four: file references leave here as absolute URIs. The engine reports
 * workspace-relative paths, the editor opens URIs, and doing the conversion in
 * every consumer is how a UI ends up opening `src/util/validate.ts` relative to
 * whatever the process happened to have as its working directory.
 */

import { sha256Hex } from '../core/hash';

import type { FactKind, IrFactSource, SymbolFacts } from '../core/intelligence-provider';
import type { SymbolRef } from '../core/focus-protocol';
import type {
    CallerRef,
    CallSite,
    ComplexityFlags,
    DataRef,
    Evidence,
    ExternalEffect,
    Fact,
    Risk,
    SemanticIR,
    TestRef,
    ThrowRef
} from '../core/semantic-ir';

import { generateChecklist } from './checklist-generator';
import { toFileUri } from './file-uri';

/**
 * Re-exported because this module has been the home of the conversion since the
 * IR builder landed and its consumers import it from here. The implementation
 * moved to `file-uri.ts` so the checklist generator can build navigation
 * targets without importing the builder that calls it.
 */
export { toFileUri } from './file-uri';

/**
 * The slice of a provider this builder needs. Kept narrow so a test fake is
 * small. Declared at the provider boundary and re-exported here, so the
 * imports of everything that already consumed it stay as they were.
 */
export type { IrFactSource } from '../core/intelligence-provider';

/** Knobs for one build. */
export interface BuildIrOptions {
    /** Pinned project identity, passed straight through to the provider. */
    projectName?: string;
    /** Index generation stamped on every piece of evidence. Defaults to 0. */
    generation?: number;
}

/** A built IR plus one sentence per family that could not be answered. */
export interface BuildIrResult {
    ir: SemanticIR;
    warnings: string[];
}

/** Strategy recorded on the purpose sentence, so its provenance is never mistaken. */
export const DERIVED_SUMMARY_STRATEGY = 'derived-summary';

/** Machine-readable family of the one risk rule this build can evaluate. */
export const RISK_UNTESTED_HUB = 'untested-hub';

/** How many callers make a symbol a hub for the purpose of the untested-hub rule. */
export const UNTESTED_HUB_CALLERS = 3;

/**
 * Structural signals the 0.9.0 engine does not record.
 *
 * Returned as zeros rather than as absent numbers so `ComplexityFlags` stays a
 * total type, and always paired with the `unsupported` state: the zeros are
 * never a claim that the symbol is trivial, they are the shape a fact takes
 * when nobody measured it. Every consumer must read the state, not the value.
 */
export function unmeasuredComplexity(): ComplexityFlags {
    return {
        cyclomatic: 0,
        cognitive: 0,
        loopDepth: 0,
        transitiveLoopDepth: 0,
        linearScanInLoop: false,
        allocInLoop: false,
        unguardedRecursion: false,
        recursive: false,
        isEntryPoint: false,
        isExported: false
    };
}

/** `n thing` or `n things`, so a generated sentence never reads as machine output. */
export function countOf(count: number, singular: string, plural: string): string {
    return `${count} ${count === 1 ? singular : plural}`;
}

/** Last path segment of a URI or path, for the purpose sentence. */
export function displayFile(uri: string): string {
    const path = uri.split('?')[0].split('#')[0];
    const segments = path.split('/');
    return decodeURIComponent(segments[segments.length - 1] || path);
}

/**
 * The one sentence the twin leads with.
 *
 * A template over counts, not a summary of behaviour: it says what the symbol
 * touches, which is exactly what the graph knows, and never why it exists,
 * which the graph does not. It is stamped `inferred` for that reason.
 */
export function purposeSentence(
    symbol: SymbolRef,
    calls: number,
    envReads: number,
    throws: number
): string {
    const kind = symbol.kind.charAt(0).toUpperCase() + symbol.kind.slice(1);
    const file = displayFile(symbol.uri);
    return `${kind} ${symbol.name} in ${file}.`
        + ` Makes ${countOf(calls, 'call', 'calls')},`
        + ` touches ${countOf(envReads, 'environment value', 'environment values')},`
        + ` can raise ${countOf(throws, 'error type', 'error types')}.`;
}

/**
 * Order the narrative.
 *
 * Ascending by call-site line is the order the reader's eye takes through the
 * body. A call whose site line the engine did not record cannot be placed, so
 * it goes last rather than being guessed into position; ties keep the order the
 * provider returned, which is the engine's own.
 */
export function orderSteps(calls: CallSite[]): CallSite[] {
    return calls
        .map((call, index) => ({ call, index }))
        .sort((a, b) => {
            const lineA = a.call.line ?? Number.MAX_SAFE_INTEGER;
            const lineB = b.call.line ?? Number.MAX_SAFE_INTEGER;
            return lineA === lineB ? a.index - b.index : lineA - lineB;
        })
        .map(entry => entry.call);
}

/**
 * The risk rules this build can evaluate.
 *
 * Only one rule fires today, and the reason the list is short is worth stating:
 * every other rule worth having needs the structural flags the engine does not
 * record, and a rule computed from absent data would be a guess wearing a
 * severity. Construction-heavy code is deliberately not a risk: `new` in a
 * factory is the point of the factory.
 */
export function evaluateRisks(
    symbol: SymbolRef,
    callers: Fact<CallerRef[]>,
    tests: Fact<TestRef[]>
): Risk[] {
    const risks: Risk[] = [];
    const isHub = callers.value.length >= UNTESTED_HUB_CALLERS;
    const untested = tests.value.length === 0 && tests.state === 'inferred';
    if (isHub && untested) {
        risks.push({
            id: RISK_UNTESTED_HUB,
            severity: 'medium',
            kind: RISK_UNTESTED_HUB,
            message: `${symbol.name} is called from ${countOf(callers.value.length, 'place', 'places')}`
                + ' and no test caller was found, so a change here is unverified everywhere it is used.'
        });
    }
    return risks;
}

// ---------------------------------------------------------------------------

/** A fact family that could not be answered: no value, no evidence, and it says so. */
function unknownFact<T>(value: T): Fact<T> {
    return { value, state: 'unknown', evidence: [] };
}

/** A family this provider never answers for this language. */
function unsupportedFact<T>(value: T): Fact<T> {
    return { value, state: 'unsupported', evidence: [] };
}

/** Outcome of one fan-out leg. A rejection is data here, not an exception. */
interface LegResult {
    facts: SymbolFacts;
    error?: string;
}

async function requestLeg(
    provider: IrFactSource,
    root: string,
    symbol: SymbolRef,
    kinds: FactKind[],
    opts: BuildIrOptions
): Promise<LegResult> {
    try {
        return { facts: await provider.getFacts(root, symbol, kinds, opts) };
    } catch (error) {
        return { facts: {}, error: messageOf(error) };
    }
}

function messageOf(error: unknown): string {
    if (error instanceof Error) {
        return error.message;
    }
    return String(error);
}

/** Rewrite every file reference in a list of records to an absolute URI. */
function withUris<T extends { file?: string }>(root: string, entries: T[]): T[] {
    return entries.map(entry => ({ ...entry, file: toFileUri(root, entry.file) }));
}

function evidenceWithUris(root: string, evidence: Evidence[]): Evidence[] {
    return evidence.map(entry => ({ ...entry, file: toFileUri(root, entry.file) }));
}

/** Normalise one fact's file references, leaving state and evidence semantics alone. */
function normalizeFact<T extends { file?: string }>(root: string, fact: Fact<T[]>): Fact<T[]> {
    return {
        ...fact,
        value: withUris(root, fact.value),
        evidence: evidenceWithUris(root, fact.evidence)
    };
}

function normalizeCalls(root: string, fact: Fact<CallSite[]>): Fact<CallSite[]> {
    return {
        ...fact,
        value: fact.value.map(call => ({ ...call, targetFile: toFileUri(root, call.targetFile) })),
        evidence: evidenceWithUris(root, fact.evidence)
    };
}

/**
 * Build the whole IR for one symbol.
 *
 * Never rejects for a fact family: only a caller that passes something that is
 * not a symbol can make this throw.
 */
export async function buildIr(
    provider: IrFactSource,
    root: string,
    symbol: SymbolRef,
    opts: BuildIrOptions = {}
): Promise<BuildIrResult> {
    const generation = opts.generation ?? 0;
    const warnings: string[] = [];

    // One leg per independent family, plus the snippet. Callers and tests
    // share a leg because the provider derives one from the other's rows.
    const [calleesLeg, callersLeg, throwsLeg, envLeg, typesLeg, snippet] = await Promise.all([
        requestLeg(provider, root, symbol, ['callees'], opts),
        requestLeg(provider, root, symbol, ['callers', 'testedBy'], opts),
        requestLeg(provider, root, symbol, ['throws'], opts),
        requestLeg(provider, root, symbol, ['envReads'], opts),
        requestLeg(provider, root, symbol, ['typeRefs'], opts),
        requestSnippet(provider, root, symbol, opts)
    ]);

    const note = (family: string, error: string | undefined): void => {
        if (error !== undefined) {
            warnings.push(`${family} is unknown: ${error}`);
        }
    };
    note('callees', calleesLeg.error);
    note('callers and tests', callersLeg.error);
    note('throws', throwsLeg.error);
    note('envReads', envLeg.error);
    note('typeRefs', typesLeg.error);
    if (snippet.error !== undefined) {
        warnings.push(`the source snippet is unavailable: ${snippet.error}`);
    }

    const calls = normalizeCalls(root, calleesLeg.facts.callees ?? unknownFact<CallSite[]>([]));
    const calledBy = normalizeFact(root, callersLeg.facts.callers ?? unknownFact<CallerRef[]>([]));
    const tests = normalizeFact(root, callersLeg.facts.testedBy ?? unknownFact<TestRef[]>([]));
    const throwsFact = normalizeFact(root, throwsLeg.facts.throws ?? unknownFact<ThrowRef[]>([]));
    const reads = normalizeFact(root, envLeg.facts.envReads ?? unknownFact<DataRef[]>([]));
    const typeRefs = normalizeFact(root, typesLeg.facts.typeRefs ?? unknownFact<DataRef[]>([]));

    const steps: Fact<CallSite[]> = { ...calls, value: orderSteps(calls.value) };

    const purpose: Fact<string> = {
        value: purposeSentence(symbol, calls.value.length, reads.value.length, throwsFact.value.length),
        state: 'inferred',
        evidence: [{
            source: 'graph-node',
            strategy: DERIVED_SUMMARY_STRATEGY,
            file: symbol.uri,
            engineGeneration: generation,
            providerId: provider.id
        }]
    };

    // "No test caller was found" and "nobody looked for one" are different
    // claims, so the boolean carries the tests family's own state.
    const missingTests: Fact<boolean> = {
        value: tests.value.length === 0,
        state: tests.state,
        evidence: tests.evidence
    };

    const ir: SemanticIR = {
        schemaVersion: 1,
        symbol,
        generation,
        snippetHash: snippet.hash,
        purpose,
        steps,
        calls,
        calledBy,
        reads,
        // The 0.9.0 engine records no write relation at all, which is a
        // different answer from an empty list of writes.
        writes: unsupportedFact<DataRef[]>([]),
        typeRefs,
        throws: throwsFact,
        externalEffects: unsupportedFact<ExternalEffect[]>([]),
        tests,
        missingTests,
        complexity: unsupportedFact(unmeasuredComplexity()),
        risks: evaluateRisks(symbol, calledBy, tests),
        checklist: generateChecklist({
            root,
            symbol,
            calls: calls.value,
            callers: calledBy.value,
            throws: throwsFact.value,
            envReads: reads.value,
            typeRefs: typeRefs.value,
            tests: tests.value,
            testsState: tests.state
        })
    };
    return { ir, warnings };
}

/**
 * Hash the stored source, so a cached IR can be invalidated by an edit rather
 * than by a clock. A provider that cannot produce the snippet is not a build
 * failure: the hash is simply absent, and the warning says why.
 */
async function requestSnippet(
    provider: IrFactSource,
    root: string,
    symbol: SymbolRef,
    opts: BuildIrOptions
): Promise<{ hash?: string; error?: string }> {
    const qualifiedName = symbol.qualifiedName;
    if (!qualifiedName) {
        return { error: 'the symbol has no qualified name to look up' };
    }
    try {
        const source = await provider.getSnippet(root, qualifiedName, opts);
        if (source.length === 0) {
            return { error: 'the provider returned no source for this symbol' };
        }
        return { hash: await sha256Hex(source) };
    } catch (error) {
        return { error: messageOf(error) };
    }
}

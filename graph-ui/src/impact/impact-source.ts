/**
 * The four readings behind one change assessment, in the order they depend on
 * each other.
 *
 * The reference does this inside its widget; here it is a function, for the
 * reason every other reading in this project is one: a browser run can prove
 * that a page appeared, and only a unit test can prove that the page was built
 * out of four readings and not out of three and a default.
 *
 *  1. **The change set.** `detect_changes` through the provider, which reads the
 *     tool's answer in whichever of its two shapes arrived and recovers the
 *     identities the answer leaves out.
 *  2. **The summary**, for the entry points and the hotspot fan-in.
 *  3. **The routes.** Not in the summary for TypeScript, so they are read off
 *     the source text; see route-scan.ts for the whole of that gap.
 *  4. **The per-symbol readings.** Complexity for the affected set in one batch,
 *     and test callers one symbol at a time, capped.
 *
 * The cap on the test lookup is the one bound that changes what the page claims,
 * so it is carried into the model rather than applied and forgotten: a symbol
 * past it is unchecked, which is neither covered nor uncovered, and the
 * narrative says how many.
 */

import { buildComplexityLookup, mapChangeImpact } from './impact-model';
import type { ImpactModel, ImpactTestLookup } from './impact-model';
import { mergeRoutes, scanRoutes } from './route-scan';
import type { ScannedSource } from './route-scan';
import { impactRouteScanNote } from './impact-strings';
import { twinTargetOf } from '../twin/twin-target';
import type {
    ArchitectureOverviewDto,
    ChangeImpactDto,
    ChangeImpactSymbol,
    ProviderQueryOptions,
    SymbolComplexity,
    SymbolFacts,
} from '../core/intelligence-provider';
import type { SymbolRef } from '../core/focus-protocol';
import type { TestRef } from '../core/semantic-ir';

/**
 * How many affected symbols are asked about their test callers.
 *
 * One query each, so this is the bound that decides how long the page takes on a
 * change that touched a hundred symbols. Twelve is the size of a list somebody
 * reads before deciding what to run.
 */
export const IMPACT_TEST_LOOKUP_CAP = 12;

/** What this reading needs from a provider. A slice, so a test needs no server. */
export interface ImpactSource {
    changeImpact(root: string, sinceRef?: string, opts?: ProviderQueryOptions): Promise<ChangeImpactDto>;
    architectureOverview(root: string, opts?: ProviderQueryOptions): Promise<ArchitectureOverviewDto>;
    getComplexity(root: string, qualifiedNames: string[], opts?: ProviderQueryOptions): Promise<SymbolComplexity[]>;
    getFacts(root: string, symbol: SymbolRef, kinds: 'testedBy'[], opts?: ProviderQueryOptions): Promise<SymbolFacts>;
}

export interface ImpactReadOptions extends ProviderQueryOptions {
    /** The comparison point, already checked for its shape. Absent means the working tree. */
    sinceRef?: string;
    /** Reads one workspace file, or answers undefined when it cannot be read. */
    readSource: (filePath: string) => Promise<ScannedSource | undefined>;
    testCap?: number;
}

export interface ImpactReading {
    model: ImpactModel;
    /** What the route scan opened and what it did not. Empty when nothing was scanned. */
    routeNote: string;
    /** The change set as it arrived, for a caller that wants to say what it asked. */
    change: ChangeImpactDto;
}

/** The symbols worth asking about, in the order the change set reported them. */
export function testCandidates(
    symbols: readonly ChangeImpactSymbol[],
    cap: number,
): ChangeImpactSymbol[] {
    const seen = new Set<string>();
    const out: ChangeImpactSymbol[] = [];
    for (const symbol of symbols) {
        const key = symbol.qualifiedName;
        if (symbol.changeKind === 'module' || symbol.isTest === true || key === undefined || seen.has(key)) {
            continue;
        }
        seen.add(key);
        out.push(symbol);
        if (out.length >= cap) {
            break;
        }
    }
    return out;
}

/**
 * Test callers for as many affected symbols as the cap allows.
 *
 * A symbol whose answer came back `unknown` or `unsupported` is left out of
 * `checked` on purpose: nobody answered, so it is unchecked, and counting it as
 * uncovered would turn a backend that did not reply into a finding about the
 * repository.
 */
async function testLookup(
    source: ImpactSource,
    root: string,
    symbols: readonly ChangeImpactSymbol[],
    cap: number,
    opts: ProviderQueryOptions,
): Promise<ImpactTestLookup> {
    const candidates = testCandidates(symbols, cap);
    const bySymbol = new Map<string, TestRef[]>();
    const checked = new Set<string>();
    for (const symbol of candidates) {
        const ref = twinTargetOf({
            name: symbol.name,
            qualifiedName: symbol.qualifiedName,
            kind: symbol.kind,
            filePath: symbol.filePath,
            startLine: symbol.line,
        });
        if (ref === undefined) {
            continue;
        }
        const facts = await source.getFacts(root, ref, ['testedBy'], opts).catch(() => undefined);
        const fact = facts?.testedBy;
        if (fact === undefined || (fact.state !== 'known' && fact.state !== 'inferred')) {
            continue;
        }
        checked.add(symbol.qualifiedName!);
        bySymbol.set(symbol.qualifiedName!, fact.value);
    }
    const more = symbols.some(
        (symbol) =>
            symbol.changeKind !== 'module'
            && symbol.isTest !== true
            && symbol.qualifiedName !== undefined
            && !candidates.some((candidate) => candidate.qualifiedName === symbol.qualifiedName),
    );
    return { bySymbol, checked, ...(more ? { cappedAt: cap } : {}) };
}

/** The whole assessment, from the four readings. */
export async function readImpact(
    source: ImpactSource,
    root: string,
    options: ImpactReadOptions,
): Promise<ImpactReading> {
    const opts: ProviderQueryOptions = {
        ...(options.projectName === undefined ? {} : { projectName: options.projectName }),
        ...(options.generation === undefined ? {} : { generation: options.generation }),
    };
    const cap = options.testCap ?? IMPACT_TEST_LOOKUP_CAP;

    const change = await source.changeImpact(root, options.sinceRef, opts);
    const overview = await source.architectureOverview(root, opts);

    const scan = await scanRoutes(overview.files, options.readSource);
    const architecture: ArchitectureOverviewDto = {
        ...overview,
        routes: mergeRoutes(scan.routes, overview.routes),
    };

    const qualifiedNames = [
        ...new Set(
            change.symbols
                .filter((symbol) => symbol.changeKind !== 'module')
                .map((symbol) => symbol.qualifiedName)
                .filter((name): name is string => name !== undefined),
        ),
    ];
    const complexity = qualifiedNames.length === 0
        ? []
        : await source.getComplexity(root, qualifiedNames, opts).catch(() => []);

    const tests = await testLookup(source, root, change.symbols, cap, opts);

    return {
        model: mapChangeImpact(change, architecture, buildComplexityLookup(complexity, architecture), tests),
        routeNote: scan.scanned === 0 && scan.cappedAt === undefined
            ? ''
            : impactRouteScanNote(scan.scanned, scan.cappedAt, scan.truncatedFiles),
        change,
    };
}

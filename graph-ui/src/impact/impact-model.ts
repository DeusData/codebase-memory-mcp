/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/src/browser/impact/impact-model.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen:
 * mapChangeImpact, buildComplexityLookup, entryPointNames, riskInputFor,
 * reasonsFor, affectedEndpoints, summariseTests, buildNarrative, endpointLabel,
 * alle DTO-Formen und jede Begruendung im Kopf. Die Datei ist rein: ein Dokument
 * rein, ein Dokument raus, kein React und kein Server.
 *
 * Aenderungen gegenueber dem Original, beide unten am Ort begruendet:
 *
 * - Die Importe zeigen auf die ebenfalls portierten Dateien dieses Projekts
 *   (core/intelligence-provider.ts, core/semantic-ir.ts, impact-strings.ts).
 *   PROGRAM_START_NAMES stand dort in map/map-model.ts; eine Karte gibt es hier
 *   nicht, also steht die Liste hier, mit derselben Begruendung.
 * - Dazu kommen zwei Bausteine, die es dort nicht gibt und hier geben muss:
 *   {@link refRejection} prueft einen git-Ref im Browser, weil dieses Backend
 *   keinen Ref ablehnt, sondern einen unbekannten kommentarlos annimmt und ueber
 *   den Arbeitsbaum antwortet; und {@link badgeRules} zaehlt die erfuellten
 *   Regeln als Saetze auf, weil das Wort neben der Erzaehlung sonst eine
 *   Behauptung ohne Herleitung waere.
 */
/**
 * Turning a change set into the page the impact view draws.
 *
 * Pure by rule, like the tour generator next door: a document in, a document
 * out, no widget and no service. That matters more here than anywhere else in
 * the product, because this is the file that decides to print the word HIGH, and
 * a judgement that can only be checked by driving a browser is a judgement
 * nobody checks.
 *
 * Four decisions shape everything below.
 *
 * **A file is not a symbol.** The analysis reports the changed file itself as
 * an impacted symbol, labelled `Module` and named after its own path. Those
 * rows are counted as changed files, never as changed symbols, and never given
 * a risk level: "the risk of orderService.ts" is not a sentence with a meaning,
 * and a page that printed one would be padding its own summary.
 *
 * **An endpoint is matched two ways and says which.** A route whose
 * registration names a handler the change reaches is a strong claim. A route
 * whose registration merely sits in a file the change reaches is a weaker one,
 * and it is the only one available for TypeScript, where the routes are read
 * off the source text and no handler is named. Both are shown; the second says
 * that it is the file rather than the function.
 *
 * **Every sentence carries the reading it came from.** The narrative is a list
 * of claims, each with an evidence row naming the source and the figures behind
 * it. There is no sentence in the output that a reader cannot trace, which is
 * what makes the risk word answerable rather than authoritative.
 *
 * **What was not looked at is stated.** Test callers are looked up for a capped
 * number of symbols and complexity is unavailable for whole languages. Both
 * produce a sentence rather than a silence, because a quiet page and a clean
 * page look identical.
 */

import type {
    ArchitectureOverviewDto,
    ChangeImpactDto,
    ChangeImpactSymbol,
    RouteRef,
    SymbolComplexity,
} from '../core/intelligence-provider';
import type { TestRef } from '../core/semantic-ir';

import {
    IMPACT_SIGNAL_ENTRY_POINT,
    IMPACT_WALK_TRUNCATED,
    MAP_SIGNAL_ALLOCATION_IN_LOOP,
    MAP_SIGNAL_SCAN_IN_LOOP,
    MAP_SIGNAL_UNGUARDED_RECURSION,
    countOf,
    impactChangedSentence,
    impactDownstreamSentence,
    impactEndpointRule,
    impactEndpointSentence,
    impactSeedSentence,
    impactSymbolRule,
    impactTestLookupCapped,
    impactTestsSentence,
    impactUnmeasuredSentence,
    impactUntestedRule,
    mapSignalBranches,
    mapSignalFanIn,
    mapSignalNesting,
    mapSignalThinking,
} from './impact-strings';
import {
    RISK_THRESHOLDS,
    completenessOf,
    overallRisk,
    perSymbolRisk,
} from './risk-rules';
import type { RiskInputCompleteness, RiskLevel, SymbolRiskInput } from './risk-rules';

/**
 * Names that mean "this is where the program starts".
 *
 * In the reference this list lives in the workspace map and the impact rules
 * import it from there; there is no map here, so it lives with the rules that
 * need it. The reason for the list is unchanged: the index flags every exported
 * top-level symbol as an entry point, so being on that list means "reachable
 * from outside its own module" and not "reachable from outside the program".
 * These names, and a route handler, are the two cases where the stronger
 * reading actually holds.
 */
export const PROGRAM_START_NAMES: readonly string[] = ['main', '__main__'];

/** Where a row of the page points. Enough to open a file and to ask about it. */
export interface ImpactTarget {
    name: string;
    qualifiedName?: string;
    /** Workspace-relative path. */
    filePath?: string;
    /** 1-based line. */
    line?: number;
}

/** One changed symbol, with the level the rules gave it and why. */
export interface ImpactDirectRow extends ImpactTarget {
    kind: string;
    risk: RiskLevel;
    /** One phrase per reading that reached a threshold. Never a score. */
    reasons: string[];
}

/** One symbol the change reaches by calls, at a known distance. */
export interface ImpactDownstreamRow extends ImpactTarget {
    risk: RiskLevel;
    reasons: string[];
    isTest?: boolean;
}

/** One band of the downstream list. */
export interface ImpactDownstreamGroup {
    distance: number;
    symbols: ImpactDownstreamRow[];
}

/** One endpoint the change reaches, and how strongly it was tied to it. */
export interface ImpactEndpoint {
    routePath: string;
    method?: string;
    /** `handler` when the registration named a symbol in the change set, `file` otherwise. */
    via: 'handler' | 'file';
    filePath?: string;
    /** 1-based line of the registration. */
    line?: number;
    origin?: RouteRef['origin'];
}

/** One test worth running, and what it reaches. */
export interface ImpactCoveringTest {
    name: string;
    file?: string;
    /** 1-based line. */
    line?: number;
    /** Display names of the affected symbols this test calls. */
    covers: string[];
}

/** What the tests section holds. Two lists, never merged into a percentage. */
export interface ImpactTests {
    covering: ImpactCoveringTest[];
    /** Affected symbols that were checked and had no test caller. */
    missing: ImpactTarget[];
    /** How many affected symbols were actually checked. */
    checked: number;
    /** The cap that stopped the lookup short, when one did. */
    cappedAt?: number;
}

/** The figures across the top. Counts of rows below, never derived scores. */
export interface ImpactSummaryTiles {
    changedFiles: number;
    directSymbols: number;
    /** One entry per distance the walk reached, ascending. */
    indirect: { distance: number; count: number }[];
    endpoints: number;
    testsAffected: number;
    untestedAffected: number;
}

/** One claim's backing. `value` is the figures, not a sentence. */
export interface ImpactEvidence {
    claim: string;
    source: 'detect_changes' | 'architecture' | 'facts';
    value: string;
}

/** The paragraph, its badge, and one evidence row per claim in it. */
export interface ImpactNarrative {
    /** The claims in reading order, joined by a space. */
    text: string;
    badge: RiskLevel;
    evidence: ImpactEvidence[];
}

/** Everything the impact view draws. */
export interface ImpactModel {
    summaryTiles: ImpactSummaryTiles;
    narrative: ImpactNarrative;
    direct: ImpactDirectRow[];
    downstream: ImpactDownstreamGroup[];
    endpoints: ImpactEndpoint[];
    tests: ImpactTests;
    /** The badge again, so a caller does not have to reach into the narrative. */
    risk: RiskLevel;
    /** How much of the risk input the analysis actually supplied. */
    completeness: RiskInputCompleteness;
}

/** What was looked up about the affected symbols, and how far the lookup got. */
export interface ImpactTestLookup {
    /** Test callers per affected symbol, keyed by qualified name. */
    bySymbol: ReadonlyMap<string, TestRef[]>;
    /** Qualified names the lookup actually covered. Anything else was not checked. */
    checked: ReadonlySet<string>;
    /** The cap that stopped the lookup, when one did. */
    cappedAt?: number;
}

/** An empty lookup, for the states where nothing has been asked yet. */
export const NO_TEST_LOOKUP: ImpactTestLookup = { bySymbol: new Map(), checked: new Set() };

/**
 * Complexity readings keyed by qualified name, from the two places they live.
 *
 * The batched read is the precise answer and the summary's hotspot list is the
 * one already paid for, so both are folded in: the read wins on the signals it
 * carries, and the hotspot contributes the fan-in, which the per-symbol columns
 * do not include. Merging rather than choosing is what keeps a symbol that the
 * summary calls a hotspot from being drawn here with nothing on it.
 */
export function buildComplexityLookup(
    readings: readonly SymbolComplexity[],
    architecture: ArchitectureOverviewDto | undefined,
): Map<string, SymbolComplexity> {
    const lookup = new Map<string, SymbolComplexity>();
    for (const hotspot of architecture?.hotspots ?? []) {
        const key = hotspot.qualifiedName;
        if (key) {
            lookup.set(key, hotspot);
        }
    }
    for (const reading of readings) {
        const key = reading.qualifiedName;
        if (!key) {
            continue;
        }
        const known = lookup.get(key);
        lookup.set(key, known === undefined ? reading : { ...reading, fanIn: reading.fanIn ?? known.fanIn });
    }
    return lookup;
}

/**
 * The whole page from one change set.
 *
 * `undefined` inputs are valid and mean the answer has not arrived: the page
 * comes back empty, which is what the surface renders while it waits.
 */
export function mapChangeImpact(
    change: ChangeImpactDto | undefined,
    architecture: ArchitectureOverviewDto | undefined,
    complexityLookup: ReadonlyMap<string, SymbolComplexity>,
    tests: ImpactTestLookup = NO_TEST_LOOKUP,
): ImpactModel {
    const symbols = change?.symbols ?? [];
    const declared = symbols.filter((symbol) => symbol.changeKind === 'declared');
    const callers = symbols.filter((symbol) => symbol.changeKind === 'caller');
    const entryPoints = entryPointNames(architecture);

    const inputFor = (symbol: ChangeImpactSymbol): SymbolRiskInput =>
        riskInputFor(symbol, complexityLookup, entryPoints, tests);

    const direct: ImpactDirectRow[] = declared.map((symbol) => {
        const input = inputFor(symbol);
        return {
            ...targetOf(symbol),
            kind: symbol.kind,
            risk: perSymbolRisk(input),
            reasons: reasonsFor(input),
        };
    });

    const downstream = groupByDistance(callers, (symbol) => {
        const input = inputFor(symbol);
        return {
            ...targetOf(symbol),
            risk: perSymbolRisk(input),
            reasons: reasonsFor(input),
            ...(symbol.isTest === undefined ? {} : { isTest: symbol.isTest }),
        };
    });

    const endpoints = affectedEndpoints(change, architecture);
    const testSummary = summariseTests(symbols, tests);
    const inputs = [...declared, ...callers].map(inputFor);
    const completeness = completenessOf(inputs);
    const risk = overallRisk(inputs.map(perSymbolRisk), endpoints.length, testSummary.missing.length);

    const summaryTiles: ImpactSummaryTiles = {
        changedFiles: change?.changedFiles.length ?? 0,
        directSymbols: declared.length,
        indirect: downstream.map((group) => ({ distance: group.distance, count: group.symbols.length })),
        endpoints: endpoints.length,
        testsAffected: testSummary.covering.length,
        untestedAffected: testSummary.missing.length,
    };

    return {
        summaryTiles,
        narrative: buildNarrative({
            change,
            architecture,
            declared: declared.length,
            callers: callers.length,
            walkedDistance: change?.walkedDistance ?? 0,
            endpoints,
            tests: testSummary,
            completeness,
            badge: risk,
        }),
        direct,
        downstream,
        endpoints,
        tests: testSummary,
        risk,
        completeness,
    };
}

// Risk inputs -----------------------------------------------------------------

function targetOf(symbol: ChangeImpactSymbol): ImpactTarget {
    return {
        name: symbol.name,
        qualifiedName: symbol.qualifiedName,
        filePath: symbol.filePath,
        line: symbol.line,
    };
}

/**
 * The symbols that really are reached from outside the program.
 *
 * The narrow reading, and the difference matters more here than anywhere else
 * in the product. The index flags every exported top-level symbol as an entry
 * point, so its list means "reachable from outside its own module": on a
 * TypeScript project that is most of the codebase. Feeding that list into a rule
 * whose output is the word HIGH would make every change to any exported function
 * high risk, which is the same as having no rule: a level that is always the
 * maximum tells a reader nothing and trains them to ignore the one time it means
 * something.
 *
 * So only two cases count. A route handler is an address someone outside the
 * process can type. A symbol named `main` is where the program starts. Both are
 * the strong claim; an export is not, and it is left to the complexity and
 * fan-in rules, which is where it belongs.
 *
 * Both spellings of a name are kept because the summary fills one or the other
 * depending on the language.
 */
export function entryPointNames(architecture: ArchitectureOverviewDto | undefined): Set<string> {
    const names = new Set<string>();
    for (const entry of architecture?.entryPoints ?? []) {
        if (!PROGRAM_START_NAMES.includes(entry.name)) {
            continue;
        }
        if (entry.qualifiedName) {
            names.add(entry.qualifiedName);
        }
        names.add(entry.name);
    }
    for (const route of architecture?.routes ?? []) {
        if (route.handler) {
            names.add(route.handler);
        }
    }
    return names;
}

/**
 * What the rules get to see about one symbol.
 *
 * A field the analysis did not fill is left undefined rather than defaulted, so
 * the rules can tell a measurement of zero from no measurement and the
 * completeness count means something.
 */
export function riskInputFor(
    symbol: ChangeImpactSymbol,
    complexity: ReadonlyMap<string, SymbolComplexity>,
    entryPoints: ReadonlySet<string>,
    tests: ImpactTestLookup,
): SymbolRiskInput {
    const reading = symbol.qualifiedName ? complexity.get(symbol.qualifiedName) : undefined;
    const covering = symbol.qualifiedName ? tests.bySymbol.get(symbol.qualifiedName) : undefined;
    return {
        isEntryPoint: entryPoints.has(symbol.qualifiedName ?? '') || entryPoints.has(symbol.name),
        transitiveLoopDepth: reading?.loopDepth,
        unguardedRecursion: reading?.unguardedRecursion,
        fanIn: reading?.fanIn,
        cyclomatic: reading?.complexity,
        cognitive: reading?.cognitive,
        allocInLoop: reading?.allocationInLoop,
        linearScanInLoop: reading?.scanInLoop,
        testedByCount: covering?.length,
    };
}

/**
 * One phrase per reading that reached a threshold, in the order they matter.
 *
 * The same phrases the reference's workspace map uses for its hotspots,
 * deliberately: the two surfaces describe the same readings of the same symbols,
 * and two vocabularies for one finding would leave a reader wondering whether
 * they were being told two different things.
 */
export function reasonsFor(input: SymbolRiskInput): string[] {
    const reasons: string[] = [];
    if (input.isEntryPoint === true) {
        reasons.push(IMPACT_SIGNAL_ENTRY_POINT);
    }
    if (input.unguardedRecursion === true) {
        reasons.push(MAP_SIGNAL_UNGUARDED_RECURSION);
    }
    if ((input.transitiveLoopDepth ?? 0) > 0) {
        reasons.push(mapSignalNesting(input.transitiveLoopDepth!));
    }
    if (input.allocInLoop === true) {
        reasons.push(MAP_SIGNAL_ALLOCATION_IN_LOOP);
    }
    if (input.linearScanInLoop === true) {
        reasons.push(MAP_SIGNAL_SCAN_IN_LOOP);
    }
    if ((input.cognitive ?? 0) > 0) {
        reasons.push(mapSignalThinking(input.cognitive!));
    }
    if ((input.cyclomatic ?? 0) > 0) {
        reasons.push(mapSignalBranches(input.cyclomatic!));
    }
    if ((input.fanIn ?? 0) > 0) {
        reasons.push(mapSignalFanIn(input.fanIn!));
    }
    return reasons;
}

function groupByDistance<T>(
    symbols: readonly ChangeImpactSymbol[],
    render: (symbol: ChangeImpactSymbol) => T,
): { distance: number; symbols: T[] }[] {
    const bands = new Map<number, T[]>();
    for (const symbol of symbols) {
        const band = bands.get(symbol.distance) ?? [];
        band.push(render(symbol));
        bands.set(symbol.distance, band);
    }
    return [...bands.entries()]
        .sort(([left], [right]) => left - right)
        .map(([distance, rows]) => ({ distance, symbols: rows }));
}

// Endpoints -------------------------------------------------------------------

/**
 * The endpoints a change reaches.
 *
 * Two passes, strongest first. A route whose registration names a handler that
 * is in the change set is reported as a handler match; anything left over whose
 * registration file the change reaches is reported as a file match. A route
 * cannot appear twice, and when both would match, the handler wins, because it
 * is the claim that says which function answers the address.
 */
export function affectedEndpoints(
    change: ChangeImpactDto | undefined,
    architecture: ArchitectureOverviewDto | undefined,
): ImpactEndpoint[] {
    const routes = architecture?.routes ?? [];
    if (routes.length === 0 || change === undefined) {
        return [];
    }
    const symbolNames = new Set<string>();
    const reachedFiles = new Set<string>(change.changedFiles);
    for (const symbol of change.symbols) {
        if (symbol.changeKind === 'module') {
            continue;
        }
        symbolNames.add(symbol.name);
        if (symbol.qualifiedName) {
            symbolNames.add(symbol.qualifiedName);
        }
        if (symbol.filePath) {
            reachedFiles.add(symbol.filePath);
        }
    }

    const seen = new Set<string>();
    const out: ImpactEndpoint[] = [];
    const add = (route: RouteRef, via: 'handler' | 'file'): void => {
        const key = `${route.method ?? ''} ${route.path}`;
        if (seen.has(key)) {
            return;
        }
        seen.add(key);
        out.push({
            routePath: route.path,
            method: route.method,
            via,
            filePath: route.filePath,
            line: route.line,
            origin: route.origin,
        });
    };
    for (const route of routes) {
        if (route.handler && symbolNames.has(route.handler)) {
            add(route, 'handler');
        }
    }
    for (const route of routes) {
        if (route.filePath && reachedFiles.has(route.filePath)) {
            add(route, 'file');
        }
    }
    return out;
}

// Tests -----------------------------------------------------------------------

/**
 * The two test lists.
 *
 * A symbol that was never looked up appears in neither: it is not covered and
 * it is not uncovered, it is unchecked, and the narrative says how many of
 * those there were. Collapsing unchecked into missing would turn a bound on
 * effort into a finding about the code.
 */
export function summariseTests(
    symbols: readonly ChangeImpactSymbol[],
    tests: ImpactTestLookup,
): ImpactTests {
    const byTest = new Map<string, ImpactCoveringTest>();
    const missing: ImpactTarget[] = [];
    let checked = 0;
    for (const symbol of symbols) {
        const key = symbol.qualifiedName;
        if (symbol.changeKind === 'module' || symbol.isTest === true || !key || !tests.checked.has(key)) {
            continue;
        }
        checked += 1;
        const covering = tests.bySymbol.get(key) ?? [];
        if (covering.length === 0) {
            missing.push(targetOf(symbol));
            continue;
        }
        for (const test of covering) {
            const testKey = `${test.name}|${test.file ?? ''}|${test.line ?? ''}`;
            const known = byTest.get(testKey);
            if (known) {
                known.covers.push(symbol.name);
                continue;
            }
            byTest.set(testKey, {
                name: test.name,
                file: test.file,
                line: test.line,
                covers: [symbol.name],
            });
        }
    }
    return {
        covering: [...byTest.values()],
        missing,
        checked,
        ...(tests.cappedAt === undefined ? {} : { cappedAt: tests.cappedAt }),
    };
}

// Narrative -------------------------------------------------------------------

interface NarrativeInputs {
    change: ChangeImpactDto | undefined;
    architecture: ArchitectureOverviewDto | undefined;
    declared: number;
    callers: number;
    walkedDistance: number;
    endpoints: ImpactEndpoint[];
    tests: ImpactTests;
    completeness: RiskInputCompleteness;
    badge: RiskLevel;
}

/**
 * The paragraph, one claim at a time.
 *
 * Every claim gets an evidence row and there are exactly as many rows as
 * claims, which is what lets the surface put an affordance beside each sentence
 * rather than one at the bottom of the paragraph. The two conditional claims at
 * the end are the honest ones: they only appear when something was not looked
 * at, and they say what.
 */
export function buildNarrative(inputs: NarrativeInputs): ImpactNarrative {
    const evidence: ImpactEvidence[] = [];
    const changedFiles = inputs.change?.changedFiles.length ?? 0;
    const reported = inputs.change?.symbols.length ?? 0;

    evidence.push({
        claim: impactChangedSentence(changedFiles, inputs.declared),
        source: 'detect_changes',
        value: `${countOf(changedFiles, 'changed file', 'changed files')}, `
            + `${countOf(reported, 'symbol', 'symbols')} reported`,
    });
    const seeds = inputs.change?.seedSymbols;
    if (seeds !== undefined && seeds !== inputs.declared) {
        evidence.push({
            claim: impactSeedSentence(seeds, inputs.declared),
            source: 'detect_changes',
            value: `${countOf(seeds, 'seed declaration', 'seed declarations')} reported, `
                + `${countOf(inputs.declared, 'symbol', 'symbols')} listed`,
        });
    }
    evidence.push({
        claim: impactDownstreamSentence(inputs.callers, inputs.walkedDistance),
        source: 'detect_changes',
        value: `${countOf(inputs.callers, 'caller', 'callers')} over `
            + `${countOf(inputs.walkedDistance, 'walked step', 'walked steps')}`,
    });
    evidence.push({
        claim: impactEndpointSentence(inputs.endpoints.map((endpoint) => endpointLabel(endpoint))),
        source: 'architecture',
        value: `${countOf(inputs.architecture?.routes.length ?? 0, 'route', 'routes')} recovered, `
            + `${inputs.endpoints.length} reached`,
    });
    evidence.push({
        claim: impactTestsSentence(inputs.tests.covering.length, inputs.tests.missing.length),
        source: 'facts',
        value: `${countOf(inputs.tests.checked, 'symbol', 'symbols')} checked for test callers`,
    });
    if (inputs.completeness.unmeasured > 0) {
        evidence.push({
            claim: impactUnmeasuredSentence(inputs.completeness.unmeasured),
            source: 'facts',
            value: `${inputs.completeness.measured} of ${inputs.completeness.total} symbols carry a complexity reading`,
        });
    }
    if (inputs.tests.cappedAt !== undefined) {
        evidence.push({
            claim: impactTestLookupCapped(inputs.tests.cappedAt),
            source: 'facts',
            value: `lookup capped at ${inputs.tests.cappedAt} symbols`,
        });
    }
    if (inputs.change?.truncated === true) {
        evidence.push({
            claim: IMPACT_WALK_TRUNCATED,
            source: 'detect_changes',
            value: `walk stopped after ${inputs.walkedDistance} steps`,
        });
    }
    return {
        text: evidence.map((entry) => entry.claim).join(' '),
        badge: inputs.badge,
        evidence,
    };
}

/** `GET /orders/:id`, or just the path when the registration named no method. */
export function endpointLabel(endpoint: Pick<ImpactEndpoint, 'method' | 'routePath'>): string {
    return endpoint.method ? `${endpoint.method} ${endpoint.routePath}` : endpoint.routePath;
}

// Zwei Bausteine, die es im Referenzprojekt nicht gibt ------------------------

/**
 * Warum das Wort da steht, Regel fuer Regel.
 *
 * Der Referenz-Widget zeigt die Begruendung je Symbol an einem Tooltip und die
 * Gesamtregeln gar nicht: dort steht die Erzaehlung daneben und die
 * Evidenz-Zeilen darunter, und wer wissen will, warum HIGH, liest sie. Diese
 * Oberflaeche hat weniger Platz und einen Beweislauf, der die Begruendung als
 * Text lesen koennen muss, also wird sie hier ausgeschrieben.
 *
 * Aufgezaehlt wird genau das, was die Regeln erfuellt hat: die Symbole, deren
 * eigenes Urteil so schwer wiegt wie das Gesamturteil, und die beiden Regeln,
 * die kein Symbol allein ausloesen kann. Ein Symbol, das leiser ist als das
 * Gesamturteil, steht nicht dabei: es hat das Wort nicht verursacht, und es
 * mitzuzaehlen waere eine laengere Liste ohne einen weiteren Grund.
 */
export function badgeRules(model: ImpactModel): string[] {
    const rules: string[] = [];
    const rows = [
        ...model.direct.map((row) => ({ name: row.name, risk: row.risk, reasons: row.reasons })),
        ...model.downstream.flatMap((group) =>
            group.symbols.map((row) => ({ name: row.name, risk: row.risk, reasons: row.reasons }))),
    ];
    for (const row of rows) {
        if (row.risk === model.risk && row.reasons.length > 0) {
            rules.push(impactSymbolRule(row.name, row.risk, row.reasons));
        }
    }
    if (model.summaryTiles.endpoints >= RISK_THRESHOLDS.endpointsMedium) {
        rules.push(impactEndpointRule(model.summaryTiles.endpoints));
    }
    if (model.summaryTiles.untestedAffected >= RISK_THRESHOLDS.untestedMedium) {
        rules.push(impactUntestedRule(model.summaryTiles.untestedAffected));
    }
    return rules;
}

/**
 * Warum ein Ref hier abgelehnt wird und nicht dort.
 *
 * Das Referenzprojekt laesst seinen Backend-Prozess ablehnen, weil der ein
 * Repository vor sich hat und `git rev-parse` fragen kann. Dieses Backend kann
 * das nicht: `detect_changes` nimmt ein unbekanntes `since` kommentarlos an und
 * antwortet ueber den Arbeitsbaum, so als waere keines gekommen (dieselbe Falle
 * wie die Schreibweise `since_ref`, siehe rpc-client.ts). Ein kaputter Ref
 * ergaebe also eine plausible Antwort auf eine andere Frage, und das ist die
 * eine Sorte Fehler, gegen die diese Flaeche gebaut ist.
 *
 * Geprueft wird die Form und nur die Form, nach den Regeln von
 * `git check-ref-format`. Was hier durchkommt, kann trotzdem ein Ref sein, den
 * es nicht gibt; das ist eine Frage an das Repository und nicht an eine Regex,
 * und die Antwort darauf holt der Aufruf.
 */
export function refRejection(ref: string): string | undefined {
    const value = ref.trim();
    if (value.length === 0) {
        return 'it is empty';
    }
    // eslint-disable-next-line no-control-regex
    if (/[\u0000-\u0020\u007f~^:?*[\\]/.test(value)) {
        return 'a git ref holds no space, no control character and none of ~ ^ : ? * [ \\';
    }
    if (value.startsWith('-')) {
        return 'a git ref does not start with a dash';
    }
    if (value.startsWith('/') || value.endsWith('/') || value.includes('//')) {
        return 'a git ref has no empty path component';
    }
    if (value.includes('..')) {
        return 'a git ref holds no two consecutive dots';
    }
    if (value.includes('@{')) {
        return 'a git ref holds no @{ sequence';
    }
    if (value.endsWith('.') || value.endsWith('.lock')) {
        return 'a git ref ends in neither a dot nor .lock';
    }
    if (value === '@') {
        return 'a git ref is not the single character @';
    }
    return undefined;
}

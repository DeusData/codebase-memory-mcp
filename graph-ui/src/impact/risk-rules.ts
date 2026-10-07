/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/src/browser/impact/risk-rules.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert: dieselbe
 * Schwellentabelle, dieselben Zweige, dieselbe Behandlung fehlender Messungen,
 * dieselben Namen. Die Datei ist rein und kennt kein React, keinen Server und
 * kein Vokabular; die Saetze stehen in impact-strings.ts und der Zusammenbau in
 * impact-model.ts, genau wie im Referenzprojekt.
 *
 * Aenderungen gegenueber dem Original: keine.
 */
/**
 * How risky a change is, decided by rules a reader can argue with.
 *
 * This file is the whole of that judgement, and it is deliberately the dullest
 * file in the product: a fixed table of thresholds, no scoring, no weights, no
 * tuning constant that only makes sense to whoever wrote it. Three reasons it
 * is shaped that way.
 *
 * **A risk level is a claim, so it has to be checkable.** CodeAtlas tells a
 * reader that a change is high risk. The only defensible version of that
 * sentence is one where the reader can ask "why" and get "because a route
 * handler is in the change set", not "because it scored 7.4". Every branch
 * below corresponds to a sentence somebody can disagree with.
 *
 * **A missing reading is not a zero.** The analysis records complexity for the
 * languages it measures and nothing for the rest, and a symbol nobody measured
 * is not a simple symbol. The rules treat an absent number as absent, which for
 * the arithmetic means it cannot raise the level, and {@link completenessOf}
 * counts how many symbols were in that position so the surface can say so out
 * loud. Silently reading a missing cyclomatic complexity as 0 and then printing
 * "low risk" would be the exact failure this product exists to avoid.
 *
 * **Pure, and no vocabulary.** No rendering library, no service, no sentence.
 * The wording lives in impact-strings.ts and the assembly in impact-model.ts, so
 * the thresholds can be read, tested and changed without touching anything that
 * renders.
 */

/** The three levels, in order. Never collapsed to a number. */
export type RiskLevel = 'low' | 'medium' | 'high';

/**
 * What is known about one symbol when its risk is decided.
 *
 * Every field is optional and that is the point: an absent field means nobody
 * measured it, which is different from a measurement of zero and is recorded as
 * such by {@link completenessOf}.
 */
export interface SymbolRiskInput {
    /** The index flagged this symbol as reachable from outside the program. */
    isEntryPoint?: boolean;
    /** Deepest loop nesting the analysis measured. */
    transitiveLoopDepth?: number;
    /** The analysis found recursion with no reachable base case. */
    unguardedRecursion?: boolean;
    /** How many other symbols reach this one. */
    fanIn?: number;
    /** Cyclomatic complexity: how many branches there are to follow. */
    cyclomatic?: number;
    /** Cognitive complexity: how hard the branching is to hold in your head. */
    cognitive?: number;
    /** Something is allocated inside a loop. */
    allocInLoop?: boolean;
    /** A linear scan runs inside a loop. */
    linearScanInLoop?: boolean;
    /**
     * How many test callers the heuristic found.
     *
     * Carried here because it belongs to the symbol, and deliberately not used
     * by {@link perSymbolRisk}: a well tested function that everything calls is
     * still the function everything calls. Coverage changes what a change to
     * the *set* costs, so it is counted once, at the overall level, where it
     * decides how much of the change would go unnoticed.
     */
    testedByCount?: number;
}

/**
 * Every threshold, in one object.
 *
 * Exported so a reviewer can read the numbers without reading the branches, and
 * so a test can assert on the boundary rather than on a literal it copied.
 */
export const RISK_THRESHOLDS = {
    /** Loop nesting at or past this is high on its own. */
    loopDepthHigh: 3,
    /** Fan-in at or past this is high on its own: too many callers to check by hand. */
    fanInHigh: 10,
    /** Fan-in from here up to `fanInMediumMax` is medium. */
    fanInMediumMin: 4,
    fanInMediumMax: 9,
    /** Cyclomatic complexity at or past this is medium. */
    cyclomaticMedium: 10,
    /** Cognitive complexity at or past this is medium. */
    cognitiveMedium: 15,
    /** Affected endpoints at or past this make the whole change high. */
    endpointsHigh: 3,
    /** Affected endpoints at or past this make the whole change medium. */
    endpointsMedium: 1,
    /** Untested affected symbols at or past this make the whole change high. */
    untestedHigh: 5,
    /** Untested affected symbols at or past this make the whole change medium. */
    untestedMedium: 1,
} as const;

/** A number that was actually measured, or undefined. Never coerced to zero here. */
function measured(value: number | undefined): number | undefined {
    return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

/** True when `value` was measured and reaches `threshold`. Absent never reaches anything. */
function atLeast(value: number | undefined, threshold: number): boolean {
    const number = measured(value);
    return number !== undefined && number >= threshold;
}

/** True when `value` was measured and sits inside the closed range. */
function within(value: number | undefined, low: number, high: number): boolean {
    const number = measured(value);
    return number !== undefined && number >= low && number <= high;
}

/**
 * The risk of changing one symbol.
 *
 * The high branch is four different sentences and they are all about blast
 * radius rather than about difficulty. An entry point is reached from outside
 * the program, so a mistake here is a mistake a user meets. Deep nesting means
 * the cost of this code follows the size of its input, so a change to it is a
 * change to how the system behaves under load. Recursion with no visible base
 * case is the one signal here that describes a defect rather than an expense.
 * Ten callers is more than anybody checks by hand.
 *
 * The medium branch is about difficulty: a lot of branches, a lot of thinking,
 * a handful of callers, or work done per item inside a loop. None of those says
 * a change will break something; all of them say it deserves a second look.
 */
export function perSymbolRisk(input: SymbolRiskInput): RiskLevel {
    if (
        input.isEntryPoint === true
        || atLeast(input.transitiveLoopDepth, RISK_THRESHOLDS.loopDepthHigh)
        || input.unguardedRecursion === true
        || atLeast(input.fanIn, RISK_THRESHOLDS.fanInHigh)
    ) {
        return 'high';
    }
    if (
        atLeast(input.cyclomatic, RISK_THRESHOLDS.cyclomaticMedium)
        || atLeast(input.cognitive, RISK_THRESHOLDS.cognitiveMedium)
        || within(input.fanIn, RISK_THRESHOLDS.fanInMediumMin, RISK_THRESHOLDS.fanInMediumMax)
        || input.allocInLoop === true
        || input.linearScanInLoop === true
    ) {
        return 'medium';
    }
    return 'low';
}

/**
 * The risk of the whole change.
 *
 * Two things raise it that no single symbol can. An affected endpoint is an
 * address someone outside the process can reach, so the change is visible past
 * the program's own boundary however tidy the code behind it is. An affected
 * symbol nothing tests is a change that no automatic check would notice, and
 * five of those together is a change nobody is watching.
 *
 * `untestedAffected` is a count of symbols, not a judgement about coverage: the
 * test relation is inferred from callers the index flagged as test code, so
 * "untested" here means "no test caller was found", which is what the narrative
 * says as well.
 */
export function overallRisk(
    perSymbol: readonly RiskLevel[],
    affectedEndpoints: number,
    untestedAffected: number,
): RiskLevel {
    if (
        perSymbol.includes('high')
        || affectedEndpoints >= RISK_THRESHOLDS.endpointsHigh
        || untestedAffected >= RISK_THRESHOLDS.untestedHigh
    ) {
        return 'high';
    }
    if (
        perSymbol.includes('medium')
        || affectedEndpoints >= RISK_THRESHOLDS.endpointsMedium
        || untestedAffected >= RISK_THRESHOLDS.untestedMedium
    ) {
        return 'medium';
    }
    return 'low';
}

/**
 * How much of the input the rules actually had.
 *
 * The rules degrade quietly by design: an absent number cannot raise a level,
 * so a project whose language the analysis does not measure produces a page of
 * low-risk rows. That is only honest if the page also says the measurements are
 * missing, and this is the count that lets it.
 */
export interface RiskInputCompleteness {
    /** Symbols the analysis measured at least one complexity signal for. */
    measured: number;
    /** Symbols it measured none for, whose level therefore rests on absence. */
    unmeasured: number;
    total: number;
}

/**
 * True when at least one complexity signal was recorded for this symbol.
 *
 * Fan-in is excluded on purpose. It is counted from the call graph, which every
 * language the analysis reads produces, so a symbol with a fan-in and nothing
 * else is exactly the case this predicate has to answer no to.
 */
export function hasComplexityReadings(input: SymbolRiskInput): boolean {
    return measured(input.cyclomatic) !== undefined
        || measured(input.cognitive) !== undefined
        || measured(input.transitiveLoopDepth) !== undefined
        || input.allocInLoop !== undefined
        || input.linearScanInLoop !== undefined
        || input.unguardedRecursion !== undefined;
}

export function completenessOf(inputs: readonly SymbolRiskInput[]): RiskInputCompleteness {
    let measuredCount = 0;
    for (const input of inputs) {
        if (hasComplexityReadings(input)) {
            measuredCount += 1;
        }
    }
    return {
        measured: measuredCount,
        unmeasured: inputs.length - measuredCount,
        total: inputs.length,
    };
}

/** Order used when a list of levels has to be sorted worst first. */
export const RISK_ORDER: Readonly<Record<RiskLevel, number>> = { high: 0, medium: 1, low: 2 };

/** The worse of two levels. */
export function worseOf(left: RiskLevel, right: RiskLevel): RiskLevel {
    return RISK_ORDER[left] <= RISK_ORDER[right] ? left : right;
}

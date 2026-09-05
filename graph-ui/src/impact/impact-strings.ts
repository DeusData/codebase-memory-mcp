/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/src/browser/strings.ts
 * (die Abschnitte MAP_SIGNAL_*, RISK_LABELS, IMPACT_*). Gleicher Urheber
 * (Bernhard Jackiewicz). Die Saetze sind woertlich uebernommen, weil sie die
 * Aussage sind: eine umformulierte Begruendung waere eine andere Begruendung.
 *
 * Warum eine eigene Datei statt eines Anbaus an src/twin/strings.ts: die dortige
 * Datei ist der Wortschatz des Twins und wird von jedem Twin-Test gelesen. Der
 * Wortschatz der Aenderungsansicht gehoert zur Aenderungsansicht, und zwei
 * Flaechen, die sich eine Zeichenkettendatei teilen, aendern sich gegenseitig,
 * sobald eine von beiden umformuliert wird.
 *
 * Weggelassen: alles, was eine Flaeche betrifft, die es hier nicht gibt (der
 * Review-Modus, das Staleness-Banner, die Diagramm-Legende). Aenderungen an dem,
 * was uebernommen wurde: keine.
 */

/** `3 files` oder `1 file`. Die eine Pluralregel dieser Datei. */
export function countOf(count: number, singular: string, plural: string): string {
    return `${count} ${count === 1 ? singular : plural}`;
}

// Signale, wortgleich mit der Karte des Referenzprojekts ----------------------

export const MAP_SIGNAL_UNGUARDED_RECURSION = 'recursion with no visible base case';
export const MAP_SIGNAL_ALLOCATION_IN_LOOP = 'allocates inside a loop';
export const MAP_SIGNAL_SCAN_IN_LOOP = 'scans a list inside a loop';

export function mapSignalNesting(depth: number): string {
    return depth === 1 ? 'one loop' : `loops nested ${depth} deep`;
}

export function mapSignalThinking(score: number): string {
    return `cognitive complexity ${score}`;
}

export function mapSignalBranches(score: number): string {
    return `${countOf(score, 'branch', 'branches')} to follow`;
}

export function mapSignalFanIn(count: number): string {
    return `reached by ${countOf(count, 'symbol', 'symbols')}`;
}

// Das Risikowort und was daneben steht ---------------------------------------

/** The risk word, and the sentence that says what kind of claim it is. */
export const RISK_LABELS: Readonly<Record<'low' | 'medium' | 'high', string>> = {
    low: 'LOW',
    medium: 'MEDIUM',
    high: 'HIGH',
};

export const IMPACT_BADGE_TOOLTIP =
    'A rule applied to what the index recorded, not a prediction. The sentences beside it are the whole of the reasoning.';

/** What a risk chip says when the symbol is reached from outside the program. */
export const IMPACT_SIGNAL_ENTRY_POINT = 'reachable from outside the program';

/** What a risk chip says when nothing about the symbol reached a threshold. */
export const IMPACT_SIGNAL_NONE = 'nothing measured on this symbol reached a threshold';

export function impactRiskTooltip(level: 'low' | 'medium' | 'high', reasons: string[]): string {
    const because = reasons.length === 0 ? IMPACT_SIGNAL_NONE : reasons.join('; ');
    return `${RISK_LABELS[level]}: ${because}.`;
}

/** How a route was tied to the change. Two different strengths of claim. */
export const IMPACT_ENDPOINT_VIA_HANDLER = 'The registration names a handler this change reaches.';
export const IMPACT_ENDPOINT_VIA_FILE =
    'The registration is written in a file this change reaches. CodeAtlas cannot name the handler here, so this is the file and not the function.';

// Die Erzaehlung, ein Satz je Lesung -----------------------------------------

export function impactChangedSentence(files: number, symbols: number): string {
    if (files === 0) {
        return 'Nothing in the working tree differs from the baseline.';
    }
    const where = files === 1 ? 'it' : 'them';
    if (symbols === 0) {
        return `${countOf(files, 'file differs', 'files differ')} from the baseline, `
            + `and the index holds no symbol in ${where}.`;
    }
    return `${countOf(files, 'file differs', 'files differ')} from the baseline, `
        + `and the index places ${countOf(symbols, 'symbol', 'symbols')} inside ${where}.`;
}

/**
 * Wie viele Deklarationen die Analyse wirklich angefasst hat.
 *
 * Steht nur da, wenn die Zahl von der Laenge der Liste abweicht, und dann ist
 * sie die wichtigste Zahl der Seite: die Liste zeigt, was der Index in den
 * geaenderten Dateien fuehrt, und der Server sagt, auf wie viele davon er den
 * Diff eingeschraenkt hat. Ohne diesen Satz laese sich die Liste als "so viel
 * hat sich geaendert", und das steht dort nicht.
 */
export function impactSeedSentence(seeds: number, listed: number): string {
    return `The analysis scoped the diff to ${countOf(seeds, 'declaration', 'declarations')}, `
        + `and the list below is the ${countOf(listed, 'symbol', 'symbols')} the index places inside those files.`;
}

export function impactDownstreamSentence(count: number, distance: number): string {
    if (count === 0) {
        return 'Nothing in the indexed workspace calls those symbols, so the change stops where it is.';
    }
    return `Walking the calls inwards reaches ${countOf(count, 'further symbol', 'further symbols')} `
        + `within ${countOf(distance, 'step', 'steps')}.`;
}

export function impactEndpointSentence(paths: string[]): string {
    if (paths.length === 0) {
        return 'No HTTP endpoint CodeAtlas recovered is registered in a file this change reaches.';
    }
    return `${countOf(paths.length, 'endpoint is', 'endpoints are')} registered in a file `
        + `this change reaches: ${paths.join(', ')}.`;
}

export function impactTestsSentence(covering: number, missing: number): string {
    if (covering === 0 && missing === 0) {
        return 'No affected symbol could be checked for test callers.';
    }
    if (missing === 0) {
        return 'Every affected symbol that was checked has a test caller '
            + `(${countOf(covering, 'test caller', 'test callers')} in total), so a change here would be noticed.`;
    }
    return `No test caller was found for ${countOf(missing, 'affected symbol', 'affected symbols')}, `
        + `so a change to ${missing === 1 ? 'it' : 'them'} would go unnoticed by any automatic check.`;
}

/**
 * Said whenever a level was decided partly on absence.
 *
 * The rules cannot raise a level on a reading nobody took, so a project the
 * analysis does not measure produces a page of quiet rows. This is the sentence
 * that keeps a quiet page from reading like a clean one.
 */
export function impactUnmeasuredSentence(count: number): string {
    return `Complexity readings were unavailable for ${countOf(count, 'symbol', 'symbols')}, `
        + 'so the level rests on what the analysis could not measure as well as on what it could.';
}

export function impactTestLookupCapped(cap: number): string {
    return `Test callers were looked up for the first ${cap} affected symbols only; the rest were not checked.`;
}

export const IMPACT_WALK_TRUNCATED =
    'The caller walk stopped at its bound, so the list of what calls this change is a floor and not a total.';

/** Evidence rows: what each claim rests on, in the reader's words. */
export const IMPACT_EVIDENCE_TITLE = 'What this rests on';

export const IMPACT_EVIDENCE_SOURCES: Readonly<Record<'detect_changes' | 'architecture' | 'facts', string>> = {
    detect_changes: 'the change set the analysis reported, plus the calls into it',
    architecture: 'the whole-project summary, including the routes CodeAtlas recovered',
    facts: 'the per-symbol readings: complexity, and callers flagged as test code',
};

export function impactEvidenceValue(
    source: 'detect_changes' | 'architecture' | 'facts',
    value: string,
): string {
    return `${IMPACT_EVIDENCE_SOURCES[source]} (${value})`;
}

// Was diese Oberflaeche zusaetzlich sagen muss -------------------------------
//
// Alles ab hier hat im Referenzprojekt keine Entsprechung, weil es dort keinen
// Browser gibt, der einen Ref selbst pruefen muesste, und keine Badge-Zeile, die
// ihre Regeln aufzaehlt. Beides steht hier, weil W4b beides verlangt.

/** Die Ueberschrift der Flaeche. */
export const IMPACT_TITLE = 'What would this change reach?';

/**
 * Wie das [a]tlas-Menue den Weg hierher nennt, mit seinem Buchstaben
 * (Audit-Befund 12 vom 2026-08-29).
 *
 * "[c]hange scope" und nicht "[s]cope a change": s waere der Buchstabe der
 * Suche, und c ist der von change, also von dem Wort, das der Leser im Kopf
 * hat, wenn er wissen will, was seine Aenderung erreicht.
 */
export const IMPACT_MENU_LABEL = '[c]hange scope';

/** Der Satz darunter: was gelesen wird und was das Wort am Ende bedeutet. */
export const IMPACT_SUBLINE =
    'Read from the change set the analysis reports, the calls into it, and the routes CodeAtlas recovered. '
    + 'The word on the right is a rule applied to those readings, never a prediction.';

export const IMPACT_MODE_LABEL = 'Compare against';
export const IMPACT_MODE_WORKING_TREE = 'Working tree';
export const IMPACT_MODE_WORKING_TREE_TOOLTIP =
    'Compare the working tree against the last commit, which is what the analysis does when nobody names a point.';
export const IMPACT_MODE_SINCE_REF = 'Since ref';
export const IMPACT_MODE_SINCE_REF_TOOLTIP = 'Compare the working tree against a named point in history.';

export const IMPACT_REF_LABEL = 'Baseline';
export const IMPACT_REF_PLACEHOLDER = 'branch, tag or commit';
export const IMPACT_REF_GO = 'Go';
export const IMPACT_REF_GO_TOOLTIP = 'Compare the working tree against this point in history.';

/**
 * Was zu einem abgelehnten Ref gesagt wird.
 *
 * Der Satz nennt die Regel, gegen die verstossen wurde, und sagt ausdruecklich,
 * dass niemand gefragt wurde. Ohne den zweiten Teil koennte ein Leser die
 * Ablehnung fuer eine Antwort der Engine halten, und dann waere "dieser Ref
 * existiert nicht" und "dieser Ref ist kein Ref" dasselbe Wort fuer zwei sehr
 * verschiedene Lagen.
 */
export function impactRefRejected(ref: string, rule: string): string {
    return `"${ref}" is not a usable git ref: ${rule}. Nothing was asked of the analysis backend.`;
}

export const IMPACT_LOADING = 'Reading the change set, the summary and the per-symbol readings.';
export const IMPACT_LOAD_FAILED = 'The change set could not be read.';

export const IMPACT_EMPTY_MESSAGE = 'No project, so there is no change set to read.';

/**
 * Die Ueberschriften der vier Listen.
 *
 * Die erste heisst NICHT "changed symbols". Sie zaehlt, was der Index in den
 * geaenderten Dateien fuehrt, und wie viele davon die Analyse als geaendert
 * behandelt hat, sagt der Satz mit der Seed-Zahl darueber. Eine Ueberschrift,
 * die "geaendert" behauptet, waere die eine Zeile dieser Flaeche, die mehr sagt
 * als ihre Lesung hergibt.
 */
export const IMPACT_DIRECT_TITLE = 'Symbols in the changed files';
export const IMPACT_DOWNSTREAM_TITLE = 'What calls them';
export const IMPACT_ENDPOINTS_TITLE = 'Endpoints reached';
export const IMPACT_TESTS_TITLE = 'Tests';

export const IMPACT_DIRECT_EMPTY =
    'The index places no symbol inside the changed files.';
export const IMPACT_DOWNSTREAM_EMPTY =
    'Nothing in the indexed workspace calls the changed symbols.';
export const IMPACT_ENDPOINTS_EMPTY =
    'No route CodeAtlas recovered is registered in a file this change reaches.';
export const IMPACT_TESTS_EMPTY = 'No affected symbol could be checked for test callers.';

/**
 * Wie viele Dateien der Routen-Scan geoeffnet hat, und was das begrenzt.
 *
 * `cap` ist die Decke NUR dann, wenn eine gegriffen hat; sonst undefined. Die
 * Unterscheidung ist der ganze Inhalt des Satzes: "die ersten 10" und "alle 10"
 * sind zwei verschiedene Aussagen ueber dieselbe Zahl.
 */
export function impactRouteScanNote(scanned: number, cap: number | undefined, truncatedFiles: number): string {
    const head = cap === undefined
        ? `Routes were read off the text of ${countOf(scanned, 'indexed source file', 'indexed source files')}.`
        : `Routes were read from the first ${cap} indexed source files only.`;
    return truncatedFiles === 0
        ? head
        : `${head} ${countOf(truncatedFiles, 'file', 'files')} arrived incomplete, so a registration past the cut is not on this list.`;
}

/** Die Zeile ueber der Badge-Begruendung. */
export const IMPACT_BADGE_WHY_TITLE = 'Which rules fired';

/** Was dort steht, wenn keine Regel griff. */
export const IMPACT_BADGE_WHY_NONE = 'No rule reached a threshold, so the level is the floor and not a finding.';

/** Eine Regel des Gesamturteils als Satz. */
export function impactEndpointRule(count: number): string {
    return `${countOf(count, 'endpoint is', 'endpoints are')} registered in a file this change reaches.`;
}

export function impactUntestedRule(count: number): string {
    return `No test caller was found for ${countOf(count, 'affected symbol', 'affected symbols')}.`;
}

/** Eine Regel eines einzelnen Symbols als Satz. */
export function impactSymbolRule(name: string, level: 'low' | 'medium' | 'high', reasons: string[]): string {
    return `${name} is ${RISK_LABELS[level]}: ${reasons.join('; ')}.`;
}

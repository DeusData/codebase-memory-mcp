/**
 * Die Saetze, mit denen der Explorer und der Reader ueber Coverage sprechen.
 *
 * An einer Stelle, weil dieselbe Aussage an drei Orten auftaucht: als Tooltip
 * an der Baumzeile, als Legende im Explorer-Fuss und als Erklaerung ueber dem
 * Editor. Drei Formulierungen fuer denselben Befund waeren drei Gelegenheiten,
 * eine davon zu schaerfen, bis sie mehr behauptet als der Server gesagt hat.
 *
 * Zwei Regeln gelten fuer jeden Satz hier:
 *
 * 1. **Kein Grund wird erfunden.** Nennt der Server keinen, endet der Satz
 *    ohne Grund, statt einen plausiblen anzuhaengen.
 * 2. **Abwesenheit wird als Abwesenheit benannt.** "nicht indiziert" heisst
 *    nicht "leer" und nicht "kaputt", sondern dass diese Oberflaeche ueber den
 *    Inhalt nichts weiss.
 */

import type { CoverageState } from './tree-model';

/**
 * Der ehrliche Satz ueber die Grenze des Baums.
 *
 * Er steht im Explorer-Fuss, weil genau dort die Frage entsteht: der Baum sieht
 * aus wie ein Dateisystem, ist aber die Vereinigung dessen, was die Discovery
 * des Indexers gesehen hat. Eine Datei, die sie nie erreicht hat, steht in
 * keiner der drei Quellen und kann darum auch hier nicht stehen.
 */
export const COVERAGE_SOURCE_NOTE =
    'files as the index discovery saw them; files it never met are invisible until the server lists directories';

/** Das kurze Wort einer Stufe, so wie es in der Legende steht. */
export const COVERAGE_LABELS: Record<CoverageState, string> = {
    indexed: 'indexed',
    partial: 'partially parsed',
    skipped: 'skipped',
    'not-indexed': 'not indexed',
    ignored: 'ignored',
};

/**
 * Das Zeichen, das eine Datei-Zeile traegt.
 *
 * Ordner tragen stattdessen einen Punkt, denn ihr Marker ist eine Aussage ueber
 * ihren Inhalt und nicht ueber sie selbst.
 */
export const COVERAGE_MARKS: Record<CoverageState, string> = {
    indexed: '',
    partial: '!',
    skipped: 'x',
    'not-indexed': '-',
    ignored: '-',
};

/** Der Punkt, den ein Ordner traegt, wenn unter ihm etwas fehlt. */
export const COVERAGE_FOLDER_MARK = '●';

/** Was die Legende zu einer Stufe sagt. */
export const COVERAGE_DESCRIPTIONS: Record<CoverageState, string> = {
    indexed: 'in the graph, no recorded issue',
    partial: 'indexed, but constructs in the listed line ranges may be missing',
    skipped: 'the indexer gave up on it, with its own reason',
    'not-indexed': 'excluded by design (gitignore/.cbmignore), with the rule as the reason',
    ignored: 'the coverage store lists it as ignored',
};

/**
 * Der Tooltip einer Zeile.
 *
 * Der Grund wird angehaengt, wenn der Server einen genannt hat, und sonst
 * nicht. Der Wortlaut fuer `partial` nennt Zeilenbereiche statt einer Zahl von
 * Symbolen: der Server meldet Bereiche und keine Zahl, und "n symbols may be
 * missing" waere eine Zahl, die niemand gezaehlt hat.
 */
export function coverageTooltip(state: CoverageState, reason = ''): string {
    const trimmed = reason.trim();
    if (state === 'indexed') {
        return 'indexed: the graph carries this file and no source recorded an issue';
    }
    if (state === 'partial') {
        return trimmed.length > 0
            ? `partially parsed: constructs inside lines ${trimmed} may be missing from the graph`
            : 'partially parsed: constructs inside the flagged ranges may be missing from the graph';
    }
    if (state === 'skipped') {
        return trimmed.length > 0
            ? `skipped by the indexer: ${trimmed}. Nothing of it is in the graph.`
            : 'skipped by the indexer, which named no reason. Nothing of it is in the graph.';
    }
    if (state === 'ignored') {
        return trimmed.length > 0
            ? `ignored by the coverage store: ${trimmed}`
            : 'ignored by the coverage store, which named no reason';
    }
    return trimmed.length > 0
        ? `not indexed by design: ${trimmed}. Change the ignore rules and re-index to include it.`
        : 'not indexed by design, and the server named no rule. Nothing of it is in the graph.';
}

/** Der Tooltip eines Ordners: was seine schlechteste Stufe bedeutet. */
export function folderTooltip(worst: CoverageState): string {
    if (worst === 'indexed') {
        return 'every file below it is in the graph without a recorded issue';
    }
    return `worst stage below this folder: ${COVERAGE_LABELS[worst]} (${COVERAGE_DESCRIPTIONS[worst]})`;
}

/**
 * Was der Reader statt eines Fehlers sagt, wenn eine Datei keinen Inhalt hat.
 *
 * Kein "failed to load": nichts ist schiefgegangen. Der Server liefert Inhalt
 * ausschliesslich ueber `get_code_snippet` auf einem indizierten Symbol
 * (src/reader/file-source.ts), also hat eine Datei ohne Modul-Knoten hier
 * keinen Inhalt, bis Ask 1 erfuellt ist. Das ist eine Aussage ueber die
 * heutige Server-Flaeche und nicht ueber die Datei.
 */
export function readerUnavailableNote(path: string, state: CoverageState, reason = ''): string {
    const trimmed = reason.trim();
    const why =
        state === 'skipped'
            ? trimmed.length > 0
                ? `the indexer skipped it (${trimmed})`
                : 'the indexer skipped it and named no reason'
            : state === 'ignored'
                ? trimmed.length > 0
                    ? `the coverage store lists it as ignored (${trimmed})`
                    : 'the coverage store lists it as ignored'
                : trimmed.length > 0
                    ? `it is excluded by design (${trimmed})`
                    : 'it is excluded by design and the server named no rule';
    return (
        `${path} has no content here: ${why}, so the index holds no module node for it. `
        + 'This server delivers file content only through get_code_snippet on an indexed symbol, '
        + 'so there is nothing to read until a file endpoint exists (Upstream-Ask 1). '
        + 'The file itself is on disk; this surface simply cannot show it.'
    );
}

/** Die Notiz ueber dem Editor, wenn nur Teile der Datei im Graphen stehen. */
export function partialFileNote(path: string, reason = ''): string {
    const trimmed = reason.trim();
    return trimmed.length > 0
        ? `${path} is only partially parsed: constructs inside lines ${trimmed} may be missing from the graph. `
          + 'The text below is the file; the graph around it is incomplete.'
        : `${path} is only partially parsed: constructs inside the flagged ranges may be missing from the graph. `
          + 'The text below is the file; the graph around it is incomplete.';
}

/**
 * Die Frische-Notiz der Statusleiste.
 *
 * Sie steht nur da, wenn sie etwas zu sagen hat: `metadata_match` zusammen mit
 * `no_recorded_issue` heisst, dass die Datei seit dem Index unveraendert ist
 * und keine Quelle etwas ueber sie gemeldet hat. Diesen Fall auch anzuzeigen
 * hiesse, eine gruene Ampel zu bauen, die immer leuchtet.
 */
export function freshnessNoteNeeded(status: string, freshness: string): boolean {
    return !(freshness === 'metadata_match' && status === 'no_recorded_issue');
}

/** Was in der Statusleiste steht, wenn eine Frische-Notiz noetig ist. */
export function freshnessNote(status: string, freshness: string, action: string): string {
    const parts = [freshness.length > 0 ? freshness : 'unknown'];
    if (status.length > 0) {
        parts.push(status);
    }
    if (action.length > 0) {
        parts.push(action);
    }
    return parts.join(' / ');
}

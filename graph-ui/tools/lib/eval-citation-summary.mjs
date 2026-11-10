/**
 * Fasst die Zitattreue einer Modellmessung zusammen.
 *
 * Eine Antwort ohne gemessene Zitatpruefung ist weder ein Treffer noch ein
 * Fehler. Sie bleibt als eigene Zahl sichtbar, damit eine Quote nicht durch
 * unmessbare Antworten freundlicher oder strenger wird als die Messung.
 */
export function citationComplianceOf(results) {
    const measured = results.filter((entry) => entry?.check?.measured === true);
    return {
        citationCompliance: measured.length === 0
            ? 0
            : measured.filter((entry) => entry.check.ok === true).length / measured.length,
        citationMeasured: measured.length,
        citationUnmeasured: results.length - measured.length,
    };
}

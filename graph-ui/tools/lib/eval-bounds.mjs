/**
 * Die harten Grenzen der Modellwahl, an einer Stelle und erzwingbar.
 *
 * Befund 6 des unabhaengigen Audits vom 2026-08-29: `docs/adr/0001-modellwahl.md`
 * spricht von "harten Grenzen 0.6/0.9", und die standen bis dahin ausschliesslich
 * im eingefrorenen Abnahmetest (tests/scaffold/w5b.test.mjs). Ein Abnahmetest
 * liest eine aufgezeichnete Datei; er kann einen Lauf nicht anhalten. `npm run
 * eval:llm` rankte darum sechs Modelle, kroente einen Sieger je Klasse und endete
 * mit 0, auch wenn dieser Sieger die Grenzen gerissen haette. Die Grenzen waren
 * damit eine Behauptung im ADR und kein Gate.
 *
 * Diese Datei ist die eine Quelle. Sie wird von drei Stellen gelesen:
 *
 *  - `tools/eval-llm.mjs` schreibt sie als `bounds` in verification/w5/eval.json
 *    und bricht ab, wenn ein Klassensieger sie reisst,
 *  - `tools/eval-check.mjs` prueft die zwei aufgezeichneten Sieger erneut gegen
 *    dieselben Zahlen,
 *  - `tools/lib/eval-bounds.test.mjs` beweist die Regel an erfundenen
 *    Ergebnissen, ohne ein Modell zu laden.
 *
 * ## Warum es zwei Zahlen sind und nicht eine
 *
 * Die Trefferquote sagt, wie oft die Antwort stimmt; die Zitattreue sagt, ob man
 * es nachsehen kann. Ein Modell mit hoher Trefferquote und schlechter Zitattreue
 * hat fuer dieses Produkt nichts geleistet: seine richtigen Antworten sind von
 * seinen falschen nicht zu unterscheiden. Darum ist die Zitattreue die haertere
 * Schranke (0.9 gegen 0.6), und darum sind es zwei Bedingungen und nicht ein
 * gemittelter Punktwert, in dem eine schlechte Haelfte von einer guten
 * ausgeglichen werden koennte.
 */

/**
 * Die Grenzen, wortgleich mit ADR 0001 und mit tests/scaffold/w5b.test.mjs.
 *
 * `source` steht mit im Objekt, weil es in eval.json landet: eine Zahl in einem
 * Beweisartefakt, deren Herkunft man nicht nachsehen kann, ist eine Zahl, die
 * beim naechsten Lauf jemand anpasst.
 */
export const EVAL_BOUNDS = Object.freeze({
    passRate: 0.6,
    citationCompliance: 0.9,
    appliesTo: 'der Sieger JEDER Modellklasse, nicht das Feld im Schnitt',
    source: 'docs/adr/0001-modellwahl.md (Abschnitt Entscheidung), '
        + 'tests/scaffold/w5b.test.mjs Zeilen 51 und 53, PLAN.md Abschnitt 5',
});

/**
 * Ob ein einzelnes Ergebnis die Grenzen haelt.
 *
 * Fehlende Zahlen gelten als Verletzung und nicht als "unbekannt": ein Lauf, der
 * keinen Wert gemessen hat, hat auch keinen bestanden.
 */
export function violationsOf(label, result) {
    const found = [];
    const passRate = Number(result?.passRate);
    const citation = Number(result?.citationCompliance);
    if (!Number.isFinite(passRate) || passRate < EVAL_BOUNDS.passRate) {
        found.push(`${label}: passRate ${Number.isFinite(passRate) ? passRate : 'nicht gemessen'} `
            + `unter der Grenze ${EVAL_BOUNDS.passRate}`);
    }
    if (!Number.isFinite(citation) || citation < EVAL_BOUNDS.citationCompliance) {
        found.push(`${label}: Zitattreue ${Number.isFinite(citation) ? citation : 'nicht gemessen'} `
            + `unter der Grenze ${EVAL_BOUNDS.citationCompliance}`);
    }
    return found;
}

/**
 * Die Verletzungen aller Klassensieger, als flache Liste von Saetzen.
 *
 * `winners` ist ein Objekt Klasse -> Ergebnis, so wie es in eval.json steht.
 * Eine Klasse ohne Sieger ist selbst eine Verletzung: der Lauf hat dann fuer
 * eine der beiden Budgetklassen kein Modell gefunden, und ein Release, das eine
 * Klasse still weglaesst, waere ein Release mit einer halben Modellwahl.
 */
export function boundViolations(winners) {
    const found = [];
    for (const [modelClass, winner] of Object.entries(winners)) {
        if (winner === undefined || winner === null) {
            found.push(`Klasse ${modelClass}: kein Sieger ermittelt`);
            continue;
        }
        found.push(...violationsOf(`Klasse ${modelClass} (${winner.name ?? 'ohne Namen'})`, winner));
    }
    return found;
}

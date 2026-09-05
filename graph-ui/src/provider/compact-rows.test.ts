import { describe, expect, it } from 'vitest';
import {
    parseCompactRows,
    parseSearchResults,
    rowsAsObjects,
    splitCells,
} from './compact-rows';

/*
 * Die Eingaben sind aufgezeichnete Antworten des gebauten C-Servers aus
 * PR 1860 (W0-Lauf gegen fixtures/atlas-sample), woertlich uebernommen. Sie
 * sind absichtlich nicht geglaettet: der Kopf hat zwei Leerzeichen vor
 * "(cols:", Zahlenspalten kommen gequotet, und der Leer-Fall traegt einen
 * hint statt einer leeren Liste.
 */

/** query_graph ueber CALLS von createUser. */
const CALLS_RESPONSE = [
    'rows: 5  (cols: b.name r.line)',
    '  validateUser "24"',
    '  ValidationError "27"',
    '  UserEntity "29"',
    '  listUsers "29"',
    '  insert "30"',
    'total: 5',
    '',
].join('\n');

/** query_graph ueber CONFIGURES, drei Spalten, keine gequoteten Zellen. */
const CONFIGURES_RESPONSE = [
    'rows: 3  (cols: b.name c.name c.kind)',
    '  loadConfig HOST env_access',
    '  loadConfig PORT env_access',
    '  loadConfig DATABASE_URL env_access',
    'total: 3',
    '',
].join('\n');

/** Leeres Ergebnis: keine Zeilen, dafuer ein hint des Servers. */
const EMPTY_RESPONSE = [
    'rows: 0  (cols: t.name n.name)',
    'total: 0',
    'hint: "Query returned no results. Use get_graph_schema() to see available labels and edge types."',
    '',
].join('\n');

/** search_graph, andere Kopfform: results statt rows, dazu mode und has_more. */
const SEARCH_RESPONSE = [
    'total: 1',
    'search_mode: bm25',
    'results: 1  (cols: qn label file lines rank)',
    '  codeatlasweb-w0-probe.src.services.userService.createUser Function src/services/userService.ts 23-36 -15.21',
    'has_more: false',
    '',
].join('\n');

describe('parseCompactRows', () => {
    it('liest die aufgezeichnete CALLS-Antwort mit Spalten, Zeilen und total', () => {
        const parsed = parseCompactRows(CALLS_RESPONSE);
        expect(parsed.columns).toEqual(['b.name', 'r.line']);
        expect(parsed.total).toBe(5);
        expect(parsed.rows).toEqual([
            ['validateUser', '24'],
            ['ValidationError', '27'],
            ['UserEntity', '29'],
            ['listUsers', '29'],
            ['insert', '30'],
        ]);
    });

    it('entquotet Zellen, statt die Anfuehrungszeichen im Wert zu lassen', () => {
        const parsed = parseCompactRows(CALLS_RESPONSE);
        expect(parsed.rows[0][1]).toBe('24');
        expect(parsed.rows[0][1]).not.toContain('"');
    });

    it('liest die dreispaltige CONFIGURES-Antwort', () => {
        const parsed = parseCompactRows(CONFIGURES_RESPONSE);
        expect(parsed.columns).toHaveLength(3);
        expect(parsed.rows).toHaveLength(3);
        expect(parsed.rows[0]).toEqual(['loadConfig', 'HOST', 'env_access']);
        expect(parsed.hint).toBeUndefined();
    });

    it('liest den Leer-Fall als leere Liste plus hint, nicht als Fehler', () => {
        const parsed = parseCompactRows(EMPTY_RESPONSE);
        expect(parsed.rows).toEqual([]);
        expect(parsed.total).toBe(0);
        expect(parsed.hint).toBe(
            'Query returned no results. Use get_graph_schema() to see available labels and edge types.',
        );
    });

    it('haelt Leerzeichen und maskierte Anfuehrungszeichen in gequoteten Zellen zusammen', () => {
        const parsed = parseCompactRows(
            ['rows: 1  (cols: n.name n.doc)', '  handler "sagt \\"hallo\\" zweimal"', 'total: 1', ''].join(
                '\n',
            ),
        );
        expect(parsed.rows[0]).toEqual(['handler', 'sagt "hallo" zweimal']);
    });

    it('wirft mit Spaltenzahl und Zeile, wenn eine Zeile nicht zum Kopf passt', () => {
        const broken = ['rows: 1  (cols: b.name r.line)', '  validateUser "24" zuviel', 'total: 1', ''].join(
            '\n',
        );
        expect(() => parseCompactRows(broken)).toThrow(/3 Zellen.*2 Spalten/s);
    });

    it('wirft bei unbekanntem Kopf', () => {
        expect(() => parseCompactRows('nodes: 3\n  a\n')).toThrow(/unbekannter Kopf/);
    });

    it('nimmt die Suchform nicht als Zeilenform an', () => {
        expect(() => parseCompactRows(SEARCH_RESPONSE)).toThrow(/unbekannter Kopf/);
    });

    it('wirft, wenn der Kopf mehr Zeilen ankuendigt als geliefert werden', () => {
        const broken = ['rows: 2  (cols: b.name r.line)', '  validateUser "24"', 'total: 2', ''].join('\n');
        expect(() => parseCompactRows(broken)).toThrow(/rows: 2 an, geliefert wurden 1/);
    });

    it('wirft ohne total-Zeile, statt total zu erfinden', () => {
        const broken = ['rows: 1  (cols: b.name)', '  validateUser', ''].join('\n');
        expect(() => parseCompactRows(broken)).toThrow(/ohne total-Zeile/);
    });
});

describe('parseSearchResults', () => {
    it('liest die aufgezeichnete search_graph-Antwort', () => {
        const parsed = parseSearchResults(SEARCH_RESPONSE);
        expect(parsed.total).toBe(1);
        expect(parsed.mode).toBe('bm25');
        expect(parsed.hasMore).toBe(false);
        expect(parsed.columns).toEqual(['qn', 'label', 'file', 'lines', 'rank']);
        expect(parsed.rows).toEqual([
            [
                'codeatlasweb-w0-probe.src.services.userService.createUser',
                'Function',
                'src/services/userService.ts',
                '23-36',
                '-15.21',
            ],
        ]);
    });

    it('gibt has_more true durch, damit die Oberflaeche die Kappung benennen kann', () => {
        const capped = [
            'total: 42',
            'search_mode: bm25',
            'results: 1  (cols: qn label)',
            '  a.b.c Function',
            'has_more: true',
            '',
        ].join('\n');
        const parsed = parseSearchResults(capped);
        expect(parsed.hasMore).toBe(true);
        expect(parsed.total).toBe(42);
        expect(parsed.rows).toHaveLength(1);
    });

    it('nimmt die Zeilenform nicht als Suchform an', () => {
        expect(() => parseSearchResults(CALLS_RESPONSE)).toThrow(/unbekannte Zeile|unbekannter Kopf/);
    });
});

describe('Hilfsfunktionen', () => {
    it('splitCells trennt an Whitespace und respektiert Anfuehrungszeichen', () => {
        expect(splitCells('loadConfig "HOST PORT" env_access')).toEqual([
            'loadConfig',
            'HOST PORT',
            'env_access',
        ]);
    });

    it('splitCells wirft bei nicht geschlossenem Anfuehrungszeichen', () => {
        expect(() => splitCells('a "offen')).toThrow(/nicht geschlossenes/);
    });

    it('rowsAsObjects verbindet Spaltennamen und Zellen', () => {
        expect(rowsAsObjects(parseCompactRows(CALLS_RESPONSE))[0]).toEqual({
            'b.name': 'validateUser',
            'r.line': '24',
        });
    });
});

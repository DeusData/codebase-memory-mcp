/**
 * Die beiden Coverage-Quellen holen und zu einem Befund zusammenlegen.
 *
 * Der Join selbst steht in src/app/tree-model.ts und kennt keinen Server; hier
 * steht, wer gefragt wird und wie oft. Drei Entscheidungen, die man sonst raten
 * muesste:
 *
 * 1. **Beide Quellen, nicht eine.** `index_status` kappt seine Listen bei 500
 *    Eintraegen je Klasse (mcp.c, COVERAGE_FILE_CAP) und sagt es ueber
 *    `truncated`. `check_index_coverage` mit `scopes: ["."]` liest dieselbe
 *    Tabelle direkt und seitenweise. Wer nur die erste fragt, verliert bei
 *    einem grossen Projekt genau die Eintraege, wegen derer man fragt; wer nur
 *    die zweite fragt, verliert die Klassenzahlen und den Kappungs-Hinweis.
 * 2. **Paginiert wird bis `has_more` false ist, mit einem Deckel.** Der Deckel
 *    ist kein Misstrauen gegen den Server, sondern gegen eine Antwort, die
 *    `has_more` sagt und `next_offset` nicht bewegt: ohne ihn waere das eine
 *    Endlosschleife im Browser. Wird er erreicht, bleibt `has_more` stehen und
 *    der Join schreibt daraus eine ehrliche Zeile.
 * 3. **Ein Fehler ist kein leerer Befund.** Scheitert eine der beiden Fragen,
 *    wirft diese Datei. Ein stiller Rueckfall auf "keine Luecken" waere die
 *    schlimmste Luege, die dieser Zyklus bauen koennte: ein Baum, der behauptet,
 *    alles sei indiziert, weil er nicht nachfragen konnte.
 */

import type { RpcIntelligenceClient } from '../provider/rpc-client';
import {
    buildCoverageIndex,
    readCoverageAnswer,
    readIndexStatusCoverage,
} from './tree-model';
import type { CoverageAnswer, CoverageIndex, CoveragePathAnswer, CoverageScope } from './tree-model';

/** Die Wurzel, wie `check_index_coverage` sie schreibt. */
export const COVERAGE_ROOT_SCOPE = '.';

/** Wie viele Eintraege je Seite geholt werden. Der Server deckelt bei 1000. */
export const COVERAGE_SCOPE_LIMIT = 1000;

/** Wie viele Seiten hoechstens geholt werden. Siehe Entscheidung 2 im Kopf. */
export const COVERAGE_MAX_PAGES = 20;

/** Was ein Coverage-Lauf zurueckbringt: der Join und die Metadaten dahinter. */
export interface CoverageReading {
    index: CoverageIndex;
    /** Die Scope-Seiten, wie sie ankamen. Der Beweislauf schreibt sie mit. */
    scopes: CoverageScope[];
    /** Die Metadaten der letzten Antwort, inklusive der ignored_files-Zahlen. */
    answer: CoverageAnswer;
}

/**
 * Beide Quellen fragen und den Befund bauen.
 *
 * `index_status` zuerst, weil es die Klassenzahlen traegt und die Scope-Antwort
 * sie nicht kennt.
 */
export async function loadCoverage(
    client: RpcIntelligenceClient,
    project: string,
): Promise<CoverageReading> {
    const status = readIndexStatusCoverage(await client.indexStatusPayload(project));

    const scopes: CoverageScope[] = [];
    let answer = readCoverageAnswer(undefined);
    let offset = 0;
    for (let page = 0; page < COVERAGE_MAX_PAGES; page += 1) {
        answer = readCoverageAnswer(
            await client.checkIndexCoverage(project, {
                scopes: [COVERAGE_ROOT_SCOPE],
                scopeLimit: COVERAGE_SCOPE_LIMIT,
                scopeOffset: offset,
            }),
        );
        const scope = answer.scopes[0];
        if (scope === undefined) {
            break;
        }
        scopes.push(scope);
        if (!scope.hasMore) {
            break;
        }
        const next = scope.nextOffset ?? offset + scope.entries.length;
        if (next <= offset) {
            // Der Server sagt "mehr", bewegt den Zeiger aber nicht. Hier wird
            // abgebrochen und die Kappung bleibt im Befund stehen.
            break;
        }
        offset = next;
    }

    return { index: buildCoverageIndex({ status, scopes }), scopes, answer };
}

/**
 * Was der Store ueber genau einen Pfad sagt, mit Frische.
 *
 * Eine eigene Frage und nicht ein Blick in den Scope-Befund: die Frische
 * (`metadata_match` gegen `metadata_changed`) wird beim Fragen aus mtime und
 * Groesse der Datei berechnet (mcp.c, coverage_path_freshness), steht also in
 * keiner Liste, die vorher geholt wurde. Genau darum meldet sie eine Datei, die
 * sich seit dem Index geaendert hat.
 */
export async function loadPathCoverage(
    client: RpcIntelligenceClient,
    project: string,
    path: string,
): Promise<CoveragePathAnswer | undefined> {
    const answer = readCoverageAnswer(
        await client.checkIndexCoverage(project, { paths: [path] }),
    );
    return answer.paths[0];
}

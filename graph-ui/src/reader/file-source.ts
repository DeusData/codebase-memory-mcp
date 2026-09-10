/**
 * Eine Datei fuer den Reader holen, und ehrlich sagen, was davon angekommen ist.
 *
 * Der Server hat keinen Endpunkt, der eine Datei ausliefert. Es gibt keinen
 * /api/file (INVENTAR.md Abschnitt 3), und das ist Ask 1 auf der Upstream-Liste.
 * Was es gibt, ist `get_code_snippet` auf einem indizierten Symbol. Der
 * Modul-Knoten ist das Symbol, das die ganze Datei umspannt, und ueber ihn
 * kommt der Reader an den Quelltext.
 *
 * Der Weg hat einen Preis und der wird hier nicht versteckt:
 *
 * 1. **500 Zeilen sind Schluss.** `MCP_SNIPPET_MAX_LINES` kappt jeden
 *    Schnipsel (cbm/src/mcp/mcp.c). Der Server sagt es selbst mit
 *    `source_clipped`. Eine laengere Datei kommt also unvollstaendig an, und
 *    der Reader schreibt darunter, welche Zeilen fehlen und warum.
 * 2. **Nachladen gibt es seit dem schlanken Ausgabevertrag (#1597).** Bis
 *    dahin wurden `start_line` und `end_line` angenommen und ignoriert
 *    (gemessen, verification/w2/reader.json unter `windowSemantics`). Seit
 *    dem Vertrag liefert `source_mode: full` Seiten zu 500 Zeilen, und die
 *    Antwort nennt in `next_start_line` die naechste; loadFileDocument holt
 *    sie nacheinander und fuegt sie zusammen. Ohne `source_mode: full`
 *    umreisst der Server jeden Container ab 200 Zeilen und schickt gar
 *    keinen Quelltext. Einen "load more"-Knopf gibt es weiterhin nicht: die
 *    Seiten kommen von selbst, und was nicht kommt, steht als Satz darunter.
 * 3. **Nur, was indiziert ist.** Eine Datei ohne Modul-Knoten ist ueber diesen
 *    Weg nicht lesbar. Der Reader sagt das, statt eine leere Flaeche zu zeigen.
 */

import { fileNodeForPath, moduleForFile, COLUMNS } from '../provider/cypher';
import type { RpcIntelligenceClient } from '../provider/rpc-client';
import type { CodeSnippetResult } from '../provider/rpc-schemas';
import { moduleQualifiedName, moduleQnFromFileQn, normalizeWorkspacePath } from '../app/module-qn';
import { reportError } from '../provider/error-observer';

/** Das Werkzeug, aus dem der Inhalt des Readers kommt. Der Beweislauf schreibt es mit. */
export const READER_RPC_TOOL = 'get_code_snippet';

/**
 * Woher der qualifizierte Name stammt, mit dem geladen wurde.
 *
 * `derived` heisst nicht "ungeprueft": es heisst, dass die Ableitung aus dem
 * Pfad und der Modul-Knoten im Graphen denselben Namen ergeben haben. Die
 * anderen beiden Werte heissen, dass sie es nicht taten und der Graph gewonnen
 * hat.
 */
export type QnSource = 'derived' | 'graph-module' | 'graph-file';

/** Eine geladene Datei, mit allem, was ueber ihre Vollstaendigkeit bekannt ist. */
export interface ReaderDocument {
    /** Workspace-relativer Pfad, so wie der Baum ihn nennt. */
    path: string;
    /** Der Name, unter dem der Quelltext geholt wurde. */
    qualifiedName: string;
    qnSource: QnSource;
    /** Der abgeleitete Name, zum Vergleich. Weicht er ab, hat der Graph recht. */
    derivedQualifiedName: string;
    source: string;
    /** Erste geladene Zeile, 1-basiert. */
    firstLine: number;
    /** Letzte geladene Zeile, 1-basiert. */
    lastLine: number;
    /** Letzte Zeile der Datei laut Graph, wenn er eine Spanne fuehrt. */
    fileLastLine?: number;
    /** Ob geladen wurde, was der Server hergibt, aber nicht, was die Datei ist. */
    truncated: boolean;
    /** Der Satz, den der Reader unter den Quelltext schreibt. Leer wenn vollstaendig. */
    truncationNote: string;
}

/** Die Datei ist ueber diesen Weg nicht lesbar, mit Begruendung statt leerer Flaeche. */
export class FileNotReadableError extends Error {
    constructor(readonly path: string, message: string) {
        super(message);
        this.name = 'FileNotReadableError';
    }
}

/**
 * A file the reader cannot show is announced to the frontend log before it
 * is thrown (provider/error-observer.ts). The /rpc failures underneath
 * announce themselves; a file the index does not know, or the server's
 * placeholder for a file gone from disk, is a successful call with nothing
 * in it, and only this module knows that it is a failure.
 */
function notReadable(path: string, message: string): FileNotReadableError {
    reportError({ source: 'reader', level: 'warn', message });
    return new FileNotReadableError(path, message);
}

function toOptionalNumber(cell: string | undefined): number | undefined {
    if (cell === undefined || cell === '') {
        return undefined;
    }
    const parsed = Number(cell);
    return Number.isFinite(parsed) ? parsed : undefined;
}

/**
 * Der Modul-Knoten dieser Datei, beim Graphen nachgefragt.
 *
 * Zuerst das Modul-Label, weil nur der Modul-Knoten eine Zeilenspanne mitbringt.
 * Wenn es keinen gibt, der File-Knoten: aus seinem Namen laesst sich der des
 * Moduls zurueckrechnen, aber eine Spanne kommt nicht mit, und dann weiss der
 * Reader spaeter nicht, wie lang die Datei wirklich ist.
 */
async function lookupModuleNode(
    client: RpcIntelligenceClient,
    project: string,
    path: string,
): Promise<{ qualifiedName: string; source: QnSource; endLine?: number } | undefined> {
    const moduleRows = await client.queryRows(project, moduleForFile(path));
    const moduleRow = moduleRows[0];
    if (moduleRow !== undefined) {
        const qualifiedName = moduleRow[COLUMNS.moduleForFile[0]] ?? '';
        if (qualifiedName.length > 0) {
            const endLine = toOptionalNumber(moduleRow[COLUMNS.moduleForFile[2]]);
            return endLine === undefined
                ? { qualifiedName, source: 'graph-module' }
                : { qualifiedName, source: 'graph-module', endLine };
        }
    }

    const fileRows = await client.queryRows(project, fileNodeForPath(path));
    const fileQualifiedName = fileRows[0]?.[COLUMNS.fileNode[0]] ?? '';
    if (fileQualifiedName.length === 0) {
        return undefined;
    }
    const derived = moduleQnFromFileQn(fileQualifiedName);
    return derived === undefined ? undefined : { qualifiedName: derived, source: 'graph-file' };
}

/** What the server writes in place of source when it cannot read the file. */
export const SOURCE_UNAVAILABLE = '(source not available)';

/** The server's page size for full source (MCP_SNIPPET_MAX_LINES). */
export const SOURCE_PAGE_LINES = 500;

/** More pages than this is not a source file this reader should be loading. */
export const MAX_SOURCE_PAGES = 100;

/** The note when the server pages source and a page did not arrive. */
export function pagingNoteFor(lastLine: number, fileLastLine: number | undefined): string {
    if (fileLastLine === undefined) {
        return `lines after ${lastLine} not loaded: a further page of source did not arrive from the server (get_code_snippet).`;
    }
    return `lines ${lastLine + 1}-${fileLastLine} not loaded: a further page of source did not arrive from the server (get_code_snippet).`;
}

/**
 * Der Satz unter dem Quelltext, wenn nicht die ganze Datei angekommen ist.
 *
 * Er nennt die fehlenden Zeilen und den Grund. Kein "..." und kein
 * ausgegrautes Ende: wer die Datei liest, muss wissen, dass unten etwas fehlt,
 * bevor er einen Schluss daraus zieht, dass dort nichts steht.
 */
export function truncationNoteFor(lastLine: number, fileLastLine: number | undefined, cap: number | undefined): string {
    const capPart = cap === undefined
        ? 'server snippet cap'
        : `server snippet cap of ${cap} lines`;
    if (fileLastLine === undefined) {
        return `lines after ${lastLine} not loaded: ${capPart} (get_code_snippet). The index did not record the file length.`;
    }
    return `lines ${lastLine + 1}-${fileLastLine} not loaded: ${capPart} (get_code_snippet). The window arguments are ignored by this server, so there is nothing to load them with.`;
}

/**
 * Eine Datei laden: Namen ableiten, gegen den Graphen halten, Quelltext holen,
 * Vollstaendigkeit beurteilen.
 */
export async function loadFileDocument(
    client: RpcIntelligenceClient,
    project: string,
    filePath: string,
): Promise<ReaderDocument> {
    const path = normalizeWorkspacePath(filePath);
    const derivedQualifiedName = moduleQualifiedName(project, path);

    const node = await lookupModuleNode(client, project, path);
    if (node === undefined) {
        throw notReadable(
            path,
            `The index has no module node for ${path}. This server delivers file content only through get_code_snippet on an indexed symbol, so a file the index did not record cannot be read here.`,
        );
    }

    const qualifiedName = node.qualifiedName;
    const snippet = await client.getCodeSnippet(project, qualifiedName);
    if (snippet.source.length === 0 || snippet.source === SOURCE_UNAVAILABLE) {
        throw notReadable(
            path,
            snippet.source === SOURCE_UNAVAILABLE
                ? `The server could not read ${path} from the repository (${qualifiedName}): `
                    + 'the index knows the file, the disk does not have it where the index expects it.'
                : `get_code_snippet returned no source for ${qualifiedName}.`,
        );
    }

    /*
     * Since the lean output contract the server pages source 500 lines at a
     * time and names the next page in `next_start_line`. The pages are
     * fetched one after the other and joined; a page that does not arrive
     * ends the loading, and the note below names what is missing rather
     * than a guess. Before the contract there was one window and no
     * `next_start_line`, and that path is unchanged.
     */
    const firstLine = snippet.start_line ?? 1;
    let source = snippet.source;
    let lastLine = snippet.end_line ?? firstLine;
    let next = snippet.next_start_line;
    let pages = 1;
    let pageFailed = false;
    while (next !== undefined && pages < MAX_SOURCE_PAGES) {
        let page: CodeSnippetResult;
        try {
            page = await client.getCodeSnippet(project, qualifiedName, {
                startLine: next,
                maxLines: SOURCE_PAGE_LINES,
            });
        } catch {
            pageFailed = true;
            break;
        }
        if (page.source.length === 0 || page.source === SOURCE_UNAVAILABLE
            || (page.start_line !== undefined && page.start_line !== next)) {
            pageFailed = true;
            break;
        }
        if (!source.endsWith('\n')) {
            source += '\n';
        }
        source += page.source;
        lastLine = page.end_line ?? lastLine;
        next = page.next_start_line;
        pages += 1;
    }
    const fileLastLine = snippet.original_end_line ?? node.endLine;
    const pagesMissing = pageFailed || next !== undefined;
    const singleWindowClipped = pages === 1 && snippet.next_start_line === undefined
        && snippet.source_clipped === true;
    const shortOfFile = fileLastLine !== undefined && fileLastLine > lastLine;
    const truncated = pagesMissing || singleWindowClipped || shortOfFile;

    const document: ReaderDocument = {
        path,
        qualifiedName,
        qnSource: qualifiedName === derivedQualifiedName ? 'derived' : node.source,
        derivedQualifiedName,
        source,
        firstLine,
        lastLine,
        truncated,
        truncationNote: truncated
            ? (pagesMissing
                ? pagingNoteFor(lastLine, fileLastLine)
                : truncationNoteFor(lastLine, fileLastLine, snippet.clipped_at_lines))
            : '',
    };
    if (fileLastLine !== undefined) {
        document.fileLastLine = fileLastLine;
    }
    return document;
}

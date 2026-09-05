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
 * 2. **Nachladen geht nicht.** `get_code_snippet` nimmt `qualified_name`,
 *    `project` und `include_neighbors`, sonst nichts. `start_line` und
 *    `end_line` werden angenommen und ignoriert: dieselbe Anfrage mit
 *    start_line 501 liefert wieder die Zeilen 1 bis 500. Gemessen am
 *    laufenden Server, nachzulesen in verification/w2/reader.json unter
 *    `windowSemantics`. Es gibt darum kein "load more": eine Schaltflaeche,
 *    die dasselbe noch einmal holt, waere eine Luege in Knopfform.
 * 3. **Nur, was indiziert ist.** Eine Datei ohne Modul-Knoten ist ueber diesen
 *    Weg nicht lesbar. Der Reader sagt das, statt eine leere Flaeche zu zeigen.
 */

import { fileNodeForPath, moduleForFile, COLUMNS } from '../provider/cypher';
import type { RpcIntelligenceClient } from '../provider/rpc-client';
import { moduleQualifiedName, moduleQnFromFileQn, normalizeWorkspacePath } from '../app/module-qn';

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
        throw new FileNotReadableError(
            path,
            `The index has no module node for ${path}. This server delivers file content only through get_code_snippet on an indexed symbol, so a file the index did not record cannot be read here.`,
        );
    }

    const qualifiedName = node.qualifiedName;
    const snippet = await client.getCodeSnippet(project, qualifiedName);
    if (snippet.source.length === 0) {
        throw new FileNotReadableError(
            path,
            `get_code_snippet returned no source for ${qualifiedName}.`,
        );
    }

    const firstLine = snippet.start_line ?? 1;
    const lastLine = snippet.end_line ?? firstLine;
    const fileLastLine = node.endLine;
    const clipped = snippet.source_clipped === true;
    const shortOfFile = fileLastLine !== undefined && fileLastLine > lastLine;
    const truncated = clipped || shortOfFile;

    const document: ReaderDocument = {
        path,
        qualifiedName,
        qnSource: qualifiedName === derivedQualifiedName ? 'derived' : node.source,
        derivedQualifiedName,
        source: snippet.source,
        firstLine,
        lastLine,
        truncated,
        truncationNote: truncated
            ? truncationNoteFor(lastLine, fileLastLine, snippet.clipped_at_lines)
            : '',
    };
    if (fileLastLine !== undefined) {
        document.fileLastLine = fileLastLine;
    }
    return document;
}

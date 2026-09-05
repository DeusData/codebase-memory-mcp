/**
 * Eine getypte Methode je Engine-Werkzeug, gesprochen ueber POST /rpc.
 *
 * Diese Schicht hat drei Aufgaben und keine vierte: das Werkzeug benennen, die
 * Antwort auspacken und ihre Form pruefen. Sie kennt kein Produktvokabular und
 * faellt kein Urteil darueber, was ein Ergebnis bedeutet; das ist Sache des
 * Providers. Vorbild ist IntelligenceClient aus CodeAtlasIDE
 * (theia-extensions/codeatlas-intelligence/src/node/client/intelligence-client.ts),
 * die Methodenliste ist dieselbe. Der Transport ist ein anderer, und die
 * Unterschiede sind der Inhalt dieser Datei.
 *
 * **Kein Kindprozess.** Das Original startete je Aufruf die CLI. Hier laeuft
 * alles im Browser gegen den Server aus PR 1860, also ueber fetch. Nichts
 * unter src/** darf einen Prozess starten, und nichts hier tut es.
 *
 * **Zwei Drahtformen statt einer.** Die 0.9.0-CLI antwortete auf alles mit
 * JSON. Der Server antwortet je Werkzeug verschieden, und das ist gegen den
 * gebauten Server geprueft, nicht angenommen:
 *
 * - `query_graph` liefert die kompakte Zeilenform. Sie geht durch
 *   parseCompactRows und wird zu `{columns, rows}`, also genau die Form, die
 *   der portierte Provider vorher von der CLI bekam.
 * - `search_graph` liefert bei einer BM25-Anfrage die flache Suchform, die
 *   parseSearchResults liest.
 * - `list_projects`, `index_status` und `get_code_snippet` liefern JSON von
 *   sich aus.
 * - `detect_changes`, `get_architecture` und `trace_path` liefern eine
 *   Textform fuer Menschen, es sei denn, man bittet mit `format: "json"` um
 *   Daten. Genau das tun die Methoden unten. Ein Parser fuer die Textform
 *   waere ein zweiter, stiller Vertrag ueber eine Anzeige gewesen.
 *
 * **Drei Werkzeuge gibt es hier nicht.** `index_repository`, `delete_project`
 * und `ingest_traces` beantwortet die Read-only-Allowlist des Servers mit 403
 * und -32601. Die Methoden sind trotzdem da und werfen sofort, statt zu
 * fehlen: eine fehlende Methode waere ein Uebersetzungsfehler beim Aufrufer,
 * ein klarer Fehler ist eine Aussage ueber diese Oberflaeche. Indiziert wird
 * ueber die CLI, nicht von hier.
 */

import { parseCompactRows, parseSearchResults, rowsAsObjects } from './compact-rows';
import type { CompactRows, SearchResults } from './compact-rows';
import { callToolJson, callToolText, RpcError } from './rpc-transport';
import type { CallToolOptions } from './rpc-transport';
import { EngineError, EngineUnavailableError } from './engine-errors';
import {
    normalizeCell,
    readArchitecture,
    readCodeSnippet,
    readDetectChanges,
    readIndexStatus,
    readListProjects,
    readPathResult,
    readSearchRows,
} from './rpc-schemas';
import type {
    ArchitectureResult,
    CodeSnippetResult,
    DetectChangesResult,
    IndexStatusResult,
    ListProjectsResult,
    PathResult,
    QueryGraphResult,
    SearchGraphResult,
} from './rpc-schemas';

/** Werkzeuge, die die Read-only-Allowlist des Servers nicht anbietet. */
export const RPC_BLOCKED_TOOLS = ['index_repository', 'delete_project', 'ingest_traces'] as const;

/** Stellschrauben des Clients. Ohne base ist der Ursprung der ausliefernde Server. */
export interface RpcClientOptions {
    /** Ursprung des Servers, ohne Schraegstrich am Ende. Leer heisst same-origin. */
    base?: string;
    /** Ersetzbares fetch, damit Tests ohne Netz laufen. */
    fetch?: typeof globalThis.fetch;
    /** Abbruchsignal, an jeden Aufruf durchgereicht. */
    signal?: AbortSignal;
}

/**
 * Eine tabellarische Antwort als ein Objekt je Zeile, benannt nach den Spalten,
 * die die Abfrage verlangt hat.
 *
 * Die Spaltennamen behalten ihre Engine-Schreibweise, und zwar mit Absicht: der
 * einzige Code, der sie liest, ist der Abfragebauer, der sie geschrieben hat.
 */
export function rowsToObjects(columns: string[], rows: string[][]): Record<string, string>[] {
    return rows.map((row) => {
        const out: Record<string, string> = {};
        columns.forEach((column, index) => {
            out[column] = row[index] ?? '';
        });
        return out;
    });
}

/**
 * Was ein gescheiterter Aufruf fuer die Produktlogik bedeutet.
 *
 * Der Transport kennt vier Arten des Scheiterns; der Provider unterscheidet
 * zwei Lagen, und nur diese zwei. "Die Engine hat geantwortet und nein gesagt"
 * ist eine Aussage ueber Projekt oder Abfrage und fuehrt zu notIndexed. "Es hat
 * niemand geantwortet" ist keine Aussage ueber den Code des Lesers und fuehrt
 * zu unknown. Die Zuordnung steht hier, damit sie einmal existiert:
 *
 * - fetch selbst hat abgelehnt: kein Server, kein Netz, abgebrochen. Nicht
 *   erreichbar.
 * - der Server bietet das Werkzeug nicht an (403 oder -32601). Nicht
 *   erreichbar fuer diese Faehigkeit, und das ist keine Aussage ueber das
 *   Projekt.
 * - HTTP 5xx: der Server ist da und kaputt. Nicht erreichbar.
 * - alles andere, inklusive einer Antwort in unerwarteter Form: die Engine hat
 *   geantwortet, also ein Engine-Fehler.
 */
function asEngineFailure(tool: string, error: unknown): Error {
    if (error instanceof RpcError) {
        if (error.notAllowed) {
            return new EngineUnavailableError(
                'not-allowed',
                `${tool}: dieser Server bietet das Werkzeug auf /rpc nicht an (${error.message})`,
                tool,
            );
        }
        if (error.kind === 'http' && (error.status ?? 0) >= 500) {
            return new EngineUnavailableError('server-error', error.message, tool);
        }
        return new EngineError(tool, error.message, { status: error.status });
    }
    const message = error instanceof Error ? error.message : String(error);
    return new EngineUnavailableError('unreachable', `${tool}: ${message}`, tool);
}

/** Eine Antwort, die ankam, aber nicht zu lesen war. Die Engine lief, also ein Engine-Fehler. */
function asParseFailure(tool: string, error: unknown): EngineError {
    const message = error instanceof Error ? error.message : String(error);
    return new EngineError(tool, `Antwort war nicht lesbar: ${message}`);
}

export class RpcIntelligenceClient {

    constructor(protected readonly options: RpcClientOptions = {}) { }

    /** Die Transportoptionen dieses Clients, je Aufruf frisch zusammengesetzt. */
    protected callOptions(): CallToolOptions {
        const opts: CallToolOptions = {};
        if (this.options.base !== undefined) {
            opts.base = this.options.base;
        }
        if (this.options.fetch !== undefined) {
            opts.fetch = this.options.fetch;
        }
        if (this.options.signal !== undefined) {
            opts.signal = this.options.signal;
        }
        return opts;
    }

    /**
     * Rohtext eines Werkzeugs, mit der Fehlerzuordnung dieser Schicht.
     *
     * `signal` ist je Aufruf und nicht je Client, und das ist seit W7b der
     * Unterschied, auf den es ankommt: der Client lebt so lange wie die Seite,
     * eine Suchanfrage nur bis zum naechsten Tastendruck. Ein Signal am Client
     * koennte also nur alles abbrechen oder nichts.
     */
    protected async text(
        tool: string,
        args: Record<string, unknown>,
        call: { signal?: AbortSignal } = {},
    ): Promise<string> {
        try {
            return await callToolText(tool, args, {
                ...this.callOptions(),
                ...(call.signal === undefined ? {} : { signal: call.signal }),
            });
        } catch (error) {
            throw asEngineFailure(tool, error);
        }
    }

    /** JSON eines Werkzeugs. `format: "json"` gehoert zur Bitte, nicht zur Antwort. */
    protected async json(tool: string, args: Record<string, unknown>): Promise<unknown> {
        try {
            return await callToolJson(tool, { ...args, format: 'json' }, this.callOptions());
        } catch (error) {
            throw asEngineFailure(tool, error);
        }
    }

    /** Ein Werkzeug, das diese Oberflaeche nicht hat, mit einer Begruendung statt einer Ausrede. */
    protected refuse(tool: string): never {
        throw new EngineUnavailableError(
            'not-allowed',
            `${tool}: die Read-only-Allowlist des Servers bietet dieses Werkzeug auf /rpc nicht an. ` +
                'Schreibende Vorgaenge laufen ueber die CLI, nicht ueber diese Oberflaeche.',
            tool,
        );
    }

    /**
     * Tabellarische Abfrage.
     *
     * Der Weg durch parseCompactRows ist der Kern dieses Zyklus: der Server
     * schickt hier keine Tabelle als JSON, sondern Kopf, eingerueckte Zeilen
     * und Fuss. Was herauskommt, ist die Form `{columns, rows}`, die der
     * portierte Provider von der 0.9.0-CLI kannte, damit die Umstellung des
     * Transports an dieser Stelle endet.
     *
     * Der Leermarker der kompakten Form wird dabei zur leeren Zeichenkette.
     * Siehe EMPTY_CELL in rpc-schemas.ts: die JSON-Form derselben Abfrage
     * schreibt an derselben Stelle `""`, und ohne die Uebersetzung wuerde ein
     * Bindestrich als Dateiname durch das Produkt wandern.
     */
    async queryGraph(project: string, query: string): Promise<QueryGraphResult> {
        const text = await this.text('query_graph', { project, query });
        let parsed: CompactRows;
        try {
            parsed = parseCompactRows(text);
        } catch (error) {
            throw asParseFailure('query_graph', error);
        }
        return {
            columns: parsed.columns,
            rows: parsed.rows.map((cells) => cells.map(normalizeCell)),
            total: parsed.total,
        };
    }

    /** Tabellarische Abfrage, schon als ein Objekt je Zeile. */
    async queryRows(project: string, query: string): Promise<Record<string, string>[]> {
        const result = await this.queryGraph(project, query);
        return rowsToObjects(result.columns, result.rows);
    }

    async listProjects(): Promise<ListProjectsResult> {
        return readListProjects(await this.json('list_projects', {}));
    }

    async indexStatus(project: string): Promise<IndexStatusResult> {
        return readIndexStatus(await this.json('index_status', { project }));
    }

    /**
     * Dieselbe Antwort, ungedeutet.
     *
     * `index_status` traegt neben den Kennzahlen eine Coverage-Beilage
     * (`parse_partial`, `skipped`, `not_indexed`). Sie wird hier absichtlich
     * nicht gelesen: gedeutet wird sie im Join des Explorers
     * (src/app/tree-model.ts), weil erst dort entschieden wird, was eine dieser
     * Listen ueber eine Zeile im Baum aussagt. Zwei Leser fuer dieselben Felder
     * waeren zwei Vertraege ueber dieselbe Antwort.
     */
    async indexStatusPayload(project: string): Promise<unknown> {
        return this.json('index_status', { project });
    }

    /**
     * Der Coverage-Store, gefragt nach Pfaden oder nach Scopes.
     *
     * Zwei Fragen in einem Werkzeug, und sie sind nicht dieselbe: `paths`
     * beantwortet "was weiss der Store ueber genau diese Datei" samt Frische
     * (`freshness`, `recommended_action`), `scopes` listet die Eintraege unter
     * einem Pfad-Praefix, seitenweise ueber `scope_offset`. Die Wurzel heisst
     * `"."`.
     *
     * Die Antwort bleibt roh, aus demselben Grund wie bei `indexStatusPayload`.
     */
    async checkIndexCoverage(
        project: string,
        args: {
            paths?: readonly string[];
            scopes?: readonly string[];
            scopeLimit?: number;
            scopeOffset?: number;
        },
    ): Promise<unknown> {
        return this.json('check_index_coverage', {
            project,
            ...(args.paths === undefined ? {} : { paths: [...args.paths] }),
            ...(args.scopes === undefined ? {} : { scopes: [...args.scopes] }),
            ...(args.scopeLimit === undefined ? {} : { scope_limit: args.scopeLimit }),
            ...(args.scopeOffset === undefined ? {} : { scope_offset: args.scopeOffset }),
        });
    }

    /**
     * Pfade von einem Symbol aus laufen. Liefert die Vereinigung: eine
     * Ablehnung wegen Mehrdeutigkeit ist hier eine normale Antwort, kein
     * Fehler.
     */
    async tracePath(
        project: string,
        functionName: string,
        args: { direction?: 'callers' | 'callees' | 'both'; maxDepth?: number } = {},
    ): Promise<PathResult> {
        return readPathResult(await this.json('trace_path', {
            project,
            function_name: functionName,
            ...(args.direction ? { direction: args.direction } : {}),
            ...(args.maxDepth !== undefined ? { max_depth: args.maxDepth } : {}),
        }));
    }

    /**
     * Symbole suchen.
     *
     * Gesendet wird `query`, nicht `name_pattern`, und das ist eine bewusste
     * Abweichung vom Original. Der Server beantwortet `name_pattern` in einer
     * nach Modulen gruppierten Anzeigeform und `query` in der flachen Form mit
     * `search_mode` und Rang, die parseSearchResults liest. Was das kostet, ist
     * benannt: die Treffer sind BM25-gerankt statt nach Namensmuster gefiltert,
     * und `is_test` und `is_exported` stehen nicht in der Antwort, bleiben also
     * undefined statt false.
     */
    async searchGraph(
        project: string,
        args: { namePattern?: string; query?: string; limit?: number; signal?: AbortSignal },
    ): Promise<SearchGraphResult> {
        const needle = args.query ?? args.namePattern ?? '';
        const text = await this.text('search_graph', {
            project,
            query: needle,
            ...(args.limit !== undefined ? { limit: args.limit } : {}),
        }, args.signal === undefined ? {} : { signal: args.signal });
        let parsed: SearchResults;
        try {
            parsed = parseSearchResults(text);
        } catch (error) {
            throw asParseFailure('search_graph', error);
        }
        const rows = rowsAsObjects(parsed).map((row) => {
            const out: Record<string, string> = {};
            for (const [key, value] of Object.entries(row)) {
                out[key] = normalizeCell(value);
            }
            return out;
        });
        return readSearchRows(rows, parsed.total);
    }

    async getCodeSnippet(project: string, qualifiedName: string): Promise<CodeSnippetResult> {
        return readCodeSnippet(await this.json('get_code_snippet', {
            project,
            qualified_name: qualifiedName,
        }));
    }

    async getArchitecture(project: string, aspects: string[] = ['all']): Promise<ArchitectureResult> {
        return readArchitecture(await this.json('get_architecture', { project, aspects }));
    }

    /**
     * Die Aenderungsmenge eines Projekts.
     *
     * Der Vergleichspunkt geht als `since` auf den Draht, so wie das Werkzeug
     * ihn deklariert. Eine fruehere Schreibweise im Referenzprojekt war
     * `since_ref`, und sie sah aus genau dem falschen Grund richtig aus: das
     * Werkzeug nimmt unbekannte Argumente kommentarlos an und antwortet, als
     * waere keines gekommen. Ein Ref unter dem falschen Namen ergibt also eine
     * plausible Antwort ueber den Arbeitsbaum statt einer Ablehnung.
     */
    async detectChanges(
        project: string,
        sinceRef?: string,
        args: { depth?: number } = {},
    ): Promise<DetectChangesResult> {
        return readDetectChanges(await this.json('detect_changes', {
            project,
            ...(sinceRef ? { since: sinceRef } : {}),
            ...(args.depth !== undefined ? { depth: args.depth } : {}),
        }));
    }

    async indexRepository(): Promise<never> {
        return this.refuse('index_repository');
    }

    async deleteProject(): Promise<never> {
        return this.refuse('delete_project');
    }

    async ingestTraces(): Promise<never> {
        return this.refuse('ingest_traces');
    }
}

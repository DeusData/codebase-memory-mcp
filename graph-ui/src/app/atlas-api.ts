/**
 * Die /api-Routen des Servers, so weit dieses Chrome sie braucht.
 *
 * Zwei Wege fuehren zum Server und sie sind nicht dasselbe: `/rpc` traegt die
 * MCP-Werkzeuge (src/provider/rpc-client.ts), `/api` traegt die fertig
 * gerechneten Atlas-Antworten. Der Datei-Baum ist eine davon, und er kommt
 * ausdruecklich aus dem Graphen und nicht vom Dateisystem: was hier nicht
 * steht, hat der Index nicht gesehen.
 *
 * Same-origin ist der Normalfall. Der Server nimmt `/api` nur an, wenn der
 * Host-Kopf genau seine eigene Loopback-Adresse nennt und der Origin-Kopf
 * entweder fehlt oder dieselbe Adresse nennt (INVENTAR.md Abschnitt 5). Ein
 * `base` ist darum nur fuer Node-Aufrufer da, die keinen Origin senden.
 */

import { readTreeLevel } from './tree-model';
import type { TreeLevel } from './tree-model';
import {
    readFlowDetail,
    readFlowSummaries,
    readTraceAnswer,
} from '../traces/trace-schemas';
import type { FlowDetail, FlowSummary, TraceAnswer } from '../traces/trace-schemas';

export interface AtlasApiOptions {
    /** Ursprung des Servers, ohne Schraegstrich am Ende. Leer heisst same-origin. */
    base?: string;
    /** Ersetzbares fetch, damit Tests ohne Netz laufen. */
    fetch?: typeof globalThis.fetch;
}

/** Was `/api/repo-info` ueber ein Projekt weiss. Leere Felder heissen "unbekannt". */
export interface RepoInfo {
    rootPath: string;
    branch: string;
    remoteUrl: string;
}

/** Ein Fehler einer /api-Route, mit dem Status, den sie geliefert hat. */
export class AtlasApiError extends Error {
    constructor(
        readonly route: string,
        readonly status: number,
        message: string,
    ) {
        super(message);
        this.name = 'AtlasApiError';
    }
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

const text = (value: unknown): string => (typeof value === 'string' ? value : '');

export class AtlasApi {

    constructor(private readonly options: AtlasApiOptions = {}) { }

    private async getJson(route: string): Promise<unknown> {
        const url = `${this.options.base ?? ''}${route}`;
        const doFetch = this.options.fetch ?? globalThis.fetch;
        let response: Response;
        try {
            response = await doFetch(url, { headers: { Accept: 'application/json' } });
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            throw new AtlasApiError(route, 0, `${route} war nicht erreichbar: ${message}`);
        }
        const body = await response.text();
        if (!response.ok) {
            throw new AtlasApiError(route, response.status, `${route} antwortete mit HTTP ${response.status}: ${body.slice(0, 200)}`);
        }
        try {
            return JSON.parse(body) as unknown;
        } catch {
            throw new AtlasApiError(route, response.status, `${route} lieferte kein JSON: ${body.slice(0, 200)}`);
        }
    }

    /**
     * Eine Ebene des Datei-Baums. `path` leer heisst Projektwurzel.
     *
     * Der Server antwortet je Anfrage mit genau einer Ebene. Der Explorer
     * fragt darum je aufgeklapptem Ordner einmal nach, statt einen ganzen Baum
     * zu erwarten, den es auf dieser Route nicht gibt.
     */
    async tree(project: string, path = ''): Promise<TreeLevel> {
        const query = new URLSearchParams({ project });
        if (path.length > 0) {
            query.set('path', path);
        }
        return readTreeLevel(await this.getJson(`/api/tree?${query.toString()}`));
    }

    /**
     * Der kuerzeste Weg, den der Index von `from` nach `to` kennt, mit dem, was
     * davon beobachtet wurde.
     *
     * `mode` bleibt `calls`: `data` laeuft ueber DATA_FLOWS-Kanten, und die
     * schreibt die Analyse fuer TypeScript nicht. Die genaue Bedeutung jeder
     * Antwortform steht in src/traces/bug-paths.ts, wo sie gelesen wird.
     */
    async trace(project: string, from: string, to: string): Promise<TraceAnswer> {
        const query = new URLSearchParams({ project, from, to, mode: 'calls' });
        return readTraceAnswer(await this.getJson(`${TRACE_ROUTE}?${query.toString()}`));
    }

    /** Die gereihten Ablaeufe des Projekts, ohne ihre Schritte. */
    async flows(project: string): Promise<FlowSummary[]> {
        const query = new URLSearchParams({ project });
        return readFlowSummaries(await this.getJson(`${FLOWS_ROUTE}?${query.toString()}`));
    }

    /** Die Schritte eines Ablaufs, mit dem, was davon beobachtet wurde. */
    async flow(project: string, id: number): Promise<FlowDetail> {
        const query = new URLSearchParams({ project, id: String(id) });
        return readFlowDetail(await this.getJson(`${FLOW_ROUTE}?${query.toString()}`));
    }

    /** Wurzelpfad, Branch und Remote eines Projekts. Leere Felder bleiben leer. */
    async repoInfo(project: string): Promise<RepoInfo> {
        const raw = await this.getJson(`/api/repo-info?${new URLSearchParams({ project }).toString()}`);
        const record = isRecord(raw) ? raw : {};
        return {
            rootPath: text(record['root_path']),
            branch: text(record['branch']),
            remoteUrl: text(record['remote_url']),
        };
    }
}

/** Der Name der Route, aus der der Baum kommt. Der Beweislauf schreibt ihn mit. */
export const TREE_ROUTE = '/api/tree';

/**
 * Die drei Routen, ueber die Beobachtetes lesbar ist.
 *
 * Es gibt keine vierte: `observed_calls` wird ausschliesslich von
 * `cbm_atlas_attach_observed` an eine Trace- oder Flow-Antwort geheftet
 * (cbm/src/ui/http_server.c). Was das fuer den BUG-Assistenten bedeutet, steht
 * im Kopf von src/traces/bug-paths.ts.
 */
export const TRACE_ROUTE = '/api/trace';
export const FLOWS_ROUTE = '/api/flows';
export const FLOW_ROUTE = '/api/flow';

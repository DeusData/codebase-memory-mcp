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
import {
    readAdr,
    readBrowse,
    readHealth,
    readIndexJobs,
    readIndexStarted,
    readLogs,
    readProcesses,
} from '../projects/projects-model';
import type {
    AdrRecord,
    BrowseLevel,
    IndexJob,
    IndexStarted,
    LogTail,
    ProcessReport,
    ProjectHealth,
} from '../projects/projects-model';

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

    private getJson(route: string): Promise<unknown> {
        return this.request('GET', route);
    }

    /**
     * One request, any method. The writing routes of the projects panel
     * (POST /api/index, POST /api/adr, DELETE /api/project) go through here
     * with a JSON body; the error carries the status so a caller can tell
     * "busy, try later" (423) from "refused" (403) from "not there" (404).
     */
    private async request(method: 'GET' | 'POST' | 'DELETE', route: string, payload?: unknown): Promise<unknown> {
        const url = `${this.options.base ?? ''}${route}`;
        const doFetch = this.options.fetch ?? globalThis.fetch;
        let response: Response;
        try {
            const init: RequestInit = { method, headers: { Accept: 'application/json' } };
            if (payload !== undefined) {
                init.headers = { Accept: 'application/json', 'Content-Type': 'application/json' };
                init.body = JSON.stringify(payload);
            }
            response = await doFetch(url, init);
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

    // ------------------------------------------------ the projects panel --

    /** The folders under `path`; the server's own roots when `path` is empty. */
    async browse(path = ''): Promise<BrowseLevel> {
        const query = new URLSearchParams();
        if (path.length > 0) {
            query.set('path', path);
        }
        const suffix = path.length > 0 ? `?${query.toString()}` : '';
        return readBrowse(await this.getJson(`/api/browse${suffix}`));
    }

    /** Ask the server to index `rootPath` under `projectName`, in the background. */
    async startIndex(rootPath: string, projectName: string): Promise<IndexStarted> {
        return readIndexStarted(
            await this.request('POST', '/api/index', { root_path: rootPath, project_name: projectName }),
        );
    }

    /** Every index job the server has run since it started, with its state. */
    async indexJobs(): Promise<IndexJob[]> {
        return readIndexJobs(await this.getJson('/api/index-status'));
    }

    /** Remove the index file of a project. The repository itself is not touched. */
    async deleteProject(name: string): Promise<void> {
        await this.request('DELETE', `/api/project?${new URLSearchParams({ name }).toString()}`);
    }

    /** Does the index file open, and what does it hold. */
    async projectHealth(name: string): Promise<ProjectHealth> {
        return readHealth(await this.getJson(`/api/project-health?${new URLSearchParams({ name }).toString()}`));
    }

    /** The decision record the server keeps for a project. */
    async adr(project: string): Promise<AdrRecord> {
        return readAdr(await this.getJson(`/api/adr?${new URLSearchParams({ project }).toString()}`));
    }

    /** Store the decision record of a project. */
    async saveAdr(project: string, content: string): Promise<void> {
        await this.request('POST', '/api/adr', { project, content });
    }

    /** The last `lines` lines of the server log. */
    async logs(lines: number): Promise<LogTail> {
        return readLogs(await this.getJson(`/api/logs?${new URLSearchParams({ lines: String(lines) }).toString()}`));
    }

    /** The codebase-memory-mcp processes on this machine, as the server sees them. */
    async processes(): Promise<ProcessReport> {
        return readProcesses(await this.getJson('/api/processes'));
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

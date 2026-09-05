/**
 * Transport zum C-Server: POST /rpc im MCP-Format, genau wie der MCP-Client.
 *
 * Vorbild ist cbm/graph-ui/src/api/rpc.ts aus PR 1860. Der Unterschied ist die
 * Fehlertaxonomie: dort wird jeder Fehler zu einem RpcError mit Zahlencode,
 * hier traegt der Fehler eine Art. Die Oberflaeche muss "der Server war nicht
 * erreichbar", "das Tool ist auf /rpc gesperrt", "das Tool hat inhaltlich nein
 * gesagt" und "die Antwort hatte eine andere Form als erwartet" verschieden
 * darstellen koennen. Ein einziger Fehlertyp ohne Art zwingt sie, alle vier
 * als denselben roten Kasten zu zeigen, und das waere eine Luege ueber das,
 * was gerade schiefging.
 */

/**
 * Woran der Aufruf gescheitert ist.
 *
 * - `http`: die Antwort kam mit einem Status ausserhalb 2xx. Der Server
 *   antwortet auf gesperrte Tools mit 403 und trotzdem einem JSON-RPC-Body;
 *   dessen Code wird dann mitgetragen.
 * - `rpc`: HTTP war in Ordnung, der Body trug ein `error`-Objekt.
 * - `tool`: das Tool lief und meldete `isError: true` im MCP-Result, so wie
 *   get_code_snippet fuer ein Symbol, das es nicht gibt.
 * - `shape`: die Antwort hatte kein `result.content[0].text`, war also nicht
 *   das, was das Protokoll verspricht.
 */
export type RpcErrorKind = 'http' | 'rpc' | 'tool' | 'shape';

/** JSON-RPC-Fehlercode fuer eine Methode, die der Server nicht anbietet. */
export const RPC_METHOD_NOT_FOUND = -32601;

/** HTTP-Status, mit dem die UI-Allowlist ein gesperrtes Tool abweist. */
export const HTTP_FORBIDDEN = 403;

/** Ein Aufruf, der nicht durchkam, mit der Art des Scheiterns. */
export class RpcError extends Error {
    readonly kind: RpcErrorKind;
    /** Name des Tools, das gerufen wurde. */
    readonly toolName: string;
    /** HTTP-Status, sofern eine Antwort ankam. */
    readonly status?: number;
    /** JSON-RPC-Fehlercode, sofern der Body einen trug. */
    readonly code?: number;
    /**
     * True, wenn der Server das Tool nicht erlaubt (403 oder -32601).
     *
     * Eine eigene Eigenschaft und kein Vergleich an der Aufrufstelle, weil der
     * Server beide Signale gleichzeitig sendet und beide einzeln unvollstaendig
     * sind: 403 ohne Body sagt nichts ueber das Tool, -32601 ohne Status nichts
     * ueber die Allowlist.
     */
    readonly notAllowed: boolean;
    /** Rohtext der Antwort, gekuerzt, fuer die Diagnose in der Oberflaeche. */
    readonly bodyText?: string;

    constructor(
        kind: RpcErrorKind,
        toolName: string,
        message: string,
        details: {
            status?: number;
            code?: number;
            notAllowed?: boolean;
            bodyText?: string;
        } = {},
    ) {
        super(message);
        this.name = 'RpcError';
        this.kind = kind;
        this.toolName = toolName;
        this.status = details.status;
        this.code = details.code;
        this.notAllowed = details.notAllowed ?? false;
        this.bodyText = details.bodyText;
    }
}

/** True, wenn der Fehler daran liegt, dass /rpc dieses Tool nicht anbietet. */
export function isNotAllowed(err: unknown): boolean {
    return err instanceof RpcError && err.notAllowed;
}

/** Ein Textblock im MCP-Result. */
export interface McpContent {
    type?: string;
    text: string;
}

/** Das entpackte MCP-Result eines geglueckten tools/call. */
export interface McpToolResult {
    content: McpContent[];
    isError?: boolean;
}

/** Stellschrauben eines Aufrufs. Alles optional, der Default ist same-origin. */
export interface CallToolOptions {
    /**
     * Ursprung des Servers, ohne Schraegstrich am Ende. Leer heisst same-origin,
     * also der Fall, in dem der C-Server das Frontend selbst ausliefert.
     */
    base?: string;
    /** Abbruchsignal, durchgereicht an fetch. */
    signal?: AbortSignal;
    /** Ersetzbares fetch, damit Tests ohne Netz laufen koennen. */
    fetch?: typeof globalThis.fetch;
}

let nextId = 1;

/** Nur fuer Tests: setzt den Zaehler der Request-Ids zurueck. */
export function resetRequestIds(): void {
    nextId = 1;
}

function truncate(text: string, max = 400): string {
    return text.length > max ? `${text.slice(0, max)}...` : text;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
    return typeof value === 'object' && value !== null && !Array.isArray(value)
        ? (value as Record<string, unknown>)
        : undefined;
}

function readRpcError(
    body: Record<string, unknown> | undefined,
): { code?: number; message?: string } | undefined {
    const err = asRecord(body?.['error']);
    if (err === undefined) {
        return undefined;
    }
    const code = typeof err['code'] === 'number' ? err['code'] : undefined;
    const message = typeof err['message'] === 'string' ? err['message'] : undefined;
    return { code, message };
}

function firstText(result: Record<string, unknown> | undefined): string | undefined {
    const content = result?.['content'];
    if (!Array.isArray(content) || content.length === 0) {
        return undefined;
    }
    const first = asRecord(content[0]);
    const text = first?.['text'];
    return typeof text === 'string' ? text : undefined;
}

/**
 * Ruft ein Tool ueber POST /rpc und liefert das gepruefte MCP-Result.
 *
 * Geprueft heisst: die Antwort trug wirklich `content[0].text`. Der Text selbst
 * bleibt unangetastet, denn nur der Aufrufer weiss, ob dahinter JSON oder das
 * kompakte Zeilenformat steckt.
 */
export async function callTool(
    name: string,
    args: Record<string, unknown> = {},
    opts: CallToolOptions = {},
): Promise<McpToolResult> {
    const base = opts.base ?? '';
    const doFetch = opts.fetch ?? globalThis.fetch;
    const url = `${base}/rpc`;

    const request: RequestInit = {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            jsonrpc: '2.0',
            id: nextId++,
            method: 'tools/call',
            params: { name, arguments: args },
        }),
    };
    if (opts.signal !== undefined) {
        request.signal = opts.signal;
    }

    const res = await doFetch(url, request);
    const raw = await res.text();

    let body: Record<string, unknown> | undefined;
    let parseFailed = false;
    try {
        body = asRecord(JSON.parse(raw));
        if (body === undefined) {
            parseFailed = true;
        }
    } catch {
        parseFailed = true;
    }

    if (!res.ok) {
        const rpcError = readRpcError(body);
        const notAllowed =
            res.status === HTTP_FORBIDDEN || rpcError?.code === RPC_METHOD_NOT_FOUND;
        const detail = rpcError?.message ?? truncate(raw);
        throw new RpcError(
            'http',
            name,
            `/rpc ${name}: HTTP ${res.status}${detail.length > 0 ? ` (${detail})` : ''}`,
            {
                status: res.status,
                ...(rpcError?.code !== undefined ? { code: rpcError.code } : {}),
                notAllowed,
                bodyText: truncate(raw),
            },
        );
    }

    if (parseFailed || body === undefined) {
        throw new RpcError('shape', name, `/rpc ${name}: Antwort war kein JSON-Objekt`, {
            status: res.status,
            bodyText: truncate(raw),
        });
    }

    const rpcError = readRpcError(body);
    if (rpcError !== undefined) {
        throw new RpcError(
            'rpc',
            name,
            `/rpc ${name}: ${rpcError.message ?? 'Fehler ohne Meldung'}`,
            {
                status: res.status,
                ...(rpcError.code !== undefined ? { code: rpcError.code } : {}),
                notAllowed: rpcError.code === RPC_METHOD_NOT_FOUND,
                bodyText: truncate(raw),
            },
        );
    }

    const result = asRecord(body['result']);
    const text = firstText(result);
    if (result === undefined || text === undefined) {
        throw new RpcError(
            'shape',
            name,
            `/rpc ${name}: Antwort ohne result.content[0].text`,
            { status: res.status, bodyText: truncate(raw) },
        );
    }

    if (result['isError'] === true) {
        throw new RpcError('tool', name, `/rpc ${name}: ${truncate(text)}`, {
            status: res.status,
            bodyText: truncate(raw),
        });
    }

    const content = (result['content'] as unknown[]).map((entry) => {
        const record = asRecord(entry) ?? {};
        const value = record['text'];
        const item: McpContent = { text: typeof value === 'string' ? value : '' };
        if (typeof record['type'] === 'string') {
            item.type = record['type'];
        }
        return item;
    });

    const out: McpToolResult = { content };
    if (typeof result['isError'] === 'boolean') {
        out.isError = result['isError'];
    }
    return out;
}

/** Liefert `content[0].text` roh, ohne jede Deutung. */
export async function callToolText(
    name: string,
    args: Record<string, unknown> = {},
    opts: CallToolOptions = {},
): Promise<string> {
    const result = await callTool(name, args, opts);
    return result.content[0].text;
}

/** Liefert `content[0].text` als JSON. Kein JSON heisst Formfehler. */
export async function callToolJson<T = unknown>(
    name: string,
    args: Record<string, unknown> = {},
    opts: CallToolOptions = {},
): Promise<T> {
    const text = await callToolText(name, args, opts);
    try {
        return JSON.parse(text) as T;
    } catch (err) {
        throw new RpcError(
            'shape',
            name,
            `/rpc ${name}: Antworttext war kein JSON (${(err as Error).message})`,
            { bodyText: truncate(text) },
        );
    }
}

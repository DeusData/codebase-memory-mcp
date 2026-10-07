/**
 * Aufgezeichnete Antworten des gebauten Servers, und ein fetch, das sie spielt.
 *
 * Vorbild ist test/support/recordings.ts aus CodeAtlasIDE: dort spielt ein
 * FakeTransport aufgezeichnete CLI-Antworten, hier spielt ein fetch
 * aufgezeichnete /rpc-Antworten. Der Unterschied ist der Punkt des Zyklus.
 * Die Texte unten sind woertlich das, was
 * cbm/build/c/codebase-memory-mcp am 2026-08-28 auf POST /rpc geantwortet hat,
 * indiziert war fixtures/atlas-sample als Projekt `codeatlasweb-probe`.
 *
 * Woertlich heisst woertlich: der Kopf hat zwei Leerzeichen vor "(cols:",
 * Zahlenspalten kommen gequotet, ein nicht gesetzter Wert ist ein nackter
 * Bindestrich, und der Leer-Fall traegt einen hint statt einer leeren Liste.
 * Eine geglaettete Aufzeichnung waere keine Aufzeichnung mehr, sondern eine
 * Behauptung ueber den Server, und genau die soll hier gepruefte Tatsache
 * bleiben.
 *
 * Zwei Antworten sind gebaut statt aufgezeichnet und sagen es an Ort und
 * Stelle: die Java-THROWS-Zeile, weil das Fixture TypeScript ist und gar keine
 * deklarierte Ausnahme enthaelt, und die Ablehnung der Allowlist, die zwar
 * aufgezeichnet ist, aber zu einem Werkzeug gehoert, das die Tests hier nicht
 * ueber ein Projekt rufen.
 */

/** Projektname, unter dem das Fixture fuer diese Aufzeichnungen indiziert war. */
export const RECORDED_PROJECT = 'codeatlasweb-probe';

/** Workspace-Wurzel, wie sie in der aufgezeichneten Projektliste steht. */
export const RECORDED_ROOT = '/Users/bernhard/Desktop/CodeAtlasWeb/fixtures/atlas-sample';

const lines = (...parts: string[]): string => parts.join('\n') + '\n';

const EMPTY_HINT =
    'hint: "Query returned no results. Use get_graph_schema() to see available labels and edge types."';

/** Aufgezeichnete Antworten, benannt nach der Frage, die sie beantwortet haben. */
export const RECORDINGS = {
    /** CALLS aus createUser heraus, sechs Ziele, nach Site-Zeile geordnet. */
    'calls-out': lines(
        'rows: 6  (cols: b.name b.qualified_name b.file_path b.start_line r.line)',
        '  validateUser codeatlasweb-probe.src.util.validate.validateUser src/util/validate.ts "19" "24"',
        '  ValidationError codeatlasweb-probe.src.util.validate.ValidationError src/util/validate.ts "4" "27"',
        '  UserEntity codeatlasweb-probe.src.types.UserEntity src/types.ts "37" "29"',
        '  listUsers codeatlasweb-probe.src.services.userService.listUsers src/services/userService.ts "18" "29"',
        '  insert codeatlasweb-probe.src.repo.db.insert src/repo/db.ts "31" "30"',
        '  toUser codeatlasweb-probe.src.services.userService.toUser src/services/userService.ts "9" "35"',
        'total: 6',
    ),

    /** Dieselben CALLS, auf Klassen eingeschraenkt: das trennt `new Foo()` von `foo()`. */
    'class-targets': lines(
        'rows: 2  (cols: b.name b.qualified_name)',
        '  ValidationError codeatlasweb-probe.src.util.validate.ValidationError',
        '  UserEntity codeatlasweb-probe.src.types.UserEntity',
        'total: 2',
    ),

    /** CALLS in listUsers hinein: drei Aufrufer, einer davon Testcode. */
    'calls-in': lines(
        'rows: 3  (cols: a.name a.qualified_name a.file_path a.start_line a.is_test r.line)',
        '  test/userService.test.ts codeatlasweb-probe.test.userService.test test/userService.test.ts "1" "true" "9"',
        '  registerUserRoutes codeatlasweb-probe.src.routes.users.registerUserRoutes src/routes/users.ts "7" "false" "10"',
        '  createUser codeatlasweb-probe.src.services.userService.createUser src/services/userService.ts "23" "false" "29"',
        'total: 3',
    ),

    /** RAISES aus createUser: ein Typ, und die Site-Zeile hat der Server nicht. */
    raises: lines(
        'rows: 1  (cols: b.name b.file_path b.start_line r.line)',
        '  ValidationError src/util/validate.ts "4" -',
        'total: 1',
    ),

    /** THROWS aus createUser: fuer TypeScript schreibt die Analyse diese Relation nicht. */
    'throws-empty': lines(
        'rows: 0  (cols: b.name b.file_path b.start_line r.line)',
        'total: 0',
        EMPTY_HINT,
    ),

    /** CONFIGURES aus createUser: DB_URL, ohne Datei und ohne Zeile. */
    'env-reads': lines(
        'rows: 1  (cols: b.name b.env_key b.file_path r.line r.strategy)',
        '  DB_URL DB_URL - - env_access',
        'total: 1',
    ),

    /** USAGE aus createUser: der Typ User. */
    'type-refs': lines(
        'rows: 1  (cols: b.name b.qualified_name b.file_path b.start_line r.line)',
        '  User codeatlasweb-probe.src.types.User src/types.ts "27" -',
        'total: 1',
    ),

    /** Method-Kandidaten fuer src/types.ts an Zeile 47: der Konstruktor. */
    'enclosing-method': lines(
        'rows: 1  (cols: n.name n.qualified_name n.start_line n.end_line)',
        '  constructor codeatlasweb-probe.src.types.UserEntity.constructor "43" "48"',
        'total: 1',
    ),

    /** Class-Kandidaten fuer dieselbe Stelle: die umschliessende Klasse. */
    'enclosing-class': lines(
        'rows: 1  (cols: n.name n.qualified_name n.start_line n.end_line)',
        '  UserEntity codeatlasweb-probe.src.types.UserEntity "37" "53"',
        'total: 1',
    ),

    /** Kandidaten eines Labels, das an dieser Stelle nichts hat. */
    'enclosing-empty': lines(
        'rows: 0  (cols: n.name n.qualified_name n.start_line n.end_line)',
        'total: 0',
        EMPTY_HINT,
    ),

    /** Der Dateiknoten von src/types.ts: die Datei ist indiziert. */
    'file-exists': lines(
        'rows: 1  (cols: n.name n.file_path)',
        '  types.ts src/types.ts',
        'total: 1',
    ),

    /** Kein Dateiknoten: die Datei kennt der Index nicht. */
    'file-missing': lines(
        'rows: 0  (cols: n.name n.file_path)',
        'total: 0',
        EMPTY_HINT,
    ),

    /** search_graph mit BM25-Anfrage: die flache Suchform mit Rang. */
    search: lines(
        'total: 1',
        'search_mode: bm25',
        'results: 1  (cols: qn label file lines rank)',
        '  codeatlasweb-probe.src.services.userService.createUser Function src/services/userService.ts 23-36 -15.21',
        'has_more: false',
    ),
} as const;

/** Name einer Aufzeichnung. */
export type RecordingName = keyof typeof RECORDINGS;

/**
 * Eine Antwort des Servers, die keine Aufzeichnung ist.
 *
 * `text` ist der Inhalt von content[0].text, `json` wird dorthin serialisiert.
 * `toolError` ist die Form, in der ein Werkzeug inhaltlich nein sagt.
 * `notAllowed` ist die woertliche Ablehnung der Read-only-Allowlist, und
 * `networkError` ist der Fall, in dem gar niemand antwortet.
 */
export interface Route {
    tool: string;
    when?: (args: Record<string, unknown>) => boolean;
    recording?: RecordingName;
    text?: string;
    json?: unknown;
    toolError?: string;
    notAllowed?: boolean;
    networkError?: string;
}

/** Passt auf eine Abfrage, die ein Stueck Cypher enthaelt. */
export function queryContains(fragment: string): (args: Record<string, unknown>) => boolean {
    return (args) => typeof args['query'] === 'string' && args['query'].includes(fragment);
}

/** Die aufgezeichnete Projektliste, in der das Fixture unter ROOT steht. */
export function listProjectsRoute(root = RECORDED_ROOT, name = RECORDED_PROJECT): Route {
    return {
        tool: 'list_projects',
        json: { projects: [{ name, root_path: root }], total: 1, offset: 0, limit: 50, returned: 1, has_more: false },
    };
}

/** Eine gebaute Zeilenantwort, fuer die Faelle, die das Fixture nicht hergibt. */
export function rowsText(columns: string[], rows: string[][], hint?: string): string {
    const body = rows.map((cells) => `  ${cells.join(' ')}`);
    const out = [`rows: ${rows.length}  (cols: ${columns.join(' ')})`, ...body, `total: ${rows.length}`];
    if (hint !== undefined) {
        out.push(`hint: "${hint}"`);
    }
    return out.join('\n') + '\n';
}

const ALLOWLIST_REFUSAL =
    '{"jsonrpc":"2.0","error":{"code":-32601,"message":"UI RPC method is not allowed"},"id":null}';

/**
 * Ein fetch, das die Routen spielt und mitschreibt, wonach gefragt wurde.
 *
 * Der Transport darunter ist der echte: die Tests laufen durch rpc-transport
 * und rpc-client hindurch, nicht daran vorbei. Was ersetzt ist, ist genau die
 * Leitung. Eine Anfrage, fuer die keine Route passt, wird zu einem Fehler mit
 * Werkzeug und Argumenten im Text: ein Test, der eine Route vergessen hat,
 * soll das erfahren und nicht in einem leeren Ergebnis versanden.
 */
export class FakeRpc {

    /** Jeder Aufruf, in der Reihenfolge, in der er kam. */
    readonly calls: { tool: string; args: Record<string, unknown> }[] = [];

    constructor(private readonly routes: Route[]) { }

    /** Die Werkzeuge, die gerufen wurden, in Reihenfolge. */
    toolsCalled(): string[] {
        return this.calls.map((call) => call.tool);
    }

    /** Aufrufe eines Werkzeugs. */
    callsTo(tool: string): { tool: string; args: Record<string, unknown> }[] {
        return this.calls.filter((call) => call.tool === tool);
    }

    readonly fetch: typeof globalThis.fetch = async (_input, init) => {
        const body = JSON.parse(String(init?.body ?? '{}')) as {
            params?: { name?: string; arguments?: Record<string, unknown> };
            id?: number;
        };
        const tool = body.params?.name ?? '';
        const args = body.params?.arguments ?? {};
        this.calls.push({ tool, args });

        const route = this.routes.find(
            (candidate) => candidate.tool === tool && (candidate.when === undefined || candidate.when(args)),
        );
        if (route === undefined) {
            throw new Error(
                `FakeRpc hat keine Route fuer ${tool} mit ${JSON.stringify(args)}. ` +
                    'Die fehlende Route ist der Befund, nicht ein leeres Ergebnis.',
            );
        }
        if (route.networkError !== undefined) {
            throw new TypeError(route.networkError);
        }
        if (route.notAllowed === true) {
            return new Response(ALLOWLIST_REFUSAL, {
                status: 403,
                headers: { 'Content-Type': 'application/json' },
            });
        }

        const text = route.recording !== undefined
            ? RECORDINGS[route.recording]
            : route.text ?? (route.json !== undefined ? JSON.stringify(route.json) : route.toolError ?? '');
        const result: Record<string, unknown> = {
            content: [{ type: 'text', text }],
            isError: route.toolError !== undefined,
        };
        return new Response(JSON.stringify({ jsonrpc: '2.0', id: body.id ?? 1, result }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
        });
    };
}

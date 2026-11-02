/**
 * Die Antwortformen von /api/trace, /api/flows und /api/flow, gelesen statt
 * angenommen.
 *
 * Dieselbe Rolle wie src/provider/rpc-schemas.ts fuer die Werkzeuge: hier wird
 * gepruefte Form aus unbekanntem JSON, und nichts weiter. Kein Produktvokabular,
 * kein Urteil darueber, was ein Ergebnis bedeutet. Was die Felder heissen,
 * heissen sie im C-Quelltext des geclonten Servers; die Uebersetzung in die
 * Sprache dieses Projekts passiert eine Schicht hoeher.
 *
 * Alle drei Formen stammen aus cbm/src/ui/atlas_flows.c
 * (`cbm_atlas_trace_json`, `cbm_atlas_flows_json`, `cbm_atlas_flow_json`) und
 * cbm/src/ui/http_server.c (`cbm_atlas_attach_observed`, das jeder Antwort ihre
 * `observed`-Objekte anhaengt).
 *
 * Zwei Eigenheiten, die man sonst raten muesste:
 *
 * 1. **Ein Knoten traegt keinen qualifizierten Namen.** Er traegt `id`, `name`
 *    und meistens `file_path`. Der Index wird deshalb ueber das Paar aus Datei
 *    und Namen wieder angesprochen und nicht ueber eine Kennung, die es in der
 *    Antwort nicht gibt.
 * 2. **`observed` steht am Ziel-Knoten, nicht an einer Kante.** Der Server
 *    heftet es an den Schritt und meint damit die Kante von dessen Vorgaenger
 *    (`path[i-1]` beim Trace, `steps[step.parent]` beim Flow). Wer es als
 *    Eigenschaft des Knotens liest, schreibt eine Beobachtung dem falschen
 *    Aufruf zu.
 */

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

const text = (value: unknown): string => (typeof value === 'string' ? value : '');

const num = (value: unknown): number | undefined =>
    typeof value === 'number' && Number.isFinite(value) ? value : undefined;

/** Was der Server ueber eine beobachtete Kante weiss. Alle drei Felder kommen zusammen. */
export interface ObservedRecord {
    /** Wie oft die Kante gemeldet wurde. Der Server liefert nur Werte ueber null. */
    count: number;
    /** Der Lauf, unter dem sie gemeldet wurde. `unlabeled`, wenn keiner genannt war. */
    label: string;
    /** Zeitstempel der letzten Meldung, so wie der Server ihn schreibt. */
    lastSeen: string;
}

/** Ein Knoten einer Trace- oder Flow-Antwort. */
export interface TraceNode {
    /** Die Knoten-Id des Speichers. Hier nur zur Unterscheidung gleicher Namen. */
    id?: number;
    name: string;
    filePath?: string;
    /** Die Beobachtung der Kante, die zu diesem Knoten fuehrt. */
    observed?: ObservedRecord;
}

/** Was /api/trace antwortet. `path` fehlt, wenn nichts erreichbar war. */
export interface TraceAnswer {
    mode: string;
    maxDepth?: number;
    reachable: boolean;
    /** Warum nicht, wenn der Server einen Grund nennt. */
    error?: string;
    /** Wie viele Kanten die Suche angefasst hat. */
    explored?: number;
    path: TraceNode[];
    hops?: number;
}

/** Eine Zeile von /api/flows. */
export interface FlowSummary {
    id: number;
    label: string;
    entry: TraceNode;
    terminal: TraceNode;
    steps: number;
}

/** Ein Schritt von /api/flow. `parent` ist ein Index in dieselbe Liste, -1 an der Wurzel. */
export interface FlowStep extends TraceNode {
    depth: number;
    parent: number;
}

export interface FlowDetail {
    id: number;
    entry: TraceNode;
    terminal: TraceNode;
    steps: FlowStep[];
}

function readObserved(value: unknown): ObservedRecord | undefined {
    if (!isRecord(value)) {
        return undefined;
    }
    const count = num(value['count']);
    if (count === undefined || count <= 0) {
        // Der Server heftet nichts an, was er nicht gezaehlt hat. Eine Null
        // waere hier eine Behauptung ueber einen Aufruf, den niemand gesehen
        // hat, und die Abwesenheit sagt genau das schon.
        return undefined;
    }
    return { count, label: text(value['label']), lastSeen: text(value['last_seen']) };
}

function readNode(value: unknown): TraceNode {
    const record = isRecord(value) ? value : {};
    const node: TraceNode = { name: text(record['name']) };
    const id = num(record['id']);
    if (id !== undefined) {
        node.id = id;
    }
    const filePath = text(record['file_path']);
    if (filePath.length > 0) {
        node.filePath = filePath;
    }
    const observed = readObserved(record['observed']);
    if (observed !== undefined) {
        node.observed = observed;
    }
    return node;
}

export function readTraceAnswer(raw: unknown): TraceAnswer {
    const record = isRecord(raw) ? raw : {};
    const path = Array.isArray(record['path']) ? record['path'].map(readNode) : [];
    const answer: TraceAnswer = {
        mode: text(record['mode']),
        reachable: record['reachable'] === true,
        path,
    };
    const maxDepth = num(record['max_depth']);
    if (maxDepth !== undefined) {
        answer.maxDepth = maxDepth;
    }
    const explored = num(record['explored']);
    if (explored !== undefined) {
        answer.explored = explored;
    }
    const hops = num(record['hops']);
    if (hops !== undefined) {
        answer.hops = hops;
    }
    const error = text(record['error']);
    if (error.length > 0) {
        answer.error = error;
    }
    return answer;
}

export function readFlowSummaries(raw: unknown): FlowSummary[] {
    const record = isRecord(raw) ? raw : {};
    const flows = Array.isArray(record['flows']) ? record['flows'] : [];
    const out: FlowSummary[] = [];
    for (const entry of flows) {
        if (!isRecord(entry)) {
            continue;
        }
        const id = num(entry['id']);
        if (id === undefined) {
            continue;
        }
        out.push({
            id,
            label: text(entry['label']),
            entry: readNode(entry['entry']),
            terminal: readNode(entry['terminal']),
            steps: num(entry['steps']) ?? 0,
        });
    }
    return out;
}

export function readFlowDetail(raw: unknown): FlowDetail {
    const record = isRecord(raw) ? raw : {};
    const rawSteps = Array.isArray(record['steps']) ? record['steps'] : [];
    const steps: FlowStep[] = rawSteps.map((value) => {
        const inner = isRecord(value) ? value : {};
        return {
            ...readNode(value),
            depth: num(inner['depth']) ?? 0,
            parent: num(inner['parent']) ?? -1,
        };
    });
    return {
        id: num(record['id']) ?? -1,
        entry: readNode(record['entry']),
        terminal: readNode(record['terminal']),
        steps,
    };
}

/**
 * Der Zustand des lokalen Modell-Sidecars, und was die Oberflaeche darueber
 * sagen darf.
 *
 * Der Sidecar ist ein eigener Prozess: llama-server aus vendor/llama, gestartet
 * ueber llm/start.sh, gebunden an 127.0.0.1:4141. Diese Datei startet nichts und
 * kann nichts starten. Sie fragt und sie deutet, und das ist die ganze
 * Architektur-Grenze aus ADR 0001: eine SPA ohne eigenes Backend darf keinen
 * Prozess versprechen, den sie nicht anfassen kann.
 *
 * Drei Entscheidungen, die man sonst raten muesste:
 *
 * 1. **Aus heisst aus, nicht "leise".** Solange die Praeferenz aus ist oder eine
 *    Policy das LLM verbietet, faellt kein einziger fetch Richtung 4141. Kein
 *    Aufwaerm-Ping, keine "gibt es dich"-Probe beim Laden. Der Beweislauf misst
 *    genau diese Null am Netz-Mitschnitt, und ein einzelner Ping waere der
 *    Unterschied zwischen einem Opt-out und einem Schalter, der nur die Anzeige
 *    umfaerbt.
 * 2. **Kein Prozess ist kein Fehler.** `not-running` ist der Normalfall nach dem
 *    Einschalten, nicht ein Ausfall: der Leser hat das Skript noch nicht
 *    gefahren. Die Oberflaeche nennt deshalb den konkreten Aufruf, statt eine
 *    rote Meldung zu zeigen, die nach einem Defekt aussieht.
 * 3. **Die Zahlen kommen aus dem laufenden Prozess, nicht aus einer Tabelle.**
 *    Modell, Quantisierung, Kontext und Slots stehen in `/props`; die Groesse
 *    der geladenen Gewichte steht dort NICHT und wird darum aus `/v1/models`
 *    (`meta.size`) geholt und als "weights" benannt, nicht als
 *    Prozessspeicher. Eine im Code gepflegte Modellgroessen-Tabelle waere eine
 *    zweite Wahrheit neben dem Prozess, der die Datei wirklich geladen hat.
 *
 * ## Zwei Betriebsarten, und woran man sie unterscheidet (W10)
 *
 * llama-server kann als ROUTER ueber ein Cache-Verzeichnis laufen
 * (`--models-dir`) statt mit einer festen Datei (`-m`). Nur im Router-Modus
 * waehlt das Feld `model` einer Chat-Anfrage wirklich aus; ein Einzel-Server
 * ignoriert ein fremdes `model` STILLSCHWEIGEND und antwortet mit dem Modell,
 * das er ohnehin geladen hat (gemessen am 2026-08-29 an vendor/llama, Version
 * 0.3.0-dev, build b1-90c26fc: die Anfrage mit `"model":"SomeOtherModel"` kam
 * mit `"model":"models/MiniCPM5-1B-Q4_K_M.gguf"` und einem normalen Text
 * zurueck, ohne Fehler und ohne Warnung).
 *
 * Genau darum wird die Betriebsart gelesen und nicht angenommen: eine Auswahl
 * anzubieten, die stillschweigend nichts tut, waere die teuerste Sorte von
 * Bedienelement. Das Merkmal ist `props.role === 'router'`; im Einzel-Modus
 * fehlt das Feld ganz.
 *
 * Im Router-Modus sind die Zahlen in `/props` NICHT die des Modells: dort steht
 * `model_path: "none"` und `n_ctx: 0`, weil der Router selbst keins geladen hat.
 * Die Zahlen einer Instanz stehen in `/props?model=<id>`, und genau die werden
 * dann geholt. Ohne diesen zweiten Weg wuerde die Statusleiste "ready: none"
 * behaupten, waehrend ein Modell antwortet.
 */

/** Der Port des Sidecars. Fest, weil llm/start.sh ihn fest bindet. */
export const SIDECAR_PORT = 4141;

/** Der Ursprung, gegen den gefragt wird. Loopback und nichts sonst. */
export const SIDECAR_ORIGIN = `http://127.0.0.1:${SIDECAR_PORT}`;

/**
 * Der Abstand zwischen zwei Proben, solange das LLM an ist.
 *
 * Drei Sekunden sind der Kompromiss aus zwei Groessen, die beide gemessen sind:
 * ein 4B-Modell braucht auf dieser Maschine rund vier Sekunden bis
 * `{"status":"ok"}`, und ein Leser, der eben `llm/start.sh` getippt hat, soll
 * die Umschaltung sehen, ohne auf die Anzeige zu warten. Haeufiger zu fragen
 * kostet einen Prozess, der ohnehin gerade ein Modell laedt, nur Aufmerksamkeit.
 */
export const SIDECAR_POLL_MS = 3000;

/**
 * Die fuenf Lagen, in denen der Sidecar sein kann.
 *
 * `off` und `disabled-by-policy` sehen im Ergebnis gleich aus und sind es
 * nicht: das erste ist die Entscheidung des Lesers und mit einem Klick
 * umkehrbar, das zweite ist die Entscheidung des Projekts und mit keinem.
 */
export type SidecarState = 'off' | 'disabled-by-policy' | 'not-running' | 'starting' | 'ready';

/** Was ueber einen laufenden Sidecar bekannt ist. Alles davon kommt aus ihm selbst. */
export interface SidecarFacts {
    /** Der Anzeigename, aus dem Dateinamen gekuerzt. Zum Beispiel `Qwen3.5-2B`. */
    model: string;
    /** Der Pfad, den der Prozess selbst nennt. Ungekuerzt. */
    modelPath: string;
    /** `Q4_K - Medium`, so wie der Prozess es schreibt. Leer, wenn er nichts sagt. */
    quantization: string;
    /** Das Kontextfenster dieses Laufs, in Token. */
    contextTokens?: number;
    /** Wofuer das Modell trainiert wurde, in Token. Nur zum Vergleich. */
    trainedContextTokens?: number;
    /** Wie viele Anfragen der Prozess parallel bedienen kann. */
    slots?: number;
    /** Die geladenen Gewichte in Byte, aus `/v1/models`. */
    weightsBytes?: number;
    /** Die Parameterzahl, aus `/v1/models`. */
    parameters?: number;
}

/**
 * Ein Modell, das der Prozess in seinem Cache-Verzeichnis fuehrt.
 *
 * Was hier steht, kommt aus `/v1/models` und aus nichts sonst. `meta` liefert
 * der Server NUR fuer geladene Instanzen, also bleiben die Zahlen bei einem
 * ungeladenen Modell leer. Sie aus dem Dateinamen zu schaetzen waere eine
 * Angabe ueber eine Datei, die niemand geoeffnet hat.
 */
export interface CacheModel {
    /** Die Kennung, mit der eine Anfrage dieses Modell waehlt. */
    id: string;
    /** Der Anzeigename, aus der Kennung gekuerzt. */
    name: string;
    /** Ob gerade eine Instanz davon laeuft. */
    loaded: boolean;
    /** Was der Prozess ueber die Lage sagt (`loaded`, `unloaded`). Leer, wenn nichts. */
    status: string;
    /** Die Datei in Byte. Nur bei geladenen Instanzen. */
    weightsBytes?: number;
    /** Die Parameterzahl. Nur bei geladenen Instanzen. */
    parameters?: number;
    /** Wofuer trainiert wurde, in Token. Nur bei geladenen Instanzen. */
    trainedContextTokens?: number;
    /** Das Kontextfenster dieser Instanz. Nur bei geladenen Instanzen. */
    contextTokens?: number;
    /** Die Quantisierung, so wie der Prozess sie schreibt. */
    quantization?: string;
}

/** Eine Probe: die Lage plus, wenn es eine gibt, die Auskunft des Prozesses. */
export interface SidecarReading {
    state: SidecarState;
    facts?: SidecarFacts;
    /**
     * Ob der Prozess als Router ueber ein Cache-Verzeichnis laeuft.
     *
     * Die eine Angabe, an der haengt, ob eine Modellauswahl ueberhaupt wirkt.
     * Siehe den Kopf dieser Datei.
     */
    router: boolean;
    /** Was `/v1/models` gelistet hat. Leer, solange niemand geantwortet hat. */
    models: CacheModel[];
    /** Was schiefging, wenn etwas schiefging. Leer im Normalfall. */
    detail: string;
}

/** Nur das, was diese Datei von `fetch` braucht, damit ein Test es ersetzen kann. */
export type FetchLike = (input: string, init?: { signal?: AbortSignal }) => Promise<Response>;

/** Was der Aufrufer der Probe mitgeben kann. */
export interface ProbeOptions {
    /**
     * Die Modellwahl des Lesers, wenn er eine getroffen hat.
     *
     * Sie wirkt NUR im Router-Modus, und zwar an genau zwei Stellen: die Karte
     * zeigt dann die Zahlen dieses Modells, und die Statusleiste nennt seinen
     * Namen. Ob eine Chat-Anfrage sie mitschickt, entscheidet der Aufrufer und
     * nicht diese Datei.
     */
    model?: string;
}

// ------------------------------------------------------------------ Namen ---

/**
 * Der Anzeigename eines Modells, aus seinem Dateinamen.
 *
 * `models/Qwen3.5-2B-Q4_K_M.gguf` wird zu `Qwen3.5-2B`. Gekuerzt wird nur, was
 * nachweislich die Quantisierung benennt; alles andere bleibt stehen. Ein
 * Kuerzungsversuch, der raet, wuerde aus einem Modellnamen frueher oder spaeter
 * einen anderen machen, und der Name ist das, was der Leser gegen das ADR haelt.
 */
export function modelDisplayName(modelPath: string): string {
    const base = modelPath.split('/').pop() ?? modelPath;
    const withoutExtension = base.replace(/\.gguf$/i, '');
    const withoutQuant = withoutExtension.replace(/[-_.](?:Q\d+_[A-Z0-9_]+|F16|F32|BF16)$/i, '');
    return withoutQuant.length > 0 ? withoutQuant : base;
}

/** Byte in eine Zeile, die ein Mensch liest. Basis 1024, wie jeder Modell-Downloader. */
export function humanBytes(bytes: number): string {
    if (!Number.isFinite(bytes) || bytes <= 0) {
        return '';
    }
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let value = bytes;
    let unit = 0;
    while (value >= 1024 && unit < units.length - 1) {
        value /= 1024;
        unit += 1;
    }
    return `${value >= 10 || unit === 0 ? Math.round(value) : value.toFixed(2)} ${units[unit]}`;
}

// -------------------------------------------------------------- Die Probe ---

function asRecord(value: unknown): Record<string, unknown> {
    return typeof value === 'object' && value !== null ? (value as Record<string, unknown>) : {};
}

function asNumber(value: unknown): number | undefined {
    return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

function asString(value: unknown): string {
    return typeof value === 'string' ? value : '';
}

/**
 * Die Auskunft von `/props` lesen.
 *
 * Der Prozess antwortet mit weit mehr, als hier gebraucht wird (unter anderem
 * dem ganzen Chat-Template). Gelesen wird nur, was die Oberflaeche zeigt, und
 * jedes Feld einzeln: eine Antwort, der ein Feld fehlt, ist kein Grund, die
 * anderen wegzuwerfen.
 */
export function readProps(payload: unknown): SidecarFacts {
    const props = asRecord(payload);
    const modelPath = asString(props['model_path']) || asString(props['model_alias']);
    const settings = asRecord(props['default_generation_settings']);
    const facts: SidecarFacts = {
        model: modelDisplayName(modelPath),
        modelPath,
        quantization: asString(props['model_ftype']),
    };
    const contextTokens = asNumber(settings['n_ctx']);
    if (contextTokens !== undefined) {
        facts.contextTokens = contextTokens;
    }
    const slots = asNumber(props['total_slots']);
    if (slots !== undefined) {
        facts.slots = slots;
    }
    return facts;
}

/**
 * Die Betriebsart aus `/props` lesen.
 *
 * Genau ein Feld, und die Abwesenheit ist die Auskunft: ein Einzel-Server
 * schreibt `role` nicht. Siehe den Kopf dieser Datei.
 */
export function readsAsRouter(payload: unknown): boolean {
    return asString(asRecord(payload)['role']) === 'router';
}

/**
 * Die Liste aus `/v1/models`.
 *
 * Der Server antwortet im Router-Modus mit einem Eintrag je Datei des
 * Cache-Verzeichnisses und im Einzel-Modus mit genau einem Eintrag, dessen `id`
 * der Modellpfad ist. Beide Formen werden gleich gelesen: eine Liste mit einem
 * Eintrag ist eine Liste.
 */
export function readModelList(payload: unknown): CacheModel[] {
    const data = asRecord(payload)['data'];
    if (!Array.isArray(data)) {
        return [];
    }
    const out: CacheModel[] = [];
    for (const entry of data) {
        const record = asRecord(entry);
        const id = asString(record['id']);
        if (id.length === 0) {
            continue;
        }
        const status = asRecord(record['status']);
        const stateWord = asString(status['value']);
        const meta = asRecord(record['meta']);
        const model: CacheModel = {
            id,
            name: modelDisplayName(id),
            // Ein Eintrag ohne `status` kommt aus dem Einzel-Modus, wo genau
            // dieses eine Modell geladen IST. Ein `meta` gibt es nur fuer
            // geladene Instanzen, also ist es hier das ehrlichere Merkmal.
            loaded: stateWord.length > 0 ? stateWord === 'loaded' : Object.keys(meta).length > 0,
            status: stateWord,
        };
        const size = asNumber(meta['size']);
        if (size !== undefined) {
            model.weightsBytes = size;
        }
        const params = asNumber(meta['n_params']);
        if (params !== undefined) {
            model.parameters = params;
        }
        const trained = asNumber(meta['n_ctx_train']);
        if (trained !== undefined) {
            model.trainedContextTokens = trained;
        }
        const context = asNumber(meta['n_ctx']);
        if (context !== undefined) {
            model.contextTokens = context;
        }
        const ftype = asString(meta['ftype']);
        if (ftype.length > 0) {
            model.quantization = ftype;
        }
        out.push(model);
    }
    return out;
}

/**
 * Welches Modell die Karte zeigt, wenn der Router mehrere fuehrt.
 *
 * Die Wahl des Lesers, wenn sie noch in der Liste steht; sonst das Modell, das
 * gerade geladen ist; sonst das erste. Der mittlere Fall ist der wichtige: er
 * ist die Lage nach einem Neustart des Sidecars, in der die gespeicherte Wahl
 * auf eine Datei zeigt, die aus dem Verzeichnis verschwunden ist. Eine Karte
 * mit den Zahlen eines Modells, das es nicht mehr gibt, waere schlimmer als
 * eine Karte mit denen des laufenden.
 */
export function activeModelOf(models: readonly CacheModel[], selected: string): CacheModel | undefined {
    if (models.length === 0) {
        return undefined;
    }
    return models.find((model) => model.id === selected)
        ?? models.find((model) => model.loaded)
        ?? models[0];
}

/**
 * Die Groessenangaben aus `/v1/models` dazulegen.
 *
 * Ein zweites Werkzeug fuer zwei Zahlen, und das ist kein Versehen: `/props`
 * fuehrt keine Speicherangabe. Die Alternative waere, die Dateigroesse aus einer
 * Tabelle im Quelltext zu nehmen, und die stuende neben einem Prozess, der die
 * Datei wirklich geladen hat. Schlaegt die Abfrage fehl, bleiben die beiden
 * Felder leer und der Rest der Karte steht trotzdem.
 */
export function readModelMeta(payload: unknown): { weightsBytes?: number; parameters?: number; trainedContextTokens?: number } {
    const data = asRecord(payload)['data'];
    const first = Array.isArray(data) ? asRecord(data[0]) : {};
    const meta = asRecord(first['meta']);
    const out: { weightsBytes?: number; parameters?: number; trainedContextTokens?: number } = {};
    const size = asNumber(meta['size']);
    if (size !== undefined) {
        out.weightsBytes = size;
    }
    const params = asNumber(meta['n_params']);
    if (params !== undefined) {
        out.parameters = params;
    }
    const trained = asNumber(meta['n_ctx_train']);
    if (trained !== undefined) {
        out.trainedContextTokens = trained;
    }
    return out;
}

/**
 * Einmal fragen, wie es dem Sidecar geht.
 *
 * Der Weg ist genau der, den der Prozess anbietet: `/health` antwortet mit 200
 * und `{"status":"ok"}`, sobald das Modell geladen ist, und waehrend des Ladens
 * mit 503 und `Loading model` (beides an llama-server b10675 gemessen,
 * verification/w5/sidecar.json). Eine abgelehnte Verbindung ist kein Fehler,
 * sondern die Auskunft "da laeuft nichts".
 *
 * Diese Funktion wird NUR gerufen, wenn das LLM an ist. Der Aufrufer entscheidet
 * das; hier steht keine Praeferenz und keine Policy, damit es genau eine Stelle
 * gibt, an der ueber das Fragen entschieden wird.
 */
export async function probeSidecar(
    fetchImpl: FetchLike,
    origin = SIDECAR_ORIGIN,
    options: ProbeOptions = {},
): Promise<SidecarReading> {
    let health: Response;
    try {
        health = await fetchImpl(`${origin}/health`);
    } catch (error) {
        // Kein Prozess, kein Port, keine Antwort. Der erwartete Fall nach dem
        // Einschalten und kein Grund fuer eine Fehlerfarbe.
        return {
            state: 'not-running',
            router: false,
            models: [],
            detail: error instanceof Error ? error.message : String(error),
        };
    }
    if (health.status === 503) {
        return { state: 'starting', router: false, models: [], detail: '' };
    }
    if (!health.ok) {
        return {
            state: 'not-running',
            router: false,
            models: [],
            detail: `GET /health antwortete mit ${health.status}`,
        };
    }

    let facts: SidecarFacts;
    let router: boolean;
    try {
        const props = await fetchImpl(`${origin}/props`);
        if (!props.ok) {
            throw new Error(`GET /props antwortete mit ${props.status}`);
        }
        const payload = await props.json();
        router = readsAsRouter(payload);
        facts = readProps(payload);
    } catch (error) {
        // Der Prozess lebt, sagt aber nicht, was er geladen hat. `ready` ohne
        // Karte ist die ehrliche Lage; ein erfundener Modellname waere die
        // teuerste Zeile dieses Panels.
        return {
            state: 'ready',
            router: false,
            models: [],
            detail: `die Auskunft ueber das Modell war nicht lesbar: ${error instanceof Error ? error.message : String(error)}`,
        };
    }

    let models: CacheModel[] = [];
    try {
        const payload = await (await fetchImpl(`${origin}/v1/models`)).json();
        models = readModelList(payload);
        if (!router) {
            facts = { ...facts, ...readModelMeta(payload) };
        }
    } catch {
        // Die Groessen fehlen dann, der Rest der Karte steht. Ein Panel, das
        // wegen einer fehlenden Zahl gar nichts sagt, waere die schlechtere
        // Antwort.
    }

    if (!router) {
        return { state: 'ready', facts, router, models, detail: '' };
    }

    /*
     * Im Router-Modus stehen die Zahlen einer Instanz und nicht die des
     * Routers. `/props` hat gerade `model_path: "none"` und `n_ctx: 0` gemeldet;
     * das ist die Wahrheit ueber den Router und eine Unwahrheit ueber das
     * Modell, das antwortet. Es wird darum ein zweites Mal gefragt, mit der id
     * daneben.
     */
    const active = activeModelOf(models, options.model ?? '');
    if (active === undefined) {
        return {
            state: 'ready',
            router,
            models,
            detail: 'der Router fuehrt kein Modell in seinem Cache-Verzeichnis.',
        };
    }
    try {
        const scoped = await fetchImpl(`${origin}/props?model=${encodeURIComponent(active.id)}`);
        if (!scoped.ok) {
            throw new Error(`GET /props?model= antwortete mit ${scoped.status}`);
        }
        facts = readProps(await scoped.json());
    } catch (error) {
        /*
         * Ein ungeladenes Modell hat noch keine Zahlen, und ein Router, der die
         * id nicht kennt, antwortet mit 400. Beides ist kein Ausfall: der Name
         * aus der Liste steht, die Zahlen fehlen, und der Grund steht daneben.
         */
        facts = { model: active.name, modelPath: active.id, quantization: active.quantization ?? '' };
    }
    const listed = models.find((model) => model.id === active.id);
    if (listed !== undefined) {
        facts = {
            ...facts,
            ...(listed.weightsBytes === undefined ? {} : { weightsBytes: listed.weightsBytes }),
            ...(listed.parameters === undefined ? {} : { parameters: listed.parameters }),
            ...(listed.trainedContextTokens === undefined
                ? {}
                : { trainedContextTokens: listed.trainedContextTokens }),
        };
    }
    return { state: 'ready', facts, router, models, detail: '' };
}

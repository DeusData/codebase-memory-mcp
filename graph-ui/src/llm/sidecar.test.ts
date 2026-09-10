/**
 * Was der Sidecar-Manager aus einer Antwort macht, und was er tut, wenn keine
 * kommt.
 *
 * Der Kern dieser Datei ist nicht die Deutung von JSON, sondern die
 * Beweisfuehrung ueber das, was NICHT passiert: `probeSidecar` fragt genau drei
 * Adressen und keine vierte, und der Aufrufer ruft sie nur, wenn das LLM an ist.
 * Die zweite Haelfte pruefen die Praeferenz-Tests und der Beweislauf; hier wird
 * mitgezaehlt, welche Adressen ueberhaupt angefasst wurden.
 */

import { describe, expect, it } from 'vitest';

import {
    activeModelOf,
    humanBytes,
    modelDisplayName,
    probeSidecar,
    readModelList,
    readModelMeta,
    readProps,
    readsAsRouter,
    SIDECAR_ORIGIN,
    SIDECAR_POLL_MS,
    SIDECAR_PORT,
} from './sidecar';
import type { FetchLike } from './sidecar';
import { llmChipValue } from './strings';
import { readLlmPreference, recordLlmPreference, llmKey } from './preference';
import type { KeyValueStore } from '../checklist/understanding-store';

/** Die Antworten des laufenden Prozesses, gekuerzt auf die gelesenen Felder. */
const PROPS = {
    default_generation_settings: { n_ctx: 3072, params: {} },
    total_slots: 4,
    model_alias: 'models/Qwen3.5-2B-Q4_K_M.gguf',
    model_path: 'models/Qwen3.5-2B-Q4_K_M.gguf',
    model_ftype: 'Q4_K - Medium',
};

const MODELS = {
    object: 'list',
    data: [
        {
            id: 'models/Qwen3.5-2B-Q4_K_M.gguf',
            meta: {
                n_ctx: 3072,
                n_ctx_train: 262144,
                n_params: 1881825088,
                size: 1269873920,
                ftype: 'Q4_K - Medium',
            },
        },
    ],
};

function response(status: number, body: unknown): Response {
    return {
        ok: status >= 200 && status < 300,
        status,
        json: async () => body,
    } as unknown as Response;
}

/** Ein fetch, das mitschreibt, welche Adressen gefragt wurden. */
function recordingFetch(answer: (url: string) => Response | Error): { fetch: FetchLike; urls: string[] } {
    const urls: string[] = [];
    const fetchImpl: FetchLike = async (url) => {
        urls.push(url);
        const result = answer(url);
        if (result instanceof Error) {
            throw result;
        }
        return result;
    };
    return { fetch: fetchImpl, urls };
}

describe('die Adresse des Sidecars', () => {
    it('ist der Produktport auf Loopback und nichts sonst', () => {
        expect(SIDECAR_PORT).toBe(4141);
        expect(SIDECAR_ORIGIN).toBe('http://127.0.0.1:4141');
    });

    it('wird in Abstaenden gefragt, die ein Modellstart ueberlebt', () => {
        expect(SIDECAR_POLL_MS).toBe(3000);
    });
});

describe('der Modellname', () => {
    it('kommt aus dem Dateinamen, ohne Endung und ohne Quantisierung', () => {
        expect(modelDisplayName('models/Qwen3.5-2B-Q4_K_M.gguf')).toBe('Qwen3.5-2B');
        expect(modelDisplayName('models/gemma-4-E4B-it-Q4_K_M.gguf')).toBe('gemma-4-E4B-it');
        expect(modelDisplayName('models/LFM2.5-1.2B-Instruct-Q4_K_M.gguf')).toBe('LFM2.5-1.2B-Instruct');
    });

    it('laesst stehen, was er nicht sicher als Quantisierung erkennt', () => {
        expect(modelDisplayName('/x/y/mein-modell.gguf')).toBe('mein-modell');
        expect(modelDisplayName('mein-modell')).toBe('mein-modell');
    });
});

describe('die Zahlen des laufenden Prozesses', () => {
    it('kommen aus /props, Feld fuer Feld', () => {
        const facts = readProps(PROPS);
        expect(facts.model).toBe('Qwen3.5-2B');
        expect(facts.modelPath).toBe('models/Qwen3.5-2B-Q4_K_M.gguf');
        expect(facts.quantization).toBe('Q4_K - Medium');
        expect(facts.contextTokens).toBe(3072);
        expect(facts.slots).toBe(4);
    });

    it('ueberleben eine Antwort, der Felder fehlen', () => {
        const facts = readProps({ model_path: 'models/x-Q4_K_M.gguf' });
        expect(facts.model).toBe('x');
        expect(facts.contextTokens).toBeUndefined();
        expect(facts.slots).toBeUndefined();
    });

    it('holen die Groesse aus /v1/models, weil /props keine fuehrt', () => {
        expect(Object.keys(PROPS)).not.toContain('size');
        const meta = readModelMeta(MODELS);
        expect(meta.weightsBytes).toBe(1269873920);
        expect(meta.parameters).toBe(1881825088);
        expect(meta.trainedContextTokens).toBe(262144);
    });

    it('bleiben leer, wenn die Antwort die Felder nicht hat', () => {
        expect(readModelMeta({ data: [] })).toEqual({});
        expect(readModelMeta('nonsense')).toEqual({});
    });
});

describe('Byte fuer Menschen', () => {
    it('rundet in der Einheit, in der Modelle verteilt werden', () => {
        expect(humanBytes(1269873920)).toBe('1.18 GB');
        expect(humanBytes(688065920)).toBe('656 MB');
        expect(humanBytes(0)).toBe('');
        expect(humanBytes(Number.NaN)).toBe('');
    });
});

describe('die Probe', () => {
    it('meldet not-running, wenn die Verbindung abgelehnt wird', async () => {
        const { fetch, urls } = recordingFetch(() => new TypeError('Failed to fetch'));
        const reading = await probeSidecar(fetch);
        expect(reading.state).toBe('not-running');
        // Nach einer abgelehnten Verbindung wird nicht weitergefragt.
        expect(urls).toEqual(['http://127.0.0.1:4141/health']);
    });

    it('meldet starting, solange der Prozess sein Modell laedt', async () => {
        const { fetch, urls } = recordingFetch(() =>
            response(503, { error: { message: 'Loading model', code: 503 } }));
        const reading = await probeSidecar(fetch);
        expect(reading.state).toBe('starting');
        expect(urls).toEqual(['http://127.0.0.1:4141/health']);
    });

    it('meldet ready mit den Zahlen des Prozesses', async () => {
        const { fetch, urls } = recordingFetch((url) =>
            url.endsWith('/health')
                ? response(200, { status: 'ok' })
                : url.endsWith('/props')
                    ? response(200, PROPS)
                    : response(200, MODELS));
        const reading = await probeSidecar(fetch);
        expect(reading.state).toBe('ready');
        expect(reading.facts?.model).toBe('Qwen3.5-2B');
        expect(reading.facts?.contextTokens).toBe(3072);
        expect(reading.facts?.weightsBytes).toBe(1269873920);
        expect(urls).toEqual([
            'http://127.0.0.1:4141/health',
            'http://127.0.0.1:4141/props',
            'http://127.0.0.1:4141/v1/models',
        ]);
    });

    it('bleibt ready und erfindet keinen Modellnamen, wenn /props schweigt', async () => {
        const { fetch } = recordingFetch((url) =>
            url.endsWith('/health') ? response(200, { status: 'ok' }) : response(500, {}));
        const reading = await probeSidecar(fetch);
        expect(reading.state).toBe('ready');
        expect(reading.facts).toBeUndefined();
        expect(reading.detail).toMatch(/nicht lesbar/);
    });

    it('zeigt die Karte auch dann, wenn nur die Groessen fehlen', async () => {
        const { fetch } = recordingFetch((url) =>
            url.endsWith('/health')
                ? response(200, { status: 'ok' })
                : url.endsWith('/props')
                    ? response(200, PROPS)
                    : response(500, {}));
        const reading = await probeSidecar(fetch);
        expect(reading.state).toBe('ready');
        expect(reading.facts?.model).toBe('Qwen3.5-2B');
        expect(reading.facts?.weightsBytes).toBeUndefined();
    });
});

/*
 * Der Router-Modus (W10), und die Antworten, an denen er gemessen wurde.
 *
 * Alle drei Formen unten sind am 2026-08-29 an vendor/llama (llama-server
 * 0.3.0-dev, build b1-90c26fc) aufgezeichnet worden und hier auf die gelesenen
 * Felder gekuerzt. Der entscheidende Unterschied steht in ROUTER_PROPS: `role`
 * ist da, `model_path` ist "none" und `n_ctx` ist 0, weil der Router selbst
 * kein Modell geladen hat. Wer daraus eine Modellkarte baut, behauptet ein
 * Modell namens "none".
 */
const ROUTER_PROPS = {
    role: 'router',
    max_instances: 2,
    models_autoload: true,
    model_alias: 'llama-server',
    model_path: 'none',
    default_generation_settings: { params: null, n_ctx: 0 },
    build_info: 'b1-90c26fc',
};

const ROUTER_MODELS = {
    object: 'list',
    data: [
        {
            id: 'LFM2.5-1.2B-Instruct-Q4_K_M',
            object: 'model',
            status: { value: 'unloaded' },
            source: 'models_dir',
        },
        {
            id: 'MiniCPM5-1B-Q4_K_M',
            object: 'model',
            status: { value: 'loaded' },
            source: 'models_dir',
            meta: {
                n_ctx: 2048,
                n_ctx_train: 131072,
                n_params: 1080632832,
                size: 682930176,
                ftype: 'Q4_K - Medium',
            },
        },
    ],
};

const SCOPED_PROPS = {
    total_slots: 4,
    model_alias: 'MiniCPM5-1B-Q4_K_M',
    model_ftype: 'Q4_K - Medium',
    model_path: '/cache/MiniCPM5-1B-Q4_K_M.gguf',
    default_generation_settings: { n_ctx: 2048 },
};

function routerFetch(): { fetch: FetchLike; urls: string[] } {
    return recordingFetch((url) => {
        if (url.endsWith('/health')) {
            return response(200, { status: 'ok' });
        }
        if (url.includes('/props?model=')) {
            return response(200, SCOPED_PROPS);
        }
        if (url.endsWith('/props')) {
            return response(200, ROUTER_PROPS);
        }
        return response(200, ROUTER_MODELS);
    });
}

describe('die Betriebsart', () => {
    it('ist genau dann Router, wenn /props es sagt', () => {
        expect(readsAsRouter(ROUTER_PROPS)).toBe(true);
        // Im Einzel-Modus fehlt das Feld ganz. Die Abwesenheit ist die Auskunft.
        expect(readsAsRouter(PROPS)).toBe(false);
        expect(readsAsRouter('nonsense')).toBe(false);
    });
});

describe('die Liste des Cache-Verzeichnisses', () => {
    it('liest id, Lage und die Zahlen, die es nur bei geladenen gibt', () => {
        const models = readModelList(ROUTER_MODELS);
        expect(models.map((model) => model.id))
            .toEqual(['LFM2.5-1.2B-Instruct-Q4_K_M', 'MiniCPM5-1B-Q4_K_M']);
        expect(models[0].loaded).toBe(false);
        // Ein ungeladenes Modell hat kein meta, also auch keine Zahlen. Sie aus
        // dem Dateinamen zu schaetzen waere eine Angabe ueber eine Datei, die
        // niemand geoeffnet hat.
        expect(models[0].weightsBytes).toBeUndefined();
        expect(models[1].loaded).toBe(true);
        expect(models[1].weightsBytes).toBe(682930176);
        expect(models[1].parameters).toBe(1080632832);
        expect(models[1].trainedContextTokens).toBe(131072);
    });

    it('liest die Einzel-Modus-Form als Liste mit einem Eintrag', () => {
        const models = readModelList(MODELS);
        expect(models).toHaveLength(1);
        // Ohne `status`, aber mit `meta`: das ist die Form des Einzel-Modus, und
        // dort IST das eine Modell geladen.
        expect(models[0].loaded).toBe(true);
        expect(models[0].name).toBe('Qwen3.5-2B');
    });

    it('nimmt aus einer Antwort ohne data nichts an', () => {
        expect(readModelList({})).toEqual([]);
        expect(readModelList('nonsense')).toEqual([]);
    });
});

describe('welches Modell die Karte zeigt', () => {
    const models = readModelList(ROUTER_MODELS);

    it('ist die Wahl des Lesers, wenn es sie noch gibt', () => {
        expect(activeModelOf(models, 'LFM2.5-1.2B-Instruct-Q4_K_M')?.id)
            .toBe('LFM2.5-1.2B-Instruct-Q4_K_M');
    });

    it('faellt auf das geladene zurueck, wenn die Wahl verschwunden ist', () => {
        // Die Lage nach einem Neustart mit einem anderen Cache-Verzeichnis. Eine
        // Karte mit den Zahlen eines Modells, das es nicht mehr gibt, waere
        // schlimmer als eine mit denen des laufenden.
        expect(activeModelOf(models, 'ein-modell-das-es-nicht-gibt')?.id)
            .toBe('MiniCPM5-1B-Q4_K_M');
    });

    it('nimmt ohne Wahl und ohne geladenes das erste', () => {
        const unloaded = models.map((model) => ({ ...model, loaded: false }));
        expect(activeModelOf(unloaded, '')?.id).toBe('LFM2.5-1.2B-Instruct-Q4_K_M');
    });

    it('erfindet aus einer leeren Liste keines', () => {
        expect(activeModelOf([], 'irgendwas')).toBeUndefined();
    });
});

describe('die Probe im Router-Modus', () => {
    it('fragt ein viertes Mal, mit der id daneben, und nimmt DEREN Zahlen', async () => {
        const { fetch, urls } = routerFetch();
        const reading = await probeSidecar(fetch, SIDECAR_ORIGIN, { model: 'MiniCPM5-1B-Q4_K_M' });
        expect(reading.state).toBe('ready');
        expect(reading.router).toBe(true);
        expect(reading.models).toHaveLength(2);
        // NICHT "none" und nicht 0: das waeren die Zahlen des Routers.
        expect(reading.facts?.model).toBe('MiniCPM5-1B');
        expect(reading.facts?.contextTokens).toBe(2048);
        expect(reading.facts?.quantization).toBe('Q4_K - Medium');
        // Die Groessen kommen aus der Liste, weil /props sie nicht fuehrt.
        expect(reading.facts?.weightsBytes).toBe(682930176);
        expect(reading.facts?.trainedContextTokens).toBe(131072);
        expect(urls).toEqual([
            'http://127.0.0.1:4141/health',
            'http://127.0.0.1:4141/props',
            'http://127.0.0.1:4141/v1/models',
            'http://127.0.0.1:4141/props?model=MiniCPM5-1B-Q4_K_M',
        ]);
    });

    it('nimmt ohne Wahl das geladene Modell', async () => {
        const { fetch, urls } = routerFetch();
        const reading = await probeSidecar(fetch, SIDECAR_ORIGIN);
        expect(reading.facts?.model).toBe('MiniCPM5-1B');
        expect(urls[3]).toBe('http://127.0.0.1:4141/props?model=MiniCPM5-1B-Q4_K_M');
    });

    it('behaelt den Namen aus der Liste, wenn die zweite Frage scheitert', async () => {
        const { fetch } = recordingFetch((url) => {
            if (url.endsWith('/health')) {
                return response(200, { status: 'ok' });
            }
            if (url.includes('/props?model=')) {
                // Genau die Antwort, die der Router auf eine unbekannte id gibt.
                return response(400, { error: { code: 400, message: "model 'X' not found" } });
            }
            if (url.endsWith('/props')) {
                return response(200, ROUTER_PROPS);
            }
            return response(200, ROUTER_MODELS);
        });
        const reading = await probeSidecar(fetch, SIDECAR_ORIGIN, { model: 'MiniCPM5-1B-Q4_K_M' });
        expect(reading.state).toBe('ready');
        expect(reading.facts?.model).toBe('MiniCPM5-1B');
        // Die Zahlen fehlen dann, der Name steht. Ein erfundener Kontext waere
        // die teuerste Zeile dieses Panels.
        expect(reading.facts?.contextTokens).toBeUndefined();
        expect(reading.facts?.weightsBytes).toBe(682930176);
    });

    it('sagt es, wenn der Router gar kein Modell fuehrt', async () => {
        const { fetch, urls } = recordingFetch((url) => {
            if (url.endsWith('/health')) {
                return response(200, { status: 'ok' });
            }
            if (url.endsWith('/props')) {
                return response(200, ROUTER_PROPS);
            }
            return response(200, { object: 'list', data: [] });
        });
        const reading = await probeSidecar(fetch, SIDECAR_ORIGIN);
        expect(reading.state).toBe('ready');
        expect(reading.router).toBe(true);
        expect(reading.models).toEqual([]);
        expect(reading.facts).toBeUndefined();
        expect(reading.detail.length).toBeGreaterThan(0);
        // Kein vierter Aufruf: es gibt keine id, nach der man fragen koennte.
        expect(urls).toHaveLength(3);
    });
});

describe('die Probe im Einzel-Modus bleibt, was sie war', () => {
    it('fragt drei Adressen und meldet keinen Router', async () => {
        const { fetch, urls } = recordingFetch((url) =>
            url.endsWith('/health')
                ? response(200, { status: 'ok' })
                : url.endsWith('/props')
                    ? response(200, PROPS)
                    : response(200, MODELS));
        const reading = await probeSidecar(fetch, SIDECAR_ORIGIN, { model: 'irgendein-modell' });
        expect(reading.router).toBe(false);
        expect(reading.facts?.model).toBe('Qwen3.5-2B');
        // Die Wahl aendert hier NICHTS, auch nicht die Zahl der Anfragen: ein
        // Einzel-Server ignoriert ein fremdes model-Feld stillschweigend.
        expect(urls).toEqual([
            'http://127.0.0.1:4141/health',
            'http://127.0.0.1:4141/props',
            'http://127.0.0.1:4141/v1/models',
        ]);
    });
});

describe('der Statusleisten-Chip', () => {
    it('sagt in jeder Lage genau eine Sache', () => {
        expect(llmChipValue('off', '')).toBe('off');
        expect(llmChipValue('disabled-by-policy', '')).toBe('off by policy');
        expect(llmChipValue('not-running', '')).toBe('not running');
        expect(llmChipValue('starting', '')).toBe('starting');
        expect(llmChipValue('ready', 'Qwen3.5-2B')).toBe('ready: Qwen3.5-2B');
    });

    it('behauptet keinen Modellnamen, wenn keiner bekannt ist', () => {
        expect(llmChipValue('ready', '')).toBe('ready');
    });
});

function memoryStore(seed: Record<string, string> = {}): KeyValueStore {
    const map = new Map<string, string>(Object.entries(seed));
    return {
        getItem: (key) => map.get(key) ?? null,
        setItem: (key, value) => {
            map.set(key, value);
        },
    };
}

describe('die Praeferenz dieses Browsers', () => {
    it('liegt unter einem Schluessel, der das Projekt nennt', () => {
        expect(llmKey('atlas-sample')).toBe('atlas-llm:atlas-sample');
    });

    it('ist beim Erststart aus', () => {
        expect(readLlmPreference(memoryStore(), 'p')).toEqual({ on: false });
    });

    it('ist nach dem Einschalten an und ueberlebt das Nachlesen', () => {
        const store = memoryStore();
        expect(recordLlmPreference(store, 'p', true)).toEqual({ on: true });
        expect(readLlmPreference(store, 'p')).toEqual({ on: true });
    });

    it('faellt bei jedem Zweifel auf aus zurueck', () => {
        expect(readLlmPreference(memoryStore({ 'atlas-llm:p': 'kaputt' }), 'p')).toEqual({ on: false });
        expect(readLlmPreference(memoryStore({ 'atlas-llm:p': '{"on":"ja"}' }), 'p')).toEqual({ on: false });
        const refusing: KeyValueStore = {
            getItem: () => {
                throw new Error('access denied');
            },
            setItem: () => {
                throw new Error('access denied');
            },
        };
        expect(readLlmPreference(refusing, 'p')).toEqual({ on: false });
    });

    it('haelt die Projekte auseinander', () => {
        const store = memoryStore();
        recordLlmPreference(store, 'a', true);
        expect(readLlmPreference(store, 'b')).toEqual({ on: false });
    });
});

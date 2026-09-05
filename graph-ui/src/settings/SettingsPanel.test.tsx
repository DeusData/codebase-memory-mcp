// @vitest-environment jsdom
/*
 * Das Einstellungen-Panel in jsdom: was es zeigt, was es NICHT zeigt, und was
 * ein Klick daraus macht.
 *
 * Der Kern dieser Datei sind die drei Faelle, in denen das Panel etwas
 * WEGLAESST, denn ein weggelassenes Element sieht man auf keinem Screenshot:
 *
 *  1. Bei ausgeschaltetem Modell gibt es keinen Aktualisieren-Knopf, und das
 *     Panel steht trotzdem da und erklaert sich.
 *  2. Ohne Router gibt es keine anklickbare Auswahl, sondern die Meldung mit
 *     dem Grund und dem Startbefehl.
 *  3. Vor der ersten Messung steht bei jeder Leistungseinstellung, dass nichts
 *     gemessen ist, und keine Zahl.
 *
 * Was hier NICHT geprueft wird, prueft der Beweislauf im Browser: dass die
 * Bildrate wirklich faellt oder steigt, dass sich nichts ueberlagert und dass
 * eine Wahl den Reload ueberlebt. Das sind Fragen an eine laufende Szene, und
 * jsdom zeichnet keine.
 */

import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import SettingsPanel, { ROUTER_START_COMMAND } from './SettingsPanel';
import type { SettingsPanelProps } from './SettingsPanel';
import { MODEL_SUGGESTIONS } from './model-catalog';
import { DEFAULT_GRAPH_DISPLAY, THRIFTY_GRAPH_DISPLAY } from '../galaxy/density';
import { resetFrameRate } from '../galaxy/frame-rate';
import type { CacheModel, SidecarFacts } from '../llm/sidecar';
import { messages } from '../i18n/messages';

let container: HTMLDivElement;
let root: Root;

const FACTS: SidecarFacts = {
    model: 'MiniCPM5-1B',
    modelPath: '/cache/MiniCPM5-1B-Q4_K_M.gguf',
    quantization: 'Q4_K - Medium',
    contextTokens: 2048,
    trainedContextTokens: 131072,
    weightsBytes: 682930176,
    parameters: 1080632832,
};

const MODELS: CacheModel[] = [
    { id: 'LFM2.5-1.2B-Instruct-Q4_K_M', name: 'LFM2.5-1.2B-Instruct', loaded: false, status: 'unloaded' },
    { id: 'MiniCPM5-1B-Q4_K_M', name: 'MiniCPM5-1B', loaded: true, status: 'loaded' },
];

beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    resetFrameRate();
});

afterEach(async () => {
    await act(async () => {
        root.unmount();
    });
    container.remove();
    Reflect.deleteProperty(navigator, 'clipboard');
});

async function render(overrides: Partial<SettingsPanelProps> = {}): Promise<SettingsPanelProps> {
    const props: SettingsPanelProps = {
        project: 'atlas-sample',
        state: 'ready',
        facts: FACTS,
        router: true,
        models: MODELS,
        selectedModel: 'MiniCPM5-1B-Q4_K_M',
        onSelectModel: vi.fn(),
        onRefresh: vi.fn(),
        display: DEFAULT_GRAPH_DISPLAY,
        onDisplay: vi.fn(),
        onClose: vi.fn(),
        ...overrides,
    };
    await act(async () => {
        root.render(<SettingsPanel {...props} />);
    });
    return props;
}

const find = (testId: string): Element | null =>
    container.querySelector(`[data-testid="${testId}"]`);
const all = (testId: string): Element[] =>
    [...container.querySelectorAll(`[data-testid="${testId}"]`)];
const textOf = (node: Element | null): string => (node?.textContent ?? '').replace(/\s+/g, ' ').trim();

/**
 * In ein gesteuertes Feld schreiben, so wie ein Mensch es tut.
 *
 * Ueber den nativen Setter und nicht ueber `field.value = x`: React merkt sich
 * den zuletzt gezeichneten Wert am Element selbst, und eine direkte Zuweisung
 * aktualisiert diese Merkung mit, sodass das darauf folgende Ereignis wie ein
 * Ereignis ohne Aenderung aussieht. Dasselbe Vorgehen wie in
 * src/twin/TwinPanel.test.tsx.
 */
async function type(field: HTMLInputElement, value: string): Promise<void> {
    await act(async () => {
        const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
        setter?.call(field, value);
        field.dispatchEvent(new Event('input', { bubbles: true }));
    });
}

describe('das Panel als Ganzes', () => {
    it('traegt die Wurzel-Testmarke aus dem Contract', async () => {
        await render();
        expect(find('atlas-settings')).not.toBeNull();
    });

    it('zeigt die vier Abschnitte in ihrer Reihenfolge', async () => {
        await render();
        expect(all('atlas-settings-section').map((node) => node.getAttribute('data-section')))
            .toEqual(['model-running', 'model-cache', 'model-fetch', 'display']);
    });

    it('schliesst auf Escape und auf den Knopf', async () => {
        const props = await render();
        await act(async () => {
            find('atlas-settings')?.dispatchEvent(
                new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }),
            );
        });
        expect(props.onClose).toHaveBeenCalled();
        await act(async () => {
            (find('atlas-settings-close') as HTMLButtonElement).click();
        });
        expect(props.onClose).toHaveBeenCalledTimes(2);
    });
});

describe('das laufende Modell', () => {
    it('zeigt die vier Zahlen, jede mit ihrer Herkunft', async () => {
        await render();
        const facts = all('atlas-settings-fact');
        expect(facts.map((node) => node.getAttribute('data-fact')))
            .toEqual(['name', 'quantization', 'context', 'weights']);
        for (const fact of facts) {
            const value = fact.querySelector('[data-testid="atlas-settings-fact-value"]');
            // Keine Zahl ohne Herkunft: der Ursprung steht im DOM, damit ein
            // Lauf ihn lesen kann, und nicht nur in einem Tooltip.
            expect((value?.getAttribute('data-source') ?? '').length).toBeGreaterThan(0);
        }
        expect(textOf(container)).toContain('2048');
        expect(textOf(container)).toContain('Q4_K - Medium');
    });

    it('nennt im Router-Modus die Anfrage mit der id als Quelle', async () => {
        await render();
        const source = find('atlas-settings-fact-value')?.getAttribute('data-source') ?? '';
        expect(source).toContain('model=');
    });

    it('erfindet keine Zahl, die der Prozess nicht genannt hat', async () => {
        await render({ facts: { model: 'X', modelPath: 'X', quantization: '' } });
        expect(textOf(container)).toContain(messages.settings.valueUnreported);
    });
});

describe('aus heisst aus', () => {
    it('erklaert sich bei ausgeschaltetem Modell und bietet keinen Knopf an', async () => {
        await render({ state: 'off', facts: undefined, router: false, models: [], onRefresh: undefined });
        // Sichtbar und erklaerend: die Flaeche verschwindet nicht, sie sagt, was
        // sie koennte und was der Leser dafuer tun muesste.
        expect(find('atlas-settings')).not.toBeNull();
        expect(find('atlas-settings-llm-off')).not.toBeNull();
        // Kein Aktualisieren-Knopf: er waere der einzige Weg, aus diesem Panel
        // heraus eine Anfrage an den Sidecar zu schicken.
        expect(find('atlas-settings-refresh')).toBeNull();
        expect(find('atlas-settings-models')).toBeNull();
    });

    it('zeigt den Darstellungs-Teil auch dann vollstaendig', async () => {
        await render({ state: 'off', facts: undefined, router: false, models: [], onRefresh: undefined });
        // Der Teil hat mit dem Modell nichts zu tun, also faellt er nicht mit ihm aus.
        expect(all('atlas-settings-effect').length).toBeGreaterThanOrEqual(3);
        expect(find('atlas-settings-profiles')).not.toBeNull();
    });

    it('nennt bei laufendem Modell ohne Prozess den Startbefehl', async () => {
        await render({ state: 'not-running', facts: undefined, router: false, models: [] });
        expect(textOf(find('atlas-settings-start-command'))).toContain('llm/start.sh');
    });
});

describe('umschalten, oder der Grund, warum nicht', () => {
    it('macht jede Zeile anklickbar, wenn ein Router mehr als eines fuehrt', async () => {
        const props = await render();
        const rows = all('atlas-settings-model');
        expect(rows.map((node) => node.getAttribute('data-model')))
            .toEqual(['LFM2.5-1.2B-Instruct-Q4_K_M', 'MiniCPM5-1B-Q4_K_M']);
        expect(rows[1].getAttribute('data-active')).toBe('true');
        await act(async () => {
            (all('atlas-settings-model-pick')[0] as HTMLButtonElement).click();
        });
        expect(props.onSelectModel).toHaveBeenCalledWith('LFM2.5-1.2B-Instruct-Q4_K_M');
    });

    it('bietet ohne Router keine Auswahl an, sondern den Grund und den Befehl', async () => {
        await render({ router: false, models: [MODELS[1]] });
        expect(find('atlas-settings-no-router')).not.toBeNull();
        expect(textOf(find('atlas-settings-router-command'))).toBe(ROUTER_START_COMMAND);
        // Kein Klick in dieser Lage: er waere wirkungslos, und das sagt der Text
        // daneben ausdruecklich.
        expect(all('atlas-settings-model-pick').filter((node) => node.tagName === 'BUTTON'))
            .toHaveLength(0);
        expect(textOf(find('atlas-settings-no-router'))).toContain('ignores');
    });

    it('bietet auch bei genau einem Modell keinen Klick an', async () => {
        await render({ models: [MODELS[1]] });
        expect(all('atlas-settings-model-pick').filter((node) => node.tagName === 'BUTTON'))
            .toHaveLength(0);
    });

    it('ruft mit dem Aktualisieren-Knopf denselben Weg wie der Manager', async () => {
        const props = await render();
        await act(async () => {
            (find('atlas-settings-refresh') as HTMLButtonElement).click();
        });
        expect(props.onRefresh).toHaveBeenCalled();
    });
});

describe('ein Modell holen', () => {
    it('fuehrt die sechs Vorschlaege mit ihren gemessenen Zahlen', async () => {
        await render();
        const rows = all('atlas-settings-suggestion');
        expect(rows).toHaveLength(MODEL_SUGGESTIONS.length);
        for (const [index, row] of rows.entries()) {
            const suggestion = MODEL_SUGGESTIONS[index];
            expect(row.getAttribute('data-suggestion')).toBe(suggestion.id);
            expect(row.getAttribute('data-pass-rate')).toBe(String(suggestion.passRate));
            expect(row.getAttribute('data-citation')).toBe(String(suggestion.citationCompliance));
            expect(row.getAttribute('data-bytes')).toBe(String(suggestion.bytes));
            expect(row.getAttribute('data-repo')).toBe(suggestion.repo);
        }
    });

    it('sagt bei jedem Vorschlag, dass die ungemessenen Antworten nicht ausgewiesen sind', async () => {
        await render();
        const marks = all('atlas-settings-unmeasured');
        expect(marks).toHaveLength(MODEL_SUGGESTIONS.length);
        for (const mark of marks) {
            // Der aufgezeichnete Lauf von W5 fuehrt das Feld nicht. Eine Null
            // waere die Behauptung, es habe keine ungemessene Antwort gegeben.
            expect(textOf(mark)).toBe(messages.settings.unmeasuredMissing);
        }
    });

    it('stellt zu jedem Vorschlag den fertigen Befehl als Text und zum Kopieren', async () => {
        await render();
        const commands = all('atlas-settings-command');
        expect(commands).toHaveLength(MODEL_SUGGESTIONS.length);
        expect(textOf(commands[0])).toContain(MODEL_SUGGESTIONS[0].repo);
        expect(textOf(commands[0])).toContain('llm/fetch-model.sh');
        expect(all('atlas-settings-copy').length).toBeGreaterThanOrEqual(MODEL_SUGGESTIONS.length);
    });

    it('antwortet sofort, waehrend die Zwischenablage noch offen ist', async () => {
        let finish = (): void => undefined;
        const writeText = vi.fn(() => new Promise<void>((resolve) => {
            finish = resolve;
        }));
        Object.defineProperty(navigator, 'clipboard', {
            configurable: true,
            value: { writeText },
        });
        await render();
        const button = all('atlas-settings-copy')[0] as HTMLButtonElement;
        await act(async () => {
            button.click();
        });
        expect(writeText).toHaveBeenCalledTimes(1);
        expect(textOf(button)).toBe(messages.settings.copying);
        expect(button.getAttribute('data-copying')).toBe('true');
        await act(async () => {
            finish();
        });
        expect(textOf(button)).toBe(messages.settings.copied);
    });

    it('sagt ueber dem Ganzen die drei Dinge, um die es geht', async () => {
        await render();
        const honesty = textOf(find('atlas-settings-honesty'));
        expect(honesty).toContain('huggingface.co');
        expect(honesty).toContain('models/');
        expect(honesty).toContain('downloads nothing itself');
    });

    it('verspricht keinen Fortschritt, den es nicht sehen kann', async () => {
        await render();
        expect(textOf(find('atlas-settings-no-progress'))).toContain('no progress bar');
        expect(container.querySelector('progress')).toBeNull();
        expect(container.querySelector('[role="progressbar"]')).toBeNull();
    });

    it('nimmt ein beliebiges Repo an und benennt, was nicht der Form entspricht', async () => {
        await render();
        const field = find('atlas-settings-repo-input') as HTMLInputElement;
        expect(find('atlas-settings-repo-state')?.getAttribute('data-problem')).toBe('empty');
        expect(find('atlas-settings-repo-command')).toBeNull();

        await type(field, 'kein-slash');
        expect(find('atlas-settings-repo-state')?.getAttribute('data-valid')).toBe('false');
        expect(find('atlas-settings-repo-state')?.getAttribute('data-problem')).toBe('shape');
        expect(find('atlas-settings-repo-command')).toBeNull();

        await type(field, 'unsloth/Qwen3.5-9B-GGUF:Q5_K_M');
        expect(find('atlas-settings-repo-state')?.getAttribute('data-valid')).toBe('true');
        expect(textOf(find('atlas-settings-repo-command')))
            .toBe('llm/fetch-model.sh unsloth/Qwen3.5-9B-GGUF:Q5_K_M');
    });
});

describe('Darstellung und Leistung', () => {
    it('fuehrt 2D/3D, mindestens drei Effekte und den Bildratendeckel', async () => {
        await render();
        const settings = all('atlas-settings-choice').concat(all('atlas-settings-effect'))
            .map((node) => node.getAttribute('data-setting'));
        expect(settings).toContain('projection');
        expect(settings).toContain('frameCap');
        expect(all('atlas-settings-effect').map((node) => node.getAttribute('data-effect')))
            .toEqual(['halos', 'bloom', 'edges', 'labels', 'agents',
                'agentTails', 'agentTrails', 'agentWaves', 'agentTimeline']);
        expect(all('atlas-settings-effect').length).toBeGreaterThanOrEqual(3);
    });

    it('legt 2D um und meldet die neue Einstellung nach oben', async () => {
        const props = await render();
        const projection = all('atlas-settings-choice')
            .find((node) => node.getAttribute('data-setting') === 'projection');
        const flat = [...(projection?.querySelectorAll('[data-testid="atlas-settings-option"]') ?? [])]
            .find((node) => node.getAttribute('data-option') === 'flat') as HTMLButtonElement;
        await act(async () => {
            flat.click();
        });
        expect(props.onDisplay).toHaveBeenCalledWith({
            ...DEFAULT_GRAPH_DISPLAY,
            projection: 'flat',
        });
    });

    it('setzt das Sparprofil in einem Zug und findet zurueck auf die Vorgabe', async () => {
        const props = await render({ display: DEFAULT_GRAPH_DISPLAY });
        const profiles = all('atlas-settings-profile') as HTMLButtonElement[];
        await act(async () => {
            profiles.find((node) => node.getAttribute('data-profile') === 'thrifty')?.click();
        });
        expect(props.onDisplay).toHaveBeenCalledWith(THRIFTY_GRAPH_DISPLAY);
        await act(async () => {
            profiles.find((node) => node.getAttribute('data-profile') === 'default')?.click();
        });
        expect(props.onDisplay).toHaveBeenLastCalledWith(DEFAULT_GRAPH_DISPLAY);
    });

    it('zeigt, ob die Einstellungen auf ihrer Vorgabe stehen', async () => {
        await render({ display: DEFAULT_GRAPH_DISPLAY });
        expect(find('atlas-settings-profiles')?.getAttribute('data-default')).toBe('true');
        await render({ display: THRIFTY_GRAPH_DISPLAY });
        expect(find('atlas-settings-profiles')?.getAttribute('data-default')).toBe('false');
    });

    it('sagt vor der ersten Messung, dass nichts gemessen ist, und nennt keine Zahl', async () => {
        await render();
        const lines = all('atlas-settings-measure');
        expect(lines.length).toBeGreaterThanOrEqual(6);
        for (const line of lines) {
            expect(line.getAttribute('data-verdict')).toBe('not-measured');
            expect(line.getAttribute('data-before')).toBe('');
            expect(line.getAttribute('data-after')).toBe('');
            expect(textOf(line)).toBe(messages.settings.measureIdle);
        }
    });

    it('sagt, dass an einem stehenden Graphen nichts zu messen ist', async () => {
        const props = await render();
        // Kein Fenster ist je angekommen, also zeichnet nichts. Das Panel
        // behauptet dann keine Bildrate, sondern nennt die Lage.
        expect(find('atlas-settings-perf')?.getAttribute('data-running')).toBe('false');
        expect(textOf(find('atlas-settings-perf'))).toBe(messages.settings.liveIdle);

        const halos = all('atlas-settings-effect')
            .find((node) => node.getAttribute('data-effect') === 'halos');
        const off = [...(halos?.querySelectorAll('[data-testid="atlas-settings-option"]') ?? [])]
            .find((node) => node.getAttribute('data-option') === 'false') as HTMLButtonElement;
        await act(async () => {
            off.click();
        });
        expect(props.onDisplay).toHaveBeenCalledWith({ ...DEFAULT_GRAPH_DISPLAY, halos: false });
        const line = all('atlas-settings-measure')
            .find((node) => node.getAttribute('data-setting') === 'halos');
        expect(line?.getAttribute('data-verdict')).toBe('not-drawing');
        expect(textOf(line ?? null)).toBe(messages.settings.measureNotDrawing);
    });

    it('nennt beide Speicherorte, damit ein Lauf sie nicht nachbauen muss', async () => {
        await render();
        expect(textOf(find('atlas-settings-storage'))).toContain('atlas-display:atlas-sample');
        expect(textOf(find('atlas-settings-model-storage'))).toContain('atlas-model:atlas-sample');
    });

    it('sagt, welche Schalter ausdruecklich NICHT hierher umgezogen sind', async () => {
        await render();
        expect(textOf(find('atlas-settings-keeps'))).toContain('legend');
    });
});

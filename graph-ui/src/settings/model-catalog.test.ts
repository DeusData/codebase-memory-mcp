/**
 * Jede Zahl der Vorschlagsliste gegen die Quelle, aus der sie stammt.
 *
 * Der Katalog (src/settings/model-catalog.ts) traegt sechs Zeilen mit
 * Trefferquote, Zitattreue, Tempo, Groesse und Repo-Kennung. Alle fuenf Angaben
 * sind irgendwo gemessen oder recherchiert worden, und alle fuenf stehen im
 * Quelltext abgeschrieben. Abgeschriebene Zahlen altern still: die Quelle
 * aendert sich, die Kopie bleibt, und das Panel behauptet danach etwas, das
 * niemand gemessen hat.
 *
 * Dieser Test ist die Bruecke. Er liest die Quellen selbst:
 *
 *  - verification/w5/eval.json fuer passRate, citationCompliance und tok/s,
 *  - docs/adr/0001-modellwahl.md fuer die Bytes,
 *  - dieselbe ADR und verification/w5/modellrecherche.md fuer die Repo-Kennung.
 *
 * Er liest sie als DATEIEN und nicht ueber einen Import: der Katalog laeuft im
 * Browser, und eval.json ist eine Viertelmegabyte grosse Aufzeichnung mit den
 * Antworttexten aller 264 Laeufe. Sie in das Bundle zu ziehen, um sechs Zahlen
 * daraus zu lesen, waere ein Beweisartefakt im Auslieferungspfad.
 */

import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import {
    MODEL_SUGGESTIONS,
    fetchCommand,
    percentText,
    readRepoInput,
    speedText,
} from './model-catalog';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (relative: string): string => readFileSync(join(ROOT, relative), 'utf8');

interface EvalModel {
    name: string;
    file: string;
    passRate: number;
    citationCompliance: number;
    meanTokPerSec: number;
    /** Erst seit W10 im Bericht. In der aufgezeichneten Datei von W5 fehlt es. */
    citationUnmeasured?: number;
}

const evalRun = JSON.parse(read('verification/w5/eval.json')) as { models: EvalModel[] };
const adr = read('docs/adr/0001-modellwahl.md');
const research = read('verification/w5/modellrecherche.md');

describe('die Vorschlaege sind die sechs Kandidaten der ADR', () => {
    it('fuehrt genau sechs, so viele wie die Eval gemessen hat', () => {
        expect(MODEL_SUGGESTIONS).toHaveLength(6);
        expect(evalRun.models).toHaveLength(6);
    });

    it('vergibt jede Kennung genau einmal', () => {
        const ids = MODEL_SUGGESTIONS.map((entry) => entry.id);
        expect(new Set(ids).size).toBe(ids.length);
    });

    it('nennt dieselben Modelle wie der aufgezeichnete Lauf', () => {
        expect(MODEL_SUGGESTIONS.map((entry) => entry.name).sort())
            .toEqual(evalRun.models.map((entry) => entry.name).sort());
    });
});

describe('jede gemessene Zahl steht so in verification/w5/eval.json', () => {
    for (const suggestion of MODEL_SUGGESTIONS) {
        it(`${suggestion.name}: Trefferquote, Zitattreue, Tempo und Datei`, () => {
            const recorded = evalRun.models.find((entry) => entry.name === suggestion.name);
            expect(recorded, `${suggestion.name} steht nicht in eval.json`).toBeDefined();
            expect(suggestion.passRate).toBe(recorded?.passRate);
            expect(suggestion.citationCompliance).toBe(recorded?.citationCompliance);
            expect(suggestion.tokensPerSecond).toBe(recorded?.meanTokPerSec);
            expect(suggestion.file).toBe(recorded?.file);
        });
    }
});

describe('die nicht gemessenen Antworten werden nicht erfunden', () => {
    /*
     * Der Fall, den AC8 ausdruecklich vorsieht: das Feld gibt es erst seit W10,
     * und verification/w5/eval.json wird NICHT neu erzeugt (das hiesse, die Eval
     * noch einmal zu fahren). Der Katalog darf die Zahl deshalb nicht haben, und
     * das Panel muss ohne sie auskommen. Eine Null waere die Behauptung, es habe
     * keine ungemessene Antwort gegeben, und das weiss niemand.
     */
    for (const suggestion of MODEL_SUGGESTIONS) {
        it(`${suggestion.name}: so viele, wie der Lauf ausweist, und keine erfundene`, () => {
            const recorded = evalRun.models.find((entry) => entry.name === suggestion.name);
            if (recorded?.citationUnmeasured === undefined) {
                expect(suggestion.citationUnmeasured).toBeUndefined();
                return;
            }
            expect(suggestion.citationUnmeasured).toBe(recorded.citationUnmeasured);
        });
    }
});

describe('jede Groessenangabe steht so in docs/adr/0001-modellwahl.md', () => {
    for (const suggestion of MODEL_SUGGESTIONS) {
        it(`${suggestion.name}: ${suggestion.bytes} Bytes`, () => {
            /*
             * Woertlich gesucht, mit dem Wort "Bytes" daneben. Ohne das Wort
             * koennte eine beliebige andere Zahl der Datei zufaellig passen,
             * und der Test wuerde eine Uebereinstimmung melden, die keine ist.
             */
            expect(adr, `${suggestion.bytes} steht nicht als Byte-Angabe in der ADR`)
                .toContain(`${suggestion.bytes} Bytes`);
        });
    }

    it('haelt die Groessen in der Reihenfolge, in der die Klassen wachsen', () => {
        // Kein Selbstzweck: eine vertauschte Zeile im Katalog wuerde die Zahlen
        // eines Modells neben den Namen eines anderen stellen, und alle
        // Einzelpruefungen darueber blieben trotzdem gruen.
        const classA = MODEL_SUGGESTIONS.filter((entry) => entry.modelClass === 'A');
        const classB = MODEL_SUGGESTIONS.filter((entry) => entry.modelClass === 'B');
        expect(classA).toHaveLength(4);
        expect(classB).toHaveLength(2);
        expect(Math.max(...classA.map((entry) => entry.bytes)))
            .toBeLessThan(Math.min(...classB.map((entry) => entry.bytes)));
    });
});

describe('jede Repo-Kennung ist dokumentiert und nicht erfunden', () => {
    for (const suggestion of MODEL_SUGGESTIONS) {
        it(`${suggestion.name}: ${suggestion.repo}`, () => {
            const documented = adr.includes(suggestion.repo) || research.includes(suggestion.repo);
            expect(
                documented,
                `${suggestion.repo} steht weder in der ADR noch in der Modellrecherche`,
            ).toBe(true);
        });
    }
});

describe('das freie Feld prueft die Form und behauptet nichts darueber hinaus', () => {
    it('nimmt user/repo an', () => {
        expect(readRepoInput('unsloth/Qwen3.5-9B-GGUF'))
            .toEqual({ ok: true, repo: 'unsloth/Qwen3.5-9B-GGUF', quant: '', problem: '' });
    });

    it('nimmt user/repo:quant an und trennt beides', () => {
        expect(readRepoInput(' unsloth/Qwen3.5-9B-GGUF:Q5_K_M '))
            .toEqual({ ok: true, repo: 'unsloth/Qwen3.5-9B-GGUF', quant: 'Q5_K_M', problem: '' });
    });

    it('benennt eine leere Eingabe als leer und nicht als falsch', () => {
        expect(readRepoInput('   ').problem).toBe('empty');
    });

    it('weist zurueck, was nicht die Form hat', () => {
        for (const bad of ['kein slash', 'zu/viele/teile', 'user/', '/repo', 'user/repo:', 'user repo']) {
            expect(readRepoInput(bad).ok, `${bad} haette abgelehnt werden muessen`).toBe(false);
            expect(readRepoInput(bad).problem).toBe('shape');
        }
    });

    it('nimmt dieselbe Form an wie llm/fetch-model.sh', () => {
        // Die Pruefung steht an zwei Stellen, weil sie an zwei Stellen gebraucht
        // wird: im Feld und im Skript. Dass es dieselbe ist, wird hier gemessen
        // und nicht behauptet.
        const script = read('llm/fetch-model.sh');
        expect(script).toContain('[A-Za-z0-9._-]+/[A-Za-z0-9._-]+(:[A-Za-z0-9._-]+)?');
    });
});

describe('der Befehl, der ein Modell holt', () => {
    it('nennt das Skript dieses Projekts und die Repo-Kennung', () => {
        expect(fetchCommand('unsloth/Qwen3.5-2B-GGUF', 'Q4_K_M'))
            .toBe('llm/fetch-model.sh unsloth/Qwen3.5-2B-GGUF:Q4_K_M');
    });

    it('laesst den Quant weg, wenn keiner genannt ist', () => {
        expect(fetchCommand('unsloth/Qwen3.5-2B-GGUF')).toBe('llm/fetch-model.sh unsloth/Qwen3.5-2B-GGUF');
    });

    it('zeigt auf ein Skript, das es wirklich gibt', () => {
        expect(read('llm/fetch-model.sh').length).toBeGreaterThan(0);
    });
});

describe('wie die Zahlen auf dem Schirm stehen', () => {
    it('schreibt einen Anteil mit einer Nachkommastelle, wo sie etwas sagt', () => {
        expect(percentText(0.682)).toBe('68.2%');
        expect(percentText(0.932)).toBe('93.2%');
        expect(percentText(0.25)).toBe('25%');
        expect(percentText(1)).toBe('100%');
    });

    it('schreibt das Tempo als ganze Zahl', () => {
        expect(speedText(86.214)).toBe('86');
        expect(speedText(170.848)).toBe('171');
    });

    it('erfindet aus einer Nicht-Zahl keine Angabe', () => {
        expect(percentText(Number.NaN)).toBe('');
        expect(speedText(Number.NaN)).toBe('');
    });
});

/**
 * Die Saetze des Sidecar-Panels, und die eine Zusicherung, die keiner von ihnen
 * selbst halten kann.
 *
 * `LLM_MODEL_CHOICES` sagt von sich, es sei "wortgleich mit llm/start.sh". Das
 * war bis zum 2026-08-29 eine Behauptung im Docblock: die Liste stand zweimal
 * da, einmal im Shell-Skript und einmal in dieser Datei, und nichts hielt sie
 * zusammen. Befund 17 des unabhaengigen Audits hat genau diese Naht getroffen
 * (die Wahl hiess "1b" und laedt ein 2B-Modell), also wird die Naht hier
 * gemessen statt versprochen: dieser Test liest das Skript und vergleicht.
 */

import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import {
    LLM_CLASS_NOTE,
    LLM_MODEL_CHOICES,
    LLM_NOT_RUNNING_HINT,
    LLM_START_COMMAND,
    LLM_TITLE,
    llmMenuLabel,
} from './strings';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const startScript = readFileSync(join(ROOT, 'llm', 'start.sh'), 'utf8');

/**
 * Die Wahlen, die das Skript in seiner Fehlermeldung als erlaubt aufzaehlt.
 *
 * Gelesen bis zum schliessenden Anfuehrungszeichen und nicht bis zum
 * Zeilenende: dahinter steht die Umleitung nach stderr, und die gehoert dem
 * Skript und nicht der Liste.
 */
const allowedInScript = /erlaubt: ([^"]+)"/.exec(startScript)?.[1].trim() ?? '';

describe('die Modellwahlen', () => {
    it('stehen im Panel wortgleich so wie im Startskript', () => {
        expect(allowedInScript.length).toBeGreaterThan(0);
        expect(LLM_MODEL_CHOICES).toBe(allowedInScript);
    });

    it('werden vom Skript auch wirklich angenommen, jede einzelne', () => {
        for (const choice of LLM_MODEL_CHOICES.split('|').map((word) => word.trim())) {
            expect(choice.length).toBeGreaterThan(0);
            expect(startScript, `${choice} fehlt im case des Skripts`)
                .toMatch(new RegExp(`(^|\\|)\\s*${choice}[\\s|)]`, 'm'));
        }
    });

    it('nennt der Startbefehl eine Wahl, die das Skript kennt', () => {
        const choice = LLM_START_COMMAND.split(/\s+/).pop() ?? '';
        expect(LLM_MODEL_CHOICES.split('|').map((word) => word.trim())).toContain(choice);
    });

    /*
     * Befund 17, als Regel und nicht als einmalige Korrektur.
     *
     * Eine Wahl darf keine Parameterzahl behaupten, weil die Klasse am
     * Kontextfenster haengt und nicht an der Groesse: `1b` fuer Qwen3.5-2B war
     * um ein ganzes Milliarde Parameter daneben, und die naechste Umbenennung
     * soll nicht wieder in dieselbe Falle laufen.
     */
    it('behauptet in keiner Wahl eine Parameterzahl', () => {
        for (const choice of LLM_MODEL_CHOICES.split('|').map((word) => word.trim())) {
            expect(choice, `${choice} traegt eine Groessenangabe`).not.toMatch(/\d+\s*b\b/i);
        }
        expect(LLM_START_COMMAND).not.toMatch(/\b\d+b\b/i);
    });

    it('sagt trotzdem, welches Modell hinter den zwei Klassen steht', () => {
        expect(LLM_CLASS_NOTE).toContain('Qwen3.5-2B');
        expect(LLM_CLASS_NOTE).toContain('gemma-4-E4B');
        expect(LLM_CLASS_NOTE).toContain('3072');
        expect(LLM_CLASS_NOTE).toContain('8192');
        expect(LLM_NOT_RUNNING_HINT).toContain(LLM_CLASS_NOTE);
    });
});

describe('das Etikett des Menuepunktes', () => {
    /* Befund 12: jeder Eintrag der Atlas-Zeile traegt seinen Buchstaben. */
    it('traegt seinen Buchstaben in beiden Lagen', () => {
        expect(llmMenuLabel('off')).toBe('[l]ocal llm off');
        expect(llmMenuLabel('ready')).toBe('[l]ocal llm on');
        expect(llmMenuLabel('disabled-by-policy')).toBe('[l]ocal llm off');
        expect(llmMenuLabel('not-running')).toBe('[l]ocal llm on');
    });

    /*
     * Nutzerwunsch 2026-08-29: dieselbe Sache heisst an allen drei Stellen
     * gleich. Geprueft wird das hier und nicht nur im Beweislauf, weil eine
     * Sprachregelung, die nur ein Browserlauf haelt, beim naechsten Umbenennen
     * still zerfaellt.
     */
    it('nennt das Modell lokal, im Menue und in der Ueberschrift', () => {
        const withoutBrackets = (label: string): string => label.replace(/[[\]]/g, '').toLowerCase();
        expect(withoutBrackets(llmMenuLabel('off'))).toContain('local llm');
        expect(withoutBrackets(llmMenuLabel('ready'))).toContain('local llm');
        expect(LLM_TITLE.toLowerCase()).toContain('local llm');
    });
});

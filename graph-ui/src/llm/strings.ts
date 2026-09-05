/**
 * Was das Panel und die Statusleiste ueber den Sidecar sagen.
 *
 * Eigene Datei, wie twin/strings.ts und traces/bug-wizard-strings.ts, aus
 * demselben Grund: die Saetze sind der Vertrag mit dem Leser, sie werden im
 * Beweislauf woertlich gelesen, und sie sollen an einer Stelle stehen, an der
 * man sie ohne die Zustandslogik daneben pruefen kann.
 *
 * Die Regel hinter allen Saetzen: sie sagen, was IST, und im Zweifel, was nicht
 * ist. Kein "wird geladen", wenn niemand laedt; kein "nicht verfuegbar", wenn
 * einfach niemand das Skript gefahren hat.
 */

import type { SidecarState } from './sidecar';
import { SIDECAR_PORT } from './sidecar';
import { POLICY_PATH } from './policy';

/**
 * Die Ueberschrift des Panels.
 *
 * Sie hiess bis zum 2026-08-29 `LOCAL_MODEL`, waehrend der Menuepunkt `[l]lm`
 * hiess und der Statusleisten-Chip `llm`. Drei Namen fuer eine Sache, und der
 * Nutzer hat genau das gemeldet. Seit W7c heisst sie an allen drei Stellen
 * gleich: `LOCAL LLM` hier, `[l]ocal llm on/off` im Menue, `local llm` im Chip.
 * Der Unterstrich faellt mit weg; er war die Schreibweise einer Konstanten und
 * nicht die eines Namens, den jemand liest.
 */
export const LLM_TITLE = 'LOCAL LLM';

/**
 * Der konkrete Aufruf, den das Panel im Zustand `not-running` nennt.
 *
 * Er hiess bis zum 2026-08-29 `llm/start.sh 1b`, und das war Befund 17 des
 * unabhaengigen Audits: der Sieger dieser Klasse ist Qwen3.5-2B, also ein
 * 2B-Modell, und eine Beschriftung, die es "1b" nennt, sagt dem Leser eine
 * Groesse, die die Datei nicht hat. Die Klasse haengt am Kontextfenster
 * (`modelClassOf`) und nicht an der Parameterzahl, also heisst die Wahl jetzt
 * nach der Klasse und die Groesse wird nirgends behauptet.
 */
export const LLM_START_COMMAND = 'llm/start.sh class-a';

/** Der Gegenbefehl, damit die Anleitung nicht nur in eine Richtung geht. */
export const LLM_STOP_COMMAND = 'llm/stop.sh';

/** Die Modellwahlen, die das Startskript kennt. Wortgleich mit llm/start.sh. */
export const LLM_MODEL_CHOICES =
    'class-a | class-a-lfm | class-a-minicpm | class-a-coder | class-b | class-b-gemma';

/**
 * Was die zwei Klassen bedeuten, in einem Satz und mit echten Namen.
 *
 * Die Wahl heisst nach der Klasse, weil die Klasse das ist, was das Programm
 * unterscheidet. Ein Leser will trotzdem wissen, welche Datei `class-a` laedt,
 * und die Antwort darf nicht "ungefaehr 1B" lauten: sie lautet Qwen3.5-2B, mit
 * 3072 Token Kontext, und das ist nachpruefbar.
 */
export const LLM_CLASS_NOTE =
    'class-a is Qwen3.5-2B at 3072 tokens of context, class-b is gemma-4-E4B at 8192. '
    + 'The class is the context window, not a parameter count.';

/** Der Satz im Zustand `off`. Er sagt ausdruecklich, dass nichts gefragt wird. */
export const LLM_OFF_MESSAGE =
    'off. Nothing is started and nothing is probed: while this is off, this page '
    + `sends no request to 127.0.0.1:${SIDECAR_PORT}.`;

/** Was daruntersteht, damit der Schalter auffindbar ist. */
export const LLM_OFF_HINT =
    'Turn it on here or in the [a]tlas menu. Everything else on this surface keeps '
    + 'showing the same deterministic text either way.';

/** Der Satz im Zustand `not-running`: eine Anleitung, keine Fehlermeldung. */
export const LLM_NOT_RUNNING_MESSAGE =
    `on, but nothing is listening on 127.0.0.1:${SIDECAR_PORT}. This page has no backend and `
    + 'cannot start a process, so the sidecar is started from a shell:';

/** Die zweite Zeile derselben Anleitung. */
export const LLM_NOT_RUNNING_HINT =
    `Other models: ${LLM_MODEL_CHOICES}. ${LLM_CLASS_NOTE} Stop it again with ${LLM_STOP_COMMAND}.`;

/** Der Satz im Zustand `starting`. */
export const LLM_STARTING_MESSAGE =
    `a sidecar on 127.0.0.1:${SIDECAR_PORT} answers "Loading model". Waiting for it to finish.`;

/** Der Satz im Zustand `ready`, vor dem Modellnamen. */
export const LLM_READY_MESSAGE = 'ready on 127.0.0.1:' + String(SIDECAR_PORT) + '.';

/**
 * Der Satz im Zustand `disabled-by-policy`.
 *
 * Er nennt die Datei, und er nennt sie als Datei des indizierten Projekts. Ohne
 * diesen Zusatz suchte der Leser sie in diesem Repository, wo sie nicht liegt.
 */
export function llmPolicyMessage(project: string): string {
    const where = project.length > 0 ? `of the indexed project "${project}"` : 'of the indexed project';
    return `off by policy. The committed ${POLICY_PATH} ${where} says "llm": "deny", `
        + 'and a committed policy beats any preference of this browser.';
}

/** Was unter dem Policy-Satz steht, wenn die Datei zwar da, aber unlesbar war. */
export function llmPolicyDetail(detail: string): string {
    return detail.length === 0
        ? ''
        : `The switch stays visible and stays without effect. Reason: ${detail}`;
}

/** Der Wert des Statusleisten-Chips zu einer Lage. Der Beweislauf liest ihn woertlich. */
export function llmChipValue(state: SidecarState, model: string): string {
    switch (state) {
        case 'off':
            return 'off';
        case 'disabled-by-policy':
            return 'off by policy';
        case 'not-running':
            return 'not running';
        case 'starting':
            return 'starting';
        case 'ready':
            return model.length > 0 ? `ready: ${model}` : 'ready';
        default:
            return 'off';
    }
}

/** Der `title` des Menuepunktes, je nach Lage. */
export function llmMenuTitle(state: SidecarState): string {
    if (state === 'disabled-by-policy') {
        return `atlas: the local model is denied by the project's ${POLICY_PATH}; the switch has no effect`;
    }
    return state === 'off'
        ? 'atlas: turn the local model on (nothing is probed while it is off)'
        : 'atlas: turn the local model off (no request goes to the sidecar afterwards)';
}

/**
 * Das Etikett des Menuepunktes. Es traegt seinen Zustand, weil ein Extra keinen
 * hat, und seit dem 2026-08-29 auch seinen Buchstaben (Audit-Befund 12).
 *
 * Der Buchstabe steht vorn und der Zustand hinten, in beiden Lagen an derselben
 * Stelle: die zwei Etiketten sind eine Zeile, die sich nur an einem Wort
 * aendert, und ein Kuerzel, das mit der Lage die Stelle wechselte, waere ein
 * Kuerzel, das man beim Lesen suchen muss.
 *
 * Der Punkt heisst seit W7c `[l]ocal llm` und nicht mehr `[l]lm`
 * (Nutzerwunsch 2026-08-29): `llm` ist eine Abkuerzung, die nicht sagt, wo das
 * Modell laeuft, und genau das ist die Eigenschaft, um die es bei diesem
 * Schalter geht. Der Buchstabe bleibt `l`, also aendert sich kein Kuerzel und
 * keine Verdrahtung; es aendert sich, was danebensteht.
 */
export function llmMenuLabel(state: SidecarState): string {
    return state === 'off' || state === 'disabled-by-policy' ? '[l]ocal llm off' : '[l]ocal llm on';
}
